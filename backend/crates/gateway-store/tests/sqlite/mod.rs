mod account_groups;
mod admin_accounts;
mod admin_observability;
mod admin_security_audit;
mod admin_settings;
mod backup;
mod client_keys;
mod execution;
#[cfg(unix)]
mod file_permissions;
mod migrations;
mod name_keys;
mod plugin_artifacts;
mod plugin_distribution;
mod plugin_resources;
mod plugin_state;
mod provider_leases;
mod provider_state;
mod proxies;
mod retention;
mod session_cleanup;
mod value;

use std::sync::Arc;

use gateway_admin::ports::backup::{DatabaseDumpPort, DumpRequest};
use gateway_core::{
    account::{
        CredentialCasOutcome, CredentialCasUpdate, CredentialRevision, NewProviderAccount,
        OpaqueProviderData, PlaintextCredential, ProviderAccount, ProviderAccountId,
        ProviderAccountStore, ProviderAccountUpdate, QuotaAccessState, QuotaObservation,
        QuotaState, QuotaWriteOutcome,
    },
    engine::{
        ModelRequestId,
        budget::{ClientBudgetCharge, ClientBudgetPort},
    },
    health::{HealthProbe, HealthState},
    metering::Decimal,
    policy::ClientApiKeyId,
    provider_ports::{
        ProviderCooldown, ProviderCooldownKind, ProviderCooldownPort, ProviderLeasePort,
        ProviderSessionAffinityKey, ProviderSessionAffinityPort, ProviderSessionAlias,
        ProviderSessionExclusionPort,
    },
    routing::ProviderKind,
};
use gateway_store::{
    ClientAdmissionRecoveryRepository, CredentialLeaseRepository, CredentialLeaseRequest,
    CredentialLeaseScope, RuntimeSettingsRepository, RuntimeSettingsUpdate, SqliteHealthProbe,
    SqliteStoreConfig,
    backup::{sqlite_dump::SqliteDumpAdapter, staging::StagingArea},
    sqlite,
    sqlite::{
        SqliteClientAdmissionRecoveryRepository, SqliteClientBudgetStore,
        SqliteCredentialLeaseRepository, SqliteProviderAccountRepository,
        SqliteProviderCooldownRepository, SqliteProviderLeaseCoordinator,
        SqliteProviderSessionAffinityRepository, SqliteProviderSessionExclusionRepository,
    },
};

#[tokio::test]
async fn sqlite_connects_migrates_reopens_read_only_and_reports_health() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("gateway.sqlite3");
    let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .expect("create and migrate SQLite file");

    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .expect("read SQLite journal mode");
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await
        .expect("read SQLite foreign key setting");
    assert_eq!(foreign_keys, 1);
    let coordination_tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'credential_leases'",
    )
    .fetch_one(&pool)
    .await
    .expect("read migrated table");
    assert_eq!(coordination_tables, 1);

    let ledger_tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('model_requests', 'ops_events')",
    )
    .fetch_one(&pool)
    .await
    .expect("read execution ledger tables");
    assert_eq!(ledger_tables, 2);
    let admin_schema_tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN (
           'outbound_proxies', 'plugin_update_sources', 'plugin_artifacts',
           'plugin_artifact_platforms', 'plugin_source_credentials',
           'plugin_artifact_credentials', 'plugin_instances', 'plugin_instance_secrets',
           'plugin_version_configurations', 'plugin_state_generations', 'plugin_state_records',
           'authorization_receipts', 'plugin_group_resources', 'plugin_key_resources',
           'backup_settings', 'backup_records'
         )",
    )
    .fetch_one(&pool)
    .await
    .expect("read migrated admin schema tables");
    assert_eq!(admin_schema_tables, 16);
    let cost_storage_type: String = sqlx::query_scalar(
        "SELECT type FROM pragma_table_info('model_requests') WHERE name = 'cost_amount'",
    )
    .fetch_one(&pool)
    .await
    .expect("read cost storage type");
    assert_eq!(cost_storage_type, "TEXT");
    let notes_column_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pragma_table_info('provider_accounts') WHERE name = 'notes'",
    )
    .fetch_one(&pool)
    .await
    .expect("read provider account notes column");
    assert_eq!(notes_column_count, 1);
    let fast_limit_column_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pragma_table_info('runtime_settings') WHERE name = 'disable_fast'",
    )
    .fetch_one(&pool)
    .await
    .expect("read runtime fast-limit column");
    assert_eq!(fast_limit_column_count, 1);

    let response_id_storage_type: String = sqlx::query_scalar(
        "SELECT type FROM pragma_table_info('model_requests') WHERE name = 'client_response_id'",
    )
    .fetch_one(&pool)
    .await
    .expect("read opaque response ID storage type");
    assert_eq!(response_id_storage_type, "BLOB");

    let probe = SqliteHealthProbe::new(pool.clone(), 5);
    assert_eq!(probe.name(), "sqlite");
    assert_eq!(probe.check().await, HealthState::Healthy);
    pool.close().await;

    let read_only = sqlite::connect_read_only(&path, &SqliteStoreConfig::default())
        .await
        .expect("open existing SQLite file read-only");
    assert!(
        sqlx::query("INSERT INTO provider_cooldowns VALUES ('a', 'test', 1, 1)")
            .execute(&read_only)
            .await
            .is_err()
    );
    read_only.close().await;
}

#[tokio::test]
async fn sqlite_busy_timeout_bounds_waiting_for_a_second_writer() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("busy-timeout.sqlite3");
    let config = SqliteStoreConfig {
        max_connections: 1,
        acquire_timeout_seconds: 1,
        busy_timeout_ms: 100,
        ..SqliteStoreConfig::default()
    };
    let first_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create SQLite database");
    let second_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open independent SQLite pool");
    let mut transaction = first_pool.begin().await.expect("begin first writer");
    sqlx::query("update cpr_store_write_lock set revision = revision + 1 where id = 1")
        .execute(&mut *transaction)
        .await
        .expect("hold SQLite write lock");

    let started = std::time::Instant::now();
    let result =
        sqlx::query("update cpr_store_write_lock set revision = revision + 1 where id = 1")
            .execute(&second_pool)
            .await;
    let elapsed = started.elapsed();
    assert!(
        result.is_err(),
        "second writer must time out while lock is held"
    );
    assert!(
        elapsed >= std::time::Duration::from_millis(75),
        "elapsed: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "elapsed: {elapsed:?}"
    );

    transaction.rollback().await.expect("release first writer");
    first_pool.close().await;
    second_pool.close().await;
}

#[tokio::test]
async fn sqlite_runtime_snapshot_loads_settings_keys_and_group_bindings_consistently() {
    use gateway_core::routing::snapshot::SnapshotStorePort;
    use gateway_store::RuntimeSnapshotRepository;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("runtime-snapshot.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite snapshot database");
    let group_id = "grp_0123456789abcdef0123456789abcdef";
    let key_id = "key_snapshot";
    let key = format!("sk_{}", "a".repeat(43));
    sqlx::query(
        "insert into account_groups (id, name, created_at_us, updated_at_us)
         values (?1, 'snapshot group', 1, 1)",
    )
    .bind(group_id)
    .execute(&pool)
    .await
    .expect("insert snapshot group");
    sqlx::query(
        "insert into client_api_keys
         (id, enabled, name, key, max_concurrency, requests_per_minute, created_at_us, updated_at_us)
         values (?1, 1, 'snapshot key', ?2, 7, 45, 1, 1)",
    )
    .bind(key_id)
    .bind(&key)
    .execute(&pool)
    .await
    .expect("insert snapshot key");
    sqlx::query(
        "insert into client_api_key_groups (client_api_key_id, account_group_id, created_at_us)
         values (?1, ?2, 1)",
    )
    .bind(key_id)
    .bind(group_id)
    .execute(&pool)
    .await
    .expect("bind key to group");

    let repository = gateway_store::sqlite::SqliteRuntimeSnapshotRepository::new(pool.clone());
    let snapshot = RuntimeSnapshotRepository::load_runtime_snapshot(&repository)
        .await
        .expect("load one consistent SQLite snapshot");
    assert_eq!(snapshot.config_revision.get(), 1);
    assert_eq!(snapshot.observed_current_revision.get(), 1);
    assert_eq!(snapshot.settings.max_concurrent_per_account, 3);
    assert_eq!(snapshot.settings.request_location.city, "Piketon");
    assert_eq!(snapshot.client_api_keys.len(), 1);
    assert_eq!(snapshot.client_api_keys[0].id.as_str(), key_id);
    assert_eq!(snapshot.client_api_keys[0].limits.max_concurrency, 7);
    assert_eq!(snapshot.client_api_keys[0].group_ids.len(), 1);
    assert_eq!(snapshot.account_groups.len(), 1);
    assert_eq!(snapshot.account_groups[0].id.as_str(), group_id);
    assert!(
        SnapshotStorePort::load_snapshot_facts(&repository)
            .await
            .is_ok()
    );

    sqlx::query("update runtime_settings set config_revision = 2 where id = 1")
        .execute(&pool)
        .await
        .expect("publish next config revision");
    assert_eq!(
        SnapshotStorePort::current_config_revision(&repository)
            .await
            .expect("read current revision")
            .get(),
        2
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_runtime_settings_update_atomically_and_fence_provider_profiles_by_revision() {
    use std::collections::BTreeMap;

    use gateway_core::{
        account::{OpaqueProviderData, SmartSchedulingConfig},
        provider_ports::ProviderRuntimePolicyPort,
        routing::{ConfigRevision, ProviderKind},
    };

    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("runtime-settings.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite settings database");
    let repository = gateway_store::sqlite::SqliteRuntimeSettingsRepository::new(pool.clone());
    let defaults = repository.load_runtime_settings().await.unwrap();
    assert_eq!(defaults.config_revision.get(), 1);
    assert_eq!(
        defaults.openai_account_affinity,
        gateway_core::account::AccountAffinity::Strict
    );
    assert_eq!(defaults.max_account_rotations, 3);
    assert_eq!(defaults.openai_session_affinity_ttl_hours, 24);
    assert_eq!(defaults.account_auto_freeze_threshold, 12);
    assert_eq!(defaults.account_auto_freeze_window_seconds, 600);
    assert_eq!(defaults.account_auto_freeze_duration_seconds, 7_200);
    assert_eq!(defaults.account_warmup_schedule_time, "08:00");

    let provider = ProviderKind::new("example").unwrap();
    let initial_profile = OpaqueProviderData::new(
        serde_json::json!({"setting":"from-update"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let make_update = |refresh_margin_seconds, request_profile_updates| RuntimeSettingsUpdate {
        codex_privacy_policy: Default::default(),
        request_profile_updates,
        request_location_enabled: false,
        request_location: Default::default(),
        refresh_margin_seconds,
        refresh_concurrency: 2,
        max_concurrent_per_account: 3,
        request_interval_ms: 50,
        max_waiting_per_key: 0,
        max_waiting_per_account: 0,
        concurrency_wait_timeout_seconds: 30,
        openai_guardian_reserved_concurrency: 0,
        openai_account_affinity: gateway_core::account::AccountAffinity::Relaxed,
        max_account_rotations: 3,
        openai_session_affinity_ttl_hours: 24,
        responses_max_decompressed_body_bytes: 64 * 1024 * 1024,
        smart_scheduling: SmartSchedulingConfig::default(),
        rotation_strategy: "smart".to_owned(),
        model_mappings: BTreeMap::new(),
        min_codex_desktop_version: None,
        min_codex_cli_version: None,
        usage_retention_days: 31,
        ops_event_retention_days: 30,
        audit_retention_days: 90,
        account_auto_freeze_enabled: false,
        account_auto_freeze_threshold: 12,
        account_auto_freeze_window_seconds: 600,
        account_auto_freeze_duration_seconds: 7_200,
        account_auto_freeze_probe_enabled: true,
        account_auto_freeze_probe_model: None,
        account_auto_freeze_adaptive_concurrency: true,
        account_warmup_enabled: false,
        account_warmup_schedule_time: "08:00".to_owned(),
        account_warmup_model: None,
    };
    let revision = repository
        .update_runtime_settings(make_update(
            3_600,
            BTreeMap::from([(provider.clone(), Some(initial_profile.clone()))]),
        ))
        .await
        .unwrap();
    assert_eq!(revision.get(), 2);
    let saved = repository.load_runtime_settings().await.unwrap();
    assert_eq!(saved.config_revision, revision);
    assert_eq!(
        saved.request_profiles.get(&provider),
        Some(&initial_profile)
    );
    assert!(saved.updated_at.timestamp_micros() > 0);

    let initialized = ProviderRuntimePolicyPort::initialize_request_profile(
        &repository,
        &provider,
        OpaqueProviderData::new(
            serde_json::json!({"setting":"must-not-replace"})
                .as_object()
                .unwrap()
                .clone(),
        ),
    )
    .await
    .unwrap();
    assert_eq!(initialized, initial_profile);
    let profile_values = ProviderRuntimePolicyPort::load_request_profile_configurations(
        &repository,
        ConfigRevision::new(2).unwrap(),
        &provider,
    )
    .await
    .unwrap();
    assert_eq!(profile_values, vec![initial_profile]);

    repository
        .update_runtime_settings(make_update(3_700, BTreeMap::new()))
        .await
        .unwrap();
    assert!(
        ProviderRuntimePolicyPort::load_request_profile_configurations(
            &repository,
            ConfigRevision::new(2).unwrap(),
            &provider,
        )
        .await
        .is_err()
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_admission_recovery_reads_recent_and_running_requests_across_pools() {
    use chrono::{Duration, Utc};
    use gateway_core::engine::admission::ClientAdmissionRecoveryPort;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("admission-recovery.sqlite3");
    let config = SqliteStoreConfig::default();
    let writer = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create SQLite database");
    let reader = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open an independent SQLite pool");
    let now = Utc::now();
    let since = now - Duration::seconds(60);

    let insert = |id: &'static str,
                  started_at: chrono::DateTime<Utc>,
                  deadline_at: chrono::DateTime<Utc>,
                  outcome: &'static str,
                  completed_at: Option<chrono::DateTime<Utc>>| {
        let writer = writer.clone();
        async move {
            sqlx::query(
                "insert into model_requests (
               id, client_api_key_ref, operation, client_transport, started_at_us, deadline_at_us, completed_at_us, outcome, request_observation_json
             ) values (
               ?1, 'key_recovery', 'responses', 'http', ?2, ?3, ?4, ?5,
               json_object('request', json_object('configRevision', 1, 'protocol', 'openai', 'endpoint', '/v1/responses', 'compact', json('false')), 'routing', json_object('scope', 'all', 'groupRefs', json('[]'), 'groupNamesSnapshot', json('[]')))
             )",
            )
            .bind(id)
            .bind(started_at.timestamp_micros())
            .bind(deadline_at.timestamp_micros())
            .bind(completed_at.map(|value| value.timestamp_micros()))
            .bind(outcome)
            .execute(&writer)
            .await
            .expect("insert recovery fixture");
        }
    };
    insert(
        "req_recent_running",
        now - Duration::seconds(5),
        now + Duration::seconds(30),
        "running",
        None,
    )
    .await;
    insert(
        "req_recent_completed",
        now - Duration::seconds(10),
        now,
        "succeeded",
        Some(now - Duration::seconds(1)),
    )
    .await;
    insert(
        "req_old_running",
        since - Duration::seconds(30),
        now + Duration::seconds(60),
        "running",
        None,
    )
    .await;

    let repository = SqliteClientAdmissionRecoveryRepository::new(reader.clone());
    let recoveries = repository
        .load_client_admission_recovery(since)
        .await
        .expect("load recovery facts through the second pool");
    assert_eq!(recoveries.len(), 1);
    assert_eq!(recoveries[0].client_api_key_ref, "key_recovery");
    assert_eq!(recoveries[0].recent_requests.len(), 2);
    assert_eq!(recoveries[0].running_requests.len(), 2);
    let core_recoveries = ClientAdmissionRecoveryPort::load_recovery(&repository, since.into())
        .await
        .expect("map records to Core admission recovery facts");
    assert_eq!(core_recoveries.len(), 1);
    assert_eq!(core_recoveries[0].running_requests.len(), 2);
    reader.close().await;
    writer.close().await;
}

#[tokio::test]
async fn sqlite_runtime_changes_discover_external_revision_updates_by_polling() {
    use futures::StreamExt;
    use gateway_core::runtime::SnapshotSubscriptionPort;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("runtime-change.sqlite3");
    let config = SqliteStoreConfig::default();
    let writer = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create SQLite database");
    let observer_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open independent SQLite pool");
    let observer = gateway_store::sqlite::SqliteRuntimeChangeRepository::new(observer_pool.clone());
    let mut revisions = SnapshotSubscriptionPort::subscribe_snapshot_revisions(&observer)
        .await
        .expect("subscribe to SQLite revision changes");

    sqlx::query("update runtime_settings set config_revision = config_revision + 1 where id = 1")
        .execute(&writer)
        .await
        .expect("commit change through the other pool");
    let revision = tokio::time::timeout(std::time::Duration::from_secs(3), revisions.next())
        .await
        .expect("revision poll completes within three seconds")
        .expect("revision stream remains open")
        .expect("read external revision change");
    assert_eq!(revision.get(), 2);

    writer.close().await;
    observer_pool.close().await;
}

#[tokio::test]
async fn sqlite_connects_when_filename_has_no_parent_directory() {
    if std::env::var_os("CPR_SQLITE_RELATIVE_PATH_TEST_CHILD").is_none() {
        let root = tempfile::tempdir().expect("isolated working directory");
        let status =
            std::process::Command::new(std::env::current_exe().expect("current test executable"))
                .current_dir(root.path())
                .args([
                    "--exact",
                    "sqlite::sqlite_connects_when_filename_has_no_parent_directory",
                ])
                .env("CPR_SQLITE_RELATIVE_PATH_TEST_CHILD", "1")
                .status()
                .expect("run isolated relative path test");
        assert!(status.success(), "child SQLite path test failed");
        return;
    }

    let path = std::path::Path::new("gateway.sqlite3");
    let pool = sqlite::connect_and_migrate(path, &SqliteStoreConfig::default())
        .await
        .expect("create database file in the current directory");
    pool.close().await;
    assert!(path.is_file());
}

#[tokio::test]
async fn sqlite_read_only_open_does_not_create_a_missing_database() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("missing.sqlite3");

    assert!(
        sqlite::connect_read_only(&path, &SqliteStoreConfig::default())
            .await
            .is_err()
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn sqlite_dump_publishes_a_hashed_standalone_database_and_cleans_cancelled_work() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let database_path = root.path().join("gateway.sqlite3");
    let pool = sqlite::connect_and_migrate(&database_path, &SqliteStoreConfig::default())
        .await
        .expect("create SQLite file");
    sqlx::query("CREATE TABLE snapshot_fixture (value TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create snapshot fixture");
    sqlx::query("INSERT INTO snapshot_fixture (value) VALUES ('before')")
        .execute(&pool)
        .await
        .expect("insert snapshot fixture");
    let staging = Arc::new(
        StagingArea::open(root.path().join("staging"), 16 * 1024 * 1024)
            .expect("create backup staging directory"),
    );
    let dump = SqliteDumpAdapter::new(pool.clone(), staging);
    let request = DumpRequest {
        backup_id: "snapshot-1".to_owned(),
        cancellation: gateway_core::lifecycle::CancellationToken::new(),
    };
    let artifact = dump.dump(request).await.expect("create SQLite snapshot");
    assert_eq!(
        artifact.path.extension().and_then(|ext| ext.to_str()),
        Some("sqlite3")
    );
    assert_eq!(artifact.sha256.len(), 64);
    assert_eq!(
        tokio::fs::metadata(&artifact.path).await.unwrap().len(),
        artifact.size_bytes
    );
    let inspected = dump
        .inspect_staging("snapshot-1")
        .await
        .unwrap()
        .expect("inspect completed snapshot");
    assert_eq!(inspected.sha256, artifact.sha256);

    sqlx::query("UPDATE snapshot_fixture SET value = 'after'")
        .execute(&pool)
        .await
        .expect("mutate source after snapshot");
    let restored = sqlite::connect_read_only(&artifact.path, &SqliteStoreConfig::default())
        .await
        .expect("open standalone snapshot");
    let value: String = sqlx::query_scalar("SELECT value FROM snapshot_fixture")
        .fetch_one(&restored)
        .await
        .expect("read snapshot data");
    assert_eq!(value, "before");
    restored.close().await;

    let cancellation = gateway_core::lifecycle::CancellationToken::new();
    cancellation.cancel();
    assert!(
        dump.dump(DumpRequest {
            backup_id: "cancelled".to_owned(),
            cancellation,
        })
        .await
        .is_err()
    );
    assert!(dump.inspect_staging("cancelled").await.unwrap().is_none());
}

#[tokio::test]
async fn sqlite_provider_leases_are_shared_and_fenced_across_independent_pools() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("coordination.sqlite3");
    let config = SqliteStoreConfig::default();
    let first_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create first SQLite pool");
    let second_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open second SQLite pool");
    let first = SqliteCredentialLeaseRepository::new(first_pool.clone());
    let second = SqliteCredentialLeaseRepository::new(second_pool.clone());
    let first_request = CredentialLeaseRequest {
        scope: CredentialLeaseScope::ProviderAccount,
        resource_id: "account-1".to_owned(),
        owner_id: "gateway-a".to_owned(),
        ttl: std::time::Duration::from_secs(5),
    };
    let competing_request = CredentialLeaseRequest {
        owner_id: "gateway-b".to_owned(),
        ..first_request.clone()
    };

    let grant = first
        .acquire_credential_lease(&first_request)
        .await
        .unwrap()
        .expect("first process acquires lease");
    assert!(
        second
            .acquire_credential_lease(&competing_request)
            .await
            .unwrap()
            .is_none()
    );
    let signals = second
        .credential_runtime_signals(&["account-1".to_owned()])
        .await
        .unwrap();
    assert_eq!(signals[0].in_flight, 1);
    assert!(signals[0].last_started_at.is_some());

    let renewed = second
        .renew_credential_lease(&first_request, &grant)
        .await
        .unwrap()
        .expect("second process renews matching owner and fence");
    assert!(renewed.expires_at >= grant.expires_at);
    assert!(
        second
            .renew_credential_lease(&competing_request, &grant)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        first
            .release_credential_lease(&first_request, &renewed)
            .await
            .unwrap()
    );

    let next = second
        .acquire_credential_lease(&competing_request)
        .await
        .unwrap()
        .expect("lease can be claimed after release");
    assert!(next.fencing_token > grant.fencing_token);
    assert!(
        second
            .release_credential_lease(&competing_request, &next)
            .await
            .unwrap()
    );

    let expiring_request = CredentialLeaseRequest {
        resource_id: "account-expiring".to_owned(),
        ttl: std::time::Duration::from_millis(15),
        ..first_request
    };
    let expiring = first
        .acquire_credential_lease(&expiring_request)
        .await
        .unwrap()
        .expect("short lease acquired");
    tokio::time::sleep(std::time::Duration::from_millis(35)).await;
    assert!(
        first
            .renew_credential_lease(&expiring_request, &expiring)
            .await
            .unwrap()
            .is_none()
    );
    let reclaimed = second
        .acquire_credential_lease(&CredentialLeaseRequest {
            owner_id: "gateway-b".to_owned(),
            ..expiring_request.clone()
        })
        .await
        .unwrap()
        .expect("expired lease can be reclaimed");
    assert!(reclaimed.fencing_token > expiring.fencing_token);

    first_pool.close().await;
    second_pool.close().await;
}

#[tokio::test]
async fn sqlite_concurrent_lease_claims_have_one_winner_across_pools() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("race.sqlite3");
    let config = SqliteStoreConfig::default();
    let first_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create first SQLite pool");
    let second_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open second SQLite pool");
    let first = SqliteCredentialLeaseRepository::new(first_pool.clone());
    let second = SqliteCredentialLeaseRepository::new(second_pool.clone());
    let request_a = CredentialLeaseRequest {
        scope: CredentialLeaseScope::ProviderTask,
        resource_id: "worker-scheduled-backup".to_owned(),
        owner_id: "process-a".to_owned(),
        ttl: std::time::Duration::from_secs(2),
    };
    let request_b = CredentialLeaseRequest {
        owner_id: "process-b".to_owned(),
        ..request_a.clone()
    };
    let (a, b) = tokio::join!(
        first.acquire_credential_lease(&request_a),
        second.acquire_credential_lease(&request_b),
    );
    let acquired = usize::from(a.unwrap().is_some()) + usize::from(b.unwrap().is_some());
    assert_eq!(acquired, 1);

    first_pool.close().await;
    second_pool.close().await;
}

#[tokio::test]
async fn sqlite_session_affinity_cas_and_exclusions_are_visible_across_pools() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("sessions.sqlite3");
    let config = SqliteStoreConfig::default();
    let first_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create first SQLite pool");
    let second_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open second SQLite pool");
    let first_affinity = SqliteProviderSessionAffinityRepository::new(first_pool.clone());
    let second_affinity = SqliteProviderSessionAffinityRepository::new(second_pool.clone());
    let first_exclusion = SqliteProviderSessionExclusionRepository::new(first_pool.clone());
    let second_exclusion = SqliteProviderSessionExclusionRepository::new(second_pool.clone());
    let kind = ProviderKind::new("example").unwrap();
    let key = ProviderSessionAffinityKey::try_new("session-fingerprint").unwrap();
    let account_a = ProviderAccountId::new("acct_a").unwrap();
    let account_b = ProviderAccountId::new("acct_b").unwrap();
    let ttl = std::time::Duration::from_secs(60);

    let initial = first_affinity
        .compare_and_bind(&kind, &key, None, &account_a, ttl)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second_affinity.load(&kind, &key).await.unwrap(),
        Some(initial.clone())
    );
    assert_eq!(
        second_affinity
            .compare_and_bind(&kind, &key, Some(&initial), &account_a, ttl)
            .await
            .unwrap(),
        Some(initial.clone()),
    );
    assert!(
        second_affinity
            .compare_and_bind(&kind, &key, None, &account_b, ttl)
            .await
            .unwrap()
            .is_none()
    );
    let migrated = second_affinity
        .compare_and_bind(&kind, &key, Some(&initial), &account_b, ttl)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(initial.revision(), migrated.revision());
    assert_eq!(
        first_affinity.load(&kind, &key).await.unwrap(),
        Some(migrated.clone()),
    );
    assert!(
        first_affinity
            .compare_and_bind(&kind, &key, Some(&initial), &account_a, ttl)
            .await
            .unwrap()
            .is_none()
    );

    let alias_key = ProviderSessionAffinityKey::try_new("observed-request").unwrap();
    let alias = ProviderSessionAlias {
        session_key: key.clone(),
        root_session_key: Some(ProviderSessionAffinityKey::try_new("root-session").unwrap()),
        follow_only: true,
    };
    assert!(
        first_affinity
            .bind_alias(&kind, &alias_key, &alias, ttl)
            .await
            .unwrap()
    );
    assert_eq!(
        second_affinity.load_alias(&kind, &alias_key).await.unwrap(),
        Some(alias.clone()),
    );
    assert!(
        second_affinity
            .bind_alias(&kind, &alias_key, &alias, ttl)
            .await
            .unwrap()
    );
    assert!(
        !second_affinity
            .bind_alias(
                &kind,
                &alias_key,
                &ProviderSessionAlias {
                    session_key: ProviderSessionAffinityKey::try_new("different-session").unwrap(),
                    root_session_key: None,
                    follow_only: false,
                },
                ttl,
            )
            .await
            .unwrap()
    );

    let first_state = first_exclusion
        .record_failure(&kind, &key, &account_a, ttl)
        .await
        .unwrap();
    let second_state = second_exclusion
        .record_failure(&kind, &key, &account_b, ttl)
        .await
        .unwrap();
    assert_ne!(first_state.revision(), second_state.revision());
    assert_eq!(second_state.excluded_accounts().len(), 2);
    assert!(
        !first_exclusion
            .clear(&kind, &key, first_state.revision())
            .await
            .unwrap()
    );
    assert!(
        second_exclusion
            .clear(&kind, &key, second_state.revision())
            .await
            .unwrap()
    );
    assert!(first_exclusion.load(&kind, &key).await.unwrap().is_none());

    first_pool.close().await;
    second_pool.close().await;
}

#[tokio::test]
async fn sqlite_cooldowns_and_capacity_evidence_are_revision_safe_across_pools() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("cooldowns.sqlite3");
    let config = SqliteStoreConfig::default();
    let first_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create first SQLite pool");
    let second_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open second SQLite pool");
    let first = SqliteProviderCooldownRepository::new(first_pool.clone());
    let second = SqliteProviderCooldownRepository::new(second_pool.clone());
    let account = ProviderAccountId::new("acct_cooldown").unwrap();
    let revision_one = gateway_core::account::CredentialRevision::new(1).unwrap();
    let revision_two = gateway_core::account::CredentialRevision::new(2).unwrap();
    let now_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let until = std::time::UNIX_EPOCH + std::time::Duration::from_secs(now_seconds + 30);
    let cooldown = ProviderCooldown::new(account.clone(), revision_one, until);

    assert!(first.put_if_later(cooldown.clone()).await.unwrap());
    assert!(
        !second
            .put_if_later(ProviderCooldown::new(
                account.clone(),
                revision_one,
                until - std::time::Duration::from_secs(1),
            ))
            .await
            .unwrap()
    );
    assert_eq!(second.read(&account).await.unwrap(), Some(cooldown));

    assert_eq!(
        first
            .record_capacity_failure(&account, std::time::Duration::from_secs(5), 3)
            .await
            .unwrap(),
        1,
    );
    assert_eq!(
        second
            .record_capacity_failure(&account, std::time::Duration::from_secs(5), 5)
            .await
            .unwrap(),
        2,
    );
    assert_eq!(
        first.capacity_peak_in_flight(&account).await.unwrap(),
        Some(5)
    );
    second
        .clear_after_success(&account, revision_one)
        .await
        .unwrap();
    assert_eq!(first.read(&account).await.unwrap(), None);
    assert_eq!(
        second.capacity_peak_in_flight(&account).await.unwrap(),
        None
    );

    let freeze = ProviderCooldown::new_with_kind(
        account.clone(),
        revision_two,
        until,
        ProviderCooldownKind::CapacityFreeze,
    );
    assert!(first.put_if_later(freeze.clone()).await.unwrap());
    assert!(!second.clear(&account, revision_one).await.unwrap());
    second
        .clear_after_success(&account, revision_two)
        .await
        .unwrap();
    assert_eq!(first.read(&account).await.unwrap(), Some(freeze));
    assert!(second.clear(&account, revision_two).await.unwrap());

    first_pool.close().await;
    second_pool.close().await;
}

#[tokio::test]
async fn sqlite_provider_runtime_caches_are_local_and_apply_revision_and_artifact_fences() {
    use std::time::{Duration, SystemTime};

    use gateway_core::{
        account::{CredentialRevision, CredentialState, OpaqueProviderData, ProviderAccountId},
        provider_ports::{
            ProviderArtifactProfile, ProviderArtifactProfileCachePort, ProviderCatalogCacheKey,
            ProviderCatalogCachePort, ProviderCatalogScope, ProviderCredentialState,
            ProviderCredentialStatePort, ProviderStoreErrorKind,
        },
        routing::ProviderKind,
    };
    use gateway_store::sqlite::SqliteProviderRuntimeCache;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("provider-cache.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite cache database");
    let cache = SqliteProviderRuntimeCache::new(pool.clone());
    let account_id = ProviderAccountId::new("acct_cache-account").expect("account ID");
    let state = |revision, enabled| {
        ProviderCredentialState::new(
            account_id.clone(),
            CredentialRevision::new(revision).expect("credential revision"),
            enabled,
            CredentialState::Ready,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
        )
    };

    ProviderCredentialStatePort::replace(&cache, state(1, true))
        .await
        .expect("cache state");
    ProviderCredentialStatePort::replace(&cache, state(1, false))
        .await
        .expect("ignore same revision");
    assert_eq!(
        ProviderCredentialStatePort::read(&cache, &account_id)
            .await
            .expect("read credential state"),
        Some(state(1, true))
    );
    ProviderCredentialStatePort::replace(&cache, state(2, false))
        .await
        .expect("replace newer state");
    assert_eq!(
        ProviderCredentialStatePort::read(&cache, &account_id)
            .await
            .expect("read newer state"),
        Some(state(2, false))
    );
    assert!(cache.clear(&account_id).await.expect("clear cache"));
    assert!(
        ProviderCredentialStatePort::read(&cache, &account_id)
            .await
            .expect("read cleared cache")
            .is_none()
    );

    let provider = ProviderKind::new("openai").expect("provider kind");
    let catalog_key = ProviderCatalogCacheKey::new(
        provider.clone(),
        ProviderCatalogScope::new("pro").expect("catalog scope"),
    );
    let catalog = OpaqueProviderData::new(
        serde_json::json!({"models":["gpt-test"]})
            .as_object()
            .expect("JSON object")
            .clone(),
    );
    ProviderCatalogCachePort::replace(&cache, &catalog_key, &catalog, Duration::from_secs(30))
        .await
        .expect("replace provider catalog");
    assert_eq!(
        ProviderCatalogCachePort::read(&cache, &catalog_key)
            .await
            .expect("read provider catalog"),
        Some(catalog)
    );

    let profile = |sequence, version: &str| {
        ProviderArtifactProfile::new(
            provider.clone(),
            "desktop-linux-x64".to_owned(),
            sequence,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000),
            OpaqueProviderData::new(
                serde_json::json!({"version":version})
                    .as_object()
                    .expect("JSON object")
                    .clone(),
            ),
        )
    };
    let current = profile(2, "2.0.0");
    assert!(
        cache
            .replace_if_newer(current.clone(), Duration::from_secs(30))
            .await
            .expect("replace current artifact profile")
    );
    assert!(
        !cache
            .replace_if_newer(profile(1, "1.0.0"), Duration::from_secs(30))
            .await
            .expect("reject old artifact profile")
    );
    assert_eq!(
        ProviderArtifactProfileCachePort::read(&cache, &provider, "desktop-linux-x64")
            .await
            .unwrap(),
        Some(current)
    );
    assert_eq!(
        cache
            .replace_if_newer(profile(2, "different"), Duration::from_secs(30))
            .await
            .expect_err("same sequence content change must conflict")
            .kind(),
        ProviderStoreErrorKind::Conflict
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_refresh_backoff_is_shared_across_pools_and_expires_as_a_sliding_window() {
    use std::time::Duration;

    use gateway_core::{account::ProviderAccountId, provider_ports::ProviderCredentialStatePort};
    use gateway_store::sqlite::SqliteProviderRuntimeCache;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("refresh-backoff.sqlite3");
    let config = SqliteStoreConfig::default();
    let first_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("create SQLite database");
    let second_pool = sqlite::connect_and_migrate(&path, &config)
        .await
        .expect("open second SQLite pool");
    let first = SqliteProviderRuntimeCache::new(first_pool.clone());
    let second = SqliteProviderRuntimeCache::new(second_pool.clone());
    let account_id = ProviderAccountId::new("acct_refresh-account").expect("account ID");
    let window = Duration::from_millis(30);

    assert_eq!(
        first
            .record_refresh_backoff(&account_id, window)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        second
            .record_refresh_backoff(&account_id, window)
            .await
            .unwrap(),
        2
    );
    tokio::time::sleep(window + Duration::from_millis(10)).await;
    assert_eq!(
        first
            .record_refresh_backoff(&account_id, window)
            .await
            .unwrap(),
        1
    );
    second
        .clear_refresh_backoff(&account_id)
        .await
        .expect("clear refresh backoff");
    assert_eq!(
        first
            .record_refresh_backoff(&account_id, window)
            .await
            .unwrap(),
        1
    );

    first_pool.close().await;
    second_pool.close().await;
}

#[tokio::test]
async fn sqlite_provider_account_core_store_persists_credentials_with_revision_cas() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("provider-accounts.sqlite3");
    let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .expect("create and migrate SQLite file");
    let store = SqliteProviderAccountRepository::new(pool.clone());
    let account_id = ProviderAccountId::new("acct_sqlite".to_owned()).unwrap();
    let provider = ProviderKind::new("example").unwrap();
    let account = ProviderAccount::new(
        account_id.clone(),
        provider.clone(),
        "SQLite account".to_owned(),
        Some("upstream-user".to_owned()),
        "api_key".to_owned(),
        CredentialRevision::new(1).unwrap(),
        None,
    );
    let credential = |value: &str| {
        PlaintextCredential::new(
            serde_json::json!({"token": value})
                .as_object()
                .unwrap()
                .clone(),
        )
    };
    store
        .create_account(NewProviderAccount {
            account,
            model_access: None,
            credential: credential("initial"),
        })
        .await
        .expect("persist new account");

    let loaded = store
        .load_current_credential(&account_id)
        .await
        .expect("load account and credential");
    assert_eq!(loaded.account.revision().get(), 1);
    assert_eq!(
        loaded.credential.expose_to_provider().get("token").unwrap(),
        "initial"
    );

    let update = CredentialCasUpdate::new(
        account_id.clone(),
        CredentialRevision::new(1).unwrap(),
        ProviderAccountUpdate {
            account_id: account_id.clone(),
            name: "SQLite account".to_owned(),
            email: None,
            plan_type: Some("pro".to_owned()),
        },
        credential("rotated"),
        false,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        store.compare_and_swap_credential(update).await.unwrap(),
        CredentialCasOutcome::Updated(CredentialRevision::new(2).unwrap())
    );
    let stale = CredentialCasUpdate::new(
        account_id.clone(),
        CredentialRevision::new(1).unwrap(),
        ProviderAccountUpdate {
            account_id: account_id.clone(),
            name: "stale".to_owned(),
            email: None,
            plan_type: None,
        },
        credential("stale"),
        false,
        None,
        None,
    )
    .unwrap();
    assert_eq!(
        store.compare_and_swap_credential(stale).await.unwrap(),
        CredentialCasOutcome::Conflict
    );

    let quota = QuotaObservation {
        account_id: account_id.clone(),
        expected_revision: CredentialRevision::new(2).unwrap(),
        quota: OpaqueProviderData::new(
            serde_json::json!({"remaining": 4})
                .as_object()
                .unwrap()
                .clone(),
        ),
        plan_type: Some("pro".to_owned()),
        observed_at: std::time::SystemTime::now(),
        state: QuotaState::allowed(std::time::SystemTime::now()),
    };
    assert_eq!(
        store.compare_and_swap_quota(quota).await.unwrap(),
        QuotaWriteOutcome::Updated
    );
    let quotas = store
        .get_quotas(std::slice::from_ref(&account_id))
        .await
        .unwrap();
    assert_eq!(quotas.len(), 1);
    assert_eq!(quotas[0].state.access(), QuotaAccessState::Allowed);
    assert_eq!(
        quotas[0]
            .quota
            .expose_to_provider()
            .get("remaining")
            .unwrap(),
        4
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_client_budgets_sum_exactly_and_ignore_duplicate_charges() {
    use std::str::FromStr;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("client-budgets.sqlite3");
    let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .expect("create and migrate SQLite file");
    sqlx::query(
        "insert into client_api_keys (id, enabled, daily_limit_usd, weekly_limit_usd)
         values (?1, 1, ?2, ?3)",
    )
    .bind("key-budget")
    .bind(gateway_store::sqlite::value::encode_amount(
        Decimal::from_str("5").unwrap(),
    ))
    .bind(gateway_store::sqlite::value::encode_amount(
        Decimal::from_str("0").unwrap(),
    ))
    .execute(&pool)
    .await
    .expect("insert budgeted key");

    let store = SqliteClientBudgetStore::new(pool.clone(), Default::default());
    let key_id = ClientApiKeyId::new("key-budget").unwrap();
    store
        .admit(key_id.clone())
        .await
        .expect("admit under budget");
    let first = ClientBudgetCharge {
        key_id: key_id.clone(),
        request_id: ModelRequestId::new("req_budget_1").unwrap(),
        amount_usd: Decimal::from_str("3.0000000001").unwrap(),
        completed_at: std::time::SystemTime::now(),
    };
    store
        .settle(first.clone())
        .await
        .expect("settle first charge");
    store
        .settle(first)
        .await
        .expect("duplicate charge is idempotent");
    store
        .admit(key_id.clone())
        .await
        .expect("three dollars remains under limit");
    store
        .settle(ClientBudgetCharge {
            key_id: key_id.clone(),
            request_id: ModelRequestId::new("req_budget_2").unwrap(),
            amount_usd: Decimal::from_str("2.0000000000").unwrap(),
            completed_at: std::time::SystemTime::now(),
        })
        .await
        .expect("settle second charge");
    assert!(
        store.admit(key_id).await.is_err(),
        "limit blocks after exact total passes five dollars"
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_provider_lease_coordinator_loads_cross_process_signals_and_scopes_local_cursor() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let path = root.path().join("provider-lease-coordinator.sqlite3");
    let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
        .await
        .expect("create and migrate SQLite file");
    let repository = SqliteCredentialLeaseRepository::new(pool.clone());
    let coordinator = SqliteProviderLeaseCoordinator::new(repository);
    let client = ClientApiKeyId::new("key_cursor").unwrap();
    let provider = ProviderKind::new("example").unwrap();
    let account = ProviderAccountId::new("acct_cursor").unwrap();

    let first = coordinator
        .load_state(
            &client,
            &provider,
            std::slice::from_ref(&account),
            gateway_core::provider_ports::ProviderConcurrencyPool::Shared,
        )
        .await
        .unwrap();
    assert_eq!(first.round_robin_cursor(), 0);
    assert_eq!(first.signals().get(&account).unwrap().in_flight, 0);
    let second = coordinator
        .load_state(
            &client,
            &provider,
            std::slice::from_ref(&account),
            gateway_core::provider_ports::ProviderConcurrencyPool::Shared,
        )
        .await
        .unwrap();
    assert_eq!(second.round_robin_cursor(), 1);

    let other_client = ClientApiKeyId::new("key_cursor_other").unwrap();
    let isolated = coordinator
        .load_state(
            &other_client,
            &provider,
            std::slice::from_ref(&account),
            gateway_core::provider_ports::ProviderConcurrencyPool::Shared,
        )
        .await
        .unwrap();
    assert_eq!(isolated.round_robin_cursor(), 0);
    pool.close().await;
}
