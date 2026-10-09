//! 验证 SQLite 设置变更、审计、隐私策略与升级兼容

mod tests {
    use std::collections::BTreeMap;

    use gateway_admin::model::pricing::{PricingChange, StoredPricing, UpdatePricing};
    use gateway_admin::model::settings::{ReplaceRuntimeSettings, RuntimeSettingsValues};
    use gateway_admin::model::{MutationActor, MutationContext, Revision};
    use gateway_core::metering::ModelPriceOverride;
    use gateway_store::{SqliteStoreConfig, sqlite};
    use sqlx::sqlite::SqlitePoolOptions;

    fn context(request_id: &str) -> MutationContext {
        MutationContext {
            actor: MutationActor::System,
            request_id: request_id.to_owned(),
        }
    }

    fn runtime_update(expected_revision: u64) -> ReplaceRuntimeSettings {
        ReplaceRuntimeSettings {
            expected_revision: Revision::new(expected_revision).unwrap(),
            request_profile_updates: BTreeMap::new(),
            model_mappings: BTreeMap::new(),
            rotation_strategy: gateway_core::account::RotationStrategy::Smart,
            values: RuntimeSettingsValues {
                codex_privacy_policy: Default::default(),
                request_location_enabled: false,
                request_location: Default::default(),
                refresh_margin_seconds: 3_600,
                refresh_concurrency: 2,
                max_concurrent_per_account: 3,
                request_interval_ms: 50,
                max_waiting_per_key: 0,
                max_waiting_per_account: 0,
                concurrency_wait_timeout_seconds: 30,
                openai_guardian_reserved_concurrency: 0,
                openai_account_affinity: gateway_core::account::AccountAffinity::Preferred,
                max_account_rotations: 5,
                openai_session_affinity_ttl_hours: 48,
                responses_max_decompressed_body_bytes: 64 * 1024 * 1024,
                smart_scheduling: Default::default(),
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
            },
        }
    }

    fn sample_price() -> ModelPriceOverride {
        serde_json::from_value(serde_json::json!({
            "multiplierBps": 20000,
            "bands": {
                "standard": {
                    "input": "1.25",
                    "output": "2",
                    "cacheRead": "0.2",
                    "cacheWrite": "0.1"
                }
            }
        }))
        .expect("valid model price")
    }

    #[tokio::test]
    async fn privacy_policy_survives_reopen_and_snapshot_and_stale_writes() {
        use gateway_core::settings::privacy::*;
        use gateway_store::RuntimeSnapshotRepository;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("privacy.sqlite3");
        let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
            .await
            .unwrap();
        let repository = sqlite::admin_settings_store(pool.clone());
        assert_eq!(
            repository
                .load_runtime_settings()
                .await
                .unwrap()
                .values
                .codex_privacy_policy,
            CodexPrivacyPolicy::default()
        );
        let mut update = runtime_update(1);
        update.values.codex_privacy_policy = CodexPrivacyPolicy {
            enabled: true,
            on_error: PrivacyFailureMode::RejectRequest,
            rules: vec![PrivacyRule {
                id: "remove-auth".into(),
                name: "配置者控制".into(),
                enabled: true,
                scope: PrivacyScope::RequestHeader,
                selector: "authorization".into(),
                action: PrivacyAction::RemoveField,
                pattern: None,
                replacement: String::new(),
                value: serde_json::Value::Null,
                replace_all: true,
                case_insensitive: false,
                multi_line: false,
            }],
        };
        let expected = update.values.codex_privacy_policy.clone();
        let saved = repository
            .replace_runtime_settings(update.clone(), &context("privacy-save"))
            .await
            .unwrap();
        assert_eq!(saved.config_revision.get(), 2);
        assert!(
            repository
                .replace_runtime_settings(update, &context("privacy-stale"))
                .await
                .is_err()
        );
        let changes: String = sqlx::query_scalar(
            "select changed_fields_json from admin_audit_events where admin_request_id = 'privacy-save'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(changes.contains("codex_privacy_policy_json"));
        drop(repository);
        pool.close().await;
        let pool = sqlite::connect_and_migrate(&path, &SqliteStoreConfig::default())
            .await
            .unwrap();
        assert_eq!(
            sqlite::admin_settings_store(pool.clone())
                .load_runtime_settings()
                .await
                .unwrap()
                .values
                .codex_privacy_policy,
            expected
        );
        let snapshot = sqlite::SqliteRuntimeSnapshotRepository::new(pool.clone())
            .load_runtime_snapshot()
            .await
            .unwrap();
        assert_eq!(snapshot.settings.codex_privacy_policy, expected);
        pool.close().await;
    }

    #[tokio::test]
    async fn pricing_and_admin_key_changes_advance_revision_and_audit_atomically() {
        let root = tempfile::tempdir().expect("SQLite directory");
        let pool = sqlite::connect_and_migrate(
            &root.path().join("admin-settings.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .expect("migrate SQLite database");
        let repository = sqlite::admin_settings_store(pool.clone());

        assert_eq!(
            repository.load_pricing().await.unwrap(),
            StoredPricing::default()
        );
        let price = sample_price();
        let revision = repository
            .update_pricing(
                UpdatePricing {
                    provider: "openai".to_owned(),
                    models: vec!["model-a".to_owned()],
                    change: PricingChange::Replace(price.clone()),
                },
                &context("pricing.update"),
            )
            .await
            .unwrap();
        assert_eq!(revision.get(), 2);
        assert_eq!(
            repository.load_pricing().await.unwrap().overrides["openai"]["model-a"],
            price
        );

        let mut provider_changes = BTreeMap::new();
        provider_changes.insert("model-b".to_owned(), Some(sample_price()));
        let mut changes = BTreeMap::new();
        changes.insert("openai".to_owned(), provider_changes);
        let revision = repository
            .sync_pricing(changes, &context("pricing.sync"))
            .await
            .unwrap();
        assert_eq!(revision.get(), 3);
        let stored = repository.load_pricing().await.unwrap();
        assert!(stored.synced.contains_key("openai"));
        assert!(stored.synced_at.is_some());

        let revision = repository
            .replace_admin_api_key(
                gateway_admin::model::settings::AdminApiKey::new("test-key"),
                &context("admin_api_key.changed"),
            )
            .await
            .unwrap();
        assert_eq!(revision.config_revision.get(), 4);
        assert!(repository.admin_api_key_exists().await.unwrap());

        let audited: Vec<(String, i64)> = sqlx::query_as(
            "select action, config_revision from admin_audit_events order by created_at_us, id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(audited.len(), 3);
        assert_eq!(
            audited
                .iter()
                .map(|(_, revision)| *revision)
                .collect::<Vec<_>>(),
            [2, 3, 4]
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn runtime_settings_replacement_uses_revision_cas_and_rejects_overflow() {
        let root = tempfile::tempdir().expect("SQLite directory");
        let pool = sqlite::connect_and_migrate(
            &root.path().join("admin-settings-cas.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .expect("migrate SQLite database");
        let repository = sqlite::admin_settings_store(pool.clone());

        let saved = repository
            .replace_runtime_settings(runtime_update(1), &context("settings.replace"))
            .await
            .unwrap();
        assert_eq!(saved.config_revision.get(), 2);
        assert_eq!(
            saved.values.openai_account_affinity,
            gateway_core::account::AccountAffinity::Preferred
        );
        assert_eq!(saved.values.max_account_rotations, 5);
        assert_eq!(saved.values.openai_session_affinity_ttl_hours, 48);

        let stale = repository
            .replace_runtime_settings(runtime_update(1), &context("settings.replace"))
            .await
            .unwrap_err();
        assert_eq!(
            stale.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::StaleRevision
        );
        let events: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(events, 1);

        sqlx::query(
            "update runtime_settings set config_revision = 9223372036854775807 where id = 1",
        )
        .execute(&pool)
        .await
        .unwrap();
        let overflow = repository
            .replace_runtime_settings(
                runtime_update(i64::MAX as u64),
                &context("settings.replace"),
            )
            .await
            .unwrap_err();
        assert_eq!(
            overflow.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::Invalid
        );
        pool.close().await;
    }

    #[tokio::test]
    async fn affinity_migration_preserves_error_details_and_existing_revision_semantics() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("create SQLite migration fixture");
        sqlx::raw_sql(
            "create table model_requests (id text primary key, raw_upstream_error text);
             create table ops_events (id text primary key, raw_upstream_error text);
             create table provider_session_aliases (
               alias_fingerprint text primary key, session_key text not null,
               follow_only integer not null, expires_at_us integer not null
             );
             create table runtime_settings (id integer primary key, config_revision integer not null);
             insert into model_requests values ('request-1', 'response body');
             insert into ops_events values ('event-1', 'source chain');
             insert into provider_session_aliases values ('alias-1', 'root-session', 0, 1000);
             insert into runtime_settings values (1, 1), (2, 2);",
        )
        .execute(&pool)
        .await
        .expect("seed pre-migration schema");

        sqlx::raw_sql(include_str!(
            "../../../../migrations/sqlite/0015_error_details_and_account_affinity.sql"
        ))
        .execute(&pool)
        .await
        .expect("apply SQLite 0015");

        let request_details: String =
            sqlx::query_scalar("select error_details from model_requests where id = 'request-1'")
                .fetch_one(&pool)
                .await
                .expect("read renamed request details");
        let event_details: String =
            sqlx::query_scalar("select error_details from ops_events where id = 'event-1'")
                .fetch_one(&pool)
                .await
                .expect("read renamed event details");
        assert_eq!(request_details, "response body");
        assert_eq!(event_details, "source chain");
        let root_session_key: Option<String> = sqlx::query_scalar(
            "select root_session_key from provider_session_aliases where alias_fingerprint = 'alias-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("read migrated session alias");
        assert_eq!(root_session_key, None);

        let affinities: Vec<(i64, String, i64, i64)> = sqlx::query_as(
            "select config_revision, openai_account_affinity, max_account_rotations,
                    openai_session_affinity_ttl_hours
               from runtime_settings order by id",
        )
        .fetch_all(&pool)
        .await
        .expect("read migrated affinity settings");
        assert_eq!(
            affinities,
            [
                (1, "strict".to_owned(), 3, 24),
                (2, "relaxed".to_owned(), 3, 24)
            ]
        );

        sqlx::query(
            "update runtime_settings set openai_account_affinity = 'preferred' where id = 1",
        )
        .execute(&pool)
        .await
        .expect("preferred affinity is accepted");
        let invalid = sqlx::query(
            "update runtime_settings set openai_account_affinity = 'unsupported' where id = 1",
        )
        .execute(&pool)
        .await
        .expect_err("unsupported affinity is rejected");
        assert!(invalid.to_string().contains("CHECK constraint failed"));
        pool.close().await;
    }

    #[tokio::test]
    async fn invalid_pricing_rolls_back_without_advancing_revision() {
        let root = tempfile::tempdir().expect("SQLite directory");
        let pool = sqlite::connect_and_migrate(
            &root.path().join("admin-settings-invalid.sqlite3"),
            &SqliteStoreConfig::default(),
        )
        .await
        .expect("migrate SQLite database");
        let repository = sqlite::admin_settings_store(pool.clone());
        let error = repository
            .update_pricing(
                UpdatePricing {
                    provider: "openai".to_owned(),
                    models: vec!["model-a".to_owned()],
                    change: PricingChange::Replace(ModelPriceOverride {
                        multiplier_bps: 1_000_001,
                        bands: BTreeMap::new(),
                    }),
                },
                &context("pricing.update"),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            gateway_admin::ports::store::AdminStoreErrorKind::Invalid
        );
        let revision: i64 =
            sqlx::query_scalar("select config_revision from runtime_settings where id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let audits: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(revision, 1);
        assert_eq!(audits, 0);
        pool.close().await;
    }
}
