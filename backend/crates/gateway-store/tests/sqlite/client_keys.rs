use std::time::Duration;

use gateway_core::{
    engine::execution::ClientApiKeyUsageSink, lifecycle::CancellationToken, policy::ClientApiKeyId,
    task::DaemonTask,
};
use gateway_store::{
    SqliteStoreConfig, sqlite,
    sqlite::{SqliteClientApiKeyRepository, SqliteClientApiKeyUsageSink},
};

#[tokio::test]
async fn sqlite_key_usage_writer_coalesces_touches_and_flushes_on_shutdown() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("client-key-usage.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let key_id = ClientApiKeyId::new("key_usage_test").expect("client key ID");
    sqlx::query(
        "insert into client_api_keys (id, name, key, created_at_us, updated_at_us)
         values (?1, 'usage test', 'sk_usage_test', 1, 1)",
    )
    .bind(key_id.as_str())
    .execute(&pool)
    .await
    .expect("seed Client API Key");

    let (sink, writer) =
        SqliteClientApiKeyUsageSink::with_flush_delay(pool.clone(), Duration::from_secs(60));
    sink.record_used(&key_id);
    sink.record_used(&key_id);
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move { writer.run(task_cancellation).await });

    cancellation.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("usage writer drains on shutdown")
        .expect("usage writer task joins")
        .expect("usage writer exits cleanly");
    let used_at: Option<i64> =
        sqlx::query_scalar("select last_used_at_us from client_api_keys where id = ?1")
            .bind(key_id.as_str())
            .fetch_one(&pool)
            .await
            .expect("load last-used timestamp");
    assert!(used_at.is_some_and(|timestamp| timestamp > 1));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_client_key_status_checks_only_the_requested_key() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("client-key-status.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let enabled = ClientApiKeyId::new("auth_key_enabled").expect("client key ID");
    let disabled = ClientApiKeyId::new("auth_key_disabled").expect("client key ID");
    for (id, active) in [(enabled.as_str(), 1_i64), (disabled.as_str(), 0_i64)] {
        sqlx::query(
            "insert into client_api_keys (id, enabled, name, key, created_at_us, updated_at_us)
             values (?1, ?2, ?1, ?1, 1, 1)",
        )
        .bind(id)
        .bind(active)
        .execute(&pool)
        .await
        .expect("seed client API key");
    }

    let repository = SqliteClientApiKeyRepository::new(pool.clone());
    assert!(
        repository
            .is_enabled(&enabled)
            .await
            .expect("read enabled key")
    );
    assert!(
        !repository
            .is_enabled(&disabled)
            .await
            .expect("read disabled key")
    );
    assert!(
        !repository
            .is_enabled(&ClientApiKeyId::new("missing_key").expect("client key ID"))
            .await
            .expect("read missing key")
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_admin_client_keys_round_trip_and_advance_audited_revision() {
    use gateway_admin::{
        model::{
            MutationActor, MutationContext,
            client_keys::{
                ClientKeyBudgetMutationOrigin, ClientKeyBudgetPeriod, ClientKeyListQuery,
                ClientKeyPageSize, ClientKeySort, ClientKeySortField, DeleteClientKey,
                NewClientKey, ResetClientKeyBudget, SetClientKeyEnabled, SortDirection,
                UpdateClientKeyBudgetLimits,
            },
        },
        ports::store::ClientKeyStore as _,
    };
    use gateway_core::{metering::Decimal, policy::RateLimits};
    use gateway_store::sqlite::SqliteAdminClientKeyStore;

    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("admin-client-keys.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let store = SqliteAdminClientKeyStore::new(pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "sqlite-client-key-test".to_owned(),
    };
    let id = ClientApiKeyId::new("sqlite_admin_key").expect("Client API Key id");
    let (created_revision, created) = store
        .create_client_key(
            NewClientKey {
                request_profile_overrides: Default::default(),
                id: id.clone(),
                name: "SQLite Admin Key".to_owned(),
                label: Some("local".to_owned()),
                group_ids: Vec::new(),
                limits: RateLimits {
                    max_concurrency: 4,
                    requests_per_minute: 30,
                },
                budget: Default::default(),
                plaintext: "sk_sqlite_admin_key".to_owned(),
            },
            &context,
        )
        .await
        .expect("create Client API Key");
    assert_eq!(created_revision.get(), 2);
    assert_eq!(created.name, "SQLite Admin Key");
    assert_eq!(created.limits.max_concurrency, 4);

    let page = store
        .list_client_keys(ClientKeyListQuery {
            cursor: None,
            page_size: ClientKeyPageSize::new(10).expect("page size"),
            search: Some("sqlite admin".to_owned()),
            sort: ClientKeySort {
                field: ClientKeySortField::CreatedAt,
                direction: SortDirection::Desc,
            },
        })
        .await
        .expect("list Client API Keys");
    assert_eq!(page.total, 1);
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, id);

    let secret = store
        .reveal_client_key(&id)
        .await
        .expect("reveal Client API Key")
        .expect("created key exists");
    assert_eq!(secret.expose_for_response(), "sk_sqlite_admin_key");

    let budget_limit = Decimal::from_scaled(12_340_000_000).expect("budget limit");
    let budget_revision = store
        .update_client_key_budget_limits(
            UpdateClientKeyBudgetLimits {
                id: id.clone(),
                daily_limit_usd: Some(budget_limit),
                weekly_limit_usd: None,
            },
            ClientKeyBudgetMutationOrigin::Admin,
            &context,
        )
        .await
        .expect("update Client API Key budget limit")
        .expect("budget limit changed");
    assert_eq!(budget_revision.get(), 3);
    store
        .reset_client_key_budget(
            ResetClientKeyBudget {
                id: id.clone(),
                period: ClientKeyBudgetPeriod::All,
            },
            ClientKeyBudgetMutationOrigin::Admin,
            &context,
        )
        .await
        .expect("reset Client API Key budget");
    assert_eq!(
        store
            .get_client_key(&id)
            .await
            .expect("load key")
            .expect("key exists")
            .budget
            .limits
            .daily_usd,
        budget_limit,
    );

    let (enabled_revision, disabled) = store
        .set_client_key_enabled(
            SetClientKeyEnabled {
                id: id.clone(),
                enabled: false,
            },
            &context,
        )
        .await
        .expect("disable Client API Key");
    assert_eq!(enabled_revision.get(), 4);
    assert!(!disabled.enabled);
    let audits: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
        .fetch_one(&pool)
        .await
        .expect("count Client API Key audits");
    assert_eq!(audits, 4);
    let deleted_revision = store
        .delete_client_key(DeleteClientKey { id: id.clone() }, &context)
        .await
        .expect("delete Client API Key");
    assert_eq!(deleted_revision.get(), 5);
    assert!(
        store
            .get_client_key(&id)
            .await
            .expect("load deleted key")
            .is_none()
    );
    pool.close().await;
}
