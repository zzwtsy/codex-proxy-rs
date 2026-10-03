use gateway_admin::{
    model::{
        MutationActor, MutationContext, Revision,
        account_groups::{AccountGroupColor, NewAccountGroup},
        client_keys::NewClientKey,
        plugin_resources::PluginResourceOwner,
    },
    ports::{plugin_resources::PluginResourceStore, store::AdminStoreErrorKind},
};
use gateway_core::{
    policy::{ClientApiKeyId, RateLimits},
    routing::AccountGroupId,
};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqlitePluginResourceStore};

#[tokio::test]
async fn sqlite_plugin_resources_commit_ownership_revision_and_audit_atomically() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("plugin-resources.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite database");
    seed_enabled_plugin(&pool).await;
    let store = SqlitePluginResourceStore::new(pool.clone());
    let owner = owner();
    let group_id = AccountGroupId::new("grp_0123456789abcdef0123456789abcdef").expect("group ID");
    let group = store
        .ensure_group(
            &owner,
            "primary".to_owned(),
            NewAccountGroup {
                disable_fast: false,
                id: group_id.clone(),
                name: "Plugin Straße".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563ebff").expect("group color"),
            },
            &context("plugin-group-create"),
        )
        .await
        .expect("create plugin-owned group");
    assert_eq!(
        group.revision.as_ref().map(|revision| revision.get()),
        Some(2)
    );
    let (stored_name, stored_name_key): (String, String) =
        sqlx::query_as("select name, name_key from account_groups where id = ?1")
            .bind(group_id.as_str())
            .fetch_one(&pool)
            .await
            .expect("read plugin-owned group name key");
    assert_eq!(stored_name, "Plugin Straße");
    assert_eq!(stored_name_key, "plugin strasse");

    let no_op = store
        .ensure_group(
            &owner,
            "primary".to_owned(),
            NewAccountGroup {
                disable_fast: true,
                id: AccountGroupId::new("grp_abcdef0123456789abcdef0123456789")
                    .expect("unused group ID"),
                name: "Ignored replacement".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563ebff").expect("group color"),
            },
            &context("plugin-group-noop"),
        )
        .await
        .expect("reconcile existing plugin-owned group");
    assert!(no_op.revision.is_none());
    assert_eq!(no_op.value.id, group_id.as_str());

    let key = store
        .ensure_key(
            &owner,
            "gateway-key".to_owned(),
            vec!["primary".to_owned()],
            NewClientKey {
                request_profile_overrides: Default::default(),
                id: ClientApiKeyId::new("plugin_owned_key").expect("Client API Key ID"),
                name: "Plugin managed key".to_owned(),
                label: Some("plugin".to_owned()),
                group_ids: Vec::new(),
                limits: RateLimits {
                    max_concurrency: 2,
                    requests_per_minute: 10,
                },
                budget: Default::default(),
                plaintext: "sk_plugin_owned_key".to_owned(),
            },
            &context("plugin-key-create"),
        )
        .await
        .expect("create plugin-owned Client API Key");
    assert_eq!(
        key.revision.as_ref().map(|revision| revision.get()),
        Some(3)
    );

    let linked_groups: i64 = sqlx::query_scalar(
        "select count(*) from client_api_key_groups where client_api_key_id = 'plugin_owned_key'",
    )
    .fetch_one(&pool)
    .await
    .expect("read Client API Key group binding");
    assert_eq!(linked_groups, 1);
    let owned_keys: i64 = sqlx::query_scalar(
        "select count(*) from plugin_key_resources
         where instance_id = 'plugin-instance' and resource_key = 'gateway-key'
           and key_id = 'plugin_owned_key'",
    )
    .fetch_one(&pool)
    .await
    .expect("read plugin key ownership");
    assert_eq!(owned_keys, 1);
    let audits: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events
         where action = 'plugin_reconcile'
           and entity_kind in ('account_group', 'client_api_key')",
    )
    .fetch_one(&pool)
    .await
    .expect("read plugin resource audit events");
    assert_eq!(audits, 2);

    sqlx::query("update plugin_instances set revision = 2 where id = 'plugin-instance'")
        .execute(&pool)
        .await
        .expect("make plugin owner stale");
    let stale = match store
        .ensure_group(
            &owner,
            "stale".to_owned(),
            NewAccountGroup {
                disable_fast: false,
                id: AccountGroupId::new("grp_11111111111111111111111111111111")
                    .expect("unused group ID"),
                name: "Stale owner group".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563ebff").expect("group color"),
            },
            &context("plugin-group-stale"),
        )
        .await
    {
        Ok(_) => panic!("stale instance revision must be rejected"),
        Err(error) => error,
    };
    assert_eq!(stale.kind(), AdminStoreErrorKind::StaleRevision);
    let stale_rows: i64 = sqlx::query_scalar(
        "select count(*) from plugin_group_resources where resource_key = 'stale'",
    )
    .fetch_one(&pool)
    .await
    .expect("confirm failed transaction left no resource");
    assert_eq!(stale_rows, 0);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_plugin_resource_membership_reconciliation_is_idempotent() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("plugin-resource-members.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite database");
    seed_enabled_plugin(&pool).await;
    let store = SqlitePluginResourceStore::new(pool.clone());
    let owner = owner();
    let group_id = AccountGroupId::new("grp_0123456789abcdef0123456789abcdef").expect("group ID");
    store
        .ensure_group(
            &owner,
            "primary".to_owned(),
            NewAccountGroup {
                disable_fast: false,
                id: group_id,
                name: "Plugin group".to_owned(),
                description: None,
                color: AccountGroupColor::parse("#2563ebff").expect("group color"),
            },
            &context("plugin-group-create"),
        )
        .await
        .expect("create plugin-owned group");

    let unchanged = store
        .change_members(
            &owner,
            gateway_admin::model::plugin_resources::GroupMembersChange {
                resource_key: "primary".to_owned(),
                add: Vec::new(),
                remove: Vec::new(),
            },
            &context("plugin-members-noop"),
        )
        .await
        .expect("reconcile empty membership change");
    assert!(unchanged.revision.is_none());
    assert_eq!(unchanged.value.added, 0);
    assert_eq!(unchanged.value.removed, 0);
    pool.close().await;
}

fn owner() -> PluginResourceOwner {
    PluginResourceOwner {
        instance_id: "plugin-instance".to_owned(),
        artifact_sha256: "a".repeat(64),
        revision: Revision::new(1).expect("instance revision"),
    }
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}

async fn seed_enabled_plugin(pool: &sqlx::SqlitePool) {
    let now = chrono::Utc::now().timestamp_micros();
    sqlx::query(
        "insert into plugin_update_sources (plugin_id, source_json)
         values ('test.example', '{}')",
    )
    .execute(pool)
    .await
    .expect("seed plugin source");
    sqlx::query(
        "insert into plugin_artifacts
         (sha256, plugin_id, version, metadata_json, source_json, archive, installed_at_us, accepted_at_us)
         values (?1, 'test.example', '1.0.0', '{}', '{}', x'01', ?2, ?2)",
    )
    .bind("a".repeat(64))
    .bind(now)
    .execute(pool)
    .await
    .expect("seed accepted plugin artifact");
    sqlx::query(
        "insert into plugin_instances
         (id, artifact_sha256, name, enabled, configuration_json, bindings_json, revision)
         values ('plugin-instance', ?1, 'Example', 1, '{}', '[]', 1)",
    )
    .bind("a".repeat(64))
    .execute(pool)
    .await
    .expect("seed enabled plugin instance");
}
