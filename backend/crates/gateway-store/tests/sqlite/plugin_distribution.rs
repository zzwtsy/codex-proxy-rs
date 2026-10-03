use gateway_admin::model::{
    MutationActor, MutationContext,
    plugins::distribution::{
        DownloadPurpose, PluginSourceBinding, PluginUpdatePolicy, PluginUpdateSource,
        SourceAuthentication, SourceCredential, SourceCredentialInfo,
    },
};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqlitePluginDistributionStore};
use secrecy::{ExposeSecret, SecretString};

#[tokio::test]
async fn sqlite_plugin_sources_and_credentials_persist_with_audited_revisions() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("plugin-distribution.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite database");
    sqlx::query(
        "insert into plugin_update_sources (plugin_id, source_json)
         values ('test.example', '{\"kind\":\"upload\"}')",
    )
    .execute(&pool)
    .await
    .expect("seed plugin source");
    let store = SqlitePluginDistributionStore::new(pool.clone());

    let sources = store.list_sources().await.expect("list plugin sources");
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].plugin_id, "test.example");
    assert_eq!(sources[0].source, PluginUpdateSource::Upload);

    let revision = store
        .change_source(
            PluginSourceBinding {
                plugin_id: "test.example".to_owned(),
                source: PluginUpdateSource::Github {
                    repository: "OWNER/PLUGIN".to_owned(),
                },
                policy: PluginUpdatePolicy::Pinned {
                    tag: "v1.2.3".to_owned(),
                    allow_prerelease: false,
                },
                outbound_proxy_id: None,
            },
            &context("plugin-source-change"),
        )
        .await
        .expect("change plugin source");
    assert_eq!(revision.get(), 2);
    let source_json: String = sqlx::query_scalar(
        "select source_json from plugin_update_sources where plugin_id = 'test.example'",
    )
    .fetch_one(&pool)
    .await
    .expect("read changed source");
    assert_eq!(
        serde_json::from_str::<PluginUpdateSource>(&source_json).unwrap(),
        PluginUpdateSource::Github {
            repository: "OWNER/PLUGIN".to_owned()
        }
    );

    let credential_id = "01942d7e-7d4b-7c10-9000-000000000001";
    store
        .save_credential(
            SourceCredential {
                info: SourceCredentialInfo {
                    id: credential_id.to_owned(),
                    name: "Plugin access".to_owned(),
                    origin: "https://github.com".to_owned(),
                    path_prefix: "/owner/plugin".to_owned(),
                    purposes: vec![DownloadPurpose::Metadata, DownloadPurpose::Artifact],
                },
                authentication: SourceAuthentication::Bearer {
                    token: SecretString::from("test-secret"),
                },
            },
            &context("plugin-credential-create"),
        )
        .await
        .expect("save plugin source credential");
    let loaded = store
        .load_credential(credential_id)
        .await
        .expect("load plugin source credential");
    match loaded.authentication {
        SourceAuthentication::Bearer { token } => {
            assert_eq!(token.expose_secret(), "test-secret");
        }
        _ => panic!("stored credential authentication type changed"),
    }
    let listed = store
        .list_credentials()
        .await
        .expect("list plugin source credential info");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, credential_id);

    store
        .delete_credential(credential_id, &context("plugin-credential-delete"))
        .await
        .expect("delete plugin source credential");
    let remaining: i64 = sqlx::query_scalar("select count(*) from plugin_source_credentials")
        .fetch_one(&pool)
        .await
        .expect("count remaining credentials");
    assert_eq!(remaining, 0);

    let audits: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events
         where entity_kind in ('plugin_source', 'plugin_source_credential')",
    )
    .fetch_one(&pool)
    .await
    .expect("count plugin distribution audit events");
    assert_eq!(audits, 3);
    let stored_revision: i64 =
        sqlx::query_scalar("select config_revision from runtime_settings where id = 1")
            .fetch_one(&pool)
            .await
            .expect("read config revision");
    assert_eq!(stored_revision, 4);
    pool.close().await;
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}
