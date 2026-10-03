use std::collections::BTreeMap;

use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        plugins::{
            InspectedPluginArtifact, PluginArtifactMetadata, PluginContribution, PluginSource,
        },
    },
    ports::store::AdminStoreErrorKind,
};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqlitePluginArtifactStore};

#[tokio::test]
async fn sqlite_plugin_artifacts_preserve_identity_acceptance_and_atomic_cleanup() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("plugin-artifacts.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite database");
    let store = SqlitePluginArtifactStore::new(pool.clone());
    let fixture = artifact('a', &["linux-x86_64", "darwin-aarch64"]);

    let installed = store
        .install(
            fixture.clone(),
            PluginSource::Upload,
            &context("plugin-install"),
        )
        .await
        .expect("install plugin artifact");
    assert_eq!(installed.config_revision.get(), 2);
    assert!(installed.artifact.accepted_at.is_none());
    assert_eq!(installed.artifact.metadata.sha256, fixture.metadata.sha256);

    let repeated = store
        .install(
            fixture.clone(),
            PluginSource::Upload,
            &context("plugin-install-repeat"),
        )
        .await
        .expect("repeat identical artifact installation");
    assert_eq!(repeated.config_revision.get(), 2);

    let wrong_origin = store
        .install(
            fixture.clone(),
            PluginSource::Builtin {
                release: "v1.0.0".to_owned(),
            },
            &context("plugin-install-conflict"),
        )
        .await;
    let conflict = match wrong_origin {
        Ok(_) => panic!("same digest cannot change trusted source"),
        Err(error) => error,
    };
    assert_eq!(conflict.kind(), AdminStoreErrorKind::Conflict);

    let platforms: i64 = sqlx::query_scalar(
        "select count(*) from plugin_artifact_platforms where artifact_sha256 = ?1",
    )
    .bind(&fixture.metadata.sha256)
    .fetch_one(&pool)
    .await
    .expect("count plugin platform indexes");
    assert_eq!(platforms, 2);

    let loaded = store
        .load(&fixture.metadata.sha256)
        .await
        .expect("load stored package");
    assert_eq!(&*loaded.archive, b"verified plugin package");
    assert_eq!(loaded.metadata, fixture.metadata);

    let accepted = store
        .accept(&fixture.metadata.sha256, &context("plugin-accept"))
        .await
        .expect("accept plugin artifact");
    assert_eq!(accepted.config_revision.get(), 3);
    assert!(accepted.artifact.accepted_at.is_some());
    let accepted_again = store
        .accept(&fixture.metadata.sha256, &context("plugin-accept-repeat"))
        .await
        .expect("accepting an accepted artifact is idempotent");
    assert_eq!(accepted_again.config_revision.get(), 3);

    let deleted = store
        .delete(&fixture.metadata.sha256, &context("plugin-delete"))
        .await
        .expect("delete unused plugin artifact");
    assert_eq!(deleted.get(), 4);
    let artifacts: i64 = sqlx::query_scalar("select count(*) from plugin_artifacts")
        .fetch_one(&pool)
        .await
        .expect("count plugin artifacts");
    let sources: i64 = sqlx::query_scalar("select count(*) from plugin_update_sources")
        .fetch_one(&pool)
        .await
        .expect("count remaining plugin sources");
    assert_eq!(artifacts, 0);
    assert_eq!(sources, 0);
    let audit_count: i64 = sqlx::query_scalar(
        "select count(*) from admin_audit_events where entity_kind in ('plugin_artifact', 'plugin_source')",
    )
    .fetch_one(&pool)
    .await
    .expect("count artifact lifecycle audit events");
    assert_eq!(audit_count, 4);
    pool.close().await;
}

fn artifact(digest: char, platforms: &[&str]) -> InspectedPluginArtifact {
    InspectedPluginArtifact {
        metadata: PluginArtifactMetadata {
            plugin_id: "test.example".into(),
            version: "1.0.0".into(),
            name: "example".into(),
            display_name: "Example".into(),
            publisher: "tests".into(),
            author: Some("Tests".into()),
            description: "SQLite test package".into(),
            license: "MIT".into(),
            sha256: digest.to_string().repeat(64),
            platforms: platforms.iter().map(|value| (*value).into()).collect(),
            icon: None,
            contributes: BTreeMap::from([(
                "middleware".into(),
                PluginContribution {
                    id: "test.example.middleware".into(),
                    version: 1,
                    stages: vec!["attempt".into()],
                    input_formats: vec!["openai".into()],
                    output_formats: vec!["openai".into()],
                },
            )]),
            configuration_schema: serde_json::json!({}),
            secret_fields: Vec::new(),
            state_namespaces: Vec::new(),
        },
        archive: b"verified plugin package".as_slice().into(),
    }
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}
