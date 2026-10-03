use std::collections::BTreeMap;

use gateway_admin::{
    model::{
        MutationActor, MutationContext, Revision,
        plugins::{
            InspectedPluginArtifact, PluginArtifactMetadata, PluginContribution, PluginSource,
            instances::PluginInstance,
            state::{
                ApplyPluginStateMigration, PluginStateCommit, PluginStateConfiguration,
                PluginStateMigrationAction, PluginStateMigrationChange, PluginStateOwnerRequest,
                PluginStateSchema, PutPluginState,
            },
        },
    },
    ports::plugins::{PluginStateStore, PluginStateStoreErrorKind, PluginStore},
};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqlitePluginStore};
use serde_json::json;

#[tokio::test]
async fn sqlite_plugin_state_enforces_cas_quotas_fences_and_migration_promotion() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("plugin-state.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("migrate SQLite database");
    let store = SqlitePluginStore::new(pool.clone());
    let first = install(&store, 'a', "1.0.0").await;
    let initial_state = configuration(schema(1, vec![], 2));
    let saved = store
        .save_instance_with_state(
            instance(first.artifact.metadata.sha256.clone()),
            first.config_revision,
            PluginStateCommit {
                configuration: initial_state.clone(),
                transition_id: None,
            },
            &context("plugin-state-create"),
        )
        .await
        .expect("save instance and initial state generation");
    let owner = store
        .load_owner(owner_request(&saved.instance, initial_state.clone()))
        .await
        .expect("load state owner")
        .expect("current instance owns state");

    let first_write = store
        .put(
            &owner,
            PutPluginState {
                namespace: "cache".into(),
                key: "item".into(),
                value: json!({"value": 1}),
                expected_version: None,
            },
        )
        .await
        .expect("create plugin state record");
    assert_eq!(first_write.version, 1);
    let second_write = store
        .put(
            &owner,
            PutPluginState {
                namespace: "cache".into(),
                key: "other".into(),
                value: json!({"value": 2}),
                expected_version: None,
            },
        )
        .await
        .expect("create second state record");
    assert_eq!(second_write.version, 2);
    let quota_error = store
        .put(
            &owner,
            PutPluginState {
                namespace: "cache".into(),
                key: "overflow".into(),
                value: json!({"value": 3}),
                expected_version: None,
            },
        )
        .await
        .expect_err("record quota is enforced");
    assert_eq!(quota_error.kind(), PluginStateStoreErrorKind::Quota);

    let updated = store
        .put(
            &owner,
            PutPluginState {
                namespace: "cache".into(),
                key: "item".into(),
                value: json!({"value": 3}),
                expected_version: Some(first_write.version),
            },
        )
        .await
        .expect("compare-and-swap update");
    assert_eq!(updated.version, 3);
    let stale_error = store
        .put(
            &owner,
            PutPluginState {
                namespace: "cache".into(),
                key: "item".into(),
                value: json!({"value": 4}),
                expected_version: Some(first_write.version),
            },
        )
        .await
        .expect_err("stale record version is rejected");
    assert_eq!(stale_error.kind(), PluginStateStoreErrorKind::Conflict);

    let second = install(&store, 'b', "2.0.0").await;
    let target_state = configuration(schema(2, vec![1], 4));
    assert!(
        store
            .transition_required(&saved.instance.id, &target_state)
            .await
            .expect("check state schema transition")
    );
    let transition = store
        .begin_transition(
            &saved.instance.id,
            saved.instance.revision,
            &second.artifact.metadata.sha256,
            target_state.clone(),
        )
        .await
        .expect("begin staging generation");
    let batch = store
        .migration_batch(&transition.id, "cache", 100)
        .await
        .expect("read migration batch");
    let expected_keys = batch
        .records
        .iter()
        .map(|record| record.key.clone())
        .collect::<Vec<_>>();
    assert_eq!(expected_keys, ["item", "other"]);
    store
        .apply_migration_batch(ApplyPluginStateMigration {
            transition_id: transition.id.clone(),
            namespace: "cache".into(),
            cursor: batch.cursor,
            changes: expected_keys
                .iter()
                .map(|key| PluginStateMigrationChange {
                    key: key.clone(),
                    action: PluginStateMigrationAction::Keep,
                })
                .collect(),
            expected_keys,
        })
        .await
        .expect("apply state batch");
    let final_batch = store
        .migration_batch(&transition.id, "cache", 100)
        .await
        .expect("read empty completion batch");
    assert!(final_batch.records.is_empty());
    store
        .apply_migration_batch(ApplyPluginStateMigration {
            transition_id: transition.id.clone(),
            namespace: "cache".into(),
            cursor: final_batch.cursor,
            expected_keys: Vec::new(),
            changes: Vec::new(),
        })
        .await
        .expect("complete state migration");

    let mut upgraded = saved.instance;
    upgraded.artifact_sha256 = second.artifact.metadata.sha256.clone();
    let upgraded = store
        .save_instance_with_state(
            upgraded,
            second.config_revision,
            PluginStateCommit {
                configuration: target_state.clone(),
                transition_id: Some(transition.id),
            },
            &context("plugin-state-promote"),
        )
        .await
        .expect("atomically promote migrated state")
        .instance;
    let stale_owner = match store.get(&owner, "cache", "item").await {
        Ok(_) => panic!("promotion fences the previous owner"),
        Err(error) => error,
    };
    assert_eq!(
        stale_owner.kind(),
        PluginStateStoreErrorKind::PermissionDenied
    );
    let owner = store
        .load_owner(owner_request(&upgraded, target_state))
        .await
        .expect("reload promoted owner")
        .expect("promoted instance owns new generation");
    let record = store
        .get(&owner, "cache", "item")
        .await
        .expect("read migrated value")
        .expect("migrated record exists");
    assert_eq!(record.value, json!({"value": 3}));
    assert_eq!(record.schema_version, 2);

    store
        .delete_artifact(
            &first.artifact.metadata.sha256,
            &context("plugin-state-cleanup"),
        )
        .await
        .expect("old state generation no longer references old artifact");
    let remaining: i64 = sqlx::query_scalar("select count(*) from plugin_state_generations")
        .fetch_one(&pool)
        .await
        .expect("count active generation");
    assert_eq!(remaining, 1);
    pool.close().await;
}

fn schema(version: u32, migrates_from: Vec<u32>, maximum_records: u32) -> PluginStateSchema {
    PluginStateSchema {
        namespace: "cache".into(),
        schema_version: version,
        schema_sha256: char::from_digit(version, 16)
            .expect("hex schema version")
            .to_string()
            .repeat(64),
        schema: json!({"type": "object"}),
        maximum_records,
        maximum_bytes: 1024,
        maximum_value_bytes: 128,
        migrates_from,
    }
}

fn configuration(schema: PluginStateSchema) -> PluginStateConfiguration {
    PluginStateConfiguration {
        namespaces: vec![schema],
    }
}

fn instance(artifact_sha256: String) -> PluginInstance {
    PluginInstance {
        id: uuid::Uuid::now_v7().to_string(),
        name: "State fixture".into(),
        artifact_sha256,
        enabled: false,
        trusted_process: true,
        configuration: json!({}),
        secrets: BTreeMap::new(),
        bindings: Vec::new(),
        revision: Revision::new(1).expect("initial instance revision"),
    }
}

fn owner_request(
    instance: &PluginInstance,
    configuration: PluginStateConfiguration,
) -> PluginStateOwnerRequest {
    PluginStateOwnerRequest {
        instance_id: instance.id.clone(),
        artifact_sha256: instance.artifact_sha256.clone(),
        instance_revision: instance.revision,
        configuration,
    }
}

async fn install(
    store: &SqlitePluginStore,
    digest: char,
    version: &str,
) -> gateway_admin::model::plugins::PluginArtifactMutation {
    let mut artifact = artifact(digest);
    artifact.metadata.version = version.to_owned();
    let installed = store
        .install_artifact(artifact, PluginSource::Upload, &context("plugin-install"))
        .await
        .expect("install plugin artifact");
    store
        .accept_artifact(
            &installed.artifact.metadata.sha256,
            &context("plugin-accept"),
        )
        .await
        .expect("accept plugin artifact")
}

fn artifact(digest: char) -> InspectedPluginArtifact {
    InspectedPluginArtifact {
        metadata: PluginArtifactMetadata {
            plugin_id: "state.example".into(),
            version: "1.0.0".into(),
            name: "state".into(),
            display_name: "State".into(),
            publisher: "tests".into(),
            author: Some("Tests".into()),
            description: "SQLite plugin state test package".into(),
            license: "MIT".into(),
            sha256: digest.to_string().repeat(64),
            platforms: vec!["linux-x86_64".into()],
            icon: None,
            contributes: BTreeMap::from([(
                "middleware".into(),
                PluginContribution {
                    id: "state.example.middleware".into(),
                    version: 1,
                    stages: vec!["attempt".into()],
                    input_formats: vec!["openai".into()],
                    output_formats: vec!["openai".into()],
                },
            )]),
            configuration_schema: json!({}),
            secret_fields: Vec::new(),
            state_namespaces: Vec::new(),
        },
        archive: b"verified state package".as_slice().into(),
    }
}

fn context(request_id: &str) -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: request_id.to_owned(),
    }
}
