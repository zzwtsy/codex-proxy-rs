//! 组合 SQLite 插件实例、来源和制品仓储以实现 Admin 插件端口。

use async_trait::async_trait;
use gateway_admin::{
    model::{
        MutationContext, Revision,
        plugins::{
            InspectedPluginArtifact, InstalledPluginArtifact, PluginArtifactMutation, PluginSource,
            distribution::{PluginSourceBinding, SourceCredential, SourceCredentialInfo},
            instances::{
                PluginInstance, PluginInstanceMutation, PluginInstanceReplacement,
                PluginInstanceSnapshot, PluginVersionConfiguration,
            },
            state::{
                ApplyPluginStateMigration, DeletePluginState, PluginStateCommit,
                PluginStateConfiguration, PluginStateMigrationBatch, PluginStateOwner,
                PluginStateOwnerRequest, PluginStateRecord, PluginStateTransition,
                PluginStateWrite, PutPluginState,
            },
        },
    },
    ports::{
        plugins::{PluginStateStore, PluginStateStoreResult, PluginStore},
        store::AdminStoreResult,
    },
};
use sqlx::SqlitePool;

use super::{SqlitePluginArtifactStore, SqlitePluginDistributionStore, SqlitePluginInstanceStore};

#[derive(Clone)]
pub struct SqlitePluginStore {
    instances: SqlitePluginInstanceStore,
    distribution: SqlitePluginDistributionStore,
    artifacts: SqlitePluginArtifactStore,
    state: super::SqlitePluginStateStore,
}

impl SqlitePluginStore {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            instances: SqlitePluginInstanceStore::new(pool.clone()),
            distribution: SqlitePluginDistributionStore::new(pool.clone()),
            artifacts: SqlitePluginArtifactStore::new(pool.clone()),
            state: super::SqlitePluginStateStore::new(pool),
        }
    }
}

#[async_trait]
impl PluginStore for SqlitePluginStore {
    async fn management_target_is_current(
        &self,
        target: &gateway_admin::model::plugins::management::PluginManagementTarget,
    ) -> AdminStoreResult<bool> {
        self.instances.management_target_is_current(target).await
    }

    async fn load_instances(&self) -> AdminStoreResult<PluginInstanceSnapshot> {
        self.instances.load().await
    }

    async fn load_version_configuration(
        &self,
        id: &str,
        digest: &str,
    ) -> AdminStoreResult<Option<PluginVersionConfiguration>> {
        self.instances.load_version_configuration(id, digest).await
    }

    async fn configuration_versions(&self, id: &str) -> AdminStoreResult<Vec<String>> {
        self.instances.configuration_versions(id).await
    }

    async fn disable_instances(
        &self,
        ids: &[String],
        expected_revision: Revision,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        self.instances
            .disable(ids, expected_revision, context)
            .await
    }

    async fn save_instance(
        &self,
        instance: PluginInstance,
        expected_revision: Revision,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginInstanceMutation> {
        self.instances
            .save(instance, expected_revision, context)
            .await
    }

    async fn save_instance_with_state(
        &self,
        instance: PluginInstance,
        expected_revision: Revision,
        state: PluginStateCommit,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginInstanceMutation> {
        self.instances
            .save_with_state(instance, expected_revision, &state, &[], context)
            .await
    }

    async fn save_instance_replacing(
        &self,
        instance: PluginInstance,
        expected_revision: Revision,
        state: PluginStateCommit,
        replacements: &[PluginInstanceReplacement],
        context: &MutationContext,
    ) -> AdminStoreResult<PluginInstanceMutation> {
        self.instances
            .save_with_state(instance, expected_revision, &state, replacements, context)
            .await
    }

    async fn delete_instance(
        &self,
        id: &str,
        expected_revision: Revision,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        self.instances.delete(id, expected_revision, context).await
    }

    async fn list_update_sources(&self) -> AdminStoreResult<Vec<PluginSourceBinding>> {
        self.distribution.list_sources().await
    }

    async fn change_update_source(
        &self,
        binding: PluginSourceBinding,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        self.distribution.change_source(binding, context).await
    }

    async fn list_source_credentials(&self) -> AdminStoreResult<Vec<SourceCredentialInfo>> {
        self.distribution.list_credentials().await
    }

    async fn load_source_credential(&self, id: &str) -> AdminStoreResult<SourceCredential> {
        self.distribution.load_credential(id).await
    }

    async fn save_source_credential(
        &self,
        credential: SourceCredential,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        self.distribution.save_credential(credential, context).await
    }

    async fn delete_source_credential(
        &self,
        id: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        self.distribution.delete_credential(id, context).await
    }

    async fn list_artifacts(&self) -> AdminStoreResult<Vec<InstalledPluginArtifact>> {
        self.artifacts.list().await
    }

    async fn load_artifact(&self, digest: &str) -> AdminStoreResult<InspectedPluginArtifact> {
        self.artifacts.load(digest).await
    }

    async fn install_artifact(
        &self,
        artifact: InspectedPluginArtifact,
        source: PluginSource,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginArtifactMutation> {
        self.artifacts.install(artifact, source, context).await
    }

    async fn accept_artifact(
        &self,
        digest: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginArtifactMutation> {
        self.artifacts.accept(digest, context).await
    }

    async fn delete_artifact(
        &self,
        digest: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        self.artifacts.delete(digest, context).await
    }
}

#[async_trait]
impl PluginStateStore for SqlitePluginStore {
    async fn load_owner(
        &self,
        request: PluginStateOwnerRequest,
    ) -> PluginStateStoreResult<Option<PluginStateOwner>> {
        self.state.load_owner(request).await
    }

    async fn get(
        &self,
        owner: &PluginStateOwner,
        namespace: &str,
        key: &str,
    ) -> PluginStateStoreResult<Option<PluginStateRecord>> {
        self.state.get(owner, namespace, key).await
    }

    async fn put(
        &self,
        owner: &PluginStateOwner,
        command: PutPluginState,
    ) -> PluginStateStoreResult<PluginStateWrite> {
        self.state.put(owner, command).await
    }

    async fn delete(
        &self,
        owner: &PluginStateOwner,
        command: DeletePluginState,
    ) -> PluginStateStoreResult<bool> {
        self.state.delete(owner, command).await
    }

    async fn transition_required(
        &self,
        instance_id: &str,
        target: &PluginStateConfiguration,
    ) -> PluginStateStoreResult<bool> {
        self.state.transition_required(instance_id, target).await
    }

    async fn begin_transition(
        &self,
        instance_id: &str,
        expected_instance_revision: Revision,
        artifact_sha256: &str,
        target: PluginStateConfiguration,
    ) -> PluginStateStoreResult<PluginStateTransition> {
        self.state
            .begin_transition(
                instance_id,
                expected_instance_revision,
                artifact_sha256,
                target,
            )
            .await
    }

    async fn migration_batch(
        &self,
        transition_id: &str,
        namespace: &str,
        maximum_records: u32,
    ) -> PluginStateStoreResult<PluginStateMigrationBatch> {
        self.state
            .migration_batch(transition_id, namespace, maximum_records)
            .await
    }

    async fn apply_migration_batch(
        &self,
        command: ApplyPluginStateMigration,
    ) -> PluginStateStoreResult<()> {
        self.state.apply_migration_batch(command).await
    }

    async fn abort_transition(&self, transition_id: &str) -> PluginStateStoreResult<()> {
        self.state.abort_transition(transition_id).await
    }
}
