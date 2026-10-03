//! 插件包体、来源、信任接受和平台索引的 SQLite 事务。

use std::sync::Arc;

use gateway_admin::{
    model::{
        MutationContext, Revision,
        audit::MutationAuditOperation,
        plugins::{
            InspectedPluginArtifact, InstalledPluginArtifact, PluginArtifactMetadata,
            PluginArtifactMutation, PluginSource,
        },
    },
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use sqlx::{Row, SqlitePool, Transaction};

use crate::{admin_revision, admin_store_error, mutation_audit};

use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, bump_config_revision,
    plugin_distribution, value::datetime_from_micros,
};

#[derive(Clone)]
pub struct SqlitePluginArtifactStore {
    pool: SqlitePool,
}

impl SqlitePluginArtifactStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> AdminStoreResult<Vec<InstalledPluginArtifact>> {
        let rows = sqlx::query(
            "select metadata_json, source_json, installed_at_us, accepted_at_us
             from plugin_artifacts order by plugin_id, installed_at_us, sha256",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable("list plugin artifacts"))?;
        rows.iter().map(decode_installed).collect()
    }

    pub async fn load(&self, digest: &str) -> AdminStoreResult<InspectedPluginArtifact> {
        let row =
            sqlx::query("select metadata_json, archive from plugin_artifacts where sha256 = ?1")
                .bind(digest)
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| unavailable("load plugin artifact"))?
                .ok_or_else(|| not_found("plugin artifact does not exist"))?;
        let metadata = decode_metadata(&row)?;
        let archive: Vec<u8> = row
            .try_get("archive")
            .map_err(|_| unavailable("decode plugin artifact archive"))?;
        Ok(InspectedPluginArtifact {
            metadata,
            archive: Arc::from(archive),
        })
    }

    pub async fn install(
        &self,
        artifact: InspectedPluginArtifact,
        source: PluginSource,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginArtifactMutation> {
        let metadata = artifact.metadata;
        let source_json =
            serde_json::to_string(&source).map_err(|_| invalid("plugin source is invalid"))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin plugin artifact installation"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
        plugin_distribution::bind_source(
            &mut transaction,
            &metadata.plugin_id,
            (&source).into(),
            source.outbound_proxy(),
        )
        .await?;

        if let Some(row) = sqlx::query(
            "select metadata_json, source_json, installed_at_us, accepted_at_us
             from plugin_artifacts where sha256 = ?1",
        )
        .bind(&metadata.sha256)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("check existing plugin artifact"))?
        {
            let installed = decode_installed(&row)?;
            let same_trust_origin = matches!(&installed.source, PluginSource::Builtin { .. })
                == matches!(&source, PluginSource::Builtin { .. });
            if installed.metadata != metadata || !same_trust_origin {
                return Err(conflict(
                    "plugin artifact already exists with different content or trust origin",
                ));
            }
            let revision = current_revision(&mut transaction).await?;
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit repeated plugin artifact installation"))?;
            return Ok(PluginArtifactMutation {
                config_revision: admin_revision(revision)?,
                artifact: installed,
            });
        }

        let revision =
            bump_config_revision(&mut transaction, chrono::Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("plugin", error))?;
        let now = chrono::Utc::now().timestamp_micros();
        let outbound_proxy_id = source.outbound_proxy().map(|proxy| proxy.id.as_str());
        let row = sqlx::query(
            "insert into plugin_artifacts (
               sha256, plugin_id, version, metadata_json, source_json,
               outbound_proxy_id, archive, installed_at_us
             ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             returning metadata_json, source_json, installed_at_us, accepted_at_us",
        )
        .bind(&metadata.sha256)
        .bind(&metadata.plugin_id)
        .bind(&metadata.version)
        .bind(serde_json::to_string(&metadata).map_err(|_| invalid("plugin metadata is invalid"))?)
        .bind(source_json)
        .bind(outbound_proxy_id)
        .bind(artifact.archive.as_ref())
        .bind(now)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|error| {
            if error
                .as_database_error()
                .is_some_and(|database| database.is_unique_violation())
            {
                conflict("plugin artifact already exists")
            } else {
                unavailable("insert plugin artifact")
            }
        })?;

        for credential_id in source.credential_ids() {
            let credential_id = canonical_uuid(credential_id)
                .ok_or_else(|| conflict("plugin source credential ID is invalid"))?;
            sqlx::query(
                "insert into plugin_artifact_credentials (artifact_sha256, credential_id)
                 values (?1, ?2)",
            )
            .bind(&metadata.sha256)
            .bind(credential_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| conflict("plugin source credential does not exist"))?;
        }

        let mut platforms = metadata.platforms.clone();
        platforms.sort();
        for platform in platforms {
            sqlx::query(
                "insert into plugin_artifact_platforms
                 (plugin_id, version, platform, artifact_sha256)
                 values (?1, ?2, ?3, ?4)",
            )
            .bind(&metadata.plugin_id)
            .bind(&metadata.version)
            .bind(platform)
            .bind(&metadata.sha256)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                if error
                    .as_database_error()
                    .is_some_and(|database| database.is_unique_violation())
                {
                    conflict("plugin platform version already exists")
                } else {
                    unavailable("insert plugin artifact platform")
                }
            })?;
        }

        let mut audit = mutation_audit(
            context,
            MutationAuditOperation::PluginArtifactInstall,
            &metadata.plugin_id,
            vec!["artifact".into(), "source".into()],
        );
        audit.config_revision = Some(revision_i64(revision)?);
        append_admin_audit_event_in_transaction(&mut transaction, audit)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
        let installed = decode_installed(&row)?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit plugin artifact installation"))?;
        Ok(PluginArtifactMutation {
            config_revision: admin_revision(revision)?,
            artifact: installed,
        })
    }

    pub async fn accept(
        &self,
        digest: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginArtifactMutation> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin plugin artifact acceptance"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
        let row = sqlx::query(
            "select metadata_json, source_json, installed_at_us, accepted_at_us
             from plugin_artifacts where sha256 = ?1",
        )
        .bind(digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("load plugin artifact for acceptance"))?
        .ok_or_else(|| not_found("plugin artifact does not exist"))?;
        let mut artifact = decode_installed(&row)?;
        let revision = if artifact.accepted_at.is_some() {
            current_revision(&mut transaction).await?
        } else {
            let accepted_at_us = chrono::Utc::now().timestamp_micros();
            sqlx::query("update plugin_artifacts set accepted_at_us = ?2 where sha256 = ?1")
                .bind(digest)
                .bind(accepted_at_us)
                .execute(&mut *transaction)
                .await
                .map_err(|_| unavailable("accept plugin artifact"))?;
            artifact.accepted_at = Some(
                datetime_from_micros(accepted_at_us)
                    .map_err(|_| unavailable("decode plugin artifact acceptance time"))?,
            );
            let revision = bump_config_revision(&mut transaction, accepted_at_us)
                .await
                .map_err(|error| admin_store_error("plugin", error))?;
            let mut audit = mutation_audit(
                context,
                MutationAuditOperation::PluginArtifactAccept,
                digest,
                vec!["acceptedAt".into()],
            );
            audit.config_revision = Some(revision_i64(revision)?);
            append_admin_audit_event_in_transaction(&mut transaction, audit)
                .await
                .map_err(|error| admin_store_error("plugin", error))?;
            revision
        };
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit plugin artifact acceptance"))?;
        Ok(PluginArtifactMutation {
            config_revision: admin_revision(revision)?,
            artifact,
        })
    }

    pub async fn delete(
        &self,
        digest: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin plugin artifact deletion"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
        let credential_ids = sqlx::query_scalar::<_, String>(
            "select credential_id from plugin_artifact_credentials where artifact_sha256 = ?1",
        )
        .bind(digest)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable("load plugin artifact credential references"))?;
        let plugin_id = sqlx::query_scalar::<_, String>(
            "delete from plugin_artifacts where sha256 = ?1 returning plugin_id",
        )
        .bind(digest)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|error| {
            if error
                .as_database_error()
                .is_some_and(|database| database.is_foreign_key_violation())
            {
                conflict("plugin artifact is still referenced")
            } else {
                unavailable("delete plugin artifact")
            }
        })?
        .ok_or_else(|| not_found("plugin artifact does not exist"))?;
        let revision =
            bump_config_revision(&mut transaction, chrono::Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("plugin", error))?;
        plugin_distribution::delete_source_if_unused(
            &mut transaction,
            &plugin_id,
            context,
            revision,
        )
        .await?;
        plugin_distribution::delete_credentials_if_unused(
            &mut transaction,
            &credential_ids,
            context,
            revision,
        )
        .await?;
        let mut audit = mutation_audit(
            context,
            MutationAuditOperation::PluginArtifactDelete,
            &plugin_id,
            vec!["artifact".into()],
        );
        audit.config_revision = Some(revision_i64(revision)?);
        append_admin_audit_event_in_transaction(&mut transaction, audit)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit plugin artifact deletion"))?;
        admin_revision(revision)
    }
}

fn decode_metadata(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<PluginArtifactMetadata> {
    let metadata: String = row
        .try_get("metadata_json")
        .map_err(|_| unavailable("read plugin artifact metadata"))?;
    serde_json::from_str(&metadata).map_err(|_| unavailable("decode plugin artifact metadata"))
}

fn decode_installed(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<InstalledPluginArtifact> {
    let source: String = row
        .try_get("source_json")
        .map_err(|_| unavailable("read plugin artifact source"))?;
    let installed_at_us: i64 = row
        .try_get("installed_at_us")
        .map_err(|_| unavailable("read plugin artifact installation time"))?;
    let accepted_at_us: Option<i64> = row
        .try_get("accepted_at_us")
        .map_err(|_| unavailable("read plugin artifact acceptance time"))?;
    Ok(InstalledPluginArtifact {
        metadata: decode_metadata(row)?,
        source: serde_json::from_str(&source)
            .map_err(|_| unavailable("decode plugin artifact source"))?,
        installed_at: datetime_from_micros(installed_at_us)
            .map_err(|_| unavailable("decode plugin artifact installation time"))?,
        accepted_at: accepted_at_us
            .map(datetime_from_micros)
            .transpose()
            .map_err(|_| unavailable("decode plugin artifact acceptance time"))?,
    })
}

async fn current_revision(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
) -> AdminStoreResult<crate::Revision> {
    let revision =
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id = 1")
            .fetch_one(&mut **transaction)
            .await
            .map_err(|_| unavailable("read plugin config revision"))?;
    crate::Revision::new(
        u64::try_from(revision).map_err(|_| unavailable("decode plugin config revision"))?,
    )
    .map_err(|_| unavailable("decode plugin config revision"))
}

fn canonical_uuid(value: &str) -> Option<String> {
    uuid::Uuid::parse_str(value)
        .ok()
        .map(|value| value.hyphenated().to_string())
}

fn revision_i64(revision: crate::Revision) -> AdminStoreResult<i64> {
    i64::try_from(revision.get()).map_err(|_| invalid("plugin config revision overflow"))
}

fn unavailable(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Unavailable, "plugin", message)
}

fn not_found(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::NotFound, "plugin", message)
}

fn conflict(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Conflict, "plugin", message)
}

fn invalid(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Invalid, "plugin", message)
}
