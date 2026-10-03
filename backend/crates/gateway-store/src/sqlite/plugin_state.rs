//! SQLite 插件私有状态 generation、配额和迁移事务。

use std::collections::{BTreeMap, BTreeSet};

use crate::plugin_state_rules::{target_map, valid_key};

use async_trait::async_trait;
use gateway_admin::{
    model::{
        Revision,
        plugins::{
            instances::PluginInstance,
            state::{
                ApplyPluginStateMigration, DeletePluginState, PluginStateCommit,
                PluginStateConfiguration, PluginStateMigrationAction, PluginStateMigrationBatch,
                PluginStateMigrationNamespace, PluginStateNamespaceOwner, PluginStateOwner,
                PluginStateOwnerRequest, PluginStateRecord, PluginStateTransition,
                PluginStateWrite, PutPluginState,
            },
        },
    },
    ports::{
        plugins::{
            PluginStateStore, PluginStateStoreError, PluginStateStoreErrorKind,
            PluginStateStoreResult,
        },
        store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
    },
};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::admin_store_error;

use super::{acquire_write_lock, sqlite_unavailable};

const MIGRATION_BATCH_MAXIMUM: u32 = 100;

#[derive(Clone)]
pub struct SqlitePluginStateStore {
    pool: SqlitePool,
}

impl SqlitePluginStateStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

struct ActiveGeneration {
    id: String,
    namespace: String,
    schema_version: u32,
    schema_sha256: String,
    record_count: u64,
    total_bytes: u64,
    maximum_record_bytes: u64,
}

struct WritableGeneration {
    id: String,
    maximum_records: u64,
    maximum_bytes: u64,
    maximum_value_bytes: u64,
    next_record_version: u64,
    record_count: u64,
    total_bytes: u64,
}

fn state_error(kind: PluginStateStoreErrorKind) -> PluginStateStoreError {
    PluginStateStoreError::new(kind)
}

fn invalid() -> PluginStateStoreError {
    state_error(PluginStateStoreErrorKind::Invalid)
}

fn not_found() -> PluginStateStoreError {
    state_error(PluginStateStoreErrorKind::NotFound)
}

fn conflict() -> PluginStateStoreError {
    state_error(PluginStateStoreErrorKind::Conflict)
}

fn quota() -> PluginStateStoreError {
    state_error(PluginStateStoreErrorKind::Quota)
}

fn denied() -> PluginStateStoreError {
    state_error(PluginStateStoreErrorKind::PermissionDenied)
}

fn unavailable() -> PluginStateStoreError {
    state_error(PluginStateStoreErrorKind::Unavailable)
}

fn admin_conflict() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Conflict,
        "plugin state",
        "plugin state configuration conflicts with the active generation",
    )
}

fn admin_invalid() -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Invalid,
        "plugin state",
        "plugin state configuration is invalid",
    )
}

fn admin_unavailable() -> AdminStoreError {
    admin_store_error(
        "plugin state",
        sqlite_unavailable("SQLite plugin state operation"),
    )
}

fn validate_configuration(configuration: &PluginStateConfiguration) -> PluginStateStoreResult<()> {
    if crate::plugin_state_rules::configuration_is_valid(configuration) {
        Ok(())
    } else {
        Err(invalid())
    }
}

fn revision_i64(revision: Revision) -> Result<i64, PluginStateStoreError> {
    i64::try_from(revision.get()).map_err(|_| invalid())
}

fn u64_from_i64(value: i64) -> PluginStateStoreResult<u64> {
    u64::try_from(value).map_err(|_| unavailable())
}

fn u32_from_i64(value: i64) -> PluginStateStoreResult<u32> {
    u32::try_from(value).map_err(|_| unavailable())
}

fn canonical_uuid(value: &str) -> Option<String> {
    uuid::Uuid::parse_str(value)
        .ok()
        .map(|value| value.hyphenated().to_string())
}

async fn active_generations(
    transaction: &mut Transaction<'_, Sqlite>,
    instance_id: &str,
) -> PluginStateStoreResult<Vec<ActiveGeneration>> {
    let rows = sqlx::query(
        "select g.id, g.namespace, g.schema_version, g.schema_sha256, g.record_count,
                g.total_bytes,
                coalesce((select max(r.value_bytes) from plugin_state_records r
                          where r.generation_id = g.id), 0) as maximum_record_bytes
         from plugin_state_generations g
         where g.instance_id = ?1 and g.status = 'active'
         order by g.namespace",
    )
    .bind(instance_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| unavailable())?;
    rows.into_iter()
        .map(|row| {
            Ok(ActiveGeneration {
                id: row.try_get("id").map_err(|_| unavailable())?,
                namespace: row.try_get("namespace").map_err(|_| unavailable())?,
                schema_version: u32_from_i64(
                    row.try_get("schema_version").map_err(|_| unavailable())?,
                )?,
                schema_sha256: row.try_get("schema_sha256").map_err(|_| unavailable())?,
                record_count: u64_from_i64(
                    row.try_get("record_count").map_err(|_| unavailable())?,
                )?,
                total_bytes: u64_from_i64(row.try_get("total_bytes").map_err(|_| unavailable())?)?,
                maximum_record_bytes: u64_from_i64(
                    row.try_get("maximum_record_bytes")
                        .map_err(|_| unavailable())?,
                )?,
            })
        })
        .collect()
}

async fn writable_generation(
    transaction: &mut Transaction<'_, Sqlite>,
    owner: &PluginStateOwner,
    namespace: &str,
) -> PluginStateStoreResult<WritableGeneration> {
    let namespace_owner = owner.namespace(namespace).ok_or_else(denied)?;
    let generation = canonical_uuid(namespace_owner.generation_id()).ok_or_else(denied)?;
    let fence = canonical_uuid(namespace_owner.fence()).ok_or_else(denied)?;
    let instance = canonical_uuid(owner.instance_id()).ok_or_else(denied)?;
    let row = sqlx::query(
        "select g.id, g.maximum_records, g.maximum_bytes, g.maximum_value_bytes,
                g.next_record_version, g.record_count, g.total_bytes
         from plugin_state_generations g
         join plugin_instances i on i.id = g.instance_id
         where g.id = ?1 and g.fence = ?2 and g.instance_id = ?3 and g.namespace = ?4
           and g.artifact_sha256 = ?5 and g.instance_revision = ?6 and g.status = 'active'
           and i.artifact_sha256 = g.artifact_sha256 and i.revision = g.instance_revision",
    )
    .bind(generation)
    .bind(fence)
    .bind(instance)
    .bind(namespace)
    .bind(owner.artifact_sha256())
    .bind(revision_i64(owner.instance_revision())?)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| unavailable())?
    .ok_or_else(denied)?;
    Ok(WritableGeneration {
        id: row.try_get("id").map_err(|_| unavailable())?,
        maximum_records: u64_from_i64(row.try_get("maximum_records").map_err(|_| unavailable())?)?,
        maximum_bytes: u64_from_i64(row.try_get("maximum_bytes").map_err(|_| unavailable())?)?,
        maximum_value_bytes: u64_from_i64(
            row.try_get("maximum_value_bytes")
                .map_err(|_| unavailable())?,
        )?,
        next_record_version: u64_from_i64(
            row.try_get("next_record_version")
                .map_err(|_| unavailable())?,
        )?,
        record_count: u64_from_i64(row.try_get("record_count").map_err(|_| unavailable())?)?,
        total_bytes: u64_from_i64(row.try_get("total_bytes").map_err(|_| unavailable())?)?,
    })
}

#[async_trait]
impl PluginStateStore for SqlitePluginStateStore {
    async fn load_owner(
        &self,
        request: PluginStateOwnerRequest,
    ) -> PluginStateStoreResult<Option<PluginStateOwner>> {
        validate_configuration(&request.configuration)?;
        let instance_id = canonical_uuid(&request.instance_id).ok_or_else(invalid)?;
        let revision = revision_i64(request.instance_revision)?;
        let current =
            sqlx::query("select artifact_sha256, revision from plugin_instances where id = ?1")
                .bind(&instance_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| unavailable())?;
        let Some(current) = current else {
            return Ok(None);
        };
        if current
            .try_get::<String, _>("artifact_sha256")
            .map_err(|_| unavailable())?
            != request.artifact_sha256
            || current
                .try_get::<i64, _>("revision")
                .map_err(|_| unavailable())?
                != revision
        {
            return Ok(None);
        }
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        let rows = sqlx::query(
            "select id, namespace, artifact_sha256, schema_version, schema_sha256,
                    instance_revision, fence, maximum_records, maximum_bytes, maximum_value_bytes
             from plugin_state_generations
             where instance_id = ?1 and status = 'active' order by namespace",
        )
        .bind(&instance_id)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        let targets = target_map(&request.configuration);
        if rows.len() != targets.len() {
            transaction.commit().await.map_err(|_| unavailable())?;
            return Ok(None);
        }
        let mut namespaces = BTreeMap::new();
        for row in rows {
            let namespace: String = row.try_get("namespace").map_err(|_| unavailable())?;
            let Some(schema) = targets.get(namespace.as_str()) else {
                transaction.commit().await.map_err(|_| unavailable())?;
                return Ok(None);
            };
            if row
                .try_get::<String, _>("artifact_sha256")
                .map_err(|_| unavailable())?
                != request.artifact_sha256
                || row
                    .try_get::<i64, _>("instance_revision")
                    .map_err(|_| unavailable())?
                    != revision
                || u32_from_i64(row.try_get("schema_version").map_err(|_| unavailable())?)?
                    != schema.schema_version
                || row
                    .try_get::<String, _>("schema_sha256")
                    .map_err(|_| unavailable())?
                    != schema.schema_sha256
                || u64_from_i64(row.try_get("maximum_records").map_err(|_| unavailable())?)?
                    != u64::from(schema.maximum_records)
                || u64_from_i64(row.try_get("maximum_bytes").map_err(|_| unavailable())?)?
                    != schema.maximum_bytes
                || u64_from_i64(
                    row.try_get("maximum_value_bytes")
                        .map_err(|_| unavailable())?,
                )? != u64::from(schema.maximum_value_bytes)
            {
                transaction.commit().await.map_err(|_| unavailable())?;
                return Ok(None);
            }
            namespaces.insert(
                namespace,
                PluginStateNamespaceOwner::from_store(
                    row.try_get("id").map_err(|_| unavailable())?,
                    row.try_get("fence").map_err(|_| unavailable())?,
                    schema.schema_version,
                ),
            );
        }
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(Some(PluginStateOwner::from_store(
            instance_id,
            request.artifact_sha256,
            request.instance_revision,
            namespaces,
        )))
    }

    async fn get(
        &self,
        owner: &PluginStateOwner,
        namespace: &str,
        key: &str,
    ) -> PluginStateStoreResult<Option<PluginStateRecord>> {
        if !valid_key(key) {
            return Err(invalid());
        }
        let namespace_owner = owner.namespace(namespace).ok_or_else(denied)?;
        let generation = canonical_uuid(namespace_owner.generation_id()).ok_or_else(denied)?;
        let fence = canonical_uuid(namespace_owner.fence()).ok_or_else(denied)?;
        let instance = canonical_uuid(owner.instance_id()).ok_or_else(denied)?;
        let row = sqlx::query(
            "select g.schema_version, r.state_key, r.value_json, r.record_version
             from plugin_state_generations g
             join plugin_instances i on i.id = g.instance_id
             left join plugin_state_records r on r.generation_id = g.id and r.state_key = ?7
             where g.id = ?1 and g.fence = ?2 and g.instance_id = ?3 and g.namespace = ?4
               and g.artifact_sha256 = ?5 and g.instance_revision = ?6 and g.status = 'active'
               and i.artifact_sha256 = g.artifact_sha256 and i.revision = g.instance_revision",
        )
        .bind(generation)
        .bind(fence)
        .bind(instance)
        .bind(namespace)
        .bind(owner.artifact_sha256())
        .bind(revision_i64(owner.instance_revision())?)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(denied)?;
        let Some(state_key) = row
            .try_get::<Option<String>, _>("state_key")
            .map_err(|_| unavailable())?
        else {
            return Ok(None);
        };
        let encoded: String = row.try_get("value_json").map_err(|_| unavailable())?;
        let value = serde_json::from_str(&encoded).map_err(|_| unavailable())?;
        Ok(Some(PluginStateRecord {
            key: state_key,
            value,
            version: u64_from_i64(row.try_get("record_version").map_err(|_| unavailable())?)?,
            schema_version: u32_from_i64(
                row.try_get("schema_version").map_err(|_| unavailable())?,
            )?,
        }))
    }

    async fn put(
        &self,
        owner: &PluginStateOwner,
        command: PutPluginState,
    ) -> PluginStateStoreResult<PluginStateWrite> {
        if !valid_key(&command.key) {
            return Err(invalid());
        }
        let encoded = serde_json::to_string(&command.value).map_err(|_| invalid())?;
        let value_bytes = u64::try_from(encoded.len()).map_err(|_| quota())?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|_| unavailable())?;
        let generation = writable_generation(&mut transaction, owner, &command.namespace).await?;
        if value_bytes == 0 || value_bytes > generation.maximum_value_bytes {
            return Err(quota());
        }
        let existing = sqlx::query(
            "select record_version, value_bytes from plugin_state_records
             where generation_id = ?1 and state_key = ?2",
        )
        .bind(&generation.id)
        .bind(&command.key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        let previous_bytes = match (command.expected_version, existing) {
            (None, None) => 0,
            (None, Some(_)) | (Some(_), None) => return Err(conflict()),
            (Some(expected), Some(row)) => {
                if u64_from_i64(row.try_get("record_version").map_err(|_| unavailable())?)?
                    != expected
                {
                    return Err(conflict());
                }
                u64_from_i64(row.try_get("value_bytes").map_err(|_| unavailable())?)?
            }
        };
        let record_count = generation
            .record_count
            .checked_add(u64::from(previous_bytes == 0))
            .ok_or_else(quota)?;
        let total_bytes = generation
            .total_bytes
            .checked_sub(previous_bytes)
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or_else(quota)?;
        if record_count > generation.maximum_records || total_bytes > generation.maximum_bytes {
            return Err(quota());
        }
        let version = generation.next_record_version;
        let next_version = version.checked_add(1).ok_or_else(quota)?;
        sqlx::query(
            "insert into plugin_state_records
             (generation_id, state_key, value_json, value_bytes, record_version)
             values (?1, ?2, ?3, ?4, ?5)
             on conflict (generation_id, state_key) do update set
               value_json = excluded.value_json, value_bytes = excluded.value_bytes,
               record_version = excluded.record_version",
        )
        .bind(&generation.id)
        .bind(&command.key)
        .bind(encoded)
        .bind(i64::try_from(value_bytes).map_err(|_| quota())?)
        .bind(i64::try_from(version).map_err(|_| quota())?)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        sqlx::query(
            "update plugin_state_generations
             set next_record_version = ?2, record_count = ?3, total_bytes = ?4
             where id = ?1",
        )
        .bind(&generation.id)
        .bind(i64::try_from(next_version).map_err(|_| quota())?)
        .bind(i64::try_from(record_count).map_err(|_| quota())?)
        .bind(i64::try_from(total_bytes).map_err(|_| quota())?)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(PluginStateWrite { version })
    }

    async fn delete(
        &self,
        owner: &PluginStateOwner,
        command: DeletePluginState,
    ) -> PluginStateStoreResult<bool> {
        if !valid_key(&command.key) || command.expected_version == 0 {
            return Err(invalid());
        }
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|_| unavailable())?;
        let generation = writable_generation(&mut transaction, owner, &command.namespace).await?;
        let row = sqlx::query(
            "select record_version, value_bytes from plugin_state_records
             where generation_id = ?1 and state_key = ?2",
        )
        .bind(&generation.id)
        .bind(&command.key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        let Some(row) = row else {
            transaction.commit().await.map_err(|_| unavailable())?;
            return Ok(false);
        };
        if u64_from_i64(row.try_get("record_version").map_err(|_| unavailable())?)?
            != command.expected_version
        {
            return Err(conflict());
        }
        let value_bytes = u64_from_i64(row.try_get("value_bytes").map_err(|_| unavailable())?)?;
        sqlx::query("delete from plugin_state_records where generation_id = ?1 and state_key = ?2")
            .bind(&generation.id)
            .bind(&command.key)
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable())?;
        sqlx::query(
            "update plugin_state_generations
             set record_count = ?2, total_bytes = ?3 where id = ?1",
        )
        .bind(&generation.id)
        .bind(i64::try_from(generation.record_count.saturating_sub(1)).map_err(|_| unavailable())?)
        .bind(
            i64::try_from(generation.total_bytes.saturating_sub(value_bytes))
                .map_err(|_| unavailable())?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(true)
    }

    async fn transition_required(
        &self,
        instance_id: &str,
        target: &PluginStateConfiguration,
    ) -> PluginStateStoreResult<bool> {
        validate_configuration(target)?;
        let instance_id = canonical_uuid(instance_id).ok_or_else(invalid)?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        let active = active_generations(&mut transaction, &instance_id).await?;
        transaction.commit().await.map_err(|_| unavailable())?;
        let targets = target_map(target);
        let mut required = false;
        for generation in active {
            let Some(schema) = targets.get(generation.namespace.as_str()) else {
                if generation.record_count != 0 {
                    return Err(conflict());
                }
                continue;
            };
            if schema.schema_version == generation.schema_version
                && schema.schema_sha256 == generation.schema_sha256
            {
                if generation.record_count > u64::from(schema.maximum_records)
                    || generation.total_bytes > schema.maximum_bytes
                    || generation.maximum_record_bytes > u64::from(schema.maximum_value_bytes)
                {
                    return Err(conflict());
                }
                continue;
            }
            if !schema.migrates_from.contains(&generation.schema_version) {
                return Err(conflict());
            }
            required = true;
        }
        Ok(required)
    }

    async fn begin_transition(
        &self,
        instance_id: &str,
        expected_instance_revision: Revision,
        artifact_sha256: &str,
        target: PluginStateConfiguration,
    ) -> PluginStateStoreResult<PluginStateTransition> {
        validate_configuration(&target)?;
        let instance_id = canonical_uuid(instance_id).ok_or_else(invalid)?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|_| unavailable())?;
        let current = sqlx::query("select enabled, revision from plugin_instances where id = ?1")
            .bind(&instance_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| unavailable())?
            .ok_or_else(not_found)?;
        if current
            .try_get::<i64, _>("enabled")
            .map_err(|_| unavailable())?
            != 0
            || current
                .try_get::<i64, _>("revision")
                .map_err(|_| unavailable())?
                != revision_i64(expected_instance_revision)?
        {
            return Err(conflict());
        }
        let staging_exists: bool = sqlx::query_scalar(
            "select exists(select 1 from plugin_state_generations
             where instance_id = ?1 and status = 'staging')",
        )
        .bind(&instance_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        if staging_exists {
            return Err(conflict());
        }
        let active = active_generations(&mut transaction, &instance_id).await?;
        let targets = target_map(&target);
        let transition_id = uuid::Uuid::new_v4().hyphenated().to_string();
        let mut namespaces = Vec::new();
        for generation in &active {
            let Some(schema) = targets.get(generation.namespace.as_str()) else {
                if generation.record_count != 0 {
                    return Err(conflict());
                }
                continue;
            };
            if schema.schema_version == generation.schema_version
                && schema.schema_sha256 == generation.schema_sha256
            {
                if generation.record_count > u64::from(schema.maximum_records)
                    || generation.total_bytes > schema.maximum_bytes
                    || generation.maximum_record_bytes > u64::from(schema.maximum_value_bytes)
                {
                    return Err(conflict());
                }
                continue;
            }
            if !schema.migrates_from.contains(&generation.schema_version) {
                return Err(conflict());
            }
            let generation_id = uuid::Uuid::new_v4().hyphenated().to_string();
            sqlx::query(
                "insert into plugin_state_generations (
                   id, transition_id, instance_id, namespace, artifact_sha256, schema_version,
                   schema_sha256, maximum_records, maximum_bytes, maximum_value_bytes,
                   instance_revision, fence, status, source_generation_id, created_at_us
                 ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'staging', ?13, ?14)",
            )
            .bind(&generation_id)
            .bind(&transition_id)
            .bind(&instance_id)
            .bind(&schema.namespace)
            .bind(artifact_sha256)
            .bind(i64::from(schema.schema_version))
            .bind(&schema.schema_sha256)
            .bind(i64::from(schema.maximum_records))
            .bind(i64::try_from(schema.maximum_bytes).map_err(|_| invalid())?)
            .bind(i64::from(schema.maximum_value_bytes))
            .bind(revision_i64(expected_instance_revision)?)
            .bind(uuid::Uuid::new_v4().hyphenated().to_string())
            .bind(&generation.id)
            .bind(chrono::Utc::now().timestamp_micros())
            .execute(&mut *transaction)
            .await
            .map_err(|_| unavailable())?;
            namespaces.push(PluginStateMigrationNamespace {
                namespace: schema.namespace.clone(),
                from_schema_version: generation.schema_version,
                to_schema_version: schema.schema_version,
            });
        }
        if namespaces.is_empty() {
            return Err(conflict());
        }
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(PluginStateTransition {
            id: transition_id,
            instance_id,
            artifact_sha256: artifact_sha256.to_owned(),
            namespaces,
        })
    }

    async fn migration_batch(
        &self,
        transition_id: &str,
        namespace: &str,
        maximum_records: u32,
    ) -> PluginStateStoreResult<PluginStateMigrationBatch> {
        if maximum_records == 0 || maximum_records > MIGRATION_BATCH_MAXIMUM {
            return Err(invalid());
        }
        let transition_id = canonical_uuid(transition_id).ok_or_else(invalid)?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        let staging = sqlx::query(
            "select source_generation_id, migration_cursor, migration_complete
             from plugin_state_generations
             where transition_id = ?1 and namespace = ?2 and status = 'staging'",
        )
        .bind(&transition_id)
        .bind(namespace)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(not_found)?;
        if staging
            .try_get::<i64, _>("migration_complete")
            .map_err(|_| unavailable())?
            != 0
        {
            return Err(conflict());
        }
        let source: String = staging
            .try_get("source_generation_id")
            .map_err(|_| unavailable())?;
        let cursor: Option<String> = staging
            .try_get("migration_cursor")
            .map_err(|_| unavailable())?;
        let rows = sqlx::query(
            "select r.state_key, r.value_json, r.record_version, g.schema_version
             from plugin_state_records r
             join plugin_state_generations g on g.id = r.generation_id
             where r.generation_id = ?1 and (?2 is null or r.state_key > ?2)
             order by r.state_key limit ?3",
        )
        .bind(source)
        .bind(cursor.as_deref())
        .bind(i64::from(maximum_records))
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())?;
        let records = rows
            .into_iter()
            .map(|row| {
                let encoded: String = row.try_get("value_json").map_err(|_| unavailable())?;
                Ok(PluginStateRecord {
                    key: row.try_get("state_key").map_err(|_| unavailable())?,
                    value: serde_json::from_str(&encoded).map_err(|_| unavailable())?,
                    version: u64_from_i64(
                        row.try_get("record_version").map_err(|_| unavailable())?,
                    )?,
                    schema_version: u32_from_i64(
                        row.try_get("schema_version").map_err(|_| unavailable())?,
                    )?,
                })
            })
            .collect::<PluginStateStoreResult<_>>()?;
        Ok(PluginStateMigrationBatch { cursor, records })
    }

    async fn apply_migration_batch(
        &self,
        command: ApplyPluginStateMigration,
    ) -> PluginStateStoreResult<()> {
        if command.expected_keys.len() > MIGRATION_BATCH_MAXIMUM as usize
            || command.changes.len() != command.expected_keys.len()
            || command.expected_keys.iter().any(|key| !valid_key(key))
            || command
                .expected_keys
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || command
                .changes
                .iter()
                .zip(&command.expected_keys)
                .any(|(change, expected)| change.key != *expected)
        {
            return Err(invalid());
        }
        let transition_id = canonical_uuid(&command.transition_id).ok_or_else(invalid)?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|_| unavailable())?;
        let row = sqlx::query(
            "select id, source_generation_id, migration_cursor, migration_complete,
                    maximum_records, maximum_bytes, maximum_value_bytes, next_record_version,
                    record_count, total_bytes
             from plugin_state_generations
             where transition_id = ?1 and namespace = ?2 and status = 'staging'",
        )
        .bind(&transition_id)
        .bind(&command.namespace)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(not_found)?;
        let cursor: Option<String> = row.try_get("migration_cursor").map_err(|_| unavailable())?;
        if cursor != command.cursor
            || row
                .try_get::<i64, _>("migration_complete")
                .map_err(|_| unavailable())?
                != 0
        {
            return Err(conflict());
        }
        let generation = WritableGeneration {
            id: row.try_get("id").map_err(|_| unavailable())?,
            maximum_records: u64_from_i64(
                row.try_get("maximum_records").map_err(|_| unavailable())?,
            )?,
            maximum_bytes: u64_from_i64(row.try_get("maximum_bytes").map_err(|_| unavailable())?)?,
            maximum_value_bytes: u64_from_i64(
                row.try_get("maximum_value_bytes")
                    .map_err(|_| unavailable())?,
            )?,
            next_record_version: u64_from_i64(
                row.try_get("next_record_version")
                    .map_err(|_| unavailable())?,
            )?,
            record_count: u64_from_i64(row.try_get("record_count").map_err(|_| unavailable())?)?,
            total_bytes: u64_from_i64(row.try_get("total_bytes").map_err(|_| unavailable())?)?,
        };
        let source: String = row
            .try_get("source_generation_id")
            .map_err(|_| unavailable())?;
        let limit =
            i64::try_from(command.expected_keys.len().saturating_add(1)).map_err(|_| invalid())?;
        let source_rows = sqlx::query(
            "select state_key, value_json from plugin_state_records
             where generation_id = ?1 and (?2 is null or state_key > ?2)
             order by state_key limit ?3",
        )
        .bind(&source)
        .bind(command.cursor.as_deref())
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        if source_rows.len() < command.expected_keys.len()
            || source_rows
                .iter()
                .take(command.expected_keys.len())
                .zip(&command.expected_keys)
                .any(|(row, expected)| {
                    row.try_get::<String, _>("state_key").ok().as_ref() != Some(expected)
                })
            || (command.expected_keys.is_empty() && !source_rows.is_empty())
        {
            return Err(conflict());
        }
        let mut next_version = generation.next_record_version;
        let mut record_count = generation.record_count;
        let mut total_bytes = generation.total_bytes;
        for ((change, source), expected_key) in command
            .changes
            .iter()
            .zip(source_rows.iter())
            .zip(&command.expected_keys)
        {
            let value = match &change.action {
                PluginStateMigrationAction::Keep => {
                    let encoded: String =
                        source.try_get("value_json").map_err(|_| unavailable())?;
                    Some(serde_json::from_str(&encoded).map_err(|_| unavailable())?)
                }
                PluginStateMigrationAction::Replace(value) => Some(value.clone()),
                PluginStateMigrationAction::Delete => None,
            };
            let Some(value) = value else {
                continue;
            };
            let encoded = serde_json::to_string(&value).map_err(|_| invalid())?;
            let bytes = u64::try_from(encoded.len()).map_err(|_| quota())?;
            if bytes == 0 || bytes > generation.maximum_value_bytes {
                return Err(quota());
            }
            record_count = record_count.checked_add(1).ok_or_else(quota)?;
            total_bytes = total_bytes.checked_add(bytes).ok_or_else(quota)?;
            if record_count > generation.maximum_records || total_bytes > generation.maximum_bytes {
                return Err(quota());
            }
            let version = next_version;
            next_version = next_version.checked_add(1).ok_or_else(quota)?;
            sqlx::query(
                "insert into plugin_state_records
                 (generation_id, state_key, value_json, value_bytes, record_version)
                 values (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(&generation.id)
            .bind(expected_key)
            .bind(encoded)
            .bind(i64::try_from(bytes).map_err(|_| quota())?)
            .bind(i64::try_from(version).map_err(|_| quota())?)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                if error
                    .as_database_error()
                    .is_some_and(|database| database.is_unique_violation())
                {
                    conflict()
                } else {
                    unavailable()
                }
            })?;
        }
        let complete = command.expected_keys.is_empty();
        let next_cursor = command.expected_keys.last().or(command.cursor.as_ref());
        sqlx::query(
            "update plugin_state_generations
             set migration_cursor = ?2, migration_complete = ?3,
                 next_record_version = ?4, record_count = ?5, total_bytes = ?6
             where id = ?1",
        )
        .bind(&generation.id)
        .bind(next_cursor)
        .bind(i64::from(complete))
        .bind(i64::try_from(next_version).map_err(|_| quota())?)
        .bind(i64::try_from(record_count).map_err(|_| quota())?)
        .bind(i64::try_from(total_bytes).map_err(|_| quota())?)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(())
    }

    async fn abort_transition(&self, transition_id: &str) -> PluginStateStoreResult<()> {
        let transition_id = canonical_uuid(transition_id).ok_or_else(invalid)?;
        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|_| unavailable())?;
        sqlx::query(
            "delete from plugin_state_generations
             where transition_id = ?1 and status = 'staging'",
        )
        .bind(transition_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
        transaction.commit().await.map_err(|_| unavailable())?;
        Ok(())
    }
}

/// 实例与状态代次在同一事务绑定；只有这里会晋升 staging generation。
pub(crate) async fn commit_configuration(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &PluginInstance,
    revision: Revision,
    commit: &PluginStateCommit,
) -> AdminStoreResult<()> {
    validate_configuration(&commit.configuration).map_err(|_| admin_invalid())?;
    let instance_id = canonical_uuid(&instance.id).ok_or_else(admin_invalid)?;
    let active = active_generations(transaction, &instance_id)
        .await
        .map_err(|_| admin_unavailable())?;
    let targets = target_map(&commit.configuration);
    let transition = match commit.transition_id.as_deref() {
        Some(value) => Some(canonical_uuid(value).ok_or_else(admin_invalid)?),
        None => None,
    };
    let revision_i64 = i64::try_from(revision.get()).map_err(|_| admin_invalid())?;
    let mut handled = BTreeSet::new();
    for generation in active {
        let Some(schema) = targets.get(generation.namespace.as_str()) else {
            if generation.record_count != 0 {
                return Err(admin_conflict());
            }
            sqlx::query("delete from plugin_state_generations where id = ?1")
                .bind(generation.id)
                .execute(&mut **transaction)
                .await
                .map_err(|_| admin_unavailable())?;
            continue;
        };
        handled.insert(generation.namespace.clone());
        if schema.schema_version == generation.schema_version
            && schema.schema_sha256 == generation.schema_sha256
        {
            if generation.record_count > u64::from(schema.maximum_records)
                || generation.total_bytes > schema.maximum_bytes
                || generation.maximum_record_bytes > u64::from(schema.maximum_value_bytes)
            {
                return Err(admin_conflict());
            }
            sqlx::query(
                "update plugin_state_generations set artifact_sha256 = ?2,
                   maximum_records = ?3, maximum_bytes = ?4, maximum_value_bytes = ?5,
                   instance_revision = ?6, fence = ?7 where id = ?1",
            )
            .bind(generation.id)
            .bind(&instance.artifact_sha256)
            .bind(i64::from(schema.maximum_records))
            .bind(i64::try_from(schema.maximum_bytes).map_err(|_| admin_invalid())?)
            .bind(i64::from(schema.maximum_value_bytes))
            .bind(revision_i64)
            .bind(uuid::Uuid::new_v4().hyphenated().to_string())
            .execute(&mut **transaction)
            .await
            .map_err(|_| admin_unavailable())?;
            continue;
        }
        let transition = transition.as_deref().ok_or_else(admin_conflict)?;
        let staging = sqlx::query(
            "select id, record_count, total_bytes, migration_complete, source_generation_id,
                    schema_version, schema_sha256, artifact_sha256
             from plugin_state_generations
             where transition_id = ?1 and instance_id = ?2 and namespace = ?3
               and status = 'staging'",
        )
        .bind(transition)
        .bind(&instance_id)
        .bind(&generation.namespace)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| admin_unavailable())?
        .ok_or_else(admin_conflict)?;
        if staging
            .try_get::<i64, _>("migration_complete")
            .map_err(|_| admin_unavailable())?
            == 0
            || staging
                .try_get::<String, _>("source_generation_id")
                .map_err(|_| admin_unavailable())?
                != generation.id
            || u32::try_from(
                staging
                    .try_get::<i64, _>("schema_version")
                    .map_err(|_| admin_unavailable())?,
            )
            .map_err(|_| admin_unavailable())?
                != schema.schema_version
            || staging
                .try_get::<String, _>("schema_sha256")
                .map_err(|_| admin_unavailable())?
                != schema.schema_sha256
            || staging
                .try_get::<String, _>("artifact_sha256")
                .map_err(|_| admin_unavailable())?
                != instance.artifact_sha256
            || u64::try_from(
                staging
                    .try_get::<i64, _>("record_count")
                    .map_err(|_| admin_unavailable())?,
            )
            .map_err(|_| admin_unavailable())?
                > u64::from(schema.maximum_records)
            || u64::try_from(
                staging
                    .try_get::<i64, _>("total_bytes")
                    .map_err(|_| admin_unavailable())?,
            )
            .map_err(|_| admin_unavailable())?
                > schema.maximum_bytes
        {
            return Err(admin_conflict());
        }
        let staging_id: String = staging.try_get("id").map_err(|_| admin_unavailable())?;
        sqlx::query(
            "update plugin_state_generations set source_generation_id = null where id = ?1",
        )
        .bind(&staging_id)
        .execute(&mut **transaction)
        .await
        .map_err(|_| admin_unavailable())?;
        sqlx::query("delete from plugin_state_generations where id = ?1")
            .bind(&generation.id)
            .execute(&mut **transaction)
            .await
            .map_err(|_| admin_unavailable())?;
        sqlx::query(
            "update plugin_state_generations set status = 'active', instance_revision = ?2,
             fence = ?3, promoted_at_us = ?4, transition_id = null, migration_cursor = null
             where id = ?1",
        )
        .bind(&staging_id)
        .bind(revision_i64)
        .bind(uuid::Uuid::new_v4().hyphenated().to_string())
        .bind(chrono::Utc::now().timestamp_micros())
        .execute(&mut **transaction)
        .await
        .map_err(|_| admin_unavailable())?;
    }
    for schema in &commit.configuration.namespaces {
        if handled.contains(&schema.namespace) {
            continue;
        }
        let id = uuid::Uuid::new_v4().hyphenated().to_string();
        sqlx::query(
            "insert into plugin_state_generations (
               id, instance_id, namespace, artifact_sha256, schema_version, schema_sha256,
               maximum_records, maximum_bytes, maximum_value_bytes, instance_revision, fence,
               status, migration_complete, created_at_us, promoted_at_us
             ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'active', 1, ?12, ?12)",
        )
        .bind(id)
        .bind(&instance_id)
        .bind(&schema.namespace)
        .bind(&instance.artifact_sha256)
        .bind(i64::from(schema.schema_version))
        .bind(&schema.schema_sha256)
        .bind(i64::from(schema.maximum_records))
        .bind(i64::try_from(schema.maximum_bytes).map_err(|_| admin_invalid())?)
        .bind(i64::from(schema.maximum_value_bytes))
        .bind(revision_i64)
        .bind(uuid::Uuid::new_v4().hyphenated().to_string())
        .bind(chrono::Utc::now().timestamp_micros())
        .execute(&mut **transaction)
        .await
        .map_err(|error| {
            if error
                .as_database_error()
                .is_some_and(|database| database.is_unique_violation())
            {
                admin_conflict()
            } else {
                admin_unavailable()
            }
        })?;
    }
    if let Some(transition) = transition {
        let incomplete: bool = sqlx::query_scalar(
            "select exists(select 1 from plugin_state_generations
             where transition_id = ?1 and status = 'staging')",
        )
        .bind(transition)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| admin_unavailable())?;
        if incomplete {
            return Err(admin_conflict());
        }
    }
    Ok(())
}

/// 紧急停用不解析损坏的插件 schema；只允许原制品续绑并轮换 fence。
pub(crate) async fn rebind_existing_configuration(
    transaction: &mut Transaction<'_, Sqlite>,
    instance_id: &str,
    artifact_sha256: &str,
    revision: Revision,
) -> AdminStoreResult<()> {
    let instance_id = canonical_uuid(instance_id).ok_or_else(admin_invalid)?;
    let rows = sqlx::query(
        "select id, artifact_sha256 from plugin_state_generations
         where instance_id = ?1 and status = 'active'",
    )
    .bind(&instance_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| admin_unavailable())?;
    for row in &rows {
        let stored_artifact: String = row
            .try_get("artifact_sha256")
            .map_err(|_| admin_unavailable())?;
        if stored_artifact != artifact_sha256 {
            return Err(admin_conflict());
        }
    }
    let revision = i64::try_from(revision.get()).map_err(|_| admin_invalid())?;
    for row in rows {
        let id: String = row.try_get("id").map_err(|_| admin_unavailable())?;
        sqlx::query(
            "update plugin_state_generations set instance_revision = ?2, fence = ?3
             where id = ?1 and status = 'active'",
        )
        .bind(id)
        .bind(revision)
        .bind(uuid::Uuid::new_v4().hyphenated().to_string())
        .execute(&mut **transaction)
        .await
        .map_err(|_| admin_unavailable())?;
    }
    Ok(())
}
