//! 插件实例配置与 revision 事务。

use std::collections::BTreeSet;

use gateway_admin::{
    model::{
        MutationContext, Revision as AdminRevision,
        audit::MutationAuditOperation,
        plugins::{
            instances::{
                PluginInstance, PluginInstanceMutation, PluginInstanceReplacement,
                PluginInstanceSnapshot, PluginVersionConfiguration,
            },
            state::PluginStateCommit,
        },
    },
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use secrecy::ExposeSecret;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::{admin_revision, admin_store_error, mutation_audit};

use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, bump_config_revision, plugin_state,
};

#[derive(Clone)]
pub struct SqlitePluginInstanceStore {
    pool: SqlitePool,
}

impl SqlitePluginInstanceStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn management_target_is_current(
        &self,
        target: &gateway_admin::model::plugins::management::PluginManagementTarget,
    ) -> AdminStoreResult<bool> {
        let Some(id) = canonical_uuid(&target.instance_id) else {
            return Ok(false);
        };
        let Ok(revision) = i64::try_from(target.revision) else {
            return Ok(false);
        };
        sqlx::query_scalar(
            "select exists (
               select 1 from plugin_instances i
               join plugin_artifacts a on a.sha256 = i.artifact_sha256
               where i.id = ?1 and i.artifact_sha256 = ?2 and i.revision = ?3
                 and i.enabled = 1 and a.accepted_at_us is not null
             )",
        )
        .bind(id)
        .bind(&target.artifact_sha256)
        .bind(revision)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| unavailable("check current plugin management target"))
    }

    pub async fn load(&self) -> AdminStoreResult<PluginInstanceSnapshot> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin plugin instance snapshot"))?;
        let revision = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| unavailable("read plugin config revision"))?;
        let rows = sqlx::query(
            "select i.*, a.metadata_json, a.accepted_at_us,
                    coalesce(s.secrets_json, '{}') as secrets_json
             from plugin_instances i
             join plugin_artifacts a on a.sha256 = i.artifact_sha256
             left join plugin_instance_secrets s on s.instance_id = i.id
             order by i.id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable("load plugin instances"))?;
        let instances = rows
            .iter()
            .map(decode_instance)
            .collect::<AdminStoreResult<_>>()?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("finish plugin instance snapshot"))?;
        Ok(PluginInstanceSnapshot {
            config_revision: revision_from_i64(revision)?,
            instances,
        })
    }

    pub async fn load_version_configuration(
        &self,
        id: &str,
        digest: &str,
    ) -> AdminStoreResult<Option<PluginVersionConfiguration>> {
        let id = canonical_uuid(id).ok_or_else(|| not_found("plugin instance does not exist"))?;
        let row = sqlx::query(
            "select configuration_json, secrets_json, bindings_json
             from plugin_version_configurations
             where instance_id = ?1 and artifact_sha256 = ?2",
        )
        .bind(id)
        .bind(digest)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable("load plugin version configuration"))?;
        row.map(decode_version_configuration).transpose()
    }

    pub async fn configuration_versions(&self, id: &str) -> AdminStoreResult<Vec<String>> {
        let id = canonical_uuid(id).ok_or_else(|| not_found("plugin instance does not exist"))?;
        sqlx::query_scalar(
            "select artifact_sha256 from plugin_version_configurations
             where instance_id = ?1 order by artifact_sha256",
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable("list plugin version configurations"))
    }

    pub async fn save(
        &self,
        instance: PluginInstance,
        expected: AdminRevision,
        context: &MutationContext,
    ) -> AdminStoreResult<PluginInstanceMutation> {
        save_inner(&self.pool, instance, expected, None, &[], context).await
    }

    pub async fn save_with_state(
        &self,
        instance: PluginInstance,
        expected: AdminRevision,
        state: &PluginStateCommit,
        replacements: &[PluginInstanceReplacement],
        context: &MutationContext,
    ) -> AdminStoreResult<PluginInstanceMutation> {
        save_inner(
            &self.pool,
            instance,
            expected,
            Some(state),
            replacements,
            context,
        )
        .await
    }

    pub async fn delete(
        &self,
        id: &str,
        expected: AdminRevision,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminRevision> {
        let id = canonical_uuid(id).ok_or_else(|| not_found("plugin instance does not exist"))?;
        let mut transaction = self.begin_checked(expected).await?;
        let enabled =
            sqlx::query_scalar::<_, i64>("select enabled from plugin_instances where id = ?1")
                .bind(&id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| unavailable("read plugin instance state"))?
                .ok_or_else(|| not_found("plugin instance does not exist"))?;
        if enabled != 0 {
            return Err(conflict("enabled plugin instances cannot be deleted"));
        }
        let revision =
            bump_config_revision(&mut transaction, chrono::Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("plugin", error))?;
        sqlx::query("delete from plugin_instances where id = ?1")
            .bind(&id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| conflict("plugin instance is still referenced"))?;
        append_audit(
            &mut transaction,
            context,
            MutationAuditOperation::PluginInstanceDelete,
            &id,
            vec!["instance".into()],
            revision,
        )
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit plugin instance deletion"))?;
        admin_revision(revision)
    }

    pub async fn disable(
        &self,
        ids: &[String],
        expected: AdminRevision,
        context: &MutationContext,
    ) -> AdminStoreResult<AdminRevision> {
        let mut transaction = self.begin_checked(expected).await?;
        if ids.is_empty() {
            transaction
                .commit()
                .await
                .map_err(|_| unavailable("finish empty plugin disable"))?;
            return Ok(expected);
        }
        let revision =
            bump_config_revision(&mut transaction, chrono::Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("plugin", error))?;
        let revision_admin = admin_revision(revision)?;
        for value in ids {
            let id =
                canonical_uuid(value).ok_or_else(|| conflict("plugin instance ID is invalid"))?;
            let artifact_sha256 = sqlx::query_scalar::<_, String>(
                "update plugin_instances set enabled = 0, revision = ?2
                 where id = ?1 and enabled = 1 returning artifact_sha256",
            )
            .bind(&id)
            .bind(revision_i64(revision)?)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| unavailable("disable plugin instance"))?
            .ok_or_else(|| conflict("plugin instance changed before it could be disabled"))?;
            plugin_state::rebind_existing_configuration(
                &mut transaction,
                &id,
                &artifact_sha256,
                revision_admin,
            )
            .await?;
            append_audit(
                &mut transaction,
                context,
                MutationAuditOperation::PluginInstanceConfigure,
                &id,
                vec!["enabled".into()],
                revision,
            )
            .await?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| unavailable("commit plugin instance disable"))?;
        Ok(revision_admin)
    }

    async fn begin_checked(
        &self,
        expected: AdminRevision,
    ) -> AdminStoreResult<Transaction<'_, Sqlite>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin plugin instance mutation"))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
        let current = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| unavailable("read plugin config revision"))?;
        if revision_from_i64(current)? != expected {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::StaleRevision,
                "plugin",
                "plugin configuration revision changed",
            ));
        }
        Ok(transaction)
    }
}

async fn save_inner(
    pool: &SqlitePool,
    mut instance: PluginInstance,
    expected: AdminRevision,
    state: Option<&PluginStateCommit>,
    replacements: &[PluginInstanceReplacement],
    context: &MutationContext,
) -> AdminStoreResult<PluginInstanceMutation> {
    let id =
        canonical_uuid(&instance.id).ok_or_else(|| conflict("plugin instance ID is invalid"))?;
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| unavailable("begin plugin instance mutation"))?;
    acquire_write_lock(&mut transaction)
        .await
        .map_err(|error| admin_store_error("plugin", error))?;
    let current_revision =
        sqlx::query_scalar::<_, i64>("select config_revision from runtime_settings where id = 1")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| unavailable("read plugin config revision"))?;
    if revision_from_i64(current_revision)? != expected {
        return Err(AdminStoreError::new(
            AdminStoreErrorKind::StaleRevision,
            "plugin",
            "plugin configuration revision changed",
        ));
    }
    validate_artifact_acceptance(&mut transaction, &instance).await?;
    if instance.enabled {
        validate_binding_references(&mut transaction, &instance).await?;
    }
    let revision = bump_config_revision(&mut transaction, chrono::Utc::now().timestamp_micros())
        .await
        .map_err(|error| admin_store_error("plugin", error))?;
    let revision_admin = admin_revision(revision)?;
    let configuration_json = serde_json::to_string(&instance.configuration)
        .map_err(|_| invalid("plugin configuration is invalid"))?;
    let bindings_json = serde_json::to_string(&instance.bindings)
        .map_err(|_| invalid("plugin bindings are invalid"))?;
    let secrets_json = encode_secrets(&instance)?;
    sqlx::query(
        "insert into plugin_instances
         (id, artifact_sha256, name, enabled, configuration_json, bindings_json, revision)
         values (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         on conflict (id) do update set artifact_sha256 = excluded.artifact_sha256,
           name = excluded.name, enabled = excluded.enabled,
           configuration_json = excluded.configuration_json,
           bindings_json = excluded.bindings_json, revision = excluded.revision",
    )
    .bind(&id)
    .bind(&instance.artifact_sha256)
    .bind(&instance.name)
    .bind(i64::from(instance.enabled))
    .bind(configuration_json)
    .bind(bindings_json)
    .bind(revision_i64(revision)?)
    .execute(&mut *transaction)
    .await
    .map_err(|error| {
        if error
            .as_database_error()
            .is_some_and(|database| database.is_foreign_key_violation())
        {
            conflict("plugin artifact does not exist")
        } else {
            unavailable("save plugin instance")
        }
    })?;
    sqlx::query(
        "insert into plugin_instance_secrets (instance_id, secrets_json)
         values (?1, ?2)
         on conflict (instance_id) do update set secrets_json = excluded.secrets_json",
    )
    .bind(&id)
    .bind(&secrets_json)
    .execute(&mut *transaction)
    .await
    .map_err(|_| unavailable("save plugin instance secrets"))?;

    if instance.enabled {
        sqlx::query(
            "insert into plugin_version_configurations
             (instance_id, artifact_sha256, configuration_json, secrets_json, bindings_json)
             values (?1, ?2, ?3, ?4, ?5)
             on conflict (instance_id, artifact_sha256) do update set
               configuration_json = excluded.configuration_json,
               secrets_json = excluded.secrets_json,
               bindings_json = excluded.bindings_json",
        )
        .bind(&id)
        .bind(&instance.artifact_sha256)
        .bind(
            serde_json::to_string(&instance.configuration)
                .map_err(|_| invalid("plugin configuration is invalid"))?,
        )
        .bind(&secrets_json)
        .bind(
            serde_json::to_string(&instance.bindings)
                .map_err(|_| invalid("plugin bindings are invalid"))?,
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| unavailable("save plugin version configuration"))?;
    }

    for replacement in replacements {
        let replacement_id = canonical_uuid(&replacement.id)
            .ok_or_else(|| conflict("replacement plugin instance ID is invalid"))?;
        if replacement_id == id || !instance.enabled {
            return Err(conflict("plugin instance replacement is invalid"));
        }
        let replacement_artifact = sqlx::query_scalar::<_, String>(
            "update plugin_instances
             set enabled = 0, revision = ?3
             where id = ?1 and revision = ?2 and enabled = 1
               and (select json_extract(a.metadata_json, '$.pluginId')
                    from plugin_artifacts a where a.sha256 = plugin_instances.artifact_sha256)
                   = (select json_extract(a.metadata_json, '$.pluginId')
                      from plugin_artifacts a where a.sha256 = ?4)
             returning artifact_sha256",
        )
        .bind(&replacement_id)
        .bind(
            i64::try_from(replacement.expected_revision)
                .map_err(|_| conflict("replacement revision is invalid"))?,
        )
        .bind(revision_i64(revision)?)
        .bind(&instance.artifact_sha256)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable("disable replaced plugin instance"))?
        .ok_or_else(|| conflict("replacement plugin instance changed"))?;
        plugin_state::rebind_existing_configuration(
            &mut transaction,
            &replacement_id,
            &replacement_artifact,
            revision_admin,
        )
        .await?;
        append_audit(
            &mut transaction,
            context,
            MutationAuditOperation::PluginInstanceConfigure,
            &replacement_id,
            vec!["enabled".into()],
            revision,
        )
        .await?;
    }

    if let Some(state) = state {
        plugin_state::commit_configuration(&mut transaction, &instance, revision_admin, state)
            .await?;
    } else {
        plugin_state::rebind_existing_configuration(
            &mut transaction,
            &id,
            &instance.artifact_sha256,
            revision_admin,
        )
        .await?;
    }
    append_audit(
        &mut transaction,
        context,
        MutationAuditOperation::PluginInstanceConfigure,
        &id,
        vec![
            "artifact".into(),
            "configuration".into(),
            "bindings".into(),
            "enabled".into(),
        ],
        revision,
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|_| unavailable("commit plugin instance mutation"))?;
    instance.id = id;
    instance.revision = revision_admin;
    Ok(PluginInstanceMutation {
        config_revision: revision_admin,
        instance,
    })
}

async fn validate_artifact_acceptance(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &PluginInstance,
) -> AdminStoreResult<()> {
    let accepted = sqlx::query_scalar::<_, i64>(
        "select accepted_at_us is not null from plugin_artifacts where sha256 = ?1",
    )
    .bind(&instance.artifact_sha256)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| unavailable("load plugin artifact acceptance"))?
    .ok_or_else(|| not_found("plugin artifact does not exist"))?
        != 0;
    if instance.trusted_process != accepted || (instance.enabled && !accepted) {
        return Err(invalid("plugin acceptance does not match its artifact"));
    }
    Ok(())
}

async fn validate_binding_references(
    transaction: &mut Transaction<'_, Sqlite>,
    instance: &PluginInstance,
) -> AdminStoreResult<()> {
    let client_key_ids = instance
        .bindings
        .iter()
        .flat_map(|binding| {
            binding.client_key_ids.iter().chain(
                binding
                    .identity_bindings
                    .iter()
                    .map(|identity| &identity.client_key_id),
            )
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let account_group_ids = instance
        .bindings
        .iter()
        .flat_map(|binding| &binding.account_group_ids)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    for (table, ids) in [
        ("client_api_keys", client_key_ids),
        ("account_groups", account_group_ids),
    ] {
        if ids.is_empty() {
            continue;
        }
        let encoded = serde_json::to_string(&ids)
            .map_err(|_| invalid("plugin binding references are invalid"))?;
        let count = match table {
            "client_api_keys" => {
                sqlx::query_scalar::<_, i64>(
                    "select count(*) from client_api_keys where id in (select value from json_each(?1))",
                )
                .bind(encoded)
                .fetch_one(&mut **transaction)
                .await
            }
            "account_groups" => {
                sqlx::query_scalar::<_, i64>(
                    "select count(*) from account_groups where id in (select value from json_each(?1))",
                )
                .bind(encoded)
                .fetch_one(&mut **transaction)
                .await
            }
            _ => unreachable!("table is selected from a closed set"),
        }
        .map_err(|_| unavailable("validate plugin binding references"))?;
        if usize::try_from(count).ok() != Some(ids.len()) {
            return Err(invalid("plugin binding references do not exist"));
        }
    }
    Ok(())
}

fn decode_instance(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<PluginInstance> {
    let id: String = row
        .try_get("id")
        .map_err(|_| unavailable("decode plugin instance ID"))?;
    let accepted_at_us: Option<i64> = row
        .try_get("accepted_at_us")
        .map_err(|_| unavailable("decode plugin accepted state"))?;
    let enabled: i64 = row
        .try_get("enabled")
        .map_err(|_| unavailable("decode plugin enabled state"))?;
    let configuration_json: String = row
        .try_get("configuration_json")
        .map_err(|_| unavailable("decode plugin configuration"))?;
    let secrets_json: String = row
        .try_get("secrets_json")
        .map_err(|_| unavailable("decode plugin secrets"))?;
    let bindings_json: String = row
        .try_get("bindings_json")
        .map_err(|_| unavailable("decode plugin bindings"))?;
    Ok(PluginInstance {
        id,
        name: row
            .try_get("name")
            .map_err(|_| unavailable("decode plugin instance name"))?,
        artifact_sha256: row
            .try_get("artifact_sha256")
            .map_err(|_| unavailable("decode plugin artifact reference"))?,
        enabled: enabled != 0,
        trusted_process: accepted_at_us.is_some(),
        configuration: serde_json::from_str(&configuration_json)
            .map_err(|_| unavailable("decode plugin configuration"))?,
        secrets: serde_json::from_str(&secrets_json)
            .map_err(|_| unavailable("decode plugin secrets"))?,
        bindings: serde_json::from_str(&bindings_json)
            .map_err(|_| unavailable("decode plugin bindings"))?,
        revision: revision_from_i64(
            row.try_get("revision")
                .map_err(|_| unavailable("decode plugin instance revision"))?,
        )?,
    })
}

fn decode_version_configuration(
    row: sqlx::sqlite::SqliteRow,
) -> AdminStoreResult<PluginVersionConfiguration> {
    let configuration_json: String = row
        .try_get("configuration_json")
        .map_err(|_| unavailable("decode plugin version configuration"))?;
    let secrets_json: String = row
        .try_get("secrets_json")
        .map_err(|_| unavailable("decode plugin version secrets"))?;
    let bindings_json: String = row
        .try_get("bindings_json")
        .map_err(|_| unavailable("decode plugin version bindings"))?;
    Ok(PluginVersionConfiguration {
        configuration: serde_json::from_str(&configuration_json)
            .map_err(|_| unavailable("decode plugin version configuration"))?,
        secrets: serde_json::from_str(&secrets_json)
            .map_err(|_| unavailable("decode plugin version secrets"))?,
        bindings: serde_json::from_str(&bindings_json)
            .map_err(|_| unavailable("decode plugin version bindings"))?,
    })
}

fn encode_secrets(instance: &PluginInstance) -> AdminStoreResult<String> {
    let secrets = instance
        .secrets
        .iter()
        .map(|(name, value)| (name, value.expose_secret()))
        .collect::<std::collections::BTreeMap<_, _>>();
    serde_json::to_string(&secrets).map_err(|_| invalid("plugin secrets are invalid"))
}

async fn append_audit(
    transaction: &mut Transaction<'_, Sqlite>,
    context: &MutationContext,
    operation: MutationAuditOperation,
    id: &str,
    fields: Vec<String>,
    revision: crate::Revision,
) -> AdminStoreResult<()> {
    let mut audit = mutation_audit(context, operation, id, fields);
    audit.config_revision = Some(revision_i64(revision)?);
    append_admin_audit_event_in_transaction(transaction, audit)
        .await
        .map_err(|error| admin_store_error("plugin", error))
}

fn canonical_uuid(value: &str) -> Option<String> {
    uuid::Uuid::parse_str(value)
        .ok()
        .map(|value| value.hyphenated().to_string())
}

fn revision_i64(revision: crate::Revision) -> AdminStoreResult<i64> {
    i64::try_from(revision.get()).map_err(|_| invalid("plugin revision is outside SQLite INTEGER"))
}

fn revision_from_i64(value: i64) -> AdminStoreResult<AdminRevision> {
    AdminRevision::new(u64::try_from(value).map_err(|_| unavailable("decode plugin revision"))?)
        .map_err(|_| unavailable("decode plugin revision"))
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
