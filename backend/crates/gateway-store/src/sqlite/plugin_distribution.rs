//! 插件更新来源和下载凭据的 SQLite 持久化边界。

use gateway_admin::{
    model::{
        MutationContext, Revision,
        audit::MutationAuditOperation,
        plugins::{
            PluginSourceEgress,
            distribution::{
                PluginSourceBinding, PluginUpdatePolicy, PluginUpdateSource, SourceAuthentication,
                SourceCredential, SourceCredentialInfo,
            },
        },
    },
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use secrecy::ExposeSecret;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::{admin_revision, admin_store_error, mutation_audit};

use super::{acquire_write_lock, append_admin_audit_event_in_transaction, bump_config_revision};

#[derive(Clone)]
pub struct SqlitePluginDistributionStore {
    pool: SqlitePool,
}

impl SqlitePluginDistributionStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn list_sources(&self) -> AdminStoreResult<Vec<PluginSourceBinding>> {
        list_sources(&self.pool).await
    }

    pub async fn change_source(
        &self,
        binding: PluginSourceBinding,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        change_source(&self.pool, binding, context).await
    }

    pub async fn list_credentials(&self) -> AdminStoreResult<Vec<SourceCredentialInfo>> {
        list_credentials(&self.pool).await
    }

    pub async fn load_credential(&self, id: &str) -> AdminStoreResult<SourceCredential> {
        load_credential(&self.pool, id).await
    }

    pub async fn save_credential(
        &self,
        credential: SourceCredential,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        save_credential(&self.pool, credential, context).await
    }

    pub async fn delete_credential(
        &self,
        id: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        delete_credential(&self.pool, id, context).await
    }
}

pub(crate) async fn list_sources(pool: &SqlitePool) -> AdminStoreResult<Vec<PluginSourceBinding>> {
    let rows = sqlx::query(
        "select plugin_id, source_json, policy_json, outbound_proxy_id
         from plugin_update_sources order by plugin_id",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable("list plugin update sources"))?;
    rows.into_iter()
        .map(|row| {
            let source = row
                .try_get::<String, _>("source_json")
                .map_err(|_| unavailable("decode plugin update source"))?;
            let policy = row
                .try_get::<String, _>("policy_json")
                .map_err(|_| unavailable("decode plugin update policy"))?;
            Ok(PluginSourceBinding {
                plugin_id: row
                    .try_get("plugin_id")
                    .map_err(|_| unavailable("decode plugin ID"))?,
                source: serde_json::from_str::<PluginUpdateSource>(&source)
                    .map_err(|_| unavailable("decode plugin update source"))?,
                policy: serde_json::from_str::<PluginUpdatePolicy>(&policy)
                    .map_err(|_| unavailable("decode plugin update policy"))?,
                outbound_proxy_id: row
                    .try_get("outbound_proxy_id")
                    .map_err(|_| unavailable("decode plugin source proxy"))?,
            })
        })
        .collect()
}

pub(crate) async fn change_source(
    pool: &SqlitePool,
    binding: PluginSourceBinding,
    context: &MutationContext,
) -> AdminStoreResult<Revision> {
    let mut transaction = begin_mutation(pool).await?;
    if let Some(proxy_id) = binding.outbound_proxy_id.as_deref() {
        lock_proxy(&mut transaction, proxy_id, None).await?;
    }
    let source_json = serde_json::to_string(&binding.source)
        .map_err(|_| invalid("plugin update source is invalid"))?;
    let policy_json = serde_json::to_string(&binding.policy)
        .map_err(|_| invalid("plugin update policy is invalid"))?;
    let changed = sqlx::query(
        "update plugin_update_sources
         set source_json = ?2, policy_json = ?3, outbound_proxy_id = ?4
         where plugin_id = ?1",
    )
    .bind(&binding.plugin_id)
    .bind(source_json)
    .bind(policy_json)
    .bind(binding.outbound_proxy_id)
    .execute(&mut *transaction)
    .await
    .map_err(|_| unavailable("update plugin source"))?
    .rows_affected();
    if changed == 0 {
        return Err(not_found("plugin update source does not exist"));
    }
    let revision = finish_mutation(
        &mut transaction,
        context,
        MutationAuditOperation::PluginSourceChangeSource,
        &binding.plugin_id,
        vec!["source".into(), "policy".into(), "outbound_proxy".into()],
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|_| unavailable("commit plugin source update"))?;
    admin_revision(revision)
}

pub(crate) async fn bind_source(
    transaction: &mut Transaction<'_, Sqlite>,
    plugin_id: &str,
    source: PluginUpdateSource,
    outbound_proxy: Option<&PluginSourceEgress>,
) -> AdminStoreResult<()> {
    if let Some(proxy) = outbound_proxy {
        lock_proxy(transaction, &proxy.id, Some(proxy.revision)).await?;
    }
    let source_json =
        serde_json::to_string(&source).map_err(|_| invalid("plugin update source is invalid"))?;
    let outbound_proxy_id = outbound_proxy.map(|proxy| proxy.id.as_str());
    sqlx::query(
        "insert into plugin_update_sources (plugin_id, source_json, outbound_proxy_id)
         values (?1, ?2, ?3) on conflict do nothing",
    )
    .bind(plugin_id)
    .bind(source_json)
    .bind(outbound_proxy_id)
    .execute(&mut **transaction)
    .await
    .map_err(|_| unavailable("record plugin update source"))?;
    let row = sqlx::query(
        "select source_json, outbound_proxy_id from plugin_update_sources where plugin_id = ?1",
    )
    .bind(plugin_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| unavailable("read recorded plugin update source"))?
    .ok_or_else(|| unavailable("recorded plugin update source is missing"))?;
    let stored_source: String = row
        .try_get("source_json")
        .map_err(|_| unavailable("decode recorded plugin update source"))?;
    let stored_proxy: Option<String> = row
        .try_get("outbound_proxy_id")
        .map_err(|_| unavailable("decode recorded plugin source proxy"))?;
    let stored_source: PluginUpdateSource = serde_json::from_str(&stored_source)
        .map_err(|_| unavailable("decode recorded plugin update source"))?;
    if stored_source != source || stored_proxy.as_deref() != outbound_proxy_id {
        return Err(conflict(
            "plugin update source conflicts with the installed artifact",
        ));
    }
    Ok(())
}

pub(crate) async fn delete_source_if_unused(
    transaction: &mut Transaction<'_, Sqlite>,
    plugin_id: &str,
    context: &MutationContext,
    revision: crate::Revision,
) -> AdminStoreResult<()> {
    let result = sqlx::query(
        "delete from plugin_update_sources
         where plugin_id = ?1
           and not exists (select 1 from plugin_artifacts where plugin_id = ?1)",
    )
    .bind(plugin_id)
    .execute(&mut **transaction)
    .await
    .map_err(|_| unavailable("delete unused plugin update source"))?;
    if result.rows_affected() > 0 {
        let mut audit = mutation_audit(
            context,
            MutationAuditOperation::PluginSourceDelete,
            plugin_id,
            vec!["source".into(), "policy".into(), "outbound_proxy".into()],
        );
        audit.config_revision = Some(revision_to_i64(revision)?);
        append_admin_audit_event_in_transaction(transaction, audit)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
    }
    Ok(())
}

pub(crate) async fn list_credentials(
    pool: &SqlitePool,
) -> AdminStoreResult<Vec<SourceCredentialInfo>> {
    let rows = sqlx::query_scalar::<_, String>(
        "select info_json from plugin_source_credentials order by id",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable("list plugin source credentials"))?;
    rows.into_iter()
        .map(|value| {
            serde_json::from_str(&value)
                .map_err(|_| unavailable("decode plugin source credential info"))
        })
        .collect()
}

pub(crate) async fn load_credential(
    pool: &SqlitePool,
    id: &str,
) -> AdminStoreResult<SourceCredential> {
    let id =
        canonical_uuid(id).ok_or_else(|| not_found("plugin source credential does not exist"))?;
    let row =
        sqlx::query("select info_json, secret_json from plugin_source_credentials where id = ?1")
            .bind(id)
            .fetch_optional(pool)
            .await
            .map_err(|_| unavailable("load plugin source credential"))?
            .ok_or_else(|| not_found("plugin source credential does not exist"))?;
    let info = row
        .try_get::<String, _>("info_json")
        .map_err(|_| unavailable("decode plugin source credential info"))?;
    let secret = row
        .try_get::<String, _>("secret_json")
        .map_err(|_| unavailable("decode plugin source credential secret"))?;
    Ok(SourceCredential {
        info: serde_json::from_str(&info)
            .map_err(|_| unavailable("decode plugin source credential info"))?,
        authentication: serde_json::from_str::<SourceAuthentication>(&secret)
            .map_err(|_| unavailable("decode plugin source credential secret"))?,
    })
}

pub(crate) async fn save_credential(
    pool: &SqlitePool,
    credential: SourceCredential,
    context: &MutationContext,
) -> AdminStoreResult<Revision> {
    let id = canonical_uuid(&credential.info.id)
        .ok_or_else(|| invalid("plugin source credential ID must be a UUID"))?;
    let info_json = serde_json::to_string(&credential.info)
        .map_err(|_| invalid("plugin source credential info is invalid"))?;
    let secret_json = encode_authentication(&credential.authentication)?;
    let mut transaction = begin_mutation(pool).await?;
    sqlx::query(
        "insert into plugin_source_credentials (id, info_json, secret_json)
         values (?1, ?2, ?3)",
    )
    .bind(id)
    .bind(info_json)
    .bind(secret_json)
    .execute(&mut *transaction)
    .await
    .map_err(|_| conflict("plugin source credential ID already exists"))?;
    let revision = finish_mutation(
        &mut transaction,
        context,
        MutationAuditOperation::PluginSourceCredentialCreate,
        &credential.info.id,
        vec!["credential".into()],
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|_| unavailable("commit plugin source credential"))?;
    admin_revision(revision)
}

pub(crate) async fn delete_credential(
    pool: &SqlitePool,
    id: &str,
    context: &MutationContext,
) -> AdminStoreResult<Revision> {
    let id =
        canonical_uuid(id).ok_or_else(|| not_found("plugin source credential does not exist"))?;
    let mut transaction = begin_mutation(pool).await?;
    sqlx::query("delete from plugin_source_credentials where id = ?1")
        .bind(&id)
        .execute(&mut *transaction)
        .await
        .map_err(|error| {
            if error
                .as_database_error()
                .is_some_and(|database| database.is_foreign_key_violation())
            {
                conflict("plugin source credential is still referenced")
            } else {
                unavailable("delete plugin source credential")
            }
        })?
        .rows_affected()
        .gt(&0)
        .then_some(())
        .ok_or_else(|| not_found("plugin source credential does not exist"))?;
    let revision = finish_mutation(
        &mut transaction,
        context,
        MutationAuditOperation::PluginSourceCredentialDelete,
        &id,
        vec!["credential".into()],
    )
    .await?;
    transaction
        .commit()
        .await
        .map_err(|_| unavailable("commit plugin source credential deletion"))?;
    admin_revision(revision)
}

/// 删除制品后只回收这批制品曾经引用且已经无其他引用的下载凭据。
pub(crate) async fn delete_credentials_if_unused(
    transaction: &mut Transaction<'_, Sqlite>,
    ids: &[String],
    context: &MutationContext,
    revision: crate::Revision,
) -> AdminStoreResult<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let encoded = serde_json::to_string(ids)
        .map_err(|_| invalid("plugin source credential IDs could not be encoded"))?;
    let removed = sqlx::query_scalar::<_, String>(
        "delete from plugin_source_credentials
         where id in (select value from json_each(?1))
           and not exists (
             select 1 from plugin_artifact_credentials r
             where r.credential_id = plugin_source_credentials.id
           )
         returning id",
    )
    .bind(encoded)
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| unavailable("delete unused plugin source credentials"))?;
    for id in removed {
        let mut audit = mutation_audit(
            context,
            MutationAuditOperation::PluginSourceCredentialDelete,
            &id,
            vec!["credential".into()],
        );
        audit.config_revision = Some(revision_to_i64(revision)?);
        append_admin_audit_event_in_transaction(transaction, audit)
            .await
            .map_err(|error| admin_store_error("plugin", error))?;
    }
    Ok(())
}

async fn begin_mutation<'a>(pool: &'a SqlitePool) -> AdminStoreResult<Transaction<'a, Sqlite>> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(|_| unavailable("begin plugin mutation"))?;
    acquire_write_lock(&mut transaction)
        .await
        .map_err(|error| admin_store_error("plugin", error))?;
    Ok(transaction)
}

async fn finish_mutation(
    transaction: &mut Transaction<'_, Sqlite>,
    context: &MutationContext,
    operation: MutationAuditOperation,
    entity_ref: &str,
    fields: Vec<String>,
) -> AdminStoreResult<crate::Revision> {
    let revision = bump_config_revision(transaction, chrono::Utc::now().timestamp_micros())
        .await
        .map_err(|error| admin_store_error("plugin", error))?;
    let mut audit = mutation_audit(context, operation, entity_ref, fields);
    audit.config_revision = Some(revision_to_i64(revision)?);
    append_admin_audit_event_in_transaction(transaction, audit)
        .await
        .map_err(|error| admin_store_error("plugin", error))?;
    Ok(revision)
}

async fn lock_proxy(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
    expected_revision: Option<u64>,
) -> AdminStoreResult<()> {
    let revision =
        sqlx::query_scalar::<_, i64>("select revision from outbound_proxies where id = ?1")
            .bind(id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|_| unavailable("load plugin source outbound proxy"))?;
    let Some(revision) = revision else {
        return Err(if expected_revision.is_some() {
            conflict("plugin source outbound proxy revision changed")
        } else {
            not_found("plugin source outbound proxy does not exist")
        });
    };
    if expected_revision.is_some_and(|expected| u64::try_from(revision).ok() != Some(expected)) {
        return Err(conflict("plugin source outbound proxy revision changed"));
    }
    Ok(())
}

fn encode_authentication(authentication: &SourceAuthentication) -> AdminStoreResult<String> {
    let secret = match authentication {
        SourceAuthentication::Github { token } => {
            serde_json::json!({"kind":"github", "token": token.expose_secret()})
        }
        SourceAuthentication::Bearer { token } => {
            serde_json::json!({"kind":"bearer", "token": token.expose_secret()})
        }
        SourceAuthentication::Basic { username, password } => {
            serde_json::json!({"kind":"basic", "username": username, "password": password.expose_secret()})
        }
        SourceAuthentication::Header { name, value } => {
            serde_json::json!({"kind":"header", "name": name, "value": value.expose_secret()})
        }
    };
    serde_json::to_string(&secret)
        .map_err(|_| invalid("plugin source credential secret is invalid"))
}

fn canonical_uuid(value: &str) -> Option<String> {
    uuid::Uuid::parse_str(value)
        .ok()
        .map(|value| value.hyphenated().to_string())
}

fn revision_to_i64(revision: crate::Revision) -> AdminStoreResult<i64> {
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
