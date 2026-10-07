//! SQLite Client API Key 管理仓储与配置审计事务。

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use chrono::Utc;
use futures::future::BoxFuture;
use gateway_admin::{
    model::{
        MutationContext,
        audit::MutationAuditOperation,
        client_keys::{
            ClientKeyBudgetMutationOrigin, ClientKeyBudgetPeriod, ClientKeyListQuery,
            ClientKeyPage, ClientKeyRecord, ClientKeySecret, DeleteClientKey, NewClientKey,
            ResetClientKeyBudget, SetClientKeyEnabled, UpdateClientKey,
            UpdateClientKeyBudgetLimits,
        },
        plugin_resources::PluginResourceOwner,
    },
    ports::store::{AdminStoreError, AdminStoreErrorKind, AdminStoreResult, ClientKeyStore},
};
use gateway_core::{
    account::OpaqueProviderData,
    engine::budget::{ClientBudgetLimits, ClientBudgetStatus},
    metering::Decimal,
    policy::{ClientApiKeyId, PlaintextClientApiKey},
    routing::ProviderKind,
};
use serde_json::{Map, Value};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction, sqlite::SqliteRow};

use crate::{
    AdminAuditEvent, ConflictKind, Revision, StoreError, StoreResult, admin_revision,
    admin_store_error, mutation_audit,
};

use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, bump_config_revision,
    name_key::normalize_name_key,
    sqlite_unavailable,
    value::{datetime_from_micros, decode_amount, encode_amount},
};

const ENTITY: &str = "client API key";

#[derive(Clone)]
pub struct SqliteAdminClientKeyStore {
    pool: SqlitePool,
}

impl SqliteAdminClientKeyStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn current_revision(&self) -> AdminStoreResult<gateway_admin::model::Revision> {
        let revision = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable("read config revision"))?
        .ok_or_else(|| not_found("runtime settings", "1"))?;
        let revision = u64::try_from(revision).map_err(|_| invalid("invalid config revision"))?;
        let revision = Revision::new(revision).map_err(|_| invalid("invalid config revision"))?;
        admin_revision(revision)
    }

    async fn required_record(&self, id: &ClientApiKeyId) -> AdminStoreResult<ClientKeyRecord> {
        self.load_record(id)
            .await?
            .ok_or_else(|| not_found(ENTITY, id.as_str()))
    }

    async fn load_record(&self, id: &ClientApiKeyId) -> AdminStoreResult<Option<ClientKeyRecord>> {
        let row = sqlx::query(
            "select k.id, k.name, k.label, k.provider_request_profiles_json,
                    substr(k.key, 1, min(10, length(k.key) / 2)) as prefix,
                    k.enabled, k.max_concurrency, k.requests_per_minute,
                    k.last_used_at_us, k.created_at_us, k.updated_at_us
             from client_api_keys k where k.id = ?1",
        )
        .bind(id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable("load Client API Key"))?;
        let Some(row) = row else { return Ok(None) };
        let mut records =
            vec![client_record_from_row(&row).map_err(|error| admin_store_error(ENTITY, error))?];
        load_memberships(&self.pool, &mut records)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        load_budgets(&self.pool, &mut records)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let record = records
            .pop()
            .ok_or_else(|| invalid("missing Client API Key record"))?;
        let record = crate::postgres::admin_client_key_record(record)?;
        Ok(Some(record))
    }

    async fn mutate<T, F>(
        &self,
        mut audit: AdminAuditEvent,
        owner: Option<&PluginResourceOwner>,
        mutation: F,
    ) -> AdminStoreResult<(Revision, T)>
    where
        F: for<'a> FnOnce(&'a mut Transaction<'_, Sqlite>) -> BoxFuture<'a, StoreResult<T>>,
    {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin Client API Key mutation"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            if let Some(owner) = owner {
                verify_plugin_owner(&mut transaction, owner).await?;
            }
            let value = mutation(&mut transaction).await?;
            let revision =
                bump_config_revision(&mut transaction, Utc::now().timestamp_micros()).await?;
            audit.config_revision = Some(
                i64::try_from(revision.get())
                    .map_err(|_| invalid_store("config revision exceeds SQLite INTEGER"))?,
            );
            append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
            Ok((revision, value))
        }
        .await;
        match result {
            Ok((revision, value)) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| unavailable("commit Client API Key mutation"))?;
                Ok((revision, value))
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable("rollback Client API Key mutation"))?;
                Err(admin_store_error(ENTITY, error))
            }
        }
    }
}

#[async_trait]
impl ClientKeyStore for SqliteAdminClientKeyStore {
    async fn get_client_key(
        &self,
        id: &ClientApiKeyId,
    ) -> AdminStoreResult<Option<ClientKeyRecord>> {
        self.load_record(id).await
    }

    async fn list_client_keys(&self, query: ClientKeyListQuery) -> AdminStoreResult<ClientKeyPage> {
        let config_revision = self.current_revision().await?;
        let query = crate::postgres::store_client_key_query(query)?;
        query
            .validate()
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let mut count =
            QueryBuilder::<Sqlite>::new("select count(*) from client_api_keys k where 1=1");
        push_search(&mut count, query.search.as_deref());
        let total = count
            .build_query_scalar::<i64>()
            .fetch_one(&self.pool)
            .await
            .map_err(|_| unavailable("count Client API Keys"))?;
        let total = u64::try_from(total).map_err(|_| invalid("invalid Client API Key count"))?;

        let mut statement = QueryBuilder::<Sqlite>::new(
            "select k.id, k.name, k.label, k.provider_request_profiles_json,
                    substr(k.key, 1, min(10, length(k.key) / 2)) as prefix,
                    k.enabled, k.max_concurrency, k.requests_per_minute,
                    k.last_used_at_us, k.created_at_us, k.updated_at_us
             from client_api_keys k where 1=1",
        );
        push_search(&mut statement, query.search.as_deref());
        if let Some(cursor) = &query.cursor {
            push_cursor(&mut statement, cursor);
        }
        push_order(&mut statement, query.sort);
        statement.push(" limit ");
        statement.push_bind(i64::from(query.page_size) + 1);
        let rows = statement
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| unavailable("list Client API Keys"))?;
        let mut items = rows
            .iter()
            .map(client_record_from_row)
            .collect::<StoreResult<Vec<_>>>()
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let has_more = items.len() > usize::from(query.page_size);
        if has_more {
            items.pop();
        }
        let next_cursor = if has_more {
            items
                .last()
                .map(|record| crate::postgres::ClientApiKeyCursor::from_record(query.sort, record))
                .map(crate::postgres::admin_client_key_cursor)
                .transpose()?
        } else {
            None
        };
        load_memberships(&self.pool, &mut items)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        load_budgets(&self.pool, &mut items)
            .await
            .map_err(|error| admin_store_error(ENTITY, error))?;
        Ok(ClientKeyPage {
            config_revision,
            items: items
                .into_iter()
                .map(crate::postgres::admin_client_key_record)
                .collect::<AdminStoreResult<Vec<_>>>()?,
            total,
            next_cursor,
        })
    }

    async fn reveal_client_key(
        &self,
        id: &ClientApiKeyId,
    ) -> AdminStoreResult<Option<ClientKeySecret>> {
        let key = sqlx::query_scalar::<_, String>("select key from client_api_keys where id = ?1")
            .bind(id.as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| unavailable("reveal Client API Key"))?;
        let Some(key) = key else { return Ok(None) };
        let record = self.required_record(id).await?;
        Ok(Some(ClientKeySecret::new(record, key)))
    }

    async fn create_client_key(
        &self,
        command: NewClientKey,
        context: &MutationContext,
    ) -> AdminStoreResult<(gateway_admin::model::Revision, ClientKeyRecord)> {
        validate_new_key(&command)?;
        let id = command.id.clone();
        let profile_json = encode_profiles(&command.request_profile_overrides)?;
        let group_ids = command
            .group_ids
            .iter()
            .map(|group| group.as_str().to_owned())
            .collect::<Vec<_>>();
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ClientApiKeyCreate,
            id.as_str(),
            [
                "name",
                "label",
                "group_ids",
                "key",
                "enabled",
                "max_concurrency",
                "requests_per_minute",
                "daily_limit_usd",
                "weekly_limit_usd",
                "provider_request_profiles_json",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        );
        let (revision, ()) = self
            .mutate(audit, None, move |transaction| {
                Box::pin(async move {
                    let now = Utc::now().timestamp_micros();
                    insert_client_key_in_transaction(
                        transaction,
                        &command,
                        &profile_json,
                        &group_ids,
                        now,
                    )
                    .await
                })
            })
            .await?;
        Ok((admin_revision(revision)?, self.required_record(&id).await?))
    }

    async fn update_client_key(
        &self,
        command: UpdateClientKey,
        context: &MutationContext,
    ) -> AdminStoreResult<(gateway_admin::model::Revision, ClientKeyRecord)> {
        validate_name(&command.name)?;
        validate_profile_updates(&command.request_profile_override_updates)?;
        validate_group_ids(
            &command
                .group_ids
                .iter()
                .map(|group| group.as_str().to_owned())
                .collect::<Vec<_>>(),
        )?;
        let id = command.id.clone();
        let mutation_id = id.clone();
        let group_ids = command
            .group_ids
            .iter()
            .map(|group| group.as_str().to_owned())
            .collect::<Vec<_>>();
        let updates = command.request_profile_override_updates;
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ClientApiKeyUpdate,
            id.as_str(),
            [
                "name",
                "label",
                "group_ids",
                "max_concurrency",
                "requests_per_minute",
                "daily_limit_usd",
                "weekly_limit_usd",
                "provider_request_profiles_json",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        );
        let (revision, ()) = self
            .mutate(audit, None, move |transaction| {
                Box::pin(async move {
                    ensure_key_name_available(
                        transaction,
                        mutation_id.as_str(),
                        command.name.trim(),
                    )
                    .await?;
                    ensure_group_ids(transaction, &group_ids).await?;
                    let existing = sqlx::query_scalar::<_, String>(
                        "select provider_request_profiles_json from client_api_keys where id = ?1",
                    )
                    .bind(mutation_id.as_str())
                    .fetch_optional(&mut **transaction)
                    .await
                    .map_err(|_| sqlite_unavailable("load SQLite Client API Key profiles"))?
                    .ok_or_else(|| StoreError::NotFound {
                        entity: ENTITY,
                        id: mutation_id.as_str().to_owned(),
                        source: None,
                    })?;
                    let mut profiles: BTreeMap<String, Map<String, Value>> =
                        serde_json::from_str(&existing)
                            .map_err(|_| invalid_store("invalid stored request profiles"))?;
                    for (provider, value) in updates {
                        match value {
                            Some(value) => {
                                profiles.insert(
                                    provider.as_str().to_owned(),
                                    value.expose_to_provider().clone(),
                                );
                            }
                            None => {
                                profiles.remove(provider.as_str());
                            }
                        }
                    }
                    let profile_json = serde_json::to_string(&profiles)
                        .map_err(|_| invalid_store("encode request profiles"))?;
                    let name_key = normalize_name_key(command.name.trim());
                    let now = Utc::now().timestamp_micros();
                    let updated = sqlx::query(
                        "update client_api_keys set name = ?2, label = ?3,
                           max_concurrency = ?4, requests_per_minute = ?5,
                           updated_at_us = max(updated_at_us, ?6),
                           daily_limit_usd = coalesce(?7, daily_limit_usd),
                           weekly_limit_usd = coalesce(?8, weekly_limit_usd),
                           provider_request_profiles_json = ?9, name_key = ?10
                         where id = ?1",
                    )
                    .bind(mutation_id.as_str())
                    .bind(command.name.trim())
                    .bind(&command.label)
                    .bind(to_i64(command.limits.max_concurrency)?)
                    .bind(to_i64(command.limits.requests_per_minute)?)
                    .bind(now)
                    .bind(command.daily_limit_usd.map(encode_amount))
                    .bind(command.weekly_limit_usd.map(encode_amount))
                    .bind(profile_json)
                    .bind(name_key)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|_| sqlite_unavailable("update SQLite Client API Key"))?;
                    if updated.rows_affected() == 0 {
                        return Err(StoreError::NotFound {
                            entity: ENTITY,
                            id: mutation_id.as_str().to_owned(),
                            source: None,
                        });
                    }
                    sqlx::query("delete from client_api_key_groups where client_api_key_id = ?1")
                        .bind(mutation_id.as_str())
                        .execute(&mut **transaction)
                        .await
                        .map_err(|_| sqlite_unavailable("replace SQLite Client API Key groups"))?;
                    insert_group_bindings(transaction, mutation_id.as_str(), &group_ids, now).await
                })
            })
            .await?;
        Ok((admin_revision(revision)?, self.required_record(&id).await?))
    }

    async fn set_client_key_enabled(
        &self,
        command: SetClientKeyEnabled,
        context: &MutationContext,
    ) -> AdminStoreResult<(gateway_admin::model::Revision, ClientKeyRecord)> {
        let id = command.id.clone();
        let mutation_id = id.clone();
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ClientApiKeyEnabled {
                enabled: command.enabled,
            },
            id.as_str(),
            vec!["enabled".to_owned()],
        );
        let (revision, ()) = self
            .mutate(audit, None, move |transaction| {
                Box::pin(async move {
                    let result = sqlx::query(
                        "update client_api_keys set enabled = ?2, updated_at_us = max(updated_at_us, ?3) where id = ?1",
                    )
                    .bind(mutation_id.as_str())
                    .bind(i64::from(command.enabled))
                    .bind(Utc::now().timestamp_micros())
                    .execute(&mut **transaction)
                    .await
                    .map_err(|_| sqlite_unavailable("set SQLite Client API Key enabled state"))?;
                    if result.rows_affected() == 0 {
                        return Err(StoreError::NotFound { entity: ENTITY, id: mutation_id.as_str().to_owned(), source: None, });
                    }
                    Ok(())
                })
            })
            .await?;
        Ok((admin_revision(revision)?, self.required_record(&id).await?))
    }

    async fn delete_client_key(
        &self,
        command: DeleteClientKey,
        context: &MutationContext,
    ) -> AdminStoreResult<gateway_admin::model::Revision> {
        let id = command.id;
        let audit = mutation_audit(
            context,
            MutationAuditOperation::ClientApiKeyDelete,
            id.as_str(),
            Vec::new(),
        );
        let (revision, ()) = self
            .mutate(audit, None, move |transaction| {
                Box::pin(async move {
                    let result = sqlx::query("delete from client_api_keys where id = ?1")
                        .bind(id.as_str())
                        .execute(&mut **transaction)
                        .await
                        .map_err(|_| sqlite_unavailable("delete SQLite Client API Key"))?;
                    if result.rows_affected() == 0 {
                        return Err(StoreError::NotFound {
                            entity: ENTITY,
                            id: id.as_str().to_owned(),
                            source: None,
                        });
                    }
                    Ok(())
                })
            })
            .await?;
        admin_revision(revision)
    }

    async fn update_client_key_budget_limits(
        &self,
        command: UpdateClientKeyBudgetLimits,
        origin: ClientKeyBudgetMutationOrigin,
        context: &MutationContext,
    ) -> AdminStoreResult<Option<gateway_admin::model::Revision>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin SQLite budget limit update"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            let owner = match &origin {
                ClientKeyBudgetMutationOrigin::Admin => None,
                ClientKeyBudgetMutationOrigin::Plugin(owner) => Some(owner),
            };
            if let Some(owner) = owner {
                verify_plugin_owner(&mut transaction, owner).await?;
            }
            let row = sqlx::query(
                "select daily_limit_usd, weekly_limit_usd from client_api_keys where id = ?1",
            )
            .bind(command.id.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("read SQLite budget limits"))?
            .ok_or_else(|| StoreError::NotFound {
                entity: ENTITY,
                id: command.id.as_str().to_owned(),
                source: None,
            })?;
            let daily_encoded: String = row
                .try_get("daily_limit_usd")
                .map_err(|_| invalid_store("invalid daily budget"))?;
            let daily = decode_amount(&daily_encoded)?;
            let weekly_encoded: String = row
                .try_get("weekly_limit_usd")
                .map_err(|_| invalid_store("invalid weekly budget"))?;
            let weekly = decode_amount(&weekly_encoded)?;
            let daily_changed = command.daily_limit_usd.is_some_and(|value| value != daily);
            let weekly_changed = command
                .weekly_limit_usd
                .is_some_and(|value| value != weekly);
            if !daily_changed && !weekly_changed {
                return Ok(None);
            }
            sqlx::query(
                "update client_api_keys set
                   daily_limit_usd = coalesce(?2, daily_limit_usd),
                   weekly_limit_usd = coalesce(?3, weekly_limit_usd),
                   updated_at_us = max(updated_at_us, ?4)
                 where id = ?1",
            )
            .bind(command.id.as_str())
            .bind(command.daily_limit_usd.map(encode_amount))
            .bind(command.weekly_limit_usd.map(encode_amount))
            .bind(Utc::now().timestamp_micros())
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("update SQLite budget limits"))?;
            let revision =
                bump_config_revision(&mut transaction, Utc::now().timestamp_micros()).await?;
            let mut fields = Vec::new();
            if daily_changed {
                fields.push("daily_limit_usd".to_owned());
            }
            if weekly_changed {
                fields.push("weekly_limit_usd".to_owned());
            }
            let mut audit = mutation_audit(
                context,
                MutationAuditOperation::ClientApiKeyUpdateBudgetLimits,
                command.id.as_str(),
                fields,
            );
            audit.config_revision = Some(
                i64::try_from(revision.get())
                    .map_err(|_| invalid_store("config revision exceeds SQLite INTEGER"))?,
            );
            append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
            Ok(Some(revision))
        }
        .await;
        match result {
            Ok(revision) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| unavailable("commit SQLite budget limits"))?;
                revision.map(admin_revision).transpose()
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable("rollback SQLite budget limit update"))?;
                Err(admin_store_error(ENTITY, error))
            }
        }
    }

    async fn reset_client_key_budget(
        &self,
        command: ResetClientKeyBudget,
        origin: ClientKeyBudgetMutationOrigin,
        context: &MutationContext,
    ) -> AdminStoreResult<()> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin SQLite budget reset"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            if let ClientKeyBudgetMutationOrigin::Plugin(owner) = &origin {
                verify_plugin_owner(&mut transaction, owner).await?;
            }
            let exists =
                sqlx::query_scalar::<_, String>("select id from client_api_keys where id = ?1")
                    .bind(command.id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|_| sqlite_unavailable("lock SQLite budget reset key"))?;
            if exists.is_none() {
                return Err(StoreError::NotFound {
                    entity: ENTITY,
                    id: command.id.as_str().to_owned(),
                    source: None,
                });
            }
            let daily = matches!(
                command.period,
                ClientKeyBudgetPeriod::Daily | ClientKeyBudgetPeriod::All
            );
            let weekly = matches!(
                command.period,
                ClientKeyBudgetPeriod::Weekly | ClientKeyBudgetPeriod::All
            );
            let now = Utc::now().timestamp_micros();
            sqlx::query(
                "update client_key_budget_windows set
                   daily_used_usd = case when ?2 then ?4 else daily_used_usd end,
                   daily_start_us = case when ?2 then ?3 else daily_start_us end,
                   daily_end_us = case when ?2 then ?3 else daily_end_us end,
                   weekly_used_usd = case when ?5 then ?4 else weekly_used_usd end,
                   weekly_start_us = case when ?5 then ?3 else weekly_start_us end,
                   weekly_end_us = case when ?5 then ?3 else weekly_end_us end
                 where client_api_key_id = ?1",
            )
            .bind(command.id.as_str())
            .bind(i64::from(daily))
            .bind(now)
            .bind(encode_amount(Decimal::ZERO))
            .bind(i64::from(weekly))
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("reset SQLite budget"))?;
            let mut fields = Vec::new();
            if daily {
                fields.extend([
                    "daily_used_usd".to_owned(),
                    "daily_start".to_owned(),
                    "daily_end".to_owned(),
                ]);
            }
            if weekly {
                fields.extend([
                    "weekly_used_usd".to_owned(),
                    "weekly_start".to_owned(),
                    "weekly_end".to_owned(),
                ]);
            }
            let audit = mutation_audit(
                context,
                MutationAuditOperation::ClientApiKeyResetBudget,
                command.id.as_str(),
                fields,
            );
            append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => transaction
                .commit()
                .await
                .map_err(|_| unavailable("commit SQLite budget reset")),
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable("rollback SQLite budget reset"))?;
                Err(admin_store_error(ENTITY, error))
            }
        }
    }
}

fn client_record_from_row(row: &SqliteRow) -> StoreResult<crate::postgres::ClientApiKeyRecord> {
    let profiles_json: String = row
        .try_get("provider_request_profiles_json")
        .map_err(|_| invalid_store("invalid request profiles"))?;
    let profiles: BTreeMap<String, Map<String, Value>> = serde_json::from_str(&profiles_json)
        .map_err(|_| invalid_store("invalid request profiles"))?;
    let request_profile_overrides = profiles
        .into_iter()
        .map(|(provider, value)| {
            Ok((
                ProviderKind::new(provider)
                    .map_err(|_| invalid_store("invalid request profile provider"))?,
                OpaqueProviderData::new(value),
            ))
        })
        .collect::<StoreResult<BTreeMap<_, _>>>()?;
    Ok(crate::postgres::ClientApiKeyRecord {
        request_profile_overrides,
        id: row
            .try_get("id")
            .map_err(|_| invalid_store("invalid Client API Key id"))?,
        name: row
            .try_get("name")
            .map_err(|_| invalid_store("invalid Client API Key name"))?,
        label: row
            .try_get("label")
            .map_err(|_| invalid_store("invalid Client API Key label"))?,
        groups: Vec::new(),
        provider_kinds: Vec::new(),
        prefix: row
            .try_get("prefix")
            .map_err(|_| invalid_store("invalid Client API Key prefix"))?,
        enabled: row
            .try_get::<i64, _>("enabled")
            .map_err(|_| invalid_store("invalid Client API Key enabled"))?
            != 0,
        max_concurrency: u64::try_from(
            row.try_get::<i64, _>("max_concurrency")
                .map_err(|_| invalid_store("invalid max concurrency"))?,
        )
        .map_err(|_| invalid_store("invalid max concurrency"))?,
        requests_per_minute: u64::try_from(
            row.try_get::<i64, _>("requests_per_minute")
                .map_err(|_| invalid_store("invalid request rate"))?,
        )
        .map_err(|_| invalid_store("invalid request rate"))?,
        budget: ClientBudgetStatus::default(),
        last_used_at: row
            .try_get::<Option<i64>, _>("last_used_at_us")
            .map_err(|_| invalid_store("invalid last used time"))?
            .map(datetime_from_micros)
            .transpose()?,
        created_at: datetime_from_micros(
            row.try_get("created_at_us")
                .map_err(|_| invalid_store("invalid created time"))?,
        )?,
        updated_at: datetime_from_micros(
            row.try_get("updated_at_us")
                .map_err(|_| invalid_store("invalid updated time"))?,
        )?,
    })
}

async fn load_memberships(
    pool: &SqlitePool,
    records: &mut [crate::postgres::ClientApiKeyRecord],
) -> StoreResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let ids = records
        .iter()
        .map(|record| record.id.as_str())
        .collect::<Vec<_>>();
    let encoded_ids =
        serde_json::to_string(&ids).map_err(|_| invalid_store("encode Client API Key ids"))?;
    let group_rows = sqlx::query(
        "select requested.value as key_id, g.id as group_id, g.name, g.color, g.enabled
         from json_each(?1) requested
         left join client_api_key_groups binding on binding.client_api_key_id = requested.value
         left join account_groups g on g.id = binding.account_group_id
         order by requested.value, g.id",
    )
    .bind(&encoded_ids)
    .fetch_all(pool)
    .await
    .map_err(|_| sqlite_unavailable("load SQLite Client API Key groups"))?;
    let mut groups = BTreeMap::<String, Vec<crate::postgres::ClientApiKeyGroupRecord>>::new();
    for row in group_rows {
        let key_id: String = row
            .try_get("key_id")
            .map_err(|_| invalid_store("invalid Client API Key group key"))?;
        let group_id: Option<String> = row
            .try_get("group_id")
            .map_err(|_| invalid_store("invalid Client API Key group id"))?;
        if let Some(group_id) = group_id {
            groups
                .entry(key_id)
                .or_default()
                .push(crate::postgres::ClientApiKeyGroupRecord {
                    id: group_id,
                    name: row
                        .try_get("name")
                        .map_err(|_| invalid_store("invalid group name"))?,
                    color: row
                        .try_get("color")
                        .map_err(|_| invalid_store("invalid group color"))?,
                    enabled: row
                        .try_get::<i64, _>("enabled")
                        .map_err(|_| invalid_store("invalid group enabled"))?
                        != 0,
                });
        }
    }
    let provider_rows = sqlx::query(
        "with requested(key_id) as (select value from json_each(?1))
         select requested.key_id, accounts.provider_kind
         from requested
         join provider_accounts accounts
         where not exists (
           select 1 from client_api_key_groups binding
           where binding.client_api_key_id = requested.key_id
         )
         union
         select distinct requested.key_id, accounts.provider_kind
         from requested
         join client_api_key_groups binding on binding.client_api_key_id = requested.key_id
         join account_groups groups on groups.id = binding.account_group_id and groups.enabled = 1
         join account_group_accounts members on members.account_group_id = groups.id
         join provider_accounts accounts on accounts.id = members.provider_account_id",
    )
    .bind(&encoded_ids)
    .fetch_all(pool)
    .await
    .map_err(|_| sqlite_unavailable("load SQLite Client API Key providers"))?;
    let mut providers = BTreeMap::<String, BTreeSet<String>>::new();
    for row in provider_rows {
        let key_id: String = row
            .try_get("key_id")
            .map_err(|_| invalid_store("invalid Client API Key provider owner"))?;
        let provider: String = row
            .try_get("provider_kind")
            .map_err(|_| invalid_store("invalid provider kind"))?;
        providers.entry(key_id).or_default().insert(provider);
    }
    for record in records {
        record.groups = groups.remove(&record.id).unwrap_or_default();
        record.provider_kinds = providers
            .remove(&record.id)
            .unwrap_or_default()
            .into_iter()
            .collect();
    }
    Ok(())
}

async fn load_budgets(
    pool: &SqlitePool,
    records: &mut [crate::postgres::ClientApiKeyRecord],
) -> StoreResult<()> {
    if records.is_empty() {
        return Ok(());
    }
    let ids = records
        .iter()
        .map(|record| record.id.as_str())
        .collect::<Vec<_>>();
    let encoded_ids =
        serde_json::to_string(&ids).map_err(|_| invalid_store("encode Client API Key ids"))?;
    let rows = sqlx::query(
        "select k.id, k.daily_limit_usd, k.weekly_limit_usd,
                w.daily_used_usd, w.weekly_used_usd, w.daily_end_us, w.weekly_end_us
         from json_each(?1) requested
         join client_api_keys k on k.id = requested.value
         left join client_key_budget_windows w on w.client_api_key_id = k.id",
    )
    .bind(encoded_ids)
    .fetch_all(pool)
    .await
    .map_err(|_| sqlite_unavailable("load SQLite Client API Key budgets"))?;
    let now = Utc::now().timestamp_micros();
    let mut budgets = BTreeMap::new();
    for row in rows {
        let amount = |field: &'static str| -> StoreResult<Decimal> {
            let value: String = row
                .try_get(field)
                .map_err(|_| invalid_store("invalid stored budget"))?;
            decode_amount(&value)
        };
        let daily_end: Option<i64> = row
            .try_get("daily_end_us")
            .map_err(|_| invalid_store("invalid daily budget expiry"))?;
        let weekly_end: Option<i64> = row
            .try_get("weekly_end_us")
            .map_err(|_| invalid_store("invalid weekly budget expiry"))?;
        let daily_end = daily_end.filter(|end| *end > now);
        let weekly_end = weekly_end.filter(|end| *end > now);
        let daily_used = if daily_end.is_some() {
            amount("daily_used_usd")?
        } else {
            Decimal::ZERO
        };
        let weekly_used = if weekly_end.is_some() {
            amount("weekly_used_usd")?
        } else {
            Decimal::ZERO
        };
        let status = ClientBudgetStatus {
            limits: ClientBudgetLimits {
                daily_usd: amount("daily_limit_usd")?,
                weekly_usd: amount("weekly_limit_usd")?,
            },
            daily_used_usd: daily_used,
            weekly_used_usd: weekly_used,
            daily_resets_at: daily_end.and_then(system_time_from_micros),
            weekly_resets_at: weekly_end.and_then(system_time_from_micros),
        };
        budgets.insert(
            row.try_get::<String, _>("id")
                .map_err(|_| invalid_store("invalid Client API Key id"))?,
            status,
        );
    }
    for record in records {
        record.budget = budgets.remove(&record.id).unwrap_or_default();
    }
    Ok(())
}

fn system_time_from_micros(value: i64) -> Option<SystemTime> {
    let micros = u64::try_from(value).ok()?;
    UNIX_EPOCH.checked_add(Duration::from_micros(micros))
}

fn push_search(statement: &mut QueryBuilder<Sqlite>, search: Option<&str>) {
    if let Some(search) = search {
        statement.push(" and (k.name_key like ");
        statement.push_bind(literal_prefix_pattern(&normalize_name_key(search)));
        statement.push(" escape '\\' or lower(coalesce(k.label, '')) like ");
        statement.push_bind(literal_prefix_pattern(&search.to_lowercase()));
        statement.push(" escape '\\')");
    }
}

fn push_cursor(statement: &mut QueryBuilder<Sqlite>, cursor: &crate::postgres::ClientApiKeyCursor) {
    let comparison = match cursor.sort.direction {
        crate::postgres::ClientApiKeySortDirection::Asc => " > ",
        crate::postgres::ClientApiKeySortDirection::Desc => " < ",
    };
    match &cursor.value {
        crate::postgres::ClientApiKeyCursorValue::Name(value) => {
            statement.push(" and (k.name_key, k.id)");
            statement.push(comparison);
            statement.push("(");
            statement.push_bind(normalize_name_key(value));
            statement.push(", ");
            statement.push_bind(cursor.id.clone());
            statement.push(")");
        }
        crate::postgres::ClientApiKeyCursorValue::Enabled(value) => {
            statement.push(" and (k.enabled, k.id)");
            statement.push(comparison);
            statement.push("(");
            statement.push_bind(i64::from(*value));
            statement.push(", ");
            statement.push_bind(cursor.id.clone());
            statement.push(")");
        }
        crate::postgres::ClientApiKeyCursorValue::CreatedAt(value) => {
            statement.push(" and (k.created_at_us, k.id)");
            statement.push(comparison);
            statement.push("(");
            statement.push_bind(value.timestamp_micros());
            statement.push(", ");
            statement.push_bind(cursor.id.clone());
            statement.push(")");
        }
        crate::postgres::ClientApiKeyCursorValue::LastUsedAt(Some(value)) => {
            statement.push(" and (k.last_used_at_us is null or k.last_used_at_us");
            statement.push(comparison);
            statement.push_bind(value.timestamp_micros());
            statement.push(" or (k.last_used_at_us = ");
            statement.push_bind(value.timestamp_micros());
            statement.push(" and k.id");
            statement.push(comparison);
            statement.push_bind(cursor.id.clone());
            statement.push("))");
        }
        crate::postgres::ClientApiKeyCursorValue::LastUsedAt(None) => {
            statement.push(" and k.last_used_at_us is null and k.id");
            statement.push(comparison);
            statement.push_bind(cursor.id.clone());
        }
    }
}

fn push_order(statement: &mut QueryBuilder<Sqlite>, sort: crate::postgres::ClientApiKeySort) {
    let direction = match sort.direction {
        crate::postgres::ClientApiKeySortDirection::Asc => " asc",
        crate::postgres::ClientApiKeySortDirection::Desc => " desc",
    };
    match sort.field {
        crate::postgres::ClientApiKeySortField::Name => statement.push(" order by k.name_key"),
        crate::postgres::ClientApiKeySortField::Enabled => statement.push(" order by k.enabled"),
        crate::postgres::ClientApiKeySortField::CreatedAt => {
            statement.push(" order by k.created_at_us")
        }
        crate::postgres::ClientApiKeySortField::LastUsedAt => {
            statement.push(" order by case when k.last_used_at_us is null then 1 else 0 end asc, k.last_used_at_us")
        }
    };
    statement.push(direction);
    statement.push(", k.id");
    statement.push(direction);
}

fn literal_prefix_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len().saturating_add(1));
    for character in value.chars() {
        if matches!(character, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped.push('%');
    escaped
}

pub(super) fn validate_new_key(command: &NewClientKey) -> AdminStoreResult<()> {
    validate_name(&command.name)?;
    PlaintextClientApiKey::validate(&command.plaintext)
        .map_err(|_| invalid("invalid Client API Key"))?;
    validate_group_ids(
        &command
            .group_ids
            .iter()
            .map(|id| id.as_str().to_owned())
            .collect::<Vec<_>>(),
    )?;
    validate_profiles(&command.request_profile_overrides)?;
    to_i64(command.limits.max_concurrency).map_err(|error| admin_store_error(ENTITY, error))?;
    to_i64(command.limits.requests_per_minute).map_err(|error| admin_store_error(ENTITY, error))?;
    Ok(())
}

fn validate_name(name: &str) -> AdminStoreResult<()> {
    if name.trim().is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
        return Err(invalid("invalid Client API Key name"));
    }
    Ok(())
}

fn validate_group_ids(group_ids: &[String]) -> AdminStoreResult<()> {
    if group_ids.len() > 1000
        || group_ids.iter().collect::<BTreeSet<_>>().len() != group_ids.len()
        || group_ids
            .iter()
            .any(|id| gateway_core::routing::AccountGroupId::new(id.clone()).is_err())
    {
        return Err(invalid("invalid or duplicate account group ids"));
    }
    Ok(())
}

fn validate_profiles(
    profiles: &BTreeMap<ProviderKind, OpaqueProviderData>,
) -> AdminStoreResult<()> {
    if profiles.len() > 256
        || profiles.values().any(|profile| {
            serde_json::to_vec(profile.expose_to_provider())
                .map_or(true, |bytes| bytes.len() > 64 * 1024)
        })
    {
        return Err(invalid("request profiles exceed supported bounds"));
    }
    Ok(())
}

fn validate_profile_updates(
    updates: &BTreeMap<ProviderKind, Option<OpaqueProviderData>>,
) -> AdminStoreResult<()> {
    if updates.len() > 256
        || updates.values().filter_map(Option::as_ref).any(|profile| {
            serde_json::to_vec(profile.expose_to_provider())
                .map_or(true, |bytes| bytes.len() > 64 * 1024)
        })
    {
        return Err(invalid("request profiles exceed supported bounds"));
    }
    Ok(())
}

fn encode_profiles(
    profiles: &BTreeMap<ProviderKind, OpaqueProviderData>,
) -> AdminStoreResult<String> {
    let values = profiles
        .iter()
        .map(|(provider, profile)| (provider.as_str(), profile.expose_to_provider()))
        .collect::<BTreeMap<_, _>>();
    serde_json::to_string(&values).map_err(|_| invalid("request profiles could not be encoded"))
}

fn to_i64(value: u64) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| invalid_store("numeric limit exceeds SQLite INTEGER"))
}

pub(super) async fn insert_client_key_in_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    command: &NewClientKey,
    profile_json: &str,
    group_ids: &[String],
    now: i64,
) -> StoreResult<()> {
    ensure_key_name_available(transaction, command.id.as_str(), command.name.trim()).await?;
    ensure_group_ids(transaction, group_ids).await?;
    let name_key = normalize_name_key(command.name.trim());
    sqlx::query(
        "insert into client_api_keys (
           id, name, label, key, enabled, max_concurrency, requests_per_minute,
           last_used_at_us, created_at_us, updated_at_us,
           daily_limit_usd, weekly_limit_usd, provider_request_profiles_json, name_key
         ) values (?1, ?2, ?3, ?4, 1, ?5, ?6, null, ?7, ?7, ?8, ?9, ?10, ?11)",
    )
    .bind(command.id.as_str())
    .bind(command.name.trim())
    .bind(&command.label)
    .bind(&command.plaintext)
    .bind(to_i64(command.limits.max_concurrency)?)
    .bind(to_i64(command.limits.requests_per_minute)?)
    .bind(now)
    .bind(encode_amount(command.budget.daily_usd))
    .bind(encode_amount(command.budget.weekly_usd))
    .bind(profile_json)
    .bind(name_key)
    .execute(&mut **transaction)
    .await
    .map_err(|_| sqlite_unavailable("insert SQLite Client API Key"))?;
    insert_group_bindings(transaction, command.id.as_str(), group_ids, now).await
}

async fn ensure_key_name_available(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
    name: &str,
) -> StoreResult<()> {
    let conflict = sqlx::query_scalar::<_, i64>(
        "select exists(select 1 from client_api_keys where id <> ?1 and name_key = ?2)",
    )
    .bind(id)
    .bind(normalize_name_key(name))
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| sqlite_unavailable("check SQLite Client API Key name"))?;
    if conflict != 0 {
        return Err(StoreError::Conflict {
            entity: ENTITY,
            id: id.to_owned(),
            kind: ConflictKind::DuplicateName,
            source: None,
        });
    }
    Ok(())
}

async fn ensure_group_ids(
    transaction: &mut Transaction<'_, Sqlite>,
    ids: &[String],
) -> StoreResult<()> {
    validate_group_ids(ids).map_err(|_| invalid_store("invalid or duplicate account group ids"))?;
    if ids.is_empty() {
        return Ok(());
    }
    let encoded =
        serde_json::to_string(ids).map_err(|_| invalid_store("encode account group ids"))?;
    let found = sqlx::query_scalar::<_, i64>(
        "select count(*) from account_groups where id in (select value from json_each(?1))",
    )
    .bind(encoded)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| sqlite_unavailable("validate SQLite Client API Key groups"))?;
    if usize::try_from(found).ok() != Some(ids.len()) {
        return Err(StoreError::NotFound {
            entity: "account group",
            id: "Client API Key group selection".to_owned(),
            source: None,
        });
    }
    Ok(())
}

async fn insert_group_bindings(
    transaction: &mut Transaction<'_, Sqlite>,
    key_id: &str,
    group_ids: &[String],
    created_at_us: i64,
) -> StoreResult<()> {
    let encoded =
        serde_json::to_string(group_ids).map_err(|_| invalid_store("encode account group ids"))?;
    sqlx::query(
        "insert into client_api_key_groups (client_api_key_id, account_group_id, created_at_us)
         select ?1, value, ?2 from json_each(?3)",
    )
    .bind(key_id)
    .bind(created_at_us)
    .bind(encoded)
    .execute(&mut **transaction)
    .await
    .map_err(|_| sqlite_unavailable("insert SQLite Client API Key groups"))?;
    Ok(())
}

pub(super) async fn verify_plugin_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    owner: &PluginResourceOwner,
) -> StoreResult<()> {
    let revision = i64::try_from(owner.revision.get()).map_err(|_| StoreError::Conflict {
        entity: "plugin mutation",
        id: owner.instance_id.clone(),
        kind: ConflictKind::StaleRevision,
        source: None,
    })?;
    let allowed = sqlx::query_scalar::<_, i64>(
        "select exists(
           select 1 from plugin_instances i
           join plugin_artifacts a on a.sha256 = i.artifact_sha256
           where i.id = ?1 and i.enabled = 1 and i.revision = ?2
             and i.artifact_sha256 = ?3 and a.accepted_at_us is not null
         )",
    )
    .bind(&owner.instance_id)
    .bind(revision)
    .bind(&owner.artifact_sha256)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|_| sqlite_unavailable("verify SQLite plugin mutation owner"))?;
    if allowed == 0 {
        return Err(StoreError::Conflict {
            entity: "plugin mutation",
            id: owner.instance_id.clone(),
            kind: ConflictKind::StaleRevision,
            source: None,
        });
    }
    Ok(())
}

fn invalid(message: &str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::Invalid, ENTITY, message)
}

fn unavailable(operation: &'static str) -> AdminStoreError {
    admin_store_error(ENTITY, sqlite_unavailable(operation))
}

fn not_found(resource: &'static str, id: &str) -> AdminStoreError {
    admin_store_error(
        resource,
        StoreError::NotFound {
            entity: resource,
            id: id.to_owned(),
            source: None,
        },
    )
}

fn invalid_store(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: ENTITY,
        message: message.to_owned(),
        source: None,
    }
}
