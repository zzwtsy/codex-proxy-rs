//! SQLite 账号分组目录、成员事实及事务审计。

use std::{
    collections::BTreeMap,
    str::FromStr as _,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use chrono::Utc;
use futures::future::BoxFuture;
use gateway_admin::{
    model::{
        MutationContext,
        account_groups::{
            AccountGroupAccountSummary, AccountGroupCapacity, AccountGroupColor,
            AccountGroupListQuery, AccountGroupMemberFact, AccountGroupMutation,
            AccountGroupOptionsPage, AccountGroupPage, AccountGroupRecord, AccountGroupRef,
            AccountGroupUsage, DeleteAccountGroup, NewAccountGroup, SetAccountGroupEnabled,
            UpdateAccountGroup,
        },
        observability::DecimalAmount,
    },
    ports::store::{AccountGroupStore, AdminStoreError, AdminStoreResult},
};
use gateway_core::{
    account::{
        AccountErrorReason, AccountStatusFacts, CredentialState, FastMode, QuotaAccessState,
        QuotaEvidence, QuotaState,
    },
    metering::Decimal,
    routing::AccountGroupId,
};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};

use crate::{
    AdminAuditEvent, ConflictKind, Revision, StoreError, StoreResult, admin_revision,
    admin_store_error, mutation_audit,
};

use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, completed_usage_fact_predicate,
    name_key::normalize_name_key, sqlite_unavailable, value::datetime_from_micros,
};

const ENTITY: &str = "account group";

#[derive(Clone)]
pub struct SqliteAccountGroupRepository {
    pool: SqlitePool,
    timezone: gateway_core::time::DeploymentTimeZone,
}

impl SqliteAccountGroupRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            timezone: Default::default(),
        }
    }

    #[must_use]
    pub fn with_timezone(mut self, timezone: gateway_core::time::DeploymentTimeZone) -> Self {
        self.timezone = timezone;
        self
    }

    async fn current_revision(&self) -> AdminStoreResult<gateway_admin::model::Revision> {
        let revision = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable_admin("read config revision"))?
        .ok_or_else(|| not_found_admin("runtime settings", "1"))?;
        let revision =
            u64::try_from(revision).map_err(|_| invalid_admin("invalid config revision"))?;
        admin_revision(Revision::new(revision).map_err(|error| admin_store_error(ENTITY, error))?)
    }

    async fn required_record(&self, id: &AccountGroupId) -> AdminStoreResult<AccountGroupRecord> {
        self.load_record(id.as_str())
            .await?
            .ok_or_else(|| not_found_admin(ENTITY, id.as_str()))
    }

    async fn load_record(&self, id: &str) -> AdminStoreResult<Option<AccountGroupRecord>> {
        let row = sqlx::query(
            "select g.id, g.name, g.description, g.color, g.enabled, g.fast_mode,
                    g.created_at_us, g.updated_at_us,
                    (select count(*) from account_group_accounts m where m.account_group_id = g.id)
                      as member_count,
                    (select count(*) from client_api_key_groups k where k.account_group_id = g.id)
                      as client_key_count
             from account_groups g where g.id = ?1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable_admin("load account group"))?;
        let Some(row) = row else { return Ok(None) };
        let mut records = records_from_rows(vec![row])?;
        let mut record = records
            .pop()
            .ok_or_else(|| invalid_admin("missing account group"))?;
        let mut counts = provider_counts(&self.pool, &[id.to_owned()]).await?;
        record.provider_counts = counts.remove(id).unwrap_or_default();
        let usage = group_usage(&self.pool, &[id.to_owned()], self.timezone).await?;
        if let Some(usage) = usage.get(id) {
            record.usage = usage.clone();
        }
        Ok(Some(record))
    }

    async fn mutate<F>(
        &self,
        mut audit: AdminAuditEvent,
        mutation: F,
    ) -> AdminStoreResult<gateway_admin::model::Revision>
    where
        F: for<'a> FnOnce(&'a mut Transaction<'_, Sqlite>) -> BoxFuture<'a, StoreResult<()>>,
    {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable_admin("begin account group mutation"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            mutation(&mut transaction).await?;
            let revision =
                super::bump_config_revision(&mut transaction, Utc::now().timestamp_micros())
                    .await?;
            audit.config_revision =
                Some(i64::try_from(revision.get()).map_err(|_| invalid("config revision"))?);
            append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
            Ok(revision)
        }
        .await;
        match result {
            Ok(revision) => {
                transaction
                    .commit()
                    .await
                    .map_err(|_| unavailable_admin("commit account group mutation"))?;
                admin_revision(revision)
            }
            Err(error) => {
                transaction
                    .rollback()
                    .await
                    .map_err(|_| unavailable_admin("rollback account group mutation"))?;
                Err(admin_store_error(ENTITY, error))
            }
        }
    }
}

#[async_trait]
impl AccountGroupStore for SqliteAccountGroupRepository {
    async fn list_account_groups(
        &self,
        query: AccountGroupListQuery,
    ) -> AdminStoreResult<AccountGroupPage> {
        validate_page_query(&query)?;
        let mut count =
            sqlx::QueryBuilder::<Sqlite>::new("select count(*) from account_groups g where 1 = 1");
        push_filter(&mut count, &query);
        let total = count
            .build_query_scalar::<i64>()
            .fetch_one(&self.pool)
            .await
            .map_err(|_| unavailable_admin("count account groups"))?;
        let total =
            u64::try_from(total).map_err(|_| invalid_admin("negative account group count"))?;
        let offset = u64::from(query.page.saturating_sub(1))
            .checked_mul(u64::from(query.page_size.get()))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| invalid_admin("page is too large"))?;

        let mut statement = sqlx::QueryBuilder::<Sqlite>::new(
            "select g.id, g.name, g.description, g.color, g.enabled, g.fast_mode,
                    g.created_at_us, g.updated_at_us,
                    (select count(*) from account_group_accounts m where m.account_group_id = g.id)
                      as member_count,
                    (select count(*) from client_api_key_groups k where k.account_group_id = g.id)
                      as client_key_count
             from account_groups g where 1 = 1",
        );
        push_filter(&mut statement, &query);
        statement
            .push(" order by g.created_at_us desc, g.id desc limit ")
            .push_bind(i64::from(query.page_size.get()))
            .push(" offset ")
            .push_bind(offset);
        let rows = statement
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| unavailable_admin("list account groups"))?;
        let mut items = records_from_rows(rows)?;
        let group_ids = items
            .iter()
            .map(|item| item.id.as_str().to_owned())
            .collect::<Vec<_>>();
        let counts = provider_counts(&self.pool, &group_ids).await?;
        for item in &mut items {
            item.provider_counts = counts.get(item.id.as_str()).cloned().unwrap_or_default();
        }
        let usage = group_usage(&self.pool, &group_ids, self.timezone).await?;
        for item in &mut items {
            if let Some(value) = usage.get(item.id.as_str()) {
                item.usage = value.clone();
            }
        }
        Ok(AccountGroupPage {
            config_revision: self.current_revision().await?,
            items,
            total,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn list_account_group_options(
        &self,
        query: AccountGroupListQuery,
    ) -> AdminStoreResult<AccountGroupOptionsPage> {
        validate_page_query(&query)?;
        let mut count =
            sqlx::QueryBuilder::<Sqlite>::new("select count(*) from account_groups g where 1 = 1");
        push_filter(&mut count, &query);
        let total = count
            .build_query_scalar::<i64>()
            .fetch_one(&self.pool)
            .await
            .map_err(|_| unavailable_admin("count account group options"))?;
        let offset = u64::from(query.page.saturating_sub(1))
            .checked_mul(u64::from(query.page_size.get()))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| invalid_admin("page is too large"))?;
        let mut statement = sqlx::QueryBuilder::<Sqlite>::new(
            "select g.id, g.name, g.color, g.enabled from account_groups g where 1 = 1",
        );
        push_filter(&mut statement, &query);
        statement
            .push(" order by g.created_at_us desc, g.id desc limit ")
            .push_bind(i64::from(query.page_size.get()))
            .push(" offset ")
            .push_bind(offset);
        let rows = statement
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| unavailable_admin("list account group options"))?;
        let items = rows
            .iter()
            .map(|row| {
                let id: String = row
                    .try_get("id")
                    .map_err(|_| invalid_admin("group ID is invalid"))?;
                let color: String = row
                    .try_get("color")
                    .map_err(|_| invalid_admin("group color is invalid"))?;
                Ok(AccountGroupRef {
                    id: AccountGroupId::new(id)
                        .map_err(|_| invalid_admin("group ID is invalid"))?,
                    name: row
                        .try_get("name")
                        .map_err(|_| invalid_admin("group name is invalid"))?,
                    color: AccountGroupColor::parse(&color)
                        .ok_or_else(|| invalid_admin("group color is invalid"))?,
                    enabled: row
                        .try_get("enabled")
                        .map_err(|_| invalid_admin("group enabled flag is invalid"))?,
                })
            })
            .collect::<AdminStoreResult<Vec<_>>>()?;
        Ok(AccountGroupOptionsPage {
            config_revision: self.current_revision().await?,
            items,
            total: u64::try_from(total)
                .map_err(|_| invalid_admin("negative account group count"))?,
            page: query.page,
            page_size: query.page_size.get(),
        })
    }

    async fn load_account_group_members(
        &self,
        group_ids: &[AccountGroupId],
    ) -> AdminStoreResult<Vec<AccountGroupMemberFact>> {
        if group_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids = group_ids.iter().map(|id| id.as_str()).collect::<Vec<_>>();
        let encoded_ids =
            serde_json::to_string(&ids).map_err(|_| invalid_admin("invalid account group IDs"))?;
        let rows = sqlx::query(
            "select membership.account_group_id, account.id, account.enabled,
                    account.credential_state, account.access_token_expires_at_us,
                    account.quota_access_state, account.quota_evidence,
                    account.quota_access_observed_at_us, account.quota_reset_at_us,
                    account.last_error_reason, account.last_error_message,
                    account.concurrency_limit, settings.max_concurrent_per_account
             from account_group_accounts membership
             join provider_accounts account on account.id = membership.provider_account_id
             cross join runtime_settings settings
             where settings.id = 1
               and membership.account_group_id in (select value from json_each(?1))
             order by membership.account_group_id, membership.provider_account_id",
        )
        .bind(encoded_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable_admin("load account group members"))?;
        rows.into_iter()
            .map(|row| {
                let group_id = AccountGroupId::new(read_text(&row, "account_group_id")?)
                    .map_err(|_| invalid("invalid group ID"))?;
                let account_id = read_text(&row, "id")?;
                let enabled = read_i64(&row, "enabled")? != 0;
                let credential_state =
                    CredentialState::parse(&read_text(&row, "credential_state")?)
                        .ok_or_else(|| invalid("invalid credential state"))?;
                let access_token_expires_at =
                    read_optional_i64(&row, "access_token_expires_at_us")?
                        .map(system_time_from_micros)
                        .transpose()?;
                let quota_access = QuotaAccessState::parse(&read_text(&row, "quota_access_state")?)
                    .ok_or_else(|| invalid("invalid quota access state"))?;
                let quota_evidence = read_optional_text(&row, "quota_evidence")?
                    .map(|value| {
                        QuotaEvidence::parse(&value)
                            .ok_or_else(|| invalid("invalid quota evidence"))
                    })
                    .transpose()?;
                let quota_observed = read_optional_i64(&row, "quota_access_observed_at_us")?
                    .map(system_time_from_micros)
                    .transpose()?;
                let quota_reset = read_optional_i64(&row, "quota_reset_at_us")?
                    .map(system_time_from_micros)
                    .transpose()?;
                let quota = QuotaState::from_persisted(
                    quota_access,
                    quota_evidence,
                    quota_observed,
                    quota_reset,
                )
                .ok_or_else(|| invalid("inconsistent persisted quota state"))?;
                let last_error_reason = read_optional_text(&row, "last_error_reason")?
                    .map(|value| {
                        AccountErrorReason::parse(&value)
                            .ok_or_else(|| invalid("invalid account error reason"))
                    })
                    .transpose()?;
                let last_error_message = read_optional_text(&row, "last_error_message")?;
                let custom_slots = read_optional_i64(&row, "concurrency_limit")?
                    .map(|value| {
                        u64::try_from(value).map_err(|_| invalid("invalid concurrency limit"))
                    })
                    .transpose()?;
                let default_slots = u64::try_from(read_i64(&row, "max_concurrent_per_account")?)
                    .map_err(|_| invalid("invalid default concurrency"))?;
                Ok(AccountGroupMemberFact {
                    group_id,
                    account_id,
                    status: AccountStatusFacts {
                        enabled,
                        credential_state,
                        access_token_expires_at,
                        quota,
                        cooldown: None,
                        last_error_reason,
                        last_error_message,
                    },
                    total_slots: custom_slots
                        .or_else(|| (default_slots > 0).then_some(default_slots)),
                })
            })
            .collect::<StoreResult<Vec<_>>>()
            .map_err(|error| admin_store_error(ENTITY, error))
    }

    async fn create_account_group(
        &self,
        command: NewAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        validate_group_fields(&command.name, command.description.as_deref())?;
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            gateway_admin::model::audit::MutationAuditOperation::AccountGroupCreate,
            id.as_str(),
            vec![
                "name".to_owned(),
                "description".to_owned(),
                "fast_mode".to_owned(),
            ],
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let now = Utc::now().timestamp_micros();
                    let name_key = normalize_name_key(&command.name);
                    sqlx::query(
                        "insert into account_groups
                         (id, name, description, color, fast_mode, enabled, created_at_us, updated_at_us, name_key)
                         values (?1, ?2, ?3, ?4, ?5, 1, ?6, ?6, ?7)",
                    )
                    .bind(command.id.as_str())
                    .bind(&command.name)
                    .bind(&command.description)
                    .bind(command.color.as_str())
                    .bind(command.fast_mode.as_str())
                    .bind(now)
                    .bind(name_key)
                    .execute(&mut **transaction)
                    .await
                    .map_err(map_write_error)?;
                    Ok(())
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id: id.clone(),
            record: Some(self.required_record(&id).await?),
        })
    }

    async fn update_account_group(
        &self,
        command: UpdateAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        validate_group_fields(&command.name, command.description.as_deref())?;
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            gateway_admin::model::audit::MutationAuditOperation::AccountGroupUpdate,
            id.as_str(),
            vec![
                "name".to_owned(),
                "description".to_owned(),
                "fast_mode".to_owned(),
            ],
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let name_key = normalize_name_key(&command.name);
                    let now = Utc::now().timestamp_micros();
                    let result = sqlx::query(
                        "update account_groups set name = ?2, description = ?3, color = ?4,
                           fast_mode = coalesce(?5, fast_mode),
                           updated_at_us = max(updated_at_us, ?6), name_key = ?7
                         where id = ?1",
                    )
                    .bind(command.id.as_str())
                    .bind(command.name)
                    .bind(command.description)
                    .bind(command.color.as_str())
                    .bind(command.fast_mode.map(FastMode::as_str))
                    .bind(now)
                    .bind(name_key)
                    .execute(&mut **transaction)
                    .await
                    .map_err(map_write_error)?;
                    require_one(result.rows_affected(), command.id.as_str())
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id: id.clone(),
            record: Some(self.required_record(&id).await?),
        })
    }

    async fn set_account_group_enabled(
        &self,
        command: SetAccountGroupEnabled,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            gateway_admin::model::audit::MutationAuditOperation::AccountGroupEnabled {
                enabled: command.enabled,
            },
            id.as_str(),
            vec!["enabled".to_owned()],
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let now = Utc::now().timestamp_micros();
                    let result = sqlx::query(
                        "update account_groups
                         set enabled = ?2, updated_at_us = max(updated_at_us, ?3)
                         where id = ?1",
                    )
                    .bind(command.id.as_str())
                    .bind(i64::from(command.enabled))
                    .bind(now)
                    .execute(&mut **transaction)
                    .await
                    .map_err(|_| unavailable("set account group state"))?;
                    require_one(result.rows_affected(), command.id.as_str())
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id: id.clone(),
            record: Some(self.required_record(&id).await?),
        })
    }

    async fn delete_account_group(
        &self,
        command: DeleteAccountGroup,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountGroupMutation> {
        let id = command.id.clone();
        let audit = mutation_audit(
            context,
            gateway_admin::model::audit::MutationAuditOperation::AccountGroupDelete,
            id.as_str(),
            Vec::new(),
        );
        let revision = self
            .mutate(audit, |transaction| {
                Box::pin(async move {
                    let deleted = sqlx::query(
                        "delete from account_groups where id = ?1 and not exists (
                           select 1 from client_api_key_groups where account_group_id = ?1
                         )",
                    )
                    .bind(command.id.as_str())
                    .execute(&mut **transaction)
                    .await
                    .map_err(|_| unavailable("delete account group"))?
                    .rows_affected();
                    if deleted == 1 {
                        return Ok(());
                    }
                    let exists = sqlx::query_scalar::<_, i64>(
                        "select exists(select 1 from account_groups where id = ?1)",
                    )
                    .bind(command.id.as_str())
                    .fetch_one(&mut **transaction)
                    .await
                    .map_err(|_| unavailable("check account group delete conflict"))?;
                    if exists != 0 {
                        Err(StoreError::Conflict {
                            entity: ENTITY,
                            id: command.id.as_str().to_owned(),
                            kind: ConflictKind::InvalidTransition,
                            source: None,
                        })
                    } else {
                        Err(StoreError::NotFound {
                            entity: ENTITY,
                            id: command.id.as_str().to_owned(),
                            source: None,
                        })
                    }
                })
            })
            .await?;
        Ok(AccountGroupMutation {
            config_revision: revision,
            id,
            record: None,
        })
    }
}

fn push_filter(statement: &mut sqlx::QueryBuilder<Sqlite>, query: &AccountGroupListQuery) {
    if let Some(search) = &query.search {
        statement.push(" and g.name_key like ");
        statement.push_bind(format!("%{}%", normalize_name_key(search)));
    }
    if let Some(enabled) = query.enabled {
        statement.push(" and g.enabled = ");
        statement.push_bind(i64::from(enabled));
    }
}

fn records_from_rows(
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> AdminStoreResult<Vec<AccountGroupRecord>> {
    let zero =
        DecimalAmount::from_str("0").map_err(|_| invalid_admin("invalid zero usage amount"))?;
    rows.into_iter()
        .map(|row| {
            let id_value =
                read_text(&row, "id").map_err(|error| admin_store_error(ENTITY, error))?;
            let id = AccountGroupId::new(id_value)
                .map_err(|_| invalid_admin("invalid account group ID"))?;
            let name = read_text(&row, "name").map_err(|error| admin_store_error(ENTITY, error))?;
            let color = AccountGroupColor::parse(
                &read_text(&row, "color").map_err(|error| admin_store_error(ENTITY, error))?,
            )
            .ok_or_else(|| invalid_admin("invalid account group color"))?;
            let description = read_optional_text(&row, "description")
                .map_err(|error| admin_store_error(ENTITY, error))?;
            let created_at = datetime_from_micros(
                read_i64(&row, "created_at_us")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
            )
            .map_err(|error| admin_store_error(ENTITY, error))?;
            let updated_at = datetime_from_micros(
                read_i64(&row, "updated_at_us")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
            )
            .map_err(|error| admin_store_error(ENTITY, error))?;
            Ok(AccountGroupRecord {
                fast_mode: FastMode::parse(
                    &read_text(&row, "fast_mode")
                        .map_err(|error| admin_store_error(ENTITY, error))?,
                )
                .ok_or_else(|| invalid_admin("invalid account group fast mode"))?,
                id,
                name,
                description,
                color,
                enabled: read_i64(&row, "enabled")
                    .map_err(|error| admin_store_error(ENTITY, error))?
                    != 0,
                member_count: nonnegative_count(&row, "member_count")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
                provider_counts: BTreeMap::new(),
                client_key_count: nonnegative_count(&row, "client_key_count")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
                account_summary: AccountGroupAccountSummary {
                    available: 0,
                    limited: 0,
                    total: 0,
                },
                capacity: AccountGroupCapacity {
                    used_slots: None,
                    total_slots: Some(0),
                },
                usage: AccountGroupUsage {
                    today_usd: zero.clone(),
                    retained_total_usd: zero.clone(),
                },
                created_at,
                updated_at,
            })
        })
        .collect()
}

async fn provider_counts(
    pool: &SqlitePool,
    group_ids: &[String],
) -> AdminStoreResult<BTreeMap<String, BTreeMap<String, u64>>> {
    if group_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let encoded_ids =
        serde_json::to_string(group_ids).map_err(|_| invalid_admin("invalid account group IDs"))?;
    let rows = sqlx::query(
        "select membership.account_group_id, account.provider_kind, count(*) as provider_count
         from account_group_accounts membership
         join provider_accounts account on account.id = membership.provider_account_id
         where membership.account_group_id in (select value from json_each(?1))
         group by membership.account_group_id, account.provider_kind",
    )
    .bind(encoded_ids)
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable_admin("load account group provider counts"))?;
    let mut result = BTreeMap::new();
    for row in rows {
        result
            .entry(
                read_text(&row, "account_group_id")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
            )
            .or_insert_with(BTreeMap::new)
            .insert(
                read_text(&row, "provider_kind")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
                nonnegative_count(&row, "provider_count")
                    .map_err(|error| admin_store_error(ENTITY, error))?,
            );
    }
    Ok(result)
}

async fn group_usage(
    pool: &SqlitePool,
    group_ids: &[String],
    timezone: gateway_core::time::DeploymentTimeZone,
) -> AdminStoreResult<BTreeMap<String, AccountGroupUsage>> {
    if group_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let now = Utc::now();
    let day_start = timezone
        .day_start(now)
        .ok_or_else(|| invalid_admin("invalid business date"))?
        .timestamp_micros();
    let usage_retention_days = sqlx::query_scalar::<_, i64>(
        "select usage_retention_days from runtime_settings where id = 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(|_| unavailable_admin("read usage retention"))?
    .ok_or_else(|| not_found_admin("runtime settings", "1"))?;
    let now_us = now.timestamp_micros();
    let retention_cutoff = now_us.saturating_sub(
        usage_retention_days
            .max(0)
            .saturating_mul(1_000_000 * 60 * 60 * 24),
    );
    let encoded_ids =
        serde_json::to_string(group_ids).map_err(|_| invalid_admin("invalid account group IDs"))?;
    let mut query = QueryBuilder::<Sqlite>::new(
        "select membership.account_group_id, request.cost_amount, request.started_at_us
         from account_group_accounts membership
         join model_request_observations request
           on request.provider_account_ref = membership.provider_account_id
         where membership.account_group_id in (select value from json_each(",
    );
    query
        .push_bind(encoded_ids)
        .push(")) and request.started_at_us >= ")
        .push_bind(retention_cutoff)
        .push(" and (")
        .push(completed_usage_fact_predicate("request"))
        .push(
            ")
           and request.cost_currency = 'USD' and request.cost_amount is not null
         order by membership.account_group_id, request.id",
        );
    let rows = query
        .build()
        .fetch_all(pool)
        .await
        .map_err(|_| unavailable_admin("load account group costs"))?;
    let mut totals: BTreeMap<String, (Decimal, Decimal)> = BTreeMap::new();
    for row in rows {
        let group_id = read_text(&row, "account_group_id")
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let encoded_amount =
            read_text(&row, "cost_amount").map_err(|error| admin_store_error(ENTITY, error))?;
        let amount = super::value::decode_amount(&encoded_amount)
            .map_err(|error| admin_store_error(ENTITY, error))?;
        let started_at =
            read_i64(&row, "started_at_us").map_err(|error| admin_store_error(ENTITY, error))?;
        let totals = totals
            .entry(group_id)
            .or_insert((Decimal::ZERO, Decimal::ZERO));
        totals.1 = totals
            .1
            .checked_add(amount)
            .ok_or_else(|| invalid_admin("usage total overflow"))?;
        if started_at >= day_start {
            totals.0 = totals
                .0
                .checked_add(amount)
                .ok_or_else(|| invalid_admin("daily usage overflow"))?;
        }
    }
    let mut result = BTreeMap::new();
    for group_id in group_ids {
        let (today, retained) = totals.get(group_id).copied().unwrap_or_default();
        result.insert(
            group_id.clone(),
            AccountGroupUsage {
                today_usd: DecimalAmount::from_str(&today.canonical())
                    .map_err(|_| invalid_admin("invalid daily usage"))?,
                retained_total_usd: DecimalAmount::from_str(&retained.canonical())
                    .map_err(|_| invalid_admin("invalid retained usage"))?,
            },
        );
    }
    Ok(result)
}

fn validate_page_query(query: &AccountGroupListQuery) -> AdminStoreResult<()> {
    if query.page == 0 {
        return Err(invalid_admin("page must be positive"));
    }
    if query.search.as_deref().is_some_and(|search| {
        search.trim().is_empty() || search.len() > 256 || search.chars().any(char::is_control)
    }) {
        return Err(invalid_admin("invalid search"));
    }
    Ok(())
}

pub(super) fn validate_group_fields(name: &str, description: Option<&str>) -> AdminStoreResult<()> {
    if name.trim() != name
        || name.is_empty()
        || name.chars().count() > 100
        || name.chars().any(char::is_control)
        || description
            .is_some_and(|value| value.len() > 4096 || value.chars().any(char::is_control))
    {
        return Err(invalid_admin("invalid account group fields"));
    }
    Ok(())
}

fn require_one(rows: u64, id: &str) -> StoreResult<()> {
    if rows == 1 {
        Ok(())
    } else {
        Err(StoreError::NotFound {
            entity: ENTITY,
            id: id.to_owned(),
            source: None,
        })
    }
}

pub(super) fn map_write_error(error: sqlx::Error) -> StoreError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        StoreError::Conflict {
            entity: ENTITY,
            id: "duplicate".to_owned(),
            kind: ConflictKind::DuplicateName,
            source: None,
        }
    } else {
        unavailable("write account group")
    }
}

fn read_i64(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<i64> {
    row.try_get(field)
        .map_err(|_| invalid("invalid stored integer"))
}

fn read_optional_i64(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> StoreResult<Option<i64>> {
    row.try_get(field)
        .map_err(|_| invalid("invalid stored nullable integer"))
}

fn read_text(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<String> {
    row.try_get(field)
        .map_err(|_| invalid("invalid stored text"))
}

fn read_optional_text(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> StoreResult<Option<String>> {
    row.try_get(field)
        .map_err(|_| invalid("invalid stored nullable text"))
}

fn nonnegative_count(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<u64> {
    u64::try_from(read_i64(row, field)?).map_err(|_| invalid("negative stored count"))
}

fn system_time_from_micros(value: i64) -> StoreResult<SystemTime> {
    let datetime = datetime_from_micros(value)?;
    let delta = u64::try_from(datetime.timestamp_micros())
        .map_err(|_| invalid("negative account timestamp"))?;
    Ok(SystemTime::UNIX_EPOCH + Duration::from_micros(delta))
}

fn invalid_admin(message: &str) -> AdminStoreError {
    admin_store_error(ENTITY, invalid(message))
}

fn unavailable_admin(message: &'static str) -> AdminStoreError {
    admin_store_error(ENTITY, unavailable(message))
}

fn not_found_admin(entity: &'static str, id: &str) -> AdminStoreError {
    admin_store_error(
        ENTITY,
        StoreError::NotFound {
            entity,
            id: id.to_owned(),
            source: None,
        },
    )
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: ENTITY,
        message: message.to_owned(),
        source: None,
    }
}

fn unavailable(message: &'static str) -> StoreError {
    sqlite_unavailable(message)
}
