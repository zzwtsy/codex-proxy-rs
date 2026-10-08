//! SQLite Admin 账号查询与凭据事务适配

use std::{collections::HashMap, str::FromStr, time::SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_admin::{
    model::{
        MutationContext, Revision,
        account_groups::{AccountGroupColor, AccountGroupRef},
        accounts::{
            AccountCapacity, AccountCost, AccountGroupFilter, AccountListQuery, AccountModelUsage,
            AccountPage, AccountPageItem, AccountRecord, AccountRequestBucket,
            AccountRuntimeSnapshot, AccountSortField, AccountSummary, AccountUpdateResult,
            AccountUsage, AccountUsageWindowQuery, AccountUsageWindowResult, AccountsUpdateResult,
            BatchUpdateAccounts, DeleteAccounts, SortDirection, UpdateAccount,
        },
        audit::MutationAuditOperation,
        observability::{CostCoverage, DecimalAmount, TimeRange},
        provider_credentials::{
            AuthorizationCommit, AuthorizationCommitResult, AuthorizationCredentialCommit,
            AuthorizationMutationTarget, AuthorizationReceiptKey, CredentialDetails,
            CredentialImportCommit, CredentialImportResult, CredentialMutationResult,
            CredentialRotationCommit, PluginAccountListQuery, PluginAccountPage,
            PreparedCredentialImport, ProviderDocument, ProviderExportCredentialInput,
        },
        quota_forecast_sampling::{QuotaForecastHistory, QuotaForecastUsage},
    },
    ports::store::{AccountStore, AdminStoreError, AdminStoreErrorKind, AdminStoreResult},
};
use gateway_core::{
    account::{
        AccountConcurrencyLimit, AccountStatus, OpaqueProviderData, ProviderAccount,
        ProviderAccountId, ProviderAccountStore,
    },
    provider_ports::ProviderCooldownPort,
    routing::ProviderKind,
};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, Transaction};

use crate::{
    StoreError, StoreResult, admin_revision, admin_store_error, mutation_audit,
    sqlite::value::datetime_from_micros,
};

use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, bump_config_revision,
    sqlite_unavailable,
};

const fn account_status_sort_rank(status: AccountStatus) -> u8 {
    match status {
        AccountStatus::Normal => 0,
        AccountStatus::RateLimited => 1,
        AccountStatus::QuotaExhausted => 2,
        AccountStatus::Error => 3,
        AccountStatus::Disabled => 4,
    }
}

#[derive(Clone)]
pub struct SqliteAdminAccountStore {
    pool: SqlitePool,
    accounts: super::SqliteProviderAccountRepository,
}

struct AccountMetadata {
    notes: Option<String>,
    credential_observed_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

struct AccountSettingsUpdate<'a> {
    account_ids: &'a [String],
    enabled: Option<bool>,
    concurrency_limit: Option<Option<AccountConcurrencyLimit>>,
    weight: Option<gateway_core::account::AccountWeight>,
    model_access: Option<&'a gateway_core::account::AccountModelAccess>,
    group_ids: Option<&'a [gateway_core::routing::AccountGroupId]>,
    outbound_proxy: Option<&'a gateway_admin::model::proxies::AccountProxySelection>,
    notes: Option<&'a str>,
}

struct CredentialImportTransaction<'a> {
    prepared: PreparedCredentialImport,
    settings: Option<gateway_admin::model::accounts::AccountImportSettings>,
    context: &'a MutationContext,
    operation: MutationAuditOperation,
    outbound_proxy: Option<gateway_admin::model::proxies::ImportProxyBinding>,
    revision: crate::Revision,
}

impl SqliteAdminAccountStore {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            accounts: super::SqliteProviderAccountRepository::new(pool.clone()),
            pool,
        }
    }

    async fn metadata_for(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<HashMap<String, AccountMetadata>> {
        let mut metadata = HashMap::new();
        for chunk in account_ids.chunks(500) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "select id, notes, credential_observed_at_us, created_at_us, updated_at_us
                   from provider_accounts where id in ({placeholders})"
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|_| unavailable("load account metadata"))?;
            for row in rows {
                let id: String = row
                    .try_get("id")
                    .map_err(|_| invalid("decode account metadata"))?;
                let credential_observed_at = decode_time(
                    row.try_get("credential_observed_at_us")
                        .map_err(|_| invalid("decode account metadata"))?,
                )?;
                let created_at = decode_time(
                    row.try_get("created_at_us")
                        .map_err(|_| invalid("decode account metadata"))?,
                )?;
                let updated_at = decode_time(
                    row.try_get("updated_at_us")
                        .map_err(|_| invalid("decode account metadata"))?,
                )?;
                metadata.insert(
                    id,
                    AccountMetadata {
                        notes: row
                            .try_get("notes")
                            .map_err(|_| invalid("decode account metadata"))?,
                        credential_observed_at,
                        created_at,
                        updated_at,
                    },
                );
            }
        }
        Ok(metadata)
    }

    async fn groups_for(
        &self,
        account_ids: &[String],
    ) -> AdminStoreResult<HashMap<String, Vec<AccountGroupRef>>> {
        let mut groups: HashMap<String, Vec<AccountGroupRef>> = HashMap::new();
        for chunk in account_ids.chunks(500) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "select membership.provider_account_id as account_id, g.id, g.name, g.color, g.enabled
                   from account_group_accounts membership
                   join account_groups g on g.id = membership.account_group_id
                  where membership.provider_account_id in ({placeholders})
                  order by membership.provider_account_id, g.name, g.id"
            );
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(&self.pool)
                .await
                .map_err(|_| unavailable("load account groups"))?;
            for row in rows {
                let account_id: String = row
                    .try_get("account_id")
                    .map_err(|_| invalid("decode account groups"))?;
                let id: String = row
                    .try_get("id")
                    .map_err(|_| invalid("decode account groups"))?;
                let name: String = row
                    .try_get("name")
                    .map_err(|_| invalid("decode account groups"))?;
                let color: String = row
                    .try_get("color")
                    .map_err(|_| invalid("decode account groups"))?;
                let enabled: i64 = row
                    .try_get("enabled")
                    .map_err(|_| invalid("decode account groups"))?;
                let reference = AccountGroupRef {
                    id: gateway_core::routing::AccountGroupId::new(id)
                        .map_err(|_| invalid("decode account group ID"))?,
                    name,
                    color: AccountGroupColor::parse(&color)
                        .ok_or_else(|| invalid("decode account group color"))?,
                    enabled: enabled != 0,
                };
                groups.entry(account_id).or_default().push(reference);
            }
        }
        Ok(groups)
    }

    async fn account_record(&self, account: ProviderAccount) -> AdminStoreResult<AccountRecord> {
        let id = account.id().as_str().to_owned();
        let mut metadata = self.metadata_for(std::slice::from_ref(&id)).await?;
        let metadata = metadata.remove(&id).ok_or_else(|| {
            AdminStoreError::new(
                AdminStoreErrorKind::NotFound,
                "account",
                "account not found",
            )
        })?;
        let mut groups = self.groups_for(std::slice::from_ref(&id)).await?;
        account_record(account, metadata, groups.remove(&id).unwrap_or_default())
    }

    async fn usage_sort_values(
        &self,
        now: DateTime<Utc>,
    ) -> AdminStoreResult<HashMap<String, (u64, Option<DateTime<Utc>>)>> {
        let retention_days = sqlx::query_scalar::<_, i64>(
            "select usage_retention_days from runtime_settings where id = 1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|_| unavailable("load account usage retention"))?;
        let retention_micros = retention_days
            .checked_mul(86_400_000_000)
            .ok_or_else(|| invalid("usage retention overflows"))?;
        let start_us = now
            .timestamp_micros()
            .checked_sub(retention_micros)
            .ok_or_else(|| invalid("usage retention range overflows"))?;
        let rows = sqlx::query(
            "select mr.provider_account_ref,
                    coalesce(sum(coalesce(
                      mr.total_tokens,
                      coalesce(mr.input_tokens, 0) + coalesce(mr.output_tokens, 0)
                    )), 0) as total_tokens,
                    max(mr.started_at_us) as last_used_at_us
               from model_requests mr
              where mr.provider_account_ref is not null
                and mr.started_at_us >= ?1 and mr.started_at_us < ?2
                and mr.outcome = 'succeeded'
                and mr.downstream_committed_at_us is not null
                and (mr.provider_kind is not 'openai' or mr.request_kind is not 'prewarm')
                and ((mr.client_transport = 'websocket' and mr.client_status_code is null)
                     or mr.client_status_code between 200 and 399)
                and mr.recovered_at_us is null
                and (mr.requested_model_id is not null or mr.upstream_model_id is not null
                     or mr.image_generation_requested = 1 or mr.input_tokens is not null
                     or mr.output_tokens is not null or mr.cached_tokens is not null
                     or mr.cache_write_tokens is not null or mr.reasoning_tokens is not null
                     or mr.image_input_tokens is not null or mr.image_output_tokens is not null
                     or mr.total_tokens is not null or mr.cost_amount is not null)
              group by mr.provider_account_ref",
        )
        .bind(start_us)
        .bind(now.timestamp_micros())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| unavailable("load account usage sort values"))?;
        let mut values = HashMap::with_capacity(rows.len());
        for row in rows {
            let account_id: String = row
                .try_get("provider_account_ref")
                .map_err(|_| invalid("decode account usage sort values"))?;
            let total_tokens = row
                .try_get::<i64, _>("total_tokens")
                .map_err(|_| invalid("decode account usage sort values"))?;
            let last_used_at = row
                .try_get::<Option<i64>, _>("last_used_at_us")
                .map_err(|_| invalid("decode account usage sort values"))?
                .map(decode_time)
                .transpose()?;
            values.insert(
                account_id,
                (
                    u64::try_from(total_tokens)
                        .map_err(|_| invalid("decode account usage token total"))?,
                    last_used_at,
                ),
            );
        }
        Ok(values)
    }

    async fn config_revision(&self) -> AdminStoreResult<Revision> {
        let revision = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable("load config revision"))?
        .ok_or_else(|| {
            AdminStoreError::new(
                AdminStoreErrorKind::NotFound,
                "runtime settings",
                "runtime settings not found",
            )
        })?;
        Revision::new(u64::try_from(revision).map_err(|_| invalid("decode config revision"))?)
            .map_err(|_| invalid("decode config revision"))
    }

    async fn loaded_credential(
        &self,
        account_id: &ProviderAccountId,
    ) -> AdminStoreResult<Option<gateway_core::account::LoadedCredential>> {
        let Some(account) = self
            .accounts
            .get_account(account_id)
            .await
            .map_err(|error| core_error("load provider account", error))?
        else {
            return Ok(None);
        };
        self.accounts
            .load_credential(account_id, account.revision())
            .await
            .map(Some)
            .map_err(|error| core_error("load provider credential", error))
    }

    async fn page_item(
        &self,
        account: ProviderAccount,
        runtime: &AccountRuntimeSnapshot,
        default_concurrency: u64,
        observed_at: DateTime<Utc>,
    ) -> AdminStoreResult<AccountPageItem> {
        let id = account.id().as_str().to_owned();
        let projection = account.status_projection(
            SystemTime::from(observed_at),
            runtime.cooldown.get(&id).copied(),
        );
        let record = self.account_record(account).await?;
        let total_slots = record
            .concurrency_limit
            .map(|limit| u64::from(limit.get()))
            .or_else(|| (default_concurrency > 0).then_some(default_concurrency));
        let used_slots = runtime
            .in_flight
            .as_ref()
            .map(|counts| counts.get(&id).copied().unwrap_or(0));
        Ok(AccountPageItem {
            account: record,
            projection,
            capacity: AccountCapacity {
                used_slots,
                total_slots,
            },
        })
    }

    async fn commit_rotation(
        &self,
        command: CredentialRotationCommit,
        context: &MutationContext,
        operation: MutationAuditOperation,
        refresh_only: bool,
    ) -> AdminStoreResult<CredentialMutationResult> {
        if refresh_only && command.settings.is_some() {
            return Err(invalid("credential refresh cannot change account settings"));
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin credential rotation transaction"))?;
        let result = async {
            acquire_write_lock(&mut transaction)
                .await
                .map_err(|error| admin_store_error("SQLite account", error))?;
            let revision = bump_config_revision(&mut transaction, Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("SQLite account", error))?;
            self.rotate_credential_in_transaction(
                &mut transaction,
                command.prepared,
                command.settings,
                context,
                operation,
                revision,
            )
            .await
        }
        .await;
        finish_account_admin_transaction(transaction, result, "credential rotation").await
    }

    async fn rotate_credential_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        prepared: gateway_admin::model::provider_credentials::PreparedCredentialRotationFacts,
        settings: Option<UpdateAccount>,
        context: &MutationContext,
        operation: MutationAuditOperation,
        revision: crate::Revision,
    ) -> AdminStoreResult<CredentialMutationResult> {
        let account_id = prepared.account_id.as_str().to_owned();
        if let Some(settings) = &settings
            && settings.account_id != account_id
        {
            return Err(invalid("credential and account settings IDs differ"));
        }
        let provider_json =
            serde_json::Value::Object(prepared.provider_material.into_provider_data().into_inner());
        let encoded_credentials = serde_json::to_string(&provider_json)
            .map_err(|_| invalid("encode provider credentials"))?;
        if encoded_credentials.len() > 256 * 1024 {
            return Err(invalid("provider credentials exceed the size limit"));
        }
        if let Some(identity) = &prepared.replacement_identity
            && (identity.upstream_user_id().trim().is_empty()
                || identity
                    .upstream_account_id()
                    .is_some_and(|value| value.trim().is_empty()))
        {
            return Err(invalid("replacement provider identity is invalid"));
        }
        let now = Utc::now().timestamp_micros();
        let expected = i64::try_from(prepared.expected_credential_revision.get())
            .map_err(|_| invalid("credential revision exceeds SQLite INTEGER"))?;
        let replacement_user = prepared
            .replacement_identity
            .as_ref()
            .map(gateway_core::account::ProviderAccountIdentity::upstream_user_id);
        let replacement_account = prepared
            .replacement_identity
            .as_ref()
            .and_then(gateway_core::account::ProviderAccountIdentity::upstream_account_id);
        let row = sqlx::query(
            "update provider_accounts
                set name = case when ?1 = 1 then name else ?2 end,
                    email = case when ?1 = 1 then email else ?3 end,
                    plan_type = case when ?1 = 1 then plan_type else ?4 end,
                    provider_credentials_json = ?5,
                    credential_revision = credential_revision + 1,
                    has_refresh_token = ?6,
                    access_token_expires_at_us = ?7,
                    next_refresh_at_us = ?8,
                    upstream_user_id = case when ?9 = 1 then ?10 else upstream_user_id end,
                    upstream_account_id = case when ?9 = 1 then ?11 else upstream_account_id end,
                    credential_state = case when ?12 = 1 then credential_state
                      when ?9 = 1 or credential_state <> 'unknown' then 'ready' else 'unknown' end,
                    credential_observed_at_us = case when ?12 = 1 then credential_observed_at_us else ?13 end,
                    last_error_reason = case when ?12 = 1 then last_error_reason else null end,
                    last_error_message = case when ?12 = 1 then last_error_message else null end,
                    updated_at_us = max(updated_at_us, ?13)
              where id = ?14 and provider_kind = ?15 and credential_revision = ?16
              returning credential_revision",
        )
        .bind(i64::from(prepared.preserve_profile))
        .bind(&prepared.name).bind(&prepared.email).bind(&prepared.plan_type)
        .bind(encoded_credentials).bind(i64::from(prepared.has_refresh_token))
        .bind(prepared.access_token_expires_at.map(|value| value.timestamp_micros()))
        .bind(prepared.next_refresh_at.map(|value| value.timestamp_micros()))
        .bind(i64::from(prepared.replacement_identity.is_some()))
        .bind(replacement_user).bind(replacement_account)
        .bind(i64::from(prepared.preserve_credential_state)).bind(now)
        .bind(&account_id).bind(prepared.provider_kind.as_str()).bind(expected)
        .fetch_optional(&mut **transaction).await
        .map_err(|_| unavailable("rotate provider credential"))?
        .ok_or_else(|| AdminStoreError::new(
            AdminStoreErrorKind::StaleRevision, "SQLite account", "provider credential revision changed",
        ))?;
        let stored_revision: i64 = row
            .try_get("credential_revision")
            .map_err(|_| unavailable("decode rotated credential revision"))?;
        let credential_revision =
            u64::try_from(stored_revision).map_err(|_| invalid("decode credential revision"))?;
        if let Some(settings) = &settings {
            self.update_account_settings(
                transaction,
                AccountSettingsUpdate {
                    account_ids: std::slice::from_ref(&account_id),
                    enabled: Some(settings.enabled),
                    concurrency_limit: Some(settings.concurrency_limit),
                    weight: Some(settings.weight),
                    model_access: settings.model_access.as_ref(),
                    group_ids: Some(&settings.group_ids),
                    outbound_proxy: settings.outbound_proxy.as_ref(),
                    notes: settings.notes.as_deref(),
                },
            )
            .await
            .map_err(|error| admin_store_error("SQLite account", error))?;
        }
        let mut fields = vec!["credentials".to_owned()];
        if let Some(settings) = &settings {
            fields.extend(["enabled", "concurrency_limit", "weight", "groups"].map(str::to_owned));
            if settings.model_access.is_some() {
                fields.push("model_access".to_owned());
            }
            if settings.outbound_proxy.is_some() {
                fields.push("outbound_proxy".to_owned());
            }
            if settings.notes.is_some() {
                fields.push("notes".to_owned());
            }
        }
        append_account_audit(
            transaction,
            mutation_audit(context, operation, &account_id, fields),
            revision,
        )
        .await
        .map_err(|error| admin_store_error("SQLite account", error))?;
        Ok(CredentialMutationResult {
            config_revision: admin_revision(revision)?,
            account_id: prepared.account_id,
            credential_revision: Some(
                Revision::new(credential_revision)
                    .map_err(|_| invalid("decode credential revision"))?,
            ),
        })
    }

    async fn import_accounts_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        command: CredentialImportTransaction<'_>,
    ) -> AdminStoreResult<CredentialImportResult> {
        let CredentialImportTransaction {
            prepared,
            settings,
            context,
            operation,
            outbound_proxy,
            revision,
        } = command;
        if prepared.credentials.is_empty() || prepared.credentials.len() > 200 {
            return Err(invalid(
                "credential import requires between 1 and 200 accounts",
            ));
        }
        if let Some(binding) = &outbound_proxy {
            let current = sqlx::query_scalar::<_, String>(
                "select proxy_url from outbound_proxies where id = ?1",
            )
            .bind(&binding.id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| unavailable("validate imported outbound proxy").with_source(error))?
            .ok_or_else(|| not_found("imported outbound proxy does not exist"))?;
            if current != binding.proxy.expose_url() {
                return Err(AdminStoreError::new(
                    AdminStoreErrorKind::StaleRevision,
                    "SQLite account",
                    "imported outbound proxy changed",
                ));
            }
        }
        let mut ids = std::collections::HashSet::new();
        let mut account_ids = Vec::with_capacity(prepared.credentials.len());
        let mut explicit_model_access = false;
        for credential in prepared.credentials {
            if credential.provider_kind != prepared.provider_kind
                || !ids.insert(credential.account_id.as_str().to_owned())
            {
                return Err(invalid(
                    "credential import Provider or account IDs are inconsistent",
                ));
            }
            explicit_model_access |= credential.model_access.is_some();
            let account = crate::postgres::prepared_account(credential)
                .map_err(|error| admin_store_error("SQLite account", error))?;
            account
                .validate()
                .map_err(|error| admin_store_error("SQLite account", error))?;
            let default_model_access = gateway_core::account::AccountModelAccess::all();
            let account_model_access = serde_json::to_string(
                account
                    .model_access
                    .as_ref()
                    .unwrap_or(&default_model_access),
            )
            .map_err(|_| invalid("encode account model access"))?;
            let model_access_explicit = account.model_access.is_some();
            let credential_json =
                serde_json::to_string(&account.provider_credentials_json.as_value())
                    .map_err(|_| invalid("encode provider credentials"))?;
            let outbound_proxy_url = account
                .outbound_proxy
                .as_ref()
                .map(|proxy| proxy.expose_url());
            let outbound_proxy_id = match outbound_proxy_url {
                Some(url) => sqlx::query_scalar::<_, String>(
                    "select id from outbound_proxies where proxy_url = ?1 order by id limit 1",
                )
                .bind(url)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|error| {
                    unavailable("resolve imported outbound proxy").with_source(error)
                })?,
                None => None,
            };
            let observed = account.credential_observed_at.timestamp_micros();
            let created = Utc::now().timestamp_micros();
            let imported = sqlx::query(
                "insert into provider_accounts (
                   id, provider_kind, name, email, upstream_user_id, upstream_account_id, plan_type,
                   authentication_kind, provider_credentials_json, credential_revision,
                   has_refresh_token, access_token_expires_at_us, next_refresh_at_us, enabled,
                   concurrency_limit, weight, model_access_json, credential_state,
                   credential_observed_at_us, quota_access_state, outbound_proxy_url,
                   outbound_proxy_id, created_at_us, updated_at_us
                 ) values (
                   ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, ?11, ?12, ?13,
                   ?14, ?15, ?16, ?17, ?18, 'unknown', ?19, ?20, ?21, max(?21, ?18)
                 )
                 on conflict(provider_kind, upstream_user_id, coalesce(upstream_account_id, ''))
                 do update set
                   model_access_json = case when ?22 = 1 then excluded.model_access_json else provider_accounts.model_access_json end,
                   name = excluded.name, email = excluded.email, plan_type = excluded.plan_type,
                   authentication_kind = excluded.authentication_kind,
                   provider_credentials_json = excluded.provider_credentials_json,
                   outbound_proxy_url = coalesce(excluded.outbound_proxy_url, provider_accounts.outbound_proxy_url),
                   outbound_proxy_id = coalesce(excluded.outbound_proxy_id, provider_accounts.outbound_proxy_id),
                   credential_revision = provider_accounts.credential_revision + 1,
                   has_refresh_token = excluded.has_refresh_token,
                   access_token_expires_at_us = excluded.access_token_expires_at_us,
                   next_refresh_at_us = excluded.next_refresh_at_us,
                   enabled = excluded.enabled, credential_state = excluded.credential_state,
                   provider_quota_json = null, quota_observed_at_us = null,
                   quota_access_state = 'unknown', quota_evidence = null,
                   quota_access_observed_at_us = null, quota_reset_at_us = null,
                   credential_observed_at_us = excluded.credential_observed_at_us,
                   last_error_reason = null, last_error_message = null,
                   updated_at_us = max(provider_accounts.updated_at_us, excluded.credential_observed_at_us, ?21)
                 returning id, credential_revision",
            )
            .bind(&account.id).bind(&account.provider_kind).bind(&account.name)
            .bind(&account.email).bind(&account.upstream_user_id).bind(&account.upstream_account_id)
            .bind(&account.plan_type).bind(&account.authentication_kind).bind(credential_json)
            .bind(i64::from(account.has_refresh_token))
            .bind(account.access_token_expires_at.map(|value| value.timestamp_micros()))
            .bind(account.next_refresh_at.map(|value| value.timestamp_micros()))
            .bind(i64::from(account.enabled))
            .bind(account.concurrency_limit.map(|limit| i64::from(limit.get())))
            .bind(i64::from(account.weight.get())).bind(account_model_access).bind(account.credential_state.as_str())
            .bind(observed).bind(outbound_proxy_url).bind(outbound_proxy_id).bind(created)
            .bind(i64::from(model_access_explicit))
            .fetch_one(&mut **transaction).await
            .map_err(|error| AdminStoreError::new(AdminStoreErrorKind::Conflict, "SQLite account", "credential import conflicts with an existing account").with_source(error))?;
            let id: String = imported
                .try_get("id")
                .map_err(|error| unavailable("decode imported account ID").with_source(error))?;
            account_ids.push(id);
        }
        let mut unique_ids = account_ids.clone();
        unique_ids.sort();
        unique_ids.dedup();
        if let Some(settings) = &settings {
            self.update_account_settings(
                transaction,
                AccountSettingsUpdate {
                    account_ids: &unique_ids,
                    enabled: Some(settings.enabled),
                    concurrency_limit: Some(settings.concurrency_limit),
                    weight: Some(settings.weight),
                    model_access: settings.model_access.as_ref(),
                    group_ids: Some(&settings.group_ids),
                    outbound_proxy: None,
                    notes: settings.notes.as_deref(),
                },
            )
            .await
            .map_err(|error| admin_store_error("SQLite account", error))?;
        }
        let mut fields = vec!["credentials".to_owned()];
        if explicit_model_access
            || settings
                .as_ref()
                .is_some_and(|settings| settings.model_access.is_some())
        {
            fields.push("model_access".to_owned());
        }
        if let Some(settings) = &settings {
            fields
                .extend(["enabled", "concurrency_limit", "weight", "group_ids"].map(str::to_owned));
            if settings.notes.is_some() {
                fields.push("notes".to_owned());
            }
        }
        append_account_audit(
            transaction,
            mutation_audit(context, operation, prepared.provider_kind.as_str(), fields),
            revision,
        )
        .await
        .map_err(|error| admin_store_error("SQLite account", error))?;
        let credential_ids = account_ids
            .into_iter()
            .map(|id| {
                ProviderAccountId::new(id).map_err(|error| {
                    unavailable("import returned an invalid account ID").with_source(error)
                })
            })
            .collect::<AdminStoreResult<Vec<_>>>()?;
        Ok(CredentialImportResult {
            config_revision: admin_revision(revision)?,
            credential_ids,
        })
    }

    async fn update_account_settings(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        update: AccountSettingsUpdate<'_>,
    ) -> StoreResult<()> {
        let AccountSettingsUpdate {
            account_ids,
            enabled,
            concurrency_limit,
            weight,
            model_access,
            group_ids,
            outbound_proxy,
            notes,
        } = update;
        validate_ids(account_ids, 1000, "account batch update")?;
        ensure_accounts_exist(transaction, account_ids).await?;

        let (proxy_id, proxy_url) = match outbound_proxy {
            Some(gateway_admin::model::proxies::AccountProxySelection::Direct) => (None, None),
            Some(gateway_admin::model::proxies::AccountProxySelection::Url(proxy)) => {
                (None, Some(proxy.expose_url().to_owned()))
            }
            Some(gateway_admin::model::proxies::AccountProxySelection::Saved(id)) => {
                let url = sqlx::query_scalar::<_, String>(
                    "select proxy_url from outbound_proxies where id = ?1",
                )
                .bind(id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(|error| {
                    sqlite_unavailable("load account outbound proxy").with_source(error)
                })?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "outbound proxy",
                    id: id.clone(),
                    source: None,
                })?;
                (Some(id.clone()), Some(url))
            }
            None => (None, None),
        };
        let model_access = model_access
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| invalid_store("encode account model access"))?;
        let now = Utc::now().timestamp_micros();
        let mut query =
            QueryBuilder::<Sqlite>::new("update provider_accounts set enabled = coalesce(");
        query.push_bind(enabled.map(bool_i64));
        query.push(", enabled), concurrency_limit = case when ");
        query.push_bind(i64::from(concurrency_limit.is_some()));
        query.push(" = 1 then ");
        query.push_bind(
            concurrency_limit
                .flatten()
                .map(|limit| i64::from(limit.get())),
        );
        query.push(" else concurrency_limit end, weight = coalesce(");
        query.push_bind(weight.map(|value| i64::from(value.get())));
        query.push(" , weight), model_access_json = coalesce(");
        query.push_bind(model_access);
        query.push(" , model_access_json), outbound_proxy_id = case when ");
        query.push_bind(i64::from(outbound_proxy.is_some()));
        query.push(" = 1 then ");
        query.push_bind(proxy_id);
        query.push(" else outbound_proxy_id end, outbound_proxy_url = case when ");
        query.push_bind(i64::from(outbound_proxy.is_some()));
        query.push(" = 1 then ");
        query.push_bind(proxy_url);
        query.push(" else outbound_proxy_url end, updated_at_us = max(updated_at_us, ");
        query.push_bind(now);
        query.push(") where id in (");
        {
            let mut separated = query.separated(", ");
            for account_id in account_ids {
                separated.push_bind(account_id);
            }
        }
        query.push(")");
        query
            .build()
            .execute(&mut **transaction)
            .await
            .map_err(|error| {
                sqlite_unavailable("update SQLite provider account settings").with_source(error)
            })?;

        if let Some(notes) = notes {
            let notes = (!notes.trim().is_empty()).then(|| notes.trim());
            let mut query = QueryBuilder::<Sqlite>::new("update provider_accounts set notes = ");
            query.push_bind(notes);
            query.push(", updated_at_us = max(updated_at_us, ");
            query.push_bind(now);
            query.push(") where id in (");
            {
                let mut separated = query.separated(", ");
                for account_id in account_ids {
                    separated.push_bind(account_id);
                }
            }
            query.push(")");
            query
                .build()
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    sqlite_unavailable("update SQLite provider account notes").with_source(error)
                })?;
        }

        if let Some(group_ids) = group_ids {
            if group_ids.len() > 1000 {
                return Err(invalid_store(
                    "account batch update contains too many group IDs",
                ));
            }
            let mut unique = std::collections::HashSet::new();
            if group_ids
                .iter()
                .any(|group_id| !unique.insert(group_id.as_str()))
            {
                return Err(invalid_store(
                    "account batch update contains duplicate group IDs",
                ));
            }
            if !group_ids.is_empty() {
                let known = count_group_ids(transaction, group_ids).await?;
                if usize::try_from(known).ok() != Some(group_ids.len()) {
                    return Err(StoreError::NotFound {
                        entity: "account group",
                        id: "one or more group IDs".to_owned(),
                        source: None,
                    });
                }
            }
            let mut delete = QueryBuilder::<Sqlite>::new(
                "delete from account_group_accounts where provider_account_id in (",
            );
            {
                let mut separated = delete.separated(", ");
                for account_id in account_ids {
                    separated.push_bind(account_id);
                }
            }
            delete.push(")");
            delete
                .build()
                .execute(&mut **transaction)
                .await
                .map_err(|error| {
                    sqlite_unavailable("clear account group assignments").with_source(error)
                })?;
            if !group_ids.is_empty() {
                let now = Utc::now().timestamp_micros();
                for group_id in group_ids {
                    for account_id in account_ids {
                        sqlx::query(
                            "insert into account_group_accounts
                             (account_group_id, provider_account_id, created_at_us)
                             values (?1, ?2, ?3)",
                        )
                        .bind(group_id.as_str())
                        .bind(account_id)
                        .bind(now)
                        .execute(&mut **transaction)
                        .await
                        .map_err(|error| {
                            sqlite_unavailable("assign account group").with_source(error)
                        })?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct StoredAuthorizationAccount {
    account_id: String,
    credential_revision: Option<u64>,
}

async fn load_authorization_receipt<'e>(
    executor: impl sqlx::Executor<'e, Database = Sqlite>,
    key: &AuthorizationReceiptKey,
) -> AdminStoreResult<Option<CredentialMutationResult>> {
    let row = sqlx::query(
        "select owner_digest, config_revision, accounts_json
           from authorization_receipts
          where provider_kind = ?1 and flow_digest = ?2 and expires_at_us > ?3",
    )
    .bind(key.provider_kind().as_str())
    .bind(key.flow_digest())
    .bind(Utc::now().timestamp_micros())
    .fetch_optional(executor)
    .await
    .map_err(|_| unavailable("load authorization receipt"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let owner: String = row
        .try_get("owner_digest")
        .map_err(|_| unavailable("decode authorization receipt"))?;
    if owner != key.owner_digest() {
        return Err(invalid("authorization receipt belongs to another owner"));
    }
    let revision: i64 = row
        .try_get("config_revision")
        .map_err(|_| unavailable("decode authorization receipt"))?;
    let accounts_json: String = row
        .try_get("accounts_json")
        .map_err(|_| unavailable("decode authorization receipt"))?;
    let accounts: Vec<StoredAuthorizationAccount> = serde_json::from_str(&accounts_json)
        .map_err(|_| unavailable("decode authorization receipt accounts"))?;
    let [account]: [StoredAuthorizationAccount; 1] = accounts
        .try_into()
        .map_err(|_| unavailable("authorization receipt does not contain exactly one account"))?;
    let revision = crate::Revision::new(
        u64::try_from(revision).map_err(|_| invalid("decode authorization receipt revision"))?,
    )
    .map_err(|_| invalid("decode authorization receipt revision"))?;
    Ok(Some(CredentialMutationResult {
        config_revision: admin_revision(revision)?,
        account_id: ProviderAccountId::new(account.account_id)
            .map_err(|_| unavailable("decode authorization receipt account ID"))?,
        credential_revision: account
            .credential_revision
            .map(|value| {
                let revision = crate::Revision::new(value)
                    .map_err(|_| unavailable("decode authorization credential revision"))?;
                admin_revision(revision)
            })
            .transpose()?,
    }))
}

async fn store_authorization_receipt(
    transaction: &mut Transaction<'_, Sqlite>,
    key: &AuthorizationReceiptKey,
    result: &CredentialMutationResult,
) -> AdminStoreResult<()> {
    let now = Utc::now().timestamp_micros();
    let expires = now
        .checked_add(24 * 60 * 60 * 1_000_000)
        .ok_or_else(|| invalid("authorization receipt expiry overflows"))?;
    let accounts = [StoredAuthorizationAccount {
        account_id: result.account_id.as_str().to_owned(),
        credential_revision: result.credential_revision.map(|revision| revision.get()),
    }];
    let accounts_json =
        serde_json::to_string(&accounts).map_err(|_| invalid("encode authorization receipt"))?;
    sqlx::query(
        "delete from authorization_receipts
          where provider_kind = ?1 and flow_digest = ?2 and expires_at_us <= ?3",
    )
    .bind(key.provider_kind().as_str())
    .bind(key.flow_digest())
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| unavailable("remove expired authorization receipt"))?;
    sqlx::query(
        "insert into authorization_receipts (
           provider_kind, flow_digest, owner_digest, config_revision, accounts_json,
           created_at_us, expires_at_us
         ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind(key.provider_kind().as_str())
    .bind(key.flow_digest())
    .bind(key.owner_digest())
    .bind(
        i64::try_from(result.config_revision.get())
            .map_err(|_| invalid("config revision exceeds SQLite INTEGER"))?,
    )
    .bind(accounts_json)
    .bind(now)
    .bind(expires)
    .execute(&mut **transaction)
    .await
    .map_err(|_| unavailable("store authorization receipt"))?;
    sqlx::query(
        "delete from authorization_receipts where rowid in (
           select rowid from authorization_receipts
            where expires_at_us <= ?1 order by expires_at_us limit 128
         )",
    )
    .bind(now)
    .execute(&mut **transaction)
    .await
    .map_err(|_| unavailable("prune authorization receipts"))?;
    Ok(())
}

async fn finish_account_admin_transaction<T>(
    transaction: Transaction<'_, Sqlite>,
    result: AdminStoreResult<T>,
    operation: &'static str,
) -> AdminStoreResult<T> {
    match result {
        Ok(value) => {
            transaction
                .commit()
                .await
                .map_err(|error| unavailable(operation).with_source(error))?;
            Ok(value)
        }
        Err(error) => {
            // 显式等待回滚，清理失败作为附属原因保留，不覆盖主失败
            match transaction.rollback().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.with_cleanup(cleanup)),
            }
        }
    }
}

fn not_found(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(AdminStoreErrorKind::NotFound, "SQLite account", message)
}

fn bool_i64(value: bool) -> i64 {
    i64::from(value)
}

fn invalid_store(message: &'static str) -> StoreError {
    StoreError::InvalidData {
        entity: "provider account",
        message: message.to_owned(),
        source: None,
    }
}

fn validate_ids(ids: &[String], max: usize, operation: &'static str) -> StoreResult<()> {
    if ids.is_empty() || ids.len() > max {
        return Err(invalid_store(operation));
    }
    let mut unique = std::collections::HashSet::new();
    if ids
        .iter()
        .any(|id| id.trim().is_empty() || !unique.insert(id.as_str()))
    {
        return Err(invalid_store("account IDs must be non-empty and unique"));
    }
    Ok(())
}

async fn ensure_accounts_exist(
    transaction: &mut Transaction<'_, Sqlite>,
    account_ids: &[String],
) -> StoreResult<()> {
    let mut query =
        QueryBuilder::<Sqlite>::new("select count(*) from provider_accounts where id in (");
    {
        let mut separated = query.separated(", ");
        for id in account_ids {
            separated.push_bind(id);
        }
    }
    query.push(")");
    let count: i64 = query
        .build_query_scalar()
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| {
            sqlite_unavailable("validate provider account selection").with_source(error)
        })?;
    if usize::try_from(count).ok() != Some(account_ids.len()) {
        return Err(StoreError::NotFound {
            entity: "provider account",
            id: "one or more account IDs".to_owned(),
            source: None,
        });
    }
    Ok(())
}

async fn count_group_ids(
    transaction: &mut Transaction<'_, Sqlite>,
    group_ids: &[gateway_core::routing::AccountGroupId],
) -> StoreResult<i64> {
    let mut query =
        QueryBuilder::<Sqlite>::new("select count(*) from account_groups where id in (");
    {
        let mut separated = query.separated(", ");
        for id in group_ids {
            separated.push_bind(id.as_str());
        }
    }
    query.push(")");
    query
        .build_query_scalar()
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| sqlite_unavailable("validate account group selection").with_source(error))
}

async fn bump_account_revision(
    transaction: &mut Transaction<'_, Sqlite>,
) -> StoreResult<crate::Revision> {
    acquire_write_lock(transaction).await?;
    bump_config_revision(transaction, Utc::now().timestamp_micros()).await
}

fn revision_i64(revision: crate::Revision) -> StoreResult<i64> {
    i64::try_from(revision.get())
        .map_err(|_| invalid_store("config revision exceeds SQLite INTEGER"))
}

async fn append_account_audit(
    transaction: &mut Transaction<'_, Sqlite>,
    mut audit: crate::AdminAuditEvent,
    revision: crate::Revision,
) -> StoreResult<()> {
    audit.config_revision = Some(revision_i64(revision)?);
    append_admin_audit_event_in_transaction(transaction, audit).await
}

async fn finish_account_transaction<T>(
    transaction: Transaction<'_, Sqlite>,
    result: StoreResult<T>,
    operation: &'static str,
) -> StoreResult<T> {
    match result {
        Ok(value) => {
            transaction
                .commit()
                .await
                .map_err(|error| sqlite_unavailable(operation).with_source(error))?;
            Ok(value)
        }
        Err(error) => {
            // 显式等待回滚，清理失败作为附属原因保留，不覆盖主失败
            match transaction.rollback().await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.with_cleanup(cleanup)),
            }
        }
    }
}

fn account_update_fields(command: &UpdateAccount) -> Vec<String> {
    let mut fields = vec![
        "enabled".to_owned(),
        "concurrency_limit".to_owned(),
        "weight".to_owned(),
        "groups".to_owned(),
    ];
    if command.model_access.is_some() {
        fields.push("model_access".to_owned());
    }
    if command.outbound_proxy.is_some() {
        fields.push("outbound_proxy".to_owned());
    }
    if command.notes.is_some() {
        fields.push("notes".to_owned());
    }
    fields
}

#[derive(Default)]
struct OptionalTokenTotal {
    value: u64,
    seen: bool,
}

impl OptionalTokenTotal {
    fn add(&mut self, value: Option<i64>) -> AdminStoreResult<()> {
        if let Some(value) = value {
            let value =
                u64::try_from(value).map_err(|_| invalid("negative persisted token count"))?;
            self.value = self
                .value
                .checked_add(value)
                .ok_or_else(|| invalid("token total overflows"))?;
            self.seen = true;
        }
        Ok(())
    }

    fn finish(self) -> Option<u64> {
        self.seen.then_some(self.value)
    }
}

#[derive(Default)]
struct AccountModelUsageAccumulator {
    request_count: u64,
    input_tokens: OptionalTokenTotal,
    output_tokens: OptionalTokenTotal,
    cached_tokens: OptionalTokenTotal,
    cache_write_tokens: OptionalTokenTotal,
    reasoning_tokens: OptionalTokenTotal,
    image_input_tokens: OptionalTokenTotal,
    image_output_tokens: OptionalTokenTotal,
    total_tokens: u64,
    image_request_count: u64,
    image_request_failed_count: u64,
    costs: HashMap<String, gateway_core::metering::Decimal>,
    coverage: CostCoverage,
    last_used_at: Option<DateTime<Utc>>,
}

#[derive(Default)]
struct AccountUsageAccumulator {
    request_count: u64,
    input_tokens: OptionalTokenTotal,
    output_tokens: OptionalTokenTotal,
    cached_tokens: OptionalTokenTotal,
    cache_write_tokens: OptionalTokenTotal,
    reasoning_tokens: OptionalTokenTotal,
    image_input_tokens: OptionalTokenTotal,
    image_output_tokens: OptionalTokenTotal,
    total_tokens: u64,
    image_request_count: u64,
    image_request_failed_count: u64,
    costs: HashMap<String, gateway_core::metering::Decimal>,
    coverage: CostCoverage,
    last_used_at: Option<DateTime<Utc>>,
    buckets: std::collections::BTreeMap<i64, u64>,
    models: HashMap<String, AccountModelUsageAccumulator>,
}

impl AccountUsageAccumulator {
    fn add_row(&mut self, row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<()> {
        let input: Option<i64> = row
            .try_get("input_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let output: Option<i64> = row
            .try_get("output_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let total: Option<i64> = row
            .try_get("total_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let cached: Option<i64> = row
            .try_get("cached_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let cache_write: Option<i64> = row
            .try_get("cache_write_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let reasoning: Option<i64> = row
            .try_get("reasoning_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let image_input: Option<i64> = row
            .try_get("image_input_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let image_output: Option<i64> = row
            .try_get("image_output_tokens")
            .map_err(|_| unavailable("decode account usage tokens"))?;
        let fallback_total = input
            .unwrap_or(0)
            .checked_add(output.unwrap_or(0))
            .ok_or_else(|| invalid("request token count overflows"))?;
        let token_total = u64::try_from(total.unwrap_or(fallback_total))
            .map_err(|_| invalid("negative persisted total token count"))?;
        let started_at_us: i64 = row
            .try_get("started_at_us")
            .map_err(|_| unavailable("decode account usage timestamp"))?;
        let started_at = decode_time(started_at_us)?;
        let model: Option<String> = row
            .try_get("model")
            .map_err(|_| unavailable("decode account usage model"))?;
        let image_succeeded: Option<i64> = row
            .try_get("image_generation_succeeded")
            .map_err(|_| unavailable("decode image usage outcome"))?;
        let cost_source: String = row
            .try_get("cost_source")
            .map_err(|_| unavailable("decode cost source"))?;
        let cost_currency: Option<String> = row
            .try_get("cost_currency")
            .map_err(|_| unavailable("decode cost currency"))?;
        let cost_text: Option<String> = row
            .try_get("cost_amount")
            .map_err(|_| unavailable("decode cost amount"))?;
        let cost = cost_text
            .map(|text| {
                super::value::decode_amount(&text)
                    .map_err(|error| admin_store_error("SQLite account usage", error))
            })
            .transpose()?;

        self.request_count = checked_inc(self.request_count, "account request count")?;
        self.input_tokens.add(input)?;
        self.output_tokens.add(output)?;
        self.cached_tokens.add(cached)?;
        self.cache_write_tokens.add(cache_write)?;
        self.reasoning_tokens.add(reasoning)?;
        self.image_input_tokens.add(image_input)?;
        self.image_output_tokens.add(image_output)?;
        self.total_tokens = self
            .total_tokens
            .checked_add(token_total)
            .ok_or_else(|| invalid("account token total overflows"))?;
        self.image_request_count = self
            .image_request_count
            .checked_add(u64::from(image_succeeded == Some(1)))
            .ok_or_else(|| invalid("image request count overflows"))?;
        self.image_request_failed_count = self
            .image_request_failed_count
            .checked_add(u64::from(image_succeeded == Some(0)))
            .ok_or_else(|| invalid("failed image request count overflows"))?;
        add_cost_source(&mut self.coverage, &cost_source)?;
        if let (Some(currency), Some(cost)) = (cost_currency.as_deref(), cost.as_ref()) {
            add_amount(&mut self.costs, currency, *cost)?;
        }
        self.last_used_at = Some(
            self.last_used_at
                .map_or(started_at, |value| value.max(started_at)),
        );
        let hour = started_at_us.div_euclid(3_600_000_000);
        *self.buckets.entry(hour).or_default() = checked_inc(
            self.buckets.get(&hour).copied().unwrap_or_default(),
            "account request bucket",
        )?;
        if let (Some(model), Some(currency), Some(cost)) =
            (model.clone(), cost_currency.clone(), cost)
        {
            let usage = self.models.entry(model).or_default();
            usage.request_count = checked_inc(usage.request_count, "account model request count")?;
            usage.input_tokens.add(input)?;
            usage.output_tokens.add(output)?;
            usage.cached_tokens.add(cached)?;
            usage.cache_write_tokens.add(cache_write)?;
            usage.reasoning_tokens.add(reasoning)?;
            usage.image_input_tokens.add(image_input)?;
            usage.image_output_tokens.add(image_output)?;
            usage.total_tokens = usage
                .total_tokens
                .checked_add(token_total)
                .ok_or_else(|| invalid("model token total overflows"))?;
            usage.image_request_count = usage
                .image_request_count
                .checked_add(u64::from(image_succeeded == Some(1)))
                .ok_or_else(|| invalid("image request count overflows"))?;
            usage.image_request_failed_count = usage
                .image_request_failed_count
                .checked_add(u64::from(image_succeeded == Some(0)))
                .ok_or_else(|| invalid("failed image request count overflows"))?;
            add_cost_source(&mut usage.coverage, &cost_source)?;
            usage.last_used_at = Some(
                usage
                    .last_used_at
                    .map_or(started_at, |value| value.max(started_at)),
            );
            add_amount(&mut usage.costs, &currency, cost)?;
        } else if let Some(model) = model {
            let usage = self.models.entry(model).or_default();
            usage.request_count = checked_inc(usage.request_count, "account model request count")?;
            usage.input_tokens.add(input)?;
            usage.output_tokens.add(output)?;
            usage.cached_tokens.add(cached)?;
            usage.cache_write_tokens.add(cache_write)?;
            usage.reasoning_tokens.add(reasoning)?;
            usage.image_input_tokens.add(image_input)?;
            usage.image_output_tokens.add(image_output)?;
            usage.total_tokens = usage
                .total_tokens
                .checked_add(token_total)
                .ok_or_else(|| invalid("model token total overflows"))?;
            usage.image_request_count = usage
                .image_request_count
                .checked_add(u64::from(image_succeeded == Some(1)))
                .ok_or_else(|| invalid("image request count overflows"))?;
            usage.image_request_failed_count = usage
                .image_request_failed_count
                .checked_add(u64::from(image_succeeded == Some(0)))
                .ok_or_else(|| invalid("failed image request count overflows"))?;
            add_cost_source(&mut usage.coverage, &cost_source)?;
            usage.last_used_at = Some(
                usage
                    .last_used_at
                    .map_or(started_at, |value| value.max(started_at)),
            );
        }
        Ok(())
    }

    fn finish(self, account_id: &str) -> AdminStoreResult<AccountUsage> {
        let mut models = self
            .models
            .into_iter()
            .map(|(model, usage)| {
                Ok(AccountModelUsage {
                    model,
                    request_count: usage.request_count,
                    success_count: usage.request_count,
                    input_tokens: usage.input_tokens.finish(),
                    output_tokens: usage.output_tokens.finish(),
                    cached_tokens: usage.cached_tokens.finish(),
                    cache_write_tokens: usage.cache_write_tokens.finish(),
                    reasoning_tokens: usage.reasoning_tokens.finish(),
                    image_input_tokens: usage.image_input_tokens.finish(),
                    image_output_tokens: usage.image_output_tokens.finish(),
                    image_request_count: usage.image_request_count,
                    image_request_failed_count: usage.image_request_failed_count,
                    total_tokens: Some(usage.total_tokens),
                    cost_coverage: usage.coverage,
                    costs: finish_costs(usage.costs)?,
                    last_used_at: usage
                        .last_used_at
                        .ok_or_else(|| invalid("model usage timestamp is missing"))?,
                })
            })
            .collect::<AdminStoreResult<Vec<_>>>()?;
        models.sort_by(|left, right| {
            right
                .request_count
                .cmp(&left.request_count)
                .then_with(|| left.model.cmp(&right.model))
        });
        let request_buckets = self
            .buckets
            .into_iter()
            .map(|(hour, request_count)| {
                let seconds = hour
                    .checked_mul(3600)
                    .ok_or_else(|| invalid("usage bucket timestamp overflows"))?;
                let bucket_start = DateTime::from_timestamp(seconds, 0)
                    .ok_or_else(|| invalid("usage bucket timestamp is invalid"))?;
                Ok(AccountRequestBucket {
                    bucket_start,
                    request_count,
                })
            })
            .collect::<AdminStoreResult<Vec<_>>>()?;
        Ok(AccountUsage {
            account_id: account_id.to_owned(),
            request_count: self.request_count,
            success_count: self.request_count,
            input_tokens: self.input_tokens.finish(),
            output_tokens: self.output_tokens.finish(),
            cached_tokens: self.cached_tokens.finish(),
            cache_write_tokens: self.cache_write_tokens.finish(),
            reasoning_tokens: self.reasoning_tokens.finish(),
            image_input_tokens: self.image_input_tokens.finish(),
            image_output_tokens: self.image_output_tokens.finish(),
            image_request_count: self.image_request_count,
            image_request_failed_count: self.image_request_failed_count,
            total_tokens: Some(self.total_tokens),
            cost_coverage: self.coverage,
            costs: finish_costs(self.costs)?,
            last_used_at: self.last_used_at,
            request_buckets,
            models,
        })
    }
}

fn add_cost_source(coverage: &mut CostCoverage, source: &str) -> AdminStoreResult<()> {
    match source {
        "provider_reported" => {
            coverage.provider_reported_count =
                checked_inc(coverage.provider_reported_count, "provider cost coverage")?
        }
        "calculated" => {
            coverage.calculated_count =
                checked_inc(coverage.calculated_count, "calculated cost coverage")?
        }
        "unavailable" => {
            coverage.unavailable_count =
                checked_inc(coverage.unavailable_count, "unavailable cost coverage")?
        }
        _ => return Err(invalid("unknown cost source")),
    }
    Ok(())
}

fn checked_inc(value: u64, entity: &'static str) -> AdminStoreResult<u64> {
    value.checked_add(1).ok_or_else(|| invalid(entity))
}

fn add_amount(
    amounts: &mut HashMap<String, gateway_core::metering::Decimal>,
    currency: &str,
    amount: gateway_core::metering::Decimal,
) -> AdminStoreResult<()> {
    let current = amounts
        .get(currency)
        .copied()
        .unwrap_or(gateway_core::metering::Decimal::ZERO);
    let total = current
        .checked_add(amount)
        .ok_or_else(|| invalid("account cost total overflows"))?;
    amounts.insert(currency.to_owned(), total);
    Ok(())
}

fn finish_costs(
    amounts: HashMap<String, gateway_core::metering::Decimal>,
) -> AdminStoreResult<Vec<AccountCost>> {
    let mut costs = amounts
        .into_iter()
        .map(|(currency, amount)| {
            let amount = DecimalAmount::from_str(&amount.canonical())
                .map_err(|_| invalid("account cost amount is invalid"))?;
            Ok(AccountCost { currency, amount })
        })
        .collect::<AdminStoreResult<Vec<_>>>()?;
    costs.sort_by(|left, right| left.currency.cmp(&right.currency));
    Ok(costs)
}

async fn load_quota_forecast_history(
    pool: &SqlitePool,
    window: &AccountUsageWindowQuery,
) -> AdminStoreResult<QuotaForecastHistory> {
    if window.account_id.trim().is_empty()
        || window.key.trim().is_empty()
        || window.range.start >= window.range.end
        || window.range.end - window.range.start > chrono::TimeDelta::days(32)
    {
        return Err(invalid(
            "quota forecast window is invalid or exceeds 32 days",
        ));
    }
    let start_us = window.range.start.timestamp_micros();
    let end_us = window.range.end.timestamp_micros();
    let rows = sqlx::query(
        "select id, started_at_us, completed_at_us, outcome, provider_observation_json,
                input_tokens, output_tokens, cached_tokens, total_tokens, cost_source,
                cost_amount, cost_currency, provider_kind, request_kind, downstream_committed_at_us,
                client_transport, client_status_code, requested_model_id, upstream_model_id,
                image_generation_requested, recovered_at_us, cache_write_tokens, reasoning_tokens,
                image_input_tokens, image_output_tokens
           from model_requests
          where provider_account_ref = ?1 and started_at_us >= ?2 and started_at_us < ?3
          order by completed_at_us, id",
    )
    .bind(&window.account_id)
    .bind(start_us)
    .bind(end_us)
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable("load quota forecast history"))?;

    let mut by_completed_at =
        std::collections::BTreeMap::<i64, Vec<sqlx::sqlite::SqliteRow>>::new();
    let mut pending_request_count = 0_u64;
    for row in rows {
        let completed: Option<i64> = row
            .try_get("completed_at_us")
            .map_err(|_| unavailable("decode quota forecast completion time"))?;
        let outcome: String = row
            .try_get("outcome")
            .map_err(|_| unavailable("decode quota forecast outcome"))?;
        if outcome == "running" || completed.is_none_or(|value| value > end_us) {
            pending_request_count =
                checked_inc(pending_request_count, "pending forecast request count")?;
        } else if let Some(completed) = completed {
            by_completed_at.entry(completed).or_default().push(row);
        }
    }

    let mut history = QuotaForecastHistory::default();
    let duration = end_us
        .checked_sub(start_us)
        .ok_or_else(|| invalid("quota forecast range overflows"))?;
    let mut cumulative = QuotaForecastUsage::default();
    let mut selected = std::collections::BTreeMap::<
        usize,
        gateway_admin::model::quota_forecast_sampling::QuotaForecastHistoryPoint,
    >::new();
    for (completed_us, group) in by_completed_at {
        let mut latest_document = None;
        let mut group_start = None;
        for row in &group {
            let included = sqlite_completed_usage_fact(row)?;
            if included {
                cumulative.request_count =
                    checked_inc(cumulative.request_count, "forecast request count")?;
                let input = row
                    .try_get::<Option<i64>, _>("input_tokens")
                    .map_err(|_| unavailable("decode quota forecast input tokens"))?;
                let output = row
                    .try_get::<Option<i64>, _>("output_tokens")
                    .map_err(|_| unavailable("decode quota forecast output tokens"))?;
                let total = row
                    .try_get::<Option<i64>, _>("total_tokens")
                    .map_err(|_| unavailable("decode quota forecast total tokens"))?;
                let tokens = total.unwrap_or(
                    input
                        .unwrap_or(0)
                        .checked_add(output.unwrap_or(0))
                        .ok_or_else(|| invalid("quota forecast token count overflows"))?,
                );
                cumulative.tokens = cumulative
                    .tokens
                    .checked_add(
                        u64::try_from(tokens)
                            .map_err(|_| invalid("negative quota forecast token count"))?,
                    )
                    .ok_or_else(|| invalid("quota forecast token total overflows"))?;
                cumulative.input_tokens = cumulative
                    .input_tokens
                    .checked_add(input.unwrap_or(0) as u64)
                    .ok_or_else(|| invalid("quota forecast input total overflows"))?;
                cumulative.output_tokens = cumulative
                    .output_tokens
                    .checked_add(output.unwrap_or(0) as u64)
                    .ok_or_else(|| invalid("quota forecast output total overflows"))?;
                let cached = row
                    .try_get::<Option<i64>, _>("cached_tokens")
                    .map_err(|_| unavailable("decode quota forecast cached tokens"))?
                    .unwrap_or(0);
                cumulative.cached_tokens = cumulative
                    .cached_tokens
                    .checked_add(
                        u64::try_from(cached)
                            .map_err(|_| invalid("negative quota forecast cached tokens"))?,
                    )
                    .ok_or_else(|| invalid("quota forecast cached token total overflows"))?;
                if total.is_none() && (input.is_none() || output.is_none()) {
                    cumulative.missing_token_count = checked_inc(
                        cumulative.missing_token_count,
                        "missing forecast token count",
                    )?;
                }
                let amount: Option<String> = row
                    .try_get("cost_amount")
                    .map_err(|_| unavailable("decode quota forecast cost"))?;
                let currency: Option<String> = row
                    .try_get("cost_currency")
                    .map_err(|_| unavailable("decode quota forecast currency"))?;
                let source: String = row
                    .try_get("cost_source")
                    .map_err(|_| unavailable("decode quota forecast cost source"))?;
                if matches!(source.as_str(), "calculated" | "provider_reported")
                    && currency.as_deref() == Some("USD")
                    && amount.is_some()
                {
                    cumulative.known_cost_count =
                        checked_inc(cumulative.known_cost_count, "known forecast cost count")?;
                    let decimal =
                        super::value::decode_amount(amount.as_deref().unwrap_or_default())
                            .map_err(|error| admin_store_error("SQLite account forecast", error))?;
                    let usd: f64 = decimal
                        .canonical()
                        .parse()
                        .map_err(|_| invalid("quota forecast USD amount is invalid"))?;
                    cumulative.usd += usd;
                    if !cumulative.usd.is_finite() {
                        return Err(invalid("quota forecast USD total overflows"));
                    }
                } else {
                    cumulative.unavailable_cost_count = checked_inc(
                        cumulative.unavailable_cost_count,
                        "unavailable forecast cost count",
                    )?;
                }
            } else {
                cumulative.excluded_request_count = checked_inc(
                    cumulative.excluded_request_count,
                    "excluded forecast request count",
                )?;
            }
            if let Some(value) = row
                .try_get::<Option<i64>, _>("started_at_us")
                .map_err(|_| unavailable("decode quota forecast start time"))?
            {
                group_start = Some(value);
            }
            let document: Option<String> = row
                .try_get("provider_observation_json")
                .map_err(|_| unavailable("decode quota forecast provider observation"))?;
            if let Some(document) = document {
                let value: serde_json::Value = serde_json::from_str(&document)
                    .map_err(|_| unavailable("decode quota forecast provider observation"))?;
                let object = value.as_object().cloned().ok_or_else(|| {
                    unavailable("quota forecast Provider observation is not an object")
                })?;
                latest_document = Some((document_id(row)?, object));
            }
        }
        if let Some((_, document)) = latest_document {
            let offset = completed_us.saturating_sub(start_us).max(0);
            let bucket = usize::try_from((offset as i128 * 128 / duration as i128).min(127))
                .map_err(|_| invalid("quota forecast sample bucket is invalid"))?;
            let started_at = decode_time(group_start.unwrap_or(completed_us))?;
            let completed_at = decode_time(completed_us)?;
            selected.insert(
                bucket,
                gateway_admin::model::quota_forecast_sampling::QuotaForecastHistoryPoint {
                    started_at,
                    completed_at,
                    usage: cumulative.clone(),
                    provider_observation: ProviderDocument::new(OpaqueProviderData::new(document)),
                },
            );
        }
    }
    history.points = selected.into_values().collect();
    history.usage = cumulative;
    history.pending_request_count = pending_request_count;
    Ok(history)
}

fn document_id(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<String> {
    row.try_get("id")
        .map_err(|_| unavailable("decode quota forecast request ID"))
}

fn sqlite_completed_usage_fact(row: &sqlx::sqlite::SqliteRow) -> AdminStoreResult<bool> {
    let outcome: String = row
        .try_get("outcome")
        .map_err(|_| unavailable("decode usage outcome"))?;
    let committed: Option<i64> = row
        .try_get("downstream_committed_at_us")
        .map_err(|_| unavailable("decode usage commit"))?;
    let provider: Option<String> = row
        .try_get("provider_kind")
        .map_err(|_| unavailable("decode usage Provider"))?;
    let request_kind: Option<String> = row
        .try_get("request_kind")
        .map_err(|_| unavailable("decode usage kind"))?;
    let transport: String = row
        .try_get("client_transport")
        .map_err(|_| unavailable("decode usage transport"))?;
    let client_status: Option<i64> = row
        .try_get("client_status_code")
        .map_err(|_| unavailable("decode usage status"))?;
    let requested: Option<String> = row
        .try_get("requested_model_id")
        .map_err(|_| unavailable("decode usage model"))?;
    let upstream: Option<String> = row
        .try_get("upstream_model_id")
        .map_err(|_| unavailable("decode usage model"))?;
    let image: i64 = row
        .try_get("image_generation_requested")
        .map_err(|_| unavailable("decode image usage intent"))?;
    let input: Option<i64> = row
        .try_get("input_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let output: Option<i64> = row
        .try_get("output_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let cached: Option<i64> = row
        .try_get("cached_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let cache_write: Option<i64> = row
        .try_get("cache_write_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let reasoning: Option<i64> = row
        .try_get("reasoning_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let image_input: Option<i64> = row
        .try_get("image_input_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let image_output: Option<i64> = row
        .try_get("image_output_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let total: Option<i64> = row
        .try_get("total_tokens")
        .map_err(|_| unavailable("decode usage tokens"))?;
    let cost: Option<String> = row
        .try_get("cost_amount")
        .map_err(|_| unavailable("decode usage cost"))?;
    let recovered: Option<i64> = row
        .try_get("recovered_at_us")
        .map_err(|_| unavailable("decode recovery state"))?;
    Ok(outcome == "succeeded"
        && committed.is_some()
        && recovered.is_none()
        && !(provider.as_deref() == Some("openai") && request_kind.as_deref() == Some("prewarm"))
        && ((transport == "websocket" && client_status.is_none())
            || client_status.is_some_and(|status| (200..=399).contains(&status)))
        && (requested.is_some()
            || upstream.is_some()
            || image != 0
            || input.is_some()
            || output.is_some()
            || cached.is_some()
            || cache_write.is_some()
            || reasoning.is_some()
            || image_input.is_some()
            || image_output.is_some()
            || total.is_some()
            || cost.is_some()))
}

async fn account_usage_in_range(
    pool: &SqlitePool,
    account_id: &str,
    range: TimeRange,
) -> AdminStoreResult<AccountUsage> {
    if range.start >= range.end {
        return Err(invalid("usage range must be positive"));
    }
    let rows = sqlx::query(
        "select coalesce(upstream_model_id, requested_model_id) as model,
                input_tokens, output_tokens, cached_tokens, cache_write_tokens, reasoning_tokens,
                image_input_tokens, image_output_tokens, image_generation_succeeded, total_tokens,
                cost_source, cost_amount, cost_currency, started_at_us
           from model_requests mr
          where provider_account_ref = ?1
            and started_at_us >= ?2 and started_at_us < ?3
            and outcome = 'succeeded' and downstream_committed_at_us is not null
            and (provider_kind is not 'openai' or request_kind is not 'prewarm')
            and ((client_transport = 'websocket' and client_status_code is null)
                 or client_status_code between 200 and 399)
            and recovered_at_us is null
            and (requested_model_id is not null or upstream_model_id is not null
                 or image_generation_requested = 1 or input_tokens is not null
                 or output_tokens is not null or cached_tokens is not null
                 or cache_write_tokens is not null or reasoning_tokens is not null
                 or image_input_tokens is not null or image_output_tokens is not null
                 or total_tokens is not null or cost_amount is not null)
          order by started_at_us, id",
    )
    .bind(account_id)
    .bind(range.start.timestamp_micros())
    .bind(range.end.timestamp_micros())
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable("load account usage"))?;
    let mut usage = AccountUsageAccumulator::default();
    for row in &rows {
        usage.add_row(row)?;
    }
    usage.finish(account_id)
}

fn account_record(
    account: ProviderAccount,
    metadata: AccountMetadata,
    groups: Vec<AccountGroupRef>,
) -> AdminStoreResult<AccountRecord> {
    let credential_revision = Revision::new(account.revision().get())
        .map_err(|_| invalid("decode account credential revision"))?;
    Ok(AccountRecord {
        id: account.id().as_str().to_owned(),
        provider_kind: account.provider().clone(),
        groups,
        name: account.name().to_owned(),
        notes: metadata.notes,
        email: account.email().map(ToOwned::to_owned),
        upstream_user_id: account.upstream_user_id().map(ToOwned::to_owned),
        upstream_account_id: account.upstream_account_id().map(ToOwned::to_owned),
        plan_type: account.plan_type().map(ToOwned::to_owned),
        authentication_kind: account.authentication_kind().to_owned(),
        credential_revision,
        has_refresh_token: account.has_refresh_token(),
        access_token_expires_at: account.access_token_expires_at().map(DateTime::<Utc>::from),
        next_refresh_at: account.next_refresh_at().map(DateTime::<Utc>::from),
        enabled: account.enabled(),
        concurrency_limit: account.concurrency_limit(),
        weight: account.weight(),
        model_access: account.model_access().clone(),
        outbound_proxy: account.outbound_proxy().cloned(),
        credential_state: account.credential_state(),
        credential_observed_at: metadata.credential_observed_at,
        quota: account.quota(),
        last_error_reason: account.last_error_reason(),
        last_error_message: account.last_error_message().map(ToOwned::to_owned),
        created_at: metadata.created_at,
        updated_at: metadata.updated_at,
    })
}

fn decode_time(micros: i64) -> AdminStoreResult<DateTime<Utc>> {
    datetime_from_micros(micros).map_err(|_| invalid("decode account timestamp"))
}

fn unavailable(operation: &'static str) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Unavailable,
        "SQLite account",
        format!("SQLite account operation failed: {operation}"),
    )
}

fn invalid(operation: &'static str) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Invalid,
        "SQLite account",
        format!("invalid persisted account data: {operation}"),
    )
}

fn core_error(operation: &'static str, error: gateway_core::error::StoreError) -> AdminStoreError {
    use gateway_core::error::StoreErrorKind as CoreKind;
    let kind = match error.kind() {
        CoreKind::Conflict => AdminStoreErrorKind::Conflict,
        CoreKind::InvalidData | CoreKind::InvalidState => AdminStoreErrorKind::Invalid,
        CoreKind::Unavailable => AdminStoreErrorKind::Unavailable,
        _ => AdminStoreErrorKind::Unavailable,
    };
    AdminStoreError::new(
        kind,
        "SQLite account",
        format!("SQLite operation failed: {operation}"),
    )
}

#[async_trait]
impl AccountStore for SqliteAdminAccountStore {
    async fn list_plugin_accounts(
        &self,
        query: PluginAccountListQuery,
    ) -> AdminStoreResult<PluginAccountPage> {
        let accounts = self
            .accounts
            .list_accounts()
            .await
            .map_err(|error| core_error("list plugin accounts", error))?;
        let mut accounts = accounts
            .into_iter()
            .filter(|account| {
                query
                    .provider_kind
                    .as_ref()
                    .is_none_or(|kind| kind == account.provider())
                    && query
                        .cursor
                        .as_ref()
                        .is_none_or(|cursor| account.id() > cursor)
            })
            .collect::<Vec<_>>();
        accounts.sort_by(|left, right| left.id().cmp(right.id()));
        let limit = usize::from(query.limit.get());
        let has_more = accounts.len() > limit;
        accounts.truncate(limit);
        let next_cursor = has_more
            .then(|| accounts.last().map(|account| account.id().clone()))
            .flatten();
        let records = futures::future::try_join_all(
            accounts
                .into_iter()
                .map(|account| self.account_record(account)),
        )
        .await?;
        Ok(PluginAccountPage {
            accounts: records,
            next_cursor,
        })
    }

    async fn list_accounts(
        &self,
        query: AccountListQuery,
        runtime: AccountRuntimeSnapshot,
    ) -> AdminStoreResult<AccountPage> {
        if query.page == 0 {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "SQLite account",
                "page number must be positive",
            ));
        }
        let now = Utc::now();
        let accounts = self
            .accounts
            .list_accounts()
            .await
            .map_err(|error| core_error("list accounts", error))?;
        let ids = accounts
            .iter()
            .map(|account| account.id().as_str().to_owned())
            .collect::<Vec<_>>();
        let mut metadata = self.metadata_for(&ids).await?;
        let mut groups = self.groups_for(&ids).await?;
        let settings = sqlx::query_as::<_, (i64, i64)>(
            "select config_revision, max_concurrent_per_account from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| unavailable("load account page settings"))?
        .ok_or_else(|| {
            AdminStoreError::new(
                AdminStoreErrorKind::NotFound,
                "runtime settings",
                "runtime settings not found",
            )
        })?;
        let config_revision = Revision::new(
            u64::try_from(settings.0).map_err(|_| invalid("decode config revision"))?,
        )
        .map_err(|_| invalid("decode config revision"))?;
        let default_concurrency =
            u64::try_from(settings.1).map_err(|_| invalid("decode default concurrency"))?;
        let usage_sort_values = if query.sort.is_some_and(|sort| {
            matches!(
                sort.field,
                AccountSortField::Usage | AccountSortField::LastUsedAt
            )
        }) {
            self.usage_sort_values(now).await?
        } else {
            HashMap::new()
        };
        let mut items = Vec::with_capacity(accounts.len());
        let mut summary = AccountSummary {
            total: 0,
            normal: 0,
            quota_exhausted: 0,
            rate_limited: 0,
            disabled: 0,
            error: 0,
        };
        for account in accounts {
            let id = account.id().as_str().to_owned();
            let projection = account
                .status_projection(SystemTime::from(now), runtime.cooldown.get(&id).copied());
            summary.total = summary.total.saturating_add(1);
            match projection.status {
                AccountStatus::Normal => summary.normal = summary.normal.saturating_add(1),
                AccountStatus::QuotaExhausted => {
                    summary.quota_exhausted = summary.quota_exhausted.saturating_add(1)
                }
                AccountStatus::RateLimited => {
                    summary.rate_limited = summary.rate_limited.saturating_add(1)
                }
                AccountStatus::Disabled => summary.disabled = summary.disabled.saturating_add(1),
                AccountStatus::Error => summary.error = summary.error.saturating_add(1),
            }
            if query
                .provider_kind
                .as_ref()
                .is_some_and(|kind| kind != account.provider())
                || query
                    .status
                    .is_some_and(|status| status != projection.status)
                || query.search.as_ref().is_some_and(|needle| {
                    let needle = needle.to_lowercase();
                    ![
                        account.id().as_str(),
                        account.name(),
                        account.email().unwrap_or_default(),
                        account.upstream_user_id().unwrap_or_default(),
                        account.upstream_account_id().unwrap_or_default(),
                        account.plan_type().unwrap_or_default(),
                    ]
                    .iter()
                    .any(|value| value.to_lowercase().contains(&needle))
                })
            {
                continue;
            }
            let account_groups = groups.remove(&id).unwrap_or_default();
            match query.group_filter.as_ref() {
                Some(AccountGroupFilter::Group(group_id))
                    if !account_groups.iter().any(|group| &group.id == group_id) =>
                {
                    continue;
                }
                Some(AccountGroupFilter::Ungrouped) if !account_groups.is_empty() => continue,
                _ => {}
            }
            let metadata = metadata
                .remove(&id)
                .ok_or_else(|| invalid("account metadata is missing"))?;
            let account = account_record(account, metadata, account_groups)?;
            let total_slots = account
                .concurrency_limit
                .map(|limit| u64::from(limit.get()))
                .or_else(|| (default_concurrency > 0).then_some(default_concurrency));
            let used_slots = runtime
                .in_flight
                .as_ref()
                .map(|counts| counts.get(&id).copied().unwrap_or(0));
            items.push(AccountPageItem {
                account,
                projection,
                capacity: AccountCapacity {
                    used_slots,
                    total_slots,
                },
            });
        }
        if let Some(AccountGroupFilter::Group(group_id)) = &query.group_filter {
            let exists = sqlx::query_scalar::<_, i64>(
                "select exists(select 1 from account_groups where id = ?1)",
            )
            .bind(group_id.as_str())
            .fetch_one(&self.pool)
            .await
            .map_err(|_| unavailable("validate account group filter"))?;
            if exists == 0 {
                return Err(AdminStoreError::new(
                    AdminStoreErrorKind::NotFound,
                    "account group",
                    "account group not found",
                ));
            }
        }
        let total = u64::try_from(items.len()).unwrap_or(u64::MAX);
        if let Some(sort) = query.sort {
            items.sort_by(|left, right| {
                let ordering = match sort.field {
                    AccountSortField::Email => left.account.email.cmp(&right.account.email),
                    AccountSortField::Status => account_status_sort_rank(left.projection.status)
                        .cmp(&account_status_sort_rank(right.projection.status)),
                    AccountSortField::PlanType => {
                        left.account.plan_type.cmp(&right.account.plan_type)
                    }
                    AccountSortField::Usage => usage_sort_values
                        .get(&left.account.id)
                        .map(|values| values.0)
                        .unwrap_or(0)
                        .cmp(
                            &usage_sort_values
                                .get(&right.account.id)
                                .map(|values| values.0)
                                .unwrap_or(0),
                        ),
                    AccountSortField::LastUsedAt => usage_sort_values
                        .get(&left.account.id)
                        .and_then(|values| values.1)
                        .cmp(
                            &usage_sort_values
                                .get(&right.account.id)
                                .and_then(|values| values.1),
                        ),
                    AccountSortField::ExpiresAt => left
                        .account
                        .access_token_expires_at
                        .cmp(&right.account.access_token_expires_at),
                };
                let ordering = if sort.direction == SortDirection::Desc {
                    ordering.reverse()
                } else {
                    ordering
                };
                ordering.then_with(|| {
                    if sort.direction == SortDirection::Desc {
                        right.account.id.cmp(&left.account.id)
                    } else {
                        left.account.id.cmp(&right.account.id)
                    }
                })
            });
        } else {
            items.sort_by(|left, right| left.account.id.cmp(&right.account.id));
        }
        let page_size = usize::from(query.page_size.get());
        let start = usize::try_from(query.page - 1)
            .unwrap_or(usize::MAX)
            .saturating_mul(page_size);
        let items = items.into_iter().skip(start).take(page_size).collect();
        Ok(AccountPage {
            config_revision,
            items,
            total,
            summary,
        })
    }

    async fn load_account(
        &self,
        account_id: &str,
        runtime: AccountRuntimeSnapshot,
    ) -> AdminStoreResult<Option<AccountPageItem>> {
        let account_id = ProviderAccountId::new(account_id.to_owned()).map_err(|_| {
            AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "SQLite account",
                "invalid account ID",
            )
        })?;
        let account = self
            .accounts
            .get_account(&account_id)
            .await
            .map_err(|error| core_error("load account", error))?;
        let Some(account) = account else {
            return Ok(None);
        };
        let default_concurrency = sqlx::query_scalar::<_, i64>(
            "select max_concurrent_per_account from runtime_settings where id = 1",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|_| unavailable("load default account concurrency"))?;
        let default_concurrency = u64::try_from(default_concurrency)
            .map_err(|_| invalid("decode default account concurrency"))?;
        self.page_item(account, &runtime, default_concurrency, Utc::now())
            .await
            .map(Some)
    }

    async fn load_account_usage(
        &self,
        range: TimeRange,
        account_ids: &[String],
    ) -> AdminStoreResult<Vec<AccountUsage>> {
        if account_ids.len() > 200 || account_ids.iter().any(|id| id.trim().is_empty()) {
            return Err(invalid("account usage query contains invalid account IDs"));
        }
        let mut unique = std::collections::HashSet::new();
        if account_ids.iter().any(|id| !unique.insert(id.as_str())) {
            return Err(invalid(
                "account usage query contains duplicate account IDs",
            ));
        }
        futures::future::try_join_all(
            account_ids
                .iter()
                .map(|id| account_usage_in_range(&self.pool, id, range)),
        )
        .await
    }
    async fn load_account_usage_by_windows(
        &self,
        windows: &[AccountUsageWindowQuery],
    ) -> AdminStoreResult<Vec<AccountUsageWindowResult>> {
        let mut results = Vec::with_capacity(windows.len());
        for window in windows {
            if window.account_id.trim().is_empty() || window.key.trim().is_empty() {
                return Err(invalid(
                    "account usage window key and account ID must be non-empty",
                ));
            }
            let usage =
                account_usage_in_range(&self.pool, &window.account_id, window.range).await?;
            results.push(AccountUsageWindowResult {
                account_id: window.account_id.clone(),
                key: window.key.clone(),
                usage,
            });
        }
        Ok(results)
    }
    async fn load_quota_forecast_history(
        &self,
        window: &AccountUsageWindowQuery,
    ) -> AdminStoreResult<QuotaForecastHistory> {
        load_quota_forecast_history(&self.pool, window).await
    }

    async fn credential_details(
        &self,
        provider_kind: &ProviderKind,
        account_id: &ProviderAccountId,
    ) -> AdminStoreResult<Option<CredentialDetails>> {
        let loaded = self.loaded_credential(account_id).await?;
        let Some(loaded) = loaded.filter(|loaded| loaded.account.provider() == provider_kind)
        else {
            return Ok(None);
        };
        Ok(Some(CredentialDetails {
            config_revision: self.config_revision().await?,
            credential: self.account_record(loaded.account).await?,
        }))
    }

    async fn credential_details_by_id(
        &self,
        account_id: &ProviderAccountId,
    ) -> AdminStoreResult<Option<CredentialDetails>> {
        let Some(loaded) = self.loaded_credential(account_id).await? else {
            return Ok(None);
        };
        Ok(Some(CredentialDetails {
            config_revision: self.config_revision().await?,
            credential: self.account_record(loaded.account).await?,
        }))
    }

    async fn load_credentials_for_export(
        &self,
        provider_kind: &ProviderKind,
        account_ids: &[ProviderAccountId],
    ) -> AdminStoreResult<Vec<ProviderExportCredentialInput>> {
        let mut result = Vec::with_capacity(account_ids.len());
        for account_id in account_ids {
            let loaded = self.loaded_credential(account_id).await?.ok_or_else(|| {
                AdminStoreError::new(
                    AdminStoreErrorKind::NotFound,
                    "SQLite account",
                    "one or more exported credentials do not exist",
                )
            })?;
            if loaded.account.provider() != provider_kind {
                return Err(AdminStoreError::new(
                    AdminStoreErrorKind::NotFound,
                    "SQLite account",
                    "one or more exported credentials belong to another Provider",
                ));
            }
            result.push(ProviderExportCredentialInput {
                account: self.account_record(loaded.account).await?,
                provider_material: ProviderDocument::new(OpaqueProviderData::new(
                    loaded.credential.into_inner(),
                )),
            });
        }
        Ok(result)
    }

    async fn load_credential_for_plugin(
        &self,
        account_id: &ProviderAccountId,
    ) -> AdminStoreResult<Option<ProviderExportCredentialInput>> {
        let Some(loaded) = self.loaded_credential(account_id).await? else {
            return Ok(None);
        };
        Ok(Some(ProviderExportCredentialInput {
            account: self.account_record(loaded.account).await?,
            provider_material: ProviderDocument::new(OpaqueProviderData::new(
                loaded.credential.into_inner(),
            )),
        }))
    }

    async fn commit_credential_import(
        &self,
        command: CredentialImportCommit,
        context: &MutationContext,
    ) -> AdminStoreResult<CredentialImportResult> {
        let mut transaction = self.pool.begin().await.map_err(|error| {
            unavailable("begin credential import transaction").with_source(error)
        })?;
        let result = async {
            acquire_write_lock(&mut transaction)
                .await
                .map_err(|error| admin_store_error("SQLite account", error))?;
            let revision = bump_config_revision(&mut transaction, Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("SQLite account", error))?;
            self.import_accounts_in_transaction(
                &mut transaction,
                CredentialImportTransaction {
                    prepared: command.prepared,
                    settings: command.settings,
                    context,
                    operation: MutationAuditOperation::ProviderAccountImportDocument,
                    outbound_proxy: command.outbound_proxy,
                    revision,
                },
            )
            .await
        }
        .await;
        finish_account_admin_transaction(transaction, result, "credential import").await
    }
    async fn commit_authorization(
        &self,
        command: AuthorizationCommit,
        context: &MutationContext,
    ) -> AdminStoreResult<AuthorizationCommitResult> {
        if command.key.provider_kind() != command.pending.provider_kind()
            || !command.key.matches_context(context)
            || !command.pending.owner_binding().matches_context(context)
        {
            return Err(invalid("invalid authorization receipt binding"));
        }
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin authorization transaction"))?;
        let result = async {
            acquire_write_lock(&mut transaction)
                .await
                .map_err(|_| unavailable("lock authorization transaction"))?;
            if let Some(result) =
                load_authorization_receipt(&mut *transaction, &command.key).await?
            {
                return Ok(AuthorizationCommitResult {
                    result,
                    newly_committed: false,
                });
            }
            let revision = bump_config_revision(&mut transaction, Utc::now().timestamp_micros())
                .await
                .map_err(|error| admin_store_error("SQLite account", error))?;
            let binding = command
                .pending
                .outbound_proxy_id()
                .zip(command.pending.outbound_proxy())
                .map(
                    |(id, proxy)| gateway_admin::model::proxies::ImportProxyBinding {
                        id: id.to_owned(),
                        proxy: proxy.clone(),
                    },
                );
            let result =
                match (command.pending.target().clone(), command.credential) {
                    (
                        AuthorizationMutationTarget::Create { .. },
                        AuthorizationCredentialCommit::Create(credential),
                    ) => {
                        if &credential.provider_kind != command.key.provider_kind() {
                            return Err(invalid("invalid authorization provider binding"));
                        }
                        let prepared = PreparedCredentialImport {
                            provider_kind: command.key.provider_kind().clone(),
                            credentials: vec![*credential],
                        };
                        let imported = self
                            .import_accounts_in_transaction(
                                &mut transaction,
                                CredentialImportTransaction {
                                    prepared,
                                    settings: command.settings,
                                    context,
                                    operation: MutationAuditOperation::ProviderAccountAuthorize,
                                    outbound_proxy: binding,
                                    revision,
                                },
                            )
                            .await?;
                        if imported.credential_ids.len() != 1 {
                            return Err(unavailable(
                                "authorization did not import exactly one account",
                            ));
                        }
                        let account_id = imported
                            .credential_ids
                            .into_iter()
                            .next()
                            .expect("one account");
                        let credential_revision = sqlx::query_scalar::<_, i64>(
                            "select credential_revision from provider_accounts where id = ?1",
                        )
                        .bind(account_id.as_str())
                        .fetch_optional(&mut *transaction)
                        .await
                        .map_err(|_| unavailable("load authorized credential revision"))?
                        .ok_or_else(|| not_found("authorized account was not persisted"))?;
                        CredentialMutationResult {
                            config_revision: imported.config_revision,
                            account_id,
                            credential_revision: Some(admin_revision(
                                crate::Revision::new(u64::try_from(credential_revision).map_err(
                                    |_| invalid("decode authorized credential revision"),
                                )?)
                                .map_err(|_| invalid("decode authorized credential revision"))?,
                            )?),
                        }
                    }
                    (
                        AuthorizationMutationTarget::Reauthorize { account_id },
                        AuthorizationCredentialCommit::Reauthorize(prepared),
                    ) => {
                        if command.settings.is_some()
                            || prepared.account_id != account_id
                            || prepared.provider_kind != *command.key.provider_kind()
                        {
                            return Err(invalid("invalid authorization reauthorization binding"));
                        }
                        self.rotate_credential_in_transaction(
                            &mut transaction,
                            *prepared,
                            None,
                            context,
                            MutationAuditOperation::ProviderAccountReauthorize,
                            revision,
                        )
                        .await?
                    }
                    _ => {
                        return Err(invalid(
                            "authorization credential does not match its target",
                        ));
                    }
                };
            store_authorization_receipt(&mut transaction, &command.key, &result).await?;
            Ok(AuthorizationCommitResult {
                result,
                newly_committed: true,
            })
        }
        .await;
        finish_account_admin_transaction(transaction, result, "authorization").await
    }
    async fn authorization_receipt(
        &self,
        key: &AuthorizationReceiptKey,
    ) -> AdminStoreResult<Option<CredentialMutationResult>> {
        load_authorization_receipt(&self.pool, key).await
    }
    async fn commit_credential_rotation(
        &self,
        command: CredentialRotationCommit,
        context: &MutationContext,
    ) -> AdminStoreResult<CredentialMutationResult> {
        self.commit_rotation(
            command,
            context,
            MutationAuditOperation::ProviderAccountRotateCredential,
            false,
        )
        .await
    }
    async fn commit_credential_refresh(
        &self,
        command: CredentialRotationCommit,
        context: &MutationContext,
    ) -> AdminStoreResult<CredentialMutationResult> {
        self.commit_rotation(
            command,
            context,
            MutationAuditOperation::ProviderAccountRefreshCredential,
            true,
        )
        .await
    }
    async fn update_account(
        &self,
        command: UpdateAccount,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountUpdateResult> {
        let account_id = ProviderAccountId::new(command.account_id.clone()).map_err(|_| {
            AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "SQLite account",
                "invalid account ID",
            )
        })?;
        let mut transaction =
            self.pool.begin().await.map_err(|error| {
                unavailable("begin account update transaction").with_source(error)
            })?;
        let result = async {
            let revision = bump_account_revision(&mut transaction).await?;
            self.update_account_settings(
                &mut transaction,
                AccountSettingsUpdate {
                    account_ids: std::slice::from_ref(&command.account_id),
                    enabled: Some(command.enabled),
                    concurrency_limit: Some(command.concurrency_limit),
                    weight: Some(command.weight),
                    model_access: command.model_access.as_ref(),
                    group_ids: Some(&command.group_ids),
                    outbound_proxy: command.outbound_proxy.as_ref(),
                    notes: command.notes.as_deref(),
                },
            )
            .await?;
            append_account_audit(
                &mut transaction,
                mutation_audit(
                    context,
                    MutationAuditOperation::ProviderAccountUpdate,
                    &command.account_id,
                    account_update_fields(&command),
                ),
                revision,
            )
            .await?;
            Ok(revision)
        }
        .await;
        let revision = finish_account_transaction(transaction, result, "account update")
            .await
            .map_err(|error| admin_store_error("SQLite account", error))?;
        Ok(AccountUpdateResult {
            config_revision: admin_revision(revision)?,
            account_id,
        })
    }
    async fn lower_concurrency_limit(
        &self,
        account_id: &ProviderAccountId,
        limit: AccountConcurrencyLimit,
        context: &MutationContext,
    ) -> AdminStoreResult<Option<AccountUpdateResult>> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin account concurrency transaction"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            let default_limit = sqlx::query_scalar::<_, i64>(
                "select max_concurrent_per_account from runtime_settings where id = 1",
            )
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("load default account concurrency"))?;
            let now = Utc::now().timestamp_micros();
            let updated = sqlx::query(
                "update provider_accounts
                   set concurrency_limit = ?1, updated_at_us = max(updated_at_us, ?2)
                 where id = ?3 and enabled = 1
                   and (coalesce(concurrency_limit, ?4) = 0
                        or coalesce(concurrency_limit, ?4) > ?1)",
            )
            .bind(i64::from(limit.get()))
            .bind(now)
            .bind(account_id.as_str())
            .bind(default_limit)
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("lower account concurrency"))?;
            if updated.rows_affected() == 0 {
                return Ok(None);
            }
            let revision = bump_config_revision(&mut transaction, now).await?;
            append_account_audit(
                &mut transaction,
                mutation_audit(
                    context,
                    MutationAuditOperation::ProviderAccountAdaptConcurrency,
                    account_id.as_str(),
                    vec!["concurrency_limit".to_owned()],
                ),
                revision,
            )
            .await?;
            Ok(Some(revision))
        }
        .await;
        let revision =
            finish_account_transaction(transaction, result, "account concurrency update")
                .await
                .map_err(|error| admin_store_error("SQLite account", error))?;
        revision
            .map(|revision| {
                Ok(AccountUpdateResult {
                    config_revision: admin_revision(revision)?,
                    account_id: account_id.clone(),
                })
            })
            .transpose()
    }
    async fn recover_account(
        &self,
        account_id: &ProviderAccountId,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountUpdateResult> {
        super::SqliteProviderCooldownRepository::new(self.pool.clone())
            .clear_all(account_id)
            .await
            .map_err(|_| unavailable("clear account cooldowns"))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin account recovery transaction"))?;
        let result = async {
            let revision = bump_account_revision(&mut transaction).await?;
            let now = Utc::now().timestamp_micros();
            let updated = sqlx::query(
                "update provider_accounts
                   set enabled = 1,
                       credential_state = case when credential_state = 'unknown' then 'unknown' else 'ready' end,
                       credential_observed_at_us = ?1,
                       access_token_expires_at_us = case
                         when access_token_expires_at_us <= ?1 then null
                         else access_token_expires_at_us end,
                       provider_quota_json = null, quota_observed_at_us = null,
                       quota_access_state = 'allowed', quota_evidence = null,
                       quota_access_observed_at_us = ?1, quota_reset_at_us = null,
                       last_error_reason = null, last_error_message = null,
                       updated_at_us = max(updated_at_us, ?1)
                 where id = ?2",
            ).bind(now).bind(account_id.as_str()).execute(&mut *transaction).await
                .map_err(|_| sqlite_unavailable("recover provider account"))?;
            if updated.rows_affected() != 1 {
                return Err(StoreError::NotFound { entity: "provider account", id: account_id.as_str().to_owned(), source: None, });
            }
            append_account_audit(
                &mut transaction,
                mutation_audit(context, MutationAuditOperation::ProviderAccountRecover,
                    account_id.as_str(), vec!["status".to_owned(), "quota".to_owned()]),
                revision,
            ).await?;
            Ok(revision)
        }.await;
        let revision = finish_account_transaction(transaction, result, "account recovery")
            .await
            .map_err(|error| admin_store_error("SQLite account", error))?;
        Ok(AccountUpdateResult {
            config_revision: admin_revision(revision)?,
            account_id: account_id.clone(),
        })
    }
    async fn batch_update_accounts(
        &self,
        command: BatchUpdateAccounts,
        context: &MutationContext,
    ) -> AdminStoreResult<AccountsUpdateResult> {
        let account_ids = command
            .account_ids
            .iter()
            .map(|id| {
                ProviderAccountId::new(id.clone()).map_err(|_| {
                    AdminStoreError::new(
                        AdminStoreErrorKind::Invalid,
                        "SQLite account",
                        "invalid account ID",
                    )
                })
            })
            .collect::<AdminStoreResult<Vec<_>>>()?;
        let audit_target = if command.account_ids.len() == 1 {
            command.account_ids[0].clone()
        } else {
            "provider_accounts".to_owned()
        };
        let mut fields = Vec::new();
        for (present, name) in [
            (command.enabled.is_some(), "enabled"),
            (command.concurrency_limit.is_some(), "concurrency_limit"),
            (command.weight.is_some(), "weight"),
            (command.group_ids.is_some(), "groups"),
            (command.model_access.is_some(), "model_access"),
            (command.outbound_proxy.is_some(), "outbound_proxy"),
        ] {
            if present {
                fields.push(name.to_owned());
            }
        }
        let mut transaction = self.pool.begin().await.map_err(|error| {
            unavailable("begin account batch update transaction").with_source(error)
        })?;
        let result = async {
            let revision = bump_account_revision(&mut transaction).await?;
            self.update_account_settings(
                &mut transaction,
                AccountSettingsUpdate {
                    account_ids: &command.account_ids,
                    enabled: command.enabled,
                    concurrency_limit: command.concurrency_limit,
                    weight: command.weight,
                    model_access: command.model_access.as_ref(),
                    group_ids: command.group_ids.as_deref(),
                    outbound_proxy: command.outbound_proxy.as_ref(),
                    notes: None,
                },
            )
            .await?;
            append_account_audit(
                &mut transaction,
                mutation_audit(
                    context,
                    MutationAuditOperation::ProviderAccountBatchUpdate,
                    &audit_target,
                    fields,
                ),
                revision,
            )
            .await?;
            Ok(revision)
        }
        .await;
        let revision = finish_account_transaction(transaction, result, "account batch update")
            .await
            .map_err(|error| admin_store_error("SQLite account", error))?;
        Ok(AccountsUpdateResult {
            config_revision: admin_revision(revision)?,
            account_ids,
        })
    }
    async fn delete_accounts(
        &self,
        command: DeleteAccounts,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision> {
        validate_ids(&command.account_ids, 200, "account deletion")
            .map_err(|error| admin_store_error("SQLite account", error))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin account deletion transaction"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            let first_kind = sqlx::query_scalar::<_, String>(
                "select provider_kind from provider_accounts where id = ?1",
            )
            .bind(&command.account_ids[0])
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("load account deletion scope"))?
            .ok_or_else(|| StoreError::NotFound {
                entity: "provider account",
                id: command.account_ids[0].clone(),
                source: None,
            })?;
            let mut query = QueryBuilder::<Sqlite>::new(
                "select count(*) from provider_accounts where provider_kind = ",
            );
            query.push_bind(&first_kind);
            query.push(" and id in (");
            {
                let mut separated = query.separated(", ");
                for id in &command.account_ids {
                    separated.push_bind(id);
                }
            }
            query.push(")");
            let matched: i64 = query
                .build_query_scalar()
                .fetch_one(&mut *transaction)
                .await
                .map_err(|_| sqlite_unavailable("validate account deletion scope"))?;
            if usize::try_from(matched).ok() != Some(command.account_ids.len()) {
                return Err(StoreError::InvalidData {
                    entity: "provider account",
                    message: "all deleted accounts must exist and match Provider scope".to_owned(),
                    source: None,
                });
            }
            let revision =
                bump_config_revision(&mut transaction, Utc::now().timestamp_micros()).await?;
            let mut query =
                QueryBuilder::<Sqlite>::new("delete from provider_accounts where provider_kind = ");
            query.push_bind(first_kind);
            query.push(" and id in (");
            {
                let mut separated = query.separated(", ");
                for id in &command.account_ids {
                    separated.push_bind(id);
                }
            }
            query.push(")");
            query
                .build()
                .execute(&mut *transaction)
                .await
                .map_err(|_| sqlite_unavailable("delete provider accounts"))?;
            let target = if command.account_ids.len() == 1 {
                command.account_ids[0].as_str()
            } else {
                "provider_accounts"
            };
            append_account_audit(
                &mut transaction,
                mutation_audit(
                    context,
                    MutationAuditOperation::ProviderAccountDelete,
                    target,
                    Vec::new(),
                ),
                revision,
            )
            .await?;
            Ok(revision)
        }
        .await;
        let revision = finish_account_transaction(transaction, result, "account deletion")
            .await
            .map_err(|error| admin_store_error("SQLite account", error))?;
        admin_revision(revision)
    }
    async fn record_credential_export(
        &self,
        account_ids: &[ProviderAccountId],
        context: &MutationContext,
    ) -> AdminStoreResult<()> {
        if account_ids.is_empty() || account_ids.len() > 200 {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "SQLite account",
                "account export requires between 1 and 200 IDs",
            ));
        }
        let mut unique = std::collections::HashSet::new();
        if account_ids.iter().any(|id| !unique.insert(id.as_str())) {
            return Err(AdminStoreError::new(
                AdminStoreErrorKind::Invalid,
                "SQLite account",
                "account export contains duplicate IDs",
            ));
        }
        let ids = account_ids
            .iter()
            .map(|id| id.as_str().to_owned())
            .collect::<Vec<_>>();
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| unavailable("begin credential export audit"))?;
        let result = async {
            acquire_write_lock(&mut transaction).await?;
            ensure_accounts_exist(&mut transaction, &ids).await?;
            let revision = sqlx::query_scalar::<_, i64>(
                "select config_revision from runtime_settings where id = 1",
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("load export audit config revision"))?
            .ok_or_else(|| StoreError::NotFound {
                entity: "runtime settings",
                id: "1".to_owned(),
                source: None,
            })?;
            let revision = crate::Revision::new(
                u64::try_from(revision).map_err(|_| invalid_store("decode config revision"))?,
            )?;
            for id in &ids {
                let mut audit = mutation_audit(
                    context,
                    MutationAuditOperation::ProviderAccountExportCredentials,
                    id,
                    Vec::new(),
                );
                audit.config_revision = Some(revision_i64(revision)?);
                append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
            }
            Ok(())
        }
        .await;
        finish_account_transaction(transaction, result, "credential export audit")
            .await
            .map_err(|error| admin_store_error("SQLite account", error))
    }
}
