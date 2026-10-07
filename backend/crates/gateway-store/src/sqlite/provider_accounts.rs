//! SQLite 对 Provider 账号 Core 持久化端口的独立适配。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{
    account::{
        AccountConcurrencyLimit, AccountErrorReason, AccountModelAccess, AccountStateChange,
        AccountWeight, CredentialCasOutcome, CredentialCasUpdate, CredentialCasUpdateParts,
        CredentialRevision, CredentialState, LoadedCredential, NewProviderAccount,
        OpaqueProviderData, PlaintextCredential, ProviderAccount, ProviderAccountId,
        ProviderAccountStore, ProviderAccountUpdate, ProviderRefreshQuery, QuotaAccessChange,
        QuotaAccessState, QuotaEvidence, QuotaObservation, QuotaObservationTouch, QuotaState,
        QuotaWriteOutcome,
    },
    error::{StoreError as CoreStoreError, StoreErrorKind as CoreStoreErrorKind},
    routing::ProviderKind,
};
use serde_json::{Map, Value};
use sqlx::{Row, SqlitePool};

use super::{
    sqlite_unavailable,
    value::{datetime_from_micros, datetime_to_micros},
};
use crate::{StoreError, StoreResult};

const CREDENTIALS_MAX_BYTES: usize = 256 * 1024;
const QUOTA_MAX_BYTES: usize = 128 * 1024;

#[derive(Clone)]
pub struct SqliteProviderAccountRepository {
    pool: SqlitePool,
}

impl SqliteProviderAccountRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn load(&self, id: &str) -> Result<Option<LoadedCredential>, StoreError> {
        let row = sqlx::query(ACCOUNT_SELECT)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| sqlite_unavailable("load Provider account"))?;
        row.map(loaded_account_from_row).transpose()
    }
}

#[async_trait]
impl ProviderAccountStore for SqliteProviderAccountRepository {
    async fn create_account(&self, account: NewProviderAccount) -> Result<(), CoreStoreError> {
        if account.account.revision().get() != 1
            || account.account.name().trim().is_empty()
            || !valid_authentication_kind(account.account.authentication_kind())
            || (!account.account.has_refresh_token() && account.account.next_refresh_at().is_some())
        {
            return Err(core_invalid());
        }
        let credentials = serde_json::Value::Object(account.credential.into_inner());
        validate_json_size(
            "provider_credentials_json",
            &credentials,
            CREDENTIALS_MAX_BYTES,
        )
        .map_err(core_store_error)?;
        let model_access = account
            .model_access
            .unwrap_or_else(|| account.account.model_access().clone());
        let model_access_json = serde_json::to_value(&model_access).map_err(|_| core_invalid())?;
        validate_json_size("model_access_json", &model_access_json, 128 * 1024)
            .map_err(core_store_error)?;
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "insert into provider_accounts (
               id, provider_kind, name, email, upstream_user_id, upstream_account_id, plan_type,
               authentication_kind, provider_credentials_json, credential_revision, has_refresh_token,
               access_token_expires_at_us, next_refresh_at_us, enabled, concurrency_limit, weight,
               model_access_json, credential_state, credential_observed_at_us, quota_access_state,
               outbound_proxy_url, created_at_us, updated_at_us
             ) values (
               ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 1, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
               ?18, 'unknown', ?19, ?20, max(?20, ?18)
             )",
        )
        .bind(account.account.id().as_str())
        .bind(account.account.provider().as_str())
        .bind(account.account.name())
        .bind(account.account.email())
        .bind(account.account.upstream_user_id())
        .bind(account.account.upstream_account_id())
        .bind(account.account.plan_type())
        .bind(account.account.authentication_kind())
        .bind(serialize_json(&credentials).map_err(core_store_error)?)
        .bind(bool_i64(account.account.has_refresh_token()))
        .bind(account.account.access_token_expires_at().map(system_time_micros).transpose().map_err(core_store_error)?)
        .bind(account.account.next_refresh_at().map(system_time_micros).transpose().map_err(core_store_error)?)
        .bind(bool_i64(account.account.enabled()))
        .bind(account.account.concurrency_limit().map(|value| i64::from(value.get())))
        .bind(i64::from(account.account.weight().get()))
        .bind(serialize_json(&model_access_json).map_err(core_store_error)?)
        .bind(account.account.credential_state().as_str())
        .bind(now)
        .bind(account.account.outbound_proxy().map(|proxy| proxy.expose_url()))
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        Ok(())
    }

    async fn get_account(
        &self,
        account: &ProviderAccountId,
    ) -> Result<Option<ProviderAccount>, CoreStoreError> {
        self.load(account.as_str())
            .await
            .map_err(core_store_error)?
            .map(|loaded| Ok(loaded.account))
            .transpose()
    }

    async fn list_accounts(&self) -> Result<Vec<ProviderAccount>, CoreStoreError> {
        let rows = sqlx::query(ACCOUNT_SELECT_ALL)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        rows.into_iter()
            .map(|row| {
                loaded_account_from_row(row)
                    .map(|loaded| loaded.account)
                    .map_err(core_store_error)
            })
            .collect()
    }

    async fn list_for_provider(
        &self,
        provider: &ProviderKind,
    ) -> Result<Vec<ProviderAccount>, CoreStoreError> {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "{ACCOUNT_SELECT_ALL} where provider_kind = ?1 and enabled = 1"
        )))
        .bind(provider.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        rows.into_iter()
            .map(|row| {
                loaded_account_from_row(row)
                    .map(|loaded| loaded.account)
                    .map_err(core_store_error)
            })
            .collect()
    }

    async fn list_refresh_candidates(
        &self,
        query: ProviderRefreshQuery,
    ) -> Result<Vec<LoadedCredential>, CoreStoreError> {
        let excluded = query
            .excluded_account_ids()
            .iter()
            .map(|id| id.as_str().to_owned())
            .collect::<Vec<_>>();
        let mut sql = format!(
            "{ACCOUNT_SELECT_ALL}
             where provider_kind = ?1 and has_refresh_token = 1
               and credential_state in ('unknown', 'ready')
               and access_token_expires_at_us is not null
               and (access_token_expires_at_us <= ?3 or
                    (access_token_expires_at_us <= ?2 and
                     (next_refresh_at_us is null or next_refresh_at_us <= ?4)))"
        );
        if !excluded.is_empty() {
            let placeholders = (5..5 + excluded.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" and id not in ({placeholders})"));
        }
        sql.push_str(" order by access_token_expires_at_us, id limit ?");
        sql.push_str(&(5 + excluded.len()).to_string());
        let mut statement = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(query.provider().as_str())
            .bind(system_time_micros(query.refresh_due_before()).map_err(|_| core_invalid())?)
            .bind(system_time_micros(query.force_due_before()).map_err(|_| core_invalid())?)
            .bind(system_time_micros(query.observed_at()).map_err(|_| core_invalid())?);
        for id in excluded {
            statement = statement.bind(id);
        }
        let rows = statement
            .bind(i64::from(query.limit().get()))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        rows.into_iter()
            .map(|row| loaded_account_from_row(row).map_err(core_store_error))
            .collect()
    }

    async fn load_credential(
        &self,
        account: &ProviderAccountId,
        expected_revision: CredentialRevision,
    ) -> Result<LoadedCredential, CoreStoreError> {
        let loaded = self.load_current_credential(account).await?;
        if loaded.account.revision().get() != expected_revision.get() {
            return Err(CoreStoreError::new(CoreStoreErrorKind::Conflict));
        }
        Ok(loaded)
    }

    async fn load_current_credential(
        &self,
        account: &ProviderAccountId,
    ) -> Result<LoadedCredential, CoreStoreError> {
        self.load(account.as_str())
            .await
            .map_err(core_store_error)?
            .ok_or_else(core_invalid)
    }

    async fn compare_and_swap_credential(
        &self,
        update: CredentialCasUpdate,
    ) -> Result<CredentialCasOutcome, CoreStoreError> {
        let CredentialCasUpdateParts {
            account_id,
            expected_revision,
            profile,
            preserve_profile,
            credential,
            has_refresh_token,
            access_token_expires_at,
            next_refresh_at,
            account_state,
        } = update.into_parts();
        if profile.account_id != account_id {
            return Err(core_invalid());
        }
        let credentials = Value::Object(credential.into_inner());
        validate_json_size(
            "provider_credentials_json",
            &credentials,
            CREDENTIALS_MAX_BYTES,
        )
        .map_err(core_store_error)?;
        let observed_at = account_state
            .as_ref()
            .map(|state| system_time_micros(state.observed_at))
            .transpose()
            .map_err(core_store_error)?;
        let next = sqlx::query_scalar::<_, i64>(
            "update provider_accounts
             set name = case when ?8 = 1 then name else ?3 end,
                 email = case when ?8 = 1 then email else ?4 end,
                 plan_type = case when ?8 = 1 then plan_type else ?5 end,
                 provider_credentials_json = ?6,
                 credential_revision = credential_revision + 1,
                 has_refresh_token = ?7,
                 access_token_expires_at_us = ?9,
                 next_refresh_at_us = ?10,
                 credential_state = case when ?11 is null then credential_state else ?12 end,
                 credential_observed_at_us = case when ?11 is null then credential_observed_at_us else ?11 end,
                 last_error_reason = case when ?11 is null then last_error_reason else ?13 end,
                 last_error_message = case when ?11 is null then last_error_message else ?14 end,
                 updated_at_us = max(updated_at_us, ?15, coalesce(?11, ?15))
             where id = ?1 and credential_revision = ?2
             returning credential_revision",
        )
        .bind(account_id.as_str())
        .bind(revision_i64(expected_revision.get()).map_err(core_store_error)?)
        .bind(profile.name)
        .bind(profile.email)
        .bind(profile.plan_type)
        .bind(serialize_json(&credentials).map_err(core_store_error)?)
        .bind(bool_i64(has_refresh_token))
        .bind(bool_i64(preserve_profile))
        .bind(access_token_expires_at.map(system_time_micros).transpose().map_err(core_store_error)?)
        .bind(next_refresh_at.map(system_time_micros).transpose().map_err(core_store_error)?)
        .bind(observed_at)
        .bind(account_state.as_ref().map(|state| state.credential_state.as_str()))
        .bind(account_state.as_ref().and_then(|state| state.error_reason.map(AccountErrorReason::as_str)))
        .bind(account_state.as_ref().and_then(|state| state.message.as_deref()))
        .bind(Utc::now().timestamp_micros())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        match next {
            Some(revision) => Ok(CredentialCasOutcome::Updated(
                CredentialRevision::new(u64::try_from(revision).map_err(|_| core_invalid())?)
                    .map_err(|_| core_invalid())?,
            )),
            None => Ok(CredentialCasOutcome::Conflict),
        }
    }

    async fn get_quotas(
        &self,
        accounts: &[ProviderAccountId],
    ) -> Result<Vec<QuotaObservation>, CoreStoreError> {
        if accounts.is_empty() {
            return Ok(Vec::new());
        }
        let mut observations = Vec::new();
        for chunk in accounts.chunks(500) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("?{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "select id, credential_revision, plan_type, provider_quota_json, quota_observed_at_us,
                        quota_access_state, quota_evidence, quota_access_observed_at_us, quota_reset_at_us
                 from provider_accounts where id in ({placeholders}) and quota_observed_at_us is not null"
            );
            let mut statement = sqlx::query(sqlx::AssertSqlSafe(sql));
            for account in chunk {
                statement = statement.bind(account.as_str());
            }
            let rows = statement
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
            observations.extend(
                rows.into_iter()
                    .map(quota_observation_from_row)
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        Ok(observations)
    }

    async fn compare_and_swap_quota(
        &self,
        observation: QuotaObservation,
    ) -> Result<QuotaWriteOutcome, CoreStoreError> {
        let quota = Value::Object(observation.quota.into_inner());
        validate_json_size("provider_quota_json", &quota, QUOTA_MAX_BYTES)
            .map_err(core_store_error)?;
        let access_observed_at = observation
            .state
            .observed_at()
            .map(system_time_micros)
            .transpose()
            .map_err(core_store_error)?;
        let observed_at = system_time_micros(observation.observed_at).map_err(core_store_error)?;
        let result = sqlx::query(
            "update provider_accounts
             set provider_quota_json = ?3, quota_observed_at_us = ?4,
                 plan_type = coalesce(?9, plan_type),
                 quota_access_state = case when ?7 is not null and
                   (quota_access_observed_at_us is null or quota_access_observed_at_us <= ?7) then ?5 else quota_access_state end,
                 quota_evidence = case when ?7 is not null and
                   (quota_access_observed_at_us is null or quota_access_observed_at_us <= ?7) then ?6 else quota_evidence end,
                 quota_access_observed_at_us = case when ?7 is not null and
                   (quota_access_observed_at_us is null or quota_access_observed_at_us <= ?7) then ?7 else quota_access_observed_at_us end,
                 quota_reset_at_us = case when ?7 is not null and
                   (quota_access_observed_at_us is null or quota_access_observed_at_us <= ?7) then ?8 else quota_reset_at_us end,
                 updated_at_us = max(updated_at_us, ?4, coalesce(?7, ?4), ?10)
             where id = ?1 and credential_revision = ?2
               and (quota_observed_at_us is null or quota_observed_at_us <= ?4)",
        )
        .bind(observation.account_id.as_str())
        .bind(revision_i64(observation.expected_revision.get()).map_err(core_store_error)?)
        .bind(serialize_json(&quota).map_err(core_store_error)?)
        .bind(observed_at)
        .bind(observation.state.access().as_str())
        .bind(observation.state.evidence().map(QuotaEvidence::as_str))
        .bind(access_observed_at)
        .bind(observation.state.reset_at().map(system_time_micros).transpose().map_err(core_store_error)?)
        .bind(observation.plan_type)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        Ok(if result.rows_affected() == 1 {
            QuotaWriteOutcome::Updated
        } else {
            QuotaWriteOutcome::Conflict
        })
    }

    async fn touch_quota_observation(
        &self,
        touch: QuotaObservationTouch,
    ) -> Result<QuotaWriteOutcome, CoreStoreError> {
        let observed_at = system_time_micros(touch.observed_at).map_err(|_| core_invalid())?;
        let result = sqlx::query(
            "update provider_accounts set quota_observed_at_us = ?3,
               updated_at_us = max(updated_at_us, ?3, ?4)
             where id = ?1 and credential_revision = ?2 and provider_quota_json is not null
               and (quota_observed_at_us is null or quota_observed_at_us <= ?3)",
        )
        .bind(touch.account_id.as_str())
        .bind(revision_i64(touch.expected_revision.get()).map_err(|_| core_invalid())?)
        .bind(observed_at)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        Ok(if result.rows_affected() == 1 {
            QuotaWriteOutcome::Updated
        } else {
            QuotaWriteOutcome::Conflict
        })
    }

    async fn apply_quota_access(
        &self,
        change: QuotaAccessChange,
    ) -> Result<QuotaWriteOutcome, CoreStoreError> {
        let observed_at = change
            .state
            .observed_at()
            .map(system_time_micros)
            .transpose()
            .map_err(|_| core_invalid())?
            .ok_or_else(core_invalid)?;
        let result = sqlx::query(
            "update provider_accounts set quota_access_observed_at_us = ?3,
               quota_access_state = ?4, quota_evidence = ?5, quota_reset_at_us = ?6,
               updated_at_us = max(updated_at_us, ?3, ?7)
             where id = ?1 and credential_revision = ?2
               and (quota_access_observed_at_us is null or quota_access_observed_at_us <= ?3)",
        )
        .bind(change.account_id.as_str())
        .bind(revision_i64(change.expected_revision.get()).map_err(|_| core_invalid())?)
        .bind(observed_at)
        .bind(change.state.access().as_str())
        .bind(change.state.evidence().map(QuotaEvidence::as_str))
        .bind(
            change
                .state
                .reset_at()
                .map(system_time_micros)
                .transpose()
                .map_err(|_| core_invalid())?,
        )
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        Ok(if result.rows_affected() == 1 {
            QuotaWriteOutcome::Updated
        } else {
            QuotaWriteOutcome::Conflict
        })
    }

    async fn apply_state_change(&self, change: AccountStateChange) -> Result<(), CoreStoreError> {
        let observed_at = system_time_micros(change.observed_at).map_err(|_| core_invalid())?;
        let result = sqlx::query(
            "update provider_accounts set credential_state = ?3,
               credential_observed_at_us = ?4, last_error_reason = ?5, last_error_message = ?6,
               updated_at_us = max(updated_at_us, ?4, ?7)
             where id = ?1 and credential_revision = ?2
               and credential_observed_at_us <= ?4",
        )
        .bind(change.account_id.as_str())
        .bind(revision_i64(change.expected_revision.get()).map_err(|_| core_invalid())?)
        .bind(change.credential_state.as_str())
        .bind(observed_at)
        .bind(change.error_reason.map(AccountErrorReason::as_str))
        .bind(change.message)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(CoreStoreError::new(CoreStoreErrorKind::Conflict))
        }
    }

    async fn update_account(&self, update: ProviderAccountUpdate) -> Result<(), CoreStoreError> {
        let result = sqlx::query(
            "update provider_accounts set name = ?2, email = ?3, plan_type = ?4,
               updated_at_us = max(updated_at_us, ?5) where id = ?1",
        )
        .bind(update.account_id.as_str())
        .bind(update.name)
        .bind(update.email)
        .bind(update.plan_type)
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        require_core_update(result.rows_affected() == 1)
    }

    async fn set_enabled(
        &self,
        account: &ProviderAccountId,
        enabled: bool,
    ) -> Result<(), CoreStoreError> {
        let result = sqlx::query(
            "update provider_accounts set enabled = ?2, updated_at_us = max(updated_at_us, ?3) where id = ?1",
        )
        .bind(account.as_str())
        .bind(bool_i64(enabled))
        .bind(Utc::now().timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        require_core_update(result.rows_affected() == 1)
    }

    async fn delete_account(&self, account: &ProviderAccountId) -> Result<(), CoreStoreError> {
        let result = sqlx::query("delete from provider_accounts where id = ?1 and enabled = 0")
            .bind(account.as_str())
            .execute(&self.pool)
            .await
            .map_err(|_| CoreStoreError::new(CoreStoreErrorKind::Unavailable))?;
        require_core_update(result.rows_affected() == 1)
    }
}

const ACCOUNT_SELECT: &str = "select * from provider_accounts where id = ?1";
const ACCOUNT_SELECT_ALL: &str = "select * from provider_accounts";

fn loaded_account_from_row(row: sqlx::sqlite::SqliteRow) -> StoreResult<LoadedCredential> {
    let account = account_from_row(&row)?;
    let credential_json = row
        .try_get::<String, _>("provider_credentials_json")
        .map_err(|_| invalid("provider_credentials_json"))?;
    let credential: Map<String, Value> =
        serde_json::from_str(&credential_json).map_err(|_| invalid("provider_credentials_json"))?;
    Ok(LoadedCredential {
        account,
        credential: PlaintextCredential::new(credential),
    })
}

fn account_from_row(row: &sqlx::sqlite::SqliteRow) -> StoreResult<ProviderAccount> {
    let id = ProviderAccountId::new(row.try_get::<String, _>("id").map_err(|_| invalid("id"))?)
        .map_err(|_| invalid("id"))?;
    let provider = ProviderKind::new(
        row.try_get::<String, _>("provider_kind")
            .map_err(|_| invalid("provider_kind"))?,
    )
    .map_err(|_| invalid("provider_kind"))?;
    let revision = CredentialRevision::new(
        u64::try_from(
            row.try_get::<i64, _>("credential_revision")
                .map_err(|_| invalid("credential_revision"))?,
        )
        .map_err(|_| invalid("credential_revision"))?,
    )
    .map_err(|_| invalid("credential_revision"))?;
    let access = QuotaAccessState::parse(
        &row.try_get::<String, _>("quota_access_state")
            .map_err(|_| invalid("quota_access_state"))?,
    )
    .ok_or_else(|| invalid("quota_access_state"))?;
    let evidence = row
        .try_get::<Option<String>, _>("quota_evidence")
        .map_err(|_| invalid("quota_evidence"))?
        .map(|value| QuotaEvidence::parse(&value).ok_or_else(|| invalid("quota_evidence")))
        .transpose()?;
    let access_observed = from_optional_micros(
        row.try_get("quota_access_observed_at_us")
            .map_err(|_| invalid("quota_access_observed_at_us"))?,
    )?;
    let reset_at = from_optional_micros(
        row.try_get("quota_reset_at_us")
            .map_err(|_| invalid("quota_reset_at_us"))?,
    )?;
    let quota = QuotaState::from_persisted(access, evidence, access_observed, reset_at)
        .ok_or_else(|| invalid("quota state"))?;
    let model_access_json: String = row
        .try_get("model_access_json")
        .map_err(|_| invalid("model_access_json"))?;
    let model_access: AccountModelAccess =
        serde_json::from_str(&model_access_json).map_err(|_| invalid("model_access_json"))?;
    let outbound_proxy = row
        .try_get::<Option<String>, _>("outbound_proxy_url")
        .map_err(|_| invalid("outbound_proxy_url"))?
        .map(|value| {
            gateway_core::account::OutboundProxy::parse(&value)
                .map_err(|_| invalid("outbound_proxy_url"))
        })
        .transpose()?;
    let state = CredentialState::parse(
        &row.try_get::<String, _>("credential_state")
            .map_err(|_| invalid("credential_state"))?,
    )
    .ok_or_else(|| invalid("credential_state"))?;
    let error_reason = row
        .try_get::<Option<String>, _>("last_error_reason")
        .map_err(|_| invalid("last_error_reason"))?
        .map(|value| AccountErrorReason::parse(&value).ok_or_else(|| invalid("last_error_reason")))
        .transpose()?;
    let concurrency_limit = row
        .try_get::<Option<i64>, _>("concurrency_limit")
        .map_err(|_| invalid("concurrency_limit"))?
        .map(|value| {
            u32::try_from(value)
                .ok()
                .and_then(AccountConcurrencyLimit::new)
                .ok_or_else(|| invalid("concurrency_limit"))
        })
        .transpose()?;
    let weight = u16::try_from(
        row.try_get::<i64, _>("weight")
            .map_err(|_| invalid("weight"))?,
    )
    .ok()
    .and_then(AccountWeight::new)
    .ok_or_else(|| invalid("weight"))?;
    let mut account = ProviderAccount::new(
        id,
        provider,
        row.try_get("name").map_err(|_| invalid("name"))?,
        row.try_get("upstream_user_id")
            .map_err(|_| invalid("upstream_user_id"))?,
        row.try_get("authentication_kind")
            .map_err(|_| invalid("authentication_kind"))?,
        revision,
        from_optional_micros(
            row.try_get("access_token_expires_at_us")
                .map_err(|_| invalid("access_token_expires_at_us"))?,
        )?,
    )
    .with_profile(
        row.try_get("email").map_err(|_| invalid("email"))?,
        row.try_get("upstream_account_id")
            .map_err(|_| invalid("upstream_account_id"))?,
        row.try_get("plan_type").map_err(|_| invalid("plan_type"))?,
    )
    .with_account_facts(
        row.try_get::<i64, _>("enabled")
            .map_err(|_| invalid("enabled"))?
            != 0,
        state,
        quota,
        error_reason,
        row.try_get("last_error_message")
            .map_err(|_| invalid("last_error_message"))?,
    )
    .with_scheduling(concurrency_limit, weight)
    .with_model_access(model_access)
    .with_refresh_schedule(
        row.try_get::<i64, _>("has_refresh_token")
            .map_err(|_| invalid("has_refresh_token"))?
            != 0,
        from_optional_micros(
            row.try_get("next_refresh_at_us")
                .map_err(|_| invalid("next_refresh_at_us"))?,
        )?,
    );
    if let Some(proxy) = outbound_proxy {
        account = account.with_outbound_proxy(Some(proxy));
    }
    Ok(account)
}

fn quota_observation_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<QuotaObservation, CoreStoreError> {
    let account_id =
        ProviderAccountId::new(row.try_get::<String, _>("id").map_err(|_| core_invalid())?)
            .map_err(|_| core_invalid())?;
    let revision = CredentialRevision::new(
        u64::try_from(
            row.try_get::<i64, _>("credential_revision")
                .map_err(|_| core_invalid())?,
        )
        .map_err(|_| core_invalid())?,
    )
    .map_err(|_| core_invalid())?;
    let json = row
        .try_get::<Option<String>, _>("provider_quota_json")
        .map_err(|_| core_invalid())?
        .ok_or_else(core_invalid)?;
    let value: Value = serde_json::from_str(&json).map_err(|_| core_invalid())?;
    let quota = value.as_object().cloned().ok_or_else(core_invalid)?;
    let access = QuotaAccessState::parse(
        &row.try_get::<String, _>("quota_access_state")
            .map_err(|_| core_invalid())?,
    )
    .ok_or_else(core_invalid)?;
    let evidence = row
        .try_get::<Option<String>, _>("quota_evidence")
        .map_err(|_| core_invalid())?
        .map(|value| QuotaEvidence::parse(&value).ok_or_else(core_invalid))
        .transpose()?;
    let observed = from_optional_micros(
        row.try_get("quota_access_observed_at_us")
            .map_err(|_| core_invalid())?,
    )
    .map_err(|_| core_invalid())?;
    let reset = from_optional_micros(
        row.try_get("quota_reset_at_us")
            .map_err(|_| core_invalid())?,
    )
    .map_err(|_| core_invalid())?;
    let state =
        QuotaState::from_persisted(access, evidence, observed, reset).ok_or_else(core_invalid)?;
    let observed_at = row
        .try_get::<i64, _>("quota_observed_at_us")
        .map_err(|_| core_invalid())?;
    Ok(QuotaObservation {
        plan_type: row.try_get("plan_type").map_err(|_| core_invalid())?,
        account_id,
        expected_revision: revision,
        quota: OpaqueProviderData::new(quota),
        observed_at: datetime_from_micros(observed_at)
            .map_err(|_| core_invalid())?
            .into(),
        state,
    })
}

fn valid_authentication_kind(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && value.len() <= 64
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_json_size(field: &'static str, value: &Value, max: usize) -> StoreResult<()> {
    let size = serde_json::to_vec(value).map_err(|_| invalid(field))?.len();
    if size > max || !value.is_object() {
        Err(invalid(field))
    } else {
        Ok(())
    }
}

fn serialize_json(value: &Value) -> StoreResult<String> {
    serde_json::to_string(value).map_err(|_| invalid("JSON value"))
}

fn system_time_micros(value: std::time::SystemTime) -> StoreResult<i64> {
    Ok(datetime_to_micros(DateTime::<Utc>::from(value)))
}

fn from_optional_micros(value: Option<i64>) -> StoreResult<Option<std::time::SystemTime>> {
    value
        .map(|value| datetime_from_micros(value).map(Into::into))
        .transpose()
}

fn revision_i64(value: u64) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| invalid("credential revision"))
}

const fn bool_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn require_core_update(updated: bool) -> Result<(), CoreStoreError> {
    if updated {
        Ok(())
    } else {
        Err(CoreStoreError::new(CoreStoreErrorKind::InvalidState))
    }
}

fn core_store_error(error: StoreError) -> CoreStoreError {
    CoreStoreError::new(match error {
        StoreError::Unavailable { .. } => CoreStoreErrorKind::Unavailable,
        StoreError::Conflict { .. } => CoreStoreErrorKind::Conflict,
        StoreError::NotFound { .. } | StoreError::InvalidData { .. } => {
            CoreStoreErrorKind::InvalidData
        }
    })
}

fn core_invalid() -> CoreStoreError {
    CoreStoreError::new(CoreStoreErrorKind::InvalidData)
}

fn invalid(field: &'static str) -> StoreError {
    StoreError::InvalidData {
        entity: "provider account",
        message: format!("invalid {field}"),
        source: None,
    }
}
