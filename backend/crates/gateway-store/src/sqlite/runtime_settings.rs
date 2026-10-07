//! SQLite 运行设置持久化及 Provider 运行策略适配。

use std::{collections::BTreeMap, num::NonZeroU32, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{
    account::OpaqueProviderData,
    provider_ports::{
        ProviderFreezePolicy, ProviderRefreshPolicy, ProviderRuntimePolicyPort, ProviderStoreError,
        ProviderStoreErrorKind, ProviderWarmupPolicy,
    },
    routing::{ConfigRevision, ProviderKind},
};
use sqlx::{Row, SqlitePool};

use crate::{
    StoreError, StoreResult,
    runtime_settings::{RuntimeSettings, RuntimeSettingsRepository, RuntimeSettingsUpdate},
};

use super::{acquire_write_lock, sqlite_unavailable};

#[derive(Clone)]
pub struct SqliteRuntimeSettingsRepository {
    pool: SqlitePool,
}

impl SqliteRuntimeSettingsRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
    async fn apply_settings_update(
        &self,
        update: RuntimeSettingsUpdate,
        expected_revision: Option<crate::Revision>,
        audit: Option<crate::AdminAuditEvent>,
    ) -> StoreResult<RuntimeSettings> {
        update.validate()?;
        let location = update
            .request_location
            .clone()
            .normalized()
            .map_err(|_| invalid("request location is invalid"))?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite runtime settings update"))?;
        acquire_write_lock(&mut transaction).await?;
        if let Some(expected_revision) = expected_revision {
            let current_revision = sqlx::query_scalar::<_, i64>(
                "select config_revision from runtime_settings where id = 1",
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("read SQLite settings revision"))?
            .ok_or_else(|| StoreError::NotFound {
                entity: "runtime settings",
                id: "1".to_owned(),
                source: None,
            })?;
            if u64::try_from(current_revision).ok() != Some(expected_revision.get()) {
                return Err(StoreError::Conflict {
                    entity: "runtime settings",
                    id: "1".to_owned(),
                    kind: crate::ConflictKind::StaleRevision,
                    source: None,
                });
            }
        }

        let current_profiles = sqlx::query_scalar::<_, String>(
            "select provider_request_profiles_json from runtime_settings where id = 1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite request profiles for update"))?
        .ok_or_else(|| StoreError::NotFound {
            entity: "runtime settings",
            id: "1".to_owned(),
            source: None,
        })?;
        let mut profiles = decode_profiles(&current_profiles)?;
        for (provider, profile) in &update.request_profile_updates {
            if let Some(profile) = profile {
                profiles.insert(
                    provider.as_str().to_owned(),
                    profile.expose_to_provider().clone(),
                );
            } else {
                profiles.remove(provider.as_str());
            }
        }
        let profiles = encode_json(&profiles)?;
        let model_mappings = encode_json(&update.model_mappings)?;
        let smart_scheduling = encode_json(&update.smart_scheduling)?;
        let request_location = encode_json(&location)?;
        let now = Utc::now().timestamp_micros();
        let next_revision = sqlx::query_scalar::<_, i64>(
            "update runtime_settings set
               config_revision = config_revision + 1,
               refresh_margin_seconds = ?1,
               refresh_concurrency = ?2,
               max_concurrent_per_account = ?3,
               request_interval_ms = ?4,
               rotation_strategy = ?5,
               model_mappings_json = ?6,
               usage_retention_days = ?7,
               ops_event_retention_days = ?8,
               audit_retention_days = ?9,
               min_codex_desktop_version = ?10,
               min_codex_cli_version = ?11,
               max_waiting_per_key = ?12,
               max_waiting_per_account = ?13,
               concurrency_wait_timeout_seconds = ?14,
               account_auto_freeze_enabled = ?15,
               account_auto_freeze_threshold = ?16,
               account_auto_freeze_window_seconds = ?17,
               account_auto_freeze_duration_seconds = ?18,
               account_auto_freeze_probe_enabled = ?19,
               account_auto_freeze_probe_model = ?20,
               account_auto_freeze_adaptive_concurrency = ?21,
               request_location_json = ?22,
               request_location_enabled = ?23,
               responses_max_decompressed_body_bytes = ?24,
               provider_request_profiles_json = ?25,
               account_warmup_enabled = ?26,
               account_warmup_schedule_time = ?27,
               account_warmup_model = ?28,
               smart_scheduling_json = ?29,
               openai_guardian_reserved_concurrency = ?30,
               openai_account_affinity = ?31,
               max_account_rotations = ?32,
               openai_session_affinity_ttl_hours = ?33,
               updated_at_us = max(updated_at_us, ?34)
             where id = 1 and config_revision < 9223372036854775807
             returning config_revision",
        )
        .bind(to_i64(update.refresh_margin_seconds)?)
        .bind(i64::from(update.refresh_concurrency))
        .bind(i64::from(update.max_concurrent_per_account))
        .bind(to_i64(update.request_interval_ms)?)
        .bind(&update.rotation_strategy)
        .bind(model_mappings)
        .bind(i64::from(update.usage_retention_days))
        .bind(i64::from(update.ops_event_retention_days))
        .bind(i64::from(update.audit_retention_days))
        .bind(update.min_codex_desktop_version.as_deref())
        .bind(update.min_codex_cli_version.as_deref())
        .bind(i64::from(update.max_waiting_per_key))
        .bind(i64::from(update.max_waiting_per_account))
        .bind(i64::from(update.concurrency_wait_timeout_seconds))
        .bind(bool_i64(update.account_auto_freeze_enabled))
        .bind(i64::from(update.account_auto_freeze_threshold))
        .bind(to_i64(update.account_auto_freeze_window_seconds)?)
        .bind(to_i64(update.account_auto_freeze_duration_seconds)?)
        .bind(bool_i64(update.account_auto_freeze_probe_enabled))
        .bind(update.account_auto_freeze_probe_model.as_deref())
        .bind(bool_i64(update.account_auto_freeze_adaptive_concurrency))
        .bind(request_location)
        .bind(bool_i64(update.request_location_enabled))
        .bind(to_i64(update.responses_max_decompressed_body_bytes)?)
        .bind(profiles)
        .bind(bool_i64(update.account_warmup_enabled))
        .bind(&update.account_warmup_schedule_time)
        .bind(update.account_warmup_model.as_deref())
        .bind(smart_scheduling)
        .bind(i64::from(update.openai_guardian_reserved_concurrency))
        .bind(update.openai_account_affinity.as_str())
        .bind(i64::from(update.max_account_rotations))
        .bind(i64::from(update.openai_session_affinity_ttl_hours))
        .bind(now)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("update SQLite runtime settings"))?
        .ok_or_else(|| StoreError::InvalidData {
            entity: "runtime settings",
            message: "config revision cannot be advanced".to_owned(),
            source: None,
        })?;
        if let Some(mut audit) = audit {
            audit.config_revision = Some(next_revision);
            crate::sqlite::append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
        }
        let settings = load_runtime_settings_in_transaction(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite runtime settings update"))?;
        Ok(settings)
    }

    pub(crate) async fn replace_runtime_settings(
        &self,
        expected_revision: crate::Revision,
        update: RuntimeSettingsUpdate,
        audit: crate::AdminAuditEvent,
    ) -> StoreResult<RuntimeSettings> {
        self.apply_settings_update(update, Some(expected_revision), Some(audit))
            .await
    }
}

#[async_trait]
impl RuntimeSettingsRepository for SqliteRuntimeSettingsRepository {
    async fn load_runtime_settings(&self) -> StoreResult<RuntimeSettings> {
        let row = sqlx::query(
            "select provider_request_profiles_json, config_revision, admin_api_key,
                    refresh_margin_seconds, refresh_concurrency, max_concurrent_per_account,
                    request_interval_ms, smart_scheduling_json, rotation_strategy,
                    request_location_enabled, request_location_json, model_mappings_json,
                    usage_retention_days, ops_event_retention_days, audit_retention_days,
                    min_codex_desktop_version, min_codex_cli_version, updated_at_us,
                    max_waiting_per_key, max_waiting_per_account,
                    concurrency_wait_timeout_seconds, openai_guardian_reserved_concurrency,
                    openai_account_affinity, max_account_rotations,
                    openai_session_affinity_ttl_hours,
                    responses_max_decompressed_body_bytes, account_auto_freeze_enabled,
                    account_auto_freeze_threshold, account_auto_freeze_window_seconds,
                    account_auto_freeze_duration_seconds, account_auto_freeze_probe_enabled,
                    account_auto_freeze_probe_model, account_auto_freeze_adaptive_concurrency,
                    account_warmup_enabled, account_warmup_schedule_time, account_warmup_model
             from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite runtime settings"))?
        .ok_or_else(|| StoreError::NotFound {
            entity: "runtime settings",
            id: "1".to_owned(),
            source: None,
        })?;
        settings_from_row(&row)
    }

    async fn update_runtime_settings(
        &self,
        update: RuntimeSettingsUpdate,
    ) -> StoreResult<crate::Revision> {
        let settings = self.apply_settings_update(update, None, None).await?;
        Ok(settings.config_revision)
    }
}

impl ProviderRuntimePolicyPort for SqliteRuntimeSettingsRepository {
    fn claim_warmup_slot<'a>(
        &'a self,
        timezone: gateway_core::time::DeploymentTimeZone,
        slot: chrono::NaiveDateTime,
    ) -> futures::future::BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let slot = timezone.resolve_local(slot).ok_or_else(|| {
                provider_error(ProviderStoreErrorKind::InvalidData, "resolve warmup slot")
            })?;
            let mut transaction = self.pool.begin().await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "begin warmup slot claim",
                )
            })?;
            acquire_write_lock(&mut transaction).await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "lock warmup slot claim",
                )
            })?;
            let claimed = sqlx::query(
                "update runtime_settings set account_warmup_cursor_us = ?1
                 where id = 1 and (account_warmup_cursor_us is null or account_warmup_cursor_us < ?1)",
            )
            .bind(slot.timestamp_micros())
            .execute(&mut *transaction)
            .await
            .map_err(|_| provider_error(ProviderStoreErrorKind::Unavailable, "claim warmup slot"))?
            .rows_affected()
                == 1;
            transaction.commit().await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "commit warmup slot claim",
                )
            })?;
            Ok(claimed)
        })
    }

    fn initialize_request_profile<'a>(
        &'a self,
        provider: &'a ProviderKind,
        initial: OpaqueProviderData,
    ) -> futures::future::BoxFuture<'a, Result<OpaqueProviderData, ProviderStoreError>> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "begin request profile initialization",
                )
            })?;
            acquire_write_lock(&mut transaction).await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "lock request profile initialization",
                )
            })?;
            let current = sqlx::query_scalar::<_, String>(
                "select provider_request_profiles_json from runtime_settings where id = 1",
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| {
                provider_error(ProviderStoreErrorKind::Unavailable, "load request profile")
            })?
            .ok_or_else(|| {
                provider_error(ProviderStoreErrorKind::Unavailable, "load runtime settings")
            })?;
            let mut profiles = decode_profiles(&current).map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::InvalidData,
                    "decode request profile",
                )
            })?;
            let changed = !profiles.contains_key(provider.as_str());
            let profile = profiles
                .entry(provider.as_str().to_owned())
                .or_insert_with(|| initial.expose_to_provider().clone())
                .clone();
            let value = OpaqueProviderData::new(profile);
            if changed {
                let encoded = encode_json(&profiles).map_err(|_| {
                    provider_error(
                        ProviderStoreErrorKind::InvalidData,
                        "encode request profile",
                    )
                })?;
                let updated = sqlx::query(
                    "update runtime_settings set provider_request_profiles_json = ?1,
                         config_revision = config_revision + 1
                     where id = 1 and config_revision < 9223372036854775807",
                )
                .bind(encoded)
                .execute(&mut *transaction)
                .await
                .map_err(|_| {
                    provider_error(
                        ProviderStoreErrorKind::Unavailable,
                        "persist request profile",
                    )
                })?
                .rows_affected();
                if updated != 1 {
                    return Err(provider_error(
                        ProviderStoreErrorKind::InvalidData,
                        "config revision cannot be advanced",
                    ));
                }
            }
            transaction.commit().await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "commit request profile",
                )
            })?;
            Ok(value)
        })
    }

    fn load_request_profile_configurations<'a>(
        &'a self,
        revision: ConfigRevision,
        provider: &'a ProviderKind,
    ) -> futures::future::BoxFuture<'a, Result<Vec<OpaqueProviderData>, ProviderStoreError>> {
        Box::pin(async move {
            let mut transaction = self.pool.begin().await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "begin request profile read",
                )
            })?;
            let settings = sqlx::query(
                "select config_revision, provider_request_profiles_json
                 from runtime_settings where id = 1",
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "load request profile revision",
                )
            })?
            .ok_or_else(|| {
                provider_error(ProviderStoreErrorKind::Unavailable, "load runtime settings")
            })?;
            let actual_revision = settings.try_get::<i64, _>("config_revision").map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::InvalidData,
                    "decode request profile revision",
                )
            })?;
            if nonnegative(actual_revision).ok() != Some(revision.get()) {
                return Err(provider_error(
                    ProviderStoreErrorKind::Conflict,
                    "request profile revision changed",
                ));
            }
            let mut profiles = Vec::new();
            let runtime = settings
                .try_get::<String, _>("provider_request_profiles_json")
                .map_err(|_| {
                    provider_error(
                        ProviderStoreErrorKind::InvalidData,
                        "decode runtime request profiles",
                    )
                })?;
            if let Some(profile) = decode_profiles(&runtime)
                .map_err(|_| {
                    provider_error(
                        ProviderStoreErrorKind::InvalidData,
                        "decode runtime request profiles",
                    )
                })?
                .remove(provider.as_str())
            {
                profiles.push(OpaqueProviderData::new(profile));
            }
            let key_rows =
                sqlx::query("select provider_request_profiles_json from client_api_keys")
                    .fetch_all(&mut *transaction)
                    .await
                    .map_err(|_| {
                        provider_error(
                            ProviderStoreErrorKind::Unavailable,
                            "load key request profiles",
                        )
                    })?;
            for row in key_rows {
                let encoded = row
                    .try_get::<String, _>("provider_request_profiles_json")
                    .map_err(|_| {
                        provider_error(
                            ProviderStoreErrorKind::InvalidData,
                            "decode key request profiles",
                        )
                    })?;
                if let Some(profile) = decode_profiles(&encoded)
                    .map_err(|_| {
                        provider_error(
                            ProviderStoreErrorKind::InvalidData,
                            "decode key request profiles",
                        )
                    })?
                    .remove(provider.as_str())
                {
                    profiles.push(OpaqueProviderData::new(profile));
                }
            }
            transaction.commit().await.map_err(|_| {
                provider_error(
                    ProviderStoreErrorKind::Unavailable,
                    "commit request profile read",
                )
            })?;
            Ok(profiles)
        })
    }

    fn load_refresh_policy(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<ProviderRefreshPolicy, ProviderStoreError>> {
        Box::pin(async move {
            let settings = self.load_runtime_settings().await.map_err(|_| {
                provider_error(ProviderStoreErrorKind::Unavailable, "load refresh policy")
            })?;
            let concurrency = NonZeroU32::new(settings.refresh_concurrency).ok_or_else(|| {
                provider_error(ProviderStoreErrorKind::InvalidData, "decode refresh policy")
            })?;
            ProviderRefreshPolicy::try_new(
                Duration::from_secs(settings.refresh_margin_seconds),
                concurrency,
            )
        })
    }

    fn load_freeze_policy(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<ProviderFreezePolicy, ProviderStoreError>> {
        Box::pin(async move {
            let settings = self.load_runtime_settings().await.map_err(|_| {
                provider_error(ProviderStoreErrorKind::Unavailable, "load freeze policy")
            })?;
            ProviderFreezePolicy::try_new(
                settings.account_auto_freeze_enabled,
                settings.account_auto_freeze_threshold,
                settings.account_auto_freeze_window_seconds,
                settings.account_auto_freeze_duration_seconds,
                settings.account_auto_freeze_probe_enabled,
                settings.account_auto_freeze_probe_model,
                settings.account_auto_freeze_adaptive_concurrency,
            )
        })
    }

    fn load_warmup_policy(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<ProviderWarmupPolicy, ProviderStoreError>> {
        Box::pin(async move {
            let settings = self.load_runtime_settings().await.map_err(|_| {
                provider_error(ProviderStoreErrorKind::Unavailable, "load warmup policy")
            })?;
            ProviderWarmupPolicy::try_new(
                settings.account_warmup_enabled,
                settings.account_warmup_schedule_time,
                settings.account_warmup_model,
            )
        })
    }
}

fn settings_from_row(row: &sqlx::sqlite::SqliteRow) -> StoreResult<RuntimeSettings> {
    let profiles = decode_profiles(&read_string(row, "provider_request_profiles_json")?)?;
    let request_profiles = profiles
        .into_iter()
        .map(|(provider, profile)| {
            Ok((
                ProviderKind::new(provider)
                    .map_err(|_| invalid("request profile provider is invalid"))?,
                OpaqueProviderData::new(profile),
            ))
        })
        .collect::<StoreResult<BTreeMap<_, _>>>()?;
    let request_location: gateway_core::account::RequestLocation =
        decode_json(row, "request_location_json")?;
    Ok(RuntimeSettings {
        request_profiles,
        config_revision: crate::Revision::new(nonnegative(read_i64(row, "config_revision")?)?)?,
        admin_api_key: read_optional_string(row, "admin_api_key")?,
        refresh_margin_seconds: nonnegative(read_i64(row, "refresh_margin_seconds")?)?,
        refresh_concurrency: to_u32(read_i64(row, "refresh_concurrency")?)?,
        max_concurrent_per_account: to_u32(read_i64(row, "max_concurrent_per_account")?)?,
        request_interval_ms: nonnegative(read_i64(row, "request_interval_ms")?)?,
        smart_scheduling: decode_json(row, "smart_scheduling_json")?,
        rotation_strategy: read_string(row, "rotation_strategy")?,
        request_location_enabled: read_i64(row, "request_location_enabled")? != 0,
        request_location: request_location
            .normalized()
            .map_err(|_| invalid("request location is invalid"))?,
        model_mappings: decode_json(row, "model_mappings_json")?,
        usage_retention_days: to_u32(read_i64(row, "usage_retention_days")?)?,
        ops_event_retention_days: to_u32(read_i64(row, "ops_event_retention_days")?)?,
        audit_retention_days: to_u32(read_i64(row, "audit_retention_days")?)?,
        min_codex_desktop_version: read_optional_string(row, "min_codex_desktop_version")?,
        min_codex_cli_version: read_optional_string(row, "min_codex_cli_version")?,
        max_waiting_per_key: to_u32(read_i64(row, "max_waiting_per_key")?)?,
        max_waiting_per_account: to_u32(read_i64(row, "max_waiting_per_account")?)?,
        concurrency_wait_timeout_seconds: to_u32(read_i64(
            row,
            "concurrency_wait_timeout_seconds",
        )?)?,
        openai_guardian_reserved_concurrency: to_u32(read_i64(
            row,
            "openai_guardian_reserved_concurrency",
        )?)?,
        openai_account_affinity: gateway_core::account::AccountAffinity::parse(&read_string(
            row,
            "openai_account_affinity",
        )?)
        .ok_or_else(|| invalid("persisted account affinity is invalid"))?,
        max_account_rotations: to_u32(read_i64(row, "max_account_rotations")?)?,
        openai_session_affinity_ttl_hours: to_u32(read_i64(
            row,
            "openai_session_affinity_ttl_hours",
        )?)?,
        responses_max_decompressed_body_bytes: nonnegative(read_i64(
            row,
            "responses_max_decompressed_body_bytes",
        )?)?,
        account_auto_freeze_enabled: read_i64(row, "account_auto_freeze_enabled")? != 0,
        account_auto_freeze_threshold: to_u32(read_i64(row, "account_auto_freeze_threshold")?)?,
        account_auto_freeze_window_seconds: nonnegative(read_i64(
            row,
            "account_auto_freeze_window_seconds",
        )?)?,
        account_auto_freeze_duration_seconds: nonnegative(read_i64(
            row,
            "account_auto_freeze_duration_seconds",
        )?)?,
        account_auto_freeze_probe_enabled: read_i64(row, "account_auto_freeze_probe_enabled")? != 0,
        account_auto_freeze_probe_model: read_optional_string(
            row,
            "account_auto_freeze_probe_model",
        )?,
        account_auto_freeze_adaptive_concurrency: read_i64(
            row,
            "account_auto_freeze_adaptive_concurrency",
        )? != 0,
        account_warmup_enabled: read_i64(row, "account_warmup_enabled")? != 0,
        account_warmup_schedule_time: read_string(row, "account_warmup_schedule_time")?,
        account_warmup_model: read_optional_string(row, "account_warmup_model")?,
        updated_at: datetime_from_micros(read_i64(row, "updated_at_us")?)?,
    })
}

fn decode_profiles(
    value: &str,
) -> StoreResult<BTreeMap<String, serde_json::Map<String, serde_json::Value>>> {
    serde_json::from_str(value).map_err(|_| invalid("persisted request profiles are invalid"))
}

fn decode_json<T: serde::de::DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> StoreResult<T> {
    let value = read_string(row, field)?;
    serde_json::from_str(&value).map_err(|_| invalid("persisted JSON settings are invalid"))
}

fn encode_json(value: &impl serde::Serialize) -> StoreResult<String> {
    serde_json::to_string(value).map_err(|_| invalid("encode runtime setting"))
}

fn read_i64(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<i64> {
    row.try_get(field)
        .map_err(|_| invalid("persisted integer setting is invalid"))
}

fn read_string(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<String> {
    row.try_get(field)
        .map_err(|_| invalid("persisted text setting is invalid"))
}

fn read_optional_string(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> StoreResult<Option<String>> {
    row.try_get(field)
        .map_err(|_| invalid("persisted nullable text setting is invalid"))
}

fn nonnegative(value: i64) -> StoreResult<u64> {
    u64::try_from(value).map_err(|_| invalid("numeric field is negative"))
}

fn to_u32(value: i64) -> StoreResult<u32> {
    u32::try_from(value).map_err(|_| invalid("numeric setting is outside u32"))
}

fn to_i64(value: u64) -> StoreResult<i64> {
    i64::try_from(value).map_err(|_| invalid("numeric setting exceeds SQLite range"))
}

fn bool_i64(value: bool) -> i64 {
    i64::from(value)
}

fn datetime_from_micros(value: i64) -> StoreResult<DateTime<Utc>> {
    DateTime::from_timestamp_micros(value).ok_or_else(|| invalid("timestamp is outside UTC range"))
}

fn provider_error(kind: ProviderStoreErrorKind, operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(kind, operation)
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        source: None,
        entity: "runtime settings",
        message: message.to_owned(),
    }
}

async fn load_runtime_settings_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> StoreResult<RuntimeSettings> {
    let row = sqlx::query(
        "select provider_request_profiles_json, config_revision, admin_api_key,
                refresh_margin_seconds, refresh_concurrency, max_concurrent_per_account,
                request_interval_ms, smart_scheduling_json, rotation_strategy,
                request_location_enabled, request_location_json, model_mappings_json,
                usage_retention_days, ops_event_retention_days, audit_retention_days,
                min_codex_desktop_version, min_codex_cli_version, updated_at_us,
                max_waiting_per_key, max_waiting_per_account,
                concurrency_wait_timeout_seconds, openai_guardian_reserved_concurrency,
                openai_account_affinity, max_account_rotations,
                openai_session_affinity_ttl_hours,
                responses_max_decompressed_body_bytes, account_auto_freeze_enabled,
                account_auto_freeze_threshold, account_auto_freeze_window_seconds,
                account_auto_freeze_duration_seconds, account_auto_freeze_probe_enabled,
                account_auto_freeze_probe_model, account_auto_freeze_adaptive_concurrency,
                account_warmup_enabled, account_warmup_schedule_time, account_warmup_model
         from runtime_settings where id = 1",
    )
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| sqlite_unavailable("load SQLite runtime settings in transaction"))?
    .ok_or_else(|| StoreError::NotFound {
        entity: "runtime settings",
        id: "1".to_owned(),
        source: None,
    })?;
    settings_from_row(&row)
}
