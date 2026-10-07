//! SQLite 运行配置快照读取与 Core 快照端口实现。

use std::collections::BTreeMap;

use async_trait::async_trait;
use gateway_core::routing::{
    ConfigRevision,
    snapshot::{SnapshotFacts, SnapshotStoreError, SnapshotStorePort},
};
use serde::de::DeserializeOwned;
use sqlx::{Row, SqlitePool};

use crate::{
    Revision, StoreError, StoreResult,
    runtime_snapshot::{
        ClientApiKeySnapshot, RuntimeSnapshotData, RuntimeSnapshotRepository,
        SnapshotAccountGroupData, SnapshotGroupMembershipData, SnapshotProviderAccountData,
        SnapshotRuntimeSettings, core_revision, decode_request_profiles, revision_from_i64,
        snapshot_data_into_facts, to_u32, to_u64,
    },
};

use super::sqlite_unavailable;

#[derive(Clone)]
pub struct SqliteRuntimeSnapshotRepository {
    pool: SqlitePool,
}

impl SqliteRuntimeSnapshotRepository {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RuntimeSnapshotRepository for SqliteRuntimeSnapshotRepository {
    async fn load_runtime_snapshot(&self) -> StoreResult<RuntimeSnapshotData> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite runtime snapshot"))?;
        let settings_row = sqlx::query(
            "select config_revision, refresh_margin_seconds, refresh_concurrency,
                    max_concurrent_per_account, request_interval_ms, rotation_strategy,
                    smart_scheduling_json, model_mappings_json, min_codex_desktop_version,
                    min_codex_cli_version, max_waiting_per_key, max_waiting_per_account,
                    concurrency_wait_timeout_seconds, openai_guardian_reserved_concurrency,
                    openai_account_affinity, max_account_rotations,
                    openai_session_affinity_ttl_hours,
                    request_location_json, request_location_enabled,
                    responses_max_decompressed_body_bytes, provider_request_profiles_json,
                    pricing_overrides_json, pricing_synced_json
             from runtime_settings where id = 1",
        )
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite snapshot settings"))?
        .ok_or_else(|| StoreError::NotFound {
            entity: "runtime settings",
            id: "1".to_owned(),
            source: None,
        })?;

        let revision = revision_from_i64(read_i64(&settings_row, "config_revision")?)?;
        let smart_scheduling = decode_json(&settings_row, "smart_scheduling_json")?;
        let model_mappings = decode_json(&settings_row, "model_mappings_json")?;
        let request_location: gateway_core::account::RequestLocation =
            decode_json(&settings_row, "request_location_json")?;
        request_location
            .validate()
            .map_err(|_| invalid("request location is invalid"))?;
        let request_profiles_json = decode_json::<
            BTreeMap<String, serde_json::Map<String, serde_json::Value>>,
        >(&settings_row, "provider_request_profiles_json")?;
        let request_profiles = decode_request_profiles(request_profiles_json)?;
        let pricing_overrides = decode_json(&settings_row, "pricing_overrides_json")?;
        let pricing_synced = decode_json(&settings_row, "pricing_synced_json")?;
        crate::pricing_validation::validate_pricing(&pricing_overrides)?;
        crate::pricing_validation::validate_pricing(&pricing_synced)?;
        let settings = SnapshotRuntimeSettings {
            pricing: gateway_core::metering::merge_pricing(pricing_synced, &pricing_overrides),
            request_profiles,
            request_location_enabled: read_i64(&settings_row, "request_location_enabled")? != 0,
            request_location,
            refresh_margin_seconds: to_u64(read_i64(&settings_row, "refresh_margin_seconds")?)?,
            refresh_concurrency: to_u32(read_i64(&settings_row, "refresh_concurrency")?)?,
            max_concurrent_per_account: to_u32(read_i64(
                &settings_row,
                "max_concurrent_per_account",
            )?)?,
            request_interval_ms: to_u64(read_i64(&settings_row, "request_interval_ms")?)?,
            max_waiting_per_key: to_u32(read_i64(&settings_row, "max_waiting_per_key")?)?,
            max_waiting_per_account: to_u32(read_i64(&settings_row, "max_waiting_per_account")?)?,
            concurrency_wait_timeout_seconds: to_u32(read_i64(
                &settings_row,
                "concurrency_wait_timeout_seconds",
            )?)?,
            openai_guardian_reserved_concurrency: to_u32(read_i64(
                &settings_row,
                "openai_guardian_reserved_concurrency",
            )?)?,
            openai_account_affinity: gateway_core::account::AccountAffinity::parse(&read_string(
                &settings_row,
                "openai_account_affinity",
            )?)
            .ok_or_else(|| invalid("persisted account affinity is invalid"))?,
            max_account_rotations: to_u32(read_i64(&settings_row, "max_account_rotations")?)?,
            openai_session_affinity_ttl_hours: to_u32(read_i64(
                &settings_row,
                "openai_session_affinity_ttl_hours",
            )?)?,
            responses_max_decompressed_body_bytes: to_u64(read_i64(
                &settings_row,
                "responses_max_decompressed_body_bytes",
            )?)?,
            smart_scheduling,
            rotation_strategy: read_string(&settings_row, "rotation_strategy")?,
            model_mappings,
            min_codex_desktop_version: read_optional_string(
                &settings_row,
                "min_codex_desktop_version",
            )?,
            min_codex_cli_version: read_optional_string(&settings_row, "min_codex_cli_version")?,
        };

        let keys = sqlx::query(
            "select id, key, max_concurrency, requests_per_minute, provider_request_profiles_json
             from client_api_keys where enabled = 1 order by id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite snapshot client policies"))?;
        let key_groups = sqlx::query(
            "select groups.client_api_key_id, groups.account_group_id
             from client_api_key_groups groups
             join client_api_keys keys on keys.id = groups.client_api_key_id and keys.enabled = 1
             order by groups.client_api_key_id, groups.account_group_id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite snapshot key groups"))?;
        let mut groups_by_key = BTreeMap::<String, Vec<String>>::new();
        for row in key_groups {
            groups_by_key
                .entry(read_string(&row, "client_api_key_id")?)
                .or_default()
                .push(read_string(&row, "account_group_id")?);
        }
        let client_api_keys = keys
            .into_iter()
            .map(|row| {
                let id = read_string(&row, "id")?;
                let mut key = ClientApiKeySnapshot::from_persisted(
                    id.clone(),
                    read_string(&row, "key")?,
                    groups_by_key.remove(&id).unwrap_or_default(),
                    read_i64(&row, "max_concurrency")?,
                    read_i64(&row, "requests_per_minute")?,
                )?;
                let profiles = decode_json::<
                    BTreeMap<String, serde_json::Map<String, serde_json::Value>>,
                >(&row, "provider_request_profiles_json")?;
                key.request_profiles = decode_request_profiles(profiles)?;
                Ok(key)
            })
            .collect::<StoreResult<Vec<_>>>()?;

        let group_rows =
            sqlx::query("select id, name, enabled, fast_mode from account_groups order by id")
                .fetch_all(&mut *transaction)
                .await
                .map_err(|_| sqlite_unavailable("load SQLite snapshot account groups"))?;
        let account_groups = group_rows
            .into_iter()
            .map(|row| {
                let fast_mode = read_string(&row, "fast_mode")?;
                Ok(SnapshotAccountGroupData {
                    id: gateway_core::routing::AccountGroupId::new(read_string(&row, "id")?)
                        .map_err(|_| invalid("persisted account group ID is invalid"))?,
                    name: read_string(&row, "name")?,
                    enabled: read_i64(&row, "enabled")? != 0,
                    fast_mode: gateway_core::account::FastMode::parse(&fast_mode)
                        .ok_or_else(|| invalid("persisted account group fast mode is invalid"))?,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;

        let account_rows = sqlx::query(
            "select id, provider_kind, model_access_json from provider_accounts order by id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite snapshot provider accounts"))?;
        let provider_accounts = account_rows
            .into_iter()
            .map(|row| {
                Ok(SnapshotProviderAccountData {
                    id: read_string(&row, "id")?,
                    provider_kind: read_string(&row, "provider_kind")?,
                    model_access: decode_json(&row, "model_access_json")?,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;

        let membership_rows = sqlx::query(
            "select account_group_id, provider_account_id
             from account_group_accounts order by account_group_id, provider_account_id",
        )
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite snapshot group memberships"))?;
        let group_memberships = membership_rows
            .into_iter()
            .map(|row| {
                Ok(SnapshotGroupMembershipData {
                    group_id: gateway_core::routing::AccountGroupId::new(read_string(
                        &row,
                        "account_group_id",
                    )?)
                    .map_err(|_| invalid("persisted membership group ID is invalid"))?,
                    account_id: read_string(&row, "provider_account_id")?,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;

        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite runtime snapshot"))?;
        let observed_current_revision =
            RuntimeSnapshotRepository::current_config_revision(self).await?;
        Ok(RuntimeSnapshotData {
            config_revision: revision,
            observed_current_revision,
            settings,
            client_api_keys,
            account_groups,
            provider_accounts,
            group_memberships,
        })
    }

    async fn current_config_revision(&self) -> StoreResult<Revision> {
        let revision = sqlx::query_scalar::<_, i64>(
            "select config_revision from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| sqlite_unavailable("read SQLite config revision"))?
        .ok_or_else(|| StoreError::NotFound {
            entity: "runtime settings",
            id: "1".to_owned(),
            source: None,
        })?;
        revision_from_i64(revision)
    }
}

impl SnapshotStorePort for SqliteRuntimeSnapshotRepository {
    fn load_snapshot_facts(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<SnapshotFacts, SnapshotStoreError>> {
        Box::pin(async move {
            let data = self
                .load_runtime_snapshot()
                .await
                .map_err(|_| SnapshotStoreError::unavailable())?;
            snapshot_data_into_facts(data)
        })
    }

    fn current_config_revision(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<ConfigRevision, SnapshotStoreError>> {
        Box::pin(async move {
            RuntimeSnapshotRepository::current_config_revision(self)
                .await
                .map_err(|_| SnapshotStoreError::unavailable())
                .and_then(core_revision)
        })
    }
}

fn decode_json<T: DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> StoreResult<T> {
    let value = read_string(row, field)?;
    serde_json::from_str(&value).map_err(|_| invalid("persisted JSON value is invalid"))
}

fn read_i64(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<i64> {
    row.try_get(field)
        .map_err(|_| invalid("persisted integer field is invalid"))
}

fn read_string(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> StoreResult<String> {
    row.try_get(field)
        .map_err(|_| invalid("persisted text field is invalid"))
}

fn read_optional_string(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> StoreResult<Option<String>> {
    row.try_get(field)
        .map_err(|_| invalid("persisted nullable text field is invalid"))
}

fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        entity: "SQLite runtime snapshot",
        message: message.to_owned(),
        source: None,
    }
}
