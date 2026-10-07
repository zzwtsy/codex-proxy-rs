//! SQLite 管理设置、定价与配置 revision 仓储。

use chrono::{DateTime, Utc};
use gateway_admin::model::pricing::{
    PricingChange, PricingSyncChanges, StoredPricing, UpdatePricing,
};
use gateway_core::metering::{ModelPriceOverride, PricingOverrides};
use sqlx::{Row, SqlitePool};

use crate::{
    AdminAuditEvent, Revision, StoreError, StoreResult,
    pricing_validation::validate_pricing,
    runtime_settings::{RuntimeSettings, RuntimeSettingsRepository, RuntimeSettingsUpdate},
};

use super::runtime_settings::SqliteRuntimeSettingsRepository;
use super::{
    acquire_write_lock, append_admin_audit_event_in_transaction, bump_config_revision,
    sqlite_unavailable,
};

#[derive(Clone)]
pub struct SqliteAdminSettingsRepository {
    pool: SqlitePool,
    runtime_settings: SqliteRuntimeSettingsRepository,
}

impl SqliteAdminSettingsRepository {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            runtime_settings: SqliteRuntimeSettingsRepository::new(pool.clone()),
            pool,
        }
    }

    pub(crate) async fn load_runtime_settings(&self) -> StoreResult<RuntimeSettings> {
        self.runtime_settings.load_runtime_settings().await
    }

    pub(crate) async fn replace_runtime_settings(
        &self,
        expected_revision: Revision,
        update: RuntimeSettingsUpdate,
        audit: AdminAuditEvent,
    ) -> StoreResult<RuntimeSettings> {
        self.runtime_settings
            .replace_runtime_settings(expected_revision, update, audit)
            .await
    }

    pub(crate) async fn load_pricing(&self) -> StoreResult<StoredPricing> {
        let row = sqlx::query(
            "select pricing_overrides_json, pricing_synced_json, pricing_synced_at_us
             from runtime_settings where id = 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| sqlite_unavailable("load SQLite model pricing"))?
        .ok_or_else(|| StoreError::NotFound {
            entity: "runtime settings",
            id: "1".to_owned(),
            source: None,
        })?;
        let overrides: PricingOverrides = decode_json(&row, "pricing_overrides_json")?;
        let synced: PricingOverrides = decode_json(&row, "pricing_synced_json")?;
        validate_pricing(&overrides)?;
        validate_pricing(&synced)?;
        let synced_at_us: Option<i64> = row
            .try_get("pricing_synced_at_us")
            .map_err(|_| invalid_pricing())?;
        let synced_at = match synced_at_us {
            Some(value) => {
                Some(DateTime::<Utc>::from_timestamp_micros(value).ok_or_else(invalid_pricing)?)
            }
            None => None,
        };
        Ok(StoredPricing {
            overrides,
            synced,
            synced_at,
        })
    }

    pub(crate) async fn sync_pricing(
        &self,
        changes: PricingSyncChanges,
        mut audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite model pricing sync"))?;
        acquire_write_lock(&mut transaction).await?;
        let mut prices: PricingOverrides =
            sqlx::query_scalar("select pricing_synced_json from runtime_settings where id = 1")
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| sqlite_unavailable("load SQLite synced model pricing"))?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "runtime settings",
                    id: "1".to_owned(),
                    source: None,
                })
                .and_then(|encoded: String| {
                    serde_json::from_str(&encoded).map_err(|_| invalid_pricing())
                })?;
        for (provider, models) in changes {
            let stored = prices.entry(provider).or_default();
            for (model, price) in models {
                if let Some(price) = price {
                    stored.insert(model, price);
                } else {
                    stored.remove(&model);
                }
            }
        }
        prices.retain(|_, models| !models.is_empty());
        validate_pricing(&prices)?;
        let encoded = serde_json::to_string(&prices).map_err(|_| invalid_pricing())?;
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "update runtime_settings
             set pricing_synced_json = ?1, pricing_synced_at_us = ?2
             where id = 1",
        )
        .bind(encoded)
        .bind(now)
        .execute(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("sync SQLite model pricing"))?;
        let revision = bump_config_revision(&mut transaction, now).await?;
        audit.config_revision = Some(revision_to_i64(revision)?);
        append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite model pricing sync"))?;
        Ok(revision)
    }

    pub(crate) async fn update_pricing(
        &self,
        command: UpdatePricing,
        mut audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite model pricing update"))?;
        acquire_write_lock(&mut transaction).await?;
        let (mut pricing, mut synced): (PricingOverrides, PricingOverrides) = {
            let row = sqlx::query(
                "select pricing_overrides_json, pricing_synced_json
                 from runtime_settings where id = 1",
            )
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("load SQLite model pricing for update"))?
            .ok_or_else(|| StoreError::NotFound {
                entity: "runtime settings",
                id: "1".to_owned(),
                source: None,
            })?;
            (
                decode_json(&row, "pricing_overrides_json")?,
                decode_json(&row, "pricing_synced_json")?,
            )
        };
        let models = pricing.entry(command.provider.clone()).or_default();
        let source = synced.entry(command.provider).or_default();
        for model in command.models {
            match &command.change {
                PricingChange::Reset => {
                    models.remove(&model);
                }
                PricingChange::Delete => {
                    models.remove(&model);
                    source.remove(&model);
                }
                PricingChange::Replace(value) => {
                    models.insert(model, value.clone());
                }
                PricingChange::Multiplier(bps) => {
                    models
                        .entry(model)
                        .or_insert_with(|| ModelPriceOverride {
                            multiplier_bps: 10_000,
                            bands: Default::default(),
                        })
                        .multiplier_bps = *bps;
                }
            }
        }
        pricing.retain(|_, entries| !entries.is_empty());
        synced.retain(|_, entries| !entries.is_empty());
        validate_pricing(&pricing)?;
        validate_pricing(&synced)?;
        let pricing = serde_json::to_string(&pricing).map_err(|_| invalid_pricing())?;
        let synced = serde_json::to_string(&synced).map_err(|_| invalid_pricing())?;
        let now = Utc::now().timestamp_micros();
        sqlx::query(
            "update runtime_settings set
               pricing_overrides_json = ?1, pricing_synced_json = ?2
             where id = 1",
        )
        .bind(pricing)
        .bind(synced)
        .execute(&mut *transaction)
        .await
        .map_err(|_| sqlite_unavailable("update SQLite model pricing"))?;
        let revision = bump_config_revision(&mut transaction, now).await?;
        audit.config_revision = Some(revision_to_i64(revision)?);
        append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite model pricing update"))?;
        Ok(revision)
    }

    pub(crate) async fn replace_admin_api_key(
        &self,
        admin_api_key: Option<String>,
        mut audit: AdminAuditEvent,
    ) -> StoreResult<Revision> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| sqlite_unavailable("begin SQLite admin API key update"))?;
        acquire_write_lock(&mut transaction).await?;
        let now = Utc::now().timestamp_micros();
        let changed = sqlx::query("update runtime_settings set admin_api_key = ?1 where id = 1")
            .bind(admin_api_key)
            .execute(&mut *transaction)
            .await
            .map_err(|_| sqlite_unavailable("update SQLite admin API key"))?
            .rows_affected()
            == 1;
        if !changed {
            return Err(StoreError::NotFound {
                entity: "runtime settings",
                id: "1".to_owned(),
                source: None,
            });
        }
        let revision = bump_config_revision(&mut transaction, now).await?;
        audit.config_revision = Some(revision_to_i64(revision)?);
        append_admin_audit_event_in_transaction(&mut transaction, audit).await?;
        transaction
            .commit()
            .await
            .map_err(|_| sqlite_unavailable("commit SQLite admin API key update"))?;
        Ok(revision)
    }
}

fn decode_json<T: serde::de::DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    column: &'static str,
) -> StoreResult<T> {
    let encoded: String = row.try_get(column).map_err(|_| invalid_pricing())?;
    serde_json::from_str(&encoded).map_err(|_| invalid_pricing())
}

fn revision_to_i64(revision: Revision) -> StoreResult<i64> {
    i64::try_from(revision.get()).map_err(|_| StoreError::InvalidData {
        entity: "config revision",
        message: "revision exceeds SQLite INTEGER range".to_owned(),
        source: None,
    })
}

fn invalid_pricing() -> StoreError {
    StoreError::InvalidData {
        entity: "model pricing",
        message: "persisted model pricing is invalid".to_owned(),
        source: None,
    }
}
