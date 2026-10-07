//! SQLite 客户端 Key 预算适配；所有金额运算都在 Rust 定点类型中完成。

use std::{collections::BTreeMap, sync::Mutex, time::Duration};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use gateway_core::{
    engine::budget::{ClientBudgetCharge, ClientBudgetError, ClientBudgetLimits, ClientBudgetPort},
    error::{GatewayError, GatewayErrorKind},
    metering::Decimal,
    policy::ClientApiKeyId,
};
use sqlx::{Row, SqlitePool, Transaction};

use super::{acquire_write_lock, value::datetime_to_micros};
use crate::sqlite::value::{checked_amount_sum, decode_amount, encode_amount};

pub struct SqliteClientBudgetStore {
    timezone: gateway_core::time::DeploymentTimeZone,
    pool: SqlitePool,
    retry: Mutex<BTreeMap<String, ClientBudgetCharge>>,
}

impl SqliteClientBudgetStore {
    #[must_use]
    pub fn new(pool: SqlitePool, timezone: gateway_core::time::DeploymentTimeZone) -> Self {
        Self {
            timezone,
            pool,
            retry: Mutex::new(BTreeMap::new()),
        }
    }

    async fn admit_inner(&self, key_id: ClientApiKeyId) -> Result<(), GatewayError> {
        let retries = self
            .retry
            .lock()
            .map_err(|_| unavailable())?
            .values()
            .filter(|charge| charge.key_id == key_id)
            .cloned()
            .collect::<Vec<_>>();
        for charge in retries {
            self.settle(charge).await.map_err(|_| unavailable())?;
        }

        let mut transaction = self.pool.begin().await.map_err(|_| unavailable())?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|_| unavailable())?;
        let row = sqlx::query(
            "select daily_limit_usd, weekly_limit_usd, enabled
             from client_api_keys where id = ?1",
        )
        .bind(key_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::Unauthorized,
                "client API key no longer exists",
            )
        })?;
        if row
            .try_get::<i64, _>("enabled")
            .map_err(|_| unavailable())?
            == 0
        {
            return Err(GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "client API key is disabled",
            ));
        }
        let limits = ClientBudgetLimits {
            daily_usd: read_amount(&row, "daily_limit_usd")?,
            weekly_usd: read_amount(&row, "weekly_limit_usd")?,
        };
        let now = Utc::now();
        let now_us = now.timestamp_micros();
        let window = advance_windows(&mut transaction, key_id.as_str(), now, now, self.timezone)
            .await
            .map_err(|_| unavailable())?;
        if limits.is_limited() {
            let daily = decode_amount(&window.daily_used).map_err(|_| unavailable())?;
            let weekly = decode_amount(&window.weekly_used).map_err(|_| unavailable())?;
            let daily_exceeded = limits.daily_usd != Decimal::ZERO && daily >= limits.daily_usd;
            let weekly_exceeded = limits.weekly_usd != Decimal::ZERO && weekly >= limits.weekly_usd;
            if daily_exceeded || weekly_exceeded {
                let reset = if weekly_exceeded {
                    window.weekly_end
                } else {
                    window.daily_end
                };
                let retry_after = reset
                    .checked_sub(now_us)
                    .and_then(|micros| u64::try_from(micros).ok())
                    .map(Duration::from_micros)
                    .filter(|duration| !duration.is_zero())
                    .unwrap_or(Duration::from_secs(1));
                return Err(GatewayError::new(
                    GatewayErrorKind::RateLimited,
                    "client API key budget is exhausted",
                )
                .with_client_code(if weekly_exceeded {
                    "key_weekly_budget_exceeded"
                } else {
                    "key_daily_budget_exceeded"
                })
                .with_retry_after(retry_after));
            }
        }
        transaction.commit().await.map_err(|_| unavailable())
    }

    async fn settle_inner(&self, charge: &ClientBudgetCharge) -> Result<(), ClientBudgetError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| ClientBudgetError(Some(source.into())))?;
        acquire_write_lock(&mut transaction)
            .await
            .map_err(|source| ClientBudgetError(Some(source.into())))?;
        let exists =
            sqlx::query_scalar::<_, String>("select id from client_api_keys where id = ?1")
                .bind(charge.key_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| ClientBudgetError(Some(source.into())))?;
        if exists.is_none() {
            transaction
                .commit()
                .await
                .map_err(|source| ClientBudgetError(Some(source.into())))?;
            return Ok(());
        }

        let completed_at = DateTime::<Utc>::from(charge.completed_at);
        let completed_at_us = datetime_to_micros(completed_at);
        let now = Utc::now();
        let window = advance_windows(
            &mut transaction,
            charge.key_id.as_str(),
            now,
            completed_at,
            self.timezone,
        )
        .await
        .map_err(|source| ClientBudgetError(Some(source.into())))?;
        let inserted = sqlx::query(
            "insert into client_key_charge_events
               (request_id, client_api_key_id, amount_usd, completed_at_us)
             values (?1, ?2, ?3, ?4) on conflict (request_id) do nothing",
        )
        .bind(charge.request_id.as_str())
        .bind(charge.key_id.as_str())
        .bind(encode_amount(charge.amount_usd))
        .bind(completed_at_us)
        .execute(&mut *transaction)
        .await
        .map_err(|source| ClientBudgetError(Some(source.into())))?
        .rows_affected();
        if inserted == 1 {
            if completed_at_us >= window.daily_start && completed_at_us < window.daily_end {
                let used = decode_amount(&window.daily_used)
                    .map_err(|source| ClientBudgetError(Some(source.into())))?;
                let total = checked_amount_sum([used, charge.amount_usd])
                    .map_err(|source| ClientBudgetError(Some(source.into())))?;
                sqlx::query(
                    "update client_key_budget_windows set daily_used_usd = ?2 where client_api_key_id = ?1",
                )
                .bind(charge.key_id.as_str())
                .bind(encode_amount(total))
                .execute(&mut *transaction)
                .await
                .map_err(|source| ClientBudgetError(Some(source.into())))?;
            }
            if completed_at_us >= window.weekly_start && completed_at_us < window.weekly_end {
                let used = decode_amount(&window.weekly_used)
                    .map_err(|source| ClientBudgetError(Some(source.into())))?;
                let total = checked_amount_sum([used, charge.amount_usd])
                    .map_err(|source| ClientBudgetError(Some(source.into())))?;
                sqlx::query(
                    "update client_key_budget_windows set weekly_used_usd = ?2 where client_api_key_id = ?1",
                )
                .bind(charge.key_id.as_str())
                .bind(encode_amount(total))
                .execute(&mut *transaction)
                .await
                .map_err(|source| ClientBudgetError(Some(source.into())))?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|source| ClientBudgetError(Some(source.into())))
    }
}

impl ClientBudgetPort for SqliteClientBudgetStore {
    fn admit(&self, key_id: ClientApiKeyId) -> BoxFuture<'_, Result<(), GatewayError>> {
        Box::pin(async move { self.admit_inner(key_id).await })
    }

    fn settle(&self, charge: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async move {
            let result = self.settle_inner(&charge).await;
            let mut retry = self.retry.lock().map_err(|_| ClientBudgetError(None))?;
            if result.is_err() {
                retry.insert(charge.request_id.as_str().to_owned(), charge);
            } else {
                retry.remove(charge.request_id.as_str());
            }
            result
        })
    }
}

struct BudgetWindow {
    daily_start: i64,
    daily_end: i64,
    weekly_start: i64,
    weekly_end: i64,
    daily_used: String,
    weekly_used: String,
}

async fn advance_windows(
    transaction: &mut Transaction<'_, sqlx::Sqlite>,
    key: &str,
    now: DateTime<Utc>,
    used_at: DateTime<Utc>,
    timezone: gateway_core::time::DeploymentTimeZone,
) -> Result<BudgetWindow, sqlx::Error> {
    let now_us = now.timestamp_micros();
    let used_at_us = used_at.timestamp_micros();
    let day_start = timezone
        .day_start(now)
        .ok_or_else(|| sqlx::Error::Protocol("invalid budget day".to_owned()))?;
    let day_end = timezone
        .days_after(now, 1)
        .ok_or_else(|| sqlx::Error::Protocol("invalid budget day end".to_owned()))?;
    let week_end = timezone
        .days_after(now, 7)
        .ok_or_else(|| sqlx::Error::Protocol("invalid budget week end".to_owned()))?;
    let proposed_daily_start = day_start.timestamp_micros();
    let proposed_daily_end = day_end.timestamp_micros();
    let proposed_weekly_end = week_end.timestamp_micros();
    let current = sqlx::query_as::<_, (i64, i64, i64, i64, String, String)>(
        "select daily_start_us, daily_end_us, weekly_start_us, weekly_end_us,
                daily_used_usd, weekly_used_usd
         from client_key_budget_windows where client_api_key_id = ?1",
    )
    .bind(key)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some((daily_start, daily_end, weekly_start, weekly_end, daily_used, weekly_used)) = current
    else {
        sqlx::query(
            "insert into client_key_budget_windows
               (client_api_key_id, daily_start_us, daily_end_us, weekly_start_us, weekly_end_us)
             values (?1, ?2, ?3, ?2, ?4)",
        )
        .bind(key)
        .bind(proposed_daily_start)
        .bind(proposed_daily_end)
        .bind(proposed_weekly_end)
        .execute(&mut **transaction)
        .await?;
        return Ok(BudgetWindow {
            daily_start: proposed_daily_start,
            daily_end: proposed_daily_end,
            weekly_start: proposed_daily_start,
            weekly_end: proposed_weekly_end,
            daily_used: encode_amount(Decimal::ZERO),
            weekly_used: encode_amount(Decimal::ZERO),
        });
    };

    let reset_daily = daily_end <= now_us && daily_start <= used_at_us;
    let reset_weekly = weekly_end <= now_us && weekly_start <= used_at_us;
    let (daily_start, daily_end, daily_used) = if reset_daily {
        let start = proposed_daily_start.max(daily_end).max(daily_start);
        let start_time = DateTime::from_timestamp_micros(start)
            .ok_or_else(|| sqlx::Error::Protocol("invalid budget day start".to_owned()))?;
        let end = timezone
            .days_after(start_time, 1)
            .ok_or_else(|| sqlx::Error::Protocol("invalid budget day end".to_owned()))?
            .timestamp_micros();
        (start, end, encode_amount(Decimal::ZERO))
    } else {
        (daily_start, daily_end, daily_used)
    };
    let (weekly_start, weekly_end, weekly_used) = if reset_weekly {
        let start = proposed_daily_start.max(weekly_end).max(weekly_start);
        let start_time = DateTime::from_timestamp_micros(start)
            .ok_or_else(|| sqlx::Error::Protocol("invalid budget week start".to_owned()))?;
        let end = timezone
            .days_after(start_time, 7)
            .ok_or_else(|| sqlx::Error::Protocol("invalid budget week end".to_owned()))?
            .timestamp_micros();
        (start, end, encode_amount(Decimal::ZERO))
    } else {
        (weekly_start, weekly_end, weekly_used)
    };
    sqlx::query(
        "update client_key_budget_windows set
           daily_start_us = ?2, daily_end_us = ?3, daily_used_usd = ?4,
           weekly_start_us = ?5, weekly_end_us = ?6, weekly_used_usd = ?7
         where client_api_key_id = ?1",
    )
    .bind(key)
    .bind(daily_start)
    .bind(daily_end)
    .bind(&daily_used)
    .bind(weekly_start)
    .bind(weekly_end)
    .bind(&weekly_used)
    .execute(&mut **transaction)
    .await?;
    Ok(BudgetWindow {
        daily_start,
        daily_end,
        weekly_start,
        weekly_end,
        daily_used,
        weekly_used,
    })
}

fn read_amount(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> Result<Decimal, GatewayError> {
    let value = row.try_get::<String, _>(field).map_err(|_| unavailable())?;
    decode_amount(&value).map_err(|_| unavailable())
}

fn unavailable() -> GatewayError {
    GatewayError::new(
        GatewayErrorKind::ProviderInfrastructureUnavailable,
        "client budget service is temporarily unavailable",
    )
    .with_client_code("key_budget_unavailable")
}
