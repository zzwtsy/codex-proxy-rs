//! SQLite 管理观测端口组合；查询统一委托给 SQLite 专属观测模块。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use gateway_admin::{
    model::observability::{
        DashboardObservation, DashboardQuery, DashboardRuntimeSlots, DiagnosticDimension,
        DiagnosticsObservation, Granularity, OpsErrorPage, OpsErrorQuery, RequestMetricPoint,
        TimeRange, UsageDetail, UsageFilter, UsageOverview, UsagePage, UsageQuery,
    },
    ports::store::{
        AdminStoreError, AdminStoreErrorKind, AdminStoreResult, ObservabilityStore,
        UsageCalculatedBillingStream,
    },
};
use gateway_core::{
    account::{
        AccountStatus, AccountStatusFacts, ProviderAccount, ProviderAccountStore,
        resolve_account_status,
    },
    provider_ports::ProviderCooldownPort,
};
use sqlx::SqlitePool;

use crate::{
    CredentialLeaseRepository,
    postgres::{
        DashboardObservation as StoreDashboardObservation, ProviderAccountMetrics,
        UsageRecordFilter, UsageRecordQuery, admin_calculated_usage_billing_fact,
        admin_dashboard_observation, admin_diagnostics_observation, admin_request_metric_point,
        admin_usage_overview, observability_error, store_ops_error_query, store_range,
        store_usage_filter, store_usage_query,
    },
    sqlite::{self, observability},
};

/// SQLite 的持久化观测数据与 Provider 冷却端口。
#[derive(Clone)]
pub struct SqliteAdminObservabilityStore {
    pool: SqlitePool,
    cooldowns: Arc<dyn ProviderCooldownPort>,
    timezone: gateway_core::time::DeploymentTimeZone,
}

impl SqliteAdminObservabilityStore {
    #[must_use]
    pub fn new(pool: SqlitePool, cooldowns: Arc<dyn ProviderCooldownPort>) -> Self {
        Self {
            pool,
            cooldowns,
            timezone: Default::default(),
        }
    }

    #[must_use]
    pub fn with_timezone(mut self, timezone: gateway_core::time::DeploymentTimeZone) -> Self {
        self.timezone = timezone;
        self
    }

    async fn account_status_snapshot(
        &self,
        observed_at: DateTime<Utc>,
    ) -> AdminStoreResult<(ProviderAccountMetrics, Vec<ProviderAccount>)> {
        let accounts = super::SqliteProviderAccountRepository::new(self.pool.clone())
            .list_accounts()
            .await
            .map_err(|_| {
                AdminStoreError::new(
                    AdminStoreErrorKind::Unavailable,
                    "SQLite observability",
                    "load provider accounts failed",
                )
            })?;
        let mut metrics = ProviderAccountMetrics {
            total: u64::try_from(accounts.len()).unwrap_or(u64::MAX),
            ..ProviderAccountMetrics::default()
        };
        let mut normal = Vec::new();
        for account in accounts {
            let cooldown = self
                .cooldowns
                .read(account.id())
                .await
                .ok()
                .flatten()
                .map(|cooldown| cooldown.scheduling_state());
            let status = resolve_account_status(
                &AccountStatusFacts {
                    enabled: account.enabled(),
                    credential_state: account.credential_state(),
                    access_token_expires_at: account.access_token_expires_at(),
                    quota: account.quota(),
                    cooldown,
                    last_error_reason: account.last_error_reason(),
                    last_error_message: account.last_error_message().map(str::to_owned),
                },
                observed_at.into(),
            )
            .status;
            match status {
                AccountStatus::Normal => {
                    metrics.normal = metrics.normal.saturating_add(1);
                    normal.push(account);
                }
                AccountStatus::QuotaExhausted => {
                    metrics.quota_exhausted = metrics.quota_exhausted.saturating_add(1)
                }
                AccountStatus::RateLimited => {
                    metrics.rate_limited = metrics.rate_limited.saturating_add(1)
                }
                AccountStatus::Disabled => metrics.disabled = metrics.disabled.saturating_add(1),
                AccountStatus::Error => metrics.error = metrics.error.saturating_add(1),
            }
        }
        Ok((metrics, normal))
    }

    async fn dashboard_runtime_slots(
        &self,
        normal_accounts: &[ProviderAccount],
    ) -> AdminStoreResult<DashboardRuntimeSlots> {
        let inherited_accounts = u64::try_from(
            normal_accounts
                .iter()
                .filter(|account| account.concurrency_limit().is_none())
                .count(),
        )
        .map_err(|_| invalid_admin("inherited account count overflows u64"))?;
        let overridden_slots = normal_accounts.iter().fold(0_u64, |total, account| {
            total.saturating_add(
                account
                    .concurrency_limit()
                    .map_or(0, |limit| u64::from(limit.get())),
            )
        });
        if normal_accounts.is_empty() {
            return Ok(DashboardRuntimeSlots {
                inherited_accounts,
                overridden_slots,
                used_slots: Some(0),
            });
        }
        let ids = normal_accounts
            .iter()
            .map(|account| account.id().as_str().to_owned())
            .collect::<Vec<_>>();
        let leases = sqlite::SqliteCredentialLeaseRepository::new(self.pool.clone());
        let signals = match leases.credential_runtime_signals(&ids).await {
            Ok(signals) => signals,
            Err(_) => {
                return Ok(DashboardRuntimeSlots {
                    inherited_accounts,
                    overridden_slots,
                    used_slots: None,
                });
            }
        };
        let used_slots = signals.into_iter().fold(0_u64, |total, signal| {
            total.saturating_add(u64::from(signal.in_flight))
        });
        Ok(DashboardRuntimeSlots {
            inherited_accounts,
            overridden_slots,
            used_slots: Some(used_slots),
        })
    }
}

#[async_trait]
impl ObservabilityStore for SqliteAdminObservabilityStore {
    async fn dashboard_summary(
        &self,
        query: DashboardQuery,
        observed_at: DateTime<Utc>,
    ) -> AdminStoreResult<DashboardObservation> {
        let range = store_range(query.range).map_err(observability_error)?;
        let (provider_accounts, normal_accounts) =
            self.account_status_snapshot(observed_at).await?;
        let runtime_slots = self.dashboard_runtime_slots(&normal_accounts).await?;
        let totals = observability::dashboard_totals(&self.pool)
            .await
            .map_err(observability_error)?;
        let trend = observability::metric_series(
            &self.pool,
            range,
            &UsageRecordFilter::default(),
            query.granularity,
            self.timezone,
            false,
        )
        .await
        .map_err(observability_error)?;
        let recent_range_query = UsageRecordQuery {
            range,
            filter: store_usage_filter(query.recent_request_filter),
            current_page: 1,
            page_size: crate::postgres::ObservabilityPageSize::new(query.recent_request_limit)
                .map_err(|_| invalid_admin("invalid dashboard recent request limit"))?,
        };
        let recent_requests = observability::list_usage_records(&self.pool, recent_range_query)
            .await
            .map_err(observability_error)?
            .items;
        let mut account_usage = observability::dashboard_account_usage(&self.pool, range)
            .await
            .map_err(observability_error)?;
        account_usage.truncate(usize::from(query.account_limit));
        let observation = StoreDashboardObservation {
            range,
            totals,
            provider_accounts,
            runtime_slots,
            trend,
            account_usage,
            recent_requests,
        };
        Ok(admin_dashboard_observation(observation))
    }

    async fn dashboard_trend(
        &self,
        range: TimeRange,
        granularity: Granularity,
    ) -> AdminStoreResult<Vec<RequestMetricPoint>> {
        metric_points(self, range, UsageFilter::default(), granularity, false).await
    }

    async fn usage_trend(
        &self,
        range: TimeRange,
        filter: UsageFilter,
        granularity: Granularity,
    ) -> AdminStoreResult<Vec<RequestMetricPoint>> {
        metric_points(self, range, filter, granularity, true).await
    }

    fn usage_calculated_billing_facts(
        &self,
        range: TimeRange,
        filter: UsageFilter,
        granularity: Granularity,
    ) -> UsageCalculatedBillingStream<'_> {
        let range = match store_range(range).map_err(observability_error) {
            Ok(range) => range,
            Err(error) => return Box::pin(futures::stream::once(async { Err(error) })),
        };
        observability::calculated_billing_facts(
            &self.pool,
            range,
            store_usage_filter(filter),
            granularity,
            self.timezone,
        )
        .map_err(observability_error)
        .map_ok(admin_calculated_usage_billing_fact)
        .boxed()
    }

    async fn list_usage_records(&self, query: UsageQuery) -> AdminStoreResult<UsagePage> {
        let query = store_usage_query(query)?;
        let page = observability::list_usage_records(&self.pool, query)
            .await
            .map_err(observability_error)?;
        Ok(page)
    }

    async fn usage_record_detail(&self, request_id: &str) -> AdminStoreResult<UsageDetail> {
        let detail = observability::usage_record_detail(&self.pool, request_id)
            .await
            .map_err(observability_error)?;
        Ok(detail)
    }

    async fn usage_summary(
        &self,
        range: TimeRange,
        filter: UsageFilter,
    ) -> AdminStoreResult<UsageOverview> {
        let range = store_range(range).map_err(observability_error)?;
        let filter = store_usage_filter(filter);
        let overview = observability::usage_overview(&self.pool, range, &filter)
            .await
            .map_err(observability_error)?;
        Ok(admin_usage_overview(overview))
    }

    async fn usage_diagnostics(
        &self,
        range: TimeRange,
        filter: UsageFilter,
        dimension: DiagnosticDimension,
        limit: u16,
    ) -> AdminStoreResult<DiagnosticsObservation> {
        let range = store_range(range).map_err(observability_error)?;
        let filter = store_usage_filter(filter);
        observability::usage_diagnostics(&self.pool, range, &filter, dimension, limit)
            .await
            .map_err(observability_error)
            .map(admin_diagnostics_observation)
    }

    async fn list_ops_errors(&self, query: OpsErrorQuery) -> AdminStoreResult<OpsErrorPage> {
        let query = store_ops_error_query(query)?;
        let page = observability::ops_errors(&self.pool, query)
            .await
            .map_err(observability_error)?;
        Ok(page)
    }
}

async fn metric_points(
    store: &SqliteAdminObservabilityStore,
    range: TimeRange,
    filter: UsageFilter,
    granularity: Granularity,
    include_costs: bool,
) -> AdminStoreResult<Vec<RequestMetricPoint>> {
    let points = observability::metric_series(
        &store.pool,
        store_range(range).map_err(observability_error)?,
        &store_usage_filter(filter),
        granularity,
        store.timezone,
        include_costs,
    )
    .await
    .map_err(observability_error)?
    .into_iter()
    .map(admin_request_metric_point)
    .collect::<Vec<_>>();
    Ok(points)
}

fn invalid_admin(message: &'static str) -> AdminStoreError {
    AdminStoreError::new(
        AdminStoreErrorKind::Invalid,
        "SQLite observability",
        message,
    )
}
