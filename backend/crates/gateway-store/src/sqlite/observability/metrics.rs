//! SQLite 用量指标、趋势和计费事实查询。

use std::{collections::BTreeMap, str::FromStr};

use chrono::{DateTime, TimeDelta, Utc};
use futures::{TryStreamExt, stream::BoxStream};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, sqlite::SqliteRow};

use crate::{
    DecimalAmount, StoreResult,
    postgres::{
        CalculatedUsageBillingFact, CostCoverage, CurrencyCostTotal, DashboardTotals,
        DiagnosticDimension, DiagnosticObservation, DiagnosticsObservation, LatencyPercentiles,
        ObservabilityRange, ObservationGranularity, PercentileMilliseconds,
        ProviderAccountModelUsageObservation, ProviderAccountRequestBucket,
        ProviderAccountUsageObservation, ProviderObservation, RequestMetricPoint, RequestMetrics,
        UsageOverview, UsageRecordFilter,
    },
    sqlite::value::{datetime_from_micros, decode_amount},
};

use super::{
    checked_u64, completed_usage_fact_predicate, invalid, push_range, push_usage_filter,
    unavailable,
};

#[derive(Default)]
struct UsageFact {
    request_count: u64,
    success_count: u64,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    image_input_tokens: Option<u64>,
    image_output_tokens: Option<u64>,
    image_generation_succeeded: Option<bool>,
    total_tokens: Option<u64>,
    cost_source: String,
    cost_currency: Option<String>,
    cost_amount: Option<DecimalAmount>,
}

#[derive(Default)]
struct AccountUsageAccumulator {
    account_id: String,
    provider_kind: String,
    authentication_kind: String,
    name: String,
    email: Option<String>,
    plan_type: Option<String>,
    request_count: u64,
    success_count: u64,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    image_input_tokens: Option<u64>,
    image_output_tokens: Option<u64>,
    image_request_count: u64,
    image_request_failed_count: u64,
    total_tokens: Option<u64>,
    cost_coverage: CostCoverage,
    costs: BTreeMap<String, gateway_core::metering::Decimal>,
    last_used_at: DateTime<Utc>,
    hourly_requests: BTreeMap<i64, u64>,
    models: BTreeMap<String, AccountModelAccumulator>,
}

impl AccountUsageAccumulator {
    fn push(
        &mut self,
        usage: &UsageFact,
        model: Option<&str>,
        started_at: DateTime<Utc>,
        in_hourly_timeline: bool,
    ) -> StoreResult<()> {
        self.request_count = add_u64(self.request_count, usage.request_count)?;
        self.success_count = add_u64(self.success_count, usage.success_count)?;
        self.last_used_at = self.last_used_at.max(started_at);
        if in_hourly_timeline {
            self.add_hourly_request(started_at)?;
        }
        add_option_sum(&mut self.input_tokens, usage.input_tokens)?;
        add_option_sum(&mut self.output_tokens, usage.output_tokens)?;
        add_option_sum(&mut self.cached_tokens, usage.cached_tokens)?;
        add_option_sum(&mut self.cache_write_tokens, usage.cache_write_tokens)?;
        add_option_sum(&mut self.reasoning_tokens, usage.reasoning_tokens)?;
        add_option_sum(&mut self.image_input_tokens, usage.image_input_tokens)?;
        add_option_sum(&mut self.image_output_tokens, usage.image_output_tokens)?;
        add_option_sum(&mut self.total_tokens, usage.total_tokens)?;
        match usage.image_generation_succeeded {
            Some(true) => self.image_request_count = add_u64(self.image_request_count, 1)?,
            Some(false) => {
                self.image_request_failed_count = add_u64(self.image_request_failed_count, 1)?
            }
            None => {}
        }
        self.add_cost(usage)?;
        if let Some(model) = model {
            self.models
                .entry(model.to_owned())
                .or_default()
                .push(usage, started_at)?;
        }
        Ok(())
    }

    fn add_hourly_request(&mut self, started_at: DateTime<Utc>) -> StoreResult<()> {
        let bucket_hour = started_at.timestamp().div_euclid(3600) * 3600;
        let count = self.hourly_requests.entry(bucket_hour).or_default();
        *count = add_u64(*count, 1)?;
        Ok(())
    }

    fn add_cost(&mut self, usage: &UsageFact) -> StoreResult<()> {
        match usage.cost_source.as_str() {
            "provider_reported" => {
                self.cost_coverage.provider_reported_count =
                    add_u64(self.cost_coverage.provider_reported_count, 1)?
            }
            "calculated" => {
                self.cost_coverage.calculated_count =
                    add_u64(self.cost_coverage.calculated_count, 1)?
            }
            "unavailable" => {
                self.cost_coverage.unavailable_count =
                    add_u64(self.cost_coverage.unavailable_count, 1)?
            }
            _ => {}
        }
        if let (Some(currency), Some(amount)) = (&usage.cost_currency, &usage.cost_amount) {
            let total = self
                .costs
                .entry(currency.clone())
                .or_insert(gateway_core::metering::Decimal::ZERO);
            *total = total
                .checked_add(amount_decimal(amount)?)
                .ok_or_else(|| invalid("account cost exceeds numeric(20,10)"))?;
        }
        Ok(())
    }

    fn into_observation(
        self,
        request_buckets: Vec<ProviderAccountRequestBucket>,
        models: Vec<ProviderAccountModelUsageObservation>,
    ) -> StoreResult<ProviderAccountUsageObservation> {
        Ok(ProviderAccountUsageObservation {
            account_id: self.account_id,
            provider_kind: self.provider_kind,
            authentication_kind: self.authentication_kind,
            name: self.name,
            email: self.email,
            plan_type: self.plan_type,
            request_count: self.request_count,
            success_count: self.success_count,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cached_tokens: self.cached_tokens,
            cache_write_tokens: self.cache_write_tokens,
            reasoning_tokens: self.reasoning_tokens,
            image_input_tokens: self.image_input_tokens,
            image_output_tokens: self.image_output_tokens,
            image_request_count: self.image_request_count,
            image_request_failed_count: self.image_request_failed_count,
            total_tokens: self.total_tokens,
            cost_coverage: self.cost_coverage,
            costs: cost_totals(self.costs)?,
            last_used_at: Some(self.last_used_at),
            request_buckets,
            models,
        })
    }
}

#[derive(Default)]
struct AccountModelAccumulator {
    request_count: u64,
    success_count: u64,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    image_input_tokens: Option<u64>,
    image_output_tokens: Option<u64>,
    image_request_count: u64,
    image_request_failed_count: u64,
    total_tokens: Option<u64>,
    cost_coverage: CostCoverage,
    costs: BTreeMap<String, gateway_core::metering::Decimal>,
    last_used_at: DateTime<Utc>,
}

impl AccountModelAccumulator {
    fn push(&mut self, usage: &UsageFact, started_at: DateTime<Utc>) -> StoreResult<()> {
        self.request_count = add_u64(self.request_count, 1)?;
        self.success_count = add_u64(self.success_count, usage.success_count)?;
        add_option_sum(&mut self.input_tokens, usage.input_tokens)?;
        add_option_sum(&mut self.output_tokens, usage.output_tokens)?;
        add_option_sum(&mut self.cached_tokens, usage.cached_tokens)?;
        add_option_sum(&mut self.cache_write_tokens, usage.cache_write_tokens)?;
        add_option_sum(&mut self.reasoning_tokens, usage.reasoning_tokens)?;
        add_option_sum(&mut self.image_input_tokens, usage.image_input_tokens)?;
        add_option_sum(&mut self.image_output_tokens, usage.image_output_tokens)?;
        add_option_sum(&mut self.total_tokens, usage.total_tokens)?;
        self.last_used_at = self.last_used_at.max(started_at);
        match usage.image_generation_succeeded {
            Some(true) => self.image_request_count = add_u64(self.image_request_count, 1)?,
            Some(false) => {
                self.image_request_failed_count = add_u64(self.image_request_failed_count, 1)?
            }
            None => {}
        }
        match usage.cost_source.as_str() {
            "provider_reported" => {
                self.cost_coverage.provider_reported_count =
                    add_u64(self.cost_coverage.provider_reported_count, 1)?
            }
            "calculated" => {
                self.cost_coverage.calculated_count =
                    add_u64(self.cost_coverage.calculated_count, 1)?
            }
            "unavailable" => {
                self.cost_coverage.unavailable_count =
                    add_u64(self.cost_coverage.unavailable_count, 1)?
            }
            _ => {}
        }
        if let (Some(currency), Some(amount)) = (&usage.cost_currency, &usage.cost_amount) {
            let total = self
                .costs
                .entry(currency.clone())
                .or_insert(gateway_core::metering::Decimal::ZERO);
            *total = total
                .checked_add(amount_decimal(amount)?)
                .ok_or_else(|| invalid("model cost exceeds numeric(20,10)"))?;
        }
        Ok(())
    }

    fn into_model(self, model: String) -> StoreResult<ProviderAccountModelUsageObservation> {
        Ok(ProviderAccountModelUsageObservation {
            model,
            request_count: self.request_count,
            success_count: self.success_count,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cached_tokens: self.cached_tokens,
            cache_write_tokens: self.cache_write_tokens,
            reasoning_tokens: self.reasoning_tokens,
            image_input_tokens: self.image_input_tokens,
            image_output_tokens: self.image_output_tokens,
            image_request_count: self.image_request_count,
            image_request_failed_count: self.image_request_failed_count,
            total_tokens: self.total_tokens,
            cost_coverage: self.cost_coverage,
            costs: cost_totals(self.costs)?,
            last_used_at: self.last_used_at,
        })
    }
}

fn cost_totals(
    costs: BTreeMap<String, gateway_core::metering::Decimal>,
) -> StoreResult<Vec<CurrencyCostTotal>> {
    costs
        .into_iter()
        .map(|(currency, amount)| {
            Ok(CurrencyCostTotal {
                currency,
                amount: DecimalAmount::from_str(&amount.canonical())?,
            })
        })
        .collect()
}

fn add_option_sum(target: &mut Option<u64>, value: Option<u64>) -> StoreResult<()> {
    if let Some(value) = value {
        let total = target.get_or_insert(0);
        *total = add_u64(*total, value)?;
    }
    Ok(())
}

fn optional_u64(row: &SqliteRow, column: &'static str) -> StoreResult<Option<u64>> {
    let value: Option<i64> = row.try_get(column).map_err(|_| unavailable())?;
    value.map(checked_u64).transpose()
}

fn optional_bool(row: &SqliteRow, column: &'static str) -> StoreResult<Option<bool>> {
    let value: Option<i64> = row.try_get(column).map_err(|_| unavailable())?;
    value
        .map(|value| match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("stored boolean value is invalid")),
        })
        .transpose()
}

#[derive(Debug)]
struct MetricRecord {
    started_at: DateTime<Utc>,
    outcome: String,
    attempt_count: u64,
    error_kind: Option<String>,
    upstream_status_code: Option<i64>,
    client_status_code: Option<i64>,
    provider_kind: Option<String>,
    request_kind: Option<String>,
    client_transport: String,
    downstream_committed: bool,
    requested_model_id: Option<String>,
    upstream_model_id: Option<String>,
    image_generation_requested: bool,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    image_input_tokens: Option<u64>,
    image_output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    cost_source: String,
    cost_amount: Option<DecimalAmount>,
    cost_currency: Option<String>,
    latency_ms: Option<u64>,
    first_token_ms: Option<u64>,
    admission_decision_ms: Option<u64>,
    account_selection_wait_ms: Option<u64>,
    capacity_used_slots: Option<u64>,
    capacity_total_slots: Option<u64>,
}

impl MetricRecord {
    fn is_completed_usage(&self) -> bool {
        self.outcome == "succeeded"
            && self.downstream_committed
            && (self.provider_kind.as_deref() != Some("openai")
                || self.request_kind.as_deref() != Some("prewarm"))
            && ((self.client_transport == "websocket" && self.client_status_code.is_none())
                || self
                    .client_status_code
                    .is_some_and(|code| (200..=399).contains(&code)))
            && (self.requested_model_id.is_some()
                || self.upstream_model_id.is_some()
                || self.image_generation_requested
                || self.input_tokens.is_some()
                || self.output_tokens.is_some()
                || self.cached_tokens.is_some()
                || self.cache_write_tokens.is_some()
                || self.reasoning_tokens.is_some()
                || self.image_input_tokens.is_some()
                || self.image_output_tokens.is_some()
                || self.total_tokens.is_some()
                || self.cost_amount.is_some())
    }
}

#[derive(Default)]
struct MetricAccumulator {
    metrics: RequestMetrics,
    coverage: CostCoverage,
    latency_ms: Vec<f64>,
    first_token_ms: Vec<f64>,
    admission_ms: Vec<f64>,
    account_wait_ms: Vec<f64>,
    throughput: Vec<f64>,
    capacity: Vec<f64>,
    costs: BTreeMap<String, gateway_core::metering::Decimal>,
}

impl MetricAccumulator {
    fn push(&mut self, record: &MetricRecord, include_costs: bool) -> StoreResult<()> {
        self.metrics.request_count = add_u64(self.metrics.request_count, 1)?;
        match record.outcome.as_str() {
            "succeeded" => self.metrics.success_count = add_u64(self.metrics.success_count, 1)?,
            "failed" => self.metrics.failure_count = add_u64(self.metrics.failure_count, 1)?,
            "cancelled" => self.metrics.cancelled_count = add_u64(self.metrics.cancelled_count, 1)?,
            "incomplete" => {
                self.metrics.incomplete_count = add_u64(self.metrics.incomplete_count, 1)?
            }
            _ => {}
        }
        if record
            .client_status_code
            .is_some_and(|status| (400..=499).contains(&status))
        {
            self.metrics.caller_error_count = add_u64(self.metrics.caller_error_count, 1)?;
        }
        if let Some(value) = record.admission_decision_ms {
            self.metrics.admission_decision_count =
                add_u64(self.metrics.admission_decision_count, 1)?;
            self.admission_ms.push(value as f64);
        }
        if let Some(value) = record.account_selection_wait_ms {
            self.metrics.account_selection_wait_count =
                add_u64(self.metrics.account_selection_wait_count, 1)?;
            self.account_wait_ms.push(value as f64);
        }
        if let Some(total) = record.capacity_total_slots.filter(|value| *value > 0)
            && let Some(used) = record.capacity_used_slots
        {
            self.metrics.capacity_sample_count = add_u64(self.metrics.capacity_sample_count, 1)?;
            self.capacity.push((used as f64 / total as f64) * 10_000.0);
        }
        if !record.is_completed_usage() {
            return Ok(());
        }

        add_optional(&mut self.metrics.input_tokens, record.input_tokens)?;
        add_optional(&mut self.metrics.output_tokens, record.output_tokens)?;
        add_optional(&mut self.metrics.cached_tokens, record.cached_tokens)?;
        add_optional(
            &mut self.metrics.cache_write_tokens,
            record.cache_write_tokens,
        )?;
        add_optional(&mut self.metrics.reasoning_tokens, record.reasoning_tokens)?;
        add_optional(&mut self.metrics.total_tokens, record.total_tokens)?;
        if record.input_tokens.is_some() {
            self.metrics.cache_eligible_request_count =
                add_u64(self.metrics.cache_eligible_request_count, 1)?;
            if record.cached_tokens.is_some_and(|tokens| tokens > 0) {
                self.metrics.cache_hit_request_count =
                    add_u64(self.metrics.cache_hit_request_count, 1)?;
            }
        }
        if let Some(value) = record.latency_ms {
            self.metrics.latency_count = add_u64(self.metrics.latency_count, 1)?;
            self.metrics.latency_sum = add_u64(self.metrics.latency_sum, value)?;
            self.metrics.max_latency_ms = Some(self.metrics.max_latency_ms.unwrap_or(0).max(value));
            self.metrics.min_latency_ms = Some(
                self.metrics
                    .min_latency_ms
                    .map_or(value, |old| old.min(value)),
            );
            self.latency_ms.push(value as f64);
        }
        if let Some(value) = record.first_token_ms {
            self.metrics.first_token_latency_count =
                add_u64(self.metrics.first_token_latency_count, 1)?;
            self.metrics.first_token_latency_sum =
                add_u64(self.metrics.first_token_latency_sum, value)?;
            self.first_token_ms.push(value as f64);
        }
        if let (Some(output), Some(latency), Some(first_token)) = (
            record.output_tokens,
            record.latency_ms,
            record.first_token_ms,
        ) && output > 0
            && latency > first_token
        {
            self.throughput
                .push(output as f64 * 1000.0 / (latency - first_token).max(1) as f64);
        }
        match record.cost_source.as_str() {
            "provider_reported" => {
                self.coverage.provider_reported_count =
                    add_u64(self.coverage.provider_reported_count, 1)?;
            }
            "calculated" => {
                self.coverage.calculated_count = add_u64(self.coverage.calculated_count, 1)?;
            }
            "unavailable" => {
                self.coverage.unavailable_count = add_u64(self.coverage.unavailable_count, 1)?;
            }
            _ => {}
        }
        if include_costs
            && let (Some(currency), Some(amount)) = (&record.cost_currency, &record.cost_amount)
        {
            let amount = amount_decimal(amount)?;
            let total = self
                .costs
                .entry(currency.clone())
                .or_insert(gateway_core::metering::Decimal::ZERO);
            *total = total
                .checked_add(amount)
                .ok_or_else(|| invalid("cost sum exceeds numeric(20,10)"))?;
        }
        Ok(())
    }

    fn finish(self) -> StoreResult<(RequestMetrics, CostCoverage, Vec<CurrencyCostTotal>)> {
        let mut metrics = self.metrics;
        metrics.latency_percentiles = percentiles(&self.latency_ms)?;
        metrics.first_token_latency_percentiles = percentiles(&self.first_token_ms)?;
        metrics.admission_decision_percentiles = percentiles(&self.admission_ms)?;
        metrics.account_selection_wait_percentiles = percentiles(&self.account_wait_ms)?;
        metrics.output_throughput_p10 = rounded_percentile(&self.throughput, 0.10)?;
        metrics.output_throughput_p50 = rounded_percentile(&self.throughput, 0.50)?;
        metrics.output_throughput_p90 = rounded_percentile(&self.throughput, 0.90)?;
        if !self.capacity.is_empty() {
            let mean = self.capacity.iter().sum::<f64>() / self.capacity.len() as f64;
            metrics.capacity_utilization_avg_basis_points = Some(round_u64(mean)?);
            metrics.capacity_utilization_p95_basis_points =
                rounded_percentile(&self.capacity, 0.95)?;
        }
        let costs = self
            .costs
            .into_iter()
            .map(|(currency, amount)| {
                Ok(CurrencyCostTotal {
                    currency,
                    amount: DecimalAmount::from_str(&amount.canonical())?,
                })
            })
            .collect::<StoreResult<Vec<_>>>()?;
        Ok((metrics, self.coverage, costs))
    }
}

pub(crate) async fn dashboard_account_usage(
    pool: &SqlitePool,
    range: ObservabilityRange,
) -> StoreResult<Vec<ProviderAccountUsageObservation>> {
    let bucket_end = range.end.timestamp().div_euclid(3600) * 3600;
    let bucket_start = DateTime::from_timestamp(bucket_end - 23 * 3600, 0)
        .ok_or_else(|| invalid("account request timeline is outside supported timestamps"))?;
    let query_start_us = range
        .start
        .timestamp_micros()
        .min(bucket_start.timestamp_micros());
    let fact = completed_usage_fact_predicate("mr");
    let mut statement = QueryBuilder::<Sqlite>::new(
        "select mr.provider_account_ref as account_id,
                coalesce(pa.provider_kind, mr.provider_kind, 'unknown') as provider_kind,
                coalesce(pa.authentication_kind, mr.provider_account_authentication_kind_snapshot, 'unknown') as authentication_kind,
                coalesce(pa.name, mr.provider_account_name_snapshot, mr.provider_account_ref) as account_name,
                coalesce(pa.email, mr.provider_account_email_snapshot) as account_email,
                pa.plan_type as account_plan_type,
                coalesce(mr.upstream_model_id, mr.requested_model_id) as model,
                mr.started_at_us, mr.outcome, mr.input_tokens, mr.output_tokens,
                mr.cached_tokens, mr.cache_write_tokens, mr.reasoning_tokens,
                mr.image_input_tokens, mr.image_output_tokens,
                mr.image_generation_succeeded, mr.total_tokens, mr.cost_source,
                mr.cost_currency, mr.cost_amount
           from model_requests mr left join provider_accounts pa on pa.id = mr.provider_account_ref
          where mr.started_at_us >= ",
    );
    statement.push_bind(query_start_us);
    statement.push(" and mr.started_at_us < ");
    statement.push_bind(range.end.timestamp_micros());
    statement.push(" and mr.provider_account_ref is not null and ");
    statement.push(fact);
    statement.push(" order by mr.started_at_us desc, mr.id desc");
    let mut rows = statement.build().fetch(pool);
    let mut accounts = BTreeMap::<String, AccountUsageAccumulator>::new();
    while let Some(row) = rows.try_next().await.map_err(|_| unavailable())? {
        let account_id: String = row.try_get("account_id").map_err(|_| unavailable())?;
        let started_at_us: i64 = row.try_get("started_at_us").map_err(|_| unavailable())?;
        let started_at = datetime_from_micros(started_at_us)?;
        let in_range = started_at >= range.start && started_at < range.end;
        let in_hourly_timeline = started_at >= bucket_start && started_at < range.end;
        if !in_range {
            if in_hourly_timeline && let Some(account) = accounts.get_mut(&account_id) {
                account.add_hourly_request(started_at)?;
            }
            continue;
        }
        let outcome: String = row.try_get("outcome").map_err(|_| unavailable())?;
        let model: Option<String> = row.try_get("model").map_err(|_| unavailable())?;
        let amount: Option<String> = row.try_get("cost_amount").map_err(|_| unavailable())?;
        let cost_amount = amount
            .map(|value| {
                let decimal = decode_amount(&value)?;
                DecimalAmount::from_str(&decimal.canonical())
            })
            .transpose()?;
        let usage = UsageFact {
            request_count: 1,
            success_count: u64::from(outcome == "succeeded"),
            input_tokens: optional_u64(&row, "input_tokens")?,
            output_tokens: optional_u64(&row, "output_tokens")?,
            cached_tokens: optional_u64(&row, "cached_tokens")?,
            cache_write_tokens: optional_u64(&row, "cache_write_tokens")?,
            reasoning_tokens: optional_u64(&row, "reasoning_tokens")?,
            image_input_tokens: optional_u64(&row, "image_input_tokens")?,
            image_output_tokens: optional_u64(&row, "image_output_tokens")?,
            total_tokens: optional_u64(&row, "total_tokens")?,
            image_generation_succeeded: optional_bool(&row, "image_generation_succeeded")?,
            cost_source: row.try_get("cost_source").map_err(|_| unavailable())?,
            cost_currency: row.try_get("cost_currency").map_err(|_| unavailable())?,
            cost_amount,
        };
        let provider_kind: String = row.try_get("provider_kind").map_err(|_| unavailable())?;
        let authentication_kind: String = row
            .try_get("authentication_kind")
            .map_err(|_| unavailable())?;
        let name: String = row.try_get("account_name").map_err(|_| unavailable())?;
        let email: Option<String> = row.try_get("account_email").map_err(|_| unavailable())?;
        let plan_type: Option<String> = row
            .try_get("account_plan_type")
            .map_err(|_| unavailable())?;
        let account =
            accounts
                .entry(account_id.clone())
                .or_insert_with(|| AccountUsageAccumulator {
                    account_id,
                    provider_kind,
                    authentication_kind,
                    name,
                    email,
                    plan_type,
                    last_used_at: started_at,
                    ..AccountUsageAccumulator::default()
                });
        account.push(&usage, model.as_deref(), started_at, in_hourly_timeline)?;
    }
    let mut items = accounts.into_values().collect::<Vec<_>>();
    items.sort_by(|left, right| {
        right
            .last_used_at
            .cmp(&left.last_used_at)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.account_id.cmp(&right.account_id))
    });
    items.truncate(4);
    items
        .into_iter()
        .map(|mut account| {
            let request_buckets = (0..24)
                .map(|index| {
                    let bucket_at = bucket_start + TimeDelta::hours(index);
                    ProviderAccountRequestBucket {
                        bucket_start: bucket_at,
                        request_count: account
                            .hourly_requests
                            .remove(&bucket_at.timestamp())
                            .unwrap_or_default(),
                    }
                })
                .collect();
            let model_groups = std::mem::take(&mut account.models);
            let mut models = model_groups
                .into_iter()
                .map(|(model, aggregate)| aggregate.into_model(model))
                .collect::<StoreResult<Vec<_>>>()?;
            models.sort_by(|left, right| {
                right
                    .request_count
                    .cmp(&left.request_count)
                    .then_with(|| left.model.cmp(&right.model))
            });
            account.into_observation(request_buckets, models)
        })
        .collect()
}

pub(crate) async fn usage_overview(
    pool: &SqlitePool,
    range: ObservabilityRange,
    filter: &UsageRecordFilter,
) -> StoreResult<UsageOverview> {
    let mut records = metric_records(pool, range, filter);
    let mut aggregate = MetricAccumulator::default();
    let mut attempts = crate::postgres::AttemptMetrics::default();
    let mut providers = BTreeMap::<String, ProviderObservation>::new();
    while let Some(record) = records.try_next().await? {
        aggregate.push(&record, true)?;
        attempts.attempt_count = add_u64(attempts.attempt_count, record.attempt_count)?;
        if record.attempt_count > 0 {
            match record.outcome.as_str() {
                "succeeded" => attempts.success_count = add_u64(attempts.success_count, 1)?,
                "cancelled" => attempts.cancelled_count = add_u64(attempts.cancelled_count, 1)?,
                "incomplete" => attempts.incomplete_count = add_u64(attempts.incomplete_count, 1)?,
                "failed" => {
                    attempts.failure_count = add_u64(attempts.failure_count, 1)?;
                    count_failure_kind(
                        &mut attempts,
                        record.error_kind.as_deref(),
                        record.upstream_status_code.or(record.client_status_code),
                    )?;
                }
                _ => {}
            }
        }
        let provider_kind = record
            .provider_kind
            .as_deref()
            .unwrap_or("unrouted")
            .to_owned();
        let provider = providers
            .entry(provider_kind.clone())
            .or_insert(ProviderObservation {
                provider_kind,
                request_count: 0,
                attempt_count: 0,
                failure_count: 0,
                total_tokens: 0,
            });
        provider.request_count = add_u64(provider.request_count, 1)?;
        provider.attempt_count = add_u64(provider.attempt_count, record.attempt_count)?;
        if record.outcome == "failed" {
            provider.failure_count = add_u64(provider.failure_count, 1)?;
        }
        if record.is_completed_usage()
            && let Some(tokens) = record.total_tokens
        {
            provider.total_tokens = add_u64(provider.total_tokens, tokens)?;
        }
    }
    let mut events = QueryBuilder::<Sqlite>::new(
        "select oe.failure_kind, oe.status_code from ops_events oe
         join model_requests mr on mr.id = oe.model_request_id where ",
    );
    push_range(&mut events, "mr", range);
    events.push(" and mr.recovered_at_us is null");
    push_usage_filter(&mut events, filter, "mr");
    let mut events = events.build().fetch(pool);
    while let Some(row) = events.try_next().await.map_err(|_| unavailable())? {
        let failure_kind: String = row.try_get("failure_kind").map_err(|_| unavailable())?;
        let status_code: Option<i64> = row.try_get("status_code").map_err(|_| unavailable())?;
        attempts.failure_count = add_u64(attempts.failure_count, 1)?;
        count_failure_kind(&mut attempts, Some(&failure_kind), status_code)?;
    }
    let (requests, coverage, costs) = aggregate.finish()?;
    attempts.cost_coverage = coverage;
    attempts.costs = costs;
    let mut providers = providers.into_values().collect::<Vec<_>>();
    providers.sort_by_key(|provider| std::cmp::Reverse(provider.request_count));
    Ok(UsageOverview {
        range,
        requests,
        attempts,
        providers,
    })
}

fn count_failure_kind(
    attempts: &mut crate::postgres::AttemptMetrics,
    error_kind: Option<&str>,
    status_code: Option<i64>,
) -> StoreResult<()> {
    if matches!(error_kind, Some("rate_limited" | "quota_exhausted")) || status_code == Some(429) {
        attempts.rate_limited_count = add_u64(attempts.rate_limited_count, 1)?;
    }
    if matches!(
        error_kind,
        Some("authentication" | "authorization" | "invalid_credential")
    ) || matches!(status_code, Some(401 | 403))
    {
        attempts.auth_failure_count = add_u64(attempts.auth_failure_count, 1)?;
    }
    if status_code.is_some_and(|status| (500..=599).contains(&status)) {
        attempts.provider_5xx_count = add_u64(attempts.provider_5xx_count, 1)?;
    }
    Ok(())
}

pub(crate) async fn usage_diagnostics(
    pool: &SqlitePool,
    range: ObservabilityRange,
    filter: &UsageRecordFilter,
    dimension: DiagnosticDimension,
) -> StoreResult<DiagnosticsObservation> {
    filter.validate()?;
    let mut query = QueryBuilder::<Sqlite>::new("select ");
    query.push(diagnostic_dimension_sql(dimension));
    query.push(
        " as dimension_name, mr.*, account.provider_kind as account_provider_kind,
                account.plan_type as account_plan_type, key.name as api_key_name
           from model_requests mr
           left join provider_accounts account on account.id = mr.provider_account_ref
           left join client_api_keys key on key.id = mr.client_api_key_ref
          where ",
    );
    push_range(&mut query, "mr", range);
    query.push(" and mr.recovered_at_us is null");
    match dimension {
        DiagnosticDimension::Failure => {
            query.push(" and mr.error_kind is not null");
        }
        DiagnosticDimension::Model => {
            query.push(" and coalesce(mr.upstream_model_id, mr.requested_model_id) is not null");
        }
        DiagnosticDimension::Status => {
            query.push(" and coalesce(mr.upstream_status_code, mr.client_status_code) is not null");
        }
        DiagnosticDimension::Account => {
            query.push(
                " and mr.provider_account_ref is not null
                    and mr.provider_kind = 'openai'
                    and coalesce(
                      mr.provider_account_authentication_kind_snapshot,
                      account.authentication_kind
                    ) = 'oauth'",
            );
        }
        DiagnosticDimension::AccountApiKey => {
            query.push(
                " and mr.provider_account_ref is not null
                    and mr.client_api_key_ref is not null
                    and mr.provider_kind = 'openai'
                    and coalesce(
                      mr.provider_account_authentication_kind_snapshot,
                      account.authentication_kind
                    ) = 'oauth'",
            );
        }
        DiagnosticDimension::Provider
        | DiagnosticDimension::ApiKey
        | DiagnosticDimension::Transport => {}
    }
    push_usage_filter(&mut query, filter, "mr");
    query.push(" order by mr.started_at_us desc, mr.id desc");
    let mut rows = query.build().fetch(pool);
    let mut total_request_count = 0;
    let mut groups = BTreeMap::<String, DiagnosticAccumulator>::new();
    while let Some(row) = rows.try_next().await.map_err(|_| unavailable())? {
        total_request_count = add_u64(total_request_count, 1)?;
        let dimension_name: String = row.try_get("dimension_name").map_err(|_| unavailable())?;
        let account_id: Option<String> = row
            .try_get("provider_account_ref")
            .map_err(|_| unavailable())?;
        let client_api_key_id: Option<String> = row
            .try_get("client_api_key_ref")
            .map_err(|_| unavailable())?;
        let key = if dimension == DiagnosticDimension::AccountApiKey {
            let account_id = account_id
                .as_deref()
                .ok_or_else(|| invalid("account-key diagnostic is missing an account ID"))?;
            let client_api_key_id = client_api_key_id
                .as_deref()
                .ok_or_else(|| invalid("account-key diagnostic is missing a client key ID"))?;
            serde_json::to_string(&[account_id, client_api_key_id]).map_err(|_| unavailable())?
        } else {
            dimension_name
        };
        let metric = metric_record_from_row(&row)?;
        let group = groups
            .entry(key.clone())
            .or_insert_with(|| DiagnosticAccumulator {
                display_name: key.clone(),
                account_id: if dimension == DiagnosticDimension::AccountApiKey {
                    account_id.clone()
                } else {
                    None
                },
                client_api_key_id: if dimension == DiagnosticDimension::AccountApiKey {
                    client_api_key_id.clone()
                } else {
                    None
                },
                account_provider_kind: row.try_get("account_provider_kind").unwrap_or(None),
                account_plan_type: row.try_get("account_plan_type").unwrap_or(None),
                ..DiagnosticAccumulator::default()
            });
        if dimension == DiagnosticDimension::Account && !group.has_display_name {
            let email: Option<String> = row
                .try_get("provider_account_email_snapshot")
                .unwrap_or(None);
            let account_name: Option<String> = row
                .try_get("provider_account_name_snapshot")
                .unwrap_or(None);
            if let Some(display_name) = email.or(account_name) {
                group.display_name = display_name;
                group.has_display_name = true;
            }
        } else if dimension == DiagnosticDimension::AccountApiKey {
            if group.account_name.is_none() {
                let email: Option<String> = row
                    .try_get("provider_account_email_snapshot")
                    .unwrap_or(None);
                let account_name: Option<String> = row
                    .try_get("provider_account_name_snapshot")
                    .unwrap_or(None);
                group.account_name = email.or(account_name);
            }
            if group.client_api_key_name.is_none() {
                group.client_api_key_name = row.try_get("api_key_name").unwrap_or(None);
            }
        } else if dimension == DiagnosticDimension::ApiKey && !group.has_display_name {
            let display_name: Option<String> = row.try_get("api_key_name").unwrap_or(None);
            if let Some(display_name) = display_name {
                group.display_name = display_name;
                group.has_display_name = true;
            }
        }
        group.push(&metric)?;
    }
    let mut items = groups
        .into_iter()
        .map(|(key, mut group)| {
            if dimension == DiagnosticDimension::AccountApiKey {
                let account_name = group
                    .account_name
                    .clone()
                    .or_else(|| group.account_id.clone())
                    .unwrap_or_else(|| "未知账号".to_owned());
                let client_api_key_name = group
                    .client_api_key_name
                    .clone()
                    .or_else(|| group.client_api_key_id.clone())
                    .unwrap_or_else(|| "未知密钥".to_owned());
                group.display_name = format!("{account_name} → {client_api_key_name}");
                group.account_name = Some(account_name);
                group.client_api_key_name = Some(client_api_key_name);
            }
            group.into_item(key)
        })
        .collect::<StoreResult<Vec<_>>>()?;
    if dimension == DiagnosticDimension::AccountApiKey {
        items.sort_by(|left, right| {
            left.account_name
                .cmp(&right.account_name)
                .then_with(|| right.total_tokens.cmp(&left.total_tokens))
                .then_with(|| left.client_api_key_name.cmp(&right.client_api_key_name))
        });
    } else {
        items.sort_by(|left, right| {
            right
                .request_count
                .cmp(&left.request_count)
                .then_with(|| left.key.cmp(&right.key))
        });
    }
    if !matches!(
        dimension,
        DiagnosticDimension::Account | DiagnosticDimension::AccountApiKey
    ) {
        items.truncate(100);
    }
    Ok(DiagnosticsObservation {
        total_request_count,
        items,
    })
}

fn diagnostic_dimension_sql(dimension: DiagnosticDimension) -> &'static str {
    match dimension {
        DiagnosticDimension::Provider => "coalesce(mr.provider_kind, 'unrouted')",
        DiagnosticDimension::Model => "coalesce(mr.upstream_model_id, mr.requested_model_id)",
        DiagnosticDimension::Account => "coalesce(mr.provider_account_ref, 'unrouted')",
        DiagnosticDimension::ApiKey => "mr.client_api_key_ref",
        DiagnosticDimension::AccountApiKey => "mr.client_api_key_ref",
        DiagnosticDimension::Transport => {
            "coalesce(mr.upstream_transport, mr.client_transport, 'unknown')"
        }
        DiagnosticDimension::Failure => "coalesce(mr.error_kind, 'none')",
        DiagnosticDimension::Status => {
            "cast(coalesce(mr.upstream_status_code, mr.client_status_code) as text)"
        }
    }
}

#[derive(Default)]
struct DiagnosticAccumulator {
    display_name: String,
    has_display_name: bool,
    account_id: Option<String>,
    account_name: Option<String>,
    client_api_key_id: Option<String>,
    client_api_key_name: Option<String>,
    account_provider_kind: Option<String>,
    account_plan_type: Option<String>,
    request_count: u64,
    success_count: u64,
    failure_count: u64,
    attempt_count: u64,
    total_tokens: u64,
    latency_sum: u64,
    latency_count: u64,
    latencies: Vec<f64>,
    first_token_latencies: Vec<f64>,
    non_completion_count: u64,
    retry_count: u64,
    retried_request_count: u64,
    cost_coverage: CostCoverage,
    costs: BTreeMap<String, gateway_core::metering::Decimal>,
}

impl DiagnosticAccumulator {
    fn push(&mut self, record: &MetricRecord) -> StoreResult<()> {
        self.request_count = add_u64(self.request_count, 1)?;
        self.attempt_count = add_u64(self.attempt_count, record.attempt_count)?;
        match record.outcome.as_str() {
            "succeeded" => self.success_count = add_u64(self.success_count, 1)?,
            "failed" => self.failure_count = add_u64(self.failure_count, 1)?,
            "cancelled" | "incomplete" => {
                self.non_completion_count = add_u64(self.non_completion_count, 1)?
            }
            _ => {}
        }
        self.retry_count = add_u64(self.retry_count, record.attempt_count.saturating_sub(1))?;
        if record.attempt_count > 1 {
            self.retried_request_count = add_u64(self.retried_request_count, 1)?;
        }
        if record.is_completed_usage() {
            if let Some(tokens) = record.total_tokens {
                self.total_tokens = add_u64(self.total_tokens, tokens)?;
            }
            if let Some(latency) = record.latency_ms {
                self.latency_sum = add_u64(self.latency_sum, latency)?;
                self.latency_count = add_u64(self.latency_count, 1)?;
                self.latencies.push(latency as f64);
            }
            if let Some(latency) = record.first_token_ms {
                self.first_token_latencies.push(latency as f64);
            }
            match record.cost_source.as_str() {
                "provider_reported" => {
                    self.cost_coverage.provider_reported_count =
                        add_u64(self.cost_coverage.provider_reported_count, 1)?
                }
                "calculated" => {
                    self.cost_coverage.calculated_count =
                        add_u64(self.cost_coverage.calculated_count, 1)?
                }
                "unavailable" => {
                    self.cost_coverage.unavailable_count =
                        add_u64(self.cost_coverage.unavailable_count, 1)?
                }
                _ => {}
            }
            if let (Some(currency), Some(amount)) = (&record.cost_currency, &record.cost_amount) {
                let total = self
                    .costs
                    .entry(currency.clone())
                    .or_insert(gateway_core::metering::Decimal::ZERO);
                *total = total
                    .checked_add(amount_decimal(amount)?)
                    .ok_or_else(|| invalid("diagnostic cost exceeds numeric(20,10)"))?;
            }
        }
        Ok(())
    }

    fn into_item(self, key: String) -> StoreResult<DiagnosticObservation> {
        let average_latency_ms = (self.latency_count > 0)
            .then(|| (self.latency_sum as f64 / self.latency_count as f64).round() as u64);
        Ok(DiagnosticObservation {
            key,
            name: self.display_name,
            account_id: self.account_id,
            account_name: self.account_name,
            client_api_key_id: self.client_api_key_id,
            client_api_key_name: self.client_api_key_name,
            account_provider_kind: self.account_provider_kind,
            account_plan_type: self.account_plan_type,
            request_count: self.request_count,
            success_count: self.success_count,
            failure_count: self.failure_count,
            attempt_count: self.attempt_count,
            total_tokens: self.total_tokens,
            average_latency_ms,
            latency_p95_ms: rounded_percentile(&self.latencies, 0.95)?,
            first_token_p95_ms: rounded_percentile(&self.first_token_latencies, 0.95)?,
            non_completion_count: self.non_completion_count,
            retry_count: self.retry_count,
            retried_request_count: self.retried_request_count,
            cost_coverage: self.cost_coverage,
            costs: cost_totals(self.costs)?,
        })
    }
}

pub(crate) async fn metric_series(
    pool: &SqlitePool,
    range: ObservabilityRange,
    filter: &UsageRecordFilter,
    timezone: gateway_core::time::DeploymentTimeZone,
    include_costs: bool,
) -> StoreResult<Vec<RequestMetricPoint>> {
    filter.validate()?;
    let granularity = granularity_for(range);
    let mut groups = BTreeMap::<DateTime<Utc>, MetricAccumulator>::new();
    let mut rows = metric_records(pool, range, filter);
    while let Some(row) = rows.try_next().await? {
        let bucket = metric_bucket(row.started_at, granularity, timezone)?;
        groups
            .entry(bucket)
            .or_default()
            .push(&row, include_costs)?;
    }
    let mut points = BTreeMap::new();
    for (bucket_start, aggregate) in groups {
        let (metrics, cost_coverage, costs) = aggregate.finish()?;
        points.insert(
            bucket_start,
            RequestMetricPoint {
                bucket_start,
                granularity,
                metrics,
                cost_coverage,
                costs,
            },
        );
    }
    fill_gaps(range, granularity, timezone, points)
}

fn metric_records<'a>(
    pool: &'a SqlitePool,
    range: ObservabilityRange,
    filter: &'a UsageRecordFilter,
) -> BoxStream<'a, StoreResult<MetricRecord>> {
    Box::pin(async_stream::try_stream! {
        filter.validate()?;
        let mut query = QueryBuilder::<Sqlite>::new(
            "select mr.started_at_us, mr.outcome, mr.attempt_count, mr.error_kind,
                    mr.upstream_status_code, mr.client_status_code, mr.provider_kind,
                    mr.request_kind, mr.client_transport, mr.downstream_committed_at_us,
                    mr.requested_model_id, mr.upstream_model_id, mr.image_generation_requested,
                    mr.input_tokens, mr.output_tokens, mr.cached_tokens, mr.cache_write_tokens,
                    mr.reasoning_tokens, mr.image_input_tokens, mr.image_output_tokens,
                    mr.total_tokens, mr.cost_source, mr.cost_amount, mr.cost_currency,
                    mr.latency_ms, mr.first_token_ms, mr.admission_decision_ms,
                    mr.account_selection_wait_ms, mr.capacity_used_slots, mr.capacity_total_slots
               from model_requests mr where ",
        );
        push_range(&mut query, "mr", range);
        query.push(" and mr.recovered_at_us is null");
        push_usage_filter(&mut query, filter, "mr");
        let mut rows = query.build().fetch(pool);
        while let Some(row) = rows.try_next().await.map_err(|_| unavailable())? {
            yield metric_record_from_row(&row)?;
        }
    })
}

fn metric_record_from_row(row: &SqliteRow) -> StoreResult<MetricRecord> {
    let started_at_us: i64 = row.try_get("started_at_us").map_err(|_| unavailable())?;
    let optional_u64 = |column: &'static str| -> StoreResult<Option<u64>> {
        let value: Option<i64> = row.try_get(column).map_err(|_| unavailable())?;
        value.map(checked_u64).transpose()
    };
    let cost_amount: Option<String> = row.try_get("cost_amount").map_err(|_| unavailable())?;
    let cost_amount = cost_amount
        .map(|value| {
            let decimal = decode_amount(&value)?;
            DecimalAmount::from_str(&decimal.canonical())
        })
        .transpose()?;
    Ok(MetricRecord {
        started_at: datetime_from_micros(started_at_us)?,
        outcome: row.try_get("outcome").map_err(|_| unavailable())?,
        attempt_count: checked_u64(row.try_get("attempt_count").map_err(|_| unavailable())?)?,
        error_kind: row.try_get("error_kind").map_err(|_| unavailable())?,
        upstream_status_code: row
            .try_get("upstream_status_code")
            .map_err(|_| unavailable())?,
        client_status_code: row
            .try_get("client_status_code")
            .map_err(|_| unavailable())?,
        provider_kind: row.try_get("provider_kind").map_err(|_| unavailable())?,
        request_kind: row.try_get("request_kind").map_err(|_| unavailable())?,
        client_transport: row.try_get("client_transport").map_err(|_| unavailable())?,
        downstream_committed: row
            .try_get::<Option<i64>, _>("downstream_committed_at_us")
            .map_err(|_| unavailable())?
            .is_some(),
        requested_model_id: row
            .try_get("requested_model_id")
            .map_err(|_| unavailable())?,
        upstream_model_id: row
            .try_get("upstream_model_id")
            .map_err(|_| unavailable())?,
        image_generation_requested: row
            .try_get::<i64, _>("image_generation_requested")
            .map_err(|_| unavailable())?
            != 0,
        input_tokens: optional_u64("input_tokens")?,
        output_tokens: optional_u64("output_tokens")?,
        cached_tokens: optional_u64("cached_tokens")?,
        cache_write_tokens: optional_u64("cache_write_tokens")?,
        reasoning_tokens: optional_u64("reasoning_tokens")?,
        image_input_tokens: optional_u64("image_input_tokens")?,
        image_output_tokens: optional_u64("image_output_tokens")?,
        total_tokens: optional_u64("total_tokens")?,
        cost_source: row.try_get("cost_source").map_err(|_| unavailable())?,
        cost_amount,
        cost_currency: row.try_get("cost_currency").map_err(|_| unavailable())?,
        latency_ms: optional_u64("latency_ms")?,
        first_token_ms: optional_u64("first_token_ms")?,
        admission_decision_ms: optional_u64("admission_decision_ms")?,
        account_selection_wait_ms: optional_u64("account_selection_wait_ms")?,
        capacity_used_slots: optional_u64("capacity_used_slots")?,
        capacity_total_slots: optional_u64("capacity_total_slots")?,
    })
}

pub(crate) async fn dashboard_totals(pool: &SqlitePool) -> StoreResult<DashboardTotals> {
    let fact = completed_usage_fact_predicate("mr");
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "select count(*) as request_count,
                coalesce(sum(case when {fact} then coalesce(input_tokens, 0) else 0 end), 0) as input_tokens,
                coalesce(sum(case when {fact} then coalesce(cached_tokens, 0) else 0 end), 0) as cached_tokens,
                coalesce(sum(case when {fact} then coalesce(total_tokens, 0) else 0 end), 0) as total_tokens
           from model_requests mr where mr.recovered_at_us is null"
    )))
    .fetch_one(pool)
    .await
    .map_err(|_| unavailable())?;
    let mut billing = gateway_core::metering::Decimal::ZERO;
    let mut has_billing = false;
    let mut costs = sqlx::query(sqlx::AssertSqlSafe(format!(
        "select mr.cost_amount from model_requests mr
          where mr.recovered_at_us is null and ({fact})
            and mr.cost_currency = 'USD' and mr.cost_amount is not null"
    )))
    .fetch(pool);
    while let Some(row) = costs.try_next().await.map_err(|_| unavailable())? {
        let amount: String = row.try_get("cost_amount").map_err(|_| unavailable())?;
        billing = billing
            .checked_add(decode_amount(&amount)?)
            .ok_or_else(|| invalid("dashboard USD total exceeds numeric(20,10)"))?;
        has_billing = true;
    }
    let billing_usd = has_billing
        .then(|| DecimalAmount::from_str(&billing.canonical()))
        .transpose()?;
    Ok(DashboardTotals {
        request_count: checked_u64(row.try_get("request_count").map_err(|_| unavailable())?)?,
        input_tokens: checked_u64(row.try_get("input_tokens").map_err(|_| unavailable())?)?,
        cached_tokens: checked_u64(row.try_get("cached_tokens").map_err(|_| unavailable())?)?,
        total_tokens: checked_u64(row.try_get("total_tokens").map_err(|_| unavailable())?)?,
        billing_usd,
    })
}

pub(crate) fn calculated_billing_facts(
    pool: &SqlitePool,
    range: ObservabilityRange,
    filter: UsageRecordFilter,
    timezone: gateway_core::time::DeploymentTimeZone,
) -> BoxStream<'_, StoreResult<CalculatedUsageBillingFact>> {
    Box::pin(async_stream::try_stream! {
        filter.validate()?;
        let granularity = granularity_for(range);
        let mut query = QueryBuilder::<Sqlite>::new(
            "select mr.started_at_us, mr.provider_kind, mr.upstream_model_id, mr.service_tier,
                    mr.input_tokens, mr.output_tokens, mr.cached_tokens, mr.cache_write_tokens,
                    mr.billing_snapshot_json, mr.cost_currency, mr.cost_amount
               from model_requests mr where ",
        );
        push_range(&mut query, "mr", range);
        query.push(" and mr.cost_source = 'calculated' and mr.cost_amount is not null
                    and mr.cost_currency is not null and mr.provider_kind is not null
                    and mr.upstream_model_id is not null and mr.recovered_at_us is null and ");
        query.push(completed_usage_fact_predicate("mr"));
        push_usage_filter(&mut query, &filter, "mr");
        let mut rows = query.build().fetch(pool);
        while let Some(row) = rows.try_next().await.map_err(|_| unavailable())? {
            let started_at_us: i64 = row.try_get("started_at_us").map_err(|_| unavailable())?;
            let started_at = datetime_from_micros(started_at_us)?;
            let bucket_start = metric_bucket(started_at, granularity, timezone)?;
            let currency: String = row.try_get("cost_currency").map_err(|_| unavailable())?;
            let amount: String = row.try_get("cost_amount").map_err(|_| unavailable())?;
            let decimal = decode_amount(&amount)?;
            let snapshot: Option<String> = row.try_get("billing_snapshot_json").map_err(|_| unavailable())?;
            let billing_snapshot_json = snapshot
                .map(|value| serde_json::from_str(&value).map_err(|_| invalid("stored billing snapshot is malformed")))
                .transpose()?;
            let optional_u64 = |column: &'static str| -> StoreResult<Option<u64>> {
                let value: Option<i64> = row.try_get(column).map_err(|_| unavailable())?;
                value.map(checked_u64).transpose()
            };
            yield CalculatedUsageBillingFact {
                billing_snapshot_json,
                bucket_start,
                provider_kind: row.try_get("provider_kind").map_err(|_| unavailable())?,
                upstream_model_id: row.try_get("upstream_model_id").map_err(|_| unavailable())?,
                service_tier: row.try_get("service_tier").map_err(|_| unavailable())?,
                input_tokens: optional_u64("input_tokens")?,
                output_tokens: optional_u64("output_tokens")?,
                cached_tokens: optional_u64("cached_tokens")?,
                cache_write_tokens: optional_u64("cache_write_tokens")?,
                total: CurrencyCostTotal {
                    currency,
                    amount: DecimalAmount::from_str(&decimal.canonical())?,
                },
            };
        }
    })
}

fn granularity_for(range: ObservabilityRange) -> ObservationGranularity {
    let seconds = range.end.signed_duration_since(range.start).num_seconds();
    if seconds <= 2 * 24 * 60 * 60 {
        ObservationGranularity::FifteenMinutes
    } else if seconds <= 31 * 24 * 60 * 60 {
        ObservationGranularity::Hour
    } else {
        ObservationGranularity::Day
    }
}

fn metric_bucket(
    value: DateTime<Utc>,
    granularity: ObservationGranularity,
    timezone: gateway_core::time::DeploymentTimeZone,
) -> StoreResult<DateTime<Utc>> {
    if granularity == ObservationGranularity::Day {
        return timezone
            .day_start(value)
            .ok_or_else(|| invalid("invalid calendar day"));
    }
    let seconds = granularity.seconds();
    DateTime::from_timestamp(value.timestamp().div_euclid(seconds) * seconds, 0)
        .ok_or_else(|| invalid("invalid metric bucket"))
}

fn fill_gaps(
    range: ObservabilityRange,
    granularity: ObservationGranularity,
    timezone: gateway_core::time::DeploymentTimeZone,
    mut points: BTreeMap<DateTime<Utc>, RequestMetricPoint>,
) -> StoreResult<Vec<RequestMetricPoint>> {
    let mut buckets = Vec::new();
    if granularity == ObservationGranularity::Day {
        let mut day = timezone
            .day_start(range.start)
            .ok_or_else(|| invalid("invalid calendar day"))?;
        while day < range.end {
            buckets.push(day);
            day = timezone
                .days_after(day, 1)
                .ok_or_else(|| invalid("invalid next calendar day"))?;
        }
    } else {
        let seconds = granularity.seconds();
        let mut bucket =
            DateTime::from_timestamp(range.start.timestamp().div_euclid(seconds) * seconds, 0)
                .ok_or_else(|| invalid("invalid metric start"))?;
        while bucket < range.end {
            buckets.push(bucket);
            bucket = bucket
                .checked_add_signed(TimeDelta::seconds(seconds))
                .ok_or_else(|| invalid("invalid next metric bucket"))?;
        }
    }
    buckets
        .into_iter()
        .map(|bucket_start| {
            points.remove(&bucket_start).map_or_else(
                || {
                    Ok(RequestMetricPoint {
                        bucket_start,
                        granularity,
                        metrics: RequestMetrics::default(),
                        cost_coverage: CostCoverage::default(),
                        costs: Vec::new(),
                    })
                },
                Ok,
            )
        })
        .collect()
}

fn percentiles(values: &[f64]) -> StoreResult<LatencyPercentiles> {
    Ok(LatencyPercentiles {
        p50_ms: percentile(values, 0.50)?
            .map(PercentileMilliseconds::new)
            .transpose()?,
        p95_ms: percentile(values, 0.95)?
            .map(PercentileMilliseconds::new)
            .transpose()?,
        p99_ms: percentile(values, 0.99)?
            .map(PercentileMilliseconds::new)
            .transpose()?,
    })
}

fn rounded_percentile(values: &[f64], probability: f64) -> StoreResult<Option<u64>> {
    percentile(values, probability)?.map(round_u64).transpose()
}

fn percentile(values: &[f64], probability: f64) -> StoreResult<Option<f64>> {
    if values.is_empty() {
        return Ok(None);
    }
    let mut ordered = values.to_vec();
    ordered.sort_by(f64::total_cmp);
    let position = (ordered.len() - 1) as f64 * probability;
    let low = position.floor() as usize;
    let high = position.ceil() as usize;
    let fraction = position - low as f64;
    Ok(Some(
        ordered[low] + (ordered[high] - ordered[low]) * fraction,
    ))
}

fn round_u64(value: f64) -> StoreResult<u64> {
    if !value.is_finite() || value < 0.0 || value.round() > u64::MAX as f64 {
        return Err(invalid("metric value exceeds supported range"));
    }
    Ok(value.round() as u64)
}

fn add_u64(left: u64, right: u64) -> StoreResult<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid("metric sum exceeds supported range"))
}

fn add_optional(target: &mut u64, value: Option<u64>) -> StoreResult<()> {
    if let Some(value) = value {
        *target = add_u64(*target, value)?;
    }
    Ok(())
}

fn amount_decimal(amount: &DecimalAmount) -> StoreResult<gateway_core::metering::Decimal> {
    amount
        .as_str()
        .parse()
        .map_err(|_| invalid("stored amount is malformed"))
}
