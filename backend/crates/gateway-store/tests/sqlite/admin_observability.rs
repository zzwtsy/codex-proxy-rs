use std::sync::Arc;

use chrono::{TimeDelta, Utc};
use futures::TryStreamExt;
use gateway_admin::{
    model::{
        PageSize,
        observability::{
            DiagnosticDimension, OpsErrorFilter, OpsErrorQuery, TimeRange, UsageFilter, UsageQuery,
        },
    },
    ports::store::ObservabilityStore,
};
use gateway_core::provider_ports::ProviderCooldownPort;
use gateway_store::{
    SqliteStoreConfig, sqlite,
    sqlite::{SqliteAdminObservabilityStore, SqliteProviderCooldownRepository},
};

#[tokio::test]
async fn sqlite_admin_observability_reads_metrics_usage_diagnostics_and_ops_errors() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("admin-observability.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let now = Utc::now();
    let range = TimeRange::new(now - TimeDelta::hours(2), now + TimeDelta::minutes(1))
        .expect("valid usage range");
    let started_at_us = (now - TimeDelta::minutes(2)).timestamp_micros();
    let completed_at_us = now.timestamp_micros();

    insert_request(
        &pool,
        RequestFixture {
            id: "req_obs_success",
            outcome: "succeeded",
            model: Some("gpt-test"),
            cost_source: Some("provider_reported"),
            cost_amount: Some("00000000001000000001"),
            cost_currency: Some("USD"),
            status: Some(200),
            total_tokens: Some(48),
            error_kind: None,
            started_at_us,
            completed_at_us,
        },
    )
    .await;
    insert_request(
        &pool,
        RequestFixture {
            id: "req_obs_failure",
            outcome: "failed",
            model: Some("gpt-test"),
            cost_source: None,
            cost_amount: None,
            cost_currency: None,
            status: Some(502),
            total_tokens: None,
            error_kind: Some("upstream_error"),
            started_at_us: started_at_us + 10,
            completed_at_us: completed_at_us + 10,
        },
    )
    .await;
    insert_request(
        &pool,
        RequestFixture {
            id: "req_obs_calculated",
            outcome: "succeeded",
            model: Some("claude-3"),
            cost_source: Some("calculated"),
            cost_amount: Some("00000000002000000002"),
            cost_currency: Some("USD"),
            status: Some(200),
            total_tokens: Some(10),
            error_kind: None,
            started_at_us: started_at_us + 20,
            completed_at_us: completed_at_us + 20,
        },
    )
    .await;
    sqlx::query(
        "update model_requests set provider_kind = 'anthropic', upstream_model_id = 'claude-3',
                                   billing_snapshot_json = '{}' where id = 'req_obs_calculated'",
    )
    .execute(&pool)
    .await
    .expect("set calculated billing facts");
    sqlx::query(
        "insert into ops_events (
           id, model_request_id, attempt_index, level, component, operation,
           failure_kind, message, created_at_us
         ) values ('ops_obs_failure', 'req_obs_failure', 1, 'error', 'provider',
                   'responses.create', 'upstream_error', 'upstream failed', ?1)",
    )
    .bind(completed_at_us + 10)
    .execute(&pool)
    .await
    .expect("insert ops event");

    let cooldowns: Arc<dyn ProviderCooldownPort> =
        Arc::new(SqliteProviderCooldownRepository::new(pool.clone()));
    let store = SqliteAdminObservabilityStore::new(pool.clone(), cooldowns);

    let dashboard = store
        .dashboard_summary(range, now)
        .await
        .expect("dashboard summary");
    assert_eq!(dashboard.totals.request_count, 3);
    assert_eq!(dashboard.totals.total_tokens, 58);
    assert_eq!(
        dashboard
            .trend
            .iter()
            .map(|point| point.metrics.request_count)
            .sum::<u64>(),
        3
    );
    assert_eq!(dashboard.recent_requests.len(), 2);
    assert_eq!(dashboard.recent_requests[0].id, "req_obs_calculated");
    assert_eq!(dashboard.recent_requests[1].id, "req_obs_success");

    let page = store
        .list_usage_records(UsageQuery {
            range,
            filter: UsageFilter {
                search: Some("req_obs_success".to_owned()),
                ..UsageFilter::default()
            },
            current_page: 1,
            page_size: PageSize::new(20).expect("valid page size"),
        })
        .await
        .expect("usage page with prefix search");
    assert_eq!(page.total, 1);
    assert_eq!(page.items[0].id, "req_obs_success");
    assert_eq!(
        page.items[0]
            .cost_amount
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("0.1000000001")
    );

    let usage_trend = store
        .usage_trend(range, UsageFilter::default())
        .await
        .expect("usage trend with precise currency totals");
    let usd_total = usage_trend
        .iter()
        .flat_map(|point| point.costs.iter())
        .find(|cost| cost.currency == "USD")
        .expect("USD cost bucket");
    assert_eq!(usd_total.amount.to_string(), "0.3000000003");

    let mut billing_facts = store.usage_calculated_billing_facts(range, UsageFilter::default());
    let calculated_fact = billing_facts
        .try_next()
        .await
        .expect("load calculated fact")
        .expect("one calculated fact");
    assert_eq!(calculated_fact.provider_kind, "anthropic");
    assert_eq!(calculated_fact.total.amount.to_string(), "0.2000000002");
    assert!(
        billing_facts
            .try_next()
            .await
            .expect("finish billing fact stream")
            .is_none()
    );

    let overview = store
        .usage_summary(range, UsageFilter::default())
        .await
        .expect("usage overview");
    assert_eq!(overview.requests.request_count, 3);
    assert_eq!(overview.requests.total_tokens, 58);
    assert_eq!(overview.attempts.failure_count, 1);
    assert_eq!(overview.providers.len(), 2);
    assert_eq!(overview.providers[0].provider_kind, "unrouted");
    assert_eq!(overview.providers[0].request_count, 2);

    let diagnostics = store
        .usage_diagnostics(range, UsageFilter::default(), DiagnosticDimension::Provider)
        .await
        .expect("provider diagnostics");
    assert_eq!(diagnostics.total_request_count, 3);
    assert_eq!(diagnostics.items.len(), 2);
    let unrouted = diagnostics
        .items
        .iter()
        .find(|item| item.key == "unrouted")
        .expect("unrouted provider diagnostic");
    assert_eq!(unrouted.request_count, 2);
    assert_eq!(unrouted.failure_count, 1);

    let errors = store
        .list_ops_errors(OpsErrorQuery {
            range,
            filter: OpsErrorFilter::default(),
            current_page: 1,
            page_size: PageSize::new(20).expect("valid page size"),
        })
        .await
        .expect("ops errors");
    assert_eq!(errors.total, 2);

    let detail = store
        .usage_record_detail("req_obs_failure")
        .await
        .expect("request detail");
    assert_eq!(detail.request.id, "req_obs_failure");
    assert_eq!(detail.attempts.len(), 1);
    assert_eq!(detail.attempts[0].source, "ops_event");
    pool.close().await;
}

struct RequestFixture<'a> {
    id: &'a str,
    outcome: &'a str,
    model: Option<&'a str>,
    cost_source: Option<&'a str>,
    cost_amount: Option<&'a str>,
    cost_currency: Option<&'a str>,
    status: Option<i64>,
    total_tokens: Option<i64>,
    error_kind: Option<&'a str>,
    started_at_us: i64,
    completed_at_us: i64,
}

async fn insert_request(pool: &sqlx::SqlitePool, fixture: RequestFixture<'_>) {
    let RequestFixture {
        id,
        outcome,
        model,
        cost_source,
        cost_amount,
        cost_currency,
        status,
        total_tokens,
        error_kind,
        started_at_us,
        completed_at_us,
    } = fixture;
    let usage = outcome == "succeeded";
    sqlx::query(
        "insert into model_requests (
           id, client_api_key_ref, config_revision, protocol, operation, endpoint,
           client_transport, requested_model_id, outcome, client_status_code,
           error_kind, input_tokens, output_tokens, total_tokens,
           downstream_committed_at_us, cost_source, cost_amount, cost_currency,
           latency_ms, first_token_ms, started_at_us, deadline_at_us, completed_at_us,
           routing_scope
         ) values (
           ?1, 'key-observability', 1, 'openai', 'responses.create', '/v1/responses',
           'http', ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
           ?13, ?14, ?15, ?16, ?17, 'legacy_provider'
         )",
    )
    .bind(id)
    .bind(model)
    .bind(outcome)
    .bind(status)
    .bind(error_kind)
    .bind(usage.then_some(31_i64))
    .bind(usage.then_some(17_i64))
    .bind(total_tokens)
    .bind(usage.then_some(completed_at_us))
    .bind(cost_source.unwrap_or("unavailable"))
    .bind(cost_amount)
    .bind(cost_currency)
    .bind(usage.then_some(220_i64))
    .bind(usage.then_some(80_i64))
    .bind(started_at_us)
    .bind(started_at_us + 60_000_000)
    .bind(completed_at_us)
    .execute(pool)
    .await
    .expect("insert model request");
}
