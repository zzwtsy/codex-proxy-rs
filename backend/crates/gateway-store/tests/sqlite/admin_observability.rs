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

#[tokio::test]
async fn sqlite_account_diagnostics_include_non_oauth_and_unrouted_requests() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("codex-account-diagnostics.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let now = Utc::now();
    let now_us = now.timestamp_micros();
    let range = TimeRange::new(now - TimeDelta::hours(1), now + TimeDelta::minutes(1))
        .expect("valid usage range");

    for (id, provider, authentication) in [
        ("codex-one", "openai", "oauth"),
        ("codex-two", "openai", "oauth"),
        ("openai-api-key", "openai", "api_key"),
        ("other-provider", "anthropic", "oauth"),
    ] {
        insert_provider_account(&pool, id, provider, authentication, now_us).await;
    }

    for (id, name, key) in [
        ("key-alpha", "Alpha", "synthetic-alpha-key"),
        ("key-beta", "Beta", "synthetic-beta-key"),
    ] {
        sqlx::query(
            "insert into client_api_keys (id, name, key, created_at_us, updated_at_us)
             values (?1, ?2, ?3, ?4, ?4)",
        )
        .bind(id)
        .bind(name)
        .bind(key)
        .bind(now_us)
        .execute(&pool)
        .await
        .expect("insert diagnostic client key");
    }

    for (id, account, authentication, client_key, outcome, tokens) in [
        (
            "codex-one-a",
            Some("codex-one"),
            Some("oauth"),
            "key-alpha",
            "succeeded",
            60,
        ),
        (
            "codex-one-b",
            Some("codex-one"),
            Some("oauth"),
            "key-beta",
            "succeeded",
            40,
        ),
        (
            "codex-one-failed",
            Some("codex-one"),
            Some("oauth"),
            "key-beta",
            "failed",
            999,
        ),
        (
            "codex-two",
            Some("codex-two"),
            None,
            "key-alpha",
            "succeeded",
            100,
        ),
        (
            "api-key-request",
            Some("openai-api-key"),
            None,
            "key-alpha",
            "succeeded",
            900,
        ),
        (
            "other-provider-request",
            Some("other-provider"),
            Some("oauth"),
            "key-alpha",
            "succeeded",
            900,
        ),
        (
            "unlinked-request",
            None,
            None,
            "key-alpha",
            "succeeded",
            900,
        ),
    ] {
        insert_request(
            &pool,
            RequestFixture {
                id,
                outcome,
                model: Some("gpt-test"),
                cost_source: None,
                cost_amount: None,
                cost_currency: None,
                status: Some(if outcome == "succeeded" { 200 } else { 502 }),
                total_tokens: Some(tokens),
                error_kind: (outcome == "failed").then_some("upstream_error"),
                started_at_us: now_us,
                completed_at_us: now_us,
            },
        )
        .await;
        sqlx::query(
            "update model_requests
             set client_api_key_ref = ?1, provider_kind = 'openai', provider_account_ref = ?2,
                 provider_account_authentication_kind_snapshot = ?3
             where id = ?4",
        )
        .bind(client_key)
        .bind(account)
        .bind(authentication)
        .bind(id)
        .execute(&pool)
        .await
        .expect("assign request account facts");
        if id == "other-provider-request" {
            sqlx::query("update model_requests set provider_kind = 'anthropic' where id = ?1")
                .bind(id)
                .execute(&pool)
                .await
                .expect("set other provider");
        }
    }

    let cooldowns: Arc<dyn ProviderCooldownPort> =
        Arc::new(SqliteProviderCooldownRepository::new(pool.clone()));
    let store = SqliteAdminObservabilityStore::new(pool.clone(), cooldowns);
    let diagnostics = store
        .usage_diagnostics(range, UsageFilter::default(), DiagnosticDimension::Account)
        .await
        .expect("account diagnostics");

    assert_eq!(diagnostics.total_request_count, 7);
    assert_eq!(diagnostics.items.len(), 5);
    let account_request_counts = diagnostics
        .items
        .iter()
        .map(|item| (item.key.as_str(), item.request_count))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        account_request_counts,
        std::collections::BTreeMap::from([
            ("codex-one", 3),
            ("codex-two", 1),
            ("openai-api-key", 1),
            ("other-provider", 1),
            ("unrouted", 1),
        ])
    );
    let codex_one = diagnostics
        .items
        .iter()
        .find(|item| item.key == "codex-one")
        .expect("first Codex account");
    assert_eq!(codex_one.request_count, 3);
    assert_eq!(codex_one.total_tokens, 100);
    assert_eq!(codex_one.account_id, None);
    assert_eq!(codex_one.client_api_key_id, None);
    let codex_two = diagnostics
        .items
        .iter()
        .find(|item| item.key == "codex-two")
        .expect("second Codex account");
    assert_eq!(codex_two.total_tokens, 100);

    let account_keys = store
        .usage_diagnostics(
            range,
            UsageFilter::default(),
            DiagnosticDimension::AccountApiKey,
        )
        .await
        .expect("OpenAI OAuth account-key diagnostics");
    assert_eq!(account_keys.total_request_count, 4);
    assert_eq!(account_keys.items.len(), 3);
    let alpha_one = account_keys
        .items
        .iter()
        .find(|item| {
            item.account_id.as_deref() == Some("codex-one")
                && item.client_api_key_id.as_deref() == Some("key-alpha")
        })
        .expect("shared key in first Codex account");
    assert_eq!(alpha_one.total_tokens, 60);
    assert_eq!(alpha_one.account_name.as_deref(), Some("codex-one"));
    assert_eq!(alpha_one.client_api_key_name.as_deref(), Some("Alpha"));
    let beta_one = account_keys
        .items
        .iter()
        .find(|item| {
            item.account_id.as_deref() == Some("codex-one")
                && item.client_api_key_id.as_deref() == Some("key-beta")
        })
        .expect("second key in first Codex account");
    assert_eq!(beta_one.total_tokens, 40);
    let alpha_two = account_keys
        .items
        .iter()
        .find(|item| {
            item.account_id.as_deref() == Some("codex-two")
                && item.client_api_key_id.as_deref() == Some("key-alpha")
        })
        .expect("same key in second Codex account");
    assert_eq!(alpha_two.total_tokens, 100);
    pool.close().await;
}

async fn insert_provider_account(
    pool: &sqlx::SqlitePool,
    id: &str,
    provider: &str,
    authentication: &str,
    now_us: i64,
) {
    sqlx::query(
        "insert into provider_accounts (
           id, provider_kind, name, authentication_kind, provider_credentials_json,
           credential_revision, has_refresh_token, enabled, weight, model_access_json,
           credential_state, credential_observed_at_us, quota_access_state,
           quota_access_observed_at_us, created_at_us, updated_at_us
         ) values (?1, ?2, ?1, ?3, '{}', 1, 0, 1, 1, '{\"mode\":\"all\",\"models\":[]}',
                   'ready', ?4, 'allowed', ?4, ?4, ?4)",
    )
    .bind(id)
    .bind(provider)
    .bind(authentication)
    .bind(now_us)
    .execute(pool)
    .await
    .expect("insert provider account");
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
