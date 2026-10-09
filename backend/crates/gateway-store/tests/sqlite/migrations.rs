//! 验证已发布 SQLite schema 升级的数据保全与失败回滚

use sqlx::{
    Column, Row, SqlitePool, TypeInfo, ValueRef,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow},
};
use std::str::FromStr;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations/sqlite");

async fn legacy_database(path: &std::path::Path) -> SqlitePool {
    let options = SqliteConnectOptions::from_str(path.to_str().unwrap())
        .unwrap()
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    MIGRATOR.run_to(15, &pool).await.unwrap();
    sqlx::raw_sql(r#"
insert into model_requests (
 id, client_api_key_ref, config_revision, protocol, operation, endpoint, client_transport,
 requested_model_id, provider_kind, upstream_model_id, upstream_response_model,
 provider_account_ref, provider_account_name_snapshot, provider_account_email_snapshot,
 provider_account_authentication_kind_snapshot, upstream_transport, http_version, websocket_pool,
 provider_observation_json, attempt_count, upstream_send_state, outcome, client_status_code,
 upstream_status_code, client_response_id, upstream_request_id, upstream_response_id,
 error_kind, provider_error_code, error_message, error_details, retry_after_ms,
 input_tokens, output_tokens, cached_tokens, total_tokens, cost_source, cost_amount, cost_currency,
 transport_decision_wait_ms, connect_ms, headers_ms, first_event_ms, first_reasoning_ms,
 first_text_ms, first_token_ms, provider_processing_ms, latency_ms,
 client_ip, user_agent, reasoning_effort, reasoning_preset, subagent_kind, compact,
 started_at_us, deadline_at_us, completed_at_us, routing_scope,
 routing_group_refs_json, routing_group_names_snapshot_json,
 admission_decision_ms, account_selection_wait_ms, capacity_used_slots, capacity_total_slots,
 continuation_affinity_hash, continuation_previous_response_id_hash, continuation_requested,
 continuation_unavailable_reason, upstream_connection_id, upstream_connection_exit_reason,
 upstream_connection_age_ms, upstream_connection_idle_ms,
 recovery_request_id, recovered_at_us, recovery_attempt_count, recovery_retry_delay_ms,
 recovery_total_latency_ms, diagnostic_trace_json
) values (
 'historical-request', 'deleted-key', 7, 'openai', 'generate', '/v1/responses', 'websocket',
 'coding', 'openai', 'gpt-5-codex', 'response-model',
 'deleted-account', '历史账号', 'history@example.invalid', 'oauth', 'websocket', 'HTTP/2', 'reuse',
 '{"vendor":true}', 1, 'sent', 'failed', 502, 429, x'726573706f6e7365', 'upstream-request', x'726573706f6e7365',
 'continuation_recovery_required', 'rate_limit', 'safe error', '{"stage":"fixture"}', 0,
 10, 20, 5, 30, 'provider_reported', '00000000000123456789', 'USD',
 0, 2, 3, 4, 5, 6, 5, 7, 1000,
 '192.0.2.10', 'fixture', 'high', 'balanced', 'review', 1,
 1000000, 3000000, 2000000, 'groups', '["deleted-group"]', '["历史组"]',
 0, 8, 0, 20,
 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', 1,
 'missing_response', 'connection', 'peer_closed', 10, 0,
 'recovery-request', 3000000, 1, 0, 1000, '{"attempts":[]}'
);
insert into ops_events (id, model_request_id, attempt_index, level, component, operation,
 failure_kind, message, created_at_us, error_details)
values ('historical-event', 'historical-request', 1, 'warning', 'provider', 'generate',
 'upstream_error', '历史诊断', 2000000, '{"stage":"fixture"}');
insert into credential_lease_counters (scope, resource_fingerprint, fencing_token, last_started_at_us)
values ('account', 'historical-fingerprint', 9, 1000000);
insert into credential_leases (scope, resource_fingerprint, lease_id, owner_fingerprint, fencing_token, expires_at_us)
values ('account', 'historical-fingerprint', 'historical-lease', 'historical-owner', 9, 9000000);
"#).execute(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn upgrades_preserve_all_request_fields_events_and_lease_fences() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("historical.sqlite3");
    let pool = legacy_database(&path).await;
    let before = sqlx::query("select * from model_requests where id = 'historical-request'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let columns = before
        .columns()
        .iter()
        .map(|column| column.name().to_owned())
        .collect::<Vec<_>>();
    MIGRATOR.run(&pool).await.unwrap();
    let after =
        sqlx::query("select * from model_request_observations where id = 'historical-request'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        request_facts(&after, &columns),
        request_facts(&before, &columns)
    );
    let event: (String, String, String) = sqlx::query_as("select model_request_id, message, error_details from ops_events where id = 'historical-event'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(
        event,
        (
            "historical-request".into(),
            "历史诊断".into(),
            "{\"stage\":\"fixture\"}".into()
        )
    );
    let lease: (i64, i64) = sqlx::query_as("select c.fencing_token, l.fencing_token from credential_lease_counters c join credential_leases l using(scope, resource_fingerprint) where l.lease_id = 'historical-lease'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(lease, (9, 9));
    assert!(
        sqlx::query("pragma foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    let unknown: (Option<i64>, Option<f64>) = sqlx::query_as("select upstream_response_ms, upstream_engine_ms from model_request_observations where id = 'historical-request'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(unknown, (None, None));
    pool.close().await;
}

#[tokio::test]
async fn rejected_observation_upgrade_rolls_back_requests_and_events() {
    let root = tempfile::tempdir().unwrap();
    let pool = legacy_database(&root.path().join("rollback.sqlite3")).await;
    // 旧 schema 允许空协议，新观测合同拒绝该行，验证失败不会留下半套表
    sqlx::query("update model_requests set protocol = '' where id = 'historical-request'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(MIGRATOR.run(&pool).await.is_err());
    let protocol: String =
        sqlx::query_scalar("select protocol from model_requests where id = 'historical-request'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(protocol, "");
    let events: i64 =
        sqlx::query_scalar("select count(*) from ops_events where id = 'historical-event'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(events, 1);
    let applied: i64 =
        sqlx::query_scalar("select count(*) from _sqlx_migrations where version = 16")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(applied, 0);
    assert!(
        sqlx::query("pragma foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    pool.close().await;
}

fn request_facts(row: &SqliteRow, columns: &[String]) -> serde_json::Value {
    let facts = columns
        .iter()
        .map(|name| {
            let raw = row.try_get_raw(name.as_str()).unwrap();
            let value = if raw.is_null() {
                serde_json::Value::Null
            } else {
                match raw.type_info().name() {
                    "INTEGER" => serde_json::json!(row.try_get::<i64, _>(name.as_str()).unwrap()),
                    "REAL" => serde_json::json!(row.try_get::<f64, _>(name.as_str()).unwrap()),
                    "BLOB" => serde_json::json!(row.try_get::<Vec<u8>, _>(name.as_str()).unwrap()),
                    "TEXT" => serde_json::json!(row.try_get::<String, _>(name.as_str()).unwrap()),
                    other => panic!("unexpected SQLite fact type: {other}"),
                }
            };
            (name.clone(), value)
        })
        .collect::<serde_json::Map<_, _>>();
    serde_json::Value::Object(facts)
}
