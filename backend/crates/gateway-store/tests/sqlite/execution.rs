use std::{
    num::NonZeroU32,
    time::{Duration, SystemTime},
};

use gateway_core::{
    account::ProviderAccountId,
    engine::{
        AttemptRecord, AttemptTrigger, ExecutionOutcome, ExecutionStore, ModelRequestFinalization,
        ModelRequestId, ModelRequestTimings, NewModelRequest,
    },
    identity::ProviderKind,
    metering::{CalculatedCost, CostEstimate, Usage},
    operation::OperationKind,
    policy::ClientApiKeyId,
    routing::{AccountRoutingSnapshot, ConfigRevision, PublicModelId, UpstreamModelId},
    upstream::UpstreamSendState,
};
use gateway_store::{SqliteStoreConfig, sqlite, sqlite::SqliteExecutionStore};
use sqlx::SqlitePool;

#[tokio::test]
async fn sqlite_merged_first_attempt_is_atomic_and_retries_keep_sent_state() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = database(&root.path().join("execution-atomic.sqlite3")).await;
    let store = SqliteExecutionStore::new(pool.clone());

    let request = request("req_atomic_merged", SystemTime::now());
    assert!(
        store
            .create_model_request_with_attempt(
                request.clone(),
                attempt(&request.id, 2, "acct_atomic")
            )
            .await
            .is_err()
    );
    let rolled_back: i64 =
        sqlx::query_scalar("select count(*) from model_requests where id = 'req_atomic_merged'")
            .fetch_one(&pool)
            .await
            .expect("count rolled-back request");
    assert_eq!(rolled_back, 0);

    store
        .create_model_request_with_attempt(request.clone(), attempt(&request.id, 1, "acct_a"))
        .await
        .expect("create request and first attempt in one transaction");
    let persisted: (i64, String) = sqlx::query_as(
        "select attempt_count, upstream_send_state from model_requests where id = ?1",
    )
    .bind(request.id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load merged request");
    assert_eq!(persisted, (1, "not_sent".to_owned()));

    store
        .mark_send_state(&request.id, UpstreamSendState::Sent)
        .await
        .expect("record upstream send");
    store
        .record_attempt(attempt(&request.id, 2, "acct_b"))
        .await
        .expect("record retry");
    let persisted: (i64, String, Option<String>) = sqlx::query_as(
        "select attempt_count, upstream_send_state, provider_account_ref
         from model_requests where id = ?1",
    )
    .bind(request.id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load retried request");
    assert_eq!(persisted, (2, "sent".to_owned(), Some("acct_b".to_owned())));
    pool.close().await;
}

#[tokio::test]
async fn sqlite_running_request_lease_is_renewed_without_a_request_timeout() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = database(&root.path().join("execution-lease.sqlite3")).await;
    let store = SqliteExecutionStore::new(pool.clone());
    let mut request = request("req_renewed_lease", SystemTime::now());
    request.deadline_at = Default::default();
    store
        .create_model_request(request.clone())
        .await
        .expect("create unbounded request");

    let stale_deadline = SystemTime::now() + Duration::from_secs(1);
    let stale_deadline_us =
        chrono::DateTime::<chrono::Utc>::from(stale_deadline).timestamp_micros();
    sqlx::query("update model_requests set deadline_at_us = ?2 where id = ?1")
        .bind(request.id.as_str())
        .bind(stale_deadline_us)
        .execute(&pool)
        .await
        .expect("shorten recovery lease for renewal check");

    let _lease = store.maintain_request(&request.id, request.deadline_at);
    let renewed_deadline_us = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let deadline_us: i64 =
                sqlx::query_scalar("select deadline_at_us from model_requests where id = ?1")
                    .bind(request.id.as_str())
                    .fetch_one(&pool)
                    .await
                    .expect("load request recovery lease");
            if deadline_us > stale_deadline_us {
                break deadline_us;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request recovery lease is renewed promptly");
    assert!(renewed_deadline_us > stale_deadline_us + 9 * 60 * 1_000_000);
    pool.close().await;
}

#[tokio::test]
async fn sqlite_downstream_commit_status_is_not_replaced_by_finalization() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = database(&root.path().join("execution-commit.sqlite3")).await;
    let store = SqliteExecutionStore::new(pool.clone());
    let request = request("req_commit_status", SystemTime::now());
    store
        .create_model_request_with_attempt(request.clone(), attempt(&request.id, 1, "acct_commit"))
        .await
        .expect("create request");

    let committed_at = SystemTime::now();
    store
        .mark_downstream_committed(&request.id, committed_at, Some(202))
        .await
        .expect("record downstream commit");
    assert!(
        store
            .mark_downstream_committed(&request.id, SystemTime::now(), Some(201))
            .await
            .is_err()
    );

    let mut terminal = make_finalization(&request.id, ExecutionOutcome::Succeeded);
    terminal.downstream_committed_at = Some(committed_at);
    terminal.client_status_code = Some(500);
    store
        .finalize_model_request(terminal)
        .await
        .expect("finalize request");
    let persisted: (Option<i64>, Option<i64>) = sqlx::query_as(
        "select downstream_committed_at_us, client_status_code
         from model_requests where id = ?1",
    )
    .bind(request.id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load terminal request");
    assert!(persisted.0.is_some());
    assert_eq!(persisted.1, Some(202));
    assert!(
        store
            .finalize_model_request(make_finalization(&request.id, ExecutionOutcome::Succeeded,))
            .await
            .is_err()
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_finalization_keeps_fixed_point_cost_and_rejects_duplicate_terminal_write() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = database(&root.path().join("execution-cost.sqlite3")).await;
    let store = SqliteExecutionStore::new(pool.clone());
    let request = request("req_exact_cost", SystemTime::now());
    store
        .create_model_request_with_attempt(request.clone(), attempt(&request.id, 1, "acct_cost"))
        .await
        .expect("create request");

    let mut terminal = make_finalization(&request.id, ExecutionOutcome::Succeeded);
    terminal.usage = Usage {
        input_tokens: Some(31),
        output_tokens: Some(17),
        total_tokens: Some(48),
        ..Usage::new()
    };
    terminal.cost = CalculatedCost::from_usd_ticks(12_345)
        .expect("ten decimal places")
        .into_estimate();
    store
        .finalize_model_request(terminal)
        .await
        .expect("persist terminal usage and cost");

    let persisted: (
        String,
        String,
        String,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    ) = sqlx::query_as(
        "select cost_source, cost_amount, cost_currency,
                    input_tokens, output_tokens, total_tokens
             from model_requests where id = ?1",
    )
    .bind(request.id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load terminal usage");
    assert_eq!(
        persisted,
        (
            "calculated".to_owned(),
            "00000000000000012345".to_owned(),
            "USD".to_owned(),
            Some(31),
            Some(17),
            Some(48),
        )
    );

    assert!(
        store
            .finalize_model_request(make_finalization(&request.id, ExecutionOutcome::Succeeded,))
            .await
            .is_err()
    );
    pool.close().await;
}

#[tokio::test]
async fn sqlite_deadline_recovery_and_continuation_retry_are_persisted() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = database(&root.path().join("execution-recovery.sqlite3")).await;
    let store = SqliteExecutionStore::new(pool.clone());
    let now = SystemTime::now();

    let expired = request("req_expired", now - std::time::Duration::from_secs(60));
    store
        .create_model_request(expired.clone())
        .await
        .expect("create expired request");
    let first_recovery = store
        .recover_expired(now)
        .await
        .expect("recover expired request");
    let second_recovery = store
        .recover_expired(now)
        .await
        .expect("recovery is idempotent");
    assert_eq!(first_recovery.requests, 1);
    assert_eq!(second_recovery.requests, 0);
    let recovered: (String, i64, Option<i64>) = sqlx::query_as(
        "select outcome, completed_at_us, started_at_us from model_requests where id = ?1",
    )
    .bind(expired.id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load recovered request");
    assert_eq!(recovered.0, "incomplete");
    assert!(recovered.1 >= recovered.2.expect("start time"));

    let affinity = "a".repeat(64);
    let mut original = request("req_continuation_original", now);
    original.continuation.affinity_hash = Some(affinity.clone());
    original.continuation.requested = true;
    store
        .create_model_request_with_attempt(
            original.clone(),
            attempt(&original.id, 1, "acct_continuation"),
        )
        .await
        .expect("create original continuation request");
    let mut failed = make_finalization(&original.id, ExecutionOutcome::Failed);
    failed.completed_at = now + std::time::Duration::from_secs(1);
    failed.failure_observation.continuation_unavailable_reason =
        Some("previous_response_not_found".to_owned());
    store
        .finalize_model_request(failed)
        .await
        .expect("finalize original continuation failure");

    let retry_start = now + std::time::Duration::from_secs(3);
    let mut retry = request("req_continuation_retry", retry_start);
    retry.continuation.affinity_hash = Some(affinity);
    store
        .create_model_request_with_attempt(
            retry.clone(),
            attempt(&retry.id, 1, "acct_continuation"),
        )
        .await
        .expect("create retry request");
    let mut succeeded = make_finalization(&retry.id, ExecutionOutcome::Succeeded);
    succeeded.completed_at = retry_start + std::time::Duration::from_secs(1);
    store
        .finalize_model_request(succeeded)
        .await
        .expect("finalize successful continuation retry");

    let recovery: (Option<String>, Option<i64>, Option<i64>, i64) = sqlx::query_as(
        "select recovery_request_id, recovered_at_us, recovery_retry_delay_ms,
                recovery_attempt_count
         from model_requests where id = ?1",
    )
    .bind(original.id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load continuation recovery");
    assert_eq!(recovery.0.as_deref(), Some(retry.id.as_str()));
    assert!(recovery.1.is_some());
    assert_eq!(recovery.2, Some(2_000));
    assert_eq!(recovery.3, 1);
    pool.close().await;
}

async fn database(path: &std::path::Path) -> SqlitePool {
    sqlite::connect_and_migrate(path, &SqliteStoreConfig::default())
        .await
        .expect("create SQLite test database")
}

fn request(id: &str, started_at: SystemTime) -> NewModelRequest {
    NewModelRequest {
        id: ModelRequestId::new(id).expect("request ID"),
        client_api_key_id: None,
        client_api_key_ref: ClientApiKeyId::new("key_sqlite_test").expect("client key ref"),
        config_revision: ConfigRevision::new(1).expect("config revision"),
        routing: AccountRoutingSnapshot::all(),
        protocol: "openai".to_owned(),
        operation: OperationKind::Generate,
        endpoint: "/v1/responses".to_owned(),
        client_transport: "http_sse".to_owned(),
        requested_model: Some(PublicModelId::new("coding").expect("model")),
        client_ip: None,
        user_agent: None,
        reasoning_effort: None,
        reasoning_preset: None,
        request_kind: None,
        subagent_kind: None,
        compact: false,
        continuation: Default::default(),
        image_generation_requested: false,
        admission_decision_ms: Some(2),
        started_at,
        deadline_at: (started_at + std::time::Duration::from_secs(30)).into(),
    }
}

fn attempt(request_id: &ModelRequestId, count: u32, account_ref: &str) -> AttemptRecord {
    AttemptRecord {
        request_id: request_id.clone(),
        attempt_count: NonZeroU32::new(count).expect("nonzero attempt number"),
        trigger: if count == 1 {
            AttemptTrigger::Initial
        } else {
            AttemptTrigger::AccountRetry
        },
        provider_kind: ProviderKind::new("openai").expect("provider kind"),
        provider_account_id: None,
        provider_account_ref: Some(ProviderAccountId::new(account_ref).expect("account reference")),
        upstream_model_id: Some(UpstreamModelId::new("gpt-5-codex").expect("upstream model")),
        upstream_transport: "websocket".to_owned(),
        http_version: Some("HTTP/2".to_owned()),
        account_selection_wait_ms: Some(5),
        capacity_used_slots: Some(1),
        capacity_total_slots: Some(3),
    }
}

fn make_finalization(
    request_id: &ModelRequestId,
    outcome: ExecutionOutcome,
) -> ModelRequestFinalization {
    ModelRequestFinalization {
        request_id: request_id.clone(),
        outcome,
        send_state: UpstreamSendState::Sent,
        attempt_count: 1,
        downstream_committed_at: None,
        client_status_code: Some(200),
        client_response_id: None,
        upstream_status_code: Some(200),
        upstream_request_id: None,
        upstream_response_id: None,
        upstream_transport: Some("websocket".to_owned()),
        http_version: Some("HTTP/2".to_owned()),
        websocket_pool: Some("new".to_owned()),
        service_tier: None,
        upstream_response_model: None,
        provider_metadata_json: None,
        diagnostic_trace_json: None,
        error: None,
        provider_error_code: None,
        error_details: None,
        failure_observation: Default::default(),
        retry_after_ms: None,
        usage: Usage::new(),
        image_generation_succeeded: None,
        cost: CostEstimate::unavailable(),
        timings: ModelRequestTimings::default(),
        completed_at: SystemTime::now(),
    }
}
