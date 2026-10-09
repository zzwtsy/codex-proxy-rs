use std::{num::NonZeroU32, time::SystemTime};

use chrono::{Duration, Utc};
use gateway_admin::{
    model::retention::{RetentionPolicy, RetentionTarget},
    ports::retention::RetentionStore,
};
use gateway_core::{
    engine::{
        ExecutionOutcome, ExecutionStore, ModelRequestFinalization, ModelRequestId,
        ModelRequestTimings, NewModelRequest,
    },
    metering::CostEstimate,
    operation::OperationKind,
    policy::ClientApiKeyId,
    routing::{AccountRoutingSnapshot, ConfigRevision, PublicModelId},
    upstream::UpstreamSendState,
};
use gateway_store::{
    SqliteStoreConfig, sqlite, sqlite::SqliteExecutionStore, sqlite::SqliteRetentionRepository,
};

#[tokio::test]
async fn sqlite_retention_purges_bounded_batches_for_all_persistent_targets() {
    let root = tempfile::tempdir().expect("SQLite data directory");
    let pool = sqlite::connect_and_migrate(
        &root.path().join("retention.sqlite3"),
        &SqliteStoreConfig::default(),
    )
    .await
    .expect("create SQLite database");
    let retention = SqliteRetentionRepository::new(pool.clone());
    let configured_policy = retention.load_policy().await.expect("load default policy");
    assert_eq!(
        configured_policy,
        RetentionPolicy::try_new(31, 30, 90).unwrap()
    );

    let now = Utc::now();
    let old = now - Duration::days(100);
    let request = old_model_request(SystemTime::from(old));
    let execution = SqliteExecutionStore::new(pool.clone());
    execution
        .create_model_request(request.clone())
        .await
        .expect("create old request");
    execution
        .finalize_model_request(ModelRequestFinalization {
            request_id: request.id.clone(),
            outcome: ExecutionOutcome::Incomplete,
            send_state: UpstreamSendState::NotSent,
            attempt_count: 0,
            downstream_committed_at: None,
            client_status_code: None,
            client_response_id: None,
            upstream_status_code: None,
            upstream_request_id: None,
            upstream_response_id: None,
            upstream_transport: None,
            http_version: None,
            websocket_pool: None,
            service_tier: None,
            upstream_response_model: None,
            provider_metadata_json: None,
            diagnostic_trace_json: None,
            error: None,
            provider_error_code: None,
            error_details: None,
            failure_observation: Default::default(),
            retry_after_ms: None,
            usage: Default::default(),
            image_generation_succeeded: None,
            cost: CostEstimate::unavailable(),
            timings: ModelRequestTimings::default(),
            completed_at: SystemTime::from(old + Duration::seconds(1)),
        })
        .await
        .expect("finalize old request");
    let old_us = old.timestamp_micros();
    sqlx::query(
        "insert into ops_events (id, level, component, operation, failure_kind, message, created_at_us)
         values ('evt_old', 'warning', 'request_entry', 'reject', 'rate_limited', 'old event', ?1)",
    )
    .bind(old_us)
    .execute(&pool)
    .await
    .expect("insert old unassociated event");
    sqlx::query(
        "insert into admin_audit_events
         (id, actor_kind, actor_ref, action, entity_kind, entity_ref, created_at_us)
         values ('audit_old', 'anonymous', 'anonymous', 'login_failed', 'auth', 'admin', ?1)",
    )
    .bind(old_us)
    .execute(&pool)
    .await
    .expect("insert old audit event");

    let batch_size = NonZeroU32::new(1).unwrap();
    assert_eq!(
        retention
            .purge_batch(
                RetentionTarget::ModelRequests,
                now,
                configured_policy,
                batch_size
            )
            .await
            .expect("purge model requests"),
        1
    );
    assert_eq!(
        retention
            .purge_batch(
                RetentionTarget::OpsEvents,
                now,
                configured_policy,
                batch_size
            )
            .await
            .expect("purge unassociated ops events"),
        1
    );
    assert_eq!(
        retention
            .purge_batch(
                RetentionTarget::AdminAuditEvents,
                now,
                configured_policy,
                batch_size
            )
            .await
            .expect("purge admin audit events"),
        1
    );
    let request_count: i64 = sqlx::query_scalar("select count(*) from model_request_observations")
        .fetch_one(&pool)
        .await
        .unwrap();
    let ops_count: i64 = sqlx::query_scalar("select count(*) from ops_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    let audit_count: i64 = sqlx::query_scalar("select count(*) from admin_audit_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((request_count, ops_count, audit_count), (0, 0, 0));
    pool.close().await;
}

fn old_model_request(started_at: SystemTime) -> NewModelRequest {
    NewModelRequest {
        id: ModelRequestId::new("req_retention_old").expect("request ID"),
        client_api_key_id: None,
        client_api_key_ref: ClientApiKeyId::new("key_retention").expect("key ref"),
        config_revision: ConfigRevision::new(1).expect("revision"),
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
        admission_decision_ms: None,
        started_at,
        deadline_at: (started_at + std::time::Duration::from_secs(30)).into(),
    }
}
