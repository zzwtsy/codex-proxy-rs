//! SQLite 单行执行账本与必要运行事件适配。

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{
    engine::{
        AttemptRecord, EntryRejection, ExecutionOutcome, ExecutionStore, IntermediateFailure,
        ModelRequestFinalization, ModelRequestId, NewModelRequest, ProbeFailure, RecoveryReport,
    },
    error::{
        ProviderErrorKind, StoreError as CoreStoreError, StoreErrorKind as CoreStoreErrorKind,
    },
    metering::CostSource,
    routing::AccountRoutingSnapshot,
    upstream::UpstreamSendState,
};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, SqlitePool, Transaction};

use crate::sqlite::value::encode_amount;

#[derive(Clone)]
pub struct SqliteExecutionStore {
    pool: SqlitePool,
}

impl SqliteExecutionStore {
    #[must_use]
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn insert_request(
        &self,
        request: &NewModelRequest,
        attempt: Option<&AttemptRecord>,
    ) -> Result<(), CoreStoreError> {
        validate_new_request(request)?;
        let (routing_scope, group_refs, group_names) = routing_snapshot(&request.routing);
        let group_refs = serde_json::to_string(&group_refs).map_err(|_| core_invalid())?;
        let group_names = serde_json::to_string(&group_names).map_err(|_| core_invalid())?;
        let mut transaction = self.pool.begin().await.map_err(|_| core_unavailable())?;
        sqlx::query(
            "insert into model_requests (
               id, client_api_key_id, client_api_key_ref, config_revision, protocol,
               routing_scope, routing_group_refs_json, routing_group_names_snapshot_json,
               operation, endpoint, client_transport, requested_model_id,
               client_ip, user_agent, reasoning_effort, reasoning_preset, request_kind,
               subagent_kind, compact, image_generation_requested, admission_decision_ms,
               started_at_us, deadline_at_us, continuation_affinity_hash,
               continuation_previous_response_id_hash, continuation_requested
             ) values (
               ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
               ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26
             )",
        )
        .bind(request.id.as_str())
        .bind(request.client_api_key_id.as_ref().map(|id| id.as_str()))
        .bind(request.client_api_key_ref.as_str())
        .bind(to_i64(request.config_revision.get())?)
        .bind(&request.protocol)
        .bind(routing_scope)
        .bind(group_refs)
        .bind(group_names)
        .bind(request.operation.as_str())
        .bind(&request.endpoint)
        .bind(&request.client_transport)
        .bind(request.requested_model.as_ref().map(|model| model.as_str()))
        .bind(request.client_ip.map(|address| address.to_string()))
        .bind(&request.user_agent)
        .bind(&request.reasoning_effort)
        .bind(&request.reasoning_preset)
        .bind(&request.request_kind)
        .bind(&request.subagent_kind)
        .bind(bool_i64(request.compact))
        .bind(bool_i64(request.image_generation_requested))
        .bind(optional_i64(request.admission_decision_ms)?)
        .bind(DateTime::<Utc>::from(request.started_at).timestamp_micros())
        .bind(DateTime::<Utc>::from(request.deadline_at.lease_deadline()).timestamp_micros())
        .bind(&request.continuation.affinity_hash)
        .bind(&request.continuation.previous_response_id_hash)
        .bind(bool_i64(request.continuation.requested))
        .execute(&mut *transaction)
        .await
        .map_err(|_| core_unavailable())?;

        if let Some(attempt) = attempt {
            if attempt.attempt_count.get() != 1 || attempt.request_id != request.id {
                return Err(core_invalid_state());
            }
            update_attempt(&mut transaction, attempt).await?;
        }
        transaction.commit().await.map_err(|_| core_unavailable())
    }

    async fn record_ops_event(&self, event: OpsEvent) -> Result<(), CoreStoreError> {
        sqlx::query(
            "insert into ops_events (
               id, model_request_id, attempt_index, level, component, operation,
               provider_kind, provider_account_id, provider_account_ref,
               provider_account_name_snapshot, provider_account_email_snapshot,
               provider_account_authentication_kind_snapshot, upstream_model_id,
               failure_kind, upstream_send_state, error_details, status_code,
               provider_error_code, retry_after_ms, upstream_request_id, latency_ms,
               message, created_at_us
             ) select
               ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
               account.name, account.email, account.authentication_kind, ?10,
               ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20
             from (select 1) as seed
             left join provider_accounts account on account.id = ?8",
        )
        .bind(event.id)
        .bind(event.model_request_id)
        .bind(event.attempt_index.map(i64::from))
        .bind(event.level)
        .bind(event.component)
        .bind(event.operation)
        .bind(event.provider_kind)
        .bind(event.provider_account_id.as_deref())
        .bind(event.provider_account_id)
        .bind(event.upstream_model_id)
        .bind(event.failure_kind)
        .bind(event.upstream_send_state)
        .bind(event.error_details)
        .bind(event.status_code.map(i64::from))
        .bind(event.provider_error_code)
        .bind(event.retry_after_ms.map(to_i64).transpose()?)
        .bind(event.upstream_request_id)
        .bind(event.latency_ms.map(to_i64).transpose()?)
        .bind(event.message)
        .bind(event.created_at.timestamp_micros())
        .execute(&self.pool)
        .await
        .map_err(|_| core_unavailable())?;
        Ok(())
    }
}

#[async_trait]
impl gateway_core::diagnostics::OperationalDiagnostics for SqliteExecutionStore {
    async fn record_failure(
        &self,
        failure: gateway_core::diagnostics::OperationalFailure,
    ) -> Result<(), CoreStoreError> {
        let account_id = failure.account_id.map(|id| id.as_str().to_owned());
        self.record_ops_event(OpsEvent {
            id: uuid::Uuid::now_v7().to_string(),
            model_request_id: None,
            attempt_index: None,
            level: "warning".to_owned(),
            component: failure.component.to_owned(),
            operation: failure.operation.to_owned(),
            provider_kind: failure.provider_kind.map(|kind| kind.as_str().to_owned()),
            provider_account_id: account_id,
            upstream_model_id: None,
            failure_kind: failure.kind.to_owned(),
            upstream_send_state: None,
            error_details: failure
                .details
                .map(gateway_core::error::ErrorDetails::into_string),
            status_code: failure.upstream_status,
            provider_error_code: failure.upstream_code.map(|code| code.as_str().to_owned()),
            retry_after_ms: None,
            upstream_request_id: None,
            latency_ms: None,
            created_at: failure.occurred_at.into(),
            message: json!({
                "correlationId": failure.correlation_id,
                "message": failure.message,
            })
            .to_string(),
        })
        .await
    }
}

#[async_trait]
impl ExecutionStore for SqliteExecutionStore {
    fn maintain_request(
        &self,
        request_id: &ModelRequestId,
        deadline: gateway_core::lifecycle::Deadline,
    ) -> Box<dyn gateway_core::lifecycle::LeaseGuard> {
        let pool = self.pool.clone();
        let request_id = request_id.as_str().to_owned();
        Box::new(crate::lease_renewal::LeaseRenewal::spawn(
            deadline,
            // 请求记录为 best-effort 观测，续期失败不能取消客户端执行。
            None,
            move |ttl| {
                let pool = pool.clone();
                let request_id = request_id.clone();
                Box::pin(async move {
                    let ttl_micros = i64::try_from(ttl.as_micros()).map_err(|_| {
                        crate::StoreError::InvalidData {
                            entity: "model request",
                            message: "lease TTL is invalid".to_owned(),
                            source: None,
                        }
                    })?;
                    let now = Utc::now().timestamp_micros();
                    let deadline_at = now.checked_add(ttl_micros).ok_or_else(|| {
                        crate::StoreError::InvalidData {
                            entity: "model request",
                            message: "lease expiry is outside SQLite timestamp range".to_owned(),
                            source: None,
                        }
                    })?;
                    let updated = sqlx::query(
                        "update model_requests set deadline_at_us = ?2
                         where id = ?1 and outcome = 'running' and deadline_at_us > ?3",
                    )
                    .bind(&request_id)
                    .bind(deadline_at)
                    .bind(now)
                    .execute(&pool)
                    .await
                    .map_err(|_| {
                        crate::sqlite::sqlite_unavailable("renew model request recovery lease")
                    })?;
                    if updated.rows_affected() == 1 {
                        return Ok(true);
                    }
                    let still_running = sqlx::query_scalar::<_, bool>(
                        "select exists(
                           select 1 from model_requests where id = ?1 and outcome = 'running'
                         )",
                    )
                    .bind(&request_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|_| {
                        crate::sqlite::sqlite_unavailable("check model request recovery lease")
                    })?;
                    Ok(!still_running)
                })
            },
        ))
    }

    async fn create_model_request(&self, request: NewModelRequest) -> Result<(), CoreStoreError> {
        self.insert_request(&request, None).await
    }

    async fn record_attempt(&self, attempt: AttemptRecord) -> Result<(), CoreStoreError> {
        validate_attempt(&attempt)?;
        let mut transaction = self.pool.begin().await.map_err(|_| core_unavailable())?;
        update_attempt(&mut transaction, &attempt).await?;
        transaction.commit().await.map_err(|_| core_unavailable())
    }

    async fn create_model_request_with_attempt(
        &self,
        request: NewModelRequest,
        attempt: AttemptRecord,
    ) -> Result<(), CoreStoreError> {
        validate_attempt(&attempt)?;
        self.insert_request(&request, Some(&attempt)).await
    }

    async fn mark_send_state(
        &self,
        request_id: &ModelRequestId,
        state: UpstreamSendState,
    ) -> Result<(), CoreStoreError> {
        let result = sqlx::query(
            "update model_requests set upstream_send_state = ?2
             where id = ?1 and outcome = 'running' and attempt_count > 0",
        )
        .bind(request_id.as_str())
        .bind(send_state_str(state))
        .execute(&self.pool)
        .await
        .map_err(|_| core_unavailable())?;
        require_update(result.rows_affected() == 1)
    }

    async fn mark_downstream_committed(
        &self,
        request_id: &ModelRequestId,
        committed_at: SystemTime,
        client_status_code: Option<u16>,
    ) -> Result<(), CoreStoreError> {
        validate_status_code(client_status_code)?;
        let result = sqlx::query(
            "update model_requests
             set downstream_committed_at_us = ?2, client_status_code = ?3
             where id = ?1 and outcome = 'running' and downstream_committed_at_us is null
               and client_status_code is null",
        )
        .bind(request_id.as_str())
        .bind(DateTime::<Utc>::from(committed_at).timestamp_micros())
        .bind(client_status_code.map(i64::from))
        .execute(&self.pool)
        .await
        .map_err(|_| core_unavailable())?;
        require_update(result.rows_affected() == 1)
    }

    async fn record_client_status(
        &self,
        request_id: &ModelRequestId,
        client_status_code: u16,
    ) -> Result<(), CoreStoreError> {
        validate_status_code(Some(client_status_code))?;
        let result = sqlx::query(
            "update model_requests set client_status_code = ?2
             where id = ?1 and client_status_code is null",
        )
        .bind(request_id.as_str())
        .bind(i64::from(client_status_code))
        .execute(&self.pool)
        .await
        .map_err(|_| core_unavailable())?;
        require_update(result.rows_affected() == 1)
    }

    async fn record_intermediate_failure(
        &self,
        failure: IntermediateFailure,
    ) -> Result<(), CoreStoreError> {
        let error = failure.error;
        self.record_ops_event(OpsEvent {
            id: uuid::Uuid::now_v7().to_string(),
            model_request_id: Some(failure.request_id.as_str().to_owned()),
            attempt_index: Some(failure.attempt_index.get()),
            level: "warning".to_owned(),
            component: "routing".to_owned(),
            operation: failure.trigger.as_str().to_owned(),
            provider_kind: Some(failure.provider_kind.as_str().to_owned()),
            provider_account_id: failure.account_id.map(|id| id.as_str().to_owned()),
            upstream_model_id: failure
                .upstream_model_id
                .map(|model| model.as_str().to_owned()),
            failure_kind: error.kind().as_str().to_owned(),
            upstream_send_state: Some(error.send_state().as_str().to_owned()),
            error_details: error.error_details(),
            status_code: error.upstream_status().or(failure.upstream_status_code),
            provider_error_code: error.upstream_code().map(|code| code.as_str().to_owned()),
            retry_after_ms: duration_ms(error.retry_after())?,
            upstream_request_id: error
                .upstream_request_id()
                .map(|id| id.as_str().to_owned())
                .or(failure.upstream_request_id),
            latency_ms: Some(duration_ms(Some(failure.latency))?.ok_or_else(core_invalid)?),
            created_at: Utc::now(),
            message: error.diagnostic().map_or_else(
                || "intermediate upstream failure".to_owned(),
                |diagnostic| diagnostic.as_str().to_owned(),
            ),
        })
        .await
    }

    async fn record_entry_rejection(
        &self,
        rejection: EntryRejection,
    ) -> Result<(), CoreStoreError> {
        let error = rejection.error;
        self.record_ops_event(OpsEvent {
            id: uuid::Uuid::now_v7().to_string(),
            model_request_id: None,
            attempt_index: None,
            level: "warning".to_owned(),
            component: "request_entry".to_owned(),
            operation: "reject".to_owned(),
            provider_kind: None,
            provider_account_id: None,
            upstream_model_id: None,
            failure_kind: error.kind().as_str().to_owned(),
            upstream_send_state: None,
            error_details: None,
            status_code: None,
            provider_error_code: error.client_error_code().map(str::to_owned),
            retry_after_ms: duration_ms(error.retry_after())?,
            upstream_request_id: None,
            latency_ms: duration_ms(Some(rejection.latency))?,
            created_at: Utc::now(),
            message: json!({
                "requestId": rejection.request_id.as_str(),
                "clientKeyId": rejection.client_key_id.as_str(),
                "message": error.client_message(),
            })
            .to_string(),
        })
        .await
    }

    async fn record_probe_failure(&self, failure: ProbeFailure) -> Result<(), CoreStoreError> {
        let error = failure.error;
        self.record_ops_event(OpsEvent {
            id: uuid::Uuid::now_v7().to_string(),
            model_request_id: None,
            attempt_index: None,
            level: "warning".to_owned(),
            component: "account_probe".to_owned(),
            operation: "connection_test".to_owned(),
            provider_kind: Some(failure.provider_kind.as_str().to_owned()),
            provider_account_id: Some(failure.account_id.as_str().to_owned()),
            upstream_model_id: Some(failure.upstream_model_id.as_str().to_owned()),
            failure_kind: error.kind().as_str().to_owned(),
            upstream_send_state: Some(error.send_state().as_str().to_owned()),
            error_details: error.error_details(),
            status_code: error.upstream_status(),
            provider_error_code: error.upstream_code().map(|code| code.as_str().to_owned()),
            retry_after_ms: duration_ms(error.retry_after())?,
            upstream_request_id: error.upstream_request_id().map(|id| id.as_str().to_owned()),
            latency_ms: Some(duration_ms(Some(failure.latency))?.ok_or_else(core_invalid)?),
            created_at: Utc::now(),
            message: error.diagnostic().map_or_else(
                || "account connection test failed".to_owned(),
                |diagnostic| diagnostic.as_str().to_owned(),
            ),
        })
        .await
    }

    async fn finalize_model_request(
        &self,
        finalization: ModelRequestFinalization,
    ) -> Result<(), CoreStoreError> {
        let outcome = outcome_str(finalization.outcome)?;
        validate_status_code(finalization.client_status_code)?;
        validate_status_code(finalization.upstream_status_code)?;
        validate_finalization_json(&finalization)?;
        validate_connection_observation(&finalization)?;
        if let Some(tier) = finalization.service_tier.as_deref()
            && (tier.is_empty() || tier.len() > 64 || tier.chars().any(char::is_control))
        {
            return Err(core_invalid());
        }

        let continuation_reason = finalization
            .failure_observation
            .continuation_unavailable_reason
            .clone();
        validate_stable_reason(continuation_reason.as_deref())?;
        let (cost_source, cost_amount, cost_currency) = match finalization.cost.total() {
            Some(total) => (
                cost_source_str(finalization.cost.source()),
                Some(encode_amount(total.amount())),
                Some(total.currency().as_str().to_owned()),
            ),
            None => ("unavailable", None, None),
        };
        let error_kind = if continuation_reason.is_some() {
            Some(
                ProviderErrorKind::ContinuationRecoveryRequired
                    .as_str()
                    .to_owned(),
            )
        } else {
            finalization
                .error
                .as_ref()
                .map(|error| error.kind().as_str().to_owned())
        };
        let error_message = finalization.error.as_ref().map(|error| {
            error.diagnostic().map_or_else(
                || error.safe_message().to_owned(),
                |diagnostic| diagnostic.as_str().to_owned(),
            )
        });
        let connection = finalization
            .failure_observation
            .upstream_connection
            .as_ref();
        let completed_at_us = DateTime::<Utc>::from(finalization.completed_at).timestamp_micros();
        let provider_metadata = finalization
            .provider_metadata_json
            .as_deref()
            .map(parse_json_object)
            .transpose()?;
        let diagnostic_trace = finalization
            .diagnostic_trace_json
            .as_deref()
            .map(parse_json_object)
            .transpose()?;
        let billing_snapshot = finalization
            .cost
            .breakdown()
            .map(crate::billing::encode_billing_snapshot)
            .map(|value| value.to_string());

        let mut transaction = self.pool.begin().await.map_err(|_| core_unavailable())?;
        let current = sqlx::query(
            "update model_requests
             set outcome = ?2, upstream_send_state = ?3, attempt_count = ?4,
                 downstream_committed_at_us = ?5,
                 client_status_code = coalesce(client_status_code, ?6),
                 upstream_status_code = ?7,
                 client_response_id = ?8, upstream_request_id = ?9, upstream_response_id = ?10,
                 error_kind = ?11, provider_error_code = ?12, error_message = ?13,
                 retry_after_ms = ?14, input_tokens = ?15, output_tokens = ?16,
                 cached_tokens = ?17, cache_write_tokens = ?18, reasoning_tokens = ?19,
                 image_input_tokens = ?20, image_output_tokens = ?21, total_tokens = ?22,
                 image_generation_succeeded = ?23, cost_source = ?24,
                 cost_amount = ?25, cost_currency = ?26,
                 transport_decision_wait_ms = ?27, connect_ms = ?28,
                 headers_ms = ?29, first_event_ms = ?30, first_reasoning_ms = ?31,
                 first_text_ms = ?32, first_token_ms = ?33, provider_processing_ms = ?34,
                 latency_ms = ?35, completed_at_us = max(started_at_us, ?36),
                 upstream_transport = coalesce(?37, upstream_transport),
                 http_version = coalesce(?38, http_version), websocket_pool = ?39,
                 service_tier = ?40, provider_observation_json = ?41,
                 error_details = ?42,
                 continuation_unavailable_reason = ?43,
                 upstream_connection_id = ?44,
                 upstream_connection_exit_reason = ?45,
                 upstream_connection_age_ms = ?46,
                 upstream_connection_idle_ms = ?47, diagnostic_trace_json = ?48,
                 upstream_response_model = ?49, billing_snapshot_json = ?50
             where id = ?1 and outcome = 'running'
             returning id, client_api_key_ref, continuation_affinity_hash,
                       continuation_requested, provider_kind, upstream_transport,
                       outcome, started_at_us, completed_at_us",
        )
        .bind(finalization.request_id.as_str())
        .bind(outcome)
        .bind(send_state_str(finalization.send_state))
        .bind(i64::from(finalization.attempt_count))
        .bind(
            finalization
                .downstream_committed_at
                .map(|value| DateTime::<Utc>::from(value).timestamp_micros()),
        )
        .bind(finalization.client_status_code.map(i64::from))
        .bind(finalization.upstream_status_code.map(i64::from))
        .bind(finalization.client_response_id.map(String::into_bytes))
        .bind(&finalization.upstream_request_id)
        .bind(finalization.upstream_response_id.map(String::into_bytes))
        .bind(error_kind)
        .bind(&finalization.provider_error_code)
        .bind(error_message)
        .bind(optional_i64(finalization.retry_after_ms)?)
        .bind(optional_i64(finalization.usage.input_tokens)?)
        .bind(optional_i64(finalization.usage.output_tokens)?)
        .bind(optional_i64(finalization.usage.cached_tokens)?)
        .bind(optional_i64(finalization.usage.cache_write_tokens)?)
        .bind(optional_i64(finalization.usage.reasoning_tokens)?)
        .bind(optional_i64(finalization.usage.image_input_tokens)?)
        .bind(optional_i64(finalization.usage.image_output_tokens)?)
        .bind(optional_i64(finalization.usage.total_tokens)?)
        .bind(finalization.image_generation_succeeded.map(bool_i64))
        .bind(cost_source)
        .bind(cost_amount)
        .bind(cost_currency)
        .bind(optional_i64(
            finalization.timings.transport_decision_wait_ms,
        )?)
        .bind(optional_i64(finalization.timings.connect_ms)?)
        .bind(optional_i64(finalization.timings.headers_ms)?)
        .bind(optional_i64(finalization.timings.first_event_ms)?)
        .bind(optional_i64(finalization.timings.first_reasoning_ms)?)
        .bind(optional_i64(finalization.timings.first_text_ms)?)
        .bind(optional_i64(finalization.timings.first_token_ms)?)
        .bind(optional_i64(finalization.timings.provider_processing_ms)?)
        .bind(optional_i64(finalization.timings.latency_ms)?)
        .bind(completed_at_us)
        .bind(&finalization.upstream_transport)
        .bind(&finalization.http_version)
        .bind(&finalization.websocket_pool)
        .bind(&finalization.service_tier)
        .bind(provider_metadata.map(|value| value.to_string()))
        .bind(&finalization.error_details)
        .bind(&continuation_reason)
        .bind(connection.map(|observation| observation.connection_id()))
        .bind(connection.map(|observation| observation.exit_reason()))
        .bind(
            connection
                .map(|observation| optional_i64(Some(observation.age_ms())))
                .transpose()?
                .flatten(),
        )
        .bind(
            connection
                .map(|observation| optional_i64(Some(observation.idle_ms())))
                .transpose()?
                .flatten(),
        )
        .bind(diagnostic_trace.map(|value| value.to_string()))
        .bind(&finalization.upstream_response_model)
        .bind(billing_snapshot)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| core_unavailable())?;
        let Some(current) = current else {
            transaction
                .rollback()
                .await
                .map_err(|_| core_unavailable())?;
            return Err(core_invalid_state());
        };
        update_continuation_recovery(&mut transaction, &current).await?;
        update_session_transport_recovery(&mut transaction, &current).await?;
        transaction.commit().await.map_err(|_| core_unavailable())
    }

    async fn recover_expired(&self, now: SystemTime) -> Result<RecoveryReport, CoreStoreError> {
        let now = DateTime::<Utc>::from(now).timestamp_micros();
        let result = sqlx::query(
            "update model_requests
             set outcome = 'incomplete', error_kind = 'process_interrupted',
                 error_message = 'request did not reach a terminal state',
                 image_generation_succeeded = case
                   when image_generation_requested = 1 then 0 else null
                 end,
                 completed_at_us = ?1
             where outcome = 'running' and deadline_at_us <= ?1",
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|_| core_unavailable())?;
        Ok(RecoveryReport {
            requests: result.rows_affected(),
        })
    }
}

async fn update_attempt(
    transaction: &mut Transaction<'_, Sqlite>,
    attempt: &AttemptRecord,
) -> Result<(), CoreStoreError> {
    validate_attempt(attempt)?;
    let result = sqlx::query(
        "update model_requests
         set provider_kind = ?2, provider_account_id = ?3, provider_account_ref = ?4,
             provider_account_name_snapshot = (select name from provider_accounts where id = ?3),
             provider_account_email_snapshot = (select email from provider_accounts where id = ?3),
             provider_account_authentication_kind_snapshot =
               (select authentication_kind from provider_accounts where id = ?3),
             upstream_model_id = ?5, upstream_transport = ?6, http_version = ?7,
             attempt_count = ?8,
             account_selection_wait_ms = case
               when ?9 is null then account_selection_wait_ms
               else coalesce(account_selection_wait_ms, 0) + ?9
             end,
             capacity_used_slots = coalesce(?10, capacity_used_slots),
             capacity_total_slots = coalesce(?11, capacity_total_slots)
         where id = ?1 and outcome = 'running' and downstream_committed_at_us is null
           and ?8 = attempt_count + 1",
    )
    .bind(attempt.request_id.as_str())
    .bind(attempt.provider_kind.as_str())
    .bind(attempt.provider_account_id.as_ref().map(|id| id.as_str()))
    .bind(attempt.provider_account_ref.as_ref().map(|id| id.as_str()))
    .bind(
        attempt
            .upstream_model_id
            .as_ref()
            .map(|model| model.as_str()),
    )
    .bind(&attempt.upstream_transport)
    .bind(&attempt.http_version)
    .bind(i64::from(attempt.attempt_count.get()))
    .bind(optional_i64(attempt.account_selection_wait_ms)?)
    .bind(optional_i64(attempt.capacity_used_slots)?)
    .bind(optional_i64(attempt.capacity_total_slots)?)
    .execute(&mut **transaction)
    .await
    .map_err(|_| core_unavailable())?;
    require_update(result.rows_affected() == 1)
}

async fn update_continuation_recovery(
    transaction: &mut Transaction<'_, Sqlite>,
    current: &sqlx::sqlite::SqliteRow,
) -> Result<(), CoreStoreError> {
    let continuation_requested = row_bool(current, "continuation_requested")?;
    let Some(affinity_hash) = row_optional_string(current, "continuation_affinity_hash")? else {
        return Ok(());
    };
    if continuation_requested {
        return Ok(());
    }
    let client_ref = row_string(current, "client_api_key_ref")?;
    let current_id = row_string(current, "id")?;
    let outcome = row_string(current, "outcome")?;
    let started_at = row_i64(current, "started_at_us")?;
    let completed_at = row_i64(current, "completed_at_us")?;
    let target = sqlx::query(
        "select id, completed_at_us from model_requests
         where client_api_key_ref = ?1 and continuation_affinity_hash = ?2
           and continuation_requested = 1 and outcome = 'failed'
           and error_kind = 'continuation_recovery_required' and recovered_at_us is null
           and ?3 >= completed_at_us and ?3 <= completed_at_us + 30000000
         order by completed_at_us desc, id desc limit 1",
    )
    .bind(&client_ref)
    .bind(&affinity_hash)
    .bind(started_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| core_unavailable())?;
    let Some(target) = target else {
        return Ok(());
    };
    let target_id: String = target.try_get("id").map_err(|_| core_invalid())?;
    let prior_completed_at: i64 = target
        .try_get("completed_at_us")
        .map_err(|_| core_invalid())?;
    let recovered = outcome == "succeeded";
    sqlx::query(
        "update model_requests set
           recovery_attempt_count = recovery_attempt_count + 1,
           recovery_request_id = case when ?2 = 1 then ?3 else recovery_request_id end,
           recovered_at_us = case when ?2 = 1 then ?4 else recovered_at_us end,
           recovery_retry_delay_ms = case when ?2 = 1 then max(0, (?5 - ?6) / 1000)
                                          else recovery_retry_delay_ms end,
           recovery_total_latency_ms = case when ?2 = 1 then max(0, (?4 - ?6) / 1000)
                                            else recovery_total_latency_ms end
         where id = ?1 and recovered_at_us is null",
    )
    .bind(target_id)
    .bind(bool_i64(recovered))
    .bind(current_id)
    .bind(completed_at)
    .bind(started_at)
    .bind(prior_completed_at)
    .execute(&mut **transaction)
    .await
    .map_err(|_| core_unavailable())?;
    Ok(())
}

async fn update_session_transport_recovery(
    transaction: &mut Transaction<'_, Sqlite>,
    current: &sqlx::sqlite::SqliteRow,
) -> Result<(), CoreStoreError> {
    if row_string(current, "outcome")? != "succeeded"
        || row_optional_string(current, "provider_kind")?.as_deref() != Some("openai")
        || row_optional_string(current, "upstream_transport")?.as_deref() != Some("http_sse")
    {
        return Ok(());
    }
    let Some(affinity_hash) = row_optional_string(current, "continuation_affinity_hash")? else {
        return Ok(());
    };
    let client_ref = row_string(current, "client_api_key_ref")?;
    let current_id = row_string(current, "id")?;
    let started_at = row_i64(current, "started_at_us")?;
    let completed_at = row_i64(current, "completed_at_us")?;
    let target = sqlx::query(
        "select id, completed_at_us from model_requests
         where client_api_key_ref = ?1 and continuation_affinity_hash = ?2
           and provider_kind = 'openai' and outcome = 'failed'
           and error_kind = 'upstream_unavailable' and upstream_transport = 'websocket'
           and upstream_send_state = 'ambiguous' and downstream_committed_at_us is null
           and recovered_at_us is null and ?3 >= completed_at_us
           and ?3 <= completed_at_us + 30000000
         order by completed_at_us desc, id desc limit 1",
    )
    .bind(client_ref)
    .bind(affinity_hash)
    .bind(started_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| core_unavailable())?;
    let Some(target) = target else {
        return Ok(());
    };
    let target_id: String = target.try_get("id").map_err(|_| core_invalid())?;
    let prior_completed_at: i64 = target
        .try_get("completed_at_us")
        .map_err(|_| core_invalid())?;
    sqlx::query(
        "update model_requests set
           recovery_attempt_count = recovery_attempt_count + 1,
           recovery_request_id = ?2, recovered_at_us = ?3,
           recovery_retry_delay_ms = max(0, (?4 - ?5) / 1000),
           recovery_total_latency_ms = max(0, (?3 - ?5) / 1000)
         where id = ?1 and recovered_at_us is null",
    )
    .bind(target_id)
    .bind(current_id)
    .bind(completed_at)
    .bind(started_at)
    .bind(prior_completed_at)
    .execute(&mut **transaction)
    .await
    .map_err(|_| core_unavailable())?;
    Ok(())
}

struct OpsEvent {
    id: String,
    model_request_id: Option<String>,
    attempt_index: Option<u32>,
    level: String,
    component: String,
    operation: String,
    provider_kind: Option<String>,
    provider_account_id: Option<String>,
    upstream_model_id: Option<String>,
    failure_kind: String,
    upstream_send_state: Option<String>,
    error_details: Option<String>,
    status_code: Option<u16>,
    provider_error_code: Option<String>,
    retry_after_ms: Option<u64>,
    upstream_request_id: Option<String>,
    latency_ms: Option<u64>,
    created_at: DateTime<Utc>,
    message: String,
}

fn validate_new_request(request: &NewModelRequest) -> Result<(), CoreStoreError> {
    if request.id.as_str().is_empty()
        || request.client_api_key_ref.as_str().is_empty()
        || request.protocol.is_empty()
        || request.endpoint.is_empty()
        || request.client_transport.is_empty()
        || request
            .deadline_at
            .at()
            .is_some_and(|deadline| request.started_at > deadline)
        || request
            .client_api_key_id
            .as_ref()
            .is_some_and(|id| id != &request.client_api_key_ref)
    {
        return Err(core_invalid());
    }
    let hashes = [
        request.continuation.affinity_hash.as_deref(),
        request.continuation.previous_response_id_hash.as_deref(),
    ];
    if hashes.into_iter().flatten().any(|hash| !is_hash(hash))
        || (!request.continuation.requested
            && request.continuation.previous_response_id_hash.is_some())
    {
        return Err(core_invalid());
    }
    Ok(())
}

fn validate_attempt(attempt: &AttemptRecord) -> Result<(), CoreStoreError> {
    if attempt.upstream_transport.is_empty()
        || attempt
            .upstream_model_id
            .as_ref()
            .is_some_and(|model| model.as_str().is_empty())
        || attempt
            .provider_account_id
            .as_ref()
            .is_some_and(|id| Some(id) != attempt.provider_account_ref.as_ref())
    {
        return Err(core_invalid());
    }
    match (attempt.capacity_used_slots, attempt.capacity_total_slots) {
        (None, None) => {}
        (Some(used), Some(total)) if total > 0 && used <= total => {}
        _ => return Err(core_invalid()),
    }
    Ok(())
}

fn routing_snapshot(snapshot: &AccountRoutingSnapshot) -> (String, Vec<String>, Vec<String>) {
    let groups = snapshot.groups_snapshot();
    (
        snapshot.kind().as_str().to_owned(),
        groups
            .iter()
            .map(|group| group.id().as_str().to_owned())
            .collect(),
        groups.iter().map(|group| group.name().to_owned()).collect(),
    )
}

fn validate_finalization_json(
    finalization: &ModelRequestFinalization,
) -> Result<(), CoreStoreError> {
    for value in [
        finalization.provider_metadata_json.as_deref(),
        finalization.diagnostic_trace_json.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        parse_json_object(value)?;
    }
    if finalization
        .diagnostic_trace_json
        .as_ref()
        .is_some_and(|value| value.len() > 64 * 1024)
    {
        return Err(core_invalid());
    }
    Ok(())
}

fn parse_json_object(value: &str) -> Result<Value, CoreStoreError> {
    let value: Value = serde_json::from_str(value).map_err(|_| core_invalid())?;
    if !value.is_object() {
        return Err(core_invalid());
    }
    Ok(value)
}

fn validate_connection_observation(
    finalization: &ModelRequestFinalization,
) -> Result<(), CoreStoreError> {
    let Some(connection) = finalization
        .failure_observation
        .upstream_connection
        .as_ref()
    else {
        return Ok(());
    };
    let id = connection.connection_id();
    let exit = connection.exit_reason();
    if id.is_empty()
        || id.len() > 128
        || id.chars().any(char::is_control)
        || !valid_stable_reason(exit)
        || connection.idle_ms() > connection.age_ms()
    {
        return Err(core_invalid());
    }
    Ok(())
}

fn validate_stable_reason(value: Option<&str>) -> Result<(), CoreStoreError> {
    if value.is_some_and(|value| !valid_stable_reason(value)) {
        return Err(core_invalid());
    }
    Ok(())
}

fn valid_stable_reason(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || (index > 0 && (byte.is_ascii_digit() || byte == b'_'))
        })
}

fn validate_status_code(value: Option<u16>) -> Result<(), CoreStoreError> {
    if value.is_some_and(|status| !(100..=599).contains(&status)) {
        return Err(core_invalid());
    }
    Ok(())
}

fn require_update(updated: bool) -> Result<(), CoreStoreError> {
    if updated {
        Ok(())
    } else {
        Err(core_invalid_state())
    }
}

fn outcome_str(value: ExecutionOutcome) -> Result<&'static str, CoreStoreError> {
    match value {
        ExecutionOutcome::Running => Err(core_invalid_state()),
        ExecutionOutcome::Succeeded => Ok("succeeded"),
        ExecutionOutcome::Failed => Ok("failed"),
        ExecutionOutcome::Cancelled => Ok("cancelled"),
        ExecutionOutcome::Incomplete => Ok("incomplete"),
    }
}

const fn send_state_str(value: UpstreamSendState) -> &'static str {
    match value {
        UpstreamSendState::NotSent => "not_sent",
        UpstreamSendState::Sent => "sent",
        UpstreamSendState::Ambiguous => "ambiguous",
    }
}

const fn cost_source_str(value: CostSource) -> &'static str {
    match value {
        CostSource::ProviderReported => "provider_reported",
        CostSource::Calculated => "calculated",
        CostSource::Unavailable => "unavailable",
    }
}

fn duration_ms(value: Option<Duration>) -> Result<Option<u64>, CoreStoreError> {
    value
        .map(|duration| u64::try_from(duration.as_millis()).map_err(|_| core_invalid()))
        .transpose()
}

fn optional_i64(value: Option<u64>) -> Result<Option<i64>, CoreStoreError> {
    value.map(to_i64).transpose()
}

fn to_i64(value: u64) -> Result<i64, CoreStoreError> {
    i64::try_from(value).map_err(|_| core_invalid())
}

const fn bool_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn row_string(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> Result<String, CoreStoreError> {
    row.try_get(field).map_err(|_| core_invalid())
}

fn row_optional_string(
    row: &sqlx::sqlite::SqliteRow,
    field: &'static str,
) -> Result<Option<String>, CoreStoreError> {
    row.try_get(field).map_err(|_| core_invalid())
}

fn row_i64(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> Result<i64, CoreStoreError> {
    row.try_get(field).map_err(|_| core_invalid())
}

fn row_bool(row: &sqlx::sqlite::SqliteRow, field: &'static str) -> Result<bool, CoreStoreError> {
    let value = row_i64(row, field)?;
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(core_invalid()),
    }
}

fn core_unavailable() -> CoreStoreError {
    CoreStoreError::new(CoreStoreErrorKind::Unavailable)
}

fn core_invalid() -> CoreStoreError {
    CoreStoreError::new(CoreStoreErrorKind::InvalidData)
}

fn core_invalid_state() -> CoreStoreError {
    CoreStoreError::new(CoreStoreErrorKind::InvalidState)
}
