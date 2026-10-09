//! SQLite 单行执行账本与必要运行事件适配。

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gateway_core::{
    engine::{
        AttemptRecord, EntryRejection, ExecutionStore, IntermediateFailure,
        ModelRequestFinalization, ModelRequestId, NewModelRequest, ProbeFailure, RecoveryReport,
    },
    error::{StoreError as CoreStoreError, StoreErrorKind as CoreStoreErrorKind},
    upstream::UpstreamSendState,
};
use serde_json::json;
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
        request: NewModelRequest,
        attempt: Option<&AttemptRecord>,
    ) -> Result<(), CoreStoreError> {
        let request = crate::execution::new_model_request_row(request);
        request.validate().map_err(crate::core_store_error)?;
        let observation = crate::request_observation::RequestObservation::initial(&request)
            .map_err(crate::core_store_error)?;
        let mut transaction = self.pool.begin().await.map_err(|_| core_unavailable())?;
        sqlx::query(
            "insert into model_requests (
               id, client_api_key_id, client_api_key_ref, operation, client_transport,
               requested_model_id, image_generation_requested, started_at_us, deadline_at_us,
               continuation_affinity_hash, continuation_requested, request_kind, request_observation_json
             ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        )
        .bind(&request.id)
        .bind(request.client_api_key_id)
        .bind(request.client_api_key_ref)
        .bind(request.operation)
        .bind(request.client_transport)
        .bind(request.requested_model_id)
        .bind(bool_i64(request.image_generation_requested))
        .bind(request.started_at.timestamp_micros())
        .bind(request.deadline_at.timestamp_micros())
        .bind(request.continuation.affinity_hash)
        .bind(bool_i64(request.continuation.requested))
        .bind(request.request_kind)
        .bind(observation)
        .execute(&mut *transaction)
        .await
        .map_err(|source| crate::core_store_error(super::sqlite_unavailable("insert SQLite model request").with_source(source)))?;
        if let Some(attempt) = attempt {
            if attempt.attempt_count.get() != 1 || attempt.request_id.as_str() != request.id {
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
        self.insert_request(request, None).await
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
        self.insert_request(request, Some(&attempt)).await
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
        let finalization = crate::execution::finalization_row(finalization)?;
        finalization.validate().map_err(crate::core_store_error)?;
        let observation = crate::request_observation::RequestObservation::finalized(&finalization)
            .map_err(crate::core_store_error)?;
        let cost_amount = finalization
            .cost_amount
            .as_ref()
            .map(|value| {
                value
                    .as_str()
                    .parse::<gateway_core::metering::Decimal>()
                    .map(encode_amount)
                    .map_err(|_| core_invalid())
            })
            .transpose()?;
        let mut transaction = self.pool.begin().await.map_err(|_| core_unavailable())?;
        let current = sqlx::query(
            "update model_requests
             set outcome = ?2, upstream_send_state = ?3, attempt_count = ?4,
                 downstream_committed_at_us = ?5,
                 client_status_code = coalesce(client_status_code, ?6), upstream_status_code = ?7,
                 client_response_id = ?8, upstream_request_id = ?9, upstream_response_id = ?10,
                 error_kind = ?11, input_tokens = ?12, output_tokens = ?13,
                 cached_tokens = ?14, cache_write_tokens = ?15, reasoning_tokens = ?16,
                 image_input_tokens = ?17, image_output_tokens = ?18, total_tokens = ?19,
                 image_generation_succeeded = ?20, cost_source = ?21,
                 cost_amount = ?22, cost_currency = ?23,
                 completed_at_us = max(started_at_us, ?24),
                 upstream_transport = coalesce(?25, upstream_transport), service_tier = ?26,
                 provider_observation_json = ?27, error_details = ?28, diagnostic_trace_json = ?29,
                 billing_snapshot_json = ?30,
                 request_observation_json = json_patch(request_observation_json, ?31)
             where id = ?1 and outcome = 'running'
             returning id, client_api_key_ref, continuation_affinity_hash,
                       continuation_requested, provider_kind, upstream_transport,
                       outcome, started_at_us, completed_at_us",
        )
        .bind(&finalization.model_request_id)
        .bind(finalization.outcome.as_str())
        .bind(finalization.upstream_send_state.as_str())
        .bind(i64::from(finalization.attempt_count))
        .bind(
            finalization
                .downstream_committed_at
                .map(|value| value.timestamp_micros()),
        )
        .bind(finalization.client_status_code.map(i64::from))
        .bind(finalization.upstream_status_code.map(i64::from))
        .bind(finalization.client_response_id.map(String::into_bytes))
        .bind(finalization.upstream_request_id)
        .bind(finalization.upstream_response_id.map(String::into_bytes))
        .bind(finalization.error_kind)
        .bind(optional_i64(finalization.usage.input_tokens)?)
        .bind(optional_i64(finalization.usage.output_tokens)?)
        .bind(optional_i64(finalization.usage.cached_tokens)?)
        .bind(optional_i64(finalization.usage.cache_write_tokens)?)
        .bind(optional_i64(finalization.usage.reasoning_tokens)?)
        .bind(optional_i64(finalization.usage.image_input_tokens)?)
        .bind(optional_i64(finalization.usage.image_output_tokens)?)
        .bind(optional_i64(finalization.usage.total_tokens)?)
        .bind(finalization.image_generation_succeeded.map(bool_i64))
        .bind(finalization.cost_source.as_str())
        .bind(cost_amount)
        .bind(finalization.cost_currency)
        .bind(finalization.completed_at.timestamp_micros())
        .bind(finalization.upstream_transport)
        .bind(finalization.service_tier)
        .bind(finalization.provider_metadata_json.map(sqlx::types::Json))
        .bind(finalization.error_details)
        .bind(finalization.diagnostic_trace_json.map(sqlx::types::Json))
        .bind(finalization.billing_snapshot_json.map(sqlx::types::Json))
        .bind(observation)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| {
            crate::core_store_error(
                super::sqlite_unavailable("finalize SQLite model request").with_source(source),
            )
        })?;
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
                 request_observation_json = json_set(request_observation_json, '$.error.message', 'request did not reach a terminal state'),
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
             upstream_model_id = ?5, upstream_transport = ?6, attempt_count = ?8,
             request_observation_json = json_set(request_observation_json,
               '$.account', json_object(
                 'name', (select name from provider_accounts where id = ?3),
                 'email', (select email from provider_accounts where id = ?3),
                 'authenticationKind', (select authentication_kind from provider_accounts where id = ?3)),
               '$.transport', json_object('httpVersion', ?7),
               '$.scheduling.accountSelectionWaitMs', case when ?9 is null
                 then json_extract(request_observation_json, '$.scheduling.accountSelectionWaitMs')
                 else coalesce(json_extract(request_observation_json, '$.scheduling.accountSelectionWaitMs'), 0) + ?9 end,
               '$.scheduling.capacityUsedSlots', coalesce(?10, json_extract(request_observation_json, '$.scheduling.capacityUsedSlots')),
               '$.scheduling.capacityTotalSlots', coalesce(?11, json_extract(request_observation_json, '$.scheduling.capacityTotalSlots')))
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
           request_observation_json = case when ?2 = 1 then json_set(request_observation_json,
               '$.recovery', json_object('retryDelayMs', max(0, (?5 - ?6) / 1000),
                                        'totalLatencyMs', max(0, (?4 - ?6) / 1000)))
               else request_observation_json end
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
           request_observation_json = json_set(request_observation_json, '$.recovery',
               json_object('retryDelayMs', max(0, (?4 - ?5) / 1000),
                           'totalLatencyMs', max(0, (?3 - ?5) / 1000)))
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

const fn send_state_str(value: UpstreamSendState) -> &'static str {
    match value {
        UpstreamSendState::NotSent => "not_sent",
        UpstreamSendState::Sent => "sent",
        UpstreamSendState::Ambiguous => "ambiguous",
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
