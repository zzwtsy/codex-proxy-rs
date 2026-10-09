//! 单行 `model_requests` 生命周期与最终 usage/cost 的 PostgreSQL owner

use crate::core_store_error;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use gateway_core::engine::{
    AttemptRecord as CoreAttemptRecord, ExecutionStore, IntermediateFailure,
    ModelRequestFinalization as CoreModelRequestFinalization, ModelRequestId,
    NewModelRequest as CoreNewModelRequest, ProbeFailure, RecoveryReport as CoreRecoveryReport,
};
use gateway_core::error::{StoreError as CoreStoreError, StoreErrorKind as CoreStoreErrorKind};
use gateway_core::upstream::UpstreamSendState as CoreUpstreamSendState;

use crate::{ConflictKind, StoreError, StoreResult, postgres_unavailable, require_nonempty};

pub use crate::execution::{
    ContinuationRequestObservation, CostSource, ModelRequestAttemptStart, ModelRequestFinalization,
    ModelRequestOutcome, ModelRequestTimings, ModelRequestUsage, NewModelRequest,
    UpstreamSendState,
};
use crate::execution::{
    ENTITY, finalization_row, invalid, new_model_request_row, send_state_from_core,
};
use crate::request_observation::RequestObservation;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ModelRequestRecoveryReport {
    pub requests: u64,
}

#[async_trait]
pub trait ModelRequestRepository: Send + Sync {
    async fn insert_model_request(&self, request: NewModelRequest) -> StoreResult<()>;
    /// 请求行与首次 attempt 列一次插入；`attempt.attempt_count` 必须为 1
    async fn insert_model_request_with_first_attempt(
        &self,
        request: NewModelRequest,
        attempt: ModelRequestAttemptStart,
    ) -> StoreResult<()>;
    async fn begin_model_request_attempt(
        &self,
        attempt: ModelRequestAttemptStart,
    ) -> StoreResult<u32>;
    async fn mark_upstream_send_state(
        &self,
        model_request_id: &str,
        state: UpstreamSendState,
    ) -> StoreResult<bool>;
    async fn mark_downstream_committed(
        &self,
        model_request_id: &str,
        committed_at: DateTime<Utc>,
        client_status_code: Option<u16>,
    ) -> StoreResult<bool>;
    async fn record_client_status_code(
        &self,
        model_request_id: &str,
        client_status_code: u16,
    ) -> StoreResult<bool>;
    async fn finalize_model_request(
        &self,
        finalization: ModelRequestFinalization,
    ) -> StoreResult<bool>;
    async fn recover_expired_model_requests(
        &self,
        now: DateTime<Utc>,
    ) -> StoreResult<ModelRequestRecoveryReport>;
}

#[derive(Clone)]
pub struct PgExecutionStore {
    pool: PgPool,
}

impl PgExecutionStore {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl ModelRequestRepository for PgExecutionStore {
    async fn insert_model_request(&self, request: NewModelRequest) -> StoreResult<()> {
        request.validate()?;
        let observation = RequestObservation::initial(&request)?;
        sqlx::query(
            "insert into model_requests (
               id, client_api_key_id, client_api_key_ref, operation, client_transport,
               requested_model_id, image_generation_requested, started_at, deadline_at,
               continuation_affinity_hash, continuation_requested, request_kind, request_observation_json
             ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(request.id)
        .bind(request.client_api_key_id)
        .bind(request.client_api_key_ref)
        .bind(request.operation)
        .bind(request.client_transport)
        .bind(request.requested_model_id)
        .bind(request.image_generation_requested)
        .bind(request.started_at)
        .bind(request.deadline_at)
        .bind(request.continuation.affinity_hash)
        .bind(request.continuation.requested)
        .bind(request.request_kind)
        .bind(observation)
        .execute(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("insert model request", source))?;
        Ok(())
    }

    async fn insert_model_request_with_first_attempt(
        &self,
        request: NewModelRequest,
        attempt: ModelRequestAttemptStart,
    ) -> StoreResult<()> {
        request.validate()?;
        attempt.validate()?;
        if attempt.attempt_count != 1 || attempt.model_request_id != request.id {
            return Err(invalid("first attempt must target the inserted request"));
        }
        let observation = RequestObservation::initial(&request)?;
        sqlx::query(
            "insert into model_requests (
               id, client_api_key_id, client_api_key_ref, operation, client_transport,
               requested_model_id, image_generation_requested, started_at, deadline_at,
               continuation_affinity_hash, continuation_requested, request_kind, request_observation_json,
               provider_kind, provider_account_id, provider_account_ref, upstream_model_id,
               upstream_transport, attempt_count, upstream_send_state
             ) select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
               $13::jsonb || jsonb_build_object(
                 'account', jsonb_strip_nulls(jsonb_build_object(
                   'name', account.name, 'email', account.email,
                   'authenticationKind', account.authentication_kind)),
                 'transport', jsonb_strip_nulls(jsonb_build_object('httpVersion', $19::text)),
                 'scheduling', ($13::jsonb -> 'scheduling') || jsonb_strip_nulls(jsonb_build_object(
                   'accountSelectionWaitMs', $20::bigint,
                   'capacityUsedSlots', $21::bigint, 'capacityTotalSlots', $22::bigint))),
               $14, $15, $16, $17, $18, 1, 'not_sent'
             from (values (true)) as seed(present)
             left join provider_accounts account on account.id = $15",
        )
        .bind(request.id)
        .bind(request.client_api_key_id)
        .bind(request.client_api_key_ref)
        .bind(request.operation)
        .bind(request.client_transport)
        .bind(request.requested_model_id)
        .bind(request.image_generation_requested)
        .bind(request.started_at)
        .bind(request.deadline_at)
        .bind(request.continuation.affinity_hash)
        .bind(request.continuation.requested)
        .bind(request.request_kind)
        .bind(observation)
        .bind(attempt.provider_kind)
        .bind(attempt.provider_account_id)
        .bind(attempt.provider_account_ref)
        .bind(attempt.upstream_model_id)
        .bind(attempt.upstream_transport)
        .bind(attempt.http_version)
        .bind(optional_i64(
            attempt.account_selection_wait_ms,
            "account_selection_wait_ms",
        )?)
        .bind(optional_i64(
            attempt.capacity_used_slots,
            "capacity_used_slots",
        )?)
        .bind(optional_i64(
            attempt.capacity_total_slots,
            "capacity_total_slots",
        )?)
        .execute(&self.pool)
        .await
        .map_err(|source| {
            postgres_unavailable("insert model request with first attempt", source)
        })?;
        Ok(())
    }

    async fn begin_model_request_attempt(
        &self,
        attempt: ModelRequestAttemptStart,
    ) -> StoreResult<u32> {
        attempt.validate()?;
        // upstream_send_state 是请求级单调水位（sent > ambiguous > not_sent），
        // 由 mark/finalize 抬升；开启新 attempt 不得把已持久化的 sent 重置回
        // not_sent——崩溃恢复的终态写回会原样继承本列
        let count = sqlx::query_scalar::<_, i32>(
            "update model_requests
             set provider_kind = $2,
                 provider_account_id = $3,
                 provider_account_ref = $4,
                 upstream_model_id = $5,
                 upstream_transport = $6,
                 attempt_count = $8,
                 request_observation_json = request_observation_json || jsonb_build_object(
                   'account', coalesce((select jsonb_strip_nulls(jsonb_build_object(
                     'name', name, 'email', email, 'authenticationKind', authentication_kind))
                     from provider_accounts where id = $3), '{}'::jsonb),
                   'transport', jsonb_strip_nulls(jsonb_build_object('httpVersion', $7::text)),
                   'scheduling', coalesce(request_observation_json -> 'scheduling', '{}'::jsonb) || jsonb_strip_nulls(jsonb_build_object(
                     'accountSelectionWaitMs', case when $9::bigint is null
                       then (request_observation_json #>> '{scheduling,accountSelectionWaitMs}')::bigint
                       else coalesce((request_observation_json #>> '{scheduling,accountSelectionWaitMs}')::bigint, 0) + $9 end,
                     'capacityUsedSlots', coalesce($10::bigint, (request_observation_json #>> '{scheduling,capacityUsedSlots}')::bigint),
                     'capacityTotalSlots', coalesce($11::bigint, (request_observation_json #>> '{scheduling,capacityTotalSlots}')::bigint))))
             where id = $1 and outcome = 'running' and downstream_committed_at is null
               and $8 = attempt_count + 1
             returning attempt_count",
        )
        .bind(&attempt.model_request_id)
        .bind(attempt.provider_kind)
        .bind(attempt.provider_account_id)
        .bind(attempt.provider_account_ref)
        .bind(attempt.upstream_model_id)
        .bind(attempt.upstream_transport)
        .bind(attempt.http_version)
        .bind(
            i32::try_from(attempt.attempt_count)
                .map_err(|source| invalid("attempt_count is too large").with_source(source))?,
        )
        .bind(optional_i64(
            attempt.account_selection_wait_ms,
            "account_selection_wait_ms",
        )?)
        .bind(optional_i64(
            attempt.capacity_used_slots,
            "capacity_used_slots",
        )?)
        .bind(optional_i64(
            attempt.capacity_total_slots,
            "capacity_total_slots",
        )?)
        .fetch_optional(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("begin model request attempt", source))?
        .ok_or(StoreError::Conflict {
            source: None,
            entity: ENTITY,
            id: attempt.model_request_id,
            kind: ConflictKind::DownstreamAlreadyCommitted,
        })?;
        u32::try_from(count)
            .map_err(|source| invalid("attempt_count is invalid").with_source(source))
    }

    async fn mark_upstream_send_state(
        &self,
        model_request_id: &str,
        state: UpstreamSendState,
    ) -> StoreResult<bool> {
        require_nonempty(ENTITY, "id", model_request_id)?;
        let result = sqlx::query(
            "update model_requests set upstream_send_state = $2
             where id = $1 and outcome = 'running' and attempt_count > 0",
        )
        .bind(model_request_id)
        .bind(state.as_str())
        .execute(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("mark upstream send state", source))?;
        Ok(result.rows_affected() == 1)
    }

    async fn mark_downstream_committed(
        &self,
        model_request_id: &str,
        committed_at: DateTime<Utc>,
        client_status_code: Option<u16>,
    ) -> StoreResult<bool> {
        require_nonempty(ENTITY, "id", model_request_id)?;
        validate_status_code(client_status_code)?;
        let result = sqlx::query(
            "update model_requests
             set downstream_committed_at = $2, client_status_code = $3
             where id = $1 and outcome = 'running' and downstream_committed_at is null
               and client_status_code is null",
        )
        .bind(model_request_id)
        .bind(committed_at)
        .bind(client_status_code.map(i32::from))
        .execute(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("mark downstream committed", source))?;
        Ok(result.rows_affected() == 1)
    }

    async fn record_client_status_code(
        &self,
        model_request_id: &str,
        client_status_code: u16,
    ) -> StoreResult<bool> {
        require_nonempty(ENTITY, "id", model_request_id)?;
        validate_status_code(Some(client_status_code))?;
        let result = sqlx::query(
            "update model_requests set client_status_code = $2
             where id = $1 and client_status_code is null",
        )
        .bind(model_request_id)
        .bind(i32::from(client_status_code))
        .execute(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("record client status code", source))?;
        Ok(result.rows_affected() == 1)
    }

    async fn finalize_model_request(
        &self,
        finalization: ModelRequestFinalization,
    ) -> StoreResult<bool> {
        finalization.validate()?;
        let observation = RequestObservation::finalized(&finalization)?;
        // 墙上时间可能回拨；终态必须可落盘，实际耗时仍保留 Core 的单调时钟观测
        let finalized = sqlx::query_scalar::<_, i64>(
            "with finalized as (
             update model_requests
             set outcome = $2, upstream_send_state = $3, attempt_count = $4,
                 downstream_committed_at = $5,
                 client_status_code = coalesce(client_status_code, $6), upstream_status_code = $7,
                 client_response_id = $8, upstream_request_id = $9, upstream_response_id = $10,
                 error_kind = $11, input_tokens = $12, output_tokens = $13,
                 cached_tokens = $14, cache_write_tokens = $15, reasoning_tokens = $16,
                 image_input_tokens = $17, image_output_tokens = $18, total_tokens = $19,
                 image_generation_succeeded = $20, cost_source = $21,
                 cost_amount = $22::numeric, cost_currency = $23,
                 completed_at = greatest($24, started_at),
                 upstream_transport = coalesce($25, upstream_transport), service_tier = $26,
                 provider_observation_json = $27, error_details = $28,
                 diagnostic_trace_json = $29, billing_snapshot_json = $30,
                 request_observation_json = request_observation_json || $31::jsonb || jsonb_build_object(
                   'transport', ($31::jsonb -> 'transport') || jsonb_strip_nulls(jsonb_build_object(
                     'httpVersion', coalesce($31::jsonb #>> '{transport,httpVersion}', request_observation_json #>> '{transport,httpVersion}'))),
                   'continuation', coalesce(request_observation_json -> 'continuation', '{}'::jsonb)
                     || ($31::jsonb -> 'continuation'))
             where id = $1 and outcome = 'running'
             returning id, client_api_key_ref, continuation_affinity_hash,
                       continuation_requested, provider_kind, upstream_transport,
                       outcome, started_at, completed_at
           ), recovery_target as (
             select prior.id
             from model_requests prior
             cross join finalized current
             where not current.continuation_requested
               and current.continuation_affinity_hash is not null
               and prior.client_api_key_ref = current.client_api_key_ref
               and prior.continuation_affinity_hash = current.continuation_affinity_hash
               and prior.continuation_requested
               and prior.outcome = 'failed'
               and prior.error_kind = 'continuation_recovery_required'
               and prior.recovered_at is null
               and current.started_at >= prior.completed_at
               and current.started_at <= prior.completed_at + interval '30 seconds'
             order by prior.completed_at desc, prior.id desc
             limit 1
           ), recovery_update as (
             update model_requests prior
             set recovery_attempt_count = prior.recovery_attempt_count + 1,
                 recovery_request_id = case
                   when current.outcome = 'succeeded' then current.id
                   else prior.recovery_request_id
                 end,
                 recovered_at = case
                   when current.outcome = 'succeeded' then current.completed_at
                   else prior.recovered_at
                 end,
                 request_observation_json = case when current.outcome = 'succeeded' then
                   prior.request_observation_json || jsonb_build_object('recovery', jsonb_build_object(
                     'retryDelayMs', greatest(0, floor(extract(epoch from (current.started_at - prior.completed_at)) * 1000))::bigint,
                     'totalLatencyMs', greatest(0, floor(extract(epoch from (current.completed_at - prior.completed_at)) * 1000))::bigint))
                   else prior.request_observation_json end
             from finalized current
             join recovery_target target on true
             where prior.id = target.id
               and prior.recovered_at is null
             returning prior.id
           ), session_transport_recovery_target as (
             select prior.id
             from model_requests prior
             cross join finalized current
             where current.outcome = 'succeeded'
               and current.provider_kind = 'openai'
               and current.upstream_transport = 'http_sse'
               and current.continuation_affinity_hash is not null
               and prior.client_api_key_ref = current.client_api_key_ref
               and prior.continuation_affinity_hash = current.continuation_affinity_hash
               and prior.provider_kind = current.provider_kind
               and prior.outcome = 'failed'
               and prior.error_kind = 'upstream_unavailable'
               and prior.upstream_transport = 'websocket'
               and prior.upstream_send_state = 'ambiguous'
               and prior.downstream_committed_at is null
               and prior.recovered_at is null
               and current.started_at >= prior.completed_at
               and current.started_at <= prior.completed_at + interval '30 seconds'
           ), session_transport_recovery_update as (
             update model_requests prior
             set recovery_attempt_count = prior.recovery_attempt_count + 1,
                 recovery_request_id = current.id,
                 recovered_at = current.completed_at,
                 request_observation_json = prior.request_observation_json || jsonb_build_object('recovery', jsonb_build_object(
                   'retryDelayMs', greatest(0, floor(extract(epoch from (current.started_at - prior.completed_at)) * 1000))::bigint,
                   'totalLatencyMs', greatest(0, floor(extract(epoch from (current.completed_at - prior.completed_at)) * 1000))::bigint))
             from finalized current
             join session_transport_recovery_target target on true
             where prior.id = target.id
               and prior.recovered_at is null
             returning prior.id
           )
           select (select count(*) from finalized)::bigint
                + (select count(*) * 0 from recovery_update)::bigint
                + (select count(*) * 0 from session_transport_recovery_update)::bigint",
        )
        .bind(&finalization.model_request_id)
        .bind(finalization.outcome.as_str())
        .bind(finalization.upstream_send_state.as_str())
        .bind(
            i32::try_from(finalization.attempt_count)
                .map_err(|source| invalid("attempt_count is too large").with_source(source))?,
        )
        .bind(finalization.downstream_committed_at)
        .bind(finalization.client_status_code.map(i32::from))
        .bind(finalization.upstream_status_code.map(i32::from))
        .bind(finalization.client_response_id.map(String::into_bytes))
        .bind(finalization.upstream_request_id)
        .bind(finalization.upstream_response_id.map(String::into_bytes))
        .bind(finalization.error_kind)
        .bind(optional_i64(finalization.usage.input_tokens, "input_tokens")?)
        .bind(optional_i64(finalization.usage.output_tokens, "output_tokens")?)
        .bind(optional_i64(finalization.usage.cached_tokens, "cached_tokens")?)
        .bind(optional_i64(finalization.usage.cache_write_tokens, "cache_write_tokens")?)
        .bind(optional_i64(finalization.usage.reasoning_tokens, "reasoning_tokens")?)
        .bind(optional_i64(finalization.usage.image_input_tokens, "image_input_tokens")?)
        .bind(optional_i64(finalization.usage.image_output_tokens, "image_output_tokens")?)
        .bind(optional_i64(finalization.usage.total_tokens, "total_tokens")?)
        .bind(finalization.image_generation_succeeded)
        .bind(finalization.cost_source.as_str())
        .bind(finalization.cost_amount.map(|amount| amount.to_string()))
        .bind(finalization.cost_currency)
        .bind(finalization.completed_at)
        .bind(finalization.upstream_transport)
        .bind(finalization.service_tier)
        .bind(finalization.provider_metadata_json.map(sqlx::types::Json))
        .bind(finalization.error_details)
        .bind(finalization.diagnostic_trace_json.map(sqlx::types::Json))
        .bind(finalization.billing_snapshot_json.map(sqlx::types::Json))
        .bind(observation)
        .fetch_one(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("finalize model request", source))?;
        Ok(finalized == 1)
    }

    async fn recover_expired_model_requests(
        &self,
        now: DateTime<Utc>,
    ) -> StoreResult<ModelRequestRecoveryReport> {
        let result = sqlx::query(
            "update model_requests
             set outcome = 'incomplete', error_kind = 'process_interrupted',
                 request_observation_json = request_observation_json || jsonb_build_object('error',
                   jsonb_build_object('message', 'request did not reach a terminal state')),
                 image_generation_succeeded = case
                   when image_generation_requested then false else null
                 end,
                 completed_at = $1
             where outcome = 'running' and deadline_at <= $1",
        )
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|source| postgres_unavailable("recover expired model requests", source))?;
        Ok(ModelRequestRecoveryReport {
            requests: result.rows_affected(),
        })
    }
}

#[async_trait]
impl ExecutionStore for PgExecutionStore {
    fn maintain_request(
        &self,
        request_id: &ModelRequestId,
        deadline: gateway_core::lifecycle::Deadline,
    ) -> Box<dyn gateway_core::lifecycle::LeaseGuard> {
        let pool = self.pool.clone();
        let request_id = request_id.as_str().to_owned();
        Box::new(crate::lease_renewal::LeaseRenewal::spawn(
            deadline,
            // 请求记录为 best-effort 观测，续期失败不能取消客户端执行
            None,
            move |ttl| {
                let pool = pool.clone();
                let request_id = request_id.clone();
                Box::pin(async move {
                    let ttl_ms = i64::try_from(ttl.as_millis()).map_err(|_| {
                        crate::StoreError::InvalidData {
                            source: None,
                            entity: "model request",
                            message: "lease TTL is invalid".to_owned(),
                        }
                    })?;
                    // 首次观测可能尚在队列中；缺行不创建记录，已终结行不改写
                    sqlx::query_scalar::<_, bool>(
                        "with renewed as (
                           update model_requests set deadline_at = now() + $2 * interval '1 millisecond'
                           where id = $1 and outcome = 'running' and deadline_at > now()
                           returning id
                         )
                         select exists(select 1 from renewed)
                           or not exists(select 1 from model_requests where id = $1 and outcome = 'running')",
                    )
                    .bind(&request_id)
                    .bind(ttl_ms)
                    .fetch_one(&pool)
                    .await
                    .map_err(|source| postgres_unavailable("renew model request recovery lease", source))
                })
            },
        ))
    }

    async fn create_model_request(
        &self,
        request: CoreNewModelRequest,
    ) -> Result<(), CoreStoreError> {
        self.insert_model_request(new_model_request_row(request))
            .await
            .map_err(core_store_error)
    }

    async fn record_attempt(&self, attempt: CoreAttemptRecord) -> Result<(), CoreStoreError> {
        let expected_count = attempt.attempt_count.get();
        let persisted = self
            .begin_model_request_attempt(attempt_start_row(attempt))
            .await
            .map_err(core_store_error)?;
        if persisted == expected_count {
            Ok(())
        } else {
            Err(CoreStoreError::new(CoreStoreErrorKind::InvalidState))
        }
    }

    async fn create_model_request_with_attempt(
        &self,
        request: CoreNewModelRequest,
        attempt: CoreAttemptRecord,
    ) -> Result<(), CoreStoreError> {
        // 合并写只对首次 attempt 成立（插入即带 attempt 列）；其余走基础两步
        if attempt.attempt_count.get() != 1 {
            self.create_model_request(request).await?;
            return self.record_attempt(attempt).await;
        }
        self.insert_model_request_with_first_attempt(
            new_model_request_row(request),
            attempt_start_row(attempt),
        )
        .await
        .map_err(core_store_error)
    }

    async fn mark_send_state(
        &self,
        request_id: &ModelRequestId,
        state: CoreUpstreamSendState,
    ) -> Result<(), CoreStoreError> {
        let updated = self
            .mark_upstream_send_state(request_id.as_str(), send_state_from_core(state))
            .await
            .map_err(core_store_error)?;
        require_core_update(updated)
    }

    async fn mark_downstream_committed(
        &self,
        request_id: &ModelRequestId,
        committed_at: std::time::SystemTime,
        client_status_code: Option<u16>,
    ) -> Result<(), CoreStoreError> {
        let updated = ModelRequestRepository::mark_downstream_committed(
            self,
            request_id.as_str(),
            DateTime::<Utc>::from(committed_at),
            client_status_code,
        )
        .await
        .map_err(core_store_error)?;
        require_core_update(updated)
    }

    async fn record_client_status(
        &self,
        request_id: &ModelRequestId,
        client_status_code: u16,
    ) -> Result<(), CoreStoreError> {
        let updated = ModelRequestRepository::record_client_status_code(
            self,
            request_id.as_str(),
            client_status_code,
        )
        .await
        .map_err(core_store_error)?;
        require_core_update(updated)
    }

    async fn record_intermediate_failure(
        &self,
        failure: IntermediateFailure,
    ) -> Result<(), CoreStoreError> {
        let error = failure.error;
        let retry_after_ms = error
            .retry_after()
            .map(|duration| u64::try_from(duration.as_millis()))
            .transpose()
            .map_err(|source| CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source))?;
        super::OpsEventRepository::append_ops_event(
            &super::PgOpsEventRepository::new(self.pool.clone()),
            super::OpsEvent {
                id: Uuid::now_v7().to_string(),
                model_request_id: Some(failure.request_id.as_str().to_owned()),
                attempt_index: Some(failure.attempt_index.get()),
                level: super::OpsEventLevel::Warning,
                component: "routing".to_owned(),
                operation: failure.trigger.as_str().to_owned(),
                provider_kind: Some(failure.provider_kind.as_str().to_owned()),
                provider_account_id: failure.account_id.as_ref().map(|id| id.as_str().to_owned()),
                provider_account_ref: failure.account_id.as_ref().map(|id| id.as_str().to_owned()),
                upstream_model_id: failure
                    .upstream_model_id
                    .as_ref()
                    .map(|model| model.as_str().to_owned()),
                failure_kind: error.kind().as_str().to_owned(),
                upstream_send_state: Some(error.send_state().as_str().to_owned()),
                error_details: error.error_details(),
                status_code: error.upstream_status().or(failure.upstream_status_code),
                provider_error_code: error.upstream_code().map(|code| code.as_str().to_owned()),
                retry_after_ms,
                upstream_request_id: error
                    .upstream_request_id()
                    .map(|id| id.as_str().to_owned())
                    .or(failure.upstream_request_id),
                latency_ms: Some(
                    u64::try_from(failure.latency.as_millis()).map_err(|source| {
                        CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source)
                    })?,
                ),
                message: error.diagnostic().map_or_else(
                    || "intermediate upstream failure".to_owned(),
                    |diagnostic| diagnostic.as_str().to_owned(),
                ),
                created_at: Utc::now(),
            },
        )
        .await
        .map_err(core_store_error)
    }

    async fn record_entry_rejection(
        &self,
        rejection: gateway_core::engine::EntryRejection,
    ) -> Result<(), CoreStoreError> {
        let error = rejection.error;
        super::OpsEventRepository::append_ops_event(
            &super::PgOpsEventRepository::new(self.pool.clone()),
            super::OpsEvent {
                id: Uuid::now_v7().to_string(),
                model_request_id: None,
                attempt_index: None,
                level: super::OpsEventLevel::Warning,
                component: "request_entry".to_owned(),
                operation: "reject".to_owned(),
                provider_kind: None,
                provider_account_id: None,
                provider_account_ref: None,
                upstream_model_id: None,
                failure_kind: error.kind().as_str().to_owned(),
                upstream_send_state: None,
                error_details: error
                    .error_details()
                    .map(gateway_core::error::ErrorDetails::into_string),
                status_code: None,
                provider_error_code: error.client_error_code().map(str::to_owned),
                retry_after_ms: error
                    .retry_after()
                    .and_then(|delay| u64::try_from(delay.as_millis()).ok()),
                upstream_request_id: None,
                latency_ms: u64::try_from(rejection.latency.as_millis()).ok(),
                // 入口尚无 model_requests 行，关联 ID 放在安全消息中，不能伪造外键
                message: serde_json::json!({
                    "requestId": rejection.request_id.as_str(),
                    "clientKeyId": rejection.client_key_id.as_str(),
                    "message": error.client_message(),
                })
                .to_string(),
                created_at: Utc::now(),
            },
        )
        .await
        .map_err(core_store_error)
    }

    async fn record_probe_failure(&self, failure: ProbeFailure) -> Result<(), CoreStoreError> {
        let error = failure.error;
        let retry_after_ms = error
            .retry_after()
            .map(|duration| u64::try_from(duration.as_millis()))
            .transpose()
            .map_err(|source| CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source))?;
        let latency_ms = u64::try_from(failure.latency.as_millis())
            .map_err(|source| CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source))?;
        let provider_error_code = error.upstream_code().map(|code| code.as_str().to_owned());
        super::OpsEventRepository::append_ops_event(
            &super::PgOpsEventRepository::new(self.pool.clone()),
            super::OpsEvent {
                id: Uuid::now_v7().to_string(),
                model_request_id: None,
                attempt_index: None,
                level: super::OpsEventLevel::Warning,
                component: "account_probe".to_owned(),
                operation: "connection_test".to_owned(),
                provider_kind: Some(failure.provider_kind.as_str().to_owned()),
                provider_account_id: Some(failure.account_id.as_str().to_owned()),
                provider_account_ref: Some(failure.account_id.as_str().to_owned()),
                upstream_model_id: Some(failure.upstream_model_id.as_str().to_owned()),
                failure_kind: error.kind().as_str().to_owned(),
                upstream_send_state: Some(error.send_state().as_str().to_owned()),
                error_details: error.error_details(),
                status_code: error.upstream_status(),
                provider_error_code,
                retry_after_ms,
                upstream_request_id: error.upstream_request_id().map(|id| id.as_str().to_owned()),
                latency_ms: Some(latency_ms),
                message: error.diagnostic().map_or_else(
                    || "account connection test failed".to_owned(),
                    |diagnostic| diagnostic.as_str().to_owned(),
                ),
                created_at: Utc::now(),
            },
        )
        .await
        .map_err(core_store_error)
    }

    async fn finalize_model_request(
        &self,
        finalization: CoreModelRequestFinalization,
    ) -> Result<(), CoreStoreError> {
        let completed =
            ModelRequestRepository::finalize_model_request(self, finalization_row(finalization)?)
                .await
                .map_err(core_store_error)?;
        require_core_update(completed)
    }

    async fn recover_expired(
        &self,
        now: std::time::SystemTime,
    ) -> Result<CoreRecoveryReport, CoreStoreError> {
        let report = self
            .recover_expired_model_requests(DateTime::<Utc>::from(now))
            .await
            .map_err(core_store_error)?;
        Ok(CoreRecoveryReport {
            requests: report.requests,
        })
    }
}

fn require_core_update(updated: bool) -> Result<(), CoreStoreError> {
    if updated {
        Ok(())
    } else {
        Err(CoreStoreError::new(CoreStoreErrorKind::InvalidState))
    }
}

fn attempt_start_row(attempt: CoreAttemptRecord) -> ModelRequestAttemptStart {
    ModelRequestAttemptStart {
        model_request_id: attempt.request_id.as_str().to_owned(),
        attempt_count: attempt.attempt_count.get(),
        provider_kind: attempt.provider_kind.as_str().to_owned(),
        provider_account_id: attempt
            .provider_account_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        provider_account_ref: attempt
            .provider_account_ref
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        upstream_model_id: attempt
            .upstream_model_id
            .map(|model| model.as_str().to_owned()),
        upstream_transport: attempt.upstream_transport,
        http_version: attempt.http_version,
        account_selection_wait_ms: attempt.account_selection_wait_ms,
        capacity_used_slots: attempt.capacity_used_slots,
        capacity_total_slots: attempt.capacity_total_slots,
    }
}

fn optional_i64(value: Option<u64>, field: &'static str) -> StoreResult<Option<i64>> {
    value.map(|value| to_i64(value, field)).transpose()
}

fn validate_status_code(status: Option<u16>) -> StoreResult<()> {
    if status.is_some_and(|status| !(100..=599).contains(&status)) {
        return Err(invalid("HTTP status must be between 100 and 599"));
    }
    Ok(())
}

fn to_i64(value: u64, field: &'static str) -> StoreResult<i64> {
    i64::try_from(value).map_err(|source| invalid(field).with_source(source))
}

#[async_trait]
impl gateway_core::diagnostics::OperationalDiagnostics for PgExecutionStore {
    async fn record_failure(
        &self,
        failure: gateway_core::diagnostics::OperationalFailure,
    ) -> Result<(), CoreStoreError> {
        use super::{OpsEvent, OpsEventLevel, OpsEventRepository, PgOpsEventRepository};
        let event = OpsEvent {
            id: Uuid::now_v7().to_string(),
            model_request_id: None,
            attempt_index: None,
            level: OpsEventLevel::Warning,
            component: failure.component.to_owned(),
            operation: failure.operation.to_owned(),
            provider_kind: failure.provider_kind.map(|kind| kind.as_str().to_owned()),
            provider_account_id: failure.account_id.as_ref().map(|id| id.as_str().to_owned()),
            provider_account_ref: failure.account_id.map(|id| id.as_str().to_owned()),
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
            message: serde_json::json!({
                "correlationId": failure.correlation_id,
                "message": failure.message,
            })
            .to_string(),
            created_at: failure.occurred_at.into(),
        };
        PgOpsEventRepository::new(self.pool.clone())
            .append_ops_event(event)
            .await
            .map_err(core_store_error)
    }
}
