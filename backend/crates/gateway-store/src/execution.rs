//! 请求持久化事实与 Core 映射，两种数据库共用同一校验合同

use crate::{DecimalAmount, StoreError, StoreResult, require_nonempty};
use chrono::{DateTime, Utc};
use gateway_core::{
    engine::{
        ModelRequestFinalization as CoreModelRequestFinalization,
        NewModelRequest as CoreNewModelRequest,
    },
    error::{
        ProviderErrorKind, StoreError as CoreStoreError, StoreErrorKind as CoreStoreErrorKind,
    },
    metering::CostSource as CoreCostSource,
    routing::{AccountRoutingScopeKind, AccountRoutingSnapshot},
    upstream::UpstreamSendState as CoreUpstreamSendState,
};
use serde_json::Value;

pub(crate) const ENTITY: &str = "model request";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamSendState {
    NotSent,
    Sent,
    Ambiguous,
}

impl UpstreamSendState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotSent => "not_sent",
            Self::Sent => "sent",
            Self::Ambiguous => "ambiguous",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRequestOutcome {
    Succeeded,
    Failed,
    Cancelled,
    Incomplete,
}

impl ModelRequestOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Incomplete => "incomplete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostSource {
    ProviderReported,
    Calculated,
    Unavailable,
}

impl CostSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProviderReported => "provider_reported",
            Self::Calculated => "calculated",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewModelRequest {
    pub id: String,
    pub client_api_key_id: Option<String>,
    pub client_api_key_ref: String,
    pub config_revision: u64,
    pub routing_scope: String,
    pub routing_group_refs: Vec<String>,
    pub routing_group_names_snapshot: Value,
    pub protocol: String,
    pub operation: String,
    pub endpoint: String,
    pub client_transport: String,
    pub requested_model_id: Option<String>,
    pub client_ip: Option<String>,
    pub user_agent: Option<String>,
    pub reasoning_effort: Option<String>,
    pub reasoning_preset: Option<String>,
    pub request_kind: Option<String>,
    pub subagent_kind: Option<String>,
    pub compact: bool,
    pub continuation: ContinuationRequestObservation,
    pub image_generation_requested: bool,
    pub admission_decision_ms: Option<u64>,
    pub started_at: DateTime<Utc>,
    pub deadline_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuationRequestObservation {
    pub affinity_hash: Option<String>,
    pub previous_response_id_hash: Option<String>,
    pub requested: bool,
}

impl NewModelRequest {
    pub fn validate(&self) -> StoreResult<()> {
        require_nonempty(ENTITY, "id", &self.id)?;
        require_nonempty(ENTITY, "client_api_key_ref", &self.client_api_key_ref)?;
        require_nonempty(ENTITY, "protocol", &self.protocol)?;
        require_nonempty(ENTITY, "operation", &self.operation)?;
        require_nonempty(ENTITY, "endpoint", &self.endpoint)?;
        require_nonempty(ENTITY, "client_transport", &self.client_transport)?;
        if let Some(requested_model_id) = self.requested_model_id.as_deref() {
            require_nonempty(ENTITY, "requested_model_id", requested_model_id)?;
        }
        validate_routing_snapshot(
            &self.routing_scope,
            &self.routing_group_refs,
            &self.routing_group_names_snapshot,
        )?;
        if self.config_revision == 0 || self.started_at > self.deadline_at {
            return Err(invalid(
                "revision and deadline violate the frozen constraints",
            ));
        }
        if self
            .client_api_key_id
            .as_ref()
            .is_some_and(|id| id != &self.client_api_key_ref)
        {
            return Err(invalid("live client key ID must equal its historical ref"));
        }
        self.continuation.validate()?;
        Ok(())
    }
}

impl ContinuationRequestObservation {
    fn validate(&self) -> StoreResult<()> {
        for value in [
            self.affinity_hash.as_deref(),
            self.previous_response_id_hash.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if value.len() != 64
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(invalid("continuation observation hash is invalid"));
            }
        }
        if !self.requested && self.previous_response_id_hash.is_some() {
            return Err(invalid("continuation request facts do not agree"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRequestAttemptStart {
    pub model_request_id: String,
    pub attempt_count: u32,
    pub provider_kind: String,
    pub provider_account_id: Option<String>,
    pub provider_account_ref: Option<String>,
    pub upstream_model_id: Option<String>,
    pub upstream_transport: String,
    pub http_version: Option<String>,
    pub account_selection_wait_ms: Option<u64>,
    pub capacity_used_slots: Option<u64>,
    pub capacity_total_slots: Option<u64>,
}

impl ModelRequestAttemptStart {
    pub fn validate(&self) -> StoreResult<()> {
        for (field, value) in [
            ("model_request_id", self.model_request_id.as_str()),
            ("provider_kind", self.provider_kind.as_str()),
            ("upstream_transport", self.upstream_transport.as_str()),
        ] {
            require_nonempty(ENTITY, field, value)?;
        }
        if let Some(upstream_model_id) = self.upstream_model_id.as_deref() {
            require_nonempty(ENTITY, "upstream_model_id", upstream_model_id)?;
        }
        if self.attempt_count == 0 {
            return Err(invalid("attempt_count must be positive"));
        }
        match (self.capacity_used_slots, self.capacity_total_slots) {
            (None, None) => {}
            (Some(used), Some(total)) if total > 0 && used <= total => {}
            _ => return Err(invalid("capacity snapshot is invalid")),
        }
        if self
            .provider_account_id
            .as_ref()
            .is_some_and(|id| Some(id) != self.provider_account_ref.as_ref())
        {
            return Err(invalid(
                "live provider account ID must equal its historical ref",
            ));
        }
        Ok(())
    }
}

pub use gateway_core::{engine::ModelRequestTimings, metering::Usage as ModelRequestUsage};

fn validate_timings(timings: &ModelRequestTimings) -> StoreResult<()> {
    if let Some(total) = timings.latency_ms {
        let phases = [
            timings.transport_decision_wait_ms,
            timings.connect_ms,
            timings.headers_ms,
            timings.first_event_ms,
            timings.first_reasoning_ms,
            timings.first_text_ms,
            timings.first_token_ms,
        ];
        if phases.into_iter().flatten().any(|phase| phase > total) {
            return Err(invalid("timing phase exceeds total latency"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelRequestFinalization {
    pub billing_snapshot_json: Option<Value>,
    pub model_request_id: String,
    pub outcome: ModelRequestOutcome,
    pub upstream_send_state: UpstreamSendState,
    pub attempt_count: u32,
    pub downstream_committed_at: Option<DateTime<Utc>>,
    pub client_status_code: Option<u16>,
    pub upstream_status_code: Option<u16>,
    pub client_response_id: Option<String>,
    pub upstream_request_id: Option<String>,
    pub upstream_response_id: Option<String>,
    pub upstream_transport: Option<String>,
    pub http_version: Option<String>,
    pub websocket_pool: Option<String>,
    pub service_tier: Option<String>,
    pub upstream_response_model: Option<String>,
    pub provider_metadata_json: Option<Value>,
    pub diagnostic_trace_json: Option<Value>,
    pub error_kind: Option<String>,
    pub provider_error_code: Option<String>,
    pub error_message: Option<String>,
    pub error_details: Option<String>,
    pub continuation_unavailable_reason: Option<String>,
    pub upstream_connection_id: Option<String>,
    pub upstream_connection_exit_reason: Option<String>,
    pub upstream_connection_age_ms: Option<u64>,
    pub upstream_connection_idle_ms: Option<u64>,
    pub retry_after_ms: Option<u64>,
    pub usage: ModelRequestUsage,
    pub image_generation_succeeded: Option<bool>,
    pub cost_source: CostSource,
    pub cost_amount: Option<DecimalAmount>,
    pub cost_currency: Option<String>,
    pub timings: ModelRequestTimings,
    pub completed_at: DateTime<Utc>,
}

impl ModelRequestFinalization {
    pub fn validate(&self) -> StoreResult<()> {
        require_nonempty(ENTITY, "model_request_id", &self.model_request_id)?;
        for status in [self.client_status_code, self.upstream_status_code]
            .into_iter()
            .flatten()
        {
            if !(100..=599).contains(&status) {
                return Err(invalid("HTTP status must be between 100 and 599"));
            }
        }
        let cost_is_absent = self.cost_amount.is_none() && self.cost_currency.is_none();
        let cost_is_complete = self.cost_amount.is_some() && self.cost_currency.is_some();
        if (self.cost_source == CostSource::Unavailable && !cost_is_absent)
            || (self.cost_source != CostSource::Unavailable && !cost_is_complete)
        {
            return Err(invalid("cost source, amount, and currency do not agree"));
        }
        if let Some(currency) = &self.cost_currency
            && (currency.len() != 3 || !currency.bytes().all(|byte| byte.is_ascii_uppercase()))
        {
            return Err(invalid("cost currency must be three uppercase characters"));
        }
        for (field, value) in [
            ("upstream_transport", self.upstream_transport.as_deref()),
            ("http_version", self.http_version.as_deref()),
        ] {
            if let Some(value) = value {
                require_nonempty(ENTITY, field, value)?;
            }
        }
        if self
            .websocket_pool
            .as_deref()
            .is_some_and(|kind| !matches!(kind, "new" | "reuse"))
        {
            return Err(invalid("websocket pool must be new or reuse"));
        }
        if let Some(service_tier) = self.service_tier.as_deref() {
            require_nonempty(ENTITY, "service_tier", service_tier)?;
            if service_tier.len() > 64 || service_tier.chars().any(char::is_control) {
                return Err(invalid("service tier is invalid"));
            }
        }
        if let Some(model) = self.upstream_response_model.as_deref() {
            require_nonempty(ENTITY, "upstream_response_model", model)?;
            if model.len() > 256 || model.chars().any(char::is_control) {
                return Err(invalid("upstream response model is invalid"));
            }
        }
        if self
            .provider_metadata_json
            .as_ref()
            .is_some_and(|metadata| !metadata.is_object())
        {
            return Err(invalid("provider observation must be a JSON object"));
        }
        if self
            .diagnostic_trace_json
            .as_ref()
            .is_some_and(|trace| !trace.is_object() || trace.to_string().len() > 64 * 1024)
        {
            return Err(invalid(
                "diagnostic trace must be a JSON object within 64 KiB",
            ));
        }
        validate_optional_stable_reason(
            self.continuation_unavailable_reason.as_deref(),
            "continuation unavailable reason",
        )?;
        validate_connection_observation(self)?;
        validate_timings(&self.timings)
    }
}

pub(crate) const fn send_state_from_core(value: CoreUpstreamSendState) -> UpstreamSendState {
    match value {
        CoreUpstreamSendState::NotSent => UpstreamSendState::NotSent,
        CoreUpstreamSendState::Sent => UpstreamSendState::Sent,
        CoreUpstreamSendState::Ambiguous => UpstreamSendState::Ambiguous,
    }
}

fn outcome_from_core(
    value: gateway_core::engine::ExecutionOutcome,
) -> Result<ModelRequestOutcome, CoreStoreError> {
    match value {
        gateway_core::engine::ExecutionOutcome::Running => {
            Err(CoreStoreError::new(CoreStoreErrorKind::InvalidState))
        }
        gateway_core::engine::ExecutionOutcome::Succeeded => Ok(ModelRequestOutcome::Succeeded),
        gateway_core::engine::ExecutionOutcome::Failed => Ok(ModelRequestOutcome::Failed),
        gateway_core::engine::ExecutionOutcome::Cancelled => Ok(ModelRequestOutcome::Cancelled),
        gateway_core::engine::ExecutionOutcome::Incomplete => Ok(ModelRequestOutcome::Incomplete),
    }
}

const fn cost_source_from_core(value: CoreCostSource) -> CostSource {
    match value {
        CoreCostSource::ProviderReported => CostSource::ProviderReported,
        CoreCostSource::Calculated => CostSource::Calculated,
        CoreCostSource::Unavailable => CostSource::Unavailable,
    }
}

pub(crate) fn new_model_request_row(request: CoreNewModelRequest) -> NewModelRequest {
    let (routing_scope, routing_group_refs, routing_group_names_snapshot) =
        routing_snapshot_row(&request.routing);
    NewModelRequest {
        id: request.id.as_str().to_owned(),
        client_api_key_id: request
            .client_api_key_id
            .as_ref()
            .map(|id| id.as_str().to_owned()),
        client_api_key_ref: request.client_api_key_ref.as_str().to_owned(),
        config_revision: request.config_revision.get(),
        routing_scope,
        routing_group_refs,
        routing_group_names_snapshot,
        protocol: request.protocol,
        operation: request.operation.as_str().to_owned(),
        endpoint: request.endpoint,
        client_transport: request.client_transport,
        requested_model_id: request
            .requested_model
            .map(|model| model.as_str().to_owned()),
        client_ip: request.client_ip.map(|address| address.to_string()),
        user_agent: request.user_agent,
        reasoning_effort: request.reasoning_effort,
        reasoning_preset: request.reasoning_preset,
        request_kind: request.request_kind,
        subagent_kind: request.subagent_kind,
        compact: request.compact,
        continuation: ContinuationRequestObservation {
            affinity_hash: request.continuation.affinity_hash,
            previous_response_id_hash: request.continuation.previous_response_id_hash,
            requested: request.continuation.requested,
        },
        image_generation_requested: request.image_generation_requested,
        admission_decision_ms: request.admission_decision_ms,
        started_at: DateTime::<Utc>::from(request.started_at),
        deadline_at: DateTime::<Utc>::from(request.deadline_at.lease_deadline()),
    }
}

fn routing_snapshot_row(snapshot: &AccountRoutingSnapshot) -> (String, Vec<String>, Value) {
    let groups = snapshot.groups_snapshot();
    (
        snapshot.kind().as_str().to_owned(),
        groups
            .iter()
            .map(|group| group.id().as_str().to_owned())
            .collect(),
        Value::Array(
            groups
                .iter()
                .map(|group| Value::String(group.name().to_owned()))
                .collect(),
        ),
    )
}

fn validate_routing_snapshot(scope: &str, refs: &[String], names: &Value) -> StoreResult<()> {
    let Some(names) = names.as_array() else {
        return Err(invalid("routing group names snapshot must be an array"));
    };
    let valid = match scope {
        value if value == AccountRoutingScopeKind::All.as_str() => {
            refs.is_empty() && names.is_empty()
        }
        value if value == AccountRoutingScopeKind::Groups.as_str() => {
            !refs.is_empty()
                && refs.len() == names.len()
                && refs.iter().all(|id| !id.is_empty())
                && names
                    .iter()
                    .all(|name| name.as_str().is_some_and(|name| !name.is_empty()))
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid("invalid routing scope snapshot"))
    }
}

fn validate_optional_stable_reason(value: Option<&str>, field: &str) -> StoreResult<()> {
    if value.is_some_and(|value| {
        value.is_empty()
            || value.len() > 64
            || !value.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase() || (index > 0 && (byte.is_ascii_digit() || byte == b'_'))
            })
    }) {
        return Err(invalid(field));
    }
    Ok(())
}

fn validate_connection_observation(finalization: &ModelRequestFinalization) -> StoreResult<()> {
    let fields_present = [
        finalization.upstream_connection_id.is_some(),
        finalization.upstream_connection_exit_reason.is_some(),
        finalization.upstream_connection_age_ms.is_some(),
        finalization.upstream_connection_idle_ms.is_some(),
    ];
    if fields_present.iter().any(|present| *present)
        && !fields_present.iter().all(|present| *present)
    {
        return Err(invalid("upstream connection observation is incomplete"));
    }
    if let Some(connection_id) = finalization.upstream_connection_id.as_deref()
        && (connection_id.is_empty()
            || connection_id.len() > 128
            || connection_id.chars().any(char::is_control))
    {
        return Err(invalid("upstream connection ID is invalid"));
    }
    validate_optional_stable_reason(
        finalization.upstream_connection_exit_reason.as_deref(),
        "upstream connection exit reason is invalid",
    )?;
    if finalization
        .upstream_connection_age_ms
        .zip(finalization.upstream_connection_idle_ms)
        .is_some_and(|(age, idle)| idle > age)
    {
        return Err(invalid("upstream connection idle time exceeds its age"));
    }
    Ok(())
}

pub(crate) fn invalid(message: &str) -> StoreError {
    StoreError::InvalidData {
        source: None,
        entity: ENTITY,
        message: message.to_owned(),
    }
}
pub(crate) fn finalization_row(
    finalization: CoreModelRequestFinalization,
) -> Result<ModelRequestFinalization, CoreStoreError> {
    let continuation_unavailable_reason = finalization
        .failure_observation
        .continuation_unavailable_reason
        .clone();
    let connection_observation = finalization
        .failure_observation
        .upstream_connection
        .as_ref();
    let upstream_connection_id =
        connection_observation.map(|observation| observation.connection_id().to_owned());
    let upstream_connection_exit_reason =
        connection_observation.map(|observation| observation.exit_reason().to_owned());
    let upstream_connection_age_ms = connection_observation.map(|observation| observation.age_ms());
    let upstream_connection_idle_ms =
        connection_observation.map(|observation| observation.idle_ms());
    let provider_metadata_json = finalization
        .provider_metadata_json
        .as_deref()
        .map(serde_json::from_str::<Value>)
        .transpose()
        .map_err(|source| CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source))?;
    if provider_metadata_json
        .as_ref()
        .is_some_and(|metadata| !metadata.is_object())
    {
        return Err(CoreStoreError::new(CoreStoreErrorKind::InvalidData));
    }
    let (cost_source, cost_amount, cost_currency) = match finalization.cost.total() {
        Some(total) => (
            cost_source_from_core(finalization.cost.source()),
            Some(
                total
                    .amount()
                    .to_string()
                    .parse::<DecimalAmount>()
                    .map_err(|source| {
                        CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source)
                    })?,
            ),
            Some(total.currency().as_str().to_owned()),
        ),
        None => (CostSource::Unavailable, None, None),
    };
    let error_kind = if continuation_unavailable_reason.is_some() {
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
    Ok(ModelRequestFinalization {
        billing_snapshot_json: finalization
            .cost
            .breakdown()
            .map(crate::billing::encode_billing_snapshot),
        model_request_id: finalization.request_id.as_str().to_owned(),
        outcome: outcome_from_core(finalization.outcome)?,
        upstream_send_state: send_state_from_core(finalization.send_state),
        attempt_count: finalization.attempt_count,
        downstream_committed_at: finalization
            .downstream_committed_at
            .map(DateTime::<Utc>::from),
        client_status_code: finalization.client_status_code,
        upstream_status_code: finalization.upstream_status_code,
        client_response_id: finalization.client_response_id,
        upstream_request_id: finalization.upstream_request_id,
        upstream_response_id: finalization.upstream_response_id,
        upstream_transport: finalization.upstream_transport,
        http_version: finalization.http_version,
        websocket_pool: finalization.websocket_pool,
        service_tier: finalization.service_tier,
        upstream_response_model: finalization.upstream_response_model,
        provider_metadata_json,
        diagnostic_trace_json: finalization
            .diagnostic_trace_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|source| CoreStoreError::caused_by(CoreStoreErrorKind::InvalidData, source))?,
        error_kind,
        provider_error_code: finalization.provider_error_code,
        error_message,
        error_details: finalization.error_details,
        continuation_unavailable_reason,
        upstream_connection_id,
        upstream_connection_exit_reason,
        upstream_connection_age_ms,
        upstream_connection_idle_ms,
        retry_after_ms: finalization.retry_after_ms,
        usage: finalization.usage,
        image_generation_succeeded: finalization.image_generation_succeeded,
        cost_source,
        cost_amount,
        cost_currency,
        timings: finalization.timings,
        completed_at: DateTime::<Utc>::from(finalization.completed_at),
    })
}
