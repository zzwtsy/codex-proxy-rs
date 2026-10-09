-- 请求观测统一为 JSON，业务状态与计量保留列
-- 先保存并移除事件子表，避免重建请求表触发外键级联删除
create temporary table request_observation_events_backup as select * from ops_events;
drop table ops_events;
create table model_requests_observation_upgrade (
  id text primary key,
  client_api_key_id text,
  client_api_key_ref text not null,
  operation text not null,
  client_transport text not null,
  requested_model_id text,
  provider_kind text,
  upstream_model_id text,
  service_tier text,
  provider_account_id text,
  provider_account_ref text,
  upstream_transport text,
  provider_observation_json text check (
    provider_observation_json is null or
    (json_valid(provider_observation_json) and json_type(provider_observation_json) = 'object')
  ),
  attempt_count integer not null default 0 check (attempt_count >= 0),
  upstream_send_state text not null default 'not_sent'
    check (upstream_send_state in ('not_sent', 'sent', 'ambiguous')),
  downstream_committed_at_us integer,
  outcome text not null default 'running'
    check (outcome in ('running', 'succeeded', 'failed', 'cancelled', 'incomplete')),
  client_status_code integer check (client_status_code is null or client_status_code between 100 and 599),
  upstream_status_code integer check (upstream_status_code is null or upstream_status_code between 100 and 599),
  client_response_id blob,
  upstream_request_id text,
  upstream_response_id blob,
  error_kind text,
  error_details text,
  input_tokens integer check (input_tokens is null or input_tokens >= 0),
  output_tokens integer check (output_tokens is null or output_tokens >= 0),
  cached_tokens integer check (cached_tokens is null or cached_tokens >= 0),
  cache_write_tokens integer check (cache_write_tokens is null or cache_write_tokens >= 0),
  reasoning_tokens integer check (reasoning_tokens is null or reasoning_tokens >= 0),
  image_input_tokens integer check (image_input_tokens is null or image_input_tokens >= 0),
  image_output_tokens integer check (image_output_tokens is null or image_output_tokens >= 0),
  total_tokens integer check (total_tokens is null or total_tokens >= 0),
  image_generation_succeeded integer check (
    image_generation_succeeded is null or image_generation_succeeded in (0, 1)
  ),
  cost_source text not null default 'unavailable'
    check (cost_source in ('provider_reported', 'calculated', 'unavailable')),
  cost_amount text check (
    cost_amount is null or (length(cost_amount) = 20 and cost_amount not glob '*[^0-9]*')
  ),
  cost_currency text,
  billing_snapshot_json text check (
    billing_snapshot_json is null or
    (json_valid(billing_snapshot_json) and json_type(billing_snapshot_json) = 'object')
  ),
  request_kind text,
  image_generation_requested integer not null default 0 check (image_generation_requested in (0, 1)),
  started_at_us integer not null,
  deadline_at_us integer not null,
  completed_at_us integer,
  continuation_affinity_hash text,
  continuation_requested integer not null default 0 check (continuation_requested in (0, 1)),
  recovery_request_id text,
  recovered_at_us integer,
  recovery_attempt_count integer not null default 0 check (recovery_attempt_count >= 0),
  diagnostic_trace_json text check (
    diagnostic_trace_json is null or
    (json_valid(diagnostic_trace_json) and json_type(diagnostic_trace_json) = 'object'
      and length(diagnostic_trace_json) <= 65536)
  ),
  request_observation_json text not null check (json_valid(request_observation_json) and json_type(request_observation_json) = 'object' and length(cast(request_observation_json as blob)) <= 1048576),
  check (json_extract(request_observation_json, '$.request.configRevision') > 0),
  check (json_type(request_observation_json, '$.request.configRevision') is not null and json_type(request_observation_json, '$.request.configRevision') <> 'null'),
  check (json_type(request_observation_json, '$.request.protocol') is not null and json_type(request_observation_json, '$.request.protocol') <> 'null'),
  check (json_type(request_observation_json, '$.request.endpoint') is not null and json_type(request_observation_json, '$.request.endpoint') <> 'null'),
  check (json_extract(request_observation_json, '$.error.retryAfterMs') is null or json_extract(request_observation_json, '$.error.retryAfterMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.transportDecisionWaitMs') is null or json_extract(request_observation_json, '$.timings.local.transportDecisionWaitMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.connectMs') is null or json_extract(request_observation_json, '$.timings.local.connectMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.headersMs') is null or json_extract(request_observation_json, '$.timings.local.headersMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.firstEventMs') is null or json_extract(request_observation_json, '$.timings.local.firstEventMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.firstReasoningMs') is null or json_extract(request_observation_json, '$.timings.local.firstReasoningMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.firstTextMs') is null or json_extract(request_observation_json, '$.timings.local.firstTextMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.firstTokenMs') is null or json_extract(request_observation_json, '$.timings.local.firstTokenMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.upstream.processingMs') is null or json_extract(request_observation_json, '$.timings.upstream.processingMs') >= 0),
  check (json_extract(request_observation_json, '$.timings.local.latencyMs') is null or json_extract(request_observation_json, '$.timings.local.latencyMs') >= 0),
  check (json_extract(request_observation_json, '$.request.compact') in (0, 1)),
  check (json_type(request_observation_json, '$.request.compact') is not null and json_type(request_observation_json, '$.request.compact') <> 'null'),
  check (json_extract(request_observation_json, '$.routing.scope') in ('legacy_provider', 'all', 'groups')),
  check (json_type(request_observation_json, '$.routing.scope') is not null and json_type(request_observation_json, '$.routing.scope') <> 'null'),
  check (json_valid(json_extract(request_observation_json, '$.routing.groupRefs')) and json_type(json_extract(request_observation_json, '$.routing.groupRefs')) = 'array'),
  check (json_type(request_observation_json, '$.routing.groupRefs') is not null and json_type(request_observation_json, '$.routing.groupRefs') <> 'null'),
  check (json_valid(json_extract(request_observation_json, '$.routing.groupNamesSnapshot')) and json_type(json_extract(request_observation_json, '$.routing.groupNamesSnapshot')) = 'array'),
  check (json_type(request_observation_json, '$.routing.groupNamesSnapshot') is not null and json_type(request_observation_json, '$.routing.groupNamesSnapshot') <> 'null'),
  check (json_extract(request_observation_json, '$.scheduling.admissionDecisionMs') is null or json_extract(request_observation_json, '$.scheduling.admissionDecisionMs') >= 0),
  check (json_extract(request_observation_json, '$.scheduling.accountSelectionWaitMs') is null or json_extract(request_observation_json, '$.scheduling.accountSelectionWaitMs') >= 0),
  check (client_api_key_id is null or client_api_key_id = client_api_key_ref),
  check (provider_account_id is null or provider_account_id = provider_account_ref),
  check (
    (attempt_count = 0 and upstream_send_state = 'not_sent') or
    (attempt_count > 0 and provider_kind is not null and provider_account_ref is not null
      and upstream_transport is not null)
  ),
  check (
    (image_generation_requested = 0 and image_generation_succeeded is null) or
    (image_generation_requested = 1 and
      ((outcome = 'running' and image_generation_succeeded is null) or
       (outcome <> 'running' and image_generation_succeeded is not null)))
  ),
  check (json_extract(request_observation_json, '$.transport.websocketPool') is null or json_extract(request_observation_json, '$.transport.websocketPool') in ('new', 'reuse')),
  check (service_tier is null or (length(service_tier) between 1 and 64 and service_tier not glob '*[' || char(1) || '-' || char(31) || ']*')),
  check (
    (json_extract(request_observation_json, '$.routing.scope') in ('legacy_provider', 'all') and
      json_array_length(json_extract(request_observation_json, '$.routing.groupRefs')) = 0 and
      json_array_length(json_extract(request_observation_json, '$.routing.groupNamesSnapshot')) = 0) or
    (json_extract(request_observation_json, '$.routing.scope') = 'groups' and json_array_length(json_extract(request_observation_json, '$.routing.groupRefs')) > 0 and
      json_array_length(json_extract(request_observation_json, '$.routing.groupRefs')) = json_array_length(json_extract(request_observation_json, '$.routing.groupNamesSnapshot')))
  ),
  check (
    (cost_source = 'unavailable' and cost_amount is null and cost_currency is null) or
    (cost_source in ('provider_reported', 'calculated') and cost_amount is not null
      and cost_currency glob '[A-Z][A-Z][A-Z]')
  ),
  check (cost_amount is null or cost_amount not glob '*[^0-9]*'),
  check (started_at_us <= deadline_at_us),
  check (
    (outcome = 'running' and completed_at_us is null) or
    (outcome <> 'running' and completed_at_us is not null and completed_at_us >= started_at_us)
  ),
  check (
    json_extract(request_observation_json, '$.timings.local.latencyMs') is null or
    ((json_extract(request_observation_json, '$.timings.local.transportDecisionWaitMs') is null or json_extract(request_observation_json, '$.timings.local.transportDecisionWaitMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')) and
     (json_extract(request_observation_json, '$.timings.local.connectMs') is null or json_extract(request_observation_json, '$.timings.local.connectMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')) and
     (json_extract(request_observation_json, '$.timings.local.headersMs') is null or json_extract(request_observation_json, '$.timings.local.headersMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')) and
     (json_extract(request_observation_json, '$.timings.local.firstEventMs') is null or json_extract(request_observation_json, '$.timings.local.firstEventMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')) and
     (json_extract(request_observation_json, '$.timings.local.firstReasoningMs') is null or json_extract(request_observation_json, '$.timings.local.firstReasoningMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')) and
     (json_extract(request_observation_json, '$.timings.local.firstTextMs') is null or json_extract(request_observation_json, '$.timings.local.firstTextMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')) and
     (json_extract(request_observation_json, '$.timings.local.firstTokenMs') is null or json_extract(request_observation_json, '$.timings.local.firstTokenMs') <= json_extract(request_observation_json, '$.timings.local.latencyMs')))
  ),
  check (
    (json_extract(request_observation_json, '$.scheduling.capacityUsedSlots') is null and json_extract(request_observation_json, '$.scheduling.capacityTotalSlots') is null) or
    (json_extract(request_observation_json, '$.scheduling.capacityUsedSlots') is not null and json_extract(request_observation_json, '$.scheduling.capacityTotalSlots') is not null and
      json_extract(request_observation_json, '$.scheduling.capacityTotalSlots') > 0 and json_extract(request_observation_json, '$.scheduling.capacityUsedSlots') >= 0 and
      json_extract(request_observation_json, '$.scheduling.capacityUsedSlots') <= json_extract(request_observation_json, '$.scheduling.capacityTotalSlots'))
  ),
  check (
    (continuation_affinity_hash is null or
      (length(continuation_affinity_hash) = 64 and continuation_affinity_hash not glob '*[^0-9a-f]*')) and
    (json_extract(request_observation_json, '$.continuation.previousResponseIdHash') is null or
      (length(json_extract(request_observation_json, '$.continuation.previousResponseIdHash')) = 64 and json_extract(request_observation_json, '$.continuation.previousResponseIdHash') not glob '*[^0-9a-f]*')) and
    (continuation_requested = 1 or json_extract(request_observation_json, '$.continuation.previousResponseIdHash') is null)
  ),
  check (json_extract(request_observation_json, '$.continuation.unavailableReason') is null or
    (length(json_extract(request_observation_json, '$.continuation.unavailableReason')) between 1 and 64 and
      json_extract(request_observation_json, '$.continuation.unavailableReason') glob '[a-z]*')),
  check (
    (json_extract(request_observation_json, '$.transport.connection.id') is null and json_extract(request_observation_json, '$.transport.connection.exitReason') is null and
      json_extract(request_observation_json, '$.transport.connection.ageMs') is null and json_extract(request_observation_json, '$.transport.connection.idleMs') is null) or
    (json_extract(request_observation_json, '$.transport.connection.id') is not null and length(json_extract(request_observation_json, '$.transport.connection.id')) between 1 and 128 and
      json_extract(request_observation_json, '$.transport.connection.exitReason') is not null and length(json_extract(request_observation_json, '$.transport.connection.exitReason')) between 1 and 64 and
      json_extract(request_observation_json, '$.transport.connection.ageMs') >= 0 and json_extract(request_observation_json, '$.transport.connection.idleMs') between 0 and json_extract(request_observation_json, '$.transport.connection.ageMs'))
  ),
  check (
    (recovered_at_us is null and recovery_request_id is null and
      json_extract(request_observation_json, '$.recovery.retryDelayMs') is null and json_extract(request_observation_json, '$.recovery.totalLatencyMs') is null) or
    (recovered_at_us is not null and completed_at_us is not null and
      recovered_at_us >= completed_at_us and recovery_request_id is not null and
      recovery_attempt_count > 0 and json_extract(request_observation_json, '$.recovery.retryDelayMs') >= 0 and
      json_extract(request_observation_json, '$.recovery.totalLatencyMs') >= json_extract(request_observation_json, '$.recovery.retryDelayMs'))
  ),
  foreign key (client_api_key_id) references client_api_keys(id) on update restrict on delete set null,
  foreign key (provider_account_id) references provider_accounts(id) on update restrict on delete set null,
  check (json_type(request_observation_json, '$.request.configRevision') = 'integer' and json_type(request_observation_json, '$.request.protocol') = 'text' and length(json_extract(request_observation_json, '$.request.protocol')) > 0 and json_type(request_observation_json, '$.request.endpoint') = 'text' and length(json_extract(request_observation_json, '$.request.endpoint')) > 0 and json_type(request_observation_json, '$.request.compact') in ('true','false')),
  check (json_type(request_observation_json, '$.timings.upstream.responseMs') is null or json_type(request_observation_json, '$.timings.upstream.responseMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.responseMs') in ('integer') and json_extract(request_observation_json, '$.timings.upstream.responseMs') > 0 and json_extract(request_observation_json, '$.timings.upstream.responseMs') <= 9223372036854775807)),
  check (json_type(request_observation_json, '$.timings.upstream.apiOverheadMs') is null or json_type(request_observation_json, '$.timings.upstream.apiOverheadMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.apiOverheadMs') in ('integer','real') and json_extract(request_observation_json, '$.timings.upstream.apiOverheadMs') >= 0 and json_extract(request_observation_json, '$.timings.upstream.apiOverheadMs') <= 9223372036854775807)),
  check (json_type(request_observation_json, '$.timings.upstream.engineMs') is null or json_type(request_observation_json, '$.timings.upstream.engineMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.engineMs') in ('integer','real') and json_extract(request_observation_json, '$.timings.upstream.engineMs') >= 0 and json_extract(request_observation_json, '$.timings.upstream.engineMs') <= 9223372036854775807)),
  check (json_type(request_observation_json, '$.timings.upstream.engineIapiTtftMs') is null or json_type(request_observation_json, '$.timings.upstream.engineIapiTtftMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.engineIapiTtftMs') in ('integer','real') and json_extract(request_observation_json, '$.timings.upstream.engineIapiTtftMs') >= 0 and json_extract(request_observation_json, '$.timings.upstream.engineIapiTtftMs') <= 9223372036854775807)),
  check (json_type(request_observation_json, '$.timings.upstream.engineServiceTtftMs') is null or json_type(request_observation_json, '$.timings.upstream.engineServiceTtftMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.engineServiceTtftMs') in ('integer','real') and json_extract(request_observation_json, '$.timings.upstream.engineServiceTtftMs') >= 0 and json_extract(request_observation_json, '$.timings.upstream.engineServiceTtftMs') <= 9223372036854775807)),
  check (json_type(request_observation_json, '$.timings.upstream.engineIapiTbtMs') is null or json_type(request_observation_json, '$.timings.upstream.engineIapiTbtMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.engineIapiTbtMs') in ('integer','real') and json_extract(request_observation_json, '$.timings.upstream.engineIapiTbtMs') >= 0 and json_extract(request_observation_json, '$.timings.upstream.engineIapiTbtMs') <= 9223372036854775807)),
  check (json_type(request_observation_json, '$.timings.upstream.engineServiceTbtMs') is null or json_type(request_observation_json, '$.timings.upstream.engineServiceTbtMs') = 'null' or (json_type(request_observation_json, '$.timings.upstream.engineServiceTbtMs') in ('integer','real') and json_extract(request_observation_json, '$.timings.upstream.engineServiceTbtMs') >= 0 and json_extract(request_observation_json, '$.timings.upstream.engineServiceTbtMs') <= 9223372036854775807))
);
insert into model_requests_observation_upgrade (
  id, client_api_key_id, client_api_key_ref, operation, client_transport, requested_model_id, provider_kind, upstream_model_id, service_tier, provider_account_id, provider_account_ref, upstream_transport, provider_observation_json, attempt_count, upstream_send_state, downstream_committed_at_us, outcome, client_status_code, upstream_status_code, client_response_id, upstream_request_id, upstream_response_id, error_kind, error_details, input_tokens, output_tokens, cached_tokens, cache_write_tokens, reasoning_tokens, image_input_tokens, image_output_tokens, total_tokens, image_generation_succeeded, cost_source, cost_amount, cost_currency, billing_snapshot_json, request_kind, image_generation_requested, started_at_us, deadline_at_us, completed_at_us, continuation_affinity_hash, continuation_requested, recovery_request_id, recovered_at_us, recovery_attempt_count, diagnostic_trace_json, request_observation_json
) select
  id, client_api_key_id, client_api_key_ref, operation, client_transport, requested_model_id, provider_kind, upstream_model_id, service_tier, provider_account_id, provider_account_ref, upstream_transport, provider_observation_json, attempt_count, upstream_send_state, downstream_committed_at_us, outcome, client_status_code, upstream_status_code, client_response_id, upstream_request_id, upstream_response_id, error_kind, error_details, input_tokens, output_tokens, cached_tokens, cache_write_tokens, reasoning_tokens, image_input_tokens, image_output_tokens, total_tokens, image_generation_succeeded, cost_source, cost_amount, cost_currency, billing_snapshot_json, request_kind, image_generation_requested, started_at_us, deadline_at_us, completed_at_us, continuation_affinity_hash, continuation_requested, recovery_request_id, recovered_at_us, recovery_attempt_count, diagnostic_trace_json,
  json_object('request', json_object('configRevision', config_revision, 'protocol', protocol, 'endpoint', endpoint, 'clientIp', client_ip, 'userAgent', user_agent, 'reasoningEffort', reasoning_effort, 'reasoningPreset', reasoning_preset, 'subagentKind', subagent_kind, 'compact', json(case compact when 1 then 'true' else 'false' end)), 'account', json_object('name', provider_account_name_snapshot, 'email', provider_account_email_snapshot, 'authenticationKind', provider_account_authentication_kind_snapshot), 'routing', json_object('scope', routing_scope, 'groupRefs', json(routing_group_refs_json), 'groupNamesSnapshot', json(routing_group_names_snapshot_json)), 'error', json_object('providerErrorCode', provider_error_code, 'message', error_message, 'retryAfterMs', retry_after_ms), 'timings', json_object('local', json_object('transportDecisionWaitMs', transport_decision_wait_ms, 'connectMs', connect_ms, 'headersMs', headers_ms, 'firstEventMs', first_event_ms, 'firstReasoningMs', first_reasoning_ms, 'firstTextMs', first_text_ms, 'firstTokenMs', first_token_ms, 'latencyMs', latency_ms), 'upstream', json_object('processingMs', provider_processing_ms)), 'scheduling', json_object('admissionDecisionMs', admission_decision_ms, 'accountSelectionWaitMs', account_selection_wait_ms, 'capacityUsedSlots', capacity_used_slots, 'capacityTotalSlots', capacity_total_slots), 'transport', json_object('httpVersion', http_version, 'websocketPool', websocket_pool, 'connection', json_object('id', upstream_connection_id, 'exitReason', upstream_connection_exit_reason, 'ageMs', upstream_connection_age_ms, 'idleMs', upstream_connection_idle_ms)), 'response', json_object('responseModel', upstream_response_model), 'continuation', json_object('previousResponseIdHash', continuation_previous_response_id_hash, 'unavailableReason', continuation_unavailable_reason), 'recovery', json_object('retryDelayMs', recovery_retry_delay_ms, 'totalLatencyMs', recovery_total_latency_ms))
from model_requests;
drop table model_requests;
alter table model_requests_observation_upgrade rename to model_requests;
CREATE TABLE ops_events (
  id text primary key,
  model_request_id text,
  attempt_index integer,
  level text not null check (level in ('warning', 'error')),
  component text not null,
  operation text not null,
  provider_kind text,
  provider_account_id text,
  provider_account_ref text,
  provider_account_name_snapshot text,
  provider_account_email_snapshot text,
  provider_account_authentication_kind_snapshot text,
  upstream_model_id text,
  failure_kind text not null,
  status_code integer check (status_code is null or status_code between 100 and 599),
  provider_error_code text,
  retry_after_ms integer check (retry_after_ms is null or retry_after_ms >= 0),
  upstream_request_id text,
  latency_ms integer check (latency_ms is null or latency_ms >= 0),
  message text not null,
  created_at_us integer not null,
  upstream_send_state text check (
    upstream_send_state is null or upstream_send_state in ('not_sent', 'sent', 'ambiguous')
  ),
  error_details text,
  check ((model_request_id is null and attempt_index is null) or
    (model_request_id is not null and attempt_index is not null and attempt_index > 0)),
  check (provider_account_id is null or provider_account_id = provider_account_ref),
  check (provider_account_id is null or provider_account_ref is not null),
  foreign key (model_request_id) references model_requests(id) on update restrict on delete cascade,
  foreign key (provider_account_id) references provider_accounts(id) on update restrict on delete set null
);
insert into ops_events select * from request_observation_events_backup;
drop table request_observation_events_backup;
CREATE INDEX model_requests_account_ref_idx on model_requests (provider_account_ref, started_at_us desc, id desc)
  where provider_account_ref is not null;
CREATE INDEX model_requests_client_ref_idx on model_requests (client_api_key_ref, started_at_us desc, id desc);
CREATE INDEX model_requests_continuation_recovery_idx
  on model_requests (client_api_key_ref, continuation_affinity_hash, completed_at_us desc, id desc)
  where error_kind = 'continuation_recovery_required' and recovered_at_us is null
    and continuation_affinity_hash is not null;
CREATE INDEX model_requests_outcome_idx on model_requests (outcome, started_at_us desc, id desc);
CREATE INDEX model_requests_provider_idx on model_requests (provider_kind, started_at_us desc, id desc)
  where provider_kind is not null;
CREATE INDEX model_requests_recovery_request_idx on model_requests (recovery_request_id)
  where recovery_request_id is not null;
CREATE INDEX model_requests_requested_model_idx on model_requests (requested_model_id, started_at_us desc, id desc);
CREATE INDEX model_requests_retention_idx on model_requests (completed_at_us)
  where outcome <> 'running';
CREATE INDEX model_requests_running_deadline_idx on model_requests (deadline_at_us, id)
  where outcome = 'running';
CREATE INDEX model_requests_started_idx on model_requests (started_at_us desc, id desc);
CREATE INDEX model_requests_upstream_model_idx on model_requests (upstream_model_id, started_at_us desc, id desc)
  where upstream_model_id is not null;
CREATE INDEX ops_events_account_ref_idx on ops_events (provider_account_ref, created_at_us desc, id desc)
  where provider_account_ref is not null;
CREATE INDEX ops_events_created_idx on ops_events (created_at_us desc, id desc);
CREATE INDEX ops_events_request_idx on ops_events (model_request_id, attempt_index, id);
CREATE INDEX ops_events_retention_idx on ops_events (created_at_us)
  where model_request_id is null;

-- 查询使用只读投影，不保留第二份可写观测事实
create view model_request_observations as
select mr.*,
       json_extract(mr.request_observation_json, '$.request.configRevision') as config_revision,
       json_extract(mr.request_observation_json, '$.request.protocol') as protocol,
       json_extract(mr.request_observation_json, '$.request.endpoint') as endpoint,
       json_extract(mr.request_observation_json, '$.request.clientIp') as client_ip,
       json_extract(mr.request_observation_json, '$.request.userAgent') as user_agent,
       json_extract(mr.request_observation_json, '$.request.reasoningEffort') as reasoning_effort,
       json_extract(mr.request_observation_json, '$.request.reasoningPreset') as reasoning_preset,
       json_extract(mr.request_observation_json, '$.request.subagentKind') as subagent_kind,
       json_extract(mr.request_observation_json, '$.request.compact') as compact,
       json_extract(mr.request_observation_json, '$.account.name') as provider_account_name_snapshot,
       json_extract(mr.request_observation_json, '$.account.email') as provider_account_email_snapshot,
       json_extract(mr.request_observation_json, '$.account.authenticationKind') as provider_account_authentication_kind_snapshot,
       json_extract(mr.request_observation_json, '$.routing.scope') as routing_scope,
       json_extract(mr.request_observation_json, '$.routing.groupRefs') as routing_group_refs_json,
       json_extract(mr.request_observation_json, '$.routing.groupNamesSnapshot') as routing_group_names_snapshot_json,
       json_extract(mr.request_observation_json, '$.error.providerErrorCode') as provider_error_code,
       json_extract(mr.request_observation_json, '$.error.message') as error_message,
       json_extract(mr.request_observation_json, '$.error.retryAfterMs') as retry_after_ms,
       json_extract(mr.request_observation_json, '$.timings.local.transportDecisionWaitMs') as transport_decision_wait_ms,
       json_extract(mr.request_observation_json, '$.timings.local.connectMs') as connect_ms,
       json_extract(mr.request_observation_json, '$.timings.local.headersMs') as headers_ms,
       json_extract(mr.request_observation_json, '$.timings.local.firstEventMs') as first_event_ms,
       json_extract(mr.request_observation_json, '$.timings.local.firstReasoningMs') as first_reasoning_ms,
       json_extract(mr.request_observation_json, '$.timings.local.firstTextMs') as first_text_ms,
       json_extract(mr.request_observation_json, '$.timings.local.firstTokenMs') as first_token_ms,
       json_extract(mr.request_observation_json, '$.timings.local.latencyMs') as latency_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.processingMs') as provider_processing_ms,
       json_extract(mr.request_observation_json, '$.scheduling.admissionDecisionMs') as admission_decision_ms,
       json_extract(mr.request_observation_json, '$.scheduling.accountSelectionWaitMs') as account_selection_wait_ms,
       json_extract(mr.request_observation_json, '$.scheduling.capacityUsedSlots') as capacity_used_slots,
       json_extract(mr.request_observation_json, '$.scheduling.capacityTotalSlots') as capacity_total_slots,
       json_extract(mr.request_observation_json, '$.transport.httpVersion') as http_version,
       json_extract(mr.request_observation_json, '$.transport.websocketPool') as websocket_pool,
       json_extract(mr.request_observation_json, '$.transport.connection.id') as upstream_connection_id,
       json_extract(mr.request_observation_json, '$.transport.connection.exitReason') as upstream_connection_exit_reason,
       json_extract(mr.request_observation_json, '$.transport.connection.ageMs') as upstream_connection_age_ms,
       json_extract(mr.request_observation_json, '$.transport.connection.idleMs') as upstream_connection_idle_ms,
       json_extract(mr.request_observation_json, '$.response.responseModel') as upstream_response_model,
       json_extract(mr.request_observation_json, '$.continuation.previousResponseIdHash') as continuation_previous_response_id_hash,
       json_extract(mr.request_observation_json, '$.continuation.unavailableReason') as continuation_unavailable_reason,
       json_extract(mr.request_observation_json, '$.recovery.retryDelayMs') as recovery_retry_delay_ms,
       json_extract(mr.request_observation_json, '$.recovery.totalLatencyMs') as recovery_total_latency_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.responseMs') as upstream_response_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.apiOverheadMs') as upstream_api_overhead_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.engineMs') as upstream_engine_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.engineIapiTtftMs') as upstream_engine_iapi_ttft_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.engineServiceTtftMs') as upstream_engine_service_ttft_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.engineIapiTbtMs') as upstream_engine_iapi_tbt_ms,
       json_extract(mr.request_observation_json, '$.timings.upstream.engineServiceTbtMs') as upstream_engine_service_tbt_ms
from model_requests mr;
