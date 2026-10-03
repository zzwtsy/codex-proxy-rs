-- Core 请求生命周期与运行事件。时间使用 UTC epoch 微秒，金额使用缩放整数文本。
create table model_requests (
  id text primary key,
  client_api_key_id text,
  client_api_key_ref text not null,
  config_revision integer not null check (config_revision > 0),
  protocol text not null,
  operation text not null,
  endpoint text not null,
  client_transport text not null,
  requested_model_id text,
  provider_kind text,
  upstream_model_id text,
  upstream_response_model text,
  service_tier text,
  provider_account_id text,
  provider_account_ref text,
  provider_account_name_snapshot text,
  provider_account_email_snapshot text,
  provider_account_authentication_kind_snapshot text,
  upstream_transport text,
  http_version text,
  websocket_pool text,
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
  provider_error_code text,
  error_message text,
  raw_upstream_error text,
  retry_after_ms integer check (retry_after_ms is null or retry_after_ms >= 0),
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
  transport_decision_wait_ms integer check (transport_decision_wait_ms is null or transport_decision_wait_ms >= 0),
  connect_ms integer check (connect_ms is null or connect_ms >= 0),
  headers_ms integer check (headers_ms is null or headers_ms >= 0),
  first_event_ms integer check (first_event_ms is null or first_event_ms >= 0),
  first_reasoning_ms integer check (first_reasoning_ms is null or first_reasoning_ms >= 0),
  first_text_ms integer check (first_text_ms is null or first_text_ms >= 0),
  first_token_ms integer check (first_token_ms is null or first_token_ms >= 0),
  provider_processing_ms integer check (provider_processing_ms is null or provider_processing_ms >= 0),
  latency_ms integer check (latency_ms is null or latency_ms >= 0),
  client_ip text,
  user_agent text,
  reasoning_effort text,
  reasoning_preset text,
  request_kind text,
  subagent_kind text,
  compact integer not null default 0 check (compact in (0, 1)),
  image_generation_requested integer not null default 0 check (image_generation_requested in (0, 1)),
  started_at_us integer not null,
  deadline_at_us integer not null,
  completed_at_us integer,
  routing_scope text not null check (routing_scope in ('legacy_provider', 'all', 'groups')),
  routing_group_refs_json text not null default '[]'
    check (json_valid(routing_group_refs_json) and json_type(routing_group_refs_json) = 'array'),
  routing_group_names_snapshot_json text not null default '[]'
    check (json_valid(routing_group_names_snapshot_json) and json_type(routing_group_names_snapshot_json) = 'array'),
  admission_decision_ms integer check (admission_decision_ms is null or admission_decision_ms >= 0),
  account_selection_wait_ms integer check (account_selection_wait_ms is null or account_selection_wait_ms >= 0),
  capacity_used_slots integer,
  capacity_total_slots integer,
  continuation_affinity_hash text,
  continuation_previous_response_id_hash text,
  continuation_requested integer not null default 0 check (continuation_requested in (0, 1)),
  continuation_unavailable_reason text,
  upstream_connection_id text,
  upstream_connection_exit_reason text,
  upstream_connection_age_ms integer,
  upstream_connection_idle_ms integer,
  recovery_request_id text,
  recovered_at_us integer,
  recovery_attempt_count integer not null default 0 check (recovery_attempt_count >= 0),
  recovery_retry_delay_ms integer,
  recovery_total_latency_ms integer,
  diagnostic_trace_json text check (
    diagnostic_trace_json is null or
    (json_valid(diagnostic_trace_json) and json_type(diagnostic_trace_json) = 'object'
      and length(diagnostic_trace_json) <= 65536)
  ),
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
  check (websocket_pool is null or websocket_pool in ('new', 'reuse')),
  check (service_tier is null or (length(service_tier) between 1 and 64 and service_tier not glob '*[' || char(1) || '-' || char(31) || ']*')),
  check (
    (routing_scope in ('legacy_provider', 'all') and
      json_array_length(routing_group_refs_json) = 0 and
      json_array_length(routing_group_names_snapshot_json) = 0) or
    (routing_scope = 'groups' and json_array_length(routing_group_refs_json) > 0 and
      json_array_length(routing_group_refs_json) = json_array_length(routing_group_names_snapshot_json))
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
    latency_ms is null or
    ((transport_decision_wait_ms is null or transport_decision_wait_ms <= latency_ms) and
     (connect_ms is null or connect_ms <= latency_ms) and
     (headers_ms is null or headers_ms <= latency_ms) and
     (first_event_ms is null or first_event_ms <= latency_ms) and
     (first_reasoning_ms is null or first_reasoning_ms <= latency_ms) and
     (first_text_ms is null or first_text_ms <= latency_ms) and
     (first_token_ms is null or first_token_ms <= latency_ms) and
     (provider_processing_ms is null or provider_processing_ms <= latency_ms))
  ),
  check (
    (capacity_used_slots is null and capacity_total_slots is null) or
    (capacity_used_slots is not null and capacity_total_slots is not null and
      capacity_total_slots > 0 and capacity_used_slots >= 0 and
      capacity_used_slots <= capacity_total_slots)
  ),
  check (
    (continuation_affinity_hash is null or
      (length(continuation_affinity_hash) = 64 and continuation_affinity_hash not glob '*[^0-9a-f]*')) and
    (continuation_previous_response_id_hash is null or
      (length(continuation_previous_response_id_hash) = 64 and continuation_previous_response_id_hash not glob '*[^0-9a-f]*')) and
    (continuation_requested = 1 or continuation_previous_response_id_hash is null)
  ),
  check (continuation_unavailable_reason is null or
    (length(continuation_unavailable_reason) between 1 and 64 and
      continuation_unavailable_reason glob '[a-z]*')),
  check (
    (upstream_connection_id is null and upstream_connection_exit_reason is null and
      upstream_connection_age_ms is null and upstream_connection_idle_ms is null) or
    (upstream_connection_id is not null and length(upstream_connection_id) between 1 and 128 and
      upstream_connection_exit_reason is not null and length(upstream_connection_exit_reason) between 1 and 64 and
      upstream_connection_age_ms >= 0 and upstream_connection_idle_ms between 0 and upstream_connection_age_ms)
  ),
  check (
    (recovered_at_us is null and recovery_request_id is null and
      recovery_retry_delay_ms is null and recovery_total_latency_ms is null) or
    (recovered_at_us is not null and completed_at_us is not null and
      recovered_at_us >= completed_at_us and recovery_request_id is not null and
      recovery_attempt_count > 0 and recovery_retry_delay_ms >= 0 and
      recovery_total_latency_ms >= recovery_retry_delay_ms)
  ),
  foreign key (client_api_key_id) references client_api_keys(id) on update restrict on delete set null,
  foreign key (provider_account_id) references provider_accounts(id) on update restrict on delete set null
);

create index model_requests_started_idx on model_requests (started_at_us desc, id desc);
create index model_requests_client_ref_idx on model_requests (client_api_key_ref, started_at_us desc, id desc);
create index model_requests_account_ref_idx on model_requests (provider_account_ref, started_at_us desc, id desc)
  where provider_account_ref is not null;
create index model_requests_requested_model_idx on model_requests (requested_model_id, started_at_us desc, id desc);
create index model_requests_upstream_model_idx on model_requests (upstream_model_id, started_at_us desc, id desc)
  where upstream_model_id is not null;
create index model_requests_provider_idx on model_requests (provider_kind, started_at_us desc, id desc)
  where provider_kind is not null;
create index model_requests_outcome_idx on model_requests (outcome, started_at_us desc, id desc);
create index model_requests_running_deadline_idx on model_requests (deadline_at_us, id)
  where outcome = 'running';
create index model_requests_retention_idx on model_requests (completed_at_us)
  where outcome <> 'running';
create index model_requests_continuation_recovery_idx
  on model_requests (client_api_key_ref, continuation_affinity_hash, completed_at_us desc, id desc)
  where error_kind = 'continuation_recovery_required' and recovered_at_us is null
    and continuation_affinity_hash is not null;
create index model_requests_recovery_request_idx on model_requests (recovery_request_id)
  where recovery_request_id is not null;

create table ops_events (
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
  raw_upstream_error text,
  check ((model_request_id is null and attempt_index is null) or
    (model_request_id is not null and attempt_index is not null and attempt_index > 0)),
  check (provider_account_id is null or provider_account_id = provider_account_ref),
  check (provider_account_id is null or provider_account_ref is not null),
  foreign key (model_request_id) references model_requests(id) on update restrict on delete cascade,
  foreign key (provider_account_id) references provider_accounts(id) on update restrict on delete set null
);
create index ops_events_request_idx on ops_events (model_request_id, attempt_index, id);
create index ops_events_account_ref_idx on ops_events (provider_account_ref, created_at_us desc, id desc)
  where provider_account_ref is not null;
create index ops_events_created_idx on ops_events (created_at_us desc, id desc);
create index ops_events_retention_idx on ops_events (created_at_us)
  where model_request_id is null;
