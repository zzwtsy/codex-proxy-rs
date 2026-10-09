-- 请求观测以类型化 JSON 保存，业务状态保留列，查询通过只读投影取值
create function model_request_observation_valid(document jsonb) returns boolean
language plpgsql immutable parallel safe as $$
declare
  spec record;
  item jsonb;
  number numeric;
begin
  if document is null or jsonb_typeof(document) <> 'object'
     or octet_length(document::text) > 1048576 then
    return false;
  end if;
  for spec in select * from (values
    ('{}'::text[], array['account','continuation','error','recovery','request','response','routing','scheduling','timings','transport']::text[]),
    ('{request}'::text[], array['clientIp','compact','configRevision','endpoint','protocol','reasoningEffort','reasoningPreset','subagentKind','userAgent']::text[]),
    ('{account}'::text[], array['authenticationKind','email','name']::text[]),
    ('{routing}'::text[], array['groupNamesSnapshot','groupRefs','scope']::text[]),
    ('{error}'::text[], array['message','providerErrorCode','retryAfterMs']::text[]),
    ('{timings}'::text[], array['local','upstream']::text[]),
    ('{timings,local}'::text[], array['connectMs','firstEventMs','firstReasoningMs','firstTextMs','firstTokenMs','headersMs','latencyMs','transportDecisionWaitMs']::text[]),
    ('{timings,upstream}'::text[], array['processingMs','responseMs','apiOverheadMs','engineMs','engineIapiTtftMs','engineServiceTtftMs','engineIapiTbtMs','engineServiceTbtMs']::text[]),
    ('{scheduling}'::text[], array['accountSelectionWaitMs','admissionDecisionMs','capacityTotalSlots','capacityUsedSlots']::text[]),
    ('{transport}'::text[], array['connection','httpVersion','websocketPool']::text[]),
    ('{transport,connection}'::text[], array['ageMs','exitReason','id','idleMs']::text[]),
    ('{response}'::text[], array['responseModel']::text[]),
    ('{continuation}'::text[], array['previousResponseIdHash','unavailableReason']::text[]),
    ('{recovery}'::text[], array['retryDelayMs','totalLatencyMs']::text[])) as groups(path, allowed_keys) loop
    item := document #> spec.path;
    if item is not null and (jsonb_typeof(item) <> 'object' or item - spec.allowed_keys <> '{}'::jsonb) then
      return false;
    end if;
  end loop;
  for spec in select * from (values
    ('{request,configRevision}'::text[], 'number'),
    ('{request,protocol}'::text[], 'string'),
    ('{request,endpoint}'::text[], 'string'),
    ('{account,name}'::text[], 'string'),
    ('{account,email}'::text[], 'string'),
    ('{account,authenticationKind}'::text[], 'string'),
    ('{routing,scope}'::text[], 'string'),
    ('{routing,groupRefs}'::text[], 'array'),
    ('{routing,groupNamesSnapshot}'::text[], 'array'),
    ('{error,providerErrorCode}'::text[], 'string'),
    ('{error,message}'::text[], 'string'),
    ('{error,retryAfterMs}'::text[], 'number'),
    ('{request,clientIp}'::text[], 'string'),
    ('{request,userAgent}'::text[], 'string'),
    ('{request,reasoningEffort}'::text[], 'string'),
    ('{request,reasoningPreset}'::text[], 'string'),
    ('{request,subagentKind}'::text[], 'string'),
    ('{request,compact}'::text[], 'boolean'),
    ('{timings,local,transportDecisionWaitMs}'::text[], 'number'),
    ('{timings,local,connectMs}'::text[], 'number'),
    ('{timings,local,headersMs}'::text[], 'number'),
    ('{timings,local,firstEventMs}'::text[], 'number'),
    ('{timings,local,firstReasoningMs}'::text[], 'number'),
    ('{timings,local,firstTextMs}'::text[], 'number'),
    ('{timings,local,firstTokenMs}'::text[], 'number'),
    ('{timings,local,latencyMs}'::text[], 'number'),
    ('{timings,upstream,processingMs}'::text[], 'number'),
    ('{timings,upstream,apiOverheadMs}'::text[], 'milliseconds'),
    ('{timings,upstream,engineMs}'::text[], 'milliseconds'),
    ('{timings,upstream,engineIapiTtftMs}'::text[], 'milliseconds'),
    ('{timings,upstream,engineServiceTtftMs}'::text[], 'milliseconds'),
    ('{timings,upstream,engineIapiTbtMs}'::text[], 'milliseconds'),
    ('{timings,upstream,engineServiceTbtMs}'::text[], 'milliseconds'),
    ('{scheduling,admissionDecisionMs}'::text[], 'number'),
    ('{scheduling,accountSelectionWaitMs}'::text[], 'number'),
    ('{scheduling,capacityUsedSlots}'::text[], 'number'),
    ('{scheduling,capacityTotalSlots}'::text[], 'number'),
    ('{transport,httpVersion}'::text[], 'string'),
    ('{transport,websocketPool}'::text[], 'string'),
    ('{transport,connection,id}'::text[], 'string'),
    ('{transport,connection,exitReason}'::text[], 'string'),
    ('{transport,connection,ageMs}'::text[], 'number'),
    ('{transport,connection,idleMs}'::text[], 'number'),
    ('{response,responseModel}'::text[], 'string'),
    ('{continuation,previousResponseIdHash}'::text[], 'string'),
    ('{continuation,unavailableReason}'::text[], 'string'),
    ('{recovery,retryDelayMs}'::text[], 'number'),
    ('{recovery,totalLatencyMs}'::text[], 'number'),
    ('{timings,upstream,responseMs}'::text[], 'number')) as fields(path, kind) loop
    item := document #> spec.path;
    if item is null or item = 'null'::jsonb then continue; end if;
    if spec.kind = 'milliseconds' then
      if jsonb_typeof(item) <> 'number' then return false; end if;
      number := (item #>> '{}')::numeric;
      if number < 0 or number > 9223372036854775807 then return false; end if;
      continue;
    end if;
    if jsonb_typeof(item) <> spec.kind then return false; end if;
    if spec.kind = 'number' then
      number := (item #>> '{}')::numeric;
      if item #>> '{}' !~ '^[0-9]+$' or number > 9223372036854775807 then return false; end if;
    elsif spec.kind = 'array' then
      if exists (select 1 from jsonb_array_elements(item) member where jsonb_typeof(member) <> 'string') then return false; end if;
    end if;
  end loop;
  if (document #> '{request,configRevision}' is not null
          and document #> '{request,configRevision}' <> 'null'::jsonb
          and document #> '{request,compact}' in ('true'::jsonb, 'false'::jsonb)
          and coalesce(length(document #>> '{request,protocol}'), 0) > 0
          and coalesce(length(document #>> '{request,endpoint}'), 0) > 0
          and document #>> '{routing,scope}' in ('legacy_provider', 'all', 'groups')
          and jsonb_typeof(document #> '{routing,groupRefs}') = 'array'
          and jsonb_typeof(document #> '{routing,groupNamesSnapshot}') = 'array') is not true then
    return false;
  end if;
  if document #>> '{request,clientIp}' is not null then
    perform (document #>> '{request,clientIp}')::inet;
  end if;
  return true;
exception when invalid_text_representation or numeric_value_out_of_range then
  return false;
end;
$$;

alter table model_requests add column request_observation_json jsonb;

update model_requests set request_observation_json = jsonb_strip_nulls(jsonb_build_object(
  'request', jsonb_build_object(
    'configRevision', config_revision,
    'protocol', protocol,
    'endpoint', endpoint,
    'clientIp', client_ip::text,
    'userAgent', user_agent,
    'reasoningEffort', reasoning_effort,
    'reasoningPreset', reasoning_preset,
    'subagentKind', subagent_kind,
    'compact', compact),
  'account', jsonb_build_object(
    'name', provider_account_name_snapshot,
    'email', provider_account_email_snapshot,
    'authenticationKind', provider_account_authentication_kind_snapshot),
  'routing', jsonb_build_object(
    'scope', routing_scope,
    'groupRefs', routing_group_refs,
    'groupNamesSnapshot', routing_group_names_snapshot),
  'error', jsonb_build_object(
    'providerErrorCode', provider_error_code,
    'message', error_message,
    'retryAfterMs', retry_after_ms),
  'timings', jsonb_build_object(
    'local', jsonb_build_object(
      'transportDecisionWaitMs', transport_decision_wait_ms,
      'connectMs', connect_ms,
      'headersMs', headers_ms,
      'firstEventMs', first_event_ms,
      'firstReasoningMs', first_reasoning_ms,
      'firstTextMs', first_text_ms,
      'firstTokenMs', first_token_ms,
      'latencyMs', latency_ms),
    'upstream', jsonb_build_object(
      'processingMs', provider_processing_ms)),
  'scheduling', jsonb_build_object(
    'admissionDecisionMs', admission_decision_ms,
    'accountSelectionWaitMs', account_selection_wait_ms,
    'capacityUsedSlots', capacity_used_slots,
    'capacityTotalSlots', capacity_total_slots),
  'transport', jsonb_build_object(
    'httpVersion', http_version,
    'websocketPool', websocket_pool,
    'connection', jsonb_build_object(
      'id', upstream_connection_id,
      'exitReason', upstream_connection_exit_reason,
      'ageMs', upstream_connection_age_ms,
      'idleMs', upstream_connection_idle_ms)),
  'response', jsonb_build_object(
    'responseModel', upstream_response_model),
  'continuation', jsonb_build_object(
    'previousResponseIdHash', continuation_previous_response_id_hash,
    'unavailableReason', continuation_unavailable_reason),
  'recovery', jsonb_build_object(
    'retryDelayMs', recovery_retry_delay_ms,
    'totalLatencyMs', recovery_total_latency_ms)));

alter table model_requests
    alter column request_observation_json set not null,
    drop constraint model_requests_revision_attempt_ck,
    drop constraint model_requests_status_ck,
    drop constraint model_requests_websocket_pool_ck,
    drop constraint model_requests_routing_scope_ck,
    drop constraint model_requests_routing_group_names_ck,
    drop constraint model_requests_latency_ck,
    drop constraint model_requests_runtime_pressure_ck,
    drop constraint model_requests_continuation_hashes_ck,
    drop constraint model_requests_continuation_reason_ck,
    drop constraint model_requests_upstream_connection_ck,
    drop constraint model_requests_recovery_ck,
    drop constraint model_requests_fact_completeness_ck,
    drop column config_revision,
    drop column protocol,
    drop column endpoint,
    drop column provider_account_name_snapshot,
    drop column provider_account_email_snapshot,
    drop column provider_account_authentication_kind_snapshot,
    drop column routing_scope,
    drop column routing_group_refs,
    drop column routing_group_names_snapshot,
    drop column provider_error_code,
    drop column error_message,
    drop column retry_after_ms,
    drop column client_ip,
    drop column user_agent,
    drop column reasoning_effort,
    drop column reasoning_preset,
    drop column subagent_kind,
    drop column compact,
    drop column transport_decision_wait_ms,
    drop column connect_ms,
    drop column headers_ms,
    drop column first_event_ms,
    drop column first_reasoning_ms,
    drop column first_text_ms,
    drop column first_token_ms,
    drop column latency_ms,
    drop column provider_processing_ms,
    drop column admission_decision_ms,
    drop column account_selection_wait_ms,
    drop column capacity_used_slots,
    drop column capacity_total_slots,
    drop column http_version,
    drop column websocket_pool,
    drop column upstream_connection_id,
    drop column upstream_connection_exit_reason,
    drop column upstream_connection_age_ms,
    drop column upstream_connection_idle_ms,
    drop column upstream_response_model,
    drop column continuation_previous_response_id_hash,
    drop column continuation_unavailable_reason,
    drop column recovery_retry_delay_ms,
    drop column recovery_total_latency_ms;

alter table model_requests
    add constraint model_requests_observation_shape_ck check (model_request_observation_valid(request_observation_json) is true),
    add constraint model_requests_upstream_duration_ck check ((request_observation_json #>> '{timings,upstream,responseMs}')::bigint > 0),
    add constraint model_requests_revision_attempt_ck check (
    (request_observation_json #>> '{request,configRevision}')::bigint > 0 and attempt_count >= 0
    ),
    add constraint model_requests_status_ck check (
    (client_status_code is null or client_status_code between 100 and 599)
    and (upstream_status_code is null or upstream_status_code between 100 and 599)
    and ((request_observation_json #>> '{error,retryAfterMs}')::bigint is null or (request_observation_json #>> '{error,retryAfterMs}')::bigint >= 0)
    ),
    add constraint model_requests_websocket_pool_ck check (
    (request_observation_json #>> '{transport,websocketPool}') is null or (request_observation_json #>> '{transport,websocketPool}') in ('new', 'reuse')
    ),
    add constraint model_requests_routing_scope_ck check (
    (request_observation_json #>> '{routing,scope}') in ('legacy_provider', 'all', 'groups')
    ),
    add constraint model_requests_routing_group_names_ck check (
    jsonb_typeof((request_observation_json #> '{routing,groupNamesSnapshot}')) = 'array'
    and (
      (
        (request_observation_json #>> '{routing,scope}') in ('legacy_provider', 'all')
        and jsonb_array_length(request_observation_json #> '{routing,groupRefs}') = 0
        and jsonb_array_length((request_observation_json #> '{routing,groupNamesSnapshot}')) = 0
      )
      or (
        (request_observation_json #>> '{routing,scope}') = 'groups'
        and jsonb_array_length(request_observation_json #> '{routing,groupRefs}') > 0
        and not jsonb_path_exists(request_observation_json, '$.routing.groupRefs[*] ? (@ == null)')
        and jsonb_array_length((request_observation_json #> '{routing,groupNamesSnapshot}'))
          = jsonb_array_length(request_observation_json #> '{routing,groupRefs}')
      )
    )
    ),
    add constraint model_requests_latency_ck check (
    ((request_observation_json #>> '{timings,local,transportDecisionWaitMs}')::bigint is null or (request_observation_json #>> '{timings,local,transportDecisionWaitMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,connectMs}')::bigint is null or (request_observation_json #>> '{timings,local,connectMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,headersMs}')::bigint is null or (request_observation_json #>> '{timings,local,headersMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,firstEventMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstEventMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,firstReasoningMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstReasoningMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,firstTextMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstTextMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,firstTokenMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstTokenMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,upstream,processingMs}')::bigint is null or (request_observation_json #>> '{timings,upstream,processingMs}')::bigint >= 0)
    and ((request_observation_json #>> '{timings,local,latencyMs}')::bigint is null or (request_observation_json #>> '{timings,local,latencyMs}')::bigint >= 0)
    and (
      (request_observation_json #>> '{timings,local,latencyMs}')::bigint is null
      or (
        ((request_observation_json #>> '{timings,local,transportDecisionWaitMs}')::bigint is null or (request_observation_json #>> '{timings,local,transportDecisionWaitMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
        and ((request_observation_json #>> '{timings,local,connectMs}')::bigint is null or (request_observation_json #>> '{timings,local,connectMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
        and ((request_observation_json #>> '{timings,local,headersMs}')::bigint is null or (request_observation_json #>> '{timings,local,headersMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
        and ((request_observation_json #>> '{timings,local,firstEventMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstEventMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
        and ((request_observation_json #>> '{timings,local,firstReasoningMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstReasoningMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
        and ((request_observation_json #>> '{timings,local,firstTextMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstTextMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
        and ((request_observation_json #>> '{timings,local,firstTokenMs}')::bigint is null or (request_observation_json #>> '{timings,local,firstTokenMs}')::bigint <= (request_observation_json #>> '{timings,local,latencyMs}')::bigint)
      )
    )
    ),
    add constraint model_requests_runtime_pressure_ck check (
    ((request_observation_json #>> '{scheduling,admissionDecisionMs}')::bigint is null or (request_observation_json #>> '{scheduling,admissionDecisionMs}')::bigint >= 0)
    and ((request_observation_json #>> '{scheduling,accountSelectionWaitMs}')::bigint is null or (request_observation_json #>> '{scheduling,accountSelectionWaitMs}')::bigint >= 0)
    and (
      ((request_observation_json #>> '{scheduling,capacityUsedSlots}')::bigint is null and (request_observation_json #>> '{scheduling,capacityTotalSlots}')::bigint is null)
      or (
        (request_observation_json #>> '{scheduling,capacityUsedSlots}')::bigint >= 0
        and (request_observation_json #>> '{scheduling,capacityTotalSlots}')::bigint > 0
        and (request_observation_json #>> '{scheduling,capacityUsedSlots}')::bigint <= (request_observation_json #>> '{scheduling,capacityTotalSlots}')::bigint
      )
    )
    ),
    add constraint model_requests_continuation_hashes_ck check (
    (continuation_affinity_hash is null
      or continuation_affinity_hash ~ '^[0-9a-f]{64}$')
    and ((request_observation_json #>> '{continuation,previousResponseIdHash}') is null
      or (request_observation_json #>> '{continuation,previousResponseIdHash}') ~ '^[0-9a-f]{64}$')
    and (continuation_requested
      or (request_observation_json #>> '{continuation,previousResponseIdHash}') is null)
    ),
    add constraint model_requests_continuation_reason_ck check (
    (request_observation_json #>> '{continuation,unavailableReason}') is null
    or (
      octet_length((request_observation_json #>> '{continuation,unavailableReason}')) between 1 and 64
      and (request_observation_json #>> '{continuation,unavailableReason}') ~ '^[a-z][a-z0-9_]*$'
    )
    ),
    add constraint model_requests_upstream_connection_ck check (
    (
      (request_observation_json #>> '{transport,connection,id}') is null
      and (request_observation_json #>> '{transport,connection,exitReason}') is null
      and (request_observation_json #>> '{transport,connection,ageMs}')::bigint is null
      and (request_observation_json #>> '{transport,connection,idleMs}')::bigint is null
    )
    or (
      octet_length((request_observation_json #>> '{transport,connection,id}')) between 1 and 128
      and (request_observation_json #>> '{transport,connection,id}') !~ '[[:cntrl:]]'
      and octet_length((request_observation_json #>> '{transport,connection,exitReason}')) between 1 and 64
      and (request_observation_json #>> '{transport,connection,exitReason}') ~ '^[a-z][a-z0-9_]*$'
      and (request_observation_json #>> '{transport,connection,ageMs}')::bigint >= 0
      and (request_observation_json #>> '{transport,connection,idleMs}')::bigint between 0 and (request_observation_json #>> '{transport,connection,ageMs}')::bigint
    )
    ),
    add constraint model_requests_recovery_ck check (
    recovery_attempt_count >= 0
    and (
      (
        recovered_at is null
        and recovery_request_id is null
        and (request_observation_json #>> '{recovery,retryDelayMs}')::bigint is null
        and (request_observation_json #>> '{recovery,totalLatencyMs}')::bigint is null
      )
      or (
        recovered_at is not null
        and recovered_at >= completed_at
        and recovery_request_id is not null
        and octet_length(recovery_request_id) between 1 and 128
        and recovery_attempt_count > 0
        and (request_observation_json #>> '{recovery,retryDelayMs}')::bigint >= 0
        and (request_observation_json #>> '{recovery,totalLatencyMs}')::bigint >= (request_observation_json #>> '{recovery,retryDelayMs}')::bigint
      )
    )
    ),
    add constraint model_requests_fact_completeness_ck check (
    (provider_account_id is null or provider_account_ref is not null)
    and (cost_source = 'unavailable' or cost_currency is not null)
    and num_nonnulls((request_observation_json #>> '{scheduling,capacityUsedSlots}')::bigint, (request_observation_json #>> '{scheduling,capacityTotalSlots}')::bigint) in (0, 2)
    and num_nonnulls(
      (request_observation_json #>> '{transport,connection,id}'), (request_observation_json #>> '{transport,connection,exitReason}'),
      (request_observation_json #>> '{transport,connection,ageMs}')::bigint, (request_observation_json #>> '{transport,connection,idleMs}')::bigint
    ) in (0, 4)
    and (
      recovered_at is null
      or (
        completed_at is not null
        and (request_observation_json #>> '{recovery,retryDelayMs}')::bigint is not null
        and (request_observation_json #>> '{recovery,totalLatencyMs}')::bigint is not null
      )
    )
    );

-- 只读视图集中 JSON 路径与类型转换，不保存冗余事实
-- 计时与调度投影逐行计算一次，避免聚合的多个分位数重复提取同一个 JSON 值
-- 请求与账号筛选保留在主扫描上，分页后才计算所需观测
create view model_request_observations as
select mr.*,
       (mr.request_observation_json #>> '{request,configRevision}')::bigint as config_revision,
       (mr.request_observation_json #>> '{request,protocol}') as protocol,
       (mr.request_observation_json #>> '{request,endpoint}') as endpoint,
       (mr.request_observation_json #>> '{account,name}') as provider_account_name_snapshot,
       (mr.request_observation_json #>> '{account,email}') as provider_account_email_snapshot,
       (mr.request_observation_json #>> '{account,authenticationKind}') as provider_account_authentication_kind_snapshot,
       (mr.request_observation_json #>> '{routing,scope}') as routing_scope,
       array(select jsonb_array_elements_text(mr.request_observation_json #> '{routing,groupRefs}')) as routing_group_refs,
       (mr.request_observation_json #> '{routing,groupNamesSnapshot}') as routing_group_names_snapshot,
       (mr.request_observation_json #>> '{error,providerErrorCode}') as provider_error_code,
       (mr.request_observation_json #>> '{error,message}') as error_message,
       (mr.request_observation_json #>> '{error,retryAfterMs}')::bigint as retry_after_ms,
       (mr.request_observation_json #>> '{request,clientIp}')::inet as client_ip,
       (mr.request_observation_json #>> '{request,userAgent}') as user_agent,
       (mr.request_observation_json #>> '{request,reasoningEffort}') as reasoning_effort,
       (mr.request_observation_json #>> '{request,reasoningPreset}') as reasoning_preset,
       (mr.request_observation_json #>> '{request,subagentKind}') as subagent_kind,
       (mr.request_observation_json #>> '{request,compact}')::boolean as compact,
       measured.transport_decision_wait_ms as transport_decision_wait_ms,
       measured.connect_ms as connect_ms,
       measured.headers_ms as headers_ms,
       measured.first_event_ms as first_event_ms,
       measured.first_reasoning_ms as first_reasoning_ms,
       measured.first_text_ms as first_text_ms,
       measured.first_token_ms as first_token_ms,
       measured.latency_ms as latency_ms,
       measured.provider_processing_ms as provider_processing_ms,
       measured.admission_decision_ms as admission_decision_ms,
       measured.account_selection_wait_ms as account_selection_wait_ms,
       measured.capacity_used_slots as capacity_used_slots,
       measured.capacity_total_slots as capacity_total_slots,
       (mr.request_observation_json #>> '{transport,httpVersion}') as http_version,
       (mr.request_observation_json #>> '{transport,websocketPool}') as websocket_pool,
       (mr.request_observation_json #>> '{transport,connection,id}') as upstream_connection_id,
       (mr.request_observation_json #>> '{transport,connection,exitReason}') as upstream_connection_exit_reason,
       (mr.request_observation_json #>> '{transport,connection,ageMs}')::bigint as upstream_connection_age_ms,
       (mr.request_observation_json #>> '{transport,connection,idleMs}')::bigint as upstream_connection_idle_ms,
       (mr.request_observation_json #>> '{response,responseModel}') as upstream_response_model,
       (mr.request_observation_json #>> '{continuation,previousResponseIdHash}') as continuation_previous_response_id_hash,
       (mr.request_observation_json #>> '{continuation,unavailableReason}') as continuation_unavailable_reason,
       (mr.request_observation_json #>> '{recovery,retryDelayMs}')::bigint as recovery_retry_delay_ms,
       (mr.request_observation_json #>> '{recovery,totalLatencyMs}')::bigint as recovery_total_latency_ms,
       (mr.request_observation_json #>> '{timings,upstream,apiOverheadMs}')::double precision as upstream_api_overhead_ms,
       (mr.request_observation_json #>> '{timings,upstream,engineMs}')::double precision as upstream_engine_ms,
       (mr.request_observation_json #>> '{timings,upstream,engineIapiTtftMs}')::double precision as upstream_engine_iapi_ttft_ms,
       (mr.request_observation_json #>> '{timings,upstream,engineServiceTtftMs}')::double precision as upstream_engine_service_ttft_ms,
       (mr.request_observation_json #>> '{timings,upstream,engineIapiTbtMs}')::double precision as upstream_engine_iapi_tbt_ms,
       (mr.request_observation_json #>> '{timings,upstream,engineServiceTbtMs}')::double precision as upstream_engine_service_tbt_ms,
       measured.upstream_response_ms as upstream_response_ms
from model_requests mr
cross join lateral (
  select (mr.request_observation_json #>> '{timings,local,transportDecisionWaitMs}')::bigint as transport_decision_wait_ms,
         (mr.request_observation_json #>> '{timings,local,connectMs}')::bigint as connect_ms,
         (mr.request_observation_json #>> '{timings,local,headersMs}')::bigint as headers_ms,
         (mr.request_observation_json #>> '{timings,local,firstEventMs}')::bigint as first_event_ms,
         (mr.request_observation_json #>> '{timings,local,firstReasoningMs}')::bigint as first_reasoning_ms,
         (mr.request_observation_json #>> '{timings,local,firstTextMs}')::bigint as first_text_ms,
         (mr.request_observation_json #>> '{timings,local,firstTokenMs}')::bigint as first_token_ms,
         (mr.request_observation_json #>> '{timings,local,latencyMs}')::bigint as latency_ms,
         (mr.request_observation_json #>> '{timings,upstream,processingMs}')::bigint as provider_processing_ms,
         (mr.request_observation_json #>> '{scheduling,admissionDecisionMs}')::bigint as admission_decision_ms,
         (mr.request_observation_json #>> '{scheduling,accountSelectionWaitMs}')::bigint as account_selection_wait_ms,
         (mr.request_observation_json #>> '{scheduling,capacityUsedSlots}')::bigint as capacity_used_slots,
         (mr.request_observation_json #>> '{scheduling,capacityTotalSlots}')::bigint as capacity_total_slots,
         (mr.request_observation_json #>> '{timings,upstream,responseMs}')::bigint as upstream_response_ms
  offset 0
) measured;
