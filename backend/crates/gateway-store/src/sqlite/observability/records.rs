//! SQLite 用量列表、请求详情和运维错误查询。

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool, sqlite::SqliteRow};

use crate::{
    DecimalAmount, StoreError, StoreResult,
    postgres::{
        ObservabilityRange, OpsErrorPage, OpsErrorQuery, OpsErrorRecord, UsageAttemptObservation,
        UsageListRecord, UsageRecord, UsageRecordDetail, UsageRecordFilter, UsageRecordPage,
        UsageRecordQuery,
    },
    sqlite::value::{datetime_from_micros, decode_amount},
};

use super::{
    checked_u64, completed_usage_fact_predicate, invalid, push_range, push_usage_filter,
    unavailable,
};

pub(crate) async fn list_usage_records(
    pool: &SqlitePool,
    query: UsageRecordQuery,
) -> StoreResult<UsageRecordPage> {
    query.filter.validate()?;
    let total = count_usage_records(pool, query.range, &query.filter).await?;
    let offset = crate::postgres::observability_page_offset(query.current_page, query.page_size)?;
    let mut statement = QueryBuilder::<Sqlite>::new(USAGE_LIST_SELECT);
    statement.push(" where ");
    push_range(&mut statement, "mr", query.range);
    statement.push(" and ");
    statement.push(completed_usage_fact_predicate("mr"));
    push_usage_filter(&mut statement, &query.filter, "mr");
    statement.push(" order by mr.started_at_us desc, mr.id desc limit ");
    statement.push_bind(i64::from(query.page_size.get()));
    statement.push(" offset ");
    statement.push_bind(offset);
    let rows = statement
        .build()
        .fetch_all(pool)
        .await
        .map_err(|_| unavailable())?;
    let items = rows
        .iter()
        .map(usage_list_record_from_row)
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(UsageRecordPage {
        items,
        current_page: query.current_page,
        page_size: query.page_size.get(),
        total,
    })
}

async fn count_usage_records(
    pool: &SqlitePool,
    range: ObservabilityRange,
    filter: &UsageRecordFilter,
) -> StoreResult<u64> {
    let mut statement =
        QueryBuilder::<Sqlite>::new("select count(*) from model_request_observations mr where ");
    push_range(&mut statement, "mr", range);
    statement.push(" and ");
    statement.push(completed_usage_fact_predicate("mr"));
    push_usage_filter(&mut statement, filter, "mr");
    let total: i64 = statement
        .build_query_scalar()
        .fetch_one(pool)
        .await
        .map_err(|_| unavailable())?;
    checked_u64(total)
}

pub(crate) async fn usage_record_detail(
    pool: &SqlitePool,
    request_id: &str,
) -> StoreResult<UsageRecordDetail> {
    crate::require_nonempty("model request", "id", request_id)?;
    if request_id.len() > 256 || request_id.chars().any(char::is_control) {
        return Err(invalid("request ID is invalid"));
    }
    let row = sqlx::query("select * from model_request_observations where id = ?")
        .bind(request_id)
        .fetch_optional(pool)
        .await
        .map_err(|_| unavailable())?
        .ok_or_else(|| StoreError::NotFound {
            entity: "model request",
            id: request_id.to_owned(),
            source: None,
        })?;
    let request = usage_record_from_row(&row)?;
    let attempt_rows = sqlx::query(
        "select oe.id, oe.attempt_index, oe.component, oe.operation,
                oe.provider_kind, oe.provider_account_ref,
                oe.provider_account_name_snapshot as provider_account_name,
                oe.provider_account_email_snapshot as provider_account_email,
                oe.provider_account_authentication_kind_snapshot as provider_account_authentication_kind,
                oe.upstream_model_id, oe.failure_kind, oe.status_code, oe.provider_error_code,
                oe.retry_after_ms, oe.upstream_request_id, oe.latency_ms, oe.message, oe.created_at_us
           from ops_events oe where oe.model_request_id = ?
          order by oe.attempt_index, oe.created_at_us, oe.id",
    )
    .bind(request_id)
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable())?;
    let mut attempts = attempt_rows
        .iter()
        .map(intermediate_attempt_from_row)
        .collect::<StoreResult<Vec<_>>>()?;
    if request.attempt_count > 0 {
        attempts.push(crate::postgres::final_attempt_from_request(&request));
    }
    let trace: Option<String> = row
        .try_get("diagnostic_trace_json")
        .map_err(|_| unavailable())?;
    let trace = trace
        .map(|value| {
            serde_json::from_str(&value).map_err(|_| invalid("request trace is malformed"))
        })
        .transpose()?;
    let related_rows = sqlx::query(
        "select related.id, related.outcome, related.completed_at_us,
                case when related.id = current.recovery_request_id then 'recovered_by' else 'recovers' end as relation
           from model_request_observations current join model_request_observations related
             on related.id = current.recovery_request_id or related.recovery_request_id = current.id
          where current.id = ? order by related.started_at_us limit 20",
    )
    .bind(request_id)
    .fetch_all(pool)
    .await
    .map_err(|_| unavailable())?;
    let related_requests = related_rows
        .iter()
        .map(|related| {
            let completed_at_us: Option<i64> = related
                .try_get("completed_at_us")
                .map_err(|_| unavailable())?;
            let completed_at = completed_at_us
                .map(datetime_from_micros)
                .transpose()?
                .map(|value| value.to_rfc3339());
            Ok(serde_json::json!({
                "requestId": related.try_get::<String, _>("id").map_err(|_| unavailable())?,
                "outcome": related.try_get::<String, _>("outcome").map_err(|_| unavailable())?,
                "relation": related.try_get::<String, _>("relation").map_err(|_| unavailable())?,
                "completedAt": completed_at,
            }))
        })
        .collect::<StoreResult<Vec<_>>>()?;
    Ok(UsageRecordDetail {
        request,
        attempts,
        trace,
        related_requests,
    })
}

pub(crate) async fn ops_errors(
    pool: &SqlitePool,
    query: OpsErrorQuery,
) -> StoreResult<OpsErrorPage> {
    crate::postgres::validate_ops_error_filter(&query.filter)?;
    let offset = crate::postgres::observability_page_offset(query.current_page, query.page_size)?;
    // 名称匹配、总数和页面共用只读快照，避免并发写入造成分页计数不一致
    let mut transaction = pool.begin().await.map_err(|_| unavailable())?;
    let matching_keys = if let Some(search) = &query.filter.search {
        let names: Vec<(String, String)> = sqlx::query_as("select id, name from client_api_keys")
            .fetch_all(&mut *transaction)
            .await
            .map_err(|_| unavailable())?;
        let search = search.to_lowercase();
        let ids: Vec<String> = names
            .into_iter()
            .filter_map(|(id, name)| name.to_lowercase().starts_with(&search).then_some(id))
            .collect();
        serde_json::to_string(&ids).map_err(|_| invalid("matching key IDs cannot be encoded"))?
    } else {
        String::from("[]")
    };
    let mut count = QueryBuilder::<Sqlite>::new("select sum(source_count) from (");
    count.push("select count(*) as source_count from model_request_observations mr where ");
    push_ops_error_filter(&mut count, &query, true, &matching_keys);
    count.push(" union all select count(*) as source_count from ops_events oe left join model_request_observations mr on mr.id = oe.model_request_id where ");
    push_ops_error_filter(&mut count, &query, false, &matching_keys);
    count.push(")");
    let total: i64 = count
        .build_query_scalar()
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
    let mut page = QueryBuilder::<Sqlite>::new("select * from (");
    page.push(OPS_REQUEST_SELECT).push(" where ");
    push_ops_error_filter(&mut page, &query, true, &matching_keys);
    page.push(" union all ")
        .push(OPS_EVENT_SELECT)
        .push(" where ");
    push_ops_error_filter(&mut page, &query, false, &matching_keys);
    page.push(") errors order by occurred_at_us desc, stable_sort_id desc limit ")
        .push_bind(i64::from(query.page_size.get()))
        .push(" offset ")
        .push_bind(offset);
    let rows = page
        .build()
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| unavailable())?;
    let items = rows
        .iter()
        .map(ops_error_from_row)
        .collect::<StoreResult<Vec<_>>>()?;
    transaction.commit().await.map_err(|_| unavailable())?;
    Ok(OpsErrorPage {
        items,
        current_page: query.current_page,
        page_size: query.page_size.get(),
        total: checked_u64(total)?,
    })
}

fn push_ops_error_filter(
    statement: &mut QueryBuilder<Sqlite>,
    query: &OpsErrorQuery,
    request: bool,
    matching_keys: &str,
) {
    let filter = &query.filter;
    let (alias, time, request_id, event_id, attempt, status) = if request {
        statement.push("mr.error_kind is not null and mr.error_kind <> 'cancelled' and mr.outcome <> 'running' and ");
        (
            "mr",
            "mr.completed_at_us",
            "mr.id",
            "mr.id",
            "nullif(mr.attempt_count, 0)",
            "mr.upstream_status_code",
        )
    } else {
        (
            "oe",
            "oe.created_at_us",
            "oe.model_request_id",
            "oe.id",
            "oe.attempt_index",
            "oe.status_code",
        )
    };
    statement
        .push(time)
        .push(" >= ")
        .push_bind(query.range.start.timestamp_micros())
        .push(" and ")
        .push(time)
        .push(" < ")
        .push_bind(query.range.end.timestamp_micros());
    for (column, expected) in [
        ("mr.client_api_key_ref", &filter.client_api_key_ref),
        (request_id, &filter.request_id),
    ] {
        if let Some(value) = expected {
            statement
                .push(" and ")
                .push(column)
                .push(" = ")
                .push_bind(value);
        }
    }
    for (column, expected) in [
        ("provider_kind", &filter.provider_kind),
        ("provider_account_ref", &filter.provider_account_ref),
        ("operation", &filter.operation),
        ("upstream_model_id", &filter.model),
        ("upstream_request_id", &filter.upstream_request_id),
    ] {
        if let Some(value) = expected {
            statement
                .push(" and ")
                .push(alias)
                .push(".")
                .push(column)
                .push(" = ")
                .push_bind(value);
        }
    }
    if let Some(transport) = &filter.transport {
        if request {
            statement
                .push(" and mr.upstream_transport = ")
                .push_bind(transport);
        } else {
            statement.push(" and 0");
        }
    }
    for (column, value) in [
        (attempt, filter.attempt_index.map(i64::from)),
        (status, filter.status_code.map(i64::from)),
    ] {
        if let Some(value) = value {
            statement
                .push(" and ")
                .push(column)
                .push(" = ")
                .push_bind(value);
        }
    }
    if let Some(value) = &filter.response_id {
        statement
            .push(" and mr.client_response_id = ")
            .push_bind(value.as_bytes());
    }
    if let Some(search) = &filter.search {
        // instr 保持标识符的大小写和字面前缀语义，不将 % 与 _ 解释为通配符
        statement.push(" and (");
        for (index, column) in [
            event_id.to_owned(),
            request_id.to_owned(),
            "mr.client_api_key_ref".to_owned(),
            format!("{alias}.provider_account_ref"),
            format!("{alias}.upstream_request_id"),
            format!("{alias}.provider_error_code"),
        ]
        .into_iter()
        .enumerate()
        {
            if index > 0 {
                statement.push(" or ");
            }
            statement
                .push("instr(")
                .push(column)
                .push(", ")
                .push_bind(search)
                .push(") = 1");
        }
        statement
            .push(" or mr.client_api_key_ref in (select value from json_each(")
            .push_bind(matching_keys)
            .push(")))");
    }
}

fn usage_list_record_from_row(row: &SqliteRow) -> StoreResult<UsageListRecord> {
    let cost_source: String = get(row, "cost_source")?;
    let cost_amount = optional_amount(row, "cost_amount")?;
    let cost_currency: Option<String> = get(row, "cost_currency")?;
    let billing_snapshot = optional_json(row, "billing_snapshot_json")?;
    let billing = crate::postgres::billing_from_values(
        &cost_source,
        cost_amount.as_ref(),
        cost_currency.as_deref(),
        billing_snapshot.as_ref(),
    )?;
    Ok(UsageListRecord {
        client_api_key_name: get(row, "client_api_key_name")?,
        billing,
        id: get(row, "id")?,
        endpoint: get(row, "endpoint")?,
        client_transport: get(row, "client_transport")?,
        requested_model_id: get(row, "requested_model_id")?,
        provider_kind: get(row, "provider_kind")?,
        provider_account_ref: get(row, "provider_account_ref")?,
        provider_account_name: get(row, "provider_account_name_snapshot")?,
        provider_account_email: get(row, "provider_account_email_snapshot")?,
        provider_account_notes: get(row, "provider_account_notes")?,
        provider_account_plan_type: get(row, "provider_account_plan_type")?,
        provider_account_plan_type_display: None,
        provider_account_authentication_kind: get(
            row,
            "provider_account_authentication_kind_snapshot",
        )?,
        upstream_model_id: get(row, "upstream_model_id")?,
        upstream_transport: get(row, "upstream_transport")?,
        upstream_response_model: get(row, "upstream_response_model")?,
        service_tier: get(row, "service_tier")?,
        input_tokens: optional_unsigned(row, "input_tokens")?,
        output_tokens: optional_unsigned(row, "output_tokens")?,
        cached_tokens: optional_unsigned(row, "cached_tokens")?,
        cache_write_tokens: optional_unsigned(row, "cache_write_tokens")?,
        reasoning_tokens: optional_unsigned(row, "reasoning_tokens")?,
        image_input_tokens: optional_unsigned(row, "image_input_tokens")?,
        image_output_tokens: optional_unsigned(row, "image_output_tokens")?,
        total_tokens: optional_unsigned(row, "total_tokens")?,
        cost_source,
        cost_amount,
        cost_currency,
        transport_decision_wait_ms: optional_unsigned(row, "transport_decision_wait_ms")?,
        connect_ms: optional_unsigned(row, "connect_ms")?,
        headers_ms: optional_unsigned(row, "headers_ms")?,
        first_event_ms: optional_unsigned(row, "first_event_ms")?,
        first_reasoning_ms: optional_unsigned(row, "first_reasoning_ms")?,
        first_text_ms: optional_unsigned(row, "first_text_ms")?,
        first_token_ms: optional_unsigned(row, "first_token_ms")?,
        provider_processing_ms: optional_unsigned(row, "provider_processing_ms")?,
        upstream_response_ms: optional_unsigned(row, "upstream_response_ms")?,
        upstream_api_overhead_ms: get(row, "upstream_api_overhead_ms")?,
        upstream_engine_ms: get(row, "upstream_engine_ms")?,
        upstream_engine_iapi_ttft_ms: get(row, "upstream_engine_iapi_ttft_ms")?,
        upstream_engine_service_ttft_ms: get(row, "upstream_engine_service_ttft_ms")?,
        upstream_engine_iapi_tbt_ms: get(row, "upstream_engine_iapi_tbt_ms")?,
        upstream_engine_service_tbt_ms: get(row, "upstream_engine_service_tbt_ms")?,
        latency_ms: optional_unsigned(row, "latency_ms")?,
        admission_decision_ms: optional_unsigned(row, "admission_decision_ms")?,
        account_selection_wait_ms: optional_unsigned(row, "account_selection_wait_ms")?,
        capacity_used_slots: optional_unsigned(row, "capacity_used_slots")?,
        capacity_total_slots: optional_unsigned(row, "capacity_total_slots")?,
        client_ip: get(row, "client_ip")?,
        user_agent: get(row, "user_agent")?,
        reasoning_effort: get(row, "reasoning_effort")?,
        reasoning_preset: get(row, "reasoning_preset")?,
        subagent_kind: get(row, "subagent_kind")?,
        compact: bool_value(row, "compact")?,
        started_at: required_time(row, "started_at_us")?,
    })
}

fn usage_record_from_row(row: &SqliteRow) -> StoreResult<UsageRecord> {
    let cost_source: String = get(row, "cost_source")?;
    let cost_amount = optional_amount(row, "cost_amount")?;
    let cost_currency: Option<String> = get(row, "cost_currency")?;
    let billing_snapshot = optional_json(row, "billing_snapshot_json")?;
    let billing = crate::postgres::billing_from_values(
        &cost_source,
        cost_amount.as_ref(),
        cost_currency.as_deref(),
        billing_snapshot.as_ref(),
    )?;
    let response_id = |column: &'static str| -> StoreResult<Option<String>> {
        let value: Option<Vec<u8>> = get(row, column)?;
        value
            .map(String::from_utf8)
            .transpose()
            .map_err(|_| invalid("response ID is not UTF-8"))
    };
    Ok(UsageRecord {
        billing,
        id: get(row, "id")?,
        client_api_key_ref: get(row, "client_api_key_ref")?,
        config_revision: required_unsigned(row, "config_revision")?,
        routing_scope: get(row, "routing_scope")?,
        routing_group_refs: required_json(row, "routing_group_refs_json")?,
        routing_group_names_snapshot: required_json(row, "routing_group_names_snapshot_json")?,
        protocol: get(row, "protocol")?,
        operation: get(row, "operation")?,
        endpoint: get(row, "endpoint")?,
        client_transport: get(row, "client_transport")?,
        requested_model_id: get(row, "requested_model_id")?,
        provider_kind: get(row, "provider_kind")?,
        provider_account_ref: get(row, "provider_account_ref")?,
        provider_account_name: get(row, "provider_account_name_snapshot")?,
        provider_account_email: get(row, "provider_account_email_snapshot")?,
        provider_account_authentication_kind: get(
            row,
            "provider_account_authentication_kind_snapshot",
        )?,
        upstream_model_id: get(row, "upstream_model_id")?,
        upstream_transport: get(row, "upstream_transport")?,
        http_version: get(row, "http_version")?,
        websocket_pool: get(row, "websocket_pool")?,
        upstream_response_model: get(row, "upstream_response_model")?,
        service_tier: get(row, "service_tier")?,
        provider_metadata_json: optional_json(row, "provider_observation_json")?
            .map(|value| {
                serde_json::to_string(&value)
                    .map_err(|_| invalid("provider observation is malformed"))
            })
            .transpose()?,
        attempt_count: required_u32(row, "attempt_count")?,
        upstream_send_state: get(row, "upstream_send_state")?,
        downstream_committed_at: optional_time(row, "downstream_committed_at_us")?,
        outcome: crate::postgres::request_outcome(&get::<String>(row, "outcome")?)?,
        client_status_code: optional_status(row, "client_status_code")?,
        upstream_status_code: optional_status(row, "upstream_status_code")?,
        client_response_id: response_id("client_response_id")?,
        upstream_request_id: get(row, "upstream_request_id")?,
        upstream_response_id: response_id("upstream_response_id")?,
        error_kind: get(row, "error_kind")?,
        provider_error_code: get(row, "provider_error_code")?,
        error_message: get(row, "error_message")?,
        retry_after_ms: optional_unsigned(row, "retry_after_ms")?,
        input_tokens: optional_unsigned(row, "input_tokens")?,
        output_tokens: optional_unsigned(row, "output_tokens")?,
        cached_tokens: optional_unsigned(row, "cached_tokens")?,
        cache_write_tokens: optional_unsigned(row, "cache_write_tokens")?,
        reasoning_tokens: optional_unsigned(row, "reasoning_tokens")?,
        image_input_tokens: optional_unsigned(row, "image_input_tokens")?,
        image_output_tokens: optional_unsigned(row, "image_output_tokens")?,
        total_tokens: optional_unsigned(row, "total_tokens")?,
        cost_source,
        cost_amount,
        cost_currency,
        transport_decision_wait_ms: optional_unsigned(row, "transport_decision_wait_ms")?,
        connect_ms: optional_unsigned(row, "connect_ms")?,
        headers_ms: optional_unsigned(row, "headers_ms")?,
        first_event_ms: optional_unsigned(row, "first_event_ms")?,
        first_reasoning_ms: optional_unsigned(row, "first_reasoning_ms")?,
        first_text_ms: optional_unsigned(row, "first_text_ms")?,
        first_token_ms: optional_unsigned(row, "first_token_ms")?,
        provider_processing_ms: optional_unsigned(row, "provider_processing_ms")?,
        upstream_response_ms: optional_unsigned(row, "upstream_response_ms")?,
        upstream_api_overhead_ms: get(row, "upstream_api_overhead_ms")?,
        upstream_engine_ms: get(row, "upstream_engine_ms")?,
        upstream_engine_iapi_ttft_ms: get(row, "upstream_engine_iapi_ttft_ms")?,
        upstream_engine_service_ttft_ms: get(row, "upstream_engine_service_ttft_ms")?,
        upstream_engine_iapi_tbt_ms: get(row, "upstream_engine_iapi_tbt_ms")?,
        upstream_engine_service_tbt_ms: get(row, "upstream_engine_service_tbt_ms")?,
        latency_ms: optional_unsigned(row, "latency_ms")?,
        admission_decision_ms: optional_unsigned(row, "admission_decision_ms")?,
        account_selection_wait_ms: optional_unsigned(row, "account_selection_wait_ms")?,
        capacity_used_slots: optional_unsigned(row, "capacity_used_slots")?,
        capacity_total_slots: optional_unsigned(row, "capacity_total_slots")?,
        client_ip: get(row, "client_ip")?,
        user_agent: get(row, "user_agent")?,
        reasoning_effort: get(row, "reasoning_effort")?,
        reasoning_preset: get(row, "reasoning_preset")?,
        request_kind: get(row, "request_kind")?,
        subagent_kind: get(row, "subagent_kind")?,
        compact: bool_value(row, "compact")?,
        image_generation_requested: bool_value(row, "image_generation_requested")?,
        image_generation_succeeded: optional_bool_value(row, "image_generation_succeeded")?,
        started_at: required_time(row, "started_at_us")?,
        deadline_at: required_time(row, "deadline_at_us")?,
        completed_at: optional_time(row, "completed_at_us")?,
    })
}

fn intermediate_attempt_from_row(row: &SqliteRow) -> StoreResult<UsageAttemptObservation> {
    let status = optional_status(row, "status_code")?;
    Ok(UsageAttemptObservation {
        source: "ops_event".to_owned(),
        id: get(row, "id")?,
        attempt_index: required_u32(row, "attempt_index")?,
        component: get(row, "component")?,
        operation: get(row, "operation")?,
        provider_kind: get(row, "provider_kind")?,
        provider_account_ref: get(row, "provider_account_ref")?,
        provider_account_name: get(row, "provider_account_name")?,
        provider_account_email: get(row, "provider_account_email")?,
        provider_account_authentication_kind: get(row, "provider_account_authentication_kind")?,
        upstream_model_id: get(row, "upstream_model_id")?,
        upstream_transport: None,
        upstream_send_state: None,
        outcome: crate::postgres::request_outcome("failed")?,
        downstream_committed: false,
        status_code: status,
        provider_error_code: get(row, "provider_error_code")?,
        failure_kind: get(row, "failure_kind")?,
        retry_after_ms: optional_unsigned(row, "retry_after_ms")?,
        upstream_request_id: get(row, "upstream_request_id")?,
        latency_ms: optional_unsigned(row, "latency_ms")?,
        message: get(row, "message")?,
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        cache_write_tokens: None,
        reasoning_tokens: None,
        total_tokens: None,
        cost_source: None,
        cost_amount: None,
        cost_currency: None,
        occurred_at: required_time(row, "created_at_us")?,
    })
}

#[allow(clippy::too_many_lines)]
fn ops_error_from_row(row: &SqliteRow) -> StoreResult<OpsErrorRecord> {
    Ok(OpsErrorRecord {
        client_api_key_name: get(row, "client_api_key_name")?,
        source: get(row, "source")?,
        event_id: get(row, "event_id")?,
        request_id: get(row, "request_id")?,
        attempt_index: optional_u32(row, "attempt_index")?,
        client_api_key_ref: get(row, "client_api_key_ref")?,
        component: get(row, "component")?,
        operation: get(row, "operation")?,
        protocol: get(row, "protocol")?,
        client_transport: get(row, "client_transport")?,
        requested_model_id: get(row, "requested_model_id")?,
        service_tier: get(row, "service_tier")?,
        endpoint: get(row, "endpoint")?,
        provider_kind: get(row, "provider_kind")?,
        provider_account_ref: get(row, "provider_account_ref")?,
        provider_account_name: get(row, "provider_account_name")?,
        provider_account_email: get(row, "provider_account_email")?,
        provider_account_plan_type: get(row, "provider_account_plan_type")?,
        provider_account_plan_type_display: None,
        provider_account_authentication_kind: get(row, "provider_account_authentication_kind")?,
        upstream_model_id: get(row, "upstream_model_id")?,
        upstream_transport: get(row, "upstream_transport")?,
        failure_kind: get(row, "failure_kind")?,
        upstream_send_state: get(row, "upstream_send_state")?,
        client_status_code: optional_status(row, "client_status_code")?,
        upstream_status_code: optional_status(row, "upstream_status_code")?,
        provider_error_code: get(row, "provider_error_code")?,
        client_response_id: response_id(row, "client_response_id")?,
        upstream_request_id: get(row, "upstream_request_id")?,
        latency_ms: optional_unsigned(row, "latency_ms")?,
        message: get(row, "message")?,
        error_details: get(row, "error_details")?,
        client_ip: get(row, "client_ip")?,
        user_agent: get(row, "user_agent")?,
        reasoning_effort: get(row, "reasoning_effort")?,
        reasoning_preset: get(row, "reasoning_preset")?,
        request_kind: get(row, "request_kind")?,
        subagent_kind: get(row, "subagent_kind")?,
        compact: optional_bool_value(row, "compact")?,
        continuation_affinity_hash: get(row, "continuation_affinity_hash")?,
        continuation_previous_response_id_hash: get(row, "continuation_previous_response_id_hash")?,
        continuation_unavailable_reason: get(row, "continuation_unavailable_reason")?,
        upstream_connection_id: get(row, "upstream_connection_id")?,
        upstream_connection_exit_reason: get(row, "upstream_connection_exit_reason")?,
        upstream_connection_age_ms: optional_unsigned(row, "upstream_connection_age_ms")?,
        upstream_connection_idle_ms: optional_unsigned(row, "upstream_connection_idle_ms")?,
        recovery_request_id: get(row, "recovery_request_id")?,
        recovered_at: optional_time(row, "recovered_at_us")?,
        recovery_attempt_count: required_u32(row, "recovery_attempt_count")?,
        recovery_retry_delay_ms: optional_unsigned(row, "recovery_retry_delay_ms")?,
        recovery_total_latency_ms: optional_unsigned(row, "recovery_total_latency_ms")?,
        occurred_at: required_time(row, "occurred_at_us")?,
        stable_sort_id: get(row, "stable_sort_id")?,
    })
}

const USAGE_LIST_SELECT: &str = "select mr.*,
       client_key.name as client_api_key_name,
       account.notes as provider_account_notes,
       account.plan_type as provider_account_plan_type,
       mr.provider_account_name_snapshot as provider_account_name,
       mr.provider_account_email_snapshot as provider_account_email
  from model_request_observations mr
  left join client_api_keys client_key on client_key.id = mr.client_api_key_ref
  left join provider_accounts account on account.id = mr.provider_account_ref";

const OPS_REQUEST_SELECT: &str =
    " select 'model_request' as source, mr.id as event_id, mr.id as request_id,
        nullif(mr.attempt_count, 0) as attempt_index, mr.client_api_key_ref,
        client_key.name as client_api_key_name, 'model_request' as component, mr.operation,
        mr.protocol, mr.client_transport, mr.requested_model_id, mr.service_tier, mr.endpoint,
        mr.provider_kind, mr.provider_account_ref,
        mr.provider_account_name_snapshot as provider_account_name,
        mr.provider_account_email_snapshot as provider_account_email,
        account.plan_type as provider_account_plan_type,
        mr.provider_account_authentication_kind_snapshot as provider_account_authentication_kind,
        mr.upstream_model_id, mr.upstream_transport, mr.error_kind as failure_kind,
        mr.upstream_send_state, mr.client_status_code, mr.upstream_status_code,
        mr.provider_error_code, mr.client_response_id, mr.upstream_request_id, mr.latency_ms,
        coalesce(mr.error_message, mr.error_kind) as message, mr.error_details,
        mr.client_ip, mr.user_agent, mr.reasoning_effort, mr.reasoning_preset,
        mr.request_kind, mr.subagent_kind, mr.compact,
        mr.continuation_affinity_hash, mr.continuation_previous_response_id_hash,
        mr.continuation_unavailable_reason, mr.upstream_connection_id,
        mr.upstream_connection_exit_reason, mr.upstream_connection_age_ms,
        mr.upstream_connection_idle_ms, mr.recovery_request_id, mr.recovered_at_us,
        mr.recovery_attempt_count, mr.recovery_retry_delay_ms, mr.recovery_total_latency_ms,
        mr.completed_at_us as occurred_at_us,
        'model_request:' || mr.id as stable_sort_id
   from model_request_observations mr
   left join client_api_keys client_key on client_key.id = mr.client_api_key_ref
   left join provider_accounts account on account.id = mr.provider_account_ref
";

const OPS_EVENT_SELECT: &str =
    " select 'ops_event' as source, oe.id as event_id, oe.model_request_id as request_id,
        oe.attempt_index, mr.client_api_key_ref, client_key.name as client_api_key_name,
        oe.component, oe.operation, mr.protocol, mr.client_transport,
        mr.requested_model_id, mr.service_tier, mr.endpoint, oe.provider_kind,
        oe.provider_account_ref, oe.provider_account_name_snapshot as provider_account_name,
        oe.provider_account_email_snapshot as provider_account_email,
        account.plan_type as provider_account_plan_type,
        oe.provider_account_authentication_kind_snapshot as provider_account_authentication_kind,
        oe.upstream_model_id, null as upstream_transport, oe.failure_kind,
        oe.upstream_send_state, null as client_status_code, oe.status_code as upstream_status_code,
        oe.provider_error_code, mr.client_response_id, oe.upstream_request_id, oe.latency_ms,
        oe.message, oe.error_details, mr.client_ip, mr.user_agent,
        mr.reasoning_effort, mr.reasoning_preset, mr.request_kind, mr.subagent_kind, mr.compact,
        mr.continuation_affinity_hash, mr.continuation_previous_response_id_hash,
        null as continuation_unavailable_reason, null as upstream_connection_id,
        null as upstream_connection_exit_reason, null as upstream_connection_age_ms,
        null as upstream_connection_idle_ms, mr.recovery_request_id, mr.recovered_at_us,
        coalesce(mr.recovery_attempt_count, 0) as recovery_attempt_count,
        mr.recovery_retry_delay_ms, mr.recovery_total_latency_ms,
        oe.created_at_us as occurred_at_us,
        'ops_event:' || oe.id as stable_sort_id
   from ops_events oe
   left join model_request_observations mr on mr.id = oe.model_request_id
   left join client_api_keys client_key on client_key.id = mr.client_api_key_ref
   left join provider_accounts account on account.id = oe.provider_account_ref
";

fn get<'r, T>(row: &'r SqliteRow, column: &'static str) -> StoreResult<T>
where
    T: sqlx::Decode<'r, Sqlite> + sqlx::Type<Sqlite>,
{
    row.try_get(column).map_err(|_| unavailable())
}

fn optional_unsigned(row: &SqliteRow, column: &'static str) -> StoreResult<Option<u64>> {
    let value: Option<i64> = get(row, column)?;
    value.map(checked_u64).transpose()
}

fn required_unsigned(row: &SqliteRow, column: &'static str) -> StoreResult<u64> {
    let value: i64 = get(row, column)?;
    checked_u64(value)
}

fn required_u32(row: &SqliteRow, column: &'static str) -> StoreResult<u32> {
    u32::try_from(required_unsigned(row, column)?).map_err(|_| invalid("integer exceeds u32"))
}

fn optional_u32(row: &SqliteRow, column: &'static str) -> StoreResult<Option<u32>> {
    optional_unsigned(row, column)?
        .map(|value| u32::try_from(value).map_err(|_| invalid("integer exceeds u32")))
        .transpose()
}

fn optional_status(row: &SqliteRow, column: &'static str) -> StoreResult<Option<u16>> {
    optional_unsigned(row, column)?
        .map(|value| {
            u16::try_from(value)
                .ok()
                .filter(|value| (100..=599).contains(value))
                .ok_or_else(|| invalid("status code is outside its supported range"))
        })
        .transpose()
}

fn bool_value(row: &SqliteRow, column: &'static str) -> StoreResult<bool> {
    let value: i64 = get(row, column)?;
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invalid("stored boolean value is invalid")),
    }
}

fn optional_bool_value(row: &SqliteRow, column: &'static str) -> StoreResult<Option<bool>> {
    let value: Option<i64> = get(row, column)?;
    value
        .map(|value| match value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("stored boolean value is invalid")),
        })
        .transpose()
}

fn required_time(row: &SqliteRow, column: &'static str) -> StoreResult<DateTime<Utc>> {
    datetime_from_micros(get(row, column)?)
}

fn optional_time(row: &SqliteRow, column: &'static str) -> StoreResult<Option<DateTime<Utc>>> {
    let value: Option<i64> = get(row, column)?;
    value.map(datetime_from_micros).transpose()
}

fn optional_amount(row: &SqliteRow, column: &'static str) -> StoreResult<Option<DecimalAmount>> {
    let value: Option<String> = get(row, column)?;
    value
        .map(|value| {
            let amount = decode_amount(&value)?;
            crate::postgres::parse_decimal_amount(&amount.canonical())
        })
        .transpose()
}

fn optional_json(row: &SqliteRow, column: &'static str) -> StoreResult<Option<Value>> {
    let value: Option<String> = get(row, column)?;
    value
        .map(|value| {
            serde_json::from_str(&value).map_err(|_| invalid("stored JSON value is malformed"))
        })
        .transpose()
}

fn required_json<T>(row: &SqliteRow, column: &'static str) -> StoreResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let value: String = get(row, column)?;
    serde_json::from_str(&value).map_err(|_| invalid("stored JSON value is malformed"))
}

fn response_id(row: &SqliteRow, column: &'static str) -> StoreResult<Option<String>> {
    let value: Option<Vec<u8>> = get(row, column)?;
    value
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| invalid("response ID is not UTF-8"))
}
