//! SQLite 管理观测查询的 SQLite 专属 SQL 与行辅助函数。

mod metrics;
mod records;

pub(crate) use metrics::{
    calculated_billing_facts, dashboard_account_usage, dashboard_totals, metric_series,
    usage_diagnostics, usage_overview,
};
pub(crate) use records::{list_usage_records, ops_errors, usage_record_detail};

use crate::postgres::{ObservabilityRange, UsageRecordFilter};
use crate::{StoreError, StoreResult};
use sqlx::{QueryBuilder, Sqlite};

pub(crate) fn completed_usage_fact_predicate(alias: &str) -> String {
    format!(
        "{alias}.outcome = 'succeeded'
         and {alias}.downstream_committed_at_us is not null
         and ({alias}.provider_kind is not 'openai'
              or {alias}.request_kind is not 'prewarm')
         and (({alias}.client_transport = 'websocket' and {alias}.client_status_code is null)
              or {alias}.client_status_code between 200 and 399)
         and ({alias}.requested_model_id is not null
              or {alias}.upstream_model_id is not null
              or {alias}.image_generation_requested = 1
              or {alias}.input_tokens is not null
              or {alias}.output_tokens is not null
              or {alias}.cached_tokens is not null
              or {alias}.cache_write_tokens is not null
              or {alias}.reasoning_tokens is not null
              or {alias}.image_input_tokens is not null
              or {alias}.image_output_tokens is not null
              or {alias}.total_tokens is not null
              or {alias}.cost_amount is not null)"
    )
}

pub(crate) fn push_range(query: &mut QueryBuilder<Sqlite>, alias: &str, range: ObservabilityRange) {
    query.push(format!("{alias}.started_at_us >= "));
    query.push_bind(range.start.timestamp_micros());
    query.push(format!(" and {alias}.started_at_us < "));
    query.push_bind(range.end.timestamp_micros());
}

pub(crate) fn push_usage_filter(
    query: &mut QueryBuilder<Sqlite>,
    filter: &UsageRecordFilter,
    alias: &str,
) {
    if let Some(value) = &filter.client_api_key_ref {
        query.push(format!(" and {alias}.client_api_key_ref = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = &filter.request_id {
        query.push(format!(" and {alias}.id = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = &filter.provider_account_ref {
        query.push(format!(" and {alias}.provider_account_ref = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = &filter.operation {
        query.push(format!(" and {alias}.operation = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = &filter.provider_kind {
        query.push(format!(" and {alias}.provider_kind = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = &filter.model {
        query.push(format!(" and ({alias}.requested_model_id = "));
        query.push_bind(value.clone());
        query.push(format!(" or {alias}.upstream_model_id = "));
        query.push_bind(value.clone());
        query.push(")");
    }
    if let Some(value) = &filter.outcome {
        query.push(format!(" and {alias}.outcome = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = filter.status_code {
        query.push(format!(" and ({alias}.client_status_code = "));
        query.push_bind(i64::from(value));
        query.push(format!(" or {alias}.upstream_status_code = "));
        query.push_bind(i64::from(value));
        query.push(")");
    }
    if let Some(value) = &filter.transport {
        query.push(format!(" and ({alias}.client_transport = "));
        query.push_bind(value.clone());
        query.push(format!(" or {alias}.upstream_transport = "));
        query.push_bind(value.clone());
        query.push(")");
    }
    if let Some(value) = filter.attempt_index {
        let attempt_index = i64::from(value);
        query.push(format!(" and ({alias}.attempt_count = "));
        query.push_bind(attempt_index);
        query.push(format!(
            " or exists (select 1 from ops_events attempt_event
                          where attempt_event.model_request_id = {alias}.id
                            and attempt_event.attempt_index = "
        ));
        query.push_bind(attempt_index);
        query.push("))");
    }
    if let Some(value) = &filter.response_id {
        query.push(format!(" and ({alias}.client_response_id = "));
        query.push_bind(value.as_bytes().to_vec());
        query.push(format!(" or {alias}.upstream_response_id = "));
        query.push_bind(value.as_bytes().to_vec());
        query.push(")");
    }
    if let Some(value) = &filter.upstream_request_id {
        query.push(format!(" and {alias}.upstream_request_id = "));
        query.push_bind(value.clone());
    }
    if let Some(value) = &filter.search {
        query.push(" and (");
        let columns = [
            format!("{alias}.id"),
            format!("{alias}.client_api_key_ref"),
            format!("{alias}.provider_account_ref"),
            format!("{alias}.provider_account_email_snapshot"),
            format!("{alias}.provider_account_name_snapshot"),
            format!("{alias}.requested_model_id"),
            format!("{alias}.upstream_model_id"),
            format!("{alias}.upstream_request_id"),
        ];
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                query.push(" or ");
            }
            query.push(format!("substr({column}, 1, length("));
            query.push_bind(value.clone());
            query.push(")) = ");
            query.push_bind(value.clone());
        }
        query.push(" or exists (select 1 from client_api_keys searched_client_key where searched_client_key.id = ");
        query.push(format!("{alias}.client_api_key_ref"));
        query.push(" and substr(lower(searched_client_key.name), 1, length(lower(");
        query.push_bind(value.clone());
        query.push("))) = lower(");
        query.push_bind(value.clone());
        query.push(")))");
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> StoreError {
    StoreError::InvalidData {
        entity: "SQLite observability",
        message: message.into(),
    }
}

pub(crate) fn unavailable() -> StoreError {
    StoreError::Unavailable {
        backend: crate::StoreBackend::Sqlite,
        message: "SQLite observability query failed".to_owned(),
    }
}

pub(crate) fn checked_u64(value: i64) -> StoreResult<u64> {
    u64::try_from(value).map_err(|_| invalid("observation value is negative"))
}
