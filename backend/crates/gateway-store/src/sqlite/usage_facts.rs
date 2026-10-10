//! SQLite 用量事实的共享资格规则。

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
