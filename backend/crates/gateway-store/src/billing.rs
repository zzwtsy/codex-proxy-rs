//! Provider 成本账单快照的后端无关编码。

pub(crate) fn encode_billing_snapshot(
    b: &gateway_core::metering::CalculatedCostBreakdown,
) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "longContextBillingApplied": b.long_context_billing_applied(),
        "image": b.image().map(|image| serde_json::json!({
            "inputTokens": image.input_tokens, "cachedTokens": image.cached_tokens,
            "input": image.input_amount.amount().canonical(),
            "cacheRead": image.cache_read_amount.amount().canonical(),
            "inputPrice": image.input_price_per_million.amount().canonical(),
            "cacheReadPrice": image.cache_read_price_per_million.amount().canonical(),
        })),
        "input": b.input_amount().amount().canonical(),
        "output": b.output_amount().amount().canonical(),
        "cacheRead": b.cache_read_amount().amount().canonical(),
        "cacheWrite": b.cache_write_amount().amount().canonical(),
        "standard": b.standard_amount().amount().canonical(),
        "total": b.total_amount().amount().canonical(),
        "inputPrice": b.input_price_per_million().amount().canonical(),
        "outputPrice": b.output_price_per_million().amount().canonical(),
        "cacheReadPrice": b.cache_read_price_per_million().amount().canonical(),
        "cacheWritePrice": b.cache_write_price_per_million().amount().canonical(),
        "currency": b.total_amount().currency().as_str(),
        "serviceTier": b.service_tier(),
        "multiplierPercent": b.multiplier_percent(),
        "customMultiplierBps": b.custom_multiplier_bps(),
    })
}
