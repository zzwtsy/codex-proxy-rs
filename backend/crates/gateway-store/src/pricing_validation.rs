//! 价格覆盖的后端无关校验。

use gateway_core::{metering::PricingOverrides, routing::ProviderKind};

use crate::{StoreError, StoreResult};

pub(crate) fn validate_pricing(pricing: &PricingOverrides) -> StoreResult<()> {
    if pricing
        .values()
        .map(std::collections::BTreeMap::len)
        .sum::<usize>()
        > 10_000
    {
        return Err(invalid_pricing());
    }
    for (provider, models) in pricing {
        if ProviderKind::new(provider.clone()).is_err() {
            return Err(invalid_pricing());
        }
        for (model, pricing) in models {
            if model.is_empty()
                || model.len() > 128
                || model.chars().any(char::is_whitespace)
                || model.chars().any(char::is_control)
                || pricing.validate().is_err()
            {
                return Err(invalid_pricing());
            }
        }
    }
    Ok(())
}

fn invalid_pricing() -> StoreError {
    StoreError::InvalidData {
        entity: "model pricing",
        message: "invalid model pricing".to_owned(),
        source: None,
    }
}
