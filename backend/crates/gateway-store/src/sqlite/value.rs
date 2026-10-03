//! SQLite 对 `Decimal` 的无损文本编码。

use gateway_core::metering::Decimal;

use crate::{StoreError, StoreResult};

/// `numeric(20, 10)` 的缩放整数写成固定宽度十进制文本，避免 SQLite REAL 精度损失。
pub fn encode_amount(amount: Decimal) -> String {
    format!("{:020}", amount.scaled())
}

/// 解码固定宽度定点文本，并复用 Core 的金额范围校验。
pub fn decode_amount(value: &str) -> StoreResult<Decimal> {
    if value.len() != 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_amount());
    }
    let scaled = value.parse::<u128>().map_err(|_| invalid_amount())?;
    Decimal::from_scaled(scaled).map_err(|_| invalid_amount())
}

/// Rust 端精确求和；SQLite 不对缩放金额文本执行近似聚合。
pub fn checked_amount_sum(values: impl IntoIterator<Item = Decimal>) -> StoreResult<Decimal> {
    values.into_iter().try_fold(Decimal::ZERO, |sum, amount| {
        sum.checked_add(amount).ok_or_else(invalid_amount)
    })
}

pub(crate) fn datetime_to_micros(value: chrono::DateTime<chrono::Utc>) -> i64 {
    value.timestamp_micros()
}

pub(crate) fn datetime_from_micros(value: i64) -> StoreResult<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp_micros(value).ok_or_else(|| StoreError::InvalidData {
        entity: "SQLite timestamp",
        message: "timestamp is outside UTC range".to_owned(),
    })
}

pub(crate) fn duration_micros(value: std::time::Duration) -> StoreResult<i64> {
    i64::try_from(value.as_micros()).map_err(|_| StoreError::InvalidData {
        entity: "SQLite duration",
        message: "duration is outside SQLite timestamp range".to_owned(),
    })
}

fn invalid_amount() -> StoreError {
    StoreError::InvalidData {
        entity: "decimal amount",
        message: "amount is malformed or exceeds numeric(20, 10)".to_owned(),
    }
}
