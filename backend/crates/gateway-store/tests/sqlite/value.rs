mod tests {
    use std::str::FromStr as _;

    use gateway_core::metering::Decimal;
    use gateway_store::sqlite::value::{checked_amount_sum, decode_amount, encode_amount};

    #[test]
    fn fixed_width_amounts_round_trip_and_keep_lexical_order() {
        let tiny = Decimal::from_str("0.0000000001").expect("ten decimal places");
        let whole = Decimal::from_str("7").expect("whole amount");
        let max = Decimal::MAX;

        assert_eq!(decode_amount(&encode_amount(tiny)).unwrap(), tiny);
        assert_eq!(decode_amount(&encode_amount(whole)).unwrap(), whole);
        assert_eq!(decode_amount(&encode_amount(max)).unwrap(), max);
        assert!(encode_amount(tiny) < encode_amount(whole));
        assert!(encode_amount(whole) < encode_amount(max));
    }

    #[test]
    fn amount_sum_is_checked_at_the_domain_limit() {
        let one = Decimal::from_str("1").expect("one dollar");
        let tenth = Decimal::from_str("0.1").expect("one tenth");
        let total = checked_amount_sum([one, tenth]).expect("sum fits");
        assert_eq!(total.canonical(), "1.1");
        assert!(checked_amount_sum([Decimal::MAX, tenth]).is_err());
    }

    #[test]
    fn malformed_and_out_of_range_amounts_are_rejected() {
        assert!(decode_amount("1").is_err());
        assert!(decode_amount("0000000000000000000x").is_err());
        assert_eq!(decode_amount("99999999999999999999").unwrap(), Decimal::MAX);
    }
}
