use anyhow::{bail, Context, Result};
use num_bigint::BigUint;
use num_traits::Num;

pub fn hex_quantity_to_u64(value: &str) -> Result<u64> {
    let digits = strip_prefix(value)?;
    if digits.is_empty() {
        return Ok(0);
    }
    if digits.len() > 16 {
        bail!("hex quantity does not fit in u64: {value}");
    }
    u64::from_str_radix(digits, 16).with_context(|| format!("invalid hex quantity: {value}"))
}

pub fn hex_quantity_to_decimal(value: &str) -> Result<String> {
    let digits = strip_prefix(value)?;
    if digits.is_empty() {
        return Ok("0".to_string());
    }
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("invalid hex quantity: {value}");
    }
    let number = BigUint::from_str_radix(digits, 16)
        .with_context(|| format!("invalid hex quantity: {value}"))?;
    Ok(number.to_str_radix(10))
}

pub fn normalize_hash(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    let lower = trimmed.to_ascii_lowercase();
    let Some(hex) = lower.strip_prefix("0x") else {
        bail!("hash missing 0x prefix: {trimmed}");
    };
    if hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(format!("0x{hex}"))
    } else {
        bail!("invalid 32-byte hash: {trimmed}");
    }
}

pub fn normalize_data(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "0x".to_string();
    }
    let lower = trimmed.to_ascii_lowercase();
    match lower.strip_prefix("0x") {
        Some(rest) => format!("0x{rest}"),
        None => format!("0x{lower}"),
    }
}

fn strip_prefix(value: &str) -> Result<&str> {
    let value = value.trim();
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .with_context(|| format!("hex quantity missing 0x prefix: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_wei_hex_to_decimal() {
        assert_eq!(hex_quantity_to_decimal("0x0").unwrap(), "0");
        assert_eq!(hex_quantity_to_decimal("0x00").unwrap(), "0");
        assert_eq!(
            hex_quantity_to_decimal("0xde0b6b3a7640000").unwrap(),
            "1000000000000000000"
        );
    }

    #[test]
    fn converts_small_quantities() {
        assert_eq!(hex_quantity_to_u64("0x5208").unwrap(), 21_000);
        assert_eq!(hex_quantity_to_u64("0x3b9aca00").unwrap(), 1_000_000_000);
    }

    #[test]
    fn rejects_quantities_without_prefix() {
        assert!(hex_quantity_to_u64("10").is_err());
    }
}
