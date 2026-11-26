use anyhow::{bail, Result};

pub fn normalize_required_address(raw: &str) -> Result<String> {
    let hex = address_hex(raw)?;
    Ok(format!("0x{hex}"))
}

pub fn normalize_optional_address(raw: Option<&str>) -> Result<Option<String>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let trimmed = raw.trim().trim_start_matches('\u{feff}');
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    Ok(Some(normalize_required_address(trimmed)?))
}

fn address_hex(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_start_matches('\u{feff}');
    if trimmed.is_empty() {
        bail!("empty address");
    }
    let lower = trimmed.to_ascii_lowercase();
    let hex = lower.strip_prefix("0x").unwrap_or(lower.as_str());
    if hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(hex.to_string())
    } else {
        bail!("invalid address: {trimmed}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_case_and_optional_prefix() {
        let mixed = "0xAbCdEf0000000000000000000000000000000001";
        let bare = "abcdef0000000000000000000000000000000001";
        assert_eq!(
            normalize_required_address(mixed).unwrap(),
            normalize_required_address(bare).unwrap()
        );
        assert!(normalize_required_address(mixed).unwrap().starts_with("0x"));
    }

    #[test]
    fn rejects_short_addresses() {
        assert!(normalize_required_address("0x1234").is_err());
    }

    #[test]
    fn optional_address_treats_null_as_absent() {
        assert_eq!(normalize_optional_address(None).unwrap(), None);
        assert_eq!(normalize_optional_address(Some("")).unwrap(), None);
        assert_eq!(normalize_optional_address(Some("null")).unwrap(), None);
    }
}
