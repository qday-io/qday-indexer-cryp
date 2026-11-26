use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::addr::normalize_required_address;

pub fn load_seeds(path: &Path) -> Result<HashSet<String>> {
    let text =
        fs::read_to_string(path).with_context(|| format!("read seed file {}", path.display()))?;
    let text = text.trim_start_matches('\u{feff}');
    let seeds = if is_csv(path, text) {
        parse_csv(text)?
    } else {
        parse_lines(text)?
    };
    if seeds.is_empty() {
        bail!("seed file {} contains no addresses", path.display());
    }
    Ok(seeds)
}

fn is_csv(path: &Path, text: &str) -> bool {
    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
    {
        return true;
    }
    let first = text
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'));
    matches!(
        first,
        Some(line) if line.contains(',') || line.eq_ignore_ascii_case("address")
    )
}

fn parse_lines(text: &str) -> Result<HashSet<String>> {
    let mut seeds = HashSet::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let addr = normalize_required_address(line)
            .with_context(|| format!("seed file line {}", index + 1))?;
        seeds.insert(addr);
    }
    Ok(seeds)
}

fn parse_csv(text: &str) -> Result<HashSet<String>> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .flexible(true)
        .from_reader(text.as_bytes());
    let column = {
        let headers = reader.headers().context("read csv header")?;
        headers
            .iter()
            .position(|header| {
                header
                    .trim_start_matches('\u{feff}')
                    .eq_ignore_ascii_case("address")
            })
            .context("csv seed file must include an `address` column")?
    };
    let mut seeds = HashSet::new();
    for (index, record) in reader.records().enumerate() {
        let record = record.with_context(|| format!("csv row {}", index + 2))?;
        let raw = record.get(column).unwrap_or("").trim();
        if raw.is_empty() {
            continue;
        }
        let addr =
            normalize_required_address(raw).with_context(|| format!("csv row {}", index + 2))?;
        seeds.insert(addr);
    }
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn addr(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    #[test]
    fn loads_text_comments_and_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seeds.txt");
        let mut file = fs::File::create(&path).unwrap();
        writeln!(file, "# comment").unwrap();
        writeln!(file).unwrap();
        writeln!(file, "{}", addr(0x11).to_uppercase()).unwrap();
        writeln!(file, "{}", addr(0x11)).unwrap();
        writeln!(file, "  {}  ", addr(0x22)).unwrap();

        let seeds = load_seeds(&path).unwrap();
        assert_eq!(seeds.len(), 2);
        assert!(seeds.contains(&addr(0x11)));
        assert!(seeds.contains(&addr(0x22)));
    }

    #[test]
    fn loads_csv_address_column() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seeds.csv");
        fs::write(
            &path,
            format!("label,address\nalpha,{}\n,\n", addr(0x33).to_uppercase()),
        )
        .unwrap();
        let seeds = load_seeds(&path).unwrap();
        assert_eq!(seeds, HashSet::from([addr(0x33)]));
    }

    #[test]
    fn rejects_invalid_text_address() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seeds.txt");
        fs::write(&path, "not-an-address\n").unwrap();
        assert!(load_seeds(&path).is_err());
    }
}
