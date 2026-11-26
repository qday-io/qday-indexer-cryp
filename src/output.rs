use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;

use anyhow::{Context, Result};

use crate::hop::OutputRow;

pub const COLUMNS: &[&str] = &[
    "hop_stage",
    "block_number",
    "transaction_index",
    "transaction_hash",
    "from_address",
    "to_address",
    "value",
    "nonce",
    "gas_limit",
    "gas_price",
    "max_fee_per_gas",
    "max_priority_fee_per_gas",
    "transaction_type",
    "block_hash",
    "input",
];

pub fn write_csv(path: &Path, rows: &[OutputRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = csv::Writer::from_writer(BufWriter::new(file));
    writer
        .write_record(COLUMNS.iter().copied())
        .context("write csv header")?;
    for row in rows {
        writer
            .write_record(csv_record(row))
            .context("write csv row")?;
    }
    writer.flush().context("flush csv")?;
    Ok(())
}

fn csv_record(row: &OutputRow) -> [String; 15] {
    [
        row.hop_stage.to_string(),
        row.tx.block_number.to_string(),
        row.tx.transaction_index.to_string(),
        row.tx.transaction_hash.clone(),
        row.tx.from_address.clone(),
        opt_string(&row.tx.to_address),
        row.tx.value.clone(),
        row.tx.nonce.to_string(),
        row.tx.gas_limit.to_string(),
        opt_string(&row.tx.gas_price),
        opt_string(&row.tx.max_fee_per_gas),
        opt_string(&row.tx.max_priority_fee_per_gas),
        opt_u64(row.tx.transaction_type),
        row.tx.block_hash.clone(),
        row.tx.input.clone(),
    ]
}

fn opt_string(value: &Option<String>) -> String {
    value.clone().unwrap_or_default()
}

fn opt_u64(value: Option<u64>) -> String {
    value.map(|number| number.to_string()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::Tx;

    fn addr(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    #[test]
    fn writes_csv_rows() {
        let dir = tempfile::tempdir().unwrap();
        let tx = Tx::sample(3, 1, &addr(1), Some(&addr(2)));
        let rows = vec![OutputRow {
            hop_stage: "hop1",
            tx,
        }];
        let csv_path = dir.path().join("out.csv");
        write_csv(&csv_path, &rows).unwrap();

        let mut reader = csv::Reader::from_path(&csv_path).unwrap();
        let headers: Vec<String> = reader
            .headers()
            .unwrap()
            .iter()
            .map(str::to_string)
            .collect();
        assert_eq!(
            headers,
            COLUMNS
                .iter()
                .copied()
                .map(str::to_string)
                .collect::<Vec<_>>()
        );
        let records: Vec<_> = reader.records().map(|record| record.unwrap()).collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].get(0), Some("hop1"));
        assert_eq!(records[0].get(1), Some("3"));
        assert_eq!(records[0].get(2), Some("1"));
        assert_eq!(records[0].get(6), Some("0"));
        assert!(dir.path().read_dir().unwrap().all(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "csv")
        }));
    }

    #[test]
    fn writes_header_only_csv() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("out.csv");
        write_csv(&csv_path, &[]).unwrap();
        let mut reader = csv::Reader::from_path(&csv_path).unwrap();
        assert_eq!(reader.records().count(), 0);
    }
}
