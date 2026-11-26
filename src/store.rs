use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::tx::Tx;

pub fn batch_path(dir: &Path, start: u64, end: u64) -> PathBuf {
    dir.join(format!("batch_{start}_{end}.jsonl"))
}

pub fn prepare_raw_dir(dir: &Path, chunks: &[(u64, u64)], resume: bool) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    let paths: Vec<PathBuf> = chunks
        .iter()
        .map(|(start, end)| batch_path(dir, *start, *end))
        .collect();
    let expected: HashSet<String> = paths
        .iter()
        .filter_map(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .collect();

    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let managed = name.starts_with("batch_")
            && (name.ends_with(".jsonl") || name.ends_with(".jsonl.tmp"));
        if !managed {
            continue;
        }
        let unexpected = !expected.contains(name.as_ref());
        let refresh = !resume && expected.contains(name.as_ref());
        if name.ends_with(".jsonl.tmp") || unexpected || refresh {
            fs::remove_file(entry.path())
                .with_context(|| format!("remove {}", entry.path().display()))?;
        }
    }
    Ok(paths)
}

pub fn write_txs(path: &Path, txs: &[Tx]) -> Result<()> {
    let mut order: Vec<&Tx> = txs.iter().collect();
    order.sort_by(|left, right| {
        (
            left.block_number,
            left.transaction_index,
            left.transaction_hash.as_str(),
        )
            .cmp(&(
                right.block_number,
                right.transaction_index,
                right.transaction_hash.as_str(),
            ))
    });
    let tmp = tmp_path(path);
    let write_result = write_jsonl(&tmp, &order);
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    write_result.with_context(|| format!("write {}", path.display()))?;
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("replace {}", path.display()))?;
    }
    fs::rename(&tmp, path).with_context(|| format!("rename {}", path.display()))?;
    Ok(())
}

pub fn count_txs(path: &Path) -> Result<u64> {
    let reader = TxReader::open(&[path.to_path_buf()])?;
    let mut count = 0u64;
    for item in reader {
        item?;
        count += 1;
    }
    Ok(count)
}

pub struct TxReader {
    paths: Vec<PathBuf>,
    path_index: usize,
    current: PathBuf,
    lines: Option<std::io::Lines<BufReader<File>>>,
    line_no: usize,
}

impl TxReader {
    pub fn open(paths: &[PathBuf]) -> Result<Self> {
        for path in paths {
            if !path.is_file() {
                anyhow::bail!("missing raw transaction file {}", path.display());
            }
        }
        Ok(Self {
            paths: paths.to_vec(),
            path_index: 0,
            current: PathBuf::new(),
            lines: None,
            line_no: 0,
        })
    }
}

impl Iterator for TxReader {
    type Item = Result<Tx>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.lines.is_none() {
                if self.path_index >= self.paths.len() {
                    return None;
                }
                let path = self.paths[self.path_index].clone();
                self.path_index += 1;
                match File::open(&path) {
                    Ok(file) => {
                        self.current = path;
                        self.line_no = 0;
                        self.lines = Some(BufReader::new(file).lines());
                    }
                    Err(err) => {
                        return Some(Err(err).with_context(|| format!("open {}", path.display())));
                    }
                }
            }

            let line = {
                let lines = self.lines.as_mut().expect("lines just opened");
                lines.next()
            };
            match line {
                None => {
                    self.lines = None;
                }
                Some(Err(err)) => {
                    return Some(
                        Err(err).with_context(|| format!("read {}", self.current.display())),
                    );
                }
                Some(Ok(line)) => {
                    self.line_no += 1;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let line_no = self.line_no;
                    let path = self.current.display().to_string();
                    return Some(
                        serde_json::from_str(&line)
                            .with_context(|| format!("{path}:{line_no} invalid transaction json")),
                    );
                }
            }
        }
    }
}

fn write_jsonl(path: &Path, txs: &[&Tx]) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for tx in txs {
        serde_json::to_writer(&mut writer, tx)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.into_inner()?.sync_all()?;
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::Tx;

    fn addr(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    #[test]
    fn jsonl_roundtrip_preserves_optional_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("batch_1_1.jsonl");
        let mut tx = Tx::sample(1, 0, &addr(0x11), None);
        tx.gas_price = None;
        tx.transaction_type = None;
        write_txs(&path, &[tx.clone()]).unwrap();
        let loaded: Vec<Tx> = TxReader::open(&[path])
            .unwrap()
            .map(|item| item.unwrap())
            .collect();
        assert_eq!(loaded, vec![tx]);
    }

    #[test]
    fn prepare_keeps_only_expected_resume_files() {
        let dir = tempfile::tempdir().unwrap();
        let stale = batch_path(dir.path(), 9, 9);
        let kept = batch_path(dir.path(), 5, 6);
        fs::create_dir_all(dir.path()).unwrap();
        fs::write(&stale, "{}\n").unwrap();
        fs::write(&kept, "{}\n").unwrap();

        prepare_raw_dir(dir.path(), &[(5, 6)], true).unwrap();
        assert!(kept.exists());
        assert!(!stale.exists());

        prepare_raw_dir(dir.path(), &[(5, 6)], false).unwrap();
        assert!(!kept.exists());
    }
}
