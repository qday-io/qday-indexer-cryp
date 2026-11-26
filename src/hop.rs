use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};

use crate::addr::{normalize_optional_address, normalize_required_address};
use crate::tx::Tx;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct TxPos {
    block_number: u64,
    transaction_index: u64,
}

pub struct HopOutput {
    pub hop1: Vec<Tx>,
    pub hop2: Vec<Tx>,
    pub recipients: usize,
    pub scanned: u64,
}

pub struct OutputRow {
    pub hop_stage: &'static str,
    pub tx: Tx,
}

impl HopOutput {
    pub fn into_rows(self) -> Vec<OutputRow> {
        let mut rows = Vec::with_capacity(self.hop1.len() + self.hop2.len());
        for tx in self.hop1 {
            rows.push(OutputRow {
                hop_stage: "hop1",
                tx,
            });
        }
        for tx in self.hop2 {
            rows.push(OutputRow {
                hop_stage: "hop2",
                tx,
            });
        }
        rows.sort_by(|left, right| {
            (
                left.tx.block_number,
                left.tx.transaction_index,
                stage_ord(left.hop_stage),
            )
                .cmp(&(
                    right.tx.block_number,
                    right.tx.transaction_index,
                    stage_ord(right.hop_stage),
                ))
        });
        rows
    }
}

pub fn collect_hops<I, F>(
    mut open: F,
    seeds: &HashSet<String>,
    from_block: u64,
    to_block: u64,
    exclude_seeds_from_hop2: bool,
) -> Result<HopOutput>
where
    F: FnMut() -> Result<I>,
    I: Iterator<Item = Result<Tx>>,
{
    let mut hop1 = Vec::new();
    let mut hop1_seen = HashSet::new();
    let mut first_recv: HashMap<String, TxPos> = HashMap::new();
    let mut scanned = 0u64;

    for item in open()? {
        let tx = item?;
        scanned += 1;
        if !in_range(&tx, from_block, to_block) {
            continue;
        }
        let from = sender(&tx)?;
        if !seeds.contains(&from) {
            continue;
        }
        if let Some(to) = recipient(&tx)? {
            if !(exclude_seeds_from_hop2 && seeds.contains(&to)) {
                observe_recipient(
                    &mut first_recv,
                    to,
                    TxPos {
                        block_number: tx.block_number,
                        transaction_index: tx.transaction_index,
                    },
                );
            }
        }
        if hop1_seen.insert((tx.block_number, tx.transaction_index)) {
            hop1.push(tx);
        }
    }

    let mut hop2 = Vec::new();
    let mut hop2_seen = HashSet::new();
    for item in open()? {
        let tx = item?;
        if !in_range(&tx, from_block, to_block) {
            continue;
        }
        let from = sender(&tx)?;
        let pos = TxPos {
            block_number: tx.block_number,
            transaction_index: tx.transaction_index,
        };
        let is_later = first_recv.get(&from).is_some_and(|first| pos > *first);
        if is_later && hop2_seen.insert((tx.block_number, tx.transaction_index)) {
            hop2.push(tx);
        }
    }

    let recipients = first_recv.len();
    Ok(HopOutput {
        hop1,
        hop2,
        recipients,
        scanned,
    })
}

fn in_range(tx: &Tx, from_block: u64, to_block: u64) -> bool {
    tx.block_number >= from_block && tx.block_number <= to_block
}

fn sender(tx: &Tx) -> Result<String> {
    normalize_required_address(&tx.from_address)
        .with_context(|| format!("transaction {} from_address", tx.transaction_hash))
}

fn recipient(tx: &Tx) -> Result<Option<String>> {
    normalize_optional_address(tx.to_address.as_deref())
        .with_context(|| format!("transaction {} to_address", tx.transaction_hash))
}

fn observe_recipient(map: &mut HashMap<String, TxPos>, addr: String, pos: TxPos) {
    map.entry(addr)
        .and_modify(|existing| {
            if pos < *existing {
                *existing = pos;
            }
        })
        .or_insert(pos);
}

fn stage_ord(stage: &str) -> u8 {
    match stage {
        "hop1" => 0,
        "hop2" => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::Tx;

    fn addr(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    fn run(txs: &[Tx], seeds: &[&str], from: u64, to: u64, exclude: bool) -> HopOutput {
        let owned = txs.to_vec();
        let seeds = seeds.iter().map(|seed| (*seed).to_string()).collect();
        collect_hops(
            || Ok(owned.clone().into_iter().map(Ok)),
            &seeds,
            from,
            to,
            exclude,
        )
        .unwrap()
    }

    #[test]
    fn same_block_and_min_receipt_define_hop2() {
        let a = addr(0x11);
        let b = addr(0x22);
        let c = addr(0x33);
        let d = addr(0x44);
        let txs = vec![
            Tx::sample(1, 0, &c, Some(&d)),
            Tx::sample(1, 1, &b, Some(&c)),
            Tx::sample(1, 2, &c, Some(&d)),
            Tx::sample(1, 3, &c, Some(&d)),
            Tx::sample(1, 5, &a, Some(&c)),
        ];
        let out = run(&txs, &[&a, &b], 1, 1, false);
        let hop2: Vec<_> = out.hop2.iter().map(|tx| tx.transaction_index).collect();
        assert_eq!(hop2, vec![2, 3]);
        assert_eq!(out.recipients, 1);
        assert_eq!(out.hop1.len(), 2);
    }

    #[test]
    fn contract_creation_does_not_create_a_recipient() {
        let a = addr(0x11);
        let txs = vec![Tx::sample(4, 0, &a, None)];
        let out = run(&txs, &[&a], 4, 4, false);
        assert_eq!(out.hop1.len(), 1);
        assert_eq!(out.recipients, 0);
        assert!(out.hop2.is_empty());
    }

    #[test]
    fn seed_to_seed_can_be_excluded_from_hop2() {
        let a = addr(0x11);
        let b = addr(0x22);
        let c = addr(0x33);
        let txs = vec![
            Tx::sample(1, 0, &b, Some(&a)),
            Tx::sample(1, 1, &a, Some(&c)),
        ];
        let included = run(&txs, &[&a, &b], 1, 1, false).into_rows();
        assert_eq!(
            included
                .iter()
                .map(|row| (row.hop_stage, row.tx.transaction_index))
                .collect::<Vec<_>>(),
            vec![("hop1", 0), ("hop1", 1), ("hop2", 1)]
        );

        let excluded = run(&txs, &[&a, &b], 1, 1, true);
        assert_eq!(excluded.hop2.len(), 0);
        assert_eq!(excluded.hop1.len(), 2);
    }

    #[test]
    fn hop1_outside_the_range_does_not_seed_hop2() {
        let a = addr(0x11);
        let c = addr(0x33);
        let d = addr(0x44);
        let txs = vec![
            Tx::sample(1, 0, &a, Some(&c)),
            Tx::sample(2, 0, &c, Some(&d)),
        ];
        let out = run(&txs, &[&a], 2, 2, false);
        assert!(out.hop1.is_empty());
        assert!(out.hop2.is_empty());
    }
}
