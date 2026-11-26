use serde::{Deserialize, Serialize};

use crate::tx::Tx;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub block_number: u64,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
    pub miner: String,
    pub gas_used: u64,
    pub gas_limit: u64,
    pub base_fee_per_gas: Option<String>,
    pub difficulty: String,
    pub total_difficulty: Option<String>,
    pub size: u64,
    pub transaction_count: u64,
    pub state_root: String,
    pub transactions_root: String,
    pub receipts_root: String,
    pub extra_data: String,
    pub nonce: Option<String>,
    pub mix_hash: Option<String>,
    pub withdrawals_root: Option<String>,
    pub blob_gas_used: Option<u64>,
    pub excess_blob_gas: Option<u64>,
    pub parent_beacon_block_root: Option<String>,
}

#[derive(Debug)]
pub struct ParsedBlock {
    pub block: Block,
    pub txs: Vec<Tx>,
}

#[cfg(test)]
impl Block {
    pub(crate) fn sample(number: u64) -> Self {
        Self {
            block_number: number,
            block_hash: format!("0x{number:064x}"),
            parent_hash: format!("0x{:064x}", number.saturating_sub(1)),
            timestamp: 1_700_000_000 + number,
            miner: format!("0x{}", "99".repeat(20)),
            gas_used: 21_000,
            gas_limit: 30_000_000,
            base_fee_per_gas: Some("1000000000".to_string()),
            difficulty: "0".to_string(),
            total_difficulty: Some("0".to_string()),
            size: 1_000,
            transaction_count: 0,
            state_root: format!("0x{:064x}", number + 1),
            transactions_root: format!("0x{:064x}", number + 2),
            receipts_root: format!("0x{:064x}", number + 3),
            extra_data: "0x".to_string(),
            nonce: Some("0x0000000000000000".to_string()),
            mix_hash: Some(format!("0x{:064x}", number + 4)),
            withdrawals_root: Some(format!("0x{:064x}", number + 5)),
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
        }
    }
}
