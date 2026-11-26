use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Log {
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub transaction_hash: String,
    pub block_hash: String,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
}

#[cfg(test)]
impl Log {
    pub(crate) fn sample(block: u64, tx_index: u64, log_index: u64) -> Self {
        Self {
            block_number: block,
            transaction_index: tx_index,
            log_index,
            transaction_hash: format!("0x{block:016x}{tx_index:048x}"),
            block_hash: format!("0x{block:064x}"),
            address: format!("0x{}", "aa".repeat(20)),
            topics: vec![format!("0x{:064x}", 1)],
            data: "0x".to_string(),
        }
    }
}
