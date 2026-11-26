use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tx {
    pub block_number: u64,
    pub transaction_index: u64,
    pub transaction_hash: String,
    pub block_hash: String,
    pub from_address: String,
    pub to_address: Option<String>,
    pub value: String,
    pub nonce: u64,
    pub gas_limit: u64,
    pub gas_price: Option<String>,
    pub max_fee_per_gas: Option<String>,
    pub max_priority_fee_per_gas: Option<String>,
    pub transaction_type: Option<u64>,
    pub input: String,
}

#[cfg(test)]
impl Tx {
    pub(crate) fn sample(block: u64, index: u64, from: &str, to: Option<&str>) -> Self {
        Self {
            block_number: block,
            transaction_index: index,
            transaction_hash: format!("0x{block:016x}{index:048x}"),
            block_hash: format!("0x{block:064x}"),
            from_address: from.to_string(),
            to_address: to.map(str::to_string),
            value: "0".to_string(),
            nonce: 0,
            gas_limit: 21_000,
            gas_price: Some("0".to_string()),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            transaction_type: Some(0),
            input: "0x".to_string(),
        }
    }
}
