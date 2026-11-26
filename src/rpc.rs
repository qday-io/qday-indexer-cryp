use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::{json, Map, Value};

use crate::addr::{normalize_optional_address, normalize_required_address};
use crate::block::{Block, ParsedBlock};
use crate::hexutil::{
    hex_quantity_to_decimal, hex_quantity_to_u64, normalize_data, normalize_hash,
};
use crate::logs::Log;
use crate::tx::Tx;

const ATTEMPTS: u32 = 5;

#[derive(Clone)]
pub struct Rpc {
    client: Client,
    url: String,
    batch_warning: Arc<AtomicBool>,
}

impl Rpc {
    pub fn new(url: &str) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(180))
            .user_agent(concat!("qday-index/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("build rpc client")?;
        Ok(Self {
            client,
            url: url.to_string(),
            batch_warning: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn block_number(&self) -> Result<u64> {
        let result = self.call("eth_blockNumber", json!([])).await?;
        let text = result
            .as_str()
            .context("eth_blockNumber result is not a string")?;
        hex_quantity_to_u64(text)
    }

    pub async fn chain_id(&self) -> Result<u64> {
        let result = self.call("eth_chainId", json!([])).await?;
        let text = result
            .as_str()
            .context("eth_chainId result is not a string")?;
        hex_quantity_to_u64(text)
    }

    pub async fn fetch_range(
        &self,
        start: u64,
        end: u64,
        concurrency: usize,
        rpc_batch: usize,
    ) -> Result<Vec<ParsedBlock>> {
        let chunks = rpc_chunks(start, end, rpc_batch);
        let total = chunks.iter().map(Vec::len).sum::<usize>();
        let progress = Arc::new(AtomicUsize::new(0));
        let mut parsed = Vec::new();
        let mut stream = stream::iter(chunks)
            .map(|chunk| {
                let rpc = self.clone();
                let progress = Arc::clone(&progress);
                async move {
                    let count = chunk.len();
                    let part = rpc.fetch_chunk(&chunk).await?;
                    let done = progress.fetch_add(count, Ordering::Relaxed) + count;
                    if done == total || done / 2_000 != (done - count) / 2_000 {
                        eprintln!("  fetched {done}/{total} blocks");
                    }
                    Ok::<Vec<ParsedBlock>, anyhow::Error>(part)
                }
            })
            .buffer_unordered(concurrency);

        while let Some(part) = stream.next().await {
            parsed.extend(part?);
        }
        Ok(parsed)
    }

    pub async fn fetch_logs(
        &self,
        start: u64,
        end: u64,
        concurrency: usize,
        rpc_batch: usize,
    ) -> Result<Vec<Log>> {
        let chunks = rpc_chunks(start, end, rpc_batch);
        let total = chunks.len();
        let progress = Arc::new(AtomicUsize::new(0));
        let mut logs = Vec::new();
        let mut stream = stream::iter(chunks)
            .map(|chunk| {
                let rpc = self.clone();
                let progress = Arc::clone(&progress);
                async move {
                    let part = rpc.fetch_logs_chunk(&chunk).await?;
                    let done = progress.fetch_add(1, Ordering::Relaxed) + 1;
                    if done == total || done / 50 != (done - 1) / 50 {
                        eprintln!("  fetched logs for {done}/{total} block ranges");
                    }
                    Ok::<Vec<Log>, anyhow::Error>(part)
                }
            })
            .buffer_unordered(concurrency);

        while let Some(part) = stream.next().await {
            logs.extend(part?);
        }
        Ok(logs)
    }

    async fn fetch_logs_chunk(&self, chunk: &[u64]) -> Result<Vec<Log>> {
        if chunk.is_empty() {
            return Ok(Vec::new());
        }
        let first = chunk[0];
        let last = chunk[chunk.len() - 1];
        match self.fetch_logs_range(first, last).await {
            Ok(logs) => Ok(logs),
            Err(err) if chunk.len() > 1 => {
                eprintln!(
                    "warning: eth_getLogs for blocks {first}-{last} failed ({err:#}); querying one block at a time"
                );
                let mut logs = Vec::new();
                for block in chunk {
                    logs.extend(self.fetch_logs_range(*block, *block).await?);
                }
                Ok(logs)
            }
            Err(err) => Err(err).with_context(|| format!("logs for blocks {first}-{last}")),
        }
    }

    async fn fetch_logs_range(&self, start: u64, end: u64) -> Result<Vec<Log>> {
        let result = self
            .call(
                "eth_getLogs",
                json!([{
                    "fromBlock": format!("0x{start:x}"),
                    "toBlock": format!("0x{end:x}"),
                }]),
            )
            .await?;
        parse_logs_result(&result).with_context(|| format!("eth_getLogs blocks {start}-{end}"))
    }

    async fn fetch_chunk(&self, chunk: &[u64]) -> Result<Vec<ParsedBlock>> {
        if chunk.is_empty() {
            return Ok(Vec::new());
        }
        match self.try_batch(chunk).await {
            Ok(txs) => Ok(txs),
            Err(err) if chunk.len() > 1 && !is_missing_block(&err) => {
                self.warn_batch(&err);
                self.fetch_singles(chunk).await
            }
            Err(err) if chunk.len() == 1 && is_batch_shape_error(&err) => {
                self.fetch_one(chunk[0]).await.map(|block| vec![block])
            }
            Err(err) => {
                let first = chunk[0];
                let last = chunk[chunk.len() - 1];
                Err(err).with_context(|| format!("blocks {first}-{last}"))
            }
        }
    }

    async fn fetch_singles(&self, chunk: &[u64]) -> Result<Vec<ParsedBlock>> {
        let mut parsed = Vec::new();
        for block in chunk {
            parsed.push(self.fetch_one(*block).await?);
        }
        Ok(parsed)
    }

    async fn fetch_one(&self, block: u64) -> Result<ParsedBlock> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{block:x}"), true],
        });
        let value = self.post(&body).await?;
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            bail!("block {block} rpc error: {error}");
        }
        let result = value
            .get("result")
            .with_context(|| format!("block {block} missing result"))?;
        parse_block(block, result)
    }

    async fn try_batch(&self, chunk: &[u64]) -> Result<Vec<ParsedBlock>> {
        let request = Value::Array(
            chunk
                .iter()
                .enumerate()
                .map(|(index, block)| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": index + 1,
                        "method": "eth_getBlockByNumber",
                        "params": [format!("0x{block:x}"), true],
                    })
                })
                .collect(),
        );
        let body = self.post(&request).await?;
        let items = body.as_array().with_context(|| {
            format!(
                "rpc batch response was not an array: {}",
                truncate(&body.to_string())
            )
        })?;
        let mut by_id = HashMap::new();
        for item in items {
            let id = parse_id(item.get("id")).context("rpc batch response missing id")?;
            by_id.insert(id, item);
        }
        let mut parsed = Vec::new();
        for (index, block) in chunk.iter().enumerate() {
            let item = by_id
                .get(&(index as u64 + 1))
                .with_context(|| format!("missing rpc response for block {block}"))?;
            if let Some(error) = item.get("error").filter(|error| !error.is_null()) {
                bail!("block {block} rpc error: {error}");
            }
            let result = item
                .get("result")
                .with_context(|| format!("block {block} missing result"))?;
            parsed.push(parse_block(*block, result)?);
        }
        Ok(parsed)
    }

    fn warn_batch(&self, err: &anyhow::Error) {
        if !self.batch_warning.swap(true, Ordering::Relaxed) {
            eprintln!("warning: JSON-RPC batching failed ({err:#}); requesting one block per call");
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let value = self.post(&body).await?;
        if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
            bail!("{method} rpc error: {error}");
        }
        value
            .get("result")
            .cloned()
            .with_context(|| format!("{method} missing result"))
    }

    async fn post(&self, body: &Value) -> Result<Value> {
        for attempt in 1..=ATTEMPTS {
            match self.post_once(body).await {
                Ok(value) => return Ok(value),
                Err(PostErr::Fatal(message)) => {
                    return Err(anyhow!(message)).context(format!("rpc {}", self.url));
                }
                Err(PostErr::Retry(message)) => {
                    if attempt == ATTEMPTS {
                        return Err(anyhow!(message))
                            .context(format!("rpc {} failed after {ATTEMPTS} attempts", self.url));
                    }
                    let shift = attempt.saturating_sub(1).min(5);
                    let delay = 500u64.saturating_mul(1u64 << shift);
                    eprintln!(
                        "warning: rpc attempt {attempt}/{ATTEMPTS} failed: {message}; retrying in {delay}ms"
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
        bail!("rpc {} failed after {ATTEMPTS} attempts", self.url)
    }

    async fn post_once(&self, body: &Value) -> std::result::Result<Value, PostErr> {
        let response = match self.client.post(&self.url).json(body).send().await {
            Ok(response) => response,
            Err(err) => return Err(PostErr::Retry(err.to_string())),
        };
        let status = response.status();
        let text = match response.text().await {
            Ok(text) => text,
            Err(err) => return Err(PostErr::Retry(err.to_string())),
        };
        if status.as_u16() == 429 || status.as_u16() == 408 || status.is_server_error() {
            return Err(PostErr::Retry(format!(
                "HTTP {status}: {}",
                truncate(&text)
            )));
        }
        if !status.is_success() {
            return Err(PostErr::Fatal(format!(
                "HTTP {status}: {}",
                truncate(&text)
            )));
        }
        let value = match serde_json::from_str::<Value>(&text) {
            Ok(value) => value,
            Err(err) => {
                return Err(PostErr::Fatal(format!(
                    "invalid JSON response: {err}: {}",
                    truncate(&text)
                )));
            }
        };
        if error_is_retryable(&value) {
            return Err(PostErr::Retry(format!(
                "rpc rate limited: {}",
                error_snippet(&value)
            )));
        }
        Ok(value)
    }
}

enum PostErr {
    Retry(String),
    Fatal(String),
}

pub fn parse_block(requested: u64, value: &Value) -> Result<ParsedBlock> {
    if value.is_null() {
        bail!("block {requested} not found; the RPC node does not have this block");
    }
    let obj = value
        .as_object()
        .with_context(|| format!("block {requested} is not an object"))?;
    let number = hex_quantity_to_u64(req_str(obj, "number")?)?;
    if number != requested {
        bail!("requested block {requested} but node returned {number}");
    }
    let block_hash = normalize_hash(req_str(obj, "hash")?)?;
    let transactions = obj
        .get("transactions")
        .with_context(|| format!("block {requested} missing transactions"))?
        .as_array()
        .with_context(|| format!("block {requested} transactions is not an array"))?;
    if transactions.iter().any(Value::is_string) {
        bail!("block {requested} did not include full transactions");
    }
    let mut txs = Vec::with_capacity(transactions.len());
    for (index, tx) in transactions.iter().enumerate() {
        txs.push(
            parse_tx(requested, &block_hash, tx)
                .with_context(|| format!("block {requested} transaction {index}"))?,
        );
    }
    let block = Block {
        block_number: number,
        block_hash: block_hash.clone(),
        parent_hash: normalize_hash(req_str(obj, "parentHash")?)?,
        timestamp: hex_quantity_to_u64(req_str(obj, "timestamp")?)?,
        miner: normalize_required_address(req_str(obj, "miner")?)?,
        gas_used: hex_quantity_to_u64(req_str(obj, "gasUsed")?)?,
        gas_limit: hex_quantity_to_u64(req_str(obj, "gasLimit")?)?,
        base_fee_per_gas: optional_decimal(obj, "baseFeePerGas")?,
        difficulty: hex_quantity_to_decimal(req_str(obj, "difficulty")?)?,
        total_difficulty: optional_decimal(obj, "totalDifficulty")?,
        size: hex_quantity_to_u64(req_str(obj, "size")?)?,
        transaction_count: u64::try_from(txs.len()).unwrap_or(u64::MAX),
        state_root: normalize_hash(req_str(obj, "stateRoot")?)?,
        transactions_root: normalize_hash(req_str(obj, "transactionsRoot")?)?,
        receipts_root: normalize_hash(req_str(obj, "receiptsRoot")?)?,
        extra_data: normalize_data(req_str(obj, "extraData")?),
        nonce: opt_str(obj, "nonce")?.map(normalize_data),
        mix_hash: optional_hash(obj, "mixHash")?,
        withdrawals_root: optional_hash(obj, "withdrawalsRoot")?,
        blob_gas_used: optional_u64(obj, "blobGasUsed")?,
        excess_blob_gas: optional_u64(obj, "excessBlobGas")?,
        parent_beacon_block_root: optional_hash(obj, "parentBeaconBlockRoot")?,
    };
    Ok(ParsedBlock { block, txs })
}

fn parse_tx(block_number: u64, block_hash: &str, value: &Value) -> Result<Tx> {
    let obj = value.as_object().context("transaction is not an object")?;
    let input = match (opt_str(obj, "input")?, opt_str(obj, "data")?) {
        (Some(raw), _) => normalize_data(raw),
        (None, Some(raw)) => normalize_data(raw),
        (None, None) => "0x".to_string(),
    };
    Ok(Tx {
        block_number,
        transaction_index: hex_quantity_to_u64(req_str(obj, "transactionIndex")?)?,
        transaction_hash: normalize_hash(req_str(obj, "hash")?)?,
        block_hash: block_hash.to_string(),
        from_address: normalize_required_address(req_str(obj, "from")?)?,
        to_address: normalize_optional_address(opt_str(obj, "to")?)?,
        value: hex_quantity_to_decimal(req_str(obj, "value")?)?,
        nonce: hex_quantity_to_u64(req_str(obj, "nonce")?)?,
        gas_limit: hex_quantity_to_u64(req_str(obj, "gas")?)?,
        gas_price: optional_decimal(obj, "gasPrice")?,
        max_fee_per_gas: optional_decimal(obj, "maxFeePerGas")?,
        max_priority_fee_per_gas: optional_decimal(obj, "maxPriorityFeePerGas")?,
        transaction_type: optional_u64(obj, "type")?,
        input,
    })
}

fn parse_logs_result(value: &Value) -> Result<Vec<Log>> {
    let items = value
        .as_array()
        .context("eth_getLogs result is not an array")?;
    items
        .iter()
        .enumerate()
        .map(|(index, item)| parse_log(item).with_context(|| format!("log {index}")))
        .collect()
}

fn parse_log(value: &Value) -> Result<Log> {
    let obj = value.as_object().context("log is not an object")?;
    let topics = obj
        .get("topics")
        .context("missing topics")?
        .as_array()
        .context("topics is not an array")?;
    let mut parsed_topics = Vec::with_capacity(topics.len());
    for topic in topics {
        let raw = topic.as_str().context("topic is not a string")?;
        parsed_topics.push(normalize_hash(raw)?);
    }
    Ok(Log {
        block_number: hex_quantity_to_u64(req_str(obj, "blockNumber")?)?,
        transaction_index: hex_quantity_to_u64(req_str(obj, "transactionIndex")?)?,
        log_index: hex_quantity_to_u64(req_str(obj, "logIndex")?)?,
        transaction_hash: normalize_hash(req_str(obj, "transactionHash")?)?,
        block_hash: normalize_hash(req_str(obj, "blockHash")?)?,
        address: normalize_required_address(req_str(obj, "address")?)?,
        topics: parsed_topics,
        data: normalize_data(req_str(obj, "data")?),
    })
}

fn optional_decimal(obj: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match opt_str(obj, key)? {
        Some(raw) if !raw.trim().is_empty() => Ok(Some(hex_quantity_to_decimal(raw)?)),
        _ => Ok(None),
    }
}

fn optional_hash(obj: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match opt_str(obj, key)? {
        Some(raw) if !raw.trim().is_empty() => Ok(Some(normalize_hash(raw)?)),
        _ => Ok(None),
    }
}

fn optional_u64(obj: &Map<String, Value>, key: &str) -> Result<Option<u64>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => Ok(Some(hex_quantity_to_u64(text)?)),
        Some(Value::Number(number)) => Ok(Some(
            number
                .as_u64()
                .with_context(|| format!("{key} is out of range"))?,
        )),
        Some(other) => bail!("{key} is not a hex quantity: {other}"),
    }
}

fn req_str<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    match obj.get(key) {
        Some(Value::String(text)) => Ok(text),
        Some(Value::Null) | None => bail!("missing {key}"),
        Some(other) => bail!("{key} is not a string: {other}"),
    }
}

fn opt_str<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(other) => bail!("{key} is not a string: {other}"),
    }
}

fn rpc_chunks(start: u64, end: u64, rpc_batch: usize) -> Vec<Vec<u64>> {
    let size = u64::try_from(rpc_batch).unwrap_or(1).max(1);
    let mut chunks = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        let chunk_end = cursor.saturating_add(size - 1).min(end);
        let mut chunk = Vec::new();
        let mut block = cursor;
        loop {
            chunk.push(block);
            if block == chunk_end {
                break;
            }
            block += 1;
        }
        chunks.push(chunk);
        if chunk_end == end {
            break;
        }
        cursor = chunk_end + 1;
    }
    chunks
}

fn parse_id(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.parse().ok(),
        _ => None,
    }
}

fn is_missing_block(err: &anyhow::Error) -> bool {
    format!("{err:#}")
        .to_ascii_lowercase()
        .contains("not found")
}

fn is_batch_shape_error(err: &anyhow::Error) -> bool {
    let message = format!("{err:#}").to_ascii_lowercase();
    message.contains("not an array") || message.contains("batch")
}

fn error_is_retryable(value: &Value) -> bool {
    if let Some(items) = value.as_array() {
        return items
            .iter()
            .any(|item| message_retryable(item.get("error")));
    }
    message_retryable(value.get("error"))
}

fn message_retryable(error: Option<&Value>) -> bool {
    let Some(error) = error else {
        return false;
    };
    if error.is_null() {
        return false;
    }
    let text = error.to_string().to_ascii_lowercase();
    text.contains("rate")
        || text.contains("too many")
        || text.contains("timeout")
        || text.contains("temporar")
        || text.contains("busy")
}

fn error_snippet(value: &Value) -> String {
    if let Some(items) = value.as_array() {
        for item in items {
            if let Some(error) = item.get("error").filter(|error| !error.is_null()) {
                return truncate(&error.to_string());
            }
        }
    }
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return truncate(&error.to_string());
    }
    truncate(&value.to_string())
}

fn truncate(text: &str) -> String {
    if text.len() <= 300 {
        return text.to_string();
    }
    let mut end = 300;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    #[test]
    fn parses_full_block_transactions() {
        let block = json!({
            "number": "0x10",
            "hash": format!("0x{:064x}", 4),
            "parentHash": format!("0x{:064x}", 3),
            "timestamp": "0x100",
            "miner": addr(0x55).to_uppercase(),
            "gasUsed": "0x5308",
            "gasLimit": "0x100000",
            "baseFeePerGas": "0x3b9aca00",
            "difficulty": "0x10",
            "size": "0x300",
            "stateRoot": format!("0x{:064x}", 5),
            "transactionsRoot": format!("0x{:064x}", 6),
            "receiptsRoot": format!("0x{:064x}", 7),
            "extraData": "0xAB",
            "transactions": [
                {
                    "hash": format!("0x{:064x}", 8),
                    "nonce": "0x1",
                    "blockHash": format!("0x{:064x}", 4),
                    "blockNumber": "0x10",
                    "transactionIndex": "0x0",
                    "from": addr(0x11).to_uppercase(),
                    "to": addr(0x22),
                    "value": "0xde0b6b3a7640000",
                    "gas": "0x5208",
                    "gasPrice": "0x3b9aca00",
                    "input": "0xAb",
                    "type": "0x2",
                    "maxFeePerGas": "0x3b9aca00",
                    "maxPriorityFeePerGas": "0x1"
                },
                {
                    "hash": format!("0x{:064x}", 9),
                    "nonce": "0x2",
                    "transactionIndex": "0x1",
                    "from": addr(0x11),
                    "to": null,
                    "value": "0x0",
                    "gas": "0x100",
                    "input": "0x"
                }
            ]
        });
        let parsed = parse_block(16, &block).unwrap();
        let txs = parsed.txs;
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].from_address, addr(0x11));
        assert_eq!(txs[0].to_address.as_deref(), Some(addr(0x22).as_str()));
        assert_eq!(txs[0].value, "1000000000000000000");
        assert_eq!(txs[0].gas_limit, 21_000);
        assert_eq!(txs[0].gas_price.as_deref(), Some("1000000000"));
        assert_eq!(txs[0].transaction_type, Some(2));
        assert_eq!(txs[0].input, "0xab");
        assert_eq!(txs[1].to_address, None);
        assert_eq!(txs[1].gas_price, None);
        assert_eq!(txs[1].block_hash, format!("0x{:064x}", 4));

        let block = parsed.block;
        assert_eq!(block.block_number, 16);
        assert_eq!(block.block_hash, format!("0x{:064x}", 4));
        assert_eq!(block.parent_hash, format!("0x{:064x}", 3));
        assert_eq!(block.timestamp, 0x100);
        assert_eq!(block.miner, addr(0x55));
        assert_eq!(block.gas_used, 0x5308);
        assert_eq!(block.gas_limit, 0x100000);
        assert_eq!(block.base_fee_per_gas.as_deref(), Some("1000000000"));
        assert_eq!(block.difficulty, "16");
        assert_eq!(block.total_difficulty, None);
        assert_eq!(block.size, 0x300);
        assert_eq!(block.transaction_count, 2);
        assert_eq!(block.state_root, format!("0x{:064x}", 5));
        assert_eq!(block.transactions_root, format!("0x{:064x}", 6));
        assert_eq!(block.receipts_root, format!("0x{:064x}", 7));
        assert_eq!(block.extra_data, "0xab");
        assert_eq!(block.nonce, None);
        assert_eq!(block.withdrawals_root, None);
        assert_eq!(block.blob_gas_used, None);
    }

    #[test]
    fn parses_get_logs_results() {
        let value = json!([
            {
                "address": addr(0x66).to_uppercase(),
                "topics": [format!("0x{:064x}", 1), format!("0x{:064x}", 2)],
                "data": "0xABCD",
                "blockNumber": "0x10",
                "transactionHash": format!("0x{:064x}", 9),
                "transactionIndex": "0x1",
                "blockHash": format!("0x{:064x}", 4),
                "logIndex": "0x2",
                "removed": false
            },
            {
                "address": addr(0x77),
                "topics": [],
                "data": "0x",
                "blockNumber": "0x10",
                "transactionHash": format!("0x{:064x}", 10),
                "transactionIndex": "0x2",
                "blockHash": format!("0x{:064x}", 4),
                "logIndex": "0x3"
            }
        ]);
        let logs = parse_logs_result(&value).unwrap();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].block_number, 16);
        assert_eq!(logs[0].transaction_index, 1);
        assert_eq!(logs[0].log_index, 2);
        assert_eq!(logs[0].address, addr(0x66));
        assert_eq!(
            logs[0].topics,
            vec![format!("0x{:064x}", 1), format!("0x{:064x}", 2)]
        );
        assert_eq!(logs[0].data, "0xabcd");
        assert_eq!(logs[0].transaction_hash, format!("0x{:064x}", 9));
        assert_eq!(logs[1].topics.len(), 0);
        assert_eq!(logs[1].data, "0x");
    }

    #[test]
    fn missing_block_is_an_error() {
        let err = parse_block(3, &Value::Null).unwrap_err();
        assert!(is_missing_block(&err));
    }
}
