use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use crate::block::{Block, ParsedBlock};
use crate::cli::Cli;
use crate::rpc::Rpc;
use crate::store::{
    batch_path, block_batch_path, count_jsonl_lines, count_txs, logs_batch_path,
    prepare_follow_dir, prepare_raw_dir, write_blocks, write_logs, write_txs,
};
use crate::tx::Tx;

const MAX_BATCH: u64 = 2_000_000;
const BATCH_PAUSE_MIN: Duration = Duration::from_secs(2);
const BATCH_PAUSE_JITTER_MS: u64 = 1_000;

/// Fetch the block range and write raw transaction files.
pub async fn run(cli: Cli) -> Result<()> {
    validate(&cli)?;
    if cli.batch > 100_000 {
        eprintln!(
            "warning: --batch {} is large; lower it if the RPC or disk is tight",
            cli.batch
        );
    }

    let rpc = Rpc::new(cli.rpc_url.trim())?;
    match cli.to_block {
        Some(to_block) => run_range(&cli, &rpc, to_block).await,
        None => run_follow(&cli, &rpc).await,
    }
}

async fn run_range(cli: &Cli, rpc: &Rpc, to_block: u64) -> Result<()> {
    let head = rpc.block_number().await?;
    eprintln!("chain head: {head}");
    let confirmed = require_confirmed(head, cli.confirm)?;
    if to_block > confirmed {
        if cli.confirm == 0 {
            bail!("--to-block ({to_block}) is greater than chain head {head}");
        }
        bail!(
            "--to-block ({to_block}) is greater than confirmed head {confirmed} (chain head {head}, --confirm {})",
            cli.confirm
        );
    }
    log_chain_id(rpc).await;

    let chunks = block_chunks(cli.from_block, to_block, cli.batch);
    let paths = prepare_raw_dir(&cli.raw_dir, &chunks, cli.resume)?;
    let (total_txs, total_blocks, total_logs) = write_chunks(cli, rpc, &chunks, &paths).await?;
    println!(
        "Extraction complete | raw txs: {total_txs} | blocks: {total_blocks} | logs: {total_logs}"
    );
    println!("raw dir: {}", cli.raw_dir.display());
    Ok(())
}

async fn run_follow(cli: &Cli, rpc: &Rpc) -> Result<()> {
    prepare_follow_dir(&cli.raw_dir, cli.resume)?;
    log_chain_id(rpc).await;
    eprintln!(
        "following from block {}, staying {} blocks behind the chain head",
        cli.from_block, cli.confirm
    );

    let mut cursor = cli.from_block;
    let mut total_txs = 0u64;
    let mut total_blocks = 0u64;
    let mut total_logs = 0u64;
    let mut fetched_a_batch = false;
    let mut seen_head: Option<u64> = None;
    let mut waiting_for: Option<u64> = None;

    loop {
        let head = rpc.block_number().await?;
        if seen_head != Some(head) {
            eprintln!("chain head: {head}");
            seen_head = Some(head);
        }
        let Some(confirmed) = confirmed_tip(head, cli.confirm) else {
            if waiting_for != Some(head) {
                eprintln!(
                    "chain head {head} is inside the --confirm {} window; waiting",
                    cli.confirm
                );
                waiting_for = Some(head);
            }
            tokio::time::sleep(batch_pause()).await;
            continue;
        };
        if cursor > confirmed {
            if waiting_for != Some(confirmed) {
                eprintln!(
                    "confirmed tip {confirmed} (chain head {head}); waiting for block {cursor}"
                );
                waiting_for = Some(confirmed);
            }
            tokio::time::sleep(batch_pause()).await;
            continue;
        }
        waiting_for = None;

        let end = cursor.saturating_add(cli.batch - 1).min(confirmed);
        let tx_path = batch_path(&cli.raw_dir, cursor, end);
        let block_path = block_batch_path(&cli.raw_dir, cursor, end);
        let logs_path = logs_batch_path(&cli.raw_dir, cursor, end);
        if cli.resume && tx_path.is_file() && block_path.is_file() && logs_path.is_file() {
            let count = count_txs(&tx_path)?;
            total_txs += count;
            total_blocks += end - cursor + 1;
            total_logs += count_jsonl_lines(&logs_path)?;
            eprintln!(
                "blocks {cursor}-{end}: reused {} ({count} txs, {total_txs} total)",
                tx_path.display()
            );
            cursor = end.saturating_add(1);
            continue;
        }
        if fetched_a_batch {
            let pause = batch_pause();
            eprintln!("waiting {:.1}s before the next batch", pause.as_secs_f64());
            tokio::time::sleep(pause).await;
        }
        let started = Instant::now();
        eprintln!("fetching blocks {cursor}-{end}");
        let parsed = rpc
            .fetch_range(cursor, end, cli.concurrency, cli.rpc_batch)
            .await
            .with_context(|| format!("fetch blocks {cursor}-{end}"))?;
        let logs = rpc
            .fetch_logs(cursor, end, cli.concurrency, cli.rpc_batch)
            .await
            .with_context(|| format!("fetch logs {cursor}-{end}"))?;
        let (blocks, txs) = split_parsed(parsed);
        write_txs(&tx_path, &txs)?;
        write_blocks(&block_path, &blocks)?;
        write_logs(&logs_path, &logs)?;
        total_txs += u64::try_from(txs.len()).unwrap_or(u64::MAX);
        total_blocks += u64::try_from(blocks.len()).unwrap_or(u64::MAX);
        total_logs += u64::try_from(logs.len()).unwrap_or(u64::MAX);
        eprintln!(
            "blocks {cursor}-{end}: wrote {} txs, {} blocks, {} logs to {} in {:.1}s ({total_txs} txs, {total_blocks} blocks, {total_logs} logs total)",
            txs.len(),
            blocks.len(),
            logs.len(),
            tx_path.display(),
            started.elapsed().as_secs_f64()
        );
        fetched_a_batch = true;
        cursor = end.saturating_add(1);
    }
}

async fn write_chunks(
    cli: &Cli,
    rpc: &Rpc,
    chunks: &[(u64, u64)],
    paths: &[(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)],
) -> Result<(u64, u64, u64)> {
    let mut total_txs = 0u64;
    let mut total_blocks = 0u64;
    let mut total_logs = 0u64;
    let mut fetched_a_batch = false;
    for (index, ((start, end), (tx_path, block_path, logs_path))) in
        chunks.iter().zip(paths).enumerate()
    {
        if cli.resume && tx_path.is_file() && block_path.is_file() && logs_path.is_file() {
            let count = count_txs(tx_path)?;
            total_txs += count;
            total_blocks += end - start + 1;
            total_logs += count_jsonl_lines(logs_path)?;
            eprintln!(
                "batch {}/{} blocks {start}-{end}: reused {} ({count} txs)",
                index + 1,
                chunks.len(),
                tx_path.display()
            );
            continue;
        }
        if fetched_a_batch {
            let pause = batch_pause();
            eprintln!("waiting {:.1}s before the next batch", pause.as_secs_f64());
            tokio::time::sleep(pause).await;
        }
        let started = Instant::now();
        eprintln!(
            "batch {}/{} fetching blocks {start}-{end}",
            index + 1,
            chunks.len()
        );
        let parsed = rpc
            .fetch_range(*start, *end, cli.concurrency, cli.rpc_batch)
            .await
            .with_context(|| format!("fetch blocks {start}-{end}"))?;
        let logs = rpc
            .fetch_logs(*start, *end, cli.concurrency, cli.rpc_batch)
            .await
            .with_context(|| format!("fetch logs {start}-{end}"))?;
        let (blocks, txs) = split_parsed(parsed);
        write_txs(tx_path, &txs)?;
        write_blocks(block_path, &blocks)?;
        write_logs(logs_path, &logs)?;
        total_txs += u64::try_from(txs.len()).unwrap_or(u64::MAX);
        total_blocks += u64::try_from(blocks.len()).unwrap_or(u64::MAX);
        total_logs += u64::try_from(logs.len()).unwrap_or(u64::MAX);
        eprintln!(
            "batch {}/{} blocks {start}-{end}: wrote {} txs, {} blocks, {} logs to {} in {:.1}s",
            index + 1,
            chunks.len(),
            txs.len(),
            blocks.len(),
            logs.len(),
            tx_path.display(),
            started.elapsed().as_secs_f64()
        );
        fetched_a_batch = true;
    }
    Ok((total_txs, total_blocks, total_logs))
}

fn split_parsed(parsed: Vec<ParsedBlock>) -> (Vec<Block>, Vec<Tx>) {
    let mut blocks = Vec::with_capacity(parsed.len());
    let mut txs = Vec::new();
    for item in parsed {
        blocks.push(item.block);
        txs.extend(item.txs);
    }
    (blocks, txs)
}

async fn log_chain_id(rpc: &Rpc) {
    match rpc.chain_id().await {
        Ok(chain_id) => eprintln!("chain id: {chain_id}"),
        Err(err) => eprintln!("warning: eth_chainId failed: {err:#}"),
    }
}

fn require_confirmed(head: u64, confirm: u64) -> Result<u64> {
    confirmed_tip(head, confirm).ok_or_else(|| {
        anyhow::anyhow!("chain head {head} is inside the --confirm {confirm} window")
    })
}

fn confirmed_tip(head: u64, confirm: u64) -> Option<u64> {
    head.checked_sub(confirm)
}

fn validate(cli: &Cli) -> Result<()> {
    if let Some(to_block) = cli.to_block {
        if cli.from_block > to_block {
            bail!(
                "--from-block ({}) is greater than --to-block ({to_block})",
                cli.from_block
            );
        }
    }
    if cli.batch == 0 {
        bail!("--batch must be >= 1");
    }
    if cli.batch > MAX_BATCH {
        bail!(
            "--batch {} is too large; use a value up to {MAX_BATCH}",
            cli.batch
        );
    }
    if cli.concurrency == 0 || cli.concurrency > 1_024 {
        bail!("--concurrency must be between 1 and 1024");
    }
    if cli.rpc_batch == 0 || cli.rpc_batch > 500 {
        bail!("--rpc-batch must be between 1 and 500");
    }
    let url = cli.rpc_url.trim().to_ascii_lowercase();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        bail!("--rpc-url must start with http:// or https://");
    }
    Ok(())
}

fn batch_pause() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(0);
    let span = u32::try_from(BATCH_PAUSE_JITTER_MS + 1).unwrap_or(1);
    BATCH_PAUSE_MIN + Duration::from_millis(u64::from(nanos % span))
}

fn block_chunks(from: u64, to: u64, batch: u64) -> Vec<(u64, u64)> {
    let mut chunks = Vec::new();
    let mut start = from;
    loop {
        let end = start.saturating_add(batch - 1).min(to);
        chunks.push((start, end));
        if end == to {
            break;
        }
        start = end + 1;
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use crate::logs::Log;
    use crate::store::TxReader;
    use crate::tx::Tx;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn addr(byte: u8) -> String {
        format!("0x{}", format!("{byte:02x}").repeat(20))
    }

    #[test]
    fn pause_stays_between_two_and_three_seconds() {
        for _ in 0..8 {
            let pause = batch_pause();
            assert!(pause >= Duration::from_secs(2));
            assert!(pause <= Duration::from_secs(3));
        }
    }

    #[test]
    fn confirmation_depth_trims_the_head() {
        assert_eq!(confirmed_tip(100, 0), Some(100));
        assert_eq!(confirmed_tip(100, 12), Some(88));
        assert_eq!(confirmed_tip(5, 12), None);
    }

    #[test]
    fn splits_inclusive_ranges() {
        assert_eq!(block_chunks(10, 20, 5), vec![(10, 14), (15, 19), (20, 20)]);
        assert_eq!(block_chunks(10, 19, 5), vec![(10, 14), (15, 19)]);
        assert_eq!(block_chunks(10, 10, 5), vec![(10, 10)]);
        assert_eq!(
            block_chunks(u64::MAX - 5, u64::MAX, 10),
            vec![(u64::MAX - 5, u64::MAX)]
        );
    }

    #[tokio::test]
    async fn end_to_end_against_mock_rpc() {
        let a = addr(0x11);
        let c = addr(0x22);
        let d = addr(0x33);
        let e = addr(0x44);
        let other = addr(0x55);
        let blocks = HashMap::from([
            (
                1,
                block(
                    1,
                    vec![
                        rpc_tx(1, 0, &c, Some(&e)),
                        rpc_tx(1, 1, &a, Some(&c)),
                        rpc_tx(1, 2, &c, Some(&d)),
                    ],
                ),
            ),
            (
                2,
                block(
                    2,
                    vec![rpc_tx(2, 0, &c, Some(&d)), rpc_tx(2, 1, &other, Some(&a))],
                ),
            ),
        ]);
        let hits = Arc::new(AtomicUsize::new(0));
        let logs = HashMap::from([
            (1, vec![rpc_log(1, 1, 0, &c)]),
            (2, vec![rpc_log(2, 0, 0, &d), rpc_log(2, 0, 1, &d)]),
        ]);
        let addr = serve_with_logs(blocks, logs, Arc::clone(&hits)).await;

        let dir = tempfile::tempdir().unwrap();
        let cli = Cli {
            from_block: 1,
            to_block: Some(2),
            confirm: 0,
            batch: 1,
            rpc_url: format!("http://{addr}"),
            raw_dir: dir.path().join("raw"),
            concurrency: 2,
            rpc_batch: 1,
            resume: false,
        };

        tokio::time::timeout(Duration::from_secs(30), run(cli))
            .await
            .expect("pipeline timed out")
            .unwrap();

        let raw = dir.path().join("raw");
        let batch1 = read_txs(&raw.join("batch_1_1.jsonl"));
        let batch2 = read_txs(&raw.join("batch_2_2.jsonl"));
        assert_eq!(
            batch1
                .iter()
                .map(|tx| (tx.transaction_index, tx.from_address.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, c.as_str()), (1, a.as_str()), (2, c.as_str())]
        );
        assert_eq!(
            batch2
                .iter()
                .map(|tx| (tx.transaction_index, tx.from_address.as_str()))
                .collect::<Vec<_>>(),
            vec![(0, c.as_str()), (1, other.as_str())]
        );
        let blocks1 = read_blocks(&raw.join("blocks_1_1.jsonl"));
        let blocks2 = read_blocks(&raw.join("blocks_2_2.jsonl"));
        assert_eq!(blocks1.len(), 1);
        assert_eq!(blocks1[0].block_number, 1);
        assert_eq!(blocks1[0].transaction_count, 3);
        assert_eq!(blocks2.len(), 1);
        assert_eq!(blocks2[0].block_number, 2);
        assert_eq!(blocks2[0].transaction_count, 2);
        let logs1 = read_logs(&raw.join("logs_1_1.jsonl"));
        let logs2 = read_logs(&raw.join("logs_2_2.jsonl"));
        assert_eq!(logs1.len(), 1);
        assert_eq!(logs1[0].block_number, 1);
        assert_eq!(logs1[0].transaction_index, 1);
        assert_eq!(logs1[0].address, c);
        assert_eq!(logs2.len(), 2);
        assert_eq!(logs2[0].log_index, 0);
        assert_eq!(logs2[1].log_index, 1);
        assert_eq!(logs2[1].address, d);

        let after_first = hits.load(Ordering::Relaxed);
        assert!(
            after_first >= 4,
            "expected block fetches, got {after_first}"
        );

        let cli = Cli {
            from_block: 1,
            to_block: Some(2),
            confirm: 0,
            batch: 1,
            rpc_url: format!("http://{addr}"),
            raw_dir: dir.path().join("raw"),
            concurrency: 2,
            rpc_batch: 1,
            resume: true,
        };
        tokio::time::timeout(Duration::from_secs(30), run(cli))
            .await
            .expect("resume timed out")
            .unwrap();
        let extra = hits.load(Ordering::Relaxed) - after_first;
        assert_eq!(extra, 2, "resume should only query head and chain id");
    }

    #[tokio::test]
    async fn follow_stays_behind_the_chain_head() {
        let blocks = HashMap::from([
            (1, block(1, vec![rpc_tx(1, 0, &addr(0x11), Some(&addr(0x22)))])),
            (2, block(2, vec![rpc_tx(2, 0, &addr(0x22), Some(&addr(0x33)))])),
        ]);
        let hits = Arc::new(AtomicUsize::new(0));
        let addr = serve(blocks, Arc::clone(&hits)).await;
        let dir = tempfile::tempdir().unwrap();
        let raw = dir.path().join("raw");
        let cli = Cli {
            from_block: 1,
            to_block: None,
            confirm: 1,
            batch: 1,
            rpc_url: format!("http://{addr}"),
            raw_dir: raw.clone(),
            concurrency: 1,
            rpc_batch: 1,
            resume: false,
        };
        let task = tokio::spawn(async move { run(cli).await });
        let logs1_path = raw.join("logs_1_1.jsonl");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !logs1_path.is_file() {
            if task.is_finished() {
                panic!("follow exited early: {:?}", task.await);
            }
            if tokio::time::Instant::now() > deadline {
                panic!("timed out waiting for the confirmed block");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(read_txs(&raw.join("batch_1_1.jsonl")).len(), 1);
        let blocks1 = read_blocks(&raw.join("blocks_1_1.jsonl"));
        assert_eq!(blocks1.len(), 1);
        assert_eq!(blocks1[0].block_number, 1);
        assert_eq!(read_logs(&logs1_path).len(), 0);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !raw.join("batch_2_2.jsonl").exists(),
            "block 2 is the chain head and --confirm 1 must leave it unindexed"
        );
        assert!(!raw.join("blocks_2_2.jsonl").exists());
        assert!(!raw.join("logs_2_2.jsonl").exists());
        task.abort();
    }

    #[tokio::test]
    async fn bounded_range_rejects_unconfirmed_tip() {
        let hits = Arc::new(AtomicUsize::new(0));
        let addr = serve(HashMap::new(), hits).await;
        let dir = tempfile::tempdir().unwrap();
        let err = run(Cli {
            from_block: 1,
            to_block: Some(2),
            confirm: 1,
            batch: 1,
            rpc_url: format!("http://{addr}"),
            raw_dir: dir.path().join("raw"),
            concurrency: 1,
            rpc_batch: 1,
            resume: false,
        })
        .await
        .unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("confirmed head 1"),
            "unexpected error: {message}"
        );
    }

    fn read_txs(path: &std::path::Path) -> Vec<Tx> {
        TxReader::open(&[path.to_path_buf()])
            .unwrap()
            .map(|item| item.unwrap())
            .collect()
    }

    fn read_blocks(path: &std::path::Path) -> Vec<Block> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn read_logs(path: &std::path::Path) -> Vec<Log> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn rpc_log(block_number: u64, tx_index: u64, log_index: u64, address: &str) -> Value {
        json!({
            "address": address,
            "topics": [format!("0x{:064x}", block_number * 10 + log_index)],
            "data": "0x",
            "blockNumber": format!("0x{block_number:x}"),
            "transactionHash": format!("0x{:064x}", block_number * 100 + tx_index + 1),
            "transactionIndex": format!("0x{tx_index:x}"),
            "blockHash": format!("0x{:064x}", block_number + 1),
            "logIndex": format!("0x{log_index:x}"),
            "removed": false,
        })
    }

    fn block(number: u64, txs: Vec<Value>) -> Value {
        json!({
            "number": format!("0x{number:x}"),
            "hash": format!("0x{:064x}", number + 1),
            "parentHash": format!("0x{:064x}", number),
            "timestamp": format!("0x{number:x}"),
            "miner": addr(0x99),
            "gasUsed": "0x5208",
            "gasLimit": "0x100000",
            "difficulty": "0x0",
            "size": "0x200",
            "stateRoot": format!("0x{:064x}", number + 2),
            "transactionsRoot": format!("0x{:064x}", number + 3),
            "receiptsRoot": format!("0x{:064x}", number + 4),
            "extraData": "0x",
            "transactions": txs,
        })
    }

    fn rpc_tx(block_number: u64, index: u64, from: &str, to: Option<&str>) -> Value {
        json!({
            "hash": format!("0x{:064x}", block_number * 100 + index + 1),
            "nonce": "0x0",
            "blockHash": format!("0x{:064x}", block_number + 1),
            "blockNumber": format!("0x{block_number:x}"),
            "transactionIndex": format!("0x{index:x}"),
            "from": from,
            "to": to,
            "value": "0x1",
            "gas": "0x5208",
            "gasPrice": "0x1",
            "input": "0x",
            "type": "0x0",
        })
    }

    async fn serve(blocks: HashMap<u64, Value>, hits: Arc<AtomicUsize>) -> std::net::SocketAddr {
        serve_with_logs(blocks, HashMap::new(), hits).await
    }

    async fn serve_with_logs(
        blocks: HashMap<u64, Value>,
        logs: HashMap<u64, Vec<Value>>,
        hits: Arc<AtomicUsize>,
    ) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let blocks = Arc::new(blocks);
        let logs = Arc::new(logs);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let blocks = Arc::clone(&blocks);
                let logs = Arc::clone(&logs);
                let hits = Arc::clone(&hits);
                tokio::spawn(async move {
                    let _ = handle_conn(socket, blocks, logs, hits).await;
                });
            }
        });
        addr
    }

    async fn handle_conn(
        mut socket: TcpStream,
        blocks: Arc<HashMap<u64, Value>>,
        logs: Arc<HashMap<u64, Vec<Value>>>,
        hits: Arc<AtomicUsize>,
    ) -> Result<()> {
        let body = read_http_body(&mut socket).await?;
        let response = dispatch(&body, &blocks, &logs, &hits);
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.len()
        );
        socket.write_all(header.as_bytes()).await?;
        socket.write_all(response.as_bytes()).await?;
        socket.shutdown().await?;
        Ok(())
    }

    async fn read_http_body(socket: &mut TcpStream) -> Result<String> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let read = socket.read(&mut tmp).await?;
            if read == 0 {
                bail!("client closed before the request finished");
            }
            buf.extend_from_slice(&tmp[..read]);
            let Some(header_end) = find_header_end(&buf) else {
                if buf.len() > 2_000_000 {
                    bail!("request headers too large");
                }
                continue;
            };
            let headers = std::str::from_utf8(&buf[..header_end]).context("request headers")?;
            let length = content_length(headers);
            let body_start = header_end + 4;
            while buf.len() < body_start + length {
                let read = socket.read(&mut tmp).await?;
                if read == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..read]);
            }
            if buf.len() < body_start + length {
                bail!("truncated request body");
            }
            let body = &buf[body_start..body_start + length];
            return String::from_utf8(body.to_vec()).context("request body");
        }
    }

    fn find_header_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn content_length(headers: &str) -> usize {
        headers
            .split("\r\n")
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())?
            })
            .unwrap_or(0)
    }

    fn dispatch(
        body: &str,
        blocks: &HashMap<u64, Value>,
        logs: &HashMap<u64, Vec<Value>>,
        hits: &AtomicUsize,
    ) -> String {
        hits.fetch_add(1, Ordering::Relaxed);
        let value: Value = match serde_json::from_str(body) {
            Ok(value) => value,
            Err(err) => {
                return json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": -32700, "message": err.to_string()}
                })
                .to_string();
            }
        };
        if let Some(items) = value.as_array() {
            let out: Vec<Value> = items
                .iter()
                .map(|item| dispatch_one(item, blocks, logs))
                .collect();
            Value::Array(out).to_string()
        } else {
            dispatch_one(&value, blocks, logs).to_string()
        }
    }

    fn dispatch_one(
        item: &Value,
        blocks: &HashMap<u64, Value>,
        logs: &HashMap<u64, Vec<Value>>,
    ) -> Value {
        let id = item.get("id").cloned().unwrap_or_else(|| json!(1));
        let method = item.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_blockNumber" => json!("0x2"),
            "eth_chainId" => json!("0x1"),
            "eth_getBlockByNumber" => {
                let number = hex_param(item, 0);
                blocks.get(&number).cloned().unwrap_or(Value::Null)
            }
            "eth_getLogs" => {
                let filter = item
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let from = filter
                    .get("fromBlock")
                    .and_then(Value::as_str)
                    .map(parse_hex)
                    .unwrap_or(0);
                let to = filter
                    .get("toBlock")
                    .and_then(Value::as_str)
                    .map(parse_hex)
                    .unwrap_or(u64::MAX);
                Value::Array(
                    logs.iter()
                        .filter(|(block, _)| **block >= from && **block <= to)
                        .flat_map(|(_, entries)| entries.clone())
                        .collect(),
                )
            }
            _ => {
                return json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {"code": -32601, "message": "method not found"}
                });
            }
        };
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    }

    fn hex_param(item: &Value, index: usize) -> u64 {
        item.get("params")
            .and_then(Value::as_array)
            .and_then(|params| params.get(index))
            .and_then(Value::as_str)
            .map(parse_hex)
            .unwrap_or(0)
    }

    fn parse_hex(hex: &str) -> u64 {
        u64::from_str_radix(hex.trim_start_matches("0x"), 16).unwrap_or(0)
    }
}
