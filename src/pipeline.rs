use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

use crate::cli::Cli;
use crate::hop::collect_hops;
use crate::output::write_csv;
use crate::rpc::Rpc;
use crate::seed::load_seeds;
use crate::store::{count_txs, prepare_raw_dir, write_txs, TxReader};

const MAX_BATCH: u64 = 2_000_000;
const BATCH_PAUSE_MIN: Duration = Duration::from_secs(2);
const BATCH_PAUSE_JITTER_MS: u64 = 1_000;

/// Fetch the block range and write Hop 1 / Hop 2 transaction files.
pub async fn run(cli: Cli) -> Result<()> {
    validate(&cli)?;
    if cli.batch > 100_000 {
        eprintln!(
            "warning: --batch {} is large; lower it if the RPC or disk is tight",
            cli.batch
        );
    }

    let seeds = load_seeds(&cli.config)?;
    eprintln!(
        "loaded {} unique seed addresses from {}",
        seeds.len(),
        cli.config.display()
    );

    let chunks = block_chunks(cli.from_block, cli.to_block, cli.batch);
    let paths = prepare_raw_dir(&cli.raw_dir, &chunks, cli.resume)?;
    let rpc = Rpc::new(cli.rpc_url.trim())?;
    let head = rpc.block_number().await?;
    eprintln!("chain head: {head}");
    if cli.to_block > head {
        bail!(
            "--to-block ({}) is greater than chain head {head}",
            cli.to_block
        );
    }
    match rpc.chain_id().await {
        Ok(chain_id) => eprintln!("chain id: {chain_id}"),
        Err(err) => eprintln!("warning: eth_chainId failed: {err:#}"),
    }

    let mut fetched_a_batch = false;
    for (index, ((start, end), path)) in chunks.iter().zip(&paths).enumerate() {
        if cli.resume && path.is_file() {
            let count = count_txs(path)?;
            eprintln!(
                "batch {}/{} blocks {start}-{end}: reused {} ({count} txs)",
                index + 1,
                chunks.len(),
                path.display()
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
        let txs = rpc
            .fetch_range(*start, *end, cli.concurrency, cli.rpc_batch)
            .await
            .with_context(|| format!("fetch blocks {start}-{end}"))?;
        write_txs(path, &txs)?;
        eprintln!(
            "batch {}/{} blocks {start}-{end}: wrote {} txs to {} in {:.1}s",
            index + 1,
            chunks.len(),
            txs.len(),
            path.display(),
            started.elapsed().as_secs_f64()
        );
        fetched_a_batch = true;
    }

    eprintln!("filtering hop 1 and hop 2");
    let hops = collect_hops(
        || TxReader::open(&paths),
        &seeds,
        cli.from_block,
        cli.to_block,
        cli.exclude_seeds_from_hop2,
    )?;
    let hop1_count = hops.hop1.len();
    let hop2_count = hops.hop2.len();
    let recipients = hops.recipients;
    let scanned = hops.scanned;
    let rows = hops.into_rows();
    write_csv(&cli.output, &rows)?;

    println!(
        "Extraction complete | Hop 1 txs: {hop1_count} | Unique recipients: {recipients} | Hop 2 txs: {hop2_count}"
    );
    println!("scanned raw txs: {scanned}");
    println!("csv: {}", cli.output.display());
    Ok(())
}

fn validate(cli: &Cli) -> Result<()> {
    if cli.from_block > cli.to_block {
        bail!(
            "--from-block ({}) is greater than --to-block ({})",
            cli.from_block,
            cli.to_block
        );
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
    use crate::hop::collect_hops;
    use crate::store::{write_txs, TxReader};
    use crate::tx::Tx;
    use std::collections::{HashMap, HashSet};
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
    fn splits_inclusive_ranges() {
        assert_eq!(block_chunks(10, 20, 5), vec![(10, 14), (15, 19), (20, 20)]);
        assert_eq!(block_chunks(10, 19, 5), vec![(10, 14), (15, 19)]);
        assert_eq!(block_chunks(10, 10, 5), vec![(10, 10)]);
        assert_eq!(
            block_chunks(u64::MAX - 5, u64::MAX, 10),
            vec![(u64::MAX - 5, u64::MAX)]
        );
    }

    #[test]
    fn hop2_spans_batch_files() {
        let dir = tempfile::tempdir().unwrap();
        let a = addr(0x11);
        let c = addr(0x22);
        let d = addr(0x33);
        let first = dir.path().join("batch_1_1.jsonl");
        let second = dir.path().join("batch_2_2.jsonl");
        write_txs(&first, &[Tx::sample(1, 0, &a, Some(&c))]).unwrap();
        write_txs(&second, &[Tx::sample(2, 0, &c, Some(&d))]).unwrap();
        let paths = vec![first, second];
        let seeds = HashSet::from([a]);
        let out = collect_hops(|| TxReader::open(&paths), &seeds, 1, 2, false).unwrap();
        assert_eq!(out.hop1.len(), 1);
        assert_eq!(out.hop2.len(), 1);
        assert_eq!(out.recipients, 1);
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
        let addr = serve(blocks, Arc::clone(&hits)).await;

        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("seeds.txt");
        std::fs::write(&config, a.to_uppercase()).unwrap();
        let cli = Cli {
            config,
            from_block: 1,
            to_block: 2,
            batch: 1,
            rpc_url: format!("http://{addr}"),
            raw_dir: dir.path().join("raw"),
            output: dir.path().join("out.csv"),
            concurrency: 2,
            rpc_batch: 1,
            exclude_seeds_from_hop2: false,
            resume: false,
        };

        tokio::time::timeout(Duration::from_secs(30), run(cli))
            .await
            .expect("pipeline timed out")
            .unwrap();

        let csv_path = dir.path().join("out.csv");
        let mut reader = csv::Reader::from_path(&csv_path).unwrap();
        let rows: Vec<(String, u64, u64, String)> = reader
            .records()
            .map(|record| {
                let record = record.unwrap();
                (
                    record.get(0).unwrap().to_string(),
                    record.get(1).unwrap().parse().unwrap(),
                    record.get(2).unwrap().parse().unwrap(),
                    record.get(4).unwrap().to_string(),
                )
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                ("hop1".to_string(), 1, 1, a.clone()),
                ("hop2".to_string(), 1, 2, c.clone()),
                ("hop2".to_string(), 2, 0, c),
            ]
        );
        assert!(dir.path().join("raw").join("batch_1_1.jsonl").is_file());
        assert!(dir.path().join("raw").join("batch_2_2.jsonl").is_file());
        assert!(!dir.path().join("out.parquet").exists());

        let after_first = hits.load(Ordering::Relaxed);
        assert!(
            after_first >= 4,
            "expected block fetches, got {after_first}"
        );

        let cli = Cli {
            config: dir.path().join("seeds.txt"),
            from_block: 1,
            to_block: 2,
            batch: 1,
            rpc_url: format!("http://{addr}"),
            raw_dir: dir.path().join("raw"),
            output: dir.path().join("out.csv"),
            concurrency: 2,
            rpc_batch: 1,
            exclude_seeds_from_hop2: false,
            resume: true,
        };
        tokio::time::timeout(Duration::from_secs(30), run(cli))
            .await
            .expect("resume timed out")
            .unwrap();
        let extra = hits.load(Ordering::Relaxed) - after_first;
        assert_eq!(extra, 2, "resume should only query head and chain id");
    }

    fn block(number: u64, txs: Vec<Value>) -> Value {
        json!({
            "number": format!("0x{number:x}"),
            "hash": format!("0x{:064x}", number + 1),
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let blocks = Arc::new(blocks);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let blocks = Arc::clone(&blocks);
                let hits = Arc::clone(&hits);
                tokio::spawn(async move {
                    let _ = handle_conn(socket, blocks, hits).await;
                });
            }
        });
        addr
    }

    async fn handle_conn(
        mut socket: TcpStream,
        blocks: Arc<HashMap<u64, Value>>,
        hits: Arc<AtomicUsize>,
    ) -> Result<()> {
        let body = read_http_body(&mut socket).await?;
        let response = dispatch(&body, &blocks, &hits);
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

    fn dispatch(body: &str, blocks: &HashMap<u64, Value>, hits: &AtomicUsize) -> String {
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
                .map(|item| dispatch_one(item, blocks))
                .collect();
            Value::Array(out).to_string()
        } else {
            dispatch_one(&value, blocks).to_string()
        }
    }

    fn dispatch_one(item: &Value, blocks: &HashMap<u64, Value>) -> Value {
        let id = item.get("id").cloned().unwrap_or_else(|| json!(1));
        let method = item.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_blockNumber" => json!("0x2"),
            "eth_chainId" => json!("0x1"),
            "eth_getBlockByNumber" => {
                let hex = item
                    .get("params")
                    .and_then(Value::as_array)
                    .and_then(|params| params.first())
                    .and_then(Value::as_str)
                    .unwrap_or("0x0");
                let digits = hex.trim_start_matches("0x");
                let number = u64::from_str_radix(digits, 16).unwrap_or(0);
                blocks.get(&number).cloned().unwrap_or(Value::Null)
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
}
