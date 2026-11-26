# qday-indexer-cryp

`qday-index` extracts raw blocks, their top-level transactions, and the logs those transactions emit from an EVM block range, and stores them as local JSONL files. It is built for long, resumable runs against a full or archive node: batched JSON-RPC requests, concurrent fetching, atomic batch files, and an optional follow mode that trails the chain head.

## Features

- **Two run modes**: extract one inclusive range and exit, or omit `--to-block` to follow the chain head indefinitely.
- **Blocks, transactions, and logs**: every batch writes block headers (`blocks_<start>_<end>.jsonl`), transactions (`batch_<start>_<end>.jsonl`), and event logs (`logs_<start>_<end>.jsonl`). Logs are collected with `eth_getLogs` per batch range, falling back to one block per query when the node rejects wide ranges.
- **Confirmation depth**: `--confirm N` keeps the indexer N blocks behind the latest block, so unconfirmed tip blocks are never written.
- **Resumable**: each batch is one file written atomically (tmp file, rename, fsync). `--resume` reuses batch files that match the current range and batch size and cleans up stale ones.
- **RPC fault tolerance**: batched requests fall back to per-block requests; rate limits, timeouts, and 5xx responses retry with exponential backoff.
- **Normalized output**: lowercase addresses and hashes, decimal values (arbitrary precision), both legacy and EIP-1559 fee fields.

## Quick start

Follow the chain from genesis, staying 12 blocks behind the head:

```bash
qday-index \
  --rpc-url "$ETH_RPC_URL" \
  --batch 1000 \
  --confirm 12 \
  --resume
```

Extract one inclusive range and exit:

```bash
qday-index \
  --from-block 18000000 \
  --to-block 18001000 \
  --batch 1000 \
  --rpc-url "$ETH_RPC_URL"
```

Output lands in `raw_txs/`: `batch_<start>_<end>.jsonl` holds one transaction per line, `blocks_<start>_<end>.jsonl` one block header, `logs_<start>_<end>.jsonl` one event log.

## Run modes

### Range mode (`--to-block` set)

Fetches `[from_block, to_block]` once and exits. `to_block` must satisfy `to_block <= chain_head - confirm`; otherwise the run fails instead of indexing unconfirmed blocks.

### Follow mode (`--to-block` omitted)

Polls `eth_blockNumber` in a loop. After catching up it only writes batches up to `chain_head - confirm` and sleeps while waiting for new confirmed blocks. Combine with `--resume` so restarts continue where the previous run stopped.

## CLI reference

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--rpc-url <URL>` | — | Ethereum JSON-RPC endpoint (http/https). Must serve historical blocks. |
| `--from-block <N>` | `1` | First block to include. |
| `--to-block <N>` | — | Last block to include. Omit to follow the chain head forever. |
| `--confirm <N>` | `0` | Blocks to stay behind the latest block. Newest written block is `head - N`. |
| `--batch <N>` | — | Blocks fetched per batch file. Must be 1..=2,000,000; keep it reasonable for memory and rate limits. |
| `--raw-dir <DIR>` | `raw_txs` | Directory for batch files. |
| `--concurrency <N>` | `8` | Parallel HTTP requests inside one batch (1..=1024). |
| `--rpc-batch <N>` | `20` | Blocks packed into one JSON-RPC batch call (1..=500). |
| `--resume` | off | Reuse existing batch files for this range and batch size. |

## Output format

### Transaction files

Each `batch_<start>_<end>.jsonl` line is one top-level transaction:

```json
{"block_number":18000000,"transaction_index":0,"transaction_hash":"0x…","block_hash":"0x…","from_address":"0x…","to_address":"0x…","value":"1000000000000000000","nonce":0,"gas_limit":21000,"gas_price":"1000000000","max_fee_per_gas":null,"max_priority_fee_per_gas":null,"transaction_type":0,"input":"0x"}
```

### Block files

Each `blocks_<start>_<end>.jsonl` line is one block header:

```json
{"block_number":18000000,"block_hash":"0x…","parent_hash":"0x…","timestamp":1695875003,"miner":"0x…","gas_used":12345678,"gas_limit":30000000,"base_fee_per_gas":"1000000000","difficulty":"0","total_difficulty":null,"size":54321,"transaction_count":150,"state_root":"0x…","transactions_root":"0x…","receipts_root":"0x…","extra_data":"0x","nonce":null,"mix_hash":"0x…","withdrawals_root":"0x…","blob_gas_used":null,"excess_blob_gas":null,"parent_beacon_block_root":"0x…"}
```

### Log files

Each `logs_<start>_<end>.jsonl` line is one event log:

```json
{"block_number":18000000,"transaction_index":12,"log_index":3,"transaction_hash":"0x…","block_hash":"0x…","address":"0x…","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],"data":"0x…"}
```

Notes:

- `value`, `gas_price`, `base_fee_per_gas`, `difficulty`, and the EIP-1559 fee fields are decimal strings; `null` means the field is absent (pre-EIP-1559 transactions, pre-merge `base_fee_per_gas`, and fork-dependent fields such as `withdrawals_root`, the blob fields, and `parent_beacon_block_root`).
- `to_address` is `null` for contract-creation transactions.
- Logs are fetched per batch with `eth_getLogs`; a batch with no matching logs still writes an (empty) `logs_` file, so `--resume` stays consistent.
- Receipts and internal traces are not indexed.
- Follow mode does not detect reorgs; set `--confirm` to a depth you are comfortable with.

## Build from source

Requires Rust 1.95+.

```bash
cargo build --release --locked
./target/release/qday-index --help
```

Run the test suite (includes an end-to-end test against a mock JSON-RPC server):

```bash
cargo test --locked
```

## Docker

Pull the published image and configure it through `.env`:

```bash
cp .env.example .env   # edit RPC_URL and friends
docker compose pull && docker compose up
```

Batch files are written to `./data/raw_txs` on the host. `host.docker.internal:8545` reaches a node running on the Docker host.

Environment variables map one-to-one to CLI flags:

| Variable | Default | CLI flag |
| :--- | :--- | :--- |
| `RPC_URL` | — (required) | `--rpc-url` |
| `FROM_BLOCK` | `1` | `--from-block` |
| `TO_BLOCK` | empty (follow) | `--to-block` |
| `CONFIRM` | `0` | `--confirm` |
| `BATCH` | `1000` | `--batch` |
| `RAW_DIR` | `/data/raw_txs` | `--raw-dir` |
| `CONCURRENCY` | `8` | `--concurrency` |
| `RPC_BATCH` | `20` | `--rpc-batch` |
| `RESUME` | `false` | `--resume` |

Passing arguments to the container bypasses the environment variables entirely.

## How it works

```text
+-----------------------+       +-----------------------------+       +--------------------------+
|  EVM RPC Node         | ----> |  qday-index                 | ----> |  Local JSONL Store       |
|  eth_getBlockByNumber |       |  batch -> fetch -> write    |       |  (raw_txs/*.jsonl)       |
|  eth_getLogs          |       |                             |       |                          |
+-----------------------+       +-----------------------------+       +--------------------------+
```

1. The range (or confirmed tip) is split into chunks of `--batch` blocks.
2. Each chunk fetches blocks with `--concurrency` parallel HTTP requests, packing `--rpc-batch` blocks per JSON-RPC batch call. If the node rejects batching, requests fall back to one block per call.
3. Transactions are normalized, sorted by `(block_number, transaction_index)`, and written to `batch_<start>_<end>.jsonl` atomically; block headers go to `blocks_<start>_<end>.jsonl` and logs (fetched with `eth_getLogs` over the same range) to `logs_<start>_<end>.jsonl` the same way. All three files are written only after the whole batch is fetched, and `--resume` reuses a batch only when all three exist.
4. Between fetched batches the indexer pauses 2–3 seconds to stay friendly to rate limits.

## Project layout

```text
src/
  main.rs       binary entry point
  lib.rs        crate root (module wiring)
  cli.rs        command-line definition (clap)
  pipeline.rs   run modes, chunk scheduling, confirmation window
  rpc.rs        JSON-RPC client: batching, fallback, retries
  store.rs      atomic batch files, streaming JSONL reader
  tx.rs         transaction model
  block.rs      block header model and parsed-block pair
  logs.rs       event log model
  addr.rs       address normalization
  hexutil.rs    hex quantities, hashes, calldata
Dockerfile      two-stage build -> debian-slim runtime
docker-entrypoint.sh / docker-compose.yml / .env.example
.github/workflows/build.yml   cargo test + release build, publishes GHCR image
```

## CI and image publishing

`.github/workflows/build.yml` runs `cargo test --locked` and a release build on every push and PR. Pushes to `main`/`master`, `v*` tags, and manual dispatches additionally build and push `ghcr.io/qday-io/qday-indexer-cryp` (tagged `latest`, `sha`, the git tag, or a custom tag).
