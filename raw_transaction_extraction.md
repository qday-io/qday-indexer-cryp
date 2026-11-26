# Raw On-Chain Block, Transaction & Log Extraction

## 1. Overview

Extract every block, its top-level transactions, and the event logs those transactions emit in an inclusive block range \([a, b]\) from an EVM JSON-RPC node and store the result locally.

Input:

* **Start block**: `--from-block`, inclusive. Defaults to 1.
* **End block**: `--to-block`, inclusive. Omit it to keep following the chain head.
* **Confirmations**: `--confirm` blocks behind the latest block. The newest stored block is `latest - confirm`.
* **RPC URL**: a full or archive node (`--rpc-url`).

Output:

* One transaction file per batch, `raw_txs/batch_<start>_<end>.jsonl` — one top-level transaction per line.
* One block file per batch, `raw_txs/blocks_<start>_<end>.jsonl` — one block header per line (number, hash, parent hash, timestamp, miner, gas fields, fee fields, state/transactions/receipts roots, plus fork-dependent optional fields).
* One log file per batch, `raw_txs/logs_<start>_<end>.jsonl` — one event log per line (address, topics, data, plus block/transaction/log position), collected with `eth_getLogs` over the batch range with a per-block fallback. A batch with no logs still writes an empty file.
* Receipts and internal traces are not included.

## 2. Flow

```text
+-----------------------+       +------------------------+       +--------------------------+
|  EVM RPC Node         | ----> |  qday-index            | ----> |  Local JSONL Store       |
|  (Block Range [a, b]) |       |  eth_getBlockByNumber  |       |  (./raw_txs/*.jsonl)     |
+-----------------------+       +------------------------+       +--------------------------+
```

The range is split into batches of `--batch` blocks. Each batch is fetched with parallel HTTP calls, and each call packs `--rpc-batch` blocks. A finished batch file is replaced atomically. `--resume` keeps batch files that already match the current range and batch size.

## 3. Run

Follow the chain, staying 12 blocks behind the head:

```bash
qday-index \
  --batch 1000 \
  --confirm 12 \
  --rpc-url "$ETH_RPC_URL" \
  --raw-dir ./raw_txs
```

Extract one inclusive range:

```bash
qday-index \
  --from-block <block_start> \
  --to-block <block_end> \
  --batch 1000 \
  --rpc-url "$ETH_RPC_URL" \
  --raw-dir ./raw_txs
```

For a large range, keep `--batch` small enough that one batch fits in memory and on the RPC rate limit. Interrupted runs can continue with `--resume`.
