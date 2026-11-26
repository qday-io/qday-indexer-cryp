# Multi-Seed Two-Hop On-Chain Transaction Extraction Specification

## 1. Overview & Objective
This document outlines the technical specification and implementation pipeline for extracting two-hop on-chain transaction flows within a specified block range $[a, b]$ using **Paradigm's `cryo`** for high-throughput RPC extraction and **DuckDB** for local relational filtering.

### Core Requirements
1. **Input Sources**:
   * **Block Range**: Inclusive interval $[a, b]$ (`block_start` to `block_end`).
   * **Seed Address List**: A local file (`seed_addresses.txt` or `seed_addresses.csv`) containing multiple source addresses.
2. **Hop 1 Extraction**: Identify all top-level transactions within $[a, b]$ where `from_address` matches any address in the seed address list.
3. **Hop 2 Extraction**: Identify all recipient addresses (`to_address`) from Hop 1 transactions, and extract all subsequent top-level transactions within $[a, b]$ initiated (`from_address`) by those recipients strictly after receiving funds.
4. **Output**: Persist the combined, deduplicated, and ordered Hop 1 and Hop 2 transaction datasets locally in **Parquet** and **CSV** formats.

---

## 2. System Architecture

Because `cryo` operates as a high-concurrency batch extractor rather than a recursive graph traversal engine, the pipeline separates **data ingestion** from **multi-hop relational filtering**:

```text
+-----------------------+       +------------------------+       +--------------------------+
|  EVM RPC Node         | ----> |  cryo (Rust Extractor) | ----> |  Local Parquet Store     |
|  (Block Range [a, b]) |       |  `cryo txs -b a:b`     |       |  (./raw_txs/*.parquet)   |
+-----------------------+       +------------------------+       +--------------------------+
                                                                               |
                                                                               v
+-----------------------+       +------------------------+       +--------------------------+
|  Final Output Files   | <---- |  DuckDB SQL Engine     | <---- |  Seed Address File       |
|  (.parquet / .csv)    |       |  (Hop 1 -> Hop 2 Join) |       |  (seed_addresses.txt)    |
+-----------------------+       +------------------------+       +--------------------------+
```

### Key Design Decisions
* **Single-Pass RPC Extraction**: Fetching all transactions in $[a, b]$ once avoids redundant RPC overhead regardless of whether the seed file contains 10 or 100,000 addresses.
* **Intra-Block Ordering Precision**: Ordering is evaluated using a composite position key:
  $$\text{tx\_pos} = \text{block\_number} \times 10^6 + \text{transaction\_index}$$
  This ensures that if a recipient receives funds and forwards them within the **same block**, the outgoing transaction is accurately captured as a valid Hop 2 event.

---

## 3. Prerequisites & Input Preparation

### 3.1 Environment Dependencies
* **Cryo CLI**: Installed via Cargo (`cargo install cryo_cli`) or prebuilt binaries.
* **Python 3.9+** with `duckdb` installed (`pip install duckdb`).
* **EVM Archive/Full Node RPC URL**: Accessible via environment variable `$ETH_RPC_URL`.

### 3.2 Seed Address File Format
Prepare either a plain text file or a CSV file:

* **Option A: Plain Text (`seed_addresses.txt`)** — One EVM address per line:
  ```text
  0x1111111111111111111111111111111111111111
  0x2222222222222222222222222222222222222222
  ```
* **Option B: CSV (`seed_addresses.csv`)** — Must include an `address` header column:
  ```csv
  address
  0x1111111111111111111111111111111111111111
  0x2222222222222222222222222222222222222222
  ```

---

## 4. Implementation Steps

### Step 1: Extract Raw Transactions with `cryo`
Run `cryo` to download all transactions within the target block range $[a, b]$ into local Parquet files:

```bash
cryo txs \
  -b <block_start>:<block_end> \
  --rpc "$ETH_RPC_URL" \
  -o ./raw_txs
```

> **Note**: For very large block ranges (e.g., $> 100,000$ blocks), `cryo` automatically partitions output into chunked Parquet files and supports resumable execution if interrupted.

### Step 2: Execute Multi-Seed Hop 1 & Hop 2 Filtering (`extract_two_hop.py`)
Use the following Python script powered by DuckDB to perform address normalization, two-hop causal joins, and local file export:

```python
import duckdb

# ==========================================
# Configuration Parameters
# ==========================================
BLOCK_START = 18000000          # Replace with block a
BLOCK_END = 18001000            # Replace with block b
SEED_FILE = "./seed_addresses.txt"  # Supports .txt or .csv
RAW_TXS_GLOB = "./raw_txs/*.parquet"
OUTPUT_PARQUET = "./multi_seed_hop1_hop2.parquet"
OUTPUT_CSV = "./multi_seed_hop1_hop2.csv"
EXCLUDE_SEEDS_FROM_HOP2 = False  # Set True to prevent seed-to-seed overlap in Hop 2

con = duckdb.connect()

# 1. Load, trim, lowercase, and deduplicate seed addresses
if SEED_FILE.endswith(".csv"):
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE seed_addrs AS
        SELECT DISTINCT lower(trim(address)) AS addr
        FROM read_csv_auto('{SEED_FILE}')
        WHERE address IS NOT NULL AND trim(address) != ''
    """)
else:
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE seed_addrs AS
        SELECT DISTINCT lower(trim(column0)) AS addr
        FROM read_csv('{SEED_FILE}', header=false, columns={{'column0': 'VARCHAR'}})
        WHERE column0 IS NOT NULL AND trim(column0) != ''
    """)

# 2. Hop 1: Transactions sent from any seed address within [BLOCK_START, BLOCK_END]
con.execute(f"""
    CREATE OR REPLACE TEMP TABLE hop1_txs AS
    SELECT
        t.*,
        (t.block_number * 1000000 + t.transaction_index) AS tx_pos
    FROM read_parquet('{RAW_TXS_GLOB}') t
    JOIN seed_addrs s
      ON lower(t.from_address) = s.addr
    WHERE t.block_number BETWEEN {BLOCK_START} AND {BLOCK_END}
""")

# 3. Extract unique Hop 1 recipients and their earliest receipt position
seed_exclusion_clause = (
    "AND lower(to_address) NOT IN (SELECT addr FROM seed_addrs)"
    if EXCLUDE_SEEDS_FROM_HOP2
    else ""
)

con.execute(f"""
    CREATE OR REPLACE TEMP TABLE recipients AS
    SELECT
        lower(to_address) AS addr,
        MIN(tx_pos) AS first_recv_pos
    FROM hop1_txs
    WHERE to_address IS NOT NULL
      AND to_address != ''
      {seed_exclusion_clause}
    GROUP BY lower(to_address)
""")

# 4. Hop 2: Subsequent transactions sent by Hop 1 recipients within [BLOCK_START, BLOCK_END]
con.execute(f"""
    CREATE OR REPLACE TEMP TABLE hop2_txs AS
    SELECT
        t.*,
        (t.block_number * 1000000 + t.transaction_index) AS tx_pos
    FROM read_parquet('{RAW_TXS_GLOB}') t
    JOIN recipients r
      ON lower(t.from_address) = r.addr
    WHERE t.block_number BETWEEN {BLOCK_START} AND {BLOCK_END}
      AND (t.block_number * 1000000 + t.transaction_index) > r.first_recv_pos
""")

# 5. Combine and export results to Parquet and CSV
combined_query = """
    SELECT 'hop1' AS hop_stage, * EXCLUDE (tx_pos) FROM hop1_txs
    UNION ALL
    SELECT 'hop2' AS hop_stage, * EXCLUDE (tx_pos) FROM hop2_txs
    ORDER BY block_number, transaction_index
"""

con.execute(f"COPY ({combined_query}) TO '{OUTPUT_PARQUET}' (FORMAT PARQUET)")
con.execute(f"COPY ({combined_query}) TO '{OUTPUT_CSV}' (HEADER, DELIMITER ',')")

# Summary Metrics
hop1_count = con.execute("SELECT COUNT(*) FROM hop1_txs").fetchone()[0]
recipient_count = con.execute("SELECT COUNT(*) FROM recipients").fetchone()[0]
hop2_count = con.execute("SELECT COUNT(*) FROM hop2_txs").fetchone()[0]

print(f"Extraction Complete | Hop 1 Txs: {hop1_count} | Unique Recipients: {recipient_count} | Hop 2 Txs: {hop2_count}")
```

---

## 5. Edge Cases & Operational Considerations

| Scenario | Impact on Results | Recommended Handling |
| :--- | :--- | :--- |
| **Address Checksum Casing** | EVM addresses may appear in mixed-case (EIP-55) or lowercase across sources. | Always normalize addresses with `lower(trim(...))` prior to joins. |
| **Contract Creation Txs** | `to_address` is `NULL` or empty when deploying a smart contract. | Explicitly filter out `to_address IS NULL OR to_address = ''` when building the `recipients` table. |
| **Seed-to-Seed Transfers** | If `Seed_A` transfers to `Seed_B`, `Seed_B`'s subsequent transactions qualify as both Hop 1 and Hop 2. | Toggle `EXCLUDE_SEEDS_FROM_HOP2 = True` if you only want non-seed intermediaries in Hop 2. |
| **ERC-20 / Token Transfers** | Top-level `txs` only record the token contract in `to_address`, not the actual token recipient. | If tracking ERC-20 flows, extract `cryo logs` for `Transfer(address,address,uint256)` events alongside `cryo txs`. |
| **Smart Contract Recipients** | Contracts cannot initiate top-level EOA transactions unless triggered internally. | If tracing internal fund forwarding through contracts, supplement with `cryo traces`. |