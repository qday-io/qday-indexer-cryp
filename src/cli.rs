use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "qday-index",
    version,
    about = "Extract raw blocks, transactions, and logs from an EVM block range",
    after_help = "\
Example:
  qday-index --batch 1000 --rpc-url http://127.0.0.1:8545 --confirm 12
  qday-index --from-block 18000000 --to-block 18001000 --batch 1000 --rpc-url http://127.0.0.1:8545

--from-block defaults to 1. --from-block and --to-block are inclusive.
Omit --to-block to keep following the chain. --confirm is how many blocks to
stay behind the latest block, so the newest block written is latest - confirm.

--batch is how many blocks are fetched at a time so a wide range does not
become one request. Existing batch files are reused when --resume is set.

Each batch writes three files: batch_<start>_<end>.jsonl with transactions,
blocks_<start>_<end>.jsonl with block headers, and logs_<start>_<end>.jsonl
with event logs collected through eth_getLogs.

The RPC endpoint must serve historical blocks (full or archive node). Output is
top-level transactions, block headers, and logs — not receipts or internal traces."
)]
pub struct Cli {
    /// First block to include. Defaults to 1.
    #[arg(long, value_name = "N", default_value_t = 1)]
    pub from_block: u64,

    /// Last block to include. Omit to follow the chain head.
    #[arg(long, value_name = "N")]
    pub to_block: Option<u64>,

    /// Blocks to stay behind the latest block.
    #[arg(long, value_name = "N", default_value_t = 0)]
    pub confirm: u64,

    /// Blocks to fetch per batch.
    #[arg(long, value_name = "N")]
    pub batch: u64,

    /// Ethereum JSON-RPC URL.
    #[arg(long, value_name = "URL")]
    pub rpc_url: String,

    /// Directory for per-batch raw transaction files.
    #[arg(long, value_name = "DIR", default_value = "raw_txs")]
    pub raw_dir: PathBuf,

    /// Parallel JSON-RPC HTTP requests inside one batch.
    #[arg(long, value_name = "N", default_value_t = 8)]
    pub concurrency: usize,

    /// Blocks packed into one JSON-RPC HTTP call.
    #[arg(long, value_name = "N", default_value_t = 20)]
    pub rpc_batch: usize,

    /// Reuse raw batch files already present for this range and batch size.
    #[arg(long, default_value_t = false)]
    pub resume: bool,
}
