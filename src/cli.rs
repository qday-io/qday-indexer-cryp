use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "qday-index",
    version,
    about = "Extract multi-seed two-hop transactions from an EVM block range",
    after_help = "\
Example:
  qday-index --config seeds.txt --from-block 18000000 --to-block 18001000 --batch 1000 --rpc-url http://127.0.0.1:8545

--from-block and --to-block are inclusive. --batch is how many blocks are fetched
at a time so a wide range does not become one request. Hop filtering runs after the
full range is stored, so a receipt in an earlier batch still matches a later send.

The RPC endpoint must serve historical blocks (full or archive node). Output is
top-level transactions, not logs, receipts, or internal traces."
)]
pub struct Cli {
    /// Seed address file. `.txt`: one address per line. `.csv`: an `address` column.
    #[arg(long, value_name = "FILE")]
    pub config: PathBuf,

    /// First block to include.
    #[arg(long, value_name = "N")]
    pub from_block: u64,

    /// Last block to include.
    #[arg(long, value_name = "N")]
    pub to_block: u64,

    /// Blocks to fetch per batch.
    #[arg(long, value_name = "N")]
    pub batch: u64,

    /// Ethereum JSON-RPC URL.
    #[arg(long, value_name = "URL")]
    pub rpc_url: String,

    /// Directory for per-batch raw transaction files.
    #[arg(long, value_name = "DIR", default_value = "raw_txs")]
    pub raw_dir: PathBuf,

    /// CSV result file.
    #[arg(long, value_name = "FILE", default_value = "out.csv")]
    pub output: PathBuf,

    /// Parallel JSON-RPC HTTP requests inside one batch.
    #[arg(long, value_name = "N", default_value_t = 8)]
    pub concurrency: usize,

    /// Blocks packed into one JSON-RPC HTTP call.
    #[arg(long, value_name = "N", default_value_t = 20)]
    pub rpc_batch: usize,

    /// Do not treat later transactions from seed addresses as hop 2.
    #[arg(long, default_value_t = false)]
    pub exclude_seeds_from_hop2: bool,

    /// Reuse raw batch files already present for this range and batch size.
    #[arg(long, default_value_t = false)]
    pub resume: bool,
}
