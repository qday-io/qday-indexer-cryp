use clap::Parser;

#[tokio::main]
async fn main() {
    let cli = qday_index::cli::Cli::parse();
    if let Err(err) = qday_index::pipeline::run(cli).await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}
