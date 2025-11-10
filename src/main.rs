use clap::Parser;
use snapsync::{download_snapshots, DownloadConfig};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(name = "snapsync")]
#[command(about = "Download and restore RocksDB snapshots from S3/R2", long_about = None)]
#[command(version)]
struct Args {
    /// Network name (MAINNET, TESTNET, etc.)
    #[arg(short, long, default_value = "MAINNET")]
    network: String,

    /// Shard IDs to download (comma-separated, e.g., "0,1")
    #[arg(short, long, value_delimiter = ',')]
    shards: Vec<u32>,

    /// Output directory for RocksDB data
    #[arg(short, long, default_value = ".rocks")]
    output: PathBuf,

    /// Snapshot download base URL
    #[arg(long, default_value = "https://pub-d352dd8819104a778e20d08888c5a661.r2.dev")]
    snapshot_url: String,

    /// Temporary download directory
    #[arg(long, default_value = ".rocks.snapshot")]
    temp_dir: String,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize tracing
    let log_level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("snapsync={}", log_level))
        .init();

    info!("🚀 SnapSync - RocksDB Snapshot Downloader");
    info!("Network: {}", args.network);
    info!("Shards: {:?}", args.shards);
    info!("Output directory: {:?}", args.output);

    // Validate shard IDs
    if args.shards.is_empty() {
        eprintln!("Error: At least one shard ID must be specified");
        std::process::exit(1);
    }

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(&args.output)?;

    let config = DownloadConfig {
        snapshot_download_url: args.snapshot_url,
        snapshot_download_dir: args.temp_dir,
        network: args.network,
    };

    let db_dir = args.output.to_str().unwrap().to_string();

    match download_snapshots(&config, db_dir, args.shards).await {
        Ok(_) => {
            info!("✅ Snapshot download and restore completed successfully!");
            Ok(())
        }
        Err(e) => {
            eprintln!("❌ Error: {}", e);
            std::process::exit(1);
        }
    }
}
