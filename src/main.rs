//! SnapSync CLI - Command-line interface for downloading RocksDB snapshots
//!
//! This binary provides a user-friendly CLI for downloading and restoring
//! RocksDB snapshots from S3/R2 storage.

use clap::{Parser, ValueEnum};
use snapsync::{download_snapshots, DownloadConfig};
use std::path::PathBuf;
use tracing::info;

/// Execution stage for the snapshot download process
#[derive(Debug, Clone, ValueEnum)]
enum Stage {
    /// Execute all stages: download, merge, and extract
    All,
    /// Only download chunks from CDN
    Download,
    /// Only merge downloaded chunks into tar (requires downloaded chunks)
    Merge,
    /// Only extract tar to RocksDB directory (requires merged tar)
    Extract,
}

/// Command-line arguments for SnapSync.
#[derive(Parser, Debug)]
#[command(name = "snapsync")]
#[command(about = "Download and restore RocksDB snapshots from S3/R2", long_about = None)]
#[command(version)]
struct Args {
    /// Network name (FARCASTER_NETWORK_MAINNET, FARCASTER_NETWORK_TESTNET, FARCASTER_NETWORK_DEVNET)
    #[arg(short, long, default_value = "FARCASTER_NETWORK_MAINNET")]
    network: String,

    /// Shard IDs to download (comma-separated, e.g., "0,1")
    #[arg(short, long, value_delimiter = ',')]
    shards: Vec<u32>,

    /// Output directory for RocksDB data
    #[arg(short, long, default_value = ".rocks")]
    output: PathBuf,

    /// Snapshot download base URL
    #[arg(
        long,
        default_value = "https://pub-d352dd8819104a778e20d08888c5a661.r2.dev"
    )]
    snapshot_url: String,

    /// Temporary download directory
    #[arg(long, default_value = ".rocks.snapshot")]
    temp_dir: String,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Number of concurrent downloads (default: 4)
    #[arg(short, long, default_value = "4")]
    workers: usize,

    /// Skip all verification, trust existing files completely (use with caution)
    #[arg(long)]
    skip_verify: bool,

    /// Stage to execute (default: all)
    #[arg(long, default_value = "all")]
    stage: Stage,
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
        max_concurrent_downloads: args.workers,
        skip_verify: args.skip_verify,
    };

    let db_dir = args.output.to_str().unwrap().to_string();

    // Convert CLI stage to execution stage
    let execution_stage = match args.stage {
        Stage::All => snapsync::ExecutionStage::All,
        Stage::Download => snapsync::ExecutionStage::DownloadOnly,
        Stage::Merge => snapsync::ExecutionStage::MergeOnly,
        Stage::Extract => snapsync::ExecutionStage::ExtractOnly,
    };

    match download_snapshots(&config, db_dir, args.shards, execution_stage).await {
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
