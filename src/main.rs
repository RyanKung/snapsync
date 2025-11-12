//! SnapSync CLI - Command-line interface for downloading RocksDB snapshots
//!
//! This binary provides a user-friendly CLI for downloading and restoring
//! RocksDB snapshots from S3/R2 storage.

use clap::{Parser, Subcommand, ValueEnum};
use snapsync::{download_snapshots, verify_sst_magic_number, DownloadConfig};
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

/// SnapSync - RocksDB Snapshot Downloader and Verifier
#[derive(Parser, Debug)]
#[command(name = "snapsync")]
#[command(about = "Download, restore, and verify RocksDB snapshots from S3/R2", long_about = None)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,

    /// Verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Download and restore RocksDB snapshots
    Download {
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

        /// Number of concurrent downloads (default: 4)
        #[arg(short, long, default_value = "4")]
        workers: usize,

        /// Skip all verification, trust existing files completely (use with caution)
        #[arg(long)]
        skip_verify: bool,

        /// Stage to execute (default: all)
        #[arg(long, default_value = "all")]
        stage: Stage,
    },

    /// Verify integrity of RocksDB files
    Verify {
        /// Path to the file(s) to verify (e.g., .rocks/shard-2/000042.sst)
        #[arg(required = true)]
        files: Vec<PathBuf>,

        /// Show detailed information
        #[arg(short, long)]
        detailed: bool,

        /// Compare with tar archive in snapshot directory
        #[arg(long)]
        compare_tar: bool,

        /// Snapshot directory (default: .rocks.snapshot)
        #[arg(long, default_value = ".rocks.snapshot")]
        snapshot_dir: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize tracing
    let log_level = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("snapsync={}", log_level))
        .init();

    match args.command {
        Commands::Download {
            network,
            shards,
            output,
            snapshot_url,
            temp_dir,
            workers,
            skip_verify,
            stage,
        } => {
            info!("🚀 SnapSync - RocksDB Snapshot Downloader");
            info!("Network: {}", network);
            info!("Shards: {:?}", shards);
            info!("Output directory: {:?}", output);

            // Validate shard IDs
            if shards.is_empty() {
                eprintln!("Error: At least one shard ID must be specified");
                std::process::exit(1);
            }

            // Create output directory if it doesn't exist
            std::fs::create_dir_all(&output)?;

            let config = DownloadConfig {
                snapshot_download_url: snapshot_url,
                snapshot_download_dir: temp_dir,
                network,
                max_concurrent_downloads: workers,
                skip_verify,
            };

            let db_dir = output.to_str().unwrap().to_string();

            // Convert CLI stage to execution stage
            let execution_stage = match stage {
                Stage::All => snapsync::ExecutionStage::All,
                Stage::Download => snapsync::ExecutionStage::DownloadOnly,
                Stage::Merge => snapsync::ExecutionStage::MergeOnly,
                Stage::Extract => snapsync::ExecutionStage::ExtractOnly,
            };

            match download_snapshots(&config, db_dir, shards, execution_stage).await {
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

        Commands::Verify {
            files,
            detailed,
            compare_tar,
            snapshot_dir,
        } => {
            info!("🔍 SnapSync - File Verifier");

            let mut total_files = 0;
            let mut valid_files = 0;
            let mut invalid_files = 0;
            let mut error_files = 0;

            for file_path in &files {
                total_files += 1;
                let path_str = file_path.to_string_lossy();

                // Check if file exists
                if !file_path.exists() {
                    println!("❌ {}: File not found", path_str);
                    error_files += 1;
                    continue;
                }

                // Get file metadata
                let metadata = match std::fs::metadata(file_path) {
                    Ok(m) => m,
                    Err(e) => {
                        println!("❌ {}: Failed to read metadata: {}", path_str, e);
                        error_files += 1;
                        continue;
                    }
                };

                let file_size = metadata.len();

                // Check if it's a regular file
                if !metadata.is_file() {
                    println!("⚠️  {}: Not a regular file", path_str);
                    error_files += 1;
                    continue;
                }

                let is_valid = true;

                // Step 1: Verify magic number for SST files
                if path_str.ends_with(".sst") {
                    match verify_sst_magic_number(&path_str) {
                        Ok(true) => {
                            if detailed {
                                println!(
                                    "  ✓ Magic number: valid (size: {})",
                                    humanize_bytes(file_size)
                                );
                            }
                        }
                        Ok(false) => {
                            println!("❌ {}: Invalid magic number", path_str);
                            invalid_files += 1;
                            continue;
                        }
                        Err(e) => {
                            println!("❌ {}: Magic number check error: {}", path_str, e);
                            error_files += 1;
                            continue;
                        }
                    }
                }

                // Step 2: Compare with tar archive if specified
                if compare_tar {
                    // Extract shard ID from path (e.g., .rocks/shard-2/000042.sst -> 2)
                    let shard_id = match extract_shard_id(&path_str) {
                        Some(id) => id,
                        None => {
                            println!("⚠️  {}: Cannot extract shard ID from path, skipping tar comparison", path_str);
                            if is_valid {
                                valid_files += 1;
                            }
                            continue;
                        }
                    };

                    // Construct tar path
                    let tar_path = format!("{}/shard_{}_snapshot.tar", snapshot_dir, shard_id);

                    if !std::path::Path::new(&tar_path).exists() {
                        println!(
                            "⚠️  {}: Tar file not found: {}, skipping comparison",
                            path_str, tar_path
                        );
                        if is_valid {
                            valid_files += 1;
                        }
                        continue;
                    }

                    match verify_file_in_tar(file_path, &tar_path, shard_id, detailed) {
                        Ok(true) => {
                            // File matches tar
                        }
                        Ok(false) => {
                            invalid_files += 1;
                            continue;
                        }
                        Err(e) => {
                            println!("❌ {}: Tar comparison error: {}", path_str, e);
                            error_files += 1;
                            continue;
                        }
                    }
                }

                // If all checks passed
                if is_valid {
                    if !detailed && !compare_tar {
                        // Only print simple message if not detailed and no tar comparison
                        if path_str.ends_with(".sst") {
                            println!("✅ {}: Valid SST file", path_str);
                        } else {
                            println!("ℹ️  {}: Non-SST file (no checks)", path_str);
                        }
                    } else if compare_tar {
                        println!("✅ {}: All checks passed", path_str);
                    }
                    valid_files += 1;
                }
            }

            // Summary
            println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            println!("📊 Verification Summary");
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            println!("Total files:   {}", total_files);
            println!("✅ Valid:       {}", valid_files);
            if invalid_files > 0 {
                println!("❌ Invalid:     {}", invalid_files);
            }
            if error_files > 0 {
                println!("⚠️  Errors:      {}", error_files);
            }

            if invalid_files > 0 || error_files > 0 {
                std::process::exit(1);
            }

            Ok(())
        }
    }
}

/// Format bytes in human-readable format
fn humanize_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

/// Extract shard ID from file path
///
/// Examples:
/// - `.rocks/shard-2/000042.sst` -> Some(2)
/// - `/path/to/.rocks/shard-0/MANIFEST-000005` -> Some(0)
/// - `invalid/path.sst` -> None
fn extract_shard_id(path: &str) -> Option<u32> {
    // Look for "shard-N" pattern in the path
    for component in path.split('/') {
        if let Some(suffix) = component.strip_prefix("shard-") {
            if let Ok(id) = suffix.parse::<u32>() {
                return Some(id);
            }
        }
    }
    None
}

/// Verify that a file matches its counterpart in the tar archive
fn verify_file_in_tar(
    file_path: &PathBuf,
    tar_path: &str,
    shard_id: u32,
    detailed: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let file_path_str = file_path.to_string_lossy();

    // Extract filename from path
    let filename = match file_path.file_name() {
        Some(name) => name.to_string_lossy(),
        None => return Err("Cannot extract filename".into()),
    };

    // Expected path in tar: shard-N/filename
    let tar_entry_path = format!("shard-{}/{}", shard_id, filename);

    // Open tar file
    let tar_file = std::fs::File::open(tar_path)?;
    let mut archive = tar::Archive::new(tar_file);

    // Search for the file in tar
    for entry_result in archive.entries()? {
        let entry = entry_result?;
        let entry_path = entry.path()?;
        let entry_path_str = entry_path.to_string_lossy();

        if entry_path_str == tar_entry_path {
            // Found the file, compare sizes
            let tar_size = entry.header().size()?;
            let disk_size = std::fs::metadata(file_path)?.len();

            if tar_size != disk_size {
                println!(
                    "❌ {}: Size mismatch (disk: {} vs tar: {})",
                    file_path_str,
                    humanize_bytes(disk_size),
                    humanize_bytes(tar_size)
                );
                return Ok(false);
            }

            if detailed {
                println!(
                    "  ✓ Tar comparison: size matches ({}) tar",
                    humanize_bytes(tar_size)
                );
            }

            return Ok(true);
        }
    }

    // File not found in tar
    println!(
        "❌ {}: Not found in tar archive (expected: {})",
        file_path_str, tar_entry_path
    );
    Ok(false)
}
