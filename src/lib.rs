//! SnapSync - Fast, reliable RocksDB snapshot downloader with resumable downloads
//!
//! This library provides functionality for downloading and restoring RocksDB snapshots
//! from S3/R2 storage with built-in MD5 verification and resumable download support.
//!
//! # Features
//!
//! - **Resumable Downloads**: Automatically resume interrupted downloads
//! - **MD5 Verification**: Verify data integrity using ETag/MD5 checksums
//! - **Multi-Shard Support**: Download multiple shards efficiently
//! - **Progress Tracking**: Real-time progress reporting
//! - **Automatic Retry**: Built-in retry logic for transient failures
//!
//! # Example
//!
//! ```no_run
//! use snapsync::{download_snapshots, DownloadConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = DownloadConfig::default();
//! let shard_ids = vec![0, 1];
//!
//! download_snapshots(&config, ".rocks".to_string(), shard_ids).await?;
//! # Ok(())
//! # }
//! ```

use flate2::read::GzDecoder;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Read};
use std::sync::Arc;
use tar::Archive;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Semaphore;
use tokio_retry2::{Retry, RetryError};
use tracing::{error, info, warn};

/// Errors that can occur during snapshot operations.
#[derive(Error, Debug)]
pub enum SnapshotError {
    /// I/O error during file operations.
    #[error(transparent)]
    IoError(#[from] io::Error),

    /// HTTP request error during download.
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    /// JSON serialization/deserialization error.
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    /// General snapshot download failure.
    #[error("Snapshot download failed: {0}")]
    DownloadFailed(String),
}

/// Metadata for a snapshot, describing its location and chunks.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct SnapshotMetadata {
    /// Base path for the snapshot in S3/R2 storage.
    key_base: String,
    /// List of chunk filenames to download.
    chunks: Vec<String>,
    /// Unix timestamp when the snapshot was created.
    timestamp: i64,
}

/// Configuration for downloading snapshots.
///
/// # Example
///
/// ```
/// use snapsync::DownloadConfig;
///
/// let config = DownloadConfig {
///     snapshot_download_url: "https://example.com".to_string(),
///     snapshot_download_dir: ".temp".to_string(),
///     network: "FARCASTER_NETWORK_MAINNET".to_string(),
///     max_concurrent_downloads: 8, // Use 8 parallel workers
/// };
/// ```
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    /// Base URL for snapshot downloads (e.g., `<https://pub-xxx.r2.dev>`)
    pub snapshot_download_url: String,
    /// Temporary directory for downloads (e.g., `".rocks.snapshot"`)
    pub snapshot_download_dir: String,
    /// Network name (e.g., `"FARCASTER_NETWORK_MAINNET"`, `"FARCASTER_NETWORK_TESTNET"`)
    pub network: String,
    /// Maximum number of concurrent downloads (default: 4).
    ///
    /// Note: This is not limited by CPU cores. Since downloads are I/O-bound,
    /// even low-core CPUs can handle 8-16 concurrent downloads efficiently.
    /// The limiting factor is network bandwidth, not CPU.
    pub max_concurrent_downloads: usize,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            snapshot_download_url: "https://pub-d352dd8819104a778e20d08888c5a661.r2.dev"
                .to_string(),
            snapshot_download_dir: ".rocks.snapshot".to_string(),
            network: "FARCASTER_NETWORK_MAINNET".to_string(),
            max_concurrent_downloads: 4,
        }
    }
}

/// Constructs the S3/R2 path to the metadata file for a given network and shard.
///
/// # Arguments
///
/// * `network` - The network name (e.g., "MAINNET", "TESTNET")
/// * `shard_id` - The shard identifier
///
/// # Returns
///
/// The path to the `latest.json` metadata file.
fn metadata_path(network: &str, shard_id: u32) -> String {
    format!("{}/{}/latest.json", network, shard_id)
}

/// Downloads and parses the snapshot metadata for a shard.
///
/// # Arguments
///
/// * `network` - The network name
/// * `shard_id` - The shard identifier
/// * `config` - Download configuration
///
/// # Returns
///
/// The parsed metadata or an error.
async fn download_metadata(
    network: &str,
    shard_id: u32,
    config: &DownloadConfig,
) -> Result<SnapshotMetadata, SnapshotError> {
    let metadata_url = format!(
        "{}/{}",
        config.snapshot_download_url,
        metadata_path(network, shard_id)
    );
    info!("Retrieving metadata from {}", metadata_url);

    let response = reqwest::get(&metadata_url).await?;

    // Check HTTP status code
    let status = response.status();
    if !status.is_success() {
        if status.as_u16() == 404 {
            return Err(SnapshotError::DownloadFailed(format!(
                "Snapshot not found for network '{}' shard {}. The metadata URL returned 404: {}\n\
                 This usually means:\n\
                 - The shard doesn't exist for this network\n\
                 - The snapshot hasn't been created yet\n\
                 - The URL is incorrect\n\
                 Available shards for FARCASTER_NETWORK_MAINNET: 0, 1, 2\n\
                 Available shards for FARCASTER_NETWORK_TESTNET: 0, 1",
                network, shard_id, metadata_url
            )));
        } else {
            return Err(SnapshotError::DownloadFailed(format!(
                "Failed to fetch metadata from {}: HTTP {}",
                metadata_url, status
            )));
        }
    }

    // Try to parse as JSON
    let metadata = response.json::<SnapshotMetadata>().await.map_err(|e| {
        SnapshotError::DownloadFailed(format!(
            "Invalid metadata format from {}: {}\n\
             Expected JSON with fields: key_base, chunks, timestamp",
            metadata_url, e
        ))
    })?;

    Ok(metadata)
}

/// Computes the MD5 hash of a local file.
///
/// This function reads the file in chunks to avoid loading large files
/// entirely into memory.
///
/// # Arguments
///
/// * `filename` - Path to the file
///
/// # Returns
///
/// The MD5 hash as a hexadecimal string, or an error.
async fn compute_file_md5(filename: &str) -> Result<String, SnapshotError> {
    let file = tokio::fs::File::open(filename).await?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer
    let mut hasher = md5::Context::new();
    let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks for faster reading

    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.consume(&buffer[..n]);
    }

    Ok(format!("{:x}", hasher.compute()))
}

/// Verifies if a local file matches the remote file.
///
/// This function performs the following checks:
/// 1. Checks if the local file exists
/// 2. Sends a HEAD request to get remote file size and ETag
/// 3. Compares file sizes
/// 4. If ETag is available, computes local MD5 and compares
///
/// # Arguments
///
/// * `filename` - Path to the local file
/// * `remote_url` - URL of the remote file
///
/// # Returns
///
/// `Ok(true)` if the file is valid and doesn't need re-downloading,
/// `Ok(false)` if the file needs to be downloaded,
/// `Err` on verification errors.
async fn verify_local_file(filename: &str, remote_url: &str) -> Result<bool, SnapshotError> {
    let file_display_name = std::path::Path::new(filename)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(filename);

    // Check if local file exists
    let local_metadata = match tokio::fs::metadata(filename).await {
        Ok(m) => m,
        Err(_) => return Ok(false), // File doesn't exist, need to download
    };

    // Send HEAD request to get remote file info
    let client = reqwest::Client::new();
    let response = match client.head(remote_url).send().await {
        Ok(r) => match r.error_for_status() {
            Ok(resp) => resp,
            Err(e) => {
                warn!("HEAD request failed for {}: {}", remote_url, e);
                return Ok(false);
            }
        },
        Err(e) => {
            warn!("Failed to connect to {}: {}", remote_url, e);
            return Ok(false);
        }
    };

    // Get remote file size
    let remote_size = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    // Compare file sizes first (quick check)
    if let Some(remote_size) = remote_size {
        if local_metadata.len() != remote_size {
            info!(
                "Size mismatch for {}: local={} bytes, remote={} bytes",
                filename,
                local_metadata.len(),
                remote_size
            );
            return Ok(false);
        }
    } else {
        // Can't verify size, assume need to re-download
        return Ok(false);
    }

    // Get ETag (which is MD5 for simple uploads in S3/R2)
    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"'));

    // Verify MD5 if ETag is available
    if let Some(etag_val) = etag {
        // Skip multipart uploads (they have "-" in ETag)
        if etag_val.contains('-') {
            info!(
                "✅ File {} verified (size match, multipart upload)",
                file_display_name
            );
            return Ok(true);
        }

        // Compute local file MD5
        match compute_file_md5(filename).await {
            Ok(local_md5) => {
                if local_md5 == etag_val {
                    info!("✅ File {} verified (MD5 match)", file_display_name);
                    return Ok(true);
                } else {
                    info!(
                        "❌ MD5 mismatch for {}: local={}, remote={}",
                        file_display_name, local_md5, etag_val
                    );
                    return Ok(false);
                }
            }
            Err(e) => {
                warn!("⚠️  Failed to compute MD5 for {}: {}", file_display_name, e);
                return Ok(false);
            }
        }
    }

    // No ETag available, but size matches - assume valid
    info!(
        "✅ File {} verified (size match, {} bytes, no ETag)",
        file_display_name,
        local_metadata.len()
    );
    Ok(true)
}

/// Downloads a file from a URL with MD5 verification.
///
/// Simplified version that just downloads and verifies the file.
///
/// # Arguments
///
/// * `url` - The URL to download from
/// * `filename` - The local filename to save to
/// * `pb` - Progress bar for updating download progress
///
/// # Returns
///
/// `Ok(())` on successful download and verification, or an error.
async fn download_file_simple(
    url: &str,
    filename: &str,
    _pb: indicatif::ProgressBar,
) -> Result<(), SnapshotError> {
    let file_display_name = std::path::Path::new(filename)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(filename);

    // Create parent directory if needed
    if let Some(parent) = std::path::Path::new(filename).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = BufWriter::new(tokio::fs::File::create(filename).await?);
    let download_response = reqwest::get(url).await?.error_for_status()?;
    let content_length = download_response.content_length();

    // Get ETag from response headers (this is MD5 for simple S3/R2 uploads)
    let etag = download_response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string());

    // Stream download and compute MD5 simultaneously
    let mut byte_stream = download_response.bytes_stream();
    let mut hasher = if etag.is_some() {
        Some(md5::Context::new())
    } else {
        None
    };

    while let Some(piece) = byte_stream.next().await {
        let chunk = piece?;

        // Update MD5 hash
        if let Some(ref mut h) = hasher {
            h.consume(&chunk);
        }

        file.write_all(&chunk).await?;
    }
    file.flush().await?;

    // Verify file size
    let file_size = tokio::fs::metadata(filename).await?.len();
    if let Some(content_length) = content_length {
        if file_size != content_length {
            return Err(SnapshotError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "File size mismatch for {}: expected {} bytes, got {} bytes",
                    filename, content_length, file_size
                ),
            )));
        }
    } else {
        warn!(
            "Content-Length header was not present for {}. Cannot verify file size.",
            url
        );
    }

    // Verify MD5 checksum (Snapchain's ETag is always MD5 for simple uploads)
    if let (Some(expected_etag), Some(hasher)) = (etag, hasher) {
        // Skip multipart uploads (they have "-" in ETag)
        if !expected_etag.contains('-') {
            info!("🔍 Verifying MD5 for {}", file_display_name);
            let computed_md5 = format!("{:x}", hasher.compute());

            if computed_md5 != expected_etag {
                // MD5 mismatch - delete corrupted file
                let _ = tokio::fs::remove_file(filename).await;
                return Err(SnapshotError::IoError(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "❌ MD5 mismatch for {}: expected {}, got {}",
                        file_display_name, expected_etag, computed_md5
                    ),
                )));
            }
            info!("✅ MD5 verified for {}", file_display_name);
        }
    }

    Ok(())
}

/// Downloads and restores RocksDB snapshots for the specified shards.
///
/// This is the main entry point for downloading snapshots. It performs the following steps:
///
/// 1. Fetches metadata for all requested shards
/// 2. Downloads chunks with resumable support (skips verified files)
/// 3. Decompresses and merges chunks into tar archives
/// 4. Extracts tar archives to the RocksDB directory
/// 5. Cleans up temporary files
///
/// # Arguments
///
/// * `config` - Download configuration
/// * `db_dir` - Target directory for RocksDB data
/// * `shard_ids` - List of shard IDs to download
///
/// # Returns
///
/// `Ok(())` on success, or an error if any step fails.
///
/// # Example
///
/// ```no_run
/// use snapsync::{download_snapshots, DownloadConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = DownloadConfig::default();
/// download_snapshots(&config, ".rocks".to_string(), vec![0, 1]).await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_snapshots(
    config: &DownloadConfig,
    db_dir: String,
    shard_ids: Vec<u32>,
) -> Result<(), SnapshotError> {
    let snapshot_dir = config.snapshot_download_dir.clone();
    std::fs::create_dir_all(snapshot_dir.clone())?;

    // First, fetch metadata for all shards
    let mut all_metadata = HashMap::new();
    for &shard_id in &shard_ids {
        let metadata = download_metadata(&config.network, shard_id, config).await?;
        all_metadata.insert(shard_id.to_string(), metadata);
    }

    // Persist metadata.json file
    let metadata_file_path = format!("{}/metadata.json", snapshot_dir);
    let metadata_json = serde_json::to_string_pretty(&all_metadata)?;
    std::fs::write(&metadata_file_path, metadata_json)?;
    info!("Persisted metadata to {}", metadata_file_path);

    // Count total chunks for the progress bar
    let total_chunks: usize = all_metadata.values().map(|m| m.chunks.len()).sum();

    // Create a clean, single-line progress bar (more stable across terminals)
    let pb = indicatif::ProgressBar::new(total_chunks as u64);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.cyan} [{bar:40.cyan/blue}] {pos}/{len} {msg} | {elapsed_precise} elapsed, ETA {eta_precise}")
            .unwrap()
            .progress_chars("█▓▒░ "),
    );
    pb.set_message(format!(
        "📦 Downloading {} chunks from {} shard(s)",
        total_chunks,
        shard_ids.len()
    ));

    // Create semaphore to limit concurrent downloads
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_downloads));

    // Process each shard sequentially
    for &shard_id in &shard_ids {
        let metadata_json = &all_metadata[&shard_id.to_string()];
        let base_path = &metadata_json.key_base;

        std::fs::create_dir_all(format!("{}/shard-{}", snapshot_dir, shard_id))?;

        // Collect download tasks for this shard
        let mut download_tasks = vec![];
        let mut filenames_in_order = vec![];

        for (_chunk_index, chunk) in metadata_json.chunks.iter().enumerate() {
            let download_path = format!("{}/{}/{}", config.snapshot_download_url, base_path, chunk);
            let filename = format!("{}/shard-{}/{}", snapshot_dir, shard_id, chunk);

            // Check if file already exists and is valid (resumable download support)
            let chunk_display_name = chunk.clone();
            match verify_local_file(&filename, &download_path).await {
                Ok(true) => {
                    // File is already downloaded and verified, skip download
                    pb.set_message(format!("| ✅ Verified: {}", chunk_display_name));
                    pb.inc(1);
                    filenames_in_order.push(filename);
                    continue;
                }
                Ok(false) | Err(_) => {
                    // File needs to be downloaded
                }
            }

            // Prepare download task
            let semaphore = Arc::clone(&semaphore);
            let pb_clone = pb.clone();
            let shard_idx = shard_ids.iter().position(|&s| s == shard_id).unwrap() + 1;
            let total_shards = shard_ids.len();
            let _total_chunks_in_shard = metadata_json.chunks.len();
            let chunk_name = chunk.clone();
            let filename_clone = filename.clone();

            filenames_in_order.push(filename.clone());

            let task = tokio::spawn(async move {
                // Acquire semaphore permit
                let _permit = semaphore.acquire().await.unwrap();

                // Update progress message with current chunk info
                pb_clone.set_message(format!("| ⬇️  Downloading: {}", chunk_name));

                let retry_strategy =
                    tokio_retry2::strategy::FixedInterval::from_millis(10_000).take(5);

                let result = Retry::spawn(retry_strategy, || {
                    let download_path_clone = download_path.clone();
                    let filename_clone = filename_clone.clone();
                    let pb_inner = pb_clone.clone();

                    async move {
                        let result =
                            download_file_simple(&download_path_clone, &filename_clone, pb_inner)
                                .await;
                        match result {
                            Ok(_) => Ok(()),
                            Err(e) => {
                                warn!("Failed to download {} due to error: {}", filename_clone, e);
                                RetryError::to_transient(e)
                            }
                        }
                    }
                })
                .await;

                pb_clone.inc(1);
                result
            });

            download_tasks.push(task);
        }

        // Wait for all downloads in this shard to complete
        for task in download_tasks {
            match task.await {
                Ok(Ok(_)) => {
                    // Download succeeded
                }
                Ok(Err(e)) => {
                    error!("Download task failed: {}", e);
                    pb.finish_with_message("❌ Download failed!");
                    return Err(e);
                }
                Err(e) => {
                    error!("Task join error: {}", e);
                    pb.finish_with_message("❌ Download failed!");
                    return Err(SnapshotError::DownloadFailed(format!("Task failed: {}", e)));
                }
            }
        }

        let local_chunks = filenames_in_order;

        pb.set_message(format!("| 🔄 Merging shard {} chunks", shard_id));

        let tar_filename = format!("{}/shard_{}_snapshot.tar", snapshot_dir, shard_id);
        let mut tar_file = BufWriter::new(tokio::fs::File::create(tar_filename.clone()).await?);

        for filename in local_chunks {
            let file = std::fs::File::open(filename)?;
            let reader = std::io::BufReader::new(file);
            let mut gz_decoder = GzDecoder::new(reader);
            let mut buffer = Vec::new();
            // These files are small, 100MB max each
            gz_decoder.read_to_end(&mut buffer)?;
            tar_file.write_all(&buffer).await?;
        }
        tar_file.flush().await?;

        pb.set_message(format!("| 📂 Extracting shard {} to disk", shard_id));

        let file = std::fs::File::open(tar_filename.clone())?;
        let mut archive = Archive::new(file);
        std::fs::create_dir_all(&db_dir)?;
        archive.unpack(&db_dir)?;
    }

    pb.finish_with_message("✅ All snapshots downloaded and extracted successfully!");
    std::fs::remove_dir_all(snapshot_dir)?;
    Ok(())
}
