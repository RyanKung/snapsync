//! Main orchestration logic for downloading snapshots.

use crate::download::download_file_simple;
use crate::error::SnapshotError;
use crate::extract::extract_tar;
use crate::merge::merge_chunks;
use crate::metadata::download_metadata;
use crate::types::{DownloadConfig, ExecutionStage, SnapshotMetadata};
use crate::verify::verify_local_file;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_retry2::{Retry, RetryError};
use tracing::{error, info, warn};

/// Downloads and restores RocksDB snapshots for the specified shards.
///
/// This is the main entry point for downloading snapshots. It performs the following steps:
///
/// 1. Fetches metadata for all requested shards
/// 2. Downloads chunks with resumable support (skips verified files)
/// 3. Decompresses and merges chunks into tar archives
/// 4. Extracts tar archives to the RocksDB directory
///
/// # Arguments
///
/// * `config` - Download configuration
/// * `db_dir` - Target directory for RocksDB data
/// * `shard_ids` - List of shard IDs to download
/// * `stage` - Execution stage control
///
/// # Returns
///
/// `Ok(())` on success, or an error if any step fails.
///
/// # Example
///
/// ```no_run
/// use snapsync::{download_snapshots, DownloadConfig, ExecutionStage};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = DownloadConfig::default();
/// download_snapshots(&config, ".rocks".to_string(), vec![0, 1], ExecutionStage::All).await?;
/// # Ok(())
/// # }
/// ```
pub async fn download_snapshots(
    config: &DownloadConfig,
    db_dir: String,
    shard_ids: Vec<u32>,
    stage: ExecutionStage,
) -> Result<(), SnapshotError> {
    let snapshot_dir = config.snapshot_download_dir.clone();
    std::fs::create_dir_all(snapshot_dir.clone())?;

    // Load or fetch metadata
    let metadata_file_path = format!("{}/metadata.json", snapshot_dir);
    let mut all_metadata = HashMap::new();

    // For merge/extract only stages, try to load existing metadata first
    if stage == ExecutionStage::MergeOnly || stage == ExecutionStage::ExtractOnly {
        if let Ok(content) = std::fs::read_to_string(&metadata_file_path) {
            if let Ok(metadata) = serde_json::from_str(&content) {
                all_metadata = metadata;
                info!("Loaded existing metadata from {}", metadata_file_path);
            }
        }
    }

    // Fetch metadata if not loaded or if in download stage
    if all_metadata.is_empty() {
        for &shard_id in &shard_ids {
            let metadata = download_metadata(&config.network, shard_id, config).await?;
            all_metadata.insert(shard_id.to_string(), metadata);
        }

        // Persist metadata.json file
        let metadata_json = serde_json::to_string_pretty(&all_metadata)?;
        std::fs::write(&metadata_file_path, metadata_json)?;
        info!("Persisted metadata to {}", metadata_file_path);
    }

    // Determine which stages to execute
    let should_download = stage == ExecutionStage::All || stage == ExecutionStage::DownloadOnly;
    let should_merge = stage == ExecutionStage::All || stage == ExecutionStage::MergeOnly;
    let should_extract = stage == ExecutionStage::All || stage == ExecutionStage::ExtractOnly;

    // Create download progress bar only if downloading
    let pb = if should_download {
        let total_chunks: usize = all_metadata.values().map(|m| m.chunks.len()).sum();
        let progress_bar = indicatif::ProgressBar::new(total_chunks as u64);
        progress_bar.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.cyan} [{bar:40.cyan/blue}] {pos}/{len} {msg} | {elapsed_precise} elapsed, ETA {eta_precise}")
                .unwrap()
                .progress_chars("█▓▒░ "),
        );
        progress_bar.set_message(format!(
            "📦 Downloading {} chunks from {} shard(s)",
            total_chunks,
            shard_ids.len()
        ));
        Some(progress_bar)
    } else {
        None
    };

    // Create semaphore to limit concurrent downloads
    let semaphore = Arc::new(Semaphore::new(config.max_concurrent_downloads));

    // Process each shard sequentially
    for &shard_id in &shard_ids {
        let metadata_json = &all_metadata[&shard_id.to_string()];
        let base_path = &metadata_json.key_base;

        std::fs::create_dir_all(format!("{}/shard-{}", snapshot_dir, shard_id))?;

        // Download stage
        let mut filenames_in_order = vec![];

        if should_download {
            if let Some(ref pb) = pb {
                let ctx = ShardDownloadContext {
                    config,
                    metadata: metadata_json,
                    snapshot_dir: &snapshot_dir,
                    shard_id,
                    base_path,
                    pb,
                    semaphore: &semaphore,
                    shard_ids: &shard_ids,
                };
                filenames_in_order = download_shard_chunks(ctx).await?;

                pb.finish_with_message(format!(
                    "✅ Downloaded {} chunks for shard {}",
                    filenames_in_order.len(),
                    shard_id
                ));
            }
        } else {
            // If not downloading, collect existing chunk files
            for chunk in &metadata_json.chunks {
                let filename = format!("{}/shard-{}/{}", snapshot_dir, shard_id, chunk);
                filenames_in_order.push(filename);
            }
        }

        let local_chunks = filenames_in_order;

        // Return early if only downloading
        if stage == ExecutionStage::DownloadOnly {
            continue;
        }

        // Define tar filename for both merge and extract stages
        let tar_filename = format!("{}/shard_{}_snapshot.tar", snapshot_dir, shard_id);

        // Merge stage
        if !should_merge {
            // Skip to extraction
            info!("Skipping merge stage for shard {}", shard_id);
        } else {
            // Create new progress bar for merging phase
            let merge_pb = indicatif::ProgressBar::new(local_chunks.len() as u64);
            merge_pb.set_style(
                indicatif::ProgressStyle::default_bar()
                    .template("{spinner:.cyan} [{bar:40.cyan/blue}] {pos}/{len} {msg} | {elapsed_precise} elapsed, ETA {eta_precise}")
                    .unwrap()
                    .progress_chars("█▓▒░ "),
            );
            merge_pb.set_message(format!("🔄 Merging shard {} chunks", shard_id));

            merge_chunks(&local_chunks, &tar_filename, &merge_pb, shard_id).await?;
        }

        // Return early if only merging
        if stage == ExecutionStage::MergeOnly {
            continue;
        }

        // Extract stage
        if !should_extract {
            info!("Skipping extract stage for shard {}", shard_id);
            continue;
        }

        // Estimate file count based on tar size to skip expensive counting
        let tar_metadata = std::fs::metadata(&tar_filename)?;
        let tar_size_bytes = tar_metadata.len();
        let tar_size_gb = tar_size_bytes as f64 / 1_073_741_824.0;

        // RocksDB SST files are typically 10-50 MB, averaging ~25 MB
        // Use 10.5 MB based on user's actual file sizes
        const AVERAGE_FILE_SIZE_MB: f64 = 10.5;
        let estimated_files = (tar_size_bytes as f64 / (AVERAGE_FILE_SIZE_MB * 1_048_576.0)) as u64;

        info!(
            "📊 Tar file size: {:.2} GB, estimated ~{} files (skipping slow counting phase)",
            tar_size_gb, estimated_files
        );

        // Create progress bar with estimated count (will show actual count as we extract)
        let extract_pb = indicatif::ProgressBar::new_spinner();
        extract_pb.set_style(
            indicatif::ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg} {pos} files | {elapsed_precise} elapsed")
                .unwrap(),
        );
        extract_pb.set_message(format!(
            "📂 Extracting shard {} (~{} files estimated)...",
            shard_id, estimated_files
        ));
        extract_pb.enable_steady_tick(std::time::Duration::from_millis(100));

        extract_tar(&tar_filename, &db_dir, &extract_pb, shard_id)?;
    }

    if let Some(pb) = pb {
        pb.finish_with_message("✅ All snapshots downloaded and extracted successfully!");
    } else {
        info!("✅ All operations completed successfully!");
    }
    Ok(())
}

/// Context for downloading shard chunks
struct ShardDownloadContext<'a> {
    config: &'a DownloadConfig,
    metadata: &'a SnapshotMetadata,
    snapshot_dir: &'a str,
    shard_id: u32,
    base_path: &'a str,
    pb: &'a indicatif::ProgressBar,
    semaphore: &'a Arc<Semaphore>,
    shard_ids: &'a [u32],
}

/// Downloads all chunks for a single shard with parallel downloads.
async fn download_shard_chunks(
    ctx: ShardDownloadContext<'_>,
) -> Result<Vec<String>, SnapshotError> {
    let mut download_tasks = vec![];
    let mut filenames_in_order = vec![];

    for chunk in &ctx.metadata.chunks {
        let download_path = format!(
            "{}/{}/{}",
            ctx.config.snapshot_download_url, ctx.base_path, chunk
        );
        let filename = format!("{}/shard-{}/{}", ctx.snapshot_dir, ctx.shard_id, chunk);

        // Check if file already exists and is valid (resumable download support)
        let chunk_display_name = chunk.clone();
        match verify_local_file(&filename, &download_path, ctx.config.skip_verify).await {
            Ok(true) => {
                // File is already downloaded and verified, skip download
                ctx.pb
                    .set_message(format!("| ✅ Verified: {}", chunk_display_name));
                ctx.pb.inc(1);
                filenames_in_order.push(filename);
                continue;
            }
            Ok(false) | Err(_) => {
                // File needs to be downloaded
            }
        }

        // Prepare download task
        let semaphore = Arc::clone(ctx.semaphore);
        let pb_clone = ctx.pb.clone();
        let _shard_idx = ctx
            .shard_ids
            .iter()
            .position(|&s| s == ctx.shard_id)
            .unwrap()
            + 1;
        let _total_shards = ctx.shard_ids.len();
        let _total_chunks_in_shard = ctx.metadata.chunks.len();
        let chunk_name = chunk.clone();
        let filename_clone = filename.clone();

        filenames_in_order.push(filename.clone());

        let task = tokio::spawn(async move {
            // Acquire semaphore permit
            let _permit = semaphore.acquire().await.unwrap();

            // Update progress message with current chunk info
            pb_clone.set_message(format!("| ⬇️  Downloading: {}", chunk_name));

            let retry_strategy = tokio_retry2::strategy::FixedInterval::from_millis(10_000).take(5);

            let result = Retry::spawn(retry_strategy, || {
                let download_path_clone = download_path.clone();
                let filename_clone = filename_clone.clone();
                let pb_inner = pb_clone.clone();

                async move {
                    let result =
                        download_file_simple(&download_path_clone, &filename_clone, pb_inner).await;
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
                ctx.pb.finish_with_message("❌ Download failed!");
                return Err(e);
            }
            Err(e) => {
                error!("Task join error: {}", e);
                ctx.pb.finish_with_message("❌ Download failed!");
                return Err(SnapshotError::DownloadFailed(format!("Task failed: {}", e)));
            }
        }
    }

    Ok(filenames_in_order)
}
