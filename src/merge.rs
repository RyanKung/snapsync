//! Chunk merging and decompression logic.

use crate::error::SnapshotError;
use flate2::read::GzDecoder;
use std::io::Read;
use tokio::io::{AsyncWriteExt, BufWriter};

/// Merges and decompresses chunk files into a single tar archive.
///
/// Uses a sliding window approach for parallel decompression to control memory usage.
///
/// # Arguments
///
/// * `local_chunks` - List of chunk file paths to merge
/// * `tar_filename` - Output tar file path
/// * `merge_pb` - Progress bar for visual feedback
/// * `shard_id` - Shard identifier for logging
///
/// # Returns
///
/// `Ok(())` on success, or an error if merging fails.
pub(crate) async fn merge_chunks(
    local_chunks: &[String],
    tar_filename: &str,
    merge_pb: &indicatif::ProgressBar,
    shard_id: u32,
) -> Result<(), SnapshotError> {
    let mut tar_file = BufWriter::new(tokio::fs::File::create(tar_filename).await?);

    // Use sliding window for parallel decompression with controlled memory
    let total_files = local_chunks.len();
    // Auto-detect CPU cores for optimal merge performance
    let window_size = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let mut current_index = 0;
    let mut pending_tasks: Vec<tokio::task::JoinHandle<Result<Vec<u8>, SnapshotError>>> =
        Vec::new();

    while current_index < total_files || !pending_tasks.is_empty() {
        // Spawn new tasks up to window size
        while pending_tasks.len() < window_size && current_index < total_files {
            let filename = local_chunks[current_index].clone();
            let index = current_index;
            let merge_pb_clone = merge_pb.clone();

            let task = tokio::task::spawn_blocking(move || {
                let chunk_name = std::path::Path::new(&filename)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");

                merge_pb_clone.set_message(format!(
                    "| 🔄 Decompressing: {}/{} | {}",
                    index + 1,
                    total_files,
                    chunk_name
                ));

                let file = std::fs::File::open(&filename).map_err(SnapshotError::IoError)?;
                let reader = std::io::BufReader::with_capacity(4 * 1024 * 1024, file);
                let mut gz_decoder = GzDecoder::new(reader);
                let mut buffer = Vec::new();
                gz_decoder
                    .read_to_end(&mut buffer)
                    .map_err(SnapshotError::IoError)?;
                Ok::<Vec<u8>, SnapshotError>(buffer)
            });

            pending_tasks.push(task);
            current_index += 1;
        }

        // Wait for first task to complete and write it
        if !pending_tasks.is_empty() {
            let task = pending_tasks.remove(0);
            let buffer = task.await.map_err(|e| {
                SnapshotError::IoError(std::io::Error::other(format!("Task join error: {}", e)))
            })??;

            tar_file.write_all(&buffer).await?;
            merge_pb.inc(1);
        }
    }
    tar_file.flush().await?;
    merge_pb.finish_with_message(format!(
        "✅ Merged {} chunks for shard {}",
        local_chunks.len(),
        shard_id
    ));

    Ok(())
}
