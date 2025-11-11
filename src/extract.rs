//! Tar archive extraction logic.

use crate::error::SnapshotError;
use tar::Archive;
use tracing::info;

/// Extracts a tar archive to a target directory with progress tracking.
///
/// Supports resumable extraction by checking existing files:
/// - If a file exists with the correct size, it's skipped
/// - If a file is missing or has wrong size, it's extracted
///
/// # Arguments
///
/// * `tar_filename` - Path to the tar file
/// * `db_dir` - Target directory for extraction
/// * `extract_pb` - Progress bar for visual feedback (should be pre-configured with total length)
/// * `shard_id` - Shard identifier for logging
///
/// # Returns
///
/// `Ok(())` on success, or an error if extraction fails.
pub(crate) fn extract_tar(
    tar_filename: &str,
    db_dir: &str,
    extract_pb: &indicatif::ProgressBar,
    _shard_id: u32,
) -> Result<(), SnapshotError> {
    let file = std::fs::File::open(tar_filename)?;
    let mut archive = Archive::new(file);
    std::fs::create_dir_all(db_dir)?;

    let db_path = std::path::Path::new(db_dir);
    let mut file_count = 0u64;
    let mut skipped_count = 0u64;
    let mut extracted_count = 0u64;

    // Extract entries with progress
    for (index, entry) in archive.entries()?.enumerate() {
        let mut entry = entry?;
        let entry_path = entry.path()?;
        let file_name = entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        file_count = (index + 1) as u64;

        // Check if file already exists with correct size
        let target_path = db_path.join(&entry_path);
        let expected_size = entry.header().size()?;

        let should_extract = if target_path.exists() {
            match std::fs::metadata(&target_path) {
                Ok(metadata) => {
                    let actual_size = metadata.len();
                    if actual_size == expected_size {
                        // File exists with correct size, skip it
                        skipped_count += 1;
                        if index < 10 || skipped_count <= 5 {
                            info!(
                                "⏭️  Skipping existing file: {} ({} bytes)",
                                file_name, actual_size
                            );
                        }
                        false
                    } else {
                        // File exists but wrong size, re-extract
                        info!(
                            "⚠️  Re-extracting {} (size mismatch: {} vs {} bytes)",
                            file_name, actual_size, expected_size
                        );
                        true
                    }
                }
                Err(_) => {
                    // Can't read metadata, extract to be safe
                    true
                }
            }
        } else {
            // File doesn't exist, extract it
            true
        };

        if should_extract {
            entry.unpack_in(db_dir)?;
            extracted_count += 1;
        }

        // Update message more frequently at the beginning, then every 100 files
        if index < 10 || index % 100 == 0 {
            extract_pb.set_message(format!(
                "📂 Extracting: {} files ({} new, {} skipped) | {}",
                file_count, extracted_count, skipped_count, file_name
            ));
        }
    }

    if skipped_count > 0 {
        info!(
            "✅ Extraction complete: {} total files ({} extracted, {} skipped)",
            file_count, extracted_count, skipped_count
        );
    }

    extract_pb.finish_with_message(format!(
        "✅ Extracted {} files to {} ({} new, {} skipped)",
        file_count, db_dir, extracted_count, skipped_count
    ));

    Ok(())
}
