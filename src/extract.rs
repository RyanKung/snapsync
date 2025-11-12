//! Tar archive extraction logic.

use crate::error::SnapshotError;
use crate::sst_verify::verify_sst_magic_number;
use tar::Archive;
use tracing::{info, warn};

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

        // Extract metadata before checking (to avoid borrow conflicts)
        let entry_path = entry.path()?.to_path_buf();
        let file_name = entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let expected_size = entry.header().size()?;
        let is_directory = entry.header().entry_type().is_dir();

        file_count = (index + 1) as u64;

        // For directories, always extract (they're lightweight and size doesn't matter)
        if is_directory {
            entry.unpack_in(db_dir)?;
            continue;
        }

        // Check if file already exists with correct size
        let target_path = db_path.join(&entry_path);

        let should_extract = if target_path.exists() && target_path.is_file() {
            match std::fs::metadata(&target_path) {
                Ok(metadata) => {
                    let actual_size = metadata.len();
                    if actual_size == expected_size {
                        // File size matches, check magic number for .sst files
                        if file_name.ends_with(".sst") {
                            match verify_sst_magic_number(target_path.to_str().unwrap()) {
                                Ok(true) => {
                                    // Magic number valid, file is complete
                                    skipped_count += 1;
                                    info!(
                                        "✅ Verified {} (size: {} bytes, magic number: valid)",
                                        file_name, actual_size
                                    );
                                    false
                                }
                                Ok(false) => {
                                    // Magic number invalid, re-extract
                                    warn!(
                                        "⚠️  Re-extracting {} (invalid magic number - corrupt SST file)",
                                        file_name
                                    );
                                    true
                                }
                                Err(_) => {
                                    // Can't verify magic number, re-extract to be safe
                                    warn!(
                                        "⚠️  Re-extracting {} (unable to verify magic number)",
                                        file_name
                                    );
                                    true
                                }
                            }
                        } else {
                            // Non-SST file, only check size
                            skipped_count += 1;
                            // Log first few verified non-SST files
                            if skipped_count <= 3 {
                                info!("✅ Verified {} (size: {} bytes)", file_name, actual_size);
                            }
                            false
                        }
                    } else {
                        // File exists but wrong size, re-extract
                        // Only log the first few mismatches to avoid spam
                        if extracted_count < 3 {
                            info!(
                                "⚠️  Re-extracting {} (size mismatch: {} vs {} bytes)",
                                file_name, actual_size, expected_size
                            );
                        }
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

        // Update progress bar position
        extract_pb.set_position(file_count);

        // Update message more frequently at the beginning, then every 100 files
        if index < 10 || index % 100 == 0 {
            extract_pb.set_message(format!(
                "| 📂 {} new, {} skipped | {}",
                extracted_count, skipped_count, file_name
            ));
        }
    }

    extract_pb.finish_with_message(format!(
        "✅ Extracted {} files to {} ({} new, {} skipped)",
        file_count, db_dir, extracted_count, skipped_count
    ));

    // Log final summary
    if skipped_count > 0 {
        info!(
            "Extraction summary: {} total files ({} extracted, {} skipped)",
            file_count, extracted_count, skipped_count
        );
    }

    Ok(())
}
