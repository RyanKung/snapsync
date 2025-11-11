//! Tar archive extraction logic.

use crate::error::SnapshotError;
use tar::Archive;

/// Extracts a tar archive to a target directory with progress tracking.
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

    let mut file_count = 0u64;

    // Extract entries with progress
    for (index, entry) in archive.entries()?.enumerate() {
        let mut entry = entry?;
        let path = entry.path()?;
        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        file_count = (index + 1) as u64;

        // Update message more frequently at the beginning, then every 100 files
        if index < 10 || index % 100 == 0 {
            extract_pb.set_message(format!(
                "📂 Extracting: {} files | {}",
                file_count, file_name
            ));
        }

        entry.unpack_in(db_dir)?;
    }

    extract_pb.finish_with_message(format!("✅ Extracted {} files to {}", file_count, db_dir));

    Ok(())
}
