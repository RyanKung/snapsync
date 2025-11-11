//! File verification utilities (MD5 checksums and size checks).

use crate::error::SnapshotError;
use tracing::{info, warn};

/// Computes the MD5 hash of a local file.
///
/// This function reads the file in chunks to avoid loading large files
/// entirely into memory. It runs in a blocking task to avoid blocking
/// the async runtime.
///
/// # Arguments
///
/// * `filename` - Path to the file
///
/// # Returns
///
/// The MD5 hash as a hexadecimal string, or an error.
pub(crate) async fn compute_file_md5(filename: &str) -> Result<String, SnapshotError> {
    let filename = filename.to_string();

    tokio::task::spawn_blocking(move || {
        use std::io::Read;

        let file = std::fs::File::open(&filename).map_err(SnapshotError::IoError)?;
        let mut reader = std::io::BufReader::with_capacity(1024 * 1024, file);
        use md5::{Digest, Md5};

        let mut hasher = Md5::new();
        let mut buffer = vec![0u8; 1024 * 1024];

        loop {
            let n = reader.read(&mut buffer).map_err(SnapshotError::IoError)?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }

        let digest = hasher.finalize();
        Ok(format!("{:x}", digest))
    })
    .await
    .map_err(|e| SnapshotError::IoError(std::io::Error::other(format!("Task join error: {}", e))))?
}

/// Verifies if a local file matches the remote file.
///
/// This function performs the following checks:
/// 1. Checks if the local file exists
/// 2. If `skip_verify` is true, immediately returns Ok(true)
/// 3. Sends a HEAD request to get remote file size and ETag
/// 4. Compares file sizes
/// 5. If ETag is available, computes local MD5 and compares
///
/// # Arguments
///
/// * `filename` - Path to the local file
/// * `remote_url` - URL of the remote file
/// * `skip_verify` - If true, skip all verification
///
/// # Returns
///
/// `Ok(true)` if the file is valid and doesn't need re-downloading,
/// `Ok(false)` if the file needs to be downloaded,
/// `Err` on verification errors.
pub(crate) async fn verify_local_file(
    filename: &str,
    remote_url: &str,
    skip_verify: bool,
) -> Result<bool, SnapshotError> {
    let file_display_name = std::path::Path::new(filename)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(filename);

    // Check if local file exists
    let local_metadata = match tokio::fs::metadata(filename).await {
        Ok(m) => m,
        Err(_) => return Ok(false), // File doesn't exist, need to download
    };

    // If skip_verify is enabled, trust the file completely without any checks
    if skip_verify {
        info!(
            "✅ File {} trusted (exists, {} bytes, verification skipped)",
            file_display_name,
            local_metadata.len()
        );
        return Ok(true);
    }

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
