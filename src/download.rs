//! Chunk download functionality.

use crate::error::SnapshotError;
use futures_util::StreamExt;
use std::io;
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::{info, warn};

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
pub(crate) async fn download_file_simple(
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
        use md5::Digest;
        Some(md5::Md5::new())
    } else {
        None
    };

    while let Some(piece) = byte_stream.next().await {
        let chunk = piece?;

        // Update MD5 hash
        if let Some(ref mut h) = hasher {
            use md5::Digest;
            h.update(&chunk);
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
            use md5::Digest;
            info!("🔍 Verifying MD5 for {}", file_display_name);
            let computed_md5 = format!("{:x}", hasher.finalize());

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
