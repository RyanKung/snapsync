//! Metadata fetching and management.

use crate::error::SnapshotError;
use crate::types::{DownloadConfig, SnapshotMetadata};
use tracing::info;

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
pub(crate) fn metadata_path(network: &str, shard_id: u32) -> String {
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
pub(crate) async fn download_metadata(
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
