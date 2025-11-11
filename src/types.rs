//! Data structures for snapshot operations.

use serde::{Deserialize, Serialize};

/// Metadata for a snapshot, describing its location and chunks.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct SnapshotMetadata {
    /// Base path for the snapshot in S3/R2 storage.
    pub key_base: String,
    /// List of chunk filenames to download.
    pub chunks: Vec<String>,
    /// Unix timestamp when the snapshot was created.
    pub timestamp: i64,
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
///     max_concurrent_downloads: 8,
///     skip_verify: false,
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
    /// Skip all verification, trust existing files completely (default: false).
    ///
    /// When enabled, existing files are assumed to be valid without any checks
    /// (no size check, no MD5 check). This is extremely fast but should only be
    /// used when you completely trust the local files (e.g., re-running after interruption).
    pub skip_verify: bool,
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            snapshot_download_url: "https://pub-d352dd8819104a778e20d08888c5a661.r2.dev"
                .to_string(),
            snapshot_download_dir: ".rocks.snapshot".to_string(),
            network: "FARCASTER_NETWORK_MAINNET".to_string(),
            max_concurrent_downloads: 4,
            skip_verify: false,
        }
    }
}

/// Stage control for the snapshot download process
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStage {
    /// Execute all stages
    All,
    /// Only download chunks
    DownloadOnly,
    /// Only merge chunks into tar
    MergeOnly,
    /// Only extract tar to directory
    ExtractOnly,
}
