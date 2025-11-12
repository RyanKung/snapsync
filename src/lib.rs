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
//! - **Stage Control**: Execute download, merge, and extract stages independently
//!
//! # Example
//!
//! ```no_run
//! use snapsync::{download_snapshots, DownloadConfig, ExecutionStage};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = DownloadConfig::default();
//! let shard_ids = vec![0, 1];
//!
//! download_snapshots(&config, ".rocks".to_string(), shard_ids, ExecutionStage::All).await?;
//! # Ok(())
//! # }
//! ```

mod download;
mod error;
mod extract;
mod merge;
mod metadata;
mod orchestrator;
mod sst_verify;
mod types;
mod verify;

// Re-export public API
pub use error::SnapshotError;
pub use orchestrator::download_snapshots;
pub use sst_verify::verify_sst_magic_number;
pub use types::{DownloadConfig, ExecutionStage};
