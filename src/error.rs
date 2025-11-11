//! Error types for snapshot operations.

use std::io;
use thiserror::Error;

/// Errors that can occur during snapshot operations.
#[derive(Error, Debug)]
pub enum SnapshotError {
    /// I/O error during file operations.
    #[error(transparent)]
    IoError(#[from] io::Error),

    /// HTTP request error during download.
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    /// JSON serialization/deserialization error.
    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    /// General snapshot download failure.
    #[error("Snapshot download failed: {0}")]
    DownloadFailed(String),
}
