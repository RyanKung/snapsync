//! RocksDB SST file verification using magic number.

use std::io::{self, Read, Seek};

/// RocksDB SST file magic number (located at the last 8 bytes of the file).
///
/// This is the magic number that RocksDB uses to identify valid SST files.
/// Value: 0x88e241b785f4cff7 (9863518390377041911 in decimal)
const ROCKSDB_MAGIC_NUMBER: u64 = 0x88e241b785f4cff7;

/// Verifies if a file is a valid RocksDB SST file by checking the magic number.
///
/// RocksDB SST files have a Footer at the end, with the magic number in the last 8 bytes.
/// This function reads those 8 bytes and compares them to the expected value.
///
/// # Arguments
///
/// * `file_path` - Path to the SST file to verify
///
/// # Returns
///
/// `Ok(true)` if the magic number matches (valid SST file),
/// `Ok(false)` if the magic number doesn't match (corrupt or invalid file),
/// `Err` if there's an I/O error reading the file.
///
/// # Performance
///
/// Very fast: only reads 8 bytes from the end of the file (~0.1ms per file).
pub(crate) fn verify_sst_magic_number(file_path: &str) -> Result<bool, io::Error> {
    let mut file = std::fs::File::open(file_path)?;

    // Get file size to ensure it's at least 8 bytes
    let file_size = file.metadata()?.len();
    if file_size < 8 {
        // File too small to be a valid SST file
        return Ok(false);
    }

    // Seek to the last 8 bytes (magic number location)
    file.seek(io::SeekFrom::End(-8))?;

    // Read the magic number bytes
    let mut magic_bytes = [0u8; 8];
    file.read_exact(&mut magic_bytes)?;

    // Parse as little-endian u64 (RocksDB uses little-endian)
    let actual_magic = u64::from_le_bytes(magic_bytes);

    // Compare with expected RocksDB magic number
    Ok(actual_magic == ROCKSDB_MAGIC_NUMBER)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_number_constant() {
        // Verify the magic number constant matches the decimal value
        assert_eq!(ROCKSDB_MAGIC_NUMBER, 9863518390377041911);
    }
}
