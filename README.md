<div align="center">
  <img src="logo.png" alt="SnapSync Logo" width="200"/>
  
  # SnapSync 🚀
  
  **Production-ready RocksDB snapshot downloader with resumable downloads and verification.**
  
  [![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)
  [![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
  
</div>

---

## 🎯 Why SnapSync?

[Snapchain](https://github.com/farcasterxyz/snapchain) provides snapshot download functionality, but **lacks critical production features**:

| Feature | Snapchain | SnapSync |
|---------|-----------|----------|
| Resumable Downloads | ❌ No | ✅ Yes |
| File Verification (MD5) | ❌ No | ✅ Yes |
| Resumable Extraction | ❌ No | ✅ Yes |
| Corruption Detection | ❌ No | ✅ Yes |
| Stage Control | ❌ No | ✅ Yes |
| Parallel Downloads | ❌ No | ✅ Yes |

**The 80-Day Problem:**

When Snapchain's snapshot download or extraction **fails**:
1. ❌ Snapshot is incomplete or corrupted
2. ❌ Snapchain falls back to **full blockchain sync**
3. ❌ Current sync speed: **~80 days** to catch up
4. ❌ Your node is offline for nearly **3 months**

**SnapSync Solution:**

With SnapSync's resumable downloads and extraction:
1. ✅ Download interrupted? **Resume in < 1 minute**
2. ✅ Extraction interrupted? **Resume from last file**
3. ✅ Corruption detected? **Re-download only bad chunks**
4. ✅ **Zero risk** of 80-day sync penalty

**Result: Hours instead of months. Production-ready reliability.**

---

## Features

### Core Features
- 🔄 **Resumable Downloads** - Continue interrupted downloads with MD5 verification
- 🔄 **Resumable Extraction** - Resume extraction after interruption (no data loss)
- ✅ **MD5 Verification** - Automatic integrity checking using ETag/MD5 checksums
- ⚡ **Parallel Downloads** - Concurrent chunk downloads (configurable workers, default: 4)
- ⚡ **Parallel Decompression** - Multi-core CPU utilization for fast merging
- 📊 **Progress Tracking** - Real-time progress bars with accurate ETA
- 🔁 **Automatic Retry** - Built-in retry logic for transient network failures
- 🎛️ **Stage Control** - Download, merge, and extract independently
- 🌐 **Multi-Shard Support** - Efficiently download multiple shards
- 🖥️ **Cross-Platform** - Pre-built binaries for Linux and macOS

### Snapchain Compatibility
- ✅ **100% compatible** with Snapchain snapshot format
- ✅ **Same directory structure** (`.rocks` and `.rocks.snapshot`)
- ✅ **Drop-in replacement** for Snapchain's snapshot download
- ✅ **No migration needed** - Works with existing Snapchain setup

## Quick Start

### Installation

```bash
# From source
cargo install --path .

# Or download pre-built binaries from GitHub Releases
```

### Basic Usage

```bash
# Download and restore all shards (complete workflow)
snapsync --shards 0,1,2

# Download specific shard
snapsync --shards 2 --output .rocks

# Faster downloads with more workers
snapsync --shards 2 --workers 8

# Stage-based execution (download, merge, extract separately)
snapsync --shards 2 --stage download  # Download chunks only
snapsync --shards 2 --stage merge     # Merge chunks into tar
snapsync --shards 2 --stage extract   # Extract tar to directory

# Trust existing files (skip verification, fastest resume)
snapsync --shards 2 --skip-verify
```

### All Options

```bash
snapsync [OPTIONS]

Options:
  -n, --network <NETWORK>
          Network name (FARCASTER_NETWORK_MAINNET, FARCASTER_NETWORK_TESTNET, FARCASTER_NETWORK_DEVNET)
          [default: FARCASTER_NETWORK_MAINNET]

  -s, --shards <SHARDS>...
          Shard IDs to download (comma-separated, e.g., "0,1")

  -o, --output <OUTPUT>
          Output directory for RocksDB data
          [default: .rocks]

      --snapshot-url <SNAPSHOT_URL>
          Snapshot download base URL
          [default: https://pub-d352dd8819104a778e20d08888c5a661.r2.dev]

      --temp-dir <TEMP_DIR>
          Temporary download directory
          [default: .rocks.snapshot]

  -v, --verbose
          Verbose logging

  -w, --workers <WORKERS>
          Number of concurrent downloads
          [default: 4]

  -h, --help
          Print help

  -V, --version
          Print version
```

### Examples

#### Download mainnet shard 0 and 1

```bash
snapsync --network FARCASTER_NETWORK_MAINNET --shards 0,1 --output ./data/.rocks

# Or use the default (FARCASTER_NETWORK_MAINNET)
snapsync --shards 0,1 --output ./data/.rocks
```

#### Download testnet shard 0 with verbose logging

```bash
snapsync --network FARCASTER_NETWORK_TESTNET --shards 0 --output ./testnet-data --verbose
```

#### Parallel downloads with 8 workers

```bash
# Default is 4 workers, increase for faster downloads
# Note: Worker count is not limited by CPU cores (async I/O)
# Even 2-core CPUs can efficiently handle 8-16 workers
snapsync --shards 0,1,2 --workers 8 --output .rocks
```

#### Resume interrupted download

Simply run the same command again - SnapSync will:
1. Check existing files via HEAD request
2. Verify MD5 checksums 
3. Skip already-verified chunks
4. Download only missing or corrupted files

```bash
# This will resume from where it left off
snapsync --shards 0,1 --output .rocks
```

#### Compatible with Snapchain downloads

SnapSync is **100% compatible** with Snapchain's original download logic:

```bash
# If you have partial downloads from Snapchain in .rocks.snapshot/
# Just run SnapSync - it will verify and resume automatically
snapsync --shards 0,1 --output .rocks --temp-dir .rocks.snapshot
```

SnapSync will:
- ✅ Read existing `.rocks.snapshot/` directory
- ✅ Verify all existing chunks via MD5
- ✅ Skip verified files
- ✅ Download only missing/corrupted chunks

## How It Works

1. **Fetch Metadata** - Downloads `latest.json` for each shard containing chunk list
2. **Verify Local Files** - Checks if chunks already exist and match remote MD5
3. **Download Chunks** - Streams chunks with progress tracking and MD5 verification
4. **Decompress** - Unzips gzip chunks and merges into single tar
5. **Extract** - Unpacks tar archive into RocksDB directory
6. **Cleanup** - Removes temporary files

### Resume Logic

When you restart a download:

- ✅ **HEAD Request**: Queries remote file size and ETag (MD5)
- ✅ **Size Check**: Compares local file size with remote
- ✅ **MD5 Verification**: Computes local file MD5 and compares with ETag
- ✅ **Skip or Re-download**: Uses local file if valid, otherwise re-downloads

This makes interrupted downloads very cheap to resume!

## Architecture

### Core Components

- **`lib.rs`** - Core download and verification logic
  - `download_snapshots()` - Main entry point
  - `verify_local_file()` - Resume logic
  - `download_file()` - Streaming download with MD5
  - `compute_file_md5()` - File integrity checking

- **`main.rs`** - CLI interface built with `clap`

### Dependencies

- `reqwest` - HTTP client for downloading
- `md5` - Checksum verification
- `tokio` - Async runtime
- `indicatif` - Progress bars
- `flate2` + `tar` - Decompression and extraction
- `tokio-retry2` - Automatic retry logic

## Performance

### Typical Download Times

| Shard Size | Network | Estimated Time |
|-----------|---------|----------------|
| ~50 GB | 1 Gbps | ~7 minutes |
| ~50 GB | 100 Mbps | ~70 minutes |

### Optimizations

- **Streaming Downloads**: No excessive memory usage
- **MD5 During Download**: No separate verification pass
- **HEAD Requests**: Fast resume checks (~100ms per chunk)
- **Efficient Retry**: Only retries failed chunks

## Error Handling

SnapSync handles various error scenarios:

- **Network Failures**: Automatic retry with exponential backoff
- **Corrupted Files**: Detected via MD5, automatically deleted and re-downloaded
- **Missing Remote Files**: Clear error messages
- **Disk Space**: Errors during write are properly reported

## Development

### Build

```bash
cargo build --release
```

### Run Tests

```bash
cargo test
```

### Check

```bash
cargo check
cargo clippy
```

## License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.

```
SnapSync - Fast, reliable RocksDB snapshot downloader
Copyright (C) 2024 Farcaster Team

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
```

## Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Troubleshooting

### "No ETag available"

Some chunks may not have ETag headers. SnapSync will still download them but can't verify MD5. This is normal for very old snapshots.

### "Size mismatch"

Usually indicates:
- Partial download (will be re-downloaded automatically)
- Corrupted local file (will be re-downloaded)

### Slow Downloads

- Check your network connection
- Try a different time (CDN may be congested)
- Use `--verbose` to see detailed progress

### Disk Space

Ensure you have enough space:
- ~100 GB per shard for data
- ~10-20 GB for temporary files during download

## Credits

Extracted from the Snapchain codebase and optimized for standalone use.

