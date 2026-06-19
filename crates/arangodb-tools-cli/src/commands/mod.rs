//! CLI subcommand implementations.

pub(crate) mod connection;
pub(crate) mod dump;
pub(crate) mod export;
pub(crate) mod import;
pub(crate) mod restore;

use std::path::Path;

use arangodb_storage::{Compression, LocalFileSystem, ObjectStore, ObjectStoreBackend, StorageUri};
use arangodb_tools_core::{Error, Result};
use clap::ValueEnum;

/// Resolves a dump *root* (a directory or object-store prefix that holds many
/// artifacts) into a store. Accepts a path, a `file://` URI, or
/// `s3://bucket/prefix`. Used by both `dump` (writes) and `restore` (reads).
pub(crate) fn open_store_root(location: &str) -> Result<Box<dyn ObjectStore>> {
    if let Some((scheme, _)) = location.split_once("://") {
        return match scheme {
            "file" => Ok(Box::new(LocalFileSystem::new(Path::new(
                location.trim_start_matches("file://"),
            )))),
            "s3" => {
                let parsed = StorageUri::parse(location)?;
                let bucket = parsed.bucket.ok_or_else(|| {
                    Error::config(format!("s3 URI is missing a bucket: {location}"))
                })?;
                let prefix = (!parsed.path.is_empty()).then_some(parsed.path);
                Ok(Box::new(ObjectStoreBackend::s3(&bucket, prefix)?))
            }
            other => Err(Error::config(format!(
                "object-storage scheme '{other}://' is not supported yet; use s3:// or a path"
            ))),
        };
    }
    Ok(Box::new(LocalFileSystem::new(Path::new(location))))
}

/// Compression selection shared by `import` and `export`, including `auto`
/// detection from the file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum CompressionArg {
    /// Detect from the file extension; none for stdin/streams.
    Auto,
    /// No compression.
    None,
    /// gzip.
    Gzip,
    /// Zstandard.
    Zstd,
}

impl CompressionArg {
    /// Resolves to a concrete [`Compression`], detecting from `location` when
    /// set to `auto` (a non-path location such as `-` is treated as
    /// uncompressed).
    pub(crate) fn resolve(self, location: &str) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Gzip => Compression::Gzip,
            Self::Zstd => Compression::Zstd,
            Self::Auto if location == "-" => Compression::None,
            Self::Auto => Compression::infer_from_path(location),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_detects_and_explicit_overrides() {
        assert_eq!(
            CompressionArg::Auto.resolve("users.jsonl.gz"),
            Compression::Gzip
        );
        assert_eq!(
            CompressionArg::Auto.resolve("users.jsonl"),
            Compression::None
        );
        // A stream (`-`) cannot be sniffed.
        assert_eq!(CompressionArg::Auto.resolve("-"), Compression::None);
        // Explicit choice wins over the extension.
        assert_eq!(
            CompressionArg::None.resolve("users.jsonl.gz"),
            Compression::None
        );
        assert_eq!(CompressionArg::Zstd.resolve("-"), Compression::Zstd);
    }
}
