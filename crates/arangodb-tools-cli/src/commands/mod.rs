//! CLI subcommand implementations.

pub(crate) mod connection;
pub(crate) mod dump;
pub(crate) mod export;
pub(crate) mod import;
pub(crate) mod rdf;
pub(crate) mod restore;

use std::path::Path;

use arangodb_storage::{
    Compression, LocalFileSystem, ObjectPath, ObjectStore, ObjectStoreBackend, StorageUri,
};
use arangodb_tools_core::{Error, Result};
use clap::ValueEnum;

/// Resolves a dump *root* (a directory or object-store prefix that holds many
/// artifacts) into a store. Accepts a path, a `file://` URI, or an
/// object-storage URI (`s3://`, `gs://`, `az://`, `seaweed+s3://`), with the
/// URI's path used as the key prefix. Used by `dump` (writes) and `restore`
/// (reads).
pub(crate) fn open_store_root(location: &str) -> Result<Box<dyn ObjectStore>> {
    if let Some((scheme, _)) = location.split_once("://") {
        if scheme == "file" {
            return Ok(Box::new(LocalFileSystem::new(Path::new(
                location.trim_start_matches("file://"),
            ))));
        }
        let parsed = StorageUri::parse(location)?;
        return Ok(Box::new(ObjectStoreBackend::for_prefix(&parsed)?));
    }
    Ok(Box::new(LocalFileSystem::new(Path::new(location))))
}

/// Resolves a single-object location into a store and the object's path within
/// it. Accepts a filesystem path, a `file://` URI, or an object-storage URI
/// (`s3://`, `gs://`, `az://`, `seaweed+s3://`). For local paths the store is
/// rooted at the parent directory and the object path is the file name.
pub(crate) fn open_object(location: &str) -> Result<(Box<dyn ObjectStore>, ObjectPath)> {
    if let Some((scheme, _)) = location.split_once("://") {
        if scheme == "file" {
            return open_local_object(Path::new(location.trim_start_matches("file://")));
        }
        let parsed = StorageUri::parse(location)?;
        let backend = ObjectStoreBackend::for_bucket(&parsed)?;
        return Ok((Box::new(backend), ObjectPath::new(parsed.path)));
    }
    open_local_object(Path::new(location))
}

/// Roots a [`LocalFileSystem`] at a path's parent directory and returns it with
/// the file name as the object path.
fn open_local_object(path: &Path) -> Result<(Box<dyn ObjectStore>, ObjectPath)> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::config(format!("path has no file name: {}", path.display())))?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    Ok((
        Box::new(LocalFileSystem::new(parent)),
        ObjectPath::new(file_name.to_string()),
    ))
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
