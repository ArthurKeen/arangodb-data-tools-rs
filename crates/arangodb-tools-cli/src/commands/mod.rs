//! CLI subcommand implementations.

pub(crate) mod connection;
pub(crate) mod export;
pub(crate) mod import;

use arangodb_storage::Compression;
use clap::ValueEnum;

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
