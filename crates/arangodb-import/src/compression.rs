//! Transparent decompression of import inputs.
//!
//! Compression is detected from the file extension (`.gz`, `.zst`) or set
//! explicitly. [`decompress`] wraps any byte reader in the matching streaming
//! decoder, so the readers downstream see plain bytes and never buffer the
//! whole input. This composes with [`crate::read_documents`]: feed it
//! `decompress(compression, raw_reader)`.

use std::path::Path;

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use tokio::io::{AsyncRead, BufReader};

/// A supported input compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression; bytes are passed through unchanged.
    #[default]
    None,
    /// gzip (RFC 1952).
    Gzip,
    /// Zstandard.
    Zstd,
}

impl Compression {
    /// Maps a lowercase extension (without the leading dot) to a codec.
    ///
    /// Returns `None` for extensions that are not compression markers, so the
    /// caller can treat them as part of the data filename.
    #[must_use]
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "gz" | "gzip" => Some(Self::Gzip),
            "zst" | "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Detects compression from a path's final extension, defaulting to
    /// [`Compression::None`] when the extension is absent or unrecognized.
    #[must_use]
    pub fn infer_from_path(path: impl AsRef<Path>) -> Self {
        path.as_ref()
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .and_then(|ext| Self::from_extension(&ext))
            .unwrap_or(Self::None)
    }
}

/// Wraps `reader` in the streaming decoder for `compression`.
///
/// For [`Compression::None`] the reader is returned unchanged (boxed). The
/// returned reader yields decompressed bytes and can be handed directly to
/// [`crate::read_documents`].
pub fn decompress<R>(compression: Compression, reader: R) -> Box<dyn AsyncRead + Unpin + Send>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    match compression {
        Compression::None => Box::new(reader),
        // Decoders take an `AsyncBufRead`; the inner `BufReader` supplies it.
        Compression::Gzip => Box::new(GzipDecoder::new(BufReader::new(reader))),
        Compression::Zstd => Box::new(ZstdDecoder::new(BufReader::new(reader))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn detects_codecs_by_extension() {
        assert_eq!(Compression::from_extension("gz"), Some(Compression::Gzip));
        assert_eq!(Compression::from_extension("zst"), Some(Compression::Zstd));
        assert_eq!(Compression::from_extension("jsonl"), None);
    }

    #[test]
    fn infers_from_path() {
        assert_eq!(
            Compression::infer_from_path("users.jsonl.gz"),
            Compression::Gzip
        );
        assert_eq!(
            Compression::infer_from_path("users.jsonl.ZST"),
            Compression::Zstd
        );
        assert_eq!(
            Compression::infer_from_path("users.jsonl"),
            Compression::None
        );
        assert_eq!(Compression::infer_from_path("noext"), Compression::None);
    }

    #[tokio::test]
    async fn passthrough_returns_input_unchanged() {
        let mut reader = decompress(Compression::None, &b"hello"[..]);
        let mut out = String::new();
        reader.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "hello");
    }

    #[tokio::test]
    async fn round_trips_gzip() {
        use async_compression::tokio::write::GzipEncoder;
        use tokio::io::AsyncWriteExt;

        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(b"line1\nline2\n").await.unwrap();
        encoder.shutdown().await.unwrap();
        let compressed = encoder.into_inner();

        let mut reader = decompress(Compression::Gzip, std::io::Cursor::new(compressed));
        let mut out = String::new();
        reader.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "line1\nline2\n");
    }

    #[tokio::test]
    async fn round_trips_zstd() {
        use async_compression::tokio::write::ZstdEncoder;
        use tokio::io::AsyncWriteExt;

        let mut encoder = ZstdEncoder::new(Vec::new());
        encoder.write_all(b"alpha\nbeta\n").await.unwrap();
        encoder.shutdown().await.unwrap();
        let compressed = encoder.into_inner();

        let mut reader = decompress(Compression::Zstd, std::io::Cursor::new(compressed));
        let mut out = String::new();
        reader.read_to_string(&mut out).await.unwrap();
        assert_eq!(out, "alpha\nbeta\n");
    }
}
