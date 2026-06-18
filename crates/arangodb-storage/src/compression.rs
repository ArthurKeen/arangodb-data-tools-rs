//! Streaming gzip/zstd compression and decompression.
//!
//! [`decompress`] wraps a byte reader (import inputs) and [`compress`] wraps a
//! byte stream (export outputs); both stream incrementally so neither side
//! buffers the whole payload. Shared here because import and export both need
//! the codec and both already depend on this crate.

use std::path::Path;

use arangodb_tools_core::Error;
use async_compression::tokio::bufread::{GzipDecoder, GzipEncoder, ZstdDecoder, ZstdEncoder};
use futures::StreamExt;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::store::ByteStream;

/// A supported compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// No compression; bytes pass through unchanged.
    #[default]
    None,
    /// gzip (RFC 1952).
    Gzip,
    /// Zstandard.
    Zstd,
}

impl Compression {
    /// Maps a lowercase extension (without the leading dot) to a codec, or
    /// `None` for extensions that are not compression markers.
    #[must_use]
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "gz" | "gzip" => Some(Self::Gzip),
            "zst" | "zstd" => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Detects compression from a path's final extension, defaulting to
    /// [`Compression::None`].
    #[must_use]
    pub fn infer_from_path(path: impl AsRef<Path>) -> Self {
        path.as_ref()
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .and_then(|ext| Self::from_extension(&ext))
            .unwrap_or(Self::None)
    }

    /// The conventional file-extension suffix for this codec (without a
    /// leading dot), or `None` for [`Compression::None`].
    #[must_use]
    pub fn extension(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gz"),
            Self::Zstd => Some("zst"),
        }
    }
}

/// Wraps `reader` in the streaming decoder for `compression`.
///
/// For [`Compression::None`] the reader is returned boxed and unchanged.
pub fn decompress<R>(compression: Compression, reader: R) -> Box<dyn AsyncRead + Unpin + Send>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    match compression {
        Compression::None => Box::new(reader),
        Compression::Gzip => Box::new(GzipDecoder::new(BufReader::new(reader))),
        Compression::Zstd => Box::new(ZstdDecoder::new(BufReader::new(reader))),
    }
}

/// Wraps a byte stream in the streaming encoder for `compression`.
///
/// For [`Compression::None`] the stream is returned unchanged.
pub fn compress(compression: Compression, input: ByteStream) -> ByteStream {
    match compression {
        Compression::None => input,
        Compression::Gzip => {
            let reader = GzipEncoder::new(BufReader::new(byte_reader(input)));
            Box::pin(ReaderStream::new(reader).map(|chunk| chunk.map_err(Error::from)))
        }
        Compression::Zstd => {
            let reader = ZstdEncoder::new(BufReader::new(byte_reader(input)));
            Box::pin(ReaderStream::new(reader).map(|chunk| chunk.map_err(Error::from)))
        }
    }
}

/// Adapts a [`ByteStream`] into an [`AsyncRead`] for the encoders.
fn byte_reader(input: ByteStream) -> impl AsyncRead + Unpin + Send {
    StreamReader::new(
        input.map(|chunk| chunk.map_err(|err| std::io::Error::other(err.to_string()))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::io::AsyncReadExt;

    fn stream(data: &'static [u8]) -> ByteStream {
        Box::pin(futures::stream::once(async move {
            Ok(Bytes::from_static(data))
        }))
    }

    async fn read_all(mut reader: impl AsyncRead + Unpin) -> Vec<u8> {
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        out
    }

    async fn collect(mut s: ByteStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = s.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[test]
    fn detects_and_maps_extensions() {
        assert_eq!(Compression::from_extension("gz"), Some(Compression::Gzip));
        assert_eq!(Compression::from_extension("zst"), Some(Compression::Zstd));
        assert_eq!(Compression::from_extension("jsonl"), None);
        assert_eq!(
            Compression::infer_from_path("a.jsonl.gz"),
            Compression::Gzip
        );
        assert_eq!(Compression::Gzip.extension(), Some("gz"));
        assert_eq!(Compression::None.extension(), None);
    }

    #[tokio::test]
    async fn gzip_compress_decompress_round_trip() {
        let original = b"line1\nline2\nline3\n";
        let compressed = collect(compress(Compression::Gzip, stream(original))).await;
        assert_ne!(compressed, original);
        let restored = read_all(decompress(
            Compression::Gzip,
            std::io::Cursor::new(compressed),
        ))
        .await;
        assert_eq!(restored, original);
    }

    #[tokio::test]
    async fn zstd_compress_decompress_round_trip() {
        let original = b"alpha\nbeta\ngamma\n";
        let compressed = collect(compress(Compression::Zstd, stream(original))).await;
        let restored = read_all(decompress(
            Compression::Zstd,
            std::io::Cursor::new(compressed),
        ))
        .await;
        assert_eq!(restored, original);
    }

    #[tokio::test]
    async fn none_is_passthrough() {
        let restored = collect(compress(Compression::None, stream(b"plain"))).await;
        assert_eq!(restored, b"plain");
    }
}
