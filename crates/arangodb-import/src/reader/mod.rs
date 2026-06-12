//! Streaming readers that normalize each input format into a stream of JSON
//! documents.
//!
//! Every reader takes an [`AsyncRead`] and yields one [`serde_json::Value`] per
//! source document, parsing incrementally so input is never fully buffered in
//! memory. The downstream batcher and sender treat all formats uniformly.

mod delimited;
mod json_array;
mod jsonl;

use std::pin::Pin;

use arangodb_tools_core::Result;
use futures::Stream;
use serde_json::Value;
use tokio::io::AsyncRead;

use crate::format::ImportFormat;

/// A stream of parsed source documents.
///
/// Each item is either a decoded document or a [`arangodb_tools_core::Error`]
/// (typically [`arangodb_tools_core::Error::Parse`]) carrying position context.
pub type DocumentStream = Pin<Box<dyn Stream<Item = Result<Value>> + Send>>;

/// Reads `reader` as `format`, producing a stream of documents.
///
/// The returned stream borrows nothing and can be driven on any task.
pub fn read_documents<R>(format: ImportFormat, reader: R) -> DocumentStream
where
    R: AsyncRead + Unpin + Send + 'static,
{
    match format {
        ImportFormat::JsonLines => jsonl::read(reader),
        ImportFormat::JsonArray => json_array::read(reader),
        ImportFormat::Csv | ImportFormat::Tsv => {
            // `delimiter()` is always `Some` for CSV/TSV.
            delimited::read(reader, format.delimiter().unwrap_or(b','))
        }
    }
}
