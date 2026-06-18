//! Export pipeline for ArangoDB.
//!
//! Cursor-based collection and custom-AQL export with streaming output to
//! storage. The pipeline mirrors import in reverse: `/_api/cursor` is the
//! source, an [`ObjectStore`] is the sink.
//!
//! ```text
//! cursor  ->  encode (jsonl/json/csv)  ->  compress  ->  ObjectStore.put_stream
//! ```
//!
//! Each stage streams, so a whole-collection export stays bounded by the
//! cursor batch size rather than the collection size. See
//! `docs/IMPLEMENTATION_PLAN.md` (section 4, phase 3).

pub mod encode;
pub mod format;
pub mod split;

mod cursor_stream;

use std::pin::Pin;

use arangodb_client::{ArangoClient, CursorRequest};
use arangodb_storage::{
    compress, ByteStream, Compression, ObjectMetadata, ObjectPath, ObjectStore,
};
use arangodb_tools_core::Result;
use futures::Stream;
use serde_json::Value;

pub use cursor_stream::document_stream;
pub use encode::encode;
pub use format::ExportFormat;
pub use split::{run_split_export, ManifestMeta};

/// A stream of exported documents.
pub type DocumentStream = Pin<Box<dyn Stream<Item = Result<Value>> + Send>>;

/// Builds a streaming cursor request that returns every document in
/// `collection`, using a bind parameter so the name cannot inject AQL.
#[must_use]
pub fn collection_query(collection: &str, batch_size: u32) -> CursorRequest {
    CursorRequest::new("FOR doc IN @@collection RETURN doc")
        .with_bind_vars(serde_json::json!({ "@collection": collection }))
        .with_batch_size(batch_size)
}

/// Runs an export: streams `request`'s results, encodes them as `format`,
/// optionally compresses, and writes a single object to `store` at `path`.
///
/// `fields` is required for [`ExportFormat::Csv`]. Returns the written
/// object's metadata.
///
/// # Errors
/// Returns an error if the query fails, encoding fails (e.g. CSV without
/// fields), or the write fails.
pub async fn run_export(
    client: &ArangoClient,
    request: CursorRequest,
    format: ExportFormat,
    fields: Option<Vec<String>>,
    compression: Compression,
    store: &dyn ObjectStore,
    path: &ObjectPath,
) -> Result<ObjectMetadata> {
    let documents = document_stream(client.clone(), request);
    let encoded: ByteStream = encode(format, fields, documents)?;
    let output = compress(compression, encoded);
    store.put_stream(path, output).await
}
