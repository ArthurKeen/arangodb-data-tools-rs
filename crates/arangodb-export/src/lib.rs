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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arangodb_client::{ArangoClient, CursorRequest};
use arangodb_storage::{
    compress, ByteStream, Compression, ObjectMetadata, ObjectPath, ObjectStore,
};
use arangodb_tools_core::progress::{ProgressEvent, ProgressSink, ProgressSnapshot};
use arangodb_tools_core::Result;
use futures::{Stream, StreamExt};
use serde_json::Value;

pub use cursor_stream::document_stream;
pub use encode::encode;
pub use format::ExportFormat;
pub use split::{run_split_export, run_split_export_with_progress, ManifestMeta};

/// A stream of exported documents.
pub type DocumentStream = Pin<Box<dyn Stream<Item = Result<Value>> + Send>>;

/// How often a periodic [`ProgressEvent::Progress`] is emitted when a progress
/// sink is supplied to [`run_export_with_progress`].
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

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
    run_export_with_progress(
        client,
        request,
        format,
        fields,
        compression,
        store,
        path,
        None,
    )
    .await
}

/// Runs an export, emitting periodic [`ProgressEvent::Progress`] snapshots of
/// bytes written (about once per second) through `progress` when it is `Some`.
/// Lifecycle (`started`/`finished`) events are the caller's responsibility.
///
/// # Errors
/// Returns an error if the query fails, encoding fails (e.g. CSV without
/// fields), or the write fails.
#[allow(clippy::too_many_arguments)]
pub async fn run_export_with_progress(
    client: &ArangoClient,
    request: CursorRequest,
    format: ExportFormat,
    fields: Option<Vec<String>>,
    compression: Compression,
    store: &dyn ObjectStore,
    path: &ObjectPath,
    progress: Option<Arc<dyn ProgressSink>>,
) -> Result<ObjectMetadata> {
    let documents = document_stream(client.clone(), request);
    let encoded: ByteStream = encode(format, fields, documents)?;
    let compressed = compress(compression, encoded);

    // When a sink is supplied, count bytes as they stream to the store and have
    // a ticker emit periodic snapshots; otherwise write the stream untouched.
    let counter = Arc::new(AtomicU64::new(0));
    let ticker = progress.as_ref().map(|sink| {
        let sink = Arc::clone(sink);
        let counter = Arc::clone(&counter);
        let started = Instant::now();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(PROGRESS_INTERVAL);
            // Skip the immediate first tick so progress reflects real work.
            interval.tick().await;
            loop {
                interval.tick().await;
                sink.emit(&ProgressEvent::Progress(ProgressSnapshot {
                    bytes_written: counter.load(Ordering::Relaxed),
                    elapsed_secs: started.elapsed().as_secs_f64(),
                    ..ProgressSnapshot::default()
                }));
            }
        })
    });

    let output: ByteStream = if progress.is_some() {
        count_bytes(compressed, Arc::clone(&counter))
    } else {
        compressed
    };

    let result = store.put_stream(path, output).await;

    if let Some(ticker) = ticker {
        ticker.abort();
    }
    result
}

/// Forwards a byte stream while adding each chunk's length to `counter`.
fn count_bytes(input: ByteStream, counter: Arc<AtomicU64>) -> ByteStream {
    Box::pin(input.map(move |chunk| {
        if let Ok(bytes) = &chunk {
            counter.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        chunk
    }))
}
