//! Bulk import pipeline for ArangoDB.
//!
//! Streaming readers (CSV/TSV/JSON/JSONL), a unified byte+document batcher, and
//! (in a later change) an async sender pool over `/_api/import` with bounded
//! backpressure. See `docs/IMPLEMENTATION_PLAN.md` (section 4, phase 1).
//!
//! The building blocks here are deliberately network-free and independently
//! testable:
//!
//! - [`format`]: input-format detection and duplicate-handling modes.
//! - [`reader`]: streaming decoders that normalize every format into a stream
//!   of [`serde_json::Value`] documents.
//! - [`batch`]: byte- and document-bounded batching into `/_api/import` bodies.

pub mod batch;
pub mod format;
pub mod reader;

pub use batch::{into_batches, Batch};
pub use format::{ImportFormat, OnDuplicate};
pub use reader::{read_documents, DocumentStream};

/// Sends a prepared `Batch` to ArangoDB using the provided client and
/// collection name. This is a minimal helper that delegates to
/// `ArangoClient::import_raw` and returns an error on failure.
pub async fn send_batch(
    client: &arangodb_client::ArangoClient,
    collection: &str,
    batch: &Batch,
) -> arangodb_tools_core::Result<()> {
    let _ = client.import_raw(collection, &batch.body).await?;
    Ok(())
}
