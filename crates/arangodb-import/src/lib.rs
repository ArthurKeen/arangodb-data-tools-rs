//! Bulk import pipeline for ArangoDB.
//!
//! Streaming readers (CSV/TSV/JSON/JSONL), a unified byte+document batcher,
//! and a concurrent sender pool over `/_api/import` with bounded
//! backpressure. See `docs/IMPLEMENTATION_PLAN.md` (section 4, phase 1).
//!
//! The stages compose left to right:
//!
//! - [`format`]: input-format detection.
//! - [`reader`]: streaming decoders that normalize every format into a stream
//!   of [`serde_json::Value`] documents.
//! - [`batch`]: byte- and document-bounded batching into `/_api/import`
//!   bodies.
//! - [`sender`]: [`run_import`] drives batches through `N` concurrent senders
//!   under a global in-flight-byte cap.

pub mod batch;
pub mod format;
pub mod reader;
pub mod sender;

pub use arangodb_client::{ImportOptions, ImportResult, OnDuplicate};
pub use batch::{into_batches, Batch};
pub use format::ImportFormat;
pub use reader::{read_documents, DocumentStream};
pub use sender::{run_import, ArangoBatchSender, BatchSender, ImportSummary};
