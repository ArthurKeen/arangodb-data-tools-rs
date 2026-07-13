//! Bulk import pipeline for ArangoDB.
//!
//! Streaming readers (CSV/TSV/JSON/JSONL), a unified byte+document batcher,
//! and a concurrent sender pool over `/_api/import` with bounded
//! backpressure. See `docs/IMPLEMENTATION_PLAN.md` (section 4, phase 1).
//!
//! The stages compose left to right:
//!
//! - [`format`]: input-format detection.
//! - compression: transparent gzip/zstd decoding of the input bytes (the
//!   codec lives in `arangodb-storage`; [`decompress`]/[`Compression`] are
//!   re-exported here for convenience).
//! - [`reader`]: streaming decoders that normalize every format into a stream
//!   of [`serde_json::Value`] documents.
//! - [`edge`]: optional `_from`/`_to` preflight for edge-collection imports.
//! - [`batch`]: byte- and document-bounded batching into `/_api/import`
//!   bodies.
//! - [`sender`]: [`run_import`] drives batches through `N` concurrent senders
//!   under a global in-flight-byte cap.

/// The crate README, compiled as doctests so its examples stay in sync with the
/// API. `#[cfg(doctest)]` keeps this helper out of the rendered documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod adaptive;
pub mod batch;
pub mod edge;
pub mod format;
pub mod reader;
pub mod sender;

pub use adaptive::{AdaptiveConfig, AdaptiveLimiter, BatchingMetrics};
pub use arangodb_client::{ImportOptions, ImportResult, OnDuplicate};
pub use arangodb_storage::{decompress, Compression};
pub use arangodb_tools_core::manifest::ImportCheckpoint;
pub use batch::{into_batches, Batch};
pub use edge::validate_edge_documents;
pub use format::ImportFormat;
pub use reader::{read_documents, DocumentStream};
pub use sender::{
    load_checkpoint, run_import, run_import_with_checkpoint, ArangoBatchSender, BatchSender,
    CheckpointConfig, ImportSummary,
};
