//! Bulk import pipeline for ArangoDB.
//!
//! Streaming readers (CSV/TSV/JSON/JSONL), a unified byte+document batcher, and
//! an async sender pool over `/_api/import` with bounded backpressure. See
//! `docs/IMPLEMENTATION_PLAN.md` (section 4, phase 1).
