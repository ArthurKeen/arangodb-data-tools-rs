//! Database dump for ArangoDB.
//!
//! Inventory retrieval, structure/index/view metadata, parallel data dump via
//! `/_api/dump/*` (with a legacy replication fallback), a canonical manifest,
//! server-handle keep-alive, and per-collection/shard resume checkpoints. See
//! `docs/IMPLEMENTATION_PLAN.md` (section 4, phases 4 and 5).
