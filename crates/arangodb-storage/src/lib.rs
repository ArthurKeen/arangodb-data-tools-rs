//! Storage abstraction for the ArangoDB data tools.
//!
//! Defines the `ObjectStore` trait and backends (local filesystem first, then
//! S3-compatible, GCS, Azure, and SeaweedFS) plus storage-URI parsing and
//! streaming read/write. See `docs/IMPLEMENTATION_PLAN.md` (section 4, phases
//! 2 and 7) for the planned backends.
