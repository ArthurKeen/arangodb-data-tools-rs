//! ArangoDB HTTP client.
//!
//! [`ArangoClient`] wraps a `reqwest` client with connection/auth/TLS
//! configuration and an enforced [retry policy](arangodb_tools_core::RetryPolicy),
//! so every request goes through the same retry/backoff path. This crate
//! currently implements `/_api/version`, `/_api/import`, `/_api/collection`,
//! and `/_api/cursor`; further endpoint families (database, dump, replication)
//! are added in later phases per `docs/IMPLEMENTATION_PLAN.md`.

/// The crate README, compiled as doctests so its examples stay in sync with the
/// API. `#[cfg(doctest)]` keeps this helper out of the rendered documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod client;
pub mod collection;
pub mod cursor;
pub mod import;
pub mod replication;
pub mod topology;
pub mod version;

pub use client::{ArangoClient, ArangoClientBuilder};
pub use collection::{CollectionInfo, CollectionKind};
pub use cursor::{CursorBatch, CursorOptions, CursorRequest};
pub use import::{ImportOptions, ImportResult, OnDuplicate};
pub use replication::{DumpChunk, Inventory, InventoryCollection};
pub use topology::ServerRole;
pub use version::VersionInfo;
