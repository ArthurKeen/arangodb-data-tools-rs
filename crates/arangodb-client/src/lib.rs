//! ArangoDB HTTP client.
//!
//! [`ArangoClient`] wraps a `reqwest` client with connection/auth/TLS
//! configuration and an enforced [retry policy](arangodb_tools_core::RetryPolicy),
//! so every request goes through the same retry/backoff path. This crate
//! currently implements the `/_api/version` check and the `/_api/import` bulk
//! document endpoint; further endpoint families (database, collection, cursor,
//! dump, replication) are added in later phases per
//! `docs/IMPLEMENTATION_PLAN.md`.

pub mod client;
pub mod import;
pub mod version;

pub use client::{ArangoClient, ArangoClientBuilder};
pub use import::{ImportOptions, ImportResult, OnDuplicate};
pub use version::VersionInfo;
