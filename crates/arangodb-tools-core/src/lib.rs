//! Shared foundations for the ArangoDB data tools.
//!
//! This crate holds the cross-cutting building blocks used by every other
//! crate in the workspace:
//!
//! - [`error`]: the error taxonomy and rich error context.
//! - [`retry`]: retry classification and a backoff-driven retry helper.
//! - [`config`]: shared connection, TLS, batching, and concurrency config.
//! - [`progress`]: the progress-event schema and counters.
//! - [`manifest`]: the canonical dump/export manifest model.
//! - [`redact`]: the [`Secret`] wrapper for credentials.
//!
//! See `docs/IMPLEMENTATION_PLAN.md` (section 3) for the broader design.

/// The crate README, compiled as doctests so its examples stay in sync with the
/// API. `#[cfg(doctest)]` keeps this helper out of the rendered documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

pub mod config;
pub mod error;
pub mod manifest;
pub mod progress;
pub mod redact;
pub mod retry;

pub use error::{Error, ErrorContext, Result};
pub use redact::Secret;
pub use retry::{retry, RetryPolicy, Retryable};
