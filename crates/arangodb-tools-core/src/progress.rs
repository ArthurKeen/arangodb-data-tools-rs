//! Progress-event schema and counters.
//!
//! The library emits structured [`ProgressEvent`]s through a [`ProgressSink`];
//! the CLI is responsible for rendering them. Pipelines update a shared
//! [`ProgressCounters`] cheaply and produce a [`ProgressSnapshot`] on demand.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::Serialize;

/// A point-in-time view of pipeline progress.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProgressSnapshot {
    /// Bytes read from the source.
    pub bytes_read: u64,
    /// Bytes written to the destination.
    pub bytes_written: u64,
    /// Documents processed.
    pub documents: u64,
    /// Batches sent.
    pub batches: u64,
    /// Server-reported errors encountered.
    pub server_errors: u64,
    /// Retries performed.
    pub retries: u64,
    /// Elapsed wall-clock time in seconds.
    pub elapsed_secs: f64,
}

impl ProgressSnapshot {
    /// Documents processed per second, or `0.0` before any time has elapsed.
    #[must_use]
    pub fn docs_per_sec(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.documents as f64 / self.elapsed_secs
        } else {
            0.0
        }
    }

    /// Bytes read per second, or `0.0` before any time has elapsed.
    #[must_use]
    pub fn bytes_per_sec(&self) -> f64 {
        if self.elapsed_secs > 0.0 {
            self.bytes_read as f64 / self.elapsed_secs
        } else {
            0.0
        }
    }
}

/// A structured progress event emitted by a pipeline.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ProgressEvent {
    /// The operation has started.
    Started {
        /// A short operation label, e.g. `"import"`.
        operation: String,
    },
    /// A periodic progress update.
    Progress(ProgressSnapshot),
    /// A non-fatal warning.
    Warning {
        /// The warning message.
        message: String,
    },
    /// The operation finished, with final totals.
    Finished(ProgressSnapshot),
}

/// A consumer of [`ProgressEvent`]s.
pub trait ProgressSink: Send + Sync {
    /// Handles a single progress event.
    fn emit(&self, event: &ProgressEvent);
}

/// A sink that discards all events.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopSink;

impl ProgressSink for NoopSink {
    fn emit(&self, _event: &ProgressEvent) {}
}

/// Cheap, thread-safe counters that pipelines increment as work proceeds.
#[derive(Debug, Default)]
pub struct ProgressCounters {
    /// Bytes read from the source.
    pub bytes_read: AtomicU64,
    /// Bytes written to the destination.
    pub bytes_written: AtomicU64,
    /// Documents processed.
    pub documents: AtomicU64,
    /// Batches sent.
    pub batches: AtomicU64,
    /// Server-reported errors encountered.
    pub server_errors: AtomicU64,
    /// Retries performed.
    pub retries: AtomicU64,
}

impl ProgressCounters {
    /// Creates a fresh set of zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds to the bytes-read counter.
    pub fn add_bytes_read(&self, n: u64) {
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
    }

    /// Adds to the bytes-written counter.
    pub fn add_bytes_written(&self, n: u64) {
        self.bytes_written.fetch_add(n, Ordering::Relaxed);
    }

    /// Adds to the documents counter.
    pub fn add_documents(&self, n: u64) {
        self.documents.fetch_add(n, Ordering::Relaxed);
    }

    /// Increments the batches counter.
    pub fn inc_batches(&self) {
        self.batches.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds to the retries counter.
    pub fn add_retries(&self, n: u64) {
        self.retries.fetch_add(n, Ordering::Relaxed);
    }

    /// Captures a [`ProgressSnapshot`] using `started` as the elapsed-time base.
    #[must_use]
    pub fn snapshot(&self, started: Instant) -> ProgressSnapshot {
        ProgressSnapshot {
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            documents: self.documents.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            server_errors: self.server_errors.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            elapsed_secs: started.elapsed().as_secs_f64(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate_into_snapshot() {
        let counters = ProgressCounters::new();
        counters.add_documents(10);
        counters.add_bytes_read(2048);
        counters.inc_batches();
        let snap = counters.snapshot(Instant::now());
        assert_eq!(snap.documents, 10);
        assert_eq!(snap.bytes_read, 2048);
        assert_eq!(snap.batches, 1);
    }

    #[test]
    fn rates_are_zero_without_elapsed_time() {
        let snap = ProgressSnapshot {
            documents: 100,
            ..ProgressSnapshot::default()
        };
        assert_eq!(snap.docs_per_sec(), 0.0);
    }

    #[test]
    fn progress_event_serializes_with_tag() {
        let event = ProgressEvent::Started {
            operation: "import".to_owned(),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"event\":\"started\""));
        assert!(json.contains("\"operation\":\"import\""));
    }

    #[test]
    fn noop_sink_accepts_events() {
        let sink = NoopSink;
        sink.emit(&ProgressEvent::Warning {
            message: "careful".to_owned(),
        });
    }
}
