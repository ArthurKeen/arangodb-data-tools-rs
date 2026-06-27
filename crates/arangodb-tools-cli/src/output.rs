//! Output rendering for the CLI.
//!
//! The library crates never write to stdout; the CLI owns presentation. Two
//! modes are supported:
//!
//! - [`OutputMode::Text`] (default): human-readable summaries on stdout.
//! - [`OutputMode::Json`]: a single machine-readable result object on stdout
//!   plus newline-delimited [`ProgressEvent`]s on stderr. This is the mode
//!   intended for programmatic callers (e.g. Python/Go driving the CLI as a
//!   subprocess): parse stdout for the result, read stderr line-by-line for
//!   progress, and use the process exit code for success/failure.
//!
//! Lifecycle events (`started`/`finished`) are emitted today. Periodic
//! mid-run `progress` events can be layered on once the job pipelines accept a
//! progress sink; [`Reporter::progress`] already renders them.

use std::io::Write;

use arangodb_tools_core::progress::{ProgressEvent, ProgressSnapshot};
use clap::ValueEnum;
use serde_json::Value;

/// How the CLI renders results and progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub(crate) enum OutputMode {
    /// Human-readable text on stdout (default).
    #[default]
    Text,
    /// A machine-readable JSON result on stdout, with newline-delimited
    /// progress events on stderr.
    Json,
}

/// Renders progress events and final results according to the selected mode.
///
/// `Reporter` is cheap to copy and is threaded into each subcommand.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Reporter {
    mode: OutputMode,
}

impl Reporter {
    /// Creates a reporter for the given mode.
    pub(crate) fn new(mode: OutputMode) -> Self {
        Self { mode }
    }

    /// Whether the reporter is emitting machine-readable JSON.
    pub(crate) fn is_json(self) -> bool {
        self.mode == OutputMode::Json
    }

    /// Emits a structured progress event. In text mode this is a no-op (text
    /// mode prints only the final human summary); in JSON mode it writes one
    /// NDJSON line to stderr.
    pub(crate) fn event(self, event: &ProgressEvent) {
        if self.mode == OutputMode::Json {
            if let Ok(line) = serde_json::to_string(event) {
                let mut err = std::io::stderr().lock();
                let _ = writeln!(err, "{line}");
            }
        }
    }

    /// Emits a `started` lifecycle event for `operation` (e.g. `"import"`).
    pub(crate) fn started(self, operation: &str) {
        self.event(&ProgressEvent::Started {
            operation: operation.to_owned(),
        });
    }

    /// Emits a periodic `progress` event. Currently unused by the pipelines but
    /// kept so live progress can be wired in without touching presentation.
    #[allow(dead_code)]
    pub(crate) fn progress(self, snapshot: ProgressSnapshot) {
        self.event(&ProgressEvent::Progress(snapshot));
    }

    /// Emits a `finished` lifecycle event carrying final totals.
    pub(crate) fn finished(self, snapshot: ProgressSnapshot) {
        self.event(&ProgressEvent::Finished(snapshot));
    }

    /// Renders the final result. In text mode the `text` closure is printed to
    /// stdout; in JSON mode the `json` closure's value is printed as a single
    /// line to stdout. Only the closure for the active mode is evaluated.
    pub(crate) fn result(self, text: impl FnOnce() -> String, json: impl FnOnce() -> Value) {
        match self.mode {
            OutputMode::Text => println!("{}", text()),
            OutputMode::Json => println!("{}", json()),
        }
    }
}
