//! Request options and response statistics for `/_api/import`.

use serde::Deserialize;

/// How the server treats documents whose `_key` already exists.
///
/// Mirrors the `onDuplicate` query parameter of `/_api/import`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnDuplicate {
    /// Report a unique-constraint error and count the document as failed.
    #[default]
    Error,
    /// Patch the existing document with the new attributes.
    Update,
    /// Replace the existing document entirely.
    Replace,
    /// Silently skip the document.
    Ignore,
}

impl OnDuplicate {
    /// The query-parameter value understood by `/_api/import`.
    #[must_use]
    pub fn as_query_value(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Update => "update",
            Self::Replace => "replace",
            Self::Ignore => "ignore",
        }
    }
}

/// Options for a bulk document import (`POST /_api/import?type=documents`).
#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// Target collection name.
    pub collection: String,
    /// Duplicate-key handling.
    pub on_duplicate: OnDuplicate,
    /// Wait for the data to be synced to disk before the server responds.
    pub wait_for_sync: bool,
    /// All-or-nothing: if any document in a request fails, fail the request.
    pub complete: bool,
    /// Ask the server for per-document error details in the response.
    pub details: bool,
    /// Server-side prefix applied to unqualified `_from` values (edge imports).
    pub from_prefix: Option<String>,
    /// Server-side prefix applied to unqualified `_to` values (edge imports).
    pub to_prefix: Option<String>,
    /// Truncate the collection before importing. Only meaningful on the first
    /// request of an import, and non-atomic: a failure mid-import leaves the
    /// collection truncated (PRD §8.2).
    pub overwrite: bool,
}

impl ImportOptions {
    /// Creates options targeting `collection`, with defaults otherwise.
    #[must_use]
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            on_duplicate: OnDuplicate::default(),
            wait_for_sync: false,
            complete: false,
            details: false,
            from_prefix: None,
            to_prefix: None,
            overwrite: false,
        }
    }
}

/// Statistics returned by `/_api/import` for one request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ImportResult {
    /// Documents imported.
    #[serde(default)]
    pub created: u64,
    /// Documents that failed.
    #[serde(default)]
    pub errors: u64,
    /// Empty input lines (JSONL bodies only).
    #[serde(default)]
    pub empty: u64,
    /// Documents updated (`onDuplicate=update`).
    #[serde(default)]
    pub updated: u64,
    /// Documents skipped (`onDuplicate=ignore`).
    #[serde(default)]
    pub ignored: u64,
    /// Per-document error messages, present when requested via `details`.
    #[serde(default)]
    pub details: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_duplicate_query_values() {
        assert_eq!(OnDuplicate::default(), OnDuplicate::Error);
        assert_eq!(OnDuplicate::Error.as_query_value(), "error");
        assert_eq!(OnDuplicate::Update.as_query_value(), "update");
        assert_eq!(OnDuplicate::Replace.as_query_value(), "replace");
        assert_eq!(OnDuplicate::Ignore.as_query_value(), "ignore");
    }

    #[test]
    fn parses_server_response() {
        let body = r#"{"error":false,"created":10,"errors":2,"empty":1,"updated":0,"ignored":3,"details":["boom"]}"#;
        let result: ImportResult = serde_json::from_str(body).unwrap();
        assert_eq!(result.created, 10);
        assert_eq!(result.errors, 2);
        assert_eq!(result.empty, 1);
        assert_eq!(result.ignored, 3);
        assert_eq!(result.details, vec!["boom".to_owned()]);
    }

    #[test]
    fn missing_fields_default_to_zero() {
        let result: ImportResult = serde_json::from_str(r#"{"created":5}"#).unwrap();
        assert_eq!(result.created, 5);
        assert_eq!(result.errors, 0);
        assert!(result.details.is_empty());
    }
}
