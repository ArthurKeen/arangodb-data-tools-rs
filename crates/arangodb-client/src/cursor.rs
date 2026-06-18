//! AQL cursor request/response types for `/_api/cursor`.
//!
//! Bind variables and the query text can contain sensitive data, so these
//! types deliberately do not derive `Debug` in a way that prints them; callers
//! must not log a [`CursorRequest`] (PRD §17).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A request to open an AQL cursor.
#[derive(Clone, Serialize)]
pub struct CursorRequest {
    /// The AQL query text.
    pub query: String,
    /// Optional bind parameters.
    #[serde(rename = "bindVars", skip_serializing_if = "Option::is_none")]
    pub bind_vars: Option<Value>,
    /// Server-side result batch size.
    #[serde(rename = "batchSize", skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<u32>,
    /// Cursor time-to-live in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u32>,
    /// Whether to stream results (keep the query open and fetch lazily),
    /// preferred for large exports.
    pub options: CursorOptions,
}

/// Cursor `options` sub-object.
#[derive(Debug, Clone, Serialize)]
pub struct CursorOptions {
    /// Stream results rather than materializing them server-side up front.
    pub stream: bool,
}

impl CursorRequest {
    /// Builds a streaming cursor request for `query`.
    #[must_use]
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            bind_vars: None,
            batch_size: None,
            ttl: None,
            options: CursorOptions { stream: true },
        }
    }

    /// Sets bind variables.
    #[must_use]
    pub fn with_bind_vars(mut self, bind_vars: Value) -> Self {
        self.bind_vars = Some(bind_vars);
        self
    }

    /// Sets the server-side batch size.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Sets the cursor TTL in seconds.
    #[must_use]
    pub fn with_ttl(mut self, ttl_secs: u32) -> Self {
        self.ttl = Some(ttl_secs);
        self
    }
}

/// Manual `Debug` that never prints the query text or bind variables.
impl std::fmt::Debug for CursorRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CursorRequest")
            .field("query", &"<redacted>")
            .field("bind_vars", &self.bind_vars.as_ref().map(|_| "<redacted>"))
            .field("batch_size", &self.batch_size)
            .field("ttl", &self.ttl)
            .field("stream", &self.options.stream)
            .finish()
    }
}

/// One batch of cursor results from open or fetch-next.
#[derive(Debug, Clone, Deserialize)]
pub struct CursorBatch {
    /// The documents in this batch.
    #[serde(default)]
    pub result: Vec<Value>,
    /// Whether more batches remain.
    #[serde(rename = "hasMore", default)]
    pub has_more: bool,
    /// The cursor id, present while `has_more` is true.
    #[serde(default)]
    pub id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_streaming_request() {
        let req = CursorRequest::new("FOR d IN c RETURN d")
            .with_batch_size(1000)
            .with_bind_vars(serde_json::json!({"x": 1}));
        let json = serde_json::to_value(&req).unwrap();
        assert_eq!(json["query"], "FOR d IN c RETURN d");
        assert_eq!(json["batchSize"], 1000);
        assert_eq!(json["bindVars"]["x"], 1);
        assert_eq!(json["options"]["stream"], true);
        // ttl is omitted when unset.
        assert!(json.get("ttl").is_none());
    }

    #[test]
    fn parses_batch() {
        let body =
            r#"{"result":[{"n":1},{"n":2}],"hasMore":true,"id":"42","error":false,"code":201}"#;
        let batch: CursorBatch = serde_json::from_str(body).unwrap();
        assert_eq!(batch.result.len(), 2);
        assert!(batch.has_more);
        assert_eq!(batch.id.as_deref(), Some("42"));
    }

    #[test]
    fn debug_does_not_leak_query_or_bind_vars() {
        let req = CursorRequest::new("FOR u IN users FILTER u.secret == @s RETURN u")
            .with_bind_vars(serde_json::json!({"s": "hunter2"}));
        let rendered = format!("{req:?}");
        assert!(!rendered.contains("hunter2"));
        assert!(!rendered.contains("users"));
        assert!(rendered.contains("redacted"));
    }
}
