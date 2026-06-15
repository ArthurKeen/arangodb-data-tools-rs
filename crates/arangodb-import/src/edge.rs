//! Client-side preflight for edge documents.
//!
//! When importing into an edge collection, every document must carry `_from`
//! and `_to`. ArangoDB rejects malformed edges server-side, but only after a
//! batch round-trip and with the failure buried in per-document results.
//! [`validate_edge_documents`] catches the common mistakes up front — missing,
//! non-string, or empty endpoints, and bare keys when no prefix will qualify
//! them — and fails with the offending document's ordinal (PRD §8.2).
//!
//! A `_from`/`_to` value is normally a fully-qualified id (`collection/key`).
//! When a corresponding prefix is configured (the server's `fromPrefix`/
//! `toPrefix`), a bare key is valid because the server qualifies it, so the
//! preflight accepts bare keys in that case.

use arangodb_tools_core::{Error, Result};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use serde_json::Value;

use crate::reader::DocumentStream;

/// Validates each document as an edge, passing valid documents through.
///
/// `from_has_prefix`/`to_has_prefix` indicate whether a server-side prefix is
/// configured for `_from`/`_to`; when set, bare keys are accepted for that
/// endpoint. The first invalid document terminates the stream with an
/// [`Error::Parse`] naming the document ordinal and the problem.
pub fn validate_edge_documents<S>(
    documents: S,
    from_has_prefix: bool,
    to_has_prefix: bool,
) -> DocumentStream
where
    S: Stream<Item = Result<Value>> + Send + 'static,
{
    Box::pin(try_stream! {
        futures::pin_mut!(documents);
        let mut ordinal: u64 = 0;
        while let Some(document) = documents.next().await {
            let document = document?;
            ordinal += 1;
            check_edge(&document, ordinal, from_has_prefix, to_has_prefix)?;
            yield document;
        }
    })
}

/// Validates a single edge document.
fn check_edge(
    document: &Value,
    ordinal: u64,
    from_has_prefix: bool,
    to_has_prefix: bool,
) -> Result<()> {
    let object = document
        .as_object()
        .ok_or_else(|| edge_error(ordinal, "edge document must be a JSON object"))?;
    check_endpoint(object.get("_from"), "_from", from_has_prefix, ordinal)?;
    check_endpoint(object.get("_to"), "_to", to_has_prefix, ordinal)?;
    Ok(())
}

/// Validates one endpoint attribute (`_from` or `_to`).
fn check_endpoint(
    value: Option<&Value>,
    field: &str,
    has_prefix: bool,
    ordinal: u64,
) -> Result<()> {
    let value =
        value.ok_or_else(|| edge_error(ordinal, format!("edge document is missing '{field}'")))?;
    let text = value
        .as_str()
        .ok_or_else(|| edge_error(ordinal, format!("edge document '{field}' must be a string")))?;
    if text.is_empty() {
        return Err(edge_error(
            ordinal,
            format!("edge document '{field}' is empty"),
        ));
    }
    if !has_prefix && !text.contains('/') {
        return Err(edge_error(
            ordinal,
            format!(
                "edge document '{field}' = '{text}' is not a fully-qualified document id \
                 (expected 'collection/key'); configure a {field} prefix to import bare keys"
            ),
        ));
    }
    Ok(())
}

/// Builds a parse error carrying the 1-based document ordinal.
fn edge_error(ordinal: u64, message: impl Into<String>) -> Error {
    Error::Parse {
        message: message.into(),
        line: Some(ordinal),
        column: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(values: Vec<Value>, from_prefix: bool, to_prefix: bool) -> Result<Vec<Value>> {
        futures::executor::block_on(async {
            let stream = validate_edge_documents(
                futures::stream::iter(values.into_iter().map(Ok)),
                from_prefix,
                to_prefix,
            );
            futures::pin_mut!(stream);
            let mut out = Vec::new();
            while let Some(item) = stream.next().await {
                out.push(item?);
            }
            Ok(out)
        })
    }

    #[test]
    fn passes_qualified_edges() {
        let edges = vec![
            serde_json::json!({"_from": "people/alice", "_to": "people/bob", "since": 2020}),
            serde_json::json!({"_from": "people/bob", "_to": "people/carol"}),
        ];
        let out = collect(edges, false, false).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn rejects_missing_from() {
        let edges = vec![serde_json::json!({"_to": "people/bob"})];
        let err = collect(edges, false, false).unwrap_err();
        match err {
            Error::Parse { message, line, .. } => {
                assert!(message.contains("_from"), "message: {message}");
                assert_eq!(line, Some(1));
            }
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_bare_key_without_prefix() {
        let edges = vec![serde_json::json!({"_from": "alice", "_to": "people/bob"})];
        let err = collect(edges, false, false).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }

    #[test]
    fn accepts_bare_key_with_prefix() {
        let edges = vec![serde_json::json!({"_from": "alice", "_to": "bob"})];
        // Both endpoints have a prefix configured, so bare keys are valid.
        let out = collect(edges, true, true).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn bare_to_still_rejected_when_only_from_has_prefix() {
        let edges = vec![serde_json::json!({"_from": "alice", "_to": "bob"})];
        let err = collect(edges, true, false).unwrap_err();
        match err {
            Error::Parse { message, .. } => assert!(message.contains("_to"), "message: {message}"),
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_string_endpoint() {
        let edges = vec![serde_json::json!({"_from": 42, "_to": "people/bob"})];
        let err = collect(edges, false, false).unwrap_err();
        assert!(matches!(err, Error::Parse { .. }));
    }

    #[test]
    fn reports_ordinal_of_offending_document() {
        let edges = vec![
            serde_json::json!({"_from": "people/a", "_to": "people/b"}),
            serde_json::json!({"_from": "people/c", "_to": "people/d"}),
            serde_json::json!({"_to": "people/e"}),
        ];
        let err = collect(edges, false, false).unwrap_err();
        match err {
            Error::Parse { line, .. } => assert_eq!(line, Some(3)),
            other => panic!("expected parse error, got {other:?}"),
        }
    }
}
