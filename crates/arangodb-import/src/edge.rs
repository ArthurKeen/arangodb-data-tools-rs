//! Client-side preflight for edge documents.
//!
//! When importing into an edge collection, every document must carry `_from`
//! and `_to`. ArangoDB rejects malformed edges server-side, but only after a
//! batch round-trip and with the failure buried in per-document results.
//! [`validate_edge_documents`] catches the common mistakes up front — missing,
//! non-string endpoints, and bare keys when no prefix will qualify them — and
//! fails with the offending document's ordinal (PRD §8.2).
//!
//! An **empty** endpoint is different, and is skipped rather than rejected. A
//! relational export of an optional foreign key writes an empty field for a
//! row that has no link, so an empty `_from`/`_to` means "this row has no
//! edge", not "this file is malformed". Failing the import would make an
//! ordinary nullable foreign key unloadable; the row simply produces no edge.
//!
//! Skipped rows are counted and reported, never silently discarded: a required
//! foreign key that is unexpectedly empty shows up as a non-zero skip count.
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

/// Counts rows skipped because they carry no edge.
///
/// Shared with the caller so the count can be reported once the stream is
/// consumed. A skip is a legitimate outcome for an optional foreign key, but
/// an unexpected one for a required key, so the number is always surfaced.
pub type SkippedEdges = std::sync::Arc<std::sync::atomic::AtomicU64>;

/// Validates each document as an edge, passing valid documents through.
///
/// `from_has_prefix`/`to_has_prefix` indicate whether a server-side prefix is
/// configured for `_from`/`_to`; when set, bare keys are accepted for that
/// endpoint. An invalid document terminates the stream with an
/// [`Error::Parse`] naming the document ordinal and the problem.
///
/// A document whose `_from` or `_to` is present but empty is **skipped** and
/// counted in `skipped`, because that is how a relational export represents an
/// optional foreign key with no value.
pub fn validate_edge_documents<S>(
    documents: S,
    from_has_prefix: bool,
    to_has_prefix: bool,
    skipped: SkippedEdges,
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
            if !check_edge(&document, ordinal, from_has_prefix, to_has_prefix)? {
                skipped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                continue;
            }
            yield document;
        }
    })
}

/// Validates a single edge document.
///
/// Returns `Ok(false)` when the document carries no edge and should be
/// skipped, `Ok(true)` when it is a valid edge, and an error when it is
/// malformed.
fn check_edge(
    document: &Value,
    ordinal: u64,
    from_has_prefix: bool,
    to_has_prefix: bool,
) -> Result<bool> {
    let object = document
        .as_object()
        .ok_or_else(|| edge_error(ordinal, "edge document must be a JSON object"))?;
    let from = check_endpoint(object.get("_from"), "_from", from_has_prefix, ordinal)?;
    let to = check_endpoint(object.get("_to"), "_to", to_has_prefix, ordinal)?;
    Ok(from == Endpoint::Present && to == Endpoint::Present)
}

/// Whether an endpoint names a document or is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    /// The endpoint names a document.
    Present,
    /// The endpoint is present but empty: this row has no edge.
    Empty,
}

/// Validates one endpoint attribute (`_from` or `_to`).
fn check_endpoint(
    value: Option<&Value>,
    field: &str,
    has_prefix: bool,
    ordinal: u64,
) -> Result<Endpoint> {
    // A missing attribute is a mapping error — it would affect every row — so
    // it stays fatal. An empty one is per-row data, handled below.
    let value =
        value.ok_or_else(|| edge_error(ordinal, format!("edge document is missing '{field}'")))?;
    if value.is_null() {
        return Ok(Endpoint::Empty);
    }
    let text = value
        .as_str()
        .ok_or_else(|| edge_error(ordinal, format!("edge document '{field}' must be a string")))?;
    if text.is_empty() {
        // An optional foreign key with no value: this row has no edge.
        return Ok(Endpoint::Empty);
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
    Ok(Endpoint::Present)
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
    /// Runs the validator and returns both the surviving documents and the
    /// number of rows skipped for carrying no edge.
    fn collect_with_skips(
        values: Vec<Value>,
        from_prefix: bool,
        to_prefix: bool,
    ) -> (Result<Vec<Value>>, u64) {
        let skipped = SkippedEdges::default();
        let counter = std::sync::Arc::clone(&skipped);
        let out = futures::executor::block_on(async {
            let stream = validate_edge_documents(
                futures::stream::iter(values.into_iter().map(Ok)),
                from_prefix,
                to_prefix,
                counter,
            );
            futures::pin_mut!(stream);
            let mut kept = Vec::new();
            while let Some(item) = stream.next().await {
                kept.push(item?);
            }
            Ok(kept)
        });
        (out, skipped.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// An optional foreign key with no value means "this row has no edge".
    ///
    /// A relational export writes an empty field for it, so failing the import
    /// would make an ordinary nullable foreign key unloadable — one missing
    /// link would cost the entire edge collection.
    #[test]
    fn an_empty_endpoint_skips_the_row_rather_than_failing() {
        let rows = vec![
            serde_json::json!({"_from": "orders/1", "_to": "customers/5"}),
            serde_json::json!({"_from": "orders/2", "_to": ""}),
            serde_json::json!({"_from": "orders/3", "_to": "customers/7"}),
        ];
        let (result, skipped) = collect_with_skips(rows, false, false);
        let kept = result.expect("an empty endpoint must not fail the import");
        assert_eq!(kept.len(), 2, "the two real edges survive");
        assert_eq!(skipped, 1, "the skip is counted, not silent");
    }

    /// A JSON null endpoint is the same case as an empty string.
    #[test]
    fn a_null_endpoint_is_also_skipped() {
        let rows = vec![serde_json::json!({"_from": "orders/1", "_to": null})];
        let (result, skipped) = collect_with_skips(rows, false, false);
        assert!(result.expect("null is not fatal").is_empty());
        assert_eq!(skipped, 1);
    }

    /// A missing attribute is a mapping error, not per-row data: it would
    /// affect every row, so it stays fatal rather than silently skipping the
    /// entire file.
    #[test]
    fn a_missing_endpoint_attribute_is_still_fatal() {
        let rows = vec![serde_json::json!({"_from": "orders/1"})];
        let (result, _) = collect_with_skips(rows, false, false);
        let err = result
            .expect_err("a missing attribute must fail")
            .to_string();
        assert!(err.contains("missing"), "{err}");
        assert!(err.contains("_to"), "{err}");
    }

    /// A non-empty but malformed endpoint is still an error — skipping is only
    /// for the "no link" case, not for bad data.
    #[test]
    fn a_bare_key_without_a_prefix_is_still_fatal() {
        let rows = vec![serde_json::json!({"_from": "orders/1", "_to": "5"})];
        let (result, skipped) = collect_with_skips(rows, false, false);
        assert!(result.is_err(), "a bare key with no prefix is malformed");
        assert_eq!(skipped, 0, "an error is not a skip");
    }

    #[test]
    fn a_file_with_no_empty_endpoints_reports_no_skips() {
        let rows = vec![serde_json::json!({"_from": "a/1", "_to": "b/2"})];
        let (result, skipped) = collect_with_skips(rows, false, false);
        assert_eq!(result.expect("valid").len(), 1);
        assert_eq!(skipped, 0);
    }

    use super::*;

    fn collect(values: Vec<Value>, from_prefix: bool, to_prefix: bool) -> Result<Vec<Value>> {
        futures::executor::block_on(async {
            let stream = validate_edge_documents(
                futures::stream::iter(values.into_iter().map(Ok)),
                from_prefix,
                to_prefix,
                SkippedEdges::default(),
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
