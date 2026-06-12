//! Byte- and document-bounded batching.
//!
//! Documents are accumulated into a JSONL request body (the wire format for
//! `POST /_api/import?type=documents`) and flushed whenever either the byte cap
//! or the document cap is reached. A single document larger than the byte cap
//! is emitted on its own rather than being dropped or split.

use std::fmt;

use arangodb_tools_core::config::BatchConfig;
use arangodb_tools_core::Result;
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use serde_json::Value;

/// A ready-to-send batch of documents in JSONL form.
pub struct Batch {
    /// The request body: one JSON document per line, newline-terminated.
    pub body: Vec<u8>,
    /// The number of documents in the batch.
    pub documents: usize,
    /// The 1-based ordinal of this batch within the import.
    pub index: u64,
}

impl Batch {
    /// The body size in bytes.
    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.body.len()
    }
}

impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately omit `body` so document contents never leak into logs.
        f.debug_struct("Batch")
            .field("index", &self.index)
            .field("documents", &self.documents)
            .field("bytes", &self.body.len())
            .finish()
    }
}

/// Groups a stream of documents into [`Batch`]es bounded by `config`.
///
/// Flushes when adding the next document would exceed
/// [`BatchConfig::max_bytes`], or once [`BatchConfig::max_docs`] documents have
/// accumulated. Errors from the input stream are propagated in order.
pub fn into_batches<S>(
    documents: S,
    config: BatchConfig,
) -> impl Stream<Item = Result<Batch>> + Send
where
    S: Stream<Item = Result<Value>> + Send + 'static,
{
    try_stream! {
        futures::pin_mut!(documents);
        let mut body: Vec<u8> = Vec::new();
        let mut count: usize = 0;
        let mut index: u64 = 0;

        while let Some(document) = documents.next().await {
            let document = document?;
            let line = serde_json::to_vec(&document)?;

            if count > 0 && body.len() + line.len() + 1 > config.max_bytes {
                index += 1;
                yield Batch {
                    body: std::mem::take(&mut body),
                    documents: count,
                    index,
                };
                count = 0;
            }

            body.extend_from_slice(&line);
            body.push(b'\n');
            count += 1;

            if count >= config.max_docs {
                index += 1;
                yield Batch {
                    body: std::mem::take(&mut body),
                    documents: count,
                    index,
                };
                count = 0;
            }
        }

        if count > 0 {
            index += 1;
            yield Batch {
                body,
                documents: count,
                index,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arangodb_tools_core::Error;

    fn config(max_bytes: usize, max_docs: usize) -> BatchConfig {
        BatchConfig {
            max_bytes,
            max_docs,
        }
    }

    fn docs(values: Vec<Value>) -> impl Stream<Item = Result<Value>> + Send + 'static {
        futures::stream::iter(values.into_iter().map(Ok))
    }

    async fn collect_batches<S>(stream: S) -> Vec<Batch>
    where
        S: Stream<Item = Result<Batch>>,
    {
        use futures::StreamExt;
        stream.map(Result::unwrap).collect::<Vec<_>>().await
    }

    #[tokio::test]
    async fn flushes_on_document_cap() {
        let input = docs(vec![
            serde_json::json!({"n": 1}),
            serde_json::json!({"n": 2}),
            serde_json::json!({"n": 3}),
        ]);
        let batches = collect_batches(into_batches(input, config(1 << 20, 2))).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].documents, 2);
        assert_eq!(batches[1].documents, 1);
        assert_eq!(batches[0].index, 1);
        assert_eq!(batches[1].index, 2);
    }

    #[tokio::test]
    async fn flushes_on_byte_cap() {
        // Each line is `{"n":N}\n` = 8 bytes; cap at 10 forces one doc per batch.
        let input = docs(vec![
            serde_json::json!({"n": 1}),
            serde_json::json!({"n": 2}),
            serde_json::json!({"n": 3}),
        ]);
        let batches = collect_batches(into_batches(input, config(10, 1_000))).await;
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|b| b.documents == 1));
    }

    #[tokio::test]
    async fn oversized_document_is_emitted_alone() {
        let big = serde_json::json!({"blob": "x".repeat(1000)});
        let input = docs(vec![serde_json::json!({"n": 1}), big]);
        let batches = collect_batches(into_batches(input, config(16, 1_000))).await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[1].documents, 1);
        assert!(batches[1].byte_len() > 16);
    }

    #[tokio::test]
    async fn body_is_valid_jsonl() {
        let input = docs(vec![
            serde_json::json!({"n": 1}),
            serde_json::json!({"n": 2}),
        ]);
        let batches = collect_batches(into_batches(input, config(1 << 20, 1_000))).await;
        assert_eq!(batches.len(), 1);
        let lines: Vec<&[u8]> = batches[0].body.split(|&b| b == b'\n').collect();
        // Trailing newline produces a final empty element.
        assert_eq!(lines.len(), 3);
        assert!(lines[2].is_empty());
        let first: Value = serde_json::from_slice(lines[0]).unwrap();
        assert_eq!(first["n"], 1);
    }

    #[tokio::test]
    async fn empty_input_yields_no_batches() {
        let input = docs(vec![]);
        let batches = collect_batches(into_batches(input, config(1 << 20, 1_000))).await;
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn propagates_input_errors() {
        let input = futures::stream::iter(vec![
            Ok(serde_json::json!({"n": 1})),
            Err(Error::parse("boom")),
        ]);
        use futures::StreamExt;
        let results = into_batches(input, config(1 << 20, 1_000));
        futures::pin_mut!(results);
        let mut saw_error = false;
        while let Some(item) = results.next().await {
            if item.is_err() {
                saw_error = true;
            }
        }
        assert!(saw_error);
    }
}
