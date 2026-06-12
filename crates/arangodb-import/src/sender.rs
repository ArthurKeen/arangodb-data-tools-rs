//! The concurrent send stage of the import pipeline.
//!
//! [`run_import`] wires the stages together: document stream → batcher →
//! bounded channel → `workers` sender tasks. A global semaphore caps the bytes
//! held between the batcher and the senders (queued plus in transit), so
//! memory stays bounded by `max_in_flight_bytes` regardless of worker count or
//! server latency (PRD §11.2). Producers await the semaphore and the channel;
//! there is no polling.

use std::sync::Arc;

use arangodb_client::{ArangoClient, ImportOptions, ImportResult};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::{Error, Result};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;

use crate::batch::{into_batches, Batch};

/// Sends one prepared batch to its destination.
///
/// Abstracting the sink keeps the pipeline testable without a server; the
/// real implementation is [`ArangoBatchSender`].
#[async_trait]
pub trait BatchSender: Send + Sync + 'static {
    /// Sends `batch`, returning the server's statistics for it.
    async fn send(&self, batch: &Batch) -> Result<ImportResult>;
}

/// A [`BatchSender`] that posts batches to `/_api/import`.
#[derive(Debug, Clone)]
pub struct ArangoBatchSender {
    client: ArangoClient,
    options: ImportOptions,
}

impl ArangoBatchSender {
    /// Creates a sender importing into the collection named in `options`.
    #[must_use]
    pub fn new(client: ArangoClient, options: ImportOptions) -> Self {
        Self { client, options }
    }
}

#[async_trait]
impl BatchSender for ArangoBatchSender {
    async fn send(&self, batch: &Batch) -> Result<ImportResult> {
        self.client
            .import_documents(&self.options, batch.body.clone())
            .await
            .map_err(|err| with_batch_context(err, &self.options.collection, batch.index))
    }
}

/// Attaches collection and batch-number context to HTTP errors.
fn with_batch_context(err: Error, collection: &str, batch: u64) -> Error {
    match err {
        Error::Http {
            status,
            message,
            context,
        } => {
            let mut context = *context;
            if context.collection.is_none() {
                context.collection = Some(collection.to_owned());
            }
            context.batch = Some(batch);
            Error::http(status, message, context)
        }
        other => other,
    }
}

/// Aggregated statistics for a completed import.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ImportSummary {
    /// Documents the server reported as created.
    pub created: u64,
    /// Documents the server reported as failed.
    pub errors: u64,
    /// Empty input lines the server skipped.
    pub empty: u64,
    /// Documents updated (`onDuplicate=update`).
    pub updated: u64,
    /// Documents skipped (`onDuplicate=ignore`).
    pub ignored: u64,
    /// Batches successfully sent.
    pub batches: u64,
    /// Documents successfully sent (before server-side outcome).
    pub documents_sent: u64,
    /// Request-body bytes successfully sent.
    pub bytes_sent: u64,
}

impl ImportSummary {
    /// Folds one successful batch send into the summary.
    fn record(&mut self, batch: &Batch, result: &ImportResult) {
        self.created += result.created;
        self.errors += result.errors;
        self.empty += result.empty;
        self.updated += result.updated;
        self.ignored += result.ignored;
        self.batches += 1;
        self.documents_sent += batch.documents as u64;
        self.bytes_sent += batch.byte_len() as u64;
    }

    /// Merges another worker's summary into this one.
    fn merge(&mut self, other: &ImportSummary) {
        self.created += other.created;
        self.errors += other.errors;
        self.empty += other.empty;
        self.updated += other.updated;
        self.ignored += other.ignored;
        self.batches += other.batches;
        self.documents_sent += other.documents_sent;
        self.bytes_sent += other.bytes_sent;
    }
}

/// Runs the import pipeline: batches `documents` and sends the batches
/// through `concurrency.workers` concurrent senders.
///
/// Delivery is at-least-once: on failure, batches accepted by other workers
/// may already have been imported. The first error encountered is returned;
/// a producer-side parse error lets in-flight batches finish first.
///
/// # Errors
/// Returns [`Error::Config`] if the configuration is invalid — including when
/// `batch_config.max_bytes` exceeds `concurrency.max_in_flight_bytes`, which
/// could otherwise deadlock the pipeline — or the first send/parse error.
pub async fn run_import<S>(
    documents: S,
    batch_config: BatchConfig,
    concurrency: ConcurrencyConfig,
    sender: Arc<dyn BatchSender>,
) -> Result<ImportSummary>
where
    S: Stream<Item = Result<Value>> + Send + 'static,
{
    if concurrency.workers == 0 {
        return Err(Error::config("import requires at least one worker"));
    }
    if batch_config.max_bytes > concurrency.max_in_flight_bytes {
        return Err(Error::config(format!(
            "batch max_bytes ({}) exceeds max_in_flight_bytes ({}); such a batch could never \
             acquire enough in-flight budget and would deadlock the pipeline",
            batch_config.max_bytes, concurrency.max_in_flight_bytes
        )));
    }

    let cap = concurrency.max_in_flight_bytes;
    let semaphore = Arc::new(Semaphore::new(cap.min(Semaphore::MAX_PERMITS)));
    let (tx, rx) = mpsc::channel::<(Batch, OwnedSemaphorePermit)>(concurrency.workers * 2);
    let rx = Arc::new(Mutex::new(rx));

    let mut workers = JoinSet::new();
    for _ in 0..concurrency.workers {
        let rx = Arc::clone(&rx);
        let sender = Arc::clone(&sender);
        workers.spawn(async move {
            let mut summary = ImportSummary::default();
            loop {
                // The lock guards only the `recv` await; senders run unlocked.
                let next = rx.lock().await.recv().await;
                let Some((batch, permit)) = next else { break };
                let result = sender.send(&batch).await?;
                summary.record(&batch, &result);
                drop(permit);
            }
            Ok::<_, Error>(summary)
        });
    }
    drop(rx);

    let produced = async {
        let batches = into_batches(documents, batch_config);
        futures::pin_mut!(batches);
        while let Some(batch) = batches.next().await {
            let batch = batch?;
            let permits = permits_for(batch.byte_len(), cap);
            let Ok(permit) = Arc::clone(&semaphore).acquire_many_owned(permits).await else {
                break; // Semaphore closed; unreachable, but never spin.
            };
            if tx.send((batch, permit)).await.is_err() {
                // Every worker has exited (on error); the join below reports it.
                break;
            }
        }
        Ok::<_, Error>(())
    }
    .await;
    drop(tx);

    let mut summary = ImportSummary::default();
    let mut first_error = produced.err();
    while let Some(joined) = workers.join_next().await {
        match joined {
            Ok(Ok(worker_summary)) => summary.merge(&worker_summary),
            Ok(Err(err)) => {
                first_error.get_or_insert(err);
            }
            Err(join_err) if join_err.is_cancelled() => {
                first_error.get_or_insert(Error::Cancelled);
            }
            Err(join_err) => std::panic::resume_unwind(join_err.into_panic()),
        }
    }

    match first_error {
        Some(err) => Err(err),
        None => Ok(summary),
    }
}

/// Semaphore permits a batch must hold: its byte size, clamped to the global
/// cap (an oversized single-document batch may legitimately exceed the cap;
/// clamping lets it proceed alone instead of deadlocking) and to the permit
/// type's range.
fn permits_for(bytes: usize, cap: usize) -> u32 {
    u32::try_from(bytes.min(cap)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::time::Duration;

    /// A sender that records concurrency and can fail on a chosen batch.
    #[derive(Default)]
    struct FakeSender {
        in_flight_bytes: AtomicI64,
        max_in_flight_bytes: AtomicI64,
        sends: AtomicU64,
        fail_on_send: Option<u64>,
    }

    #[async_trait]
    impl BatchSender for FakeSender {
        async fn send(&self, batch: &Batch) -> Result<ImportResult> {
            let bytes = batch.byte_len() as i64;
            let now = self.in_flight_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.max_in_flight_bytes.fetch_max(now, Ordering::SeqCst);
            let send_no = self.sends.fetch_add(1, Ordering::SeqCst) + 1;

            // Hold the batch "in flight" long enough for overlap to show up.
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.in_flight_bytes.fetch_sub(bytes, Ordering::SeqCst);

            if self.fail_on_send == Some(send_no) {
                return Err(Error::http(
                    500,
                    "injected failure",
                    arangodb_tools_core::ErrorContext::new(),
                ));
            }
            Ok(ImportResult {
                created: batch.documents as u64,
                ..ImportResult::default()
            })
        }
    }

    fn doc_stream(count: usize) -> impl Stream<Item = Result<Value>> + Send + 'static {
        futures::stream::iter((0..count).map(|n| Ok(serde_json::json!({"n": n}))))
    }

    fn concurrency(workers: usize, max_in_flight_bytes: usize) -> ConcurrencyConfig {
        ConcurrencyConfig {
            workers,
            max_in_flight_bytes,
        }
    }

    fn batches(max_bytes: usize, max_docs: usize) -> BatchConfig {
        BatchConfig {
            max_bytes,
            max_docs,
        }
    }

    #[tokio::test]
    async fn imports_all_documents_and_aggregates() {
        let sender = Arc::new(FakeSender::default());
        let summary = run_import(
            doc_stream(100),
            batches(1 << 20, 7),
            concurrency(4, 1 << 20),
            Arc::clone(&sender) as Arc<dyn BatchSender>,
        )
        .await
        .unwrap();

        assert_eq!(summary.created, 100);
        assert_eq!(summary.documents_sent, 100);
        assert_eq!(summary.batches, 15); // ceil(100 / 7)
        assert_eq!(sender.sends.load(Ordering::SeqCst), 15);
        assert!(summary.bytes_sent > 0);
    }

    #[tokio::test]
    async fn in_flight_bytes_stay_under_cap() {
        // Each batch is one ~9-byte document; the cap fits only one batch at a
        // time, so 4 workers must be serialized by the semaphore.
        let sender = Arc::new(FakeSender::default());
        let cap = 15;
        let summary = run_import(
            doc_stream(20),
            batches(10, 1),
            concurrency(4, cap),
            Arc::clone(&sender) as Arc<dyn BatchSender>,
        )
        .await
        .unwrap();

        assert_eq!(summary.batches, 20);
        assert!(
            sender.max_in_flight_bytes.load(Ordering::SeqCst) <= cap as i64,
            "in-flight bytes exceeded the cap"
        );
    }

    #[tokio::test]
    async fn send_failure_is_reported() {
        let sender = Arc::new(FakeSender {
            fail_on_send: Some(2),
            ..FakeSender::default()
        });
        let result = run_import(
            doc_stream(50),
            batches(1 << 20, 10),
            concurrency(2, 1 << 20),
            sender as Arc<dyn BatchSender>,
        )
        .await;

        match result {
            Err(Error::Http { status, .. }) => assert_eq!(status, 500),
            other => panic!("expected HTTP error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parse_error_is_reported() {
        let input = futures::stream::iter(vec![
            Ok(serde_json::json!({"n": 1})),
            Err(Error::parse("bad input")),
        ]);
        let result = run_import(
            input,
            batches(1 << 20, 1),
            concurrency(2, 1 << 20),
            Arc::new(FakeSender::default()) as Arc<dyn BatchSender>,
        )
        .await;
        assert!(matches!(result, Err(Error::Parse { .. })));
    }

    #[tokio::test]
    async fn rejects_batch_cap_above_in_flight_cap() {
        let result = run_import(
            doc_stream(1),
            batches(1 << 20, 100),
            concurrency(2, 1024),
            Arc::new(FakeSender::default()) as Arc<dyn BatchSender>,
        )
        .await;
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn rejects_zero_workers() {
        let result = run_import(
            doc_stream(1),
            batches(1 << 20, 100),
            concurrency(0, 1 << 20),
            Arc::new(FakeSender::default()) as Arc<dyn BatchSender>,
        )
        .await;
        assert!(matches!(result, Err(Error::Config(_))));
    }

    #[tokio::test]
    async fn oversized_single_document_does_not_deadlock() {
        // One document bigger than the whole in-flight cap: the permit clamp
        // lets it through alone rather than waiting forever.
        let big = "x".repeat(256);
        let input = futures::stream::iter(vec![Ok(serde_json::json!({ "blob": big }))]);
        let summary = run_import(
            input,
            batches(64, 100),
            concurrency(2, 64),
            Arc::new(FakeSender::default()) as Arc<dyn BatchSender>,
        )
        .await
        .unwrap();
        assert_eq!(summary.batches, 1);
    }

    #[test]
    fn permit_clamp() {
        assert_eq!(permits_for(10, 100), 10);
        assert_eq!(permits_for(1000, 100), 100);
        assert_eq!(permits_for(usize::MAX, usize::MAX), u32::MAX);
    }

    #[test]
    fn http_error_gains_batch_context() {
        let err = Error::http(503, "busy", arangodb_tools_core::ErrorContext::new());
        let enriched = with_batch_context(err, "users", 7);
        match enriched {
            Error::Http { context, .. } => {
                assert_eq!(context.collection.as_deref(), Some("users"));
                assert_eq!(context.batch, Some(7));
            }
            other => panic!("expected HTTP error, got {other:?}"),
        }
    }
}
