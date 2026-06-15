//! Integration tests for the import pipeline.
//!
//! [`imports_one_million_jsonl_documents`] needs a live server and runs only
//! when `ARANGO_ENDPOINT` is set (the CI test job provides it); otherwise it is
//! a no-op. [`peak_in_flight_is_bounded_independent_of_input_size`] needs no
//! server and always runs.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions, ImportResult};
use arangodb_import::{
    read_documents, run_import, validate_edge_documents, ArangoBatchSender, Batch, BatchSender,
    ImportFormat,
};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::Result;

// ---------------------------------------------------------------------------
// Live 1M-document import (PRD Milestone 1 exit criterion).
// ---------------------------------------------------------------------------

/// Builds a client from the CI/integration environment, or `None` when no
/// server is configured (so the test no-ops in plain `cargo test`).
fn live_client() -> Option<ArangoClient> {
    let endpoint = std::env::var("ARANGO_ENDPOINT").ok()?;
    let password = std::env::var("ARANGO_ROOT_PASSWORD").unwrap_or_default();
    Some(
        ArangoClient::builder()
            .endpoint(endpoint)
            .database("_system")
            .basic_auth("root", password)
            .build()
            .expect("client builds from env"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn imports_one_million_jsonl_documents() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping live 1M import test");
        return;
    };

    // Default to one million; allow a smaller override for constrained runs.
    let docs: u64 = std::env::var("ARANGO_IMPORT_TEST_DOCS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let collection = "arangox_it_import_1m";

    // Start from a fresh collection.
    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .expect("create collection");

    // Generate JSONL to a temp file so the reader streams it rather than the
    // test holding the whole input in memory.
    let path = std::env::temp_dir().join("arangox_it_import_1m.jsonl");
    write_jsonl(&path, docs).await;

    let file = tokio::fs::File::open(&path).await.expect("open temp input");
    let documents = read_documents(ImportFormat::JsonLines, file);
    let options = ImportOptions::new(collection);
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(client.clone(), options));
    let batch = BatchConfig {
        max_bytes: 16 * 1024 * 1024,
        max_docs: 50_000,
    };
    let concurrency = ConcurrencyConfig {
        workers: 4,
        max_in_flight_bytes: 128 * 1024 * 1024,
    };

    let summary = run_import(documents, batch, concurrency, sender)
        .await
        .expect("import succeeds");
    assert_eq!(summary.created, docs, "server created count");
    assert_eq!(summary.errors, 0, "no server-side errors");

    let count = client
        .collection_count(collection)
        .await
        .expect("count collection");
    assert_eq!(count, docs, "collection document count");

    client
        .drop_collection(collection)
        .await
        .expect("drop collection");
    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imports_edges_with_preflight() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping live edge import test");
        return;
    };
    let collection = "arangox_it_edges";

    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Edge)
        .await
        .expect("create edge collection");

    // Qualified edges pass the preflight and import. (ArangoDB does not require
    // the referenced vertices to exist at import time.)
    let edges = "{\"_from\":\"people/alice\",\"_to\":\"people/bob\"}\n\
                 {\"_from\":\"people/bob\",\"_to\":\"people/carol\"}\n";
    let reader = std::io::Cursor::new(edges.as_bytes().to_vec());
    let documents = validate_edge_documents(
        read_documents(ImportFormat::JsonLines, reader),
        false,
        false,
    );
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(
        client.clone(),
        ImportOptions::new(collection),
    ));
    let summary = run_import(
        documents,
        BatchConfig {
            max_bytes: 1 << 20,
            max_docs: 1000,
        },
        ConcurrencyConfig {
            workers: 2,
            max_in_flight_bytes: 1 << 20,
        },
        sender,
    )
    .await
    .expect("edge import succeeds");
    assert_eq!(summary.created, 2);

    let count = client
        .collection_count(collection)
        .await
        .expect("count edges");
    assert_eq!(count, 2);

    client
        .drop_collection(collection)
        .await
        .expect("drop edge collection");
}

/// Writes `docs` JSONL records (`{"_key":"k<i>","v":<i>}`) to `path`.
async fn write_jsonl(path: &Path, docs: u64) {
    use std::fmt::Write as _;
    use tokio::io::AsyncWriteExt as _;

    let file = tokio::fs::File::create(path)
        .await
        .expect("create temp input");
    let mut writer = tokio::io::BufWriter::new(file);
    let mut line = String::new();
    for i in 0..docs {
        line.clear();
        writeln!(line, "{{\"_key\":\"k{i}\",\"v\":{i}}}").expect("format line");
        writer.write_all(line.as_bytes()).await.expect("write line");
    }
    writer.flush().await.expect("flush input");
}

// ---------------------------------------------------------------------------
// Bounded-memory property (PRD Milestone 1 exit criterion): in-flight bytes
// stay capped regardless of how large the input is. No server required.
// ---------------------------------------------------------------------------

/// A sink that never touches a network but records the peak number of bytes
/// held in flight across all workers at any instant.
#[derive(Default)]
struct InFlightProbe {
    current: AtomicUsize,
    peak: AtomicUsize,
    documents: AtomicUsize,
}

#[async_trait::async_trait]
impl BatchSender for InFlightProbe {
    async fn send(&self, batch: &Batch) -> Result<ImportResult> {
        let bytes = batch.byte_len();
        let now = self.current.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.peak.fetch_max(now, Ordering::SeqCst);
        self.documents.fetch_add(batch.documents, Ordering::SeqCst);
        // Hold the batch briefly so concurrent batches actually accumulate and
        // exercise the in-flight-byte cap.
        tokio::time::sleep(std::time::Duration::from_micros(200)).await;
        self.current.fetch_sub(bytes, Ordering::SeqCst);
        Ok(ImportResult {
            created: batch.documents as u64,
            ..ImportResult::default()
        })
    }
}

#[tokio::test]
async fn peak_in_flight_is_bounded_independent_of_input_size() {
    const CAP: usize = 64 * 1024;
    const BATCH_BYTES: usize = 16 * 1024;

    async fn run_with(n: usize, cap: usize) -> (usize, usize) {
        let probe = Arc::new(InFlightProbe::default());
        let documents = futures::stream::iter((0..n).map(|i| Ok(serde_json::json!({ "v": i }))));
        let batch = BatchConfig {
            max_bytes: BATCH_BYTES,
            max_docs: 100_000,
        };
        let concurrency = ConcurrencyConfig {
            workers: 8,
            max_in_flight_bytes: cap,
        };
        run_import(
            documents,
            batch,
            concurrency,
            Arc::clone(&probe) as Arc<dyn BatchSender>,
        )
        .await
        .expect("import succeeds");
        (
            probe.peak.load(Ordering::SeqCst),
            probe.documents.load(Ordering::SeqCst),
        )
    }

    let (peak_small, docs_small) = run_with(2_000, CAP).await;
    let (peak_large, docs_large) = run_with(200_000, CAP).await;

    // All documents flowed through in both runs.
    assert_eq!(docs_small, 2_000);
    assert_eq!(docs_large, 200_000);

    // The cap holds even though the large run read 100x more data.
    assert!(
        peak_small <= CAP,
        "small-run peak {peak_small} exceeded cap {CAP}"
    );
    assert!(
        peak_large <= CAP,
        "large-run peak {peak_large} exceeded cap {CAP}"
    );

    // The large run genuinely filled the pipeline (more than a single batch in
    // flight), so the cap — not the input size — is what bounds memory.
    assert!(
        peak_large > BATCH_BYTES,
        "expected concurrent batches in flight, saw peak {peak_large}"
    );
}
