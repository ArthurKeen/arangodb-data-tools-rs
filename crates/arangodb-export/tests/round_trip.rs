//! Live export round-trip: import documents, export them via the cursor
//! pipeline, and verify the output reproduces the input.
//!
//! Runs only when `ARANGO_ENDPOINT` is set (the CI test job provides it);
//! otherwise it is a no-op.

use std::collections::BTreeSet;

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions};
use arangodb_export::{collection_query, run_export, ExportFormat};
use arangodb_import::{read_documents, run_import, ArangoBatchSender, BatchSender, ImportFormat};
use arangodb_storage::{Compression, LocalFileSystem, ObjectPath, ObjectStore};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use std::sync::Arc;

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

async fn read_object(store: &dyn ObjectStore, path: &ObjectPath) -> Vec<u8> {
    use futures::StreamExt;
    let mut stream = store.get_stream(path, None).await.unwrap();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_jsonl_reproduces_collection() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping export round-trip test");
        return;
    };
    let collection = "arangox_it_export";
    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .unwrap();

    // Seed via the import pipeline.
    let seed = b"{\"_key\":\"a\",\"v\":1}\n{\"_key\":\"b\",\"v\":2}\n{\"_key\":\"c\",\"v\":3}\n";
    let reader = std::io::Cursor::new(seed.to_vec());
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(
        client.clone(),
        ImportOptions::new(collection),
    ));
    run_import(
        read_documents(ImportFormat::JsonLines, reader),
        BatchConfig {
            max_bytes: 1 << 20,
            max_docs: 1000,
        },
        ConcurrencyConfig {
            workers: 2,
            max_in_flight_bytes: 1 << 20,
            adaptive: true,
        },
        sender,
    )
    .await
    .unwrap();

    // Export to a temp directory as JSONL.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new(dir.path());
    let path = ObjectPath::new("export.jsonl");
    let meta = run_export(
        &client,
        collection_query(collection, 2),
        ExportFormat::JsonLines,
        None,
        Compression::None,
        &store,
        &path,
    )
    .await
    .unwrap();
    assert!(meta.size > 0);

    // The export has one JSON object per line, with the same keys we seeded.
    let bytes = read_object(&store, &path).await;
    let text = String::from_utf8(bytes).unwrap();
    let lines: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "expected three documents, got: {text}");

    let keys: BTreeSet<String> = lines
        .iter()
        .map(|line| {
            let doc: serde_json::Value = serde_json::from_str(line).unwrap();
            doc["_key"].as_str().unwrap().to_string()
        })
        .collect();
    assert_eq!(
        keys,
        ["a", "b", "c"].iter().map(|s| s.to_string()).collect()
    );

    client.drop_collection(collection).await.unwrap();
}
