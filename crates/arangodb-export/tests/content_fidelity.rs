//! End-to-end content fidelity for export.
//!
//! The existing round-trip test checks the line count and the set of `_key`
//! values. That leaves the values themselves unverified: an export that wrote
//! the right number of lines with the right keys but wrong, truncated, or
//! null fields would pass it.
//!
//! This compares every exported document against what is in the database,
//! field by field and type by type.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise it is a no-op.

use std::collections::BTreeMap;

use arangodb_client::{ArangoClient, CollectionKind, CursorRequest, ImportOptions};
use arangodb_export::{collection_query, run_export, ExportFormat};
use arangodb_storage::{Compression, LocalFileSystem, ObjectPath, ObjectStore};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;

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
    let mut stream = store.get_stream(path, None).await.expect("object opens");
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk.expect("chunk reads"));
    }
    buffer
}

/// Documents chosen so a lossy export shows up: every JSON type, multi-byte
/// text, embedded control characters, and nesting.
fn seed_documents() -> Vec<Value> {
    vec![
        serde_json::json!({
            "_key": "a", "int": 42, "float": 3.5, "bool": true,
            "null": null, "empty": "", "text": "plain",
        }),
        serde_json::json!({
            "_key": "b", "text": "café 中文 😀",
            "quoted": "a \"quoted\" value", "newline": "l1\nl2", "tab": "x\ty",
        }),
        serde_json::json!({
            "_key": "c", "nested": {"x": [1, 2, {"y": null}]},
            "array": [true, "two", 3.5], "numeric_string": "007",
        }),
    ]
}

/// Every exported document must equal the one in the database — values and
/// types, not just keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exported_documents_match_the_database_exactly() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_export";

    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .expect("collection created");
    let body: String = seed_documents().iter().map(|d| format!("{d}\n")).collect();
    let result = client
        .import_documents(&ImportOptions::new(collection), Bytes::from(body))
        .await
        .expect("seed import succeeds");
    assert_eq!(result.errors, 0, "seed must load cleanly");

    // What the database holds, keyed for comparison.
    let in_db: BTreeMap<String, Value> = client
        .cursor_open(&CursorRequest::new(format!(
            "FOR d IN {collection} SORT d._key RETURN UNSET(d, '_id', '_rev')"
        )))
        .await
        .expect("read-back query runs")
        .result
        .into_iter()
        .map(|d| (d["_key"].as_str().expect("key").to_string(), d))
        .collect();
    assert_eq!(in_db.len(), 3);

    // Export, then parse what landed.
    let dir = tempfile::tempdir().expect("temp dir");
    let store = LocalFileSystem::new(dir.path());
    let path = ObjectPath::new("export.jsonl");
    run_export(
        &client,
        collection_query(collection, 2),
        ExportFormat::JsonLines,
        None,
        Compression::None,
        &store,
        &path,
    )
    .await
    .expect("export succeeds");

    let text = String::from_utf8(read_object(&store, &path).await).expect("utf-8");
    let exported: BTreeMap<String, Value> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let mut doc: Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("exported line is not JSON: {line} ({e})"));
            // The export carries the server-assigned fields; the comparison is
            // about the data, so drop them as the read-back query did.
            if let Some(object) = doc.as_object_mut() {
                object.remove("_id");
                object.remove("_rev");
            }
            (doc["_key"].as_str().expect("key").to_string(), doc)
        })
        .collect();

    assert_eq!(
        exported.keys().collect::<Vec<_>>(),
        in_db.keys().collect::<Vec<_>>(),
        "the same documents must be exported"
    );
    for (key, expected) in &in_db {
        assert_eq!(
            exported.get(key),
            Some(expected),
            "exported document '{key}' differs from the database"
        );
    }

    // Types specifically: an export that stringified everything would still
    // have matched on a looser comparison.
    let a = &exported["a"];
    assert!(
        a["int"].is_i64(),
        "integer stayed an integer: {:?}",
        a["int"]
    );
    assert!(
        a["float"].is_f64(),
        "float stayed a float: {:?}",
        a["float"]
    );
    assert!(
        a["bool"].is_boolean(),
        "bool stayed a bool: {:?}",
        a["bool"]
    );
    assert!(a["null"].is_null(), "null stayed null: {:?}", a["null"]);
    assert!(
        exported["c"]["numeric_string"].is_string(),
        "the string \"007\" did not become a number: {:?}",
        exported["c"]["numeric_string"]
    );

    let _ = client.drop_collection(collection).await;
}
