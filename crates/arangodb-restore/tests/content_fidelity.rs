//! End-to-end content fidelity for dump and restore.
//!
//! The existing round-trip test asserts a document *count* and that an index
//! was recreated. That leaves the central claim of a backup tool unproven: a
//! defect that restored the right number of documents with wrong, truncated,
//! or null values would pass it.
//!
//! This compares every restored document against the source, field by field
//! and type by type, over a deliberately awkward dataset.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise it is a no-op.

use arangodb_client::{ArangoClient, CollectionKind, CursorRequest, ImportOptions};
use arangodb_dump::{run_dump, DumpOptions};
use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_storage::LocalFileSystem;
use bytes::Bytes;
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

/// Documents chosen to break a naive copy: every JSON type, multi-byte text,
/// an embedded newline and quote, a zero-padded identifier, an explicit null
/// against an absent attribute, nesting, and numeric edges.
fn awkward_documents() -> Vec<Value> {
    vec![
        serde_json::json!({
            "_key": "00042",
            "kind": "zero-padded key",
            "int": 42,
            "negative": -17,
            "float": 3.5,
            "zero": 0,
            "bool_true": true,
            "bool_false": false,
            "explicit_null": null,
            "empty_string": "",
        }),
        serde_json::json!({
            "_key": "unicode",
            "text": "café 中文 😀 Ω",
            "quoted": "a \"quoted\" value",
            "newline": "line1\nline2",
            "tab": "a\tb",
            "backslash": "C:\\path\\to",
        }),
        serde_json::json!({
            "_key": "nested",
            "object": {"a": 1, "b": {"c": [1, 2, 3]}},
            "array": [1, "two", true, null, {"four": 4}],
            "empty_object": {},
            "empty_array": [],
        }),
        serde_json::json!({
            "_key": "numeric-edges",
            "big": 9_007_199_254_740_991_i64,
            "small": -9_007_199_254_740_991_i64,
            "precise": 0.1,
            "exponent": 1.0e10,
        }),
    ]
}

/// Reads documents back without the server-assigned fields, so a comparison
/// sees only what was stored.
async fn stored(client: &ArangoClient, collection: &str) -> Vec<Value> {
    let query = format!("FOR d IN {collection} SORT d._key RETURN UNSET(d, '_id', '_rev')");
    client
        .cursor_open(&CursorRequest::new(query))
        .await
        .expect("read-back query runs")
        .result
}

/// Dump options restricted to one collection.
///
/// `_system` also holds the other live tests' collections; dumping all of them
/// and restoring would collide with the ones still in use.
fn only(collection: &str) -> DumpOptions {
    DumpOptions {
        filters: arangodb_dump::FilterOptions::new(Some(&format!("^{collection}$")), None)
            .expect("valid filter"),
        ..DumpOptions::default()
    }
}

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "arangox-fidelity-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// Every document must come back from a dump byte-for-byte equal in value and
/// type to the one that went in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_document_survives_a_dump_and_restore_unchanged() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_roundtrip";

    // Seed.
    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .expect("collection created");
    let body: String = awkward_documents()
        .iter()
        .map(|d| format!("{d}\n"))
        .collect();
    let result = client
        .import_documents(&ImportOptions::new(collection), Bytes::from(body))
        .await
        .expect("seed import succeeds");
    assert_eq!(result.errors, 0, "seed must load cleanly");

    let before = stored(&client, collection).await;
    assert_eq!(before.len(), 4, "all four seed documents are present");

    // Dump, destroy, restore.
    let dir = tempdir("roundtrip");
    let store = LocalFileSystem::new(&dir);
    run_dump(&client, &store, &only(collection))
        .await
        .expect("dump succeeds");
    client
        .drop_collection(collection)
        .await
        .expect("collection dropped");
    run_restore(&client, &store, &RestoreOptions::default())
        .await
        .expect("restore succeeds");

    let after = stored(&client, collection).await;

    // The assertion the count-based test could not make.
    assert_eq!(
        after.len(),
        before.len(),
        "document count must match after restore"
    );
    for (source, restored) in before.iter().zip(after.iter()) {
        assert_eq!(
            source, restored,
            "document '{}' changed across dump/restore",
            source["_key"]
        );
    }

    let _ = client.drop_collection(collection).await;
}

/// JSON types must survive specifically, since a copy that stringified every
/// value would still compare equal on a loose check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn value_types_survive_a_dump_and_restore() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_types_rt";

    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .expect("collection created");
    client
        .import_documents(
            &ImportOptions::new(collection),
            Bytes::from("{\"_key\":\"t\",\"i\":7,\"f\":1.5,\"b\":true,\"n\":null,\"s\":\"7\"}\n"),
        )
        .await
        .expect("seed import succeeds");

    let dir = tempdir("types");
    let store = LocalFileSystem::new(&dir);
    run_dump(&client, &store, &only(collection))
        .await
        .expect("dump succeeds");
    client.drop_collection(collection).await.expect("dropped");
    run_restore(&client, &store, &RestoreOptions::default())
        .await
        .expect("restore succeeds");

    let docs = stored(&client, collection).await;
    let d = &docs[0];
    assert!(d["i"].is_i64(), "integer stayed an integer: {:?}", d["i"]);
    assert!(d["f"].is_f64(), "float stayed a float: {:?}", d["f"]);
    assert!(d["b"].is_boolean(), "bool stayed a bool: {:?}", d["b"]);
    assert!(d["n"].is_null(), "null stayed null: {:?}", d["n"]);
    assert!(
        d["s"].is_string(),
        "the string \"7\" did not become the number 7: {:?}",
        d["s"]
    );

    let _ = client.drop_collection(collection).await;
}
