//! End-to-end content fidelity for CSV import.
//!
//! Every other live test asserts that documents *arrived* — a count, a summary
//! number, a set of keys. None asserted that they arrived *correct*. A defect
//! that loaded the right number of documents with wrong, truncated, or null
//! values would pass all of them.
//!
//! These read the loaded documents back and compare **values and JSON types**
//! against what was sent. Type fidelity matters most for CSV, where the reader
//! decides each column's type: a number must arrive as a JSON number and not
//! the string `"42"`, a reserved attribute must arrive as a string even when it
//! looks numeric, and a zero-padded identifier must keep its zeros.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise it is a no-op.

use arangodb_client::{ArangoClient, CursorRequest};
use arangodb_import::{
    read_documents_with, run_import, ArangoBatchSender, ColumnTransforms, ImportFormat,
};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use serde_json::Value;
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

/// Reads every document back, keyed by `_key`, with `_id`/`_rev` removed so a
/// comparison sees only what the import actually supplied.
async fn stored_documents(client: &ArangoClient, collection: &str) -> Vec<Value> {
    let query = format!("FOR d IN {collection} SORT d._key RETURN UNSET(d, '_id', '_rev')");
    let batch = client
        .cursor_open(&CursorRequest::new(query))
        .await
        .expect("read-back query runs");
    batch.result
}

/// The JSON type name of a value, for assertions that distinguish `42` from
/// `"42"` — the difference the CSV reader exists to get right.
fn type_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Imports `csv` into a fresh collection and returns the stored documents.
async fn import_csv(
    client: &ArangoClient,
    collection: &str,
    csv: &'static str,
    transforms: ColumnTransforms,
) -> Vec<Value> {
    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, arangodb_client::CollectionKind::Document)
        .await
        .expect("collection created");

    let documents = read_documents_with(ImportFormat::Csv, csv.as_bytes(), transforms);
    let sender = Arc::new(ArangoBatchSender::new(
        client.clone(),
        arangodb_client::ImportOptions::new(collection),
    ));
    let summary = run_import(
        documents,
        BatchConfig::default(),
        ConcurrencyConfig::default(),
        sender,
    )
    .await
    .expect("import succeeds");
    assert_eq!(summary.errors, 0, "no documents may be rejected");

    stored_documents(client, collection).await
}

/// A CSV column's detected type must survive into ArangoDB. Before this, the
/// inference was unit-tested but nothing proved the value reached the server
/// with that type.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn csv_column_types_survive_into_the_database() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_types";
    let csv = "_key,count,ratio,flag,note,blank\n\
               r1,42,3.5,true,hello,\n";

    let docs = import_csv(&client, collection, csv, ColumnTransforms::default()).await;
    assert_eq!(docs.len(), 1);
    let d = &docs[0];

    // Types, not just values: `42` must not arrive as the string "42".
    assert_eq!(type_of(&d["count"]), "number", "got {:?}", d["count"]);
    assert_eq!(type_of(&d["ratio"]), "number", "got {:?}", d["ratio"]);
    assert_eq!(type_of(&d["flag"]), "boolean", "got {:?}", d["flag"]);
    assert_eq!(type_of(&d["note"]), "string", "got {:?}", d["note"]);
    assert_eq!(type_of(&d["blank"]), "null", "an empty field becomes null");

    // Values.
    assert_eq!(d["count"], Value::from(42));
    assert_eq!(d["ratio"], Value::from(3.5));
    assert_eq!(d["flag"], Value::Bool(true));
    assert_eq!(d["note"], Value::String("hello".into()));

    // `_key` is reserved: it must be a string even though it is not numeric
    // here, and the server would reject a numeric one outright.
    assert_eq!(type_of(&d["_key"]), "string");

    let _ = client.drop_collection(collection).await;
}

/// A numeric-looking identifier must reach the server as a string. A JSON
/// number here is rejected with "illegal document key" (error 1221), so this
/// is the difference between a loadable CSV and an unloadable one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn numeric_and_zero_padded_identifiers_stay_strings() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_keys";
    let csv = "id,plain\n12345,x\n00789,y\n";
    // Arguments are (datatypes, translations, merges, removals).
    let transforms =
        ColumnTransforms::new(&[], &["id=_key".to_string()], &[], &[]).expect("valid transforms");

    let docs = import_csv(&client, collection, csv, transforms).await;
    let keys: Vec<&str> = docs.iter().map(|d| d["_key"].as_str().unwrap()).collect();
    assert_eq!(
        keys,
        vec!["00789", "12345"],
        "zero padding must survive; a numeric _key would be rejected outright"
    );
    for d in &docs {
        assert_eq!(type_of(&d["_key"]), "string");
    }
    let _ = client.drop_collection(collection).await;
}

/// Text that survives the reader must survive the wire: multi-byte characters,
/// emoji, quotes, embedded commas and escaped newlines.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unicode_and_special_characters_round_trip_unchanged() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_unicode";
    // Quoted fields carry a comma, a doubled quote, and a newline.
    let csv = "_key,text\n\
               u1,\"caf\u{e9} \u{4e2d}\u{6587} \u{1f600}\"\n\
               u2,\"a,b \"\"quoted\"\" c\"\n\
               u3,\"line1\nline2\"\n\
               u4,\"tab\tand\\backslash\"\n";

    let docs = import_csv(&client, collection, csv, ColumnTransforms::default()).await;
    let text = |k: &str| -> String {
        docs.iter()
            .find(|d| d["_key"] == k)
            .unwrap_or_else(|| panic!("{k} missing"))["text"]
            .as_str()
            .expect("text is a string")
            .to_string()
    };

    assert_eq!(
        text("u1"),
        "café 中文 😀",
        "multi-byte text must be unchanged"
    );
    assert_eq!(text("u2"), "a,b \"quoted\" c", "embedded comma and quotes");
    assert_eq!(text("u3"), "line1\nline2", "embedded newline");
    assert_eq!(text("u4"), "tab\tand\\backslash", "tab and backslash");

    let _ = client.drop_collection(collection).await;
}

/// `--datatype` must change what reaches the server, not merely what the
/// reader produces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datatype_overrides_reach_the_server() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping");
        return;
    };
    let collection = "arangox_fid_datatype";
    let csv = "_key,n,s\nd1,42,7\n";
    let transforms = ColumnTransforms::new(
        &["n=string".to_string(), "s=number".to_string()],
        &[],
        &[],
        &[],
    )
    .expect("valid transforms");

    let docs = import_csv(&client, collection, csv, transforms).await;
    let d = &docs[0];
    assert_eq!(type_of(&d["n"]), "string", "forced to string");
    assert_eq!(d["n"], Value::String("42".into()));
    assert_eq!(type_of(&d["s"]), "number", "forced to number");
    assert_eq!(d["s"], Value::from(7));

    let _ = client.drop_collection(collection).await;
}
