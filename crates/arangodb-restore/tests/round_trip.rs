//! Live dump -> restore round-trip: seed a collection (with data and a
//! secondary index), dump it, drop it, restore it, and verify the document
//! count and the index are reproduced.
//!
//! Runs only when `ARANGO_ENDPOINT` is set; otherwise it is a no-op.

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions};
use arangodb_dump::{run_dump, DumpOptions};
use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_storage::{Compression, LocalFileSystem};
use bytes::Bytes;

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

/// Whether `collection` has a persistent index over exactly `["v"]`.
async fn has_persistent_index_on_v(client: &ArangoClient, collection: &str) -> bool {
    let batch = client.replication_batch_create(60).await.unwrap();
    let inventory = client.replication_inventory(&batch, false).await.unwrap();
    let _ = client.replication_batch_delete(&batch).await;
    inventory
        .collections
        .iter()
        .find(|c| c.name() == Some(collection))
        .map(|c| {
            c.indexes.iter().any(|idx| {
                idx.get("type").and_then(|t| t.as_str()) == Some("persistent")
                    && idx.get("fields").and_then(|f| f.as_array())
                        == Some(&vec![serde_json::json!("v")])
            })
        })
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dump_restore_round_trip_preserves_count_and_index() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping dump/restore round-trip");
        return;
    };
    let collection = "arangox_it_dr";

    // Seed: fresh collection, three documents, and a persistent index.
    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .unwrap();
    client
        .import_documents(
            &ImportOptions::new(collection),
            Bytes::from_static(
                b"{\"_key\":\"a\",\"v\":1}\n{\"_key\":\"b\",\"v\":2}\n{\"_key\":\"c\",\"v\":3}\n",
            ),
        )
        .await
        .unwrap();
    client
        .create_index(
            collection,
            &serde_json::json!({"type": "persistent", "fields": ["v"]}),
        )
        .await
        .unwrap();
    assert!(has_persistent_index_on_v(&client, collection).await);

    // Dump to a temp directory.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new(dir.path());
    let options = DumpOptions {
        compression: Compression::Gzip,
        database: "_system".to_string(),
        created_at: "2026-06-18T00:00:00Z".to_string(),
        ..DumpOptions::default()
    };
    let manifest = run_dump(&client, &store, &options).await.unwrap();
    // structure + data artifacts for our collection are present.
    assert!(manifest
        .artifacts
        .iter()
        .any(|a| a.collection.as_deref() == Some(collection)));

    // Drop, then restore from the dump.
    client.drop_collection(collection).await.unwrap();
    let summary = run_restore(
        &client,
        &store,
        &RestoreOptions {
            overwrite: true,
            create_database: None,
        },
    )
    .await
    .unwrap();
    assert!(summary.collections >= 1);

    // Count and index are reproduced.
    assert_eq!(client.collection_count(collection).await.unwrap(), 3);
    assert!(
        has_persistent_index_on_v(&client, collection).await,
        "persistent index on [v] was not restored"
    );

    client.drop_collection(collection).await.unwrap();
}
