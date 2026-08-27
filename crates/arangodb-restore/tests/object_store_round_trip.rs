//! Live dump -> restore round-trip **through an object-store backend** (S3 /
//! MinIO), exercising the full path against a real database and a real object
//! store rather than the local filesystem.
//!
//! Runs only when both `ARANGO_ENDPOINT` and `OBJECT_STORE_S3_TEST_BUCKET` are
//! set (the CI `test` job provides both — ArangoDB plus MinIO); otherwise it is
//! a no-op. The nightly workflow points the same suite at other backends.

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions};
use arangodb_dump::{run_dump, DumpOptions, FilterOptions};
use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_storage::ObjectStoreBackend;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dump_restore_round_trip_through_s3() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping S3 dump/restore round-trip");
        return;
    };
    let Ok(bucket) = std::env::var("OBJECT_STORE_S3_TEST_BUCKET") else {
        eprintln!("OBJECT_STORE_S3_TEST_BUCKET not set; skipping S3 dump/restore round-trip");
        return;
    };

    let collection = "arangox_it_s3_dr";
    // Unique prefix so concurrent/repeat runs never collide in the bucket.
    let prefix = format!("arangox-dr/{}", std::process::id());
    let store = ObjectStoreBackend::s3(&bucket, Some(prefix)).expect("build S3 backend");

    // Seed a fresh collection with a handful of documents.
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

    // Dump to the object store (filtered so the dump is small and deterministic
    // regardless of what else lives on the server).
    let options = DumpOptions {
        database: "_system".to_string(),
        created_at: "2026-07-06T00:00:00Z".to_string(),
        filters: FilterOptions::new(Some("^arangox_it_s3_dr$"), None).unwrap(),
        ..DumpOptions::default()
    };
    let manifest = run_dump(&client, &store, &options).await.unwrap();
    assert!(
        manifest
            .artifacts
            .iter()
            .any(|a| a.collection.as_deref() == Some(collection)),
        "dump manifest should list the collection's artifacts"
    );

    // Drop, then restore straight from the object store.
    client.drop_collection(collection).await.unwrap();
    let summary = run_restore(
        &client,
        &store,
        &RestoreOptions {
            overwrite: true,
            ..RestoreOptions::default()
        },
    )
    .await
    .unwrap();
    assert!(summary.collections >= 1);
    assert_eq!(client.collection_count(collection).await.unwrap(), 3);

    client.drop_collection(collection).await.unwrap();
}
