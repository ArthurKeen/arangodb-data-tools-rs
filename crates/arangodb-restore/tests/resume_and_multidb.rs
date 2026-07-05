//! Live tests for restore resume (5.3) and multi-database restore (5.1).
//!
//! Run only when `ARANGO_ENDPOINT` is set; otherwise each test is a no-op.

use std::sync::Arc;

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions};
use arangodb_dump::{run_dump, DumpOptions, FilterOptions};
use arangodb_restore::{run_restore, RestoreCheckpointConfig, RestoreOptions};
use arangodb_storage::{LocalFileSystem, ObjectPath, ObjectStore};
use arangodb_tools_core::manifest::RestoreCheckpoint;
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

async fn seed(client: &ArangoClient, collection: &str, docs: &'static [u8]) {
    let _ = client.drop_collection(collection).await;
    client
        .ensure_collection(collection, CollectionKind::Document)
        .await
        .unwrap();
    client
        .import_documents(&ImportOptions::new(collection), Bytes::from_static(docs))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_resume_skips_completed_collections() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping restore resume test");
        return;
    };
    let c1 = "arangox_it_resume_a";
    let c2 = "arangox_it_resume_b";
    seed(
        &client,
        c1,
        b"{\"_key\":\"a\",\"v\":1}\n{\"_key\":\"b\",\"v\":2}\n",
    )
    .await;
    seed(&client, c2, b"{\"_key\":\"x\",\"v\":9}\n").await;

    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new(dir.path());
    let options = DumpOptions {
        database: "_system".to_string(),
        created_at: "2026-07-05T00:00:00Z".to_string(),
        filters: FilterOptions::new(Some("^arangox_it_resume_"), None).unwrap(),
        ..DumpOptions::default()
    };
    run_dump(&client, &store, &options).await.unwrap();

    client.drop_collection(c1).await.unwrap();
    client.drop_collection(c2).await.unwrap();

    let cp_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new(dir.path()));
    let checkpoint = RestoreCheckpointConfig::new(
        Arc::clone(&cp_store),
        ObjectPath::new("restore.progress.json"),
    );
    let restore_options = RestoreOptions {
        overwrite: true,
        checkpoint: Some(checkpoint.clone()),
        ..RestoreOptions::default()
    };

    // First run restores both collections and records them in the checkpoint.
    let first = run_restore(&client, &store, &restore_options)
        .await
        .unwrap();
    assert_eq!(first.restored, 2, "both collections restored on first run");
    assert_eq!(first.skipped, 0);
    assert_eq!(client.collection_count(c1).await.unwrap(), 2);
    assert_eq!(client.collection_count(c2).await.unwrap(), 1);

    // Second run with the same checkpoint skips everything (idempotent resume).
    let second = run_restore(&client, &store, &restore_options)
        .await
        .unwrap();
    assert_eq!(second.restored, 0, "nothing re-restored on resume");
    assert_eq!(
        second.skipped, 2,
        "both collections skipped from checkpoint"
    );

    client.drop_collection(c1).await.unwrap();
    client.drop_collection(c2).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_refuses_mismatched_checkpoint() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping checkpoint mismatch test");
        return;
    };
    let collection = "arangox_it_mismatch";
    seed(&client, collection, b"{\"_key\":\"a\",\"v\":1}\n").await;

    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new(dir.path());
    let options = DumpOptions {
        database: "_system".to_string(),
        created_at: "2026-07-05T01:00:00Z".to_string(),
        filters: FilterOptions::new(Some("^arangox_it_mismatch"), None).unwrap(),
        ..DumpOptions::default()
    };
    run_dump(&client, &store, &options).await.unwrap();
    client.drop_collection(collection).await.unwrap();

    // Pre-seed a checkpoint that belongs to a *different* dump.
    let cp_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new(dir.path()));
    let bogus = RestoreCheckpoint::new("some-other-dump-fingerprint");
    let bytes = Bytes::from(bogus.to_json().unwrap().into_bytes());
    cp_store
        .put_stream(
            &ObjectPath::new("restore.progress.json"),
            Box::pin(futures::stream::once(async move { Ok(bytes) })),
        )
        .await
        .unwrap();

    let restore_options = RestoreOptions {
        overwrite: true,
        checkpoint: Some(RestoreCheckpointConfig::new(
            Arc::clone(&cp_store),
            ObjectPath::new("restore.progress.json"),
        )),
        ..RestoreOptions::default()
    };
    let result = run_restore(&client, &store, &restore_options).await;
    assert!(result.is_err(), "mismatched checkpoint must be refused");

    let _ = client.drop_collection(collection).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_database_dump_restores_into_each_database() {
    let Some(client) = live_client() else {
        eprintln!("ARANGO_ENDPOINT not set; skipping multi-database restore test");
        return;
    };
    let db = "arangox_it_mdb";
    let collection = "arangox_it_mdb_items";
    client.create_database(db).await.unwrap();
    let db_client = client.with_database(db);
    seed(
        &db_client,
        collection,
        b"{\"_key\":\"a\"}\n{\"_key\":\"b\"}\n",
    )
    .await;

    // Dump every database, but filter to just our collection so the dump is
    // small and deterministic regardless of what else exists on the server.
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new(dir.path());
    let options = DumpOptions {
        all_databases: true,
        created_at: "2026-07-05T02:00:00Z".to_string(),
        filters: FilterOptions::new(Some("^arangox_it_mdb_"), None).unwrap(),
        ..DumpOptions::default()
    };
    let manifest = run_dump(&client, &store, &options).await.unwrap();
    assert!(
        manifest
            .artifacts
            .iter()
            .any(|a| a.database.as_deref() == Some(db)),
        "artifacts should carry their source database"
    );

    // Drop the collection (and database) and restore from the combined dump.
    db_client.drop_collection(collection).await.unwrap();

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
    assert!(summary.restored >= 1);

    // The collection is restored into its original database, not _system.
    assert_eq!(
        client
            .with_database(db)
            .collection_count(collection)
            .await
            .unwrap(),
        2
    );

    let _ = client.with_database(db).drop_collection(collection).await;
}
