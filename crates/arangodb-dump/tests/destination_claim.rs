//! The dump destination guard (PRD §9.2).
//!
//! These need no ArangoDB: they drive the storage layer directly to prove the
//! guard's behavior, including the concurrent case it exists to prevent.

use std::sync::Arc;

use arangodb_dump::DUMP_LOCK_NAME;
use arangodb_storage::{LocalFileSystem, ObjectPath, ObjectStore};
use bytes::Bytes;
use futures::stream;

fn tempdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "arangox-claim-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn body(text: &'static str) -> arangodb_storage::ByteStream {
    Box::pin(stream::once(async move {
        Ok(Bytes::from_static(text.as_bytes()))
    }))
}

/// Only one of two racing writers may create the lock. This is the property
/// that makes the guard worth having: without a conditional create, both dumps
/// proceed and interleave their artifacts under one manifest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_concurrent_writer_can_claim_a_destination() {
    let dir = tempdir("race");
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new(&dir));
    let lock = ObjectPath::new(DUMP_LOCK_NAME);

    // Many writers, one destination, started together.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let store = Arc::clone(&store);
        let lock = lock.clone();
        handles.push(tokio::spawn(async move {
            store.put_if_absent(&lock, body("claim")).await.is_ok()
        }));
    }

    let mut winners = 0;
    for handle in handles {
        if handle.await.expect("task completes") {
            winners += 1;
        }
    }
    assert_eq!(
        winners, 1,
        "exactly one writer may claim the destination, got {winners}"
    );
}

/// Releasing the claim lets the next dump take it, so the guard does not wedge
/// a destination permanently.
#[tokio::test]
async fn a_released_claim_can_be_retaken() {
    let dir = tempdir("release");
    let store = LocalFileSystem::new(&dir);
    let lock = ObjectPath::new(DUMP_LOCK_NAME);

    store
        .put_if_absent(&lock, body("first"))
        .await
        .expect("claim");
    store
        .put_if_absent(&lock, body("second"))
        .await
        .expect_err("a held claim blocks");

    store.delete(&lock).await.expect("release");
    store
        .put_if_absent(&lock, body("third"))
        .await
        .expect("a released claim can be retaken");
}

/// A claim left behind by a crashed dump keeps blocking, which is the safe
/// direction: the destination holds a partial dump.
#[tokio::test]
async fn an_abandoned_claim_keeps_blocking() {
    let dir = tempdir("abandoned");
    let store = LocalFileSystem::new(&dir);
    let lock = ObjectPath::new(DUMP_LOCK_NAME);

    // Simulate a dump that died after claiming but before writing a manifest.
    store
        .put_if_absent(&lock, body("crashed"))
        .await
        .expect("claim");
    assert!(
        !store
            .exists(&ObjectPath::new("dump.manifest.json"))
            .await
            .expect("exists check"),
        "no manifest: the dump never completed"
    );
    store
        .put_if_absent(&lock, body("next run"))
        .await
        .expect_err("a destination holding a partial dump stays blocked");
}
