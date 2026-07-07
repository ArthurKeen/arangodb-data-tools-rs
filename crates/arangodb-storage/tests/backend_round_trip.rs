//! Backend-agnostic integration tests driven by a single `STORAGE_TEST_URI`.
//!
//! Set `STORAGE_TEST_URI` to any object-store root the process can reach and
//! the same suite runs against that backend:
//!
//! ```text
//! s3://bucket            # AWS S3 / MinIO / LocalStack (AWS_* env)
//! seaweed+s3://bucket    # SeaweedFS S3 gateway (AWS_* env)
//! gs://bucket            # Google Cloud Storage (GOOGLE_* env)
//! az://container         # Azure Blob / Azurite (AZURE_* env)
//! ```
//!
//! Without `STORAGE_TEST_URI` every test is a no-op, so plain `cargo test`
//! skips them. The nightly cross-backend workflow sets the URI (and the matching
//! credentials) per backend so one suite exercises them all.

use std::sync::Arc;

use arangodb_storage::{
    open_resumable, read_resumable, upload_resumable, ByteRange, ByteStream, BytesPartSource,
    ObjectPath, ObjectStore, ObjectStoreBackend, StorageUri,
};
use arangodb_tools_core::Error;
use bytes::Bytes;
use futures::StreamExt;

/// Builds the configured backend, or returns `None` when `STORAGE_TEST_URI` is
/// unset so the test becomes a skip.
fn backend() -> Option<Arc<dyn ObjectStore>> {
    let uri = std::env::var("STORAGE_TEST_URI").ok()?;
    let parsed = StorageUri::parse(&uri).expect("STORAGE_TEST_URI is a valid storage URI");
    let store =
        ObjectStoreBackend::for_prefix(&parsed).expect("build backend from STORAGE_TEST_URI");
    Some(Arc::new(store))
}

/// A unique key prefix so concurrent/repeat runs never collide.
fn run_prefix(name: &str) -> String {
    format!("arangox-it/{}/{name}", std::process::id())
}

fn once(data: Vec<u8>) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(Bytes::from(data)) }))
}

async fn read_all(mut stream: ByteStream) -> Vec<u8> {
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk.unwrap());
    }
    buffer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backend_round_trip() {
    let Some(store) = backend() else {
        eprintln!("STORAGE_TEST_URI not set; skipping backend round-trip test");
        return;
    };
    let prefix = run_prefix("round-trip");

    // put + head + exists
    let obj = ObjectPath::new(format!("{prefix}/obj.txt"));
    let meta = store
        .put_stream(&obj, once(b"hello world".to_vec()))
        .await
        .unwrap();
    assert_eq!(meta.size, 11);
    assert_eq!(store.head(&obj).await.unwrap().map(|m| m.size), Some(11));
    assert!(store.exists(&obj).await.unwrap());
    assert!(store
        .head(&ObjectPath::new(format!("{prefix}/absent")))
        .await
        .unwrap()
        .is_none());

    // full and ranged reads
    assert_eq!(
        read_all(store.get_stream(&obj, None).await.unwrap()).await,
        b"hello world"
    );
    assert_eq!(
        read_all(
            store
                .get_stream(&obj, Some(ByteRange::bounded(0, 5)))
                .await
                .unwrap()
        )
        .await,
        b"hello"
    );
    assert_eq!(
        read_all(
            store
                .get_stream(&obj, Some(ByteRange::starting_at(6)))
                .await
                .unwrap()
        )
        .await,
        b"world"
    );

    // list (prefix-relative paths)
    let listed: Vec<String> = store
        .list(&ObjectPath::new(prefix.clone()))
        .map(|m| m.unwrap().path.as_str().to_string())
        .collect()
        .await;
    assert!(
        listed.iter().any(|p| p.ends_with("obj.txt")),
        "listed: {listed:?}"
    );

    // conditional create: the second writer loses, the original is preserved
    let manifest = ObjectPath::new(format!("{prefix}/manifest.json"));
    store
        .put_if_absent(&manifest, once(b"first".to_vec()))
        .await
        .unwrap();
    assert!(matches!(
        store
            .put_if_absent(&manifest, once(b"second".to_vec()))
            .await,
        Err(Error::AlreadyExists(_))
    ));
    assert_eq!(
        read_all(store.get_stream(&manifest, None).await.unwrap()).await,
        b"first"
    );

    for path in [&obj, &manifest] {
        store.delete(path).await.unwrap();
    }
    assert!(!store.exists(&obj).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumable_upload_round_trip_and_resume() {
    let Some(store) = backend() else {
        eprintln!("STORAGE_TEST_URI not set; skipping resumable upload test");
        return;
    };
    let base = ObjectPath::new(format!("{}/big.bin", run_prefix("resumable")));
    // ~3.5 parts at a 1 MiB part size so multipart resume is exercised.
    let part_size = 1024 * 1024;
    let data: Vec<u8> = (0..3_500_000u32).map(|i| i as u8).collect();
    let source = BytesPartSource::new(data.clone());

    let info = upload_resumable(store.as_ref(), &base, &source, part_size)
        .await
        .unwrap();
    assert_eq!(info.total_size, data.len() as u64);
    assert_eq!(info.parts, 4);

    // Re-running is idempotent (every part already present at the right size).
    upload_resumable(store.as_ref(), &base, &source, part_size)
        .await
        .unwrap();

    let opened = open_resumable(store.as_ref(), &base).await.unwrap();
    let round_tripped = read_all(read_resumable(Arc::clone(&store), &base, &opened)).await;
    assert_eq!(round_tripped, data);

    arangodb_storage::delete_resumable(store.as_ref(), &base)
        .await
        .unwrap();
    assert!(open_resumable(store.as_ref(), &base).await.is_err());
}

/// Throughput baseline: uploads then reads back a large object and prints MiB/s.
///
/// Ignored by default; the nightly workflow runs it with `--ignored` per
/// backend and captures the numbers. Size is `STORAGE_BENCH_MB` (default 64).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "throughput baseline; run explicitly with --ignored and STORAGE_TEST_URI"]
async fn throughput_baseline() {
    let Some(store) = backend() else {
        eprintln!("STORAGE_TEST_URI not set; skipping throughput baseline");
        return;
    };
    let mb: u64 = std::env::var("STORAGE_BENCH_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let bytes = (mb * 1024 * 1024) as usize;
    let obj = ObjectPath::new(format!("{}/bench.bin", run_prefix("bench")));
    let payload = vec![0xABu8; bytes];

    let start = std::time::Instant::now();
    store.put_stream(&obj, once(payload.clone())).await.unwrap();
    let write_secs = start.elapsed().as_secs_f64();

    let start = std::time::Instant::now();
    let read = read_all(store.get_stream(&obj, None).await.unwrap()).await;
    let read_secs = start.elapsed().as_secs_f64();
    assert_eq!(read.len(), bytes);

    store.delete(&obj).await.unwrap();

    let write_mbps = mb as f64 / write_secs;
    let read_mbps = mb as f64 / read_secs;
    let uri = std::env::var("STORAGE_TEST_URI").unwrap_or_default();
    let line = format!("| {uri} | {mb} | {write_mbps:.1} | {read_mbps:.1} |",);
    println!("THROUGHPUT {line}");
    // Append to the GitHub Actions job summary when running in CI.
    if let Ok(summary) = std::env::var("GITHUB_STEP_SUMMARY") {
        use std::io::Write as _;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(summary)
        {
            let _ = writeln!(f, "{line}");
        }
    }
}
