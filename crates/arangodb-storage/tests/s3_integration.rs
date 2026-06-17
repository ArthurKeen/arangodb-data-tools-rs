//! S3-compatible backend integration tests (MinIO/LocalStack or real S3).
//!
//! Runs only when `OBJECT_STORE_S3_TEST_BUCKET` is set; connection settings
//! come from the standard `AWS_*` environment variables (`AWS_ENDPOINT`,
//! `AWS_ALLOW_HTTP`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`,
//! `AWS_SECRET_ACCESS_KEY`). Without them the test is a no-op, so plain
//! `cargo test` skips it.

use arangodb_storage::{ByteRange, ByteStream, ObjectPath, ObjectStore, ObjectStoreBackend};
use arangodb_tools_core::Error;
use bytes::Bytes;
use futures::StreamExt;

const PART_SIZE: usize = 8 * 1024 * 1024;

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
async fn s3_backend_round_trip() {
    let Ok(bucket) = std::env::var("OBJECT_STORE_S3_TEST_BUCKET") else {
        eprintln!("OBJECT_STORE_S3_TEST_BUCKET not set; skipping S3 integration test");
        return;
    };
    // Unique prefix so concurrent/repeat runs do not collide.
    let prefix = format!("arangox-it/{}", std::process::id());
    let store = ObjectStoreBackend::s3(&bucket, Some(prefix)).expect("build S3 backend");

    // put + head + exists
    let obj = ObjectPath::new("obj.txt");
    let meta = store
        .put_stream(&obj, once(b"hello world".to_vec()))
        .await
        .unwrap();
    assert_eq!(meta.size, 11);
    assert_eq!(store.head(&obj).await.unwrap().map(|m| m.size), Some(11));
    assert!(store.exists(&obj).await.unwrap());
    assert!(store
        .head(&ObjectPath::new("absent"))
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
        .list(&ObjectPath::new(""))
        .map(|m| m.unwrap().path.as_str().to_string())
        .collect()
        .await;
    assert!(
        listed.contains(&"obj.txt".to_string()),
        "listed: {listed:?}"
    );

    // conditional create: second writer is rejected, original is preserved
    let manifest = ObjectPath::new("manifest.json");
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

    // multipart: an object larger than one part round-trips intact
    let big_len = PART_SIZE + PART_SIZE / 2; // 12 MiB => 2 parts
    let big = ObjectPath::new("big.bin");
    let meta = store
        .put_stream(&big, once(vec![b'x'; big_len]))
        .await
        .unwrap();
    assert_eq!(meta.size as usize, big_len);
    assert_eq!(
        store.head(&big).await.unwrap().unwrap().size as usize,
        big_len
    );
    // A window straddling the part boundary reads back correctly.
    let boundary = read_all(
        store
            .get_stream(
                &big,
                Some(ByteRange::bounded(
                    (PART_SIZE - 2) as u64,
                    (PART_SIZE + 2) as u64,
                )),
            )
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(boundary, b"xxxx");

    // cleanup
    for path in [&obj, &manifest, &big] {
        store.delete(path).await.unwrap();
    }
    assert!(!store.exists(&big).await.unwrap());
}
