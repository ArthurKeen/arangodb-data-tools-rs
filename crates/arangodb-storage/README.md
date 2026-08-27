# arangodb-storage

A pluggable object/file storage abstraction for the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

It presents one streaming `ObjectStore` interface over several backends so the
dump/restore/export tools can read and write artifacts anywhere:

- **Local filesystem** (`file://` or plain paths).
- **S3-compatible** object storage (`s3://`), including MinIO, LocalStack, and
  SeaweedFS's S3 gateway (`seaweed+s3://`), via the `object_store` crate.
- **Google Cloud Storage** (`gs://`) and **Azure Blob Storage** (`az://`).

Reads and writes are streaming (`bytes`/`futures`), transparent gzip/zstd
compression helpers are included, and a backend-agnostic restart-resumable
chunked uploader (`upload_resumable`/`read_resumable`) is available for seekable
sources.

## Install

```bash
cargo add arangodb-storage
```

## Example

```rust,no_run
use arangodb_storage::{ByteStream, LocalFileSystem, ObjectPath, ObjectStore};
use bytes::Bytes;
use futures::StreamExt;

# async fn run() -> arangodb_tools_core::Result<()> {
let store = LocalFileSystem::new(".");
let path = ObjectPath::new("hello.txt");

let data: ByteStream = Box::pin(futures::stream::once(async { Ok(Bytes::from_static(b"hi")) }));
store.put_stream(&path, data).await?;

let mut reader = store.get_stream(&path, None).await?;
while let Some(chunk) = reader.next().await {
    let _bytes = chunk?;
}
# Ok(())
# }
```

To target object storage instead, build a backend from a URI with
`ObjectStoreBackend::for_prefix(&StorageUri::parse("s3://bucket/prefix")?)`.

## License

MIT
