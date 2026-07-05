# arangodb-storage

A pluggable object/file storage abstraction for the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

It presents one streaming `ObjectStore` interface over several backends so the
dump/restore/export tools can read and write artifacts anywhere:

- **Local filesystem** (`file://` or plain paths).
- **S3-compatible** object storage (`s3://`), including MinIO, LocalStack, and
  SeaweedFS's S3 gateway, via the `object_store` crate.
- GCS (`gs://`) and Azure (`azure://`) URI schemes are parsed and wiring is in
  progress.

Reads and writes are streaming (`bytes`/`futures`), and transparent gzip/zstd
compression helpers are included.

## License

MIT
