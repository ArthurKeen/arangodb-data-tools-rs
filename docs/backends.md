# Storage backends

The tools read and write through a single streaming `ObjectStore` abstraction
(the `arangodb-storage` crate), so the same `import`, `export`, `dump`,
`restore`, and `rdf` commands work against local files or any supported object
store. Locations are given as plain paths or URIs, and credentials are always
resolved from the environment — never passed on the command line.

## Where locations are accepted

| Command / flag | Kind | Helper |
| --- | --- | --- |
| `import --input`, `rdf import --input` | single object (read) | `open_object` |
| `export --output` | single object (write) | `open_object` |
| `import --checkpoint`, `restore --checkpoint` | single object (read/write) | `open_object` |
| `dump --output`, `restore --input` | artifact **root** / prefix | `open_store_root` |

For a single object the store is rooted at the bucket/container and the URI path
is the object key. For a **root**, the URI path is used as a key *prefix* that
holds many artifacts.

## Supported schemes

### Local filesystem

A bare path (`/data/dump`, `./out.jsonl`) or a `file://` URI. No credentials.

```bash
arangox dump --database mydb --output /backups/mydb
arangox dump --database mydb --output file:///backups/mydb
```

### S3-compatible (`s3://`)

AWS S3 and any S3-compatible server (MinIO, LocalStack). Configuration comes from
the standard `AWS_*` environment variables:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN`)
- `AWS_REGION`
- `AWS_ENDPOINT` — set for MinIO/LocalStack/other gateways
- `AWS_ALLOW_HTTP=true` — allow plain-HTTP endpoints (local testing)

```bash
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
arangox dump    --database mydb --output s3://backups/mydb
arangox restore --database mydb --input  s3://backups/mydb
```

MinIO / LocalStack example:

```bash
export AWS_ENDPOINT=http://localhost:9000 AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1
arangox export --collection users --output s3://exports/users.jsonl
```

### SeaweedFS (`seaweed+s3://`)

SeaweedFS is reached through its S3-compatible gateway, so it uses the same
`AWS_*` configuration as `s3://`; point `AWS_ENDPOINT` at the SeaweedFS S3
gateway. The `seaweed+s3://` scheme is a readability alias — it behaves exactly
like `s3://`.

```bash
export AWS_ENDPOINT=http://localhost:8333 AWS_ALLOW_HTTP=true
export AWS_ACCESS_KEY_ID=any AWS_SECRET_ACCESS_KEY=any AWS_REGION=us-east-1
arangox dump --database mydb --output seaweed+s3://backups/mydb
```

### Google Cloud Storage (`gs://`)

Credentials come from the standard Google environment:

- `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_SERVICE_ACCOUNT_KEY` — path to, or inline
  contents of, a service-account JSON key, or
- `GOOGLE_APPLICATION_CREDENTIALS` — path to application default credentials.

```bash
export GOOGLE_SERVICE_ACCOUNT=/secrets/gcs-sa.json
arangox dump    --database mydb --output gs://backups/mydb
arangox restore --database mydb --input  gs://backups/mydb
```

### Azure Blob Storage (`az://`)

The URI's first path segment is the **container**. Credentials come from the
standard Azure environment:

- `AZURE_STORAGE_ACCOUNT_NAME` with `AZURE_STORAGE_ACCOUNT_KEY`, or
- a SAS token (`AZURE_STORAGE_SAS_KEY` / `AZURE_STORAGE_TOKEN`), or
- `AZURE_STORAGE_USE_EMULATOR=true` for Azurite.

```bash
export AZURE_STORAGE_ACCOUNT_NAME=myacct AZURE_STORAGE_ACCOUNT_KEY=...
arangox dump --database mydb --output az://backups/mydb
```

## URI reference

```text
file:///data/dump            -> local path /data/dump
s3://bucket/prefix           -> S3 bucket "bucket", prefix "prefix"
seaweed+s3://bucket/prefix   -> SeaweedFS S3 gateway (same as s3://)
gs://bucket/prefix           -> GCS bucket "bucket", prefix "prefix"
az://container/prefix        -> Azure container "container", prefix "prefix"
```

An unrecognized scheme is rejected with a configuration error. `file://` is a
local path and cannot be used where an object store is required (it has no
bucket).

## Notes

- Uploads to object stores are streamed as multipart parts (8 MiB), so
  arbitrarily large objects transfer with bounded memory.
- Object keys are validated to reject `.`/`..` traversal segments.
- Restart-resumable *multipart* uploads (resuming a partially uploaded object
  after a crash) are not yet implemented; `dump`/`restore` resumability works at
  the artifact/collection level via checkpoints.
