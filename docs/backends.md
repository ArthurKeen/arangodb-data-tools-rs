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

## Resumable large-object uploads

Every backend also supports a **restart-resumable** chunked upload for
*seekable* sources (a local file or in-memory buffer), exposed by the
`arangodb-storage` crate: `upload_resumable`, `open_resumable`,
`read_resumable`, and `delete_resumable`.

The object at `base` is stored as ordered part objects under `"<base>.upload/"`
plus a `state.json` marker written last. `upload_resumable` uploads only the
parts that are missing (or present at the wrong size), so re-running after an
interruption finishes the tail instead of restarting from zero. Because parts
are addressed by key, no provider-specific multipart upload ID has to be
persisted, which is what makes it portable across local FS, S3, GCS, Azure, and
SeaweedFS. See [`docs/resume.md`](resume.md) for the full resumability story.

## Notes

- Uploads to object stores are streamed as multipart parts (8 MiB), so
  arbitrarily large objects transfer with bounded memory.
- Object keys are validated to reject `.`/`..` traversal segments.
- Cross-backend behavior is covered nightly (`.github/workflows/nightly.yml`)
  against MinIO, SeaweedFS, and Azurite (and real GCS when secrets are set),
  including the resumable-upload round trip and a throughput baseline.

## Troubleshooting

Storage errors surface as `storage error: …` (or `configuration error: …` for a
malformed URI). Common cases:

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `configuration error: unsupported storage scheme '…'` | URI scheme not recognized | Use one of `file`, `s3`, `seaweed+s3`, `gs`, `az`. |
| `configuration error: … missing a bucket/container` | URI has no bucket, e.g. `s3:///key` | Include the bucket: `s3://bucket/key`. |
| `configuration error: file:// is a local path, not an object store` | `file://` used where a bucket is required | Use a real object-store URI, or a plain path for local. |
| S3: `dispatch error` / connection refused | `AWS_ENDPOINT` wrong, or MinIO/gateway down | Verify the endpoint and that the server is up; set `AWS_ALLOW_HTTP=true` for plain-HTTP endpoints. |
| S3: `403 Forbidden` / `SignatureDoesNotMatch` | Bad `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, or `AWS_REGION` mismatch | Re-check credentials and region; some gateways need `AWS_REGION=us-east-1`. |
| S3/SeaweedFS: `NoSuchBucket` | Bucket not created | Create it first (`mc mb`, `aws s3 mb`, …); the tools do not auto-create buckets. |
| GCS: `credential`/`could not find default credentials` | No `GOOGLE_*` env set | Set `GOOGLE_SERVICE_ACCOUNT` (keyfile path) or `GOOGLE_APPLICATION_CREDENTIALS`. |
| Azure: `401`/auth errors | Missing account key or SAS | Set `AZURE_STORAGE_ACCOUNT_NAME` + `AZURE_STORAGE_ACCOUNT_KEY`, or a SAS token; for Azurite set `AZURE_STORAGE_USE_EMULATOR=true`. |
| Azurite: connection refused | Emulator not running / wrong port | Start Azurite on `:10000`, or set `AZURITE_BLOB_STORAGE_URL`. |
| `object path escapes storage prefix` | Key contains `.`/`..` segments | Use plain relative keys without traversal segments. |

## Performance tuning

Throughput is dominated by round-trip latency to the backend and by how much
work is in flight, so the same knobs apply across providers:

- **Batch size** (`--batch-size-bytes`, `--max-docs` for `import`/`rdf`; and the
  cursor `--batch-size` for `export`): larger batches amortize per-request
  overhead. 8–16 MiB import batches are a good default against cloud latency.
- **Concurrency** (`--threads`, `--max-in-flight-bytes`): more concurrent
  requests hide latency to remote object stores; raise in-flight bytes when you
  have bandwidth and memory. The adaptive governor backs off automatically under
  server `429/503`, so you can start high and let it settle (`--no-adaptive`
  disables it).
- **Compression** (`--compression gzip|zstd` for dump/export): trades CPU for
  fewer bytes on the wire — usually a win to cloud storage; `zstd` gives the
  best ratio/speed balance.
- **Split large exports** (`--split-bytes`): bounds the size of any single
  object and lets an interrupted transfer resume per part rather than per whole
  object.
- **Co-locate**: run the tools in the same region as the bucket/container;
  cross-region latency is the biggest throughput killer.

The nightly workflow records a `throughput_baseline` (write/read MiB/s) per
backend in its job summary, which is a useful reference point when tuning.
