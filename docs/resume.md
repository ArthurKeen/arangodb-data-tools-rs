# Resumability and checkpoints

Long-running jobs can be interrupted — a dropped connection, a killed process, a
spot-instance reclaim. `arangox` is designed so re-running the *same* command
picks up where it left off instead of starting from zero. Resumability works at
three levels, each matched to what the underlying data source allows.

## Import (batch-level checkpoint)

`arangox import --checkpoint <uri>` enables a rolling checkpoint. The importer
splits the input into indexed batches and workers commit them concurrently. A
single checkpoint object records the highest **contiguous** committed batch
index (`committed_batches`) — never a batch whose predecessors are still
outstanding — so a checkpoint can never claim work that is not durably applied.

- The checkpoint is overwritten as progress advances, at most once per second,
  plus a final write at the end. Checkpoint writes are best-effort: a failed
  write is logged and never aborts the import.
- On resume, every batch with index `<= committed_batches` is skipped, so the
  run continues from the first uncommitted batch.
- The checkpoint location can be any storage URI (`--checkpoint s3://…` or a
  local path), independent of where the input lives.

```bash
arangox import --collection users --input s3://data/users.jsonl \
  --checkpoint s3://state/users.import.json
# ...interrupted, then re-run the identical command to continue.
```

Because the input is read as an indexed stream, resume is exact regardless of
whether the source is seekable.

## Restore (collection-level checkpoint)

`arangox restore --checkpoint <uri>` records each collection identifier
(`"{database}::{collection}"`) once it is **fully** restored (data + indexes),
in the deterministic restore order. On restart, already-completed collections
are skipped.

The checkpoint stores a fingerprint of the dump manifest it belongs to and
refuses to resume against a *different* dump, so a checkpoint from one backup is
never mistakenly applied to another.

```bash
arangox restore --input s3://backups/mydb \
  --checkpoint s3://state/mydb.restore.json
```

Dump itself does not checkpoint mid-collection: its data source is a replication
cursor snapshot, and a re-run simply produces the artifacts again. Restore's
collection-level checkpoint is what makes the dump/restore round trip
resumable in practice.

## Restart-resumable object uploads (part-level)

For **seekable** upload sources — a local file staged to the cloud, or an
in-memory buffer — the `arangodb-storage` crate offers a backend-agnostic
resumable chunked upload (`upload_resumable`, `open_resumable`,
`read_resumable`, `delete_resumable`).

The logical object at `base` is stored as ordered part objects under
`"<base>.upload/"` plus a `state.json` marker written last, once every part is
present. `upload_resumable` uploads only the parts that are missing or present
at the wrong size, so an interrupted upload resumes by finishing the tail. This
works identically on local FS, S3, GCS, Azure, and SeaweedFS because parts are
addressed by key — no provider-specific multipart upload ID has to be persisted.

```rust
use arangodb_storage::{upload_resumable, open_resumable, read_resumable,
    FilePartSource, ObjectPath, DEFAULT_PART_SIZE};

let source = FilePartSource::open("big.dump").await?;
let base = ObjectPath::new("backups/big.dump");
// Safe to call repeatedly; a completed upload is a cheap no-op.
upload_resumable(store.as_ref(), &base, &source, DEFAULT_PART_SIZE).await?;

let upload = open_resumable(store.as_ref(), &base).await?;
let mut bytes = read_resumable(store.clone(), &base, &upload);
```

Note that `put_stream` (used by dump/export) is *not* restart-resumable on its
own: its multipart upload is abandoned if the process dies. That is acceptable
there because the DB data source is not seekable and resumability is provided at
the artifact/collection level above. The part-level uploader covers the
seekable-source case where finer-grained resume is both possible and useful.

## Cross-backend coverage

The resumable-upload round trip (including resume after a simulated
interruption) is unit-tested against the local backend on every run and against
MinIO, SeaweedFS, and Azurite (and real GCS when secrets are configured) in the
nightly workflow (`.github/workflows/nightly.yml`).
