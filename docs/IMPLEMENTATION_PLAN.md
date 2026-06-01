# Implementation Plan: Rust ArangoDB Data Tools

This document turns `RUST_ARANGODB_TOOLS_PRD.md` into an actionable engineering plan: architecture decisions, a crate-by-crate build order, concrete tasks per phase, exit criteria, and a risk register. It is the working plan; it will be revised as spikes resolve open questions.

Companion documents (to be created as work proceeds):

- `docs/architecture.md` — living architecture reference.
- `docs/dump-format.md` — canonical manifest + on-disk/object layout spec.
- `docs/rdf-import.md` — RDF graph model and key strategy.

---

## 1. Guiding Principles

These drive every design decision below and come directly from the PRD analysis of the C++ tools:

1. **Async-first, bounded everywhere.** Every pipeline stage is connected by bounded channels with explicit backpressure and a global in-flight-byte cap. No blocking I/O on async worker threads, no busy-wait polling.
2. **Library first, CLI second.** All behavior lives in libraries with typed builders; CLIs are thin adapters. The library owns progress *data*; the CLI owns *presentation*.
3. **Storage is abstracted.** Nothing above `arangodb-storage` knows whether it is talking to a local disk or an object store.
4. **Manifest is canonical.** Dumps/exports are described by a manifest that is the source of truth. No filename guessing.
5. **Recoverable errors over fatal exits.** Structured error taxonomy; process aborts only at the CLI boundary.
6. **Resumability is designed in, symmetric for dump and restore.** Cheap checkpoints (logical keys / uncompressed offsets in the manifest), never expensive compressed-byte seeks.
7. **Secure by default.** TLS verification on by default; redact credentials, queries, and bind vars.

---

## 2. Architecture Overview

### 2.1 Pipeline shape (shared mental model)

All four data tools share the same stage shape, wired with bounded `tokio::sync::mpsc` channels:

```text
 source  ->  parse/transform  ->  batch  ->  N senders  ->  ArangoDB / storage
(stream)      (CPU stage)      (bytes+docs)  (async HTTP)
        \__ bounded channel __/        \__ bounded channel __/
                         global in-flight-byte semaphore
```

- **Import / RDF:** source = storage/stdin; sink = `/_api/import`.
- **Export:** source = `/_api/cursor`; sink = storage.
- **Dump:** source = `/_api/dump/*`; sink = storage (+ manifest).
- **Restore:** source = storage (+ manifest); sink = `/_api/replication/restore-*`.

### 2.2 Crate dependency graph

```text
arangodb-tools-core   (no deps on other crates)
        ^   ^   ^
        |   |   |
arangodb-client  arangodb-storage
        ^   ^        ^
        |   |        |
   import export dump restore        (each depends on core + client + storage)
        ^
        |
   arangodb-rdf            (depends on import + core)
        ^
        |
 arangodb-tools-cli        (depends on all)
```

Build order follows the arrows: `core` and `storage` first, then `client`, then the job crates, then `rdf`, then the CLI.

---

## 3. Cross-Cutting Foundations (`arangodb-tools-core`)

Built once, used everywhere. These are Phase 0 deliverables and gate all later work.

### 3.1 Error taxonomy
- `enum Error` via `thiserror` with variants: `Config`, `Connection`, `Http { status, server_error, context }`, `Io`, `Storage`, `Parse { line, column, context }`, `Serialization`, `Cancelled`, `Checkpoint`.
- Every error carries optional context: collection, object path, byte range, batch number.
- `Retryable` classification trait: maps transport errors, gateway/cluster timeouts, and HTTP 429/503 to retryable; 4xx (except 429) to non-retryable.

### 3.2 Retry & backoff
- `RetryPolicy { max_attempts, base_delay, max_delay, jitter }` with exponential backoff.
- `retry(policy, op)` helper that consults the `Retryable` classification. Used by **all** HTTP callers (client crate enforces it so no path can skip retries).

### 3.3 Concurrency primitives
- `BoundedPipeline` helper: typed wrapper over `mpsc` with capacity + a shared `Arc<Semaphore>` for global in-flight bytes.
- `WorkQueue<J>`: the single async work-queue abstraction (replaces both the C++ `ClientTaskQueue` and the bespoke per-server thread pools). Structured `Result` propagation, cooperative cancellation via `CancellationToken`, no swallowed errors.

### 3.4 Progress & observability
- `tracing` + `tracing-subscriber` setup with text and JSON formats.
- `ProgressEvent` enum (schema, serde-serializable) + `ProgressSink` trait. Library emits events; CLI renders. Counters: bytes read/written, docs processed, batches sent, server errors, retries, elapsed, throughput, ETA.
- Credential/query/bind-var redaction utilities used by logging.

### 3.5 Config & batching
- Shared config structs: connection, TLS, batching (`max_bytes` + `max_docs`), concurrency (`workers`, `in_flight_bytes`).
- Byte+doc accounting types for batchers.
- `Checkpoint` metadata types (used by dump/restore/import).

### 3.6 Manifest model (shared)
- `Manifest` + `Artifact { path, kind, format, compression, byte_size, checksum }` serde types. Spec lives in `docs/dump-format.md`. Used by export, dump, restore.

---

## 4. Phase Plan

Phases map to PRD §18 milestones but add the cross-cutting foundation and split work to keep each phase independently testable. Each phase lists deliverables, key APIs, and exit criteria.

### Phase 0 — Repository & Foundations (PRD Milestone 0)

**Deliverables**
- Cargo workspace with the nine crates from PRD §7 (stubs compiling).
- `rustfmt.toml`, `clippy.toml`, CI (fmt + clippy `-D warnings` + test) via GitHub Actions.
- `arangodb-tools-core`: error taxonomy, retry policy, bounded pipeline, work queue, progress schema, config, manifest types, tracing setup (§3 above).
- `arangodb-storage`: `ObjectStore` trait + local filesystem backend + URI parsing.
- `arangodb-client`: connection/auth/TLS config, HTTP execution with enforced retry, `/_api/version`.
- Docker integration harness (testcontainers or compose) for ArangoDB 3.12.

**Key APIs**
```rust
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_stream(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata>;
    async fn get_stream(&self, path: &ObjectPath, range: Option<ByteRange>) -> Result<ByteStream>;
    async fn list(&self, prefix: &ObjectPath) -> Result<Vec<ObjectMetadata>>;
    async fn delete(&self, path: &ObjectPath) -> Result<()>;
    async fn exists(&self, path: &ObjectPath) -> Result<bool>;
}

let client = ArangoClient::builder().endpoint(..).database(..).basic_auth(..).build()?;
client.version().await?;
```

**Exit criteria**
- CI passes (fmt, clippy, unit tests).
- Integration test connects to ArangoDB in Docker and calls `/_api/version`.
- Local `ObjectStore` round-trips a streamed object; URI parser unit-tested for all schemes.

---

### Phase 1 — Import MVP (PRD Milestone 1)

**Deliverables (`arangodb-import` + CLI)**
- Streaming readers: JSONL (incremental), JSON array (incremental or documented limit), CSV/TSV.
- Single unified pipeline (reader → optional transform → byte+doc batcher → sender pool) using `BoundedPipeline`. No duplicated "rewrite" path.
- Batcher: byte cap + document cap, line-safe JSONL splitting.
- Async sender pool over `/_api/import`; global in-flight-byte semaphore.
- Duplicate modes (insert/update/replace/ignore/error) passed through; consistent `--max-errors` enforcement on all formats.
- Collection/database creation; edge vs document collection; `_from`/`_to` preflight for edge collections.
- Overwrite semantics defined (server-side truncate-and-import or documented non-atomic behavior).
- Structured per-batch error context; structured progress (docs/sec, ETA).
- `arangox-import` CLI.

**Exit criteria**
- Import 1M JSONL docs into Docker ArangoDB; counts verified.
- Memory stays bounded by configured batch size × workers (assert via test with small caps + large file).
- CSV and JSON-array imports validated; error-context surfaced on malformed input.

---

### Phase 2 — Object Storage Foundation (PRD Milestone 2)

**Deliverables (`arangodb-storage`)**
- S3-compatible backend (evaluate `object_store` crate vs hand-rolled in a spike — see §6).
- Streaming reads (range requests) and streaming writes (multipart upload).
- Object listing / prefix traversal; path validation against configured prefix.
- Wire import to read directly from `s3://` URIs.

**Exit criteria**
- Import JSONL directly from MinIO/LocalStack.
- Write + read-back streamed object via S3 backend in integration tests.
- Multipart upload exercised with an object larger than one part.

---

### Phase 3 — Export MVP (PRD Milestone 3)

**Deliverables (`arangodb-export` + CLI)**
- Cursor-based collection export (`stream: true`), configurable TTL/batch size.
- Custom AQL export with bind vars (redacted in logs).
- Output formats: JSONL (default), JSON array, CSV with explicit fields (formula-injection guard).
- Streaming output pipeline: decode cursor batch incrementally, write without buffering whole batch; fetch-next overlaps write.
- Compression (gzip; zstd via storage layer), split into multiple objects, manifest describing outputs.
- Parallel collection export via `WorkQueue`.
- `arangox-export` CLI.

**Exit criteria**
- Export collection to local and S3 storage; re-import exported JSONL and validate counts (round-trip).
- Manifest correctly enumerates split parts and is consumed by a manifest-reader test.

---

### Phase 4 — Dump & Restore MVP (PRD Milestone 4)

**Deliverables (`arangodb-dump`, `arangodb-restore` + CLIs)**
- **Dump:** inventory retrieval; structure/index/view metadata; data via parallel `/_api/dump/*` (legacy `/_api/replication/dump` fallback); manifest as canonical output; gzip/zstd; server-handle keep-alive (TTL extension) and cleanup; per-collection/shard checkpoints for resume.
- **Restore:** read manifest; validate compatibility before mutation; create DB/collections; **dependency ordering** (distributeShardsLike prototypes → docs → edges; `_analyzers` first; `_users` last); ordinary indexes before data, vector indexes after; arangosearch views before data, search-alias after; resumable via cheap checkpoints.
- Unified retry on every HTTP op in both tools.
- `arangox-dump`, `arangox-restore` CLIs.

**Exit criteria**
- Dump → restore round-trip into a fresh DB on local storage; counts + indexes validated.
- Same round-trip against S3 storage.
- Kill dump mid-run → resume completes without re-fetching finished collections.
- Kill restore mid-run → resume completes without duplicating finished collections.

---

### Phase 5 — Multi-Database, Resume Hardening, Splitting (PRD Milestone 5)

**Deliverables**
- All-databases dump.
- Restore continuation hardening (in-flight checkpoint accounting, contiguous-prefix advance).
- Collection/view filters across dump/restore.
- Refined adaptive retry/backoff.
- Split large data files/objects with manifest-tracked parts.

**Exit criteria**
- Interrupted restore resumes without duplicating completed collections.
- Large-object split restore works from S3.
- All-databases dump/restore validated.

---

### Phase 6 — RDF Import MVP (PRD Milestone 6)

**Deliverables (`arangodb-rdf` + CLI)**
- N-Triples + Turtle streaming parsers (crate chosen in spike, §6).
- Default graph model (one vertex collection, one edge collection); deterministic key generation (`sha2`/`blake3`); literal policy (vertex vs embedded), configurable.
- Bulk-load generated vertex/edge batches through the import pipeline.
- `arangox-rdf import` CLI.

**Exit criteria**
- Import known RDF fixtures; vertex/edge counts validated.
- At least one configurable literal policy exercised.

---

### Phase 7 — Cloud Backends (PRD Milestone 7)

**Deliverables**
- GCS backend, Azure backend, SeaweedFS documented path (S3-compatible first).
- Backend-specific docs incl. credentials and deployment examples.

**Exit criteria**
- Smoke tests pass per backend (behind feature flags / nightly CI).
- Docs complete.

---

## 5. Testing & CI Strategy

| Layer | Scope | When |
|-------|-------|------|
| Unit | URI parsing, batching, CSV/TSV edge cases, retry classification, manifest serde, RDF normalization, key gen | Every phase |
| Integration (Docker) | connect, create, import/export, dump/restore, filters, indexes/views | Phases 1+ |
| Storage | local (CI), S3 via MinIO/LocalStack, GCS/Azure behind flags | Phases 2, 7 |
| Resume/chaos | kill-mid-operation tests for dump and restore | Phases 4, 5 |
| Round-trip | export→import, dump→restore count/index equality | Phases 3, 4 |
| Compatibility | official `arangodump` → Rust restore (scoped subset), Rust dump → official `arangorestore` | Phases 4, 5 |

**CI gates:** `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test` (unit), and a Docker-backed integration job. Matrix on ArangoDB 3.12 + current stable.

---

## 6. Spikes To Resolve Open Questions (do early)

Run these as small, time-boxed spikes **before** committing the dependent phase:

1. **`object_store` crate vs hand-rolled** (gate Phase 2): does it satisfy streaming, multipart, range reads, and all four backends? Decide single dependency vs per-backend.
2. **RDF crate** (gate Phase 6): `oxrdf`/`oxttl` vs `rio` for streaming N-Triples/Turtle coverage and performance.
3. **HTTP client** (gate Phase 0): `reqwest` (batteries-included, rustls) vs `hyper` (control). Default to `reqwest`+`rustls` unless a blocker appears.
4. **VelocyPack**: confirmed out of MVP (refuse with clear error); revisit post-alpha.
5. **SeaweedFS**: S3-compatible only for now; native API deferred.

---

## 7. Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Server API drift across versions (parallel dump needs 3.12+) | Dump/restore breakage | Version-gate features; legacy fallback; compatibility test matrix |
| Object-store semantics (no atomic rename) | Corrupt/partial dumps | Manifest written last; multipart completion; idempotent names |
| Resume correctness with out-of-order async sends | Data duplication/loss | Contiguous-prefix checkpoint advance; logical-key checkpoints; chaos tests |
| Backpressure misconfiguration | OOM or underutilization | Global in-flight-byte semaphore; bounded channels; memory-bound tests |
| Compatibility-mode scope creep | Endless edge cases | Scope to tested subset (single-server, JSONL, no encryption); document best-effort |
| Encrypted Enterprise dumps | Silent corruption | Detect `ENCRYPTION` marker; fail loudly |
| TLS-verify default change vs C++ | User surprise | Document behavior change; explicit `--insecure` opt-in |
| Credential/query leakage in logs | Security | Central redaction utilities; default-off query/bind-var logging |

---

## 8. Suggested Sequencing & Parallelization

- **Critical path:** Phase 0 → 1 → 2 → 3 → 4 → 5. RDF (6) and Cloud backends (7) can proceed in parallel once their dependencies land (import pipeline for 6; storage trait for 7).
- **Parallelizable within phases:** storage backends are independent of job crates once the trait is stable; CLI work can track each job crate's API as it stabilizes.
- **First commit target:** Phase 0 foundations + green CI + `/_api/version` integration test. Everything else builds on that base.

---

## 9. Definition of Done for First Public Alpha (PRD §21)

- Import JSONL from local disk and from S3 into ArangoDB.
- Export a collection to JSONL on local and S3 storage.
- Dump and restore a small DB on local and S3 storage.
- CLI documented with examples; typed builders for import/export/dump/restore.
- CI runs unit + Docker integration tests.
- Documentation states compatibility limits clearly.
