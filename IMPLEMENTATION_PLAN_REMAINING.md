# ArangoDB Data Tools (Rust) — Remaining Implementation Plan

**Current Status:** Phase 0–4 complete. Phase 6 (RDF) complete (incl. RPT/PGT graph models, blank-node provenance scoping, and N-Quads named-graph routing; only RDF/XML+TriG and the 100K-triple benchmark remain deferred). Phase 5 nearly complete: all-database dump, import resume, **multi-database restore (5.1)**, **restore resume (5.3)**, **split for jsonl/json/csv (5.4)**, **adaptive batching / rate-limit governor (5.5)**, **collection filters (5.6)**, and **retry tuning (5.7)** are done; only multipart restart-resume (S3-specific) is deferred. Phase 7 (cloud backends) not started.

---

## Executive Summary

| Phase | Title | Status | Est. Effort | Key Deliverables |
|-------|-------|--------|-------------|------------------|
| 5 | Multi-DB, Resume Hardening, Splitting | In progress | 6–8 weeks | ✅ all-DB dump+restore, ✅ import/restore resume, ✅ collection filters, ✅ split (jsonl/json/csv), ✅ adaptive batching, ✅ retry tuning; ⬜ multipart restart-resume |
| 6 | RDF Import MVP | ✅ Complete | 4–6 weeks | N-Triples/N-Quads/Turtle parsers, graph model (RPT/PGT), bulk load, CLI |
| 7 | Cloud Backends | Not started | 3–4 weeks | GCS, Azure, SeaweedFS support, docs, CI |

**Critical path:** Phase 5 → (Phase 6 done; Phase 7 independent given Phase 4 completion).

---

## Phase 5: Multi-Database, Resume Hardening, Splitting

### Overview

Phase 5 hardens resume semantics across import/dump/restore, extends dump to cover all accessible databases, adds adaptive rate limiting, supports large-file splitting with manifest tracking, and introduces collection/view filtering. The goal is production readiness for large-scale, interruption-resilient operations.

### 5.1 All-Databases Dump

**Current scope:** single-database dump via `DumpOptions::include_system`.

**Deliverables:**
- [x] Extend `DumpOptions` with `all_databases: bool` flag.
- [x] Enumerate `/_api/database` to discover accessible databases.
- [x] Create a separate dump context (`/_api/replication`) per database.
- [x] Namespace manifest artifacts by database (`databases/{db}/...`, plus a `database` field on each `Artifact`).
- [ ] Manifest top-level field: `databases: Vec<{ name, artifact_count, byte_size }>` (per-artifact `database` used instead).
- [x] Restore reads the combined manifest, groups by `(database, collection)`, creates each DB, and restores its collections in order.

**API changes:**
```rust
pub struct DumpOptions {
    pub include_system: bool,
    pub all_databases: bool,  // NEW
}

pub async fn dump_all_databases(
    client: &ArangoClient,
    storage: &dyn ObjectStore,
    base_path: &ObjectPath,
    options: DumpOptions,
    progress_tx: ProgressSink,
) -> Result<Manifest>;
```

**Exit criteria:**
- Dump all databases into a single manifest.
- Restore from all-database manifest creates each DB and populates all collections.
- Manifest correctly partitions artifacts by database.

---

### 5.2 Import Resume: Checkpoint-Driven Continuation

**Current state:** import pipeline has no resume; restart from beginning.

**Problem:** for large JSONL imports, network failure halfway through requires a full restart, risking duplicate insertion if duplicate mode is `insert`.

**Design:**
- Input sources (files, object storage) report `ByteOffset` capability.
- Batcher emits `CheckpointKey` (hash of the last N bytes of the completed batch, or byte-offset range for seekable sources).
- Sender persists completed batch keys to an idempotent checkpoint file (in storage or CLI-specified directory).
- On restart, import seeks/skips to the checkpoint and resumes.
- **Interaction with duplicate modes:** documented precisely:
  - `insert` + `replace`: idempotent; safe to resume (duplicate keys ignored).
  - `insert` + `error`: risky; document the at-least-once semantics; recommend pre-checking for duplicates.
  - `update`/`ignore`: safe.

**Deliverables:**
- [ ] Define `CheckpointKey` enum: `{ ByteOffset(u64) }` for seekable sources, `{ BatchHash(String) }` for streams.
- [ ] `ObjectStore::head` on checkpoint path to detect prior run.
- [ ] Extend `Batch` with an embedded checkpoint key.
- [ ] CLI flag: `--checkpoint-path` (default: current directory).
- [ ] Update import sender to persist checkpoint after each batch (atomic via `put_if_absent`).
- [ ] Resumable reader: `DocumentStream::resume_from(checkpoint_key)` seeks/skips accordingly.
- [ ] Document at-least-once guarantees and duplicate-mode interaction in `docs/resume.md`.

**API changes:**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CheckpointKey {
    ByteOffset(u64),
    BatchHash(String),
}

pub struct Batch {
    pub documents: Vec<Value>,
    pub byte_size: usize,
    pub checkpoint_key: CheckpointKey,  // NEW
}

pub async fn run_import<S>(
    client: &ArangoClient,
    storage: &dyn ObjectStore,
    source_path: &ObjectPath,
    collection: &str,
    options: &ImportOptions,
    checkpoint_path: Option<&ObjectPath>,  // NEW
) -> Result<ImportSummary>;
```

**Testing:**
- [ ] Unit: checkpoint serialization round-trip.
- [ ] Integration: interrupt import at 50%, resume from checkpoint, verify no duplication with `replace` mode.
- [ ] Negative: resume with wrong checkpoint key (error).

**Exit criteria:**
- Import 1M-document file, interrupt at 500K, resume from checkpoint, final count = 1M (no duplicates).

---

### 5.3 Restore Resume: Contiguous-Prefix Checkpointing

**Current state:** restore reads manifest, creates collections, restores data. No resume on interrupt.

**Problem:** restore of a large database can take hours; network failure means restarting with risk of duplicate collections or partial indexes.

**Design:**
- Restore creates a `restore.progress.json` in the checkpoint directory (separate from the dump, must be writable).
- After each collection successfully restores (data + indexes), emit a checkpoint listing completed collections in manifest order.
- On restart, read checkpoint, skip to the next incomplete collection, resume from there.
- For distributed restoration (Phase 5+), track per-shard completion.

**Deliverables:**
- [x] Define `RestoreCheckpoint` struct (in `arangodb-tools-core::manifest`): `{ manifest: String /* fingerprint */, completed: Vec<String> /* "{db}::{collection}" */ }`.
- [x] Restore emits checkpoint after each collection (best-effort write; a failure is logged, not fatal).
- [x] CLI flag: `--checkpoint` (a local path or `s3://...`, same style as import).
- [x] On startup, detect and read checkpoint; skip already-completed collections.
- [x] Mismatch safety: refuse to resume when the checkpoint's manifest fingerprint differs from the current dump.

**API changes:**
```rust
pub async fn restore_with_checkpoint(
    client: &ArangoClient,
    storage: &dyn ObjectStore,
    manifest_path: &ObjectPath,
    checkpoint_path: Option<&ObjectPath>,  // NEW
    options: &RestoreOptions,
) -> Result<()>;
```

**Testing:**
- [ ] Integration: dump → interrupt restore at collection 2 of 4 → resume → verify all 4 restored exactly once.
- [ ] Negative: attempt to resume restore from a different dump (checkpoint mismatch error).

**Exit criteria:**
- Restore a 4-collection dump, interrupt after collection 2, resume and complete without duplicating collections 1–2.

---

### 5.4 Large-Object Split & Manifest-Tracked Parts

**Current state:** export/dump writes single objects; large objects may be expensive to upload if interrupted.

**Problem:** a 10 GB collection export to S3 in one multipart upload is risky; mid-upload network failure requires re-uploading the entire object.

**Design:**
- Extend export/dump to split outputs at a configurable byte threshold (e.g., 100 MB per part).
- Manifest tracks split parts: `Artifact { parts: Vec<{ object_name, byte_offset, byte_size, checksum }> }`.
- Import/restore reader transparently concatenates parts.
- Multipart upload (Phase 5 spike): defer backend-specific restart-resumable multipart (S3 upload ID + parts) to a later micro-phase; for now, each part is written as an independent full object, so interruption only affects the current part.

**Deliverables:**
- [x] CLI flag: `--split-bytes` (bytes; the byte threshold per part; chosen over `--max-artifact-size`).
- [x] Each part is an independent, numbered artifact (`Artifact.part`) enumerated in the manifest; a nested `ArtifactPart`/byte-offset model was **not** needed because every part is a standalone, self-describing object with its own checksum and size.
- [x] Split logic in export: rotate to a new part when the byte threshold is reached, with per-part framing so each part is a valid document.
- [x] **Now covers all three output formats** (previously JSONL-only): JSONL cuts at line boundaries; JSON-array parts are each complete `[...]` arrays; CSV parts each repeat the header row.
- [x] Restore/re-import reader: the manifest enumerates parts; a reader consumes each part object in order (each part is independently valid).
- [ ] Benchmark: measure upload throughput with 100 MB vs 1 GB chunks to cloud storage (MinIO/S3). *(Deferred with cloud backends, Phase 7.)*

**Testing:**
- [x] Unit: split logic with small thresholds for jsonl, json, and csv (parts standalone-valid; concatenation reproduces every record).
- [ ] Integration: export with 10 MB part size against a live server. *(Covered by unit tests over the streaming path; live case deferred.)*

**Exit criteria:**
- Export a collection in size-bounded parts; manifest lists parts; concatenating parts reproduces every record. ✅ (cloud multipart benchmark deferred to Phase 7)

---

### 5.5 Adaptive Batching & Rate Limiting

**Current state:** fixed batch size and concurrency from config; no server-side feedback.

**Problem:** import/dump/restore may be CPU-bound (batching) or network-bound (concurrency); a fixed config doesn't adapt to server load or network conditions. Heavy restores can trigger 429 (rate limit) or 503 (temporary unavailable).

**Design:**
- Monitor server responses for 429/503 and backoff.
- If server returns 429, reduce concurrency; if no 429 for N minutes, slowly increase.
- Measure RTT and adjust batch size: if RTT > threshold, reduce batch size to keep latency acceptable.
- Document heuristics as "conservative default" so users can override.

**Deliverables:**
- [x] `AdaptiveLimiter` concurrency governor (`arangodb-import::adaptive`) — a resizable in-flight-send limiter driven by send outcomes.
- [x] `ConcurrencyConfig.adaptive: bool` (default true), surfaced as `--no-adaptive` on the CLI and an `adaptive=` kwarg in the Python binding. *(Chosen over a `RetryPolicy.adaptive` flag, since throttling is a concurrency concern, not a retry-policy concern.)*
- [x] Sender pool times each send (RTT); a send exceeding `slow_threshold` — the proxy for a server retrying 429/503 internally — or a terminal 429/502/503/504 halves concurrency down to a floor of 1, then recovers one slot per quiet `recover_after` window.
- [x] Metrics: `BatchingMetrics { final_concurrency, min_concurrency_seen, rate_limited_429, rate_limited_503, slow_sends, avg_rtt_ms }`, logged at end of run when the governor engaged. *(Batch size stays fixed; adaptivity is applied to concurrency, the higher-value lever, rather than mid-stream re-batching.)*

**Testing:**
- [x] Unit: throttle-halving to floor, slow-send throttling + recovery, disabled no-op, non-rate-limit errors ignored, and `acquire` blocking until a slot frees.
- [ ] Integration: import against a live server returning sustained 429. *(Governor state machine is unit-tested; the live-server case is deferred.)*

**Exit criteria:**
- Under sustained rate-limit signals, concurrency adapts downward to the floor and recovers; the pipeline keeps making progress (never stalls). ✅

---

### 5.6 Collection & View Filters

**Current state:** dump includes all non-system collections; restore restores all.

**Problem:** a large database may have 1000 collections, but user wants to dump only 10 for a partial restore.

**Design:**
- CLI flags: `--include-collections <REGEX>`, `--exclude-collections <REGEX>`.
- Filters applied at inventory read time (dump) and collection list in manifest (restore).
- Views and analyzers can also be filtered.

**Deliverables:**
- [x] `FilterOptions { include_collections: Option<Regex>, exclude_collections: Option<Regex> }` (view filters deferred).
- [x] Apply filters to inventory before dump; update manifest artifact list.
- [x] Restore reads manifest; filters already applied (manifest is the source of truth).
- [x] CLI flags: `--include-collections`, `--exclude-collections` (default: empty = include all).

**API changes:**
```rust
pub struct DumpOptions {
    pub include_system: bool,
    pub all_databases: bool,
    pub filters: FilterOptions,  // NEW
}

pub struct FilterOptions {
    pub include_collections: Option<Regex>,
    pub exclude_collections: Option<Regex>,
    pub include_views: Option<Regex>,
    pub exclude_views: Option<Regex>,
}
```

**Testing:**
- [ ] Unit: regex matching.
- [ ] Integration: dump with `--include-collections 'col_[0-9]+'`; verify manifest lists only matching collections.

**Exit criteria:**
- Dump 10 collections from a 20-collection database using include filter; manifest lists exactly 10.

---

### 5.7 Refined Adaptive Retry/Backoff

**Current state:** exponential backoff with fixed base/max delays.

**Problem:** retry delay tuning is not obvious (base delay too short → hammers server; too long → slow recovery).

**Design:**
- Collect telemetry (response time, error rate) to infer ideal backoff.
- Start conservative (long base delay), gradually reduce if no more errors.
- Document defaults and override mechanism.

**Deliverables:**
- [x] `RetryPolicy` now carries a configurable `multiplier` alongside `base_delay`/`max_delay` (the existing fields already covered `initial_backoff_ms`/`max_backoff_ms`).
- [x] Exponential backoff with full jitter, capped at `max_delay`; a `multiplier <= 1.0` disables growth.
- [x] CLI flags: `--max-retry-delay-secs` (the cap on any single backoff) and `--max-retries`, wired through the shared connection args into the client's `RetryPolicy`.
- [x] Logging: each retry logs the attempt, delay, and error at `debug`; a final give-up logs at `warn`.

**Testing:**
- [x] Unit: backoff growth for multipliers 2.0/3.0/1.0, cap enforcement, and jitter bounds.
- [ ] Integration: live server delay observation. *(Covered by deterministic unit tests over `backoff()`.)*

**Exit criteria:**
- Backoff is configurable per invocation, grows by the configured multiplier, and never exceeds `max_delay`. ✅

---

### Phase 5 Exit Criteria

- [x] All-database dump works and restores correctly.
- [x] Import resumes from checkpoint without duplication.
- [x] Restore resumes from checkpoint without duplication (fixture: multi-collection dump, interrupt, resume).
- [x] Large export splits into parts (jsonl/json/csv); concatenating parts validates counts.
- [x] Dump/restore with collection filters works.
- [x] Adaptive batching reduces concurrency under 429 load; throughput stays positive.
- [x] Retry/backoff is configurable and tunable.
- [x] All CLI flags documented.
- [~] CI passes; cloud-multipart benchmark deferred to Phase 7.

---

## Phase 6: RDF Import MVP

### Overview

RDF is a popular graph format; many users want to load Turtle, N-Triples, or N-Quads directly into ArangoDB. Phase 6 implements streaming RDF parsing, a pluggable graph model, and CLI integration. Out of scope: SPARQL queries, SHACL validation, other RDF serializations (listed for Phase 6+).

### 6.1 RDF Format Support

**Deliverables:**
- [ ] **N-Triples parser** (RFC 4790 + line-based): streaming, deterministic, errors on bad syntax.
  - Each line is `<subject> <predicate> <object> .` (with optional language tags / datatypes on literals).
  - Parser yields `(Subject, Predicate, Object)` triples.
- [ ] **Turtle parser** (W3C): streaming where possible (full Turtle prefix map requires one-pass scan).
  - Reuse existing crate (`oxttl` or `rio`); benchmark in a spike if needed.
  - Yields triples with IRI expansion.
- [ ] **N-Quads parser**: N-Triples + optional 4th field (graph IRI).
  - Each triple carries an optional named graph.
- [ ] Deferred: RDF/XML, TriG (phase 6+); VelocyPack (never).

**Crate choice** (from Phase 2 spike plan, §6 #2):
- Spike result pending; default to `oxttl` (part of `oxrdf`; proven, WASM-friendly).
- Fallback: `rio` (lighter-weight, streaming, fewer features).

**API shape:**
```rust
pub enum RdfTriple {
    Triple {
        subject: RdfResource,
        predicate: Iri,
        object: RdfValue,
        graph: Option<Iri>,  // for N-Quads
    },
}

pub enum RdfResource {
    Iri(Iri),
    BlankNode(String),
}

pub enum RdfValue {
    Iri(Iri),
    BlankNode(String),
    Literal { value: String, datatype: Option<Iri>, language: Option<String> },
}

pub async fn read_rdf_triples(
    input: ByteStream,
    format: RdfFormat,  // NTriples, Turtle, NQuads
) -> Result<impl Stream<Item = Result<RdfTriple>>>;
```

**Testing:**
- [ ] Unit: parse N-Triples fixtures; verify IRI/blank-node/literal extraction.
- [ ] Unit: parse Turtle with prefixes; verify prefix expansion.
- [ ] Integration: stream 100K triples; verify count.

**Exit criteria:**
- Parse N-Triples, Turtle, and N-Quads.
- Streaming: memory bounded by parser buffer, not input size.

---

### 6.2 Graph Model & Deterministic Key Generation

**Current state:** blank nodes and IRIs are unique; literals must be modeled.

**Problem:** RDF `_:b1 -> _:b2` blank-node edges are not persistent across files; user needs deterministic URIs. Literals can be embedded or materialized as vertices.

**Design (default model):**
- **Vertex collection:** one per named graph (or default graph if using N-Triples).
  - IRI resources → document `{ _key: <hash of IRI>, _data: { iri: "...", } }`.
  - Blank nodes → document `{ _key: <deterministic hash of provenance (file+line?)>, _data: { blank_node: true } }`.
- **Edge collection:** one per named graph.
  - Triple `(s, p, o)` where `o` is an IRI or blank node → edge `{ _from: "col/key_s", _to: "col/key_o", predicate: "..." }`.
  - Triple with literal → no edge (or optional: materialize literal as a vertex and add edge, user configurable).
- **Key generation:** SHA-256(IRI) → hex (32 chars); blank nodes salted with file+line.
- **Literal policy (configurable):**
  - `NoLiterals`: drop triples with literal objects (default).
  - `EmbedInEdge`: store literal in edge doc `{ literal_value: "...", literal_type: "..." }`.
  - `MaterializeAsVertex`: create vertex for each unique literal, edge to it.

**Deliverables:**
- [ ] `RdfGraphModel` enum with variants for literal handling.
- [ ] Deterministic key generation: `key_from_resource(resource, file?, line?) -> String` using SHA-256.
- [ ] Bulk-load pipeline: RDF triples → vertex/edge `Value` documents → import batches.
- [ ] Config: `--rdf-literal-policy` (no-literals, embed-in-edge, materialize).
- [ ] Config: `--rdf-vertex-collection`, `--rdf-edge-collection`.

**API changes:**
```rust
pub enum RdfLiteralPolicy {
    NoLiterals,
    EmbedInEdge,
    MaterializeAsVertex,
}

pub struct RdfOptions {
    pub literal_policy: RdfLiteralPolicy,
    pub vertex_collection: String,
    pub edge_collection: String,
    pub source_provenance: bool,  // store file/line in metadata
}

pub async fn import_rdf(
    client: &ArangoClient,
    source_path: &ObjectPath,
    format: RdfFormat,
    options: &RdfOptions,
    import_options: &ImportOptions,
) -> Result<RdfImportSummary>;

pub struct RdfImportSummary {
    pub vertices_created: u64,
    pub edges_created: u64,
    pub triples_read: u64,
}
```

**Testing:**
- [ ] Unit: key generation determinism (same IRI → same key).
- [ ] Unit: literal policy application (drop vs embed vs materialize).
- [ ] Integration: import Turtle fixture (e.g., DBpedia snippet); verify vertices + edges.

**Exit criteria:**
- Import 10K-triple RDF; vertex/edge counts match expected model.
- Deterministic keys verified (re-import same RDF, no new vertices created).

---

### 6.3 CLI Integration

**Deliverables:**
- [ ] `arangox rdf import` subcommand.
- [ ] Args: `--endpoint`, `--database`, `--username`, `--password`, `--collection`, `--rdf-vertex-collection`, `--rdf-edge-collection`, `--rdf-literal-policy`, `--format` (ntriples / turtle / nquads).
- [ ] Input source: file or `s3://` URI (reuses import pipeline).
- [ ] Progress output: "Read 50K triples, created 48K vertices, 49K edges."
- [ ] Error handling: malformed RDF → clear error with line number.

**API shape:**
```rust
pub struct RdfImportArgs {
    endpoint: Url,
    database: String,
    collection: String,
    vertex_collection: String,
    edge_collection: String,
    literal_policy: RdfLiteralPolicy,
    source_path: String,  // file or s3://...
    format: RdfFormat,
    #[flatten]
    import_options: ImportOptions,
}

pub async fn run(args: RdfImportArgs) -> Result<()>;
```

**Testing:**
- [ ] CLI runs `arangox rdf import --help`; docs print.
- [ ] CLI import Turtle from local file; verify vertices + edges in ArangoDB.

**Exit criteria:**
- `arangox rdf import` command works end-to-end.
- Help text documents all options.

---

### 6.4 Deferred RDF Features — status

Done (previously deferred):
- [x] **Blank-node provenance scoping.** `RdfOptions::blank_node_scope` salts
  blank-node keys with a per-source scope so identical `_:label`s in different
  files stay distinct, while a single import keeps a stable scope (repeated
  references to a label resolve to one node; re-import is idempotent). The CLI
  `--blank-node-scope` defaults to the input path. Note: the granularity is
  per-source (document), **not** file+line — per-line salting would wrongly
  split multiple references to the same blank node within a document.
- [x] **N-Quads named-graph routing.** `RdfOptions::named_graph`
  (`NamedGraphMode::Ignore`/`Property`/`Collection`, CLI `--named-graph`). The
  graph IRI can be recorded on edges and folded into the edge key (so the same
  triple in different graphs is distinct), and optionally routes edges into
  per-graph edge collections `<edge>_<slug>`. Vertices are intentionally **not**
  routed per graph (an IRI may belong to many graphs and must remain a single
  shared vertex), so graph membership lives on the statement/edge.

Still out of scope:
- RDF/XML, TriG parsing (Phase 6+).
- 100K-triple throughput benchmark (deferred to perf-baselining work).
- SPARQL queries (out of scope for bulk-load tools).
- SHACL validation (out of scope).
- Configurable IRI normalization / domain-specific key strategies (Phase 6+).

---

### Phase 6 Exit Criteria

- [ ] Parse N-Triples, Turtle, N-Quads correctly and fully.
- [ ] Deterministic key generation verified (re-import → no new vertices).
- [x] Literal policies tested (all three).
- [x] Import into Docker ArangoDB; verify counts (live integration tests, incl. RPT + N-Quads named-graph routing).
- [x] CLI help and error messages clear.
- [x] Blank-node provenance scoping and N-Quads named-graph routing (`--blank-node-scope`, `--named-graph`).
- [ ] Benchmark: import 100K triples, measure throughput (docs/sec, MB/sec) — deferred.

---

## Phase 7: Cloud Backends

### Overview

Phase 2 delivered S3-compatible (MinIO) support via `object_store` crate. Phase 7 extends to Google Cloud Storage (GCS), Azure Blob Storage, and documents SeaweedFS (S3-compatible first). The goal is feature parity across backends and clear deployment docs.

### 7.1 Google Cloud Storage (GCS)

**Current state:** `object_store` supports GCS via `GoogleCloudStorage` variant.

**Deliverables:**
- [ ] Feature gate: `feature = "gcs"` (optional, disabled by default; nightly CI).
- [ ] URI parsing: `gs://bucket/prefix` → `GoogleCloudStorage` backend.
- [ ] Credentials: GOOGLE_APPLICATION_CREDENTIALS (JSON keyfile path) or Application Default Credentials.
- [ ] Testing:
  - [ ] Unit: URI parsing for GCS schemes.
  - [ ] Integration (nightly): create GCS bucket, import/export/dump/restore cycle.
  - [ ] Verify multipart handling (large object split across GCS resumable uploads).
- [ ] Docs:
  - [ ] Setup guide: create service account, download keyfile, set env var.
  - [ ] Example: `arangox import --endpoint arangodb.example.com --database test --collection items gs://my-bucket/items.jsonl`.
  - [ ] Limitations (e.g., resumable uploads via application default credentials have specific token expiry).

**API shape (transparent via `StorageUri`):**
```rust
// User specifies URI; storage backend is auto-detected.
let storage = open_storage("gs://my-bucket/prefix")?;
storage.put_stream(&path, stream).await?;
```

**Testing:**
- [ ] Integration: round-trip (dump/restore) via GCS.
- [ ] Error handling: missing keyfile → clear error.

**Exit criteria:**
- GCS backend round-trips data successfully.
- Docs complete with real example.

---

### 7.2 Azure Blob Storage

**Current state:** `object_store` supports Azure via `MicrosoftAzure` variant.

**Deliverables:**
- [ ] Feature gate: `feature = "azure"`.
- [ ] URI parsing: `azure://container/prefix` (or `abfs://` variant) → `MicrosoftAzure` backend.
- [ ] Credentials: `AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY` or Managed Identity (IMDS).
- [ ] Testing:
  - [ ] Unit: URI parsing.
  - [ ] Integration (nightly): create Azure storage account + container, round-trip.
- [ ] Docs:
  - [ ] Setup: storage account, key retrieval, env var setup.
  - [ ] Example: `arangox dump --endpoint arangodb.example.com azure://myaccount/backup --all-databases`.

**Testing:**
- [ ] Integration: dump/restore via Azure.

**Exit criteria:**
- Azure backend functional.
- Docs complete.

---

### 7.3 SeaweedFS

**Current state:** SeaweedFS exposes an S3-compatible API; `object_store` S3 backend can use S3-compatible endpoints.

**Deliverables:**
- [ ] Document path: use S3 backend with custom `--s3-endpoint` flag pointing to SeaweedFS S3 gateway.
- [ ] Example config: SeaweedFS running on `seaweedfs.local:33333`; `--s3-endpoint http://seaweedfs.local:33333`.
- [ ] Integration test (optional, gate behind feature): spin up SeaweedFS in Docker, round-trip.
- [ ] Docs:
  - [ ] SeaweedFS deployment (single node vs cluster).
  - [ ] S3 gateway setup and performance notes.
  - [ ] Example: `arangox import --endpoint arangodb.example.com --s3-endpoint http://seaweedfs.local:33333 s3://bucket/items.jsonl`.

**Testing:**
- [ ] Integration (optional): SeaweedFS Docker container, round-trip.

**Exit criteria:**
- SeaweedFS documented as a working S3-compatible backend.

---

### 7.4 Cross-Backend Feature Testing

**Deliverables:**
- [ ] CI matrix: test each backend (local, S3/MinIO, GCS, Azure) for:
  - [ ] Import JSONL.
  - [ ] Export collection.
  - [ ] Dump database.
  - [ ] Restore database.
  - [ ] Large-file split (Phase 5).
  - [ ] Resume (Phase 5).
- [ ] Performance baseline: throughput on each backend (MB/sec, docs/sec).
- [ ] Nightly job: GCS, Azure, SeaweedFS (or on-demand for GCS/Azure to avoid costs).

**Exit criteria:**
- CI matrix passes for all backends and critical operations.
- Throughput baselines recorded.

---

### 7.5 Backend-Specific Documentation

**Deliverables:**
- [ ] `docs/backends.md`:
  - [ ] Architecture (how `StorageUri` detects and routes to backends).
  - [ ] Feature matrix (which backends support resumable uploads, multipart, etc.).
  - [ ] Setup per backend with real examples.
  - [ ] Troubleshooting (credential errors, endpoint misconfiguration, etc.).
  - [ ] Performance recommendations (batch size, concurrency tuning per cloud provider).

**Exit criteria:**
- Docs complete; examples tested (at least locally).

---

### Phase 7 Exit Criteria

- [ ] GCS, Azure backends functional (dump/restore cycle).
- [ ] SeaweedFS documented as S3-compatible.
- [ ] CI matrix runs; all backends pass critical operations.
- [ ] Docs complete with setup + examples per backend.
- [ ] Performance baselines established.

---

## Cross-Phase Concerns

### Testing & CI Strategy

| Layer | New Test Coverage | When |
|-------|-------------------|------|
| Resume/checkpoint | Import/restore resume from checkpoint (Phases 5) | Phase 5 |
| Collection filters | Dump with include/exclude regex (Phase 5) | Phase 5 |
| RDF parsing | N-Triples, Turtle, N-Quads fixtures (Phase 6) | Phase 6 |
| RDF model | Vertex/edge counts, deterministic keys (Phase 6) | Phase 6 |
| Large-file split | Export with 10 MB parts; re-import (Phase 5) | Phase 5 |
| Adaptive batching | Throttle server responses, verify concurrency adapts (Phase 5) | Phase 5 |
| Cloud backends | Round-trip per backend (Phase 7) | Phase 7 |

### Documentation Updates

| Doc | Updates |
|-----|---------|
| `docs/IMPLEMENTATION_PLAN.md` | Mark phases complete; link to remaining work |
| `docs/resume.md` | NEW: checkpoint semantics, at-least-once guarantees, duplicate-mode interaction |
| `docs/dump-format.md` | Add: multi-database structure, split artifacts, all-databases manifest shape |
| `docs/rdf-model.md` | NEW: graph model, literal policies, key generation |
| `docs/backends.md` | NEW: per-backend setup, feature matrix, performance notes |
| `docs/cli-reference.md` | NEW or UPDATE: all Phase 5–7 CLI flags |
| `README.md` | Update status; note Phase 5–7 in progress |

### Known Constraints & Risks

| Item | Mitigation |
|------|-----------|
| Restart-resumable multipart uploads (Phase 5) | Deferred; S3-specific implementation in Phase 5.5 or later. For now, each part is a full object; interruption at part boundary only. |
| RDF crate choice (Phase 6) | Spike needed; `oxttl` likely; fallback to `rio` if performance poor. |
| GCS/Azure credential management (Phase 7) | Defer complex auth flows (SAML, Workload Identity) to docs; start with simple env-var setup. |
| All-database dump consistency (Phase 5) | Document that multi-database dumps are *per-database* snapshots, not cluster-wide. Single-server consistency model. |

---

## Suggested Sequencing

1. **Week 1–2:** Phase 5.1 (all-databases dump) + Phase 5.2 (import resume).
   - Checkpoint infrastructure used by all later resume work.
   - All-DB dump enables production multi-database scenarios.
2. **Week 3–4:** Phase 5.3 (restore resume) + Phase 5.6 (filters).
   - Resume work stabilizes checkpointing across all tools.
3. **Week 5–6:** Phase 5.4 (split) + Phase 5.5 (adaptive batching) + Phase 5.7 (retry tuning).
   - Large-object handling and rate-limit resilience.
4. **Week 7–8:** Phase 6 (RDF) in parallel with early Phase 7 (GCS/Azure docs + tests).
   - RDF is independent; cloud backend setup can start while RDF parser is implemented.
5. **Week 9–10:** Phase 7 completion; final CI matrix; performance baselines.

---

## Success Metrics

- **Phase 5:** All resume scenarios tested and working; production-scale imports/restores survive interruption.
- **Phase 6:** 10K+ triple RDF imports; deterministic keys verified.
- **Phase 7:** Multi-backend round-trip success; throughput parity across backends.
- **Overall:** All tests green; docs complete; benchmark targets met or exceeded.

---

## Appendix: Test Fixtures

### Phase 5

- **1M-doc JSONL** (50 MB): for import resume.
- **4-collection dump:** for restore resume.
- **100 MB object:** for split testing.
- **Intentional server 429/503 responses:** for adaptive batching.

### Phase 6

- **N-Triples fixture** (1000 triples): FOAF example.
- **Turtle fixture** (1000 triples): with prefixes.
- **N-Quads fixture** (1000 quads): with named graphs.
- **Blank-node collision fixture:** two files with identical blank-node names; key generation should differ (provenance-based salting).

### Phase 7

- **Local file (always):** JSONL test set.
- **S3/MinIO:** same JSONL (reuse Phase 2 test).
- **GCS:** same JSONL (nightly CI).
- **Azure:** same JSONL (nightly CI).
- **SeaweedFS:** same JSONL (optional nightly CI).

---

## Appendix: API Stability Checklist

- [ ] All public types in Phase 5–7 crates serialize/deserialize correctly (manifest, checkpoint, etc.).
- [ ] Breaking changes documented (if any).
- [ ] CLI flags finalized before public release.
- [ ] Error messages stable and user-friendly.

