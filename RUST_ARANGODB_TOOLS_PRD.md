# Product Requirements Document: Rust ArangoDB Data Tools Library

## 1. Overview

This document defines requirements for a new Rust library and CLI toolkit that reimplements the core behavior of ArangoDB's client data tools in a more extensible architecture. The initial scope covers the capabilities represented by `arangoimport`, `arangoexport`, `arangodump`, and `arangorestore`, with first-class support for pluggable storage backends and future RDF bulk-loading workflows.

The project should live in a separate repository from `arangodb/arangodb`. The ArangoDB repository should be used as a behavioral and interoperability reference, not as code to port line-for-line. Current ArangoDB source releases are licensed under Business Source License 1.1, so this project should implement behavior through public APIs, public documentation, black-box testing, and observed file formats.

## 2. Goals

- Provide a Rust library for ArangoDB bulk import, export, dump, and restore workflows.
- Support local files and distributed/object storage backends including S3, GCS, Azure Blob or Data Lake, and SeaweedFS.
- Expose reusable library APIs that can be embedded into custom data pipelines.
- Provide CLI tools that are compatible with common ArangoDB workflows where practical.
- Add RDF bulk import capabilities inspired by ArangoRDF while keeping the RDF pipeline independently extensible.
- Support high-throughput, resumable, observable data movement for large datasets.
- Build a testable, modular architecture that avoids coupling parsing, transport, storage, and ArangoDB API logic.

## 3. Non-Goals

- Do not embed or link ArangoDB C++ client tool code.
- Do not attempt full option-for-option CLI compatibility in the first release.
- Do not reimplement `arangosh`, Foxx tooling, backup administration, or benchmarking.
- Do not require a local source build of ArangoDB.
- Do not target cluster-topology-aware dump/restore in the first release. MVP dump/restore is designed and tested against single servers; cluster support is post-MVP (see §8.4).
- Do not rely on private server APIs unless no public alternative exists and the risk is documented.
- Do not support Enterprise-only encryption behavior in the first release unless a compatible public format is specified.

## 4. Target Users

- Developers who need programmable ArangoDB bulk data workflows.
- Data engineers moving data between ArangoDB and cloud/object storage.
- Teams building custom ETL or graph ingestion pipelines.
- Users who need RDF-to-ArangoDB loading without adopting a complete external platform.
- Operators who want resumable dump/restore flows for large databases.

## 5. Key Use Cases

### 5.1 Import Documents From Storage

A user imports CSV, TSV, JSON array, or JSONL data from local disk, S3, GCS, Azure, or SeaweedFS into an ArangoDB collection using configurable batching, concurrency, duplicate handling, and schema-validation options.

### 5.2 Export Query Results To Storage

A user exports an entire collection, graph, or custom AQL query result to JSONL, JSON, CSV, or XGMML-compatible output, writing directly to object storage without requiring an intermediate local directory.

### 5.3 Dump Database To Object Storage

A user creates an ArangoDB-compatible dump of one database or all accessible databases and writes metadata plus collection data to a selected storage backend.

### 5.4 Restore Database From Object Storage

A user restores collections, indexes, views, and data from a dump stored in local or object storage. Restore should support continuation after interruption when the storage backend supports durable manifests/checkpoints.

### 5.5 Bulk Load RDF

A user imports RDF formats such as Turtle, N-Triples, N-Quads, RDF/XML, or TriG into ArangoDB by mapping RDF resources to vertex documents and triples to edge documents, with configurable graph modeling rules.

## 6. Product Principles

- Library first, CLI second.
- Storage should be abstracted from data processing.
- Streaming should be preferred over full-file buffering.
- Resume and retry should be designed in from the beginning.
- The default data model should be simple, but advanced users must be able to override it.
- Favor public ArangoDB APIs over assumptions about internal implementation.
- Compatibility should be tested against live ArangoDB containers.

## 7. Proposed Crate Structure

The repository should be a Cargo workspace.

```text
arangodb-tools-rs/
  crates/
    arangodb-client/
    arangodb-tools-core/
    arangodb-storage/
    arangodb-import/
    arangodb-export/
    arangodb-dump/
    arangodb-restore/
    arangodb-rdf/
    arangodb-tools-cli/
```

### 7.1 `arangodb-client`

Responsibilities:

- Authentication and connection configuration.
- HTTP request execution.
- Retry policy integration.
- Database and collection API helpers.
- Cursor API helpers.
- Import, dump, restore, and replication endpoint wrappers.
- JSON and VelocyPack support if required.

Initial endpoint families to support:

- `/_api/version`
- `/_api/database`
- `/_api/collection`
- `/_api/import`
- `/_api/cursor`
- `/_api/replication/inventory`
- `/_api/replication/clusterInventory`
- `/_api/replication/dump`
- `/_api/replication/restore-collection`
- `/_api/replication/restore-data`
- `/_api/replication/restore-indexes`
- `/_api/replication/restore-view`
- `/_api/dump/start`
- `/_api/dump/{id}`
- `/_api/dump/next/{id}`

### 7.2 `arangodb-tools-core`

Responsibilities:

- Shared configuration types.
- Progress reporting.
- Error taxonomy.
- Concurrency primitives.
- Batching and byte accounting.
- Checkpoint metadata.
- Logging/tracing integration.
- Common validation.

### 7.3 `arangodb-storage`

Responsibilities:

- Abstract object/file storage.
- Local filesystem backend.
- S3-compatible backend.
- GCS backend.
- Azure backend.
- SeaweedFS backend, likely through S3-compatible APIs first.
- Streaming read/write.
- Multipart upload.
- Object listing and prefix traversal.
- Atomic-ish commit patterns using manifests.

Suggested traits:

```rust
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_stream(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata>;
    /// Create-only put (fails if the object already exists). Used for
    /// manifests and checkpoints to detect concurrent writers.
    async fn put_if_absent(&self, path: &ObjectPath, input: ByteStream) -> Result<ObjectMetadata>;
    /// Multipart upload whose state (upload id, completed parts) is exposed
    /// so uploads can resume across process restarts where the backend
    /// supports it (required by §10).
    async fn start_multipart(&self, path: &ObjectPath) -> Result<Box<dyn MultipartUpload>>;
    async fn get_stream(&self, path: &ObjectPath, range: Option<ByteRange>) -> Result<ByteStream>;
    /// Object metadata (size, etag/checksum) without fetching the body;
    /// `None` when the object does not exist (subsumes a bare `exists`).
    async fn head(&self, path: &ObjectPath) -> Result<Option<ObjectMetadata>>;
    /// Streaming/paginated listing; dump prefixes can contain many objects.
    fn list(&self, prefix: &ObjectPath) -> BoxStream<'_, Result<ObjectMetadata>>;
    async fn delete(&self, path: &ObjectPath) -> Result<()>;
}
```

The final design may use the Rust `object_store` crate if it satisfies backend and streaming requirements. The trait must remain able to express every §10 reliability requirement (notably restart-resumable uploads and conditional puts) — gaps here ripple through every backend, so the trait is reviewed against §10 before the first cloud backend lands.

### 7.4 `arangodb-import`

Responsibilities:

- CSV, TSV, JSON, and JSONL parsing.
- Batch construction for ArangoDB import API.
- Collection creation/truncation options.
- Duplicate handling options.
- Attribute removal and basic transformations.
- Parallel sender workers.
- Rate limiting and adaptive batching.
- Import statistics and error reporting.

### 7.5 `arangodb-export`

Responsibilities:

- Collection export through AQL cursor queries.
- Custom AQL export.
- JSONL, JSON, CSV, and XGMML output.
- Graph export where feasible.
- Streaming output directly to storage.

### 7.6 `arangodb-dump`

Responsibilities:

- Database inventory retrieval.
- Collection structure export.
- Index/view metadata export.
- Collection data export using replication/dump APIs where appropriate.
- Dump manifest creation.
- Optional compression.
- Multi-database support.
- Cluster-aware dump path where exposed by server APIs.

### 7.7 `arangodb-restore`

Responsibilities:

- Read dump manifest and collection metadata.
- Create databases when requested.
- Create or overwrite collections.
- Restore views.
- Restore data.
- Restore indexes.
- Continue interrupted restores from checkpoints.
- Validate dump compatibility before mutating server state.

### 7.8 `arangodb-rdf`

Responsibilities:

- Parse RDF formats.
- Normalize IRIs, blank nodes, literals, datatypes, and language tags.
- Map RDF terms to ArangoDB documents.
- Map RDF triples/quads to edges.
- Generate deterministic keys.
- Support configurable graph models.
- Bulk-load generated vertex and edge batches through import pipeline.

### 7.9 `arangodb-tools-cli`

Responsibilities:

- Provide binaries such as `arangox-import`, `arangox-export`, `arangox-dump`, `arangox-restore`, and `arangox-rdf`.
- Map CLI options to library config structs.
- Provide progress output and machine-readable logs.
- Support config files and environment variables.

## 8. Functional Requirements

### 8.1 Connection and Authentication

- Support endpoint URL, database name, username, and password.
- Support password via environment variable, prompt, or secrets provider hook.
- Support JWT/bearer token authentication if practical.
- Support TLS configuration, custom CA certificates, and insecure development mode.
- Support request timeouts and retry policies.
- Fail with a clear, actionable error when server permissions are insufficient for the requested operation (e.g. all-databases dump and `_users` restore require `_system`-level access). Where a cheap check exists, preflight permissions before starting long-running work.

### 8.2 Import

- Accept input from local path, object storage URI, stdin, or stream.
- Support CSV, TSV, JSON array, and JSONL.
- Support automatic type inference from file extension.
- Support reading gzip- and zstd-compressed inputs, selected by file extension (e.g. `.jsonl.gz`, `.jsonl.zst`) with an explicit override, so compressed exports produced by this project (§8.3) can be re-imported directly.
- Support explicit collection name.
- Support create collection and create database options.
- Support document and edge collection creation.
- Support overwrite/truncate behaviors.
- Support duplicate handling modes equivalent to insert, update, replace, ignore, and error where ArangoDB supports them.
- Support configurable batch size and sender concurrency.
- Support optional progress reporting.
- Preserve detailed per-batch error context.
- Support large files without loading them fully into memory.
- Stream JSONL incrementally; for JSON array input, either parse incrementally or document and enforce the in-memory limitation with a clear error rather than silently buffering an oversized file.
- Define overwrite/truncate semantics on mid-import failure. The reference C++ tool truncates non-atomically on the first batch only; the Rust implementation should either use a server-side truncate-and-import path or clearly document that a crash after truncate but before completion leaves an empty/partial collection.
- Validate `_from`/`_to` presence as a client-side preflight when importing into an edge collection, instead of relying solely on server-side rejection.
- Support `_from`/`_to` key-prefixing options for edge imports, equivalent to `arangoimport`'s `--from-collection-prefix`/`--to-collection-prefix`. The edge preflight must accept bare keys when a prefix option will rewrite them.
- Import delivery semantics are at-least-once: a retried or resumed import may re-send documents from an incomplete batch. Document this, recommend deterministic `_key` strategies for idempotency, and define the interplay with duplicate modes (resuming with `onDuplicate=error` would fail spuriously on re-sent documents; resume requires an idempotent mode or documented behavior).

#### 8.2.1 Future Input Formats (post-alpha)

The reader layer normalizes every input format into a stream of JSON documents, and the import pipeline accepts any such stream, so library users can already feed it documents from any source they can parse. The formats below are candidates for *first-class* support (extension inference, CLI flag, documented type mapping, tests) and are deliberately **out of MVP/alpha scope** — each new format multiplies the integration-test matrix and must not block the JSONL/CSV exit criteria.

- **Parquet (highest-value future format).** Columnar Parquet on object storage is the lingua franca for the data-engineering target users, and `arangoimport` cannot read it, so this is genuine differentiation rather than parity work. Parquet row groups are independently readable, seekable units that align naturally with the range-read `ObjectStore` trait and provide clean import-resume checkpoint boundaries (row-group index rather than byte offsets). Costs: a heavy `arrow`/`parquet` dependency (gate behind a Cargo feature so the core stays lean) and a documented, explicitly-lossy type mapping (timestamps, decimals, nested structs/lists → JSON). Slot as a post-alpha milestone.
- **Arrow IPC (rider on Parquet).** If the `arrow` dependency is taken for Parquet, an Arrow IPC reader is nearly free. Arrow is an interchange/in-memory format and files at rest are rare, so support it only as a documented rider on the Parquet feature, not as an independently promised format. Do **not** make Arrow an internal columnar fast path; the JSON-document pipeline is the right altitude until benchmarks prove otherwise.
- **Neo4j export formats (a mapping pipeline, not a reader).** A Neo4j → ArangoDB migration path is strategically valuable for adoption. Target the *documented export* formats produced by `neo4j-admin`/APOC — GraphML, APOC JSON, CSV-with-header-conventions — **not** Neo4j's internal binary `.dump` store format, which is undocumented, version-specific, and effectively reverse-engineering (the most fragile possible code, breaking on every Neo4j release). Property-graph import needs modeling decisions (node labels → vertex collections, relationship types → one or many edge collections, property type coercion) exactly as RDF does, so it belongs as a sibling of `arangodb-rdf` (e.g. an `arangodb-graph-import` crate) that maps to vertices/edges and bulk-loads through the existing import pipeline, not as another `ImportFormat` variant.

### 8.3 Export

- Export collection contents.
- Export custom AQL query results.
- Support JSONL as the default streaming-friendly format.
- Support JSON array for compatibility.
- Support CSV with selected fields.
- Support gzip or zstd compression.
- Support splitting large exports into multiple objects.
- Write a manifest describing output files, format, schema hints, and source query.
- Graph export and XGMML output (referenced in §5.2 and §7.5) are explicitly post-MVP: they are not scheduled in the MVP milestones and are tracked here so the use case is deferred deliberately rather than silently dropped.

### 8.4 Dump

- Dump one database by default.
- Support all accessible databases.
- Support collection filters and system collection inclusion.
- Dump collection structure and properties.
- Dump indexes.
- Dump views.
- Dump collection data.
- Preserve enough metadata for restore to recreate collections faithfully.
- Write `dump.json`-like top-level metadata for compatibility where feasible.
- Define and document the dump consistency model. A dump must capture a stable snapshot per collection (and per shard where applicable): create the replication batch / dump context **before** reading the inventory, so the inventory and all subsequent data reads observe the same snapshot. Document exactly what is and is not guaranteed (single-server point-in-time snapshot vs. weaker cross-shard guarantees on clusters) in `docs/dump-format.md` and user-facing docs — "is my dump consistent while writes continue?" must have a written answer.
- Produce a project-specific manifest that is the canonical source of truth for restore. The manifest must enumerate every artifact (structure, view, data, split parts) with format, compression, byte size, and checksum so restore never has to guess filenames. (The reference C++ restore tries six filename variants plus a split-file regex; the manifest eliminates this.)
- The manifest and checkpoint formats carry an explicit format-version field from day one, with a documented evolution policy (newer readers must read older dumps). Dumps are long-lived artifacts; the canonical format cannot be unversioned.
- Support resumable dump as a first-class capability, not a later add-on. Checkpoint per database, collection, and shard (last server tick/dump-id position, completed split parts) so an interrupted dump can resume without re-fetching completed collections. (The reference C++ dump has no resume at all and leaves partial files on failure.)
- Keep server-side handles alive for the duration of a transfer: periodically extend replication batch TTLs and dump-id/cursor lifetimes, and always release them on completion, error, or cancellation. (The reference tool uses a 600s replication batch TTL that must be extended; crashes can leak server-side batches.)
- Prefer the parallel `/_api/dump/*` protocol as the primary data path, with the legacy `/_api/replication/dump` path as a documented fallback for older servers. Apply the unified retry policy (see §10) to both paths.
- Support local filesystem output matching ArangoDB's conventional dump layout as an optional, explicitly-scoped compatibility mode (see §15).
- MVP scope is single-server dump. Cluster-aware dump (`/_api/replication/clusterInventory`, shard-level parallelism across DB-Servers) is post-MVP; the tool must detect a cluster deployment and either use a tested code path or fail with a clear error, never silently misbehave.

### 8.5 Restore

- Restore from local path or object storage prefix.
- Validate manifest and dump contents before destructive operations.
- Support create database.
- Support create collection.
- Support restore data.
- Support restore indexes.
- Restore collections in dependency order: `distributeShardsLike` prototypes before their followers, document collections before edge collections, system collections handled explicitly. Restore `_analyzers` data before views and other collections, and restore `_users` data last (it can invalidate the active credentials mid-restore).
- Make index-creation order relative to data loading configurable, and benchmark both orders before fixing the default: indexes-before-data surfaces unique-constraint violations early; indexes-after-data typically loads faster. The reference C++ restore creates all non-vector indexes before data and vector indexes after data ("restore indexes first, but skip vector indexes, since they cannot be created without data" — `client-tools/Restore/RestoreFeature.cpp`); matching it is the compatibility-safe default candidate. Vector indexes must always be created after data is loaded (a functional requirement, not a tuning choice). Document the tradeoff alongside the chosen default.
- Create arangosearch views before data and search-alias views after data (their links require target collections to exist). Note that arangosearch links present during data load reduce load throughput; document this tradeoff and consider deferring link creation as a configurable optimization.
- Support restoring into a different topology: options to override `numberOfShards` and `replicationFactor`, and to strip cluster-only collection properties when restoring a cluster-produced dump into a single server (and vice versa).
- Support overwrite behavior.
- Support collection and view filters.
- Support continuing interrupted restore. Use cheap checkpoints (logical document keys or uncompressed offsets recorded in the manifest) rather than byte offsets into compressed files, to avoid the O(n) read-and-discard seek cost the reference C++ restore incurs on gzip data.
- Restore checkpoints must not require write access to the dump location: least-privilege storage credentials (§17) may be read-only for the source prefix. The checkpoint location is independently configurable (e.g. a separate URI or local state directory), with a sensible default and a clear error when no writable location is available.
- Record progress after each collection phase.
- Fail safely before data mutation when configuration is inconsistent.

### 8.6 RDF Import

- Support at least N-Triples and Turtle for MVP.
- Add N-Quads, TriG, and RDF/XML later.
- Generate deterministic `_key` values for resources.
- Represent RDF literals with value, datatype, and language metadata.
- Support named graph handling.
- Support blank node strategy configuration.
- Support prefix mapping.
- Support configurable predicate-to-edge mapping.
- Support collection naming rules.
- Support incremental dictionary building for very large RDF inputs.

Default RDF model:

- One vertex collection for RDF terms.
- One edge collection for triples.
- Subject and object resources become vertices.
- Literal objects are represented either as literal vertices or embedded edge attributes, configurable by policy.
- Predicate IRI is stored on each edge and may optionally determine edge collection.

## 9. Storage Requirements

### 9.1 URI Scheme

The library should accept storage URIs like:

```text
file:///data/dump
s3://bucket/prefix
gs://bucket/prefix
az://container/prefix
seaweed+s3://bucket/prefix
```

### 9.2 Object Store Semantics

Object storage backends should not assume POSIX rename or directory atomicity. The project should rely on:

- Content-addressed or final object names.
- Multipart upload completion.
- Manifest files written last for completed dumps/exports.
- Checkpoint files for in-progress restore/import.
- Idempotent object naming where possible.

### 9.3 Compression

- Support no compression for debuggability.
- Support gzip for compatibility.
- Support zstd for performance-oriented workflows.
- Compression should be stream-based.

### 9.4 Encryption

MVP should not attempt to match ArangoDB Enterprise dump encryption. Optional client-side encryption may be added later using a project-defined envelope format.

However, the tools must not silently mishandle encrypted dumps. Restore and any dump-reading path must:

- Read and honor the ArangoDB `ENCRYPTION` marker file when present.
- Fail loudly with a clear, actionable error when asked to read an Enterprise-encrypted dump that this project cannot decrypt, rather than producing corrupt output.
- Write an explicit `ENCRYPTION`/encryption-type field in the project manifest for dumps the tools produce.

Note that in ArangoDB's format gzip compression and encryption are mutually exclusive; the project must preserve that constraint when writing compatibility-mode output.

## 10. Reliability Requirements

- A single unified retry policy must apply to all HTTP operations, including dump data fetches, `restore-data`, `restore-collection`, `restore-indexes`, import sends, and cursor pagination. (The reference C++ tools are inconsistent here: the legacy dump path and all restore-data calls have no automatic retry, while the parallel dump path retries up to 100 times.)
- Retries must distinguish retryable transport errors (connect/read/write failures, gateway timeouts, cluster timeouts, HTTP 429/503) from non-retryable server errors, and use bounded backoff.
- Object uploads should be resumable where backend support exists.
- Both dump and restore should checkpoint by database, collection, shard, and phase (symmetric resumability; see §8.4 and §8.5).
- Import should checkpoint input offsets only for seekable/range-readable sources.
- Error messages should include collection, object path, byte range, batch number, and server response where applicable.
- Prefer recoverable, structured errors over process-aborting fatal exits. (The reference tools call `FATAL_ERROR_EXIT()` in many paths, including parallel dump errors, which prevents cleanup and resume.)
- Destructive operations must require explicit configuration.

## 11. Performance, Concurrency, and Backpressure Requirements

### 11.1 Performance Targets

Initial performance targets should be expressed as relative goals until benchmarking is available.

- Import throughput should at least match `arangoimport` for JSONL on a local network in MVP, and the design should aim to saturate either the server or the network/storage link rather than the client. (The C++ baseline is itself bottlenecked by blocking senders and parser-thread pacing, so "within 50%" is a floor, not a goal.)
- This target is measured, not assumed: the test suite includes a benchmark harness that runs official `arangoimport` and `arangox-import` against the same fixture and the same Docker server and reports relative throughput (see §16.2). A relative performance requirement without a comparison harness is unfalsifiable.
- Export throughput should primarily be limited by ArangoDB cursor performance or storage write bandwidth, not by client-side buffering.
- Dump and restore should support concurrent collection and shard processing.
- Memory usage should remain bounded by configured batch sizes, in-flight-byte limits, and worker counts.
- RDF import should stream parse inputs and avoid retaining the full graph in memory unless requested.

### 11.2 Concurrency and Backpressure Model

The pipeline must have an explicit, documented backpressure model. This is the single largest structural weakness of the reference C++ tools (single-slot senders with 10 ms polling, unbounded task queues, and parser-thread sleeps).

- Use bounded async channels (e.g. `tokio::sync::mpsc`) between pipeline stages (read → optional transform → batch → send). Producers must block/await on a full channel rather than spinning or growing memory.
- Cap total in-flight bytes across all workers, independent of worker count, so memory stays bounded regardless of server latency.
- A single batch must never exceed the global in-flight-byte cap: clamp batch size to the cap (or reject the configuration at validation time), since a permit request larger than the semaphore's capacity can never be granted and deadlocks the pipeline.
- Use a single async work-queue abstraction with structured error propagation for dump/restore parallelism. Do not maintain two separate concurrency systems (the C++ tools have both a `ClientTaskQueue` and bespoke per-server thread pools), and do not swallow worker errors.
- Avoid busy-wait/polling loops; use async notification.

### 11.3 Adaptive Rate Limiting

- Provide optional server-feedback-driven adaptive batching: adjust batch size and send pacing based on observed round-trip latency and server pushback (HTTP 429/503), not on a bytes-sent heuristic alone.
- Rate limiting must not block the reader/parser stage (the C++ AutoTune sleeps on the parser thread, reducing read throughput).

## 12. Observability Requirements

- Use `tracing` for structured logs.
- Provide human-readable progress for CLI usage.
- Provide machine-readable JSON progress events.
- Track bytes read, bytes written, documents processed, batches sent, server errors, retry counts, and elapsed time.
- Report progress in terms of documents processed and throughput (docs/sec, bytes/sec) with an ETA where total size is known, not just input bytes read. (The C++ import reports input bytes, which misleads on variable-size rows.)
- Define a concrete progress-event schema emitted by the library and rendered by the CLI, so the library owns progress data and the CLI owns presentation (the library should not write directly to stdout).
- Expose progress callbacks in the library API.
- Provide final summaries suitable for CI logs.

## 13. CLI Requirements

### 13.1 Common Options

```text
--endpoint
--database
--username
--password-env
--password-prompt
--auth-token-env
--tls-ca
--insecure
--threads
--batch-size
--storage-config
--log-format text|json
--progress
--dry-run
```

### 13.2 Import CLI

```text
arangox-import \
  --endpoint http://localhost:8529 \
  --database mydb \
  --collection users \
  --type jsonl \
  --input s3://bucket/users.jsonl
```

### 13.3 Dump CLI

```text
arangox-dump \
  --endpoint http://localhost:8529 \
  --database mydb \
  --output s3://bucket/backups/mydb/2026-05-26
```

### 13.4 Restore CLI

```text
arangox-restore \
  --endpoint http://localhost:8529 \
  --input s3://bucket/backups/mydb/2026-05-26 \
  --create-database \
  --overwrite
```

### 13.5 RDF CLI

```text
arangox-rdf import \
  --endpoint http://localhost:8529 \
  --database knowledge \
  --input gs://datasets/example.ttl \
  --format turtle \
  --vertex-collection rdf_terms \
  --edge-collection rdf_triples
```

## 14. Library API Requirements

The library should expose strongly typed builders rather than requiring callers to construct CLI-style argument strings.

Example target API:

```rust
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb")
    .basic_auth("root", password)
    .build()?;

let storage = ObjectStoreConfig::from_uri("s3://bucket/imports")?.build().await?;

ImportJob::builder()
    .client(client)
    .storage(storage)
    .input("users.jsonl")
    .collection("users")
    .format(ImportFormat::JsonLines)
    .batch_size_bytes(16 * 1024 * 1024)
    .workers(8)
    .run()
    .await?;
```

## 15. Compatibility Strategy

- The project manifest format is canonical. ArangoDB `arangodump`/`arangorestore` interoperability is an explicitly scoped, best-effort compatibility mode, not the primary format.
- Compatibility mode targets a tested subset: single-server dumps, JSONL data, no Enterprise encryption. Everything outside that subset (cluster filename conventions, VelocyPack data, split files requiring matching tool versions, envelope format) is best-effort and must be documented as such.
- Run black-box tests against official ArangoDB Docker images.
- Start with ArangoDB 3.12 and current stable.
- Add compatibility tests for 3.11 if useful.
- Generate small dumps with official `arangodump` and ensure the Rust restore can read supported subsets.
- Generate dumps with Rust and ensure official `arangorestore` can read compatibility-mode output where practical.
- Avoid promising full compatibility until covered by tests.

## 16. Testing Strategy

### 16.1 Unit Tests

- URI parsing.
- Storage path handling.
- CSV/TSV parser edge cases.
- JSONL and JSON array batching.
- RDF term normalization.
- Key generation.
- Manifest serialization.
- Retry classification.

### 16.2 Integration Tests

- Run ArangoDB in Docker.
- Create database and collections.
- Import JSONL and CSV.
- Export data and compare counts.
- Dump and restore database.
- Restore selected collections.
- Validate indexes and views where supported.
- Negative compatibility fixtures: an Enterprise-encrypted dump (`ENCRYPTION` marker) and a VelocyPack dump must be refused loudly — assert the error messages and that no partial server-state mutation occurs (§9.4, §19).
- Throughput benchmark harness: official `arangoimport` vs `arangox-import` on a shared JSONL fixture against the same server (§11.1).

### 16.3 Storage Tests

- Local filesystem backend in CI.
- S3-compatible tests using LocalStack or MinIO.
- SeaweedFS via S3-compatible mode.
- GCS and Azure tests behind optional feature flags or nightly CI.

### 16.4 RDF Tests

- Known small RDF fixtures.
- Blank node fixtures.
- Language-tagged literals.
- Datatype literals.
- Named graph fixtures.
- Round-trip validation through AQL count and sample queries.

## 17. Security Requirements

- Never log passwords, tokens, or signed URLs.
- Redact credentials in debug output. Redaction must also cover custom AQL queries and bind variables, which can contain sensitive data and must not be echoed to stdout/logs by default. (The C++ export prints the custom query and bind vars with `--progress`.)
- Avoid logging the username on connection by default (the C++ tools log it on connect/failure).
- Support TLS certificate verification by default, with support for a custom CA bundle. This is a deliberate behavior change from the reference C++ client tools, whose `SslClientConnection` defaults to certificate verification disabled and expose no client CA-file option, meaning transport is encrypted but unauthenticated.
- Require explicit opt-in for insecure TLS (`--insecure`).
- Avoid writing credentials to manifests.
- Support least-privilege object storage credentials.
- Validate object paths to avoid accidental writes outside configured prefixes.

## 18. Milestones

### Milestone 0: Repository Foundation

Deliverables:

- Cargo workspace.
- CI with formatting, clippy, tests.
- Basic error type.
- Tracing setup.
- ArangoDB Docker integration harness.
- Local filesystem storage backend.

Exit criteria:

- CI passes.
- Integration test can connect to ArangoDB and call `/_api/version`.

### Milestone 1: Import MVP

Deliverables:

- JSONL import.
- JSON array import.
- CSV import.
- stdin input and gzip/zstd-compressed input files.
- Basic collection creation.
- Configurable batch size and worker count.
- Benchmark harness comparing throughput against official `arangoimport` (§11.1).
- CLI wrapper.

Exit criteria:

- Import 1 million JSONL documents into local Docker ArangoDB.
- Verify document counts.
- Memory remains bounded by configured batching.
- Benchmark harness runs and reports throughput relative to `arangoimport`.

### Milestone 2: Object Storage Foundation

Deliverables:

- S3-compatible backend.
- Object-store URI parsing.
- Streaming reads.
- Streaming writes.
- MinIO integration tests.

Exit criteria:

- Import JSONL directly from S3-compatible storage.
- Export JSONL directly to S3-compatible storage.

### Milestone 3: Export MVP

Deliverables:

- Collection export via cursor API.
- Custom AQL export.
- JSONL and JSON output.
- CSV output with explicit fields.
- Compression support.

Exit criteria:

- Export collection to local and S3-compatible storage.
- Re-import exported JSONL and validate counts.

### Milestone 4: Dump and Restore MVP

Deliverables:

- Single-database dump.
- Collection metadata dump.
- Index metadata dump.
- Collection data dump.
- Restore structure and data.
- Manifest and checkpoint format.

Exit criteria:

- Dump a database to local storage and restore it into a fresh database.
- Dump a database to S3-compatible storage and restore it.
- Validate collection counts and indexes.
- Encrypted (`ENCRYPTION` marker) and VelocyPack dumps are refused with clear errors.
- Dump consistency semantics documented in `docs/dump-format.md`.
- Index-ordering benchmark run and default recorded (§8.5).

### Milestone 5: Multi-Database and Resume

Deliverables:

- All-databases dump.
- Restore continuation.
- Import resume from input-offset checkpoints for seekable/range-readable sources (§10), with documented at-least-once semantics (§8.2).
- Adaptive batching and rate limiting driven by server feedback (§11.3).
- Restore topology overrides: `numberOfShards`/`replicationFactor` overrides and cluster-only property stripping (§8.5).
- Collection/view filters.
- Better retry policy.
- Split large data files/objects.

Exit criteria:

- Interrupted restore can resume without duplicating completed collections.
- Interrupted import resumes from its checkpoint without unbounded duplication (idempotent-key fixture).
- Large object split restore works from S3-compatible storage.

### Milestone 6: RDF Import MVP

Deliverables:

- N-Triples parser integration.
- Turtle parser integration.
- Default RDF graph model.
- Deterministic key generation.
- RDF import CLI.

Exit criteria:

- Import known RDF fixtures.
- Validate expected vertex and edge counts.
- Support at least one configurable literal policy.

### Milestone 7: Cloud Backends

Deliverables:

- GCS backend.
- Azure backend.
- SeaweedFS documented backend path.
- Backend-specific docs.

Exit criteria:

- Smoke tests pass for each backend in configured environments.
- Documentation includes credentials and deployment examples.

## 19. Open Design Questions

### Resolved

- Dump format: the Rust manifest is canonical; official `arangorestore` compatibility is a scoped best-effort mode (see §15).
- VelocyPack: MVP uses JSON/JSONL only. The tools must refuse to read VPack dumps with a clear error rather than mishandling them, and VPack support is deferred to a later milestone.
- Enterprise encryption / encrypted dumps: not supported in MVP; restore must detect them via the `ENCRYPTION` marker and fail loudly (see §9.4).
- Import semantics: at-least-once with documented idempotent-`_key` guidance (see §8.2). Exactly-once is out of scope.
- JSON array input: parsed incrementally with bounded memory (one top-level element held at a time), so no in-memory size limit is needed.
- Cluster-aware dump/restore: post-MVP; MVP detects cluster deployments and fails clearly rather than misbehaving (see §3, §8.4).
- Graph export / XGMML: post-MVP (see §8.3).
- Input formats beyond CSV/TSV/JSON/JSONL (Parquet, Arrow IPC, Neo4j exports): post-alpha; Neo4j internal binary `.dump` files are explicitly out of scope (see §8.2.1).

### Still open

- Which RDF crate provides the best streaming support and format coverage?
- Should SeaweedFS be treated only as S3-compatible, or should native APIs be supported?
- How should restore handle Enterprise-only collection properties (beyond encryption)?
- What is the default index-creation order relative to data load (pending the Milestone 4 benchmark; see §8.5)?
- What is the default RDF key strategy for IRIs and literals?
- Should RDF predicates map to one edge collection or many edge collections by default?
- Parquet type-mapping policy (timestamps, decimals, nested types → JSON) and which Neo4j export format to target first (gated on the §8.2.1 post-alpha work).

## 20. Recommended Initial Dependencies

Candidate crates:

- `tokio` for async runtime.
- `reqwest` or `hyper` for HTTP.
- `serde`, `serde_json` for JSON.
- `clap` for CLI parsing.
- `tracing`, `tracing-subscriber` for logs.
- `thiserror`, `anyhow` for errors.
- `object_store` for pluggable object storage if suitable.
- `csv` for CSV and TSV parsing.
- `flate2` for gzip.
- `zstd` for zstd compression.
- `sha2` or `blake3` for deterministic key generation.
- `oxrdf`, `rio`, or related RDF crates after evaluation.

Dependency choices should be finalized after small spikes, especially for object storage and RDF parsing.

## 21. Acceptance Criteria For First Public Alpha

- A user can import JSONL from local disk into ArangoDB.
- A user can import JSONL from S3-compatible storage into ArangoDB.
- A user can export a collection to JSONL in local and S3-compatible storage.
- A user can dump and restore a small database using local storage.
- A user can dump and restore a small database using S3-compatible storage.
- The CLI has documented examples.
- The library has typed builders for import, export, dump, and restore jobs.
- CI runs unit tests and ArangoDB Docker integration tests.
- Documentation clearly describes compatibility limits.

## 22. Suggested Repository Bootstrap Checklist

- Create Cargo workspace and crate directories.
- Add `rustfmt.toml`, clippy configuration, and CI.
- Add `README.md` with project goals and warning about compatibility status.
- Add `LICENSE` after deciding project licensing.
- Add `docs/architecture.md`.
- Add `docs/dump-format.md`.
- Add `docs/rdf-import.md`.
- Add Docker Compose or testcontainers setup for ArangoDB.
- Implement `arangodb-client` version check.
- Implement local `ObjectStore`.
- Implement JSONL import MVP.
