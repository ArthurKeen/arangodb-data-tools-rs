# arangodb-data-tools-rs

A Rust library and CLI toolkit for ArangoDB bulk data workflows: **import**, **export**, **dump**, and **restore**, with first-class support for pluggable storage backends (local files and object storage such as S3, GCS, Azure, and SeaweedFS) and RDF bulk-loading (N-Triples, N-Quads, and Turtle).

> **Status: pre-alpha / under active development.**
> This project is a clean-room reimplementation modeled on the behavior of ArangoDB's client tools (`arangoimport`, `arangoexport`, `arangodump`, `arangorestore`). It does **not** embed or link ArangoDB's C++ client code. Interoperability with the official tools is a scoped, best-effort goal and is not yet guaranteed. APIs, formats, and CLI options will change without notice until the first tagged release.

## Why

The official ArangoDB client tools are capable but assume local-filesystem output, use blocking I/O, and offer limited resumability and observability. This project aims to provide:

- An **async, streaming** pipeline with bounded memory and explicit backpressure.
- A **storage abstraction** so dumps/exports can stream directly to object storage.
- A **manifest-driven** dump/export format that is the canonical source of truth (no filename guessing).
- **Symmetric, resumable** dump and restore.
- **Structured, redacted** observability and **TLS verification by default**.
- Reusable **library APIs with typed builders**, with thin CLIs on top.

See [`RUST_ARANGODB_TOOLS_PRD.md`](RUST_ARANGODB_TOOLS_PRD.md) for the full product requirements and [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) for the engineering plan.

## Workspace layout

This is a Cargo workspace. See [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) for build order and design notes.

| Crate | Purpose | Status |
|-------|---------|--------|
| `arangodb-tools-core` | Shared config, errors, retry, concurrency, progress, manifest types | Implemented |
| `arangodb-client` | ArangoDB HTTP client (connection, auth, TLS, version, collections, cursor, import, replication) | Implemented |
| `arangodb-storage` | `ObjectStore` abstraction: local FS and S3-compatible (via `object_store`), plus URI parsing and compression | Local FS + S3-compatible |
| `arangodb-import` | Streaming bulk import (CSV/TSV/JSON/JSONL) with bounded batching and resumable checkpointing | Implemented |
| `arangodb-export` | Export via AQL cursors (JSONL/JSON/CSV), with optional size-split JSONL + manifest | Implemented |
| `arangodb-dump` | Database dump (manifest-driven) | Implemented |
| `arangodb-restore` | Database restore from a dump | Implemented |
| `arangodb-rdf` | RDF bulk import into a property graph (N-Triples, N-Quads, Turtle) | Implemented |
| `arangodb-tools-cli` | The `arangox` CLI: `import`, `export`, `dump`, `restore`, `rdf` subcommands | Implemented |

> Storage backends: local filesystem and S3-compatible object stores (AWS S3, MinIO/LocalStack, SeaweedFS via its S3 gateway) are wired today through `AWS_*` environment configuration. GCS (`gs://`) and Azure (`az://`) are planned and currently rejected with a clear error.

## Building

Requires a stable Rust toolchain (see `rust-toolchain.toml`).

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## Usage

The CLI is a single binary, `arangox`, with `import`, `export`, `dump`, `restore`, and `rdf` subcommands. All subcommands share connection flags: `--endpoint` (default `http://localhost:8529`), `--database` (default `_system`), `--username`, `--password-env`/`--auth-token-env` (names of env vars holding the secret; secrets are never passed on the command line), `--tls-ca`, and `--insecure`.

```bash
# Build, then run via cargo (or use the compiled ./target/release/arangox)
cargo run -p arangodb-tools-cli -- <subcommand> [flags]
```

Import CSV/TSV/JSON/JSONL (format inferred from the extension, or set `--format`). Input can be a file, `-` for stdin, a `file://` URI, or `s3://bucket/key`:

```bash
arangox import \
  --endpoint http://localhost:8529 --database mydb \
  --username root --password-env ARANGO_PASSWORD \
  --collection users --input users.jsonl --create-collection

# Resumable: re-running with the same checkpoint skips committed batches
arangox import --collection users --input users.jsonl \
  --checkpoint users.checkpoint.json
```

Export a collection or AQL query to JSONL/JSON/CSV (CSV requires `--fields`). Output can be a file, `file://`, or `s3://bucket/key`:

```bash
arangox export --collection users --output users.jsonl
arangox export --query 'FOR u IN users FILTER u.active RETURN u' \
  --output active.jsonl
# Split large exports into size-bounded JSONL parts plus a manifest
arangox export --collection events --output events.jsonl --split-bytes 134217728
```

Dump a database and restore it (the manifest is the source of truth):

```bash
arangox dump --database mydb --output ./dump-mydb
arangox restore --database mydb-copy --input ./dump-mydb --create-database
```

Bulk-load RDF (N-Triples, N-Quads, or Turtle) into a property graph. Each IRI/blank node becomes a vertex, and each triple becomes an edge carrying the predicate IRI; keys are deterministic (hashed) so re-importing the same data is idempotent. The vertex and edge collections are created if missing:

```bash
arangox rdf import --database mydb \
  --input graph.ttl \
  --vertex-collection rdf_nodes --edge-collection rdf_links
```

Literal-valued objects are dropped by default (`--literal-policy no-literals`); use `--literal-policy vertex-property` to attach them to the subject vertex, or `--literal-policy materialize` to give each literal its own vertex plus an edge. The Turtle parser covers a practical subset (prefixes/base, `a`, predicate/object lists, blank-node property lists, collections, and typed/numeric/boolean literals); RDF-star is not supported. As edges are parsed they stream to a concurrent loader, so only the deduplicated vertices are buffered.

Object storage uses the `AWS_*` environment for credentials/region/endpoint, which also works against MinIO/LocalStack and SeaweedFS's S3 gateway. `gs://` and `az://` are not wired yet.

### Machine-readable output (`--output json`)

For programmatic callers (e.g. driving the CLI as a subprocess from Python or Go), pass the global `--output json` flag. The result becomes a single JSON object on **stdout**, newline-delimited progress events stream on **stderr**, and errors are rendered as a JSON object on stderr with a non-zero exit code.

```bash
arangox --output json import --collection users --input users.jsonl
```

stdout (the result):

```json
{"operation":"import","status":"ok","collection":"users","documents_sent":1000,"batches":10,"created":1000,"errors":0,"updated":0,"ignored":0,"empty":0,"bytes_sent":123456,"elapsed_secs":1.23,"docs_per_sec":813.0}
```

stderr (newline-delimited progress; `import` emits periodic `progress` snapshots):

```json
{"event":"started","operation":"import"}
{"event":"progress","bytes_read":0,"bytes_written":65536,"documents":600,"batches":6,"server_errors":0,"retries":0,"elapsed_secs":1.0}
{"event":"finished","bytes_read":0,"bytes_written":123456,"documents":1000,"batches":10,"server_errors":0,"retries":0,"elapsed_secs":1.23}
```

All four subcommands emit a JSON result, `started`/`finished` events, and mid-run `progress` events: `import` and single-file `export` emit time-based snapshots (~1s), while `dump`, `restore`, and split `export` emit a snapshot as each collection/part completes.

### From Python or Go

Two integration paths are supported:

1. **Subprocess + `--output json`** (works for all tools today): run `arangox`, parse stdout for the result, read stderr line-by-line for progress, and use the exit code for success/failure. Language-agnostic.
2. **Native Python bindings** (sketch): a PyO3/maturin module under [`bindings/python`](bindings/python) binds the import pipeline in-process as `arangox.import_file(...)`, returning a `dict`. See its README for build/usage.

## Compatibility

- Targets ArangoDB **3.12** and current stable.
- The project manifest format is **canonical**; compatibility with official `arangodump`/`arangorestore` is limited to a tested subset (single-server, JSONL, no Enterprise encryption) and is best-effort elsewhere.
- VelocyPack data and Enterprise-encrypted dumps are **not** supported yet; the tools will refuse them with a clear error rather than mishandle them.

Compatibility limits will be documented as they are validated by tests.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Issues and discussion are welcome while the project is taking shape.

## License

Licensed under the [MIT License](LICENSE).
