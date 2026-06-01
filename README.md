# arangodb-data-tools-rs

A Rust library and CLI toolkit for ArangoDB bulk data workflows: **import**, **export**, **dump**, and **restore**, with first-class support for pluggable storage backends (local files and object storage such as S3, GCS, Azure, and SeaweedFS) and planned RDF bulk-loading.

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

This is a Cargo workspace. Crates (most are stubs today; see the implementation plan for build order):

| Crate | Purpose |
|-------|---------|
| `arangodb-tools-core` | Shared config, errors, retry, concurrency, progress, manifest types |
| `arangodb-client` | ArangoDB HTTP client (connection, auth, TLS, API helpers) |
| `arangodb-storage` | `ObjectStore` abstraction: local FS, S3, GCS, Azure, SeaweedFS |
| `arangodb-import` | Bulk import (CSV/TSV/JSON/JSONL) |
| `arangodb-export` | Export via AQL cursors (JSONL/JSON/CSV/XGMML) |
| `arangodb-dump` | Database dump |
| `arangodb-restore` | Database restore |
| `arangodb-rdf` | RDF bulk import (N-Triples, Turtle, ...) |
| `arangodb-tools-cli` | CLI binaries (`arangox-*`) |

## Building

Requires a stable Rust toolchain (see `rust-toolchain.toml`).

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## Compatibility

- Targets ArangoDB **3.12** and current stable.
- The project manifest format is **canonical**; compatibility with official `arangodump`/`arangorestore` is limited to a tested subset (single-server, JSONL, no Enterprise encryption) and is best-effort elsewhere.
- VelocyPack data and Enterprise-encrypted dumps are **not** supported yet; the tools will refuse them with a clear error rather than mishandle them.

Compatibility limits will be documented as they are validated by tests.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Issues and discussion are welcome while the project is taking shape.

## License

Licensed under the [MIT License](LICENSE).
