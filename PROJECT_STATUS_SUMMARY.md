# ArangoDB Data Tools (Rust) — Project Status Summary

**Status:** Phases 0–7 complete. Pre-alpha, version `0.1.0`, no release tagged.
**Last updated:** 2026-10-05 (commit `a66988c`).

For graded project health and the full gap analysis, see [`docs/scorecard.md`](docs/scorecard.md).
This document is the delivery view: what shipped, what is left, and in what order.

---

## What's done

| Phase | Title | Key deliverables |
|-------|-------|------------------|
| **0** | Foundations | Error taxonomy, retry, bounded pipeline, work queue, progress events, config, manifest types, Docker integration harness |
| **1** | Import MVP | JSONL/JSON/CSV/TSV readers, gzip/zstd, batch sender pool, duplicate modes, collection creation, edge validation, benchmark harness |
| **2** | Object storage | S3-compatible backend, local filesystem, URI parsing, multipart writes, streaming ranged reads, `put_if_absent`, listing |
| **3** | Export MVP | Cursor-based collection/AQL export, JSONL/JSON/CSV, compression, size-split + manifest, parallel export |
| **4** | Dump & restore MVP | Single-server inventory/structure/data dump, `/_api/replication` protocol, manifest-driven restore, index ordering, dependency resolution |
| **5** | Multi-DB, resume, splitting | All-databases dump, import resume (batch-index checkpoints), restore resume (fingerprint-bound), large-object split, adaptive batching, collection filters, retry tuning |
| **6** | RDF import | N-Triples/N-Quads/Turtle streaming parsers, PGT and RPT graph models, deterministic hashed keys, literal and named-graph policies |
| **7** | Cloud backends | GCS, Azure, SeaweedFS via the S3 gateway, cross-backend CI matrix, per-backend setup docs |

**CLI:** one `arangox` binary with `import`, `export`, `dump`, `restore`, and `rdf`
subcommands, sharing one set of connection, auth, and output flags.

**Quality gates, verified on `a66988c`:** 275 tests pass (0 fail, 1 ignored), clippy clean
under `-D warnings`, `cargo fmt --check` clean. CI runs against a live ArangoDB 3.12 service,
with the cross-backend matrix on a weekly schedule.

**Scale:** 9 crates, ~13,500 lines of Rust under `src/`, 13 integration test files.

---

## What's remaining

Phase numbering ended at 7. Remaining work comes from the PRD requirement audit: **41 open
gaps — 17 missing, 24 partial** — out of 130 requirements. Ranked by consequence.

### P0 — correctness and data safety

| Item | Requirements | Why it ranks here |
|------|------|------|
| **Dump and restore views** | REQ-038, REQ-058 | ArangoSearch and `search-alias` definitions are parsed from the inventory and then dropped. A dump of a view-using database is silently incomplete — the only open gap that loses data without an error. |
| **Read the `ENCRYPTION` marker** | REQ-078 | Encryption is detected only via the manifest, which an official ArangoDB dump directory does not carry. |
| **Restore dependency ordering** | REQ-056 | `distributeShardsLike` prototypes, `_analyzers` first, `_users` last. Restoring `_users` mid-run can invalidate the running credentials. |
| **Wire the built-but-uncalled capabilities** | REQ-076, REQ-087, REQ-105, REQ-124 | `put_if_absent`, three `ErrorContext` builders, the redaction helper, and two progress counters are each implemented, tested, and called from nowhere. Concurrent dumps to one prefix currently overwrite each other silently. |

### P1 — stated goals with nothing behind them

| Item | Requirements | Notes |
|------|------|------|
| **Typed library builders** | REQ-112 | A §21 first-alpha acceptance criterion. The §14 API sketch does not compile against the real crates. |
| **`arangodump`/`arangorestore` interop** | REQ-041, REQ-048, REQ-115, REQ-116 | Neither direction exists. Scoped best-effort in the PRD, disclosed in the README, but a headline goal. |
| **Resumable dump** | REQ-045, REQ-085 | Import and restore resume; dump restarts from zero. The PRD asks for symmetry. |

### P2 — breadth and polish

| Item | Requirements |
|------|------|
| Restore topology overrides and collection/view filters | REQ-059, REQ-061 |
| Parallel `/_api/dump/*` protocol; concurrent collection/shard processing | REQ-047, REQ-092, REQ-098 |
| Human-readable CLI progress (text mode has none) | REQ-017, REQ-103, REQ-106 |
| Index-ordering benchmark and configurable order | REQ-057 |
| Export manifest for non-split exports; schema hints | REQ-032 |
| RDF predicate-to-edge mapping; incremental dictionary for large inputs | REQ-072, REQ-074, REQ-094 |
| Permission-failure error clarity | REQ-006 |
| Negative compatibility fixtures | REQ-119 |

---

## Recommended sequence

1. **Views (REQ-038/058).** Highest consequence, self-contained, and the only item that
   changes whether a dump can be trusted as a backup.
2. **The uncalled-capability sweep.** Mostly wiring, closes a disproportionate share of the
   partial requirements, and removes the silent concurrent-dump overwrite.
3. **Typed builders (REQ-112).** Clears the last §21 alpha criterion and stabilizes the
   library surface before anyone depends on it.
4. **Interop, one direction first.** Reading official dumps is the more useful half.

Items 1–3 are what stand between this project and a defensible `v0.1.0` alpha tag.

---

## Key design decisions

1. **Async-first, bounded everywhere.** Every pipeline stage has backpressure via bounded
   channels plus a global in-flight-byte semaphore.
2. **Manifest is canonical.** No filename guessing; the manifest is written last, so a
   truncated dump is detectably incomplete rather than quietly partial.
3. **Storage abstraction.** Nothing above `arangodb-storage` knows local from S3.
4. **Batch-index resume, not byte offsets.** Deterministic batching means a restart re-derives
   the same batch sequence, so resume works for non-seekable sources too. A contiguous
   high-water mark keeps concurrent out-of-order sends from advancing the checkpoint past an
   uncommitted batch.
5. **Fail loudly over silent misbehavior.** Dump refuses clusters by server role; restore
   refuses encrypted and VelocyPack dumps; an inconclusive role probe warns and proceeds
   rather than blocking single-server users.
6. **`object_store` for S3/GCS/Azure.** Proven and correct; avoids hand-rolling three
   credential flows.

---

## References

- [`docs/scorecard.md`](docs/scorecard.md) — graded health and the full gap analysis.
- [`RUST_ARANGODB_TOOLS_PRD.md`](RUST_ARANGODB_TOOLS_PRD.md) — source of truth for requirements.
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — architecture and phase definitions.
- [`docs/cli-reference.md`](docs/cli-reference.md) — every subcommand and flag.
- [`docs/benchmarks.md`](docs/benchmarks.md) — throughput method and caveats.
