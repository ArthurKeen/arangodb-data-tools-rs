# Project Scorecard — arangodb-data-tools-rs

_Last updated: 2026-10-05 (revised later the same day: JWT authentication implemented, and a
parse-time crash found in `dump`/`export` and fixed — see Authentication and the defect-shape
section)._

_Method: full requirement audit of `RUST_ARANGODB_TOOLS_PRD.md`
(130 extracted requirements, every classification carrying a `file:line` citation checked
mechanically against the source), plus a live `cargo test --workspace` /
`cargo clippy -- -D warnings` run on commit `a66988c`._

## Overall: **A− / the core is solid and honestly scoped; the gaps are in breadth, not in foundations**

Four data pipelines — import, export, dump, restore — plus an RDF loader and a five-scheme
storage abstraction are built, tested, and documented. The engineering discipline is real:
**275 tests pass, zero fail, clippy is clean under `-D warnings`**, and CI runs against a live
ArangoDB 3.12 service with a weekly cross-backend matrix.

What keeps this short of an alpha is coverage breadth, not instability. The silent data-loss
gap that dominated the August audit — search views parsed out of the inventory and then
dropped — is closed: views now dump and restore, in the two passes the server's own ordering
rules require, verified by a round-trip that queries the restored view rather than merely
checking it exists. What remains in that area is narrower and documented: custom analyzers are
not captured, so a view that uses one restores without error and then fails at query time.

| Dimension | Grade | One-line |
|---|---|---|
| PRD requirement coverage | B+ | 88 of 130 implemented; 24 partial, 17 missing, 1 explicitly out of scope |
| Core pipeline correctness | A− | Bounded backpressure, deterministic batch-index resume, manifest-last writes; dump refuses clusters by role |
| Backup completeness | B+ | Collections, indexes, data and search views all round-trip. Custom analyzers are not dumped, so a view using one restores without error but cannot be queried |
| Interoperability | D | Neither direction of `arangodump`/`arangorestore` compatibility exists; honestly disclosed, but a headline PRD goal |
| Engineering quality | A | 275 tests green, clippy clean at `-D warnings`, live-server CI, 13.5k lines of Rust across 9 crates |
| Observability | B | Structured JSON results and NDJSON progress on all five subcommands; two counters are wired to nothing and always report `0` |
| Security posture | A− | Secrets never on argv or in process listings, TLS verified by default, `Secret` blocks `Debug`/`Display`, four explicit auth modes with conflicts refused; query/bind-var redaction helper still called from nowhere |
| Library ergonomics | C+ | Crates are clean and documented, but the typed builders the PRD names as an alpha criterion are not built |
| Documentation honesty | A | Every known limit is written down; no capability is claimed that the audit could not evidence |

## Requirement coverage

Measured against the 130 requirements extracted from PRD §8–§17, §21, and the §18 milestones.

| Status | Count | Meaning |
|---|---:|---|
| Implemented | 88 | Evidenced by a verified `file:line` citation |
| Partial | 24 | Capability present but incomplete against the requirement as written |
| Missing | 17 | No implementation |
| Out of scope | 1 | Explicitly excluded |

All 41 open gaps are tracked as individual alerts rather than aggregated into a backlog
number, so each one carries its own evidence and failure description.

## What is built

**Import.** Streaming CSV/TSV/JSON/JSONL with bounded batching, an adaptive sender pool that
halves concurrency under `429`/`503` or slow round trips and recovers as pressure eases, and
resume from deterministic batch-index checkpoints that works for any source — including
non-seekable stdin, which the original requirement had excluded.

**Export.** AQL-cursor-driven export to JSONL/JSON/CSV with optional compression and
size-bounded splitting, where each part is a standalone valid document in its format.

**Dump and restore.** A replication-batch snapshot taken before the inventory read, so a dump
is point-in-time consistent per collection and mutually consistent across collections within
one database. The manifest is written last and is the canonical source of truth — restore
never guesses a filename. Restore is resumable and refuses a checkpoint belonging to a
different dump by manifest fingerprint.

**Safety rails that fail loudly.** Dump probes the server's deployment role and refuses a
cluster coordinator, DB-Server, or agent, naming the role. Restore refuses encrypted and
VelocyPack dumps. An inconclusive role probe warns and proceeds rather than blocking
single-server users over a diagnostic call.

**Authentication.** Four credential modes with exactly one selectable at a time: basic, user
JWT via `POST /_open/auth`, superuser JWT minted locally from the server's JWT secret (the
equivalent of `--server.jwt-secret-keyfile`), and a caller-supplied token. The two JWT modes
track token expiry and refresh before it lapses — ArangoDB issues one-hour tokens, shorter than
many dumps — and retry once on a 401 before failing with a message naming the mode and what to
check. Conflicting credential flags are refused by name rather than silently resolved. The exact
claim requirements are documented in `docs/authentication.md`, verified against 3.12.4.

**Storage.** One `ObjectStore` abstraction behind five schemes — local filesystem, S3-compatible
(including SeaweedFS), GCS, and Azure — with credentials resolved from each backend's standard
environment variables and never from the command line.

**RDF.** N-Triples, N-Quads, and Turtle bulk-loaded into a property graph under two models
(PGT and RPT), with deterministic hashed keys so re-importing is idempotent.

## What is not built

Ranked by consequence rather than by effort.

1. **Custom analyzers are not dumped.** Views round-trip, but an analyzer a view references
   does not. The view restores without error — ArangoDB accepts the definitions embedded in
   its links — and then fails at query time with `Unable to look up analyzer`. This is now the
   narrowest remaining correctness gap in backup completeness.
2. **No `arangodump`/`arangorestore` interoperability.** Reading official dumps, producing the
   conventional on-disk layout, writing `dump.json`-style metadata, and both compatibility
   test directions are all absent. The PRD scopes this as best-effort, and the README says
   plainly that it does not work — but it is a stated goal with nothing behind it.
3. **No typed library builders.** Import, export, dump, and restore are functions over option
   structs, several carrying `#[allow(clippy::too_many_arguments)]`. The PRD names typed
   builders as a first-alpha acceptance criterion, and the API sketch in §14 does not compile
   against the real crates.
4. **The `ENCRYPTION` marker file is never read.** An encrypted dump is caught only when its
   manifest declares encryption, which an official ArangoDB dump directory does not have.
5. **Restore ordering is incomplete.** `distributeShardsLike` prototypes, `_analyzers` first,
   and `_users` last are all unimplemented. Restoring `_users` mid-run can invalidate the
   credentials the restore is running under.
6. **Resumable dump does not exist.** Import and restore both resume; dump restarts from zero.
   The PRD asks for symmetry here and calls it a first-class capability.

## A recurring defect shape

The audit found the same failure six times, and it is worth naming because it is the pattern
most likely to recur: **a capability is modeled and unit-tested at the type layer, then never
called from any production path.**

`put_if_absent` is implemented on both storage backends and tested, but manifests and
checkpoints are written with unconditional puts — so two concurrent dumps to one prefix
overwrite each other silently. `Inventory.views` and `ArtifactKind::View` are the view gap
above. Three `ErrorContext` builders for object path, byte range, and server response are never
called. The query/bind-variable redaction helper is never called. `ProgressCounters` has no
increment path for `server_errors` and never calls `add_retries`, so both fields are hard
`0` in every progress event emitted.

A seventh instance surfaced later the same day, and it is the most serious of the set because
it was not a silent gap but an outright crash. The global `--output text|json` flag was declared
`global = true` while `dump` and `export` each declared their own `--output` destination path.
Clap resolves arguments by an id derived from the field name, so both resolved to `output` with
different types and **every `arangox dump` and `arangox export` invocation panicked at parse
time** — including the quickstart in the project's own README. It survived because the single
CLI test exercised `import`, which has no `--output`. The global flag is now `--output-format`,
and `tests/cli_contract.rs` pins the distinction.

Every one of these passes its tests. Tests prove the unit works; they do not prove anything
calls it — and in the CLI case, nothing called the broken path at all. The cheap detection
method is to grep for public items with no non-test callers; it located roughly six gaps faster
than reading feature code did. The CLI crash needed a different instrument: actually running
each documented invocation once.

## Verification

Everything in this document was checked rather than recalled:

```
cargo test --workspace      275 passed, 0 failed, 1 ignored
cargo clippy --workspace --all-targets -- -D warnings    clean
cargo fmt --all --check     clean
```

Scale: 9 crates, ~13,500 lines of Rust under `src/`, 13 integration test files. Version
`0.1.0`, unreleased, no tags cut.

Throughput, from `docs/benchmarks.md`: ~270,000 docs/s for `arangox import` against ~382,000
docs/s for `arangoimport` on a 200k-document JSONL fixture — but the two ran in different
environments (`arangox` on the host through a forwarded port and paying process startup;
`arangoimport` in-container), which favors `arangoimport`. The tool's internal timer reported
~457,000 docs/s for the same import. The honest reading is that it is in the same class and
clears the PRD's 50% floor; a clean head-to-head needs both tools co-located and has not been
run.

## What would move the grade

- Dumping and restoring views would take backup completeness from C to A− and is the single
  highest-value change available.
- Either direction of `arangodump` interoperability, with the compatibility fixtures the PRD
  asks for, would move interoperability off D.
- Typed builders would clear the remaining §21 alpha criterion and make library ergonomics a B+.
- Wiring the six built-but-uncalled capabilities — a day's work, mostly — would close a
  disproportionate share of the partial requirements.
