# arangodb-data-tools-rs — Slide Source

Source of truth for [`deck.html`](deck.html). One `##` heading per slide.

---

## arangodb-data-tools-rs

**Bulk data for ArangoDB — streaming, resumable, object-store native**

Import · Export · Dump · Restore · RDF

Pre-alpha · v0.1.0 · 2026-10-05

---

## The problem

ArangoDB's official client tools are capable, but they assume:

- **Local filesystem output.** No path to S3, GCS, or Azure without staging.
- **Blocking I/O.** Memory grows with input; no explicit backpressure.
- **Limited resumability.** A failed 10-hour import starts over.
- **Thin observability.** Hard to drive from another program.

A backup that cannot stream to object storage is a backup with a staging problem.

---

## What we built

Nine Rust crates. One CLI: `arangox`.

| | |
|---|---|
| **import** | CSV/TSV/JSON/JSONL, bounded batching, adaptive concurrency |
| **export** | AQL cursors → JSONL/JSON/CSV, compression, size-split |
| **dump** | Replication-snapshot, manifest-driven, single-server |
| **restore** | Manifest-driven, resumable, fingerprint-bound |
| **rdf** | N-Triples/N-Quads/Turtle → property graph |

Five storage schemes behind one abstraction: local, S3, SeaweedFS, GCS, Azure.

---

## Design bet 1 — the manifest is canonical

Every dump and split export writes a manifest enumerating each artifact with its
format, compression, byte size, and checksum.

It is written **last**.

- Restore never guesses a filename.
- A truncated transfer is *detectably* incomplete, not quietly partial.
- The format carries an explicit version from day one.

---

## Design bet 2 — resume that works on streams

Import checkpoints **deterministic batch indices**, not byte offsets.

Identical input + identical batching config → identical batch sequence.
So a restart re-derives the same batches and skips committed ones.

- Works for **non-seekable** sources — stdin, pipes, streams.
- A contiguous high-water mark means concurrent out-of-order sends can never
  advance the checkpoint past an unacknowledged batch.
- Restore resumes the same way, and refuses a checkpoint from a different dump
  by manifest fingerprint.

The spec asked for byte offsets on seekable sources only. The implementation is
strictly more general — so we patched the spec.

---

## Design bet 3 — fail loudly, never misbehave quietly

`arangox dump` probes the server's deployment role **before** writing anything.

Coordinator, DB-Server, or agent → refuse, naming the role.

> Cluster-aware dump is post-MVP. The single-server path cannot guarantee
> completeness across shards — so it does not run and pretend.

Restore refuses encrypted and VelocyPack dumps. An *inconclusive* probe warns and
proceeds, rather than blocking single-server users over a diagnostic call.

---

## Where it stands

**275 tests pass · 0 fail · clippy clean at `-D warnings`**

CI against a live ArangoDB 3.12, weekly cross-backend matrix.
~13,500 lines of Rust across 9 crates.

Audited against its own PRD, requirement by requirement:

| Implemented | Partial | Missing |
|---:|---:|---:|
| **88** | **24** | **17** |

of 130 requirements. Every open gap tracked individually.

---

## What the audit found

The same defect, six times:

> A capability is implemented and unit-tested at the type layer —
> then **never called** from any production path.

- `put_if_absent` — tested, uncalled → concurrent dumps silently overwrite
- View definitions — parsed, modeled, never written *(now fixed)*
- Three error-context builders — uncalled
- The redaction helper — uncalled
- Two progress counters — always report `0`

Every one passes its tests. **A unit test proves the unit works. It proves nothing
about whether anything calls it.**

---

## The honest limitations

Ranked by consequence, not effort:

1. **Custom analyzers are not dumped.** A view using one restores *without error*,
   then fails at query time. The narrowest remaining correctness gap.
2. **No `arangodump`/`arangorestore` interop**, either direction.
3. **No typed library builders** — a stated alpha criterion.
4. **Dump is single-server only** — and refuses clusters rather than guessing.

"Pre-alpha" is not a disclosure. A tracking ID and a named failure mode is.

---

## What's next

| Priority | Work |
|---|---|
| **P0** | ~~Dump and restore views~~ **done**; custom analyzers; wire the uncalled capabilities |
| **P1** | Typed library builders; `arangodump` interop (read direction first) |
| **P2** | Resumable dump; parallel dump protocol; text-mode progress |

P0 and P1 are what stand between this and a defensible `v0.1.0` alpha tag.

---

## Links

**Repository**
github.com/arango-solutions/arangodb-data-tools-rs

**Graded audit** — `docs/scorecard.md`
**Status and sequencing** — `PROJECT_STATUS_SUMMARY.md`
**Requirements** — `RUST_ARANGODB_TOOLS_PRD.md`
