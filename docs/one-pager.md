# ArangoDB Data Tools in Rust

**Bulk data for ArangoDB that streams to object storage, resumes where it stopped, and
refuses to guess.**

## What this is

A Rust workspace of nine crates and one CLI — `arangox` — covering the four bulk data
operations an ArangoDB deployment actually runs: **import, export, dump, and restore**, plus
an **RDF bulk loader**. It is a clean-room reimplementation modeled on the behavior of
ArangoDB's own client tools, embedding none of their code.

The official tools are capable, but they assume a local filesystem, use blocking I/O, and
offer limited resumability and observability. This project targets exactly those three gaps.

## What it does differently

**Object storage is a first-class destination.** One `ObjectStore` abstraction sits behind
five schemes — local filesystem, S3-compatible (including MinIO and SeaweedFS), Google Cloud
Storage, and Azure Blob Storage. A dump streams directly to `s3://` with no local staging
step. Credentials come from each backend's standard environment variables and are never
passed on the command line.

**Resume is real, and it works on streams.** Import checkpoints deterministic batch indices
rather than byte offsets. Because identical input and batching configuration produce an
identical batch sequence, a restarted import re-derives the same batches and skips the
committed ones — which means resume works for non-seekable sources such as stdin, not just
seekable files. A contiguous high-water mark ensures concurrent, out-of-order sends can never
advance the checkpoint past a batch the server has not acknowledged. Restore resumes the same
way and refuses a checkpoint belonging to a different dump, matched by manifest fingerprint.

**The manifest is the source of truth.** Every dump and split export writes a canonical
manifest enumerating each artifact with its format, compression, byte size, and checksum. It
is written *last*, so a truncated transfer is detectably incomplete rather than quietly
partial. Restore never guesses a filename.

**Backpressure, not hope.** Every pipeline stage is bounded — bounded channels plus a global
in-flight-byte semaphore — so memory stays flat regardless of input size. The import sender
pool watches for `429`/`503` responses and slow round trips, halves its concurrency when the
server strains, and recovers as pressure eases.

**It fails loudly instead of misbehaving quietly.** `arangox dump` probes the server's
deployment role and refuses a cluster coordinator, DB-Server, or agent *by name* rather than
running a single-server code path that cannot guarantee completeness across shards. Restore
refuses encrypted and VelocyPack dumps with a clear error. Where a diagnostic probe is merely
inconclusive, the tool warns and proceeds rather than blocking legitimate work.

**Built to be driven by other programs.** A global `--output-format json` flag puts a structured
result object on stdout and newline-delimited progress events on stderr, with errors as JSON
and a non-zero exit code — so Python or Go can drive the CLI as a subprocess. Native Python
bindings are sketched via PyO3.

## Where it stands

**Pre-alpha, version `0.1.0`, no release tagged.** Phases 0 through 7 are complete: all four
pipelines, RDF loading, cloud backends, resume, splitting, and filters.

Quality gates on the current commit: **275 tests pass, zero fail**, clippy clean under
`-D warnings`, CI running against a live ArangoDB 3.12 service with a weekly cross-backend
matrix. Roughly 13,500 lines of Rust across nine crates.

The project is audited against its own PRD requirement by requirement: of **130 requirements,
88 are implemented, 24 partial, and 17 missing**, with every open gap tracked individually
rather than rolled into a backlog number.

## What it is not, yet

Three limits are worth stating plainly, because they decide what the tool is safe to use for
today:

- **Custom analyzers are not dumped.** Collections, indexes, data and search views all
  round-trip; an analyzer a view references does not, so such a view restores without error
  and then fails at query time.
- **No interoperability with `arangodump`/`arangorestore` in either direction.** The two
  toolchains are, for now, separate backup systems.
- **Dump is single-server only**, and says so by refusing clusters rather than by producing
  an unverified result.

## Links

Repository: [github.com/arango-solutions/arangodb-data-tools-rs](https://github.com/arango-solutions/arangodb-data-tools-rs)
Graded health and full gap analysis: [`docs/scorecard.md`](scorecard.md)
