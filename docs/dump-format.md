# Dump & export format

`arangox dump` and `arangox export` write a **manifest-driven** layout: a set of
artifact objects plus a single canonical manifest that enumerates them. Restore
and re-import never guess filenames — they read the manifest and load exactly
what it lists. The same format is used whether the destination is a local
directory or an object store (the artifact `path`s are backend-relative keys).

## Layout

A single-database dump root contains:

```text
dump.manifest.json                 # canonical manifest (written last)
<collection>.structure.json        # parameters + index definitions
<collection>.data.jsonl[.gz|.zst]  # replication dump markers (data)
<view>.view.json                   # search view definition
```

The manifest is always written **last**, so its presence signals a complete
dump (object stores have no atomic directory rename). `arangox restore` reads
`dump.manifest.json`, then loads each collection's structure and data.

### Multi-database dumps (`--all-databases`)

With `--all-databases`, every accessible database is dumped under a per-database
prefix and described by one combined manifest at the root:

```text
dump.manifest.json                 # combined; each artifact carries "database"
databases/<db1>/<collection>.structure.json
databases/<db1>/<collection>.data.jsonl
databases/<db1>/<view>.view.json
databases/<db2>/<collection>.structure.json
...
```

Each artifact records its source `database`; restore recreates and targets each
database from the manifest. A single-database dump omits `database` on its
artifacts and restores into the database chosen at restore time.

### Search views

Every ArangoSearch and `search-alias` view in the database is written verbatim
as `<view>.view.json`, exactly as the replication inventory reports it, and
recorded in the manifest with `kind: "view"` and the view's name in `view`.

Restore recreates views in two passes around the data load, because the server
enforces the ordering:

1. **Collections are created** (no data yet).
2. **`arangosearch` views are created.** Their `links` name target collections,
   and a link to a missing collection is refused with error 1203. Creating them
   before the load means documents are indexed as they arrive, rather than
   needing a separate indexing pass — at some cost to load throughput, which is
   the tradeoff PRD §8.5 records.
3. **Data is loaded, then indexes are built.**
4. **`search-alias` views are created.** They name inverted indexes by
   collection and index name, so they cannot exist until step 3 has built those
   indexes; attempting it earlier is refused with a 400.

`id` and `globallyUniqueId` are stripped before a view is created. The server
assigns a fresh `id` regardless, but it *honors* a supplied `globallyUniqueId`,
which would otherwise carry the source server's identifier onto the target.

Two limits apply:

- **Custom analyzers are not dumped.** They live in the `_analyzers` system
  collection. A view that references one restores **without error**, because
  ArangoDB accepts the analyzer definitions embedded in the view's links, but
  querying it then fails with `Unable to look up analyzer '<name>'`. Recreate
  custom analyzers on the target first.
- **Under collection filters, a view whose targets were excluded is skipped**
  and reported as a warning naming the view and the missing collections.
  Dumping it would produce an artifact that cannot be restored.

### Split data artifacts (`export --split-bytes`)

`arangox export --split-bytes N` writes the data as several numbered parts, each
a **standalone valid document** in the chosen format, plus a manifest:

```text
<base>.manifest.json
<base>.part-00000.jsonl
<base>.part-00001.jsonl
...
```

- **JSONL**: parts are cut at line boundaries; each part is valid NDJSON.
- **JSON array**: each part is its own complete `[...]` array; a reader flattens
  arrays across parts.
- **CSV**: every part repeats the header row, so each part parses on its own.

Parts are cut once a part reaches the byte threshold (measured on uncompressed
record bytes). Concatenating the records across parts, in `part` order,
reproduces the export exactly. Each part is a separate manifest artifact with
its `part` index set.

## Manifest schema

`dump.manifest.json` (pretty-printed JSON):

```json
{
  "manifest_version": 1,
  "tool_version": "0.1.0",
  "created_at": "2026-07-06T00:00:00Z",
  "database": "_system",
  "source": null,
  "encryption": { "algorithm": "none" },
  "artifacts": [
    {
      "path": "users.structure.json",
      "kind": "structure",
      "format": "json",
      "compression": "none",
      "byte_size": 812,
      "collection": "users"
    },
    {
      "path": "users.data.jsonl.gz",
      "kind": "data",
      "format": "jsonl",
      "compression": "gzip",
      "byte_size": 194533,
      "checksum": { "algorithm": "sha256", "value": "…" },
      "collection": "users",
      "part": 0
    }
  ]
}
```

### Fields

Top level:

| Field | Meaning |
| --- | --- |
| `manifest_version` | Schema version (currently `1`). |
| `tool_version` | Version of `arangox` that produced the dump. |
| `created_at` | RFC 3339 creation timestamp. |
| `database` | Source database (`"all"` for `--all-databases`). |
| `source` | Optional source description (e.g. an AQL query, for exports). |
| `encryption` | `{ "algorithm": "none" }` — encrypted payloads are detected and refused, not produced. |
| `artifacts` | The list of artifact entries below. |

Each artifact:

| Field | Meaning |
| --- | --- |
| `path` | Object key relative to the dump/export root. |
| `kind` | `meta` / `structure` / `view` / `data`. |
| `format` | `jsonl` / `json` / `csv` / `vpack` / `xgmml` (this project writes `jsonl`/`json`/`csv`). |
| `compression` | `none` / `gzip` / `zstd`. |
| `byte_size` | Size of the stored object in bytes. |
| `checksum` | Optional `{ algorithm, value }` (SHA-256 over the stored bytes); present on data artifacts. |
| `collection` | Owning collection, when applicable. Always absent on `view` artifacts. |
| `view` | Owning view, on `view` artifacts. A view is not a collection, and restore groups collection artifacts by `collection`, so naming a view there would fabricate a collection with no structure. |
| `database` | Owning database (multi-database dumps only). |
| `part` | Part index for split data artifacts. |

## Guarantees & limits

- The manifest is the source of truth; restore reads it rather than scanning.
- Restore is resumable at collection granularity via `--checkpoint`, which
  binds to the manifest's fingerprint (see [`docs/resume.md`](resume.md)).
- Scope is single-server, JSONL data. The parallel `/_api/dump/*` protocol,
  per-shard artifacts, VelocyPack payloads, and Enterprise-encrypted dumps are
  not produced (encrypted dumps are refused with a clear error).
- **Cluster deployments are refused, not attempted.** Before creating a
  replication batch or writing any artifact, `arangox dump` checks
  `/_admin/server/role`. A `COORDINATOR`, `DBSERVER`/`PRIMARY`, or `AGENT`
  response fails the dump with an error naming the detected role, because
  cluster-aware dump is post-MVP and the single-server path cannot guarantee
  completeness across shards. If the role cannot be read (an old server, a
  permissions or transient failure), the dump proceeds and emits a warning —
  an inconclusive probe is reported, never silently treated as a single server.

## Consistency model

**Is my dump consistent while writes continue? Yes, per collection, on a single
server.**

The dump creates a replication batch — a pinned snapshot — *before* reading the
inventory, and passes that batch id to both the inventory call and every
subsequent data read. So the collection list, each collection's structure, and
all of its documents are read from one snapshot taken at batch-creation time.
Writes that land after that point are not included, and never appear partially:
a document is either fully in the dump or absent. The batch's TTL is extended
before each collection so the snapshot survives a long transfer, and it is
released on completion, error, and cancellation alike.

What is guaranteed:

- **Per-collection point-in-time consistency**, at the instant the replication
  batch was created.
- **Cross-collection consistency** for a single-database dump: all collections
  share the one batch, so a dump cannot capture a write to collection B that
  depends on a write to A it missed.
- **Referential integrity between edge and vertex collections**, as a
  consequence of the above.

What is *not* guaranteed:

- **Across databases.** `--all-databases` creates one batch per database, in
  sequence, so each database is internally consistent but different databases
  are snapshotted at different times. Two databases are not mutually
  consistent.
- **Across shards on a cluster.** Not applicable today — cluster dumps are
  refused (above). When cluster support lands, cross-shard guarantees will be
  weaker than the single-server case and will be documented here before the
  code ships.
- **Anything after the snapshot.** A dump is not a continuous backup; it has no
  tail of changes and no point-in-time recovery beyond the snapshot instant.
