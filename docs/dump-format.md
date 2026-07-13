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
databases/<db2>/<collection>.structure.json
...
```

Each artifact records its source `database`; restore recreates and targets each
database from the manifest. A single-database dump omits `database` on its
artifacts and restores into the database chosen at restore time.

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
| `collection` | Owning collection, when applicable. |
| `database` | Owning database (multi-database dumps only). |
| `part` | Part index for split data artifacts. |

## Guarantees & limits

- The manifest is the source of truth; restore reads it rather than scanning.
- Restore is resumable at collection granularity via `--checkpoint`, which
  binds to the manifest's fingerprint (see [`docs/resume.md`](resume.md)).
- Scope is single-server, JSONL data. The parallel `/_api/dump/*` protocol,
  per-shard artifacts, VelocyPack payloads, and Enterprise-encrypted dumps are
  not produced (encrypted dumps are refused with a clear error).
