# CLI reference (`arangox`)

`arangox` is the command-line front end over the library crates. It has five
subcommands — `import`, `export`, `dump`, `restore`, and `rdf` — plus a global
`--output` mode.

```text
arangox [--output-format text|json] <command> [options]
```

- `--output text` (default): human-readable summaries on stdout.
- `--output-format json`: a machine-readable result object on stdout and
  newline-delimited progress events on stderr (for programmatic callers). See
  [`docs/backends.md`](backends.md) for how storage locations are given, and
  [`docs/resume.md`](resume.md) for checkpointing.

Storage locations (`--input`, `--output`, `--checkpoint`) accept a local path,
`file://`, `s3://`, `seaweed+s3://`, `gs://`, or `az://`. Credentials always
come from the environment, never the command line.

## CSV/TSV column reshaping

A CSV dump rarely has the shape the target graph needs. These four options
reshape columns during the load and are named to match `arangoimport`, so a
generated script can use either tool. They apply to CSV/TSV only; JSON and
JSONL documents already carry their own names and types.

| Option | Form | Effect |
| --- | --- | --- |
| `--datatype` | `COLUMN=string\|number\|boolean` | Force a column's type instead of detecting it |
| `--translate` | `SOURCE=TARGET` | Rename a column |
| `--merge-attributes` | `TARGET=[a]_[b]` | Build an attribute from others; brackets are column refs, the rest is literal |
| `--remove-attribute` | `COLUMN` | Drop a column |

All four are repeatable. Order of operations, matching `arangoimport`: detect
types, then merge, then rename, then remove. Merging before renaming is what
lets a template reference a column that is also being renamed.

```bash
# A PG dump whose primary key must become _key, dropping a payload column
arangox import --collection people --input people.csv --create-collection \
  --translate 'person_id=_key' --datatype 'person_id=string' \
  --remove-attribute internal_notes

# A composite key from two columns
arangox import --collection orders --input orders.csv --create-collection \
  --merge-attributes '_key=[order_id]_[line_no]'

# An edge file projected out of a wide table
arangox import --collection knows --input rel.csv --create-collection --edge \
  --translate 'src=_from' --translate 'dst=_to' \
  --from-collection-prefix people --to-collection-prefix people \
  --remove-attribute payload
```

### Reserved attributes are always strings

`_key`, `_from`, `_to` and `_id` are never type-detected: they take their
column's raw text. ArangoDB rejects a numeric `_key` outright ("illegal
document key", error 1221), and an edge needs `collection/key` strings, so a
CSV with numeric identifiers would otherwise be unloadable. No `--datatype` is
needed for these.

### Zero-padded values: a deliberate difference, and a bug avoided

`arangoimport` coerces a numeric-looking column to a number, which **drops
leading zeros**: `00789` is stored as `789`. This tool keeps `00789` as a
string, because silently renumbering a ZIP code or a zero-padded part number is
data loss. Where byte-for-byte parity matters, `--datatype COLUMN=string` makes
the two agree.

Forcing such a column to a number is worse in `arangoimport`, and the
difference is not cosmetic. Given `--datatype col=number` and a value of
`00042`, it writes `00042` into its JSON request body. That is not a legal JSON
number, so it fails to parse its own payload and **discards the row**:

```
WARNING at position 4: invalid JSON type (expecting object, probably parse error),
offending context: ["1003",00042,5.25,1,"padded customer"]
created: 2          <- three rows were read
```

It then **exits 0**, so a generated script running under `set -euo pipefail`
continues and reports success while a row is missing. Verified against
ArangoDB 3.12.4.

This tool parses the same value to `42` and keeps the row. If you want it kept
as the text `00042` instead, ask for `--datatype col=string`; either way
nothing is silently dropped.

## Connection options (all subcommands)

| Flag | Default | Description |
| --- | --- | --- |
| `--endpoint` | `http://localhost:8529` | ArangoDB endpoint URL. |
| `--database` | `_system` | Target database. |
| `--username` | — | Basic-auth user. |
| `--password-env VAR` | — | Env var holding the password. |
| `--auth-token-env VAR` | — | Env var holding a JWT/bearer token. |
| `--tls-ca FILE` | — | Custom CA bundle (PEM). |
| `--insecure` | off | Disable TLS verification (dev only). |
| `--request-timeout-secs` | `120` | Per-request timeout. |
| `--max-retries` | `5` | Max attempts (incl. the first) per retryable request. |
| `--max-retry-delay-secs` | `30` | Cap on any single backoff interval. |

## `arangox import`

Bulk-import CSV, TSV, JSON, or JSONL into a collection.

| Flag | Default | Description |
| --- | --- | --- |
| `--collection` | required | Target collection. |
| `--input` | required | File path, `-` (stdin), or a storage URI. |
| `--format FORMAT` | inferred | `csv`/`tsv`/`json`/`jsonl`; required for stdin. |
| `--compression` | `auto` | `auto`/`none`/`gzip`/`zstd` (auto detects from extension). |
| `--create-collection` | off | Create the collection if missing. |
| `--edge` | off | Treat as an edge collection. |
| `--on-duplicate` | `error` | `error`/`update`/`replace`/`ignore`. |
| `--overwrite` | off | Truncate before importing (non-atomic). |
| `--from-collection-prefix` / `--to-collection-prefix` | — | Prefix for unqualified `_from`/`_to` (edges). |
| `--batch-size-bytes` / `--max-docs` | tuned | Batch limits. |
| `--threads` | auto | Concurrent sender workers. |
| `--max-in-flight-bytes` | tuned | Global cap on buffered bytes in flight. |
| `--no-adaptive` | off | Disable the rate-limit-aware concurrency governor. |
| `--checkpoint URI` | — | Enable resumable import (see [resume.md](resume.md)). |

## `arangox export`

Export a collection or an AQL query to JSONL, JSON, or CSV.

| Flag | Default | Description |
| --- | --- | --- |
| `--collection` | — | Collection to export (exclusive with `--query`). |
| `--query` | — | AQL query to export (exclusive with `--collection`). |
| `--bind-vars JSON` | — | Bind variables for `--query`. |
| `--output` | required | File path, `file://`, or a storage URI. |
| `--format` | `jsonl` | `jsonl`/`json`/`csv`. |
| `--fields a,b,c` | — | Projected fields (required for CSV). |
| `--compression` | `auto` | `auto`/`none`/`gzip`/`zstd`. |
| `--batch-size` | `10000` | Cursor batch size. |
| `--split-bytes BYTES` | — | Split into standalone parts + a manifest. |

## `arangox dump`

Dump a database (or all databases) to a directory or object-store prefix.

| Flag | Default | Description |
| --- | --- | --- |
| `--output` | required | Dump root: directory, `file://`, or a storage URI. |
| `--compression` | `none` | Data-artifact compression. |
| `--include-system` | off | Include system collections. |
| `--all-databases` | off | Dump every accessible database under `databases/{name}/`. |
| `--include-collections REGEX` | — | Only matching collections. |
| `--exclude-collections REGEX` | — | Skip matching collections (after include). |
| `--batch-ttl-secs` | `600` | Replication-snapshot keep-alive interval. |

## `arangox restore`

Restore a database from a dump.

| Flag | Default | Description |
| --- | --- | --- |
| `--input` | required | Dump root: directory, `file://`, or a storage URI. |
| `--create-database` | off | Create the target DB first (single-DB dumps). |
| `--overwrite` | off | Replace existing collections. |
| `--checkpoint URI` | — | Enable resumable restore (see [resume.md](resume.md)). |

## `arangox rdf import`

Bulk-import RDF (N-Triples/N-Quads/Turtle) into a property graph. See
[`docs/rdf-model.md`](rdf-model.md) for the data model.

| Flag | Default | Description |
| --- | --- | --- |
| `--input` | required | File path, `-` (stdin), or a storage URI. |
| `--format FORMAT` | inferred | `ntriples`/`nt`, `nquads`/`nq`, `turtle`/`ttl`. |
| `--graph-model` | `pgt` | `pgt` (property graph) or `rpt` (topology-preserving). |
| `--vertex-collection` | required | Vertex collection (PGT) / base name (RPT). |
| `--edge-collection` | required | Edge collection for predicate statements. |
| `--literal-policy` | `no-literals` | `no-literals`/`vertex-property`/`materialize` (PGT only). |
| `--named-graph` | `ignore` | `ignore`/`property`/`collection` (N-Quads routing). |
| `--blank-node-scope SCOPE` | input path | Provenance scope for blank-node keys (empty = legacy). |
| `--compression` | `auto` | `auto`/`none`/`gzip`/`zstd`. |
| `--batch-size-bytes` / `--max-docs` / `--threads` / `--max-in-flight-bytes` | tuned | Batching/concurrency, as for `import`. |

## Exit status

`0` on success. On failure the process exits non-zero and prints `error: <msg>`
(text mode) or `{"status":"error","message":"<msg>"}` on stderr (JSON mode).
