# CLI reference (`arangox`)

`arangox` is the command-line front end over the library crates. It has five
subcommands — `import`, `export`, `dump`, `restore`, and `rdf` — plus a global
`--output` mode.

```text
arangox [--output text|json] <command> [options]
```

- `--output text` (default): human-readable summaries on stdout.
- `--output json`: a machine-readable result object on stdout and
  newline-delimited progress events on stderr (for programmatic callers). See
  [`docs/backends.md`](backends.md) for how storage locations are given, and
  [`docs/resume.md`](resume.md) for checkpointing.

Storage locations (`--input`, `--output`, `--checkpoint`) accept a local path,
`file://`, `s3://`, `seaweed+s3://`, `gs://`, or `az://`. Credentials always
come from the environment, never the command line.

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
