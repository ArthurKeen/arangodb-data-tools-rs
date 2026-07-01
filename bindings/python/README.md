# arangox (Python bindings)

PyO3/maturin bindings for [`arangodb-data-tools-rs`](../../README.md).
**Pre-alpha.** All four tools are bound: `import_file`, `export`, `dump`, and
`restore`.

These bindings call the same async Rust pipelines the `arangox` CLI uses, in
process — no subprocess. Each call builds a Tokio runtime, releases the GIL
during I/O, and returns a plain `dict`. Inputs/outputs accept a local path, a
`file://` URI, or `s3://bucket/key` (`s3://` uses the `AWS_*` environment).

> Not on the default Cargo build: this crate is excluded from the workspace
> (`exclude = ["bindings/python"]`) because it needs a Python interpreter at
> build time. Build it with maturin, as below.

## Build / install (development)

```bash
# from this directory: bindings/python
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release    # builds the extension and installs it into the venv
```

To produce a wheel instead:

```bash
maturin build --release      # wheel lands in target/wheels/
```

## Usage

```python
import arangox

summary = arangox.import_file(
    "users",                       # collection (positional)
    "users.jsonl",                 # input path (positional); format inferred
    endpoint="http://localhost:8529",
    database="mydb",
    username="root",
    password="...",                # or token="<jwt>"
    create_collection=True,
    on_duplicate="update",         # error | update | replace | ignore
)
print(summary)
# {'documents_sent': 1000, 'batches': 10, 'created': 1000, 'errors': 0,
#  'updated': 0, 'ignored': 0, 'empty': 0, 'bytes_sent': 123456}
```

Inputs may be `.jsonl`/`.ndjson`, `.json`, `.csv`, `.tsv` (optionally
`.gz`/`.zst`); the format is inferred from the extension unless you pass
`format=`. Errors surface as Python exceptions.

### export / dump / restore

```python
# Export a collection or AQL query (CSV requires fields=[...])
arangox.export("users.jsonl", collection="users", database="mydb")
arangox.export("active.jsonl", query="FOR u IN users FILTER u.active RETURN u")
# Split a large export into size-bounded JSONL parts + a manifest
arangox.export("events.jsonl", collection="events", split_bytes=134_217_728)

# Dump a database to a directory, then restore it elsewhere
arangox.dump("./dump-mydb", database="mydb")
arangox.restore("./dump-mydb", database="mydb-copy", create_database=True)
```

All functions accept the shared connection kwargs (`endpoint`, `database`,
`username`, `password`, `token`, `insecure`, `request_timeout_secs`). See
[`arangox.pyi`](arangox.pyi) for the full typed signatures.

## Why two integration paths?

For polyglot callers there are two supported patterns:

1. **Subprocess + `--output json`** (works for all four tools today): run the
   `arangox` CLI and parse the JSON result on stdout / NDJSON progress on
   stderr. Language-agnostic.
2. **Native bindings** (this crate): in-process, no serialization round-trip,
   idiomatic `dict` results. All four tools are bound.

## Roadmap

- Optional async API via `pyo3-async-runtimes` so `await arangox.import_file(...)`
  integrates with `asyncio`.
- Streaming progress callbacks: the pipelines already accept a `ProgressSink`;
  surface it to Python as a callback so callers can observe live progress.
