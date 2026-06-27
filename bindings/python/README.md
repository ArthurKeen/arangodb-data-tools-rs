# arangox (Python bindings)

PyO3/maturin bindings for [`arangodb-data-tools-rs`](../../README.md). This is a
**sketch / pre-alpha**: today it binds the bulk **import** pipeline; `export`,
`dump`, and `restore` are present as discoverable stubs that raise
`NotImplementedError`.

These bindings call the same async Rust pipeline the `arangox` CLI uses, in
process — no subprocess. Each call builds a Tokio runtime, releases the GIL
during I/O, and returns a plain `dict`.

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

### Keyword arguments

`endpoint`, `database`, `username`, `password`, `token`, `insecure`,
`request_timeout_secs`, `create_collection`, `edge`, `on_duplicate`,
`overwrite`, `from_collection_prefix`, `to_collection_prefix`, `format`,
`batch_size_bytes`, `max_docs`, `threads`, `max_in_flight_bytes`.

See [`arangox.pyi`](arangox.pyi) for the typed signature.

## Why two integration paths?

For polyglot callers there are two supported patterns:

1. **Subprocess + `--output json`** (works for all four tools today): run the
   `arangox` CLI and parse the JSON result on stdout / NDJSON progress on
   stderr. Language-agnostic.
2. **Native bindings** (this crate, import only so far): in-process, no
   serialization round-trip, idiomatic `dict` results.

## Roadmap

- Bind `export`, `dump`, `restore`.
- `s3://` inputs/outputs (the CLI already supports them).
- Optional async API via `pyo3-async-runtimes` so `await arangox.import_file(...)`
  integrates with `asyncio`.
- Streaming progress callbacks (once the pipelines accept a progress sink).
