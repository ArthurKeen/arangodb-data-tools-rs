# Changelog

This project follows [Semantic Versioning](https://semver.org/). While the
major version is `0`, the **CLI flag surface** additionally carries the
compatibility promise described in the README: within a `0.x` series no flag is
renamed or removed and no flag changes meaning, so generated scripts keep
working across patch upgrades. Library APIs carry no such promise before `1.0`.

## v0.1.0 — 2026-10-19

First tagged release. Alpha: the CLI is usable and its flags are stable within
`0.1.x`; library APIs will still change.

### Added

- `import` — streaming bulk load of CSV, TSV, JSON and JSONL from a file,
  stdin, or object storage, with bounded memory, adaptive concurrency under
  server pushback, and resume from deterministic batch checkpoints that work on
  non-seekable sources such as a pipe.
- CSV/TSV column reshaping during the load: `--datatype`, `--translate`,
  `--merge-attributes`, `--remove-attribute`, named to match `arangoimport` so
  a generated script runs against either tool.
- `export` — collection or AQL output to JSONL, JSON or CSV, with compression
  and size-bounded splitting.
- `dump` / `restore` — manifest-driven, resumable, with search views and a
  conditional destination claim that prevents two dumps interleaving.
- `rdf` — N-Triples, N-Quads and Turtle into a property graph (PGT and RPT).
- `graph create` / `graph drop` — named graphs over existing collections, so a
  load no longer needs `arangosh` for its final step.
- Authentication: basic, user JWT via `/_open/auth`, and superuser JWT minted
  from the server's JWT secret, each refreshed before expiry.
- Storage backends: local filesystem, S3-compatible, GCS and Azure.

### Known limitations

See the README. The notable ones: custom analyzers are not dumped; there is no
interoperability with `arangodump`/`arangorestore` in either direction; dump is
single-server and refuses a cluster by name rather than guessing.

### Deliberate differences from `arangoimport`

- A zero-padded value such as `00789` is kept as text rather than coerced to
  `789`.
- An empty field becomes `null` rather than an absent attribute.
- A row whose edge endpoint is empty is skipped and **counted**, where
  `arangoimport` reports it only as a capped warning.
- A value that cannot be converted to a requested type is an error naming the
  column and the flag, never a discarded row.
