# arangodb-dump

Create resumable, manifest-described ArangoDB database dumps, part of the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

Dumps stream collection data through the replication API to any
`arangodb-storage` backend and record a manifest describing collections,
indexes, parts, and checksums so a dump can be restored faithfully with
[`arangodb-restore`](https://crates.io/crates/arangodb-restore). Features:

- Single-database or all-databases dumps (artifacts namespaced per database).
- Collection include/exclude filters (regex).
- Resumable, checkpointed output with content hashing.

## License

MIT
