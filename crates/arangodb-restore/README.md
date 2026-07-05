# arangodb-restore

Restore ArangoDB databases from dumps produced by
[`arangodb-dump`](https://crates.io/crates/arangodb-dump), part of the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

It reads a dump manifest, recreates collections and indexes, and loads data in
dependency order (document collections before the edges that reference them).
Features:

- Multi-database restore, optionally creating target databases.
- Resumable loading via per-collection, contiguous-prefix checkpoints guarded by
  a manifest fingerprint.
- Streaming reads from any `arangodb-storage` backend.

## License

MIT
