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

## Install

```bash
cargo add arangodb-restore
```

## Example

```rust,no_run
use arangodb_client::ArangoClient;
use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_storage::LocalFileSystem;

# async fn run() -> arangodb_tools_core::Result<()> {
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb-copy")
    .basic_auth("root", "")
    .build()?;

let store = LocalFileSystem::new("./dump-mydb");
let summary = run_restore(
    &client,
    &store,
    &RestoreOptions {
        overwrite: true,
        ..RestoreOptions::default()
    },
)
.await?;
println!("restored {} collections", summary.collections);
# Ok(())
# }
```

## License

MIT
