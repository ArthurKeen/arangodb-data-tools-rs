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
- Content-hashed artifacts described by a canonical manifest.

## Install

```bash
cargo add arangodb-dump
```

## Example

```rust,no_run
use arangodb_client::ArangoClient;
use arangodb_dump::{run_dump, DumpOptions};
use arangodb_storage::LocalFileSystem;

# async fn run() -> arangodb_tools_core::Result<()> {
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb")
    .basic_auth("root", "")
    .build()?;

let store = LocalFileSystem::new("./dump-mydb");
let manifest = run_dump(
    &client,
    &store,
    &DumpOptions {
        database: "mydb".to_string(),
        ..DumpOptions::default()
    },
)
.await?;
println!("dumped {} artifacts", manifest.artifacts.len());
# Ok(())
# }
```

## License

MIT
