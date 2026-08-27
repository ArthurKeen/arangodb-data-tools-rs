# arangodb-export

Export ArangoDB collections and AQL query results, part of the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

Results are streamed off the cursor API and encoded to JSONL, JSON arrays, or
CSV, with optional gzip/zstd compression and direct writing to any
`arangodb-storage` backend (local files or object storage).

Large exports can be split into size-bounded parts while keeping each part a
standalone, valid document of its format (repeated CSV headers, self-contained
JSON arrays), tracked in a manifest.

## Install

```bash
cargo add arangodb-export
```

## Example

```rust,no_run
use arangodb_client::ArangoClient;
use arangodb_export::{collection_query, run_export, ExportFormat};
use arangodb_storage::{Compression, LocalFileSystem, ObjectPath};

# async fn run() -> arangodb_tools_core::Result<()> {
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb")
    .basic_auth("root", "")
    .build()?;

let store = LocalFileSystem::new(".");
run_export(
    &client,
    collection_query("users", 10_000),
    ExportFormat::JsonLines,
    None, // `fields` (required only for CSV)
    Compression::None,
    &store,
    &ObjectPath::new("users.jsonl"),
)
.await?;
# Ok(())
# }
```

## License

MIT
