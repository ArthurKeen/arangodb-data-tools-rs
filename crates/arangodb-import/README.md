# arangodb-import

Streaming bulk import of CSV, TSV, JSON, and JSONL data into ArangoDB, part of
the [arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

Documents flow through a bounded, backpressured pipeline into a pool of
concurrent sender workers that call ArangoDB's import API. Highlights:

- Multiple input formats with configurable batching (by document count and by
  bytes).
- Concurrency with a global in-flight byte cap and an optional adaptive,
  rate-limit-aware governor that backs off on `429`/`503` and slow responses.
- Resumable imports via a rolling, contiguous-prefix checkpoint.
- `onDuplicate` handling (error / update / replace / ignore).

## Install

```bash
cargo add arangodb-import
```

## Example

```rust,no_run
use std::sync::Arc;

use arangodb_client::ArangoClient;
use arangodb_import::{
    read_documents, run_import, ArangoBatchSender, BatchSender, ImportFormat, ImportOptions,
};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};

# async fn run() -> arangodb_tools_core::Result<()> {
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("mydb")
    .basic_auth("root", "")
    .build()?;

// Any `AsyncRead` works here (a file, stdin, an object-store stream, …).
let reader = std::io::Cursor::new(br#"{"_key":"a","v":1}"#.to_vec());
let documents = read_documents(ImportFormat::JsonLines, reader);

let sender: Arc<dyn BatchSender> =
    Arc::new(ArangoBatchSender::new(client, ImportOptions::new("people")));
let summary = run_import(
    documents,
    BatchConfig::default(),
    ConcurrencyConfig::default(),
    sender,
)
.await?;
println!("created {} documents", summary.created);
# Ok(())
# }
```

## License

MIT
