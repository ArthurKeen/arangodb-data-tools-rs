# arangodb-client

An async ArangoDB HTTP client used by the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

Built on `reqwest` and `tokio`, it provides connection setup, authentication
(basic auth or bearer token), TLS configuration (secure by default), retrying
requests, and typed helpers for the API surface the bulk tools need: the import
API, collection and database management, and the replication/dump endpoints.

## Example

```rust,no_run
use arangodb_client::ArangoClient;

# async fn run() -> arangodb_tools_core::Result<()> {
let client = ArangoClient::builder()
    .endpoint("http://localhost:8529")
    .database("_system")
    .basic_auth("root", "")
    .build()?;

let info = client.version().await?;
println!("connected to ArangoDB {}", info.version);
# Ok(())
# }
```

## License

MIT
