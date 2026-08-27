# arangodb-tools-core

Shared foundation types for the [arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit (bulk `import`, `export`, `dump`, `restore`, and `rdf` for ArangoDB).

This crate holds the pieces every tool depends on:

- **Errors** — a structured [`Error`]/[`Result`] type with rich context.
- **Config** — `BatchConfig` and `ConcurrencyConfig` (workers, in-flight byte
  caps, adaptive throttling).
- **Retry** — a configurable exponential-backoff `RetryPolicy` with jitter and a
  generic `retry` combinator.
- **Progress** — `ProgressEvent`/`ProgressSnapshot`/`ProgressSink` for live
  progress reporting.
- **Manifest** — serde models describing dump/restore artifacts.

It has no ArangoDB or network dependencies, so it is cheap to depend on from
library code that only needs the shared vocabulary.

## Install

```bash
cargo add arangodb-tools-core
```

## Example

```rust
use arangodb_tools_core::manifest::{
    Artifact, ArtifactKind, Compression, DataFormat, Manifest,
};

let mut manifest = Manifest::new("mydb", "0.1.0", "2026-01-01T00:00:00Z");
manifest.push(Artifact {
    path: "users/users.data.jsonl".to_string(),
    kind: ArtifactKind::Data,
    format: DataFormat::Jsonl,
    compression: Compression::None,
    byte_size: 1024,
    checksum: None,
    collection: Some("users".to_string()),
    database: None,
    part: Some(0),
});

let json = manifest.to_json().unwrap();
assert!(json.contains("users"));
```

## License

MIT
