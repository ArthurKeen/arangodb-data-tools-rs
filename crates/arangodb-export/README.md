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

## License

MIT
