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

## License

MIT
