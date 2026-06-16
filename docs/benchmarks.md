# Benchmarks

## Import throughput: `arangox import` vs `arangoimport`

PRD §11.1 requires the import throughput target to be **measured, not assumed**.
The harness lives at
[`crates/arangodb-tools-cli/tests/benchmark.rs`](../crates/arangodb-tools-cli/tests/benchmark.rs)
and runs both tools against the same JSONL fixture and the same server with
out-of-the-box settings, reporting each tool's docs/sec and the ratio.

### Running it

The benchmark is a normal `cargo test` that **no-ops unless both** of these
hold, so it never fails a standard CI run that lacks the client tools:

- `ARANGO_ENDPOINT` is set (and `ARANGO_ROOT_PASSWORD` for auth), and
- `arangoimport` is on `PATH`.

```bash
export ARANGO_ENDPOINT=http://localhost:8529 ARANGO_ROOT_PASSWORD=...
# optional: ARANGO_BENCH_DOCS=1000000
cargo test -p arangodb-tools-cli --test benchmark -- --nocapture
```

A clean comparison requires **both tools on the same host** as each other (the
ratio is only meaningful when client environment and network path match). The
test reports the ratio and whether it meets the PRD §11.1 floor (≥ 0.50×); it
asserts only that both tools imported every document, not the ratio (a hard
throughput gate is too sensitive to shared CI hardware).

### First measurement (2026-06, indicative)

200,000 mixed-type JSONL docs (~12 MB) into ArangoDB 3.12, defaults:

| Tool | Environment | Wall docs/s |
|------|-------------|-------------|
| `arangox import` (release) | macOS host → container (port-forwarded) | ~270,000 |
| `arangoimport` | in-container → in-container server | ~382,000 |

**Caveat — not a clean comparison:** the two tools ran in different
environments (arangox on the macOS host through a forwarded port and paying
process-startup in the wall time; arangoimport in-container with no network
forwarding), which favors arangoimport. `arangox`'s own internal timer reported
~457,000 docs/s for the same import, above arangoimport's figure. Read this as:
arangox is in the same ballpark and comfortably clears the 50% floor; a
rigorous head-to-head awaits an environment with both tools co-located (the
harness itself produces that number when run there).
