# arangodb-rdf

Bulk-load RDF into ArangoDB, part of the
[arangodb-data-tools-rs](https://github.com/ArthurKeen/arangodb-data-tools-rs)
toolkit.

Streaming parsers for N-Triples, N-Quads, and a practical subset of Turtle feed
a configurable graph model, then load through the `arangodb-import` pipeline with
deterministic (hashed) keys, so re-importing the same data is idempotent.

- **Graph models** (mirroring [ArangoRDF](https://github.com/ArangoDB-Community/ArangoRDF)):
  `PGT` (idiomatic property graph, with dropped / property / materialized literal
  policies) and `RPT` (topology-preserving, term-typed collections).
- **Named graphs (N-Quads):** ignore, record on edges, or route into per-graph
  edge collections.
- **Blank nodes:** optional per-source provenance scoping so identical labels in
  different files stay distinct.

## Example

```rust,no_run
use arangodb_client::ArangoClient;
use arangodb_rdf::{import_rdf, RdfFormat, RdfOptions};

# async fn run(client: &ArangoClient) -> arangodb_tools_core::Result<()> {
let reader = std::io::Cursor::new(&b"<http://a/s> <http://a/p> <http://a/o> .\n"[..]);
let options = RdfOptions::new("rdf_nodes", "rdf_links");
let summary = import_rdf(
    client,
    reader,
    RdfFormat::NTriples,
    &options,
    Default::default(),
    Default::default(),
)
.await?;
println!("{} triples imported", summary.triples_read);
# Ok(())
# }
```

## License

MIT
