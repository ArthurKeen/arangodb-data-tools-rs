# RDF import model

`arangox rdf import` loads RDF (N-Triples, N-Quads, and Turtle) into ArangoDB as
a graph. Two mappings are offered via `--graph-model`: **PGT** (a property
graph, the default) and **RPT** (an RDF-topology-preserving graph). Both produce
deterministic document keys so re-importing the same data is idempotent — no
duplicate vertices or edges.

## Deterministic keys

Every key is the hex SHA-256 of a **domain-separated** tuple, so different kinds
of entity never collide even if their inputs coincide:

| Entity | Key inputs (domain-separated) |
| --- | --- |
| IRI resource | `("rdf:iri", iri)` |
| Blank node | `("rdf:bnode", [scope,] label)` |
| Literal | `("rdf:literal", value, datatype, language)` |
| Predicate edge | `("rdf:edge", from_id, predicate, to_id [, graph])` |

Keys are stable and content-addressed, which is what makes imports resumable and
repeatable.

## PGT — property graph (default)

The idiomatic mapping for graph analytics:

- Each subject/object **resource** becomes a vertex in `--vertex-collection`.
  IRI vertices carry `{ iri }`; blank-node vertices carry
  `{ blank_node: true, label }`.
- Each triple becomes an edge in `--edge-collection` from subject to object,
  carrying the predicate IRI.
- **Literals** are handled per `--literal-policy`:
  - `no-literals` (default): triples whose object is a literal are dropped.
  - `vertex-property`: the literal is attached to the subject vertex as a
    property.
  - `materialize`: the literal becomes its own vertex with an edge to it.

## RPT — topology-preserving graph

A faithful, lossless mapping where **every statement is an edge** and terms are
routed by type into three collections derived from `--vertex-collection`:

- `<base>_URIRef` — IRI terms
- `<base>_BNode` — blank nodes
- `<base>_Literal` — literals (always materialized under RPT; `--literal-policy`
  is ignored)

Use RPT when you need to round-trip or query the RDF structure itself; use PGT
when you want a natural property graph to traverse.

## Blank-node scoping

Blank-node labels (`_:b0`) are only meaningful within a single document, so
identical labels from different sources must not merge. The blank-node key is
salted with a provenance **scope** (`--blank-node-scope`, defaulting to the
input path). Multiple references to the same blank node *within* a source still
resolve to one vertex; the same label in a *different* source gets a distinct
key. Pass `--blank-node-scope ""` to disable scoping (legacy label-only keys).

## Named graphs (N-Quads)

N-Quads carry a fourth "named graph" component. `--named-graph` controls it:

- `ignore` (default): the graph is dropped; quads collapse onto triples.
- `property`: the graph is recorded on each edge and folded into the edge key,
  so the *same* subject–predicate–object across different graphs yields distinct
  edges instead of colliding.
- `collection`: as `property`, and additionally each graph's edges are routed
  into a per-graph edge collection `<edge-collection>_<slug>`, where `<slug>` is
  a sanitized, collision-resistant form of the graph IRI.

## Formats and streaming

N-Triples, N-Quads, and Turtle are parsed by a hand-written streaming parser, so
arbitrarily large inputs load with bounded memory. Vertices are de-duplicated in
memory by key; edges stream to the server as they are produced (except under
`--named-graph collection`, where edges are buffered per target collection so
each collection is created and loaded as a unit). Batching and concurrency reuse
the same knobs as `arangox import` (`--batch-size-bytes`, `--max-docs`,
`--threads`, `--max-in-flight-bytes`).

> Deferred: RDF/XML and TriG parsing are not yet implemented.
