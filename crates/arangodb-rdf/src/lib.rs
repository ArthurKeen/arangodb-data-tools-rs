//! RDF bulk import for ArangoDB.
//!
//! Streaming RDF parsers (N-Triples and N-Quads today; Turtle is planned),
//! IRI/blank-node/literal normalization, deterministic key generation, a
//! configurable graph model, and bulk loading through the existing import
//! pipeline. See `IMPLEMENTATION_PLAN_REMAINING.md` §6.
//!
//! # Model
//! Each triple `(s, p, o)` maps into a property graph:
//! - `s` (and `o`, when it is an IRI or blank node) become vertices in
//!   [`RdfOptions::vertex_collection`], keyed deterministically so re-importing
//!   is idempotent.
//! - The triple becomes an edge `s -> o` in [`RdfOptions::edge_collection`],
//!   carrying the predicate IRI.
//! - Literal objects are handled per [`RdfLiteralPolicy`].
//!
//! # Example
//! ```no_run
//! # use arangodb_rdf::{import_rdf, RdfFormat, RdfOptions};
//! # use arangodb_client::ArangoClient;
//! # async fn run(client: &ArangoClient) -> arangodb_tools_core::Result<()> {
//! let reader = std::io::Cursor::new(&b"<http://a/s> <http://a/p> <http://a/o> .\n"[..]);
//! let options = RdfOptions::new("rdf_nodes", "rdf_links");
//! let summary = import_rdf(
//!     client,
//!     reader,
//!     RdfFormat::NTriples,
//!     &options,
//!     Default::default(),
//!     Default::default(),
//! )
//! .await?;
//! println!("{} triples -> {} vertices", summary.triples_read, summary.vertices_created);
//! # Ok(())
//! # }
//! ```

mod format;
mod model;
mod parser;

use std::collections::BTreeMap;
use std::sync::Arc;

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions, OnDuplicate};
use arangodb_import::{run_import, ArangoBatchSender, BatchSender};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::Result;
use futures::{stream, StreamExt};
use serde_json::{json, Value};
use tokio::io::AsyncRead;

pub use format::RdfFormat;
pub use model::{
    edge_document, edge_key, literal_key, literal_vertex, resource_key, resource_vertex,
    RdfLiteralPolicy, RdfOptions, RdfResource, RdfTerm, RdfTriple,
};
pub use parser::read_rdf_triples;

/// Statistics for a completed RDF import.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RdfImportSummary {
    /// Statements parsed from the input.
    pub triples_read: u64,
    /// Distinct vertices produced (subjects, resource objects, and, under
    /// [`RdfLiteralPolicy::Materialize`], literals).
    pub vertices_built: u64,
    /// Edges produced.
    pub edges_built: u64,
    /// Vertices the server reported as newly created.
    pub vertices_created: u64,
    /// Edges the server reported as newly created.
    pub edges_created: u64,
    /// Vertices skipped as duplicates (idempotent re-import).
    pub vertices_ignored: u64,
    /// Edges skipped as duplicates.
    pub edges_ignored: u64,
}

/// Parses RDF from `reader` and bulk-loads it into ArangoDB per `options`.
///
/// The vertex and edge collections are created if missing (as document and
/// edge collections, respectively). Vertices and edges use deterministic keys
/// and are imported with `onDuplicate=ignore`, so importing the same data
/// twice creates nothing new.
///
/// This buffers the derived vertex/edge documents in memory (parsing itself is
/// streaming); it is sized for the datasets RDF bulk-load targets. Fully
/// streaming, two-collection loading is a future optimization.
///
/// # Errors
/// Returns an error if the format is unsupported (e.g. Turtle), the input is
/// malformed (with a line number), a collection cannot be ensured, or a batch
/// send fails.
pub async fn import_rdf<R>(
    client: &ArangoClient,
    reader: R,
    format: RdfFormat,
    options: &RdfOptions,
    batch: BatchConfig,
    concurrency: ConcurrencyConfig,
) -> Result<RdfImportSummary>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    client
        .ensure_collection(&options.vertex_collection, CollectionKind::Document)
        .await?;
    client
        .ensure_collection(&options.edge_collection, CollectionKind::Edge)
        .await?;

    let (vertices, edges, triples_read) = build_documents(reader, format, options).await?;

    let vertices_built = vertices.len() as u64;
    let edges_built = edges.len() as u64;

    let vertex_summary = import_into(
        client.clone(),
        &options.vertex_collection,
        vertices.into_values().collect(),
        batch.clone(),
        concurrency.clone(),
    )
    .await?;
    let edge_summary = import_into(
        client.clone(),
        &options.edge_collection,
        edges,
        batch,
        concurrency,
    )
    .await?;

    Ok(RdfImportSummary {
        triples_read,
        vertices_built,
        edges_built,
        vertices_created: vertex_summary.created,
        edges_created: edge_summary.created,
        vertices_ignored: vertex_summary.ignored,
        edges_ignored: edge_summary.ignored,
    })
}

/// Streams the parser and folds triples into deduplicated vertex documents and
/// edge documents, applying the literal policy.
async fn build_documents<R>(
    reader: R,
    format: RdfFormat,
    options: &RdfOptions,
) -> Result<(BTreeMap<String, Value>, Vec<Value>, u64)>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut vertices: BTreeMap<String, Value> = BTreeMap::new();
    let mut edges: Vec<Value> = Vec::new();
    let mut triples_read: u64 = 0;

    let triples = read_rdf_triples(reader, format);
    futures::pin_mut!(triples);
    while let Some(triple) = triples.next().await {
        let triple = triple?;
        triples_read += 1;

        let subject_key = model::resource_key(&triple.subject);
        vertices
            .entry(subject_key.clone())
            .or_insert_with(|| model::resource_vertex(&triple.subject));

        match &triple.object {
            RdfTerm::Iri(_) | RdfTerm::BlankNode(_) => {
                let object = object_as_resource(&triple.object);
                let object_key = model::resource_key(&object);
                vertices
                    .entry(object_key.clone())
                    .or_insert_with(|| model::resource_vertex(&object));
                edges.push(model::edge_document(
                    &options.vertex_collection,
                    &subject_key,
                    &object_key,
                    &triple.predicate,
                ));
            }
            RdfTerm::Literal {
                value,
                datatype,
                language,
            } => match options.literal_policy {
                RdfLiteralPolicy::NoLiterals => {}
                RdfLiteralPolicy::VertexProperty => {
                    add_property(
                        vertices
                            .get_mut(&subject_key)
                            .expect("subject vertex just inserted"),
                        &triple.predicate,
                        value,
                    );
                }
                RdfLiteralPolicy::Materialize => {
                    let literal_key =
                        model::literal_key(value, datatype.as_deref(), language.as_deref());
                    vertices.entry(literal_key.clone()).or_insert_with(|| {
                        model::literal_vertex(value, datatype.as_deref(), language.as_deref())
                    });
                    edges.push(model::edge_document(
                        &options.vertex_collection,
                        &subject_key,
                        &literal_key,
                        &triple.predicate,
                    ));
                }
            },
        }
    }

    Ok((vertices, edges, triples_read))
}

/// Converts an IRI/blank-node object term into a resource. Panics on a literal,
/// which the caller must not pass.
fn object_as_resource(object: &RdfTerm) -> RdfResource {
    match object {
        RdfTerm::Iri(iri) => RdfResource::Iri(iri.clone()),
        RdfTerm::BlankNode(label) => RdfResource::BlankNode(label.clone()),
        RdfTerm::Literal { .. } => unreachable!("literal objects are handled separately"),
    }
}

/// Adds `value` under `properties[predicate]` on a vertex document.
fn add_property(vertex: &mut Value, predicate: &str, value: &str) {
    let object = vertex
        .as_object_mut()
        .expect("vertex documents are JSON objects");
    let properties = object
        .entry("properties")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .expect("properties is a JSON object");
    properties.insert(predicate.to_string(), json!(value));
}

/// Imports `documents` into `collection` with `onDuplicate=ignore`.
async fn import_into(
    client: ArangoClient,
    collection: &str,
    documents: Vec<Value>,
    batch: BatchConfig,
    concurrency: ConcurrencyConfig,
) -> Result<arangodb_import::ImportSummary> {
    let mut options = ImportOptions::new(collection);
    options.on_duplicate = OnDuplicate::Ignore;
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(client, options));
    let documents = stream::iter(documents.into_iter().map(Ok));
    run_import(documents, batch, concurrency, sender).await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn build(input: &str, policy: RdfLiteralPolicy) -> (BTreeMap<String, Value>, Vec<Value>) {
        let mut options = RdfOptions::new("nodes", "links");
        options.literal_policy = policy;
        let reader = std::io::Cursor::new(input.as_bytes().to_vec());
        let (vertices, edges, _) = build_documents(reader, RdfFormat::NTriples, &options)
            .await
            .unwrap();
        (vertices, edges)
    }

    #[tokio::test]
    async fn iri_triple_builds_two_vertices_and_one_edge() {
        let (vertices, edges) = build(
            "<http://a/s> <http://a/p> <http://a/o> .\n",
            RdfLiteralPolicy::NoLiterals,
        )
        .await;
        assert_eq!(vertices.len(), 2);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0]["predicate"], "http://a/p");
        assert!(edges[0]["_from"].as_str().unwrap().starts_with("nodes/"));
    }

    #[tokio::test]
    async fn repeated_iris_are_deduplicated() {
        let input = concat!(
            "<http://a/s> <http://a/p> <http://a/o> .\n",
            "<http://a/s> <http://a/q> <http://a/o> .\n",
        );
        let (vertices, edges) = build(input, RdfLiteralPolicy::NoLiterals).await;
        assert_eq!(vertices.len(), 2, "s and o are shared");
        assert_eq!(edges.len(), 2);
    }

    #[tokio::test]
    async fn no_literals_policy_drops_literal_objects() {
        let (vertices, edges) = build(
            "<http://a/s> <http://a/name> \"Alice\" .\n",
            RdfLiteralPolicy::NoLiterals,
        )
        .await;
        assert_eq!(vertices.len(), 1, "only the subject");
        assert!(edges.is_empty());
    }

    #[tokio::test]
    async fn vertex_property_policy_attaches_literal() {
        let (vertices, edges) = build(
            "<http://a/s> <http://a/name> \"Alice\" .\n",
            RdfLiteralPolicy::VertexProperty,
        )
        .await;
        assert_eq!(vertices.len(), 1);
        assert!(edges.is_empty());
        let vertex = vertices.values().next().unwrap();
        assert_eq!(vertex["properties"]["http://a/name"], "Alice");
    }

    #[tokio::test]
    async fn materialize_policy_creates_literal_vertex_and_edge() {
        let (vertices, edges) = build(
            "<http://a/s> <http://a/name> \"Alice\" .\n",
            RdfLiteralPolicy::Materialize,
        )
        .await;
        assert_eq!(vertices.len(), 2, "subject + literal vertex");
        assert_eq!(edges.len(), 1);
        assert!(vertices.values().any(|v| v["literal"] == "Alice"));
    }
}
