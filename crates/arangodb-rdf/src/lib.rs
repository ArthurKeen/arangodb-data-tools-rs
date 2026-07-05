//! RDF bulk import for ArangoDB.
//!
//! Streaming RDF parsers (N-Triples, N-Quads, and a practical subset of
//! Turtle), IRI/blank-node/literal normalization, deterministic key
//! generation, a configurable graph model, and bulk loading through the
//! existing import pipeline. See `IMPLEMENTATION_PLAN_REMAINING.md` §6.
//!
//! # Models
//! Two [`GraphModel`]s are supported, mirroring the ArangoRDF library:
//! - **PGT** (property graph, default): resources `s`/`o` share
//!   [`RdfOptions::vertex_collection`], the triple becomes an edge `s -> o` in
//!   [`RdfOptions::edge_collection`], and literal objects are handled per
//!   [`RdfLiteralPolicy`] (dropped, attached as vertex properties, or
//!   materialized).
//! - **RPT** (RDF-topology-preserving): every term becomes a vertex, routed by
//!   type into `<base>_URIRef`/`_BNode`/`_Literal`, and every statement becomes
//!   an edge — a faithful, near-lossless mapping.
//!
//! Keys are derived deterministically, so re-importing is idempotent.
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
mod turtle;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arangodb_client::{ArangoClient, CollectionKind, ImportOptions, OnDuplicate};
use arangodb_import::{run_import, ArangoBatchSender, BatchSender};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::progress::{ProgressEvent, ProgressSink, ProgressSnapshot};
use arangodb_tools_core::Result;
use futures::channel::mpsc;
use futures::{stream, SinkExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio::io::AsyncRead;

/// How often the parse phase emits a periodic progress snapshot.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Bound on edge documents buffered between the parser and the edge loader.
/// Caps parse-side memory while keeping the loader fed.
const EDGE_CHANNEL_CAP: usize = 16_384;

pub use format::RdfFormat;
pub use model::{
    edge_document, edge_key, literal_key, literal_vertex, resource_key, resource_vertex,
    GraphModel, Placement, RdfLiteralPolicy, RdfOptions, RdfResource, RdfTerm, RdfTriple,
};
pub use parser::read_rdf_triples;

/// A vertex buffer keyed by collection, then by document key (for dedup).
type VertexStore = BTreeMap<String, BTreeMap<String, Value>>;

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
/// Edges are streamed to a concurrent loader as they are parsed; only the
/// deduplicated vertices are buffered (they also accumulate literal properties
/// under `VertexProperty`). Note that Turtle input is itself read whole before
/// parsing, since its grammar is not line-oriented.
///
/// # Errors
/// Returns an error if the format is unsupported, the input is malformed (with
/// a position), a collection cannot be ensured, or a batch send fails.
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
    import_rdf_with_progress(client, reader, format, options, batch, concurrency, None).await
}

/// Like [`import_rdf`], but emits progress through `progress` when provided.
///
/// A periodic [`ProgressEvent::Progress`] snapshot (documents = triples read)
/// is emitted about once per second while parsing, and a snapshot is emitted
/// after each of the vertex and edge load phases (carrying cumulative batches
/// and bytes sent). Lifecycle (`started`/`finished`) events are the caller's
/// responsibility.
///
/// # Errors
/// See [`import_rdf`].
pub async fn import_rdf_with_progress<R>(
    client: &ArangoClient,
    reader: R,
    format: RdfFormat,
    options: &RdfOptions,
    batch: BatchConfig,
    concurrency: ConcurrencyConfig,
    progress: Option<Arc<dyn ProgressSink>>,
) -> Result<RdfImportSummary>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let started = Instant::now();
    for collection in options.vertex_collections() {
        client
            .ensure_collection(&collection, CollectionKind::Document)
            .await?;
    }
    client
        .ensure_collection(&options.edge_collection, CollectionKind::Edge)
        .await?;

    let sink = progress.as_deref();

    // Edges are streamed to a concurrent loader as they are parsed, so the
    // whole (typically largest) edge set need not be buffered. Vertices are
    // still buffered because they are deduplicated and, under
    // `VertexProperty`, accumulate literal properties across triples.
    let (mut edge_tx, edge_rx) = mpsc::channel::<Value>(EDGE_CHANNEL_CAP);
    let edge_task = {
        let client = client.clone();
        let collection = options.edge_collection.clone();
        let batch = batch.clone();
        let concurrency = concurrency.clone();
        tokio::spawn(async move {
            let docs = edge_rx.map(Ok::<Value, arangodb_tools_core::Error>);
            import_into_stream(client, &collection, docs, batch, concurrency).await
        })
    };

    let mut vertices: VertexStore = BTreeMap::new();
    let mut triples_read: u64 = 0;
    let mut edges_built: u64 = 0;
    let mut last_emit = Instant::now();
    let mut parse_error = None;

    let triples = read_rdf_triples(reader, format);
    futures::pin_mut!(triples);
    while let Some(triple) = triples.next().await {
        let triple = match triple {
            Ok(triple) => triple,
            Err(err) => {
                parse_error = Some(err);
                break;
            }
        };
        triples_read += 1;

        if let Some(edge) = fold_triple(&triple, options, &mut vertices) {
            edges_built += 1;
            if edge_tx.send(edge).await.is_err() {
                // The edge loader stopped early (its error surfaces on join).
                break;
            }
        }

        if sink.is_some() && last_emit.elapsed() >= PROGRESS_INTERVAL {
            emit_progress(
                sink,
                ProgressSnapshot {
                    documents: triples_read,
                    elapsed_secs: started.elapsed().as_secs_f64(),
                    ..ProgressSnapshot::default()
                },
            );
            last_emit = Instant::now();
        }
    }
    drop(edge_tx);

    // If parsing failed, let the edge loader wind down before surfacing it.
    if let Some(err) = parse_error {
        let _ = edge_task.await;
        return Err(err);
    }

    // Load each vertex collection (the edge loader keeps draining meanwhile).
    let vertices_built: u64 = vertices.values().map(|m| m.len() as u64).sum();
    let mut vertex_totals = arangodb_import::ImportSummary::default();
    for (collection, docs) in vertices {
        let docs = stream::iter(
            docs.into_values()
                .map(Ok::<Value, arangodb_tools_core::Error>),
        );
        let summary = import_into_stream(
            client.clone(),
            &collection,
            docs,
            batch.clone(),
            concurrency.clone(),
        )
        .await?;
        accumulate(&mut vertex_totals, &summary);
    }

    let edge_summary = edge_task.await.map_err(|err| {
        arangodb_tools_core::Error::config(format!("RDF edge import task failed: {err}"))
    })??;

    emit_progress(
        sink,
        ProgressSnapshot {
            documents: triples_read,
            batches: vertex_totals.batches + edge_summary.batches,
            bytes_written: vertex_totals.bytes_sent + edge_summary.bytes_sent,
            server_errors: vertex_totals.errors + edge_summary.errors,
            elapsed_secs: started.elapsed().as_secs_f64(),
            ..ProgressSnapshot::default()
        },
    );

    Ok(RdfImportSummary {
        triples_read,
        vertices_built,
        edges_built,
        vertices_created: vertex_totals.created,
        edges_created: edge_summary.created,
        vertices_ignored: vertex_totals.ignored,
        edges_ignored: edge_summary.ignored,
    })
}

/// Adds the created/ignored/batches/bytes/errors of `summary` into `totals`.
fn accumulate(
    totals: &mut arangodb_import::ImportSummary,
    summary: &arangodb_import::ImportSummary,
) {
    totals.created += summary.created;
    totals.ignored += summary.ignored;
    totals.errors += summary.errors;
    totals.batches += summary.batches;
    totals.bytes_sent += summary.bytes_sent;
}

/// Emits a periodic progress snapshot through the sink, if present.
fn emit_progress(sink: Option<&dyn ProgressSink>, snapshot: ProgressSnapshot) {
    if let Some(sink) = sink {
        sink.emit(&ProgressEvent::Progress(snapshot));
    }
}

/// Folds one triple into the graph model: places the subject (and IRI/blank
/// object) vertices into `vertices`, applies the model/literal policy, and
/// returns the edge document to emit, if any.
fn fold_triple(
    triple: &RdfTriple,
    options: &RdfOptions,
    vertices: &mut VertexStore,
) -> Option<Value> {
    let subject = options.resource_placement(&triple.subject);
    store_vertex(vertices, &subject, || {
        model::resource_vertex(&triple.subject)
    });

    match &triple.object {
        RdfTerm::Iri(_) | RdfTerm::BlankNode(_) => {
            let object = object_as_resource(&triple.object);
            let object_placement = options.resource_placement(&object);
            store_vertex(vertices, &object_placement, || {
                model::resource_vertex(&object)
            });
            Some(model::edge_document(
                &subject,
                &object_placement,
                &triple.predicate,
            ))
        }
        RdfTerm::Literal {
            value,
            datatype,
            language,
        } => {
            // RPT always materializes literals as their own typed vertices;
            // otherwise the PGT literal policy applies.
            let materialize = options.graph_model == GraphModel::Rpt
                || options.literal_policy == RdfLiteralPolicy::Materialize;
            if options.graph_model == GraphModel::Pgt
                && options.literal_policy == RdfLiteralPolicy::NoLiterals
            {
                return None;
            }
            if options.graph_model == GraphModel::Pgt
                && options.literal_policy == RdfLiteralPolicy::VertexProperty
            {
                add_property(vertices, &subject, &triple.predicate, value);
                return None;
            }
            debug_assert!(materialize);
            let literal =
                options.literal_placement(value, datatype.as_deref(), language.as_deref());
            store_vertex(vertices, &literal, || {
                model::literal_vertex(value, datatype.as_deref(), language.as_deref())
            });
            Some(model::edge_document(&subject, &literal, &triple.predicate))
        }
    }
}

/// Inserts a vertex document at its placement if not already present.
fn store_vertex(vertices: &mut VertexStore, placement: &Placement, make: impl FnOnce() -> Value) {
    vertices
        .entry(placement.collection.clone())
        .or_default()
        .entry(placement.key.clone())
        .or_insert_with(make);
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

/// Adds `value` under `properties[predicate]` on the subject vertex document.
fn add_property(vertices: &mut VertexStore, subject: &Placement, predicate: &str, value: &str) {
    let vertex = vertices
        .get_mut(&subject.collection)
        .and_then(|m| m.get_mut(&subject.key))
        .expect("subject vertex just inserted");
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

/// Imports a stream of `documents` into `collection` with `onDuplicate=ignore`.
async fn import_into_stream<S>(
    client: ArangoClient,
    collection: &str,
    documents: S,
    batch: BatchConfig,
    concurrency: ConcurrencyConfig,
) -> Result<arangodb_import::ImportSummary>
where
    S: Stream<Item = Result<Value>> + Send + 'static,
{
    let mut options = ImportOptions::new(collection);
    options.on_duplicate = OnDuplicate::Ignore;
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(client, options));
    run_import(documents, batch, concurrency, sender).await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn build_with(input: &str, options: &RdfOptions) -> (VertexStore, Vec<Value>) {
        let reader = std::io::Cursor::new(input.as_bytes().to_vec());
        let triples = read_rdf_triples(reader, RdfFormat::NTriples);
        futures::pin_mut!(triples);
        let mut vertices = VertexStore::new();
        let mut edges = Vec::new();
        while let Some(triple) = triples.next().await {
            if let Some(edge) = fold_triple(&triple.unwrap(), options, &mut vertices) {
                edges.push(edge);
            }
        }
        (vertices, edges)
    }

    /// PGT build that flattens the (single) vertex collection for assertions.
    async fn build(input: &str, policy: RdfLiteralPolicy) -> (BTreeMap<String, Value>, Vec<Value>) {
        let mut options = RdfOptions::new("nodes", "links");
        options.literal_policy = policy;
        let (store, edges) = build_with(input, &options).await;
        let flat = store.into_values().flatten().collect();
        (flat, edges)
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

    #[tokio::test]
    async fn rpt_routes_terms_into_typed_collections() {
        let mut options = RdfOptions::new("g", "g_stmt");
        options.graph_model = GraphModel::Rpt;
        // IRI subject, blank object, and a literal object => URIRef + BNode +
        // Literal vertices, and two statement edges.
        let input = concat!(
            "<http://a/s> <http://a/p> _:b1 .\n",
            "<http://a/s> <http://a/name> \"Alice\" .\n",
        );
        let (store, edges) = build_with(input, &options).await;

        assert_eq!(store.get("g_URIRef").map(|m| m.len()), Some(1), "one IRI");
        assert_eq!(store.get("g_BNode").map(|m| m.len()), Some(1), "one blank");
        assert_eq!(
            store.get("g_Literal").map(|m| m.len()),
            Some(1),
            "literal is materialized under RPT"
        );
        assert_eq!(edges.len(), 2, "both statements become edges");
        // Endpoints reference their term-typed collections.
        assert!(edges
            .iter()
            .any(|e| e["_to"].as_str().unwrap().starts_with("g_BNode/")));
        assert!(edges
            .iter()
            .any(|e| e["_to"].as_str().unwrap().starts_with("g_Literal/")));
        assert!(edges
            .iter()
            .all(|e| e["_from"].as_str().unwrap().starts_with("g_URIRef/")));
    }

    #[tokio::test]
    async fn rpt_ignores_literal_policy() {
        // Even with NoLiterals, RPT materializes the literal.
        let mut options = RdfOptions::new("g", "g_stmt");
        options.graph_model = GraphModel::Rpt;
        options.literal_policy = RdfLiteralPolicy::NoLiterals;
        let (store, edges) =
            build_with("<http://a/s> <http://a/name> \"Alice\" .\n", &options).await;
        assert_eq!(store.get("g_Literal").map(|m| m.len()), Some(1));
        assert_eq!(edges.len(), 1);
    }

    #[test]
    fn vertex_collections_depend_on_model() {
        let mut options = RdfOptions::new("g", "g_stmt");
        assert_eq!(options.vertex_collections(), vec!["g".to_string()]);
        options.graph_model = GraphModel::Rpt;
        assert_eq!(
            options.vertex_collections(),
            vec![
                "g_URIRef".to_string(),
                "g_BNode".to_string(),
                "g_Literal".to_string()
            ]
        );
    }
}
