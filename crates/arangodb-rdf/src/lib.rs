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
//! # Named graphs (N-Quads)
//! The named graph of a quad is mapped per [`NamedGraphMode`]: dropped
//! (default), recorded as a `graph` property on each edge (and folded into the
//! edge key so the same triple in different graphs stays distinct), or
//! additionally routed into a per-graph edge collection `<edge>_<slug>`.
//! Vertices are never routed by graph, so an IRI appearing in several graphs
//! remains a single, shared vertex.
//!
//! # Blank nodes
//! Blank-node labels are only document-scoped in RDF. Set
//! [`RdfOptions::blank_node_scope`] (e.g. to the source path) so identical
//! labels in different sources do not collide; the scope is constant within an
//! import, so repeated references to a label still resolve to one node.
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
    blank_node_key, edge_document, edge_key, graph_slug, literal_key, literal_vertex, resource_key,
    resource_vertex, GraphModel, NamedGraphMode, Placement, RdfLiteralPolicy, RdfOptions,
    RdfResource, RdfTerm, RdfTriple,
};
pub use parser::read_rdf_triples;

/// A document buffer keyed by collection, then by document key (for dedup).
/// Used for vertices and, under [`NamedGraphMode::Collection`], for edges.
type DocStore = BTreeMap<String, BTreeMap<String, Value>>;

/// An edge document together with the collection it should be loaded into.
struct BuiltEdge {
    /// The target edge collection.
    collection: String,
    /// The edge document (its `_key` is deterministic).
    document: Value,
}

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

    // In the common case every edge goes to one collection, so edges are
    // streamed to a concurrent loader as they are parsed (the whole, typically
    // largest, edge set need not be buffered). Under per-graph collection
    // routing the target varies per statement, so edges are instead buffered
    // and loaded per collection after parsing (like vertices). Vertices are
    // always buffered because they are deduplicated and, under
    // `VertexProperty`, accumulate literal properties across triples.
    let route_by_collection = options.named_graph == NamedGraphMode::Collection;
    let (mut edge_tx, edge_task) = if route_by_collection {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel::<Value>(EDGE_CHANNEL_CAP);
        let client = client.clone();
        let collection = options.edge_collection.clone();
        let batch = batch.clone();
        let concurrency = concurrency.clone();
        let task = tokio::spawn(async move {
            let docs = rx.map(Ok::<Value, arangodb_tools_core::Error>);
            import_into_stream(client, &collection, docs, batch, concurrency).await
        });
        (Some(tx), Some(task))
    };

    let mut vertices: DocStore = BTreeMap::new();
    let mut edge_buffers: DocStore = BTreeMap::new();
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

        if let Some(BuiltEdge {
            collection,
            document,
        }) = fold_triple(&triple, options, &mut vertices)
        {
            edges_built += 1;
            match &mut edge_tx {
                Some(tx) => {
                    if tx.send(document).await.is_err() {
                        // The loader stopped early (its error surfaces on join).
                        break;
                    }
                }
                None => buffer_doc(&mut edge_buffers, collection, document),
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

    // If parsing failed, let any edge loader wind down before surfacing it.
    if let Some(err) = parse_error {
        if let Some(task) = edge_task {
            let _ = task.await;
        }
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

    // Collect edge results: either from the streaming loader, or by loading the
    // per-graph edge buffers (ensuring each collection first).
    let mut edge_totals = arangodb_import::ImportSummary::default();
    if let Some(task) = edge_task {
        let summary = task.await.map_err(|err| {
            arangodb_tools_core::Error::config(format!("RDF edge import task failed: {err}"))
        })??;
        accumulate(&mut edge_totals, &summary);
    } else {
        for (collection, docs) in edge_buffers {
            client
                .ensure_collection(&collection, CollectionKind::Edge)
                .await?;
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
            accumulate(&mut edge_totals, &summary);
        }
    }

    emit_progress(
        sink,
        ProgressSnapshot {
            documents: triples_read,
            batches: vertex_totals.batches + edge_totals.batches,
            bytes_written: vertex_totals.bytes_sent + edge_totals.bytes_sent,
            server_errors: vertex_totals.errors + edge_totals.errors,
            elapsed_secs: started.elapsed().as_secs_f64(),
            ..ProgressSnapshot::default()
        },
    );

    Ok(RdfImportSummary {
        triples_read,
        vertices_built,
        edges_built,
        vertices_created: vertex_totals.created,
        edges_created: edge_totals.created,
        vertices_ignored: vertex_totals.ignored,
        edges_ignored: edge_totals.ignored,
    })
}

/// Inserts an edge document into the per-collection buffer, deduplicating by
/// its deterministic `_key`.
fn buffer_doc(store: &mut DocStore, collection: String, document: Value) {
    let key = document
        .get("_key")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    store
        .entry(collection)
        .or_default()
        .entry(key)
        .or_insert(document);
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
    vertices: &mut DocStore,
) -> Option<BuiltEdge> {
    let subject = options.resource_placement(&triple.subject);
    store_vertex(vertices, &subject, || {
        options.resource_vertex(&triple.subject)
    });

    // The named-graph provenance to record on the edge (and route it by).
    let graph = options.edge_graph(triple.graph.as_deref());
    let collection = options.edge_collection_for(triple.graph.as_deref());
    let build_edge = |from: &Placement, to: &Placement, predicate: &str| BuiltEdge {
        collection: collection.clone(),
        document: model::edge_document(from, to, predicate, graph),
    };

    match &triple.object {
        RdfTerm::Iri(_) | RdfTerm::BlankNode(_) => {
            let object = object_as_resource(&triple.object);
            let object_placement = options.resource_placement(&object);
            store_vertex(vertices, &object_placement, || {
                options.resource_vertex(&object)
            });
            Some(build_edge(&subject, &object_placement, &triple.predicate))
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
            Some(build_edge(&subject, &literal, &triple.predicate))
        }
    }
}

/// Inserts a vertex document at its placement if not already present.
fn store_vertex(vertices: &mut DocStore, placement: &Placement, make: impl FnOnce() -> Value) {
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
fn add_property(vertices: &mut DocStore, subject: &Placement, predicate: &str, value: &str) {
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

    async fn build_with(input: &str, options: &RdfOptions) -> (DocStore, Vec<Value>) {
        build_with_format(input, RdfFormat::NTriples, options).await
    }

    async fn build_with_format(
        input: &str,
        format: RdfFormat,
        options: &RdfOptions,
    ) -> (DocStore, Vec<Value>) {
        let reader = std::io::Cursor::new(input.as_bytes().to_vec());
        let triples = read_rdf_triples(reader, format);
        futures::pin_mut!(triples);
        let mut vertices = DocStore::new();
        let mut edges = Vec::new();
        while let Some(triple) = triples.next().await {
            if let Some(edge) = fold_triple(&triple.unwrap(), options, &mut vertices) {
                edges.push(edge.document);
            }
        }
        (vertices, edges)
    }

    /// Collects built edges grouped by their target collection.
    async fn build_edges_by_collection(
        input: &str,
        options: &RdfOptions,
    ) -> BTreeMap<String, Vec<Value>> {
        let reader = std::io::Cursor::new(input.as_bytes().to_vec());
        let triples = read_rdf_triples(reader, RdfFormat::NQuads);
        futures::pin_mut!(triples);
        let mut vertices = DocStore::new();
        let mut by_collection: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        while let Some(triple) = triples.next().await {
            if let Some(edge) = fold_triple(&triple.unwrap(), options, &mut vertices) {
                by_collection
                    .entry(edge.collection)
                    .or_default()
                    .push(edge.document);
            }
        }
        by_collection
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

    #[tokio::test]
    async fn blank_node_scope_changes_keys_and_records_scope() {
        let input = "_:b1 <http://a/p> <http://a/o> .\n";

        let mut unscoped = RdfOptions::new("nodes", "links");
        let (v_unscoped, _) = build_with(input, &unscoped).await;

        unscoped.blank_node_scope = Some("source-a.nq".to_string());
        let (v_scoped, edges_scoped) = build_with(input, &unscoped).await;

        // The blank-node vertex key differs once a scope is applied.
        let bnode_unscoped = v_unscoped
            .values()
            .flat_map(|m| m.values())
            .find(|v| v["blank_node"] == true)
            .unwrap();
        let bnode_scoped = v_scoped
            .values()
            .flat_map(|m| m.values())
            .find(|v| v["blank_node"] == true)
            .unwrap();
        assert_ne!(bnode_unscoped["_key"], bnode_scoped["_key"]);
        assert_eq!(bnode_scoped["scope"], "source-a.nq");
        // The edge endpoint references the scoped key.
        assert!(edges_scoped[0]["_from"]
            .as_str()
            .unwrap()
            .ends_with(bnode_scoped["_key"].as_str().unwrap()));
    }

    #[tokio::test]
    async fn named_graph_ignore_drops_the_graph() {
        let input = "<http://a/s> <http://a/p> <http://a/o> <http://a/g> .\n";
        let options = RdfOptions::new("nodes", "links"); // Ignore by default
        let (_, edges) = build_with_format(input, RdfFormat::NQuads, &options).await;
        assert_eq!(edges.len(), 1);
        assert!(edges[0].get("graph").is_none(), "graph must be dropped");
    }

    #[tokio::test]
    async fn named_graph_property_records_graph_and_disambiguates() {
        // The same triple in two graphs must become two distinct edges, both in
        // the base collection, each carrying its graph.
        let input = concat!(
            "<http://a/s> <http://a/p> <http://a/o> <http://a/g1> .\n",
            "<http://a/s> <http://a/p> <http://a/o> <http://a/g2> .\n",
        );
        let mut options = RdfOptions::new("nodes", "links");
        options.named_graph = NamedGraphMode::Property;
        let by_collection = build_edges_by_collection(input, &options).await;

        assert_eq!(by_collection.len(), 1, "all edges in the base collection");
        let edges = &by_collection["links"];
        assert_eq!(edges.len(), 2);
        assert_ne!(edges[0]["_key"], edges[1]["_key"]);
        assert_eq!(edges[0]["graph"], "http://a/g1");
        assert_eq!(edges[1]["graph"], "http://a/g2");
    }

    #[tokio::test]
    async fn named_graph_collection_routes_per_graph() {
        let input = concat!(
            "<http://a/s> <http://a/p> <http://a/o> <http://a/g1> .\n",
            "<http://a/s> <http://a/p> <http://a/o2> <http://a/g2> .\n",
            "<http://a/s> <http://a/p> <http://a/o3> .\n", // default graph
        );
        let mut options = RdfOptions::new("nodes", "links");
        options.named_graph = NamedGraphMode::Collection;
        let by_collection = build_edges_by_collection(input, &options).await;

        // One collection per named graph, plus the base for the default graph.
        assert_eq!(by_collection.len(), 3);
        assert_eq!(by_collection.get("links").map(Vec::len), Some(1));
        let graph_collections: Vec<&String> = by_collection
            .keys()
            .filter(|k| k.starts_with("links_"))
            .collect();
        assert_eq!(graph_collections.len(), 2);
        for edges in by_collection.values() {
            assert!(edges.iter().all(|e| e.get("_from").is_some()));
        }
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
