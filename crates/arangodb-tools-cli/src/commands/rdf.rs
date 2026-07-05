//! The `arangox rdf` subcommand: bulk-load RDF into a property graph.

use std::path::Path;
use std::time::Instant;

use arangodb_import::decompress;
use arangodb_rdf::{
    import_rdf_with_progress, GraphModel, NamedGraphMode, RdfFormat, RdfLiteralPolicy, RdfOptions,
};
use arangodb_tools_core::config::{default_workers, BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::progress::ProgressSnapshot;
use arangodb_tools_core::{Error, Result};
use clap::{Args, Subcommand, ValueEnum};
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use super::connection::ConnectionArgs;
use super::CompressionArg;
use crate::output::Reporter;

/// Arguments for `arangox rdf`.
#[derive(Debug, Args)]
pub(crate) struct RdfArgs {
    #[command(subcommand)]
    pub command: RdfCommand,
}

/// RDF subcommands.
#[derive(Debug, Subcommand)]
pub(crate) enum RdfCommand {
    /// Bulk-import RDF (N-Triples/N-Quads/Turtle) into a property graph (PGT)
    /// or topology-preserving graph (RPT).
    Import(RdfImportArgs),
}

/// Arguments for `arangox rdf import`.
#[derive(Debug, Args)]
pub(crate) struct RdfImportArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Input: a file path, `-` for standard input, a `file://` URI, or
    /// `s3://bucket/key` (AWS_* env for credentials/region/endpoint).
    #[arg(long)]
    pub input: String,

    /// RDF format: `ntriples` (`nt`), `nquads` (`nq`), or `turtle` (`ttl`).
    /// Inferred from the file extension when omitted.
    #[arg(long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Graph model: `pgt` (property graph, default) or `rpt`
    /// (RDF-topology-preserving). Under RPT, terms are routed into
    /// `<vertex-collection>_URIRef`/`_BNode`/`_Literal` collections and every
    /// statement becomes an edge; the literal policy is ignored.
    #[arg(long, value_enum, default_value_t = GraphModelArg::Pgt)]
    pub graph_model: GraphModelArg,

    /// Vertex collection (PGT), or the base name for the term-typed collections
    /// under RPT. Collections are created as document collections if missing.
    #[arg(long)]
    pub vertex_collection: String,

    /// Edge collection that receives predicate (statement) edges. Created as an
    /// edge collection if it does not exist.
    #[arg(long)]
    pub edge_collection: String,

    /// How to handle triples whose object is a literal (PGT only; RPT always
    /// materializes literals as their own vertices).
    #[arg(long, value_enum, default_value_t = LiteralPolicyArg::NoLiterals)]
    pub literal_policy: LiteralPolicyArg,

    /// How to map the N-Quads named graph: `ignore` (default), `property`
    /// (record the graph on each edge and disambiguate quads across graphs), or
    /// `collection` (also route each graph's edges into a per-graph edge
    /// collection `<edge-collection>_<slug>`).
    #[arg(long, value_enum, default_value_t = NamedGraphArg::Ignore)]
    pub named_graph: NamedGraphArg,

    /// Provenance scope for blank-node keys so identical `_:label`s in different
    /// sources do not collide. Defaults to the input path; pass an empty string
    /// to disable scoping (legacy label-only keys).
    #[arg(long, value_name = "SCOPE")]
    pub blank_node_scope: Option<String>,

    /// Input compression. `auto` detects gzip/zstd from the file extension.
    #[arg(long, value_enum, default_value_t = CompressionArg::Auto)]
    pub compression: CompressionArg,

    /// Maximum batch size in bytes.
    #[arg(long, default_value_t = BatchConfig::default().max_bytes)]
    pub batch_size_bytes: usize,

    /// Maximum documents per batch.
    #[arg(long, default_value_t = BatchConfig::default().max_docs)]
    pub max_docs: usize,

    /// Number of concurrent sender workers.
    #[arg(long)]
    pub threads: Option<usize>,

    /// Global cap on bytes buffered in flight across all workers.
    #[arg(long, default_value_t = ConcurrencyConfig::default().max_in_flight_bytes)]
    pub max_in_flight_bytes: usize,
}

/// Literal-handling policy, mirrored for clap value parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LiteralPolicyArg {
    /// Drop triples whose object is a literal (default).
    NoLiterals,
    /// Attach the literal to the subject vertex as a property.
    VertexProperty,
    /// Create a vertex for the literal and an edge to it.
    Materialize,
}

impl From<LiteralPolicyArg> for RdfLiteralPolicy {
    fn from(policy: LiteralPolicyArg) -> Self {
        match policy {
            LiteralPolicyArg::NoLiterals => RdfLiteralPolicy::NoLiterals,
            LiteralPolicyArg::VertexProperty => RdfLiteralPolicy::VertexProperty,
            LiteralPolicyArg::Materialize => RdfLiteralPolicy::Materialize,
        }
    }
}

/// Graph model, mirrored for clap value parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum GraphModelArg {
    /// Property-graph transformation (idiomatic LPG, default).
    Pgt,
    /// RDF-topology-preserving transformation (faithful term-typed graph).
    Rpt,
}

impl From<GraphModelArg> for GraphModel {
    fn from(model: GraphModelArg) -> Self {
        match model {
            GraphModelArg::Pgt => GraphModel::Pgt,
            GraphModelArg::Rpt => GraphModel::Rpt,
        }
    }
}

impl GraphModelArg {
    /// The canonical short name (used in JSON output).
    fn as_str(self) -> &'static str {
        match self {
            GraphModelArg::Pgt => "pgt",
            GraphModelArg::Rpt => "rpt",
        }
    }
}

/// Named-graph handling, mirrored for clap value parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum NamedGraphArg {
    /// Drop the named graph (default).
    Ignore,
    /// Record the graph on each edge and disambiguate quads across graphs.
    Property,
    /// Route each graph's edges into a per-graph edge collection.
    Collection,
}

impl From<NamedGraphArg> for NamedGraphMode {
    fn from(mode: NamedGraphArg) -> Self {
        match mode {
            NamedGraphArg::Ignore => NamedGraphMode::Ignore,
            NamedGraphArg::Property => NamedGraphMode::Property,
            NamedGraphArg::Collection => NamedGraphMode::Collection,
        }
    }
}

impl NamedGraphArg {
    /// The canonical short name (used in JSON output).
    fn as_str(self) -> &'static str {
        match self {
            NamedGraphArg::Ignore => "ignore",
            NamedGraphArg::Property => "property",
            NamedGraphArg::Collection => "collection",
        }
    }
}

/// Dispatches an `rdf` subcommand.
pub(crate) async fn run(args: RdfArgs, reporter: Reporter) -> Result<()> {
    match args.command {
        RdfCommand::Import(args) => run_import(args, reporter).await,
    }
}

/// Runs an RDF import job.
async fn run_import(args: RdfImportArgs, reporter: Reporter) -> Result<()> {
    let format = resolve_format(args.format.as_deref(), &args.input)?;
    let client = args.connection.build_client()?;

    let blank_node_scope = resolve_blank_node_scope(args.blank_node_scope.as_deref(), &args.input);
    let options = RdfOptions {
        vertex_collection: args.vertex_collection.clone(),
        edge_collection: args.edge_collection.clone(),
        graph_model: args.graph_model.into(),
        literal_policy: args.literal_policy.into(),
        named_graph: args.named_graph.into(),
        blank_node_scope: blank_node_scope.clone(),
    };

    let batch = BatchConfig {
        max_bytes: args.batch_size_bytes,
        max_docs: args.max_docs,
    };
    let concurrency = ConcurrencyConfig {
        workers: args.threads.unwrap_or_else(default_workers),
        max_in_flight_bytes: args.max_in_flight_bytes,
        adaptive: true,
    };

    let compression = args.compression.resolve(&args.input);
    let raw = open_input(&args.input).await?;
    let reader = decompress(compression, raw);

    reporter.started("rdf-import");
    let started = Instant::now();
    let summary = import_rdf_with_progress(
        &client,
        reader,
        format,
        &options,
        batch,
        concurrency,
        reporter.progress_sink(),
    )
    .await?;
    let elapsed_secs = started.elapsed().as_secs_f64();

    reporter.finished(ProgressSnapshot {
        bytes_read: 0,
        bytes_written: 0,
        documents: summary.vertices_created + summary.edges_created,
        batches: 0,
        server_errors: 0,
        retries: 0,
        elapsed_secs,
    });

    let vertex_collection = args.vertex_collection.clone();
    let edge_collection = args.edge_collection.clone();
    reporter.result(
        || {
            format!(
                "read {} triple(s); vertices: {} created, {} existing; edges: {} created, \
                 {} existing\n  vertices -> '{}', edges -> '{}' in {:.2}s",
                summary.triples_read,
                summary.vertices_created,
                summary.vertices_ignored,
                summary.edges_created,
                summary.edges_ignored,
                vertex_collection,
                edge_collection,
                elapsed_secs,
            )
        },
        || {
            serde_json::json!({
                "operation": "rdf-import",
                "status": "ok",
                "format": format_name(format),
                "graph_model": args.graph_model.as_str(),
                "named_graph": args.named_graph.as_str(),
                "blank_node_scope": blank_node_scope,
                "vertex_collection": args.vertex_collection,
                "edge_collection": args.edge_collection,
                "triples_read": summary.triples_read,
                "vertices_built": summary.vertices_built,
                "edges_built": summary.edges_built,
                "vertices_created": summary.vertices_created,
                "edges_created": summary.edges_created,
                "vertices_ignored": summary.vertices_ignored,
                "edges_ignored": summary.edges_ignored,
                "elapsed_secs": elapsed_secs,
            })
        },
    );
    Ok(())
}

/// Resolves the blank-node provenance scope.
///
/// An explicit empty string disables scoping; an explicit non-empty value is
/// used verbatim; otherwise the scope defaults to the input path (so identical
/// blank-node labels across files stay distinct while a re-import of the same
/// file stays idempotent). Stdin has no stable path, so it defaults to unscoped.
fn resolve_blank_node_scope(explicit: Option<&str>, input: &str) -> Option<String> {
    match explicit {
        Some("") => None,
        Some(scope) => Some(scope.to_string()),
        None if input == "-" => None,
        None => Some(input.to_string()),
    }
}

/// Resolves the RDF format from an explicit `--format` or the input path.
fn resolve_format(explicit: Option<&str>, input: &str) -> Result<RdfFormat> {
    if let Some(name) = explicit {
        return RdfFormat::parse(name);
    }
    if input == "-" {
        return Err(Error::config(
            "reading RDF from stdin requires an explicit --format",
        ));
    }
    RdfFormat::infer_from_path(input)
}

/// The canonical short name for a format (used in JSON output).
fn format_name(format: RdfFormat) -> &'static str {
    match format {
        RdfFormat::NTriples => "ntriples",
        RdfFormat::NQuads => "nquads",
        RdfFormat::Turtle => "turtle",
    }
}

/// Opens the RDF input as an async byte stream (file, stdin, `file://`, or an
/// object-storage URI: `s3://`, `gs://`, `az://`, `seaweed+s3://`). Mirrors the
/// `import` subcommand's input handling.
async fn open_input(input: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    if input == "-" {
        return Ok(Box::new(tokio::io::stdin()));
    }

    if let Some((scheme, _)) = input.split_once("://") {
        if scheme == "file" {
            return open_file(Path::new(input.trim_start_matches("file://"))).await;
        }
        return open_object_stream(input).await;
    }
    open_file(Path::new(input)).await
}

/// Opens a local file as a byte stream.
async fn open_file(path: &Path) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|err| Error::config(format!("cannot open input '{}': {err}", path.display())))?;
    Ok(Box::new(file))
}

/// Opens an object-storage URI as a byte stream via the storage backend.
async fn open_object_stream(uri: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let (store, path) = super::open_object(uri)?;
    let stream = store.get_stream(&path, None).await?;
    let reader = StreamReader::new(
        stream.map(|chunk| chunk.map_err(|err| std::io::Error::other(err.to_string()))),
    );
    Ok(Box::new(reader))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_format_overrides_extension() {
        assert_eq!(
            resolve_format(Some("nquads"), "data.nt").unwrap(),
            RdfFormat::NQuads
        );
    }

    #[test]
    fn infers_format_from_path() {
        assert_eq!(
            resolve_format(None, "graph.nt").unwrap(),
            RdfFormat::NTriples
        );
    }

    #[test]
    fn stdin_requires_explicit_format() {
        assert!(resolve_format(None, "-").is_err());
    }

    #[test]
    fn rejects_unknown_format() {
        assert!(resolve_format(Some("rdfxml"), "x").is_err());
    }

    #[test]
    fn blank_node_scope_defaults_to_input_path() {
        assert_eq!(
            resolve_blank_node_scope(None, "data/a.nq"),
            Some("data/a.nq".to_string())
        );
        // Explicit empty string disables scoping; stdin has no stable scope.
        assert_eq!(resolve_blank_node_scope(Some(""), "data/a.nq"), None);
        assert_eq!(resolve_blank_node_scope(None, "-"), None);
        assert_eq!(
            resolve_blank_node_scope(Some("custom"), "data/a.nq"),
            Some("custom".to_string())
        );
    }

    #[test]
    fn named_graph_maps_to_library_enum() {
        assert_eq!(
            NamedGraphMode::from(NamedGraphArg::Collection),
            NamedGraphMode::Collection
        );
    }

    #[test]
    fn literal_policy_maps_to_library_enum() {
        assert_eq!(
            RdfLiteralPolicy::from(LiteralPolicyArg::Materialize),
            RdfLiteralPolicy::Materialize
        );
    }

    #[tokio::test]
    async fn rejects_unknown_object_scheme() {
        // s3/gs/az/seaweed+s3 are supported; an unknown scheme is rejected by
        // URI parsing.
        assert!(matches!(
            open_input("ftp://host/graph.nt").await,
            Err(Error::Config(_))
        ));
    }
}
