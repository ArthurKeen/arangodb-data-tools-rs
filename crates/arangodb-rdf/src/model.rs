//! The RDF-to-property-graph model: term types, deterministic key generation,
//! and vertex/edge document construction.
//!
//! Default model (see `IMPLEMENTATION_PLAN_REMAINING.md` §6.2):
//! - Each IRI or blank node becomes a **vertex** in the vertex collection,
//!   keyed by a deterministic hash so re-importing the same data creates no
//!   duplicates.
//! - Each triple whose object is an IRI or blank node becomes an **edge**
//!   `subject -> object` carrying the predicate IRI, keyed deterministically by
//!   `(from, predicate, to)`.
//! - Literal-valued objects are handled per [`RdfLiteralPolicy`].

use arangodb_tools_core::{Error, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// A subject or edge endpoint: an IRI or a blank node (never a literal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdfResource {
    /// An IRI reference.
    Iri(String),
    /// A blank node, identified by its label within the document.
    BlankNode(String),
}

/// An RDF object term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RdfTerm {
    /// An IRI reference.
    Iri(String),
    /// A blank node.
    BlankNode(String),
    /// A literal with an optional datatype IRI and/or language tag.
    Literal {
        /// The lexical value (with escapes already decoded).
        value: String,
        /// The datatype IRI, if any (mutually exclusive with `language`).
        datatype: Option<String>,
        /// The BCP-47 language tag, if any.
        language: Option<String>,
    },
}

/// A parsed RDF statement (triple, or quad when a graph is present).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdfTriple {
    /// The subject resource.
    pub subject: RdfResource,
    /// The predicate IRI.
    pub predicate: String,
    /// The object term.
    pub object: RdfTerm,
    /// The named graph IRI (N-Quads), if any.
    pub graph: Option<String>,
}

/// How triples whose object is a literal are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RdfLiteralPolicy {
    /// Drop literal-object triples entirely (default).
    #[default]
    NoLiterals,
    /// Attach the literal to the subject vertex as a property under
    /// `properties[predicate]` (no edge is created).
    VertexProperty,
    /// Create a vertex for the literal and an edge from the subject to it.
    Materialize,
}

impl RdfLiteralPolicy {
    /// Parses a policy name (case-insensitive): `no-literals`,
    /// `vertex-property`, `materialize`.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for an unrecognized name.
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "no-literals" | "none" | "drop" => Ok(Self::NoLiterals),
            "vertex-property" | "property" => Ok(Self::VertexProperty),
            "materialize" | "vertex" => Ok(Self::Materialize),
            other => Err(Error::config(format!(
                "unknown literal policy '{other}'; expected no-literals, vertex-property, \
                 or materialize"
            ))),
        }
    }
}

/// Which graph model the RDF data is mapped into.
///
/// This mirrors the two transformation families in the ArangoRDF Python
/// library: an idiomatic labeled property graph, or a topology-preserving
/// mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GraphModel {
    /// **Property-graph transformation** (default): resources share a single
    /// vertex collection and literals are handled by [`RdfLiteralPolicy`]
    /// (dropped, attached as vertex properties, or materialized). Idiomatic to
    /// query, but not lossless.
    #[default]
    Pgt,
    /// **RDF-topology-preserving transformation**: every term becomes a vertex,
    /// routed by type into `<base>_URIRef`, `<base>_BNode`, and `<base>_Literal`
    /// collections, and every statement becomes an edge in the edge (statement)
    /// collection. Literals are always materialized (the literal policy is
    /// ignored). Faithful to the RDF graph.
    Rpt,
}

impl GraphModel {
    /// Parses a model name (case-insensitive): `pgt`/`property-graph` or
    /// `rpt`/`topology`.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for an unrecognized name.
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "pgt" | "property-graph" | "lpg" => Ok(Self::Pgt),
            "rpt" | "topology" | "rdf" => Ok(Self::Rpt),
            other => Err(Error::config(format!(
                "unknown graph model '{other}'; expected pgt or rpt"
            ))),
        }
    }
}

/// How the named graph of an N-Quads statement is mapped into ArangoDB.
///
/// Vertices are never routed by graph (an IRI can belong to many graphs and
/// must remain a single vertex so edges from any graph connect to it); graph
/// membership is a property of the *statement* (edge).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NamedGraphMode {
    /// Ignore the graph label entirely (default; N-Triples-like behavior).
    #[default]
    Ignore,
    /// Record the graph IRI as a `graph` property on each edge and fold it into
    /// the edge key, so the same triple asserted in different graphs becomes
    /// distinct edges in the one edge collection.
    Property,
    /// Like [`NamedGraphMode::Property`], but additionally route each edge into
    /// a per-graph edge collection `<edge_collection>_<slug>` (the default
    /// graph stays in the base edge collection).
    Collection,
}

impl NamedGraphMode {
    /// Parses a mode name (case-insensitive): `ignore`/`none`, `property`, or
    /// `collection`.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for an unrecognized name.
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "ignore" | "none" | "drop" => Ok(Self::Ignore),
            "property" | "edge-property" => Ok(Self::Property),
            "collection" | "per-graph" => Ok(Self::Collection),
            other => Err(Error::config(format!(
                "unknown named-graph mode '{other}'; expected ignore, property, or collection"
            ))),
        }
    }
}

/// The collection and key at which a term is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The vertex collection that holds the term.
    pub collection: String,
    /// The deterministic document key.
    pub key: String,
}

/// Options controlling the RDF graph model.
#[derive(Debug, Clone)]
pub struct RdfOptions {
    /// Vertex collection (PGT) or base name for the term-typed collections
    /// (RPT: `<base>_URIRef` / `<base>_BNode` / `<base>_Literal`).
    pub vertex_collection: String,
    /// Edge collection that receives predicate (statement) edges.
    pub edge_collection: String,
    /// Which graph model to build.
    pub graph_model: GraphModel,
    /// How literal-valued objects are handled (PGT only; ignored under RPT).
    pub literal_policy: RdfLiteralPolicy,
    /// How the N-Quads named graph is mapped into ArangoDB.
    pub named_graph: NamedGraphMode,
    /// Provenance scope that disambiguates blank-node labels. Blank-node labels
    /// are only document-scoped in RDF, so the same `_:b1` in two different
    /// sources denotes different nodes. When set (e.g. to the source path), the
    /// scope is mixed into blank-node keys so labels never collide across
    /// sources; within one import the scope is constant, so repeated references
    /// to a label still resolve to the same node. `None` preserves the legacy
    /// label-only keys.
    pub blank_node_scope: Option<String>,
}

impl RdfOptions {
    /// Creates options with the default model (`Pgt`), policy (`NoLiterals`),
    /// named-graph handling (`Ignore`), and no blank-node scope.
    #[must_use]
    pub fn new(vertex_collection: impl Into<String>, edge_collection: impl Into<String>) -> Self {
        Self {
            vertex_collection: vertex_collection.into(),
            edge_collection: edge_collection.into(),
            graph_model: GraphModel::default(),
            literal_policy: RdfLiteralPolicy::NoLiterals,
            named_graph: NamedGraphMode::default(),
            blank_node_scope: None,
        }
    }

    /// The deterministic key for a resource under this configuration (blank
    /// nodes are salted with [`RdfOptions::blank_node_scope`]).
    #[must_use]
    pub fn resource_key(&self, resource: &RdfResource) -> String {
        match resource {
            RdfResource::Iri(iri) => hash_key("rdf:iri", &[iri]),
            RdfResource::BlankNode(label) => {
                blank_node_key(label, self.blank_node_scope.as_deref())
            }
        }
    }

    /// The vertex document for a resource, using the scoped key and recording
    /// the blank-node scope when set.
    #[must_use]
    pub fn resource_vertex(&self, resource: &RdfResource) -> Value {
        let key = self.resource_key(resource);
        match resource {
            RdfResource::Iri(iri) => json!({ "_key": key, "iri": iri }),
            RdfResource::BlankNode(label) => {
                let mut doc = json!({ "_key": key, "blank_node": true, "label": label });
                if let Some(scope) = &self.blank_node_scope {
                    doc["scope"] = json!(scope);
                }
                doc
            }
        }
    }

    /// The placement of a subject/object resource under the current model.
    #[must_use]
    pub fn resource_placement(&self, resource: &RdfResource) -> Placement {
        let collection = match (self.graph_model, resource) {
            (GraphModel::Pgt, _) => self.vertex_collection.clone(),
            (GraphModel::Rpt, RdfResource::Iri(_)) => format!("{}_URIRef", self.vertex_collection),
            (GraphModel::Rpt, RdfResource::BlankNode(_)) => {
                format!("{}_BNode", self.vertex_collection)
            }
        };
        Placement {
            collection,
            key: self.resource_key(resource),
        }
    }

    /// The edge collection a statement in `graph` is routed to.
    ///
    /// Only [`NamedGraphMode::Collection`] routes by graph; every other mode
    /// (and the default graph) uses the base edge collection.
    #[must_use]
    pub fn edge_collection_for(&self, graph: Option<&str>) -> String {
        match (self.named_graph, graph) {
            (NamedGraphMode::Collection, Some(graph)) => {
                format!("{}_{}", self.edge_collection, graph_slug(graph))
            }
            _ => self.edge_collection.clone(),
        }
    }

    /// The graph IRI to attach to edges (and fold into their keys), which is
    /// `None` unless the graph is actually being tracked.
    #[must_use]
    pub fn edge_graph<'a>(&self, graph: Option<&'a str>) -> Option<&'a str> {
        match self.named_graph {
            NamedGraphMode::Ignore => None,
            NamedGraphMode::Property | NamedGraphMode::Collection => graph,
        }
    }

    /// The placement of a materialized literal under the current model.
    #[must_use]
    pub fn literal_placement(
        &self,
        value: &str,
        datatype: Option<&str>,
        language: Option<&str>,
    ) -> Placement {
        let collection = match self.graph_model {
            GraphModel::Pgt => self.vertex_collection.clone(),
            GraphModel::Rpt => format!("{}_Literal", self.vertex_collection),
        };
        Placement {
            collection,
            key: literal_key(value, datatype, language),
        }
    }

    /// The vertex collections that must exist for the current model.
    #[must_use]
    pub fn vertex_collections(&self) -> Vec<String> {
        match self.graph_model {
            GraphModel::Pgt => vec![self.vertex_collection.clone()],
            GraphModel::Rpt => vec![
                format!("{}_URIRef", self.vertex_collection),
                format!("{}_BNode", self.vertex_collection),
                format!("{}_Literal", self.vertex_collection),
            ],
        }
    }
}

/// Hex-encodes bytes.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A deterministic, ArangoDB-safe key: the hex SHA-256 of a domain-separated
/// input. Same input always yields the same key, so imports are idempotent.
fn hash_key(domain: &str, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    for part in parts {
        hasher.update([0u8]); // separator so concatenations can't collide
        hasher.update(part.as_bytes());
    }
    hex(&hasher.finalize())
}

/// The deterministic key for a resource vertex (blank nodes are unscoped; use
/// [`RdfOptions::resource_key`] to salt them with a provenance scope).
#[must_use]
pub fn resource_key(resource: &RdfResource) -> String {
    match resource {
        RdfResource::Iri(iri) => hash_key("rdf:iri", &[iri]),
        RdfResource::BlankNode(label) => blank_node_key(label, None),
    }
}

/// The deterministic key for a blank node, optionally salted with a provenance
/// `scope` so identical labels in different sources do not collide.
#[must_use]
pub fn blank_node_key(label: &str, scope: Option<&str>) -> String {
    match scope {
        Some(scope) => hash_key("rdf:bnode", &[scope, label]),
        None => hash_key("rdf:bnode", &[label]),
    }
}

/// A collection-name-safe slug for a named-graph IRI: the IRI with non
/// -alphanumeric characters replaced by `_`, truncated, and suffixed with a
/// short hash so distinct IRIs never map to the same slug.
#[must_use]
pub fn graph_slug(graph: &str) -> String {
    let sanitized: String = graph
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .take(48)
        .collect();
    let digest = hash_key("rdf:graph", &[graph]);
    format!("{sanitized}_{}", &digest[..8])
}

/// The deterministic key for a materialized-literal vertex.
#[must_use]
pub fn literal_key(value: &str, datatype: Option<&str>, language: Option<&str>) -> String {
    hash_key(
        "rdf:literal",
        &[value, datatype.unwrap_or(""), language.unwrap_or("")],
    )
}

/// The deterministic key for a predicate edge, from the fully-qualified
/// endpoint ids (`collection/key`) so edges stay unique across collections.
///
/// When `graph` is `Some`, it is folded into the key so the same triple in
/// different named graphs yields distinct edges. `None` reproduces the legacy
/// (triple-only) key, keeping non-quad imports idempotent.
#[must_use]
pub fn edge_key(from_id: &str, predicate: &str, to_id: &str, graph: Option<&str>) -> String {
    match graph {
        Some(graph) => hash_key("rdf:edge", &[from_id, predicate, to_id, graph]),
        None => hash_key("rdf:edge", &[from_id, predicate, to_id]),
    }
}

/// Builds the vertex document for a resource.
#[must_use]
pub fn resource_vertex(resource: &RdfResource) -> Value {
    let key = resource_key(resource);
    match resource {
        RdfResource::Iri(iri) => json!({ "_key": key, "iri": iri }),
        RdfResource::BlankNode(label) => {
            json!({ "_key": key, "blank_node": true, "label": label })
        }
    }
}

/// Builds the vertex document for a materialized literal.
#[must_use]
pub fn literal_vertex(value: &str, datatype: Option<&str>, language: Option<&str>) -> Value {
    let mut doc = json!({ "_key": literal_key(value, datatype, language), "literal": value });
    if let Some(dt) = datatype {
        doc["datatype"] = json!(dt);
    }
    if let Some(lang) = language {
        doc["language"] = json!(lang);
    }
    doc
}

/// Builds an edge document connecting two placed vertices, carrying the
/// predicate IRI and, when `graph` is `Some`, the named-graph IRI. Endpoints
/// may live in different collections (RPT).
#[must_use]
pub fn edge_document(
    from: &Placement,
    to: &Placement,
    predicate: &str,
    graph: Option<&str>,
) -> Value {
    let from_id = format!("{}/{}", from.collection, from.key);
    let to_id = format!("{}/{}", to.collection, to.key);
    let mut doc = json!({
        "_key": edge_key(&from_id, predicate, &to_id, graph),
        "_from": from_id,
        "_to": to_id,
        "predicate": predicate,
    });
    if let Some(graph) = graph {
        doc["graph"] = json!(graph);
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_deterministic_and_distinct() {
        let a = RdfResource::Iri("http://example.org/a".to_string());
        let a2 = RdfResource::Iri("http://example.org/a".to_string());
        let b = RdfResource::Iri("http://example.org/b".to_string());
        assert_eq!(resource_key(&a), resource_key(&a2));
        assert_ne!(resource_key(&a), resource_key(&b));
    }

    #[test]
    fn iri_and_blank_with_same_text_differ() {
        // Domain separation must keep an IRI distinct from a blank label.
        let iri = RdfResource::Iri("x".to_string());
        let blank = RdfResource::BlankNode("x".to_string());
        assert_ne!(resource_key(&iri), resource_key(&blank));
    }

    #[test]
    fn edge_key_depends_on_all_three_parts() {
        let base = edge_key("a", "p", "b", None);
        assert_ne!(base, edge_key("a", "p", "c", None));
        assert_ne!(base, edge_key("a", "q", "b", None));
        assert_ne!(base, edge_key("c", "p", "b", None));
    }

    #[test]
    fn edge_key_graph_disambiguates_and_is_backward_compatible() {
        let ungraphed = edge_key("a", "p", "b", None);
        let g1 = edge_key("a", "p", "b", Some("http://g/1"));
        let g2 = edge_key("a", "p", "b", Some("http://g/2"));
        // A graph changes the key, and different graphs differ.
        assert_ne!(ungraphed, g1);
        assert_ne!(g1, g2);
        // The unscoped key is unchanged from the legacy 3-part hash.
        assert_eq!(ungraphed, hash_key("rdf:edge", &["a", "p", "b"]));
    }

    #[test]
    fn blank_node_scope_disambiguates_labels() {
        let unscoped = blank_node_key("b1", None);
        let file_a = blank_node_key("b1", Some("a.nq"));
        let file_b = blank_node_key("b1", Some("b.nq"));
        assert_ne!(file_a, file_b, "same label, different sources must differ");
        assert_ne!(unscoped, file_a);
        // Within one scope the label is stable (idempotent re-import).
        assert_eq!(file_a, blank_node_key("b1", Some("a.nq")));
    }

    #[test]
    fn graph_slug_is_safe_and_unique() {
        let a = graph_slug("http://example.org/g1");
        let b = graph_slug("http://example.org/g2");
        assert_ne!(a, b);
        // Only ASCII alphanumerics and underscores appear.
        assert!(a.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    }

    #[test]
    fn edge_collection_routes_only_in_collection_mode() {
        let mut options = RdfOptions::new("v", "e");
        assert_eq!(options.edge_collection_for(Some("http://g")), "e");
        options.named_graph = NamedGraphMode::Property;
        assert_eq!(options.edge_collection_for(Some("http://g")), "e");
        options.named_graph = NamedGraphMode::Collection;
        assert!(options
            .edge_collection_for(Some("http://g"))
            .starts_with("e_"));
        // The default graph always stays in the base collection.
        assert_eq!(options.edge_collection_for(None), "e");
    }

    #[test]
    fn resource_vertex_shape() {
        let v = resource_vertex(&RdfResource::Iri("http://x/1".to_string()));
        assert_eq!(v["iri"], "http://x/1");
        assert!(v["_key"].is_string());
    }
}
