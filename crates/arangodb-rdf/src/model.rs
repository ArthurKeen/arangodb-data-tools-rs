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
}

impl RdfOptions {
    /// Creates options with the default model (`Pgt`) and policy
    /// (`NoLiterals`).
    #[must_use]
    pub fn new(vertex_collection: impl Into<String>, edge_collection: impl Into<String>) -> Self {
        Self {
            vertex_collection: vertex_collection.into(),
            edge_collection: edge_collection.into(),
            graph_model: GraphModel::default(),
            literal_policy: RdfLiteralPolicy::NoLiterals,
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
            key: resource_key(resource),
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

/// The deterministic key for a resource vertex.
#[must_use]
pub fn resource_key(resource: &RdfResource) -> String {
    match resource {
        RdfResource::Iri(iri) => hash_key("rdf:iri", &[iri]),
        RdfResource::BlankNode(label) => hash_key("rdf:bnode", &[label]),
    }
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
#[must_use]
pub fn edge_key(from_id: &str, predicate: &str, to_id: &str) -> String {
    hash_key("rdf:edge", &[from_id, predicate, to_id])
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
/// predicate IRI. Endpoints may live in different collections (RPT).
#[must_use]
pub fn edge_document(from: &Placement, to: &Placement, predicate: &str) -> Value {
    let from_id = format!("{}/{}", from.collection, from.key);
    let to_id = format!("{}/{}", to.collection, to.key);
    json!({
        "_key": edge_key(&from_id, predicate, &to_id),
        "_from": from_id,
        "_to": to_id,
        "predicate": predicate,
    })
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
        let base = edge_key("a", "p", "b");
        assert_ne!(base, edge_key("a", "p", "c"));
        assert_ne!(base, edge_key("a", "q", "b"));
        assert_ne!(base, edge_key("c", "p", "b"));
    }

    #[test]
    fn resource_vertex_shape() {
        let v = resource_vertex(&RdfResource::Iri("http://x/1".to_string()));
        assert_eq!(v["iri"], "http://x/1");
        assert!(v["_key"].is_string());
    }
}
