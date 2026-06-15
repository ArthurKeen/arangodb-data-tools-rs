//! Collection types and metadata for `/_api/collection`.

use serde::Deserialize;

/// The kind of collection to create.
///
/// The numeric values match ArangoDB's `type` discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CollectionKind {
    /// A document collection (`type` 2).
    #[default]
    Document,
    /// An edge collection (`type` 3).
    Edge,
}

impl CollectionKind {
    /// The numeric `type` understood by `/_api/collection`.
    #[must_use]
    pub fn type_id(self) -> u8 {
        match self {
            Self::Document => 2,
            Self::Edge => 3,
        }
    }

    /// Whether this is an edge collection.
    #[must_use]
    pub fn is_edge(self) -> bool {
        matches!(self, Self::Edge)
    }
}

/// Collection metadata as returned by `/_api/collection/{name}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CollectionInfo {
    /// The collection name.
    pub name: String,
    /// The numeric collection type (2 = document, 3 = edge).
    #[serde(rename = "type")]
    pub type_id: u8,
}

impl CollectionInfo {
    /// The collection kind, or `None` for an unrecognized type id.
    #[must_use]
    pub fn kind(&self) -> Option<CollectionKind> {
        match self.type_id {
            2 => Some(CollectionKind::Document),
            3 => Some(CollectionKind::Edge),
            _ => None,
        }
    }
}

/// The document-count response from `/_api/collection/{name}/count`.
#[derive(Debug, Clone, Copy, Deserialize)]
pub(crate) struct CollectionCount {
    /// Number of documents in the collection.
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_ids_match_arango() {
        assert_eq!(CollectionKind::Document.type_id(), 2);
        assert_eq!(CollectionKind::Edge.type_id(), 3);
        assert!(CollectionKind::Edge.is_edge());
        assert!(!CollectionKind::Document.is_edge());
    }

    #[test]
    fn parses_collection_info() {
        let info: CollectionInfo =
            serde_json::from_str(r#"{"name":"users","type":2,"status":3}"#).unwrap();
        assert_eq!(info.name, "users");
        assert_eq!(info.kind(), Some(CollectionKind::Document));
    }

    #[test]
    fn unknown_type_id_has_no_kind() {
        let info = CollectionInfo {
            name: "x".to_owned(),
            type_id: 99,
        };
        assert_eq!(info.kind(), None);
    }
}
