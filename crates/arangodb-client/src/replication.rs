//! Types for the replication dump/restore endpoints (`/_api/replication/*`).
//!
//! These back the single-server dump and restore paths. A replication *batch*
//! pins a consistent snapshot (and must be kept alive with TTL extensions for
//! the duration of a transfer); the *inventory* enumerates collections with
//! their `parameters` and `indexes`; *dump* streams a collection's documents
//! as `{"type":2300,"data":{…}}` markers, which *restore-data* consumes
//! verbatim.

use serde::Deserialize;
use serde_json::Value;

/// A collection entry from the replication inventory.
///
/// `parameters` and `indexes` are kept as opaque JSON so they round-trip to
/// restore without lossy re-modeling.
#[derive(Debug, Clone, Deserialize)]
pub struct InventoryCollection {
    /// The collection's properties (name, type, keyOptions, shard config, …).
    pub parameters: Value,
    /// Secondary index definitions (the primary index is implicit).
    #[serde(default)]
    pub indexes: Vec<Value>,
}

impl InventoryCollection {
    /// The collection name, if present.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.parameters.get("name").and_then(Value::as_str)
    }

    /// The numeric collection type (2 = document, 3 = edge), if present.
    #[must_use]
    pub fn type_id(&self) -> Option<u64> {
        self.parameters.get("type").and_then(Value::as_u64)
    }

    /// Whether this is an edge collection.
    #[must_use]
    pub fn is_edge(&self) -> bool {
        self.type_id() == Some(3)
    }

    /// Whether this is a system collection (name starts with `_`).
    #[must_use]
    pub fn is_system(&self) -> bool {
        self.parameters
            .get("isSystem")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| self.name().is_some_and(|n| n.starts_with('_')))
    }
}

/// The replication inventory of a database.
#[derive(Debug, Clone, Deserialize)]
pub struct Inventory {
    /// The collections in the database.
    #[serde(default)]
    pub collections: Vec<InventoryCollection>,
    /// View definitions (opaque; restored as-is).
    #[serde(default)]
    pub views: Vec<Value>,
}

/// One chunk of a collection's replication dump.
#[derive(Debug, Clone)]
pub struct DumpChunk {
    /// The raw `{"type":…,"data":…}` JSONL body for this chunk.
    pub body: bytes::Bytes,
    /// The highest tick included in this chunk; `0` when the chunk is empty.
    pub last_included_tick: u64,
    /// Whether the server has more data beyond this chunk.
    pub has_more: bool,
}

impl DumpChunk {
    /// Whether this chunk contains no data.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.last_included_tick == 0 || self.body.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inventory_and_classifies_collections() {
        let body = r#"{
            "collections": [
                {"parameters": {"name": "users", "type": 2, "isSystem": false}, "indexes": [{"type": "persistent"}]},
                {"parameters": {"name": "knows", "type": 3, "isSystem": false}, "indexes": []},
                {"parameters": {"name": "_apps", "type": 2, "isSystem": true}}
            ],
            "views": []
        }"#;
        let inventory: Inventory = serde_json::from_str(body).unwrap();
        assert_eq!(inventory.collections.len(), 3);

        let users = &inventory.collections[0];
        assert_eq!(users.name(), Some("users"));
        assert!(!users.is_edge());
        assert!(!users.is_system());
        assert_eq!(users.indexes.len(), 1);

        assert!(inventory.collections[1].is_edge());
        assert!(inventory.collections[2].is_system());
    }

    #[test]
    fn dump_chunk_emptiness() {
        let empty = DumpChunk {
            body: bytes::Bytes::new(),
            last_included_tick: 0,
            has_more: false,
        };
        assert!(empty.is_empty());

        let nonempty = DumpChunk {
            body: bytes::Bytes::from_static(b"{\"type\":2300}"),
            last_included_tick: 7,
            has_more: true,
        };
        assert!(!nonempty.is_empty());
    }
}
