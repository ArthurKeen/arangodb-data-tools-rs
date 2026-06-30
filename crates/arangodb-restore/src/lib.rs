//! Database restore for ArangoDB (single-server MVP).
//!
//! Reads a canonical [`Manifest`], validates it can be restored (refusing
//! encrypted or VelocyPack dumps loudly — PRD §9.4/§19), then recreates
//! collections in dependency order (document collections before edge
//! collections), creating each collection with its indexes and loading its
//! data via `/_api/replication/restore-data`.
//!
//! Scope: single-server, JSONL data. distributeShardsLike ordering, system-
//! collection ordering (`_analyzers` first / `_users` last), vector-index
//! ordering, and resume are deferred (see `docs/IMPLEMENTATION_PLAN.md`).

use std::sync::Arc;
use std::time::Instant;

use arangodb_client::ArangoClient;
use arangodb_storage::{decompress, Compression, ObjectPath, ObjectStore};
use arangodb_tools_core::manifest::{
    ArtifactKind, Compression as ManifestCompression, DataFormat, Manifest,
};
use arangodb_tools_core::progress::{ProgressEvent, ProgressSink, ProgressSnapshot};
use arangodb_tools_core::{Error, Result};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use tokio::io::AsyncReadExt;

/// The manifest object name written by the dump.
pub const MANIFEST_NAME: &str = "dump.manifest.json";

/// Options controlling a restore.
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Replace existing collections of the same name.
    pub overwrite: bool,
    /// If set, create this database (in `_system`) before restoring.
    pub create_database: Option<String>,
}

/// A summary of a completed restore.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreSummary {
    /// Number of collections restored.
    pub collections: usize,
}

/// Restores a dump from `store` into the database the `client` is connected to.
///
/// # Errors
/// Returns an error if the manifest is missing/invalid/unsupported, or any
/// restore request fails.
pub async fn run_restore(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &RestoreOptions,
) -> Result<RestoreSummary> {
    run_restore_with_progress(client, store, options, None).await
}

/// Restores a dump, emitting a [`ProgressEvent::Progress`] snapshot after each
/// collection is restored when `progress` is `Some`. Lifecycle
/// (`started`/`finished`) events are the caller's responsibility.
///
/// # Errors
/// Returns an error if the manifest is missing/invalid/unsupported, or any
/// restore request fails.
pub async fn run_restore_with_progress(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &RestoreOptions,
    progress: Option<Arc<dyn ProgressSink>>,
) -> Result<RestoreSummary> {
    let started = Instant::now();

    if let Some(database) = &options.create_database {
        client.create_database(database).await?;
    }

    let manifest = read_manifest(store).await?;
    validate(&manifest)?;

    // Resolve each collection's structure (parameters + indexes) and order
    // document collections before edge collections.
    let mut collections = Vec::new();
    for group in group_by_collection(&manifest) {
        let structure = read_structure(store, &group).await?;
        collections.push((group, structure));
    }
    collections.sort_by_key(|(_, s)| i64::from(s.is_edge));

    let mut done: u64 = 0;
    for (group, structure) in &collections {
        // Create the collection without indexes; restore-collection does not
        // build secondary indexes, so they are created explicitly after data.
        client
            .restore_collection(&structure.parameters, &[], options.overwrite)
            .await?;
        for data_path in &group.data_paths {
            let body = read_data(store, data_path, &manifest).await?;
            client.restore_data(&group.name, body).await?;
        }
        for index in &structure.indexes {
            // The primary and edge indexes are implicit; the server creates
            // them with the collection.
            let kind = index.get("type").and_then(Value::as_str).unwrap_or("");
            if kind == "primary" || kind == "edge" {
                continue;
            }
            client.create_index(&group.name, index).await?;
        }

        done += 1;
        if let Some(sink) = &progress {
            sink.emit(&ProgressEvent::Progress(ProgressSnapshot {
                batches: done,
                elapsed_secs: started.elapsed().as_secs_f64(),
                ..ProgressSnapshot::default()
            }));
        }
    }

    Ok(RestoreSummary {
        collections: collections.len(),
    })
}

/// A collection's artifacts grouped from the manifest.
struct CollectionGroup {
    name: String,
    structure_path: String,
    data_paths: Vec<String>,
}

/// Parsed structure artifact.
struct Structure {
    parameters: Value,
    indexes: Vec<Value>,
    is_edge: bool,
}

/// Reads and parses `dump.manifest.json`.
async fn read_manifest(store: &dyn ObjectStore) -> Result<Manifest> {
    let bytes = read_object(store, &ObjectPath::new(MANIFEST_NAME), Compression::None).await?;
    let text = String::from_utf8(bytes)
        .map_err(|err| Error::config(format!("manifest is not valid UTF-8: {err}")))?;
    Manifest::from_json(&text)
}

/// Validates that this dump can be restored.
fn validate(manifest: &Manifest) -> Result<()> {
    if manifest.encryption.is_encrypted() {
        return Err(Error::config(format!(
            "refusing to restore an encrypted dump (encryption: {}); this is not supported",
            manifest.encryption.algorithm
        )));
    }
    if let Some(bad) = manifest
        .artifacts
        .iter()
        .find(|a| a.kind == ArtifactKind::Data && a.format == DataFormat::Vpack)
    {
        return Err(Error::config(format!(
            "refusing to restore VelocyPack data ('{}'); only JSONL is supported",
            bad.path
        )));
    }
    Ok(())
}

/// Groups data + structure artifacts by collection, preserving part order.
fn group_by_collection(manifest: &Manifest) -> Vec<CollectionGroup> {
    let mut groups: Vec<CollectionGroup> = Vec::new();
    for artifact in &manifest.artifacts {
        let Some(name) = &artifact.collection else {
            continue;
        };
        let group = match groups.iter_mut().find(|g| &g.name == name) {
            Some(group) => group,
            None => {
                groups.push(CollectionGroup {
                    name: name.clone(),
                    structure_path: String::new(),
                    data_paths: Vec::new(),
                });
                groups.last_mut().expect("just pushed")
            }
        };
        match artifact.kind {
            ArtifactKind::Structure => group.structure_path = artifact.path.clone(),
            ArtifactKind::Data => group.data_paths.push(artifact.path.clone()),
            _ => {}
        }
    }
    groups
}

/// Reads and parses a collection's structure artifact.
async fn read_structure(store: &dyn ObjectStore, group: &CollectionGroup) -> Result<Structure> {
    if group.structure_path.is_empty() {
        return Err(Error::config(format!(
            "collection '{}' has no structure artifact in the manifest",
            group.name
        )));
    }
    let bytes = read_object(
        store,
        &ObjectPath::new(group.structure_path.clone()),
        Compression::None,
    )
    .await?;
    let value: Value = serde_json::from_slice(&bytes)?;
    let parameters = value
        .get("parameters")
        .cloned()
        .ok_or_else(|| Error::config("structure artifact is missing 'parameters'"))?;
    let indexes = value
        .get("indexes")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let is_edge = parameters.get("type").and_then(Value::as_u64) == Some(3);
    Ok(Structure {
        parameters,
        indexes,
        is_edge,
    })
}

/// Reads a data artifact, decompressing per its manifest entry.
async fn read_data(store: &dyn ObjectStore, path: &str, manifest: &Manifest) -> Result<Bytes> {
    let compression = manifest
        .artifacts
        .iter()
        .find(|a| a.path == path)
        .map(|a| storage_compression(a.compression))
        .unwrap_or(Compression::None);
    let bytes = read_object(store, &ObjectPath::new(path.to_string()), compression).await?;
    Ok(Bytes::from(bytes))
}

/// Reads an object fully, applying `compression` decoding.
async fn read_object(
    store: &dyn ObjectStore,
    path: &ObjectPath,
    compression: Compression,
) -> Result<Vec<u8>> {
    let stream = store.get_stream(path, None).await?;
    let reader = tokio_util::io::StreamReader::new(
        stream.map(|chunk| chunk.map_err(|err| std::io::Error::other(err.to_string()))),
    );
    let mut decoder = decompress(compression, reader);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).await?;
    Ok(out)
}

/// Maps a manifest compression marker to the storage codec.
fn storage_compression(compression: ManifestCompression) -> Compression {
    match compression {
        ManifestCompression::None => Compression::None,
        ManifestCompression::Gzip => Compression::Gzip,
        ManifestCompression::Zstd => Compression::Zstd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arangodb_tools_core::manifest::{Artifact, Compression as MC};

    fn data_artifact(path: &str, format: DataFormat) -> Artifact {
        Artifact {
            path: path.to_string(),
            kind: ArtifactKind::Data,
            format,
            compression: MC::None,
            byte_size: 1,
            checksum: None,
            collection: Some("c".to_string()),
            part: Some(0),
        }
    }

    #[test]
    fn rejects_encrypted_dump() {
        let mut m = Manifest::new("db", "0", "t");
        m.encryption.algorithm = "aes-256-ctr".to_string();
        assert!(validate(&m).is_err());
    }

    #[test]
    fn rejects_vpack_data() {
        let mut m = Manifest::new("db", "0", "t");
        m.push(data_artifact("c.data.vpack", DataFormat::Vpack));
        assert!(validate(&m).is_err());
    }

    #[test]
    fn accepts_jsonl_dump() {
        let mut m = Manifest::new("db", "0", "t");
        m.push(data_artifact("c.data.jsonl", DataFormat::Jsonl));
        assert!(validate(&m).is_ok());
    }

    #[test]
    fn groups_artifacts_by_collection() {
        let mut m = Manifest::new("db", "0", "t");
        m.push(Artifact {
            path: "c.structure.json".to_string(),
            kind: ArtifactKind::Structure,
            format: DataFormat::Json,
            compression: MC::None,
            byte_size: 1,
            checksum: None,
            collection: Some("c".to_string()),
            part: None,
        });
        m.push(data_artifact("c.data.jsonl", DataFormat::Jsonl));
        let groups = group_by_collection(&m);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "c");
        assert_eq!(groups[0].structure_path, "c.structure.json");
        assert_eq!(groups[0].data_paths, vec!["c.data.jsonl".to_string()]);
    }
}
