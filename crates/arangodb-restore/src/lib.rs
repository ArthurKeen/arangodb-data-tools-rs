//! Database restore for ArangoDB (single-server MVP).
//!
//! Reads a canonical [`Manifest`], validates it can be restored (refusing
//! encrypted or VelocyPack dumps loudly — PRD §9.4/§19), then recreates
//! collections in dependency order (document collections before edge
//! collections), creating each collection with its indexes and loading its
//! data via `/_api/replication/restore-data`.
//!
//! Two dump shapes are handled:
//! - **Single-database** dumps restore into the database the client targets (or
//!   a `--create-database` name).
//! - **Multi-database** dumps (produced by `dump --all-databases`) carry a
//!   `database` on each artifact; each database is created and its collections
//!   restored into it.
//!
//! Restores are **resumable**: with a [`RestoreCheckpointConfig`], the set of
//! fully-restored collections is recorded after each one, so a restart skips
//! completed collections. The checkpoint is bound to the dump's manifest
//! fingerprint and refuses to resume against a different dump.
//!
//! Scope: single-server, JSONL data. distributeShardsLike ordering, system-
//! collection ordering (`_analyzers` first / `_users` last), and vector-index
//! ordering are deferred (see `docs/IMPLEMENTATION_PLAN.md`).

/// The crate README, compiled as doctests so its examples stay in sync with the
/// API. `#[cfg(doctest)]` keeps this helper out of the rendered documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

use std::sync::Arc;
use std::time::Instant;

use arangodb_client::ArangoClient;
use arangodb_storage::{decompress, Compression, ObjectPath, ObjectStore};
use arangodb_tools_core::manifest::{
    ArtifactKind, Compression as ManifestCompression, DataFormat, Manifest, RestoreCheckpoint,
};
use arangodb_tools_core::progress::{ProgressEvent, ProgressSink, ProgressSnapshot};
use arangodb_tools_core::{Error, Result};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use tokio::io::AsyncReadExt;

/// The manifest object name written by the dump.
pub const MANIFEST_NAME: &str = "dump.manifest.json";

/// Where a resumable restore records its progress.
///
/// The checkpoint is a single object that is overwritten as collections are
/// completed. It must live somewhere writable (typically alongside the dump or
/// in a local working directory).
#[derive(Clone, Debug)]
pub struct RestoreCheckpointConfig {
    /// The store that holds the checkpoint object.
    pub store: Arc<dyn ObjectStore>,
    /// The checkpoint object's path within the store.
    pub path: ObjectPath,
}

impl RestoreCheckpointConfig {
    /// Creates a checkpoint config writing to `path` in `store`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjectPath) -> Self {
        Self { store, path }
    }
}

/// Options controlling a restore.
#[derive(Debug, Clone, Default)]
pub struct RestoreOptions {
    /// Replace existing collections of the same name.
    pub overwrite: bool,
    /// If set, create this database (in `_system`) before restoring a
    /// single-database dump. Ignored for multi-database dumps (each database is
    /// created from the manifest).
    pub create_database: Option<String>,
    /// If set, restore resumably: skip collections already recorded here and
    /// record each collection as it completes.
    pub checkpoint: Option<RestoreCheckpointConfig>,
}

/// A summary of a completed restore.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoreSummary {
    /// Number of collections in the dump (including any skipped on resume).
    pub collections: usize,
    /// Number of collections actually restored by this invocation.
    pub restored: usize,
    /// Number of collections skipped because a checkpoint marked them done.
    pub skipped: usize,
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
/// Returns an error if the manifest is missing/invalid/unsupported, a resume
/// checkpoint belongs to a different dump, or any restore request fails.
pub async fn run_restore_with_progress(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &RestoreOptions,
    progress: Option<Arc<dyn ProgressSink>>,
) -> Result<RestoreSummary> {
    let started = Instant::now();

    let manifest = read_manifest(store).await?;
    validate(&manifest)?;

    // Resolve each collection's structure and order units by database
    // (first-appearance) then document-before-edge.
    let mut units = Vec::new();
    for group in group_by_collection(&manifest) {
        let structure = read_structure(store, &group).await?;
        units.push((group, structure));
    }
    let mut db_order: Vec<Option<String>> = Vec::new();
    for (group, _) in &units {
        if !db_order.contains(&group.database) {
            db_order.push(group.database.clone());
        }
    }
    units.sort_by_key(|(group, structure)| {
        let db_index = db_order
            .iter()
            .position(|d| d == &group.database)
            .unwrap_or(0);
        (db_index, i64::from(structure.is_edge))
    });

    let multi_db = units.iter().any(|(group, _)| group.database.is_some());

    // Create target databases up front (create_database is idempotent).
    if multi_db {
        for database in db_order.iter().flatten() {
            client.create_database(database).await?;
        }
    } else if let Some(database) = &options.create_database {
        client.create_database(database).await?;
    }

    // Load or initialize the resume checkpoint, refusing a mismatched dump.
    let fingerprint = manifest.fingerprint();
    let mut checkpoint = match &options.checkpoint {
        Some(config) => match load_restore_checkpoint(config.store.as_ref(), &config.path).await? {
            Some(existing) if existing.manifest != fingerprint => {
                return Err(Error::config(
                    "restore checkpoint does not match this dump (manifest fingerprint mismatch); \
                     use a fresh checkpoint path or the dump the checkpoint was created for",
                ));
            }
            Some(existing) => existing,
            None => RestoreCheckpoint::new(fingerprint.clone()),
        },
        None => RestoreCheckpoint::new(fingerprint.clone()),
    };

    let total = units.len();
    let mut restored: usize = 0;
    let mut skipped: usize = 0;
    let mut done: u64 = 0;
    for (group, structure) in &units {
        let id = unit_id(group);
        if options.checkpoint.is_some() && checkpoint.contains(&id) {
            skipped += 1;
            done += 1;
            continue;
        }

        // A per-database client for multi-DB dumps; otherwise the target DB
        // (a create-database override or the client's own database).
        let db_client = match &group.database {
            Some(database) => client.with_database(database),
            None => match &options.create_database {
                Some(target) => client.with_database(target),
                None => client.with_database(client.database()),
            },
        };

        restore_collection(
            &db_client,
            store,
            group,
            structure,
            options.overwrite,
            &manifest,
        )
        .await?;
        restored += 1;
        done += 1;

        if let Some(config) = &options.checkpoint {
            checkpoint.completed.push(id);
            persist_restore_checkpoint(config, &checkpoint).await;
        }
        if let Some(sink) = &progress {
            sink.emit(&ProgressEvent::Progress(ProgressSnapshot {
                batches: done,
                elapsed_secs: started.elapsed().as_secs_f64(),
                ..ProgressSnapshot::default()
            }));
        }
    }

    Ok(RestoreSummary {
        collections: total,
        restored,
        skipped,
    })
}

/// Restores a single collection: create it, load its data parts, then build its
/// non-implicit indexes.
async fn restore_collection(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    group: &CollectionGroup,
    structure: &Structure,
    overwrite: bool,
    manifest: &Manifest,
) -> Result<()> {
    // Create the collection without indexes; restore-collection does not build
    // secondary indexes, so they are created explicitly after data.
    client
        .restore_collection(&structure.parameters, &[], overwrite)
        .await?;
    for data_path in &group.data_paths {
        let body = read_data(store, data_path, manifest).await?;
        client.restore_data(&group.name, body).await?;
    }
    for index in &structure.indexes {
        // The primary and edge indexes are implicit; the server creates them
        // with the collection.
        let kind = index.get("type").and_then(Value::as_str).unwrap_or("");
        if kind == "primary" || kind == "edge" {
            continue;
        }
        client.create_index(&group.name, index).await?;
    }
    Ok(())
}

/// The stable per-collection identifier used in the resume checkpoint:
/// `"{database}::{collection}"` (`database` empty for single-database dumps).
fn unit_id(group: &CollectionGroup) -> String {
    format!(
        "{}::{}",
        group.database.as_deref().unwrap_or(""),
        group.name
    )
}

/// Loads an existing restore checkpoint, returning `None` if none is present.
async fn load_restore_checkpoint(
    store: &dyn ObjectStore,
    path: &ObjectPath,
) -> Result<Option<RestoreCheckpoint>> {
    if !store.exists(path).await? {
        return Ok(None);
    }
    let mut stream = store.get_stream(path, None).await?;
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        buffer.extend_from_slice(&chunk?);
    }
    Ok(Some(RestoreCheckpoint::from_json(&buffer)?))
}

/// Writes the restore checkpoint, logging (but not failing) on error so a
/// checkpoint-store hiccup never aborts an otherwise-successful restore.
async fn persist_restore_checkpoint(config: &RestoreCheckpointConfig, state: &RestoreCheckpoint) {
    let result = async {
        let json = state.to_json()?;
        let bytes = Bytes::from(json.into_bytes());
        let stream: arangodb_storage::ByteStream =
            Box::pin(futures::stream::once(async move { Ok(bytes) }));
        config.store.put_stream(&config.path, stream).await
    }
    .await;
    if let Err(err) = result {
        tracing::warn!(
            path = %config.path,
            completed = state.completed.len(),
            error = %err,
            "failed to persist restore checkpoint; completed collections may be redone on resume",
        );
    }
}

/// A collection's artifacts grouped from the manifest.
struct CollectionGroup {
    name: String,
    database: Option<String>,
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

/// Groups data + structure artifacts by (database, collection), preserving
/// first-appearance and part order. Multi-database dumps may repeat a
/// collection name across databases, so both fields form the key.
fn group_by_collection(manifest: &Manifest) -> Vec<CollectionGroup> {
    let mut groups: Vec<CollectionGroup> = Vec::new();
    for artifact in &manifest.artifacts {
        let Some(name) = &artifact.collection else {
            continue;
        };
        let group = match groups
            .iter_mut()
            .find(|g| &g.name == name && g.database == artifact.database)
        {
            Some(group) => group,
            None => {
                groups.push(CollectionGroup {
                    name: name.clone(),
                    database: artifact.database.clone(),
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
            database: None,
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
            database: None,
            part: None,
        });
        m.push(data_artifact("c.data.jsonl", DataFormat::Jsonl));
        let groups = group_by_collection(&m);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name, "c");
        assert_eq!(groups[0].structure_path, "c.structure.json");
        assert_eq!(groups[0].data_paths, vec!["c.data.jsonl".to_string()]);
    }

    fn artifact_in(db: &str, collection: &str, kind: ArtifactKind) -> Artifact {
        let suffix = match kind {
            ArtifactKind::Structure => "structure.json",
            _ => "data.jsonl",
        };
        Artifact {
            path: format!("databases/{db}/{collection}.{suffix}"),
            kind,
            format: DataFormat::Jsonl,
            compression: MC::None,
            byte_size: 1,
            checksum: None,
            collection: Some(collection.to_string()),
            database: Some(db.to_string()),
            part: (kind == ArtifactKind::Data).then_some(0),
        }
    }

    #[test]
    fn multi_db_groups_key_on_database_and_collection() {
        let mut m = Manifest::new("all", "0", "t");
        m.push(artifact_in("db1", "users", ArtifactKind::Structure));
        m.push(artifact_in("db1", "users", ArtifactKind::Data));
        m.push(artifact_in("db2", "users", ArtifactKind::Structure));
        m.push(artifact_in("db2", "users", ArtifactKind::Data));
        let groups = group_by_collection(&m);
        assert_eq!(groups.len(), 2, "same name in two DBs stays separate");
        assert_eq!(unit_id(&groups[0]), "db1::users");
        assert_eq!(unit_id(&groups[1]), "db2::users");
    }

    #[test]
    fn single_db_unit_id_has_empty_database() {
        let group = CollectionGroup {
            name: "users".to_string(),
            database: None,
            structure_path: String::new(),
            data_paths: Vec::new(),
        };
        assert_eq!(unit_id(&group), "::users");
    }
}
