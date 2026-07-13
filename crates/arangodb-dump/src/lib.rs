//! Database dump for ArangoDB (single-server MVP).
//!
//! A dump captures a consistent snapshot by creating a replication batch
//! **before** reading the inventory, then writing, per non-system collection,
//! a structure artifact (`parameters` + `indexes`) and a data artifact (the
//! `/_api/replication/dump` marker JSONL, optionally compressed). Every
//! artifact is recorded in a canonical [`Manifest`] written last as
//! `dump.manifest.json`, so restore never guesses filenames (PRD §8.4). The
//! batch is kept alive with TTL extensions and always released.
//!
//! Scope: single-server, JSONL data. The parallel `/_api/dump/*` protocol and
//! per-shard resume are deferred (see `docs/IMPLEMENTATION_PLAN.md`).

/// The crate README, compiled as doctests so its examples stay in sync with the
/// API. `#[cfg(doctest)]` keeps this helper out of the rendered documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

use std::sync::{Arc, Mutex};
use std::time::Instant;

use arangodb_client::ArangoClient;
use arangodb_storage::{compress, ByteStream, Compression, ObjectPath, ObjectStore};
use arangodb_tools_core::manifest::{
    Artifact, ArtifactKind, Checksum, Compression as ManifestCompression, DataFormat, Manifest,
};
use arangodb_tools_core::progress::{ProgressEvent, ProgressSink, ProgressSnapshot};
use arangodb_tools_core::{Error, Result};
use bytes::Bytes;
use futures::StreamExt;
use regex::Regex;
use sha2::{Digest, Sha256};

/// Regex filters selecting which collections a dump includes.
///
/// A collection is dumped when it matches `include` (or `include` is unset)
/// **and** does not match `exclude`.
#[derive(Debug, Clone, Default)]
pub struct FilterOptions {
    /// Only collections whose name matches this pattern are included.
    pub include_collections: Option<Regex>,
    /// Collections whose name matches this pattern are excluded.
    pub exclude_collections: Option<Regex>,
}

impl FilterOptions {
    /// Compiles include/exclude patterns into filter options.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if either pattern is not a valid regex.
    pub fn new(include: Option<&str>, exclude: Option<&str>) -> Result<Self> {
        let compile = |p: Option<&str>| -> Result<Option<Regex>> {
            match p {
                Some(pattern) => Regex::new(pattern).map(Some).map_err(|err| {
                    Error::config(format!("invalid collection filter regex: {err}"))
                }),
                None => Ok(None),
            }
        };
        Ok(Self {
            include_collections: compile(include)?,
            exclude_collections: compile(exclude)?,
        })
    }

    /// Returns `true` if a collection named `name` passes the filters.
    #[must_use]
    pub fn accepts(&self, name: &str) -> bool {
        if let Some(include) = &self.include_collections {
            if !include.is_match(name) {
                return false;
            }
        }
        if let Some(exclude) = &self.exclude_collections {
            if exclude.is_match(name) {
                return false;
            }
        }
        true
    }
}

/// Options controlling a dump.
#[derive(Debug, Clone)]
pub struct DumpOptions {
    /// Include system collections (names starting with `_`).
    pub include_system: bool,
    /// Dump all accessible databases (writes per-database artifacts under
    /// `databases/{name}/...` and produces a combined manifest).
    pub all_databases: bool,
    /// Regex filters selecting which collections to dump.
    pub filters: FilterOptions,
    /// Compression for data artifacts.
    pub compression: Compression,
    /// Replication-batch TTL, in seconds (extended before each collection).
    pub batch_ttl_secs: u32,
    /// Per-request dump chunk size, in bytes.
    pub chunk_size: u64,
    /// Source database name (recorded in the manifest).
    pub database: String,
    /// Producing tool version (recorded in the manifest).
    pub tool_version: String,
    /// RFC 3339 creation timestamp (recorded in the manifest).
    pub created_at: String,
}

impl Default for DumpOptions {
    fn default() -> Self {
        Self {
            include_system: false,
            all_databases: false,
            filters: FilterOptions::default(),
            compression: Compression::None,
            batch_ttl_secs: 600,
            chunk_size: 8 * 1024 * 1024,
            database: "_system".to_string(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: String::new(),
        }
    }
}

/// Dumps the connected database to `store`, returning the manifest.
///
/// Creates a replication batch up front for a consistent snapshot and always
/// releases it, even on error.
///
/// # Errors
/// Returns an error if any inventory/dump request or storage write fails.
pub async fn run_dump(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &DumpOptions,
) -> Result<Manifest> {
    run_dump_with_progress(client, store, options, None).await
}

/// Dumps the connected database(s) to `store`, emitting a
/// [`ProgressEvent::Progress`] snapshot after each collection is written when
/// `progress` is `Some`. Lifecycle (`started`/`finished`) events are the
/// caller's responsibility.
///
/// # Errors
/// Returns an error if any inventory/dump request or storage write fails.
pub async fn run_dump_with_progress(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &DumpOptions,
    progress: Option<Arc<dyn ProgressSink>>,
) -> Result<Manifest> {
    let mut state = DumpProgress {
        sink: progress.as_deref(),
        started: Instant::now(),
        collections: 0,
    };
    if !options.all_databases {
        let batch = client
            .replication_batch_create(options.batch_ttl_secs)
            .await?;
        // Ensure the batch is released regardless of how the dump finishes.
        let result = dump_with_batch(client, store, options, &batch, &mut state).await;
        let _ = client.replication_batch_delete(&batch).await;
        result
    } else {
        // Multi-database dump: enumerate accessible databases and append each
        // database's artifacts into a combined manifest. Each artifact path is
        // prefixed with `databases/{db}/` so restores can target a specific DB.
        let dbs = client.list_databases().await?;
        let mut manifest = Manifest::new(
            "all",
            options.tool_version.clone(),
            options.created_at.clone(),
        );
        for db in dbs {
            let client_db = client.with_database(&db);
            let batch = client_db
                .replication_batch_create(options.batch_ttl_secs)
                .await?;
            // Prefix all artifact paths for this database.
            let prefix = format!("databases/{db}/");
            dump_db_into_manifest(
                &client_db,
                store,
                options,
                &batch,
                &prefix,
                Some(&db),
                &mut manifest,
                &mut state,
            )
            .await?;
            let _ = client_db.replication_batch_delete(&batch).await;
        }

        let manifest_json = manifest.to_json()?;
        store
            .put_stream(
                &ObjectPath::new("dump.manifest.json"),
                once(Bytes::from(manifest_json.into_bytes())),
            )
            .await?;
        Ok(manifest)
    }
}

/// Tracks dump progress across collections (and databases) so a periodic
/// snapshot can be emitted as each collection completes.
struct DumpProgress<'a> {
    sink: Option<&'a dyn ProgressSink>,
    started: Instant,
    collections: u64,
}

impl DumpProgress<'_> {
    /// Records one completed collection and emits a snapshot if a sink is set.
    /// `bytes` is the cumulative data-artifact size written so far.
    fn collection_done(&mut self, bytes: u64) {
        self.collections += 1;
        if let Some(sink) = self.sink {
            sink.emit(&ProgressEvent::Progress(ProgressSnapshot {
                bytes_written: bytes,
                batches: self.collections,
                elapsed_secs: self.started.elapsed().as_secs_f64(),
                ..ProgressSnapshot::default()
            }));
        }
    }
}

/// Sums the byte size of all data artifacts recorded so far.
fn data_bytes(manifest: &Manifest) -> u64 {
    manifest
        .artifacts
        .iter()
        .filter(|a| a.kind == ArtifactKind::Data)
        .map(|a| a.byte_size)
        .sum()
}

/// The dump body, run inside an active replication batch.
async fn dump_with_batch(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &DumpOptions,
    batch: &str,
    progress: &mut DumpProgress<'_>,
) -> Result<Manifest> {
    let mut manifest = Manifest::new(
        options.database.clone(),
        options.tool_version.clone(),
        options.created_at.clone(),
    );
    dump_db_into_manifest(
        client,
        store,
        options,
        batch,
        "",
        None,
        &mut manifest,
        progress,
    )
    .await?;
    let manifest_json = manifest.to_json()?;
    store
        .put_stream(
            &ObjectPath::new("dump.manifest.json"),
            once(Bytes::from(manifest_json.into_bytes())),
        )
        .await?;
    Ok(manifest)
}

/// Core per-database dump logic which appends artifacts into `manifest`.
#[allow(clippy::too_many_arguments)]
async fn dump_db_into_manifest(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &DumpOptions,
    batch: &str,
    path_prefix: &str,
    database: Option<&str>,
    manifest: &mut Manifest,
    progress: &mut DumpProgress<'_>,
) -> Result<()> {
    let inventory = client
        .replication_inventory(batch, options.include_system)
        .await?;

    for collection in &inventory.collections {
        if collection.is_system() && !options.include_system {
            continue;
        }
        let name = collection
            .name()
            .ok_or_else(|| Error::config("inventory collection is missing a name"))?
            .to_string();

        // Apply include/exclude filters (system collections bypass filtering so
        // an include pattern for user data doesn't drop required system ones).
        if !collection.is_system() && !options.filters.accepts(&name) {
            continue;
        }

        // Keep the snapshot alive across collections.
        client
            .replication_batch_extend(batch, options.batch_ttl_secs)
            .await?;

        write_structure_with_prefix(store, path_prefix, database, &name, collection, manifest)
            .await?;
        write_data_with_prefix(
            client,
            store,
            options,
            batch,
            path_prefix,
            database,
            &name,
            manifest,
        )
        .await?;

        progress.collection_done(data_bytes(manifest));
    }
    Ok(())
}

/// Writes a collection's structure (`parameters` + `indexes`) artifact under
/// `prefix` (empty for a single-database dump).
async fn write_structure_with_prefix(
    store: &dyn ObjectStore,
    prefix: &str,
    database: Option<&str>,
    name: &str,
    collection: &arangodb_client::InventoryCollection,
    manifest: &mut Manifest,
) -> Result<()> {
    let structure = serde_json::json!({
        "parameters": collection.parameters,
        "indexes": collection.indexes,
    });
    let bytes = serde_json::to_vec_pretty(&structure)?;
    let path = format!("{prefix}{name}.structure.json");
    let meta = store
        .put_stream(&ObjectPath::new(path.clone()), once(Bytes::from(bytes)))
        .await?;
    manifest.push(Artifact {
        path,
        kind: ArtifactKind::Structure,
        format: DataFormat::Json,
        compression: ManifestCompression::None,
        byte_size: meta.size,
        checksum: None,
        collection: Some(name.to_string()),
        database: database.map(str::to_string),
        part: None,
    });
    Ok(())
}

/// Streams a collection's replication dump to a (optionally compressed) data
/// artifact under `prefix` (empty for a single-database dump), recording its
/// size and checksum.
#[allow(clippy::too_many_arguments)]
async fn write_data_with_prefix(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &DumpOptions,
    batch: &str,
    prefix: &str,
    database: Option<&str>,
    name: &str,
    manifest: &mut Manifest,
) -> Result<()> {
    let suffix = match options.compression.extension() {
        Some(ext) => format!("data.jsonl.{ext}"),
        None => "data.jsonl".to_string(),
    };
    let path = format!("{prefix}{name}.{suffix}");

    let raw = dump_data_stream(
        client.clone(),
        name.to_string(),
        batch.to_string(),
        options.chunk_size,
    );
    let hasher = Arc::new(Mutex::new(Sha256::new()));
    let body = hashing(compress(options.compression, raw), Arc::clone(&hasher));
    let meta = store
        .put_stream(&ObjectPath::new(path.clone()), body)
        .await?;

    let digest = hasher
        .lock()
        .expect("hasher not poisoned")
        .clone()
        .finalize();
    manifest.push(Artifact {
        path,
        kind: ArtifactKind::Data,
        format: DataFormat::Jsonl,
        compression: map_compression(options.compression),
        byte_size: meta.size,
        checksum: Some(Checksum {
            algorithm: "sha256".to_string(),
            value: hex(&digest),
        }),
        collection: Some(name.to_string()),
        database: database.map(str::to_string),
        part: Some(0),
    });
    Ok(())
}

/// Pages `replication_dump_chunk` until the server has no more data, yielding
/// each non-empty chunk body.
fn dump_data_stream(
    client: ArangoClient,
    collection: String,
    batch: String,
    chunk_size: u64,
) -> ByteStream {
    Box::pin(async_stream::try_stream! {
        let mut from: u64 = 0;
        loop {
            let chunk = client
                .replication_dump_chunk(&collection, &batch, from, chunk_size)
                .await?;
            if !chunk.body.is_empty() {
                yield chunk.body.clone();
            }
            let next = chunk.last_included_tick;
            // Stop when the server signals no more data, or the tick fails to
            // advance (defensive: never loop forever).
            if !chunk.has_more || next == 0 || next <= from {
                break;
            }
            from = next;
        }
    })
}

/// Forwards a byte stream while updating `hasher` with each chunk.
fn hashing(input: ByteStream, hasher: Arc<Mutex<Sha256>>) -> ByteStream {
    Box::pin(input.map(move |chunk| {
        if let Ok(bytes) = &chunk {
            hasher.lock().expect("hasher not poisoned").update(bytes);
        }
        chunk
    }))
}

/// Wraps bytes in a single-chunk stream.
fn once(bytes: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(bytes) }))
}

/// Hex-encodes a digest.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Maps the storage codec to the manifest compression enum.
fn map_compression(compression: Compression) -> ManifestCompression {
    match compression {
        Compression::None => ManifestCompression::None,
        Compression::Gzip => ManifestCompression::Gzip,
        Compression::Zstd => ManifestCompression::Zstd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_filters_accept_everything() {
        let filters = FilterOptions::default();
        assert!(filters.accepts("users"));
        assert!(filters.accepts("anything"));
    }

    #[test]
    fn include_filter_selects_matching_names() {
        let filters = FilterOptions::new(Some("^col_[0-9]+$"), None).unwrap();
        assert!(filters.accepts("col_1"));
        assert!(filters.accepts("col_42"));
        assert!(!filters.accepts("users"));
        assert!(!filters.accepts("col_x"));
    }

    #[test]
    fn exclude_filter_removes_matching_names() {
        let filters = FilterOptions::new(None, Some("^tmp_")).unwrap();
        assert!(filters.accepts("users"));
        assert!(!filters.accepts("tmp_cache"));
    }

    #[test]
    fn exclude_takes_precedence_over_include() {
        let filters = FilterOptions::new(Some("^col_"), Some("_tmp$")).unwrap();
        assert!(filters.accepts("col_users"));
        assert!(!filters.accepts("col_users_tmp"));
    }

    #[test]
    fn invalid_regex_is_a_config_error() {
        assert!(FilterOptions::new(Some("("), None).is_err());
    }
}
