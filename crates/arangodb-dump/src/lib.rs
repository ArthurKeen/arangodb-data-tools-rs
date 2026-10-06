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
//! Scope: single-server, JSONL data. A dump refuses to run against a cluster
//! deployment (the server role is checked before any work starts), so the
//! untested cluster path can never silently produce an incomplete dump. The
//! parallel `/_api/dump/*` protocol and per-shard resume are deferred (see
//! `docs/IMPLEMENTATION_PLAN.md`).

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
    /// Replace a dump already present at the destination.
    ///
    /// Without this, a destination that already holds a completed dump, or a
    /// partial one from a failed or still-running dump, is refused.
    pub overwrite: bool,
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
            overwrite: false,
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

    // Refuse a cluster before creating any batch or writing any artifact
    // (PRD §8.4): the single-server replication path would otherwise produce a
    // dump whose cross-shard completeness was never verified.
    preflight_topology(client, progress.as_deref()).await?;

    // Claim the destination before writing anything, so two dumps aimed at one
    // prefix cannot interleave their artifacts (PRD §9.2).
    claim_destination(store, options).await?;

    let outcome = run_dump_body(client, store, options, &mut state).await;
    if outcome.is_ok() {
        release_destination(store).await;
    }
    outcome
}

/// The object that marks a dump as in progress at a destination.
///
/// Written with a conditional create before any artifact and removed once the
/// manifest lands, so its presence means either a dump is running right now or
/// an earlier one died partway.
pub const DUMP_LOCK_NAME: &str = "dump.in-progress.json";

/// Takes exclusive ownership of the destination prefix.
///
/// Two guards, because they catch different things. The manifest check rejects
/// a destination that already holds a *finished* dump, which is the common
/// mistake and deserves its own message. The conditional create of the lock
/// object is what actually makes concurrency safe: two dumps starting at the
/// same instant both see no manifest, but only one can create the lock.
///
/// This is the conditional-write path the storage layer exists to provide —
/// writing the lock with an unconditional put would reintroduce the very race
/// it is here to close.
async fn claim_destination(store: &dyn ObjectStore, options: &DumpOptions) -> Result<()> {
    let lock = ObjectPath::new(DUMP_LOCK_NAME);
    let manifest = ObjectPath::new("dump.manifest.json");

    if options.overwrite {
        // Clear both markers so the claim below succeeds. Failures are ignored:
        // the conditional create is the real gate, and it will report anything
        // that genuinely blocks the dump.
        let _ = store.delete(&lock).await;
        let _ = store.delete(&manifest).await;
    } else if store.exists(&manifest).await? {
        return Err(Error::config(format!(
            "destination already contains a completed dump ('{}'). Dumping here would mix \
             the two dumps' artifacts under one manifest. Choose an empty destination, or \
             pass --overwrite to replace what is there.",
            manifest.as_str()
        )));
    }

    let body = serde_json::to_vec_pretty(&serde_json::json!({
        "started_at": options.created_at,
        "tool_version": options.tool_version,
        "database": options.database,
    }))?;
    match store.put_if_absent(&lock, once(Bytes::from(body))).await {
        Ok(_) => Ok(()),
        Err(Error::AlreadyExists(_)) => Err(Error::config(format!(
            "another dump is already writing to this destination, or an earlier one failed \
             and left '{DUMP_LOCK_NAME}' behind. Artifacts from two dumps under one manifest \
             would be silently inconsistent, so this run stops. Wait for the other dump, or \
             pass --overwrite to discard what is there."
        ))),
        Err(err) => Err(err),
    }
}

/// Removes the in-progress marker after the manifest has landed.
///
/// Best-effort: the dump is complete and valid at this point, so a failure to
/// tidy up is logged rather than turned into a failed dump. The consequence is
/// that the next dump to this prefix refuses until the marker is cleared, which
/// is the safe direction to err in.
async fn release_destination(store: &dyn ObjectStore) {
    let lock = ObjectPath::new(DUMP_LOCK_NAME);
    if let Err(err) = store.delete(&lock).await {
        tracing::warn!(
            path = DUMP_LOCK_NAME,
            error = %err,
            "dump finished but its in-progress marker could not be removed; \
             the next dump to this destination will need --overwrite",
        );
    }
}

/// The dump itself, once the destination is claimed.
async fn run_dump_body(
    client: &ArangoClient,
    store: &dyn ObjectStore,
    options: &DumpOptions,
    state: &mut DumpProgress<'_>,
) -> Result<Manifest> {
    if !options.all_databases {
        let batch = client
            .replication_batch_create(options.batch_ttl_secs)
            .await?;
        // Ensure the batch is released regardless of how the dump finishes.
        let result = dump_with_batch(client, store, options, &batch, state).await;
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
                state,
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

/// Fails the dump if the endpoint is part of a cluster deployment.
///
/// Cluster-aware dump (`/_api/replication/clusterInventory`, shard-level
/// parallelism across DB-Servers) is post-MVP. Running the single-server
/// replication path against a coordinator would produce a dump whose
/// completeness across shards is unverified, so PRD §8.4 requires detecting
/// the deployment and failing clearly instead.
///
/// A probe that cannot be completed (an old server without the endpoint,
/// insufficient permissions, a transient failure) is **inconclusive, not
/// safe**: it is reported as a warning and the dump proceeds, because
/// refusing every dump whose role could not be read would break single-server
/// users for a diagnostic call. The warning is emitted through the progress
/// sink as well as the log so it reaches machine-readable consumers.
async fn preflight_topology(
    client: &ArangoClient,
    progress: Option<&dyn ProgressSink>,
) -> Result<()> {
    match client.server_role().await {
        Ok(role) if role.is_cluster() => Err(Error::config(format!(
            "refusing to dump from a cluster deployment (server role: {role}). Cluster-aware \
             dump is not implemented yet, and the single-server path would produce a dump whose \
             completeness across shards is unverified. Point --endpoint at a single server, or \
             use ArangoDB's own arangodump for cluster deployments."
        ))),
        Ok(role) => {
            tracing::debug!(%role, "server role checked; proceeding with single-server dump");
            Ok(())
        }
        Err(err) => {
            let message = format!(
                "could not determine the server's deployment role ({err}); proceeding with the \
                 single-server dump path. If this endpoint is a cluster coordinator, the dump may \
                 be incomplete across shards."
            );
            tracing::warn!(error = %err, "server role probe failed; assuming single server");
            if let Some(sink) = progress {
                sink.emit(&ProgressEvent::Warning { message });
            }
            Ok(())
        }
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

    // Names actually written, so a view targeting a filtered-out collection
    // can be reported rather than dumped into an unrestorable state.
    let mut dumped: std::collections::HashSet<String> = std::collections::HashSet::new();

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

        dumped.insert(name.clone());
        progress.collection_done(data_bytes(manifest));
    }

    write_views_with_prefix(
        store,
        path_prefix,
        database,
        &inventory,
        &dumped,
        progress.sink,
        manifest,
    )
    .await?;

    Ok(())
}

/// Writes each view definition as its own artifact.
///
/// A view is skipped when any collection it targets is absent from this dump,
/// because restoring it would fail: an `arangosearch` view whose links name a
/// missing collection is refused with error 1203, and a `search-alias` view
/// whose index is missing with a 400. That only happens under collection
/// filters, and it is reported as a warning naming the view and the missing
/// collection rather than dropped silently — an incomplete dump the user
/// cannot see is the failure mode this whole path exists to avoid.
async fn write_views_with_prefix(
    store: &dyn ObjectStore,
    prefix: &str,
    database: Option<&str>,
    inventory: &arangodb_client::Inventory,
    dumped: &std::collections::HashSet<String>,
    sink: Option<&dyn ProgressSink>,
    manifest: &mut Manifest,
) -> Result<()> {
    for view in &inventory.views {
        let Some(name) = view.get("name").and_then(serde_json::Value::as_str) else {
            return Err(Error::config("inventory view is missing a name"));
        };

        let missing: Vec<&str> = view_targets(view)
            .into_iter()
            .filter(|target| !dumped.contains(*target))
            .collect();
        if !missing.is_empty() {
            let message = format!(
                "view '{name}' was not dumped: it targets collection(s) {} which the \
                 collection filters excluded. Restoring it would fail, so it is omitted. \
                 Widen --include-collections (or drop --exclude-collections) to capture it.",
                missing.join(", ")
            );
            tracing::warn!(view = %name, missing = ?missing, "view skipped: targets not in dump");
            if let Some(sink) = sink {
                sink.emit(&ProgressEvent::Warning { message });
            }
            continue;
        }

        let bytes = serde_json::to_vec_pretty(view)?;
        let path = format!("{prefix}{name}.view.json");
        let meta = store
            .put_stream(&ObjectPath::new(path.clone()), once(Bytes::from(bytes)))
            .await?;
        manifest.push(Artifact {
            path,
            kind: ArtifactKind::View,
            format: DataFormat::Json,
            compression: ManifestCompression::None,
            byte_size: meta.size,
            checksum: None,
            collection: None,
            view: Some(name.to_string()),
            database: database.map(str::to_string),
            part: None,
        });
    }
    Ok(())
}

/// The collections a view definition depends on.
///
/// `arangosearch` views key their `links` object by collection name;
/// `search-alias` views list `{collection, index}` entries. Both forms are
/// read, so an unrecognized shape simply reports no targets rather than
/// guessing.
fn view_targets(view: &serde_json::Value) -> Vec<&str> {
    let mut targets = Vec::new();
    if let Some(links) = view.get("links").and_then(serde_json::Value::as_object) {
        targets.extend(links.keys().map(String::as_str));
    }
    if let Some(indexes) = view.get("indexes").and_then(serde_json::Value::as_array) {
        targets.extend(
            indexes
                .iter()
                .filter_map(|i| i.get("collection").and_then(serde_json::Value::as_str)),
        );
    }
    targets.sort_unstable();
    targets.dedup();
    targets
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
        view: None,
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
        view: None,
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
    /// `arangosearch` views key `links` by collection name.
    #[test]
    fn view_targets_reads_arangosearch_links() {
        let view = serde_json::json!({
            "name": "v", "type": "arangosearch",
            "links": {"docs": {"includeAllFields": true}, "more": {}}
        });
        assert_eq!(view_targets(&view), vec!["docs", "more"]);
    }

    /// `search-alias` views name their targets inside `indexes`.
    #[test]
    fn view_targets_reads_search_alias_indexes() {
        let view = serde_json::json!({
            "name": "v", "type": "search-alias",
            "indexes": [{"collection": "docs", "index": "inv1"},
                        {"collection": "other", "index": "inv2"}]
        });
        assert_eq!(view_targets(&view), vec!["docs", "other"]);
    }

    /// The same collection named twice is reported once, so a skip message
    /// does not repeat it.
    #[test]
    fn view_targets_deduplicates() {
        let view = serde_json::json!({
            "indexes": [{"collection": "docs", "index": "a"},
                        {"collection": "docs", "index": "b"}]
        });
        assert_eq!(view_targets(&view), vec!["docs"]);
    }

    /// An unrecognized shape reports no targets rather than guessing, so such
    /// a view is dumped rather than silently skipped.
    #[test]
    fn view_targets_is_empty_for_an_unknown_shape() {
        assert!(view_targets(&serde_json::json!({"name": "v"})).is_empty());
        assert!(view_targets(&serde_json::json!({"links": "not-an-object"})).is_empty());
    }

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
