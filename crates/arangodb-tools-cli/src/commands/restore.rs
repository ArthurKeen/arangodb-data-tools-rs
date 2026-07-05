//! The `arangox restore` subcommand.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arangodb_restore::{run_restore_with_progress, RestoreCheckpointConfig, RestoreOptions};
use arangodb_storage::{LocalFileSystem, ObjectPath, ObjectStore, ObjectStoreBackend, StorageUri};
use arangodb_tools_core::progress::ProgressSnapshot;
use arangodb_tools_core::{Error, Result};
use clap::Args;

use super::connection::ConnectionArgs;
use super::open_store_root;
use crate::output::Reporter;

/// Arguments for `arangox restore`.
#[derive(Debug, Args)]
pub(crate) struct RestoreArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Source dump root: a directory, `file://` URI, or `s3://bucket/prefix`.
    #[arg(long)]
    pub input: String,

    /// Create the target database (in `_system`) before restoring. Ignored for
    /// multi-database dumps, which create each database from the manifest.
    #[arg(long)]
    pub create_database: bool,

    /// Replace existing collections of the same name.
    #[arg(long)]
    pub overwrite: bool,

    /// Enable resumable restore: read and update a checkpoint at this location
    /// (a local path or `s3://bucket/key`). Re-running with the same checkpoint
    /// skips collections already restored by the previous run.
    #[arg(long, value_name = "URI")]
    pub checkpoint: Option<String>,
}

/// Runs a restore job.
pub(crate) async fn run(args: RestoreArgs, reporter: Reporter) -> Result<()> {
    let client = args.connection.build_client()?;
    let store = open_store_root(&args.input)?;

    let checkpoint = match args.checkpoint.as_deref() {
        Some(uri) => Some(build_restore_checkpoint(uri)?),
        None => None,
    };

    let options = RestoreOptions {
        overwrite: args.overwrite,
        create_database: args
            .create_database
            .then(|| args.connection.database.clone()),
        checkpoint,
    };

    reporter.started("restore");
    let started = Instant::now();
    let summary =
        run_restore_with_progress(&client, store.as_ref(), &options, reporter.progress_sink())
            .await?;

    reporter.finished(ProgressSnapshot {
        batches: summary.collections as u64,
        elapsed_secs: started.elapsed().as_secs_f64(),
        ..ProgressSnapshot::default()
    });
    reporter.result(
        || {
            format!(
                "restored {} of {} collection(s) into '{}' ({} skipped from checkpoint)",
                summary.restored, summary.collections, args.connection.database, summary.skipped
            )
        },
        || {
            serde_json::json!({
                "operation": "restore",
                "status": "ok",
                "database": args.connection.database,
                "collections": summary.collections,
                "restored": summary.restored,
                "skipped": summary.skipped,
            })
        },
    );
    Ok(())
}

/// Builds a [`RestoreCheckpointConfig`] from a checkpoint location (a local
/// path, a `file://` URI, or `s3://bucket/key`). Mirrors the import command's
/// checkpoint handling.
fn build_restore_checkpoint(uri: &str) -> Result<RestoreCheckpointConfig> {
    let (store, path): (Arc<dyn ObjectStore>, ObjectPath) = match uri.split_once("://") {
        Some(("s3", _)) => {
            let parsed = StorageUri::parse(uri)?;
            let bucket = parsed.bucket.ok_or_else(|| {
                Error::config(format!("s3 checkpoint URI is missing a bucket: {uri}"))
            })?;
            let backend = ObjectStoreBackend::s3(&bucket, None)?;
            (Arc::new(backend), ObjectPath::new(parsed.path))
        }
        Some(("file", _)) => local_checkpoint(Path::new(uri.trim_start_matches("file://")))?,
        Some((other, _)) => {
            return Err(Error::config(format!(
                "checkpoint scheme '{other}://' is not supported; use a local path or s3://"
            )));
        }
        None => local_checkpoint(Path::new(uri))?,
    };
    Ok(RestoreCheckpointConfig::new(store, path))
}

/// Resolves a local checkpoint path into a filesystem-backed store (rooted at
/// the path's parent) and the file name as its object path.
fn local_checkpoint(path: &Path) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Error::config(format!(
                "checkpoint path has no file name: {}",
                path.display()
            ))
        })?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => Path::new(".").to_path_buf(),
    };
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new(parent));
    Ok((store, ObjectPath::new(file_name.to_owned())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_local_checkpoint_with_file_name_object() {
        let cfg = build_restore_checkpoint("/tmp/restores/restore.progress.json").unwrap();
        assert_eq!(cfg.path.as_str(), "restore.progress.json");
    }

    #[test]
    fn rejects_unsupported_checkpoint_scheme() {
        assert!(matches!(
            build_restore_checkpoint("gs://bucket/cp.json"),
            Err(Error::Config(_))
        ));
    }
}
