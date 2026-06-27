//! The `arangox import` subcommand.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use arangodb_client::{CollectionKind, ImportOptions, OnDuplicate};
use arangodb_import::{
    decompress, read_documents, run_import, run_import_with_checkpoint, validate_edge_documents,
    ArangoBatchSender, BatchSender, CheckpointConfig, ImportFormat,
};
use arangodb_storage::{LocalFileSystem, ObjectPath, ObjectStore, ObjectStoreBackend, StorageUri};
use arangodb_tools_core::config::{BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::progress::ProgressSnapshot;
use arangodb_tools_core::{Error, Result};
use clap::{Args, ValueEnum};
use futures::StreamExt;
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use super::connection::ConnectionArgs;
use super::CompressionArg;
use crate::output::Reporter;

/// Arguments for `arangox import`.
#[derive(Debug, Args)]
pub(crate) struct ImportArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Target collection name.
    #[arg(long)]
    pub collection: String,

    /// Input: a file path, `-` for standard input, a `file://` URI, or
    /// `s3://bucket/key` (AWS_* env for credentials/region/endpoint).
    #[arg(long)]
    pub input: String,

    /// Input format. Inferred from the file extension when omitted; required
    /// when reading from standard input.
    #[arg(long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Input compression. `auto` detects gzip/zstd from the file extension
    /// (and assumes none for stdin).
    #[arg(long, value_enum, default_value_t = CompressionArg::Auto)]
    pub compression: CompressionArg,

    /// Create the collection if it does not exist.
    #[arg(long)]
    pub create_collection: bool,

    /// Treat the collection as an edge collection (implies edge semantics when
    /// creating it).
    #[arg(long)]
    pub edge: bool,

    /// How to handle documents whose `_key` already exists.
    #[arg(long, value_enum, default_value_t = DuplicateMode::Error)]
    pub on_duplicate: DuplicateMode,

    /// Truncate the collection before importing (non-atomic; see PRD §8.2).
    #[arg(long)]
    pub overwrite: bool,

    /// Prefix applied to unqualified `_from` values (edge imports).
    #[arg(long, value_name = "COLLECTION")]
    pub from_collection_prefix: Option<String>,

    /// Prefix applied to unqualified `_to` values (edge imports).
    #[arg(long, value_name = "COLLECTION")]
    pub to_collection_prefix: Option<String>,

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

    /// Enable resumable import: read and update a rolling checkpoint at this
    /// location (a local path or `s3://bucket/key`). Re-running with the same
    /// checkpoint skips batches already committed by the previous run.
    #[arg(long, value_name = "URI")]
    pub checkpoint: Option<String>,
}

/// Duplicate-handling mode, mirrored for clap value parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DuplicateMode {
    /// Report an error and count the document as failed.
    Error,
    /// Patch the existing document.
    Update,
    /// Replace the existing document.
    Replace,
    /// Silently skip the document.
    Ignore,
}

impl From<DuplicateMode> for OnDuplicate {
    fn from(mode: DuplicateMode) -> Self {
        match mode {
            DuplicateMode::Error => OnDuplicate::Error,
            DuplicateMode::Update => OnDuplicate::Update,
            DuplicateMode::Replace => OnDuplicate::Replace,
            DuplicateMode::Ignore => OnDuplicate::Ignore,
        }
    }
}

/// Runs an import job.
pub(crate) async fn run(args: ImportArgs, reporter: Reporter) -> Result<()> {
    let format = resolve_format(args.format.as_deref(), &args.input)?;
    let kind = if args.edge {
        CollectionKind::Edge
    } else {
        CollectionKind::Document
    };

    let client = args.connection.build_client()?;

    if args.create_collection {
        client.ensure_collection(&args.collection, kind).await?;
    }

    let mut options = ImportOptions::new(&args.collection);
    options.on_duplicate = args.on_duplicate.into();
    options.overwrite = args.overwrite;
    options.from_prefix = args.from_collection_prefix.clone();
    options.to_prefix = args.to_collection_prefix.clone();

    let batch = BatchConfig {
        max_bytes: args.batch_size_bytes,
        max_docs: args.max_docs,
    };
    let concurrency = ConcurrencyConfig {
        workers: args
            .threads
            .unwrap_or_else(arangodb_tools_core::config::default_workers),
        max_in_flight_bytes: args.max_in_flight_bytes,
    };

    let compression = args.compression.resolve(&args.input);
    let raw = open_input(&args.input).await?;
    let mut documents = read_documents(format, decompress(compression, raw));
    if args.edge {
        // Catch malformed edges before sending, rather than relying on
        // per-document server rejection. Bare keys are allowed for an endpoint
        // only when its prefix will qualify them.
        documents = validate_edge_documents(
            documents,
            args.from_collection_prefix.is_some(),
            args.to_collection_prefix.is_some(),
        );
    }
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(client, options));

    reporter.started("import");
    let started = Instant::now();
    let summary = match args.checkpoint.as_deref() {
        Some(uri) => {
            let checkpoint = build_checkpoint(uri)?;
            run_import_with_checkpoint(documents, batch, concurrency, sender, Some(checkpoint))
                .await?
        }
        None => run_import(documents, batch, concurrency, sender).await?,
    };
    let elapsed = started.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();

    let docs_per_sec = if elapsed_secs > 0.0 {
        summary.documents_sent as f64 / elapsed_secs
    } else {
        0.0
    };

    reporter.finished(ProgressSnapshot {
        bytes_read: 0,
        bytes_written: summary.bytes_sent,
        documents: summary.documents_sent,
        batches: summary.batches,
        server_errors: summary.errors,
        retries: 0,
        elapsed_secs,
    });

    let collection = args.collection.clone();
    reporter.result(
        || {
            format!(
                "imported {} document(s) into '{}' in {} batch(es) over {:.2}s ({:.0} docs/s)\n  \
                 created={} errors={} updated={} ignored={} empty={} bytes_sent={}",
                summary.documents_sent,
                collection,
                summary.batches,
                elapsed_secs,
                docs_per_sec,
                summary.created,
                summary.errors,
                summary.updated,
                summary.ignored,
                summary.empty,
                summary.bytes_sent,
            )
        },
        || {
            serde_json::json!({
                "operation": "import",
                "status": if summary.errors > 0 { "completed_with_errors" } else { "ok" },
                "collection": args.collection,
                "documents_sent": summary.documents_sent,
                "batches": summary.batches,
                "created": summary.created,
                "errors": summary.errors,
                "updated": summary.updated,
                "ignored": summary.ignored,
                "empty": summary.empty,
                "bytes_sent": summary.bytes_sent,
                "elapsed_secs": elapsed_secs,
                "docs_per_sec": docs_per_sec,
            })
        },
    );

    if summary.errors > 0 {
        return Err(Error::config(format!(
            "{} document(s) were rejected by the server",
            summary.errors
        )));
    }
    Ok(())
}

/// Resolves the import format from an explicit `--format` or the input path.
fn resolve_format(explicit: Option<&str>, input: &str) -> Result<ImportFormat> {
    if let Some(name) = explicit {
        return ImportFormat::from_extension(&name.to_ascii_lowercase()).ok_or_else(|| {
            Error::config(format!(
                "unknown import format '{name}'; expected one of jsonl, ndjson, json, csv, tsv"
            ))
        });
    }
    if input == "-" {
        return Err(Error::config(
            "reading from stdin requires an explicit --format",
        ));
    }
    ImportFormat::infer_from_path(input)
}

/// Builds a [`CheckpointConfig`] from a checkpoint location.
///
/// Accepts a local filesystem path, a `file://` URI, or an `s3://bucket/key`
/// URI. The checkpoint is a single object that is overwritten as the import
/// makes progress.
fn build_checkpoint(uri: &str) -> Result<CheckpointConfig> {
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
    Ok(CheckpointConfig::new(store, path))
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

/// Opens the import input as an async byte stream.
///
/// Accepts a filesystem path, `-` for standard input, a `file://` URI, or an
/// `s3://bucket/key` URI (credentials/region/endpoint from the `AWS_*`
/// environment; works against S3 and MinIO/LocalStack). Other object-storage
/// schemes (`gs://`, `az://`) are not supported yet.
async fn open_input(input: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    if input == "-" {
        return Ok(Box::new(tokio::io::stdin()));
    }

    if let Some((scheme, _)) = input.split_once("://") {
        return match scheme {
            "file" => open_file(Path::new(input.trim_start_matches("file://"))).await,
            "s3" => open_s3(input).await,
            other => Err(Error::config(format!(
                "object-storage scheme '{other}://' is not supported yet; \
                 use s3://, a local path, or '-' for stdin"
            ))),
        };
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

/// Opens an `s3://bucket/key` object as a byte stream via the storage backend.
async fn open_s3(uri: &str) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let parsed = StorageUri::parse(uri)?;
    let bucket = parsed
        .bucket
        .ok_or_else(|| Error::config(format!("s3 URI is missing a bucket: {uri}")))?;
    let backend = ObjectStoreBackend::s3(&bucket, None)?;
    let stream = backend
        .get_stream(&ObjectPath::new(parsed.path), None)
        .await?;
    // Adapt the byte stream into an AsyncRead for the format readers.
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
        let fmt = resolve_format(Some("csv"), "data.jsonl").unwrap();
        assert_eq!(fmt, ImportFormat::Csv);
    }

    #[test]
    fn infers_format_from_path() {
        assert_eq!(
            resolve_format(None, "users.jsonl").unwrap(),
            ImportFormat::JsonLines
        );
    }

    #[test]
    fn stdin_requires_explicit_format() {
        assert!(resolve_format(None, "-").is_err());
    }

    #[test]
    fn rejects_unknown_format() {
        assert!(resolve_format(Some("parquet"), "x").is_err());
    }

    #[tokio::test]
    async fn rejects_unsupported_object_scheme() {
        // gs:// is not wired yet; s3:// is handled elsewhere (needs a server).
        // `Box<dyn AsyncRead>` is not Debug, so match rather than unwrap_err.
        assert!(matches!(
            open_input("gs://bucket/users.jsonl").await,
            Err(Error::Config(_))
        ));
    }

    #[tokio::test]
    async fn missing_file_is_a_config_error() {
        assert!(matches!(
            open_input("/no/such/file.jsonl").await,
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn builds_local_checkpoint_with_file_name_object() {
        let cfg = build_checkpoint("/tmp/imports/users.checkpoint.json").unwrap();
        assert_eq!(cfg.path.as_str(), "users.checkpoint.json");
    }

    #[test]
    fn builds_relative_local_checkpoint() {
        let cfg = build_checkpoint("users.checkpoint.json").unwrap();
        assert_eq!(cfg.path.as_str(), "users.checkpoint.json");
    }

    #[test]
    fn rejects_unsupported_checkpoint_scheme() {
        assert!(matches!(
            build_checkpoint("gs://bucket/cp.json"),
            Err(Error::Config(_))
        ));
    }
}
