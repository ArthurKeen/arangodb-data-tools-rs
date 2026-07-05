//! The `arangox export` subcommand.

use std::time::Instant;

use arangodb_client::CursorRequest;
use arangodb_export::{
    collection_query, document_stream, run_export_with_progress, run_split_export_with_progress,
    ExportFormat, ManifestMeta,
};
use arangodb_storage::{ObjectPath, ObjectStore};
use arangodb_tools_core::progress::ProgressSnapshot;
use arangodb_tools_core::{Error, Result};
use clap::Args;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::connection::ConnectionArgs;
use super::CompressionArg;
use crate::output::Reporter;

/// Arguments for `arangox export`.
#[derive(Debug, Args)]
pub(crate) struct ExportArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Collection to export (mutually exclusive with --query).
    #[arg(long, conflicts_with = "query")]
    pub collection: Option<String>,

    /// Custom AQL query to export (mutually exclusive with --collection).
    #[arg(long, conflicts_with = "collection")]
    pub query: Option<String>,

    /// Bind variables for --query, as a JSON object.
    #[arg(long, value_name = "JSON")]
    pub bind_vars: Option<String>,

    /// Output destination: a file path, `file://` URI, or `s3://bucket/key`.
    #[arg(long)]
    pub output: String,

    /// Output format: jsonl (default), json, or csv.
    #[arg(long, default_value = "jsonl")]
    pub format: String,

    /// Fields to project, comma-separated. Required for CSV.
    #[arg(long, value_delimiter = ',')]
    pub fields: Vec<String>,

    /// Output compression. `auto` detects gzip/zstd from the output extension.
    #[arg(long, value_enum, default_value_t = CompressionArg::Auto)]
    pub compression: CompressionArg,

    /// Cursor batch size.
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: u32,

    /// Split the export into parts of at most this many bytes (uncompressed)
    /// and write a manifest enumerating them. Works for jsonl, json, and csv;
    /// each part is a standalone valid document in the chosen format.
    #[arg(long, value_name = "BYTES")]
    pub split_bytes: Option<u64>,
}

/// Runs an export job.
pub(crate) async fn run(args: ExportArgs, reporter: Reporter) -> Result<()> {
    let format = ExportFormat::parse(&args.format)?;
    let request = build_request(
        args.collection.as_deref(),
        args.query.as_deref(),
        args.bind_vars.as_deref(),
        args.batch_size,
    )?;
    let fields = if args.fields.is_empty() {
        None
    } else {
        Some(args.fields.clone())
    };
    let compression = args.compression.resolve(&args.output);

    let client = args.connection.build_client()?;
    let (store, path) = open_output(&args.output)?;

    reporter.started("export");
    let started = Instant::now();

    if let Some(max_part_bytes) = args.split_bytes {
        let meta = ManifestMeta {
            database: args.connection.database.clone(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_default(),
            source: args.collection.clone().or_else(|| args.query.clone()),
        };
        let documents = document_stream(client, request);
        let manifest = run_split_export_with_progress(
            documents,
            format,
            fields,
            compression,
            store.as_ref(),
            path.as_str(),
            max_part_bytes,
            meta,
            reporter.progress_sink(),
        )
        .await?;
        let parts = manifest.artifacts.len();
        let manifest_name = format!("{}.manifest.json", args.output);

        reporter.finished(ProgressSnapshot {
            batches: parts as u64,
            elapsed_secs: started.elapsed().as_secs_f64(),
            ..ProgressSnapshot::default()
        });
        reporter.result(
            || {
                format!(
                    "exported {} part(s) under '{}' + manifest '{}'",
                    parts, args.output, manifest_name
                )
            },
            || {
                serde_json::json!({
                    "operation": "export",
                    "status": "ok",
                    "mode": "split",
                    "output": args.output,
                    "format": format.extension(),
                    "parts": parts,
                    "manifest": manifest_name,
                })
            },
        );
        return Ok(());
    }

    let meta = run_export_with_progress(
        &client,
        request,
        format,
        fields,
        compression,
        store.as_ref(),
        &path,
        reporter.progress_sink(),
    )
    .await?;

    reporter.finished(ProgressSnapshot {
        bytes_written: meta.size,
        elapsed_secs: started.elapsed().as_secs_f64(),
        ..ProgressSnapshot::default()
    });
    reporter.result(
        || {
            format!(
                "exported to '{}' ({} bytes written)",
                args.output, meta.size
            )
        },
        || {
            serde_json::json!({
                "operation": "export",
                "status": "ok",
                "mode": "single",
                "output": args.output,
                "format": format.extension(),
                "bytes_written": meta.size,
            })
        },
    );
    Ok(())
}

/// Builds the cursor request from `--collection` or `--query`.
fn build_request(
    collection: Option<&str>,
    query: Option<&str>,
    bind_vars: Option<&str>,
    batch_size: u32,
) -> Result<CursorRequest> {
    match (collection, query) {
        (Some(collection), None) => Ok(collection_query(collection, batch_size)),
        (None, Some(query)) => {
            let mut request = CursorRequest::new(query).with_batch_size(batch_size);
            if let Some(bind) = bind_vars {
                let value = serde_json::from_str(bind)
                    .map_err(|err| Error::config(format!("invalid --bind-vars JSON: {err}")))?;
                request = request.with_bind_vars(value);
            }
            Ok(request)
        }
        (None, None) => Err(Error::config("one of --collection or --query is required")),
        (Some(_), Some(_)) => Err(Error::config(
            "--collection and --query are mutually exclusive",
        )),
    }
}

/// Resolves an output destination into a store and an object path.
///
/// Accepts a filesystem path, a `file://` URI, or an object-storage URI
/// (`s3://`, `gs://`, `az://`, `seaweed+s3://`).
fn open_output(output: &str) -> Result<(Box<dyn ObjectStore>, ObjectPath)> {
    super::open_object(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_output_scheme() {
        // gs://, az://, s3://, seaweed+s3:// are supported; an unknown scheme is
        // rejected by URI parsing.
        assert!(matches!(
            open_output("ftp://host/out.jsonl"),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn local_output_splits_parent_and_name() {
        let (_store, path) = open_output("/tmp/sub/out.jsonl").unwrap();
        assert_eq!(path.as_str(), "out.jsonl");
    }

    #[test]
    fn requires_exactly_one_source() {
        assert!(build_request(None, None, None, 10_000).is_err());
        assert!(build_request(Some("c"), Some("q"), None, 10_000).is_err());
        assert!(build_request(Some("users"), None, None, 10_000).is_ok());
    }

    #[test]
    fn query_with_bad_bind_vars_errors() {
        assert!(build_request(None, Some("RETURN 1"), Some("{not json"), 10_000).is_err());
    }
}
