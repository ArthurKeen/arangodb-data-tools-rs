//! The `arangox dump` subcommand.

use std::time::Instant;

use arangodb_dump::{run_dump_with_progress, DumpOptions, FilterOptions};
use arangodb_tools_core::progress::ProgressSnapshot;
use arangodb_tools_core::Result;
use clap::Args;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::connection::ConnectionArgs;
use super::{open_store_root, CompressionArg};
use crate::output::Reporter;

/// Arguments for `arangox dump`.
#[derive(Debug, Args)]
pub(crate) struct DumpArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Destination dump root: a directory, `file://` URI, or `s3://bucket/prefix`.
    #[arg(long)]
    pub output: String,

    /// Compression for data artifacts.
    #[arg(long, value_enum, default_value_t = CompressionArg::None)]
    pub compression: CompressionArg,

    /// Include system collections (names starting with `_`).
    #[arg(long)]
    pub include_system: bool,

    /// Dump every accessible database. Artifacts are written under
    /// `databases/{name}/...` and described by a single combined manifest.
    #[arg(long)]
    pub all_databases: bool,

    /// Only dump collections whose name matches this regular expression.
    #[arg(long, value_name = "REGEX")]
    pub include_collections: Option<String>,

    /// Skip collections whose name matches this regular expression (applied
    /// after `--include-collections`).
    #[arg(long, value_name = "REGEX")]
    pub exclude_collections: Option<String>,

    /// Replication-batch TTL in seconds (the snapshot keep-alive interval).
    #[arg(long, default_value_t = 600)]
    pub batch_ttl_secs: u32,
}

/// Runs a dump job.
pub(crate) async fn run(args: DumpArgs, reporter: Reporter) -> Result<()> {
    let client = args.connection.build_client()?;
    let store = open_store_root(&args.output)?;

    let filters = FilterOptions::new(
        args.include_collections.as_deref(),
        args.exclude_collections.as_deref(),
    )?;

    let options = DumpOptions {
        include_system: args.include_system,
        all_databases: args.all_databases,
        filters,
        // `--compression` here is an explicit codec, never auto (no extension
        // to sniff for a dump root).
        compression: args.compression.resolve(""),
        batch_ttl_secs: args.batch_ttl_secs,
        database: args.connection.database.clone(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_default(),
        ..DumpOptions::default()
    };

    reporter.started("dump");
    let started = Instant::now();
    let manifest =
        run_dump_with_progress(&client, store.as_ref(), &options, reporter.progress_sink()).await?;
    let collections = manifest
        .artifacts
        .iter()
        .filter(|a| a.kind == arangodb_tools_core::manifest::ArtifactKind::Data)
        .count();
    let artifacts = manifest.artifacts.len();

    reporter.finished(ProgressSnapshot {
        batches: collections as u64,
        elapsed_secs: started.elapsed().as_secs_f64(),
        ..ProgressSnapshot::default()
    });
    reporter.result(
        || {
            format!(
                "dumped {collections} collection(s) to '{}' ({artifacts} artifact(s) + manifest)",
                args.output
            )
        },
        || {
            serde_json::json!({
                "operation": "dump",
                "status": "ok",
                "output": args.output,
                "collections": collections,
                "artifacts": artifacts,
            })
        },
    );
    Ok(())
}
