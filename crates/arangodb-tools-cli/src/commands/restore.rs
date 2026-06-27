//! The `arangox restore` subcommand.

use std::time::Instant;

use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_tools_core::progress::ProgressSnapshot;
use arangodb_tools_core::Result;
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

    /// Create the target database (in `_system`) before restoring.
    #[arg(long)]
    pub create_database: bool,

    /// Replace existing collections of the same name.
    #[arg(long)]
    pub overwrite: bool,
}

/// Runs a restore job.
pub(crate) async fn run(args: RestoreArgs, reporter: Reporter) -> Result<()> {
    let client = args.connection.build_client()?;
    let store = open_store_root(&args.input)?;

    let options = RestoreOptions {
        overwrite: args.overwrite,
        create_database: args
            .create_database
            .then(|| args.connection.database.clone()),
    };

    reporter.started("restore");
    let started = Instant::now();
    let summary = run_restore(&client, store.as_ref(), &options).await?;

    reporter.finished(ProgressSnapshot {
        batches: summary.collections as u64,
        elapsed_secs: started.elapsed().as_secs_f64(),
        ..ProgressSnapshot::default()
    });
    reporter.result(
        || {
            format!(
                "restored {} collection(s) into '{}'",
                summary.collections, args.connection.database
            )
        },
        || {
            serde_json::json!({
                "operation": "restore",
                "status": "ok",
                "database": args.connection.database,
                "collections": summary.collections,
            })
        },
    );
    Ok(())
}
