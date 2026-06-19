//! The `arangox restore` subcommand.

use arangodb_restore::{run_restore, RestoreOptions};
use arangodb_tools_core::Result;
use clap::Args;

use super::connection::ConnectionArgs;
use super::open_store_root;

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
pub(crate) async fn run(args: RestoreArgs) -> Result<()> {
    let client = args.connection.build_client()?;
    let store = open_store_root(&args.input)?;

    let options = RestoreOptions {
        overwrite: args.overwrite,
        create_database: args
            .create_database
            .then(|| args.connection.database.clone()),
    };

    let summary = run_restore(&client, store.as_ref(), &options).await?;
    println!(
        "restored {} collection(s) into '{}'",
        summary.collections, args.connection.database
    );
    Ok(())
}
