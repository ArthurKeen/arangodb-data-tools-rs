//! `arangox` command-line entry point.
//!
//! Subcommands map CLI options to the library job crates. `import` is wired;
//! `export`, `dump`, `restore`, and `rdf` land in later phases. See
//! `docs/IMPLEMENTATION_PLAN.md`.

mod commands;

use clap::{Parser, Subcommand};

/// ArangoDB data tools.
#[derive(Debug, Parser)]
#[command(name = "arangox", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bulk-import CSV, TSV, JSON, or JSONL data into a collection.
    Import(commands::import::ImportArgs),
    /// Export a collection or AQL query to JSONL, JSON, or CSV.
    Export(commands::export::ExportArgs),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Import(args) => commands::import::run(args).await,
        Command::Export(args) => commands::export::run(args).await,
    };

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            // The library never writes to stdout; the CLI owns presentation.
            eprintln!("error: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}
