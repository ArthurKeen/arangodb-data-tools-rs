//! `arangox` command-line entry point.
//!
//! Subcommands map CLI options to the library job crates: `import`, `export`,
//! `dump`, and `restore` are wired. RDF bulk import lands in a later phase. See
//! `docs/IMPLEMENTATION_PLAN.md`.

mod commands;
mod output;

use clap::{Parser, Subcommand};
use output::{OutputMode, Reporter};

/// ArangoDB data tools.
#[derive(Debug, Parser)]
#[command(name = "arangox", version, about, long_about = None)]
struct Cli {
    /// Output mode. `text` (default) prints human-readable summaries; `json`
    /// prints a machine-readable result object on stdout and newline-delimited
    /// progress events on stderr (intended for programmatic callers).
    #[arg(long, global = true, value_enum, default_value_t = OutputMode::Text)]
    output: OutputMode,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bulk-import CSV, TSV, JSON, or JSONL data into a collection.
    Import(commands::import::ImportArgs),
    /// Export a collection or AQL query to JSONL, JSON, or CSV.
    Export(commands::export::ExportArgs),
    /// Dump a database to a directory or object-store prefix.
    Dump(commands::dump::DumpArgs),
    /// Restore a database from a dump.
    Restore(commands::restore::RestoreArgs),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let reporter = Reporter::new(cli.output);
    let result = match cli.command {
        Command::Import(args) => commands::import::run(args, reporter).await,
        Command::Export(args) => commands::export::run(args, reporter).await,
        Command::Dump(args) => commands::dump::run(args, reporter).await,
        Command::Restore(args) => commands::restore::run(args, reporter).await,
    };

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            // The library never writes to stdout; the CLI owns presentation.
            if reporter.is_json() {
                let value = serde_json::json!({ "status": "error", "message": err.to_string() });
                eprintln!("{value}");
            } else {
                eprintln!("error: {err}");
            }
            std::process::ExitCode::FAILURE
        }
    }
}
