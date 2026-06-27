//! Python bindings for `arangodb-data-tools-rs`.
//!
//! This is a **sketch**: it exposes the bulk-import pipeline to Python as a
//! synchronous, GIL-releasing function and demonstrates the pattern for the
//! other tools. `export`/`dump`/`restore` are present as discoverable stubs
//! that currently raise `NotImplementedError`.
//!
//! Design notes:
//! - The Rust library is async; Python callers want a blocking call. Each
//!   invocation builds a multi-threaded Tokio runtime, releases the GIL with
//!   [`Python::allow_threads`] so other Python threads run during I/O, and
//!   `block_on`s the pipeline.
//! - Errors from the core [`Error`] taxonomy are surfaced as Python
//!   exceptions. A richer mapping (per-variant exception types) is a natural
//!   follow-up.
//! - The compiled module is imported from Python as `arangox`.

use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use arangodb_client::{ArangoClient, CollectionKind};
use arangodb_import::{
    decompress, read_documents, run_import, ArangoBatchSender, BatchSender, Compression,
    ImportFormat, ImportOptions, ImportSummary, OnDuplicate,
};
use arangodb_tools_core::config::{default_workers, BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::{Error, Result};

/// Bulk-import a local file into a collection.
///
/// Mirrors `arangox import` for local-file input. Returns a dict with the
/// server-reported import summary.
///
/// Required: `collection`, `input` (a local path; format inferred from the
/// extension unless `format` is given). All other arguments are keyword-only.
#[pyfunction]
#[pyo3(signature = (
    collection,
    input,
    *,
    endpoint = None,
    database = None,
    username = None,
    password = None,
    token = None,
    insecure = false,
    request_timeout_secs = 120,
    create_collection = false,
    edge = false,
    on_duplicate = None,
    overwrite = false,
    from_collection_prefix = None,
    to_collection_prefix = None,
    format = None,
    batch_size_bytes = None,
    max_docs = None,
    threads = None,
    max_in_flight_bytes = None,
))]
#[allow(clippy::too_many_arguments)]
fn import_file<'py>(
    py: Python<'py>,
    collection: String,
    input: String,
    endpoint: Option<String>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    insecure: bool,
    request_timeout_secs: u64,
    create_collection: bool,
    edge: bool,
    on_duplicate: Option<String>,
    overwrite: bool,
    from_collection_prefix: Option<String>,
    to_collection_prefix: Option<String>,
    format: Option<String>,
    batch_size_bytes: Option<usize>,
    max_docs: Option<usize>,
    threads: Option<usize>,
    max_in_flight_bytes: Option<usize>,
) -> PyResult<Bound<'py, PyDict>> {
    let params = ImportParams {
        collection,
        input,
        endpoint: endpoint.unwrap_or_else(|| "http://localhost:8529".to_owned()),
        database: database.unwrap_or_else(|| "_system".to_owned()),
        username,
        password,
        token,
        insecure,
        request_timeout_secs,
        create_collection,
        edge,
        on_duplicate: on_duplicate.unwrap_or_else(|| "error".to_owned()),
        overwrite,
        from_collection_prefix,
        to_collection_prefix,
        format,
        batch_size_bytes,
        max_docs,
        threads,
        max_in_flight_bytes,
    };

    // Release the GIL while the (blocking) async pipeline runs.
    let summary = py
        .allow_threads(|| run_blocking_import(params))
        .map_err(to_py_err)?;

    summary_to_dict(py, &summary)
}

/// Raises `NotImplementedError`; a uniform message for the unbound tools.
fn unimplemented(tool: &str) -> PyResult<()> {
    Err(PyNotImplementedError::new_err(format!(
        "the `{tool}` binding is not implemented yet; for now drive the `arangox` \
         CLI with `--output json` (see bindings/python/README.md)"
    )))
}

/// Stub for export (not bound yet).
#[pyfunction]
#[pyo3(signature = (**_kwargs))]
fn export(_kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
    unimplemented("export")
}

/// Stub for dump (not bound yet).
#[pyfunction]
#[pyo3(signature = (**_kwargs))]
fn dump(_kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
    unimplemented("dump")
}

/// Stub for restore (not bound yet).
#[pyfunction]
#[pyo3(signature = (**_kwargs))]
fn restore(_kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
    unimplemented("restore")
}

/// The Python module. Imported as `import arangox`.
#[pymodule]
fn arangox(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(import_file, m)?)?;
    // Discoverable stubs for the planned surface.
    m.add_function(wrap_pyfunction!(export, m)?)?;
    m.add_function(wrap_pyfunction!(dump, m)?)?;
    m.add_function(wrap_pyfunction!(restore, m)?)?;
    Ok(())
}

/// Owned parameters for an import, decoupled from PyO3 so the blocking worker
/// is plain Rust (and `Send`).
struct ImportParams {
    collection: String,
    input: String,
    endpoint: String,
    database: String,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    insecure: bool,
    request_timeout_secs: u64,
    create_collection: bool,
    edge: bool,
    on_duplicate: String,
    overwrite: bool,
    from_collection_prefix: Option<String>,
    to_collection_prefix: Option<String>,
    format: Option<String>,
    batch_size_bytes: Option<usize>,
    max_docs: Option<usize>,
    threads: Option<usize>,
    max_in_flight_bytes: Option<usize>,
}

/// Builds a Tokio runtime and drives the async import to completion.
fn run_blocking_import(params: ImportParams) -> Result<ImportSummary> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::config(format!("failed to start async runtime: {err}")))?;
    runtime.block_on(do_import(params))
}

/// The async import, equivalent to the CLI's `import` command for local files.
async fn do_import(params: ImportParams) -> Result<ImportSummary> {
    let format = resolve_format(params.format.as_deref(), &params.input)?;
    let on_duplicate = parse_on_duplicate(&params.on_duplicate)?;
    let kind = if params.edge {
        CollectionKind::Edge
    } else {
        CollectionKind::Document
    };

    let mut builder = ArangoClient::builder()
        .endpoint(&params.endpoint)
        .database(&params.database)
        .insecure(params.insecure)
        .request_timeout(Duration::from_secs(params.request_timeout_secs));
    if let Some(token) = &params.token {
        builder = builder.bearer_auth(token);
    } else if let Some(username) = &params.username {
        builder = builder.basic_auth(username, params.password.clone().unwrap_or_default());
    }
    let client = builder.build()?;

    if params.create_collection {
        client.ensure_collection(&params.collection, kind).await?;
    }

    let mut options = ImportOptions::new(&params.collection);
    options.on_duplicate = on_duplicate;
    options.overwrite = params.overwrite;
    options.from_prefix = params.from_collection_prefix.clone();
    options.to_prefix = params.to_collection_prefix.clone();

    let batch = BatchConfig {
        max_bytes: params.batch_size_bytes.unwrap_or(BatchConfig::default().max_bytes),
        max_docs: params.max_docs.unwrap_or(BatchConfig::default().max_docs),
    };
    let concurrency = ConcurrencyConfig {
        workers: params.threads.unwrap_or_else(default_workers),
        max_in_flight_bytes: params
            .max_in_flight_bytes
            .unwrap_or(ConcurrencyConfig::default().max_in_flight_bytes),
    };

    let compression = Compression::infer_from_path(&params.input);
    let file = tokio::fs::File::open(&params.input).await.map_err(|err| {
        Error::config(format!("cannot open input '{}': {err}", params.input))
    })?;
    let documents = read_documents(format, decompress(compression, file));
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(client, options));

    run_import(documents, batch, concurrency, sender).await
}

/// Resolves the import format from an explicit name or the input path.
fn resolve_format(explicit: Option<&str>, input: &str) -> Result<ImportFormat> {
    match explicit {
        Some(name) => ImportFormat::from_extension(&name.to_ascii_lowercase()).ok_or_else(|| {
            Error::config(format!(
                "unknown import format '{name}'; expected one of jsonl, ndjson, json, csv, tsv"
            ))
        }),
        None => ImportFormat::infer_from_path(input),
    }
}

/// Parses the duplicate-handling mode.
fn parse_on_duplicate(mode: &str) -> Result<OnDuplicate> {
    match mode.to_ascii_lowercase().as_str() {
        "error" => Ok(OnDuplicate::Error),
        "update" => Ok(OnDuplicate::Update),
        "replace" => Ok(OnDuplicate::Replace),
        "ignore" => Ok(OnDuplicate::Ignore),
        other => Err(Error::config(format!(
            "unknown on_duplicate '{other}'; expected error, update, replace, or ignore"
        ))),
    }
}

/// Builds the result dict returned to Python.
fn summary_to_dict<'py>(py: Python<'py>, summary: &ImportSummary) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("documents_sent", summary.documents_sent)?;
    dict.set_item("batches", summary.batches)?;
    dict.set_item("created", summary.created)?;
    dict.set_item("errors", summary.errors)?;
    dict.set_item("updated", summary.updated)?;
    dict.set_item("ignored", summary.ignored)?;
    dict.set_item("empty", summary.empty)?;
    dict.set_item("bytes_sent", summary.bytes_sent)?;
    Ok(dict)
}

/// Maps a core [`Error`] into a Python exception.
fn to_py_err(err: Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
