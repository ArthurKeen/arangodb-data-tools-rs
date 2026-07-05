//! Python bindings for `arangodb-data-tools-rs`.
//!
//! Exposes the bulk data tools to Python as synchronous, GIL-releasing
//! functions: `import_file`, `export`, `dump`, and `restore`. Each call builds
//! a multi-threaded Tokio runtime, releases the GIL with
//! [`Python::allow_threads`] so other Python threads run during I/O,
//! `block_on`s the async pipeline, and returns a plain `dict`.
//!
//! Design notes:
//! - Inputs/outputs accept a local path, a `file://` URI, or `s3://bucket/key`
//!   (`s3://` uses the `AWS_*` environment, like the CLI). `gs://`/`az://` are
//!   not wired yet.
//! - Errors from the core [`Error`] taxonomy surface as Python exceptions. A
//!   richer per-variant exception mapping is a natural follow-up.
//! - The compiled module is imported from Python as `arangox`.

// The `#[pyfunction]` macro expands to code that converts the return value with
// `.into()`, which Clippy flags as a useless conversion on each entry point.
#![allow(clippy::useless_conversion)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use arangodb_client::{ArangoClient, CollectionKind, CursorRequest};
use arangodb_dump::{run_dump_with_progress, DumpOptions};
use arangodb_export::{
    collection_query, document_stream, run_export_with_progress, run_split_export_with_progress,
    ExportFormat, ManifestMeta,
};
use arangodb_import::{
    decompress, read_documents, run_import, ArangoBatchSender, BatchSender, ImportFormat,
    ImportOptions, ImportSummary, OnDuplicate,
};
use arangodb_restore::{run_restore_with_progress, RestoreOptions};
use arangodb_storage::{
    Compression, LocalFileSystem, ObjectPath, ObjectStore, ObjectStoreBackend, StorageUri,
};
use arangodb_tools_core::config::{default_workers, BatchConfig, ConcurrencyConfig};
use arangodb_tools_core::manifest::ArtifactKind;
use arangodb_tools_core::{Error, Result};

// ---------------------------------------------------------------------------
// Module definition
// ---------------------------------------------------------------------------

/// The Python module. Imported as `import arangox`.
#[pymodule]
fn arangox(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(import_file, m)?)?;
    m.add_function(wrap_pyfunction!(export, m)?)?;
    m.add_function(wrap_pyfunction!(dump, m)?)?;
    m.add_function(wrap_pyfunction!(restore, m)?)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// import
// ---------------------------------------------------------------------------

/// Bulk-import a local file into a collection.
///
/// Mirrors `arangox import` for local-file input. Returns a dict with the
/// server-reported import summary. Required: `collection`, `input` (a local
/// path; format inferred from the extension unless `format` is given).
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
    adaptive = None,
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
    adaptive: Option<bool>,
) -> PyResult<Bound<'py, PyDict>> {
    let conn = Conn::new(endpoint, database, username, password, token, insecure, request_timeout_secs);
    let params = ImportParams {
        conn,
        collection,
        input,
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
        adaptive,
    };

    let summary = py
        .allow_threads(|| run_blocking(do_import(params)))
        .map_err(to_py_err)?;

    let dict = PyDict::new_bound(py);
    dict.set_item("operation", "import")?;
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

/// Owned parameters for an import, decoupled from PyO3 so the worker is `Send`.
struct ImportParams {
    conn: Conn,
    collection: String,
    input: String,
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
    adaptive: Option<bool>,
}

/// The async import, equivalent to the CLI's `import` command for local files.
async fn do_import(params: ImportParams) -> Result<ImportSummary> {
    let format = resolve_import_format(params.format.as_deref(), &params.input)?;
    let on_duplicate = parse_on_duplicate(&params.on_duplicate)?;
    let kind = if params.edge {
        CollectionKind::Edge
    } else {
        CollectionKind::Document
    };

    let client = params.conn.build()?;
    if params.create_collection {
        client.ensure_collection(&params.collection, kind).await?;
    }

    let mut options = ImportOptions::new(&params.collection);
    options.on_duplicate = on_duplicate;
    options.overwrite = params.overwrite;
    options.from_prefix = params.from_collection_prefix.clone();
    options.to_prefix = params.to_collection_prefix.clone();

    let batch = BatchConfig {
        max_bytes: params
            .batch_size_bytes
            .unwrap_or(BatchConfig::default().max_bytes),
        max_docs: params.max_docs.unwrap_or(BatchConfig::default().max_docs),
    };
    let concurrency = ConcurrencyConfig {
        workers: params.threads.unwrap_or_else(default_workers),
        max_in_flight_bytes: params
            .max_in_flight_bytes
            .unwrap_or(ConcurrencyConfig::default().max_in_flight_bytes),
        adaptive: params.adaptive.unwrap_or(true),
    };

    let compression = Compression::infer_from_path(&params.input);
    let file = tokio::fs::File::open(&params.input)
        .await
        .map_err(|err| Error::config(format!("cannot open input '{}': {err}", params.input)))?;
    let documents = read_documents(format, decompress(compression, file));
    let sender: Arc<dyn BatchSender> = Arc::new(ArangoBatchSender::new(client, options));

    run_import(documents, batch, concurrency, sender).await
}

// ---------------------------------------------------------------------------
// export
// ---------------------------------------------------------------------------

/// Export a collection or AQL query to a file or object store.
///
/// Exactly one of `collection` or `query` is required. `output` accepts a local
/// path, `file://`, or `s3://bucket/key`. Returns a dict describing the result.
#[pyfunction]
#[pyo3(signature = (
    output,
    *,
    collection = None,
    query = None,
    bind_vars = None,
    format = None,
    fields = None,
    compression = None,
    batch_size = 10_000,
    split_bytes = None,
    endpoint = None,
    database = None,
    username = None,
    password = None,
    token = None,
    insecure = false,
    request_timeout_secs = 120,
))]
#[allow(clippy::too_many_arguments)]
fn export<'py>(
    py: Python<'py>,
    output: String,
    collection: Option<String>,
    query: Option<String>,
    bind_vars: Option<String>,
    format: Option<String>,
    fields: Option<Vec<String>>,
    compression: Option<String>,
    batch_size: u32,
    split_bytes: Option<u64>,
    endpoint: Option<String>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    insecure: bool,
    request_timeout_secs: u64,
) -> PyResult<Bound<'py, PyDict>> {
    let conn = Conn::new(endpoint, database, username, password, token, insecure, request_timeout_secs);
    let params = ExportParams {
        conn,
        output,
        collection,
        query,
        bind_vars,
        format: format.unwrap_or_else(|| "jsonl".to_owned()),
        fields,
        compression,
        batch_size,
        split_bytes,
    };

    let outcome = py
        .allow_threads(|| run_blocking(do_export(params)))
        .map_err(to_py_err)?;

    let dict = PyDict::new_bound(py);
    dict.set_item("operation", "export")?;
    dict.set_item("output", outcome.output)?;
    dict.set_item("format", outcome.format)?;
    match outcome.parts {
        Some((parts, manifest)) => {
            dict.set_item("mode", "split")?;
            dict.set_item("parts", parts)?;
            dict.set_item("manifest", manifest)?;
        }
        None => {
            dict.set_item("mode", "single")?;
            dict.set_item("bytes_written", outcome.bytes_written)?;
        }
    }
    Ok(dict)
}

/// Owned export parameters.
struct ExportParams {
    conn: Conn,
    output: String,
    collection: Option<String>,
    query: Option<String>,
    bind_vars: Option<String>,
    format: String,
    fields: Option<Vec<String>>,
    compression: Option<String>,
    batch_size: u32,
    split_bytes: Option<u64>,
}

/// The export result, flattened for dict construction.
struct ExportOutcome {
    output: String,
    format: String,
    bytes_written: u64,
    parts: Option<(usize, String)>,
}

/// The async export, equivalent to the CLI's `export` command.
async fn do_export(params: ExportParams) -> Result<ExportOutcome> {
    let format = ExportFormat::parse(&params.format)?;
    let request = build_request(
        params.collection.as_deref(),
        params.query.as_deref(),
        params.bind_vars.as_deref(),
        params.batch_size,
    )?;
    let fields = params.fields.filter(|f| !f.is_empty());
    let compression = parse_compression(params.compression.as_deref(), &params.output)?;
    let client = params.conn.build()?;
    let (store, path) = open_output(&params.output)?;

    if let Some(max_part_bytes) = params.split_bytes {
        let meta = ManifestMeta {
            database: params.conn.database.clone(),
            tool_version: env!("CARGO_PKG_VERSION").to_string(),
            created_at: now_rfc3339(),
            source: params.collection.clone().or_else(|| params.query.clone()),
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
            None,
        )
        .await?;
        return Ok(ExportOutcome {
            parts: Some((
                manifest.artifacts.len(),
                format!("{}.manifest.json", params.output),
            )),
            output: params.output,
            format: format.extension().to_string(),
            bytes_written: 0,
        });
    }

    let meta = run_export_with_progress(
        &client,
        request,
        format,
        fields,
        compression,
        store.as_ref(),
        &path,
        None,
    )
    .await?;
    Ok(ExportOutcome {
        output: params.output,
        format: format.extension().to_string(),
        bytes_written: meta.size,
        parts: None,
    })
}

// ---------------------------------------------------------------------------
// dump
// ---------------------------------------------------------------------------

/// Dump a database to a directory or object-store prefix.
///
/// `output` accepts a local directory path, `file://`, or `s3://bucket/prefix`.
/// Returns a dict with the collection and artifact counts.
#[pyfunction]
#[pyo3(signature = (
    output,
    *,
    include_system = false,
    compression = None,
    batch_ttl_secs = 600,
    endpoint = None,
    database = None,
    username = None,
    password = None,
    token = None,
    insecure = false,
    request_timeout_secs = 120,
))]
#[allow(clippy::too_many_arguments)]
fn dump<'py>(
    py: Python<'py>,
    output: String,
    include_system: bool,
    compression: Option<String>,
    batch_ttl_secs: u32,
    endpoint: Option<String>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    insecure: bool,
    request_timeout_secs: u64,
) -> PyResult<Bound<'py, PyDict>> {
    let conn = Conn::new(endpoint, database, username, password, token, insecure, request_timeout_secs);
    let params = DumpParams {
        conn,
        output,
        include_system,
        compression,
        batch_ttl_secs,
    };

    let outcome = py
        .allow_threads(|| run_blocking(do_dump(params)))
        .map_err(to_py_err)?;

    let dict = PyDict::new_bound(py);
    dict.set_item("operation", "dump")?;
    dict.set_item("output", outcome.output)?;
    dict.set_item("collections", outcome.collections)?;
    dict.set_item("artifacts", outcome.artifacts)?;
    Ok(dict)
}

/// Owned dump parameters.
struct DumpParams {
    conn: Conn,
    output: String,
    include_system: bool,
    compression: Option<String>,
    batch_ttl_secs: u32,
}

/// The dump result.
struct DumpOutcome {
    output: String,
    collections: usize,
    artifacts: usize,
}

/// The async dump, equivalent to the CLI's `dump` command.
async fn do_dump(params: DumpParams) -> Result<DumpOutcome> {
    let client = params.conn.build()?;
    let store = open_store_root(&params.output)?;
    let options = DumpOptions {
        include_system: params.include_system,
        compression: parse_compression(params.compression.as_deref(), "")?,
        batch_ttl_secs: params.batch_ttl_secs,
        database: params.conn.database.clone(),
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
        created_at: now_rfc3339(),
        ..DumpOptions::default()
    };

    let manifest = run_dump_with_progress(&client, store.as_ref(), &options, None).await?;
    let collections = manifest
        .artifacts
        .iter()
        .filter(|a| a.kind == ArtifactKind::Data)
        .count();
    Ok(DumpOutcome {
        output: params.output,
        collections,
        artifacts: manifest.artifacts.len(),
    })
}

// ---------------------------------------------------------------------------
// restore
// ---------------------------------------------------------------------------

/// Restore a database from a dump.
///
/// `input` accepts a local directory path, `file://`, or `s3://bucket/prefix`.
/// Returns a dict with the number of collections restored.
#[pyfunction]
#[pyo3(signature = (
    input,
    *,
    create_database = false,
    overwrite = false,
    endpoint = None,
    database = None,
    username = None,
    password = None,
    token = None,
    insecure = false,
    request_timeout_secs = 120,
))]
#[allow(clippy::too_many_arguments)]
fn restore<'py>(
    py: Python<'py>,
    input: String,
    create_database: bool,
    overwrite: bool,
    endpoint: Option<String>,
    database: Option<String>,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    insecure: bool,
    request_timeout_secs: u64,
) -> PyResult<Bound<'py, PyDict>> {
    let conn = Conn::new(endpoint, database, username, password, token, insecure, request_timeout_secs);
    let params = RestoreParams {
        conn,
        input,
        create_database,
        overwrite,
    };

    let outcome = py
        .allow_threads(|| run_blocking(do_restore(params)))
        .map_err(to_py_err)?;

    let dict = PyDict::new_bound(py);
    dict.set_item("operation", "restore")?;
    dict.set_item("database", outcome.database)?;
    dict.set_item("collections", outcome.collections)?;
    Ok(dict)
}

/// Owned restore parameters.
struct RestoreParams {
    conn: Conn,
    input: String,
    create_database: bool,
    overwrite: bool,
}

/// The restore result.
struct RestoreOutcome {
    database: String,
    collections: usize,
}

/// The async restore, equivalent to the CLI's `restore` command.
async fn do_restore(params: RestoreParams) -> Result<RestoreOutcome> {
    let client = params.conn.build()?;
    let store = open_store_root(&params.input)?;
    let options = RestoreOptions {
        overwrite: params.overwrite,
        create_database: params
            .create_database
            .then(|| params.conn.database.clone()),
        ..RestoreOptions::default()
    };

    let summary = run_restore_with_progress(&client, store.as_ref(), &options, None).await?;
    Ok(RestoreOutcome {
        database: params.conn.database,
        collections: summary.collections,
    })
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Connection/authentication parameters shared by every tool.
struct Conn {
    endpoint: String,
    database: String,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
    insecure: bool,
    request_timeout_secs: u64,
}

impl Conn {
    /// Resolves optional kwargs into a connection with CLI-equivalent defaults.
    fn new(
        endpoint: Option<String>,
        database: Option<String>,
        username: Option<String>,
        password: Option<String>,
        token: Option<String>,
        insecure: bool,
        request_timeout_secs: u64,
    ) -> Self {
        Self {
            endpoint: endpoint.unwrap_or_else(|| "http://localhost:8529".to_owned()),
            database: database.unwrap_or_else(|| "_system".to_owned()),
            username,
            password,
            token,
            insecure,
            request_timeout_secs,
        }
    }

    /// Builds an [`ArangoClient`] from these parameters.
    fn build(&self) -> Result<ArangoClient> {
        let mut builder = ArangoClient::builder()
            .endpoint(&self.endpoint)
            .database(&self.database)
            .insecure(self.insecure)
            .request_timeout(Duration::from_secs(self.request_timeout_secs));
        if let Some(token) = &self.token {
            builder = builder.bearer_auth(token);
        } else if let Some(username) = &self.username {
            builder = builder.basic_auth(username, self.password.clone().unwrap_or_default());
        }
        builder.build()
    }
}

/// Builds a Tokio runtime and drives `fut` to completion.
fn run_blocking<T, F>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| Error::config(format!("failed to start async runtime: {err}")))?;
    runtime.block_on(fut)
}

/// Builds the cursor request from `collection` or `query` (exactly one).
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
                    .map_err(|err| Error::config(format!("invalid bind_vars JSON: {err}")))?;
                request = request.with_bind_vars(value);
            }
            Ok(request)
        }
        (None, None) => Err(Error::config("one of collection or query is required")),
        (Some(_), Some(_)) => Err(Error::config("collection and query are mutually exclusive")),
    }
}

/// Resolves the import format from an explicit name or the input path.
fn resolve_import_format(explicit: Option<&str>, input: &str) -> Result<ImportFormat> {
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

/// Parses a compression name. `auto` (the default) infers from `path`'s
/// extension; an empty path with `auto` resolves to no compression.
fn parse_compression(name: Option<&str>, path: &str) -> Result<Compression> {
    match name.unwrap_or("auto").to_ascii_lowercase().as_str() {
        "auto" => Ok(Compression::infer_from_path(path)),
        "none" => Ok(Compression::None),
        "gzip" => Ok(Compression::Gzip),
        "zstd" => Ok(Compression::Zstd),
        other => Err(Error::config(format!(
            "unknown compression '{other}'; expected auto, none, gzip, or zstd"
        ))),
    }
}

/// Resolves a single-object output destination into a store and object path.
/// Accepts a filesystem path, a `file://` URI, or an object-storage URI
/// (`s3://`, `gs://`, `az://`, `seaweed+s3://`).
fn open_output(output: &str) -> Result<(Box<dyn ObjectStore>, ObjectPath)> {
    if let Some((scheme, _)) = output.split_once("://") {
        if scheme == "file" {
            return open_local(Path::new(output.trim_start_matches("file://")));
        }
        let parsed = StorageUri::parse(output)?;
        let backend = ObjectStoreBackend::for_bucket(&parsed)?;
        return Ok((Box::new(backend), ObjectPath::new(parsed.path)));
    }
    open_local(Path::new(output))
}

/// Resolves a local output path to a [`LocalFileSystem`] rooted at its parent
/// and an object path of just the file name.
fn open_local(path: &Path) -> Result<(Box<dyn ObjectStore>, ObjectPath)> {
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::config(format!("output path has no file name: {}", path.display())))?;
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    Ok((
        Box::new(LocalFileSystem::new(parent)),
        ObjectPath::new(file_name.to_string()),
    ))
}

/// Resolves a dump *root* (a directory or object-store prefix holding many
/// artifacts) into a store. Accepts a path, `file://`, or an object-storage URI
/// (`s3://`, `gs://`, `az://`, `seaweed+s3://`).
fn open_store_root(location: &str) -> Result<Box<dyn ObjectStore>> {
    if let Some((scheme, _)) = location.split_once("://") {
        if scheme == "file" {
            return Ok(Box::new(LocalFileSystem::new(Path::new(
                location.trim_start_matches("file://"),
            ))));
        }
        let parsed = StorageUri::parse(location)?;
        return Ok(Box::new(ObjectStoreBackend::for_prefix(&parsed)?));
    }
    Ok(Box::new(LocalFileSystem::new(Path::new(location))))
}

/// An RFC 3339 timestamp for the current instant (empty string if formatting
/// fails, which it never should).
fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

/// Maps a core [`Error`] into a Python exception.
fn to_py_err(err: Error) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
