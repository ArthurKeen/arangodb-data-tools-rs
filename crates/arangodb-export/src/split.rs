//! Split export: write a document stream as one or more part objects plus a
//! canonical manifest enumerating them.
//!
//! Every part is a *standalone, valid document* in the chosen format, so a
//! reader can consume any single part on its own and concatenating the records
//! across parts reproduces the export:
//!
//! - **JSONL**: parts are cut at line boundaries; each part is valid NDJSON.
//! - **JSON array**: each part is its own complete `[...]` array; the reader
//!   flattens the arrays across parts.
//! - **CSV**: every part repeats the header row, so each part is a complete
//!   CSV document.
//!
//! Parts are cut once a part reaches the configured byte threshold (measured on
//! the uncompressed record bytes). The manifest records every part with its
//! format, compression, on-object size, and a SHA-256 checksum, so a reader
//! never has to guess part filenames (PRD §8.3/§8.4).

use std::sync::Arc;
use std::time::Instant;

use arangodb_storage::{compress, ByteStream, Compression, ObjectPath, ObjectStore};
use arangodb_tools_core::manifest::{
    Artifact, ArtifactKind, Checksum, Compression as ManifestCompression, DataFormat, Manifest,
};
use arangodb_tools_core::progress::{ProgressEvent, ProgressSink, ProgressSnapshot};
use arangodb_tools_core::{Error, Result};
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::encode::{csv_header, csv_row, json_element, json_line};
use crate::format::ExportFormat;
use crate::DocumentStream;

/// Frames a splittable export so each part is a standalone valid document.
///
/// The framer is stateful across the records of a single part: [`open`] begins
/// a part (and resets per-part state), [`record`] appends one document, and
/// [`close`] finishes the part.
///
/// [`open`]: PartFramer::open
/// [`record`]: PartFramer::record
/// [`close`]: PartFramer::close
struct PartFramer {
    format: ExportFormat,
    fields: Option<Vec<String>>,
    /// JSON-array only: whether the current part has emitted an element yet
    /// (controls the leading comma).
    array_first: bool,
}

impl PartFramer {
    /// Builds a framer, validating that CSV has a non-empty field list.
    fn new(format: ExportFormat, fields: Option<Vec<String>>) -> Result<Self> {
        if format == ExportFormat::Csv {
            match &fields {
                Some(f) if !f.is_empty() => {}
                _ => {
                    return Err(Error::config(
                        "CSV split export requires an explicit, non-empty list of fields",
                    ))
                }
            }
        }
        Ok(Self {
            format,
            fields,
            array_first: true,
        })
    }

    /// The manifest [`DataFormat`] the parts are written in.
    fn data_format(&self) -> DataFormat {
        match self.format {
            ExportFormat::JsonLines => DataFormat::Jsonl,
            ExportFormat::JsonArray => DataFormat::Json,
            ExportFormat::Csv => DataFormat::Csv,
        }
    }

    /// The uncompressed file-extension stem for a part (e.g. `jsonl`).
    fn extension(&self) -> &'static str {
        self.format.extension()
    }

    /// Bytes that begin a new part; resets any per-part framing state.
    ///
    /// # Errors
    /// Returns an error if the CSV header cannot be encoded.
    fn open(&mut self) -> Result<Vec<u8>> {
        match self.format {
            ExportFormat::JsonLines => Ok(Vec::new()),
            ExportFormat::JsonArray => {
                self.array_first = true;
                Ok(b"[".to_vec())
            }
            ExportFormat::Csv => {
                let fields = self.fields.as_deref().unwrap_or(&[]);
                Ok(csv_header(fields)?.to_vec())
            }
        }
    }

    /// Appends one document's bytes to the current part.
    ///
    /// # Errors
    /// Returns an error if the document cannot be encoded.
    fn record(&mut self, document: &Value) -> Result<Vec<u8>> {
        match self.format {
            ExportFormat::JsonLines => Ok(json_line(document)?.to_vec()),
            ExportFormat::JsonArray => {
                let mut chunk = Vec::new();
                if !self.array_first {
                    chunk.push(b',');
                }
                self.array_first = false;
                chunk.extend_from_slice(&json_element(document)?);
                Ok(chunk)
            }
            ExportFormat::Csv => {
                let fields = self.fields.as_deref().unwrap_or(&[]);
                Ok(csv_row(fields, document)?.to_vec())
            }
        }
    }

    /// Bytes that finish the current part.
    fn close(&self) -> Vec<u8> {
        match self.format {
            ExportFormat::JsonLines | ExportFormat::Csv => Vec::new(),
            ExportFormat::JsonArray => b"]".to_vec(),
        }
    }
}

/// Manifest metadata supplied by the caller (kept out of this crate so the
/// export is deterministic and testable).
#[derive(Debug, Clone)]
pub struct ManifestMeta {
    /// Source database name.
    pub database: String,
    /// Producing tool version.
    pub tool_version: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// Optional source description (e.g. collection name or AQL query).
    pub source: Option<String>,
}

/// Exports `documents` as `format` split into parts of at most `max_part_bytes`
/// (uncompressed) under `base_path`, writing the parts and a
/// `<base_path>.manifest.json`, and returns the manifest.
///
/// `fields` is required for [`ExportFormat::Csv`] (the column order) and
/// ignored otherwise. Every part is a standalone valid document in `format`.
///
/// # Errors
/// Returns an error if encoding, compression, or any write fails.
#[allow(clippy::too_many_arguments)]
pub async fn run_split_export(
    documents: DocumentStream,
    format: ExportFormat,
    fields: Option<Vec<String>>,
    compression: Compression,
    store: &dyn ObjectStore,
    base_path: &str,
    max_part_bytes: u64,
    meta: ManifestMeta,
) -> Result<Manifest> {
    run_split_export_with_progress(
        documents,
        format,
        fields,
        compression,
        store,
        base_path,
        max_part_bytes,
        meta,
        None,
    )
    .await
}

/// Like [`run_split_export`], but emits a [`ProgressEvent::Progress`] snapshot
/// after each part is written when `progress` is `Some`. Lifecycle
/// (`started`/`finished`) events are the caller's responsibility.
///
/// # Errors
/// Returns an error if encoding, compression, or any write fails.
#[allow(clippy::too_many_arguments)]
pub async fn run_split_export_with_progress(
    documents: DocumentStream,
    format: ExportFormat,
    fields: Option<Vec<String>>,
    compression: Compression,
    store: &dyn ObjectStore,
    base_path: &str,
    max_part_bytes: u64,
    meta: ManifestMeta,
    progress: Option<Arc<dyn ProgressSink>>,
) -> Result<Manifest> {
    let started = Instant::now();
    let mut manifest = Manifest::new(meta.database, meta.tool_version, meta.created_at);
    manifest.source = meta.source;

    let mut framer = PartFramer::new(format, fields)?;
    let threshold = max_part_bytes.max(1);
    let mut buffer: Vec<u8> = framer.open()?;
    let mut records_in_part: u64 = 0;
    let mut part: u32 = 0;
    let mut bytes_written: u64 = 0;

    futures::pin_mut!(documents);
    while let Some(document) = documents.next().await {
        let document = document?;
        buffer.extend_from_slice(&framer.record(&document)?);
        records_in_part += 1;
        if buffer.len() as u64 >= threshold {
            buffer.extend_from_slice(&framer.close());
            write_part(
                store,
                base_path,
                part,
                buffer,
                compression,
                &framer,
                &mut manifest,
            )
            .await?;
            emit_part_progress(progress.as_deref(), &manifest, &mut bytes_written, started);
            part += 1;
            records_in_part = 0;
            buffer = framer.open()?;
        }
    }
    // Write the trailing part when it holds records, or when nothing has been
    // written yet (so an empty export still yields one valid, empty part and a
    // well-formed manifest).
    if records_in_part > 0 || part == 0 {
        buffer.extend_from_slice(&framer.close());
        write_part(
            store,
            base_path,
            part,
            buffer,
            compression,
            &framer,
            &mut manifest,
        )
        .await?;
        emit_part_progress(progress.as_deref(), &manifest, &mut bytes_written, started);
    }

    let manifest_json = manifest.to_json()?;
    store
        .put_stream(
            &ObjectPath::new(format!("{base_path}.manifest.json")),
            once(Bytes::from(manifest_json.into_bytes())),
        )
        .await?;
    Ok(manifest)
}

/// Compresses one part's bytes, writes the object, and appends its manifest
/// entry (size and checksum reflect the stored, compressed object).
async fn write_part(
    store: &dyn ObjectStore,
    base_path: &str,
    part: u32,
    payload: Vec<u8>,
    compression: Compression,
    framer: &PartFramer,
    manifest: &mut Manifest,
) -> Result<()> {
    let stem = framer.extension();
    let suffix = match compression.extension() {
        Some(ext) => format!("{stem}.{ext}"),
        None => stem.to_string(),
    };
    let path = format!("{base_path}.part-{part:05}.{suffix}");

    let compressed = collect(compress(compression, once(Bytes::from(payload)))).await?;
    let checksum = Checksum {
        algorithm: "sha256".to_string(),
        value: sha256_hex(&compressed),
    };
    let object = ObjectPath::new(path.clone());
    let written = store
        .put_stream(&object, once(Bytes::from(compressed)))
        .await?;

    manifest.push(Artifact {
        path,
        kind: ArtifactKind::Data,
        format: framer.data_format(),
        compression: map_compression(compression),
        byte_size: written.size,
        checksum: Some(checksum),
        collection: None,
        database: None,
        part: Some(part),
    });
    Ok(())
}

/// Adds the most recently written part's size to `bytes_written` and emits a
/// progress snapshot (parts written so far + cumulative on-object bytes).
fn emit_part_progress(
    sink: Option<&dyn ProgressSink>,
    manifest: &Manifest,
    bytes_written: &mut u64,
    started: Instant,
) {
    if let Some(last) = manifest.artifacts.last() {
        *bytes_written += last.byte_size;
    }
    if let Some(sink) = sink {
        sink.emit(&ProgressEvent::Progress(ProgressSnapshot {
            bytes_written: *bytes_written,
            batches: manifest.artifacts.len() as u64,
            elapsed_secs: started.elapsed().as_secs_f64(),
            ..ProgressSnapshot::default()
        }));
    }
}

/// Wraps bytes in a single-chunk stream.
fn once(bytes: Bytes) -> ByteStream {
    Box::pin(futures::stream::once(async move { Ok(bytes) }))
}

/// Drains a byte stream into a contiguous buffer.
async fn collect(mut stream: ByteStream) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

/// Hex-encodes the SHA-256 digest of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// Maps the storage codec to the manifest's compression enum.
fn map_compression(compression: Compression) -> ManifestCompression {
    match compression {
        Compression::None => ManifestCompression::None,
        Compression::Gzip => ManifestCompression::Gzip,
        Compression::Zstd => ManifestCompression::Zstd,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arangodb_storage::LocalFileSystem;
    use serde_json::Value;

    fn docs(n: usize) -> DocumentStream {
        Box::pin(futures::stream::iter(
            (0..n).map(|i| Ok(serde_json::json!({ "i": i }))),
        ))
    }

    fn meta() -> ManifestMeta {
        ManifestMeta {
            database: "mydb".to_string(),
            tool_version: "0.0.0".to_string(),
            created_at: "2026-06-01T00:00:00Z".to_string(),
            source: Some("things".to_string()),
        }
    }

    async fn read(store: &LocalFileSystem, path: &str) -> Vec<u8> {
        collect(
            store
                .get_stream(&ObjectPath::new(path), None)
                .await
                .unwrap(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn splits_into_multiple_parts_and_writes_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());

        // Each line `{"i":N}\n` is ~9 bytes; a 30-byte threshold forces splits.
        let manifest = run_split_export(
            docs(10),
            ExportFormat::JsonLines,
            None,
            Compression::None,
            &store,
            "export/things",
            30,
            meta(),
        )
        .await
        .unwrap();

        assert!(
            manifest.artifacts.len() >= 3,
            "expected several parts, got {}",
            manifest.artifacts.len()
        );
        // Parts are numbered contiguously from zero.
        for (i, artifact) in manifest.artifacts.iter().enumerate() {
            assert_eq!(artifact.part, Some(i as u32));
            assert_eq!(artifact.format, DataFormat::Jsonl);
            assert!(artifact.checksum.is_some());
            assert!(artifact.byte_size > 0);
        }

        // The manifest object is readable and reproduces the model.
        let manifest_bytes = read(&store, "export/things.manifest.json").await;
        let reloaded = Manifest::from_json(&String::from_utf8(manifest_bytes).unwrap()).unwrap();
        assert_eq!(reloaded, manifest);
        assert_eq!(reloaded.source.as_deref(), Some("things"));

        // Concatenating the parts in order reproduces every document.
        let mut all = Vec::new();
        for artifact in &manifest.artifacts {
            all.extend(read(&store, &artifact.path).await);
        }
        let lines: Vec<Value> = String::from_utf8(all)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 10);
        assert_eq!(lines[0]["i"], 0);
        assert_eq!(lines[9]["i"], 9);
    }

    #[derive(Default)]
    struct CountingSink {
        events: std::sync::atomic::AtomicU64,
    }

    impl ProgressSink for CountingSink {
        fn emit(&self, _event: &ProgressEvent) {
            self.events
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn split_export_emits_one_progress_event_per_part() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let sink = Arc::new(CountingSink::default());

        let manifest = run_split_export_with_progress(
            docs(10),
            ExportFormat::JsonLines,
            None,
            Compression::None,
            &store,
            "p/things",
            30,
            meta(),
            Some(Arc::clone(&sink) as Arc<dyn ProgressSink>),
        )
        .await
        .unwrap();

        assert_eq!(
            sink.events.load(std::sync::atomic::Ordering::SeqCst),
            manifest.artifacts.len() as u64,
            "one progress event per written part"
        );
    }

    #[tokio::test]
    async fn empty_export_yields_one_part() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let manifest = run_split_export(
            docs(0),
            ExportFormat::JsonLines,
            None,
            Compression::Gzip,
            &store,
            "e",
            1024,
            meta(),
        )
        .await
        .unwrap();
        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.artifacts[0].compression, ManifestCompression::Gzip);
        assert_eq!(manifest.artifacts[0].path, "e.part-00000.jsonl.gz");
    }

    #[tokio::test]
    async fn json_array_parts_are_each_valid_arrays() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());

        // A small threshold forces several parts.
        let manifest = run_split_export(
            docs(10),
            ExportFormat::JsonArray,
            None,
            Compression::None,
            &store,
            "arr/things",
            20,
            meta(),
        )
        .await
        .unwrap();

        assert!(manifest.artifacts.len() >= 3, "expected several parts");
        let mut flattened = Vec::new();
        for artifact in &manifest.artifacts {
            assert_eq!(artifact.format, DataFormat::Json);
            assert!(artifact.path.ends_with(".json"));
            let bytes = read(&store, &artifact.path).await;
            // Each part is a standalone JSON array.
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            let arr = value.as_array().expect("part is a JSON array");
            assert!(!arr.is_empty(), "no part should be an empty array");
            flattened.extend(arr.iter().cloned());
        }
        assert_eq!(flattened.len(), 10);
        assert_eq!(flattened[0]["i"], 0);
        assert_eq!(flattened[9]["i"], 9);
    }

    #[tokio::test]
    async fn csv_parts_each_repeat_the_header() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());

        let fields = vec!["i".to_string()];
        let manifest = run_split_export(
            docs(10),
            ExportFormat::Csv,
            Some(fields),
            Compression::None,
            &store,
            "csv/things",
            8,
            meta(),
        )
        .await
        .unwrap();

        assert!(manifest.artifacts.len() >= 3, "expected several parts");
        let mut data_rows = Vec::new();
        for artifact in &manifest.artifacts {
            assert_eq!(artifact.format, DataFormat::Csv);
            assert!(artifact.path.ends_with(".csv"));
            let text = String::from_utf8(read(&store, &artifact.path).await).unwrap();
            let mut lines = text.lines();
            // Every part starts with the header row.
            assert_eq!(lines.next(), Some("i"));
            data_rows.extend(lines.map(str::to_string));
        }
        assert_eq!(data_rows.len(), 10);
        assert_eq!(data_rows[0], "0");
        assert_eq!(data_rows[9], "9");
    }

    #[tokio::test]
    async fn csv_split_requires_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let result = run_split_export(
            docs(1),
            ExportFormat::Csv,
            None,
            Compression::None,
            &store,
            "bad",
            1024,
            meta(),
        )
        .await;
        assert!(matches!(result, Err(arangodb_tools_core::Error::Config(_))));
    }
}
