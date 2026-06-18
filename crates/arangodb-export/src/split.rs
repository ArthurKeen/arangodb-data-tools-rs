//! Split export: write a document stream as one or more JSONL part objects
//! plus a canonical manifest enumerating them.
//!
//! Parts are cut at line boundaries once a part reaches the configured byte
//! threshold, so each part is independently valid JSONL. The manifest records
//! every part with its format, compression, on-object size, and a SHA-256
//! checksum, so a reader never has to guess part filenames (PRD §8.3/§8.4).
//!
//! Splitting is JSONL-only: it is the streaming-friendly format where cutting
//! between records yields valid parts. JSON-array and CSV exports use the
//! single-object [`crate::run_export`] path.

use arangodb_storage::{compress, ByteStream, Compression, ObjectPath, ObjectStore};
use arangodb_tools_core::manifest::{
    Artifact, ArtifactKind, Checksum, Compression as ManifestCompression, DataFormat, Manifest,
};
use arangodb_tools_core::Result;
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};

use crate::encode::encode;
use crate::format::ExportFormat;
use crate::DocumentStream;

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

/// Exports `documents` as JSONL split into parts of at most `max_part_bytes`
/// (uncompressed) under `base_path`, writing the parts and a
/// `<base_path>.manifest.json`, and returns the manifest.
///
/// # Errors
/// Returns an error if encoding, compression, or any write fails.
pub async fn run_split_export(
    documents: DocumentStream,
    compression: Compression,
    store: &dyn ObjectStore,
    base_path: &str,
    max_part_bytes: u64,
    meta: ManifestMeta,
) -> Result<Manifest> {
    let mut manifest = Manifest::new(meta.database, meta.tool_version, meta.created_at);
    manifest.source = meta.source;

    let mut lines = encode(ExportFormat::JsonLines, None, documents)?;
    let threshold = max_part_bytes.max(1);
    let mut buffer: Vec<u8> = Vec::new();
    let mut part: u32 = 0;

    while let Some(chunk) = lines.next().await {
        buffer.extend_from_slice(&chunk?);
        if buffer.len() as u64 >= threshold {
            let payload = std::mem::take(&mut buffer);
            write_part(store, base_path, part, payload, compression, &mut manifest).await?;
            part += 1;
        }
    }
    // Always write a final part, so an empty export still yields one (empty)
    // data object and a well-formed manifest.
    if !buffer.is_empty() || part == 0 {
        write_part(store, base_path, part, buffer, compression, &mut manifest).await?;
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
    manifest: &mut Manifest,
) -> Result<()> {
    let suffix = match compression.extension() {
        Some(ext) => format!("jsonl.{ext}"),
        None => "jsonl".to_string(),
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
        format: DataFormat::Jsonl,
        compression: map_compression(compression),
        byte_size: written.size,
        checksum: Some(checksum),
        collection: None,
        part: Some(part),
    });
    Ok(())
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

    #[tokio::test]
    async fn empty_export_yields_one_part() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileSystem::new(dir.path());
        let manifest = run_split_export(docs(0), Compression::Gzip, &store, "e", 1024, meta())
            .await
            .unwrap();
        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.artifacts[0].compression, ManifestCompression::Gzip);
        assert_eq!(manifest.artifacts[0].path, "e.part-00000.jsonl.gz");
    }
}
