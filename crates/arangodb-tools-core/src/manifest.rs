//! The canonical manifest model for dumps and exports.
//!
//! The manifest is the source of truth for what a dump/export contains: every
//! artifact is enumerated with its format, compression, size, and checksum, so
//! restore never has to guess filenames. See `docs/dump-format.md` for the
//! on-disk/object specification.

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// The manifest schema version understood by this build.
pub const MANIFEST_VERSION: u32 = 1;

/// The kind of artifact an entry describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    /// Top-level dump/export metadata.
    Meta,
    /// Collection structure and index definitions.
    Structure,
    /// A view definition.
    View,
    /// Collection document data (possibly one of several parts).
    Data,
}

/// The serialization format of an artifact's payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataFormat {
    /// Newline-delimited JSON.
    Jsonl,
    /// A single JSON array.
    Json,
    /// Comma-separated values.
    Csv,
    /// VelocyPack.
    Vpack,
    /// XGMML graph XML.
    Xgmml,
}

/// The compression applied to an artifact's payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// No compression.
    #[default]
    None,
    /// gzip.
    Gzip,
    /// zstd.
    Zstd,
}

/// A content checksum for integrity verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksum {
    /// The algorithm name, e.g. `"sha256"` or `"blake3"`.
    pub algorithm: String,
    /// The hex-encoded digest.
    pub value: String,
}

/// A rolling checkpoint for a resumable import.
///
/// Imports record the highest *contiguous* batch index that the server has
/// acknowledged, plus the documents and request bytes those batches contained.
/// Because batching is deterministic (identical input + config yields identical
/// batches and indices), a restarted import can re-derive the same batch
/// sequence and skip every batch whose index is `<= committed_batches`.
///
/// A single contiguous high-water mark is used rather than per-batch markers so
/// out-of-order, concurrent sends can never advance the checkpoint past a batch
/// that has not actually been committed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportCheckpoint {
    /// Highest batch index for which it and all lower-numbered batches are
    /// confirmed committed by the server.
    pub committed_batches: u64,
    /// Total documents contained in the committed batches.
    pub documents_committed: u64,
    /// Total request-body bytes contained in the committed batches.
    pub bytes_committed: u64,
}

impl ImportCheckpoint {
    /// Serializes the checkpoint to compact JSON.
    ///
    /// # Errors
    /// Returns [`crate::Error::Serialization`] if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Parses a checkpoint from JSON bytes.
    ///
    /// # Errors
    /// Returns [`crate::Error::Serialization`] if the bytes are not a valid
    /// checkpoint document.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// A contiguous-prefix checkpoint for a resumable restore.
///
/// Restore processes collections in a deterministic order and, after each one
/// is fully restored (data + indexes), records its identifier here. On restart,
/// every already-completed collection is skipped. The [`Self::manifest`]
/// fingerprint guards against resuming against a *different* dump.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreCheckpoint {
    /// A fingerprint of the dump manifest this checkpoint belongs to, used to
    /// refuse resuming a restore against a manifest it was not created for.
    pub manifest: String,
    /// Identifiers of fully-restored collections, in restore order. The
    /// identifier is `"{database}::{collection}"` (`database` empty for a
    /// single-database dump).
    pub completed: Vec<String>,
}

impl RestoreCheckpoint {
    /// Creates an empty checkpoint bound to a manifest fingerprint.
    #[must_use]
    pub fn new(manifest: impl Into<String>) -> Self {
        Self {
            manifest: manifest.into(),
            completed: Vec::new(),
        }
    }

    /// Returns `true` if `id` has already been recorded as completed.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.completed.iter().any(|c| c == id)
    }

    /// Serializes the checkpoint to compact JSON.
    ///
    /// # Errors
    /// Returns [`crate::Error::Serialization`] if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Parses a checkpoint from JSON bytes.
    ///
    /// # Errors
    /// Returns [`crate::Error::Serialization`] if the bytes are not a valid
    /// checkpoint document.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Encryption metadata recorded in the manifest.
///
/// Enterprise-encrypted payloads are not produced or readable by this project
/// yet; the marker is recorded so restore can detect and refuse them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionInfo {
    /// The encryption algorithm, or `"none"`.
    pub algorithm: String,
}

impl Default for EncryptionInfo {
    fn default() -> Self {
        Self {
            algorithm: "none".to_owned(),
        }
    }
}

impl EncryptionInfo {
    /// Returns `true` if the payload is encrypted (algorithm is not `"none"`).
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.algorithm != "none"
    }
}

/// A single artifact entry in a [`Manifest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    /// Object path relative to the dump/export root.
    pub path: String,
    /// The kind of artifact.
    pub kind: ArtifactKind,
    /// The payload format.
    pub format: DataFormat,
    /// The payload compression.
    #[serde(default)]
    pub compression: Compression,
    /// The on-disk/object byte size.
    pub byte_size: u64,
    /// An optional content checksum.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<Checksum>,
    /// The collection this artifact belongs to, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
    /// The source database this artifact belongs to, for multi-database dumps.
    /// `None` means the dump is single-database and restores into the target
    /// database chosen at restore time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// The part number for split data artifacts, if applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part: Option<u32>,
}

/// The canonical description of a dump or export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The manifest schema version.
    pub manifest_version: u32,
    /// The version of the tool that produced the manifest.
    pub tool_version: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// The source database name.
    pub database: String,
    /// An optional description of the source (e.g. an AQL query for exports).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Encryption metadata.
    #[serde(default)]
    pub encryption: EncryptionInfo,
    /// The artifacts that make up this dump/export.
    pub artifacts: Vec<Artifact>,
}

impl Manifest {
    /// Creates an empty manifest for `database`, stamped with the current
    /// schema and the given `tool_version` and `created_at`.
    #[must_use]
    pub fn new(
        database: impl Into<String>,
        tool_version: impl Into<String>,
        created_at: impl Into<String>,
    ) -> Self {
        Self {
            manifest_version: MANIFEST_VERSION,
            tool_version: tool_version.into(),
            created_at: created_at.into(),
            database: database.into(),
            source: None,
            encryption: EncryptionInfo::default(),
            artifacts: Vec::new(),
        }
    }

    /// Appends an artifact entry.
    pub fn push(&mut self, artifact: Artifact) {
        self.artifacts.push(artifact);
    }

    /// Serializes the manifest to pretty JSON.
    ///
    /// # Errors
    /// Returns [`crate::Error::Serialization`] if serialization fails.
    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Parses a manifest from JSON.
    ///
    /// # Errors
    /// Returns [`crate::Error::Serialization`] if parsing fails.
    pub fn from_json(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }

    /// A deterministic identity fingerprint for this dump, used by
    /// [`RestoreCheckpoint`] to detect a mismatched dump on resume.
    ///
    /// Combines the source database, creation timestamp, and the ordered
    /// artifact paths — enough to distinguish two different dumps without a
    /// cryptographic dependency here.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        let mut parts = String::new();
        parts.push_str(&self.database);
        parts.push('|');
        parts.push_str(&self.created_at);
        parts.push('|');
        for artifact in &self.artifacts {
            parts.push_str(&artifact.path);
            parts.push(',');
        }
        parts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        let mut manifest = Manifest::new("mydb", "0.0.0", "2026-06-01T00:00:00Z");
        manifest.push(Artifact {
            path: "users_abc.data.jsonl.gz".to_owned(),
            kind: ArtifactKind::Data,
            format: DataFormat::Jsonl,
            compression: Compression::Gzip,
            byte_size: 1024,
            checksum: Some(Checksum {
                algorithm: "sha256".to_owned(),
                value: "deadbeef".to_owned(),
            }),
            collection: Some("users".to_owned()),
            database: None,
            part: Some(0),
        });
        manifest
    }

    #[test]
    fn round_trips_through_json() {
        let manifest = sample();
        let json = manifest.to_json().unwrap();
        let parsed = Manifest::from_json(&json).unwrap();
        assert_eq!(manifest, parsed);
        assert_eq!(parsed.manifest_version, MANIFEST_VERSION);
        assert_eq!(parsed.artifacts.len(), 1);
    }

    #[test]
    fn defaults_are_applied_when_fields_absent() {
        let json = r#"{
            "manifest_version": 1,
            "tool_version": "0.0.0",
            "created_at": "2026-06-01T00:00:00Z",
            "database": "mydb",
            "artifacts": [
                {"path": "x", "kind": "meta", "format": "json", "byte_size": 10}
            ]
        }"#;
        let parsed = Manifest::from_json(json).unwrap();
        assert_eq!(parsed.encryption.algorithm, "none");
        assert!(!parsed.encryption.is_encrypted());
        assert_eq!(parsed.artifacts[0].compression, Compression::None);
        assert!(parsed.artifacts[0].checksum.is_none());
    }

    #[test]
    fn detects_encryption_marker() {
        let info = EncryptionInfo {
            algorithm: "aes-256-ctr".to_owned(),
        };
        assert!(info.is_encrypted());
    }

    #[test]
    fn import_checkpoint_round_trips() {
        let checkpoint = ImportCheckpoint {
            committed_batches: 42,
            documents_committed: 4200,
            bytes_committed: 1_048_576,
        };
        let json = checkpoint.to_json().unwrap();
        let parsed = ImportCheckpoint::from_json(json.as_bytes()).unwrap();
        assert_eq!(checkpoint, parsed);
    }

    #[test]
    fn restore_checkpoint_round_trips_and_tracks_completion() {
        let mut checkpoint = RestoreCheckpoint::new("fp-1");
        checkpoint.completed.push("::users".to_string());
        assert!(checkpoint.contains("::users"));
        assert!(!checkpoint.contains("::orders"));
        let json = checkpoint.to_json().unwrap();
        let parsed = RestoreCheckpoint::from_json(json.as_bytes()).unwrap();
        assert_eq!(checkpoint, parsed);
    }

    #[test]
    fn fingerprint_differs_for_different_dumps() {
        let a = Manifest::new("db", "0", "2026-01-01T00:00:00Z");
        let b = Manifest::new("db", "0", "2026-02-02T00:00:00Z");
        assert_ne!(a.fingerprint(), b.fingerprint());
        // Same identity fields yield the same fingerprint.
        let a2 = Manifest::new("db", "0", "2026-01-01T00:00:00Z");
        assert_eq!(a.fingerprint(), a2.fingerprint());
    }
}
