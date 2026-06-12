//! Input formats for import.
//!
//! Duplicate-handling modes live in [`arangodb_client::import`] (re-exported
//! from this crate's root) since they map directly to `/_api/import`
//! parameters.

use std::path::Path;

use arangodb_tools_core::{Error, Result};

/// A supported import input format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportFormat {
    /// Newline-delimited JSON: one JSON value per line.
    JsonLines,
    /// A single JSON array of documents.
    JsonArray,
    /// Comma-separated values with a header row.
    Csv,
    /// Tab-separated values with a header row.
    Tsv,
}

impl ImportFormat {
    /// Infers the format from a file extension (case-insensitive).
    ///
    /// `.jsonl`/`.ndjson` map to [`ImportFormat::JsonLines`], `.json` to
    /// [`ImportFormat::JsonArray`], `.csv` to [`ImportFormat::Csv`], and
    /// `.tsv`/`.tab` to [`ImportFormat::Tsv`].
    ///
    /// # Errors
    /// Returns [`Error::Config`] if the extension is missing or unrecognized.
    pub fn infer_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| {
                Error::config(format!(
                    "cannot infer import format: '{}' has no file extension; pass an explicit format",
                    path.display()
                ))
            })?;

        Self::from_extension(&ext).ok_or_else(|| {
            Error::config(format!(
                "cannot infer import format from extension '.{ext}'; pass an explicit format"
            ))
        })
    }

    /// Maps a lowercase extension (without the leading dot) to a format.
    #[must_use]
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "jsonl" | "ndjson" => Some(Self::JsonLines),
            "json" => Some(Self::JsonArray),
            "csv" => Some(Self::Csv),
            "tsv" | "tab" => Some(Self::Tsv),
            _ => None,
        }
    }

    /// Returns the field delimiter for delimited formats.
    #[must_use]
    pub fn delimiter(self) -> Option<u8> {
        match self {
            Self::Csv => Some(b','),
            Self::Tsv => Some(b'\t'),
            Self::JsonLines | Self::JsonArray => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infers_known_extensions() {
        assert_eq!(
            ImportFormat::infer_from_path("data/users.jsonl").unwrap(),
            ImportFormat::JsonLines
        );
        assert_eq!(
            ImportFormat::infer_from_path("USERS.NDJSON").unwrap(),
            ImportFormat::JsonLines
        );
        assert_eq!(
            ImportFormat::infer_from_path("export.json").unwrap(),
            ImportFormat::JsonArray
        );
        assert_eq!(
            ImportFormat::infer_from_path("table.CSV").unwrap(),
            ImportFormat::Csv
        );
        assert_eq!(
            ImportFormat::infer_from_path("table.tsv").unwrap(),
            ImportFormat::Tsv
        );
    }

    #[test]
    fn rejects_unknown_or_missing_extension() {
        assert!(ImportFormat::infer_from_path("archive.parquet").is_err());
        assert!(ImportFormat::infer_from_path("noext").is_err());
    }

    #[test]
    fn delimiters_match_format() {
        assert_eq!(ImportFormat::Csv.delimiter(), Some(b','));
        assert_eq!(ImportFormat::Tsv.delimiter(), Some(b'\t'));
        assert_eq!(ImportFormat::JsonLines.delimiter(), None);
    }
}
