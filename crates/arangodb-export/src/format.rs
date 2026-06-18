//! Output formats for export.

use arangodb_tools_core::{Error, Result};

/// A supported export output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportFormat {
    /// Newline-delimited JSON: one document per line (default, streaming).
    #[default]
    JsonLines,
    /// A single JSON array of documents.
    JsonArray,
    /// Comma-separated values with a header row (requires explicit fields).
    Csv,
}

impl ExportFormat {
    /// Parses a format name (case-insensitive): `jsonl`/`ndjson`, `json`,
    /// `csv`.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for an unrecognized name.
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "jsonl" | "ndjson" => Ok(Self::JsonLines),
            "json" => Ok(Self::JsonArray),
            "csv" => Ok(Self::Csv),
            other => Err(Error::config(format!(
                "unknown export format '{other}'; expected jsonl, json, or csv"
            ))),
        }
    }

    /// The conventional file-extension stem (without compression suffix).
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::JsonLines => "jsonl",
            Self::JsonArray => "json",
            Self::Csv => "csv",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_names() {
        assert_eq!(
            ExportFormat::parse("jsonl").unwrap(),
            ExportFormat::JsonLines
        );
        assert_eq!(
            ExportFormat::parse("NDJSON").unwrap(),
            ExportFormat::JsonLines
        );
        assert_eq!(
            ExportFormat::parse("json").unwrap(),
            ExportFormat::JsonArray
        );
        assert_eq!(ExportFormat::parse("csv").unwrap(), ExportFormat::Csv);
        assert!(ExportFormat::parse("xml").is_err());
    }

    #[test]
    fn extensions() {
        assert_eq!(ExportFormat::JsonLines.extension(), "jsonl");
        assert_eq!(ExportFormat::JsonArray.extension(), "json");
        assert_eq!(ExportFormat::Csv.extension(), "csv");
    }
}
