//! RDF serialization formats.

use arangodb_tools_core::{Error, Result};

/// A supported RDF input serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdfFormat {
    /// Line-based N-Triples (one `subject predicate object .` per line).
    NTriples,
    /// N-Quads: N-Triples with an optional fourth graph term per line.
    NQuads,
    /// Turtle (W3C): a practical subset (prefixes/base, `a`, predicate and
    /// object lists, blank-node property lists, collections, and typed,
    /// numeric, and boolean literals).
    Turtle,
}

impl RdfFormat {
    /// Parses a format name (case-insensitive): `ntriples`/`nt`, `nquads`/`nq`,
    /// `turtle`/`ttl`.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for an unrecognized name.
    pub fn parse(name: &str) -> Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "ntriples" | "nt" | "n-triples" => Ok(Self::NTriples),
            "nquads" | "nq" | "n-quads" => Ok(Self::NQuads),
            "turtle" | "ttl" => Ok(Self::Turtle),
            other => Err(Error::config(format!(
                "unknown RDF format '{other}'; expected ntriples, nquads, or turtle"
            ))),
        }
    }

    /// Infers the format from a file extension (ignoring a trailing
    /// compression suffix like `.gz`/`.zst`).
    ///
    /// # Errors
    /// Returns [`Error::Config`] if the extension is missing or unrecognized.
    pub fn infer_from_path(path: &str) -> Result<Self> {
        let trimmed = path
            .strip_suffix(".gz")
            .or_else(|| path.strip_suffix(".zst"))
            .unwrap_or(path);
        let ext = trimmed.rsplit('.').next().unwrap_or_default();
        match ext.to_ascii_lowercase().as_str() {
            "nt" => Ok(Self::NTriples),
            "nq" => Ok(Self::NQuads),
            "ttl" => Ok(Self::Turtle),
            _ => Err(Error::config(format!(
                "cannot infer RDF format from '{path}'; pass an explicit --format"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_names() {
        assert_eq!(RdfFormat::parse("nt").unwrap(), RdfFormat::NTriples);
        assert_eq!(RdfFormat::parse("N-Quads").unwrap(), RdfFormat::NQuads);
        assert_eq!(RdfFormat::parse("turtle").unwrap(), RdfFormat::Turtle);
        assert!(RdfFormat::parse("rdfxml").is_err());
    }

    #[test]
    fn infers_from_path() {
        assert_eq!(
            RdfFormat::infer_from_path("data.nt").unwrap(),
            RdfFormat::NTriples
        );
        assert_eq!(
            RdfFormat::infer_from_path("data.nq.gz").unwrap(),
            RdfFormat::NQuads
        );
        assert!(RdfFormat::infer_from_path("data.json").is_err());
    }
}
