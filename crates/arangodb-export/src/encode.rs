//! Encodes a document stream into an output byte stream.
//!
//! Encoders stream incrementally — they never buffer the whole result — so a
//! large export stays bounded by the configured cursor batch size, not the
//! collection size.

use arangodb_storage::ByteStream;
use arangodb_tools_core::{Error, Result};
use async_stream::try_stream;
use bytes::Bytes;
use futures::StreamExt;
use serde_json::Value;

use crate::format::ExportFormat;
use crate::DocumentStream;

/// Encodes `documents` as `format`, producing a byte stream ready to write.
///
/// `fields` is required for [`ExportFormat::Csv`] (the column order) and
/// ignored otherwise.
///
/// # Errors
/// Returns [`Error::Config`] if CSV is requested without fields.
pub fn encode(
    format: ExportFormat,
    fields: Option<Vec<String>>,
    documents: DocumentStream,
) -> Result<ByteStream> {
    match format {
        ExportFormat::JsonLines => Ok(encode_jsonl(documents)),
        ExportFormat::JsonArray => Ok(encode_json_array(documents)),
        ExportFormat::Csv => {
            let fields = fields
                .ok_or_else(|| Error::config("CSV export requires an explicit list of fields"))?;
            if fields.is_empty() {
                return Err(Error::config("CSV export requires at least one field"));
            }
            Ok(encode_csv(fields, documents))
        }
    }
}

/// One JSON document per line.
fn encode_jsonl(documents: DocumentStream) -> ByteStream {
    Box::pin(try_stream! {
        futures::pin_mut!(documents);
        while let Some(document) = documents.next().await {
            let document = document?;
            yield json_line(&document)?;
        }
    })
}

/// A single JSON array, streamed bracket-by-bracket.
fn encode_json_array(documents: DocumentStream) -> ByteStream {
    Box::pin(try_stream! {
        futures::pin_mut!(documents);
        let mut first = true;
        yield Bytes::from_static(b"[");
        while let Some(document) = documents.next().await {
            let document = document?;
            let mut chunk = Vec::new();
            if !first {
                chunk.push(b',');
            }
            first = false;
            chunk.extend_from_slice(&serde_json::to_vec(&document)?);
            yield Bytes::from(chunk);
        }
        yield Bytes::from_static(b"]");
    })
}

/// CSV with a header row, one record per document, projecting `fields`.
fn encode_csv(fields: Vec<String>, documents: DocumentStream) -> ByteStream {
    Box::pin(try_stream! {
        yield csv_header(&fields)?;
        futures::pin_mut!(documents);
        while let Some(document) = documents.next().await {
            let document = document?;
            yield csv_row(&fields, &document)?;
        }
    })
}

/// Encodes one document as a JSONL line (`{...}\n`).
///
/// # Errors
/// Returns an error if the document cannot be serialized.
pub(crate) fn json_line(document: &Value) -> Result<Bytes> {
    let mut line = serde_json::to_vec(document)?;
    line.push(b'\n');
    Ok(Bytes::from(line))
}

/// Encodes one document as a bare JSON value (a JSON-array element, no framing).
///
/// # Errors
/// Returns an error if the document cannot be serialized.
pub(crate) fn json_element(document: &Value) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(document)?)
}

/// Encodes the CSV header row for `fields`.
///
/// # Errors
/// Returns an error if the CSV writer fails.
pub(crate) fn csv_header(fields: &[String]) -> Result<Bytes> {
    csv_record(fields.iter().map(String::as_str))
}

/// Encodes one document as a CSV row projecting `fields`.
///
/// # Errors
/// Returns an error if the CSV writer fails.
pub(crate) fn csv_row(fields: &[String], document: &Value) -> Result<Bytes> {
    let cells: Vec<String> = fields.iter().map(|field| cell(document, field)).collect();
    csv_record(cells.iter().map(String::as_str))
}

/// Serializes one CSV record (with proper quoting) into a byte chunk.
fn csv_record<'a>(values: impl Iterator<Item = &'a str>) -> Result<Bytes> {
    let mut writer = csv::WriterBuilder::new().from_writer(Vec::new());
    writer
        .write_record(values)
        .map_err(|err| Error::Serialization(err.to_string()))?;
    writer
        .flush()
        .map_err(|err| Error::Serialization(err.to_string()))?;
    let bytes = writer
        .into_inner()
        .map_err(|err| Error::Serialization(err.to_string()))?;
    Ok(Bytes::from(bytes))
}

/// Renders a document field as a CSV cell value.
///
/// Strings (and stringified compound values) get a formula-injection guard;
/// numbers and booleans are emitted verbatim so they stay numeric.
fn cell(document: &Value, field: &str) -> String {
    match document.get(field) {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => guard(s.clone()),
        Some(Value::Bool(b)) => b.to_string(),
        Some(Value::Number(n)) => n.to_string(),
        Some(other) => guard(other.to_string()),
    }
}

/// Prefixes a leading formula trigger with `'` so spreadsheets do not execute
/// the cell (CSV-injection mitigation; PRD §8.3).
fn guard(value: String) -> String {
    if value.starts_with(['=', '+', '-', '@', '\t', '\r']) {
        format!("'{value}")
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn docs(values: Vec<Value>) -> DocumentStream {
        Box::pin(futures::stream::iter(values.into_iter().map(Ok)))
    }

    async fn collect(stream: ByteStream) -> Vec<u8> {
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn jsonl_one_doc_per_line() {
        let input = docs(vec![
            serde_json::json!({"n": 1}),
            serde_json::json!({"n": 2}),
        ]);
        let out = collect(encode(ExportFormat::JsonLines, None, input).unwrap()).await;
        assert_eq!(out, b"{\"n\":1}\n{\"n\":2}\n");
    }

    #[tokio::test]
    async fn json_array_brackets_and_commas() {
        let input = docs(vec![
            serde_json::json!({"n": 1}),
            serde_json::json!({"n": 2}),
        ]);
        let out = collect(encode(ExportFormat::JsonArray, None, input).unwrap()).await;
        let value: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(value, serde_json::json!([{"n":1},{"n":2}]));
    }

    #[tokio::test]
    async fn json_array_empty() {
        let out = collect(encode(ExportFormat::JsonArray, None, docs(vec![])).unwrap()).await;
        assert_eq!(out, b"[]");
    }

    #[tokio::test]
    async fn csv_projects_fields_with_header() {
        let input = docs(vec![
            serde_json::json!({"name": "alice", "age": 30, "extra": "ignored"}),
            serde_json::json!({"name": "bob", "age": 25}),
        ]);
        let fields = Some(vec!["name".to_string(), "age".to_string()]);
        let out = collect(encode(ExportFormat::Csv, fields, input).unwrap()).await;
        assert_eq!(out, b"name,age\nalice,30\nbob,25\n");
    }

    #[tokio::test]
    async fn csv_guards_formula_injection_but_not_numbers() {
        let input = docs(vec![serde_json::json!({"f": "=SUM(A1)", "n": -5})]);
        let fields = Some(vec!["f".to_string(), "n".to_string()]);
        let out = collect(encode(ExportFormat::Csv, fields, input).unwrap()).await;
        let text = String::from_utf8(out).unwrap();
        // The formula cell is neutralized with a leading apostrophe; the
        // negative number is left intact (not treated as a formula).
        assert!(text.contains("'=SUM(A1)"), "got: {text}");
        assert!(text.contains(",-5"), "got: {text}");
    }

    #[test]
    fn csv_without_fields_errors() {
        let input = docs(vec![]);
        assert!(encode(ExportFormat::Csv, None, input).is_err());
    }
}
