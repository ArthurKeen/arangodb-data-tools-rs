//! CSV/TSV reader.
//!
//! The first row is treated as the header; each subsequent row becomes a JSON
//! object keyed by the header fields. Field values are converted to JSON
//! numbers, booleans, and null when they unambiguously look like those types
//! (matching `arangoimport`'s default `--convert true` behavior); everything
//! else stays a string. Tokens with significant leading zeros (e.g. ZIP codes)
//! are preserved as strings.

use arangodb_tools_core::Error;
use async_stream::try_stream;
use csv_async::AsyncReaderBuilder;
use futures::StreamExt;
use serde_json::Value;
use tokio::io::AsyncRead;

use super::DocumentStream;

/// Reads delimited rows as JSON objects using the given field `delimiter`.
pub(super) fn read<R>(reader: R, delimiter: u8) -> DocumentStream
where
    R: AsyncRead + Unpin + Send + 'static,
{
    Box::pin(try_stream! {
        let mut csv = AsyncReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .flexible(true)
            .create_reader(reader);

        let headers = csv
            .headers()
            .await
            .map_err(map_csv_error)?
            .clone();

        let mut records = csv.into_records();
        let mut row_no: u64 = 1;
        while let Some(record) = records.next().await {
            row_no += 1;
            let record = record.map_err(|err| Error::Parse {
                message: format!("invalid delimited row: {err}"),
                line: Some(row_no),
                column: None,
            })?;

            let mut object = serde_json::Map::with_capacity(headers.len());
            for (name, field) in headers.iter().zip(record.iter()) {
                object.insert(name.to_owned(), infer_scalar(field));
            }
            yield Value::Object(object);
        }
    })
}

/// Maps a CSV error encountered while reading headers into a parse error.
fn map_csv_error(err: csv_async::Error) -> Error {
    Error::Parse {
        message: format!("invalid delimited header: {err}"),
        line: Some(1),
        column: None,
    }
}

/// Converts a raw field into a JSON scalar, preserving ambiguous tokens as
/// strings.
fn infer_scalar(field: &str) -> Value {
    if field.is_empty() {
        return Value::Null;
    }
    match field {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        "null" => return Value::Null,
        _ => {}
    }
    if looks_numeric(field) {
        if let Ok(int) = field.parse::<i64>() {
            return Value::from(int);
        }
        if let Ok(float) = field.parse::<f64>() {
            if float.is_finite() {
                return Value::from(float);
            }
        }
    }
    Value::String(field.to_owned())
}

/// Returns `true` if `token` should be treated as a number rather than a
/// string. Rejects tokens with significant leading zeros so identifiers like
/// `"007"` or `"0123"` keep their textual form.
fn looks_numeric(token: &str) -> bool {
    let digits = token.strip_prefix(['+', '-']).unwrap_or(token);
    let integer_part = digits.split(['.', 'e', 'E']).next().unwrap_or(digits);
    if integer_part.len() > 1 && integer_part.starts_with('0') {
        return false;
    }
    // A bare leading dot (".5") and other oddities are left to the numeric
    // parsers; this guard only filters leading-zero identifiers.
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use arangodb_tools_core::Result;

    async fn collect(input: &'static str, delimiter: u8) -> Vec<Result<Value>> {
        read(input.as_bytes(), delimiter).collect().await
    }

    #[tokio::test]
    async fn reads_csv_rows_as_objects() {
        let docs = collect("name,age\nalice,30\nbob,25\n", b',').await;
        assert_eq!(docs.len(), 2);
        let alice = docs[0].as_ref().unwrap();
        assert_eq!(alice["name"], "alice");
        assert_eq!(alice["age"], 30);
    }

    #[tokio::test]
    async fn converts_scalar_types() {
        let docs = collect("s,i,f,b,empty\nx,42,3.5,true,\n", b',').await;
        let row = docs[0].as_ref().unwrap();
        assert_eq!(row["s"], "x");
        assert_eq!(row["i"], 42);
        assert_eq!(row["f"], 3.5);
        assert_eq!(row["b"], Value::Bool(true));
        assert_eq!(row["empty"], Value::Null);
    }

    #[tokio::test]
    async fn preserves_leading_zero_tokens_as_strings() {
        let docs = collect("zip\n00123\n", b',').await;
        assert_eq!(docs[0].as_ref().unwrap()["zip"], "00123");
    }

    #[tokio::test]
    async fn handles_quoted_fields_with_delimiters_and_newlines() {
        let docs = collect("name,note\n\"a,b\",\"line1\nline2\"\n", b',').await;
        let row = docs[0].as_ref().unwrap();
        assert_eq!(row["name"], "a,b");
        assert_eq!(row["note"], "line1\nline2");
    }

    #[tokio::test]
    async fn reads_tsv_with_tab_delimiter() {
        let docs = collect("a\tb\n1\t2\n", b'\t').await;
        let row = docs[0].as_ref().unwrap();
        assert_eq!(row["a"], 1);
        assert_eq!(row["b"], 2);
    }
}
