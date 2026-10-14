//! CSV/TSV reader.
//!
//! The first row is treated as the header; each subsequent row becomes a JSON
//! object keyed by the header fields. Field values are converted to JSON
//! numbers, booleans, and null when they unambiguously look like those types
//! (matching `arangoimport`'s default `--convert true` behavior); everything
//! else stays a string. Tokens with significant leading zeros (e.g. ZIP codes)
//! are preserved as strings.
//!
//! Attributes ArangoDB requires to be strings — `_key`, `_from`, `_to`, `_id`
//! — are never inferred: they take their column's raw text. A `_key` column of
//! `12345` sent as a JSON number is rejected by the server with "illegal
//! document key", so inferring it would make an ordinary CSV unloadable.
//!
//! Columns can additionally be reshaped during the load (see
//! [`super::columns`]), in the order `arangoimport` uses: infer, then merge,
//! then rename, then remove.

use arangodb_tools_core::Error;
use async_stream::try_stream;
use csv_async::AsyncReaderBuilder;
use futures::StreamExt;
use serde_json::Value;
use tokio::io::AsyncRead;

use super::columns::{is_reserved_string_field, ColumnTransforms, ColumnType};
use super::DocumentStream;

/// Reads delimited rows as JSON objects using the given field `delimiter`,
/// applying any column reshaping.
pub(super) fn read<R>(reader: R, delimiter: u8, transforms: ColumnTransforms) -> DocumentStream
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

            // 1. Decode every column under its ORIGINAL name. Merge templates
            //    reference source names, so they must be resolvable before any
            //    rename happens.
            let mut source = serde_json::Map::with_capacity(headers.len());
            for (name, field) in headers.iter().zip(record.iter()) {
                let target = transforms.target_name(name);
                let value = if is_reserved_string_field(target) {
                    // Reserved attributes take raw text, never an inferred
                    // number: the server refuses a numeric `_key`.
                    Value::String(field.to_owned())
                } else {
                    match transforms.column_type(name) {
                        Some(kind) => coerce(field, kind, name, row_no)?,
                        None => infer_scalar(field),
                    }
                };
                source.insert(name.to_owned(), value);
            }

            // 2. Merge, 3. rename, 4. remove.
            let mut object = serde_json::Map::with_capacity(headers.len());
            for (name, value) in &source {
                if transforms.is_removed(name) {
                    continue;
                }
                object.insert(transforms.target_name(name).to_owned(), value.clone());
            }
            transforms.apply_merges(&source, &mut object);

            yield Value::Object(object);
        }
    })
}

/// Converts a field to an explicitly requested type.
///
/// A value that cannot be represented is an error rather than a silent
/// fallback: the user asked for this type, and quietly storing something else
/// is how a load "succeeds" into a shape nothing downstream expects.
fn coerce(field: &str, kind: ColumnType, column: &str, row: u64) -> Result<Value, Error> {
    let bad = |expected: &str| Error::Parse {
        message: format!(
            "column '{column}' value '{field}' is not a valid {expected} \
             (requested by --datatype {column}={expected})"
        ),
        line: Some(row),
        column: None,
    };
    match kind {
        ColumnType::String => Ok(Value::String(field.to_owned())),
        ColumnType::Number => {
            if field.is_empty() {
                return Ok(Value::Null);
            }
            if let Ok(int) = field.parse::<i64>() {
                return Ok(Value::from(int));
            }
            match field.parse::<f64>() {
                Ok(float) if float.is_finite() => Ok(Value::from(float)),
                _ => Err(bad("number")),
            }
        }
        ColumnType::Boolean => match field {
            "" => Ok(Value::Null),
            "true" | "1" => Ok(Value::Bool(true)),
            "false" | "0" => Ok(Value::Bool(false)),
            _ => Err(bad("boolean")),
        },
    }
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
    use super::super::columns::ColumnTransforms;

    /// Reads with transforms, for the reshaping tests.
    fn read_with(input: &'static str, specs: (&[&str], &[&str], &[&str], &[&str])) -> Vec<Value> {
        let owned = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        let transforms = ColumnTransforms::new(
            &owned(specs.0),
            &owned(specs.1),
            &owned(specs.2),
            &owned(specs.3),
        )
        .expect("valid transforms");
        futures::executor::block_on(async {
            read(input.as_bytes(), b',', transforms)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .map(|r| r.expect("row parses"))
                .collect()
        })
    }

    /// The defect r2g reported: a numeric-looking key column became a JSON
    /// number, and ArangoDB rejects that with "illegal document key" (1221).
    /// Reserved attributes must take raw text instead.
    #[test]
    fn reserved_attributes_are_never_inferred_as_numbers() {
        let rows = read_with("_key,_from,_to,n\n12345,100,200,7\n", (&[], &[], &[], &[]));
        assert_eq!(rows[0]["_key"], Value::String("12345".into()));
        assert_eq!(rows[0]["_from"], Value::String("100".into()));
        assert_eq!(rows[0]["_to"], Value::String("200".into()));
        // An ordinary column is still inferred, or the fix would be too broad.
        assert_eq!(rows[0]["n"], Value::from(7));
    }

    /// The same protection must apply when a column is *renamed* into a
    /// reserved attribute, which is how a CSV dump actually supplies keys.
    #[test]
    fn a_column_translated_into_a_reserved_attribute_stays_a_string() {
        let rows = read_with("id,name\n12345,alice\n", (&[], &["id=_key"], &[], &[]));
        assert_eq!(rows[0]["_key"], Value::String("12345".into()));
        assert!(rows[0].get("id").is_none(), "the source name is gone");
    }

    #[test]
    fn datatype_forces_a_column_type() {
        let rows = read_with(
            "a,b,c\n42,1,x\n",
            (&["a=string", "b=boolean"], &[], &[], &[]),
        );
        assert_eq!(rows[0]["a"], Value::String("42".into()));
        assert_eq!(rows[0]["b"], Value::Bool(true));
        assert_eq!(rows[0]["c"], Value::String("x".into()));
    }

    /// A value that cannot be the requested type is an error, not a silent
    /// fallback to string.
    #[test]
    fn datatype_rejects_a_value_it_cannot_convert() {
        let owned = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        let transforms =
            ColumnTransforms::new(&owned(&["a=number"]), &[], &[], &[]).expect("valid");
        let rows: Vec<_> = futures::executor::block_on(async {
            read("a\nnot-a-number\n".as_bytes(), b',', transforms)
                .collect::<Vec<_>>()
                .await
        });
        let err = rows[0].as_ref().expect_err("must fail").to_string();
        assert!(err.contains("not a valid number"), "{err}");
        assert!(
            err.contains("--datatype"),
            "names the flag that asked: {err}"
        );
    }

    #[test]
    fn remove_attribute_drops_the_column() {
        let rows = read_with("a,b\n1,2\n", (&[], &[], &[], &["b"]));
        assert!(rows[0].get("b").is_none());
        assert_eq!(rows[0]["a"], Value::from(1));
    }

    /// Merging runs before renaming, so a template may reference a column that
    /// is also being renamed — the composite-key case.
    #[test]
    fn merge_runs_before_translate_so_both_survive() {
        let rows = read_with(
            "id,name\n7,alice\n",
            (&[], &["id=ident"], &["_key=[id]_[name]"], &[]),
        );
        assert_eq!(rows[0]["_key"], Value::String("7_alice".into()));
        assert_eq!(rows[0]["ident"], Value::from(7), "the rename still applied");
    }

    /// Leading zeros are preserved, unlike arangoimport which coerces `00789`
    /// to `789`. This is deliberate — losing them corrupts ZIP codes and
    /// zero-padded identifiers — and `--datatype col=string` reconciles the
    /// two tools when byte-for-byte parity is wanted.
    #[test]
    fn leading_zeros_are_preserved_and_datatype_reconciles_the_difference() {
        let plain = read_with("id\n00789\n", (&[], &[], &[], &[]));
        assert_eq!(plain[0]["id"], Value::String("00789".into()));

        let merged = read_with("id\n00789\n", (&["id=string"], &[], &["_key=[id]"], &[]));
        assert_eq!(merged[0]["_key"], Value::String("00789".into()));
    }

    use super::*;
    use arangodb_tools_core::Result;

    async fn collect(input: &'static str, delimiter: u8) -> Vec<Result<Value>> {
        read(input.as_bytes(), delimiter, ColumnTransforms::default())
            .collect()
            .await
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
