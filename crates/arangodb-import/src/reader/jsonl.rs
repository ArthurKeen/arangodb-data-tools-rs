//! Newline-delimited JSON reader.

use arangodb_tools_core::Error;
use async_stream::try_stream;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

use super::DocumentStream;

/// Reads one JSON value per line. Blank lines (after trimming) are skipped so
/// trailing newlines and human-edited files are tolerated.
pub(super) fn read<R>(reader: R) -> DocumentStream
where
    R: AsyncRead + Unpin + Send + 'static,
{
    Box::pin(try_stream! {
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        let mut line_no: u64 = 0;
        loop {
            line.clear();
            let read = reader.read_until(b'\n', &mut line).await?;
            if read == 0 {
                break;
            }
            line_no += 1;
            let trimmed = trim_ascii(&line);
            if trimmed.is_empty() {
                continue;
            }
            let value = serde_json::from_slice::<Value>(trimmed).map_err(|err| Error::Parse {
                message: format!("invalid JSON: {err}"),
                line: Some(line_no),
                column: Some(err.column() as u64),
            })?;
            yield value;
        }
    })
}

/// Trims leading and trailing ASCII whitespace (including `\r` and `\n`).
fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace());
    let Some(start) = start else {
        return &[];
    };
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .unwrap_or(start);
    &bytes[start..=end]
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn collect(input: &'static [u8]) -> Vec<Result<Value, Error>> {
        read(input).collect().await
    }

    #[tokio::test]
    async fn reads_each_line_as_document() {
        let docs = collect(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n").await;
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[1].as_ref().unwrap()["a"], 2);
    }

    #[tokio::test]
    async fn tolerates_blank_lines_and_missing_final_newline() {
        let docs = collect(b"\n{\"a\":1}\n\n  \n{\"a\":2}").await;
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].as_ref().unwrap()["a"], 1);
        assert_eq!(docs[1].as_ref().unwrap()["a"], 2);
    }

    #[tokio::test]
    async fn reports_line_number_on_parse_error() {
        let docs = collect(b"{\"a\":1}\n{not json}\n").await;
        assert_eq!(docs.len(), 2);
        match docs[1].as_ref().unwrap_err() {
            Error::Parse { line, .. } => assert_eq!(*line, Some(2)),
            other => panic!("expected parse error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handles_crlf_line_endings() {
        let docs = collect(b"{\"a\":1}\r\n{\"a\":2}\r\n").await;
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1].as_ref().unwrap()["a"], 2);
    }
}
