//! Incremental reader for a single top-level JSON array.
//!
//! Rather than buffering the whole file and parsing it at once, this reader
//! scans the byte stream and extracts each top-level array element as soon as
//! it is complete, then parses that element in isolation. Only one element is
//! held in memory at a time, so arbitrarily large arrays import with bounded
//! memory (subject to the size of a single element).

use arangodb_tools_core::{Error, Result};
use async_stream::try_stream;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::DocumentStream;

/// Size of each read from the underlying reader.
const CHUNK_SIZE: usize = 64 * 1024;

/// Reads the documents of a top-level JSON array, one at a time.
pub(super) fn read<R>(mut reader: R) -> DocumentStream
where
    R: AsyncRead + Unpin + Send + 'static,
{
    Box::pin(try_stream! {
        let mut scanner = ArrayScanner::new();
        let mut chunk = vec![0u8; CHUNK_SIZE];
        let mut index: u64 = 0;
        let mut completed: Vec<Vec<u8>> = Vec::new();

        loop {
            let read = reader.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            scanner.feed(&chunk[..read], &mut completed)?;
            for element in completed.drain(..) {
                index += 1;
                let value = serde_json::from_slice::<Value>(&element).map_err(|err| {
                    Error::Parse {
                        message: format!("invalid JSON array element: {err}"),
                        line: Some(index),
                        column: None,
                    }
                })?;
                yield value;
            }
        }
        scanner.finish()?;
    })
}

/// Structural state while scanning a JSON array for top-level elements.
struct ArrayScanner {
    started: bool,
    finished: bool,
    in_element: bool,
    depth: i32,
    in_string: bool,
    escaped: bool,
    current: Vec<u8>,
}

impl ArrayScanner {
    fn new() -> Self {
        Self {
            started: false,
            finished: false,
            in_element: false,
            depth: 0,
            in_string: false,
            escaped: false,
            current: Vec::new(),
        }
    }

    /// Feeds a chunk of bytes, appending any newly completed top-level elements
    /// (as raw byte slices) to `out`.
    fn feed(&mut self, bytes: &[u8], out: &mut Vec<Vec<u8>>) -> Result<()> {
        for &byte in bytes {
            if self.finished {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                return Err(Error::parse("trailing data after JSON array"));
            }

            if !self.started {
                if byte.is_ascii_whitespace() {
                    continue;
                }
                if byte == b'[' {
                    self.started = true;
                    continue;
                }
                return Err(Error::parse(format!(
                    "expected '[' at start of JSON array, found '{}'",
                    byte as char
                )));
            }

            if !self.in_element {
                if byte.is_ascii_whitespace() || byte == b',' {
                    continue;
                }
                if byte == b']' {
                    self.finished = true;
                    continue;
                }
                self.begin_element();
            }

            if self.consume_element_byte(byte) {
                out.push(std::mem::take(&mut self.current));
                self.in_element = false;
            }
        }
        Ok(())
    }

    fn begin_element(&mut self) {
        self.in_element = true;
        self.depth = 0;
        self.in_string = false;
        self.escaped = false;
        self.current.clear();
    }

    /// Processes one byte of the current element. Returns `true` when the
    /// element is complete (the terminating `,`/`]` is not part of it).
    fn consume_element_byte(&mut self, byte: u8) -> bool {
        if self.in_string {
            self.current.push(byte);
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.in_string = false;
            }
            return false;
        }

        if self.depth == 0 && (byte == b',' || byte == b']') {
            if byte == b']' {
                self.finished = true;
            }
            return true;
        }

        self.current.push(byte);
        match byte {
            b'"' => self.in_string = true,
            b'{' | b'[' => self.depth += 1,
            b'}' | b']' => self.depth -= 1,
            _ => {}
        }
        false
    }

    /// Validates that the array was properly terminated at end of input.
    fn finish(&self) -> Result<()> {
        if !self.started {
            return Err(Error::parse("expected a JSON array, found empty input"));
        }
        if !self.finished {
            return Err(Error::parse("unterminated JSON array"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn collect(input: &'static [u8]) -> Vec<Result<Value>> {
        read(input).collect().await
    }

    fn unwrap_all(results: Vec<Result<Value>>) -> Vec<Value> {
        results.into_iter().map(Result::unwrap).collect()
    }

    #[tokio::test]
    async fn reads_object_elements() {
        let docs = unwrap_all(collect(b"[{\"a\":1},{\"a\":2},{\"a\":3}]").await);
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[2]["a"], 3);
    }

    #[tokio::test]
    async fn reads_scalar_and_mixed_elements() {
        let docs = unwrap_all(collect(b"[1, \"two\", true, null, {\"k\":[1,2]}]").await);
        assert_eq!(docs.len(), 5);
        assert_eq!(docs[0], 1);
        assert_eq!(docs[1], "two");
        assert_eq!(docs[2], Value::Bool(true));
        assert_eq!(docs[3], Value::Null);
        assert_eq!(docs[4]["k"][1], 2);
    }

    #[tokio::test]
    async fn handles_commas_and_brackets_inside_strings() {
        let docs = unwrap_all(collect(b"[{\"s\":\"a,b]c\"},{\"s\":\"x\\\"y\"}]").await);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0]["s"], "a,b]c");
        assert_eq!(docs[1]["s"], "x\"y");
    }

    #[tokio::test]
    async fn handles_whitespace_and_newlines() {
        let docs = unwrap_all(collect(b"[\n  {\"a\":1},\n  {\"a\":2}\n]\n").await);
        assert_eq!(docs.len(), 2);
    }

    #[tokio::test]
    async fn empty_array_yields_nothing() {
        let docs = unwrap_all(collect(b"[]").await);
        assert!(docs.is_empty());
        let docs = unwrap_all(collect(b"  [  ]  ").await);
        assert!(docs.is_empty());
    }

    #[tokio::test]
    async fn rejects_non_array_input() {
        let results = collect(b"{\"a\":1}").await;
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn rejects_unterminated_array() {
        let results = collect(b"[{\"a\":1},{\"a\":2}").await;
        assert!(results.last().unwrap().is_err());
    }

    #[tokio::test]
    async fn rejects_trailing_data() {
        let results = collect(b"[1,2] junk").await;
        assert!(results.last().unwrap().is_err());
    }
}
