//! Streaming parser for the line-based RDF formats (N-Triples and N-Quads).
//!
//! Each line is `subject predicate object [graph] .`. Parsing is streaming and
//! memory-bounded by a single line; blank lines and `#` comments are skipped.
//! Turtle is recognized by [`RdfFormat`] but not parsed here (it needs a full
//! grammar with prefix handling); callers get a clear error.

use arangodb_tools_core::{Error, Result};
use futures::Stream;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

use crate::format::RdfFormat;
use crate::model::{RdfResource, RdfTerm, RdfTriple};

/// Parses `reader` as RDF in `format`, yielding one [`RdfTriple`] per
/// statement. Errors carry the 1-based line number.
///
/// The returned stream is lazy: nothing is read until it is polled.
pub fn read_rdf_triples<R>(
    reader: R,
    format: RdfFormat,
) -> impl Stream<Item = Result<RdfTriple>> + Send
where
    R: AsyncRead + Unpin + Send + 'static,
{
    async_stream::try_stream! {
        ensure_line_based(format)?;

        let mut lines = BufReader::new(reader).lines();
        let mut line_no: u64 = 0;
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|err| Error::config(format!("error reading RDF input: {err}")))?
        {
            line_no += 1;
            let statement = parse_statement(&line, format)
                .map_err(|err| Error::config(format!("RDF parse error on line {line_no}: {err}")))?;
            if let Some(triple) = statement {
                yield triple;
            }
        }
    }
}

/// Rejects formats this parser does not handle.
fn ensure_line_based(format: RdfFormat) -> Result<()> {
    match format {
        RdfFormat::NTriples | RdfFormat::NQuads => Ok(()),
        RdfFormat::Turtle => Err(Error::config(
            "Turtle parsing is not implemented yet; use ntriples or nquads",
        )),
    }
}

/// Parses one line into a statement, or `None` for a blank/comment line.
fn parse_statement(line: &str, format: RdfFormat) -> Result<Option<RdfTriple>> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }

    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;

    skip_ws(&chars, &mut i);
    let subject = read_resource(&chars, &mut i)?;

    skip_ws(&chars, &mut i);
    if chars.get(i) != Some(&'<') {
        return Err(Error::config("predicate must be an IRI"));
    }
    let predicate = read_iri(&chars, &mut i)?;

    skip_ws(&chars, &mut i);
    let object = read_object(&chars, &mut i)?;

    skip_ws(&chars, &mut i);
    let mut graph = None;
    if chars.get(i) != Some(&'.') {
        if format == RdfFormat::NQuads {
            graph = Some(match read_resource(&chars, &mut i)? {
                RdfResource::Iri(iri) => iri,
                RdfResource::BlankNode(label) => format!("_:{label}"),
            });
            skip_ws(&chars, &mut i);
        } else {
            return Err(Error::config("unexpected token after object; expected '.'"));
        }
    }

    if chars.get(i) != Some(&'.') {
        return Err(Error::config("statement must end with '.'"));
    }

    Ok(Some(RdfTriple {
        subject,
        predicate,
        object,
        graph,
    }))
}

/// Advances past spaces and tabs.
fn skip_ws(chars: &[char], i: &mut usize) {
    while matches!(chars.get(*i), Some(' ') | Some('\t')) {
        *i += 1;
    }
}

/// Reads a subject/graph resource (IRI or blank node).
fn read_resource(chars: &[char], i: &mut usize) -> Result<RdfResource> {
    match chars.get(*i) {
        Some('<') => Ok(RdfResource::Iri(read_iri(chars, i)?)),
        Some('_') => Ok(RdfResource::BlankNode(read_blank(chars, i)?)),
        _ => Err(Error::config("expected an IRI or blank node")),
    }
}

/// Reads an object term (IRI, blank node, or literal).
fn read_object(chars: &[char], i: &mut usize) -> Result<RdfTerm> {
    match chars.get(*i) {
        Some('<') => Ok(RdfTerm::Iri(read_iri(chars, i)?)),
        Some('_') => Ok(RdfTerm::BlankNode(read_blank(chars, i)?)),
        Some('"') => read_literal(chars, i),
        _ => Err(Error::config(
            "expected an IRI, blank node, or literal object",
        )),
    }
}

/// Reads `<iri>` starting at `chars[*i] == '<'`.
fn read_iri(chars: &[char], i: &mut usize) -> Result<String> {
    if chars.get(*i) != Some(&'<') {
        return Err(Error::config("expected '<' to start an IRI"));
    }
    *i += 1;
    let mut raw = String::new();
    loop {
        let c = *chars
            .get(*i)
            .ok_or_else(|| Error::config("unterminated IRI"))?;
        *i += 1;
        if c == '>' {
            break;
        }
        if c == '\\' {
            let next = *chars
                .get(*i)
                .ok_or_else(|| Error::config("dangling escape in IRI"))?;
            *i += 1;
            raw.push('\\');
            raw.push(next);
        } else {
            raw.push(c);
        }
    }
    decode_escapes(&raw)
}

/// Reads `_:label` starting at `chars[*i] == '_'`.
fn read_blank(chars: &[char], i: &mut usize) -> Result<String> {
    if chars.get(*i) != Some(&'_') || chars.get(*i + 1) != Some(&':') {
        return Err(Error::config("expected '_:' to start a blank node"));
    }
    *i += 2;
    let start = *i;
    while let Some(&c) = chars.get(*i) {
        if c.is_whitespace() || c == '<' || c == '"' {
            break;
        }
        *i += 1;
    }
    if *i == start {
        return Err(Error::config("empty blank node label"));
    }
    Ok(chars[start..*i].iter().collect())
}

/// Reads a `"..."` literal (with optional `^^<datatype>` or `@lang`).
fn read_literal(chars: &[char], i: &mut usize) -> Result<RdfTerm> {
    if chars.get(*i) != Some(&'"') {
        return Err(Error::config("expected '\"' to start a literal"));
    }
    *i += 1;
    let mut raw = String::new();
    loop {
        let c = *chars
            .get(*i)
            .ok_or_else(|| Error::config("unterminated literal"))?;
        *i += 1;
        if c == '"' {
            break;
        }
        if c == '\\' {
            let next = *chars
                .get(*i)
                .ok_or_else(|| Error::config("dangling escape in literal"))?;
            *i += 1;
            raw.push('\\');
            raw.push(next);
        } else {
            raw.push(c);
        }
    }
    let value = decode_escapes(&raw)?;

    let mut datatype = None;
    let mut language = None;
    match chars.get(*i) {
        Some('^') if chars.get(*i + 1) == Some(&'^') => {
            *i += 2;
            datatype = Some(read_iri(chars, i)?);
        }
        Some('@') => {
            *i += 1;
            let start = *i;
            while let Some(&c) = chars.get(*i) {
                if c.is_ascii_alphanumeric() || c == '-' {
                    *i += 1;
                } else {
                    break;
                }
            }
            if *i == start {
                return Err(Error::config("empty language tag"));
            }
            language = Some(chars[start..*i].iter().collect());
        }
        _ => {}
    }

    Ok(RdfTerm::Literal {
        value,
        datatype,
        language,
    })
}

/// Decodes N-Triples string/IRI escapes (`\t \b \n \r \f \" \' \\` and
/// `\uXXXX` / `\UXXXXXXXX`).
fn decode_escapes(raw: &str) -> Result<String> {
    if !raw.contains('\\') {
        return Ok(raw.to_string());
    }
    let mut out = String::with_capacity(raw.len());
    let mut it = raw.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match it.next() {
            Some('t') => out.push('\t'),
            Some('b') => out.push('\u{08}'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('f') => out.push('\u{0C}'),
            Some('"') => out.push('"'),
            Some('\'') => out.push('\''),
            Some('\\') => out.push('\\'),
            Some('u') => out.push(read_hex(&mut it, 4)?),
            Some('U') => out.push(read_hex(&mut it, 8)?),
            Some(other) => return Err(Error::config(format!("invalid escape '\\{other}'"))),
            None => return Err(Error::config("dangling escape backslash")),
        }
    }
    Ok(out)
}

/// Reads `n` hex digits and returns the corresponding character.
fn read_hex(it: &mut std::str::Chars<'_>, n: usize) -> Result<char> {
    let mut code: u32 = 0;
    for _ in 0..n {
        let c = it
            .next()
            .ok_or_else(|| Error::config("truncated unicode escape"))?;
        let digit = c
            .to_digit(16)
            .ok_or_else(|| Error::config("invalid hex digit in unicode escape"))?;
        code = code * 16 + digit;
    }
    char::from_u32(code).ok_or_else(|| Error::config("invalid unicode code point in escape"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn parse_all(input: &str, format: RdfFormat) -> Result<Vec<RdfTriple>> {
        let reader = std::io::Cursor::new(input.as_bytes().to_vec());
        let stream = read_rdf_triples(reader, format);
        futures::pin_mut!(stream);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn parses_ntriples_iri_and_literal() {
        let input = concat!(
            "# a comment\n",
            "<http://a/s> <http://a/p> <http://a/o> .\n",
            "\n",
            "<http://a/s> <http://a/name> \"Alice\" .\n",
        );
        let triples = parse_all(input, RdfFormat::NTriples).await.unwrap();
        assert_eq!(triples.len(), 2);
        assert_eq!(
            triples[0].subject,
            RdfResource::Iri("http://a/s".to_string())
        );
        assert_eq!(triples[0].object, RdfTerm::Iri("http://a/o".to_string()));
        assert_eq!(
            triples[1].object,
            RdfTerm::Literal {
                value: "Alice".to_string(),
                datatype: None,
                language: None,
            }
        );
    }

    #[tokio::test]
    async fn parses_literal_with_language_and_datatype() {
        let input = concat!(
            "<http://a/s> <http://a/p> \"chat\"@fr .\n",
            "<http://a/s> <http://a/n> \"42\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
        );
        let triples = parse_all(input, RdfFormat::NTriples).await.unwrap();
        assert_eq!(
            triples[0].object,
            RdfTerm::Literal {
                value: "chat".to_string(),
                datatype: None,
                language: Some("fr".to_string()),
            }
        );
        match &triples[1].object {
            RdfTerm::Literal {
                value, datatype, ..
            } => {
                assert_eq!(value, "42");
                assert_eq!(
                    datatype.as_deref(),
                    Some("http://www.w3.org/2001/XMLSchema#integer")
                );
            }
            other => panic!("expected literal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn parses_blank_nodes_and_escapes() {
        let input = "_:b0 <http://a/p> \"line1\\nline2 \\\"q\\\"\" .\n";
        let triples = parse_all(input, RdfFormat::NTriples).await.unwrap();
        assert_eq!(triples[0].subject, RdfResource::BlankNode("b0".to_string()));
        assert_eq!(
            triples[0].object,
            RdfTerm::Literal {
                value: "line1\nline2 \"q\"".to_string(),
                datatype: None,
                language: None,
            }
        );
    }

    #[tokio::test]
    async fn parses_nquads_graph() {
        let input = "<http://a/s> <http://a/p> <http://a/o> <http://a/g> .\n";
        let triples = parse_all(input, RdfFormat::NQuads).await.unwrap();
        assert_eq!(triples[0].graph.as_deref(), Some("http://a/g"));
    }

    #[tokio::test]
    async fn ntriples_rejects_a_fourth_term() {
        let input = "<http://a/s> <http://a/p> <http://a/o> <http://a/g> .\n";
        assert!(parse_all(input, RdfFormat::NTriples).await.is_err());
    }

    #[tokio::test]
    async fn reports_line_number_on_error() {
        let input = "<http://a/s> <http://a/p> <http://a/o> .\nbogus line\n";
        let err = parse_all(input, RdfFormat::NTriples).await.unwrap_err();
        assert!(err.to_string().contains("line 2"), "got: {err}");
    }

    #[tokio::test]
    async fn turtle_is_rejected() {
        assert!(parse_all("@prefix x: <y> .", RdfFormat::Turtle)
            .await
            .is_err());
    }
}
