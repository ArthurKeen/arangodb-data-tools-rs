//! A hand-written parser for a practical subset of W3C Turtle.
//!
//! Turtle is not line-oriented (it carries prefix/base state and nests blank
//! nodes and collections), so the whole document is parsed at once into a
//! `Vec<RdfTriple>`; [`crate::read_rdf_triples`] buffers the input and streams
//! the results out.
//!
//! # Supported
//! - `@prefix` / `@base` directives and the SPARQL-style `PREFIX` / `BASE`.
//! - IRIs (`<...>`), prefixed names (`ex:name`, `:name`, `ex:`), and the `a`
//!   keyword (`rdf:type`).
//! - Predicate lists (`;`) and object lists (`,`).
//! - Blank node labels (`_:b`), anonymous blank-node property lists (`[ ... ]`),
//!   and collections (`( ... )`, expanded to `rdf:first`/`rdf:rest`/`rdf:nil`).
//! - String literals (single-, and triple-quoted, `"` or `'`) with escapes,
//!   language tags (`@en`), and datatypes (`^^<iri>` / `^^ex:name`); numeric
//!   literals (integer/decimal/double) and booleans.
//!
//! # Not supported
//! - RDF-star (`<< >>`) and Turtle-1.2 additions.
//! - Full RFC 3987 relative-IRI resolution: `@base` is applied by simple
//!   concatenation, which covers the common `@base` + fragment/path cases.

use std::collections::HashMap;

use arangodb_tools_core::{Error, Result};

use crate::model::{RdfResource, RdfTerm, RdfTriple};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";

/// Parses a complete Turtle document into triples.
///
/// # Errors
/// Returns [`Error::Config`] with a line/column on any syntax error.
pub(crate) fn parse_turtle(input: &str) -> Result<Vec<RdfTriple>> {
    Parser::new(input).parse_document()
}

/// A subject or object that is either an IRI or a blank node (used while
/// building nested structures).
#[derive(Debug, Clone)]
enum Node {
    Iri(String),
    Blank(String),
}

impl Node {
    fn into_resource(self) -> RdfResource {
        match self {
            Node::Iri(iri) => RdfResource::Iri(iri),
            Node::Blank(label) => RdfResource::BlankNode(label),
        }
    }

    fn into_term(self) -> RdfTerm {
        match self {
            Node::Iri(iri) => RdfTerm::Iri(iri),
            Node::Blank(label) => RdfTerm::BlankNode(label),
        }
    }
}

/// A parsed object: a node (IRI/blank) or a literal term.
enum Object {
    Node(Node),
    Literal(RdfTerm),
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
    base: Option<String>,
    prefixes: HashMap<String, String>,
    blank_counter: usize,
    triples: Vec<RdfTriple>,
}

impl Parser {
    fn new(input: &str) -> Self {
        Self {
            chars: input.chars().collect(),
            pos: 0,
            base: None,
            prefixes: HashMap::new(),
            blank_counter: 0,
            triples: Vec::new(),
        }
    }

    fn parse_document(mut self) -> Result<Vec<RdfTriple>> {
        loop {
            self.skip_ws();
            let Some(c) = self.peek() else { break };
            if c == '@' {
                self.directive()?;
            } else if self.matches_keyword_ci("prefix") || self.matches_keyword_ci("base") {
                self.sparql_directive()?;
            } else {
                self.statement()?;
            }
        }
        Ok(self.triples)
    }

    // --- directives -------------------------------------------------------

    fn directive(&mut self) -> Result<()> {
        self.expect('@')?;
        let word = self.read_bare_word();
        match word.as_str() {
            "prefix" => {
                self.skip_ws();
                let (name, iri) = self.read_prefix_binding()?;
                self.prefixes.insert(name, iri);
            }
            "base" => {
                self.skip_ws();
                let iri = self.read_iri_ref()?;
                self.base = Some(iri);
            }
            other => return Err(self.error(format!("unknown directive '@{other}'"))),
        }
        self.skip_ws();
        self.expect('.')?;
        Ok(())
    }

    fn sparql_directive(&mut self) -> Result<()> {
        let word = self.read_bare_word().to_ascii_lowercase();
        self.skip_ws();
        match word.as_str() {
            "prefix" => {
                let (name, iri) = self.read_prefix_binding()?;
                self.prefixes.insert(name, iri);
            }
            "base" => {
                let iri = self.read_iri_ref()?;
                self.base = Some(iri);
            }
            _ => unreachable!("guarded by matches_keyword_ci"),
        }
        Ok(())
    }

    /// Reads `prefix: <iri>` (the prefix label may be empty).
    fn read_prefix_binding(&mut self) -> Result<(String, String)> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == ':' {
                break;
            }
            if c.is_whitespace() {
                return Err(self.error("expected ':' after prefix label"));
            }
            self.pos += 1;
        }
        let name: String = self.chars[start..self.pos].iter().collect();
        self.expect(':')?;
        self.skip_ws();
        let iri = self.read_iri_ref()?;
        Ok((name, iri))
    }

    // --- statements -------------------------------------------------------

    fn statement(&mut self) -> Result<()> {
        let subject = self.subject()?;
        self.skip_ws();
        self.predicate_object_list(&subject)?;
        self.skip_ws();
        self.expect('.')?;
        Ok(())
    }

    fn subject(&mut self) -> Result<Node> {
        self.skip_ws();
        match self.peek() {
            Some('[') => self.blank_node_property_list(),
            Some('(') => self.collection(),
            _ => Ok(self.iri_or_blank()?),
        }
    }

    /// Parses `verb objectList (';' verb objectList)*` for `subject`, with an
    /// optional trailing `;`. Also used inside `[ ... ]`.
    fn predicate_object_list(&mut self, subject: &Node) -> Result<()> {
        loop {
            self.skip_ws();
            let predicate = self.verb()?;
            self.object_list(subject, &predicate)?;
            self.skip_ws();
            if self.peek() == Some(';') {
                self.pos += 1;
                self.skip_ws();
                // Allow a trailing ';' before '.' or ']'.
                if matches!(self.peek(), Some('.') | Some(']') | None) {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn verb(&mut self) -> Result<String> {
        self.skip_ws();
        if self.peek() == Some('a') && self.is_token_boundary(self.pos + 1) {
            self.pos += 1;
            return Ok(RDF_TYPE.to_string());
        }
        match self.iri_or_blank()? {
            Node::Iri(iri) => Ok(iri),
            Node::Blank(_) => Err(self.error("a predicate must be an IRI, not a blank node")),
        }
    }

    fn object_list(&mut self, subject: &Node, predicate: &str) -> Result<()> {
        loop {
            self.skip_ws();
            let object = self.object()?;
            let term = match object {
                Object::Node(node) => node.into_term(),
                Object::Literal(term) => term,
            };
            self.triples.push(RdfTriple {
                subject: subject.clone().into_resource(),
                predicate: predicate.to_string(),
                object: term,
                graph: None,
            });
            self.skip_ws();
            if self.peek() == Some(',') {
                self.pos += 1;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn object(&mut self) -> Result<Object> {
        self.skip_ws();
        match self.peek() {
            Some('[') => Ok(Object::Node(self.blank_node_property_list()?)),
            Some('(') => Ok(Object::Node(self.collection()?)),
            Some('"') | Some('\'') => Ok(Object::Literal(self.string_literal()?)),
            Some(c) if c == '+' || c == '-' || c == '.' || c.is_ascii_digit() => {
                Ok(Object::Literal(self.numeric_literal()?))
            }
            Some('t') | Some('f') if self.peek_boolean() => {
                Ok(Object::Literal(self.boolean_literal()?))
            }
            _ => Ok(Object::Node(self.iri_or_blank()?)),
        }
    }

    // --- blank nodes & collections ---------------------------------------

    fn blank_node_property_list(&mut self) -> Result<Node> {
        self.expect('[')?;
        let node = self.fresh_blank();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.pos += 1;
            return Ok(node);
        }
        self.predicate_object_list(&node)?;
        self.skip_ws();
        self.expect(']')?;
        Ok(node)
    }

    fn collection(&mut self) -> Result<Node> {
        self.expect('(')?;
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                Some(')') => {
                    self.pos += 1;
                    break;
                }
                None => return Err(self.error("unterminated collection")),
                _ => {
                    let object = self.object()?;
                    items.push(match object {
                        Object::Node(node) => node.into_term(),
                        Object::Literal(term) => term,
                    });
                }
            }
        }
        if items.is_empty() {
            return Ok(Node::Iri(RDF_NIL.to_string()));
        }
        // Build the rdf:first/rdf:rest chain: each list cell is a blank node
        // with an rdf:first (the item) and an rdf:rest (the next cell, or
        // rdf:nil for the last).
        let head = self.fresh_blank();
        let mut current = head.clone();
        let len = items.len();
        for (i, item) in items.into_iter().enumerate() {
            self.triples.push(RdfTriple {
                subject: current.clone().into_resource(),
                predicate: RDF_FIRST.to_string(),
                object: item,
                graph: None,
            });
            let rest = if i + 1 == len {
                RdfTerm::Iri(RDF_NIL.to_string())
            } else {
                self.fresh_blank().into_term()
            };
            self.triples.push(RdfTriple {
                subject: current.clone().into_resource(),
                predicate: RDF_REST.to_string(),
                object: rest.clone(),
                graph: None,
            });
            if let RdfTerm::BlankNode(label) = rest {
                current = Node::Blank(label);
            }
        }
        Ok(head)
    }

    fn fresh_blank(&mut self) -> Node {
        self.blank_counter += 1;
        Node::Blank(format!("_bnl{}", self.blank_counter))
    }

    // --- terms ------------------------------------------------------------

    fn iri_or_blank(&mut self) -> Result<Node> {
        self.skip_ws();
        match self.peek() {
            Some('<') => Ok(Node::Iri(self.read_iri_ref()?)),
            Some('_') => Ok(Node::Blank(self.read_blank_label()?)),
            Some(_) => Ok(Node::Iri(self.read_prefixed_name()?)),
            None => Err(self.error("expected a term")),
        }
    }

    fn read_iri_ref(&mut self) -> Result<String> {
        self.expect('<')?;
        let mut raw = String::new();
        loop {
            let c = self
                .next_char()
                .ok_or_else(|| self.error("unterminated IRI"))?;
            match c {
                '>' => break,
                '\\' => {
                    let n = self
                        .next_char()
                        .ok_or_else(|| self.error("bad IRI escape"))?;
                    match n {
                        'u' => raw.push(self.read_hex(4)?),
                        'U' => raw.push(self.read_hex(8)?),
                        other => return Err(self.error(format!("invalid IRI escape '\\{other}'"))),
                    }
                }
                other => raw.push(other),
            }
        }
        Ok(self.resolve_iri(&raw))
    }

    fn read_blank_label(&mut self) -> Result<String> {
        self.expect('_')?;
        self.expect(':')?;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if is_pn_local_char(c) {
                self.pos += 1;
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(self.error("empty blank node label"));
        }
        // Namespace user labels so they cannot collide with generated ones.
        Ok(format!(
            "u_{}",
            self.chars[start..self.pos].iter().collect::<String>()
        ))
    }

    fn read_prefixed_name(&mut self) -> Result<String> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == ':' {
                break;
            }
            if is_pn_prefix_char(c) {
                self.pos += 1;
            } else {
                return Err(self.error(format!("unexpected character '{c}' in term")));
            }
        }
        let prefix: String = self.chars[start..self.pos].iter().collect();
        self.expect(':')?;
        let local_start = self.pos;
        while let Some(c) = self.peek() {
            if is_pn_local_char(c) {
                self.pos += 1;
            } else {
                break;
            }
        }
        let local: String = self.chars[local_start..self.pos].iter().collect();
        // A prefixed local part may not end with '.', which terminates a
        // statement; give it back if we over-read.
        let local = local.trim_end_matches('.');
        let trimmed = self.pos - (local_start + local.chars().count());
        self.pos -= trimmed;

        match self.prefixes.get(&prefix) {
            Some(namespace) => Ok(format!("{namespace}{local}")),
            None => Err(self.error(format!("unknown prefix '{prefix}:'"))),
        }
    }

    fn string_literal(&mut self) -> Result<RdfTerm> {
        let quote = self.next_char().expect("caller checked quote");
        let triple = self.peek() == Some(quote) && self.peek_at(1) == Some(quote);
        if triple {
            self.pos += 2;
        }
        let value = self.read_string_body(quote, triple)?;

        let mut datatype = None;
        let mut language = None;
        match self.peek() {
            Some('@') => {
                self.pos += 1;
                let start = self.pos;
                while let Some(c) = self.peek() {
                    if c.is_ascii_alphanumeric() || c == '-' {
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                if self.pos == start {
                    return Err(self.error("empty language tag"));
                }
                language = Some(self.chars[start..self.pos].iter().collect());
            }
            Some('^') if self.peek_at(1) == Some('^') => {
                self.pos += 2;
                self.skip_ws();
                datatype = Some(match self.peek() {
                    Some('<') => self.read_iri_ref()?,
                    _ => self.read_prefixed_name()?,
                });
            }
            _ => {}
        }

        Ok(RdfTerm::Literal {
            value,
            datatype,
            language,
        })
    }

    fn read_string_body(&mut self, quote: char, triple: bool) -> Result<String> {
        let mut value = String::new();
        loop {
            let c = self
                .next_char()
                .ok_or_else(|| self.error("unterminated string literal"))?;
            if c == quote {
                if !triple {
                    return Ok(value);
                }
                if self.peek() == Some(quote) && self.peek_at(1) == Some(quote) {
                    self.pos += 2;
                    return Ok(value);
                }
                value.push(c);
                continue;
            }
            if c == '\\' {
                value.push(self.read_string_escape()?);
            } else {
                value.push(c);
            }
        }
    }

    fn read_string_escape(&mut self) -> Result<char> {
        let c = self
            .next_char()
            .ok_or_else(|| self.error("dangling escape in string"))?;
        Ok(match c {
            't' => '\t',
            'b' => '\u{08}',
            'n' => '\n',
            'r' => '\r',
            'f' => '\u{0C}',
            '"' => '"',
            '\'' => '\'',
            '\\' => '\\',
            'u' => self.read_hex(4)?,
            'U' => self.read_hex(8)?,
            other => return Err(self.error(format!("invalid string escape '\\{other}'"))),
        })
    }

    fn numeric_literal(&mut self) -> Result<RdfTerm> {
        let start = self.pos;
        if matches!(self.peek(), Some('+') | Some('-')) {
            self.pos += 1;
        }
        let mut has_dot = false;
        let mut has_exp = false;
        while let Some(c) = self.peek() {
            match c {
                '0'..='9' => self.pos += 1,
                '.' if !has_dot && !has_exp => {
                    // A '.' is only part of the number if a digit follows;
                    // otherwise it terminates the statement.
                    if matches!(self.peek_at(1), Some(d) if d.is_ascii_digit()) {
                        has_dot = true;
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                'e' | 'E' if !has_exp => {
                    has_exp = true;
                    self.pos += 1;
                    if matches!(self.peek(), Some('+') | Some('-')) {
                        self.pos += 1;
                    }
                }
                _ => break,
            }
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if !text.chars().any(|c| c.is_ascii_digit()) {
            return Err(self.error("expected an object"));
        }
        let datatype = if has_exp {
            XSD_DOUBLE
        } else if has_dot {
            XSD_DECIMAL
        } else {
            XSD_INTEGER
        };
        Ok(RdfTerm::Literal {
            value: text,
            datatype: Some(datatype.to_string()),
            language: None,
        })
    }

    fn boolean_literal(&mut self) -> Result<RdfTerm> {
        let word = self.read_bare_word();
        if word == "true" || word == "false" {
            Ok(RdfTerm::Literal {
                value: word,
                datatype: Some(XSD_BOOLEAN.to_string()),
                language: None,
            })
        } else {
            Err(self.error(format!("expected a term, found '{word}'")))
        }
    }

    // --- lexing helpers ---------------------------------------------------

    fn peek_boolean(&self) -> bool {
        for kw in ["true", "false"] {
            if self.chars[self.pos..]
                .iter()
                .take(kw.len())
                .collect::<String>()
                == kw
                && self.is_token_boundary(self.pos + kw.len())
            {
                return true;
            }
        }
        false
    }

    fn resolve_iri(&self, raw: &str) -> String {
        if has_scheme(raw) {
            return raw.to_string();
        }
        match &self.base {
            Some(base) => format!("{base}{raw}"),
            None => raw.to_string(),
        }
    }

    fn read_hex(&mut self, n: usize) -> Result<char> {
        let mut code: u32 = 0;
        for _ in 0..n {
            let c = self
                .next_char()
                .ok_or_else(|| self.error("truncated unicode escape"))?;
            let digit = c
                .to_digit(16)
                .ok_or_else(|| self.error("invalid hex in unicode escape"))?;
            code = code * 16 + digit;
        }
        char::from_u32(code).ok_or_else(|| self.error("invalid unicode code point"))
    }

    /// Reads a run of ASCII-alphabetic characters (for keywords/directives).
    fn read_bare_word(&mut self) -> String {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.chars[start..self.pos].iter().collect()
    }

    fn matches_keyword_ci(&self, keyword: &str) -> bool {
        let end = self.pos + keyword.len();
        if end > self.chars.len() {
            return false;
        }
        let candidate: String = self.chars[self.pos..end].iter().collect();
        candidate.eq_ignore_ascii_case(keyword) && self.is_token_boundary(end)
    }

    fn is_token_boundary(&self, at: usize) -> bool {
        match self.chars.get(at) {
            None => true,
            Some(c) => c.is_whitespace() || matches!(c, '<' | '"' | '\'' | '.' | ';' | ',' | '#'),
        }
    }

    fn skip_ws(&mut self) {
        loop {
            match self.peek() {
                Some(c) if c.is_whitespace() => self.pos += 1,
                Some('#') => {
                    while let Some(c) = self.peek() {
                        self.pos += 1;
                        if c == '\n' {
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, offset: usize) -> Option<char> {
        self.chars.get(self.pos + offset).copied()
    }

    fn next_char(&mut self) -> Option<char> {
        let c = self.chars.get(self.pos).copied();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn expect(&mut self, expected: char) -> Result<()> {
        match self.next_char() {
            Some(c) if c == expected => Ok(()),
            Some(c) => Err(self.error(format!("expected '{expected}', found '{c}'"))),
            None => Err(self.error(format!("expected '{expected}', found end of input"))),
        }
    }

    /// Builds a positioned error message.
    fn error(&self, message: impl Into<String>) -> Error {
        let mut line = 1usize;
        let mut col = 1usize;
        for &c in &self.chars[..self.pos.min(self.chars.len())] {
            if c == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
        }
        Error::config(format!(
            "Turtle parse error at line {line}, column {col}: {}",
            message.into()
        ))
    }
}

/// Whether `s` looks like an absolute IRI (starts with `scheme:`).
fn has_scheme(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() => {}
        _ => return false,
    }
    let mut saw_colon = false;
    for c in chars {
        if c == ':' {
            saw_colon = true;
            break;
        }
        if !(c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) {
            return false;
        }
    }
    saw_colon
}

/// Characters allowed in a prefix label (approximation of PN_PREFIX).
fn is_pn_prefix_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.') || c as u32 > 0x7F
}

/// Characters allowed in a local name / blank label (approximation of
/// PN_LOCAL, without leading/trailing rules).
fn is_pn_local_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | '%' | ':') || c as u32 > 0x7F
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Vec<RdfTriple> {
        parse_turtle(input).unwrap()
    }

    #[test]
    fn prefixes_and_a_keyword() {
        let ttl = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "@prefix schema: <http://schema.org/> .\n",
            "ex:alice a schema:Person ; schema:name \"Alice\" .\n",
        );
        let triples = parse(ttl);
        assert_eq!(triples.len(), 2);
        assert_eq!(
            triples[0].subject,
            RdfResource::Iri("http://example.org/alice".to_string())
        );
        assert_eq!(triples[0].predicate, RDF_TYPE);
        assert_eq!(
            triples[0].object,
            RdfTerm::Iri("http://schema.org/Person".to_string())
        );
        assert_eq!(
            triples[1].object,
            RdfTerm::Literal {
                value: "Alice".to_string(),
                datatype: None,
                language: None,
            }
        );
    }

    #[test]
    fn sparql_style_directives() {
        let ttl = concat!("PREFIX ex: <http://example.org/>\n", "ex:s ex:p ex:o .\n",);
        let triples = parse(ttl);
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].predicate, "http://example.org/p");
    }

    #[test]
    fn object_list_and_predicate_list() {
        let ttl = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:p ex:a , ex:b ; ex:q ex:c .\n",
        );
        let triples = parse(ttl);
        assert_eq!(triples.len(), 3);
        assert_eq!(
            triples[0].object,
            RdfTerm::Iri("http://example.org/a".into())
        );
        assert_eq!(
            triples[1].object,
            RdfTerm::Iri("http://example.org/b".into())
        );
        assert_eq!(triples[2].predicate, "http://example.org/q");
    }

    #[test]
    fn language_and_datatype_and_numbers() {
        let ttl = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:label \"chat\"@fr ;\n",
            "     ex:age 42 ;\n",
            "     ex:score 3.14 ;\n",
            "     ex:big 1.0e6 ;\n",
            "     ex:ok true .\n",
        );
        let triples = parse(ttl);
        assert_eq!(
            triples[0].object,
            RdfTerm::Literal {
                value: "chat".into(),
                datatype: None,
                language: Some("fr".into()),
            }
        );
        assert_eq!(
            triples[1].object,
            RdfTerm::Literal {
                value: "42".into(),
                datatype: Some(XSD_INTEGER.into()),
                language: None,
            }
        );
        assert_eq!(
            triples[2].object,
            RdfTerm::Literal {
                value: "3.14".into(),
                datatype: Some(XSD_DECIMAL.into()),
                language: None,
            }
        );
        assert_eq!(
            triples[3].object,
            RdfTerm::Literal {
                value: "1.0e6".into(),
                datatype: Some(XSD_DOUBLE.into()),
                language: None,
            }
        );
        assert_eq!(
            triples[4].object,
            RdfTerm::Literal {
                value: "true".into(),
                datatype: Some(XSD_BOOLEAN.into()),
                language: None,
            }
        );
    }

    #[test]
    fn triple_quoted_string_with_newlines() {
        let ttl = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:p \"\"\"line1\nline2\"\"\" .\n",
        );
        let triples = parse(ttl);
        assert_eq!(
            triples[0].object,
            RdfTerm::Literal {
                value: "line1\nline2".into(),
                datatype: None,
                language: None,
            }
        );
    }

    #[test]
    fn blank_node_property_list() {
        let ttl = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:knows [ ex:name \"Bob\" ] .\n",
        );
        let triples = parse(ttl);
        // ex:s ex:knows _:b  and  _:b ex:name "Bob"
        assert_eq!(triples.len(), 2);
        let knows = triples
            .iter()
            .find(|t| t.predicate == "http://example.org/knows")
            .unwrap();
        assert!(matches!(knows.object, RdfTerm::BlankNode(_)));
        let name = triples
            .iter()
            .find(|t| t.predicate == "http://example.org/name")
            .unwrap();
        assert!(matches!(name.subject, RdfResource::BlankNode(_)));
    }

    #[test]
    fn collection_expands_to_rdf_list() {
        let ttl = concat!(
            "@prefix ex: <http://example.org/> .\n",
            "ex:s ex:list ( ex:a ex:b ) .\n",
        );
        let triples = parse(ttl);
        // ex:s ex:list _:head ; _:head first ex:a ; rest _:n ; _:n first ex:b ; rest nil
        let firsts: Vec<_> = triples
            .iter()
            .filter(|t| t.predicate == RDF_FIRST)
            .collect();
        let rests: Vec<_> = triples.iter().filter(|t| t.predicate == RDF_REST).collect();
        assert_eq!(firsts.len(), 2, "two list items");
        assert_eq!(rests.len(), 2, "two rdf:rest links");
        assert!(rests
            .iter()
            .any(|t| t.object == RdfTerm::Iri(RDF_NIL.to_string())));
    }

    #[test]
    fn empty_collection_is_rdf_nil() {
        let ttl = "@prefix ex: <http://example.org/> .\nex:s ex:list () .\n";
        let triples = parse(ttl);
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].object, RdfTerm::Iri(RDF_NIL.to_string()));
    }

    #[test]
    fn base_resolves_relative_iris() {
        let ttl = concat!(
            "@base <http://example.org/> .\n",
            "<alice> <http://example.org/knows> <bob> .\n",
        );
        let triples = parse(ttl);
        assert_eq!(
            triples[0].subject,
            RdfResource::Iri("http://example.org/alice".to_string())
        );
        assert_eq!(
            triples[0].object,
            RdfTerm::Iri("http://example.org/bob".to_string())
        );
    }

    #[test]
    fn comments_are_ignored() {
        let ttl = concat!(
            "# a comment\n",
            "@prefix ex: <http://example.org/> . # trailing\n",
            "ex:s ex:p ex:o . # another\n",
        );
        assert_eq!(parse(ttl).len(), 1);
    }

    #[test]
    fn reports_error_on_unknown_prefix() {
        let err = parse_turtle("ex:s ex:p ex:o .\n").unwrap_err();
        assert!(err.to_string().contains("unknown prefix"), "got: {err}");
    }
}
