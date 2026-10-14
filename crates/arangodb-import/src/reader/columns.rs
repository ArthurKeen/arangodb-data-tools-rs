//! Per-column reshaping for delimited input, matching `arangoimport`.
//!
//! A CSV dump rarely has the shape the target graph needs: keys live in a
//! differently-named column, composite keys span several, and edge files carry
//! payload columns that belong only on the vertices. `arangoimport` reshapes
//! during the load with `--datatype`, `--translate`, `--merge-attributes` and
//! `--remove-attribute`; this is the same behavior so a generated script can
//! use either tool.
//!
//! The semantics below were established by running ArangoDB 3.12.4's own
//! `arangoimport` against fixtures and reading back what landed, not from its
//! documentation. Three results were not obvious and are load-bearing:
//!
//! 1. **`_key`, `_from`, `_to` and `_id` are never type-inferred.** They take
//!    the raw text of their source column, so a numeric-looking identifier
//!    stays a string. Without this a `_key` column of `12345` is sent as a JSON
//!    number and the server rejects the document with "illegal document key"
//!    (error 1221). This is the single behavior that made CSV loads fail.
//! 2. **Merging happens before renaming.** `--merge-attributes _key=[id]`
//!    together with `--translate id=ident` yields both `_key` and `ident`,
//!    because the merge still sees the original column name.
//! 3. **A merge renders the *inferred* value, while renaming to a reserved
//!    field keeps the *raw* text.** So `00789` merged into a key becomes
//!    `"789"`, but translated into `_key` stays `"00789"`.

use std::collections::HashMap;

use arangodb_tools_core::{Error, Result};
use serde_json::{Map, Value};

/// Document attributes ArangoDB requires to be strings.
///
/// Inferring these would turn a numeric identifier into a JSON number, which
/// the server rejects for `_key` and which this crate's own edge validation
/// rejects for `_from`/`_to`.
const RESERVED_STRING_FIELDS: [&str; 4] = ["_key", "_from", "_to", "_id"];

/// The JSON type a column is forced to, via `--datatype`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// Keep the raw text as a JSON string.
    String,
    /// Parse as a JSON number; a value that will not parse is an error.
    Number,
    /// Parse as a JSON boolean; a value that will not parse is an error.
    Boolean,
}

impl ColumnType {
    /// Parses a `--datatype` type name.
    ///
    /// # Errors
    /// Returns [`Error::Config`] for an unknown type name.
    pub fn parse(name: &str) -> Result<Self> {
        match name {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "boolean" => Ok(Self::Boolean),
            other => Err(Error::config(format!(
                "unknown --datatype '{other}': expected string, number, or boolean"
            ))),
        }
    }
}

/// One `--merge-attributes` rule: a target attribute built from a template.
#[derive(Debug, Clone)]
pub struct MergeRule {
    /// The attribute to write.
    target: String,
    /// Alternating literal/placeholder segments.
    segments: Vec<Segment>,
}

/// A piece of a merge template.
#[derive(Debug, Clone)]
enum Segment {
    /// Text copied verbatim.
    Literal(String),
    /// A column whose value is substituted.
    Column(String),
}

/// Column reshaping applied to every row of a delimited file.
#[derive(Debug, Clone, Default)]
pub struct ColumnTransforms {
    types: HashMap<String, ColumnType>,
    renames: HashMap<String, String>,
    merges: Vec<MergeRule>,
    removals: Vec<String>,
}

impl ColumnTransforms {
    /// Builds transforms from the raw `name=value` CLI arguments.
    ///
    /// # Errors
    /// Returns [`Error::Config`] if any specification is malformed, a datatype
    /// name is unknown, or a merge template contains no column reference.
    pub fn new(
        datatypes: &[String],
        translations: &[String],
        merges: &[String],
        removals: &[String],
    ) -> Result<Self> {
        let mut types = HashMap::new();
        for spec in datatypes {
            let (column, name) = split_once(spec, "--datatype")?;
            types.insert(column.to_string(), ColumnType::parse(name)?);
        }

        let mut renames = HashMap::new();
        for spec in translations {
            let (source, target) = split_once(spec, "--translate")?;
            if target.is_empty() {
                return Err(Error::config(format!(
                    "--translate '{spec}' has an empty target name"
                )));
            }
            renames.insert(source.to_string(), target.to_string());
        }

        let mut merge_rules = Vec::new();
        for spec in merges {
            let (target, template) = split_once(spec, "--merge-attributes")?;
            merge_rules.push(MergeRule {
                target: target.to_string(),
                segments: parse_template(template, spec)?,
            });
        }

        Ok(Self {
            types,
            renames,
            merges: merge_rules,
            removals: removals.to_vec(),
        })
    }

    /// Whether any reshaping is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
            && self.renames.is_empty()
            && self.merges.is_empty()
            && self.removals.is_empty()
    }

    /// The type override for a column, if any.
    #[must_use]
    pub fn column_type(&self, column: &str) -> Option<ColumnType> {
        self.types.get(column).copied()
    }

    /// The attribute name a column is written under.
    #[must_use]
    pub fn target_name<'a>(&'a self, column: &'a str) -> &'a str {
        self.renames.get(column).map_or(column, String::as_str)
    }

    /// Whether a column is dropped.
    #[must_use]
    pub fn is_removed(&self, column: &str) -> bool {
        self.removals.iter().any(|name| name == column)
    }

    /// Applies the merge rules, given the row's values keyed by *source*
    /// column name.
    ///
    /// Runs before renaming, so a template may reference a column that a
    /// `--translate` rule also moves — the case a composite key built from the
    /// primary key depends on.
    pub fn apply_merges(&self, source_values: &Map<String, Value>, out: &mut Map<String, Value>) {
        for rule in &self.merges {
            let mut built = String::new();
            for segment in &rule.segments {
                match segment {
                    Segment::Literal(text) => built.push_str(text),
                    Segment::Column(name) => {
                        if let Some(value) = source_values.get(name) {
                            built.push_str(&render(value));
                        }
                    }
                }
            }
            out.insert(rule.target.clone(), Value::String(built));
        }
    }
}

/// Whether an attribute must always be a string rather than inferred.
#[must_use]
pub fn is_reserved_string_field(name: &str) -> bool {
    RESERVED_STRING_FIELDS.contains(&name)
}

/// Renders a value for substitution into a merge template.
///
/// Strings substitute as themselves rather than as quoted JSON, so
/// `[first]_[last]` yields `ada_lovelace` and not `"ada"_"lovelace"`.
fn render(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Splits a `name=value` specification, naming the flag in any error.
fn split_once<'a>(spec: &'a str, flag: &str) -> Result<(&'a str, &'a str)> {
    spec.split_once('=')
        .ok_or_else(|| Error::config(format!("{flag} '{spec}' is not in 'name=value' form")))
}

/// Parses a merge template into literal and column segments.
fn parse_template(template: &str, spec: &str) -> Result<Vec<Segment>> {
    let mut segments = Vec::new();
    let mut literal = String::new();
    let mut rest = template;
    let mut saw_column = false;

    while let Some(open) = rest.find('[') {
        literal.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after.find(']').ok_or_else(|| {
            Error::config(format!(
                "--merge-attributes '{spec}' has an unclosed '[' in its template"
            ))
        })?;
        let column = &after[..close];
        if column.is_empty() {
            return Err(Error::config(format!(
                "--merge-attributes '{spec}' has an empty '[]' column reference"
            )));
        }
        if !literal.is_empty() {
            segments.push(Segment::Literal(std::mem::take(&mut literal)));
        }
        segments.push(Segment::Column(column.to_string()));
        saw_column = true;
        rest = &after[close + 1..];
    }
    literal.push_str(rest);
    if !literal.is_empty() {
        segments.push(Segment::Literal(literal));
    }

    if !saw_column {
        return Err(Error::config(format!(
            "--merge-attributes '{spec}' references no column; expected at least one \
             '[column]' placeholder in the template"
        )));
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transforms(d: &[&str], t: &[&str], m: &[&str], r: &[&str]) -> ColumnTransforms {
        let owned = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        ColumnTransforms::new(&owned(d), &owned(t), &owned(m), &owned(r)).expect("valid")
    }

    #[test]
    fn reserved_fields_are_recognized() {
        for name in ["_key", "_from", "_to", "_id"] {
            assert!(is_reserved_string_field(name), "{name} must be reserved");
        }
        assert!(!is_reserved_string_field("id"));
        assert!(!is_reserved_string_field("key"));
    }

    #[test]
    fn datatype_parses_the_three_supported_names() {
        assert_eq!(ColumnType::parse("string").unwrap(), ColumnType::String);
        assert_eq!(ColumnType::parse("number").unwrap(), ColumnType::Number);
        assert_eq!(ColumnType::parse("boolean").unwrap(), ColumnType::Boolean);
        let err = ColumnType::parse("float").unwrap_err().to_string();
        assert!(err.contains("float"), "names the bad type: {err}");
        assert!(
            err.contains("string, number, or boolean"),
            "lists valid: {err}"
        );
    }

    #[test]
    fn translate_renames_and_leaves_others_alone() {
        let t = transforms(&[], &["id=_key"], &[], &[]);
        assert_eq!(t.target_name("id"), "_key");
        assert_eq!(t.target_name("name"), "name");
    }

    #[test]
    fn remove_drops_only_named_columns() {
        let t = transforms(&[], &[], &[], &["extra"]);
        assert!(t.is_removed("extra"));
        assert!(!t.is_removed("name"));
    }

    /// Brackets are column references; text outside them is literal.
    #[test]
    fn merge_mixes_literals_and_columns() {
        let t = transforms(&[], &[], &["_key=x[a]_[b]y"], &[]);
        let mut source = Map::new();
        source.insert("a".into(), Value::from(12_345));
        source.insert("b".into(), Value::String("alice".into()));
        let mut out = Map::new();
        t.apply_merges(&source, &mut out);
        assert_eq!(out["_key"], Value::String("x12345_alicey".into()));
    }

    /// Strings substitute bare, not as quoted JSON.
    #[test]
    fn merge_does_not_quote_string_values() {
        let t = transforms(&[], &[], &["_key=[first]_[last]"], &[]);
        let mut source = Map::new();
        source.insert("first".into(), Value::String("ada".into()));
        source.insert("last".into(), Value::String("lovelace".into()));
        let mut out = Map::new();
        t.apply_merges(&source, &mut out);
        assert_eq!(out["_key"], Value::String("ada_lovelace".into()));
    }

    /// A missing column contributes nothing rather than the text "null".
    #[test]
    fn merge_skips_absent_columns() {
        let t = transforms(&[], &[], &["_key=[a][missing]"], &[]);
        let mut source = Map::new();
        source.insert("a".into(), Value::String("x".into()));
        let mut out = Map::new();
        t.apply_merges(&source, &mut out);
        assert_eq!(out["_key"], Value::String("x".into()));
    }

    #[test]
    fn malformed_specifications_are_rejected_by_name() {
        let bad = |d: &[&str], t: &[&str], m: &[&str]| {
            let owned = |v: &[&str]| v.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
            ColumnTransforms::new(&owned(d), &owned(t), &owned(m), &[])
                .unwrap_err()
                .to_string()
        };
        assert!(bad(&["idstring"], &[], &[]).contains("--datatype"));
        assert!(bad(&[], &["id"], &[]).contains("--translate"));
        assert!(bad(&[], &[], &["_key=plain"]).contains("references no column"));
        assert!(bad(&[], &[], &["_key=[unclosed"]).contains("unclosed"));
        assert!(bad(&[], &[], &["_key=[]"]).contains("empty"));
        assert!(bad(&[], &["id="], &[]).contains("empty target"));
    }

    #[test]
    fn empty_transforms_are_detected() {
        assert!(ColumnTransforms::default().is_empty());
        assert!(!transforms(&[], &["a=b"], &[], &[]).is_empty());
    }
}
