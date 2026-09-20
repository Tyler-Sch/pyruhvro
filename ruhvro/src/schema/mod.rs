//! Avro schema handling: parsing schema documents and translating them to
//! Arrow schemas.
//!
//! The public surface is [`parse_schema`], [`parse_schema_list`] and
//! [`to_arrow_schema`]. The `resolve` submodule (inlining named references so
//! the structural walkers can treat a schema as a plain tree) is crate-private
//! because the schema it produces must not be handed back to `apache_avro`'s
//! own datum codecs — see its module docs.

mod resolve;
mod translate;

pub(crate) use resolve::{embed_definitions, resolve_refs};
pub use translate::to_arrow_schema;

use anyhow::{anyhow, Result};
use apache_avro::schema::ResolvedSchema;
use apache_avro::Schema as AvroSchema;

/// Parses string into AvroSchema object
pub fn parse_schema(schema_string: &str) -> Result<AvroSchema> {
    Ok(AvroSchema::parse_str(schema_string)?)
}

/// Parses a set of interdependent schema strings and returns the **last**
/// one, with the named types it references from the others folded in.
///
/// Use this when a top-level schema refers to types (records, enums, fixeds)
/// that live in separate documents. The final element is the schema
/// you'll serialize / deserialize with.
///
pub fn parse_schema_list<S: AsRef<str>>(schema_strings: &[S]) -> Result<AvroSchema> {
    let parsed = AvroSchema::parse_list(schema_strings)?;
    let Some(main) = parsed.last() else {
        return Err(anyhow!("parse_schema_list requires at least one schema"));
    };
    let names = ResolvedSchema::try_from(parsed.iter().collect::<Vec<_>>())?;
    embed_definitions(main, names.get_names())
}

#[cfg(test)]
mod tests {
    use super::*;

    const S_DOC: &str = r#"{"type": "record", "name": "S", "namespace": "ns",
        "fields": [{"name": "x", "type": "string"}, {"name": "y", "type": "int"}]}"#;

    const R_DOC: &str = r#"{"type": "record", "name": "R", "namespace": "ns", "fields": [
        {"name": "a", "type": "S"},
        {"name": "b", "type": "S"},
        {"name": "c", "type": ["null", "S"]},
        {"name": "d", "type": {"type": "array", "items": "S"}},
        {"name": "e", "type": {"type": "map", "values": "S"}}]}"#;

    // The same types as a single self-contained document: `S` defined at its
    // first use and referenced by name afterwards.
    const SINGLE: &str = r#"{
        "type": "record", "name": "R", "namespace": "ns",
        "fields": [
            {"name": "a", "type": {"type": "record", "name": "S",
                "fields": [{"name": "x", "type": "string"}, {"name": "y", "type": "int"}]}},
            {"name": "b", "type": "S"},
            {"name": "c", "type": ["null", "S"]},
            {"name": "d", "type": {"type": "array", "items": "S"}},
            {"name": "e", "type": {"type": "map", "values": "S"}}
        ]
    }"#;

    #[test]
    fn parse_schema_list_matches_single_document() {
        let from_list = parse_schema_list(&[S_DOC, R_DOC]).unwrap();
        let single = parse_schema(SINGLE).unwrap();
        assert_eq!(from_list.canonical_form(), single.canonical_form());
    }

    #[test]
    fn parse_schema_list_uses_last_document_as_top_level() {
        // Dependencies come first; the final element is the schema returned.
        let schema = parse_schema_list(&[S_DOC, R_DOC]).unwrap();
        assert_eq!(schema.name().map(|n| n.fullname(None)).as_deref(), Some("ns.R"));
    }

    #[test]
    fn parse_schema_list_reports_missing_dependency() {
        let r_doc = r#"{"type": "record", "name": "R", "fields": [{"name": "a", "type": "Missing"}]}"#;
        let err = parse_schema_list(&[r_doc]).unwrap_err().to_string();
        assert!(err.contains("Missing"), "{err}");
        assert!(parse_schema_list::<&str>(&[]).is_err());
    }

    #[test]
    fn to_arrow_schema_agrees_for_list_and_single_document() {
        let from_list = to_arrow_schema(&parse_schema_list(&[S_DOC, R_DOC]).unwrap()).unwrap();
        let single = to_arrow_schema(&parse_schema(SINGLE).unwrap()).unwrap();
        assert_eq!(from_list, single);
    }
}
