//! Inlines named schema references (`Schema::Ref`) so the schema-walking
//! code paths can treat every schema as a plain tree.
//!
//! Avro lets a named type (record / enum / fixed) be defined once and reused
//! by name; `apache_avro` represents each reuse as `Schema::Ref { name }`.
//! The fast encoder/decoder and the `Value`-based serializer all walk the
//! schema structurally and have no name table, so rather than threading a
//! resolver through every constructor we expand the references up front.
//!
//! The expanded schema is for *structural walking only*. It contains the same
//! named type more than once, which `apache_avro`'s own `to_avro_datum` /
//! `from_avro_datum` reject as `AmbiguousSchemaDefinition`; callers that use
//! those must keep passing the original schema.
//!
//! [`embed_definitions`] is the complementary operation for schemas parsed
//! from several strings with `Schema::parse_list`: it folds the dependency
//! definitions into the top-level schema exactly once (at the first
//! reference), producing the same self-contained schema `Schema::parse_str`
//! would give for the equivalent single document.

use anyhow::{anyhow, Result};
use apache_avro::schema::{
    ArraySchema, MapSchema, Name, NamesRef, RecordField, RecordSchema, ResolvedSchema,
    UnionSchema,
};
use apache_avro::Schema;
use std::borrow::Cow;
use std::collections::HashSet;

/// Returns `schema` with every `Schema::Ref` replaced by the definition it
/// points at. Borrows when the schema has no refs (the common case) so there
/// is no clone cost for ordinary schemas.
///
/// Errors if a reference cannot be resolved or if the schema is recursive
/// (a type that, directly or indirectly, contains itself). Recursive types
/// have no finite Arrow representation, so this matches the error
/// `super::translate::to_arrow_schema` produces for the same input.
pub fn resolve_refs(schema: &Schema) -> Result<Cow<'_, Schema>> {
    if !contains_ref(schema) {
        return Ok(Cow::Borrowed(schema));
    }
    let resolved = ResolvedSchema::try_from(schema)?;
    let names = resolved.get_names();
    let mut resolving = HashSet::new();
    Ok(Cow::Owned(inline(schema, names, &mut resolving)?))
}

fn contains_ref(schema: &Schema) -> bool {
    match schema {
        Schema::Ref { .. } => true,
        Schema::Record(rs) => rs.fields.iter().any(|f| contains_ref(&f.schema)),
        Schema::Array(a) => contains_ref(&a.items),
        Schema::Map(m) => contains_ref(&m.types),
        Schema::Union(u) => u.variants().iter().any(contains_ref),
        _ => false,
    }
}

fn inline(schema: &Schema, names: &NamesRef<'_>, resolving: &mut HashSet<Name>) -> Result<Schema> {
    match schema {
        Schema::Ref { name } => {
            let target = lookup(names, name)?;
            if !resolving.insert(name.clone()) {
                return Err(anyhow!(
                    "Recursive Avro schema reference '{}' cannot be represented as an Arrow schema",
                    name.fullname(None)
                ));
            }
            let out = inline(target, names, resolving);
            resolving.remove(name);
            out
        }
        other => map_children(other, &mut |c| inline(c, names, resolving)),
    }
}

/// Returns `schema` with every `Schema::Ref` to a type defined in `names`
/// (but not inside `schema` itself) replaced by that definition at its
/// *first* occurrence in document order. Later references stay as
/// `Schema::Ref`, so each named type is defined exactly once — the shape
/// `apache_avro` requires for `to_avro_datum` / `from_avro_datum` and for
/// building a name table.
///
/// `names` should cover every schema in the set (see
/// [`ResolvedSchema`]'s `TryFrom<Vec<&Schema>>`). Named types already
/// defined inline in `schema` are left alone.
pub fn embed_definitions(schema: &Schema, names: &NamesRef<'_>) -> Result<Schema> {
    let mut defined = HashSet::new();
    embed(schema, names, &mut defined)
}

fn embed(schema: &Schema, names: &NamesRef<'_>, defined: &mut HashSet<Name>) -> Result<Schema> {
    match schema {
        Schema::Ref { name } => {
            if !defined.insert(name.clone()) {
                return Ok(schema.clone());
            }
            let target = lookup(names, name)?;
            // The definition may itself reference other named types; embed
            // those on the same first-seen basis.
            embed_named(target, names, defined)
        }
        other => embed_named(other, names, defined),
    }
}

/// Like `embed`, but for a schema that is (or may be) a named definition:
/// records its name so a later `Ref` to it stays a reference.
fn embed_named(schema: &Schema, names: &NamesRef<'_>, defined: &mut HashSet<Name>) -> Result<Schema> {
    match schema {
        Schema::Record(RecordSchema { name, .. })
        | Schema::Enum(apache_avro::schema::EnumSchema { name, .. })
        | Schema::Fixed(apache_avro::schema::FixedSchema { name, .. }) => {
            defined.insert(name.clone());
        }
        _ => {}
    }
    map_children(schema, &mut |c| embed(c, names, defined))
}

fn lookup<'a>(names: &NamesRef<'a>, name: &Name) -> Result<&'a Schema> {
    names.get(name).copied().ok_or_else(|| {
        anyhow!(
            "Avro schema reference '{}' could not be resolved",
            name.fullname(None)
        )
    })
}

/// Rebuilds `schema` with `f` applied to each direct child schema. Leaves,
/// including `Schema::Ref`, are cloned as-is — callers handle refs before
/// delegating here.
fn map_children(schema: &Schema, f: &mut dyn FnMut(&Schema) -> Result<Schema>) -> Result<Schema> {
    match schema {
        Schema::Record(rs) => {
            let fields = rs
                .fields
                .iter()
                .map(|fld| {
                    Ok(RecordField {
                        schema: f(&fld.schema)?,
                        ..fld.clone()
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Schema::Record(RecordSchema {
                fields,
                ..rs.clone()
            }))
        }
        Schema::Array(a) => Ok(Schema::Array(ArraySchema {
            items: Box::new(f(&a.items)?),
            attributes: a.attributes.clone(),
        })),
        Schema::Map(m) => Ok(Schema::Map(MapSchema {
            types: Box::new(f(&m.types)?),
            attributes: m.attributes.clone(),
        })),
        Schema::Union(u) => {
            let variants = u
                .variants()
                .iter()
                .map(|v| f(v))
                .collect::<Result<Vec<_>>>()?;
            Ok(Schema::Union(UnionSchema::new(variants)?))
        }
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_refs(schema: &Schema) -> usize {
        match schema {
            Schema::Ref { .. } => 1,
            Schema::Record(rs) => rs.fields.iter().map(|f| count_refs(&f.schema)).sum(),
            Schema::Array(a) => count_refs(&a.items),
            Schema::Map(m) => count_refs(&m.types),
            Schema::Union(u) => u.variants().iter().map(count_refs).sum(),
            _ => 0,
        }
    }

    #[test]
    fn borrows_when_no_refs() {
        let s = Schema::parse_str(
            r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#,
        )
        .unwrap();
        assert!(matches!(resolve_refs(&s).unwrap(), Cow::Borrowed(_)));
    }

    #[test]
    fn inlines_refs_in_record_union_array_and_map() {
        let s = Schema::parse_str(
            r#"{
                "type": "record", "name": "R", "namespace": "ns",
                "fields": [
                    {"name": "a", "type": {"type": "record", "name": "S",
                        "fields": [{"name": "x", "type": "string"}]}},
                    {"name": "b", "type": "S"},
                    {"name": "c", "type": ["null", "S"]},
                    {"name": "d", "type": {"type": "array", "items": "S"}},
                    {"name": "e", "type": {"type": "map", "values": "S"}}
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(count_refs(&s), 4);
        let r = resolve_refs(&s).unwrap();
        assert!(matches!(r, Cow::Owned(_)));
        assert_eq!(count_refs(&r), 0);

        let Schema::Record(rs) = &*r else { panic!() };
        let s_def = &rs.fields[0].schema;
        assert_eq!(&rs.fields[1].schema, s_def);
        let Schema::Union(u) = &rs.fields[2].schema else { panic!() };
        assert_eq!(&u.variants()[1], s_def);
        let Schema::Array(a) = &rs.fields[3].schema else { panic!() };
        assert_eq!(&*a.items, s_def);
        let Schema::Map(m) = &rs.fields[4].schema else { panic!() };
        assert_eq!(&*m.types, s_def);
    }

    #[test]
    fn inlines_enum_refs() {
        let s = Schema::parse_str(
            r#"{
                "type": "record", "name": "R",
                "fields": [
                    {"name": "a", "type": {"type": "enum", "name": "E", "symbols": ["X", "Y"]}},
                    {"name": "b", "type": "E"}
                ]
            }"#,
        )
        .unwrap();
        let r = resolve_refs(&s).unwrap();
        let Schema::Record(rs) = &*r else { panic!() };
        assert!(matches!(rs.fields[1].schema, Schema::Enum(_)));
        assert_eq!(rs.fields[0].schema, rs.fields[1].schema);
    }

    #[test]
    fn recursive_schema_errors() {
        let s = Schema::parse_str(
            r#"{
                "type": "record", "name": "Node",
                "fields": [
                    {"name": "value", "type": "long"},
                    {"name": "next", "type": ["null", "Node"], "default": null}
                ]
            }"#,
        )
        .unwrap();
        let err = resolve_refs(&s).unwrap_err().to_string();
        assert!(err.contains("Recursive Avro schema reference 'Node'"), "{err}");
    }

    #[test]
    fn embed_defines_each_dependency_once() {
        let side = r#"{"type":"record","name":"Side","namespace":"ns",
            "fields":[{"name":"shares","type":"long"}]}"#;
        let pos = r#"{"type":"record","name":"Position","namespace":"ns","fields":[
            {"name":"long","type":"Side"},
            {"name":"short","type":"Side"},
            {"name":"fills","type":{"type":"array","items":"Side"}}]}"#;
        let parsed = Schema::parse_list([side, pos]).unwrap();
        let names = ResolvedSchema::try_from(parsed.iter().collect::<Vec<_>>()).unwrap();
        let embedded = embed_definitions(&parsed[1], names.get_names()).unwrap();

        // Identical to parsing the combined document in one go.
        let combined = Schema::parse_str(
            r#"{"type":"record","name":"Position","namespace":"ns","fields":[
                {"name":"long","type":{"type":"record","name":"Side",
                    "fields":[{"name":"shares","type":"long"}]}},
                {"name":"short","type":"Side"},
                {"name":"fills","type":{"type":"array","items":"Side"}}]}"#,
        )
        .unwrap();
        assert_eq!(embedded.canonical_form(), combined.canonical_form());
        assert_eq!(count_refs(&embedded), 2);
        // Self-contained: builds a name table and fully inlines.
        assert!(ResolvedSchema::try_from(&embedded).is_ok());
        assert_eq!(count_refs(&resolve_refs(&embedded).unwrap()), 0);
    }

    #[test]
    fn embed_handles_transitive_dependencies() {
        let venue = r#"{"type":"enum","name":"Venue","namespace":"ns","symbols":["NYSE","NSDQ"]}"#;
        let side = r#"{"type":"record","name":"Side","namespace":"ns",
            "fields":[{"name":"venue","type":"Venue"},{"name":"alt","type":"Venue"}]}"#;
        let pos = r#"{"type":"record","name":"Position","namespace":"ns","fields":[
            {"name":"long","type":"Side"},{"name":"short","type":"Side"},
            {"name":"v","type":"Venue"}]}"#;
        let parsed = Schema::parse_list([venue, side, pos]).unwrap();
        let names = ResolvedSchema::try_from(parsed.iter().collect::<Vec<_>>()).unwrap();
        let embedded = embed_definitions(&parsed[2], names.get_names()).unwrap();

        let Schema::Record(rs) = &embedded else { panic!() };
        let Schema::Record(side_rs) = &rs.fields[0].schema else { panic!() };
        assert!(matches!(side_rs.fields[0].schema, Schema::Enum(_)));
        assert!(matches!(side_rs.fields[1].schema, Schema::Ref { .. }));
        assert!(matches!(rs.fields[1].schema, Schema::Ref { .. }));
        assert!(matches!(rs.fields[2].schema, Schema::Ref { .. }));
        assert!(ResolvedSchema::try_from(&embedded).is_ok());
    }

    #[test]
    fn embed_leaves_inline_definitions_alone() {
        let pos = r#"{"type":"record","name":"Position","namespace":"ns","fields":[
            {"name":"a","type":{"type":"record","name":"Side","fields":[{"name":"x","type":"int"}]}},
            {"name":"b","type":"Side"}]}"#;
        let parsed = Schema::parse_list([pos]).unwrap();
        let names = ResolvedSchema::try_from(parsed.iter().collect::<Vec<_>>()).unwrap();
        let embedded = embed_definitions(&parsed[0], names.get_names()).unwrap();
        assert_eq!(embedded.canonical_form(), parsed[0].canonical_form());
    }
}
