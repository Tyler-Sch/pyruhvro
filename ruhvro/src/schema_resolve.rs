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
/// `schema_translate::to_arrow_schema` produces for the same input.
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
            let target = names.get(name).ok_or_else(|| {
                anyhow!(
                    "Avro schema reference '{}' could not be resolved",
                    name.fullname(None)
                )
            })?;
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
        Schema::Record(rs) => {
            let fields = rs
                .fields
                .iter()
                .map(|f| {
                    Ok(RecordField {
                        schema: inline(&f.schema, names, resolving)?,
                        ..f.clone()
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Schema::Record(RecordSchema {
                fields,
                ..rs.clone()
            }))
        }
        Schema::Array(a) => Ok(Schema::Array(ArraySchema {
            items: Box::new(inline(&a.items, names, resolving)?),
            attributes: a.attributes.clone(),
        })),
        Schema::Map(m) => Ok(Schema::Map(MapSchema {
            types: Box::new(inline(&m.types, names, resolving)?),
            attributes: m.attributes.clone(),
        })),
        Schema::Union(u) => {
            let variants = u
                .variants()
                .iter()
                .map(|v| inline(v, names, resolving))
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
}
