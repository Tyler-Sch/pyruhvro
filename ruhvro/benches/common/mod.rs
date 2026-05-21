//! Shared schemas and record builders for ruhvro benches.
//!
//! Lives in `benches/common/` (a subdirectory) so Cargo doesn't auto-discover
//! it as a bench target.

use apache_avro::types::{Record, Value};
use apache_avro::{to_avro_datum, Schema as AvroSchema};
use ruhvro::deserialize::parse_schema;

pub const NUM_CHUNKS: usize = 8;

fn record_from_fields(schema: &AvroSchema, fields: Vec<(&str, Value)>) -> Value {
    let mut rec = Record::new(schema).unwrap();
    for (k, v) in fields {
        rec.put(k, v);
    }
    rec.into()
}

fn make_records(
    schema_str: &str,
    n: usize,
    build: impl Fn(usize, &AvroSchema) -> Value,
) -> (AvroSchema, Vec<Vec<u8>>) {
    let parsed = parse_schema(schema_str).unwrap();
    let encoded: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let val = build(i, &parsed);
            to_avro_datum(&parsed, val).unwrap()
        })
        .collect();
    (parsed, encoded)
}

// ---------- 1. Flat primitives: no unions, no nesting ----------

const FLAT_PRIMITIVES_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "FlatPrim",
        "fields": [
            {"name": "i", "type": "int"},
            {"name": "l", "type": "long"},
            {"name": "f", "type": "float"},
            {"name": "d", "type": "double"},
            {"name": "b", "type": "boolean"},
            {"name": "s", "type": "string"}
        ]
    }
"#;

pub fn flat_primitives(n: usize) -> (AvroSchema, Vec<Vec<u8>>) {
    make_records(FLAT_PRIMITIVES_SCHEMA, n, |i, sch| {
        record_from_fields(sch, vec![
            ("i", Value::Int(i as i32)),
            ("l", Value::Long(i as i64 * 7)),
            ("f", Value::Float(i as f32 * 1.5)),
            ("d", Value::Double(i as f64 * 2.25)),
            ("b", Value::Boolean(i % 2 == 0)),
            ("s", Value::String(format!("row-{i}"))),
        ])
    })
}

// ---------- 2. Nullable primitives: 2-variant ["null", T] unions ----------

const NULLABLE_PRIMITIVES_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "NullPrim",
        "fields": [
            {"name": "i", "type": ["null", "int"], "default": null},
            {"name": "l", "type": ["null", "long"], "default": null},
            {"name": "d", "type": ["null", "double"], "default": null},
            {"name": "b", "type": ["null", "boolean"], "default": null},
            {"name": "s", "type": ["null", "string"], "default": null}
        ]
    }
"#;

pub fn nullable_primitives(n: usize) -> (AvroSchema, Vec<Vec<u8>>) {
    make_records(NULLABLE_PRIMITIVES_SCHEMA, n, |i, sch| {
        // Half the rows have nulls to exercise the null path.
        let is_null = i % 2 == 1;
        let mk = |v: Value| if is_null {
            Value::Union(0, Box::new(Value::Null))
        } else {
            Value::Union(1, Box::new(v))
        };
        record_from_fields(sch, vec![
            ("i", mk(Value::Int(i as i32))),
            ("l", mk(Value::Long(i as i64 * 7))),
            ("d", mk(Value::Double(i as f64 * 2.25))),
            ("b", mk(Value::Boolean(i % 4 == 0))),
            ("s", mk(Value::String(format!("row-{i}")))),
        ])
    })
}

// ---------- 3. Nested struct: outer record with inner record ----------

const NESTED_STRUCT_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "Outer",
        "fields": [
            {"name": "outer_id", "type": "long"},
            {"name": "inner", "type": {
                "type": "record",
                "name": "Inner",
                "fields": [
                    {"name": "x", "type": "int"},
                    {"name": "y", "type": "int"},
                    {"name": "label", "type": "string"}
                ]
            }}
        ]
    }
"#;

pub fn nested_struct(n: usize) -> (AvroSchema, Vec<Vec<u8>>) {
    make_records(NESTED_STRUCT_SCHEMA, n, |i, sch| {
        let inner = Value::Record(vec![
            ("x".to_string(),     Value::Int(i as i32)),
            ("y".to_string(),     Value::Int((i * 3) as i32)),
            ("label".to_string(), Value::String(format!("lbl-{i}"))),
        ]);
        record_from_fields(sch, vec![
            ("outer_id", Value::Long(i as i64)),
            ("inner", inner),
        ])
    })
}

// ---------- 4. Array<string> + Map<string,string> ----------

const ARRAY_AND_MAP_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "Collection",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "tags",  "type": {"type": "array", "items": "string"}},
            {"name": "props", "type": {"type": "map",   "values": "string"}}
        ]
    }
"#;

pub fn array_and_map(n: usize) -> (AvroSchema, Vec<Vec<u8>>) {
    make_records(ARRAY_AND_MAP_SCHEMA, n, |i, sch| {
        let tags = Value::Array(vec![
            Value::String(format!("t-{i}-a")),
            Value::String(format!("t-{i}-b")),
            Value::String(format!("t-{i}-c")),
        ]);
        let mut map = std::collections::HashMap::new();
        map.insert(format!("k{i}-1"), Value::String(format!("v{i}-1")));
        map.insert(format!("k{i}-2"), Value::String(format!("v{i}-2")));
        record_from_fields(sch, vec![
            ("id", Value::Long(i as i64)),
            ("tags", tags),
            ("props", Value::Map(map)),
        ])
    })
}
