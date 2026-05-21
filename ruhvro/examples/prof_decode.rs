//! Tight standalone profiling target: decode 10 million records in a loop.
//! Build & profile with:
//!   cargo build -p ruhvro --release --example prof_decode
//!   samply record target/release/examples/prof_decode
//!
//! Uses the same `array_and_map` schema as the criterion bench (most
//! representative of real Kafka payloads).

use apache_avro::types::{Record, Value};
use apache_avro::{to_avro_datum, Schema as AvroSchema};
use ruhvro::deserialize::{parse_schema, per_datum_deserialize_threaded};

const SCHEMA: &str = r#"
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

fn build_record(i: usize, sch: &AvroSchema) -> Value {
    let mut r = Record::new(sch).unwrap();
    let tags = Value::Array(vec![
        Value::String(format!("t-{i}-a")),
        Value::String(format!("t-{i}-b")),
        Value::String(format!("t-{i}-c")),
    ]);
    let mut map = std::collections::HashMap::new();
    map.insert(format!("k{i}-1"), Value::String(format!("v{i}-1")));
    map.insert(format!("k{i}-2"), Value::String(format!("v{i}-2")));
    r.put("id", Value::Long(i as i64));
    r.put("tags", tags);
    r.put("props", Value::Map(map));
    r.into()
}

fn main() {
    let parsed = parse_schema(SCHEMA).unwrap();
    let n = 1_000;
    let encoded: Vec<Vec<u8>> = (0..n)
        .map(|i| to_avro_datum(&parsed, build_record(i, &parsed)).unwrap())
        .collect();

    eprintln!("preparing done; running deserialize 10000 times on {n} records each…");
    let mut total_rows: usize = 0;
    for iter in 0..10_000 {
        let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        let rbs = per_datum_deserialize_threaded(refs, &parsed, 8).unwrap();
        total_rows += rbs.iter().map(|rb| rb.num_rows()).sum::<usize>();
        if iter % 1000 == 0 {
            eprintln!("  iter {iter}");
        }
    }
    eprintln!("total rows: {total_rows}");
}
