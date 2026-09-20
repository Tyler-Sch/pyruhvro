# Ruhvro

This library provides function for deserializing arrays of avro encoded data and returning a vector of Arrow
record batches and serializing Arrow record batches into avro encoded bytes.

## Usage

```rust
use ruhvro::{deserialize, serialize};
use arrow::record_batch::RecordBatch;

fn main() {
    let raw_schema = r#"
             {
                 "type": "record",
                 "name": "test",
                 "fields": [
                     {"name": "a", "type": "long", "default": 42},
                     {"name": "b",
                         "type": ["null", "string"],
                         "default": null
             }
                 ]
             }
         "#;

    let parsed_schema = ruhvro::deserialize::parse_schema(&raw_schema).unwrap();
    // create a record
    let mut record = apache_avro::types::Record::new(&parsed_schema).unwrap();
    record.put("a", 27i64);
    record.put("b", None::<String>);
    // and serialize it using the plain avro library
    let serialized = apache_avro::to_avro_datum(&parsed_schema, record).unwrap();
    
    // deserialization
     let deserialized = ruhvro::deserialize::per_datum_deserialize(&vec![&serialized[..]], &parsed_schema).unwrap();
     println!("{:?}", deserialized);
    
     // serialize the record batch
     let serialized = ruhvro::serialize::serialize_record_batch(deserialized, &parsed_schema, 1).unwrap();
     println!("{:?}", serialized);
}
```

### Named types in separate schema documents

When a schema references named types (records, enums, fixeds) defined in
other documents, parse them together with `parse_schema_list`. The last
element is the top-level schema; the rest supply its dependencies in any
order. The result is a self-contained `Schema` usable exactly like the output
of `parse_schema`.

```rust
let side = r#"{"type": "record", "name": "Side", "namespace": "com.example",
    "fields": [{"name": "shares", "type": "long"}, {"name": "venue", "type": "string"}]}"#;
let position = r#"{"type": "record", "name": "Position", "namespace": "com.example",
    "fields": [{"name": "long", "type": "Side"},
               {"name": "short", "type": "Side"},
               {"name": "fills", "type": {"type": "array", "items": "Side"}}]}"#;

let schema = std::sync::Arc::new(
    ruhvro::deserialize::parse_schema_list(&[side, position]).unwrap(),
);
let batches = ruhvro::deserialize::per_datum_deserialize_threaded(datums, schema.clone(), 8).unwrap();
```

## Benchmarks

Criterion microbenchmarks live under `benches/` and cover both the deserialize and
serialize paths across four schema shapes: `flat_primitives`, `nullable_primitives`,
`nested_struct`, and `array_and_map`. Each measures the single-threaded path and the
8-chunk rayon-parallel path on 1,000 records.

Run everything:

```sh
cargo bench -p ruhvro
```

Run one direction or one schema:

```sh
cargo bench -p ruhvro --bench deserialize
cargo bench -p ruhvro --bench serialize
cargo bench -p ruhvro --bench deserialize -- flat_primitives
```

Criterion writes HTML reports to `target/criterion/report/index.html` after each run
(with regression deltas if you've run before).

### Profiling

To see where time is going inside a hot bench, `samply` is the easiest macOS option
since it doesn't need sudo:

```sh
cargo install samply
cargo bench -p ruhvro --no-run                    # produces target/release/deps/deserialize-<hash>
samply record target/release/deps/deserialize-<hash> --bench flat_primitives/per_datum_deserialize --profile-time 5
```

On Linux, `cargo flamegraph --bench deserialize -- flat_primitives/per_datum_deserialize`
works equivalently and writes `flamegraph.svg`.
