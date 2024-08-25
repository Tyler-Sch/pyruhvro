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
     let deserialized = ruhvro::deserialize::per_datum_deserialize(&vec![&serialized[..]], &parsed_schema);
     println!("{:?}", deserialized);
    
     // serialize the record batch
     let serialized = ruhvro::serialize::serialize_record_batch(deserialized, &parsed_schema, 1);
     println!("{:?}", serialized);
}
```

