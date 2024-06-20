#![warn(clippy::pedantic)]
//! Crate to deserialize avro and return arrow.
//!
//! This library was made to enable quick efficient transformation of schemaless encoded avro
//! data into arrow record batches.
//!


/// Converts Avro to Arrow
/// Decode Avro data returning an Arrow Record Batch.
/// ## Example
/// ```
/// let raw_schema = r#"
///             {
///                 "type": "record",
///                 "name": "test",
///                 "fields": [
///                     {"name": "a", "type": "long", "default": 42},
///                     {"name": "b",
///                         "type": ["null", "string"],
///                         "default": null
///             }
///                 ]
///             }
///         "#;
///
/// let parsed_schema = ruhvro::deserialize::parse_schema(&raw_schema).unwrap();
/// // create a record
/// let mut record = apache_avro::types::Record::new(&parsed_schema).unwrap();
/// record.put("a", 27i64);
/// record.put("b", None::<String>);
/// // and serialize it
/// let serialized = apache_avro::to_avro_datum(&parsed_schema, record).unwrap();
/// // verify deserialization
/// let result = ruhvro::deserialize::per_datum_deserialize(&vec![&serialized[..]], &parsed_schema);
/// println!("{:?}", result);
///```
pub mod deserialize;
mod schema_translate;
mod complex;
pub mod conversions;
#[cfg(test)]
mod tests {
}
