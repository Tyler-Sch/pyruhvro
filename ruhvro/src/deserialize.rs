use crate::complex::StructContainer;
use crate::fast_decode;
use crate::schema_translate::to_arrow_schema;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use apache_avro::from_avro_datum;
use apache_avro::Schema as AvroSchema;
use arrow::array::{Array, BinaryArray, RecordBatch};
use rayon::prelude::*;
// TODO: binary array
// TODO: refactor full avro deserialization to lib.rs
// TODO: Durations
// TODO: Add tests to assert errors when deserializing
// TODO: add doc strings

/// Parses string into AvroSchema object
pub fn parse_schema(schema_string: &str) -> Result<AvroSchema> {
    Ok(AvroSchema::parse_str(schema_string)?)
}

/// Single threaded, takes a Vec of binary encoded schemaless avro and the parsed avro
/// schema to read them. Dispatches to the [`fast_decode`] path when the schema
/// is in its supported subset; otherwise uses the `Value`-tree path.
pub fn per_datum_deserialize(data: &Vec<&[u8]>, schema: &AvroSchema) -> Result<RecordBatch> {
    if fast_decode::is_supported(schema) {
        return fast_decode::decode(data, schema);
    }
    per_datum_deserialize_baseline(data, schema)
}

/// `Value`-tree based deserialization. Public to the crate so the fast path can
/// run differential tests against it.
pub(crate) fn per_datum_deserialize_baseline(
    data: &Vec<&[u8]>,
    schema: &AvroSchema,
) -> Result<RecordBatch> {
    let fields = to_arrow_schema(schema)?.fields;
    let mut builder = StructContainer::try_new_from_fields(fields, data.len())?;
    data.into_iter().try_for_each(|datum| {
        let mut sliced = &datum[..];
        let avro = from_avro_datum(schema, &mut sliced, None)?;
        let _ = builder.add_val(&avro);
        Ok::<(), anyhow::Error>(())
    })?;
    let sa = builder.try_build_struct_array()?;
    Ok(sa.into())
}

/// Deserializes vector of serialized avro messages with many threads.
/// Each chunk dispatches to the [`fast_decode`] path when the schema is supported.
pub fn per_datum_deserialize_threaded(
    data: Vec<&[u8]>,
    schema: &AvroSchema,
    num_chunks: usize,
) -> Result<Vec<RecordBatch>> {
    let use_fast = fast_decode::is_supported(schema);
    // Compute the Arrow schema once and share it across all chunks — avoids
    // an `to_arrow_schema` walk per chunk on the fast path.
    let arrow_schema = if use_fast {
        Some(Arc::new(to_arrow_schema(schema)?))
    } else {
        None
    };
    let arr = Arc::new(BinaryArray::from_vec(data));
    let mut slices = vec![];
    let cores = num_chunks;
    let chunk_size = arr.len() / cores;
    for i in 0..cores {
        if i == cores - 1 {
            slices.push(arr.slice(i * chunk_size, arr.len() - (i * chunk_size)));
        } else {
            slices.push(arr.slice(i * chunk_size, chunk_size));
        }
    }
    slices
        .par_iter()
        .map(|da| -> Result<RecordBatch> {
            let chunk_refs: Vec<&[u8]> = da
                .iter()
                .map(|x| x.ok_or_else(|| anyhow!("Error getting sliced data")))
                .collect::<Result<Vec<_>>>()?;
            if use_fast {
                fast_decode::decode_with_arrow_schema(
                    &chunk_refs,
                    schema,
                    arrow_schema.as_ref().unwrap(),
                )
            } else {
                per_datum_deserialize_baseline(&chunk_refs, schema)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::to_avro_datum;
    use apache_avro::types::Record;
    use arrow::array::StringArray;
    pub fn decode_hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("Error unwrapping hex"))
            .collect::<Vec<_>>()
    }
    #[test]
    fn test_per_datum_deserialize_arrow() {
        let s = r#"{
            "type": "record",
            "name": "UserData",
            "namespace": "com.example",
            "fields": [
              {
                "name": "userId",
                "type": "string"
              },
              {
                "name": "age",
                "type": "int"
              },
              {
                "name": "fullName",
                "type": {
                  "type": "record",
                  "name": "FullName",
                  "fields": [
                    {"name": "firstName", "type": "string"},
                    {"name": "lastName", "type": "string"}
                  ]
                }
              },
              {
                "name": "email",
                "type": ["null", "string"],
                "default": null
              },
              {
                "name": "phoneNumbers",
                "type": {
                  "type": "array",
                  "items": "string"
                }
              },
              {
                "name": "isPremiumMember",
                "type": "boolean"
              },
              {
                "name": "favoriteItems",
                "type": {
                  "type": "map",
                  "values": "int"
                }
              },
              {
                "name": "registrationDate",
                "type": {
                  "type": "long",
                  "logicalType": "timestamp-millis"
                }
              }
            ]
          }"#;
        let parsed_schema = parse_schema(s).unwrap();
        let avro_datum = "4834346437643065662d613264662d343833652d393261312d313532333830366164656334380a4c696e64610857617265022c6c696e646173636f7474406578616d706c652e6e6574062628323636293734302d31323737783031313432283030312d3935392d3839342d36353030783739392a3030312d3339362d3831392d363830307830303139000006044d72100866696e640e10617070726f6163680c00c0f691c7c35f";
        let encoded = decode_hex(avro_datum);
        let newv = vec![&encoded[..], &encoded[..], &encoded[..], &encoded[..]];
        let result = per_datum_deserialize(&newv, &parsed_schema).unwrap();
        assert_eq!(result.num_columns(), 8);
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_struct_nested_in_list() {
        let s = r#"{
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
        {
            "name": "firstName",
            "type": "string"
        },
        {
            "name": "lastName",
            "type": "string"
        },
        {
            "name": "age",
            "type": "int"
        },
        {
            "name": "addresses",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {
                            "name": "street",
                            "type": "string"
                        },
                        {
                            "name": "city",
                            "type": "string"
                        },
                        {
                            "name": "zipCode",
                            "type": "string"
                        }
                    ]
                }
            }
        },
        {
            "name": "email",
            "type": ["null", "string"],
            "default": "null"
        }
    ]
          }"#;
        let parsed_schema = parse_schema(s).unwrap();
        let avro_datum = "084a6f686e06446f653c041431323320456c6d20537412536f6d6577686572650a313233343514343536204f616b20537410416e7977686572650a36373839300002286a6f686e2e646f65406578616d706c652e636f6d";
        let encoded = decode_hex(avro_datum);
        let newv = vec![&encoded[..]];
        let result = per_datum_deserialize(&newv, &parsed_schema).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 5);
    }

    #[test]
    fn test_enum() {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"},
                    {
                        "name": "c",
                        "type": {
                            "type": "enum",
                            "name": "suit",
                            "symbols": ["diamonds", "spades", "clubs", "hearts"]
                        },
                        "default": "spades"
                    }
                ]
            }
        "#;
        let schema = parse_schema(&raw_schema).unwrap();
        let mut record = Record::new(&schema).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        record.put("c", "clubs");
        let serialized = to_avro_datum(&schema, record).unwrap();
        let mut record2 = Record::new(&schema).unwrap();
        record2.put("a", 28i64);
        record2.put("b", "bar");
        record2.put("c", "hearts");
        let serialized2 = to_avro_datum(&schema, record2).unwrap();
        let a = vec![&serialized[..], &serialized2[..]];
        let arr = per_datum_deserialize(&a, &schema).unwrap();
        assert_eq!(arr.num_columns(), 3);
        assert_eq!(arr.num_rows(), 2);
        let enum_vec = arr
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let expected = StringArray::from(vec!["clubs", "hearts"]);
        assert_eq!(enum_vec, &expected);
    }

    #[test]
    fn test_record_with_nulls() {
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
        let parsed_schema = parse_schema(&raw_schema).unwrap();
        let mut record = Record::new(&parsed_schema).unwrap();
        record.put("a", 27i64);
        record.put("b", None::<String>);
        let serialized = to_avro_datum(&parsed_schema, record).unwrap();
        let result = per_datum_deserialize(&vec![&serialized[..]], &parsed_schema).unwrap();
        assert_eq!(result.num_columns(), 2);
    }
}
