use crate::schema_translate::to_arrow_schema;
use std::collections::HashMap;
use std::error::Error;
use std::hash::Hasher;
use std::sync::Arc;

use anyhow::Result;
use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanBuilder,
    Int32Builder, Int64Builder, ListBuilder, MapBuilder,
     RecordBatch, StringBuilder, StructArray, StructBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use rayon::prelude::*;

pub fn parse_schema(schema_string: &str) -> Result<AvroSchema> {
    Ok(AvroSchema::parse_str(schema_string)?)
}

pub fn per_datum_deserialize(data: &Vec<Vec<u8>>, schema: &AvroSchema) -> Box<Vec<Result<Value>>> {
    Box::new(
        data.into_iter()
            .map(|datum| {
                let mut sliced = &datum[..];
                Ok(from_avro_datum(schema, &mut sliced, None)?)
            })
            .collect::<Vec<_>>(),
    )
}

pub fn per_datum_deserialize_multi(data: &Vec<Vec<u8>>, schema: &AvroSchema) -> Vec<Result<Value>> {
    data.par_iter()
        .map(|datum| {
            let mut sliced = &datum[..];
            Ok(from_avro_datum(schema, &mut sliced, None)?)
        })
        .collect::<Vec<_>>()
}

#[derive(Debug)]
struct DeserialzeError;
impl Error for DeserialzeError {}
impl std::fmt::Display for DeserialzeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub fn per_datum_arrow_deserialize(data: ArrayRef, schema: &AvroSchema) -> Vec<Vec<Result<Value>>> {
    let arr = data
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DeserialzeError)
        .unwrap();
    let mut slices = vec![];
    let cores = 24usize;
    let chunk_size = arr.len() / cores;
    for i in (0..cores) {
        let slice = arr.slice(i * chunk_size, chunk_size);
        slices.push(slice)
    }
    let p = slices
        .par_iter()
        .map(|x| {
            x.iter()
                .map(|inn| {
                    let mut a = inn.unwrap();
                    Ok(from_avro_datum(schema, &mut a, None).unwrap())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    p
}

pub fn per_datum_deserialize_arrow(data: ArrayRef, schema: &AvroSchema) -> StructArray {
    let fields = to_arrow_schema(schema).unwrap().fields;
    let arr = data
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DeserialzeError)
        .unwrap();
    let mut builder = StructBuilder::from_fields(fields.clone(), data.len());
    arr.into_iter().for_each(|datum| {
        let mut sliced = &datum.unwrap()[..];
        let avro = from_avro_datum(schema, &mut sliced, None).unwrap();
        match avro {
            Value::Record(inner) => {
                let a = inner.into_iter().map(|(x, y)| y).collect::<Vec<_>>();
                build_arrays_fields(&a, &mut builder, &fields);
            }
            _ => unimplemented!(),
        }
    });
    builder.finish()
}

pub fn per_datum_deserialize_arrow_multi(data: ArrayRef, schema: &AvroSchema) -> Vec<RecordBatch> {
    let arrow_schema = Arc::new(to_arrow_schema(schema).unwrap());
    let fields = &arrow_schema.fields;
    let arr = data
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DeserialzeError)
        .unwrap();
    let mut slices = vec![];
    let cores = 24usize;
    let chunk_size = arr.len() / cores;
    for i in (0..cores) {
        let slice = arr.slice(i * chunk_size, chunk_size);
        slices.push(slice)
    }
    let r = slices
        .par_iter()
        .map(|da| {
            let mut builder = StructBuilder::from_fields(fields.clone(), data.len());
            da.iter().for_each(|avro_data| {
                let mut sliced = avro_data.unwrap();
                let deserialized = from_avro_datum(schema, &mut sliced, None).unwrap();
                match deserialized {
                    Value::Record(inner) => {
                        let a = inner.into_iter().map(|(x, y)| y).collect::<Vec<_>>();
                        build_arrays_fields(&a, &mut builder, &fields);
                    }
                    _ => unimplemented!(),
                }
            });
            let d = builder.finish();
            let dd: RecordBatch = d.into();
            dd
        })
        .collect::<Vec<_>>();
    r
}

fn build_arrays_fields(data: &Vec<Value>, builder: &mut StructBuilder, fields: &Fields) -> () {
    data.iter()
        .zip(fields)
        .enumerate()
        // .inspect(|i| println!("{:?}", i))
        .for_each(|(idx, (avro_val, field))| {
            match field.data_type() {
                DataType::Boolean => {
                    let target = builder
                        .field_builder::<BooleanBuilder>(idx)
                        .expect("Did not find Bool builder");
                    // can be bool or null
                    if let &Value::Boolean(b) = avro_val {
                        target.append_value(b);
                    } else {
                        target.append_null();
                    }
                }
                DataType::Int32 => {
                    let target = builder
                        .field_builder::<Int32Builder>(idx)
                        .expect("Did not find Int builder");
                    if let &Value::Int(i) = avro_val {
                        target.append_value(i);
                    } else {
                        target.append_null();
                    }
                }
                DataType::Int64 => {
                    let target = builder
                        .field_builder::<Int64Builder>(idx)
                        .expect("Did not find Int64 builder");
                    if let &Value::Long(i) = avro_val {
                        target.append_value(i);
                    }
                    if let &Value::TimestampMillis(i) = avro_val {
                        target.append_value(i);
                    } else {
                        target.append_null();
                    }
                }
                DataType::UInt8 => {}
                DataType::UInt16 => {}
                DataType::UInt32 => {}
                DataType::UInt64 => {}
                DataType::Float16 => {}
                DataType::Float32 => {}
                DataType::Float64 => {}
                DataType::Timestamp(a, b) => {
                    let ta = builder
                        .field_builder::<TimestampMillisecondBuilder>(idx)
                        .expect("expected TimestampMillisecondBuilder");
                    if let &Value::TimestampMillis(i) = avro_val {
                        ta.append_value(i);
                    } else {
                        ta.append_null();
                    }
                }
                DataType::Date32 => {}
                DataType::Date64 => {}
                DataType::Time32(_) => {}
                DataType::Time64(_) => {}
                DataType::Binary => {}
                DataType::Utf8 => {
                    let target = builder
                        .field_builder::<StringBuilder>(idx)
                        .expect("Did not find StringBuilder");
                    if let Value::String(s) = avro_val {
                        target.append_value(s);
                    } else {
                        target.append_null();
                    }
                }
                DataType::List(l) => {
                    let target = builder
                        .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(idx)
                        .expect("Did not find ListBuilder");
                    let inner = target.values();
                    if let Value::Array(vs) = avro_val {
                        for v in vs {
                            add_to_list(v, inner, l);
                        }
                        target.append(true);
                    } else {
                        target.append_null();
                    }
                }
                DataType::Struct(fields) => {
                    let mut ta = builder
                        .field_builder::<StructBuilder>(idx)
                        .expect("Did not find StructBuilder");
                    if let Value::Record(items) = avro_val {
                        let avro_items = items
                            .into_iter()
                            .map(|(_, y)| y.clone())
                            .collect::<Vec<_>>();
                        build_arrays_fields(&avro_items, ta, fields);
                    } else {
                        unimplemented!();
                    }
                }
                DataType::Union(a, b) => {
                    unimplemented!();
                }
                DataType::Map(a, b) => {
                    let ta = builder
                        .field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(
                            idx,
                        )
                        .expect("MapBuilder not found");
                    if let Value::Map(hm) = avro_val {
                        hm.iter().for_each(|(k, v)| {
                            // println!("{:?}", a);
                            add_to_list(
                                &Value::String(k.clone()),
                                ta.keys(),
                                &Field::new("key", DataType::Utf8, false),
                            );
                            add_to_list(
                                v,
                                ta.values(),
                                &Field::new("value", DataType::Int32, false),
                            );
                        });
                        ta.append(true);
                    }
                }
                _ => unimplemented!(),
            };
        });
    builder.append(true);
}

fn add_to_list(data: &Value, builder: &mut Box<dyn ArrayBuilder>, field: &Field) -> () {
    match field.data_type() {
        DataType::Boolean => {
            let ta = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .expect("expected boolean builder");
            if let Value::Boolean(b) = data {
                ta.append_value(*b);
            } else {
                ta.append_null();
            }
        }
        // DataType::Int8 => {}
        // DataType::Int16 => {}
        DataType::Int32 => {
            let ta = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .expect("Did not find Int builder");
            if let &Value::Int(i) = data {
                ta.append_value(i);
            } else {
                ta.append_null();
            }
        }
        DataType::Int64 => {
            let target = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("Did not find Int64 builder");
            if let &Value::Long(i) = data {
                target.append_value(i);
            } else {
                target.append_null();
            }
        }
        // DataType::UInt8 => {}
        // DataType::UInt16 => {}
        // DataType::UInt32 => {}
        // DataType::UInt64 => {}
        // DataType::Float16 => {}
        // DataType::Float32 => {}
        // DataType::Float64 => {}
        // DataType::Timestamp(_, _) => {}
        // DataType::Date32 => {}
        // DataType::Date64 => {}
        // DataType::Time32(_) => {}
        // DataType::Time64(_) => {}
        // DataType::Duration(_) => {}
        // DataType::Interval(_) => {}
        // DataType::Binary => {}
        DataType::Utf8 => {
            let ta = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .expect("Did not find StringBuilder");
            if let Value::String(s) = data {
                ta.append_value(s.clone());
            } else {
                ta.append_null();
            }
        }
        // DataType::List(_) => {}
        // DataType::Struct(_) => {}
        // DataType::Union(_, _) => {}
        // DataType::Map(_, _) => {}
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::{Int32Array, Int32Builder, ListBuilder, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields};

    use super::*;
    use crate::utils;

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
        let encoded = utils::decode_hex(avro_datum);
        let newv = vec![&encoded[..], &encoded[..], &encoded[..], &encoded[..]];
        // let arrow_array = Arc::new(array::BinaryArray::from_vec(newv)) as ArrayRef;
        // let decoded = per_datum_arrow_deserialize(arrow_array, &parsed_schema);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        let fields = crate::schema_translate::to_arrow_schema(&parsed_schema).unwrap();
        println!("{:?}", fields);
        // let r = per_datum_deserialize_arrow(&vec![encoded.clone()], &parsed_schema, &fields.fields);
        // println!("{:?}", r);
        // println!("{:?}", decoded);
    }

    #[test]
    fn test_per_datum_deserialize() {
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
        let encoded = utils::decode_hex(avro_datum);
        let decoded = per_datum_deserialize(&vec![encoded], &parsed_schema);

        let expected = Value::Record(vec![
            (
                "userId".into(),
                Value::String("44d7d0ef-a2df-483e-92a1-1523806adec4".into()),
            ),
            ("age".into(), Value::Int(28)),
            (
                "fullName".into(),
                Value::Record(vec![
                    ("firstName".into(), Value::String("Linda".into())),
                    ("lastName".into(), Value::String("Ware".into())),
                ]),
            ),
            (
                "email".into(),
                Value::Union(1, Box::new(Value::String("lindascott@example.net".into()))),
            ),
            (
                "phoneNumbers".into(),
                Value::Array(vec![
                    Value::String("(266)740-1277x01142".into()),
                    Value::String("001-959-894-6500x799".into()),
                    Value::String("001-396-819-6800x0019".into()),
                ]),
            ),
            ("isPremiumMember".into(), Value::Boolean(false)),
            (
                "favoriteItems".into(),
                Value::Map(HashMap::from([
                    ("Mr".into(), Value::Int(8)),
                    ("find".into(), Value::Int(7)),
                    ("approach".into(), Value::Int(6)),
                ])),
            ),
            (
                "registrationDate".into(),
                Value::TimestampMillis(1641154756000i64),
            ),
        ]);
        assert!(&expected == decoded.get(0).unwrap().as_ref().unwrap());
    }
}
