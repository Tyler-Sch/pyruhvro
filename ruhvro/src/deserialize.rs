use crate::schema_translate::to_arrow_schema;
use std::error::Error;
use std::sync::Arc;

use anyhow::Result;
use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BooleanBuilder, Date32Builder,
   Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, MapBuilder, RecordBatch,
    StringBuilder, StructArray, StructBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
use arrow::ipc::DurationBuilder;
use rayon::prelude::*;

// TODO: Figure out how to deal with actual unions
// TODO: experiment with threads
// TODO: enums
// TODO: binary array
// TODO: refactor full avro deserialization to lib.rs
// TODO: maybe refactor conversion Value -> Arrow into one function if possible (Struct builder is rigid)
// TODO: Durations

macro_rules! add_val {
    ($val:expr,$target:ident,$field: expr, $($type:ident),*) => {{
        let val = get_val_from_possible_union($val, $field);
        $(
        if let &Value::$type(d) = val {
           $target.append_value(d);
        } )*
        else {
            $target.append_null()
        }
    }}
}

#[inline]
fn get_val_from_possible_union<'a>(value: &'a Value, field: &'a Field) -> &'a Value {
    let val;
    if field.is_nullable() {
        if let Value::Union(_, b) = value {
            val = b.as_ref();
        } else {
            val = value;
        }
    } else {
        val = value;
    }
    val
}

pub fn parse_schema(schema_string: &str) -> Result<AvroSchema> {
    Ok(AvroSchema::parse_str(schema_string)?)
}

#[derive(Debug)]
struct DeserialzeError;
impl Error for DeserialzeError {}
impl std::fmt::Display for DeserialzeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub fn per_datum_deserialize_arrow(data: ArrayRef, schema: &AvroSchema) -> RecordBatch {
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
                let a = inner.into_iter().map(|(_, y)| y).collect::<Vec<_>>();
                build_arrays_fields(&a, &mut builder, &fields);
            }
            _ => unimplemented!(),
        }
    });
    builder.finish().into()
}

pub fn per_datum_deserialize_arrow_multi(
    data: ArrayRef,
    schema: &AvroSchema,
    num_chunks: usize,
) -> Vec<RecordBatch> {
    let arrow_schema = Arc::new(to_arrow_schema(schema).unwrap());
    let fields = &arrow_schema.fields;
    let arr = data
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DeserialzeError)
        .unwrap();
    let mut slices = vec![];
    let cores = num_chunks;
    let chunk_size = arr.len() / cores;
    for i in 0..cores {
        if i == cores {
            slices.push(arr.slice(i * chunk_size, arr.len()));
        } else {
            slices.push(arr.slice(i * chunk_size, chunk_size));
        }
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
                        let a = inner.into_iter().map(|(_, y)| y).collect::<Vec<_>>();
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
        // .map(|(aval, f)| (get_val_from_possible_union(aval, f), f))
        .enumerate()
        // .inspect(|i| println!("{:?}", i))
        .for_each(|(idx, (avro_val, field))| {
            match field.data_type() {
                DataType::Boolean => {
                    let target = builder
                        .field_builder::<BooleanBuilder>(idx)
                        .expect("Did not find Bool builder");
                    add_val!(avro_val, target, field, Boolean);
                }
                DataType::Int32 => {
                    let target = builder
                        .field_builder::<Int32Builder>(idx)
                        .expect("Did not find Int builder");
                    add_val!(avro_val, target, field, Int);
                }
                DataType::Int64 => {
                    let target = builder
                        .field_builder::<Int64Builder>(idx)
                        .expect("Did not find Int64 builder");
                    add_val!(avro_val, target, field, Long, TimestampMillis);
                }
                DataType::Float32 => {
                    let target = builder
                        .field_builder::<Float32Builder>(idx)
                        .expect("Did not find Float32 builder");
                    add_val!(avro_val, target, field, Float);
                }
                DataType::Float64 => {
                    let target = builder
                        .field_builder::<Float64Builder>(idx)
                        .expect("Did not find Float32 builder");
                    add_val!(avro_val, target, field, Double);
                }
                DataType::Timestamp(a, _) => {
                    let _ = match a {
                        TimeUnit::Millisecond => {
                            let target = builder
                                .field_builder::<TimestampMillisecondBuilder>(idx)
                                .expect("expected TimestampMillisecond Builder");
                            add_val!(avro_val, target, field, TimestampMillis);
                        }
                        TimeUnit::Microsecond => {
                            let target = builder
                                .field_builder::<TimestampMicrosecondBuilder>(idx)
                                .expect("expected TimestampMicrosecond Builder");
                            add_val!(avro_val, target, field, TimestampMicros);
                        }
                        _ => unimplemented!(),
                    };
                }
                DataType::Date32 => {
                    let target = builder
                        .field_builder::<Date32Builder>(idx)
                        .expect("expected Date32Builder");
                    add_val!(avro_val, target, field, Date);
                }
                DataType::Time32(_) => {}
                DataType::Time64(_) => {}
                DataType::Binary => {
                    unimplemented!();
                }
                DataType::Utf8 => {
                    let target = builder
                        .field_builder::<StringBuilder>(idx)
                        .expect("Did not find StringBuilder");
                    let val = get_val_from_possible_union(&avro_val, field);
                    if let Value::String(s) = val {
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
                            add_data_to_array(v, inner, l);
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
                    let target = builder
                        .field_builder::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>(
                            idx,
                        )
                        .expect("MapBuilder not found");

                    let inner_datatype = match a.data_type() {
                        DataType::Struct(flds) => flds.to_vec()[1].clone(),
                        _ => unimplemented!(),
                    };

                    let val = get_val_from_possible_union(avro_val, field);
                    if let Value::Map(hm) = val {
                        hm.iter().for_each(|(k, v)| {
                            add_data_to_array(
                                &Value::String(k.clone()),
                                target.keys(),
                                &Field::new("key", DataType::Utf8, false),
                            );
                            add_data_to_array(v, target.values(), &inner_datatype);
                        });
                        target.append(true);
                    }
                }
                _ => unimplemented!(),
            };
        });
    builder.append(true);
}

fn add_data_to_array(data: &Value, builder: &mut Box<dyn ArrayBuilder>, field: &Field) -> () {
    let data = get_val_from_possible_union(data, field);
    match field.data_type() {
        DataType::Boolean => {
            let target = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .expect("expected boolean builder");
            add_val!(data, target, field, Boolean);
        }
        DataType::Int32 => {
            let target = builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .expect("Did not find Int builder");
            add_val!(data, target, field, Int);
        }
        DataType::Int64 => {
            let target = builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .expect("Did not find Int64 builder");
            add_val!(data, target, field, Long);
        }
        DataType::Float32 => {
            let target = builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .expect("Did not find Float32 builder");
            add_val!(data, target, field, Float);
        }
        DataType::Timestamp(a, _) => {
            let _ = match a {
                TimeUnit::Millisecond => {
                    let target = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMillisecondBuilder>()
                        .expect("Did not find TimestampMillisecond builder");
                    add_val!(data, target, field, TimestampMillis);
                }
                TimeUnit::Microsecond => {
                    let target = builder
                        .as_any_mut()
                        .downcast_mut::<TimestampMicrosecondBuilder>()
                        .expect("Did not find TimestampMicrosecond builder");
                    add_val!(data, target, field, TimestampMicros);
                }
                _ => unimplemented!(),
            };
        }
        DataType::Date32 => {
            let target = builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .expect("Did not find Date32 builder");
            add_val!(data, target, field, Date);
        }
        // DataType::Date64 => {}
        // DataType::Time32(_) => {}
        // DataType::Time64(_) => {}
        DataType::Duration(d) => {
            unimplemented!()
        }
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
        DataType::Struct(i) => {
            let ta = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .expect("Did not find StringBuilder");
            if let Value::Record(arr_data) = data {
                let inner_array_data = arr_data
                    .into_iter()
                    .map(|(x, y)| y.clone())
                    .collect::<Vec<_>>();
                build_arrays_fields(&inner_array_data, ta, i);
            }
        }
        // DataType::Union(_, _) => {}
        // DataType::Map(_, _) => {}
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils;
    use arrow::array::{
        Float32Array, Int32Array, Int64Array, MapArray, StringArray,
    };
    use std::collections::HashMap;

    #[test]
    fn test_add_add_data_to_array_null() {
        let f1 = Field::new("nullints", DataType::Int32, true);
        let f2 = Field::new("nullstring", DataType::Utf8, true);

        let mut b1: Box<dyn ArrayBuilder> = Box::new(Int32Builder::new());
        let avro_int_1 = Value::Union(1, Box::new(Value::Int(2)));
        let avro_int_2 = Value::Union(0, Box::new(Value::Null));
        add_data_to_array(&avro_int_1, &mut b1, &f1);
        add_data_to_array(&avro_int_2, &mut b1, &f1);
        let result_int = b1.finish();
        let expected_int: Box<dyn Array> = Box::new(Int32Array::from(vec![Some(2), None]));
        assert_eq!(result_int, expected_int.into());

        let mut b2: Box<dyn ArrayBuilder> = Box::new(StringBuilder::new());
        let avro_string_1 = Value::Union(1, Box::new(Value::String("hello".into())));
        let avro_string_2 = Value::Union(0, Box::new(Value::Null));
        add_data_to_array(&avro_string_1, &mut b2, &f2);
        add_data_to_array(&avro_string_2, &mut b2, &f2);
        let result_str = b2.finish();
        let expected_str: Box<dyn Array> =
            Box::new(StringArray::from(vec![Some("hello".to_string()), None]));
        assert_eq!(result_str, expected_str.into());
    }
    #[test]
    fn test_build_array_fields() {
        // basic
        let a_int = Value::Int(42);
        let a_str = Value::String("ruhvro".into());

        // nested float and list
        let a_nested_float = Value::Float(1.23);
        let a_nested_list = Value::Array(vec![Value::Int(2), Value::Int(3)]);
        let a_rec = Value::Record(vec![
            ("nested_float_field".into(), a_nested_float),
            ("nested_list_field".into(), a_nested_list),
        ]);

        // fields
        let f_int = Field::new("int_field", DataType::Int32, false);
        let f_str = Field::new("str_field", DataType::Utf8, false);
        let f_float = Field::new("nested_float_field", DataType::Float32, false);
        let f_inner_list = Field::new("item", DataType::Int32, true);
        let f_list = Field::new(
            "nested_list_field",
            DataType::List(Arc::new(f_inner_list)),
            false,
        );
        let inner_rec = Field::new(
            "inside_rec",
            DataType::Struct(Fields::from(vec![f_float, f_list])),
            false,
        );
        let top_rec = Fields::from(vec![f_int, f_str, inner_rec]);

        let mut builder = StructBuilder::from_fields(top_rec.clone(), 1);
        let avro_data = vec![a_int, a_str, a_rec];
        let _ = build_arrays_fields(&avro_data, &mut builder, &top_rec);
        let result = builder.finish();

        let e_int: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        let e_str: ArrayRef = Arc::new(StringArray::from(vec!["ruhvro".to_string()]));

        let e_nested_float: ArrayRef = Arc::new(Float32Array::from(vec![1.23]));
        let mut l = ListBuilder::new(Int32Builder::new());
        l.values().append_value(2);
        l.values().append_value(3);
        l.append(true);
        let list: ArrayRef = Arc::new(l.finish());
        let e_rec = Arc::new(
            StructArray::try_from(vec![
                ("nested_float_field", e_nested_float),
                ("nested_list_field", list),
            ])
            .unwrap(),
        );

        let result_vec = vec![
            ("int_field", e_int),
            ("str_field", e_str),
            ("inside_rec", e_rec),
        ];
        let expected = StructArray::try_from(result_vec).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_map_type() {
        let a_nested_map = Value::Map(HashMap::from([
            ("first_val".to_string(), Value::Long(2)),
            ("second_val".to_string(), Value::Long(3)),
        ]));

        let map_keys = Field::new("keys", DataType::Utf8, false);
        let map_values = Field::new("values", DataType::Int64, true);
        let map_fields = Fields::from(vec![map_keys, map_values]);
        let map_struct = Arc::new(Field::new("entries", DataType::Struct(map_fields), false));
        let f_map = Field::new("nested_map_field", DataType::Map(map_struct, false), false);
        let fields = Fields::from(vec![f_map]);

        let mut builder = StructBuilder::from_fields(fields.clone(), 1);
        let _ = build_arrays_fields(&vec![a_nested_map], &mut builder, &fields);
        let result = builder.finish();

        let i = result
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        let k = i.keys().as_any().downcast_ref::<StringArray>().unwrap();
        k.iter().for_each(|x| {
            assert!(x.unwrap() == "first_val".to_string() || x.unwrap() == "second_val".to_string())
        });
        let v = i.values().as_any().downcast_ref::<Int64Array>().unwrap();
        v.iter()
            .for_each(|x| assert!(x.unwrap() == 2i64 || x.unwrap() == 3));

        // assert!(keys.slice(0, 2) )

        // let mut map_builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        // {
        //     map_builder.keys().append_value("first_val");
        //     map_builder.values().append_value(2i64);
        //     map_builder.keys().append_value("second_val");
        //     map_builder.values().append_value(3i64);
        //     map_builder.append(true);
        // }
        // let nested_map:ArrayRef = Arc::new(map_builder.finish());
        // let e_rec: ArrayRef = Arc::new(StructArray::try_from(vec![("nested_map_field", nested_map)]).unwrap());
    }
    #[test]
    fn test_struct_in_list() {
        // avro
        let rec1 = Value::Record(vec![
            ("IntCol".to_string(), Value::Int(2)),
            ("StrCol".to_string(), Value::String("scol".to_string())),
        ]);
        let rec2 = Value::Record(vec![
            ("IntCol".to_string(), Value::Int(3)),
            ("StrCol".to_string(), Value::String("scol2".to_string())),
        ]);
        let l = Value::Array(vec![rec1, rec2]);
        // let outer_rec = Value::Record(vec![("ListCol".to_string(), l)]);

        // arrow schema
        let f1 = Field::new("IntCol", DataType::Int32, false);
        let f2 = Field::new("StrCol", DataType::Utf8, false);
        let inner_fields = Fields::from(vec![f1, f2]);
        let f3 = Arc::new(Field::new(
            "InnerStruct",
            DataType::Struct(inner_fields),
            false,
        ));
        let list_field = Field::new("ListCol", DataType::List(f3), false);
        // let list_fields = Arc::new(Fields::from(vec![list_field]);
        // let fields = Fields::from()

        // builder
        // let builder = StructBuilder::from_fields();

        // convert
        // build_arrays_fields(&vec![l], )
    }

    #[test]
    fn test_list_in_map() {
        unimplemented!();
    }

    fn test_nulls_in_complex_types() {
        unimplemented!();
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
        let encoded = utils::decode_hex(avro_datum);
        let newv = vec![&encoded[..], &encoded[..], &encoded[..], &encoded[..]];
        let arrow_array = Arc::new(BinaryArray::from_vec(newv)) as ArrayRef;
        // let decoded = per_datum_arrow_deserialize(arrow_array, &parsed_schema);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        let fields = crate::schema_translate::to_arrow_schema(&parsed_schema).unwrap();
        println!("{:?}", fields);
        // let data =
        let result = per_datum_deserialize_arrow(arrow_array, &parsed_schema);
        println!("{:?}", result);
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
        // let decoded = per_datum_deserialize(&vec![encoded], &parsed_schema);
        //
        // let expected = Value::Record(vec![
        //     (
        //         "userId".into(),
        //         Value::String("44d7d0ef-a2df-483e-92a1-1523806adec4".into()),
        //     ),
        //     ("age".into(), Value::Int(28)),
        //     (
        //         "fullName".into(),
        //         Value::Record(vec![
        //             ("firstName".into(), Value::String("Linda".into())),
        //             ("lastName".into(), Value::String("Ware".into())),
        //         ]),
        //     ),
        //     (
        //         "email".into(),
        //         Value::Union(1, Box::new(Value::String("lindascott@example.net".into()))),
        //     ),
        //     (
        //         "phoneNumbers".into(),
        //         Value::Array(vec![
        //             Value::String("(266)740-1277x01142".into()),
        //             Value::String("001-959-894-6500x799".into()),
        //             Value::String("001-396-819-6800x0019".into()),
        //         ]),
        //     ),
        //     ("isPremiumMember".into(), Value::Boolean(false)),
        //     (
        //         "favoriteItems".into(),
        //         Value::Map(HashMap::from([
        //             ("Mr".into(), Value::Int(8)),
        //             ("find".into(), Value::Int(7)),
        //             ("approach".into(), Value::Int(6)),
        //         ])),
        //     ),
        //     (
        //         "registrationDate".into(),
        //         Value::TimestampMillis(1641154756000i64),
        //     ),
        // ]);
        // assert!(&expected == decoded.get(0).unwrap().as_ref().unwrap());
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
        let encoded = utils::decode_hex(avro_datum);
        let newv = vec![&encoded[..]];
        let arrow_array = Arc::new(BinaryArray::from_vec(newv)) as ArrayRef;
        // let decoded = per_datum_arrow_deserialize(arrow_array, &parsed_schema);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        let fields = crate::schema_translate::to_arrow_schema(&parsed_schema).unwrap();
        println!("{:?}", fields);
        let result = per_datum_deserialize_arrow(arrow_array, &parsed_schema);
        println!("{:?}", result);
    }

    #[test]
    fn test_timestamps() {
        let s = r#"{
           "type": "record",
    "name": "TimestampTypes",
    "fields": [
        {"name": "date", "type": {"type": "int", "logicalType": "date"}}
    ]
          }"#;
        let parsed_schema = parse_schema(s).unwrap();
        let avro_datum = "ccb502";
        let encoded = utils::decode_hex(avro_datum);
        let newv = vec![&encoded[..]];
        let arrow_array = Arc::new(BinaryArray::from_vec(newv)) as ArrayRef;
        // let decoded = per_datum_arrow_deserialize(arrow_array, &parsed_schema);
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        let fields = crate::schema_translate::to_arrow_schema(&parsed_schema).unwrap();
        println!("{:?}", fields);
        let result = per_datum_deserialize_arrow(arrow_array, &parsed_schema);
        println!("{:?}", result);
    }
}
