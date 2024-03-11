use std::error::Error;

use anyhow::Result;
use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::array::{
    make_builder, Array, ArrayBuilder, ArrayRef, BinaryArray, Int32Builder, Int8Builder,
    StringBuilder, StructBuilder,
};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Fields};
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

pub fn per_datum_deserialize_multi(
    data: &Vec<&Vec<u8>>,
    schema: &AvroSchema,
) -> Vec<Result<Value>> {
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

// this seems to ahve been reimplementing Structbuilder
// struct BuilderArrays<'a> {
//     fields: &'a Fields,
//     avro_schema: Option<&'a AvroSchema>,
//     arrow_schema: Option<&'a ArrowSchema>,
//     builders: Vec<Box<dyn ArrayBuilder>>,
// }
//
// impl<'a> BuilderArrays<'a> {
//     fn from_arrow_schema(arrow_schema: &'a ArrowSchema, capacity: usize) -> Self {
//         let c = arrow_schema;
//         let f = c.fields.iter();
//         let mut builder_vec = vec![];
//         for i in f {
//             let b = make_builder(i.data_type(), capacity);
//             builder_vec.push(b);
//         }
//         BuilderArrays {
//             fields: &arrow_schema.fields,
//             avro_schema: None,
//             arrow_schema: Some(&arrow_schema),
//             builders: builder_vec,
//         }
//     }
// }


// maybe this needs to return result
fn build_arrays(data: Vec<&Value>, builder: &mut StructBuilder, fields: &Fields) -> () {
    // first Value should be a  and first item in builder should be first data
    let iters = data.iter().zip(fields).enumerate().for_each(|(idx, (avro_val, field))| {

        match avro_val {
           Value::Int(i) => {
               let ta = builder.field_builder::<Int32Builder>(idx).unwrap_or_else(|| panic!("Error unwrapping target array"));
               ta.append_value(*i);
           },
            Value::String(st) => {
                let ta = builder.field_builder::<StringBuilder>(idx).unwrap_or_else(|| panic!("Error unwrapping target array"));
                ta.append_value(st);
            },
            Value::Record(d) => {
                let mut ta = builder.field_builder::<StructBuilder>(idx).unwrap_or_else(|| panic!("Error unwrapping target array"));
                if let DataType::Struct(inner_fields) = field.data_type() {
                    let inner_data = d.into_iter().map(|(x,y)| y).collect::<Vec<_>>();
                    build_arrays(inner_data, &mut ta, inner_fields);
                    ta.append(true);
                } else { panic!("couldnt find field struct")}
            },
            _ => unimplemented!()
        }
    });



}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use apache_avro::to_value;
    use arrow::array;
    use arrow::array::{Int32Array, Int32Builder, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields};
    use arrow::ipc::Int;

    use super::*;
    use crate::utils;

    #[test]
    fn test_build_arrays() {
        let avro_data = vec![Value::Int(2), Value::String("abc".into()), Value::Record(vec![
            ("inner_int".into(), Value::Int(1)),
            ("inner_str".into(), Value::String("cba".into())),
        ])];
        let avro_iter = avro_data.iter().collect::<Vec<_>>();
        let inner_int_fields = Field::new("inner_int", DataType::Int32, false);
        let inner_string_field = Field::new("inner_string", DataType::Utf8, false);
        let field_struct = Field::new("struct_f", DataType::Struct(Fields::from(vec![inner_int_fields, inner_string_field])), false);
        let field1 = Field::new("int_arr", DataType::Int32, false);
        let field2 = Field::new("str_arr", DataType::Utf8, false);
        let fields = Fields::from(vec![field1, field2, field_struct]);
        let mut sb = StructBuilder::from_fields(fields.clone(), 1);
        build_arrays(avro_iter, &mut sb, &fields);
        sb.append(true);
        let r = sb.finish();
        println!("{:?}", r);
    }
    #[test]
    fn test_struct_builder() {
        let sample_avro = vec![Value::Record(vec![
            ("intcol".into(), Value::Int(3)),
            ("stringcol".into(), Value::String("abc".into())),
            ("nested".into(), Value::Record(vec![
                ("nested_string".into(), Value::String("nested_string".into())),
                ("nested_int".into(), Value::Int(123))
            ]))
        ])];
        let inner = vec![
            Field::new("nested_string", DataType::Utf8, false),
            Field::new("nested_int", DataType::Int32, false),
        ];
        let not_inner = vec![
            Field::new("intcol", DataType::Int32, false),
            Field::new("stringcol", DataType::Utf8, false),
            Field::new("nested", DataType::Struct(Fields::from(inner)), false),
        ];
        let field_outer = Field::new("outer", DataType::Struct(Fields::from(not_inner)), false);
        let fields = Fields::from(vec![field_outer]);

        println!("{:?}", fields);
        let mut b = StructBuilder::from_fields(fields.clone(), 5);

        println!("{:?}", b);
        // let iterss = sample_avro.iter().zip(fields.iter()).for_each(|(x,y)|{
        //
        //     println!("value: {:?}", x);
        //     println!("field: {:?}", y);
        // }
        // );
        let outer_int_col = b.field_builder::<StructBuilder>(0).unwrap();
        println!("outer_int_col: {:?}", outer_int_col);
        // let outer_string_col = b.field_builder::<StringBuilder>(1).unwrap();
        // let nested = b.field_builder::<StructBuilder>(2).unwrap();
        // let nest_str = nested.field_builder::<StringBuilder>(0).unwrap();
        // let nest_int = nested.field_builder::<Int32Builder>(1).unwrap();
        // let builders: Vec<Box<dyn ArrayBuilder>> = vec![Box::new(outer_int_col), Box::new(outer_string_col), Box::new(nested),
        //                                                 Box::new(nest_str), Box::new(nest_int)];

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
        let arrow_array = Arc::new(array::BinaryArray::from_vec(newv)) as ArrayRef;
        let decoded = per_datum_arrow_deserialize(arrow_array, &parsed_schema);
        println!("{:?}", decoded);
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

    #[test]
    fn test_how_to_build_array() {
        // can try pattern buffer (take iterator, but unsafe) -> Scalar buffer -> PrimitiveArray
        // how do lower and upper bounds on iterator work?
        // how to make simple array
        // can simple arrays take iterators?
        let a = vec![1, 2, 3].into_iter();
        let b = vec![5, 4, 3].into_iter();
        let mut builder = Int32Builder::new();
        let z = unsafe {
            builder.append_trusted_len_iter(a);
            builder.append_trusted_len_iter(b);
        };
        let f = builder.finish();
        // println!("{:?}", f);

        // how to make struct arrays. What is nullable buffer for?
        let f1 = Field::new("nested_string", arrow::datatypes::DataType::Utf8, false);
        let f2 = Field::new("nested_int", arrow::datatypes::DataType::Int32, true);
        let fds = Fields::from(vec![f1, f2]);
        let data1 = StringArray::from(vec!["alice", "bob", "charlie"]);
        let data2 = Int32Array::from(vec![Some(1), None, Some(2)]);
        let nb = NullBuffer::from(vec![false, false, false]);
        let a = StructArray::new(fds, vec![Arc::new(data1), Arc::new(data2)], Some(nb));
        // println!("{:?}", a);

        // how to make struct array with a builder? Is best to flatten structs then build?
        // how to build ListArray
        let f1 = Field::new("nested_string", arrow::datatypes::DataType::Utf8, false);
        let f2 = Field::new("nested_int", arrow::datatypes::DataType::Int32, true);
        let fds = Fields::from(vec![f1, f2]);
        let mut sb = StructBuilder::from_fields(fds, 3);
        let data1 = StringArray::from(vec!["alice", "bob", "charlie"]);
        let data2 = Int32Array::from(vec![Some(1), None, Some(2)]);
        let b1 = sb.field_builder::<StringBuilder>(0).unwrap();
        let _ = data1.iter().for_each(|x| {
            b1.append_value(x.unwrap());
            // &sb.append(true);
        }
        );
        let b2 = sb.field_builder::<Int32Builder>(1).unwrap();
        let _ = data2.iter().for_each(|x| {
            b2.append_option(x);
            // match x {
            //     Some(i) => b2.append_value(i),
            //     None => b2.append_null()
            // };
        });
        // println!("{:?}", b1.len());
        for i in 0..data1.len() {sb.append(true);}
        println!("{:?}", &sb.len());
        // println!("{:?}", &b1.len());
        let r= &sb.finish();
        println!("{:?}", r);

    }

    #[test]
    fn test_builder_array() {
        let a = vec![1, 2, 3, 4, 5, 6];
        let mut i = Int32Builder::with_capacity(a.len());
        for z in a {
            i.append_value(z);
        }
        let p = i.finish();
        println!("{:?}", p);
    }

    #[derive(Debug)]
    enum SimpleE {
        A(i32),
        B(String),
    }
    use itertools::izip;
    #[test]
    fn test_inner_iterator_len() {
        let mut a = vec![
            vec![1, 2, 3],
            vec![11, 12, 13],
            vec![11, 12, 13],
            vec![11, 12, 13],
        ];
        // let mut iters = vec![];
        // let iter = a.iter().map(|x| {
        //     x.iter().map(|x| x + 1)
        // });
        //
        // println!("size hint: {:?}", iter.size_hint());

        // let r = a.iter_mut().map(|x| SimpleE::B("ok this is more data".into())).collect::<Vec<_>>();
        let r = izip!(a.iter()).collect::<Vec<_>>();
        println!("{:?}", r);
    }
}
