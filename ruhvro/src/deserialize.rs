use std::error::Error;

use anyhow::Result;
use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::array::{make_builder, Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, BinaryArray, Int32Builder, Int8Builder, ListBuilder, StringArray, StringBuilder, StructBuilder, BooleanBuilder, PrimitiveBuilder};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Int32Type};
use enum_as_inner::EnumAsInner;
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

// maybe this needs to return result
fn build_arrays(data: Vec<Value>, builder: &mut StructBuilder, fields: &Fields) -> () {
    // first Value should be a  and first item in builder should be first data

    let iters = data
        .iter()
        .zip(fields)
        .enumerate()
        .for_each(|(idx, (avro_val, field))| match avro_val {
            Value::Int(i) => {
                let ta = builder
                    .field_builder::<Int32Builder>(idx)
                    .unwrap_or_else(|| panic!("Error unwrapping target array"));
                ta.append_value(*i);
            }
            Value::String(st) => {
                let ta = builder
                    .field_builder::<StringBuilder>(idx)
                    .unwrap_or_else(|| panic!("Error unwrapping target array"));
                ta.append_value(st);
            }
            Value::Record(d) => {
                let mut ta = builder
                    .field_builder::<StructBuilder>(idx)
                    .unwrap_or_else(|| panic!("Error unwrapping target array"));
                if let DataType::Struct(inner_fields) = field.data_type() {
                    let inner_data = d.into_iter().cloned().map(|(x, y)| y).collect::<Vec<_>>();
                    build_arrays(inner_data, &mut ta, inner_fields);
                } else {
                    panic!("couldnt find field struct")
                }
            }
            Value::Array(v) => {
                println!("idx: {}", idx);
                match field.data_type() {
                    DataType::List(inner) => match inner.data_type() {
                        DataType::Int32 => {
                            let ta = builder
                                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(idx)
                                .expect("should be a list builder");
                            let ib = ta.values().as_any_mut().downcast_mut::<Int32Builder>().expect("should be an int builder");
                            let vals = v
                                .iter()
                                .for_each(|x| {
                                    let t: LocalVal = x.into();
                                    println!("{:?}", t);
                                    let r = t.into_int().unwrap();
                                    ib.append_value(r);
                                });
                            ta.append(true);
                            // ib.append_value(vals);
                        },
                        DataType::Utf8 => {
                            println!("In Utf8 block");
                            let ta = builder
                            .field_builder::<ListBuilder<StringBuilder>>(idx)
                            .unwrap();
                            println!("got builder");
                            let vals = v
                            .iter()
                            .map(|x| {
                        let t: LocalVal = x.into();
                        Some(t.into_str().unwrap())
                        });
                            ta.append_value(vals);
                        }
                        _ => unimplemented!(),
                    },
                    _ => panic!("Schema mismatch: Should be list type, but found other"),
                }
            }

            _ => unimplemented!(),
        });
    builder.append(true);
}


fn build_arrays_fields(data: &Vec<Value>, builder: &mut StructBuilder, fields: &Fields) -> () {
    data.iter()
        .zip(fields)
        .enumerate()
        .for_each(|(idx, (avro_val, field))| {
            match field.data_type() {
                DataType::Boolean => {
                    let target = builder.field_builder::<BooleanBuilder>(idx).expect("Did not find Bool builder");
                    // can be bool or null
                    if let &Value::Boolean(b) = avro_val {
                        target.append_value(b);
                    }
                    else {
                        target.append_null();
                    }
                }
                DataType::Int32 => {
                    let target = builder.field_builder::<Int32Builder>(idx).expect("Did not find Int builder");
                    if let &Value::Int(i) = avro_val {
                        target.append_value(i);
                    } else {
                        target.append_null();
                    }
                }
                DataType::Int64 => {}
                DataType::UInt8 => {}
                DataType::UInt16 => {}
                DataType::UInt32 => {}
                DataType::UInt64 => {}
                DataType::Float16 => {}
                DataType::Float32 => {}
                DataType::Float64 => {}
                DataType::Timestamp(_, _) => {}
                DataType::Date32 => {}
                DataType::Date64 => {}
                DataType::Time32(_) => {}
                DataType::Time64(_) => {}
                DataType::Binary => {}
                DataType::Utf8 => {
                    let target = builder.field_builder::<StringBuilder>(idx).expect("Did not find StringBuilder");
                    if let Value::String(s) = avro_val {
                        target.append_value(s);
                    } else {
                        target.append_null();
                    }

                }
                DataType::List(_) => {}
                DataType::Struct(fields) => {
                    let mut ta = builder
                        .field_builder::<StructBuilder>(idx)
                        .expect("Did not find StructBuilder");
                    if let Value::Record(items) = avro_val {
                        let avro_items = items.into_iter().map(|(_, y)| y.clone()).collect::<Vec<_>>();
                        build_arrays_fields(&avro_items, ta, fields);
                        ta.append(true);
                    } else {
                        ta.append_null();
                    }
                }
                DataType::Union(_, _) => {}
                DataType::Dictionary(_, _) => {}
                DataType::Map(_, _) => {}
                _ => unimplemented!()
            };
        }
            )
}
fn add_to_primitive_array<T: ArrowPrimitiveType<Native = B>, B>(data: Option<B>, builder: &mut PrimitiveBuilder<T>) -> Result<()> {
    if let Some(d) = data {
        builder.append_value(d);
    } else {
        builder.append_null();
    }
    Ok(())
}


#[derive(Debug, EnumAsInner)]
enum LocalVal<'a> {
    Int(i32),
    Str(&'a str),
    List(Box<Vec<LocalVal<'a>>>),
    Null,
    Boolean(bool),
    Long(i64),
    Float(f32),
}



// impl<T: ArrowPrimitiveType> Into<T> for LocalVal<'_> {
//     fn into(self) -> T {
//         todo!()
//     }
// }


impl<'a> From<&'a Vec<Value>> for LocalVal<'a> {
    fn from(value: &'a Vec<Value>) -> Self {
        let a = value
            .iter()
            .map(|x| {
                let a: LocalVal = x.into();
                a
            })
            .collect::<Vec<_>>();
        LocalVal::List(Box::new(a))
    }
}

impl<'a> From<&'a Value> for LocalVal<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Int(i) => LocalVal::Int(*i),
            Value::String(s) => LocalVal::Str(s),
            Value::Boolean(b) => LocalVal::Boolean(*b),
            Value::Null => LocalVal::Null,
            Value::Long(i) => LocalVal::Long(*i),
            Value::Array(v) => {
                let a = v
                    .iter()
                    .map(|x| {
                        let b: LocalVal = x.into();
                        b
                    })
                    .collect::<Vec<_>>();
                LocalVal::List(Box::new(a))
            }
            _ => unimplemented!(),
        }
    }
}
#[test]
fn test_from() {
    let a = Value::Int(14);
    let b: LocalVal = (&a).into();
    let bb = b.as_int();
    println!("{:?}", bb);
    let c = Value::String("hello".to_string());
    let d: LocalVal = (&c).into();
    let dd = d.as_str();
    println!("{:?}", dd);
    let e = Value::Array(vec![Value::Int(1), Value::Int(2)]);
    let f: LocalVal = (&e).into();
    let ff = f.as_list();
    println!("{:?}", ff);
    let g = vec![Value::Int(1), Value::Int(3)];
    let gg: LocalVal = (&g).into();
    println!("{:?}", gg);
}

fn try_conversion(v: &mut Box<dyn ArrayBuilder>) {
    let a = v.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
    println!("success!");
}



#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use apache_avro::to_value;
    use arrow::array;
    use arrow::array::{downcast_primitive, GenericListBuilder, Int32Array, Int32Builder, LargeListBuilder, ListBuilder, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Fields};
    use arrow::ipc::Int;

    use super::*;
    use crate::utils;

    #[test]
    fn test_struct_builder() {
        let sample_avro = vec![Value::Record(vec![
            ("intcol".into(), Value::Int(3)),
            ("stringcol".into(), Value::String("abc".into())),
            (
                "nested".into(),
                Value::Record(vec![
                    (
                        "nested_string".into(),
                        Value::String("nested_string".into()),
                    ),
                    ("nested_int".into(), Value::Int(123)),
                ]),
            ),
            ("arrcol".into(), Value::Array(vec![
                Value::Int(2),
                Value::Int(2),
                Value::Int(2),
            ]))
        ])];
        let inner = vec![
            Field::new("nested_string", DataType::Utf8, false),
            Field::new("nested_int", DataType::Int32, false),
        ];
        let inside_arrary_field = Arc::new(Field::new("item", DataType::Int32, true));
        let not_inner = vec![
            Field::new("intcol", DataType::Int32, false),
            Field::new("stringcol", DataType::Utf8, false),
            Field::new("nested", DataType::Struct(Fields::from(inner)), false),
            Field::new("in_arr", DataType::List(inside_arrary_field), false)
        ];
        let field_outer = Field::new("outer", DataType::Struct(Fields::from(not_inner)), false);
        let fields = Fields::from(vec![field_outer]);

        for i in fields.to_vec() {
            match i.data_type() {
                DataType::Struct(fs) => {
                    fs.iter().for_each(|x| println!("{:?}", x));
                }
                _ => println!("ok")
            };
        };

        let mut b = StructBuilder::from_fields(fields.clone(), 5);
        let z = b.field_builder::<StructBuilder>(0).unwrap();
        // let c: &dyn ArrayBuilder = z.field_builder::<Box<dyn ArrayBuilder>>(0).unwrap();
        // try_conversion(c);
        // let a= b.field_builder::<Box<dyn ArrayBuilder>>(0).unwrap();
        // println!("{:?}", b);
        // let x = b.field_builder::<ListBuilder<StringBuilder>>(3);
        // let inner = b.field_builder::<StructBuilder>(0).unwrap();
        // let ta = inner.field_builder::<ListBuilder<Int32Builder>>(3);
        // println!("{:?}", ta);
        // println!("{:?}", x);
        // let mut lb:Box<dyn ArrayBuilder> = Box::new(ListBuilder::new(Int8Builder::new()));
        // let down_cast = lb.as_any_mut().downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>().unwrap();
        // println!("{}", down_cast);


        // build_arrays(sample_avro, &mut b, &fields);
        // let r = b.finish();
        // println!("{:?}", r);

        // let iterss = sample_avro.iter().zip(fields.iter()).for_each(|(x,y)|{
        //
        //     println!("value: {:?}", x);
        //     println!("field: {:?}", y);
        // }
        // );
        // let outer_string_col = b.field_builder::<StringBuilder>(1).unwrap();
        // let nested = b.field_builder::<StructBuilder>(2).unwrap();
        // let nest_str = nested.field_builder::<StringBuilder>(0).unwrap();
        // let nest_int = nested.field_builder::<Int32Builder>(1).unwrap();
        // let builders: Vec<Box<dyn ArrayBuilder>> = vec![Box::new(outer_int_col), Box::new(outer_string_col), Box::new(nested),
        //                                                 Box::new(nest_str), Box::new(nest_int)];
    }
    #[test]
    fn test_boxed_list_list_array_builder() {
        // This test is same as `test_list_list_array_builder` but uses boxed builders.
        let values_builder = make_builder(
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            10,
        );
        let mut builder = ListBuilder::new(values_builder);

        //  [[[1, 2], [3, 4]], [[5, 6, 7], null, [8]], null, [[9, 10]]]
        builder
            .values()
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .expect("should be an ListBuilder")
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .expect("should be an Int32Builder")
            .append_value(1);
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
        });
        let b2 = sb.field_builder::<Int32Builder>(1).unwrap();
        let _ = data2.iter().for_each(|x| {
            b2.append_option(x);
            // match x {
            //     Some(i) => b2.append_value(i),
            //     None => b2.append_null()
            // };
        });
        // println!("{:?}", b1.len());
        for i in 0..data1.len() {
            sb.append(true);
        }
        println!("{:?}", &sb.len());
        // println!("{:?}", &b1.len());
        let r = &sb.finish();
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
