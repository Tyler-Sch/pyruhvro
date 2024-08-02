use anyhow::anyhow;
use anyhow::Result;
use apache_avro::schema::{Name, RecordField, RecordFieldOrder, RecordSchema, UnionSchema};
use apache_avro::to_avro_datum;
use apache_avro::types::Value;
use apache_avro::Schema;
use arrow::array::UnionArray;
use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, AsArray, BooleanArray, Float32Array,
    GenericBinaryArray, GenericBinaryBuilder, GenericListArray, GenericStringArray, Int32Array,
    PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Float32Type;
use arrow::datatypes::UnionFields;
use arrow::datatypes::{DataType, Field, Fields, Int32Type, TimestampMillisecondType};
use rayon::prelude::*;
use std::any::Any;
use std::sync::Arc;

// TODO: Should be checks to make sure avro and arrow schema match
// TODO: union type with more than simple variants
// TODO: need to figure out names. Should serialize match by name or position?
// TODO: Should it include namespace in matching?
// TODO: Map types
// TODO: Add check for sparse union types
//TODO: remove any unwraps and check results/errors

/// Serializes a `RecordBatch` into a vector of `GenericBinaryArray<i32>`.
///
/// This function takes a `RecordBatch` and a schema as input and serializes the data
/// in the `RecordBatch` into a vector of `GenericBinaryArray<i32>`. Each `GenericBinaryArray<i32>`
/// represents a chunk of the serialized data.
///
/// # Arguments
///
/// * `rb` - The `RecordBatch` to be serialized.
/// * `schema` - The schema of the `RecordBatch`.
///
/// # Returns
///
/// A vector of `GenericBinaryArray<i32>` representing the serialized data.
pub fn serialize_record_batch(rb: RecordBatch, schema: &Schema) -> Vec<GenericBinaryArray<i32>> {
    let struct_arry: ArrayRef = Arc::<StructArray>::new(rb.into());
    let num_chunks = 8;
    let chunk_size = struct_arry.len() / num_chunks;
    let slices: Vec<_> = (0..num_chunks)
        .map(|i| {
            if i == num_chunks - 1 {
                struct_arry.slice(i * chunk_size, struct_arry.len() - (i * chunk_size))
            } else {
                struct_arry.slice(i * chunk_size, chunk_size)
            }
        })
        .collect();
    slices.par_iter().map(|x| serialize(schema, x)).collect()
}

fn serialize(schema: &Schema, struct_arry: &ArrayRef) -> GenericBinaryArray<i32> {
    let mut arr_container = ArrayContainers::try_new(struct_arry, schema).unwrap();
    let mut builder = GenericBinaryBuilder::new();
    (0..struct_arry.len()).for_each(|_| {
        let val = arr_container.get_next();
        let serialized = to_avro_datum(schema, val).unwrap();
        builder.append_value(serialized);
    });
    builder.finish()
}

/// Checks if column should be encoded as an avro union type
fn is_simple_null_union_type(avro_schema: &Schema) -> Option<usize> {
    let _ = match avro_schema {
        Schema::Union(us) => {
            for (idx, schema) in us.variants().iter().enumerate() {
                if let Schema::Null = schema {
                    return Some(idx);
                } else if idx > 1 {
                    return None;
                }
            }
        }
        _ => (),
    };
    None
}

trait ContainerIter {
    fn next_item(&mut self) -> Option<Value>;
    fn next_chunk(&mut self, num_items: i32) -> Vec<Value>;
}

#[derive(Debug)]
enum ArrayContainers<'a> {
    BoolContainer(PrimArrayContainer<&'a BooleanArray>),
    FloatConatiner(PrimArrayContainer<&'a Float32Array>),
    IntContainer(PrimArrayContainer<&'a PrimitiveArray<Int32Type>>),
    ListContainer(Box<ListArrayContainer<'a>>),
    RecordContainer(Box<StructArrayContainer<'a>>),
    StringContainer(PrimArrayContainer<&'a GenericStringArray<i32>>),
    TimestampMillisContainer(PrimArrayContainer<&'a PrimitiveArray<TimestampMillisecondType>>),
    UnionContainer(Box<UnionArrayContainer<'a>>),
}

impl<'a> ArrayContainers<'a> {
    fn try_new(data: &'a ArrayRef, schema: &Schema) -> Result<Self> {
        match schema {
            Schema::Int => {
                let inner_arr = data.as_primitive::<Int32Type>();
                Ok(ArrayContainers::IntContainer(PrimArrayContainer::try_new(
                    inner_arr, schema,
                )?))
            }
            Schema::Float => {
                let inner_arr = data.as_primitive::<Float32Type>();
                Ok(ArrayContainers::FloatConatiner(PrimArrayContainer::try_new(
                    inner_arr, schema,
                )?))
            }
            Schema::Boolean => {
                let inner_arr = data.as_boolean();
                Ok(ArrayContainers::BoolContainer(PrimArrayContainer::try_new(
                    inner_arr, schema,
                )?))
            }
            Schema::TimestampMillis => {
                let inner_arr = data.as_primitive::<TimestampMillisecondType>();
                Ok(ArrayContainers::TimestampMillisContainer(
                    PrimArrayContainer::try_new(inner_arr, schema)?,
                ))
            }
            Schema::String => {
                let inner_arr = data.as_string::<i32>();
                Ok(ArrayContainers::StringContainer(
                    PrimArrayContainer::try_new(inner_arr, schema)?,
                ))
            }
            Schema::Record(_) => {
                let inner_arr = data.as_struct();
                Ok(ArrayContainers::RecordContainer(Box::new(
                    StructArrayContainer::try_new(inner_arr, schema)?,
                )))
            }
            Schema::Array(s) => {
                let inner_arr = data.as_list::<i32>();
                Ok(ArrayContainers::ListContainer(Box::new(
                    ListArrayContainer::try_new(inner_arr, schema)?,
                )))
            }
            Schema::Union(us) => Ok(ArrayContainers::UnionContainer(Box::new(
                UnionArrayContainer::try_new(data, schema)?,
            ))),
            _ => unimplemented!(),
        }
    }

    fn get_next(&mut self) -> Value {
        match self {
            ArrayContainers::IntContainer(ref mut i) => i.next_item().unwrap(),
            ArrayContainers::StringContainer(ref mut i) => i.next_item().unwrap(),
            ArrayContainers::RecordContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::ListContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::UnionContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::TimestampMillisContainer(inner) => {
                let a = inner.next_item().unwrap();
                match a {
                    Value::Long(i) => Value::TimestampMillis(i),
                    _ => {
                        unreachable!()
                    }
                }
            }
            ArrayContainers::BoolContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::FloatConatiner(inner) => inner.next_item().unwrap(),
        }
    }

    fn get_next_chunk(&mut self, num_items: i32) -> Vec<Value> {
        match self {
            ArrayContainers::IntContainer(ref mut i) => i.next_chunk(num_items),
            ArrayContainers::StringContainer(ref mut i) => i.next_chunk(num_items),
            ArrayContainers::RecordContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::ListContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::UnionContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::TimestampMillisContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::BoolContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::FloatConatiner(inner) => inner.next_chunk(num_items),
        }
    }
}

#[derive(Debug)]
struct PrimArrayContainer<A: ArrayAccessor>
where
    A::Item: Into<Value>,
{
    arr_iter: ArrayIter<A>,
}

impl<A: ArrayAccessor> PrimArrayContainer<A>
where
    A::Item: Into<Value>,
{
    fn try_new(arr: A, _schema: &Schema) -> Result<Self> {
        let c = arr;
        let arr_iter = ArrayIter::new(c);
        Ok(PrimArrayContainer { arr_iter })
    }
}

impl<A: ArrayAccessor> ContainerIter for PrimArrayContainer<A>
where
    A::Item: Into<Value>,
{
    fn next_item(&mut self) -> Option<Value> {
        let z = self.arr_iter.next();
        match z {
            Some(i) => Some(i.map_or(Value::Null, |x| x.into())),
            None => {
                panic!("Tried to consume past end of array")
            }
        }
    }

    fn next_chunk(&mut self, num_items: i32) -> Vec<Value> {
        let mut result = vec![];
        for _ in 0..num_items {
            let item = self.next_item();
            match item {
                Some(item) => result.push(item),
                None => panic!("Tried to consume past end of array"),
            }
        }
        result
    }
}
#[derive(Debug)]
struct StructArrayContainer<'a> {
    // do I need schema here? I think so
    data: Vec<ArrayContainers<'a>>,
    column_names: Vec<String>,
    null_buff: Option<&'a NullBuffer>,
    current_pos: usize,
}

impl<'a> StructArrayContainer<'a> {
    fn try_new(data: &'a StructArray, schema: &Schema) -> Result<Self> {
        let mut column_names = vec![];
        let inner_arrays: Result<Vec<ArrayContainers>> = if let Schema::Record(rs) = schema {
            let schemas = &rs.fields;
            (0..schemas.len())
                .map(|idx| {
                    column_names.push(String::from(&schemas[idx].name));
                    Ok(ArrayContainers::try_new(
                        data.column(idx),
                        &schemas[idx].schema,
                    )?)
                })
                .collect()
        } else {
            panic!("Expected record schema");
        };
        // let null_buff = None;
        let null_buff = data.nulls();
        Ok(StructArrayContainer {
            data: inner_arrays?,
            column_names,
            null_buff,
            current_pos: 0,
        })
    }
}

impl ContainerIter for StructArrayContainer<'_> {
    fn next_item(&mut self) -> Option<Value> {
        // needs to handle nulls
        if let Some(nb) = self.null_buff {
            if nb.is_null(self.current_pos) {
                self.current_pos += 1;
                Some(Value::Null)
            } else {
                let mut struct_values = vec![];
                for (mut i, j) in self.data.iter_mut().zip(self.column_names.iter()) {
                    let next_val = i.get_next();
                    struct_values.push((j.clone(), next_val));
                }
                self.current_pos += 1;
                Some(Value::Record(struct_values))
            }
        } else {
            let mut struct_values = vec![];
            for (mut i, j) in self.data.iter_mut().zip(self.column_names.iter()) {
                let next_val = i.get_next();
                struct_values.push((j.clone(), next_val));
            }
            Some(Value::Record(struct_values))
        }
    }

    fn next_chunk(&mut self, num_items: i32) -> Vec<Value> {
        todo!()
    }
}

#[derive(Debug)]
struct ListArrayContainer<'a> {
    inner_data: ArrayContainers<'a>,
    offsets: &'a OffsetBuffer<i32>,
    null_buff: Option<&'a NullBuffer>,
    current_pos: usize,
}

impl<'a> ListArrayContainer<'a> {
    fn try_new(data: &'a GenericListArray<i32>, schema: &Schema) -> Result<Self> {
        let offsets = data.offsets();
        let inner = data.values();
        let inner_schema = if let Schema::Array(s) = schema {
            s
        } else {
            panic!("{}", format!("Expected list schema got {:?}", schema))
        };
        let inner_data = ArrayContainers::try_new(inner, inner_schema)?;
        let null_buff = data.nulls();
        Ok(ListArrayContainer {
            inner_data,
            offsets,
            null_buff,
            current_pos: 0,
        })
    }
}

impl<'a> ContainerIter for ListArrayContainer<'a> {
    fn next_item(&mut self) -> Option<Value> {
        let vals = if let Some(nb) = self.null_buff {
            if nb.is_null(self.current_pos) {
                Value::Null
            } else {
                let num_vals = self.offsets[self.current_pos + 1] - self.offsets[self.current_pos];
                let vals = self.inner_data.get_next_chunk(num_vals);
                Value::Array(vals)
            }
        } else {
            let num_vals = self.offsets[self.current_pos + 1] - self.offsets[self.current_pos];
            let vals = self.inner_data.get_next_chunk(num_vals);
            Value::Array(vals)
        };
        self.current_pos += 1;
        Some(vals)
    }

    fn next_chunk(&mut self, num_items: i32) -> Vec<Value> {
        todo!()
    }
}

#[derive(Debug)]
struct NullInfo {
    null_idx: usize,
    non_null_idx: usize,
}

impl NullInfo {
    fn try_new(schema: &Schema) -> Result<Self> {
        let is_null_loc = is_simple_null_union_type(schema);
        match is_null_loc {
            Some(null_idx) => {
                let non_null_idx = if null_idx == 0 { 1usize } else { 0 };
                Ok(NullInfo {
                    null_idx,
                    non_null_idx,
                })
            }
            None => Err(anyhow!("Column is not nullable")),
        }
    }
}

#[derive(Debug)]
struct UnionArrayContainer<'a> {
    variants: Vec<ArrayContainers<'a>>,
    null_info: Option<NullInfo>,
    type_ids: Option<&'a ScalarBuffer<i8>>,
}

impl<'a> UnionArrayContainer<'a> {
    fn try_new(data: &'a ArrayRef, schema: &Schema) -> Result<Self> {
        let null_info = NullInfo::try_new(schema).ok();
        // null info only matches on simple null types which would be represented as a null buffer in arrow instead of full UnionArray
        // need to add check to verify that type_ids match between arrow and avro schemas
        let variants = if let Some(nulls) = &null_info {
            // if type with null and one col, want to have a single array accessor in variants
            match schema {
                Schema::Union(us) => {
                    let vars = us.variants();
                    let inner_schema = &vars[nulls.non_null_idx];
                    (None, vec![ArrayContainers::try_new(data, inner_schema)?])
                }
                _ => panic!("Expected Schema::Union"),
            }
        } else {
            match schema {
                Schema::Union(us) => {
                    let vars = us.variants();
                    let mut containers = Vec::with_capacity(vars.len());
                    let union_arr = data.as_any().downcast_ref::<UnionArray>().unwrap();
                    let type_ids = union_arr.type_ids();
                    let _ = us
                        .variants()
                        .iter()
                        .zip(union_arr.type_ids().iter())
                        .for_each(|(s, i)| {
                            containers
                                .push(ArrayContainers::try_new(union_arr.child(*i), s).unwrap())
                        });
                    (Some(type_ids), containers)
                }
                _ => panic!("Expected Schema::Union"),
            }
        };
        Ok(UnionArrayContainer {
            variants: variants.1,
            null_info,
            type_ids: variants.0,
        })
    }
}

impl<'a> ContainerIter for UnionArrayContainer<'a> {
    fn next_item(&mut self) -> Option<Value> {
        if let Some(nulls) = &self.null_info {
            let val = self.variants[0].get_next();
            let v = match &val {
                Value::Null => Value::Union(nulls.null_idx as u32, Box::new(Value::Null)),
                _ => Value::Union(nulls.non_null_idx as u32, Box::new(val)),
            };
            Some(v)
        } else {
            let v = self
                .variants
                .iter_mut()
                .zip(self.type_ids.unwrap().iter())
                .map(|(x, y)| (x.get_next(), *y))
                .filter(|(x, _)| x != &Value::Null)
                .map(|(x, y)| Value::Union(y as u32, Box::new(x)))
                .collect::<Vec<_>>();
            if v.len() > 1 {
                panic!("Sparse union types not supported")
            } else if v.len() == 0 {
                Some(Value::Null)
            } else {
                Some(v[0].clone())
            }
        }
    }

    fn next_chunk(&mut self, num_items: i32) -> Vec<Value> {
        if let Some(nulls) = &self.null_info {
            let val = self.variants[0].get_next_chunk(num_items);
            let v = val
                .into_iter()
                .map(|x| match x {
                    Value::Null => Value::Union(nulls.null_idx as u32, Box::new(Value::Null)),
                    _ => Value::Union(nulls.non_null_idx as u32, Box::new(x)),
                })
                .collect::<Vec<_>>();
            v
        } else {
            // implementation for actual union goes here
            unimplemented!()
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialize::parse_schema;
    use apache_avro::schema::UnionSchema;
    use apache_avro::Schema;
    use arrow::array::{
        Int32Array, Int32Builder, ListBuilder, RecordBatch, StringArray, StructArray,
        TimestampMillisecondArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use std::sync::Arc;

#[test]
fn test_create_union_multiple_types() {
    let arr1 = Int32Array::from(vec![Some(1), None, None]);
    let arr2 = StringArray::from(vec![None, None, Some("def")]);
    let arr3 = BooleanArray::from(vec![None, Some(true), None]);

    let schema = Schema::Union(
        UnionSchema::new(vec![Schema::Int, Schema::String, Schema::Boolean]).unwrap(),
    );
    let fields = UnionFields::new(
        vec![0, 1, 2],
        vec![
            Field::new("int_field", DataType::Int32, true),
            Field::new("strfield", DataType::Utf8, true),
            Field::new("bool_field", DataType::Boolean, true),
        ],
    );
    let children: Vec<ArrayRef> = vec![
        Arc::new(arr1) as ArrayRef,
        Arc::new(arr2) as ArrayRef,
        Arc::new(arr3) as ArrayRef,
    ];
    let union_arr: ArrayRef =
        Arc::new(UnionArray::try_new(fields, vec![0, 1, 2].into(), None, children).unwrap());
    let mut union_container = UnionArrayContainer::try_new(&union_arr, &schema).unwrap();

    let expected_values = vec![
        Value::Union(0, Box::new(Value::Int(1))),
        Value::Union(2, Box::new(Value::Boolean(true))),
        Value::Union(1, Box::new(Value::String("def".into()))),
    ];

    for (i, expected) in expected_values.iter().enumerate() {
        let actual = union_container.next_item().unwrap();
        assert_eq!(*expected, actual, "Mismatch at index {}", i);
    }
}

#[test]
fn test_struct_next_chunk() {
    let inside_arr = Arc::new(Int32Array::from(vec![13, 2]));
    let inside_str = Arc::new(StringArray::from(vec!["abc", "xyz"]));
    let inner_f1 = Field::new("int_field", DataType::Int32, false);
    let inner_f2 = Field::new("strfield", DataType::Utf8, false);
    let fields = Fields::from(vec![inner_f1, inner_f2]);
    let null_buff = NullBuffer::from(vec![true, true]);
    let inner_struct_arr: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![inside_arr, inside_str],
        Some(null_buff),
    ));

    let schema = Schema::Record(RecordSchema {
        name: Name {
            name: "struct_name".to_string(),
            namespace: None,
        },
        aliases: None,
        doc: None,
        fields: vec![
            RecordField {
                name: "int_field".to_string(),
                doc: None,
                aliases: None,
                default: None,
                schema: Schema::Int,
                order: RecordFieldOrder::Ascending,
                position: 0,
                custom_attributes: Default::default(),
            },
            RecordField {
                name: "strfield".to_string(),
                doc: None,
                aliases: None,
                default: None,
                schema: Schema::String,
                order: RecordFieldOrder::Ascending,
                position: 0,
                custom_attributes: Default::default(),
            },
        ],
        lookup: Default::default(),
        attributes: Default::default(),
    });
    // let pa1 = ArrayContainers::IntContainer(PrimArrayContainer::try_new(&arr1, &schema1).unwrap());
    // let pa2 = ArrayContainers::StringContainer(PrimArrayContainer::try_new(&arr2, &schema2).unwrap());
    let mut sa = ArrayContainers::try_new(&inner_struct_arr, &schema).unwrap();
    // println!("{:?}", sa);
    let z = sa.get_next();
    println!("{:?}", z);
    let z2 = sa.get_next();
    println!("{:?}", z2);
}
#[test]
fn test_next_chunk() {
    let arr = Float32Array::from(vec![1.23, 2.45, 3.45]);
    let schema = Schema::Float;
    let mut pa = PrimArrayContainer::try_new(&arr, &schema).unwrap();
    let result = pa.next_chunk(2);
    assert_eq!(vec![Value::Float(1.23), Value::Float(2.45)], result);
}
#[test]
fn test_prim_arr() {
    let arr = Int32Array::from(vec![1, 2, 3]);
    let mut pa = PrimArrayContainer::try_new(&arr, &Schema::Int).unwrap();
    let r1 = pa.next_item();
    let r2 = pa.next_item();
    assert_eq!(Some(Value::Int(2)), r2);
}

#[test]
fn test_primarr_null() {
    let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("abc"), None, Some("def")]));
    let schema = Schema::Union(UnionSchema::new(vec![Schema::Null, Schema::String]).unwrap());
    let mut pa = ArrayContainers::try_new(&arr, &schema).unwrap();
    let r1 = pa.get_next();
    assert_eq!(Value::Union(1, Box::new(Value::String("abc".into()))), r1);
    let r2 = pa.get_next();
    assert_eq!(Value::Union(0, Box::new(Value::Null)), r2);
}
    #[test]
    fn test_convert_to_avro() {
        let int_arr1 = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let int_arr2 = Arc::new(Int32Array::from(vec![Some(1), None, None, Some(2)]));
        let string_arr1 = Arc::new(StringArray::from(vec![
            "one".to_string(),
            "two".into(),
            "three".into(),
            "four".into(),
        ]));
        let string_arr2 = Arc::new(StringArray::from(vec![
            Some("one".to_string()),
            None,
            Some("three".into()),
            None,
        ]));
        let mut list_builder = ListBuilder::new(Int32Builder::new());
        list_builder.append_value(vec![Some(1), Some(2), Some(3)]);
        list_builder.append_value(vec![Some(2), Some(3)]);
        list_builder.append_value(vec![]);
        list_builder.append_value(vec![None, Some(2), Some(3)]);
        let list_arr = Arc::new(list_builder.finish());
        let inner_field_ref = Arc::new(Field::new("item", DataType::Int32, true));

        let mut list_builder2 = ListBuilder::new(Int32Builder::new());
        list_builder2.append_value(vec![Some(1), Some(2), Some(3)]);
        list_builder2.append(false);
        list_builder2.append_value(vec![]);
        list_builder2.append_value(vec![None, Some(2), Some(3)]);
        // list_builder2.append_value(vec![Some(5), Some(2), Some(3)]);
        let list_arr2 = Arc::new(list_builder2.finish());
        let inner_field_ref2 = Arc::new(Field::new("item", DataType::Int32, true));

        // timestamps
        let timestamp_millis_arr = Arc::new(TimestampMillisecondArray::from(vec![1i64, 2, 3, 4]));

        let fields = Fields::from(vec![
            Field::new("int_arr_1", DataType::Int32, false),
            Field::new("int_arr_2", DataType::Int32, true),
            Field::new("str_arr_1", DataType::Utf8, false),
            Field::new("str_arr_2", DataType::Utf8, true),
            Field::new("list_arr", DataType::List(inner_field_ref), false),
            Field::new("list_arr2", DataType::List(inner_field_ref2), true),
            Field::new(
                "timestamp_arr",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]);
        let struct_arr = StructArray::new(
            fields.clone(),
            vec![
                int_arr1,
                int_arr2,
                string_arr1,
                string_arr2,
                list_arr,
                list_arr2,
                timestamp_millis_arr,
            ],
            None,
        );
        // let field = Field::new("struct_array", DataType::Struct(fields.clone()), false);
        let avro_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "int_arr_1", "type": "int"},
                    {"name": "int_arr_2", "type": ["null","int"]},
                    {"name": "str_arr_1", "type": "string"},
                    {"name": "str_arr_2", "type": ["null", "string"]},
                    {"name": "list_arr", "type": "array", "items": ["null", "int"]},
                    {"name": "list_arr2", "type": ["null", {"type": "array", "items": ["null", "int"]}]},
                    {"name": "timestamp_arr", "type": {
                        "type": "long",
                        "logicalType": "timestamp-millis"
                      }
                      }
                ]
            }
        "#;
        let schema = Schema::parse_str(avro_schema).unwrap();
        // let struct_arr_ref: ArrayRef = Arc::new(struct_arr);
        let struct_arr_ref: RecordBatch = struct_arr.into();
        // let mut ac = ArrayContainers::try_new(&struct_arr_ref, &schema).unwrap();
        // let r = (0..4).map(|x| {
        //     let val = ac.get_next();
        //     to_avro_datum(&schema, val).unwrap()
        // }).collect::<Vec<_>>();
        //         let r = t1
        //             .into_iter()
        //             .map(|x| apache_avro::to_avro_datum(&schema, x).unwrap())
        //             .collect::<Vec<_>>();
        let r = serialize_record_batch(struct_arr_ref.clone(), &schema);
        let ra = r
            .iter()
            .map(|x| x.iter().map(|j| j.unwrap()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        //
        let deserialize = crate::deserialize::per_datum_deserialize(&ra[0], &schema);
        let rb = struct_arr_ref;
        // .as_any()
        // .downcast_ref::<StructArray>()
        // .unwrap();
        assert_eq!(deserialize, rb);
    }
    //
    #[test]
    fn test_nested_nullable_record() {
        let schmea = r#"
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "maybe_nested_rec",
                            "type": ["null", {
                                "name": "nested_rec",
                                "type": "record",
                                "fields": [
                                    {"name": "int_field", "type": "int"},
                                    {"name": "strfield", "type": ["null", "string"]}
                                ]
                                }
                            ]
                        },
                        {"name": "outer_int", "type": "int"}
                    ]
                }
            "#;
        let inside_arr = Arc::new(Int32Array::from(vec![Some(13), Some(2)]));
        let inside_str = Arc::new(StringArray::from(vec![Some("abc"), None]));
        let inner_f1 = Field::new("int_field", DataType::Int32, true);
        let inner_f2 = Field::new("strfield", DataType::Utf8, true);
        let fields = Fields::from(vec![inner_f1, inner_f2]);
        let null_buff = NullBuffer::from(vec![true, false]);
        let inner_struct_arr = StructArray::new(
            fields.clone(),
            vec![inside_arr, inside_str],
            Some(null_buff),
        );

        let outer_int = Arc::new(Int32Array::from(vec![1, 2]));
        let outer_f1 = Field::new("outer_int", DataType::Int32, false);
        let inner_struct_field = Field::new("maybe_nested_rec", DataType::Struct(fields), true);
        let outer_fields = Fields::from(vec![inner_struct_field, outer_f1]);
        let outer_struct_arr = StructArray::new(
            outer_fields.clone(),
            vec![Arc::new(inner_struct_arr), outer_int],
            None,
        );
        let outer_struct_field = Field::new("test", DataType::Struct(outer_fields), false);
        let arr: RecordBatch = outer_struct_arr.into();
        let parsed_schmea = Schema::parse_str(schmea).unwrap();
        let arr_cont = serialize_record_batch(arr, &parsed_schmea);
        println!("{:?}", arr_cont);
        // let mut arr_cont = ArrayContainers::try_new(&arr, &parsed_schmea).unwrap();
        // let r = (0..2).map(|_| {
        //     let val = arr_cont.get_next();
        //     to_avro_datum(&parsed_schmea, val).unwrap()
        // }).collect::<Vec<_>>();
        // let t1 = from_avro_schema(Arc::new(outer_struct_arr), &parsed_schmea, None);
        // let r = t1
        //     .into_iter()
        //     .map(|x| apache_avro::to_avro_datum(&parsed_schmea, x).unwrap())
        //     .collect::<Vec<_>>();
        // let ra = r.iter().map(|x| &x[..]).collect::<Vec<_>>();
        // let deserialize = crate::deserialize::per_datum_deserialize(&ra, &parsed_schmea);
        // println!("{:?}", deserialize);
        // let rb = arr
        //     .as_any()
        //     .downcast_ref::<StructArray>()
        //     .unwrap();
        // println!("{:?}", deserialize);
        // assert_eq!(deserialize, rb.into());
    }

    #[test]
    fn test_timestamp_type() {
        let schmea = r#"
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {
                            "name": "timestmap",
                            "type": ["null", {
                                "name": "nested_rec",
                                "type": "record",
                                "fields": [
                                    {"name": "int_field", "type": "int"},
                                    {"name": "strfield", "type": ["null", "string"]}
                                ]
                                }
                            ]
                        },
                        {"name": "outer_int", "type": "int"}
                    ]
                }
            "#;
        let inside_arr = Arc::new(Int32Array::from(vec![Some(13), Some(2)]));
        let inside_str = Arc::new(StringArray::from(vec![Some("abc"), None]));
        let inner_f1 = Field::new("int_field", DataType::Int32, true);
        let inner_f2 = Field::new("strfield", DataType::Utf8, true);
        let fields = Fields::from(vec![inner_f1, inner_f2]);
        let null_buff = NullBuffer::from(vec![true, false]);
        let inner_struct_arr = StructArray::new(
            fields.clone(),
            vec![inside_arr, inside_str],
            Some(null_buff),
        );

        let outer_int = Arc::new(Int32Array::from(vec![1, 2]));
        let outer_f1 = Field::new("outer_int", DataType::Int32, false);
        let inner_struct_field = Field::new("maybe_nested_rec", DataType::Struct(fields), true);
        let outer_fields = Fields::from(vec![inner_struct_field, outer_f1]);
        let outer_struct_arr = StructArray::new(
            outer_fields.clone(),
            vec![Arc::new(inner_struct_arr), outer_int],
            None,
        );
        let outer_struct_field = Field::new("test", DataType::Struct(outer_fields), false);
        let arr: ArrayRef = Arc::new(outer_struct_arr);
        let parsed_schmea = Schema::parse_str(schmea).unwrap();
    }

    #[test]
    fn test_whats_failing() {
        let less_schema = r#"{
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
    }
  ]
}"#;
        let schema = parse_schema(less_schema);
    }
    //
    //     #[test]
    //     fn test_is_null_type() {
    //         let union_schema = UnionSchema::new(vec![Schema::Null, Schema::Long]);
    //         let simple_null_schema = Schema::Union(union_schema.unwrap());
    //         let got = is_null_union_type(&simple_null_schema);
    //         assert_eq!(got, Some(0usize));
    //         let avro_schema = r#"
    //             {
    //                 "type": "record",
    //                 "name": "test",
    //                 "namespace": "mynamespace",
    //                 "fields": [
    //                     {"name": "int_arr_1", "type": "int"},
    //                     {
    //                         "name": "array_list",
    //                         "type": "array",
    //                         "items": {
    //                             "type": "record",
    //                             "name": "inner_rec",
    //                             "fields": [
    //                                 {"name": "int_arr2", "type": "int"}
    //                             ]
    //                     }
    //                 }
    //                 ]
    //             }
    //         "#;
    //         let schema = Schema::parse_str(avro_schema).unwrap();
    //         let rfs = if let Schema::Record(rs) = &schema {
    //             let i1 = &rs.fields[1];
    //             if let Schema::Array(s) = &i1.schema {
    //                 println!("ok    {:?}", s);
    //             }
    //         };
    //
    //         // println!("{:?}", schema);
    //     }
}
