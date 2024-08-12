use anyhow::anyhow;
use apache_avro::types::Value;
use apache_avro::{to_avro_datum, Schema};
use arrow::array::{
    Array, ArrayAccessor, ArrayIter, ArrayRef, AsArray, BooleanArray, Float32Array,
    GenericBinaryArray, GenericBinaryBuilder, GenericListArray, GenericStringArray, MapArray,
    PrimitiveArray, StructArray, UnionArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type, TimestampMillisecondType};
use std::collections::HashMap;

pub fn serialize(schema: &Schema, struct_arry: &ArrayRef) -> GenericBinaryArray<i32> {
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
fn is_simple_null_union_type(avro_schema: &Schema) -> Option<(Option<usize>, bool)> {
    match avro_schema {
        Schema::Union(us) => {
            let null_idx = us.variants().iter().position(|x| matches!(x, Schema::Null));
            if !null_idx.is_some() {
                return None;
            }

            let is_simple_null = null_idx.is_some() && us.variants().len() == 2;
            Some((null_idx, is_simple_null))
        }
        _ => None,
    }
}

trait ContainerIter {
    fn next_item(&mut self) -> Option<Value>;
    fn next_chunk(&mut self, num_items: i32) -> Vec<Value> {
        (0..num_items).map(|_| self.next_item().unwrap()).collect()
    }
}

#[derive(Debug)]
enum ArrayContainers<'a> {
    BoolContainer(PrimArrayContainer<&'a BooleanArray>),
    FloatContainer(PrimArrayContainer<&'a Float32Array>),
    IntContainer(PrimArrayContainer<&'a PrimitiveArray<Int32Type>>),
    ListContainer(Box<ListArrayContainer<'a>>),
    RecordContainer(Box<StructArrayContainer<'a>>),
    StringContainer(PrimArrayContainer<&'a GenericStringArray<i32>>),
    TimestampMillisContainer(PrimArrayContainer<&'a PrimitiveArray<TimestampMillisecondType>>),
    UnionContainer(Box<UnionArrayContainer<'a>>),
    MapContainer(Box<MapArrayContainer<'a>>),
    NullContainer(Box<NullArrayContainer>),
    EnumContainer(Box<EnumArrayContainer<'a>>),
    DoubleContainer(PrimArrayContainer<&'a PrimitiveArray<Float64Type>>),
    LongContainer(PrimArrayContainer<&'a PrimitiveArray<Int64Type>>),
}

impl<'a> ArrayContainers<'a> {
    pub(crate) fn try_new(data: &'a ArrayRef, schema: &Schema) -> anyhow::Result<Self> {
        match schema {
            Schema::Int => {
                let inner_arr = data.as_primitive::<Int32Type>();
                Ok(ArrayContainers::IntContainer(PrimArrayContainer::try_new(
                    inner_arr, schema,
                )?))
            }
            Schema::Long => {
                let inner_arr = data.as_primitive::<Int64Type>();
                Ok(ArrayContainers::LongContainer(PrimArrayContainer::try_new(
                    inner_arr, schema,
                )?))
            }
            Schema::Null => Ok(ArrayContainers::NullContainer(Box::new(
                NullArrayContainer::try_new()?,
            ))),
            Schema::Float => {
                let inner_arr = data.as_primitive::<Float32Type>();
                Ok(ArrayContainers::FloatContainer(
                    PrimArrayContainer::try_new(inner_arr, schema)?,
                ))
            }
            Schema::Double => {
                let inner_arr = data.as_primitive::<Float64Type>();
                Ok(ArrayContainers::DoubleContainer(
                    PrimArrayContainer::try_new(inner_arr, schema)?,
                ))
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
            Schema::Enum(_) => {
                let inner_arr = data;
                Ok(ArrayContainers::EnumContainer(Box::new(
                    EnumArrayContainer::try_new(inner_arr, schema)?,
                )))
            }
            Schema::Record(_) => {
                let inner_arr = data.as_struct();
                Ok(ArrayContainers::RecordContainer(Box::new(
                    StructArrayContainer::try_new(inner_arr, schema)?,
                )))
            }
            Schema::Array(_) => {
                let inner_arr = data.as_list::<i32>();
                Ok(ArrayContainers::ListContainer(Box::new(
                    ListArrayContainer::try_new(inner_arr, schema)?,
                )))
            }
            Schema::Union(_) => Ok(ArrayContainers::UnionContainer(Box::new(
                UnionArrayContainer::try_new(data, schema)?,
            ))),
            Schema::Map(_) => Ok(ArrayContainers::MapContainer(Box::new(
                MapArrayContainer::try_new(
                    data.as_any().downcast_ref::<MapArray>().unwrap(),
                    schema,
                )?,
            ))),
            _ => unimplemented!(),
        }
    }

    fn get_next(&mut self) -> Value {
        match self {
            ArrayContainers::IntContainer(ref mut i) => i.next_item().unwrap(),
            ArrayContainers::NullContainer(ref mut i) => i.next_item().unwrap(),
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
            ArrayContainers::FloatContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::MapContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::EnumContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::DoubleContainer(inner) => inner.next_item().unwrap(),
            ArrayContainers::LongContainer(inner) => inner.next_item().unwrap(),
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
            ArrayContainers::FloatContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::MapContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::NullContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::EnumContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::DoubleContainer(inner) => inner.next_chunk(num_items),
            ArrayContainers::LongContainer(inner) => inner.next_chunk(num_items),
        }
    }
}

#[derive(Debug)]
pub struct PrimArrayContainer<A: ArrayAccessor>
where
    A::Item: Into<Value>,
{
    arr_iter: ArrayIter<A>,
}

impl<A: ArrayAccessor> PrimArrayContainer<A>
where
    A::Item: Into<Value>,
{
    fn try_new(arr: A, _schema: &Schema) -> anyhow::Result<Self> {
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
}

#[derive(Debug)]
struct NullArrayContainer;
impl NullArrayContainer {
    fn try_new() -> anyhow::Result<Self> {
        Ok(NullArrayContainer)
    }
}
impl ContainerIter for NullArrayContainer {
    fn next_item(&mut self) -> Option<Value> {
        Some(Value::Null)
    }
}

#[derive(Debug)]
struct StructArrayContainer<'a> {
    data: Vec<ArrayContainers<'a>>,
    column_names: Vec<String>,
    null_buff: Option<&'a NullBuffer>,
    current_pos: usize,
}

impl<'a> StructArrayContainer<'a> {
    fn try_new(data: &'a StructArray, schema: &Schema) -> anyhow::Result<Self> {
        let mut column_names = vec![];
        let inner_arrays: anyhow::Result<Vec<ArrayContainers>> = if let Schema::Record(rs) = schema
        {
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
                for i in self.data.iter_mut() {
                    i.get_next();
                }
                self.current_pos += 1;
                Some(Value::Null)
            } else {
                let mut struct_values = vec![];
                for (i, j) in self.data.iter_mut().zip(self.column_names.iter()) {
                    let next_val = i.get_next();
                    struct_values.push((j.clone(), next_val));
                }
                self.current_pos += 1;
                Some(Value::Record(struct_values))
            }
        } else {
            let mut struct_values = vec![];
            for (i, j) in self.data.iter_mut().zip(self.column_names.iter()) {
                let next_val = i.get_next();
                struct_values.push((j.clone(), next_val));
            }
            Some(Value::Record(struct_values))
        }
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
    fn try_new(data: &'a GenericListArray<i32>, schema: &Schema) -> anyhow::Result<Self> {
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
}

#[derive(Debug)]
struct NullInfo {
    null_idx: usize,
    non_null_idx: usize,
    is_simple_null: bool,
}

impl NullInfo {
    fn try_new(schema: &Schema) -> anyhow::Result<Self> {
        let is_null_loc = is_simple_null_union_type(schema);
        match is_null_loc {
            Some((null_idx, is_simple_null)) => {
                if is_simple_null {
                    let non_null_idx = if null_idx == Some(0) { 1usize } else { 0 };
                    Ok(NullInfo {
                        null_idx: null_idx.unwrap(),
                        non_null_idx,
                        is_simple_null,
                    })
                } else {
                    Ok(NullInfo {
                        null_idx: null_idx.unwrap(),
                        non_null_idx: 0,
                        is_simple_null,
                    })
                }
            }
            None => Err(anyhow!("Column is not nullable")),
        }
    }
    fn check_simple_null(&self) -> bool {
        self.is_simple_null
    }
}

#[derive(Debug)]
pub struct UnionArrayContainer<'a> {
    variants: Vec<ArrayContainers<'a>>,
    null_info: Option<NullInfo>,
    type_ids: Option<&'a ScalarBuffer<i8>>,
    current_pos: usize,
}

impl<'a> UnionArrayContainer<'a> {
    fn try_new(data: &'a ArrayRef, schema: &Schema) -> anyhow::Result<Self> {
        let null_info = NullInfo::try_new(schema).ok();
        // null info only matches on simple null types which would be represented as a null buffer in arrow instead of full UnionArray
        // need to add check to verify that type_ids match between arrow and avro schemas

        match &null_info {
            Some(nulls) => {
                if nulls.is_simple_null {
                    // if type with null and one col, want to have a single array accessor in variants
                    let inner_schema = match schema {
                        Schema::Union(us) => {
                            let vars = us.variants();
                            &vars[nulls.non_null_idx]
                        }
                        _ => panic!("Expected Schema::Union"),
                    };
                    let inner_data = ArrayContainers::try_new(data, inner_schema)?;
                    Ok(UnionArrayContainer {
                        variants: vec![inner_data],
                        null_info,
                        type_ids: None,
                        current_pos: 0,
                    })
                } else {
                    match schema {
                        Schema::Union(us) => {
                            let vars = us.variants();
                            let mut containers = Vec::with_capacity(vars.len());
                            let union_arr = data.as_any().downcast_ref::<UnionArray>().unwrap();
                            let type_ids = union_arr.type_ids();
                            let _ = us.variants().iter().zip(0..vars.len()).for_each(|(s, i)| {
                                containers.push(
                                    ArrayContainers::try_new(union_arr.child(i as i8), s).unwrap(),
                                )
                            });
                            Ok(UnionArrayContainer {
                                variants: containers,
                                null_info,
                                type_ids: Some(type_ids),
                                current_pos: 0,
                            })
                        }
                        _ => panic!("Expected Schema::Union"),
                    }
                }
            }

            None => match schema {
                Schema::Union(us) => {
                    let vars = us.variants();
                    let mut containers = Vec::with_capacity(vars.len());
                    let union_arr = data.as_any().downcast_ref::<UnionArray>().unwrap();
                    let type_ids = union_arr.type_ids();
                    let _ = us.variants().iter().zip(0..vars.len()).for_each(|(s, i)| {
                        containers
                            .push(ArrayContainers::try_new(union_arr.child(i as i8), s).unwrap())
                    });
                    Ok(UnionArrayContainer {
                        variants: containers,
                        null_info,
                        type_ids: Some(type_ids),
                        current_pos: 0,
                    })
                }
                _ => panic!("Expected Schema::Union"),
            },
        }
    }

    fn is_simple_null(&self) -> bool {
        if self.null_info.is_none() {
            false
        } else {
            self.null_info.as_ref().unwrap().check_simple_null()
        }
    }
}

impl<'a> ContainerIter for UnionArrayContainer<'a> {
    fn next_item(&mut self) -> Option<Value> {
        if self.is_simple_null() {
            if let Some(nulls) = &self.null_info {
                //     let val = self.variants[0].get_next();
                let val = self.variants[0].get_next();
                let v = match &val {
                    Value::Null => Value::Union(nulls.null_idx as u32, Box::new(Value::Null)),
                    _ => Value::Union(nulls.non_null_idx as u32, Box::new(val)),
                };
                Some(v)
            } else {
                unreachable!()
            }
        } else {
            let current_idx = self.type_ids.unwrap()[self.current_pos];
            let val = self.variants[current_idx as usize].get_next();
            let _ = self.variants.iter_mut().enumerate().for_each(|(idx, arr)| {
                if idx != current_idx as usize {
                    arr.get_next();
                }
            });
            self.current_pos += 1;
            Some(Value::Union(current_idx as u32, Box::new(val)))
        }
    }
}

#[derive(Debug)]
pub struct MapArrayContainer<'a> {
    key_data: ArrayContainers<'a>,
    value_data: ArrayContainers<'a>,
    offsets: &'a OffsetBuffer<i32>,
    null_buff: Option<&'a NullBuffer>,
    current_pos: usize,
}

impl<'a> MapArrayContainer<'a> {
    fn try_new(data: &'a MapArray, schema: &Schema) -> anyhow::Result<Self> {
        let offsets = data.offsets();
        let keys = data.keys();
        let inner = data.values();
        let inner_schema = if let Schema::Map(s) = schema {
            s
        } else {
            panic!("{}", format!("Expected map schema got {:?}", schema))
        };

        let value_data = ArrayContainers::try_new(inner, inner_schema)?;
        let key_data =
            ArrayContainers::try_new(keys, &Schema::String).expect("Map keys must be strings");
        let null_buff = data.nulls();
        Ok(MapArrayContainer {
            key_data,
            value_data,
            offsets,
            null_buff,
            current_pos: 0,
        })
    }
    fn get_value(&mut self) -> Value {
        let num_vals = self.offsets[self.current_pos + 1] - self.offsets[self.current_pos];
        let keys = self.key_data.get_next_chunk(num_vals);
        let values = self.value_data.get_next_chunk(num_vals);
        Value::Map(
            keys.into_iter()
                .zip(values.into_iter())
                .map(|(k, v)| {
                    if let Value::String(s) = k {
                        (s, v)
                    } else {
                        panic!("Map keys must be strings")
                    }
                })
                .collect::<HashMap<String, Value>>(),
        )
    }
}

impl ContainerIter for MapArrayContainer<'_> {
    fn next_item(&mut self) -> Option<Value> {
        let vals = if let Some(nb) = self.null_buff {
            if nb.is_null(self.current_pos) {
                Value::Null
            } else {
                self.get_value()
            }
        } else {
            self.get_value()
        };
        self.current_pos += 1;
        Some(vals)
    }
}

#[derive(Debug)]
struct EnumArrayContainer<'a> {
    inner_data: ArrayContainers<'a>,
    mapping: HashMap<String, i32>,
}

impl<'a> EnumArrayContainer<'a> {
    fn try_new(data: &'a ArrayRef, schema: &Schema) -> anyhow::Result<Self> {
        let mapping = if let Schema::Enum(es) = schema {
            let mut m: HashMap<String, i32> = HashMap::new();
            es.symbols.iter().enumerate().for_each(|(i, s)| {
                m.insert(s.clone(), i as i32);
            });
            m
        } else {
            panic!("Expected enum schema")
        };
        let modified_schema = Schema::String;
        let inner_data = ArrayContainers::try_new(data, &modified_schema)?;
        Ok(EnumArrayContainer {
            inner_data,
            mapping,
        })
    }
}

impl ContainerIter for EnumArrayContainer<'_> {
    fn next_item(&mut self) -> Option<Value> {
        let val = self.inner_data.get_next();
        match val {
            Value::String(s) => {
                let idx = self
                    .mapping
                    .get(&s)
                    .unwrap_or_else(|| panic!("Did not find string in mapping"));
                Some(Value::Enum(*idx as u32, s))
            }
            _ => panic!("Expected string value"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::schema::{
        EnumSchema, Name, RecordField, RecordFieldOrder, RecordSchema, UnionSchema,
    };
    use apache_avro::types::Value;
    use apache_avro::Schema;
    use arrow::array::{
        ArrayRef, BooleanArray, Int32Array, MapArray, NullArray, StringArray, StructArray,
        UnionArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields, UnionFields};
    use std::sync::Arc;
    #[test]
    fn test_enum_container() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let schema = Schema::Enum(EnumSchema {
            name: Name {
                name: "enum".to_owned(),
                namespace: None,
            },
            aliases: None,
            doc: None,
            symbols: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            default: None,
            attributes: Default::default(),
        });
        let mut enum_container = EnumArrayContainer::try_new(&arr, &schema).unwrap();
        let expected_values = vec![
            Value::Enum(0, "a".to_string()),
            Value::Enum(1, "b".to_string()),
            Value::Enum(2, "c".to_string()),
        ];
        for (i, expected) in expected_values.iter().enumerate() {
            let actual = enum_container.next_item().unwrap();
            assert_eq!(*expected, actual, "Mismatch at index {}", i);
        }
    }
    #[test]
    fn test_map_container() {
        // Setup the necessary data
        let source = vec!["a", "b", "c", "d"];
        let keys = source.iter().map(|x| *x);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let entry_offsets = [0, 2, 4];
        let map_array = MapArray::new_from_strings(keys, &values, &entry_offsets).unwrap();
        let schema = Schema::Map(Box::new(Schema::Int));

        // Call the try_new function
        let result = MapArrayContainer::try_new(&map_array, &schema);

        // Assert the results
        assert!(result.is_ok());
        let mut container = result.unwrap();

        let next_item = container.next_item().unwrap();
        let expected = Value::Map(
            vec![
                ("a".to_string(), Value::Int(1)),
                ("b".to_string(), Value::Int(2)),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(expected, next_item);

        let next_item = container.next_item().unwrap();
        let expected = Value::Map(
            vec![
                ("c".to_string(), Value::Int(3)),
                ("d".to_string(), Value::Int(4)),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(expected, next_item);
    }

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
            Arc::new(UnionArray::try_new(fields, vec![0, 2, 1].into(), None, children).unwrap());
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
    fn test_union_multiple_types_including_null() {
        let arr1 = Int32Array::from(vec![None, Some(1), None, None]);
        let arr2 = StringArray::from(vec![None, None, None, Some("def")]);
        let arr3 = BooleanArray::from(vec![None, None, Some(true), None]);

        let schema = Schema::Union(
            UnionSchema::new(vec![
                Schema::Null,
                Schema::Int,
                Schema::String,
                Schema::Boolean,
            ])
            .unwrap(),
        );
        let fields = UnionFields::new(
            vec![0, 1, 2, 3],
            vec![
                Field::new("null_field", DataType::Null, true),
                Field::new("int_field", DataType::Int32, true),
                Field::new("strfield", DataType::Utf8, true),
                Field::new("bool_field", DataType::Boolean, true),
            ],
        );
        let children: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(4)),
            Arc::new(arr1) as ArrayRef,
            Arc::new(arr2) as ArrayRef,
            Arc::new(arr3) as ArrayRef,
        ];
        let union_arr: ArrayRef =
            Arc::new(UnionArray::try_new(fields, vec![0, 1, 3, 2].into(), None, children).unwrap());
        let mut union_container = UnionArrayContainer::try_new(&union_arr, &schema).unwrap();

        let expected_values = vec![
            Value::Union(0, Box::new(Value::Null)),
            Value::Union(1, Box::new(Value::Int(1))),
            Value::Union(3, Box::new(Value::Boolean(true))),
            Value::Union(2, Box::new(Value::String("def".into()))),
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
        assert_eq!(Some(Value::Int(1)), r1);
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
    //
    #[test]
    fn test_null_info_simple_union() {
        let union_schema = Schema::Union(
            apache_avro::schema::UnionSchema::new(vec![Schema::Null, Schema::Int]).unwrap(),
        );
        let result = NullInfo::try_new(&union_schema).unwrap();
        assert_eq!(result.null_idx, 0);
        assert_eq!(result.non_null_idx, 1);
        assert_eq!(result.is_simple_null, true);
    }

    #[test]
    fn test_null_info_complex_union() {
        let union_schema = Schema::Union(
            apache_avro::schema::UnionSchema::new(vec![Schema::Null, Schema::Int, Schema::String])
                .unwrap(),
        );
        let result = NullInfo::try_new(&union_schema).unwrap();
        assert_eq!(result.null_idx, 0);
        assert_eq!(result.non_null_idx, 0);
        assert_eq!(result.is_simple_null, false);
    }
    #[test]
    fn test_null_info_no_nulls() {
        let union_schema = Schema::Union(
            apache_avro::schema::UnionSchema::new(vec![Schema::Int, Schema::String]).unwrap(),
        );
        let result = NullInfo::try_new(&union_schema);
        assert!(result.is_err());
    }
}
