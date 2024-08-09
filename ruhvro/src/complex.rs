use anyhow::{anyhow, Result};
use apache_avro::types::Value;
use arrow::array::{make_builder, ArrayBuilder, ArrayRef, AsArray, BooleanBufferBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, GenericListArray, Int32Builder, Int64Builder, MapArray, NullBuilder, StringBuilder, StructArray, TimestampMicrosecondBuilder, TimestampMillisecondBuilder, UnionArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit, UnionFields};
use std::sync::Arc;

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
enum AvroToArrowBuilder {
    Primitive(Box<dyn ArrayBuilder>),
    List(Box<ListContainer>),
    Struct(Box<StructContainer>),
    Union(Box<UnionContainer>),
    Map(Box<MapContainer>),
}

impl AvroToArrowBuilder {
    fn try_new(field: &FieldRef, capacity: usize) -> Result<Self> {
        match field.data_type() {
            DataType::List(_inner) => {
                let lc = ListContainer::try_new(field.clone(), capacity)?;
                Ok(Self::List(Box::new(lc)))
            }
            DataType::Struct(_flds) => {
                let sc = StructContainer::try_new(field.clone(), capacity)?;
                Ok(Self::Struct(Box::new(sc)))
            }
            DataType::Union(_union_fields, _union_type) => {
                let uc = UnionContainer::try_new(field.clone(), capacity)?;
                Ok(Self::Union(Box::new(uc)))
            }
            DataType::Map(_fr, _ordered) => {
                let mc = MapContainer::try_new(field.clone(), capacity)?;
                Ok(Self::Map(Box::new(mc)))
            }
            _ => Ok(Self::Primitive(make_builder(field.data_type(), capacity))),
        }
    }

    fn add_val(&mut self, avro_val: &Value, field: &FieldRef) -> Result<()> {
        match self {
            Self::Primitive(b) => {
                add_data_to_array_builder(avro_val, b, field);
            }
            Self::List(list_container) => {
                list_container.add_val(avro_val)?;
            }
            Self::Struct(struct_container) => {
                struct_container.add_val(avro_val)?;
            }
            Self::Union(union_container) => {
                union_container.add_val(avro_val)?;
            }
            Self::Map(map_builder) => {
                map_builder.add_val(avro_val)?;
            }
        }
        Ok(())
    }
    fn build(self) -> Result<ArrayRef> {
        match self {
            Self::Primitive(mut builder) => {
                let a = builder.finish();
                Ok(a)
            }
            Self::List(list_container) => list_container.build(),
            Self::Struct(sb) => sb.build(),
            Self::Union(ub) => ub.build(),
            Self::Map(mb) => mb.build(),
        }
    }
}

struct ListContainer {
    fields: FieldRef,
    inner_field: FieldRef,
    inner_builder: AvroToArrowBuilder,
    offsets: Vec<i32>,
    nulls: BooleanBufferBuilder,
}
impl ListContainer {
    fn try_new(field: FieldRef, capacity: usize) -> Result<Self> {
        let fields = field.clone();
        let inner_field = if let DataType::List(innerf) = field.data_type() {
            Ok(innerf)
        } else {
            Err(anyhow!(
                "could not extract inner builder from list {}",
                field.name()
            ))
        }?;
        let inner_builder = AvroToArrowBuilder::try_new(&inner_field, capacity)?;
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        let nulls = BooleanBufferBuilder::new(capacity);
        Ok(Self {
            fields,
            inner_field: inner_field.clone(),
            inner_builder,
            offsets,
            nulls,
        })
    }

    fn add_val(&mut self, avro_val: &Value) -> Result<()> {
        let av = get_val_from_possible_union(avro_val, &self.fields);

        match av {
            Value::Array(vals) => {
                // for each value in the array, add val to inner builder
                // add len of items to offset
                let last_offset = &self.offsets[self.offsets.len() - 1];
                let num_vals = vals.len() as i32;
                for val in vals {
                    self.inner_builder
                        .add_val(val, &self.inner_field)
                        .expect("Error adding value from avro array");
                }
                self.offsets.push(last_offset + num_vals);
                self.nulls.append(true);
            }
            Value::Null => {
                let last_offset = &self.offsets[self.offsets.len() - 1];
                self.offsets.push(*last_offset);
                self.nulls.append(false);
            }
            _ => unreachable!(),
        };

        Ok(())
    }

    fn build(mut self) -> Result<ArrayRef> {
        let inner_array = self.inner_builder.build()?;
        let sb = ScalarBuffer::from(self.offsets);
        let offsets = OffsetBuffer::new(sb);
        let nulls = NullBuffer::new(self.nulls.finish());
        let list_arr =
            GenericListArray::try_new(self.inner_field.clone(), offsets, inner_array, Some(nulls))?;
        Ok(Arc::new(list_arr))
    }
}
pub struct StructContainer {
    /// Contains the basic pieces of a arrow Struct Array Builder.
    fields: FieldRef,
    builders: Vec<(FieldRef, AvroToArrowBuilder)>,
    nulls: BooleanBufferBuilder,
}

impl StructContainer {
    /// takes field ref for a struct array
    /// let container_f = Arc::new(Field::new(
    /// "struct_f",
    /// DataType::Struct(Fields::from(vec![f1.clone(), f2.clone()])),
    /// false,
    /// ));
    pub fn try_new(field: FieldRef, capacity: usize) -> Result<Self> {
        let mut builders = vec![];
        let _create_builders = if let DataType::Struct(flds) = field.clone().data_type() {
            for f in flds.iter() {
                let b = AvroToArrowBuilder::try_new(f, capacity)?;
                builders.push((f.clone(), b));
            }
            Ok(())
        } else {
            Err(anyhow!("Could not build struct from {}", &field))
        }?;
        let nulls = BooleanBufferBuilder::new(capacity);
        Ok(StructContainer {
            fields: field,
            builders,
            nulls,
        })
    }

    pub fn try_new_from_fields(fields: Fields, capacity: usize) -> Result<Self> {
        let mut builders = vec![];
        for f in fields.iter() {
            let b = AvroToArrowBuilder::try_new(f, capacity)?;
            builders.push((f.clone(), b));
        }
        let nulls = BooleanBufferBuilder::new(capacity);
        Ok(Self {
            fields: Arc::new(Field::new("struct_field", DataType::Struct(fields), false)),
            builders,
            nulls,
        })
    }

    pub fn add_val(&mut self, avro_val: &Value) -> Result<()> {
        let av = get_val_from_possible_union(avro_val, &self.fields);
        match av {
            Value::Null => {
                for idx in 0..self.builders.len() {
                    let builder = &mut self.builders[idx];
                    let _ = builder.1.add_val(&Value::Null, &builder.0)?;
                }
                self.nulls.append(false);
            }
            Value::Record(inner_vals) => {
                for (idx, (_field_name, v)) in inner_vals.iter().enumerate() {
                    let builder = &mut self.builders[idx];
                    let _ = builder.1.add_val(v, &builder.0)?;
                }
                self.nulls.append(true);
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    pub fn build(mut self) -> Result<ArrayRef> {
        let mut fields = vec![];
        let a = self
            .builders
            .into_iter()
            .map(|(field, builder)| {
                fields.push(field);
                builder.build().unwrap_or_else(|e| panic!("{:?}", e))
            })
            .collect::<Vec<_>>();

        let nulls = NullBuffer::new(self.nulls.finish());

        let s_arr = Arc::new(StructArray::try_new(Fields::from(fields), a, Some(nulls))?);
        Ok(s_arr)
    }

    pub fn try_build_struct_array(self) -> Result<StructArray> {
        let built = self.build()?;
        let typed = built.as_struct().to_owned();
        Ok(typed)
    }
}
struct UnionContainer {
    type_ids: Vec<i8>,
    // type_id_vec contains the index for the array vec
    // value_offsets_buffer contains the offset for the buffer of that particular type
    // see https://docs.rs/arrow/latest/arrow/array/struct.UnionArray.html
    // builders: Vec<(FieldRef, AvroToArrowBuilder)>,
    builders: Vec<(FieldRef, AvroToArrowBuilder)>,
    // contains the current max offset for each type
    // position_mapping: HashMap<i8, i32>,
    union_fields: UnionFields,
}
impl UnionContainer {
    fn try_new(field: FieldRef, capacity: usize) -> Result<Self> {
        let mut builders = vec![];
        let type_ids = vec![];
        let mut unionf = UnionFields::empty();
        let _create_builders =
            if let DataType::Union(union_fields, _unionmode) = field.clone().data_type() {
                for (_, field_ref) in union_fields.iter() {
                    let builder = AvroToArrowBuilder::try_new(field_ref, capacity)?;
                    builders.push((field_ref.clone(), builder));
                }
                unionf = union_fields.clone();
                Ok(())
            } else {
                Err(anyhow!("error creating nested builders in Union"))
            }?;
        Ok(Self {
            type_ids,
            builders,
            union_fields: unionf,
        })
    }

    ///
    /// Adds value to Union. Union needs to track additional meta data
    fn add_val(&mut self, avro_val: &Value) -> Result<()> {
        if let Value::Union(field_idx, val) = avro_val {
            for (i, f) in self.union_fields.iter().enumerate() {
                let b = &mut self.builders[i].1;
                if &u32::try_from(f.0)? == field_idx {
                    b.add_val(val, f.1)?;
                    self.type_ids.push(f.0);
                } else {
                    b.add_val(&Value::Null, f.1)?;
                }
            }
        }
        Ok(())
    }
    fn build(self) -> Result<ArrayRef> {
        let children = self
            .builders
            .into_iter()
            .map(|(_field, arr)| {
                arr.build().unwrap_or_else(
                    |e| panic!("Error building union array {}", e), // )
                )
            })
            .collect::<Vec<_>>();
        let arr = UnionArray::try_new(self.union_fields, self.type_ids.into(), None, children)?;
        Ok(Arc::new(arr))
    }
}

struct MapContainer {
    fields: FieldRef,
    inner_list: ListContainer,
}

impl MapContainer {
    fn try_new(field: FieldRef, capacity: usize) -> Result<Self> {
        match field.data_type() {
            DataType::Map(fr, _ordered) => {
                let wrapped = Arc::new(Field::new(
                    "map_col",
                    DataType::List(fr.clone()),
                    field.is_nullable(),
                ));
                let il = ListContainer::try_new(wrapped, capacity)?;
                let mc = Self {
                    fields: field.clone(),
                    inner_list: il,
                };
                Ok(mc)
            }
            _ => Err(anyhow!("Failed to create MapContainer")),
        }
    }
    fn add_val(&mut self, avro_val: &Value) -> Result<()> {
        let av = get_val_from_possible_union(avro_val, &self.fields);
        match av {
            Value::Map(hm) => {
                let mut inside_vec = vec![];
                for (k, v) in hm.iter() {
                    let rewrapped = Value::Record(vec![
                        ("keys".to_string(), Value::String(k.to_owned())),
                        ("values".to_string(), v.to_owned()),
                    ]);
                    inside_vec.push(rewrapped)
                }
                let wrapped_list = Value::Array(inside_vec);
                self.inner_list.add_val(&wrapped_list)?;
            }
            Value::Null => self.inner_list.add_val(&Value::Null)?,
            _ => unreachable!(),
        }
        Ok(())
    }
    fn build(self) -> Result<ArrayRef> {
        let l_array = self.inner_list.build()?;
        let list_arr = l_array.as_list().to_owned();
        let (field_ref, offsets, data, null_buffer) = list_arr.into_parts();
        let m = MapArray::try_new(
            field_ref,
            offsets,
            data.as_struct().to_owned(),
            null_buffer,
            false,
        )?;
        Ok(Arc::new(m))
    }
}

fn add_data_to_array_builder(
    data: &Value,
    builder: &mut Box<dyn ArrayBuilder>,
    field: &Field,
) -> () {
    let data = get_val_from_possible_union(data, field);
    match field.data_type() {
        DataType::Boolean => {
            let target = get_typed_array::<BooleanBuilder>(builder);
            add_val!(data, target, field, Boolean);
        }
        DataType::Int32 => {
            let target = get_typed_array::<Int32Builder>(builder);
            add_val!(data, target, field, Int);
        }
        DataType::Int64 => {
            let target = get_typed_array::<Int64Builder>(builder);
            add_val!(data, target, field, Long);
        }
        DataType::Float32 => {
            let target = get_typed_array::<Float32Builder>(builder);
            add_val!(data, target, field, Float);
        }
        DataType::Float64 => {
            let target = get_typed_array::<Float64Builder>(builder);
            add_val!(data, target, field, Double)
        }
        DataType::Timestamp(a, _) => {
            let _ = match a {
                TimeUnit::Millisecond => {
                    let target = get_typed_array::<TimestampMillisecondBuilder>(builder);
                    add_val!(data, target, field, TimestampMillis);
                }
                TimeUnit::Microsecond => {
                    let target = get_typed_array::<TimestampMicrosecondBuilder>(builder);
                    add_val!(data, target, field, TimestampMicros);
                }
                _ => unimplemented!(),
            };
        }
        DataType::Date32 => {
            let target = get_typed_array::<Date32Builder>(builder);
            add_val!(data, target, field, Date);
        }
        DataType::Duration(_) => {
            unimplemented!()
        }
        DataType::Utf8 => {
            let ta = get_typed_array::<StringBuilder>(builder);
            if let Value::String(s) = data {
                ta.append_value(s);
            } else if let Value::Enum(_k, v) = data {
                ta.append_value(v);
            } else {
                ta.append_null();
            }
        }
        DataType::Null => {
            let ta = get_typed_array::<NullBuilder>(builder);
            ta.append_null();
        }
        _ => unimplemented!(),
    }
}

#[inline]
fn get_typed_array<T: ArrayBuilder>(arr: &mut Box<dyn ArrayBuilder>) -> &mut T {
    arr.as_any_mut()
        .downcast_mut::<T>()
        .expect("Did not find expected builder")
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value;
    use arrow::array::{Array, ArrayBuilder, BooleanBufferBuilder, Int32Array, Int32Builder, ListArray, NullArray, RecordBatch, StringArray, StringBuilder, StructArray, UnionArray};
    use arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_simple_builder() {
        let field = Arc::new(Field::new("int_field", DataType::Int32, false));
        let mut avro_arrow_builder = AvroToArrowBuilder::try_new(&field, 2).unwrap();
        let avro_val = Value::Int(2);
        let _ = avro_arrow_builder.add_val(&avro_val, &field);
        let avro_val2 = Value::Int(3);
        let _ = avro_arrow_builder.add_val(&avro_val2, &field);
        let result = avro_arrow_builder
            .build()
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .to_owned();

        let expected = Int32Array::from(vec![2, 3]);
        assert_eq!(result, expected);
    }
    #[test]
    fn test_build_list_array() {
        let field = Arc::new(Field::new("string_field", DataType::Utf8, true));
        let list_field = Arc::new(Field::new(
            "list_field",
            DataType::List(field.clone()),
            true,
        ));
        let inner_b = StringBuilder::new();
        let inner = AvroToArrowBuilder::Primitive(Box::new(inner_b));
        let mut lc = ListContainer {
            fields: list_field.clone(),
            inner_field: field.clone(),
            inner_builder: inner,
            offsets: vec![0],
            nulls: BooleanBufferBuilder::new(3),
        };
        let avro_data1 = Value::String("hello".into());
        let avro_data2 = Value::String("avro".into());
        let vec_avro = Value::Array(vec![avro_data1, avro_data2]);
        let _r1 = lc.add_val(&vec_avro);

        // add list with one null val
        let avro_data1 = Value::String("hello null".into());
        let avro_value_null = Value::Null;
        let vec_avro = Value::Array(vec![avro_data1, avro_value_null]);
        let _r2 = lc.add_val(&vec_avro);

        // add null val
        let null_val = Value::Null;
        let union_null = Value::Union(1, Box::new(null_val));
        let _r3 = lc.add_val(&union_null);
        let result_array = lc.build().unwrap();

        // println!("{:?}", result_array);
        assert_eq!(3, result_array.len());
        assert_eq!(1, result_array.null_count());

        assert_eq!(
            "hello",
            result_array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
        );
        assert_eq!(
            "hello null",
            result_array
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
        );
    }

    #[test]
    fn test_struct_array() {
        let f1 = Arc::new(Field::new("int_f", DataType::Int32, false));
        let f2 = Arc::new(Field::new("str_f", DataType::Utf8, false));
        let container_f = Arc::new(Field::new(
            "struct_f",
            DataType::Struct(Fields::from(vec![f1.clone(), f2.clone()])),
            false,
        ));
        let mut s = StructContainer::try_new(container_f, 2).unwrap();
        let av = Value::Record(vec![
            ("int_f".to_string(), Value::Int(1)),
            ("str_f".to_string(), Value::String("mystring".into())),
        ]);
        let _r = s.add_val(&av).unwrap();
        let v2 = Value::Record(vec![
            ("int_f".to_string(), Value::Int(2)),
            ("str_f".to_string(), Value::String("second".into())),
        ]);
        let _r = s.add_val(&v2);
        let result = s.build().unwrap();
        let a = result
            .clone()
            .as_struct()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(a, 1);
        let b = result
            .clone()
            .as_struct()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!(b, "mystring".to_string());
    }

    #[test]
    fn test_union_array() {
        let int_f = Arc::new(Field::new("int_field", DataType::Int32, false));
        let str_f = Arc::new(Field::new("str_field", DataType::Utf8, false));
        let union_fields = UnionFields::new([0, 1], [int_f.clone(), str_f.clone()]);
        let union_f = Field::new(
            "union_f",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        );

        let mut uc = UnionContainer::try_new(Arc::new(union_f), 2).unwrap();

        let av = Value::Union(0, Box::new(Value::Int(1)));
        let _r = uc.add_val(&av);
        let av = Value::Union(1, Box::new(Value::String("string time".into())));
        let _r = uc.add_val(&av);
        let av = Value::Union(0, Box::new(Value::Int(2)));
        let _r = uc.add_val(&av);
        let result = uc.build().unwrap();
        let got = result.as_any().downcast_ref::<UnionArray>().unwrap();
        let value = got
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(1, value);
        let value = got
            .value(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!("string time".to_string(), value);
    }
    #[test]
    fn test_union_array_with_many_vals() {
       let int_f = Arc::new(Field::new("int_field", DataType::Int32, false));
        let str_f = Arc::new(Field::new("str_field", DataType::Utf8, false));
        let null_f = Arc::new(Field::new("null_field", DataType::Null, false));
        let union_fields = UnionFields::new([0, 1, 2], [null_f, int_f.clone(), str_f.clone()]);
        let union_f = Field::new(
            "union_f",
            DataType::Union(union_fields, UnionMode::Sparse),
            false,
        );

        let mut uc = UnionContainer::try_new(Arc::new(union_f), 3).unwrap();

        let av = Value::Union(0, Box::new(Value::Null));
        let _r = uc.add_val(&av);
        let av = Value::Union(1, Box::new(Value::Int(1)));
        let _r = uc.add_val(&av);
        let av = Value::Union(2, Box::new(Value::String("string time".into())));
        let _r = uc.add_val(&av);

        let result = uc.build().unwrap();
        let got = result.as_any().downcast_ref::<UnionArray>().unwrap();
        let value = got
            .value(0)
            .as_any()
            .downcast_ref::<NullArray>()
            .unwrap()
            .is_null(0);
        // per the arrow docs:
        // "NullArrays do not have a null buffer, and therefore always
        // return false for is_null."
        let array = NullArray::new(1);
        assert_eq!(array.is_null(0), false);
        assert_eq!(false, value);

        let value = got
            .value(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(1, value);
        let value = got
            .value(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string();
        assert_eq!("string time".to_string(), value);

    }
    #[test]
    fn test_map_array() {
        // fix asserts. Map values can return different order so test will fail 50% of the time
        let k_field = Field::new("keys", DataType::Utf8, false);
        let v_field = Field::new("values", DataType::Int32, false);
        let struct_field = Arc::new(Field::new(
            "struct_f",
            DataType::Struct(Fields::from(vec![k_field, v_field])),
            false,
        ));
        let map_field = Field::new("map_f", DataType::Map(struct_field.clone(), false), false);
        let mut mc = MapContainer::try_new(Arc::new(map_field), 5).unwrap();

        let avro_val = Value::Map(HashMap::from_iter(vec![
            ("my_key".into(), Value::Int(1)),
            ("second".into(), Value::Int(3)),
        ]));
        let _r = mc.add_val(&avro_val);
        let result = mc.build().unwrap();
        let map_result = result.as_map();
        let key_result = map_result
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_result = map_result
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let expected_keys = StringArray::from(vec!["my_key", "second"]);
        let expected_values = Int32Array::from(vec![1, 3]);
        assert_eq!(key_result, &expected_keys);
        assert_eq!(value_result, &expected_values);
    }
    #[test]
    fn test_nested_struct() {
        let list_val_field = Arc::new(Field::new("in_list", DataType::Int32, false));
        let list_field = Field::new("list_f", DataType::List(list_val_field), false);
        let inside_nested_struct_int = Field::new("in_struct_int", DataType::Int32, false);
        let inside_nested_struct_long = Field::new("in_struct_long", DataType::Int64, false);
        let inside_struct_field = Field::new(
            "inside_struct",
            DataType::Struct(Fields::from(vec![
                inside_nested_struct_int,
                inside_nested_struct_long,
            ])),
            false,
        );
        let list_of_structs = Field::new(
            "list_of_structs",
            DataType::List(Arc::new(inside_struct_field)),
            false,
        );
        let outer_struct = Arc::new(Field::new(
            "outer_struct",
            DataType::Struct(Fields::from(vec![list_field, list_of_structs])),
            false,
        ));

        let mut sb = StructContainer::try_new(outer_struct, 1).unwrap();

        let avro_list_val = Value::Int(1);
        let avro_list_val2 = Value::Int(2);
        let avro_list = Value::Array(vec![avro_list_val, avro_list_val2]);
        let avro_inside_nested_struct_int = Value::Int(2);
        let avro_inside_nested_struct_long = Value::Long(3);
        let avro_struct = Value::Record(vec![
            ("in_struct_int".to_string(), avro_inside_nested_struct_int),
            ("in_struct_long".to_string(), avro_inside_nested_struct_long),
        ]);
        let avro_list_struct = Value::Array(vec![avro_struct]);
        let avro_outer_struct = Value::Record(vec![
            ("list_f".to_string(), avro_list),
            ("outer_struct".into(), avro_list_struct),
        ]);

        let _r = sb.add_val(&avro_outer_struct);
        let avro_list_val = Value::Int(2);
        let avro_list = Value::Array(vec![avro_list_val]);
        let avro_inside_nested_struct_int = Value::Int(3);
        let avro_inside_nested_struct_long = Value::Long(4);
        let avro_struct = Value::Record(vec![
            ("in_struct_int".to_string(), avro_inside_nested_struct_int),
            ("in_struct_long".to_string(), avro_inside_nested_struct_long),
        ]);
        let avro_list_struct = Value::Array(vec![avro_struct]);
        let avro_outer_struct = Value::Record(vec![
            ("list_f".to_string(), avro_list),
            ("outer_struct".into(), avro_list_struct),
        ]);
        let _ = sb.add_val(&avro_outer_struct);

        let finished = sb.build();
        let a: RecordBatch = finished
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .into();
        assert_eq!(a.num_rows(), 2);
        assert_eq!(a.columns().len(), 2);
    }

    #[test]
    fn test_add_add_data_to_array_null() {
        let f1 = Field::new("nullints", DataType::Int32, true);
        let f2 = Field::new("nullstring", DataType::Utf8, true);

        let mut b1: Box<dyn ArrayBuilder> = Box::new(Int32Builder::new());
        let avro_int_1 = Value::Union(1, Box::new(Value::Int(2)));
        let avro_int_2 = Value::Union(0, Box::new(Value::Null));
        add_data_to_array_builder(&avro_int_1, &mut b1, &f1);
        add_data_to_array_builder(&avro_int_2, &mut b1, &f1);
        let result_int = b1.finish();
        let expected_int: Box<dyn Array> = Box::new(Int32Array::from(vec![Some(2), None]));
        assert_eq!(result_int, expected_int.into());

        let mut b2: Box<dyn ArrayBuilder> = Box::new(StringBuilder::new());
        let avro_string_1 = Value::Union(1, Box::new(Value::String("hello".into())));
        let avro_string_2 = Value::Union(0, Box::new(Value::Null));
        add_data_to_array_builder(&avro_string_1, &mut b2, &f2);
        add_data_to_array_builder(&avro_string_2, &mut b2, &f2);
        let result_str = b2.finish();
        let expected_str: Box<dyn Array> =
            Box::new(StringArray::from(vec![Some("hello".to_string()), None]));
        assert_eq!(result_str, expected_str.into());
    }

    #[test]
    fn test_struct_array_with_nulls() {
        let f1 = Field::new("int_field", DataType::Int32, true);
        let f2 = Field::new("str_field", DataType::Utf8, true);
        let k_field = Field::new("key", DataType::Utf8, false);
        let v_field = Field::new("value", DataType::Int32, true);
        let struct_field = Arc::new(Field::new(
            "struct_f",
            DataType::Struct(Fields::from(vec![k_field, v_field])),
            false,
        ));
        let map_field = Field::new("map_f", DataType::Map(struct_field.clone(), false), true);
        let fields = Fields::from(vec![f1, f2, map_field]);
        let mut struct_container = StructContainer::try_new_from_fields(fields, 2).unwrap();
        let avro_val = Value::Record(vec![
            ("int_field".to_string(), Value::Null),
            ("str_field".to_string(), Value::Null),
            ("map_f".to_string(), Value::Null),
        ]);
        struct_container.add_val(&avro_val).unwrap();
        let got = struct_container.build().unwrap().as_struct().to_owned();
        assert_eq!(got.null_count(), 0);
        let int_col = got.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_col.null_count(), 1);
        let str_col = got
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(str_col.null_count(), 1);
    }

}
