use crate::deserialize::{add_data_to_array, get_val_from_possible_union};
use anyhow::{anyhow, Result};
use apache_avro::types::Value;
use arrow::array::{
    make_builder, Array, ArrayBuilder, ArrayRef, BooleanBufferBuilder, BooleanBuilder, Datum,
    GenericListArray, StructArray, UnionArray,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, UnionFields};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

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
            DataType::List(inner) => {
                let lc = ListContainer::try_new(field.clone(), capacity)?;
                Ok(AvroToArrowBuilder::List(Box::new(lc)))
            }
            DataType::Struct(flds) => {
                let rewrapped = Arc::new(Field::new(
                    field.name(),
                    DataType::Struct(flds.clone()),
                    field.is_nullable(),
                ));
                let sc = StructContainer::try_new(rewrapped, capacity)?;
                Ok(AvroToArrowBuilder::Struct(Box::new(sc)))
            }
            DataType::Union(union_fields, union_type) => {
                // let rewrapped = Arc::new()
            }
            // DataType::Map(_, _) => {}
            _ => Ok(AvroToArrowBuilder::Primitive(make_builder(
                field.data_type(),
                capacity,
            ))),
        }
    }

    #[inline]
    fn get_primitive(dt: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
        make_builder(dt, capacity)
    }
    fn add_val(&mut self, avro_val: &Value, field: &FieldRef) -> Result<()> {
        match self {
            AvroToArrowBuilder::Primitive(b) => {
                add_data_to_array(avro_val, b, field);
            }
            AvroToArrowBuilder::List(list_container) => {
                list_container.add_val(avro_val)?;
            }
            AvroToArrowBuilder::Struct(struct_container) => {
                struct_container.add_val(avro_val)?;
            }
            AvroToArrowBuilder::Union(union_container) => {
                union_container.add_val(avro_val)?;
            }
            AvroToArrowBuilder::Map(_) => {}
        }
        Ok(())
    }
    fn build(mut self) -> Result<ArrayRef> {
        match self {
            AvroToArrowBuilder::Primitive(mut builder) => {
                let a = builder.finish();
                Ok(a)
            }
            AvroToArrowBuilder::List(list_container) => list_container.build(),
            AvroToArrowBuilder::Struct(sb) => sb.build(),
            AvroToArrowBuilder::Union(ub) => ub.build(),
            AvroToArrowBuilder::Map(_) => {
                unimplemented!()
            }
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
        Ok(ListContainer {
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

    // fn build_inner(mut self) -> Result<ArrayRef> {
    //     self.inner_builder.build()
    // }
}
struct StructContainer {
    fields: FieldRef,
    builders: Vec<(FieldRef, AvroToArrowBuilder)>,
    nulls: BooleanBufferBuilder,
}

impl StructContainer {
    // takes field ref for a struct array
    // let container_f = Arc::new(Field::new(
    // "struct_f",
    // DataType::Struct(Fields::from(vec![f1.clone(), f2.clone()])),
    // false,
    // ));
    fn try_new(field: FieldRef, capacity: usize) -> Result<Self> {
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

    fn add_val(&mut self, avro_val: &Value) -> Result<()> {
        let av = get_val_from_possible_union(avro_val, &self.fields);
        match av {
            Value::Null => {
                self.nulls.append(false);
                // might need to append fake default values for all items
                // this wont work since null array needs to be same
                // length as first array created by builders
                unimplemented!()
            }
            Value::Record(inner_vals) => {
                for (idx, (_field_name, v)) in inner_vals.iter().enumerate() {
                    let mut builder = &mut self.builders[idx];
                    let _ = builder.1.add_val(v, &builder.0)?;
                }
                self.nulls.append(true);
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    fn build(mut self) -> Result<ArrayRef> {
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
}
struct UnionContainer {
    field: FieldRef,
    type_ids: Vec<i8>,
    // type_id_vec contains the index for the array vec
    type_id_vec: Vec<i8>,
    // value_offsets_buffer contains the offset for the buffer of that particular type
    // see https://docs.rs/arrow/latest/arrow/array/struct.UnionArray.html
    value_offsets_buffer: Vec<i32>,
    builders: Vec<(FieldRef, AvroToArrowBuilder)>,
    // contains the current max offset for each type
    position_mapping: HashMap<i8, i32>,
}
impl UnionContainer {
    fn try_new(field: FieldRef, capacity: usize) -> Result<Self> {
        let mut builders = vec![];
        let mut type_ids = vec![];
        let mut position_mapping = HashMap::new();
        let _create_builders =
            if let DataType::Union(union_fields, _unionmode) = field.clone().data_type() {
                for (idx, field_ref) in union_fields.iter() {
                    type_ids.push(idx);
                    let builder = AvroToArrowBuilder::try_new(field_ref, capacity)?;
                    builders.push((field_ref.clone(), builder));
                    position_mapping.insert(idx, 0);
                }
                Ok(())
            } else {
                Err(anyhow!("error creating nested builders in Union"))
            }?;
        Ok(UnionContainer {
            field,
            type_ids,
            type_id_vec: vec![],
            value_offsets_buffer: vec![],
            builders,
            position_mapping,
        })
    }

    fn from_union_fields(union_fields: UnionFields) {}
    ///
    /// Adds value to Union. Union needs to track additional meta data
    fn add_val(&mut self, avro_val: &Value) -> Result<()> {
        if let Value::Union(field_idx, val) = avro_val {
            let mut builder = &mut self.builders[*field_idx as usize];
            let type_idx = *field_idx as i8;
            builder.1.add_val(val, &builder.0)?;

            let current_idx = self.position_mapping.get(&type_idx).unwrap();
            self.value_offsets_buffer.push(*current_idx);
            self.type_id_vec.push(type_idx);
            self.position_mapping
                .entry(type_idx)
                .and_modify(|i| *i += 1);
        }
        Ok(())
    }
    fn build(mut self) -> Result<ArrayRef> {
        let type_id_buffer = Buffer::from_vec(self.type_id_vec);
        let value_offsets_buffer = Buffer::from_vec(self.value_offsets_buffer);
        let children = self
            .builders
            .into_iter()
            .map(|(field, arr)| {
                let a = Field::new(
                    field.name(),
                    field.data_type().to_owned(),
                    field.is_nullable(),
                );
                (
                    a,
                    arr.build()
                        .unwrap_or_else(|e| panic!("Error building union array {}", e)),
                )
            })
            .collect::<Vec<_>>();
        let arr = UnionArray::try_new(
            &self.type_ids[..],
            type_id_buffer,
            Some(value_offsets_buffer),
            children,
        )?;
        Ok(Arc::new(arr))
    }
}
struct MapContainer {}

mod tests {
    use crate::complex::{AvroToArrowBuilder, ListContainer, StructContainer, UnionContainer};
    use apache_avro::types::Value;
    use arrow::array::{
        Array, BooleanBufferBuilder, Int32Array, ListArray, StringArray, StringBuilder,
        StructArray, UnionArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
    use std::sync::Arc;

    #[test]
    fn test_simple_builder() {
        // let int_builder = Box::new(Int32Builder::new());
        let field = Arc::new(Field::new("int_field", DataType::Int32, false));
        // let mut avro_arrow_builder = AvroToArrowBuilder::Primitive(int_builder);
        let mut avro_arrow_builder = AvroToArrowBuilder::try_new(&field, 2).unwrap();
        let avro_val = Value::Int(2);
        let _r = avro_arrow_builder.add_val(&avro_val, &field);
        let avro_val2 = Value::Int(3);
        let r = avro_arrow_builder.add_val(&avro_val2, &field);
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
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(a, 1);
        let b = result
            .clone()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
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
}
