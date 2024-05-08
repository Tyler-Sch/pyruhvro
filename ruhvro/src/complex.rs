use crate::deserialize::{add_data_to_array, get_val_from_possible_union};
use anyhow::Result;
use apache_avro::types::Value;
use arrow::array::{Array, ArrayBuilder, ArrayRef, BooleanBufferBuilder, Datum, GenericListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::FieldRef;
use std::sync::Arc;

enum AvroToArrowBuilder {
    Primitive(Box<dyn ArrayBuilder>),
    List(Box<ListContainer>),
    Struct(Box<StructContainer>),
    Union(Box<UnionContainer>),
    Map(Box<MapContainer>),
}

impl AvroToArrowBuilder {
    fn add_val(&mut self, avro_val: &Value, field: &FieldRef) -> Result<()> {
        match self {
            AvroToArrowBuilder::Primitive(b) => {
                add_data_to_array(avro_val, b, field); // TODO: Make this function only accept primitives (but what about strings)
            }
            AvroToArrowBuilder::List(list_container) => {
                list_container.add_val(avro_val);
            }
            AvroToArrowBuilder::Struct(_) => {}
            AvroToArrowBuilder::Union(_) => {}
            AvroToArrowBuilder::Map(_) => {}
        }
        Ok(())
    }
    fn build(&mut self) -> Result<ArrayRef> {
        match self {
            AvroToArrowBuilder::Primitive(builder) => {
                let a = builder.finish();
                Ok(a)
            }
            AvroToArrowBuilder::List(_) => {
                unimplemented!()
            }
            AvroToArrowBuilder::Struct(_) => {
                unimplemented!()
            }
            AvroToArrowBuilder::Union(_) => {
                unimplemented!()
            }
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
        let inner_array = self.build_inner()?;
        let sb = ScalarBuffer::from(self.offsets);
        let offsets = OffsetBuffer::new(sb);
        let nulls = NullBuffer::new(self.nulls.finish());
        let list_arr =
            GenericListArray::try_new(self.inner_field.clone(), offsets, inner_array, Some(nulls))?;
        Ok(Arc::new(list_arr))
    }

    fn build_inner(&mut self) -> Result<ArrayRef> {
        self.inner_builder.build()
    }
}
struct StructContainer {
    fields: FieldRef,
    builders: Vec<AvroToArrowBuilder>,
}

struct UnionContainer {}
struct MapContainer {}

mod tests {
    use crate::complex::{AvroToArrowBuilder, ListContainer};
    use apache_avro::types::Value;
    use arrow::array::{
        Array, BooleanBufferBuilder, Int32Array, Int32Builder, ListArray, StringArray,
        StringBuilder,
    };
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_simple_builder() {
        let int_builder = Box::new(Int32Builder::new());
        let mut avro_arrow_builder = AvroToArrowBuilder::Primitive(int_builder);
        let avro_val = Value::Int(2);
        let field = Arc::new(Field::new("int_field", DataType::Int32, false));
        let r = avro_arrow_builder.add_val(&avro_val, &field);
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
        let r1 = lc.add_val(&vec_avro);

        // add list with one null val
        let avro_data1 = Value::String("hello null".into());
        let avro_value_null = Value::Null;
        let vec_avro = Value::Array(vec![avro_data1, avro_value_null]);
        let r2 = lc.add_val(&vec_avro);

        // add null val
        let null_val = Value::Null;
        let union_null = Value::Union(1, Box::new(null_val));
        let r3 = lc.add_val(&union_null);
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
        );}
}
