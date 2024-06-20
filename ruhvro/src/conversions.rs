use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int32Array, ListArray, StructArray, UnionArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use std::sync::Arc;

pub fn remove_union_arrays(rb: RecordBatch) -> RecordBatch {
    let s_arr: StructArray = rb.into();
    let outer_field = Field::new(
        "outer_field",
        DataType::Struct(s_arr.fields().clone()),
        false,
    );
    let converted = convert_unions_to_structs(Arc::new(outer_field), Arc::new(s_arr));
    let result: RecordBatch = converted.1.as_struct().into();
    result
}
fn convert_unions_to_structs(fr: FieldRef, ar: ArrayRef) -> (FieldRef, ArrayRef) {
    match fr.data_type() {
        DataType::Struct(_) => {
            let s_array = ar.as_struct().to_owned();
            let (fields, array_data, null_buff) = s_array.into_parts();
            let mut children = vec![];
            let mut field_vec = vec![];
            let _converted = fields.iter().zip(array_data).for_each(|(f, a)| {
                let (converted_schema, child) = convert_unions_to_structs(f.clone(), a);
                children.push(child);
                field_vec.push(converted_schema);
            });
            let new_struct = StructArray::new(Fields::from(field_vec), children, null_buff);

            let converted_fields = Field::new(
                fr.name(),
                DataType::Struct(new_struct.fields().clone()),
                new_struct.is_nullable(),
            );
            (
                Arc::new(converted_fields) as FieldRef,
                Arc::new(new_struct) as ArrayRef,
            )
        }
        DataType::Union(_, _) => {
            let u_array = ar.as_any().downcast_ref::<UnionArray>().unwrap().to_owned();
            let converted = convert_union(u_array).unwrap();
            let converted_fields = Field::new(
                fr.name(),
                DataType::Struct(converted.fields().clone()),
                converted.is_nullable(),
            );
            (
                Arc::new(converted_fields) as FieldRef,
                Arc::new(converted) as ArrayRef,
            )
        }
        DataType::List(_) => {
            let l_array = ar.as_list().to_owned();
            let (inner_field, offset_buff, inner_arr, null_buff) = l_array.into_parts();
            let converted = convert_unions_to_structs(inner_field, inner_arr);
            let converted_field = Field::new(
                fr.name(),
                DataType::List(converted.0.clone()),
                fr.is_nullable(),
            );
            let new_list = ListArray::new(converted.0, offset_buff, converted.1, null_buff);
            (Arc::new(converted_field), Arc::new(new_list))
        }
        _ => (fr, ar),
    }
}

fn convert_union(ua: UnionArray) -> Result<StructArray> {
    let (union_fields, _type_ids, _offsets, children) = ua.into_parts();
    let arrs = children
        .into_iter()
        .zip(union_fields.iter())
        .map(|(child, (_, fr))| convert_unions_to_structs(fr.to_owned(), child))
        .collect::<Vec<_>>();
    let struct_arr = StructArray::from(arrs);
    Ok(struct_arr)
}

mod test {
    use crate::conversions::convert_union;
    use arrow::array::{Array, Int32Array, StringArray, UnionArray};
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
    use std::sync::Arc;

    #[test]
    fn test_convert_union() {
        let int_f = Arc::new(Field::new("int_field", DataType::Int32, true));
        let str_f = Arc::new(Field::new("str_field", DataType::Utf8, true));
        let union_fields = UnionFields::new([0, 1], [int_f.clone(), str_f.clone()]);
        let union_f = Field::new(
            "union_f",
            DataType::Union(union_fields, UnionMode::Sparse),
            false,
        );
        let fields = Fields::from(vec![union_f]);
        let arr1 = Int32Array::from(vec![Some(1), None, Some(2)]);
        let arr2 = StringArray::from(vec![None, Some("test".to_string()), None]);
        let type_ids = [0_i8, 1, 0].into_iter().collect::<ScalarBuffer<i8>>();

        let union_fields = [(0, int_f), (1, str_f)]
            .into_iter()
            .collect::<UnionFields>();
        let children = vec![Arc::new(arr1) as Arc<dyn Array>, Arc::new(arr2)];
        let uarray = UnionArray::try_new(union_fields, type_ids, None, children).unwrap();
        // let (a, b,c, d) = uarray.into_parts();
        // let z = d[0];
        // let f: Fields = a.into();
        // let z = a.iter().map(|x| x.1.to_owned()).collect::<Vec<_>>();
        // let fields: Fields = z.into();
        // let sa = StructArray::try_new(fields, d, None).unwrap();
        let sa = convert_union(uarray);
        println!("{:?}", sa);

        // let a: ArrayData = uarray.into();
        // println!("{:?}", a.child_data());
        // let b = a.child_data();
        // let z = StructArray::from;
        // println!("{:?}", z);
    }
}
#[test]
fn test_list_converted_to_self() {
    let offsets = OffsetBuffer::new(vec![0, 1, 4, 5].into());
    let values = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);
    let values = Arc::new(values) as ArrayRef;

    let field = Arc::new(Field::new("element", DataType::Int32, false));
    let list_arr = Arc::new(ListArray::new(
        field.clone(),
        offsets.clone(),
        values.clone(),
        None,
    ));
    let r = convert_unions_to_structs(field, list_arr.clone());
    assert_eq!(list_arr, r.1.as_list().to_owned().into());
}

#[test]
fn test_struct_convert_to_self() {
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

    let struct_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
    ]));

    let struct_array_field = Field::new(
        "struct_arr",
        DataType::Struct(struct_array.fields().clone()),
        false,
    );

    let convert = convert_unions_to_structs(Arc::new(struct_array_field), struct_array.clone());
    assert_eq!(struct_array, convert.1.as_struct().to_owned().into());
}
