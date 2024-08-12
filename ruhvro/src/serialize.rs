use apache_avro::Schema;
use arrow::array::{
    Array, ArrayRef, GenericBinaryArray, RecordBatch, StructArray,
};
use rayon::prelude::*;
use std::sync::Arc;
use crate::serialization_containers;
// TODO: Should be checks to make sure avro and arrow schema match
// TODO: need to figure out names. Should serialize match by name or position?
// TODO: Should it include namespace in matching?
// TODO: Add check for sparse union types
// TODO: remove any unwraps and check results/errors

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
pub fn serialize_record_batch(
    rb: RecordBatch,
    schema: &Schema,
    num_chunks: usize,
) -> Vec<GenericBinaryArray<i32>> {
    let struct_arry: ArrayRef = Arc::<StructArray>::new(rb.into());
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
    slices.par_iter().map(|x| serialization_containers::serialize(schema, x)).collect()
}

#[cfg(test)]
mod test {
    use crate::serialize::serialize_record_batch;
    use apache_avro::Schema;
    use arrow::array::{
        ArrayRef, Int32Array, Int32Builder, ListBuilder, MapArray, RecordBatch, StringArray,
        StructArray, TimestampMillisecondArray,
    };
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};
    use std::sync::Arc;

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
        let r = serialize_record_batch(struct_arr_ref.clone(), &schema, 1);
        let ra = r
            .iter()
            .map(|x| x.iter().map(|j| j.unwrap()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        //
        let deserialize = crate::deserialize::per_datum_deserialize(&ra[0], &schema);
        let rb = struct_arr_ref;
        assert_eq!(deserialize, rb);
    }
    #[test]
    fn test_nested_nullable_record() {
        // TODO: Fix test. Issues with naming (namespace + column name)
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
        let arr: RecordBatch = outer_struct_arr.into();
        let parsed_schmea = Schema::parse_str(schmea).unwrap();
        let arr_cont = serialize_record_batch(arr.clone(), &parsed_schmea, 1);
        let ra = arr_cont
            .iter()
            .map(|x| x.iter().map(|j| j.unwrap()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let deserialized = crate::deserialize::per_datum_deserialize(&ra[0], &parsed_schmea);
        // assert_eq!(arr, deserialized);
    }
    #[test]
    fn test_map_record_round_trip() {
        let schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {
                        "name": "map_field",
                        "type": {
                            "type": "map",
                            "values": "int"
                        }
                    }
                ]
            }
        "#;
        let keys = vec!["a", "b"];
        let keys_iter = keys.iter().map(|x| *x);
        let inner_arr: ArrayRef = Arc::new(Int32Array::from(vec![13, 2]));

        let map_arr = MapArray::new_from_strings(keys_iter, &inner_arr, &[0, 1, 2]).unwrap();
        let key_field = Field::new("keys", DataType::Utf8, false);
        let value_field = Field::new("values", DataType::Int32, false);
        let inner_map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![key_field, value_field])),
            false,
        );
        let map_field = Field::new(
            "map_field",
            DataType::Map(Arc::new(inner_map_field), false),
            false,
        );
        let fields = Fields::from(vec![map_field]);
        let record_arr = StructArray::new(fields.clone(), vec![Arc::new(map_arr)], None);
        let schema = Schema::parse_str(schema).unwrap();
        let record_arr_ref: RecordBatch = record_arr.into();
        println!("{:?}", record_arr_ref);
        let r = serialize_record_batch(record_arr_ref.clone(), &schema, 1);
        let ra = r
            .iter()
            .map(|x| x.iter().map(|j| j.unwrap()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let deserialized = crate::deserialize::per_datum_deserialize(&ra[0], &schema);
        assert_eq!(record_arr_ref, deserialized);
    }

    #[test]
    fn test_timestamp_type() {
        // TODO: finish this test
        // let schmea = r#"
        //         {
        //             "type": "record",
        //             "name": "test",
        //             "fields": [
        //                 {
        //                     "name": "timestmap",
        //                     "type": ["null", {
        //                         "name": "nested_rec",
        //                         "type": "record",
        //                         "fields": [
        //                             {"name": "int_field", "type": "int"},
        //                             {"name": "strfield", "type": ["null", "string"]}
        //                         ]
        //                         }
        //                     ]
        //                 },
        //                 {"name": "outer_int", "type": "int"}
        //             ]
        //         }
        //     "#;
        // let inside_arr = Arc::new(Int32Array::from(vec![Some(13), Some(2)]));
        // let inside_str = Arc::new(StringArray::from(vec![Some("abc"), None]));
        // let inner_f1 = Field::new("int_field", DataType::Int32, true);
        // let inner_f2 = Field::new("strfield", DataType::Utf8, true);
        // let fields = Fields::from(vec![inner_f1, inner_f2]);
        // let null_buff = NullBuffer::from(vec![true, false]);
        // let inner_struct_arr = StructArray::new(
        //     fields.clone(),
        //     vec![inside_arr, inside_str],
        //     Some(null_buff),
        // );
        //
        // let outer_int = Arc::new(Int32Array::from(vec![1, 2]));
        // let outer_f1 = Field::new("outer_int", DataType::Int32, false);
        // let inner_struct_field = Field::new("maybe_nested_rec", DataType::Struct(fields), true);
        // let outer_fields = Fields::from(vec![inner_struct_field, outer_f1]);
        // let outer_struct_arr = StructArray::new(
        //     outer_fields.clone(),
        //     vec![Arc::new(inner_struct_arr), outer_int],
        //     None,
        // );
        // let arr: ArrayRef = Arc::new(outer_struct_arr);
        // let parsed_schmea = Schema::parse_str(schmea).unwrap();
        // let arr_cont = serialize_record_batch(arr.clone(), &parsed_schmea, 1);
    }
}
