//! Python extensions for transforming a vector of avro encoded binary data to an
//! apache arrow record batch
//!
use arrow::array::RecordBatch;
use arrow::pyarrow::PyArrowType;
use pyo3::prelude::*;
use pyo3::types::PyList;
use ruhvro::deserialize;
use ruhvro::conversions;

#[pyfunction]
fn deserialize_array(list: &PyList, schema: &str) -> PyResult<PyArrowType<RecordBatch>> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let borrow_list = list
        .iter()
        .map(|x| x.extract::<&[u8]>().unwrap())
        .collect::<Vec<_>>();
    let record_batch = deserialize::per_datum_deserialize(&borrow_list, &parsed_schema);
    Ok(PyArrowType(record_batch))
}
#[pyfunction]
fn deserialize_array_threaded(
    list: &PyList,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let borrow_list = list
        .iter()
        .map(|x| x.extract::<&[u8]>().unwrap())
        .collect::<Vec<_>>();
    let record_batches =
        deserialize::per_datum_deserialize_arrow_multi(borrow_list, &parsed_schema, num_chunks);
    let python_typed_batches = record_batches
        .into_iter()
        .map(|x| PyArrowType(x))
        .collect::<Vec<_>>();
    Ok(python_typed_batches)
}


#[pyfunction]
fn deserialize_array_threaded_remove_union(
    list: &PyList,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let borrow_list = list
        .iter()
        .map(|x| x.extract::<&[u8]>().unwrap())
        .collect::<Vec<_>>();
    let record_batches =
        deserialize::per_datum_deserialize_arrow_multi(borrow_list, &parsed_schema, num_chunks);
    let python_typed_batches = record_batches
        .into_iter()
        .map(|x| PyArrowType(conversions::remove_union_arrays(x)))
        .collect::<Vec<_>>();
    Ok(python_typed_batches)
}
/// A Python module implemented in Rust.
#[pymodule]
fn pyruhvro(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(deserialize_array, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_array_threaded, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_array_threaded_remove_union, m)?)?;
    Ok(())
}
