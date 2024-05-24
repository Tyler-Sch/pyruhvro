use std::sync::Arc;
use arrow::array::{make_array, ArrayData, RecordBatch, BinaryArray};
use arrow::pyarrow::PyArrowType;
use pyo3::prelude::*;
use ruhvro::deserialize;

// #[pyfunction]
// fn deserialize_arrow(
//     array: PyArrowType<ArrayData>,
//     schema: &str,
// ) -> PyResult<PyArrowType<RecordBatch>> {
//     let parsed_schema = deserialize::parse_schema(schema).unwrap();
//     let array = array.0;
//     let array = make_array(array);
//     let r = deserialize::per_datum_deserialize_arrow(array, &parsed_schema);
//     Ok(PyArrowType(r))
// }

#[pyfunction]
fn deserialize_arrow(
    py: Python<'_>,
    array: PyArrowType<ArrayData>,
    schema: &str,
) -> PyResult<PyArrowType<RecordBatch>> {
    py.allow_threads(|| {
        let parsed_schema = deserialize::parse_schema(schema).unwrap();
        let array = array.0;
        let array = make_array(array);
        let r = deserialize::per_datum_deserialize_arrow(array, &parsed_schema);
        Ok(PyArrowType(r))
        }
    )
}
#[pyfunction]
fn deserialize_arrow_threaded(
    array: PyArrowType<ArrayData>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let array = array.0;
    let array = make_array(array);
    let r = deserialize::per_datum_deserialize_arrow_multi(array, &parsed_schema, num_chunks);
    let i = r.into_iter().map(|x| PyArrowType(x)).collect::<Vec<_>>();
    Ok(i)
}

#[pyfunction]
fn deserialize_array(list: Vec<&[u8]>, schema: &str) -> PyResult<PyArrowType<RecordBatch>> {
   let input = Arc::new(BinaryArray::from_vec(list));
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let r = deserialize::per_datum_deserialize_arrow(input, &parsed_schema);
    Ok(PyArrowType(r))
}

#[pyfunction]
fn deserialize_array_threaded(list: Vec<&[u8]>, schema: &str, num_chunks: usize) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let input = Arc::new(BinaryArray::from_vec(list));
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let r = deserialize::per_datum_deserialize_arrow_multi(input, &parsed_schema, num_chunks);
    let i = r.into_iter().map(|x| PyArrowType(x)).collect::<Vec<_>>();
    Ok(i)
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyruhvro(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(deserialize_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_arrow_threaded, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_array, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_array_threaded, m)?)?;
    Ok(())
}
