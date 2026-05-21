//! Python extensions for transforming a vector of avro encoded binary data to an
//! apache arrow record batch
//!
use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::pyarrow::PyArrowType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyList;
use ruhvro::deserialize;
use ruhvro::serialize;

fn to_py_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn extract_bytes_list(list: &Bound<'_, PyList>) -> PyResult<Vec<PyBackedBytes>> {
    list.iter()
        .map(|x| x.extract::<PyBackedBytes>().map_err(PyErr::from))
        .collect()
}

#[pyfunction]
fn deserialize_array(list: &Bound<'_, PyList>, schema: &str) -> PyResult<PyArrowType<RecordBatch>> {
    let parsed_schema = deserialize::parse_schema(schema).map_err(to_py_err)?;
    let owned = extract_bytes_list(list)?;
    let borrow_list: Vec<&[u8]> = owned.iter().map(|b| &b[..]).collect();
    let record_batch =
        deserialize::per_datum_deserialize(&borrow_list, &parsed_schema).map_err(to_py_err)?;
    Ok(PyArrowType(record_batch))
}

#[pyfunction]
fn deserialize_array_threaded(
    list: &Bound<'_, PyList>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let parsed_schema = deserialize::parse_schema(schema).map_err(to_py_err)?;
    let owned = extract_bytes_list(list)?;
    let borrow_list: Vec<&[u8]> = owned.iter().map(|b| &b[..]).collect();
    let record_batches =
        deserialize::per_datum_deserialize_threaded(borrow_list, &parsed_schema, num_chunks)
            .map_err(to_py_err)?;
    Ok(record_batches.into_iter().map(PyArrowType).collect())
}

#[pyfunction]
fn serialize_record_batch(
    data: PyArrowType<RecordBatch>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<ArrayData>>> {
    let parsed_schema = deserialize::parse_schema(schema).map_err(to_py_err)?;
    let serialized = serialize::serialize_record_batch(data.0, &parsed_schema, num_chunks)
        .map_err(to_py_err)?;
    Ok(serialized
        .into_iter()
        .map(|x| PyArrowType(x.into_data()))
        .collect())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyruhvro(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(deserialize_array, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_array_threaded, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_record_batch, m)?)?;
    Ok(())
}
