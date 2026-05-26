//! Python extensions for transforming a vector of avro encoded binary data to an
//! apache arrow record batch.
//!
//! Two cross-cutting concerns live here:
//!   1. **Schema cache** — `ruhvro`'s threaded API takes `Arc<Schema>` so the
//!      parsed schema is shared (not cloned) across spawned tasks. We cache
//!      parsed schemas keyed by their source string so Python callers don't
//!      re-parse JSON on every call.
//!   2. **GIL release** — every Python entry point releases the GIL around the
//!      Rust work so multiple Python threads can call into pyruhvro
//!      concurrently and benefit from ruhvro's internal parallelism.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use apache_avro::Schema as AvroSchema;
use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::pyarrow::PyArrowType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyList;
use ruhvro::{deserialize, serialize};

fn to_py_err<E: std::fmt::Display>(e: E) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn extract_bytes_list(list: &Bound<'_, PyList>) -> PyResult<Vec<PyBackedBytes>> {
    list.iter()
        .map(|x| x.extract::<PyBackedBytes>().map_err(PyErr::from))
        .collect()
}

/// Schema cache, keyed by the raw schema string. Avoids reparsing on every
/// call — schema parsing dominates small-payload latency for repeated calls
/// with the same schema. Unbounded by design: real workloads use a handful
/// of distinct schemas per process.
fn schema_cache() -> &'static Mutex<HashMap<String, Arc<AvroSchema>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<AvroSchema>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_or_parse_schema(schema: &str) -> PyResult<Arc<AvroSchema>> {
    {
        let cache = schema_cache().lock().expect("schema cache poisoned");
        if let Some(s) = cache.get(schema) {
            return Ok(Arc::clone(s));
        }
    }
    let parsed = Arc::new(deserialize::parse_schema(schema).map_err(to_py_err)?);
    let mut cache = schema_cache().lock().expect("schema cache poisoned");
    Ok(Arc::clone(cache.entry(schema.to_string()).or_insert(parsed)))
}

#[pyfunction]
fn deserialize_array(
    py: Python<'_>,
    list: &Bound<'_, PyList>,
    schema: &str,
) -> PyResult<PyArrowType<RecordBatch>> {
    let parsed_schema = get_or_parse_schema(schema)?;
    let owned = extract_bytes_list(list)?;
    let record_batch = py
        .detach(move || {
            let borrow_list: Vec<&[u8]> = owned.iter().map(|b| &b[..]).collect();
            deserialize::per_datum_deserialize(&borrow_list, &parsed_schema)
        })
        .map_err(to_py_err)?;
    Ok(PyArrowType(record_batch))
}

#[pyfunction]
fn deserialize_array_threaded(
    py: Python<'_>,
    list: &Bound<'_, PyList>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let parsed_schema = get_or_parse_schema(schema)?;
    let owned = extract_bytes_list(list)?;
    let record_batches = py
        .detach(move || {
            let borrow_list: Vec<&[u8]> = owned.iter().map(|b| &b[..]).collect();
            deserialize::per_datum_deserialize_threaded(borrow_list, parsed_schema, num_chunks)
        })
        .map_err(to_py_err)?;
    Ok(record_batches.into_iter().map(PyArrowType).collect())
}

#[pyfunction]
fn serialize_record_batch(
    py: Python<'_>,
    data: PyArrowType<RecordBatch>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<ArrayData>>> {
    let parsed_schema = get_or_parse_schema(schema)?;
    let serialized = py
        .detach(move || serialize::serialize_record_batch(data.0, parsed_schema, num_chunks))
        .map_err(to_py_err)?;
    Ok(serialized
        .into_iter()
        .map(|x| PyArrowType(x.into_data()))
        .collect())
}

#[pyfunction]
fn deserialize_array_threaded_spawn(
    py: Python<'_>,
    list: &Bound<'_, PyList>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<RecordBatch>>> {
    let parsed_schema = get_or_parse_schema(schema)?;
    let owned = extract_bytes_list(list)?;
    let record_batches = py
        .detach(move || {
            let borrow_list: Vec<&[u8]> = owned.iter().map(|b| &b[..]).collect();
            deserialize::per_datum_deserialize_threaded_spawn(
                borrow_list,
                parsed_schema,
                num_chunks,
            )
        })
        .map_err(to_py_err)?;
    Ok(record_batches.into_iter().map(PyArrowType).collect())
}

#[pyfunction]
fn serialize_record_batch_spawn(
    py: Python<'_>,
    data: PyArrowType<RecordBatch>,
    schema: &str,
    num_chunks: usize,
) -> PyResult<Vec<PyArrowType<ArrayData>>> {
    let parsed_schema = get_or_parse_schema(schema)?;
    let serialized = py
        .detach(move || {
            serialize::serialize_record_batch_spawn(data.0, parsed_schema, num_chunks)
        })
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
    m.add_function(wrap_pyfunction!(deserialize_array_threaded_spawn, m)?)?;
    m.add_function(wrap_pyfunction!(serialize_record_batch_spawn, m)?)?;
    Ok(())
}
