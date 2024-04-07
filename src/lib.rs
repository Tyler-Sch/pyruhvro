use pyo3::prelude::*;
use ruhvro::deserialize;
use arrow::pyarrow::PyArrowType;
use arrow::array::{make_array, ArrayData, Array, RecordBatch};



#[pyfunction]
fn deserialize_arrow(array: PyArrowType<ArrayData>, schema: &str) -> PyResult<PyArrowType<RecordBatch>> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let array = array.0;
    let array = make_array(array);
    let r = deserialize::per_datum_deserialize_arrow(array,&parsed_schema);
    Ok(PyArrowType(r))
}

#[pyfunction]
fn deserialize_arrow_threaded(array: PyArrowType<ArrayData>, schema: &str, num_chunks: usize) -> PyResult<Vec<PyArrowType<RecordBatch>>> {

    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let array = array.0;
    let array = make_array(array);
    let r = deserialize::per_datum_deserialize_arrow_multi(array,&parsed_schema, num_chunks);
    let i = r.into_iter().map(|x| PyArrowType(x)).collect::<Vec<_>>();
    Ok(i)
}




/// A Python module implemented in Rust.
#[pymodule]
fn pyruhvro(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(deserialize_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_arrow_threaded, m)?)?;
    Ok(())
}
