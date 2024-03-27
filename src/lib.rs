use std::collections::HashMap;

use apache_avro::types;
use apache_avro::types::Value as AvroValue;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use pyo3::PyObject;
use ruhvro::deserialize;
use arrow::pyarrow::PyArrowType;
use arrow::array::{make_array, ArrayData, StringArray, Array, RecordBatch};


#[pyfunction]
fn from_arrow<'a>(array: PyArrowType<ArrayData>) -> PyResult<Vec<String>> {
    let array = array.0;
    let array = make_array(array);
    let dcast = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| PyTypeError::new_err("Not a String array"))?;
    let a = dcast.into_iter().map(|x| String::from(x.unwrap())).collect::<Vec<_>>();
    Ok(a)

}

#[pyfunction]
fn deserialize_datum_from_arrow(array: PyArrowType<ArrayData>, schema: &str) -> PyResult<String> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let array = array.0;
    let array = make_array(array);
    let r = deserialize::per_datum_arrow_deserialize(array.clone(), &parsed_schema);
    Ok(format!("shape of output is {} rows, {} columns", r.len(), r[0].len()))


}

#[pyfunction]
fn deserialize_datum<'a>(py: Python<'a>, data: Vec<Vec<u8>>, writer_schema: &'a str) -> PyResult<&'a PyList> {
    let parsed_schema = deserialize::parse_schema(writer_schema).unwrap();
    let decoded = deserialize::per_datum_deserialize_multi(&data, &parsed_schema);
    let r = decoded
        .into_iter()
        .map(|x| avro_to_pydict(&x.unwrap()).unwrap())
        .collect::<Vec<_>>();
    let ra = PyList::new(py, &r);
    Ok(ra)
}

#[pyfunction]
fn deserialize_arrow(array: PyArrowType<ArrayData>, schema: &str) -> PyResult<PyArrowType<ArrayData>> {
    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let array = array.0;
    let array = make_array(array);
    let r = deserialize::per_datum_deserialize_arrow(array,&parsed_schema);
    Ok(PyArrowType(r.into_data()))
}

#[pyfunction]
fn deserialize_arrow_threaded(array: PyArrowType<ArrayData>, schema: &str) -> PyResult<Vec<PyArrowType<RecordBatch>>> {

    let parsed_schema = deserialize::parse_schema(schema).unwrap();
    let array = array.0;
    let array = make_array(array);
    let r = deserialize::per_datum_deserialize_arrow_multi(array,&parsed_schema);
    let i = r.into_iter().map(|x| PyArrowType(x)).collect::<Vec<_>>();
    Ok(i)
}

fn avro_to_pydict(data: &types::Value) -> PyResult<PyObject> {
    Python::with_gil(|py|{
    let d = PyDict::new(py);
    if let types::Value::Record(i) = data {
        for (name, val) in i {
            let _ = d.set_item(&name, avro_to_py(d, val));
        }
    } else {
        unimplemented!("Cant have non-record type at top level")
    }

    Ok(d.into())

    })
}

fn avro_to_py(p: &PyDict, av: &AvroValue) -> PyObject {
    let py = p.py();
    match av {
        AvroValue::Int(num) => num.into_py(py),
        AvroValue::String(st) => st.into_py(py),
        AvroValue::Boolean(b) => b.into_py(py),
        AvroValue::Array(items) => {
            let mut arr = vec![];
            for i in items {
                if let AvroValue::Record(r) = i {
                    let record = avro_to_pydict(av).unwrap();
                    arr.push(record);
                } else {
                    arr.push(avro_to_py(p, i))
                }
            }
            arr.into_py(py)
        }
        AvroValue::Record(rec) => avro_to_pydict(&av).unwrap(),
        AvroValue::Union(_, y) => avro_to_py(p, y),
        AvroValue::Map(avromap) => {
            let mut hm = HashMap::new();
            avromap.into_iter().for_each(|(x, y)| {
                let r = avro_to_py(p, y);
                hm.insert(x, r);
            });
            hm.into_py(py)
        }
        AvroValue::LocalTimestampMillis(i) => i.into_py(py),
        AvroValue::TimestampMillis(i) => i.into_py(py),
        AvroValue::Long(i) => i.into_py(py),
        AvroValue::Float(dec) => dec.into_py(py),
        _ => unimplemented!(),
    }
}


/// A Python module implemented in Rust.
#[pymodule]
fn pyruhvro(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(from_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_datum_from_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_arrow_threaded, m)?)?;
    let _ = m.add_function(wrap_pyfunction!(deserialize_datum, m)?)?;
    Ok(())
}
