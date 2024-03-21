use std::collections::HashMap;

use apache_avro::types;
use apache_avro::types::Value as AvroValue;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyLong,PyDict, PyInt, PyList};
use pyo3::PyObject;
use ruhvro::deserialize;
use arrow::pyarrow::PyArrowType;
use arrow::array::{make_array, ArrayData, BinaryArray, StringArray};

#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

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

#[pyfunction]
fn mess_around(a: &PyList) -> PyResult<HashMap<String, i32>> {
    let z = a.extract::<Vec<i32>>()?;
    let mut hm = HashMap::new();
    for i in z {
        let news = format!("{}_string", i);
        hm.insert(news, i);
    }
    Ok(hm)
}

// #[pyfunction]
// fn tes(py: Python<'_>) -> PyResult<&PyDict> {
//     let a = PyDict::new(py);
//     a.set_item("mykey", 23);
//     a.set_item(1, 123);
//     a.set_item("morestring", 1.2);
//     Ok(a)

// }

#[pyfunction]
fn tes() -> PyResult<MyObj> {
    Ok(MyObj::Str("sure".into()))

}

#[pyfunction]
fn tess() -> PyResult<Vec<HashMap<MyObj, MyObj>>> {
    let mut outer = vec![];
    for i in (0..1000000) {
        let mut hm = HashMap::new();
        hm.insert(MyObj::Str("sure".into()), MyObj::Int(13));
        hm.insert(MyObj::Int(2), MyObj::Str("this thing is difficult".into()));
        outer.push(hm);
    }
    Ok(outer)
}

#[pyfunction]
fn tess2(py: Python) -> PyResult<Vec<&PyDict>> {
    let mut outer = vec![];
    for i in (0..1000000) {
        let pd = PyDict::new(py);
        pd.set_item("sure", 13);
        pd.set_item(2, "this thing is difficult");

        
        outer.push(pd);
    }
    Ok(outer)
}
// #[pyfunction]
// fn tes() -> PyResult<&PyDict> {
//     Python::with_gil(|py| {
//         let a = PyDict::new(py).to_object(py);
//     });
//     a.set_item("mykey", 23);
//     a.set_item(1, 123);
//     a.set_item("morestring", 1.2);
//     Ok(a)
// }
#[derive(FromPyObject, Debug, PartialEq, Eq, Hash)]
enum MyObj {
    Int(i32),
    Str(String),
}

impl IntoPy<PyObject> for MyObj {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let p = match self {
            MyObj::Int(i) => i.into_py(py),
            MyObj::Str(s) => s.into_py(py)
        };
        p
    }

}

enum MyCollec {
    HM(MyObj, MyObj),
    Lis(Vec<MyObj>),
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyruhvro(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(tes, m)?)?;
    m.add_function(wrap_pyfunction!(tess, m)?)?;
    m.add_function(wrap_pyfunction!(tess2, m)?)?;
    m.add_function(wrap_pyfunction!(mess_around, m)?)?;
    m.add_function(wrap_pyfunction!(from_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(deserialize_datum_from_arrow, m)?)?;
    let _ = m.add_function(wrap_pyfunction!(deserialize_datum, m)?)?;
    Ok(())
}
