# Ruhvro 
A library for deserializing schemaless avro encoded bytes into [Apache Arrow](https://github.com/apache/arrow-rs) 
record batches. This library was created as 
an experiment to gauge potential improvements in kafka messages deserialization speed - particularly from the python
ecosystem. 

The main speed-ups in this code are from releasing python's gil during deserialization
and the use of multiple cores. The speed-ups are much more noticeable on larger datasets or more complex avro schemas.

## Still experimental
This library is still experimental and has not been tested in production. Please use with caution.

# Benchmarks - comparing to fastavro
### On a 2022 m2 macbook air with 8gb memory and 8 cores processing 10000 records using timeit
```
Running pyruhvro serialize
20 loops, best of 5: 13.8 msec per loop
running fastavro serialize
5 loops, best of 5: 71.7 msec per loop
running pyruhvro deserialize
50 loops, best of 5: 6.59 msec per loop
running fastavro deserialize
5 loops, best of 5: 55.3 msec per loop
```

### Run benchmarks locally 
```commandline
pip install pyruhvro 
pip install fastavro
pip install pyarrow

cd scripts
bash benchmark.sh
```

## Usage
see scripts/generate_avro.py for a working example

```python
from typing import List
from pyarrow import  RecordBatch
from pyruhvro import deserialize_array_threaded, serialize_record_batch

schema = """
    {
      "type": "record",
      "name": "userdata",
      "namespace": "com.example",
      "fields": [
        {
          "name": "userid",
          "type": "string"
        },
        {
          "name": "age",
          "type": "int"
        },
        ... more fields...
    }
    """
    
# serialized values from kafka messages
serialized_messages: list[bytes] = [serialized_message1, serialized_message2, ...]

# num_chunks is the number of chunks to break the data down into. These chunks can be picked up by other threads/cores on your machine
num_chunks = 8
record_batches: List[RecordBatch] = deserialize_array_threaded(serialized_messages, schema, num_chunks)

# serialize the record batches back to avro
serialized_records =  [serialize_record_batch(r, schema, 8) for r in record_batches]

```
## Building from source:
requires [rust tools](https://doc.rust-lang.org/cargo/getting-started/installation.html) to be installed
- create python virtual environment
- `pip install maturin`
- `maturin build --release` 
- the previous command should yield a path to the compiled wheel file, something like this `/users/currentuser/rust/pyruhvro/target/wheels/pyruhvro-0.1.0-cp312-cp312-macosx_11_0_arm64.whl`
- `pip install /users/currentuser/rust/pyruhvro/target/wheels/pyruhvro-0.1.0-cp312-cp312-macosx_11_0_arm64.whl`
