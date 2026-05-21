# Ruhvro 
A library for deserializing schemaless avro encoded bytes into [Apache Arrow](https://github.com/apache/arrow-rs) 
record batches. This library was created as 
an experiment to gauge potential improvements in kafka messages deserialization speed - particularly from the python
ecosystem. 

The main speed-ups come from:
1. A **fast direct-decode/encode path** that walks Avro bytes and Arrow builders
   together, skipping `apache_avro::types::Value` tree construction entirely.
   This supports primitives, logical types (date, timestamp-millis/micros),
   enums, nested records, arrays, maps, and unions (2-variant nullable and
   N-variant sparse). Anything outside that subset transparently falls back
   to the original `apache_avro`-based path.
2. Releasing Python's GIL during deserialization.
3. Multi-core processing via rayon (configurable chunk count).

The speed-ups are much more noticeable on larger datasets or more complex avro schemas.

## Still experimental
This library is still experimental and has not been tested in production. Please use with caution.

# Benchmarks - comparing to fastavro

### Apple M-series, 8 cores, processing 10,000 records using `timeit` (fastavro 1.12, pyarrow 24)
```
Running pyruhvro serialize
200 loops, best of 5: 1.40 msec per loop
running fastavro serialize
5 loops, best of 5: 57.6 msec per loop
running pyruhvro deserialize
200 loops, best of 5: 1.17 msec per loop
running fastavro deserialize
5 loops, best of 5: 40.5 msec per loop
```

That works out to **≈ 41× faster than fastavro on serialize and ≈ 35× on deserialize**
for this schema (a complex Kafka-style record with nested structs, arrays, maps,
nullable fields, a multi-variant union, and an enum — see
[`scripts/generate_avro.py`](scripts/generate_avro.py)).

### Run benchmarks locally 
```commandline
pip install pyruhvro 
pip install fastavro
pip install pyarrow

cd scripts
bash run_benchmarks.sh
```

### Rust microbenchmarks (`ruhvro` crate)

For lower-noise measurements of the core Rust code (no Python interop), the
`ruhvro` crate has criterion benches covering both directions across several
schema shapes:

```sh
cargo bench -p ruhvro                                  # everything
cargo bench -p ruhvro --bench deserialize              # deserialize only
cargo bench -p ruhvro --bench serialize                # serialize only
```

HTML reports land in `target/criterion/report/index.html`. See
[`ruhvro/README.md`](ruhvro/README.md#benchmarks) for the full list of benches
and profiling tips.

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
