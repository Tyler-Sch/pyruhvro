# Ruhvro 
A library for deserializing schemaless avro encoded bytes into [Apache Arrow](https://github.com/apache/arrow-rs) 
record batches. This library was created as 
an experiment to gauge potential improvements in kafka messages deserialization speed - particularly from the python
ecosystem. 

The main speed-ups in this code are from releasing python's gil during deserialization
and the use of multiple cores. The speed-ups are much more noticable on larger datasets or while
running several python threads at once.
## Building 
### Python extension

#### building a wheel:
Requires [Rust tools](https://doc.rust-lang.org/cargo/getting-started/installation.html) to be installed
- create python virtual environment
- `pip install maturin`
- `maturin build --release` 
- the previous command should yield a path to the compiled wheel file, something like this `/Users/currentUser/rust/pyruhvro/target/wheels/pyruhvro-0.1.0-cp312-cp312-macosx_11_0_arm64.whl`
- `pip install /Users/tylerschauer/rust/pyruhvro/target/wheels/pyruhvro-0.1.0-cp312-cp312-macosx_11_0_arm64.whl`

The extension can be used like so:
```python
from pyruhvro import deserialize_array, deserialize_array_threaded 

schema = """
    {
      "type": "record",
      "name": "UserData",
      "namespace": "com.example",
      "fields": [
        {
          "name": "userId",
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
serialized_messages: List[List[u8]] = [serialized messages...]

record_batch = deserialize_array(serialized_messages, schema) 

# or if you'd like to leverage multiple cores
num_cores = 8
deserialize_array_threaded(serialized_messages, schema, num_cores)
```