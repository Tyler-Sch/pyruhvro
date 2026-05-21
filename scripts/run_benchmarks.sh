# num records to generate
NUM_RECORDS=10000
SETUP="from pyruhvro import deserialize_array_threaded, serialize_record_batch; import generate_avro; import io; records = generate_avro.generate_records($NUM_RECORDS);serialized_records=generate_avro.get_serialized_records(records);list_record_batches = generate_avro.deserialize_pyruhvro(serialized_records, generate_avro.schema_string);"

echo "Running benchmarks..."
echo "Running pyruhvro serialize"
python -m timeit -s "$SETUP" "generate_avro.serialize_pyruhvro(list_record_batches, generate_avro.schema_string)"

echo "running fastavro serialize"
python -m timeit -s "$SETUP" "generate_avro.get_serialized_records(records)"

echo "running pyruhvro deserialize"
python -m timeit -s "$SETUP" "generate_avro.deserialize_pyruhvro(serialized_records, generate_avro.schema_string)"

echo "running fastavro deserialize"
python -m timeit -s "$SETUP" "generate_avro.deserialize_fast_avro(serialized_records, generate_avro.parsed_schema)"