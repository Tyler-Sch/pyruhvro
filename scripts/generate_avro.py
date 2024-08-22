import fastavro
import io
from faker import Faker
import random
from pyruhvro import deserialize_array_threaded, serialize_record_batch
import json

# Initialize Faker
fake = Faker()

# Define the Avro schema with a union type having at least 3 variants
schema = {
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": ["null", "string"], "default": None},
        {"name": "age", "type": ["null", "int"], "default": None},
        {"name": "emails", "type": {"type": "array", "items": "string"}},
        {"name": "address", "type": ["null", {
            "type": "record",
            "name": "Address",
            "fields": [
                {"name": "street", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "zipcode", "type": "string"}
            ]
        }], "default": None},
        {"name": "phone_numbers", "type": {"type": "map", "values": "string"}},
        {"name": "preferences", "type": ["null", {
            "type": "record",
            "name": "Preferences",
            "fields": [
                {"name": "contact_method", "type": ["null", "string"], "default": None},
                {"name": "newsletter", "type": "boolean"}
            ]
        }], "default": None},
        {"name": "status", "type": ["null", "string", "int", "boolean"], "default": None},
        {"name": "created_at", "type": "long"},
        {"name": "class", "type": {"type": "enum", "name": "enum_col", "symbols": ["A", "B", "C"]}}
    ]
}

# Function to generate random data using Faker
def generate_random_record():
    return {
        "name": fake.name() if random.choice([True, False]) else None,
        "age": random.choice([None, random.randint(18, 80)]),
        "emails": [fake.email() for _ in range(random.randint(0, 3))],
        "address": random.choice([None, {
            "street": fake.street_address(),
            "city": fake.city(),
            "zipcode": fake.zipcode()
        }]),
        "phone_numbers": {fake.word(): fake.phone_number() for _ in range(random.randint(0, 3))},
        "preferences": random.choice([None, {
            "contact_method": random.choice([None, "email", "phone"]),
            "newsletter": random.choice([True, False])
        }]),
        "status": random.choice([None, fake.word(), random.randint(0, 100), random.choice([True, False])]),
        "created_at": fake.date_time_between(start_date="-1y", end_date="now").timestamp(),
        "class": random.choice(["A", "B", "C"])
    }

def get_serialized_records(records):
    result = []
    for i in records:
        output = io.BytesIO()
        fastavro.schemaless_writer(output, parsed_schema, i)
        result.append(output.getvalue())
    return result

def generate_records(num_records):
    return [generate_random_record() for _ in range(num_records)]


parsed_schema = fastavro.parse_schema(schema)
schema_string = json.dumps(schema)



def deserialize_fast_avro(records, schema):
    a = [fastavro.schemaless_reader(io.BytesIO(i), schema) for i in records]
    return a

def deserialize_pyruhvro(records, schema):
    return deserialize_array_threaded(records, schema, 8)

def serialize_pyruhvro(records, schema):
    return [serialize_record_batch(r, schema, 8) for r in records]

def serialize_fastavro(records, schema):
    result = []
    for i in records:
        output = io.BytesIO()
        fastavro.schemaless_writer(output, schema, i)
        result.append(output.getvalue())
    return result


if __name__ == "__main__":
    import time
    records = generate_records(20000)
    parsed_schmea = fastavro.parse_schema(schema)
    start = time.time()
    serialized_records = serialize_fastavro(records, parsed_schema)
    end = time.time()
    print(f"fastavro took {end - start} seconds to serialize {len(serialized_records)} records")

    # list_record_batches = deserialize_pyruhvro(serialized_records, schema_string)

    start = time.time()
    deserilized = deserialize_array_threaded(serialized_records, json.dumps(schema), 8)
    end = time.time()
    print(f"pyruhvro took {end - start} seconds to deserialize {len(serialized_records)} records")

    start = time.time()
    deserialized_fastavro = [fastavro.schemaless_reader(io.BytesIO(i), parsed_schema) for i in serialized_records]
    end = time.time()
    print(f"fastavro took {end - start} seconds to deserialize {len(serialized_records)} records")

    start = time.time()
    serialized = [serialize_record_batch(r, json.dumps(schema), 24) for r in deserilized]
    end = time.time()
    print(f"pyruhvro took {end - start} seconds to serialize {len(serialized_records)} records")
