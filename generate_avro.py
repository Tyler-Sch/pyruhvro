import fastavro
from fastavro.schema import load_schema
import io
from faker import Faker
import random

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
        {"name": "status", "type": ["null", "string", "int", "boolean"], "default": None}
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
        "status": random.choice([None, fake.word(), random.randint(0, 100), random.choice([True, False])])
    }

# Create 100,000 example data records
records = [generate_random_record() for _ in range(100000)]

parsed_schema = fastavro.parse_schema(schema)
# Serialize the records to Avro format
for i in records:
    output = io.BytesIO()
    fastavro.schemaless_writer(output, parsed_schema, i)


# Get the serialized data

# Print the size of the serialized data