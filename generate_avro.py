import json
import random
from faker import Faker
import datetime
import time
from avro.io import DatumWriter, BinaryEncoder
from avro.schema import parse
import io

# Initialize Faker
fake = Faker()

# Avro schema defined as a JSON string
avro_schema = """
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
    {
      "name": "fullName",
      "type": {
        "type": "record",
        "name": "FullName",
        "fields": [
          {"name": "firstName", "type": "string"},
          {"name": "lastName", "type": "string"}
        ]
      }
    },
    {
      "name": "email",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "phoneNumbers",
      "type": {
        "type": "array",
        "items": "string"
      }
    },
    {
      "name": "isPremiumMember",
      "type": "boolean"
    },
    {
      "name": "favoriteItems",
      "type": {
        "type": "map",
        "values": "int"
      }
    },
    {
      "name": "registrationDate",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    }
  ]
}
"""

# Parse the schema
# schema = parse(avro_schema)

def generate_fake_data():
    return {
        "userId": fake.uuid4(),
        "age": fake.random_int(min=18, max=100),
        "fullName": {
            "firstName": fake.first_name(),
            "lastName": fake.last_name()
        },
        "email": fake.email(),
        "phoneNumbers": [fake.phone_number() for _ in range(random.randint(1, 3))],
        "isPremiumMember": fake.boolean(),
        "favoriteItems": {fake.word(): fake.random_int(min=1, max=10) for _ in range(random.randint(2, 5))},
        "registrationDate": int(time.mktime(fake.date_time_between(start_date='-5y', end_date='now').timetuple()) * 1000)
    }

def serialize_record(record, schema):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(record, encoder)
#     print(bytes_writer)
    return bytes_writer.getvalue()

# Generate and serialize one record
# record = generate_fake_data()
# serialized_record = serialize_record(record, schema)
#
# # Serialized record is now in 'serialized_record' variable
# print(serialized_record)

schema = {
    "type": "record",
    "name": "User",
    "namespace": "com.example",
    "fields": [
        {
            "name": "firstName",
            "type": "string"
        },
        {
            "name": "lastName",
            "type": "string"
        },
        {
            "name": "age",
            "type": "int"
        },
        {
            "name": "addresses",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {
                            "name": "street",
                            "type": "string"
                        },
                        {
                            "name": "city",
                            "type": "string"
                        },
                        {
                            "name": "zipCode",
                            "type": "string"
                        }
                    ]
                }
            }
        },
        {
            "name": "email",
            "type": ["null", "string"],
            "default": "null"
        }
    ]
}

# Parse the schema
parsed_schema = parse(json.dumps(schema))

# Define some data to match our schema
records = [
    {
        "firstName": "John",
        "lastName": "Doe",
        "age": 30,
        "addresses": [
            {"street": "123 Elm St", "city": "Somewhere", "zipCode": "12345"},
            {"street": "456 Oak St", "city": "Anywhere", "zipCode": "67890"}
        ],
        "email": "john.doe@example.com"
    },
    {
        "firstName": "Jane",
        "lastName": "Doe",
        "age": 25,
        "addresses": [
            {"street": "789 Pine St", "city": "Nowhere", "zipCode": "24680"}
        ],
        "email": None  # This user does not have an email address
    }
]

r = serialize_record(records[0], parsed_schema)
# print(r.hex())


# Generate timestamp samples
schema = {
    "type": "record",
    "name": "TimestampTypes",
    "fields": [
        {"name": "date", "type": {"type": "int", "logicalType": "date"}},
    ]
}
# Generate some sample data
sample_data = {
    "date": datetime.date(2024, 4,1),
}

# parsed_schema = parse(json.dumps(schema))
# r = serialize_record(sample_data, parsed_schema)
# print(r.hex())

