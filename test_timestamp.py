import fastavro
from datetime import datetime, timezone
import json

# Define the schema with timestamp types
schema = {
    "type": "record",
    "name": "TimestampData",
    "namespace": "com.example",
    "fields": [
        {
            "name": "timestamp_millis",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "timestamp_micros",
            "type": {
                "type": "long",
                "logicalType": "timestamp-micros"
            }
        }
    ]
}

# Sample data matching the schema
# Using current time for simplicity
current_time = datetime.now(timezone.utc)
data = [
    {
        "timestamp_millis": int(current_time.timestamp() * 1000),  # Convert to milliseconds
        "timestamp_micros": int(current_time.timestamp() * 1000000),  # Convert to microseconds
    }
]

# Serialize sample data to Avro format
avro_file_name = "timestamp_data.avro"
with open(avro_file_name, "wb") as out:
    fastavro.writer(out, schema, data)

# Deserialize and print the data to verify
with open(avro_file_name, "rb") as f:
    avro_reader = fastavro.reader(f)
    for record in avro_reader:
        print(json.dumps(record, indent=2))