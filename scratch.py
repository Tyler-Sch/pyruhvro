from pyruhvro import deserialize_arrow, deserialize_arrow_threaded
import time

# start_time = time.time()
# with open("/Users/tylerschauer/rust/ruhvro2/ruhvro/test_20k.kafka", "r") as f:
#     data = [bytes.fromhex(i.strip()) for i in f.readlines()]
# with open("/Users/tylerschauer/rust/ruhvro2/ruhvro/test_1mil.kafka", "r") as f:
#     data = [bytes.fromhex(i.strip()) for i in f.readlines()]
#     data = [i for i in f.readlines()]
# with open("/home/goser/rust/ruhvro2/test_20k.kafka", "r") as f:
#     data = [bytes.fromhex(i.strip()) for i in f.readlines()]
with open("/home/goser/rust/ruhvro2/test_1mil.kafka", "r") as f:
    data = [bytes.fromhex(i.strip()) for i in f.readlines()]

schema = """{
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
}"""

# data[0]
start_time = time.time()
# print(deserialize_datum([data[0]], schema))
################################################################################
# import fastavro
# import json
# from io import BytesIO
# schema = fastavro.parse_schema(json.loads(schema))
# result = [fastavro.schemaless_reader(BytesIO(i), schema) for i in data]
################################################################################
# deserialize with threaded rust
# result = deserialize_datum(data, schema)

################################################################################
# import pyarrow as pa
# arr = pa.array(data, pa.binary())
# result = deserialize_datum_from_arrow(arr, schema)
# result = from_arrow(arr)
################################################################################
# import pyarrow as pa
# arr = pa.array(data, pa.binary())
# result = deserialize_arrow(arr, schema)
################################################################################
import pyarrow as pa
arr = pa.array(data, pa.binary())
result = deserialize_arrow_threaded(arr, schema, 8)
################################################################################
# result = deserialize_datum(data, schema)
# print(result)
end_time = time.time()
print(end_time - start_time)

