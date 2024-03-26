from pyruhvro import  deserialize_datum_from_arrow, deserialize_datum
import time

start_time = time.time()
# with open("../ruhvro/test_20k.kafka", "r") as f:
#     data = [bytes.fromhex(i.strip()) for i in f.readlines()]
with open("/Users/tylerschauer/rust/ruhvro2/ruhvro/test_1mil.kafka", "r") as f:
    data = [bytes.fromhex(i.strip()) for i in f.readlines()]
    # data = [i for i in f.readlines()]

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

# print(deserialize_datum([data[0]], schema))
################################################################################
# import fastavro
# import json
# from io import BytesIO
# schema = fastavro.parse_schema(json.loads(schema))
# result = [fastavro.schemaless_reader(BytesIO(i), schema) for i in data]
################################################################################
import pyarrow as pa
arr = pa.array(data, pa.binary())
result = deserialize_datum_from_arrow(arr, schema)
# result = from_arrow(arr)
################################################################################
# result = deserialize_datum(data, schema)
# print(result)
end_time = time.time()
print(end_time - start_time)

