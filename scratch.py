from pyruhvro import deserialize_datum
import time

start_time = time.time()
# with open("../ruhvro/test_20k.kafka", "r") as f:
#     data = [bytes.fromhex(i.strip()) for i in f.readlines()]
with open("../ruhvro/test_1mil.kafka", "r") as f:
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

# deserialize_datum([data[0]], schema)

result = deserialize_datum(data, schema)
end_time = time.time()
print(end_time - start_time)
