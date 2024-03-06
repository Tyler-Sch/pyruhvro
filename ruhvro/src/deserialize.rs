use anyhow::Result;
use apache_avro::from_avro_datum;
use apache_avro::types::Value;
use apache_avro::Schema;
use rayon::prelude::*;

pub fn parse_schema(schema_string: &str) -> Result<Schema> {
    Ok(Schema::parse_str(schema_string)?)
}

// pub fn per_datum_deserialize(data: &Vec<Vec<u8>>, schema: &Schema) -> Vec<Result<Value>> {
//     data.into_iter()
//         .map(|datum| {
//             let mut sliced = &datum[..];
//             Ok(from_avro_datum(schema, &mut sliced, None)?)
//         })
//         .collect::<Vec<_>>()
// }

pub fn per_datum_deserialize(data: &Vec<Vec<u8>>, schema: &Schema) -> Box<Vec<Result<Value>>> {
    Box::new(data.into_iter()
        .map(|datum| {
            let mut sliced = &datum[..];
            Ok(from_avro_datum(schema, &mut sliced, None)?)
        })
        .collect::<Vec<_>>())
}
// pub fn per_datum_deserialize(data: &Vec<&Vec<u8>>, schema: &Schema) -> Vec<Result<Value>> {
//     data.par_iter()
//         .map(|datum| {
//             let mut sliced = &datum[..];
//             Ok(from_avro_datum(schema, &mut sliced, None)?)
//         })
//         .collect::<Vec<_>>()
// }

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use apache_avro::to_value;

    use super::*;
    use crate::utils;

    #[test]
    fn test_per_datum_deserialize() {
        let s = r#"{
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
          }"#;
          let parsed_schema = parse_schema(s).unwrap();
          let avro_datum = "4834346437643065662d613264662d343833652d393261312d313532333830366164656334380a4c696e64610857617265022c6c696e646173636f7474406578616d706c652e6e6574062628323636293734302d31323737783031313432283030312d3935392d3839342d36353030783739392a3030312d3339362d3831392d363830307830303139000006044d72100866696e640e10617070726f6163680c00c0f691c7c35f";
          let encoded = utils::decode_hex(avro_datum);
          let decoded = per_datum_deserialize(&vec![encoded], &parsed_schema);
          
          let expected = Value::Record(vec![
            ("userId".into(), Value::String("44d7d0ef-a2df-483e-92a1-1523806adec4".into())),
            ("age".into(), Value::Int(28)),
            ("fullName".into(), Value::Record(vec![
                ("firstName".into(), Value::String("Linda".into())),
                ("lastName".into(), Value::String("Ware".into())),
            ])),
            ("email".into(), Value::Union(1, Box::new(Value::String("lindascott@example.net".into())))),
            ("phoneNumbers".into(), Value::Array(vec![
                Value::String("(266)740-1277x01142".into()),
                Value::String("001-959-894-6500x799".into()), 
                Value::String("001-396-819-6800x0019".into()),
                ])),
            ("isPremiumMember".into(), Value::Boolean(false)), 
            ("favoriteItems".into(), Value::Map(HashMap::from([
                ("Mr".into(), Value::Int(8)),
                ("find".into(), Value::Int(7)),
                ("approach".into(), Value::Int(6)),
            ]))),
            ("registrationDate".into(), Value::TimestampMillis(1641154756000i64))
          ]);
          assert!(&expected == decoded.get(0).unwrap().as_ref().unwrap());
    }
  }