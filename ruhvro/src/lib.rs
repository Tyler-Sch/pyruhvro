#![warn(clippy::pedantic)]
//! Crate to deserialize avro and return arrow.
//!
//! This library was made to enable quick efficient transformation of schemaless encoded avro
//! data into arrow record batches.
//!

mod complex;
/// Converts Avro to Arrow
/// Decode Avro data returning an Arrow Record Batch.
/// ## Example
/// ```
/// let raw_schema = r#"
///             {
///                 "type": "record",
///                 "name": "test",
///                 "fields": [
///                     {"name": "a", "type": "long", "default": 42},
///                     {"name": "b",
///                         "type": ["null", "string"],
///                         "default": null
///             }
///                 ]
///             }
///         "#;
///
/// let parsed_schema = ruhvro::deserialize::parse_schema(&raw_schema).unwrap();
/// // create a record
/// let mut record = apache_avro::types::Record::new(&parsed_schema).unwrap();
/// record.put("a", 27i64);
/// record.put("b", None::<String>);
/// // and serialize it
/// let serialized = apache_avro::to_avro_datum(&parsed_schema, record).unwrap();
/// // verify deserialization
/// let result = ruhvro::deserialize::per_datum_deserialize(&vec![&serialized[..]], &parsed_schema);
/// println!("{:?}", result);
///```
pub mod deserialize;
mod schema_translate;
mod serialization_containers;
pub mod serialize;

#[cfg(test)]
mod tests {
    use crate::deserialize::per_datum_deserialize;

    #[test]
    fn test_round_trip() {
        let schema = r#"
        {
            "type": "record",
            "name": "User",
            "fields": [
                {
                    "name": "name",
                    "type": [
                        "null",
                        "string"
                    ],
                    "default": null
                },
                {
                    "name": "age",
                    "type": [
                        "null",
                        "int"
                    ],
                    "default": null
                },
                {
                    "name": "emails",
                    "type": {
                        "type": "array",
                        "items": "string"
                    }
                },
                {
                    "name": "address",
                    "type": [
                        "null",
                        {
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
                                    "name": "zipcode",
                                    "type": "string"
                                }
                            ]
                        }
                    ],
                    "default": null
                },
                {
                    "name": "phone_numbers",
                    "type": {
                        "type": "map",
                        "values": "string"
                    }
                },
                {
                    "name": "preferences",
                    "type": [
                        "null",
                        {
                            "type": "record",
                            "name": "Preferences",
                            "fields": [
                                {
                                    "name": "contact_method",
                                    "type": [
                                        "null",
                                        "string"
                                    ],
                                    "default": null
                                },
                                {
                                    "name": "newsletter",
                                    "type": "boolean"
                                }
                            ]
                        }
                    ],
                    "default": null
                },
                {
                    "name": "status",
                    "type": [
                        "string",
                        "int",
                        "boolean"
                    ]
                }
            ]
        }

        "#;

        let parsed_schema = crate::deserialize::parse_schema(&schema).unwrap();
        let avro_datum = "0000062e74686f6d61736b6172656e406578616d706c652e6e657422616c6f7765406578616d706c652e6f72672664617669643738406578616d706c652e636f6d0000060a636865636b203030312d3233372d3438302d353133341065766964656e6365262b312d3732352d3336362d39323133783730300a6d616a6f722428393734293537302d3032313178333534350002020a656d61696c00020a7374616666";
        let avro_datum2 = "0218416d616e646120456c6c6973023804246e6361736579406578616d706c652e636f6d307374657761727474796c6572406578616d706c652e6e6574000230393532323120436861726c657320547261666669637761791c5a616368617279626f726f7567680a303433343300000202";
        let avro_datum3 = "021a417564726579204261726e65730000000408726963682628373136293338322d363937327837383437320e746f6e69676874243536302e3736352e3230363378373831313500020000000866726565";
        let encoded = decode_hex(avro_datum);
        let encoded2 = decode_hex(avro_datum2);
        let encoded3 = decode_hex(avro_datum3);
        let newv = vec![&encoded[..], &encoded2[..], &encoded3[..]];
        let result = per_datum_deserialize(&newv, &parsed_schema);

        let serialized = crate::serialize::serialize_record_batch(result, &parsed_schema, 1);

    }

    pub fn decode_hex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("Error unwrapping hex"))
            .collect::<Vec<_>>()
    }
}
