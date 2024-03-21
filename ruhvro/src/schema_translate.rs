use anyhow::Result;
use apache_avro::schema::{Alias, DecimalSchema, EnumSchema, FixedSchema, Name, RecordSchema};
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::datatypes::DataType::Struct;
use arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

#[derive(Debug)]
pub struct ArrowAvro {
    pub avro_schema: AvroSchema,
    pub arrow_schema: Schema,
}

pub fn read_avro_schmea_from_string(s: &str) -> Result<ArrowAvro> {
    let w = AvroSchema::parse_str(s)?;
    let arrow = to_arrow_schema(&w)?;
    Ok(ArrowAvro {
        avro_schema: w,
        arrow_schema: arrow,
    })
}
/// Converts an avro schema to an arrow schema
pub fn to_arrow_schema(avro_schema: &AvroSchema) -> Result<Schema> {
    let mut schema_fields = vec![];
    match avro_schema {
        AvroSchema::Record(RecordSchema { fields, .. }) => {
            for field in fields {
                schema_fields.push(schema_to_field_with_props(
                    &field.schema,
                    Some(&field.name),
                    false,
                    Some(external_props(&field.schema)),
                )?)
            }
        }
        schema => schema_fields.push(schema_to_field(schema, Some(""), false)?),
    }

    let schema = Schema::new(schema_fields);
    Ok(schema)
}

fn schema_to_field(schema: &AvroSchema, name: Option<&str>, nullable: bool) -> Result<Field> {
    schema_to_field_with_props(schema, name, nullable, Default::default())
}

fn schema_to_field_with_props(
    schema: &AvroSchema,
    name: Option<&str>,
    nullable: bool,
    props: Option<HashMap<String, String>>,
) -> Result<Field> {
    let mut nullable = nullable;
    let field_type: DataType = match schema {
        AvroSchema::Ref { .. } => todo!("Add support for AvroSchema::Ref"),
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Array(item_schema) => DataType::List(Arc::new(schema_to_field_with_props(
            item_schema,
            None,
            false,
            None,
        )?)),
        AvroSchema::Map(value_schema) => {
            let value_field = schema_to_field_with_props(value_schema, Some("value"), false, None)?;
            let key_field = Field::new("key", DataType::Utf8, false);
            let map_field = Arc::new(Field::new(
                name.unwrap_or("mapcol"),
                DataType::Struct(Fields::from(vec![key_field, value_field])),
                true,
            ));
            DataType::Map(map_field, true)
        }
        AvroSchema::Union(us) => {
            // If there are only two variants and one of them is null, set the other type as the field data type
            let has_nullable = us
                .find_schema_with_known_schemata::<apache_avro::Schema>(&Value::Null, None, &None)
                .is_some();
            let sub_schemas = us.variants();
            if has_nullable && sub_schemas.len() == 2 {
                nullable = true;
                if let Some(schema) = sub_schemas
                    .iter()
                    .find(|&schema| !matches!(schema, AvroSchema::Null))
                {
                    schema_to_field_with_props(schema, None, has_nullable, None)?
                        .data_type()
                        .clone()
                } else {
                    return Err(anyhow::Error::from(apache_avro::Error::GetUnionDuplicate));
                }
            } else {
                let fields = sub_schemas
                    .iter()
                    .map(|s| schema_to_field_with_props(s, None, has_nullable, None))
                    .collect::<Result<Vec<Field>>>()?;
                let type_ids = 0_i8..fields.len() as i8;
                DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Dense)
            }
        }
        AvroSchema::Record(RecordSchema { name, fields, .. }) => {
            let fields: Result<_> = fields
                .iter()
                .map(|field| {
                    let mut props = HashMap::new();
                    if let Some(doc) = &field.doc {
                        props.insert("avro::doc".to_string(), doc.clone());
                    }
                    /*if let Some(aliases) = fields.aliases {
                        props.insert("aliases", aliases);
                    }*/
                    schema_to_field_with_props(
                        &field.schema,
                        Some(&format!("{}.{}", name.fullname(None), field.name)),
                        false,
                        Some(props),
                    )
                })
                .collect();
            DataType::Struct(fields?)
        }
        AvroSchema::Enum(EnumSchema { symbols, name, .. }) => {
            return Ok(Field::new_dict(
                name.fullname(None),
                index_type(symbols.len()),
                false,
                0,
                false,
            ))
        }
        AvroSchema::Fixed(FixedSchema { size, .. }) => DataType::FixedSizeBinary(*size as i32),
        AvroSchema::Decimal(DecimalSchema {
            precision, scale, ..
        }) => DataType::Decimal128(*precision as u8, *scale as i8),
        AvroSchema::Uuid => DataType::FixedSizeBinary(16),
        AvroSchema::Date => DataType::Date32,
        AvroSchema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        AvroSchema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        AvroSchema::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        AvroSchema::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        AvroSchema::Duration => DataType::Duration(TimeUnit::Millisecond),
        _ => unimplemented!(),
    };

    let data_type = field_type.clone();
    let name = name.unwrap_or_else(|| default_field_name(&data_type));

    let mut field = Field::new(name, field_type, nullable);
    field.set_metadata(props.unwrap_or_default());
    Ok(field)
}

fn default_field_name(dt: &DataType) -> &str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "bit",
        DataType::Int8 => "tinyint",
        DataType::Int16 => "smallint",
        DataType::Int32 => "int",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "uint1",
        DataType::UInt16 => "uint2",
        DataType::UInt32 => "uint4",
        DataType::UInt64 => "uint8",
        DataType::Float16 => "float2",
        DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Date32 => "dateday",
        DataType::Date64 => "datemilli",
        DataType::Time32(tu) | DataType::Time64(tu) => match tu {
            TimeUnit::Second => "timesec",
            TimeUnit::Millisecond => "timemilli",
            TimeUnit::Microsecond => "timemicro",
            TimeUnit::Nanosecond => "timenano",
        },
        DataType::Timestamp(tu, tz) => {
            if tz.is_some() {
                match tu {
                    TimeUnit::Second => "timestampsectz",
                    TimeUnit::Millisecond => "timestampmillitz",
                    TimeUnit::Microsecond => "timestampmicrotz",
                    TimeUnit::Nanosecond => "timestampnanotz",
                }
            } else {
                match tu {
                    TimeUnit::Second => "timestampsec",
                    TimeUnit::Millisecond => "timestampmilli",
                    TimeUnit::Microsecond => "timestampmicro",
                    TimeUnit::Nanosecond => "timestampnano",
                }
            }
        }
        DataType::Duration(_) => "duration",
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => "intervalyear",
            IntervalUnit::DayTime => "intervalmonth",
            IntervalUnit::MonthDayNano => "intervalmonthdaynano",
        },
        DataType::Binary => "varbinary",
        DataType::FixedSizeBinary(_) => "fixedsizebinary",
        DataType::LargeBinary => "largevarbinary",
        DataType::Utf8 => "varchar",
        DataType::LargeUtf8 => "largevarchar",
        DataType::List(_) => "list",
        DataType::FixedSizeList(_, _) => "fixed_size_list",
        DataType::LargeList(_) => "largelist",
        DataType::Struct(_) => "struct",
        DataType::Union(_, _) => "union",
        DataType::Dictionary(_, _) => "map",
        DataType::Map(_, _) => unimplemented!("Map support not implemented"),
        DataType::RunEndEncoded(_, _) => {
            unimplemented!("RunEndEncoded support not implemented")
        }
        DataType::Decimal128(_, _) => "decimal",
        DataType::Decimal256(_, _) => "decimal",
    }
}

fn index_type(len: usize) -> DataType {
    if len <= usize::from(u8::MAX) {
        DataType::Int8
    } else if len <= usize::from(u16::MAX) {
        DataType::Int16
    } else if usize::try_from(u32::MAX).map(|i| len < i).unwrap_or(false) {
        DataType::Int32
    } else {
        DataType::Int64
    }
}

fn external_props(schema: &AvroSchema) -> HashMap<String, String> {
    let mut props = HashMap::new();
    match &schema {
        AvroSchema::Record(RecordSchema {
            doc: Some(ref doc), ..
        })
        | AvroSchema::Enum(EnumSchema {
            doc: Some(ref doc), ..
        })
        | AvroSchema::Fixed(FixedSchema {
            doc: Some(ref doc), ..
        }) => {
            props.insert("avro::doc".to_string(), doc.clone());
        }
        _ => {}
    }
    match &schema {
        AvroSchema::Record(RecordSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        })
        | AvroSchema::Enum(EnumSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        })
        | AvroSchema::Fixed(FixedSchema {
            name: Name { namespace, .. },
            aliases: Some(aliases),
            ..
        }) => {
            let aliases: Vec<String> = aliases
                .iter()
                .map(|alias| aliased(alias, namespace.as_deref(), None))
                .collect();
            props.insert(
                "avro::aliases".to_string(),
                format!("[{}]", aliases.join(",")),
            );
        }
        _ => {}
    }
    props
}

/// Returns the fully qualified name for a field
pub fn aliased(alias: &Alias, namespace: Option<&str>, default_namespace: Option<&str>) -> String {
    if alias.namespace().is_some() {
        alias.fullname(None)
    } else {
        let namespace = namespace.as_ref().copied().or(default_namespace);

        match namespace {
            Some(ref namespace) => format!("{}.{}", namespace, alias.name()),
            None => alias.fullname(None),
        }
    }
}

#[test]
fn test_avro_to_arrow() {
    // Example Avro schema
    let avro_schema_str = r#"
        {
            "type": "record",
            "name": "example_schema",
            "fields": [
                {"name": "field1", "type": "int"},
                {"name": "field2", "type": "string"},
                {"name": "field3", "type": ["null", "long"]}
            ]
        }
    "#;
    let arrow_schema = read_avro_schmea_from_string(&avro_schema_str).unwrap();

    println!("{:#?}", arrow_schema.arrow_schema);
}

#[test]
fn test_avro_to_arrow_complex() {
    // Example Avro schema
    let avro_schema_str = r#"
    {
        "type": "record",
        "name": "Person",
        "fields": [
            {
                "name": "name",
                "type": "string"
            },
            {
                "name": "age",
                "type": "int"
            },
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
                        "name": "country",
                        "type": "string"
                    }
                ]
            },
            {
                "name": "enumcol",
                "type": "enum",
                "symbols": ["a", "b", "c"]
            },
            {
                "name": "mapcol",
                "type": "map",
                "values": "string",
                "default": {}
            },
            {
                "name": "unioncol",
                "type": ["null", "string"]
            }

        ]
    }


        "#;

    //     let avro_schema_str = r#"
    // {
    //     "type": "record",
    //     "name": "Employee",
    //     "fields": [
    //         {
    //             "name": "id",
    //             "type": "int"
    //         },
    //         {
    //             "name": "name",
    //             "type": "string"
    //         },
    //         {
    //             "name": "age",
    //             "type": "int"
    //         },
    //         {
    //             "name": "department",
    //             "type": "string"
    //         }
    //     ]
    // }
    //     "#;
    let arrow_schema = read_avro_schmea_from_string(&avro_schema_str).unwrap();
    for f in &arrow_schema.arrow_schema.fields {
        println!("{:?}", f);
    }

    // println!("{:#?}", arrow_schema.arrow_schema);
}
