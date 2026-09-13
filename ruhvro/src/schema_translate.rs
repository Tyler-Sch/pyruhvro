use anyhow::{anyhow, Result};
use apache_avro::schema::{
    Alias, DecimalSchema, EnumSchema, FixedSchema, Name, NamesRef, RecordSchema, ResolvedSchema,
};
use apache_avro::types::Value;
use apache_avro::Schema as AvroSchema;
use arrow::datatypes::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Kindly borrowed and slightly modified from data fusion
// #[derive(Debug)]
// pub struct ArrowAvro {
//     pub avro_schema: AvroSchema,
//     pub arrow_schema: Schema,
// }

/// Converts an avro schema to an arrow schema
pub fn to_arrow_schema(avro_schema: &AvroSchema) -> Result<Schema> {
    let resolved_schema = ResolvedSchema::try_from(avro_schema)?;
    let names = resolved_schema.get_names();
    let mut resolving = HashSet::new();
    let mut schema_fields = vec![];
    match avro_schema {
        AvroSchema::Record(RecordSchema { fields, .. }) => {
            for field in fields {
                schema_fields.push(schema_to_field_with_props(
                    &field.schema,
                    Some(&field.name),
                    false,
                    Some(external_props(&field.schema)),
                    names,
                    &mut resolving,
                )?)
            }
        }
        schema => schema_fields.push(schema_to_field_with_props(
            schema,
            Some(""),
            false,
            None,
            names,
            &mut resolving,
        )?),
    }

    let schema = Schema::new(schema_fields);
    Ok(schema)
}

fn schema_to_field_with_props(
    schema: &AvroSchema,
    name: Option<&str>,
    nullable: bool,
    props: Option<HashMap<String, String>>,
    names: &NamesRef<'_>,
    resolving: &mut HashSet<Name>,
) -> Result<Field> {
    let mut nullable = nullable;
    let field_type: DataType = match schema {
        AvroSchema::Ref {
            name: reference_name,
        } => {
            let resolved = names.get(reference_name).ok_or_else(|| {
                anyhow!(
                    "Avro schema reference '{}' could not be resolved",
                    reference_name.fullname(None)
                )
            })?;
            if !resolving.insert(reference_name.clone()) {
                return Err(anyhow!(
                    "Recursive Avro schema reference '{}' cannot be represented as an Arrow schema",
                    reference_name.fullname(None)
                ));
            }
            let field =
                schema_to_field_with_props(resolved, name, nullable, props, names, resolving);
            resolving.remove(reference_name);
            return field;
        }
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Array(item_schema) => DataType::List(Arc::new(schema_to_field_with_props(
            &item_schema.items,
            Some("item"),
            true,
            None,
            names,
            resolving,
        )?)),
        AvroSchema::Map(value_schema) => {
            let value_field = schema_to_field_with_props(
                &value_schema.types,
                Some("values"),
                false,
                None,
                names,
                resolving,
            )?;
            let key_field = Field::new("keys", DataType::Utf8, false);
            let map_field = Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![key_field, value_field])),
                nullable,
            ));
            DataType::Map(map_field, false)
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
                    schema_to_field_with_props(schema, None, has_nullable, None, names, resolving)?
                        .data_type()
                        .clone()
                } else {
                    return Err(anyhow!("Avro union contains duplicate null variants"));
                }
            } else {
                if has_nullable {
                    nullable = true;
                }
                let fields = sub_schemas
                    .iter()
                    .map(|schema| {
                        schema_to_field_with_props(schema, None, true, None, names, resolving)
                    })
                    .collect::<Result<Vec<Field>>>()?;
                let type_ids = 0_i8..fields.len() as i8;
                DataType::Union(UnionFields::new(type_ids, fields), UnionMode::Sparse)
            }
        }
        AvroSchema::Record(RecordSchema { fields, .. }) => {
            let fields: Result<_> = fields
                .iter()
                .map(|field| {
                    let mut props = HashMap::new();
                    if let Some(doc) = &field.doc {
                        props.insert("avro::doc".to_string(), doc.clone());
                    }
                    schema_to_field_with_props(
                        &field.schema,
                        Some(&field.name),
                        nullable,
                        Some(props),
                        names,
                        resolving,
                    )
                })
                .collect();
            DataType::Struct(fields?)
        }
        AvroSchema::Enum(EnumSchema {
            symbols: _,
            name: enum_name,
            ..
        }) => {
            let field_name = match name {
                Some(n) if !n.is_empty() => n.to_string(),
                _ => enum_name.fullname(None),
            };
            return Ok(Field::new(field_name, DataType::Utf8, nullable));
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
        _ => unimplemented!("data type missing default name"),
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
fn test_sample() {
    let schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {
                        "name": "mapppy_field",
                        "type": {
                            "type": "map",
                            "values": "int"
                        }
                    }
                ]
            }
        "#;
    let avro_schema = AvroSchema::parse_str(schema).unwrap();
    let arrow_schema = to_arrow_schema(&avro_schema).unwrap();
    println!("{:?}", arrow_schema);
}

#[test]
fn test_field_names_use_avro_field_name() {
    let schema = r#"
        {
            "type": "record",
            "name": "User",
            "namespace": "com.example",
            "fields": [
                {"name": "userid", "type": "string"},
                {"name": "address", "type": ["null", {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                        {"name": "street", "type": "string"},
                        {"name": "city", "type": "string"}
                    ]
                }], "default": null},
                {"name": "class", "type": {
                    "type": "enum",
                    "name": "ClassEnum",
                    "symbols": ["A", "B", "C"]
                }}
            ]
        }
    "#;
    let avro_schema = AvroSchema::parse_str(schema).unwrap();
    let arrow_schema = to_arrow_schema(&avro_schema).unwrap();
    let names: Vec<&str> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, vec!["userid", "address", "class"]);

    let address_field = arrow_schema.field_with_name("address").unwrap();
    if let DataType::Struct(nested) = address_field.data_type() {
        let nested_names: Vec<&str> = nested.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(nested_names, vec!["street", "city"]);
    } else {
        panic!("expected struct for address field");
    }
}

#[test]
fn test_named_record_references_are_expanded() {
    let schema = r#"
        {
            "type": "record",
            "name": "PositionValue",
            "namespace": "com.example.positions",
            "fields": [
                {
                    "name": "Current",
                    "type": {
                        "type": "record",
                        "name": "PositionLongShort",
                        "fields": [
                            {
                                "name": "Long",
                                "type": {
                                    "type": "record",
                                    "name": "PositionSide",
                                    "fields": [
                                        {"name": "Shares", "type": "string"}
                                    ]
                                }
                            },
                            {"name": "Short", "type": "PositionSide"}
                        ]
                    }
                },
                {"name": "Target", "type": "PositionLongShort"}
            ]
        }
    "#;

    let avro_schema = AvroSchema::parse_str(schema).unwrap();
    let arrow_schema = to_arrow_schema(&avro_schema).unwrap();

    let current = arrow_schema.field_with_name("Current").unwrap();
    let target = arrow_schema.field_with_name("Target").unwrap();
    assert_eq!(current.data_type(), target.data_type());

    let DataType::Struct(long_short_fields) = target.data_type() else {
        panic!("expected Target to be a struct");
    };
    assert_eq!(long_short_fields[0].name(), "Long");
    assert_eq!(long_short_fields[1].name(), "Short");
    assert_eq!(
        long_short_fields[0].data_type(),
        long_short_fields[1].data_type()
    );
}

#[test]
fn test_recursive_named_record_returns_an_error() {
    let schema = r#"
        {
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "Node"], "default": null}
            ]
        }
    "#;

    let avro_schema = AvroSchema::parse_str(schema).unwrap();
    let error = to_arrow_schema(&avro_schema).unwrap_err();

    assert!(error
        .to_string()
        .contains("Recursive Avro schema reference 'Node'"));
}
