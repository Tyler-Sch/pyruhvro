//! Fast-path Arrow → Avro encoder. Symmetric to [`crate::fast_decode`].
//!
//! Walks Arrow arrays + Avro schema in lockstep and writes wire bytes directly
//! into a per-row buffer, skipping `apache_avro::types::Value` construction.
//!
//! Same supported subset as the decoder: primitives, logical types, enum,
//! nested record, 2-variant null unions, N-variant sparse unions.

use crate::fast_decode;
use anyhow::{anyhow, bail, Result};
use apache_avro::schema::{EnumSchema, RecordSchema, UnionSchema};
use apache_avro::Schema as AvroSchema;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, GenericBinaryArray,
    GenericBinaryBuilder, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UnionArray,
};
use std::collections::HashMap;

/// Same subset as [`fast_decode::is_supported`] — schemas must match shapes on
/// both directions.
pub fn is_supported(schema: &AvroSchema) -> bool {
    fast_decode::is_supported(schema)
}

/// Per-chunk fast serialize. Caller must have verified [`is_supported`].
pub fn serialize_chunk(
    schema: &AvroSchema,
    struct_arry: &ArrayRef,
) -> Result<GenericBinaryArray<i32>> {
    let sa = struct_arry
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| anyhow!("fast_encode: expected StructArray"))?;
    let rs = match schema {
        AvroSchema::Record(rs) => rs,
        _ => bail!("fast_encode: top-level schema must be a Record"),
    };
    let encoder = build_record_encoder(rs, sa)?;
    let n = sa.len();

    // Rough capacity hint: assume ~32 bytes per record on average. Both
    // buffers grow as needed; the hint just amortizes early reallocs.
    let mut builder = GenericBinaryBuilder::<i32>::with_capacity(n, n * 32);
    let mut row_buf: Vec<u8> = Vec::with_capacity(64);

    for i in 0..n {
        row_buf.clear();
        encoder.write_record(&mut row_buf, i)?;
        builder.append_value(&row_buf);
    }
    Ok(builder.finish())
}

// ============================================================================
// Encoder tree
// ============================================================================

enum FieldEncoder<'a> {
    // ---- primitives ----
    Int(&'a Int32Array),
    Long(&'a Int64Array),
    Float(&'a Float32Array),
    Double(&'a Float64Array),
    Bool(&'a BooleanArray),
    String(&'a StringArray),

    // ---- logical types (same wire as int/long) ----
    Date(&'a Date32Array),
    TimestampMillis(&'a TimestampMillisecondArray),
    TimestampMicros(&'a TimestampMicrosecondArray),

    // ---- enum: arrow holds the symbol name; encode as zigzag index ----
    Enum {
        array: &'a StringArray,
        symbol_to_idx: HashMap<&'a str, i64>,
    },

    // ---- 2-variant null unions over the inline types ----
    NullableInt { array: &'a Int32Array, null_first: bool },
    NullableLong { array: &'a Int64Array, null_first: bool },
    NullableFloat { array: &'a Float32Array, null_first: bool },
    NullableDouble { array: &'a Float64Array, null_first: bool },
    NullableBool { array: &'a BooleanArray, null_first: bool },
    NullableString { array: &'a StringArray, null_first: bool },
    NullableDate { array: &'a Date32Array, null_first: bool },
    NullableTimestampMillis {
        array: &'a TimestampMillisecondArray,
        null_first: bool,
    },
    NullableTimestampMicros {
        array: &'a TimestampMicrosecondArray,
        null_first: bool,
    },
    NullableEnum {
        array: &'a StringArray,
        symbol_to_idx: HashMap<&'a str, i64>,
        null_first: bool,
    },

    // ---- Null variant inside a union (no bytes written) ----
    Null,

    // ---- nested record ----
    Record(Box<RecordEncoder<'a>>),
    NullableRecord {
        encoder: Box<RecordEncoder<'a>>,
        null_first: bool,
    },

    // ---- N-variant sparse union ----
    Union(Box<UnionEncoder<'a>>),

    // ---- array<T> / nullable array<T> ----
    List(Box<ListEncoder<'a>>),
    NullableList { encoder: Box<ListEncoder<'a>>, null_first: bool },

    // ---- map<string, T> / nullable map<string, T> ----
    Map(Box<MapEncoder<'a>>),
    NullableMap { encoder: Box<MapEncoder<'a>>, null_first: bool },
}

struct RecordEncoder<'a> {
    children: Vec<FieldEncoder<'a>>,
    /// `Some` for nested-nullable records — the StructArray's own null buffer.
    /// `None` for top-level (or non-nullable nested) records.
    nulls: Option<&'a arrow::buffer::NullBuffer>,
}

struct UnionEncoder<'a> {
    /// Per-row Arrow type id; for sparse unions this also points into `children`.
    type_ids: &'a [i8],
    children: Vec<FieldEncoder<'a>>,
}

struct ListEncoder<'a> {
    array: &'a ListArray,
    inner: FieldEncoder<'a>,
}

struct MapEncoder<'a> {
    array: &'a MapArray,
    /// Keys are always strings in Avro (and in the Arrow representation
    /// produced by schema_translate).
    keys: &'a StringArray,
    values: FieldEncoder<'a>,
}

// ============================================================================
// Construction
// ============================================================================

fn build_record_encoder<'a>(
    rs: &'a RecordSchema,
    sa: &'a StructArray,
) -> Result<RecordEncoder<'a>> {
    // Match arrow columns to avro fields by NAME, not position — same behavior
    // as serialization_containers::StructArrayContainer::try_new. This makes
    // reordered RecordBatches work and produces a clean error for missing
    // columns.
    let arrow_name_to_idx: HashMap<&str, usize> = sa
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();
    let mut children = Vec::with_capacity(rs.fields.len());
    for field in rs.fields.iter() {
        let arrow_idx = arrow_name_to_idx
            .get(field.name.as_str())
            .ok_or_else(|| {
                let available: Vec<&str> =
                    sa.fields().iter().map(|f| f.name().as_str()).collect();
                anyhow!(
                    "Arrow struct missing column '{}' required by Avro schema. Available columns: {:?}",
                    field.name,
                    available
                )
            })?;
        children.push(build_field_encoder(&field.schema, sa.column(*arrow_idx))?);
    }
    // Top-level RecordEncoder doesn't carry its own null buffer; nullability
    // is handled by the parent NullableRecord wrapper (which reads the struct
    // array's nulls).
    Ok(RecordEncoder {
        children,
        nulls: None,
    })
}

fn build_field_encoder<'a>(
    schema: &'a AvroSchema,
    array: &'a ArrayRef,
) -> Result<FieldEncoder<'a>> {
    Ok(match schema {
        AvroSchema::Int => FieldEncoder::Int(downcast(array)?),
        AvroSchema::Long => FieldEncoder::Long(downcast(array)?),
        AvroSchema::Float => FieldEncoder::Float(downcast(array)?),
        AvroSchema::Double => FieldEncoder::Double(downcast(array)?),
        AvroSchema::Boolean => FieldEncoder::Bool(downcast(array)?),
        AvroSchema::String => FieldEncoder::String(downcast(array)?),
        AvroSchema::Date => FieldEncoder::Date(downcast(array)?),
        AvroSchema::TimestampMillis => FieldEncoder::TimestampMillis(downcast(array)?),
        AvroSchema::TimestampMicros => FieldEncoder::TimestampMicros(downcast(array)?),
        AvroSchema::Enum(EnumSchema { symbols, .. }) => FieldEncoder::Enum {
            array: downcast(array)?,
            symbol_to_idx: build_symbol_map(symbols),
        },
        AvroSchema::Null => FieldEncoder::Null,
        AvroSchema::Record(rs) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("fast_encode: expected StructArray for record"))?;
            let mut inner = build_record_encoder(rs, sa)?;
            // Non-null record nested directly (not via null union): no parent
            // null tracking needed.
            inner.nulls = None;
            FieldEncoder::Record(Box::new(inner))
        }
        AvroSchema::Union(u) => build_union_encoder(u, array)?,
        AvroSchema::Array(a) => FieldEncoder::List(Box::new(build_list_encoder(&a.items, array)?)),
        AvroSchema::Map(m) => FieldEncoder::Map(Box::new(build_map_encoder(&m.types, array)?)),
        other => bail!("fast_encode: unsupported schema: {other:?}"),
    })
}

fn build_list_encoder<'a>(
    item_schema: &'a AvroSchema,
    array: &'a ArrayRef,
) -> Result<ListEncoder<'a>> {
    let la = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("fast_encode: expected ListArray for array schema"))?;
    let inner = build_field_encoder(item_schema, la.values())?;
    Ok(ListEncoder { array: la, inner })
}

fn build_map_encoder<'a>(
    value_schema: &'a AvroSchema,
    array: &'a ArrayRef,
) -> Result<MapEncoder<'a>> {
    let ma = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| anyhow!("fast_encode: expected MapArray for map schema"))?;
    let keys = ma
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("fast_encode: map keys must be StringArray"))?;
    // ma.values() returns &ArrayRef.
    let values = build_field_encoder(value_schema, ma.values())?;
    Ok(MapEncoder { array: ma, keys, values })
}

fn build_union_encoder<'a>(
    u: &'a UnionSchema,
    array: &'a ArrayRef,
) -> Result<FieldEncoder<'a>> {
    // 2-variant null union becomes a nullable inner-type encoder.
    if let Some((inner, null_first)) = split_null_union(u) {
        return build_nullable_encoder(inner, array, null_first);
    }
    // N-variant sparse union: Arrow side is a UnionArray.
    let ua = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| anyhow!("fast_encode: expected UnionArray for multi-variant union"))?;
    let type_ids: &[i8] = ua.type_ids();
    let variants = u.variants();
    let mut children = Vec::with_capacity(variants.len());
    for (i, variant) in variants.iter().enumerate() {
        // schema_translate emits type_ids 0..N in order; we rely on that here.
        let child_arr = ua.child(i as i8);
        children.push(build_field_encoder(variant, child_arr)?);
    }
    Ok(FieldEncoder::Union(Box::new(UnionEncoder {
        type_ids,
        children,
    })))
}

fn build_nullable_encoder<'a>(
    inner: &'a AvroSchema,
    array: &'a ArrayRef,
    null_first: bool,
) -> Result<FieldEncoder<'a>> {
    Ok(match inner {
        AvroSchema::Int => FieldEncoder::NullableInt {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::Long => FieldEncoder::NullableLong {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::Float => FieldEncoder::NullableFloat {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::Double => FieldEncoder::NullableDouble {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::Boolean => FieldEncoder::NullableBool {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::String => FieldEncoder::NullableString {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::Date => FieldEncoder::NullableDate {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::TimestampMillis => FieldEncoder::NullableTimestampMillis {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::TimestampMicros => FieldEncoder::NullableTimestampMicros {
            array: downcast(array)?,
            null_first,
        },
        AvroSchema::Enum(EnumSchema { symbols, .. }) => FieldEncoder::NullableEnum {
            array: downcast(array)?,
            symbol_to_idx: build_symbol_map(symbols),
            null_first,
        },
        AvroSchema::Record(rs) => {
            let sa = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("fast_encode: expected StructArray for record"))?;
            let mut inner_enc = build_record_encoder(rs, sa)?;
            inner_enc.nulls = sa.nulls();
            FieldEncoder::NullableRecord {
                encoder: Box::new(inner_enc),
                null_first,
            }
        }
        AvroSchema::Array(a) => FieldEncoder::NullableList {
            encoder: Box::new(build_list_encoder(&a.items, array)?),
            null_first,
        },
        AvroSchema::Map(m) => FieldEncoder::NullableMap {
            encoder: Box::new(build_map_encoder(&m.types, array)?),
            null_first,
        },
        other => bail!("fast_encode: unsupported nullable inner type: {other:?}"),
    })
}

fn build_symbol_map(symbols: &[String]) -> HashMap<&str, i64> {
    symbols
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i as i64))
        .collect()
}

fn downcast<'a, T: Array + 'static>(array: &'a ArrayRef) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("fast_encode: arrow array downcast failed"))
}

fn split_null_union(u: &UnionSchema) -> Option<(&AvroSchema, bool)> {
    let variants = u.variants();
    if variants.len() != 2 {
        return None;
    }
    match (&variants[0], &variants[1]) {
        (AvroSchema::Null, other) => Some((other, true)),
        (other, AvroSchema::Null) => Some((other, false)),
        _ => None,
    }
}

// ============================================================================
// Per-row encoding
// ============================================================================

impl<'a> RecordEncoder<'a> {
    #[inline]
    fn write_record(&self, out: &mut Vec<u8>, row: usize) -> Result<()> {
        for child in &self.children {
            child.write(out, row)?;
        }
        Ok(())
    }
}

impl<'a> FieldEncoder<'a> {
    #[inline]
    fn write(&self, out: &mut Vec<u8>, row: usize) -> Result<()> {
        match self {
            Self::Int(a) => write_zigzag_long(out, a.value(row) as i64),
            Self::Long(a) => write_zigzag_long(out, a.value(row)),
            Self::Float(a) => out.extend_from_slice(&a.value(row).to_le_bytes()),
            Self::Double(a) => out.extend_from_slice(&a.value(row).to_le_bytes()),
            Self::Bool(a) => out.push(if a.value(row) { 1 } else { 0 }),
            Self::String(a) => write_string(out, a.value(row)),
            Self::Date(a) => write_zigzag_long(out, a.value(row) as i64),
            Self::TimestampMillis(a) => write_zigzag_long(out, a.value(row)),
            Self::TimestampMicros(a) => write_zigzag_long(out, a.value(row)),
            Self::Enum { array, symbol_to_idx } => {
                write_enum_idx(out, array.value(row), symbol_to_idx)?;
            }
            Self::Null => {}
            Self::NullableInt { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    write_zigzag_long(o, array.value(row) as i64)
                });
            }
            Self::NullableLong { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    write_zigzag_long(o, array.value(row))
                });
            }
            Self::NullableFloat { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    o.extend_from_slice(&array.value(row).to_le_bytes())
                });
            }
            Self::NullableDouble { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    o.extend_from_slice(&array.value(row).to_le_bytes())
                });
            }
            Self::NullableBool { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    o.push(if array.value(row) { 1 } else { 0 })
                });
            }
            Self::NullableString { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    write_string(o, array.value(row))
                });
            }
            Self::NullableDate { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    write_zigzag_long(o, array.value(row) as i64)
                });
            }
            Self::NullableTimestampMillis { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    write_zigzag_long(o, array.value(row))
                });
            }
            Self::NullableTimestampMicros { array, null_first } => {
                write_nullable(out, array.is_null(row), *null_first, |o| {
                    write_zigzag_long(o, array.value(row))
                });
            }
            Self::NullableEnum { array, symbol_to_idx, null_first } => {
                if array.is_null(row) {
                    write_zigzag_long(out, if *null_first { 0 } else { 1 });
                } else {
                    write_zigzag_long(out, if *null_first { 1 } else { 0 });
                    write_enum_idx(out, array.value(row), symbol_to_idx)?;
                }
            }
            Self::Record(r) => r.write_record(out, row)?,
            Self::NullableRecord { encoder, null_first } => {
                let is_null = encoder
                    .nulls
                    .map(|nb| nb.is_null(row))
                    .unwrap_or(false);
                if is_null {
                    write_zigzag_long(out, if *null_first { 0 } else { 1 });
                } else {
                    write_zigzag_long(out, if *null_first { 1 } else { 0 });
                    encoder.write_record(out, row)?;
                }
            }
            Self::Union(u) => u.write(out, row)?,
            Self::List(l) => l.write(out, row)?,
            Self::NullableList { encoder, null_first } => {
                if encoder.array.is_null(row) {
                    write_zigzag_long(out, if *null_first { 0 } else { 1 });
                } else {
                    write_zigzag_long(out, if *null_first { 1 } else { 0 });
                    encoder.write(out, row)?;
                }
            }
            Self::Map(m) => m.write(out, row)?,
            Self::NullableMap { encoder, null_first } => {
                if encoder.array.is_null(row) {
                    write_zigzag_long(out, if *null_first { 0 } else { 1 });
                } else {
                    write_zigzag_long(out, if *null_first { 1 } else { 0 });
                    encoder.write(out, row)?;
                }
            }
        }
        Ok(())
    }
}

impl<'a> UnionEncoder<'a> {
    fn write(&self, out: &mut Vec<u8>, row: usize) -> Result<()> {
        let type_id = self.type_ids[row];
        if type_id < 0 || (type_id as usize) >= self.children.len() {
            bail!("fast_encode: union type_id {type_id} out of range");
        }
        write_zigzag_long(out, type_id as i64);
        // For sparse unions, all children share the same row index as the
        // outer array.
        self.children[type_id as usize].write(out, row)?;
        Ok(())
    }
}

impl<'a> ListEncoder<'a> {
    #[inline]
    fn write(&self, out: &mut Vec<u8>, row: usize) -> Result<()> {
        let offsets = self.array.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let n = end - start;
        if n > 0 {
            write_zigzag_long(out, n as i64);
            for i in start..end {
                self.inner.write(out, i)?;
            }
        }
        // 0 terminator (always; an empty list emits just 0).
        write_zigzag_long(out, 0);
        Ok(())
    }
}

impl<'a> MapEncoder<'a> {
    #[inline]
    fn write(&self, out: &mut Vec<u8>, row: usize) -> Result<()> {
        let offsets = self.array.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let n = end - start;
        if n > 0 {
            write_zigzag_long(out, n as i64);
            for i in start..end {
                write_string(out, self.keys.value(i));
                self.values.write(out, i)?;
            }
        }
        write_zigzag_long(out, 0);
        Ok(())
    }
}

#[inline]
fn write_nullable(
    out: &mut Vec<u8>,
    is_null: bool,
    null_first: bool,
    write_value: impl FnOnce(&mut Vec<u8>),
) {
    if is_null {
        write_zigzag_long(out, if null_first { 0 } else { 1 });
    } else {
        write_zigzag_long(out, if null_first { 1 } else { 0 });
        write_value(out);
    }
}

#[inline]
fn write_enum_idx(out: &mut Vec<u8>, sym: &str, map: &HashMap<&str, i64>) -> Result<()> {
    let idx = map
        .get(sym)
        .copied()
        .ok_or_else(|| anyhow!("fast_encode: enum symbol '{sym}' not in schema"))?;
    write_zigzag_long(out, idx);
    Ok(())
}

// ============================================================================
// Wire-format byte writers
// ============================================================================

#[inline]
fn write_zigzag_long(out: &mut Vec<u8>, v: i64) {
    let mut zz: u64 = ((v << 1) ^ (v >> 63)) as u64;
    while (zz & !0x7F) != 0 {
        out.push(((zz & 0x7F) as u8) | 0x80);
        zz >>= 7;
    }
    out.push(zz as u8);
}

#[inline]
fn write_string(out: &mut Vec<u8>, s: &str) {
    write_zigzag_long(out, s.len() as i64);
    out.extend_from_slice(s.as_bytes());
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialize::per_datum_deserialize_baseline;
    use crate::serialize::serialize_record_batch;
    use apache_avro::to_avro_datum;
    use apache_avro::types::{Record, Value};
    use arrow::array::RecordBatch;

    /// End-to-end round trip via the fast path: avro bytes → fast decode →
    /// fast encode → bytes — assert decoding the result matches the original.
    fn assert_round_trip(schema_str: &str, n: usize, build: impl Fn(usize, &AvroSchema) -> Value) {
        let s = AvroSchema::parse_str(schema_str).unwrap();
        assert!(is_supported(&s), "schema not in fast-path subset");

        let owned: Vec<Vec<u8>> = (0..n)
            .map(|i| to_avro_datum(&s, build(i, &s)).unwrap())
            .collect();
        let refs: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();

        // Decode via fast path, then serialize via fast path.
        let rb: RecordBatch = crate::fast_decode::decode(&refs, &s).unwrap();
        let bytes_chunks = serialize_record_batch(rb.clone(), &s, 1).unwrap();
        assert_eq!(bytes_chunks.len(), 1);
        let chunk = &bytes_chunks[0];

        // Decode the re-serialized bytes via the BASELINE path and assert it
        // matches the original RecordBatch — that proves the encoder is
        // wire-compatible with apache-avro.
        let round_refs: Vec<&[u8]> = (0..chunk.len()).map(|i| chunk.value(i)).collect();
        let round = per_datum_deserialize_baseline(&round_refs.iter().copied().collect(), &s).unwrap();
        assert_eq!(rb, round, "fast-encode output diverges from original");
    }

    #[test]
    fn encodes_flat_primitives() {
        assert_round_trip(
            r#"{"type":"record","name":"P","fields":[
                {"name":"i","type":"int"},
                {"name":"l","type":"long"},
                {"name":"f","type":"float"},
                {"name":"d","type":"double"},
                {"name":"b","type":"boolean"},
                {"name":"s","type":"string"}
            ]}"#,
            5,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                r.put("i", Value::Int(i as i32));
                r.put("l", Value::Long(i as i64 * 100));
                r.put("f", Value::Float(i as f32 * 1.5));
                r.put("d", Value::Double(i as f64 * 2.25));
                r.put("b", Value::Boolean(i % 2 == 0));
                r.put("s", Value::String(format!("row-{i}")));
                r.into()
            },
        );
    }

    #[test]
    fn encodes_nullable_primitives() {
        assert_round_trip(
            r#"{"type":"record","name":"P","fields":[
                {"name":"i","type":["null","int"],"default":null},
                {"name":"s","type":["string","null"],"default":""}
            ]}"#,
            6,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let i_v = if i % 2 == 0 {
                    Value::Union(1, Box::new(Value::Int(i as i32)))
                } else {
                    Value::Union(0, Box::new(Value::Null))
                };
                let s_v = if i % 3 == 0 {
                    Value::Union(1, Box::new(Value::Null))
                } else {
                    Value::Union(0, Box::new(Value::String(format!("v-{i}"))))
                };
                r.put("i", i_v);
                r.put("s", s_v);
                r.into()
            },
        );
    }

    #[test]
    fn encodes_logical_types() {
        assert_round_trip(
            r#"{"type":"record","name":"L","fields":[
                {"name":"d","type":{"type":"int","logicalType":"date"}},
                {"name":"tm","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"tu","type":{"type":"long","logicalType":"timestamp-micros"}}
            ]}"#,
            4,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                r.put("d", Value::Date(i as i32 * 7));
                r.put("tm", Value::TimestampMillis(1_700_000_000_000 + i as i64));
                r.put("tu", Value::TimestampMicros(1_700_000_000_000_000 + i as i64));
                r.into()
            },
        );
    }

    #[test]
    fn encodes_enum() {
        assert_round_trip(
            r#"{"type":"record","name":"R","fields":[
                {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}}
            ]}"#,
            6,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let sym = ["A", "B", "C"][i % 3];
                r.put("e", Value::Enum(i as u32 % 3, sym.to_string()));
                r.into()
            },
        );
    }

    #[test]
    fn encodes_nested_record() {
        assert_round_trip(
            r#"{"type":"record","name":"O","fields":[
                {"name":"outer_id","type":"long"},
                {"name":"inner","type":{"type":"record","name":"I","fields":[
                    {"name":"x","type":"int"},
                    {"name":"label","type":"string"}
                ]}}
            ]}"#,
            5,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let inner = Value::Record(vec![
                    ("x".to_string(), Value::Int(i as i32)),
                    ("label".to_string(), Value::String(format!("lbl-{i}"))),
                ]);
                r.put("outer_id", Value::Long(i as i64));
                r.put("inner", inner);
                r.into()
            },
        );
    }

    #[test]
    fn encodes_nullable_nested_record() {
        assert_round_trip(
            r#"{"type":"record","name":"O","fields":[
                {"name":"inner","type":["null",{"type":"record","name":"I","fields":[
                    {"name":"x","type":"int"}
                ]}],"default":null}
            ]}"#,
            6,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let inner = if i % 2 == 0 {
                    Value::Union(
                        1,
                        Box::new(Value::Record(vec![(
                            "x".to_string(),
                            Value::Int(i as i32),
                        )])),
                    )
                } else {
                    Value::Union(0, Box::new(Value::Null))
                };
                r.put("inner", inner);
                r.into()
            },
        );
    }

    #[test]
    fn encodes_multi_variant_union() {
        assert_round_trip(
            r#"{"type":"record","name":"M","fields":[
                {"name":"u","type":["null","string","int","boolean"]}
            ]}"#,
            8,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let v = match i % 4 {
                    0 => Value::Union(0, Box::new(Value::Null)),
                    1 => Value::Union(1, Box::new(Value::String(format!("s-{i}")))),
                    2 => Value::Union(2, Box::new(Value::Int(i as i32 * 11))),
                    _ => Value::Union(3, Box::new(Value::Boolean(i % 8 == 3))),
                };
                r.put("u", v);
                r.into()
            },
        );
    }

    #[test]
    fn encodes_array_of_string() {
        assert_round_trip(
            r#"{"type":"record","name":"C","fields":[
                {"name":"tags","type":{"type":"array","items":"string"}}
            ]}"#,
            6,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let tags = Value::Array(vec![
                    Value::String(format!("t-{i}-a")),
                    Value::String(format!("t-{i}-b")),
                ]);
                r.put("tags", tags);
                r.into()
            },
        );
    }

    /// Fast→fast round-trip. Use this when the baseline path's `HashMap`-based
    /// `Value::Map` randomizes key order — the bytes we emit are correct, but
    /// the baseline decoder shuffles them on the way back, so RecordBatch
    /// equality fails for a reason unrelated to fast_encode.
    fn assert_round_trip_fast_fast(
        schema_str: &str,
        n: usize,
        build: impl Fn(usize, &AvroSchema) -> Value,
    ) {
        let s = AvroSchema::parse_str(schema_str).unwrap();
        assert!(is_supported(&s), "schema not in fast-path subset");
        let owned: Vec<Vec<u8>> = (0..n)
            .map(|i| to_avro_datum(&s, build(i, &s)).unwrap())
            .collect();
        let refs: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();

        let rb: RecordBatch = crate::fast_decode::decode(&refs, &s).unwrap();
        let bytes_chunks = serialize_record_batch(rb.clone(), &s, 1).unwrap();
        let chunk = &bytes_chunks[0];
        let round_refs: Vec<&[u8]> = (0..chunk.len()).map(|i| chunk.value(i)).collect();
        let round = crate::fast_decode::decode(&round_refs, &s).unwrap();
        assert_eq!(rb, round, "fast→fast round-trip diverges");
    }

    #[test]
    fn encodes_map_of_string() {
        // Use fast→fast verification: the baseline path uses HashMap
        // iteration order which is non-deterministic for maps.
        assert_round_trip_fast_fast(
            r#"{"type":"record","name":"C","fields":[
                {"name":"props","type":{"type":"map","values":"string"}}
            ]}"#,
            4,
            |i, sch| {
                let mut r = Record::new(sch).unwrap();
                let mut m = std::collections::HashMap::new();
                m.insert(format!("k{i}-1"), Value::String(format!("v{i}-1")));
                m.insert(format!("k{i}-2"), Value::String(format!("v{i}-2")));
                r.put("props", Value::Map(m));
                r.into()
            },
        );
    }
}
