//! Fast-path Avro → Arrow decoder.
//!
//! Skips `apache_avro::from_avro_datum`'s `Value` tree intermediate by walking
//! the Avro bytes and schema together, appending directly into typed Arrow
//! builders. Callers must check [`is_supported`] before dispatching here, and
//! fall back to the `Value`-based path otherwise.
//!
//! Supported subset:
//!   - Primitives: `int`, `long`, `float`, `double`, `boolean`, `string`.
//!   - Logical types: `date` (int days), `timestamp-millis`, `timestamp-micros`.
//!   - `enum` (long index into symbols, emitted as a string column).
//!   - `record` (top-level and nested).
//!   - 2-variant `["null", T]` / `[T, "null"]` unions over any of the above.
//!   - N-variant unions whose variants are any of the above (Arrow sparse union).
//!
//! Out of scope here (fall back): `array`, `map`, `bytes`, `fixed`, `decimal`,
//! `uuid`, `duration`, `time-*`.

use crate::schema_translate::to_arrow_schema;
use anyhow::{anyhow, bail, Result};
use apache_avro::schema::{EnumSchema, RecordSchema, UnionSchema};
use apache_avro::Schema as AvroSchema;
use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, BooleanBuilder, Date32Builder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListArray, MapArray, NullArray, RecordBatch,
    StringBuilder, StructArray, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    UnionArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, UnionFields, UnionMode};
use std::sync::Arc;

// ============================================================================
// Schema gating
// ============================================================================

/// Returns `true` if this top-level schema is in the fast path's supported subset.
pub fn is_supported(schema: &AvroSchema) -> bool {
    matches!(schema, AvroSchema::Record(_)) && is_supported_inner(schema)
}

fn is_supported_inner(schema: &AvroSchema) -> bool {
    match schema {
        AvroSchema::Int
        | AvroSchema::Long
        | AvroSchema::Float
        | AvroSchema::Double
        | AvroSchema::Boolean
        | AvroSchema::String
        | AvroSchema::Null
        | AvroSchema::Date
        | AvroSchema::TimestampMillis
        | AvroSchema::TimestampMicros
        | AvroSchema::Enum(_) => true,
        AvroSchema::Record(rs) => rs.fields.iter().all(|f| is_supported_inner(&f.schema)),
        AvroSchema::Union(u) => u.variants().iter().all(is_supported_inner),
        AvroSchema::Array(a) => is_supported_inner(&a.items),
        AvroSchema::Map(m) => is_supported_inner(&m.types),
        _ => false,
    }
}

// ============================================================================
// Decoder tree
// ============================================================================

/// One per Avro field. Holds whatever Arrow builder(s) the field needs.
///
/// Nullable variants for primitives/logical/enum types are inlined here rather
/// than going through a `Box<FieldDecoder>` wrapper — Box indirection + double
/// match dispatch per row measurably hurts the hot path. Only the genuinely
/// recursive cases (Record, Union, nullable nested Record) use Box.
enum FieldDecoder {
    // ---- primitives ----
    Int(Int32Builder),
    Long(Int64Builder),
    Float(Float32Builder),
    Double(Float64Builder),
    Bool(BooleanBuilder),
    String(StringBuilder),

    // ---- logical types ----
    Date(Date32Builder),
    TimestampMillis(TimestampMillisecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),

    // ---- enum → StringArray (symbol lookup at decode time) ----
    Enum {
        builder: StringBuilder,
        symbols: Arc<Vec<String>>,
    },

    // ---- 2-variant null unions over the inline types ----
    NullableInt { builder: Int32Builder, null_first: bool },
    NullableLong { builder: Int64Builder, null_first: bool },
    NullableFloat { builder: Float32Builder, null_first: bool },
    NullableDouble { builder: Float64Builder, null_first: bool },
    NullableBool { builder: BooleanBuilder, null_first: bool },
    NullableString { builder: StringBuilder, null_first: bool },
    NullableDate { builder: Date32Builder, null_first: bool },
    NullableTimestampMillis { builder: TimestampMillisecondBuilder, null_first: bool },
    NullableTimestampMicros { builder: TimestampMicrosecondBuilder, null_first: bool },
    NullableEnum {
        builder: StringBuilder,
        symbols: Arc<Vec<String>>,
        null_first: bool,
    },

    // ---- Null (used as a child in unions whose variants include "null") ----
    Null { len: usize },

    // ---- recursive types ----
    Record(Box<RecordDecoder>),
    NullableRecord { decoder: Box<RecordDecoder>, null_first: bool },
    Union(Box<UnionDecoder>),
    List(Box<ListDecoder>),
    NullableList { decoder: Box<ListDecoder>, null_first: bool },
    Map(Box<MapDecoder>),
    NullableMap { decoder: Box<MapDecoder>, null_first: bool },
}

/// Recursive record builder. `nulls` is `Some` only for nullable records
/// (i.e. records inside a 2-variant null union); top-level non-nullable
/// records leave it `None`.
struct RecordDecoder {
    /// Per-field decoders, indexed the same as `arrow_fields`. We deliberately
    /// don't store the field name per child — it's already in `arrow_fields`,
    /// never referenced in the decode hot loop, and dropping the per-tuple
    /// `String` shrinks the per-child footprint by 24 bytes (better cache
    /// behavior for records with many fields).
    children: Vec<FieldDecoder>,
    arrow_fields: Fields,
    nulls: Option<BooleanBufferBuilder>,
    len: usize,
}

struct UnionDecoder {
    children: Vec<FieldDecoder>,
    arrow_fields: UnionFields,
    type_ids: Vec<i8>,
}

/// Avro array → Arrow ListArray. Tracks one item per inner decode call and
/// emits an offset per outer row (including null rows).
struct ListDecoder {
    inner: FieldDecoder,
    item_field: FieldRef,
    /// Length-(N+1) offsets buffer; starts with `[0]`.
    offsets: Vec<i32>,
    /// `Some` only when this list itself is in a 2-variant null union.
    nulls: Option<BooleanBufferBuilder>,
    cur_offset: i32,
}

/// Avro map → Arrow MapArray. Keys are always strings in Avro; we keep a
/// dedicated StringBuilder for them and one FieldDecoder for values.
struct MapDecoder {
    keys: StringBuilder,
    values: FieldDecoder,
    /// The Field describing the MapArray's `entries` struct (keys + values).
    entries_field: FieldRef,
    /// The keys+values Fields used to build the entries StructArray.
    entries_inner_fields: Fields,
    offsets: Vec<i32>,
    nulls: Option<BooleanBufferBuilder>,
    cur_offset: i32,
}

// ============================================================================
// Construction
// ============================================================================

/// Build a field decoder given the avro schema plus the arrow `Field` that
/// `schema_translate` produced for it. Using the pre-computed arrow field
/// avoids re-walking the schema to figure out nested-field nullability.
fn make_decoder(schema: &AvroSchema, arrow_field: &Field, cap: usize) -> Result<FieldDecoder> {
    Ok(match schema {
        AvroSchema::Int => FieldDecoder::Int(Int32Builder::with_capacity(cap)),
        AvroSchema::Long => FieldDecoder::Long(Int64Builder::with_capacity(cap)),
        AvroSchema::Float => FieldDecoder::Float(Float32Builder::with_capacity(cap)),
        AvroSchema::Double => FieldDecoder::Double(Float64Builder::with_capacity(cap)),
        AvroSchema::Boolean => FieldDecoder::Bool(BooleanBuilder::with_capacity(cap)),
        AvroSchema::String => FieldDecoder::String(StringBuilder::with_capacity(cap, cap * 16)),
        AvroSchema::Date => FieldDecoder::Date(Date32Builder::with_capacity(cap)),
        AvroSchema::TimestampMillis => {
            FieldDecoder::TimestampMillis(TimestampMillisecondBuilder::with_capacity(cap))
        }
        AvroSchema::TimestampMicros => {
            FieldDecoder::TimestampMicros(TimestampMicrosecondBuilder::with_capacity(cap))
        }
        AvroSchema::Enum(EnumSchema { symbols, .. }) => FieldDecoder::Enum {
            builder: StringBuilder::with_capacity(cap, cap * 16),
            symbols: Arc::new(symbols.clone()),
        },
        AvroSchema::Null => FieldDecoder::Null { len: 0 },
        AvroSchema::Record(rs) => {
            // arrow_field.data_type() is the Struct datatype with the correctly
            // nullability-propagated inner fields.
            let inner_fields = match arrow_field.data_type() {
                DataType::Struct(f) => f.clone(),
                other => bail!("expected Struct, got {other:?} for record"),
            };
            FieldDecoder::Record(Box::new(make_record_decoder(rs, inner_fields, cap, false)?))
        }
        AvroSchema::Union(u) => make_union_decoder(u, arrow_field, cap)?,
        AvroSchema::Array(a) => {
            FieldDecoder::List(Box::new(make_list_decoder(&a.items, arrow_field, cap)?))
        }
        AvroSchema::Map(m) => {
            FieldDecoder::Map(Box::new(make_map_decoder(&m.types, arrow_field, cap)?))
        }
        other => bail!("fast_decode: unsupported schema in make_decoder: {other:?}"),
    })
}

fn make_list_decoder(
    item_schema: &AvroSchema,
    arrow_field: &Field,
    cap: usize,
) -> Result<ListDecoder> {
    let item_field = match arrow_field.data_type() {
        DataType::List(f) => f.clone(),
        other => bail!("expected List, got {other:?}"),
    };
    let inner = make_decoder(item_schema, &item_field, cap)?;
    let mut offsets = Vec::with_capacity(cap + 1);
    offsets.push(0);
    Ok(ListDecoder {
        inner,
        item_field,
        offsets,
        nulls: None,
        cur_offset: 0,
    })
}

fn make_map_decoder(
    value_schema: &AvroSchema,
    arrow_field: &Field,
    cap: usize,
) -> Result<MapDecoder> {
    let (entries_field, entries_inner_fields) = match arrow_field.data_type() {
        DataType::Map(entries, _sorted) => {
            let inner = match entries.data_type() {
                DataType::Struct(fs) => fs.clone(),
                other => bail!("expected Struct inside Map, got {other:?}"),
            };
            (entries.clone(), inner)
        }
        other => bail!("expected Map, got {other:?}"),
    };
    if entries_inner_fields.len() != 2 {
        bail!("Map entries must have exactly 2 fields (keys, values)");
    }
    let value_arrow_field = &entries_inner_fields[1];
    let values = make_decoder(value_schema, value_arrow_field, cap)?;
    let mut offsets = Vec::with_capacity(cap + 1);
    offsets.push(0);
    Ok(MapDecoder {
        keys: StringBuilder::with_capacity(cap, cap * 16),
        values,
        entries_field,
        entries_inner_fields,
        offsets,
        nulls: None,
        cur_offset: 0,
    })
}

fn make_nullable_decoder(
    inner: &AvroSchema,
    arrow_field: &Field,
    null_first: bool,
    cap: usize,
) -> Result<FieldDecoder> {
    Ok(match inner {
        AvroSchema::Int => FieldDecoder::NullableInt {
            builder: Int32Builder::with_capacity(cap),
            null_first,
        },
        AvroSchema::Long => FieldDecoder::NullableLong {
            builder: Int64Builder::with_capacity(cap),
            null_first,
        },
        AvroSchema::Float => FieldDecoder::NullableFloat {
            builder: Float32Builder::with_capacity(cap),
            null_first,
        },
        AvroSchema::Double => FieldDecoder::NullableDouble {
            builder: Float64Builder::with_capacity(cap),
            null_first,
        },
        AvroSchema::Boolean => FieldDecoder::NullableBool {
            builder: BooleanBuilder::with_capacity(cap),
            null_first,
        },
        AvroSchema::String => FieldDecoder::NullableString {
            builder: StringBuilder::with_capacity(cap, cap * 16),
            null_first,
        },
        AvroSchema::Date => FieldDecoder::NullableDate {
            builder: Date32Builder::with_capacity(cap),
            null_first,
        },
        AvroSchema::TimestampMillis => FieldDecoder::NullableTimestampMillis {
            builder: TimestampMillisecondBuilder::with_capacity(cap),
            null_first,
        },
        AvroSchema::TimestampMicros => FieldDecoder::NullableTimestampMicros {
            builder: TimestampMicrosecondBuilder::with_capacity(cap),
            null_first,
        },
        AvroSchema::Enum(EnumSchema { symbols, .. }) => FieldDecoder::NullableEnum {
            builder: StringBuilder::with_capacity(cap, cap * 16),
            symbols: Arc::new(symbols.clone()),
            null_first,
        },
        AvroSchema::Record(rs) => {
            let inner_fields = match arrow_field.data_type() {
                DataType::Struct(f) => f.clone(),
                other => bail!("expected Struct for nullable record, got {other:?}"),
            };
            FieldDecoder::NullableRecord {
                decoder: Box::new(make_record_decoder(rs, inner_fields, cap, true)?),
                null_first,
            }
        }
        AvroSchema::Array(a) => {
            let mut dec = make_list_decoder(&a.items, arrow_field, cap)?;
            dec.nulls = Some(BooleanBufferBuilder::new(cap));
            FieldDecoder::NullableList { decoder: Box::new(dec), null_first }
        }
        AvroSchema::Map(m) => {
            let mut dec = make_map_decoder(&m.types, arrow_field, cap)?;
            dec.nulls = Some(BooleanBufferBuilder::new(cap));
            FieldDecoder::NullableMap { decoder: Box::new(dec), null_first }
        }
        other => bail!("fast_decode: unsupported nullable inner type: {other:?}"),
    })
}

fn make_record_decoder(
    rs: &RecordSchema,
    arrow_fields: Fields,
    cap: usize,
    nullable: bool,
) -> Result<RecordDecoder> {
    if rs.fields.len() != arrow_fields.len() {
        bail!(
            "fast_decode: avro/arrow field count mismatch ({} vs {})",
            rs.fields.len(),
            arrow_fields.len()
        );
    }
    let mut children = Vec::with_capacity(rs.fields.len());
    for (field, arrow_f) in rs.fields.iter().zip(arrow_fields.iter()) {
        let dec = make_decoder(&field.schema, arrow_f, cap)?;
        children.push(dec);
    }
    Ok(RecordDecoder {
        children,
        arrow_fields,
        nulls: if nullable {
            Some(BooleanBufferBuilder::new(cap))
        } else {
            None
        },
        len: 0,
    })
}

fn make_union_decoder(u: &UnionSchema, arrow_field: &Field, cap: usize) -> Result<FieldDecoder> {
    // 2-variant null union collapses to a plain nullable inner type. Arrow
    // doesn't represent this as DataType::Union — `arrow_field` here is whatever
    // the inner non-null variant maps to, with nullability set on the field.
    if let Some((inner_schema, null_first)) = split_null_union(u) {
        return make_nullable_decoder(inner_schema, arrow_field, null_first, cap);
    }
    // N-variant: produce a sparse Arrow union. Extract per-variant fields from
    // arrow_field.data_type(), which schema_translate has already populated.
    let union_arrow_fields = match arrow_field.data_type() {
        DataType::Union(uf, _mode) => uf.clone(),
        other => bail!("expected Union datatype for multi-variant union, got {other:?}"),
    };
    let variants = u.variants();
    if union_arrow_fields.len() != variants.len() {
        bail!(
            "fast_decode: union variant count mismatch ({} vs {})",
            union_arrow_fields.len(),
            variants.len()
        );
    }
    let mut children = Vec::with_capacity(variants.len());
    for (variant, (_, variant_field)) in variants.iter().zip(union_arrow_fields.iter()) {
        children.push(make_decoder(variant, variant_field, cap)?);
    }
    Ok(FieldDecoder::Union(Box::new(UnionDecoder {
        children,
        arrow_fields: union_arrow_fields,
        type_ids: Vec::with_capacity(cap),
    })))
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
// Decoding
// ============================================================================

impl FieldDecoder {
    #[inline]
    fn decode(&mut self, buf: &mut &[u8]) -> Result<()> {
        match self {
            Self::Int(b) => b.append_value(read_zigzag_long(buf)? as i32),
            Self::Long(b) => b.append_value(read_zigzag_long(buf)?),
            Self::Float(b) => b.append_value(read_f32(buf)?),
            Self::Double(b) => b.append_value(read_f64(buf)?),
            Self::Bool(b) => b.append_value(read_bool(buf)?),
            Self::String(b) => b.append_value(read_string(buf)?),
            Self::Date(b) => b.append_value(read_zigzag_long(buf)? as i32),
            Self::TimestampMillis(b) => b.append_value(read_zigzag_long(buf)?),
            Self::TimestampMicros(b) => b.append_value(read_zigzag_long(buf)?),
            Self::Enum { builder, symbols } => append_enum(builder, symbols, buf)?,
            Self::NullableInt { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_zigzag_long(buf)? as i32),
            },
            Self::NullableLong { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_zigzag_long(buf)?),
            },
            Self::NullableFloat { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_f32(buf)?),
            },
            Self::NullableDouble { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_f64(buf)?),
            },
            Self::NullableBool { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_bool(buf)?),
            },
            Self::NullableString { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_string(buf)?),
            },
            Self::NullableDate { builder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => builder.append_null(),
                Branch::Value => builder.append_value(read_zigzag_long(buf)? as i32),
            },
            Self::NullableTimestampMillis { builder, null_first } => {
                match union_branch(buf, *null_first)? {
                    Branch::Null => builder.append_null(),
                    Branch::Value => builder.append_value(read_zigzag_long(buf)?),
                }
            }
            Self::NullableTimestampMicros { builder, null_first } => {
                match union_branch(buf, *null_first)? {
                    Branch::Null => builder.append_null(),
                    Branch::Value => builder.append_value(read_zigzag_long(buf)?),
                }
            }
            Self::NullableEnum { builder, symbols, null_first } => {
                match union_branch(buf, *null_first)? {
                    Branch::Null => builder.append_null(),
                    Branch::Value => append_enum(builder, symbols, buf)?,
                }
            }
            Self::Null { len } => *len += 1,
            Self::Record(r) => r.decode_present(buf)?,
            Self::NullableRecord { decoder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => decoder.append_null(),
                Branch::Value => decoder.decode_present(buf)?,
            },
            Self::Union(u) => u.decode(buf)?,
            Self::List(l) => l.decode_present(buf)?,
            Self::NullableList { decoder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => decoder.append_null(),
                Branch::Value => decoder.decode_present(buf)?,
            },
            Self::Map(m) => m.decode_present(buf)?,
            Self::NullableMap { decoder, null_first } => match union_branch(buf, *null_first)? {
                Branch::Null => decoder.append_null(),
                Branch::Value => decoder.decode_present(buf)?,
            },
        }
        Ok(())
    }

    /// Append a logical null without consuming bytes. Used by parent unions
    /// when a different variant was selected.
    fn append_null(&mut self) {
        match self {
            Self::Int(b) => b.append_null(),
            Self::Long(b) => b.append_null(),
            Self::Float(b) => b.append_null(),
            Self::Double(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::String(b) => b.append_null(),
            Self::Date(b) => b.append_null(),
            Self::TimestampMillis(b) => b.append_null(),
            Self::TimestampMicros(b) => b.append_null(),
            Self::Enum { builder, .. } => builder.append_null(),
            Self::NullableInt { builder, .. } => builder.append_null(),
            Self::NullableLong { builder, .. } => builder.append_null(),
            Self::NullableFloat { builder, .. } => builder.append_null(),
            Self::NullableDouble { builder, .. } => builder.append_null(),
            Self::NullableBool { builder, .. } => builder.append_null(),
            Self::NullableString { builder, .. } => builder.append_null(),
            Self::NullableDate { builder, .. } => builder.append_null(),
            Self::NullableTimestampMillis { builder, .. } => builder.append_null(),
            Self::NullableTimestampMicros { builder, .. } => builder.append_null(),
            Self::NullableEnum { builder, .. } => builder.append_null(),
            Self::Null { len } => *len += 1,
            Self::Record(r) => r.append_null(),
            Self::NullableRecord { decoder, .. } => decoder.append_null(),
            Self::Union(u) => u.append_null(),
            Self::List(l) => l.append_null(),
            Self::NullableList { decoder, .. } => decoder.append_null(),
            Self::Map(m) => m.append_null(),
            Self::NullableMap { decoder, .. } => decoder.append_null(),
        }
    }

    fn finish(self) -> Result<ArrayRef> {
        Ok(match self {
            Self::Int(mut b) => Arc::new(b.finish()),
            Self::Long(mut b) => Arc::new(b.finish()),
            Self::Float(mut b) => Arc::new(b.finish()),
            Self::Double(mut b) => Arc::new(b.finish()),
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => Arc::new(b.finish()),
            Self::Date(mut b) => Arc::new(b.finish()),
            Self::TimestampMillis(mut b) => Arc::new(b.finish()),
            Self::TimestampMicros(mut b) => Arc::new(b.finish()),
            Self::Enum { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableInt { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableLong { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableFloat { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableDouble { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableBool { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableString { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableDate { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableTimestampMillis { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableTimestampMicros { mut builder, .. } => Arc::new(builder.finish()),
            Self::NullableEnum { mut builder, .. } => Arc::new(builder.finish()),
            Self::Null { len } => Arc::new(NullArray::new(len)),
            Self::Record(r) => r.finish()?,
            Self::NullableRecord { decoder, .. } => decoder.finish()?,
            Self::Union(u) => u.finish()?,
            Self::List(l) => l.finish()?,
            Self::NullableList { decoder, .. } => decoder.finish()?,
            Self::Map(m) => m.finish()?,
            Self::NullableMap { decoder, .. } => decoder.finish()?,
        })
    }
}

#[inline]
fn append_enum(builder: &mut StringBuilder, symbols: &[String], buf: &mut &[u8]) -> Result<()> {
    let idx = read_zigzag_long(buf)? as usize;
    let sym = symbols
        .get(idx)
        .ok_or_else(|| anyhow!("enum index {idx} out of range"))?;
    builder.append_value(sym);
    Ok(())
}

enum Branch {
    Null,
    Value,
}

#[inline(always)]
fn union_branch(buf: &mut &[u8], null_first: bool) -> Result<Branch> {
    let idx = read_zigzag_long(buf)?;
    match (idx, null_first) {
        (0, true) | (1, false) => Ok(Branch::Null),
        (1, true) | (0, false) => Ok(Branch::Value),
        (i, _) => bail!("invalid union branch index: {i}"),
    }
}

impl RecordDecoder {
    #[inline(always)]
    fn decode_present(&mut self, buf: &mut &[u8]) -> Result<()> {
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.append(true);
        }
        self.len += 1;
        for child in self.children.iter_mut() {
            child.decode(buf)?;
        }
        Ok(())
    }

    fn append_null(&mut self) {
        if let Some(nulls) = self.nulls.as_mut() {
            nulls.append(false);
        }
        self.len += 1;
        for child in self.children.iter_mut() {
            child.append_null();
        }
    }

    fn finish(self) -> Result<ArrayRef> {
        let RecordDecoder {
            children,
            arrow_fields,
            nulls,
            len,
        } = self;
        let arrays: Vec<ArrayRef> = children
            .into_iter()
            .map(FieldDecoder::finish)
            .collect::<Result<_>>()?;
        let null_buffer = nulls.map(|mut b| NullBuffer::new(b.finish()));
        // Treat zero-length structs carefully — StructArray::try_new rejects
        // empty fields, but a Record with 0 fields is unusual and not in our
        // bench paths. If hit, surface a clear error.
        if arrow_fields.is_empty() {
            bail!("RecordDecoder produced a record with 0 fields");
        }
        let sa = StructArray::try_new(arrow_fields, arrays, null_buffer)?;
        debug_assert_eq!(sa.len(), len);
        Ok(Arc::new(sa))
    }
}

impl UnionDecoder {
    fn decode(&mut self, buf: &mut &[u8]) -> Result<()> {
        let idx = read_zigzag_long(buf)?;
        if idx < 0 || (idx as usize) >= self.children.len() {
            bail!("union branch index out of range: {idx}");
        }
        let idx_u = idx as usize;
        for (i, child) in self.children.iter_mut().enumerate() {
            if i == idx_u {
                child.decode(buf)?;
            } else {
                child.append_null();
            }
        }
        self.type_ids.push(idx as i8);
        Ok(())
    }

    fn append_null(&mut self) {
        // Append null to every child and pick variant 0 as the placeholder
        // type_id. Outer-frame consumers should not look at this row anyway
        // (a parent null buffer flags it).
        for child in self.children.iter_mut() {
            child.append_null();
        }
        self.type_ids.push(0);
    }

    fn finish(self) -> Result<ArrayRef> {
        let UnionDecoder {
            children,
            arrow_fields,
            type_ids,
        } = self;
        let children_arrays: Vec<ArrayRef> = children
            .into_iter()
            .map(FieldDecoder::finish)
            .collect::<Result<_>>()?;
        let type_ids_buf = ScalarBuffer::from(type_ids);
        let ua = UnionArray::try_new(arrow_fields, type_ids_buf, None, children_arrays)?;
        Ok(Arc::new(ua))
    }
}

/// Avro block-encoded count: positive `n` means `n` items follow; negative
/// `-n` means `n` items follow but a long block-byte-size precedes them
/// (we don't need to skip — we always read the items). Terminated by 0.
#[inline]
fn read_block_count(buf: &mut &[u8]) -> Result<i64> {
    let n = read_zigzag_long(buf)?;
    if n < 0 {
        // Block has explicit byte size after the negated count; ignore it,
        // we always consume the items.
        let _block_size = read_zigzag_long(buf)?;
        Ok(-n)
    } else {
        Ok(n)
    }
}

impl ListDecoder {
    fn decode_present(&mut self, buf: &mut &[u8]) -> Result<()> {
        loop {
            let n = read_block_count(buf)?;
            if n == 0 {
                break;
            }
            for _ in 0..n {
                self.inner.decode(buf)?;
                self.cur_offset += 1;
            }
        }
        self.offsets.push(self.cur_offset);
        if let Some(nb) = self.nulls.as_mut() {
            nb.append(true);
        }
        Ok(())
    }

    fn append_null(&mut self) {
        // Empty list at this row. Inner builder length is unchanged.
        self.offsets.push(self.cur_offset);
        if let Some(nb) = self.nulls.as_mut() {
            nb.append(false);
        }
    }

    fn finish(self) -> Result<ArrayRef> {
        let ListDecoder {
            inner,
            item_field,
            offsets,
            nulls,
            ..
        } = self;
        let values = inner.finish()?;
        let offsets = OffsetBuffer::new(offsets.into());
        let nulls = nulls.map(|mut b| NullBuffer::new(b.finish()));
        Ok(Arc::new(ListArray::new(item_field, offsets, values, nulls)))
    }
}

impl MapDecoder {
    fn decode_present(&mut self, buf: &mut &[u8]) -> Result<()> {
        loop {
            let n = read_block_count(buf)?;
            if n == 0 {
                break;
            }
            for _ in 0..n {
                self.keys.append_value(read_string(buf)?);
                self.values.decode(buf)?;
                self.cur_offset += 1;
            }
        }
        self.offsets.push(self.cur_offset);
        if let Some(nb) = self.nulls.as_mut() {
            nb.append(true);
        }
        Ok(())
    }

    fn append_null(&mut self) {
        // Empty map at this row.
        self.offsets.push(self.cur_offset);
        if let Some(nb) = self.nulls.as_mut() {
            nb.append(false);
        }
    }

    fn finish(self) -> Result<ArrayRef> {
        let MapDecoder {
            mut keys,
            values,
            entries_field,
            entries_inner_fields,
            offsets,
            nulls,
            ..
        } = self;
        let keys_arr: ArrayRef = Arc::new(keys.finish());
        let values_arr = values.finish()?;
        let entries = StructArray::try_new(
            entries_inner_fields,
            vec![keys_arr, values_arr],
            None,
        )?;
        let offsets = OffsetBuffer::new(offsets.into());
        let nulls = nulls.map(|mut b| NullBuffer::new(b.finish()));
        Ok(Arc::new(MapArray::new(
            entries_field,
            offsets,
            entries,
            nulls,
            false,
        )))
    }
}

// ============================================================================
// Entry point
// ============================================================================

/// Fast-path entry point. Caller must have verified [`is_supported`] returns `true`.
pub fn decode(data: &[&[u8]], schema: &AvroSchema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(to_arrow_schema(schema)?);
    decode_with_arrow_schema(data, schema, &arrow_schema)
}

/// Like [`decode`] but uses a pre-computed Arrow schema. Callers processing
/// multiple chunks of the same Avro schema (e.g. the threaded path) should
/// compute the Arrow schema once at the top level and share it across chunks
/// instead of paying for an `to_arrow_schema` walk per chunk.
pub fn decode_with_arrow_schema(
    data: &[&[u8]],
    schema: &AvroSchema,
    arrow_schema: &Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch> {
    let rs = match schema {
        AvroSchema::Record(rs) => rs,
        _ => return Err(anyhow!("fast_decode::decode called on non-record schema")),
    };
    let mut top = make_record_decoder(rs, arrow_schema.fields.clone(), data.len(), false)?;
    for record_bytes in data {
        let mut buf: &[u8] = record_bytes;
        top.decode_present(&mut buf)?;
    }
    let arrays: Vec<ArrayRef> = top
        .children
        .into_iter()
        .map(FieldDecoder::finish)
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(arrow_schema.clone(), arrays)?)
}

// Discourage `DataType` from looking unused if the build feature flags change.
#[allow(dead_code)]
fn _silence_unused_warnings(_: DataType, _: UnionMode) {}

// ============================================================================
// Primitive byte decoders
// ============================================================================

#[inline(always)]
fn read_byte(buf: &mut &[u8]) -> Result<u8> {
    let (first, rest) = buf
        .split_first()
        .ok_or_else(|| anyhow!("unexpected end of buffer"))?;
    *buf = rest;
    Ok(*first)
}

#[inline(always)]
fn read_zigzag_long(buf: &mut &[u8]) -> Result<i64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = read_byte(buf)?;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(((result >> 1) as i64) ^ -((result & 1) as i64));
        }
        shift += 7;
        if shift >= 64 {
            bail!("zigzag varint too long");
        }
    }
}

#[inline]
fn read_f32(buf: &mut &[u8]) -> Result<f32> {
    if buf.len() < 4 {
        bail!("unexpected end of buffer (f32)");
    }
    let bytes = [buf[0], buf[1], buf[2], buf[3]];
    *buf = &buf[4..];
    Ok(f32::from_le_bytes(bytes))
}

#[inline]
fn read_f64(buf: &mut &[u8]) -> Result<f64> {
    if buf.len() < 8 {
        bail!("unexpected end of buffer (f64)");
    }
    let bytes = [
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ];
    *buf = &buf[8..];
    Ok(f64::from_le_bytes(bytes))
}

#[inline]
fn read_bool(buf: &mut &[u8]) -> Result<bool> {
    match read_byte(buf)? {
        0 => Ok(false),
        1 => Ok(true),
        b => bail!("invalid boolean byte: {b}"),
    }
}

#[inline(always)]
fn read_string<'a>(buf: &mut &'a [u8]) -> Result<&'a str> {
    let len = read_zigzag_long(buf)?;
    if len < 0 {
        bail!("negative string length");
    }
    let len = len as usize;
    if buf.len() < len {
        bail!("unexpected end of buffer (string)");
    }
    let (head, rest) = buf.split_at(len);
    *buf = rest;
    // SAFETY: Avro's wire format guarantees `string` values are valid UTF-8
    // (Avro spec §1.11.1, "Encodings"). The `apache-avro` encoder that
    // produces these bytes also enforces this on the write side. Skipping
    // the validation here saves an O(N) byte-scan per string; in profiles
    // it was ~18% of in-binary time on string-heavy schemas. If invalid
    // bytes did slip through, downstream Arrow consumers would surface the
    // corruption (StringArray::value debug-asserts UTF-8).
    Ok(unsafe { std::str::from_utf8_unchecked(head) })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deserialize::per_datum_deserialize_baseline;
    use apache_avro::to_avro_datum;
    use apache_avro::types::{Record, Value};

    fn build_records(
        schema: &AvroSchema,
        n: usize,
        build: impl Fn(usize, &AvroSchema) -> Value,
    ) -> Vec<Vec<u8>> {
        (0..n)
            .map(|i| to_avro_datum(schema, build(i, schema)).unwrap())
            .collect()
    }

    fn assert_round_trip(schema_str: &str, n: usize, build: impl Fn(usize, &AvroSchema) -> Value) {
        let s = AvroSchema::parse_str(schema_str).unwrap();
        assert!(is_supported(&s), "schema not supported by fast path");
        let owned = build_records(&s, n, build);
        let refs: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        let fast = decode(&refs, &s).unwrap();
        let baseline = per_datum_deserialize_baseline(&refs.iter().copied().collect(), &s).unwrap();
        assert_eq!(fast, baseline, "fast path output diverges from baseline");
    }

    #[test]
    fn supports_flat_primitives() {
        let s = AvroSchema::parse_str(
            r#"{"type":"record","name":"P","fields":[
                {"name":"i","type":"int"},
                {"name":"s","type":"string"}
            ]}"#,
        )
        .unwrap();
        assert!(is_supported(&s));
    }

    #[test]
    fn supports_logical_types_and_enum() {
        let s = AvroSchema::parse_str(
            r#"{"type":"record","name":"L","fields":[
                {"name":"d","type":{"type":"int","logicalType":"date"}},
                {"name":"tm","type":{"type":"long","logicalType":"timestamp-millis"}},
                {"name":"tu","type":{"type":"long","logicalType":"timestamp-micros"}},
                {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}}
            ]}"#,
        )
        .unwrap();
        assert!(is_supported(&s));
    }

    #[test]
    fn supports_nested_record() {
        let s = AvroSchema::parse_str(
            r#"{"type":"record","name":"O","fields":[
                {"name":"inner","type":{"type":"record","name":"I","fields":[
                    {"name":"x","type":"int"}
                ]}}
            ]}"#,
        )
        .unwrap();
        assert!(is_supported(&s));
    }

    #[test]
    fn supports_multi_variant_union() {
        let s = AvroSchema::parse_str(
            r#"{"type":"record","name":"M","fields":[
                {"name":"u","type":["null","string","int","boolean"]}
            ]}"#,
        )
        .unwrap();
        assert!(is_supported(&s));
    }

    // ---- Differential tests ----

    #[test]
    fn decodes_flat_primitives() {
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
    fn decodes_nullable_primitives() {
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
    fn decodes_logical_types() {
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
    fn decodes_enum() {
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
    fn decodes_nested_record() {
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
    fn decodes_nullable_nested_record() {
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
    fn decodes_multi_variant_union() {
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
    fn decodes_array_of_string() {
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

    #[test]
    fn decodes_empty_array() {
        assert_round_trip(
            r#"{"type":"record","name":"C","fields":[
                {"name":"tags","type":{"type":"array","items":"int"}}
            ]}"#,
            3,
            |_i, sch| {
                let mut r = Record::new(sch).unwrap();
                r.put("tags", Value::Array(vec![]));
                r.into()
            },
        );
    }

    #[test]
    fn decodes_map_of_string() {
        // Baseline's `complex.rs::MapContainer` iterates `Value::Map`'s HashMap
        // in random order. Compare via per-row sorted (key, value) pairs.
        let schema_str = r#"{"type":"record","name":"C","fields":[
            {"name":"props","type":{"type":"map","values":"string"}}
        ]}"#;
        let s = AvroSchema::parse_str(schema_str).unwrap();
        let owned = build_records(&s, 4, |i, sch| {
            let mut r = Record::new(sch).unwrap();
            let mut m = std::collections::HashMap::new();
            m.insert(format!("k{i}-1"), Value::String(format!("v{i}-1")));
            m.insert(format!("k{i}-2"), Value::String(format!("v{i}-2")));
            r.put("props", Value::Map(m));
            r.into()
        });
        let refs: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
        let fast = decode(&refs, &s).unwrap();
        let baseline = per_datum_deserialize_baseline(&refs.iter().copied().collect(), &s).unwrap();

        let f = fast.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        let b = baseline.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(f.len(), b.len());
        for row in 0..f.len() {
            let mut fp = pairs(f, row);
            let mut bp = pairs(b, row);
            fp.sort();
            bp.sort();
            assert_eq!(fp, bp, "row {row} differs");
        }
    }

    fn pairs(m: &MapArray, row: usize) -> Vec<(String, String)> {
        let offsets = m.value_offsets();
        let start = offsets[row] as usize;
        let end = offsets[row + 1] as usize;
        let keys = m.keys().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        let values = m.values().as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        (start..end)
            .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
            .collect()
    }
}
