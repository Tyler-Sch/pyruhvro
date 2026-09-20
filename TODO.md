# TODO

## Accurate nullability for fields nested under a nullable struct

Today every descendant of a nullable Avro record (`["null", {"type":"record",...}]`) is
reported as nullable in the Arrow schema, and `list` items are unconditionally nullable —
which then makes the item struct's fields nullable too. So the same record `S` produces
`struct<x: int32 not null>` as a direct field but `list<item: struct<x: int32>>` inside an
array. Consumers that enforce schemas (Parquet/Delta writers, Polars) lose that information.

Why it's this way (commit `2c3e45d`): when a struct is null, both decoders push a **null**
into every child builder (`RecordDecoder::append_null` in `fast_decode.rs`,
`StructContainer::add_val` in `complex.rs`). arrow-rs's `StructArray::try_new` only allows
nulls in a non-nullable child when they're masked by the *immediate* parent's null buffer,
so a non-nullable record `U` nested inside nullable `S` breaks: `U` has no null buffer, and
`U.z`'s null is only masked by the grandparent → `Found unmasked nulls for non-nullable
StructArray field "z"`. Propagating `nullable` down in `schema/translate.rs` is the
workaround.

Fix (builders, not schema):
- On a null parent, append a **placeholder** (`0`, `""`, empty list/map, …) to non-nullable
  children instead of a null. The Arrow spec says child slots under a null parent are
  undefined, so this is legal. Nullable children keep getting a null.
- Touch points: `RecordDecoder::append_null` and each `FieldDecoder` variant's
  `append_null` in `ruhvro/src/fast_decode.rs`; `StructContainer::add_val` (the
  `Value::Null` arm) in `ruhvro/src/complex.rs`.
- Then in `ruhvro/src/schema/translate.rs` pass `false` for record children and list
  items and let each child's own `["null", T]` union set nullability.
- Verify the serialize side (`serialization_containers.rs`, `fast_encode.rs`) skips
  children under a null parent rather than reading the placeholder. The byte-exact
  round-trip on nested null records already passes, so this is likely fine, but confirm.
- Regression test: nullable `S` containing non-nullable `U { z: long }`, with a null `S`
  row, on both the fast path and `per_datum_deserialize_baseline`; assert the Arrow schema
  reports `z` as `not null` and `pyarrow` `validate(full=True)` passes.

## Slow path panics on `bytes` and `duration`

`add_data_to_array_builder` in `ruhvro/src/complex.rs` has `unimplemented!()` for
`DataType::Binary` (Avro `bytes`) and hits another `unimplemented!()` for `duration`. Any
schema containing these falls off the fast path (`fast_decode::is_supported` returns
`false`) and then panics in the slow path, which surfaces in Python as a
`pyo3_runtime.PanicException` rather than a `ValueError`.

- Implement `Binary` (`BinaryBuilder`) and `Duration` in `add_data_to_array_builder`, or
- at minimum replace the `unimplemented!()` arms with `Err(anyhow!(...))` so unsupported
  types raise a proper error. The same applies to `DataType::Map` / `RunEndEncoded` /
  the catch-all in `default_field_name` in `ruhvro/src/schema/translate.rs`.
- Longer term, add `bytes` to the fast path's supported set so common schemas don't
  hit the slow path at all.
