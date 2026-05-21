# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

Cargo workspace with two crates:

- `ruhvro/` — the core Rust library (published as `ruhvro` on crates.io). Pure Rust API for serializing/deserializing schemaless Avro to/from Arrow `RecordBatch`es. Has no Python deps.
- `src/lib.rs` (top-level) — the `pyruhvro` PyO3 extension module that wraps `ruhvro` and exposes it to Python via maturin. Re-exports three functions: `deserialize_array`, `deserialize_array_threaded`, `serialize_record_batch`.

Keep Python-facing concerns in the top-level crate; keep the Avro↔Arrow logic in `ruhvro/`. The PyO3 wrappers should stay thin — convert PyArrow types, call into `ruhvro`, return PyArrow types.

## Common commands

Rust-only (core library):
```
cargo build                       # build workspace
cargo test                        # run all tests
cargo test -p ruhvro              # tests for core crate only
cargo test --test <name>          # single integration test
cargo test <fn_substring>         # filter unit tests by name (e.g. cargo test test_round_trip)
cargo build --release             # release build (LTO + codegen-units=1, see top Cargo.toml)
```

Python extension (requires a venv with `maturin`):
```
maturin develop --release         # build + install pyruhvro into the active venv (use this while iterating)
maturin build --release           # produce a wheel under target/wheels/
```
After `maturin develop`, scripts under `scripts/` can `from pyruhvro import ...` directly.

Benchmarks (compares pyruhvro to fastavro using `timeit`):
```
cd scripts && bash run_benchmarks.sh
```
Requires `pip install fastavro pyarrow faker` and pyruhvro already installed in the venv.

## Architecture

The pipeline is **schemaless Avro bytes ⇄ Arrow `RecordBatch`**, driven by a parsed `apache_avro::Schema`. Understanding the module split matters because Avro's structural types (records, unions, arrays, maps) don't map 1:1 to Arrow builders, so the code maintains its own builder/container hierarchy.

Key modules in `ruhvro/src/`:

- `deserialize.rs` — public entry points `parse_schema`, `per_datum_deserialize` (single-threaded), `per_datum_deserialize_threaded` (rayon, splits input into `num_chunks` slices and returns one `RecordBatch` per chunk).
- `serialize.rs` — public entry point `serialize_record_batch`. Converts the `RecordBatch` into a `StructArray`, slices it into `num_chunks`, and serializes each slice in parallel via rayon into a `GenericBinaryArray<i32>` of Avro datums.
- `schema_translate.rs` — converts an `apache_avro::Schema` into an `arrow::datatypes::Schema`. This is the source of truth for type mapping (e.g. nullable-union → nullable Arrow field, multi-variant unions → Arrow `Union`, Avro `map` → Arrow `Map`, logical types → Arrow temporal types).
- `complex.rs` — `AvroToArrowBuilder` and its `Struct`/`List`/`Union`/`Map`/`Primitive` variants. This is the *deserialize* side: walks Avro `Value`s into Arrow builders. The `add_val!` macro and `get_val_from_possible_union` helper handle the common "value might be wrapped in a union" case.
- `serialization_containers.rs` — the *serialize* side: `ArrayContainers` walks Arrow arrays column-wise and re-emits `apache_avro::types::Value`s, then `to_avro_datum` encodes each row.

### Threading model

Both threaded paths use rayon and require the caller to pass `num_chunks` explicitly — the library does not infer it from `rayon::current_num_threads()`. The Python wrappers release the GIL implicitly via PyO3 while inside `ruhvro` calls, which is the whole point of the project per the README.

### Avro union handling

Nullable Avro fields (`["null", T]`) become nullable Arrow fields of type `T`. Wider unions (e.g. `["string","int","boolean"]`) become Arrow `Union` types. See `is_simple_null_union_type` in `serialization_containers.rs` and the matching logic in `schema_translate.rs` — any changes to union behavior must stay consistent across both sides.

## Release / CI

`.github/workflows/CI.yml` is maturin-generated. Tagged pushes (`refs/tags/*`) build wheels for linux/macos/windows across multiple targets and publish to PyPI. The top-level `Cargo.toml` and `ruhvro/Cargo.toml` both carry a `version` — bump both together when releasing.
