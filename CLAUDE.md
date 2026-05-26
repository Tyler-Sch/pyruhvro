# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

Cargo workspace with two crates:

- `ruhvro/` — the core Rust library (published as `ruhvro` on crates.io). Pure Rust API for serializing/deserializing schemaless Avro to/from Arrow `RecordBatch`es. Has no Python deps.
- `src/lib.rs` (top-level) — the `pyruhvro` PyO3 extension module that wraps `ruhvro` and exposes it to Python via maturin. Exposes `deserialize_array`, `deserialize_array_threaded`, `serialize_record_batch`, plus `_spawn` variants that use `tokio::spawn` instead of `spawn_blocking`. Also maintains a `String -> Arc<Schema>` cache so repeat calls don't re-parse the schema JSON, and releases the GIL via `Python::detach` around every Rust call so multiple Python threads can run concurrently.

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

- `deserialize.rs` — public entry points `parse_schema`, `per_datum_deserialize` (single-threaded), `per_datum_deserialize_threaded` (tokio `spawn_blocking`, splits input into `num_chunks` slices and returns one `RecordBatch` per chunk), plus `_spawn` variant on the work-stealing async pool. Threaded variants take an `Arc<Schema>` so callers can share one parsed schema across many calls without re-cloning.
- `serialize.rs` — public entry point `serialize_record_batch` (same `Arc<Schema>` convention). Converts the `RecordBatch` into a `StructArray`, slices it into `num_chunks`, and serializes each slice in parallel via tokio `spawn_blocking` into a `GenericBinaryArray<i32>` of Avro datums.
- `fast_decode.rs` / `fast_encode.rs` — schema-walking decoder/encoder that bypasses the `apache_avro::Value` tree and writes straight into Arrow builders. Gated by `is_supported(schema)`; falls back to the `Value`-based path for schemas containing types outside the supported subset.
- `schema_translate.rs` — converts an `apache_avro::Schema` into an `arrow::datatypes::Schema`. This is the source of truth for type mapping (e.g. nullable-union → nullable Arrow field, multi-variant unions → Arrow `Union`, Avro `map` → Arrow `Map`, logical types → Arrow temporal types).
- `complex.rs` — `AvroToArrowBuilder` and its `Struct`/`List`/`Union`/`Map`/`Primitive` variants. This is the *deserialize* side: walks Avro `Value`s into Arrow builders. The `add_val!` macro and `get_val_from_possible_union` helper handle the common "value might be wrapped in a union" case.
- `serialization_containers.rs` — the *serialize* side: `ArrayContainers` walks Arrow arrays column-wise and re-emits `apache_avro::types::Value`s, then `to_avro_datum` encodes each row.

### Threading model

A single global tokio multi-thread runtime (`OnceLock` in `ruhvro/src/lib.rs`) services all parallel work — created on first call, alive for the process lifetime. Worker threads are parked when idle, so the runtime costs nothing when unused.

Both threaded paths require the caller to pass `num_chunks` explicitly — the library does not infer it. `num_chunks` is clamped to `[1, max(rows, 1)]` so `0` doesn't panic and overshooting the row count doesn't spawn empty tasks. The Python wrappers explicitly release the GIL with `py.detach(...)` around every Rust call, so multiple Python threads can call into pyruhvro concurrently and benefit from the internal parallelism.

### Avro union handling

Nullable Avro fields (`["null", T]`) become nullable Arrow fields of type `T`. Wider unions (e.g. `["string","int","boolean"]`) become Arrow `Union` types. See `is_simple_null_union_type` in `serialization_containers.rs` and the matching logic in `schema_translate.rs` — any changes to union behavior must stay consistent across both sides.

## Release / CI

`.github/workflows/CI.yml` is maturin-generated. Tagged pushes (`refs/tags/*`) build wheels for linux/macos/windows across multiple targets and publish to PyPI. The top-level `Cargo.toml` and `ruhvro/Cargo.toml` both carry a `version` — bump both together when releasing.
