//! Criterion benchmarks for ruhvro's serialize path.
//!
//! Each bench:
//!   1. Builds N avro-encoded records, deserializes them once into a
//!      `RecordBatch` (in setup, outside the timing loop).
//!   2. Measures `serialize_record_batch` with 1 chunk (single-threaded) and
//!      8 chunks (rayon-parallel).
//!
//! Run with `cargo bench -p ruhvro --bench serialize`.

mod common;

use apache_avro::Schema as AvroSchema;
use arrow::array::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ruhvro::deserialize::per_datum_deserialize;
use ruhvro::serialize::serialize_record_batch;

const N: usize = 1_000;

fn prepare_batch(parsed: &AvroSchema, encoded: &[Vec<u8>]) -> RecordBatch {
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    per_datum_deserialize(&refs, parsed).unwrap()
}

fn run_group(c: &mut Criterion, name: &str, parsed: AvroSchema, batch: RecordBatch) {
    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("serialize_record_batch_1chunk", |b| {
        b.iter(|| {
            // RecordBatch holds Arc'd columns, so .clone() is cheap (refcount bumps).
            let bytes = serialize_record_batch(black_box(batch.clone()), black_box(&parsed), 1)
                .unwrap();
            black_box(bytes);
        })
    });

    group.bench_function("serialize_record_batch_8chunks", |b| {
        b.iter(|| {
            let bytes = serialize_record_batch(
                black_box(batch.clone()),
                black_box(&parsed),
                common::NUM_CHUNKS,
            )
            .unwrap();
            black_box(bytes);
        })
    });

    group.finish();
}

fn bench_flat_primitives(c: &mut Criterion) {
    let (parsed, encoded) = common::flat_primitives(N);
    let batch = prepare_batch(&parsed, &encoded);
    run_group(c, "flat_primitives", parsed, batch);
}

fn bench_nullable_primitives(c: &mut Criterion) {
    let (parsed, encoded) = common::nullable_primitives(N);
    let batch = prepare_batch(&parsed, &encoded);
    run_group(c, "nullable_primitives", parsed, batch);
}

fn bench_nested_struct(c: &mut Criterion) {
    let (parsed, encoded) = common::nested_struct(N);
    let batch = prepare_batch(&parsed, &encoded);
    run_group(c, "nested_struct", parsed, batch);
}

fn bench_array_and_map(c: &mut Criterion) {
    let (parsed, encoded) = common::array_and_map(N);
    let batch = prepare_batch(&parsed, &encoded);
    run_group(c, "array_and_map", parsed, batch);
}

criterion_group!(
    benches,
    bench_flat_primitives,
    bench_nullable_primitives,
    bench_nested_struct,
    bench_array_and_map
);
criterion_main!(benches);
