//! Criterion benchmarks for ruhvro's deserialize path.
//!
//! Each bench:
//!   1. Builds N avro-encoded records once (in setup).
//!   2. Measures `per_datum_deserialize` (single-threaded) and
//!      `per_datum_deserialize_threaded` (8 chunks).
//!
//! Run with `cargo bench -p ruhvro --bench deserialize`.
//! HTML reports land in `target/criterion`.

mod common;

use apache_avro::Schema as AvroSchema;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ruhvro::deserialize::{per_datum_deserialize, per_datum_deserialize_threaded};

const N: usize = 1_000;

fn run_group(c: &mut Criterion, name: &str, parsed: AvroSchema, encoded: Vec<Vec<u8>>) {
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();

    let mut group = c.benchmark_group(name);
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function("per_datum_deserialize", |b| {
        b.iter(|| {
            let rb = per_datum_deserialize(black_box(&refs), black_box(&parsed)).unwrap();
            black_box(rb);
        })
    });

    group.bench_function("per_datum_deserialize_threaded", |b| {
        b.iter(|| {
            let rbs = per_datum_deserialize_threaded(
                black_box(refs.clone()),
                black_box(&parsed),
                common::NUM_CHUNKS,
            )
            .unwrap();
            black_box(rbs);
        })
    });

    group.finish();
}

fn bench_flat_primitives(c: &mut Criterion) {
    let (parsed, encoded) = common::flat_primitives(N);
    run_group(c, "flat_primitives", parsed, encoded);
}

fn bench_nullable_primitives(c: &mut Criterion) {
    let (parsed, encoded) = common::nullable_primitives(N);
    run_group(c, "nullable_primitives", parsed, encoded);
}

fn bench_nested_struct(c: &mut Criterion) {
    let (parsed, encoded) = common::nested_struct(N);
    run_group(c, "nested_struct", parsed, encoded);
}

fn bench_array_and_map(c: &mut Criterion) {
    let (parsed, encoded) = common::array_and_map(N);
    run_group(c, "array_and_map", parsed, encoded);
}

criterion_group!(
    benches,
    bench_flat_primitives,
    bench_nullable_primitives,
    bench_nested_struct,
    bench_array_and_map
);
criterion_main!(benches);
