//! Criterion benchmarks for ruhvro's serialize path.
//! Compares single-threaded, spawn_blocking (tokio), tokio::spawn, and rayon.
//!
//! Run with `cargo bench -p ruhvro --bench serialize`.

mod common;

use apache_avro::Schema as AvroSchema;
use arrow::array::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ruhvro::deserialize::per_datum_deserialize;
use ruhvro::serialize::{
    serialize_record_batch,
    serialize_record_batch_spawn,
};

const SIZES: &[usize] = &[1_000, 10_000];

fn prepare_batch(parsed: &AvroSchema, encoded: &[Vec<u8>]) -> RecordBatch {
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    per_datum_deserialize(&refs, parsed).unwrap()
}

fn run_group(c: &mut Criterion, schema_name: &str, parsed: AvroSchema, batch: RecordBatch, n: usize) {
    let group_name = format!("{schema_name}/{n}");
    let mut group = c.benchmark_group(&group_name);
    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("single_threaded", |b| {
        b.iter(|| {
            black_box(serialize_record_batch(black_box(batch.clone()), black_box(&parsed), 1).unwrap())
        })
    });

    group.bench_function("spawn_blocking", |b| {
        b.iter(|| {
            black_box(serialize_record_batch(
                black_box(batch.clone()), black_box(&parsed), common::NUM_CHUNKS,
            ).unwrap())
        })
    });

    group.bench_function("tokio_spawn", |b| {
        b.iter(|| {
            black_box(serialize_record_batch_spawn(
                black_box(batch.clone()), black_box(&parsed), common::NUM_CHUNKS,
            ).unwrap())
        })
    });

    group.finish();
}

fn bench_flat_primitives(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::flat_primitives(n);
        let batch = prepare_batch(&parsed, &encoded);
        run_group(c, "flat_primitives", parsed, batch, n);
    }
}

fn bench_nullable_primitives(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::nullable_primitives(n);
        let batch = prepare_batch(&parsed, &encoded);
        run_group(c, "nullable_primitives", parsed, batch, n);
    }
}

fn bench_nested_struct(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::nested_struct(n);
        let batch = prepare_batch(&parsed, &encoded);
        run_group(c, "nested_struct", parsed, batch, n);
    }
}

fn bench_array_and_map(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::array_and_map(n);
        let batch = prepare_batch(&parsed, &encoded);
        run_group(c, "array_and_map", parsed, batch, n);
    }
}

criterion_group!(
    benches,
    bench_flat_primitives,
    bench_nullable_primitives,
    bench_nested_struct,
    bench_array_and_map
);
criterion_main!(benches);
