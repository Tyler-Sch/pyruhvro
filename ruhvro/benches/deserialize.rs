//! Criterion benchmarks for ruhvro's deserialize path.
//! Compares single-threaded, spawn_blocking, tokio::spawn, and rayon
//! across 1k / 10k / 100k record counts.

mod common;

use apache_avro::Schema as AvroSchema;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use ruhvro::deserialize::{
    per_datum_deserialize,
    per_datum_deserialize_threaded,
    per_datum_deserialize_threaded_spawn,
};

const SIZES: &[usize] = &[1_000, 10_000];

fn run_group(
    c: &mut Criterion,
    schema_name: &str,
    parsed: AvroSchema,
    encoded: Vec<Vec<u8>>,
    n: usize,
) {
    let refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
    let group_name = format!("{schema_name}/{n}");
    let mut group = c.benchmark_group(&group_name);
    group.throughput(Throughput::Elements(n as u64));

    group.bench_function("single_threaded", |b| {
        b.iter(|| black_box(per_datum_deserialize(black_box(&refs), black_box(&parsed)).unwrap()))
    });

    group.bench_function("spawn_blocking", |b| {
        b.iter(|| {
            black_box(per_datum_deserialize_threaded(
                black_box(refs.clone()), black_box(&parsed), common::NUM_CHUNKS,
            ).unwrap())
        })
    });

    group.bench_function("tokio_spawn", |b| {
        b.iter(|| {
            black_box(per_datum_deserialize_threaded_spawn(
                black_box(refs.clone()), black_box(&parsed), common::NUM_CHUNKS,
            ).unwrap())
        })
    });

    group.finish();
}

fn bench_flat_primitives(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::flat_primitives(n);
        run_group(c, "flat_primitives", parsed, encoded, n);
    }
}

fn bench_nested_struct(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::nested_struct(n);
        run_group(c, "nested_struct", parsed, encoded, n);
    }
}

fn bench_array_and_map(c: &mut Criterion) {
    for &n in SIZES {
        let (parsed, encoded) = common::array_and_map(n);
        run_group(c, "array_and_map", parsed, encoded, n);
    }
}

criterion_group!(
    benches,
    bench_flat_primitives,
    bench_nested_struct,
    bench_array_and_map,
);
criterion_main!(benches);
