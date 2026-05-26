"""Benchmark pyruhvro vs fastavro across dataset sizes and chunk counts."""
import io
import json
import timeit

import fastavro
from pyruhvro import deserialize_array_threaded, serialize_record_batch

import generate_avro

RECORD_COUNTS = [500, 5_000, 50_000]
CHUNK_COUNTS  = [1, 2, 4, 8, 16]
TIMEIT_REPS   = 3   # best-of-N repeats per measurement
MIN_SECONDS   = 1.5 # timeit runs enough loops to fill at least this long


def measure(stmt, setup_vars, seconds=MIN_SECONDS, reps=TIMEIT_REPS):
    """Return best ms-per-loop over `reps` rounds of auto-scaled timeit."""
    t = timeit.Timer(stmt=stmt, globals=setup_vars)
    n, _ = t.autorange()
    n = max(n, 3)
    times = t.repeat(reps, n)
    return min(times) / n * 1000  # ms


def fastavro_serialize(records, parsed_schema):
    out = []
    for r in records:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, parsed_schema, r)
        out.append(buf.getvalue())
    return out


def fastavro_deserialize(serialized, parsed_schema):
    return [fastavro.schemaless_reader(io.BytesIO(b), parsed_schema) for b in serialized]


print(f"\n{'Dataset':>8}  {'Chunks':>6}  {'py-ser ms':>10}  {'py-de ms':>10}  {'fa-ser ms':>10}  {'fa-de ms':>10}  {'ser-x':>6}  {'de-x':>5}")
print("-" * 88)

for n_rec in RECORD_COUNTS:
    records = generate_avro.generate_records(n_rec)
    serialized = generate_avro.get_serialized_records(records)
    parsed_schema = generate_avro.parsed_schema
    schema_str = generate_avro.schema_string

    # fastavro baseline (no chunks, run once per dataset size)
    g = dict(records=records, serialized=serialized, parsed_schema=parsed_schema,
             fastavro_serialize=fastavro_serialize, fastavro_deserialize=fastavro_deserialize)
    fa_ser = measure("fastavro_serialize(records, parsed_schema)", g)
    fa_de  = measure("fastavro_deserialize(serialized, parsed_schema)", g)

    first_chunk = True
    for n_chunks in CHUNK_COUNTS:
        g2 = dict(serialized=serialized, schema_str=schema_str,
                  serialize_record_batch=serialize_record_batch,
                  deserialize_array_threaded=deserialize_array_threaded,
                  n_chunks=n_chunks)

        # pre-deserialize so serialize bench has record_batches ready
        record_batches = deserialize_array_threaded(serialized, schema_str, n_chunks)
        g2["record_batches"] = record_batches

        py_ser = measure("[ serialize_record_batch(rb, schema_str, n_chunks) for rb in record_batches ]", g2)
        py_de  = measure("deserialize_array_threaded(serialized, schema_str, n_chunks)", g2)

        fa_ser_col = f"{fa_ser:>10.2f}" if first_chunk else f"{'':>10}"
        fa_de_col  = f"{fa_de:>10.2f}"  if first_chunk else f"{'':>10}"
        first_chunk = False

        ser_x = fa_ser / py_ser
        de_x  = fa_de  / py_de
        print(f"{n_rec:>8,}  {n_chunks:>6}  {py_ser:>10.2f}  {py_de:>10.2f}  {fa_ser_col}  {fa_de_col}  {ser_x:>5.1f}x  {de_x:>4.1f}x")

    print()
