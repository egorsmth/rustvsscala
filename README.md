# rustvsp4s — reading partitioned parquet from SeaweedFS

Two small, functionally-equivalent apps that read **partitioned parquet** from a
**SeaweedFS** (S3-compatible) bucket and apply a single `eq` / `gt` / `lt` filter:

| | Language | Library | Location |
|---|---|---|---|
| Scala | Scala 2.13 | [parquet4s](https://github.com/mjakubowski84/parquet4s) + Hadoop `s3a` | [`scala/`](scala/) |
| Rust  | Rust (stable) | [DataFusion](https://datafusion.apache.org/) + `object_store` | [`rust/`](rust/) |

Both take the same CLI and read S3 credentials from the same env vars.

```
<bucket> <path> <column> <op:eq|gt|lt> <value>
```

Connection via env vars (defaults shown):

```
S3_ENDPOINT=http://localhost:8333
S3_ACCESS_KEY=any
S3_SECRET_KEY=any
```

## Build status

Both compile in this repo's toolchain:

- Scala — `cd scala && sbt compile` ✅ and `sbt assembly` (fat jar) ✅
- Rust  — `cd rust && cargo check` ✅ (DataFusion 43 / arrow 53)

> The Rust side needs a modern toolchain (MSRV ~1.79+); a bare `rustc 1.75` is too
> old. Install stable via [rustup](https://rustup.rs): `curl https://sh.rustup.rs -sSf | sh`.

## Run

### 1. Start SeaweedFS

```bash
docker compose up -d          # S3 endpoint on http://localhost:8333
```

### 2. Create a bucket + upload partitioned parquet

Point any S3 client at the endpoint (path-style addressing, plain HTTP). SeaweedFS
accepts any keys by default. Lay the data out Hive-style, e.g.:

```
s3://demo/events/region=eu/part-0.parquet
s3://demo/events/region=us/part-0.parquet
```

(One quick way to generate sample data: `pyarrow.parquet.write_to_dataset(..., partition_cols=["region"])`, then `aws --endpoint-url http://localhost:8333 s3 cp --recursive ...`.)

### 3. Query

Scala:

```bash
cd scala
S3_ENDPOINT=http://localhost:8333 S3_ACCESS_KEY=any S3_SECRET_KEY=any \
  sbt "run demo events/ age gt 30"
```

Rust:

```bash
cd rust
S3_ENDPOINT=http://localhost:8333 S3_ACCESS_KEY=any S3_SECRET_KEY=any \
  cargo run --release -- demo events/ age gt 30
```

Each prints up to 20 matching rows and `Matched rows: <n> in <ms>`. Set `QUIET=1` to
suppress the row preview (used by the benchmark harness).

## Benchmarking (speed / memory / CPU)

Measured with **external tools**, not from inside the apps, so both processes are
compared with the same yardstick. No app-logic changes — the only build addition is a
fat jar so the JVM app is timed as `java -jar`, not `sbt`.

```bash
# 1. Build both artifacts
(cd scala && sbt assembly)           # -> scala/target/scala-2.13/reader.jar
(cd rust  && cargo build --release)  # -> rust/target/release/parquet-seaweed-reader

# 2. Generate + upload sample data (needs pyarrow + an S3 client)
python3 bench/gen_data.py --rows 1000000 --out bench/data
aws --endpoint-url http://localhost:8333 s3 mb s3://demo
aws --endpoint-url http://localhost:8333 s3 cp --recursive bench/data s3://demo/events/

# 3. Run the harness (optionally: cargo install hyperfine)
S3_ENDPOINT=http://localhost:8333 S3_ACCESS_KEY=any S3_SECRET_KEY=any \
  bench/bench.sh demo events/ age gt 30
```

`bench/bench.sh` reports:

| Metric | Tool | Notes |
|---|---|---|
| Wall-clock | `hyperfine` (warmup + N runs) | falls back to each app's internal timer if not installed |
| Peak RSS | `/usr/bin/time -v` | `Maximum resident set size` |
| CPU | `/usr/bin/time -v` | user+sys time and `%CPU` (>100% = multi-core) |

**Interpreting the results — read this before drawing conclusions:**

- **Two time numbers matter.** hyperfine wall-clock includes JVM startup (~0.3–1s);
  each app also prints an internal "read" timer (steady-state query cost). Compare
  like-for-like — startup is a real cost but it's not "the language being slow."
- **Scala peak RSS = the whole JVM** (heap + metaspace + runtime), capped by
  `JVM_XMX` (default `1g`), not just the workload. Rust's RSS ≈ real working set.
  Don't read the gap as "the language needs 20× more memory."
- **CPU% will favor Rust** because DataFusion parallelizes across cores while
  parquet4s reads single-threaded — an engine-architecture difference, not a
  language one. Env knobs: `JVM_XMX`, `RUNS`.

## Filters

`op` is one of `eq`, `gt`, `lt`. The `<value>` is parsed as a number when it looks
like one (compared as `Long`/`i64`, then `Double`/`f64`), otherwise as a string. The
inferred type must match the column's actual parquet type. Both engines push the
predicate down into the parquet reader.

## Partitioning — an important difference

- **parquet4s** auto-discovers Hive-style partition columns (`region=eu`) when you
  point it at the directory. Nothing extra to declare.
- **DataFusion** does *not* auto-discover them. The Rust app takes an optional 6th
  argument declaring them, e.g. `... age gt 30 region:Utf8`. Without it the `region`
  column is not exposed (though the files under those dirs are still read).

Prefer filtering on a **regular data column** for an apples-to-apples demo; partition
columns are derived from the path and handled differently on each side.

## Fair-comparison caveat

This is **not** a clean "Rust vs Scala language" benchmark. The libraries have
different architectures:

- **parquet4s** is a row-oriented reader built on the Hadoop parquet reader — it
  streams `RowParquetRecord`s one at a time.
- **DataFusion** is a vectorized, multi-threaded query engine — it reads columnar
  Arrow batches and can parallelize across files/row-groups.

On wall-clock time DataFusion will usually win, largely for architectural reasons
(vectorization + parallelism) rather than the language. Treat any timing as a
comparison of *these two stacks*, not of the languages themselves. For a lower-level
Rust counterpart you could instead use the raw `parquet` + `object_store` crates and
walk the partition directories manually — closer to parquet4s in spirit, but more code.
