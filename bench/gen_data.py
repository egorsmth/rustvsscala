#!/usr/bin/env python3
"""Generate a Hive-partitioned parquet dataset for benchmarking.

Writes <out>/region=<r>/*.parquet with columns:
  id (int64), age (int64), score (double), name (string); partitioned by `region`.

Requires pyarrow:  pip install pyarrow

Usage:
  python3 bench/gen_data.py --rows 1000000 --out bench/data

Then create the bucket and upload (path-style, plain HTTP), e.g. with the AWS CLI:
  aws --endpoint-url http://localhost:8333 s3 mb s3://demo
  aws --endpoint-url http://localhost:8333 s3 cp --recursive bench/data s3://demo/events/

Query it:
  bench/bench.sh demo events/ age gt 30            # filter a data column
  bench/bench.sh demo events/ region eq eu region:Utf8   # filter the partition column
"""
import argparse
import random

import pyarrow as pa
import pyarrow.parquet as pq

REGIONS = ["eu", "us", "apac", "latam"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--out", default="bench/data")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    n = args.rows
    table = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "age": pa.array([random.randint(18, 90) for _ in range(n)], pa.int64()),
            "score": pa.array([random.random() * 100 for _ in range(n)], pa.float64()),
            "name": pa.array([f"user_{i}" for i in range(n)], pa.string()),
            "region": pa.array([REGIONS[i % len(REGIONS)] for i in range(n)], pa.string()),
        }
    )
    pq.write_to_dataset(table, root_path=args.out, partition_cols=["region"])
    print(f"wrote {n} rows to {args.out} partitioned by region={REGIONS}")


if __name__ == "__main__":
    main()
