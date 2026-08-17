//! Reads partitioned parquet from a SeaweedFS (S3-compatible) bucket via DataFusion,
//! applies a single `eq` / `gt` / `lt` filter, prints the first rows and reports how
//! many matched.
//!
//! Usage: <bucket> <path> <column> <op:eq|gt|lt> <value> [partCol:type,...]
//! Env:   S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY
//!
//! `partCol:type,...` declares Hive-style partition columns (dirs like `region=eu`),
//! e.g. `region:Utf8,year:Int32`. Unlike parquet4s, DataFusion does not auto-discover
//! partition columns — they must be declared to be visible/queryable.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use url::Url;

const PREVIEW_ROWS: usize = 20;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 6 {
        eprintln!(
            "Usage: {} <bucket> <path> <column> <op:eq|gt|lt> <value> [partCol:type,...]\n\
             Env:   S3_ENDPOINT (default http://localhost:8333), S3_ACCESS_KEY, S3_SECRET_KEY",
            args[0]
        );
        std::process::exit(2);
    }
    let bucket = &args[1];
    let path = &args[2];
    let column = &args[3];
    let op = &args[4];
    let value = &args[5];
    let part_spec = args.get(6).map(String::as_str).unwrap_or_default();

    let endpoint =
        std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:8333".to_string());
    let access = std::env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "any".to_string());
    let secret = std::env::var("S3_SECRET_KEY").unwrap_or_else(|_| "any".to_string());
    // QUIET=1 suppresses the row preview so it doesn't pollute benchmark timings.
    let quiet = std::env::var("QUIET")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // SeaweedFS: path-style addressing, plain HTTP allowed. Region is required by the
    // builder but not meaningful for SeaweedFS.
    let s3 = AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(&access)
        .with_secret_access_key(&secret)
        .with_region("us-east-1")
        .with_allow_http(true)
        .build()?;

    let ctx = SessionContext::new();
    let store_url = Url::parse(&format!("s3://{bucket}"))?;
    ctx.runtime_env()
        .register_object_store(&store_url, Arc::new(s3));

    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(parse_partition_cols(part_spec));

    let table_path = format!("s3://{bucket}/{path}");
    eprintln!("Reading {table_path} with filter [{column} {op} {value}]");
    ctx.register_listing_table("t", &table_path, listing_options, None, None)
        .await?;

    let literal = parse_literal(value);
    let predicate = match op.as_str() {
        "eq" => col(column.as_str()).eq(literal),
        "gt" => col(column.as_str()).gt(literal),
        "lt" => col(column.as_str()).lt(literal),
        other => {
            eprintln!("Unsupported op '{other}' (expected eq|gt|lt)");
            std::process::exit(2);
        }
    };

    let started = std::time::Instant::now();
    let df = ctx.table("t").await?.filter(predicate)?;

    // Count all matches, then (unless quiet) show a small preview.
    let batches = df.clone().collect().await?;
    let matched: usize = batches.iter().map(|b| b.num_rows()).sum();
    if !quiet {
        df.limit(0, Some(PREVIEW_ROWS))?.show().await?;
    }

    println!("Matched rows: {matched} in {:.1?}", started.elapsed());
    Ok(())
}

/// Numeric-looking values become typed literals (`i64`, then `f64`); anything else is
/// a Utf8 string. The chosen type must match the column's actual parquet type.
fn parse_literal(value: &str) -> Expr {
    if let Ok(i) = value.parse::<i64>() {
        lit(i)
    } else if let Ok(f) = value.parse::<f64>() {
        lit(f)
    } else {
        lit(value.to_string())
    }
}

/// Parses `name:type,name:type` into DataFusion partition-column descriptors.
fn parse_partition_cols(spec: &str) -> Vec<(String, DataType)> {
    spec.split(',')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut parts = pair.splitn(2, ':');
            let name = parts.next().unwrap_or_default().to_string();
            let ty = match parts.next().unwrap_or("Utf8") {
                "Int32" => DataType::Int32,
                "Int64" => DataType::Int64,
                "Float64" => DataType::Float64,
                _ => DataType::Utf8,
            };
            (name, ty)
        })
        .collect()
}
