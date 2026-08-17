package rustvsp4s

import com.github.mjakubowski84.parquet4s.{Col, Filter, ParquetReader, Path, RowParquetRecord}
import org.apache.hadoop.conf.Configuration

/** Reads partitioned parquet from a SeaweedFS (S3-compatible) bucket via the s3a
  * connector, applies a single `eq` / `gt` / `lt` filter (pushed down into parquet),
  * prints the first rows and reports how many matched.
  *
  * Usage: <bucket> <path> <column> <op:eq|gt|lt> <value>
  * Env:   S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY
  */
object Main {

  private val PreviewRows = 20

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      Console.err.println(
        "Usage: <bucket> <path> <column> <op:eq|gt|lt> <value>\n" +
          "Env:   S3_ENDPOINT (default http://localhost:8333), S3_ACCESS_KEY, S3_SECRET_KEY"
      )
      sys.exit(2)
    }
    val Array(bucket, path, column, op, value) = args.take(5)

    val endpoint  = sys.env.getOrElse("S3_ENDPOINT", "http://localhost:8333")
    val accessKey = sys.env.getOrElse("S3_ACCESS_KEY", "any")
    val secretKey = sys.env.getOrElse("S3_SECRET_KEY", "any")
    // QUIET=1 suppresses per-row output so it doesn't pollute benchmark timings.
    val quiet     = sys.env.get("QUIET").exists(v => v == "1" || v.equalsIgnoreCase("true"))

    val options = ParquetReader.Options(hadoopConf = hadoopConf(endpoint, accessKey, secretKey))
    val filter  = buildFilter(column, op, value)
    val target  = Path(s"s3a://$bucket/$path")

    Console.err.println(s"Reading $target with filter [$column $op $value]")

    val started  = System.nanoTime()
    val iterable = ParquetReader.generic
      .options(options)
      .filter(filter)
      .read(target)

    var matched = 0L
    var shown   = 0
    try {
      val it = iterable.iterator
      while (it.hasNext) {
        val record: RowParquetRecord = it.next()
        if (!quiet && shown < PreviewRows) {
          println(record)
          shown += 1
        }
        matched += 1
      }
    } finally {
      iterable.close()
    }

    val elapsedMs = (System.nanoTime() - started) / 1e6
    println(f"Matched rows: $matched in $elapsedMs%.1f ms")
  }

  /** SeaweedFS speaks S3 with path-style addressing and (usually) plain HTTP. */
  private def hadoopConf(endpoint: String, accessKey: String, secretKey: String): Configuration = {
    val conf = new Configuration()
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.s3a.endpoint", endpoint)
    conf.set("fs.s3a.access.key", accessKey)
    conf.set("fs.s3a.secret.key", secretKey)
    conf.set("fs.s3a.path.style.access", "true")
    conf.setBoolean("fs.s3a.connection.ssl.enabled", endpoint.startsWith("https"))
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    conf
  }

  /** Builds a pushed-down parquet predicate. Numeric-looking values are compared as
    * `Long`, everything else as `String`; the codec picked here must match the actual
    * column type in the file. */
  private def buildFilter(column: String, op: String, value: String): Filter = {
    val col = Col(column)
    value.toLongOption match {
      case Some(v) =>
        op match {
          case "eq" => col === v
          case "gt" => col > v
          case "lt" => col < v
          case _    => unsupported(op)
        }
      case None =>
        op match {
          case "eq" => col === value
          case "gt" => col > value
          case "lt" => col < value
          case _    => unsupported(op)
        }
    }
  }

  private def unsupported(op: String): Nothing =
    throw new IllegalArgumentException(s"Unsupported op '$op' (expected eq|gt|lt)")
}
