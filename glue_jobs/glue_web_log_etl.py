"""AWS Glue 6.0 PySpark job for web access-log analytics.

Reads new CSV objects from the Glue Data Catalog using a Glue job bookmark,
cleans and validates records, writes cleaned/quarantined Parquet datasets, and
produces per-ingestion-batch analytics.

The job is intentionally batch-oriented. It does not maintain a data warehouse,
SCD dimensions, or custom high-watermark tables.
"""

import json
import sys
import uuid
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window


REQUIRED_COLUMNS = {
    "event_id",
    "event_time",
    "client_ip",
    "method",
    "path",
    "status_code",
    "response_time_ms",
    "bytes_sent",
    "user_agent",
    "referer",
}
ALLOWED_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def write_parquet(df, path, partition_columns):
    """Overwrite only touched partitions, making a retry of the same batch safe."""
    if df.rdd.isEmpty():
        print(f"No rows to write: {path}")
        return

    # The output is partitioned by immutable ingest batch. Dynamic overwrite means
    # a retry replaces only the batch/date partitions it is reprocessing.
    df.write.mode("overwrite").option("compression", "snappy").partitionBy(
        *partition_columns
    ).parquet(path)
    print(f"Wrote {path}")


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "CATALOG_DATABASE",
            "CATALOG_TABLE",
            "OUTPUT_BUCKET",
            "OUTPUT_PREFIX",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket = args["OUTPUT_BUCKET"]
    prefix = args["OUTPUT_PREFIX"].strip("/")
    base = f"s3://{bucket}/{prefix}/" if prefix else f"s3://{bucket}/"

    source_dyf = glue_context.create_dynamic_frame.from_catalog(
        database=args["CATALOG_DATABASE"],
        table_name=args["CATALOG_TABLE"],
        transformation_ctx="raw_access_log_source",
    )
    raw = source_dyf.toDF()
    raw = raw.toDF(*[column.lower() for column in raw.columns])

    missing = sorted(REQUIRED_COLUMNS - set(raw.columns))
    if missing:
        raise RuntimeError(
            "Glue Catalog table is missing required columns: " + ", ".join(missing)
        )

    if raw.rdd.isEmpty():
        print("No new source files were returned by the Glue bookmark.")
        job.commit()
        return

    run_id = uuid.uuid4().hex
    source = (
        raw.withColumn("event_id_clean", F.trim(F.col("event_id").cast("string")))
        .withColumn("event_time_text", F.trim(F.col("event_time").cast("string")))
        .withColumn(
            "event_ts",
            F.coalesce(
                F.expr("try_to_timestamp(event_time_text, 'yyyy-MM-dd''T''HH:mm:ss''Z''')"),
                F.expr("try_to_timestamp(event_time_text, 'yyyy-MM-dd HH:mm:ss')"),
                F.expr("try_to_timestamp(event_time_text)"),
            ),
        )
        .withColumn("client_ip_clean", F.trim(F.col("client_ip").cast("string")))
        .withColumn("method_clean", F.upper(F.trim(F.col("method").cast("string"))))
        .withColumn("path_clean", F.trim(F.col("path").cast("string")))
        .withColumn("status_code_num", F.expr("try_cast(status_code as int)"))
        .withColumn(
            "response_time_num", F.expr("try_cast(response_time_ms as double)")
        )
        .withColumn("bytes_sent_num", F.expr("try_cast(bytes_sent as long)"))
        .withColumn(
            "path_normalized",
            F.regexp_replace(F.col("path_clean"), r"\?.*$", ""),
        )
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_date", F.col("ingest_date").cast("string"))
        .withColumn("batch_id", F.col("batch_id").cast("string"))
    )

    invalid_reason = F.concat_ws(
        ";",
        F.when(
            F.col("event_id_clean").isNull() | (F.col("event_id_clean") == ""),
            F.lit("missing_event_id"),
        ),
        F.when(F.col("event_ts").isNull(), F.lit("invalid_event_time")),
        F.when(
            F.col("client_ip_clean").isNull() | (F.col("client_ip_clean") == ""),
            F.lit("missing_client_ip"),
        ),
        F.when(
            F.col("method_clean").isNull() | (~F.col("method_clean").isin(ALLOWED_METHODS)),
            F.lit("invalid_method"),
        ),
        F.when(
            F.col("path_normalized").isNull()
            | (F.col("path_normalized") == "")
            | (~F.col("path_normalized").startswith("/")),
            F.lit("invalid_path"),
        ),
        F.when(
            F.col("status_code_num").isNull()
            | (F.col("status_code_num") < 100)
            | (F.col("status_code_num") > 599),
            F.lit("invalid_status_code"),
        ),
        F.when(
            F.col("response_time_num").isNull()
            | (F.col("response_time_num") < 0)
            | (F.col("response_time_num") > 600000),
            F.lit("invalid_response_time"),
        ),
        F.when(
            F.col("bytes_sent_num").isNull() | (F.col("bytes_sent_num") < 0),
            F.lit("invalid_bytes_sent"),
        ),
    )
    classified = source.withColumn("rejection_reason", invalid_reason)

    invalid_rows = classified.filter(F.col("rejection_reason") != "").withColumn(
        "processed_at", F.current_timestamp()
    )
    valid_candidates = classified.filter(F.col("rejection_reason") == "")

    # De-duplicate event IDs within the set of files processed by this run.
    dedupe_window = Window.partitionBy("batch_id", "event_id_clean").orderBy(
        F.col("event_ts").desc(), F.col("source_file").desc()
    )
    ranked = valid_candidates.withColumn("_duplicate_rank", F.row_number().over(dedupe_window))
    duplicate_rows = (
        ranked.filter(F.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank", "rejection_reason")
        .withColumn("rejection_reason", F.lit("duplicate_event_id_within_batch"))
        .withColumn("processed_at", F.current_timestamp())
    )
    quarantine = invalid_rows.unionByName(duplicate_rows, allowMissingColumns=True)

    cleaned = (
        ranked.filter(F.col("_duplicate_rank") == 1)
        .drop("_duplicate_rank", "rejection_reason")
        .select(
            F.col("event_id_clean").alias("event_id"),
            F.col("event_ts"),
            F.to_date("event_ts").alias("event_date"),
            F.col("client_ip_clean").alias("client_ip"),
            F.col("method_clean").alias("method"),
            F.col("path_normalized").alias("path"),
            F.col("status_code_num").alias("status_code"),
            F.col("response_time_num").alias("response_time_ms"),
            F.col("bytes_sent_num").alias("bytes_sent"),
            F.col("user_agent").cast("string").alias("user_agent"),
            F.col("referer").cast("string").alias("referer"),
            F.col("ingest_date"),
            F.col("batch_id"),
            F.col("source_file"),
            F.current_timestamp().alias("processed_at"),
        )
        .withColumn("status_family", F.concat(F.substring(F.col("status_code").cast("string"), 1, 1), F.lit("xx")))
        .withColumn("is_error", F.col("status_code") >= 400)
        .withColumn("is_server_error", F.col("status_code") >= 500)
    )

    cleaned_path = base + "cleaned_logs/"
    quarantine_path = base + "quarantine/"
    write_parquet(
        cleaned,
        cleaned_path,
        ["ingest_date", "batch_id", "event_date"],
    )
    write_parquet(quarantine, quarantine_path, ["ingest_date", "batch_id"])

    if not cleaned.rdd.isEmpty():
        daily_metrics = cleaned.groupBy("ingest_date", "batch_id", "event_date").agg(
            F.count("*").alias("request_count"),
            F.countDistinct("client_ip").alias("unique_clients"),
            F.round(F.avg("response_time_ms"), 2).alias("avg_response_time_ms"),
            F.round(F.expr("percentile_approx(response_time_ms, 0.95)"), 2).alias(
                "p95_response_time_ms"
            ),
            F.sum(F.when(F.col("is_error"), 1).otherwise(0)).alias("error_requests"),
            F.sum(F.when(F.col("is_server_error"), 1).otherwise(0)).alias(
                "server_error_requests"
            ),
            F.sum("bytes_sent").alias("total_bytes_sent"),
        )
        write_parquet(
            daily_metrics,
            base + "analytics/daily_metrics/",
            ["ingest_date", "batch_id"],
        )

        page_metrics = cleaned.groupBy(
            "ingest_date", "batch_id", "event_date", "path"
        ).agg(
            F.count("*").alias("requests"),
            F.round(F.avg("response_time_ms"), 2).alias("avg_response_time_ms"),
            F.sum(F.when(F.col("is_error"), 1).otherwise(0)).alias("error_requests"),
        )
        page_window = Window.partitionBy(
            "ingest_date", "batch_id", "event_date"
        ).orderBy(F.col("requests").desc(), F.col("path").asc())
        top_pages = (
            page_metrics.withColumn("page_rank", F.row_number().over(page_window))
            .filter(F.col("page_rank") <= 10)
        )
        write_parquet(
            top_pages,
            base + "analytics/top_pages/",
            ["ingest_date", "batch_id"],
        )

        server_errors = (
            cleaned.filter(F.col("status_code") >= 500)
            .groupBy("ingest_date", "batch_id", "event_date", "status_code", "path")
            .agg(
                F.count("*").alias("error_count"),
                F.round(F.avg("response_time_ms"), 2).alias("avg_response_time_ms"),
            )
        )
        write_parquet(
            server_errors,
            base + "analytics/server_errors/",
            ["ingest_date", "batch_id"],
        )

    input_count = source.count()
    cleaned_count = cleaned.count()
    quarantined_count = quarantine.count()
    summary = {
        "run_id": run_id,
        "job_name": args["JOB_NAME"],
        "catalog_database": args["CATALOG_DATABASE"],
        "catalog_table": args["CATALOG_TABLE"],
        "input_rows": input_count,
        "cleaned_rows": cleaned_count,
        "quarantined_rows": quarantined_count,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=f"{prefix}/run_summaries/run_id={run_id}/summary.json",
        Body=json.dumps(summary, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    # Commit bookmarks only after all output datasets and the run summary are written.
    job.commit()
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
