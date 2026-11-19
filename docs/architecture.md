# Architecture and data flow

## Business question

A website operations team wants to understand traffic, response times, popular paths, and server-side failures. The input is a stream of immutable daily/batch CSV access-log files.

## Components

| Component | Responsibility | Why it is present |
|---|---|---|
| Amazon S3 raw prefix | Store original CSV batches | Durable landing area; separates arrival from processing |
| AWS Glue custom CSV classifier | Define expected CSV header/format | Makes schema discovery predictable |
| AWS Glue Crawler | Infer/register table and partitions | Publishes the S3 dataset in the Glue Data Catalog |
| Glue Data Catalog | Store table schema and partition metadata | Lets the ETL job read a named table |
| AWS Glue 6.0 Spark job | Validate and transform logs | Distributed batch-processing runtime |
| S3 processed prefixes | Store cleaned and aggregated Parquet | Columnar, compressed output for analytics |
| Excel script | Read analytics Parquet and create workbook | Lightweight reporting/reconciliation |

## Raw file layout

```text
s3://BUCKET/PROJECT_PREFIX/raw/access_logs/
  ingest_date=YYYY-MM-DD/
    batch_id=UNIQUE_ID/
      access_logs.csv
```

The crawler can discover the `ingest_date` and `batch_id` folder values as partition columns. One file per batch is the project's simple operating contract.

## Processing rules

1. Read the Catalog table using `create_dynamic_frame.from_catalog` and a stable `transformation_ctx`.
2. Parse event timestamp and numeric fields.
3. Normalize HTTP method and strip query strings from page paths.
4. Reject records with missing event IDs/IPs, invalid timestamps, unsupported methods, malformed paths, invalid HTTP status codes, negative/invalid response times, or negative/invalid bytes sent.
5. Deduplicate event IDs within the current input batch, retaining the latest event timestamp.
6. Write valid rows to Parquet and rejected/duplicate rows to a quarantine prefix.
7. Calculate daily metrics, top ten pages, and grouped 5xx server errors.
8. Write a run-summary JSON object.
9. Commit the Glue bookmark only after outputs are written.

## Output layout

```text
s3://BUCKET/PROJECT_PREFIX/processed/
  cleaned_logs/ingest_date=.../batch_id=.../event_date=.../
  quarantine/ingest_date=.../batch_id=.../
  analytics/
    daily_metrics/ingest_date=.../batch_id=.../
    top_pages/ingest_date=.../batch_id=.../
    server_errors/ingest_date=.../batch_id=.../
  run_summaries/run_id=.../summary.json
```

## Bookmark and retry semantics

The Glue job uses `job.init`, a stable source `transformation_ctx`, and `job.commit`. S3 job bookmarks track processed source objects. The job writes to batch/date partitions with Spark dynamic partition overwrite, so a retry of the same immutable batch replaces the partitions for that batch rather than appending another copy. Do not re-use a `batch_id` for a different file. If you reset a bookmark, expect old files to be processed again; the batch-partition overwrite reduces duplicate-output risk for the same batch.

Bookmarks do not detect an in-place change to a previously processed object. Upload corrected data as a new immutable batch.

## Why no Airflow, Snowflake, or Glue Workflow?

This project is intentionally a small Glue-native batch pipeline. One crawler followed by one Glue job is enough to teach S3, the Data Catalog, PySpark transformations, bookmarks, and Parquet outputs. Adding Airflow or another warehouse would duplicate capabilities without improving this learning objective.

## Runtime choice

The CloudFormation template pins AWS Glue 6.0. AWS documentation lists Glue 6.0 with Spark 4.1.1 and Python 3.13. The ETL job uses standard PySpark transformations and Glue's DynamicFrame source reader.
