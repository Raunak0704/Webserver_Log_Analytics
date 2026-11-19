# Interview guide

## 30-second project explanation

"I built a small web server log analytics pipeline using Amazon S3 and AWS Glue. Synthetic access logs arrive as CSV batches in S3. A Glue Crawler discovers the schema and registers the dataset in the Glue Data Catalog. A Glue 6.0 PySpark job reads new source files using Glue job bookmarks, validates and deduplicates events, quarantines bad records, writes cleaned Parquet data, and produces daily traffic, top-page, and server-error summaries. I also added an Excel reporting script and CloudFormation deployment resources."

## Concepts to be able to explain

### S3 raw and processed zones

Raw retains the original source files. Processed holds curated Parquet datasets. Keeping the raw files allows inspection and controlled replay.

### Glue Crawler

A crawler samples data in a configured S3 target, infers its schema/partitions, and updates the Glue Data Catalog. It does not perform the record-level transformation in this project.

### Glue Data Catalog

A central metadata catalogue that stores table definitions and partitions. The ETL job reads the crawler-created table by database and table name.

### Glue Spark job

A managed Apache Spark batch job. Spark DataFrames are used for parsing, validation, deduplication, aggregation, and Parquet writes.

### Job bookmarks

Glue bookmarks retain source-processing state for supported sources. For this S3 source, the bookmark tracks input files, not row-level changes inside a file. The job calls `job.init`, supplies a stable `transformation_ctx`, and calls `job.commit` after its outputs are complete.

### Quarantine

Rows that fail validation are not silently discarded. They are written to a separate output with a reason such as `invalid_event_time` or `invalid_status_code`.

### Parquet

A compressed columnar file format that is useful for analytical workloads. The output is partitioned by ingestion date and batch; cleaned logs are additionally partitioned by event date.

### Why PySpark?

It provides distributed DataFrame processing in the managed Glue Spark runtime. The project uses Spark SQL functions and aggregations instead of loading all records into a single Pandas DataFrame.

## Likely questions

**Why use a crawler?**

To infer and register the raw CSV schema and S3 partition metadata in the Glue Data Catalog. This keeps the ETL job from hardcoding the raw table schema in its source reader.

**Does a Glue bookmark track individual log records?**

No. With this S3 source, it tracks processed source objects/files. If an object is replaced in place, do not assume Glue will detect the change. Upload a corrected immutable file under a new batch key.

**What happens to invalid records?**

They are written to a quarantine Parquet dataset with one or more rejection reasons. This preserves evidence for debugging rather than silently dropping bad data.

**Why deduplicate event IDs?**

A source batch may contain repeated event IDs. The job keeps the latest timestamp within the set of files read in that run and routes the extra copies to quarantine.

**Why Parquet rather than CSV for output?**

Parquet stores data column-wise, supports compression, and preserves schema metadata, making it a practical format for analytics and downstream processing.

**How does the job produce top pages?**

It groups valid events by ingestion batch, event date, and normalized path, calculates request/error counts and average response time, ranks paths with a Spark window function, and keeps the top ten per date/batch.

**Why no Airflow?**

The learning objective is AWS Glue itself. For this small workflow, a crawler followed by a Glue job is enough. Airflow would be an additional orchestrator rather than a required transformation component.

**What are the limitations?**

This is a batch file pipeline, not real-time streaming. The bookmarks track source files, aggregates are per ingestion batch, and deduplication is within the files processed by one run. It is not a global event ledger or all-time reporting warehouse.

## Be precise in resume/interview claims

Only claim that the AWS workflow was deployed and executed after you have run it in your own AWS account and inspected successful Glue job runs and S3 outputs. Until then, describe it as an implemented project with local-tested generation/preview code and AWS deployment assets prepared.
