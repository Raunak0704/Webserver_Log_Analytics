# Web Server Log Analytics with AWS Glue

A small, end-to-end AWS data-lake ETL project. It takes synthetic web access logs, lands them in Amazon S3, discovers their schema with an AWS Glue Crawler, transforms them with one AWS Glue PySpark job, and writes curated Parquet datasets and analytics back to S3.

**This project is intentionally different from the retail Snowflake/Airflow project.** It does not use Snowflake, PostgreSQL, Apache Airflow, a warehouse, SCD logic, or custom high-watermark tables.

## What it demonstrates

- Amazon S3 raw and processed data zones.
- AWS Glue Crawler and Glue Data Catalog.
- One AWS Glue 6.0 Spark ETL job using PySpark.
- CSV parsing, validation, deduplication, quarantine, and Parquet output.
- AWS Glue job bookmarks for tracking source files already processed.
- Basic traffic, page, and server-error analytics.
- Excel reporting from the resulting Parquet datasets.
- CloudFormation-based deployment and PowerShell run scripts.

## Architecture

```text
Synthetic access-log CSV
        |
        v
Amazon S3: raw/access_logs/ingest_date=.../batch_id=.../
        |
        v
AWS Glue Crawler + custom CSV classifier
        |
        v
AWS Glue Data Catalog table: raw_access_logs
        |
        v
AWS Glue 6.0 PySpark job (job bookmark enabled)
        |
        +--> processed/cleaned_logs/       (Parquet)
        +--> processed/quarantine/         (Parquet)
        +--> processed/analytics/
                 +--> daily_metrics/        (Parquet)
                 +--> top_pages/            (Parquet)
                 +--> server_errors/        (Parquet)
        +--> processed/run_summaries/      (JSON)
        |
        v
scripts/create_excel_report.py
        |
        v
Excel workbook
```

## Repository structure

```text
web_log_analytics_glue/
├── glue_jobs/
│   └── glue_web_log_etl.py          # Main AWS Glue PySpark job
├── infra/
│   ├── glue-stack.yaml              # IAM role, Catalog DB, crawler, Glue job
│   ├── deploy.ps1                   # Creates/configures S3 and deploys CloudFormation
│   ├── run_pipeline.ps1              # Runs crawler, waits, then runs Glue job
│   └── destroy.ps1                   # Deletes Glue stack; retains S3 data
├── scripts/
│   ├── generate_logs.py              # Synthetic CSV generator; optional S3 upload
│   ├── local_preview.py               # Pandas preview without AWS/Glue costs
│   └── create_excel_report.py         # Downloads processed Parquet and builds Excel
├── tests/
│   └── test_web_log_project.py        # Generator and local transformation tests
├── docs/
│   ├── architecture.md
│   ├── runbook.md
│   └── interview_guide.md
├── data/generated/                   # Local generated batches (git-ignored)
├── reports/                          # Excel output (git-ignored)
├── requirements.txt
└── README.md
```

## 1. Prerequisites

### Local-only run

- Python 3.10 or later (Python 3.13 is supported by the current Glue 6.0 runtime).
- PowerShell or a terminal.
- The Python packages in `requirements.txt`.

### AWS run

- An AWS account and a region where AWS Glue 6.0 is available.
- AWS CLI v2 configured with permission to create S3 buckets, IAM roles, CloudFormation stacks, and Glue resources.
- A globally unique S3 bucket name. The deployment script creates the bucket if it does not already exist.

The Glue job itself runs on AWS. You do not need to install Spark or Glue locally.

## 2. Local preview — no AWS account or cloud charges

This path lets you inspect generated data, validation results, and an Excel preview without launching Glue.

### Windows PowerShell

```powershell
cd web_log_analytics_glue
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Generate 10,000 synthetic events:

```powershell
python scripts/generate_logs.py --rows 10000 --days 7
```

The script prints the exact CSV path. It will look similar to:

```text
data/generated/ingest_date=2026-11-19/batch_id=abc123.../access_logs.csv
```

Copy that path and pass it to the local preview:

```powershell
python scripts/local_preview.py --input "data/generated/ingest_date=YYYY-MM-DD/batch_id=BATCH_ID/access_logs.csv"
```

The preview creates:

```text
local_output/
├── cleaned.csv
├── quarantine.csv
├── daily_metrics.csv
├── top_pages.csv
├── server_errors.csv
└── web_log_analytics_preview.xlsx
```

**Important:** `local_preview.py` is a Pandas-based learning/smoke-test helper. It mirrors the business validation rules but does not execute the AWS Glue PySpark job. The actual cloud ETL is `glue_jobs/glue_web_log_etl.py`.

### Run unit tests

```powershell
python -m pytest -v
```

## 3. Configure AWS credentials

Use an AWS CLI profile or IAM Identity Center/SSO. Avoid putting long-lived AWS access keys in the source code.

For an existing CLI profile:

```powershell
aws configure
aws sts get-caller-identity
```

For an organisation using AWS IAM Identity Center, use the profile configured by your administrator and run `aws sso login --profile YOUR_PROFILE`.

## 4. Deploy the AWS resources

Pick a globally unique bucket name, for example:

```text
myname-web-log-analytics-2026-unique
```

From the repository root:

```powershell
.\infra\deploy.ps1 -BucketName "myname-web-log-analytics-2026-unique" -Region "ap-south-1"
```

The deployment script:

1. Checks the AWS identity.
2. Creates the S3 bucket if needed.
3. Enables S3 Block Public Access and default AES-256 server-side encryption.
4. Uploads the Glue ETL script to the project prefix.
5. Deploys `infra/glue-stack.yaml` through CloudFormation.

CloudFormation creates:

- A least-privilege project IAM role for Glue, scoped to the project S3 prefix for data access.
- A Glue Data Catalog database.
- A custom CSV classifier with the expected header names.
- A Glue Crawler for `s3://BUCKET/web-log-analytics/raw/access_logs/`.
- One Glue 6.0 Spark job with bookmarks enabled.

Default names from the stack are:

```text
Stack:    web-log-analytics
Crawler:  web-log-analytics-crawler
Job:      web-log-analytics-etl
Catalog:  web_log_analytics_catalog
```

To use a different prefix or stack name, pass `-ProjectPrefix` or `-StackName` to `deploy.ps1`. Keep the same prefix when generating and reporting data.

## 5. Generate and upload the first raw batch

```powershell
python scripts/generate_logs.py `
  --rows 20000 `
  --days 7 `
  --upload `
  --bucket "myname-web-log-analytics-2026-unique" `
  --prefix "web-log-analytics" `
  --region "ap-south-1"
```

The generator creates one CSV file and uploads it under a unique `batch_id` folder:

```text
s3://BUCKET/web-log-analytics/raw/access_logs/
    ingest_date=YYYY-MM-DD/
        batch_id=UNIQUE_BATCH_ID/
            access_logs.csv
```

One complete CSV file per batch is intentional. It makes the demo easy to reason about and makes batch-level retries/output partitioning deterministic.

The data uses documentation-only IP address ranges, not real visitor IPs. A small proportion of rows are intentionally invalid or duplicated so the quarantine and deduplication logic can be demonstrated.

## 6. Run the crawler and Glue ETL job

```powershell
.\infra\run_pipeline.ps1 -Region "ap-south-1" -StackName "web-log-analytics"
```

The script:

1. Starts the crawler.
2. Waits until the crawler finishes successfully.
3. Starts the Glue job.
4. Polls the job state and reports success or failure.

The crawler discovers the CSV schema and registers `raw_access_logs` in the configured Data Catalog database. The job reads this table through the Glue Data Catalog.

## 7. Simulate another ingestion batch

Generate and upload a second independent batch:

```powershell
python scripts/generate_logs.py `
  --rows 12000 `
  --days 2 `
  --upload `
  --bucket "myname-web-log-analytics-2026-unique" `
  --prefix "web-log-analytics" `
  --region "ap-south-1"
```

Run the crawler and Glue job again:

```powershell
.\infra\run_pipeline.ps1 -Region "ap-south-1" -StackName "web-log-analytics"
```

The Glue job uses an S3 source bookmark. It tracks files read by the stable `transformation_ctx` (`raw_access_log_source`) and commits bookmark state at the end of a successful run. It is not a row-level CDC mechanism and does not detect edits to already-processed S3 objects. Treat raw batch files as immutable; upload corrections as a new batch.

## 8. Create the Excel report

```powershell
python scripts/create_excel_report.py `
  --bucket "myname-web-log-analytics-2026-unique" `
  --prefix "web-log-analytics/processed" `
  --region "ap-south-1" `
  --output "reports/web_log_analytics.xlsx"
```

The workbook contains:

- `Daily_Metrics`: request count, unique clients, average/p95 response time, 4xx/5xx counts, and bytes sent.
- `Top_Pages`: top ten paths per event date and ingestion batch.
- `Server_Errors`: 5xx counts by date, status code, and path.

Metrics are calculated for the newly processed ingestion batch, and include `ingest_date`/`batch_id` partitions. They are not a materialized all-time roll-up.

## 9. Validate the results

In the AWS Glue console:

- Open **Data Catalog → Databases** and inspect the crawler-created `raw_access_logs` table.
- Open **ETL jobs** and inspect the run status, execution details, and logs.
- Open the S3 bucket and inspect `processed/cleaned_logs/`, `processed/quarantine/`, `processed/analytics/`, and `processed/run_summaries/`.

The run summary JSON records input, cleaned, and quarantined row counts. The CloudWatch log group for the Glue job provides execution details.

## 10. Cost and cleanup

- Glue crawlers and Glue ETL jobs incur AWS charges while running. Run the demo when needed; do not create an unnecessary schedule.
- The job defaults to two `G.1X` workers and a 20-minute timeout for a small demonstration dataset. Increase resources only after measuring.
- S3 storage and requests can incur charges.
- There is no always-on Airflow environment or Snowflake warehouse in this project.

To delete the Glue role, crawler, database, and job while retaining your data:

```powershell
.\infra\destroy.ps1 -Region "ap-south-1" -StackName "web-log-analytics"
```

The S3 bucket is deliberately retained. Review and delete its contents/bucket separately only when you are certain you no longer need the raw files or outputs.

## Design boundaries

- The source is immutable batch CSV files, not a continuously changing OLTP database.
- Glue job bookmarks track processed S3 objects, not individual log rows or updates inside an existing object.
- The project deduplicates event IDs among records read in the current run/batch; it is not a global historical deduplication service.
- Aggregations are per `ingest_date`/`batch_id` and event date. They are not cumulative across all previous batches.
- The local preview is not a substitute for a live Glue integration test.

## Official AWS references

- [AWS Glue versions and runtimes](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)
- [Using AWS Glue job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/programming-etl-connect-bookmarks.html)
- [Tracking processed data with job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html)
- [Configuring AWS Glue jobs](https://docs.aws.amazon.com/glue/latest/dg/add-job.html)
