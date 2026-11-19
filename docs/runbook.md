# Operational runbook

## Normal batch

1. Generate or receive a new immutable CSV batch.
2. Upload it under a new `ingest_date=.../batch_id=...` directory.
3. Run the Glue crawler to discover the new folder/partition.
4. Start the Glue ETL job.
5. Confirm the job run state is `SUCCEEDED`.
6. Inspect the run summary JSON and output prefixes.
7. Generate the Excel workbook if a report is needed.

## Common failures

| Symptom | Likely cause | What to check |
|---|---|---|
| Crawler cannot access S3 | Glue role or bucket policy issue | Crawler role, bucket region, S3 prefix permissions |
| Crawler succeeds but table missing | Wrong target prefix or classifier | Crawler S3 target and classifier header definition |
| Glue says table does not exist | Wrong Catalog database/table argument or crawler not run | `CATALOG_DATABASE`, `CATALOG_TABLE`, crawler result |
| AccessDenied on input | Glue role lacks `s3:GetObject`/`s3:ListBucket` | IAM role policy and bucket policy |
| AccessDenied on output | Glue role lacks write/delete access on processed prefix | IAM role policy scope |
| Job fails parsing/casting | Unexpected column/header or malformed data | Catalog table schema and CloudWatch logs |
| No new rows processed | No new S3 files, bookmark already consumed them, or crawler missed folder | Check raw object key, crawler partitions, bookmark state |
| Excel script finds no Parquet | Glue job failed or output prefix is wrong | Job run, S3 processed prefix, CLI `--prefix` |

## Reprocess a batch

Use the Glue console's job bookmark actions only when you understand the effect. Rewinding/resetting bookmarks can cause files to be read again. The ETL job uses `batch_id` partitions and dynamic partition overwrite to make replay of the same immutable batch safer, but do not upload different content under a previously used `batch_id`.

## Cost hygiene

- Do not schedule a recurring crawler for this learning project.
- Use the smallest worker configuration that completes the sample workload.
- Stop experimenting when the job is idle; Glue jobs do not need to remain running.
- Keep the S3 bucket only while you need the data. The provided destroy script removes Glue resources but intentionally retains the bucket.
