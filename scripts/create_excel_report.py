#!/usr/bin/env python3
"""Download Glue analytics Parquet outputs from S3 and create an Excel workbook."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import boto3
import pandas as pd

DATASETS = {
    "Daily_Metrics": "analytics/daily_metrics/",
    "Top_Pages": "analytics/top_pages/",
    "Server_Errors": "analytics/server_errors/",
}


def read_parquet_prefix(s3, bucket: str, prefix: str) -> pd.DataFrame:
    paginator = s3.get_paginator("list_objects_v2")
    frames: list[pd.DataFrame] = []
    with tempfile.TemporaryDirectory() as temp_dir:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.lower().endswith(".parquet"):
                    continue
                local = Path(temp_dir) / f"part_{len(frames):06d}.parquet"
                s3.download_file(bucket, key, str(local))
                frame = pd.read_parquet(local)
                # Parquet partition columns are encoded in the S3 object path and
                # are not stored inside each individual part file. Restore them
                # so they appear in the Excel reconciliation workbook.
                for segment in key.split("/"):
                    if "=" in segment:
                        name, value = segment.split("=", 1)
                        if name in {"ingest_date", "batch_id"}:
                            frame[name] = value
                frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="web-log-analytics/processed")
    parser.add_argument("--region", default="ap-south-1")
    parser.add_argument("--output", default="reports/web_log_analytics.xlsx")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)
    base = args.prefix.strip("/")
    datasets = {
        sheet: read_parquet_prefix(s3, args.bucket, f"{base}/{dataset}")
        for sheet, dataset in DATASETS.items()
    }
    if not any(not frame.empty for frame in datasets.values()):
        raise SystemExit("No analytics Parquet files found. Run the Glue job first.")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet, frame in datasets.items():
            frame.to_excel(writer, sheet_name=sheet, index=False)
        for worksheet in writer.book.worksheets:
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions
            for column_cells in worksheet.columns:
                width = min(max(len(str(cell.value or "")) for cell in column_cells) + 2, 40)
                worksheet.column_dimensions[column_cells[0].column_letter].width = width
    print(f"Created {output}")


if __name__ == "__main__":
    main()
