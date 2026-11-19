#!/usr/bin/env python3
"""Generate synthetic web access logs and optionally upload one batch to S3."""

from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

FIELDS = [
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
]

PAGES = [
    "/",
    "/products",
    "/products/laptop",
    "/products/phone",
    "/cart",
    "/checkout",
    "/login",
    "/register",
    "/about",
    "/contact",
    "/search",
    "/api/products",
    "/api/orders",
]
METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE", "HEAD"]
USER_AGENTS = [
    "Mozilla/5.0 Chrome/124.0 Windows NT 10.0",
    "Mozilla/5.0 Firefox/125.0 Windows NT 10.0",
    "Mozilla/5.0 Safari/17.4 Mac OS X",
    "Mozilla/5.0 Chrome/124.0 Android 14",
    "Mozilla/5.0 Safari/17.4 iPhone",
]
REFERERS = [
    "https://www.example.test/",
    "https://www.example.test/products",
    "https://search.example.test/",
    "",
]


def make_row(rng: random.Random, now: datetime, days: int, index: int, prior_ids: list[str]) -> dict[str, Any]:
    event_id = uuid.uuid4().hex
    if index > 0 and rng.random() < 0.006 and prior_ids:
        event_id = rng.choice(prior_ids)
    else:
        prior_ids.append(event_id)

    event_time = now - timedelta(
        days=rng.uniform(0, max(days, 1)), seconds=rng.randint(0, 86400)
    )
    method = rng.choice(METHODS)
    path = rng.choice(PAGES)
    status = rng.choices(
        [200, 201, 204, 301, 302, 400, 401, 403, 404, 429, 500, 502, 503],
        weights=[48, 5, 4, 3, 3, 4, 2, 3, 10, 2, 6, 5, 5],
        k=1,
    )[0]
    response_ms: Any = round(max(1, rng.lognormvariate(4.0, 0.8)), 2)
    bytes_sent: Any = rng.randint(150, 350000)
    event_time_text = event_time.isoformat(timespec="seconds").replace("+00:00", "Z")

    # Deliberately introduce a small amount of bad data for quarantine testing.
    defect = rng.random()
    if defect < 0.003:
        event_id = ""
    elif defect < 0.006:
        event_time_text = "not-a-timestamp"
    elif defect < 0.009:
        method = "TRACE_UNKNOWN"
    elif defect < 0.012:
        path = "missing-leading-slash"
    elif defect < 0.015:
        status = "not-a-status"
    elif defect < 0.018:
        response_ms = -5
    elif defect < 0.021:
        response_ms = "fast"
    elif defect < 0.024:
        bytes_sent = -100

    # Reserved documentation IP ranges; no real visitor IPs are used.
    ip_prefix = rng.choice(["192.0.2", "198.51.100", "203.0.113"])
    client_ip = f"{ip_prefix}.{rng.randint(1, 254)}"
    if rng.random() < 0.7:
        path = f"{path}?campaign={rng.choice(['spring', 'email', 'search', 'direct'])}"

    return {
        "event_id": event_id,
        "event_time": event_time_text,
        "client_ip": client_ip,
        "method": method,
        "path": path,
        "status_code": status,
        "response_time_ms": response_ms,
        "bytes_sent": bytes_sent,
        "user_agent": rng.choice(USER_AGENTS),
        "referer": rng.choice(REFERERS),
    }


def generate_csv(output_root: Path, rows: int, days: int, seed: int | None) -> tuple[Path, str, str]:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")
    batch_id = uuid.uuid4().hex[:16]
    batch_dir = output_root / f"ingest_date={ingest_date}" / f"batch_id={batch_id}"
    batch_dir.mkdir(parents=True, exist_ok=True)
    file_path = batch_dir / "access_logs.csv"

    prior_ids: list[str] = []
    with file_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(rows):
            writer.writerow(make_row(rng, now, days, index, prior_ids))

    return file_path, ingest_date, batch_id


def upload_file(file_path: Path, bucket: str, prefix: str, ingest_date: str, batch_id: str, region: str) -> str:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("Install boto3 with: pip install -r requirements.txt") from exc

    key = (
        f"{prefix.strip('/')}/raw/access_logs/ingest_date={ingest_date}/"
        f"batch_id={batch_id}/access_logs.csv"
    )
    client = boto3.client("s3", region_name=region)
    client.upload_file(str(file_path), bucket, key, ExtraArgs={"ServerSideEncryption": "AES256"})
    return f"s3://{bucket}/{key}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000, help="Number of log events to generate")
    parser.add_argument("--days", type=int, default=7, help="Spread event timestamps across this many past days")
    parser.add_argument("--seed", type=int, default=None, help="Optional random seed")
    parser.add_argument("--output-root", default="data/generated", help="Local output directory")
    parser.add_argument("--upload", action="store_true", help="Upload the generated batch to S3")
    parser.add_argument("--bucket", default=None, help="Existing S3 bucket name")
    parser.add_argument("--prefix", default="web-log-analytics", help="Project prefix inside the bucket")
    parser.add_argument("--region", default="ap-south-1", help="AWS region")
    args = parser.parse_args()

    if args.rows < 1 or args.days < 1:
        parser.error("--rows and --days must be positive integers")
    if args.upload and not args.bucket:
        parser.error("--bucket is required when --upload is specified")

    file_path, ingest_date, batch_id = generate_csv(Path(args.output_root), args.rows, args.days, args.seed)
    result = {
        "rows_generated": args.rows,
        "local_file": str(file_path),
        "ingest_date": ingest_date,
        "batch_id": batch_id,
    }
    if args.upload:
        result["s3_uri"] = upload_file(
            file_path, args.bucket, args.prefix, ingest_date, batch_id, args.region
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
