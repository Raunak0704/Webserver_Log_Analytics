#!/usr/bin/env python3
"""Run a small Pandas preview of the Glue job's validation and reporting rules.

This is a local learning/smoke-test helper, not a replacement for the PySpark
AWS Glue job. It lets you inspect expected outputs without launching Glue.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}


def transform_logs(input_file: Path) -> dict[str, pd.DataFrame]:
    raw = pd.read_csv(input_file, dtype=str, keep_default_na=False)
    required = {
        "event_id", "event_time", "client_ip", "method", "path", "status_code",
        "response_time_ms", "bytes_sent", "user_agent", "referer",
    }
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(f"Input is missing columns: {', '.join(sorted(missing))}")

    frame = raw.copy()
    frame["event_id"] = frame["event_id"].astype(str).str.strip()
    frame["event_ts"] = pd.to_datetime(frame["event_time"], errors="coerce", utc=True)
    frame["client_ip"] = frame["client_ip"].astype(str).str.strip()
    frame["method"] = frame["method"].astype(str).str.strip().str.upper()
    frame["path"] = frame["path"].astype(str).str.strip().str.replace(r"\?.*$", "", regex=True)
    frame["status_code"] = pd.to_numeric(frame["status_code"], errors="coerce")
    frame["response_time_ms"] = pd.to_numeric(frame["response_time_ms"], errors="coerce")
    frame["bytes_sent"] = pd.to_numeric(frame["bytes_sent"], errors="coerce")

    reasons: list[list[str]] = []
    for row in frame.itertuples(index=False):
        row_reasons: list[str] = []
        if not row.event_id:
            row_reasons.append("missing_event_id")
        if pd.isna(row.event_ts):
            row_reasons.append("invalid_event_time")
        if not row.client_ip:
            row_reasons.append("missing_client_ip")
        if row.method not in ALLOWED_METHODS:
            row_reasons.append("invalid_method")
        if not row.path or not row.path.startswith("/"):
            row_reasons.append("invalid_path")
        if pd.isna(row.status_code) or not 100 <= row.status_code <= 599:
            row_reasons.append("invalid_status_code")
        if pd.isna(row.response_time_ms) or not 0 <= row.response_time_ms <= 600000:
            row_reasons.append("invalid_response_time")
        if pd.isna(row.bytes_sent) or row.bytes_sent < 0:
            row_reasons.append("invalid_bytes_sent")
        reasons.append(row_reasons)

    frame["rejection_reason"] = [";".join(value) for value in reasons]
    invalid = frame[frame["rejection_reason"] != ""].copy()
    candidates = frame[frame["rejection_reason"] == ""].copy()

    candidates = candidates.sort_values("event_ts", ascending=True)
    duplicate_mask = candidates.duplicated(subset=["event_id"], keep="last")
    duplicates = candidates[duplicate_mask].copy()
    duplicates["rejection_reason"] = "duplicate_event_id_within_batch"
    quarantine = pd.concat([invalid, duplicates], ignore_index=True)

    cleaned = candidates[~duplicate_mask].copy()
    cleaned["event_date"] = cleaned["event_ts"].dt.date.astype(str)
    cleaned["status_code"] = cleaned["status_code"].astype(int)
    cleaned["status_family"] = cleaned["status_code"].astype(str).str[0] + "xx"
    cleaned["is_error"] = cleaned["status_code"] >= 400
    cleaned["is_server_error"] = cleaned["status_code"] >= 500

    daily = (
        cleaned.groupby("event_date", as_index=False)
        .agg(
            request_count=("event_id", "count"),
            unique_clients=("client_ip", "nunique"),
            avg_response_time_ms=("response_time_ms", "mean"),
            p95_response_time_ms=("response_time_ms", lambda x: x.quantile(0.95)),
            error_requests=("is_error", "sum"),
            server_error_requests=("is_server_error", "sum"),
            total_bytes_sent=("bytes_sent", "sum"),
        )
    )
    daily[["avg_response_time_ms", "p95_response_time_ms"]] = daily[
        ["avg_response_time_ms", "p95_response_time_ms"]
    ].round(2)

    pages = (
        cleaned.groupby(["event_date", "path"], as_index=False)
        .agg(
            requests=("event_id", "count"),
            avg_response_time_ms=("response_time_ms", "mean"),
            error_requests=("is_error", "sum"),
        )
        .sort_values(["event_date", "requests", "path"], ascending=[True, False, True])
    )
    pages["page_rank"] = pages.groupby("event_date").cumcount() + 1
    top_pages = pages[pages["page_rank"] <= 10].copy()

    server_errors = (
        cleaned[cleaned["status_code"] >= 500]
        .groupby(["event_date", "status_code", "path"], as_index=False)
        .agg(
            error_count=("event_id", "count"),
            avg_response_time_ms=("response_time_ms", "mean"),
        )
    )
    return {
        "raw": raw,
        "cleaned": cleaned,
        "quarantine": quarantine,
        "daily_metrics": daily,
        "top_pages": top_pages,
        "server_errors": server_errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to generated access_logs.csv")
    parser.add_argument("--output-dir", default="local_output", help="Output folder")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    result = transform_logs(Path(args.input))

    output_names = ["cleaned", "quarantine", "daily_metrics", "top_pages", "server_errors"]
    for name in output_names:
        result[name].to_csv(output_dir / f"{name}.csv", index=False)

    report_path = output_dir / "web_log_analytics_preview.xlsx"
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        sheet_frames = {
            "Daily_Metrics": result["daily_metrics"],
            "Top_Pages": result["top_pages"],
            "Server_Errors": result["server_errors"],
            "Quarantine_Sample": result["quarantine"],
        }
        for sheet_name, frame in sheet_frames.items():
            excel_frame = frame.copy()
            # Excel has no timezone-aware datetime type. Keep UTC semantics but
            # remove the timezone marker only in the exported workbook.
            for column in excel_frame.select_dtypes(include=["datetimetz"]).columns:
                excel_frame[column] = excel_frame[column].dt.tz_convert("UTC").dt.tz_localize(None)
            excel_frame.to_excel(writer, sheet_name=sheet_name, index=False)
        for worksheet in writer.book.worksheets:
            worksheet.freeze_panes = "A2"
            worksheet.auto_filter.ref = worksheet.dimensions
            for column_cells in worksheet.columns:
                width = min(max(len(str(cell.value or "")) for cell in column_cells) + 2, 40)
                worksheet.column_dimensions[column_cells[0].column_letter].width = width

    print(f"Input rows:      {len(result['raw']):,}")
    print(f"Cleaned rows:    {len(result['cleaned']):,}")
    print(f"Quarantined:     {len(result['quarantine']):,}")
    print(f"Excel preview:   {report_path}")


if __name__ == "__main__":
    main()
