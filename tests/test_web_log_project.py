from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from generate_logs import FIELDS, generate_csv  # noqa: E402
from local_preview import transform_logs  # noqa: E402


def test_generator_writes_expected_columns_and_rows(tmp_path):
    file_path, ingest_date, batch_id = generate_csv(tmp_path, rows=250, days=3, seed=17)
    frame = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    assert list(frame.columns) == FIELDS
    assert len(frame) == 250
    assert ingest_date in str(file_path)
    assert batch_id in str(file_path)


def test_preview_splits_clean_and_quarantine(tmp_path):
    file_path, _, _ = generate_csv(tmp_path, rows=1000, days=2, seed=7)
    result = transform_logs(file_path)
    assert len(result["cleaned"]) > 0
    assert len(result["quarantine"]) > 0
    assert len(result["cleaned"]) + len(result["quarantine"]) >= len(result["raw"])


def test_cleaned_rows_have_normalized_path_and_valid_status(tmp_path):
    file_path, _, _ = generate_csv(tmp_path, rows=600, days=2, seed=21)
    result = transform_logs(file_path)
    cleaned = result["cleaned"]
    assert cleaned["path"].str.startswith("/").all()
    assert cleaned["status_code"].between(100, 599).all()
    assert cleaned["response_time_ms"].between(0, 600000).all()
    assert not cleaned["event_id"].duplicated().any()


def test_daily_metrics_and_server_error_outputs(tmp_path):
    file_path, _, _ = generate_csv(tmp_path, rows=2000, days=3, seed=31)
    result = transform_logs(file_path)
    assert {"request_count", "unique_clients", "p95_response_time_ms"}.issubset(
        result["daily_metrics"].columns
    )
    assert set(result["server_errors"]["status_code"].dropna().astype(int)).issubset({500, 502, 503})


def test_top_pages_are_limited_to_ten_per_date(tmp_path):
    file_path, _, _ = generate_csv(tmp_path, rows=2000, days=4, seed=99)
    result = transform_logs(file_path)
    if not result["top_pages"].empty:
        counts = result["top_pages"].groupby("event_date").size()
        assert (counts <= 10).all()
