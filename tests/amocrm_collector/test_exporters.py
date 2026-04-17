from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from src.amocrm_collector.exporters import write_json_export, write_normalized_csv, write_normalized_jsonl


def _temp_dir() -> Path:
    path = Path("workspace") / "tmp_tests" / f"collector_exp_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_json_export_writes_timestamp_and_latest():
    out = _temp_dir()
    files = write_json_export(output_dir=out, name="collect-period", payload={"ok": True}, write_latest=True)
    assert files.timestamped.exists()
    assert files.latest is not None and files.latest.exists()
    payload = json.loads(files.latest.read_text(encoding="utf-8"))
    assert payload["ok"] is True
    shutil.rmtree(out, ignore_errors=True)


def test_jsonl_and_csv_exports_created():
    out = _temp_dir()
    rows = [
        {
            "deal_id": 1,
            "amo_lead_id": 1,
            "deal_name": "Deal",
            "product_values": ["A", "B"],
            "presentation_detected": True,
            "manager_scope_allowed": True,
        }
    ]
    jsonl_files = write_normalized_jsonl(output_dir=out, name="rows", rows=rows, write_latest=True)
    csv_files = write_normalized_csv(output_dir=out, name="rows", rows=rows, write_latest=True)

    assert jsonl_files.timestamped.exists()
    assert csv_files.timestamped.exists()
    assert jsonl_files.latest is not None and jsonl_files.latest.exists()
    assert csv_files.latest is not None and csv_files.latest.exists()

    jsonl_text = jsonl_files.latest.read_text(encoding="utf-8")
    csv_text = csv_files.latest.read_text(encoding="utf-8")
    assert '"deal_id": 1' in jsonl_text
    assert "deal_id" in csv_text

    shutil.rmtree(out, ignore_errors=True)
