from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from src.amocrm_collector.exporters import write_json_export, write_normalized_csv, write_normalized_jsonl


def test_json_export_writes_timestamp_and_latest():
    out = Path("workspace/amocrm_collector")
    captured: list[tuple[str, str]] = []

    def _capture(self: Path, text: str, encoding: str = "utf-8"):
        captured.append((str(self), text))
        return len(text)

    with patch("pathlib.Path.write_text", _capture):
        files = write_json_export(output_dir=out, name="collect-period", payload={"ok": True}, write_latest=True)

    assert str(files.timestamped).endswith(".json")
    assert files.latest is not None and str(files.latest).endswith("_latest.json")
    assert len(captured) == 2
    assert '"ok": true' in captured[0][1].lower()


def test_jsonl_and_csv_exports_created():
    out = Path("workspace/amocrm_collector")
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
    jsonl_calls: list[Path] = []
    csv_calls: list[Path] = []

    with patch("src.amocrm_collector.exporters._write_jsonl", side_effect=lambda p, r: jsonl_calls.append(p)), patch(
        "src.amocrm_collector.exporters._write_csv", side_effect=lambda p, r: csv_calls.append(p)
    ):
        jsonl_files = write_normalized_jsonl(output_dir=out, name="rows", rows=rows, write_latest=True)
        csv_files = write_normalized_csv(output_dir=out, name="rows", rows=rows, write_latest=True)

    assert len(jsonl_calls) == 2
    assert len(csv_calls) == 2
    assert str(jsonl_files.timestamped).endswith(".jsonl")
    assert jsonl_files.latest is not None and str(jsonl_files.latest).endswith("_latest.jsonl")
    assert str(csv_files.timestamped).endswith(".csv")
    assert csv_files.latest is not None and str(csv_files.latest).endswith("_latest.csv")
