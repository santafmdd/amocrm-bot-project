from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import load_config
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class ExportFileSet:
    timestamped: Path
    latest: Path | None


def collector_output_dir(path_hint: Path | None = None) -> Path:
    app = load_config()
    target = path_hint or (app.workspace_dir / "amocrm_collector")
    safe = ensure_inside_root(target.resolve(), app.project_root)
    safe.mkdir(parents=True, exist_ok=True)
    return safe


def write_json_export(
    *,
    output_dir: Path,
    name: str,
    payload: dict[str, Any] | list[Any],
    write_latest: bool = True,
) -> ExportFileSet:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = _safe_name(name)
    timestamped = output_dir / f"{base}_{ts}.json"
    timestamped.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    latest: Path | None = None
    if write_latest:
        latest = output_dir / f"{base}_latest.json"
        latest.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return ExportFileSet(timestamped=timestamped, latest=latest)


def write_normalized_jsonl(
    *,
    output_dir: Path,
    name: str,
    rows: list[dict[str, Any]],
    write_latest: bool = True,
) -> ExportFileSet:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = _safe_name(name)
    timestamped = output_dir / f"{base}_{ts}.jsonl"
    _write_jsonl(timestamped, rows)

    latest: Path | None = None
    if write_latest:
        latest = output_dir / f"{base}_latest.jsonl"
        _write_jsonl(latest, rows)

    return ExportFileSet(timestamped=timestamped, latest=latest)


def write_normalized_csv(
    *,
    output_dir: Path,
    name: str,
    rows: list[dict[str, Any]],
    write_latest: bool = True,
) -> ExportFileSet:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = _safe_name(name)
    timestamped = output_dir / f"{base}_{ts}.csv"
    _write_csv(timestamped, rows)

    latest: Path | None = None
    if write_latest:
        latest = output_dir / f"{base}_latest.csv"
        _write_csv(latest, rows)

    return ExportFileSet(timestamped=timestamped, latest=latest)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = [json.dumps(item, ensure_ascii=False) for item in rows]
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = _csv_fields()
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_flatten_row_for_csv(row, fieldnames))


def _flatten_row_for_csv(row: dict[str, Any], fieldnames: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in fieldnames:
        val = row.get(key)
        if isinstance(val, list):
            out[key] = "; ".join(str(x) for x in val)
        elif isinstance(val, dict):
            out[key] = json.dumps(val, ensure_ascii=False)
        elif val is None:
            out[key] = ""
        else:
            out[key] = val
    return out


def _csv_fields() -> list[str]:
    return [
        "deal_id",
        "amo_lead_id",
        "deal_name",
        "created_at",
        "updated_at",
        "responsible_user_id",
        "responsible_user_name",
        "pipeline_id",
        "pipeline_name",
        "status_id",
        "status_name",
        "product_values",
        "source_values",
        "pain_text",
        "business_tasks_text",
        "brief_url",
        "demo_result_text",
        "test_result_text",
        "probability_value",
        "company_name",
        "company_inn",
        "company_comment",
        "contact_name",
        "contact_phone",
        "contact_email",
        "contact_comment",
        "tags",
        "presentation_link_candidates",
        "presentation_detected",
        "presentation_detect_reason",
        "long_call_detected",
        "longest_call_duration_seconds",
        "training_candidate_text",
        "manager_scope_allowed",
    ]


def _safe_name(value: str) -> str:
    raw = str(value or "export").strip().lower()
    chars = [ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in raw]
    return "".join(chars).strip("_") or "export"
