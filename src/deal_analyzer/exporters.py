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


def analyzer_output_dir(path_hint: Path | None = None) -> Path:
    app = load_config()
    target = path_hint or (app.workspace_dir / "deal_analyzer")
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


def write_markdown_export(
    *,
    output_dir: Path,
    name: str,
    markdown: str,
    write_latest: bool = True,
) -> ExportFileSet:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = _safe_name(name)
    timestamped = output_dir / f"{base}_{ts}.md"
    timestamped.write_text(markdown, encoding="utf-8")

    latest: Path | None = None
    if write_latest:
        latest = output_dir / f"{base}_latest.md"
        latest.write_text(markdown, encoding="utf-8")

    return ExportFileSet(timestamped=timestamped, latest=latest)


def write_analysis_csv(
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


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = _csv_fields()
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(_flatten(row, fieldnames))


def _flatten(row: dict[str, Any], fieldnames: list[str]) -> dict[str, Any]:
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
        "score_0_100",
        "presentation_quality_flag",
        "followup_quality_flag",
        "data_completeness_flag",
        "strong_sides",
        "growth_zones",
        "risk_flags",
        "recommended_actions_for_manager",
        "recommended_training_tasks_for_employee",
        "manager_message_draft",
        "employee_training_message_draft",
    ]


def build_markdown_report(*, title: str, analyses: list[dict[str, Any]]) -> str:
    lines = [f"# {title}", "", f"Deals in report: {len(analyses)}", ""]
    for row in analyses:
        lines.append(f"## Deal {row.get('deal_id')}: {row.get('deal_name') or '-'}")
        lines.append(f"- Score: {row.get('score_0_100')}")
        lines.append(f"- Presentation flag: {row.get('presentation_quality_flag')}")
        lines.append(f"- Follow-up flag: {row.get('followup_quality_flag')}")
        lines.append(f"- Completeness flag: {row.get('data_completeness_flag')}")
        strong = ", ".join(row.get("strong_sides", [])) if isinstance(row.get("strong_sides"), list) else ""
        growth = ", ".join(row.get("growth_zones", [])) if isinstance(row.get("growth_zones"), list) else ""
        risks = ", ".join(row.get("risk_flags", [])) if isinstance(row.get("risk_flags"), list) else ""
        lines.append(f"- Strong sides: {strong}")
        lines.append(f"- Growth zones: {growth}")
        lines.append(f"- Risk flags: {risks}")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _safe_name(value: str) -> str:
    raw = str(value or "export").strip().lower()
    chars = [ch if (ch.isalnum() or ch in {"_", "-"}) else "_" for ch in raw]
    return "".join(chars).strip("_") or "export"
