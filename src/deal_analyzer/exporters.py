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
    include_executed_at: bool = False,
) -> ExportFileSet:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base = _safe_name(name)
    timestamped = output_dir / f"{base}_{ts}.csv"
    _write_csv(timestamped, rows, include_executed_at=include_executed_at)

    latest: Path | None = None
    if write_latest:
        latest = output_dir / f"{base}_latest.csv"
        _write_csv(latest, rows, include_executed_at=include_executed_at)

    return ExportFileSet(timestamped=timestamped, latest=latest)


def _write_csv(path: Path, rows: list[dict[str, Any]], *, include_executed_at: bool = False) -> None:
    fieldnames = _csv_fields(include_executed_at=include_executed_at)
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


def _csv_fields(*, include_executed_at: bool) -> list[str]:
    fields = [
        "backend",
        "analysis_backend_requested",
        "analysis_backend_used",
        "llm_repair_applied",
        "backend_requested",
        "backend_effective_summary",
        "llm_success",
        "llm_success_repaired",
        "llm_fallback",
        "llm_error",
        "period_mode_resolved",
        "period_start",
        "period_end",
        "public_period_label",
        "as_of_date",
        "deal_id",
        "amo_lead_id",
        "deal_name",
        "score_0_100",
        "enrichment_match_status",
        "enrichment_match_source",
        "enrichment_confidence",
        "matched_client_list_row_id",
        "matched_appointment_row_id",
        "call_source",
        "calls_total",
        "missing_recording_calls",
        "longest_call_duration_seconds",
        "call_warnings",
        "transcription_backend",
        "call_collection_mode",
        "transcripts_total",
        "enriched_test_started",
        "enriched_test_completed",
        "enriched_test_status",
        "enriched_test_comments",
        "enriched_appointment_date",
        "enriched_assigned_by",
        "enriched_conducted_by",
        "enriched_meeting_status",
        "enriched_transfer_cancel_flag",
        "manager_summary",
        "employee_coaching",
        "employee_fix_tasks",
        "data_quality_flags",
        "owner_ambiguity_flag",
        "crm_hygiene_confidence",
        "analysis_confidence",
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
        "loss_reason_short",
        "manager_insight_short",
        "coaching_hint_short",
    ]
    if include_executed_at:
        fields.insert(1, "executed_at")
    return fields


def build_markdown_report(
    *,
    title: str,
    analyses: list[dict[str, Any]],
    report_metadata: dict[str, Any] | None = None,
) -> str:
    lines = [f"# {title}", ""]
    if report_metadata:
        lines.append("## Report Metadata")
        if report_metadata.get("public_period_label"):
            lines.append(f"- Period: {report_metadata.get('public_period_label')}")
        if report_metadata.get("period_start") and report_metadata.get("period_end"):
            lines.append(f"- Range: {report_metadata.get('period_start')} .. {report_metadata.get('period_end')}")
        if report_metadata.get("period_mode_resolved"):
            lines.append(f"- Mode: {report_metadata.get('period_mode_resolved')}")
        if report_metadata.get("as_of_date"):
            lines.append(f"- As of: {report_metadata.get('as_of_date')}")
        if report_metadata.get("executed_at"):
            lines.append(f"- Executed at: {report_metadata.get('executed_at')}")
        if report_metadata.get("backend_requested"):
            lines.append(f"- Backend requested: {report_metadata.get('backend_requested')}")
        if report_metadata.get("backend_effective_summary"):
            lines.append(f"- Backend effective: {report_metadata.get('backend_effective_summary')}")
        if report_metadata.get("llm_success") is not None:
            lines.append(f"- LLM success: {report_metadata.get('llm_success')}")
        if report_metadata.get("llm_success_repaired") is not None:
            lines.append(f"- LLM success repaired: {report_metadata.get('llm_success_repaired')}")
        if report_metadata.get("llm_fallback") is not None:
            lines.append(f"- LLM fallback: {report_metadata.get('llm_fallback')}")
        if report_metadata.get("llm_error") is not None:
            lines.append(f"- LLM errors: {report_metadata.get('llm_error')}")
        lines.append("")

    lines.extend([f"Deals in report: {len(analyses)}", ""])
    for row in analyses:
        lines.append(f"## Deal {row.get('deal_id')}: {row.get('deal_name') or '-'}")
        if row.get("backend"):
            lines.append(f"- Backend: {row.get('backend')}")
        if row.get("analysis_backend_requested"):
            lines.append(f"- Analysis backend requested: {row.get('analysis_backend_requested')}")
        if row.get("analysis_backend_used"):
            lines.append(f"- Analysis backend used: {row.get('analysis_backend_used')}")
        lines.append(f"- LLM repair applied: {row.get('llm_repair_applied', False)}")
        lines.append(f"- LLM fallback: {row.get('llm_fallback', False)}")
        lines.append(f"- LLM error: {row.get('llm_error', False)}")
        if row.get("loss_reason_short"):
            lines.append(f"- Loss reason short: {row.get('loss_reason_short')}")
        if row.get("manager_insight_short"):
            lines.append(f"- Manager insight short: {row.get('manager_insight_short')}")
        if row.get("coaching_hint_short"):
            lines.append(f"- Coaching hint short: {row.get('coaching_hint_short')}")
        lines.append(
            "- Enrichment: status={status}, source={source}, confidence={conf}".format(
                status=row.get("enrichment_match_status", ""),
                source=row.get("enrichment_match_source", ""),
                conf=row.get("enrichment_confidence", ""),
            )
        )
        lines.append(f"- Score: {row.get('score_0_100')}")
        lines.append(f"- Presentation flag: {row.get('presentation_quality_flag')}")
        lines.append(f"- Follow-up flag: {row.get('followup_quality_flag')}")
        lines.append(f"- Completeness flag: {row.get('data_completeness_flag')}")
        lines.append(f"- Analysis confidence: {row.get('analysis_confidence', '')}")
        lines.append(f"- CRM hygiene confidence: {row.get('crm_hygiene_confidence', '')}")
        lines.append(f"- Owner ambiguity: {row.get('owner_ambiguity_flag', False)}")
        if isinstance(row.get("data_quality_flags"), list) and row.get("data_quality_flags"):
            lines.append(f"- Data quality flags: {', '.join(str(x) for x in row.get('data_quality_flags'))}")
        if row.get("manager_summary"):
            lines.append(f"- Manager summary: {row.get('manager_summary')}")
        if row.get("employee_coaching"):
            lines.append(f"- Employee coaching: {row.get('employee_coaching')}")
        if isinstance(row.get("employee_fix_tasks"), list) and row.get("employee_fix_tasks"):
            lines.append(f"- Employee fix tasks: {'; '.join(str(x) for x in row.get('employee_fix_tasks'))}")
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
