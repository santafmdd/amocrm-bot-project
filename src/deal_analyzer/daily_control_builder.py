from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from .daily_control.daily_analyzer import analyze_daily_packages
from .daily_control.day_grouper import aggregate_mix, group_by_manager_day
from .daily_control.roks_oap_parser import parse_roks_oap_snapshot
from .daily_control.source_reader import DAILY_FIELD_ALIASES, day_label_from_iso, map_headers
from .daily_control.style.deterministic_cleaner import clean_daily_text
from .daily_control.validation.text_lint import lint_daily_text_rows


def _day_label_from_iso(value: str) -> str:
    return day_label_from_iso(value)


def sanitize_daily_output(row: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(row)
    counters: dict[str, int] = {}
    for field in (
        "main_pattern",
        "strong_sides",
        "growth_zones",
        "why_it_matters",
        "what_to_reinforce",
        "what_to_fix",
        "what_to_tell_employee",
        "expected_quant_impact",
        "expected_qual_impact",
    ):
        if field in cleaned:
            cleaned[field] = clean_daily_text(str(cleaned.get(field, "") or ""), counters)
    cleaned["_cleanup_counts"] = counters
    return cleaned


def parse_call_review_rows(
    *,
    headers: list[str],
    rows: list[list[str]],
    cfg: Any,
    period_start: date,
    period_end: date,
    manager_allowlist: tuple[str, ...] | None = None,
):
    return group_by_manager_day(
        headers=headers,
        rows=rows,
        cfg=cfg,
        period_start=period_start,
        period_end=period_end,
        manager_allowlist=manager_allowlist,
    )


def extract_roks_oap_snapshot(
    *,
    client: Any,
    spreadsheet_id: str,
    period_end: date,
    manager_allowlist: tuple[str, ...],
) -> dict[str, Any]:
    return parse_roks_oap_snapshot(
        client=client,
        spreadsheet_id=spreadsheet_id,
        period_end=period_end,
        manager_allowlist=manager_allowlist,
    )


def normalize_daily_llm_runtime(runtime: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(runtime or {})
    main_error = str(out.get("main_error") or "").lower()
    fallback_error = str(out.get("fallback_error") or "").lower()
    main_ok = bool(out.get("main_ok", False)) and "not valid json object" not in main_error
    fallback_ok = bool(out.get("fallback_ok", False)) and "not valid json object" not in fallback_error

    if main_ok:
        out["selected"] = "main"
        out["reason"] = "main_ok"
    elif fallback_ok:
        out["selected"] = "fallback"
        out["reason"] = "fallback_ok"
    else:
        out["selected"] = "deterministic_fallback"
        out["reason"] = "llm_json_invalid"
        out["warning"] = "llm_json_invalid"
    return out


def _run_daily_row_llm(
    *,
    package_row: dict[str, Any],
    cfg: Any,
    logger: Any,
    llm_runtime: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    runtime = normalize_daily_llm_runtime(llm_runtime)
    row = dict(package_row)
    row["analysis_backend_used"] = str(runtime.get("selected") or "deterministic_fallback")
    debug = {
        "selected_runtime": row["analysis_backend_used"],
        "reason": runtime.get("reason", ""),
        "warning": runtime.get("warning", ""),
    }
    _ = cfg, logger
    return row, debug


def build_daily_rows(
    *,
    packages: list[Any],
    cfg: Any,
    roks_snapshot: dict[str, Any],
    llm_runtime: dict[str, Any],
    logger: Any,
    source_run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return analyze_daily_packages(
        packages=packages,
        cfg=cfg,
        roks_snapshot=roks_snapshot,
        llm_runtime=normalize_daily_llm_runtime(llm_runtime),
        logger=logger,
        source_run_id=source_run_id,
    )


def build_daily_quality_review(rows: list[dict[str, Any]], limit: int = 10) -> dict[str, Any]:
    lint = lint_daily_text_rows(rows)
    examples = lint.get("problem_examples", []) if isinstance(lint.get("problem_examples"), list) else []
    row_ids = sorted({int(item.get("row_index", -1)) for item in examples if isinstance(item, dict) and int(item.get("row_index", -1)) >= 0})
    problem_rows = []
    for row_idx in row_ids[: max(1, int(limit or 10))]:
        row = rows[row_idx] if 0 <= row_idx < len(rows) else {}
        problem_rows.append(
            {
                "row_index": row_idx,
                "manager_name": row.get("manager_name", "") if isinstance(row, dict) else "",
                "deal_ids": row.get("deal_ids", "") if isinstance(row, dict) else "",
                "main_pattern_preview": str((row.get("main_pattern", "") if isinstance(row, dict) else ""))[:200],
            }
        )
    return {
        "rows_total": len(rows),
        "problem_rows_total": len(row_ids),
        "problem_rows": problem_rows,
        "text_lint": lint,
    }


def _build_department_rows(*, rows: list[dict[str, Any]], threshold: int, source_run_id: str) -> list[dict[str, Any]]:
    _ = rows, threshold, source_run_id
    return []


def estimate_effects(
    *,
    role: str,
    main_pattern: str,
    manager: str,
    roks_snapshot: dict[str, Any],
    deals_count: int,
) -> tuple[str, str, str]:
    # Legacy compatibility API: active path is LLM-first in src.deal_analyzer.daily_control.daily_analyzer.
    # Keep deterministic layer strictly factual-only and do not synthesize management wording here.
    _ = role, main_pattern, manager, deals_count
    manager_metrics = (roks_snapshot.get("manager_metrics") or {}) if isinstance(roks_snapshot, dict) else {}
    has_roks_metrics = bool(manager_metrics.get(manager, {})) if isinstance(manager_metrics, dict) else False
    if has_roks_metrics:
        return ("", "", "roks_oap")
    return ("", "", "fallback_no_roks_metrics")


def build_roks_snapshot_markdown(snapshot: dict[str, Any]) -> list[str]:
    lines = [
        f"status: {snapshot.get('status', '')}",
        f"parse_status: {snapshot.get('parse_status', '')}",
        f"selected_current_month_sheet: {snapshot.get('selected_current_month_sheet', '')}",
        f"selected_previous_month_sheet: {snapshot.get('selected_previous_month_sheet', '')}",
    ]
    warnings = snapshot.get("warnings", []) if isinstance(snapshot.get("warnings"), list) else []
    if warnings:
        lines.append("")
        lines.append("warnings:")
        for warning in warnings[:20]:
            lines.append(f"- {warning}")
    return lines


def run_daily_llm_comparison(
    *,
    cfg: Any,
    payload_rows: list[dict[str, Any]],
    logger: Any,
    comparison_limit: int,
) -> dict[str, Any]:
    _ = cfg, payload_rows, logger, comparison_limit
    return {
        "available": False,
        "rows_checked": 0,
        "deepseek": {"ok": 0},
        "gemma": {"ok": 0},
        "reason": "comparison_not_implemented_in_llm_first_daily_path",
    }


def save_markdown(path: Path, *, title: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = [f"# {title}", ""]
    content.extend(lines)
    path.write_text("\n".join(content).strip() + "\n", encoding="utf-8")


def build_daily_payload(*args, **kwargs):
    raise RuntimeError(
        "build_daily_payload legacy API is deprecated. Use src.deal_analyzer.daily_control.cli build instead."
    )
