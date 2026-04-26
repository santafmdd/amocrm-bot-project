from __future__ import annotations

import re
from datetime import date
from typing import Any

from .roks_oap_resolver import resolve_oap_month_sheets

BLOCK_ROW_HINTS: dict[str, int] = {
    "отдел": 3,
    "гордиенко": 23,
    "бочков": 43,
    "хомидов": 63,
}

METRIC_OFFSETS: list[tuple[str, int]] = [
    ("days", 0),
    ("dials", 1),
    ("reach", 2),
    ("lpr", 3),
    ("interest", 4),
    ("demo_done", 5),
    ("test", 6),
    ("invoice_count", 7),
    ("invoice_amount", 8),
    ("payment_count", 9),
    ("payment_amount", 10),
]

WEEKLY_FACT_COLS = [5, 7, 9, 11, 13]  # F/H/J/L/N in A1 notation


def _norm_text(value: Any) -> str:
    return " ".join(re.sub(r"[^0-9a-zа-яё/ ]+", " ", str(value or "").lower().replace("ё", "е")).split())


def _parse_number(value: Any) -> float | None:
    text = str(value or "").strip().replace(" ", "")
    if not text:
        return None
    text = text.replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _number_to_json(value: float | None) -> int | float | None:
    if value is None:
        return None
    if abs(value - round(value)) < 1e-9:
        return int(round(value))
    return round(value, 4)


def _manager_surname_key(manager_name: str) -> str:
    parts = [p for p in _norm_text(manager_name).split() if p]
    if not parts:
        return ""
    return parts[-1]


def _detect_block_rows(matrix: list[list[str]]) -> dict[str, int]:
    detected: dict[str, int] = {}
    for idx, row in enumerate(matrix, start=1):
        title = _norm_text(row[0] if row else "")
        if not title:
            continue
        for key in BLOCK_ROW_HINTS:
            if key in title and key not in detected:
                detected[key] = idx
    return detected


def _sheet_preview(matrix: list[list[str]], limit: int = 12) -> list[list[str]]:
    preview: list[list[str]] = []
    for row in matrix:
        if any(str(cell or "").strip() for cell in row):
            preview.append([str(cell or "") for cell in row[:10]])
        if len(preview) >= limit:
            break
    return preview


def _extract_metric_value(row: list[str], metric_name: str) -> float | None:
    month_fact = _parse_number(row[3] if len(row) > 3 else "")
    if metric_name != "reach" or month_fact is not None:
        return month_fact
    weekly_values = [_parse_number(row[idx] if len(row) > idx else "") for idx in WEEKLY_FACT_COLS]
    weekly_clean = [value for value in weekly_values if value is not None]
    if not weekly_clean:
        return None
    return float(sum(weekly_clean))


def _parse_sheet_for_managers(
    *,
    matrix: list[list[str]],
    manager_allowlist: tuple[str, ...],
) -> tuple[dict[str, dict[str, int | float | None]], dict[str, int], list[str], str]:
    block_rows = _detect_block_rows(matrix)
    warnings: list[str] = []
    parsed: dict[str, dict[str, int | float | None]] = {}
    manager_block_row: dict[str, int] = {}

    for manager_name in manager_allowlist:
        surname_key = _manager_surname_key(manager_name)
        if not surname_key:
            warnings.append(f"manager_key_empty:{manager_name}")
            continue
        start_row = block_rows.get(surname_key)
        if not start_row:
            for known_key, known_row in BLOCK_ROW_HINTS.items():
                if known_key in surname_key:
                    start_row = known_row
                    warnings.append(f"block_row_fallback:{manager_name}:{known_row}")
                    break
        if not start_row:
            warnings.append(f"manager_block_missing:{manager_name}")
            continue

        manager_block_row[manager_name] = int(start_row)
        metrics: dict[str, int | float | None] = {}
        for metric_name, offset in METRIC_OFFSETS:
            row_index = int(start_row) - 1 + int(offset)
            if row_index < 0 or row_index >= len(matrix):
                metrics[metric_name] = None
                continue
            row = matrix[row_index]
            metrics[metric_name] = _number_to_json(_extract_metric_value(row, metric_name))

        if all(value is None for value in metrics.values()):
            warnings.append(f"manager_metrics_empty:{manager_name}")
            continue

        parsed[manager_name] = metrics

    parse_status = "metrics_extracted" if parsed else "metrics_unparsed"
    return parsed, manager_block_row, warnings, parse_status


def parse_roks_oap_snapshot(
    *,
    client: Any,
    spreadsheet_id: str,
    period_end: date,
    manager_allowlist: tuple[str, ...],
) -> dict[str, Any]:
    try:
        sheets = client.list_sheets(spreadsheet_id)
        titles = [str(item.get("title") or "").strip() for item in sheets if str(item.get("title") or "").strip()]
    except Exception as exc:
        return {
            "status": "access_error",
            "parse_status": "access_error",
            "warnings": [f"list_sheets_failed:{exc}"],
            "selected_current_month_sheet": "",
            "selected_previous_month_sheet": "",
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
        }

    resolution = resolve_oap_month_sheets(sheet_titles=titles, period_end=period_end)
    selected_current = str(resolution.get("selected_current_month_sheet") or "")
    selected_previous = str(resolution.get("selected_previous_month_sheet") or "")

    snapshot: dict[str, Any] = {
        "status": "sheets_not_found",
        "parse_status": "sheets_not_found",
        "selected_current_month_sheet": selected_current,
        "selected_previous_month_sheet": selected_previous,
        "resolution": resolution,
        "warnings": list(resolution.get("warnings") or []),
        "manager_metrics": {},
        "parsed_metrics_by_manager": {},
        "manager_block_row": {},
        "sheet_previews": {},
        "period_end": period_end.isoformat(),
    }

    if not selected_current or not selected_previous:
        return snapshot

    parsed_metrics_by_manager: dict[str, Any] = {}
    manager_metrics: dict[str, Any] = {}
    manager_block_row: dict[str, Any] = {}

    for role_name, sheet_title in (("current", selected_current), ("previous", selected_previous)):
        try:
            matrix = client.get_values(spreadsheet_id, f"'{sheet_title}'!A1:Q120")
        except Exception as exc:
            snapshot["warnings"].append(f"sheet_read_failed:{role_name}:{exc}")
            continue

        parsed, block_rows, warnings, parse_status = _parse_sheet_for_managers(
            matrix=matrix,
            manager_allowlist=manager_allowlist,
        )
        parsed_metrics_by_manager[role_name] = {
            "sheet": sheet_title,
            "parse_status": parse_status,
            "metrics": parsed,
            "manager_block_row": block_rows,
            "warnings": warnings,
        }
        snapshot["sheet_previews"][role_name] = _sheet_preview(matrix)

        for manager_name, row_no in block_rows.items():
            manager_block_row.setdefault(manager_name, {})[role_name] = row_no

        for manager_name in manager_allowlist:
            manager_metrics.setdefault(manager_name, {})
            manager_metrics[manager_name][f"{role_name}_month"] = parsed.get(manager_name, {})

        snapshot["warnings"].extend(warnings)

    snapshot["parsed_metrics_by_manager"] = parsed_metrics_by_manager
    snapshot["manager_metrics"] = manager_metrics
    snapshot["manager_block_row"] = manager_block_row

    extracted_any = False
    for manager_data in manager_metrics.values():
        if not isinstance(manager_data, dict):
            continue
        for key in ("current_month", "previous_month"):
            metric_values = manager_data.get(key, {})
            if isinstance(metric_values, dict) and any(value is not None for value in metric_values.values()):
                extracted_any = True
                break
        if extracted_any:
            break

    if extracted_any:
        snapshot["status"] = "sheets_found_metrics_extracted"
        snapshot["parse_status"] = "metrics_extracted"
    else:
        snapshot["status"] = "sheets_found_metrics_unparsed"
        snapshot["parse_status"] = "metrics_unparsed"
    return snapshot
