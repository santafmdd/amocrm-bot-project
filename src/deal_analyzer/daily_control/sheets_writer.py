from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id

from ..config import DealAnalyzerConfig
from .idempotency import build_base_key, build_idempotency_key, classify_count_relation
from .source_reader import DAILY_FIELD_ALIASES, clean_text, map_headers
from .validation.writer_preflight import evaluate_writer_preflight
from .writer_plan import build_writer_plan_payload


DEFAULT_DATA_START_ROW = 3
DEFAULT_HEADER_ROW = 2
DEFAULT_START_COL = "A"
DEFAULT_END_COL = "CS"

BASE_IDENTITY_FIELDS: tuple[str, ...] = ("week_start", "week_end", "control_day_date", "manager_name")
COUNT_IDENTITY_FIELDS: tuple[str, ...] = ("deals_count", "calls_count")
COUNT_FIELDS: tuple[str, ...] = ("sample_size", "deals_count", "calls_count")
KEY_OCCUPANCY_FIELDS: tuple[str, ...] = (
    "week_start",
    "week_end",
    "control_day_date",
    "day_label",
    "manager_name",
    "deals_count",
    "deal_links",
)

CRITICALITY_CODE_TO_RU: dict[str, str] = {
    "low": "низкая",
    "medium": "средняя",
    "high": "высокая",
    "critical": "критичная",
}
CRITICALITY_CODE_TO_EN: dict[str, str] = {
    "low": "low",
    "medium": "medium",
    "high": "high",
    "critical": "critical",
}


@dataclass(frozen=True)
class ExistingDailyRow:
    row_number: int
    identity_key: str
    base_key: str
    sort_key: tuple[str, str, str, str]
    counts: tuple[int, int, int]
    values: list[str]


def _norm_text(value: Any) -> str:
    return " ".join(re.sub(r"[^0-9a-zа-яё/ ]+", " ", str(value or "").lower().replace("ё", "е")).split())


def _parse_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    match = re.search(r"-?\d+", text)
    if not match:
        return 0
    try:
        return int(match.group(0))
    except Exception:
        return 0


def _parse_date(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    m = re.search(r"^(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"^(\d{2})\.(\d{2})\.(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return ""


def _safe_date(value: str) -> str:
    parsed = _parse_date(value)
    if not parsed:
        return ""
    try:
        return date.fromisoformat(parsed).isoformat()
    except Exception:
        return ""


def _criticality_code(value: Any) -> str:
    text = _norm_text(value)
    if not text:
        return "low"
    if text in {"low", "низкая", "низкий"}:
        return "low"
    if text in {"medium", "средняя", "средний"}:
        return "medium"
    if text in {"high", "высокая", "высокий"}:
        return "high"
    if text in {"critical", "критичная", "критическая"}:
        return "critical"
    return "low"


def _resolve_criticality_value_for_write(*, requested: Any, allowed_values: list[str]) -> tuple[str, str, str]:
    requested_raw = clean_text(requested)
    code = _criticality_code(requested_raw)
    recommended_ru = CRITICALITY_CODE_TO_RU.get(code, "низкая")
    english = CRITICALITY_CODE_TO_EN.get(code, "low")
    allowed_norm = {_norm_text(v): v for v in allowed_values if clean_text(v)}

    if not allowed_norm:
        return recommended_ru, recommended_ru, "free_input"

    if _norm_text(recommended_ru) in allowed_norm:
        return recommended_ru, clean_text(allowed_norm[_norm_text(recommended_ru)]), "dropdown_russian"
    if _norm_text(english) in allowed_norm:
        return recommended_ru, clean_text(allowed_norm[_norm_text(english)]), "dropdown_english_fallback"
    if _norm_text(requested_raw) in allowed_norm:
        return recommended_ru, clean_text(allowed_norm[_norm_text(requested_raw)]), "dropdown_requested_value"
    return recommended_ru, "", "dropdown_unmatched_blank"


def _col_letter(index: int) -> str:
    out = ""
    value = max(1, int(index))
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
    return out


def _read_dropdown_validation_by_column(
    client: GoogleSheetsApiClient,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    header_row: int,
    max_cols: int,
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    try:
        service = client.build_service()
        meta = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                ranges=[f"'{sheet_name}'!A{header_row}:{_col_letter(max_cols)}{header_row}"],
                includeGridData=True,
                fields="sheets(data.rowData.values(dataValidation))",
            )
            .execute()
        )
        sheets_meta = meta.get("sheets", []) if isinstance(meta, dict) else []
        if not sheets_meta:
            return out
        sheet_meta = sheets_meta[0] if isinstance(sheets_meta[0], dict) else {}
        blocks = sheet_meta.get("data", []) if isinstance(sheet_meta.get("data"), list) else []
        if not blocks:
            return out
        row_data = blocks[0].get("rowData", []) if isinstance(blocks[0], dict) else []
        if not row_data:
            return out
        cells = row_data[0].get("values", []) if isinstance(row_data[0], dict) else []
        for idx, cell in enumerate(cells, start=1):
            if not isinstance(cell, dict):
                continue
            dv = cell.get("dataValidation", {})
            if not isinstance(dv, dict):
                continue
            condition = dv.get("condition", {})
            if not isinstance(condition, dict):
                continue
            values = condition.get("values", [])
            allowed: list[str] = []
            if isinstance(values, list):
                for item in values:
                    if isinstance(item, dict):
                        val = clean_text(item.get("userEnteredValue"))
                        if val:
                            allowed.append(val)
            if allowed:
                out[_col_letter(idx)] = allowed
    except Exception:
        return out
    return out


def _resolve_spreadsheet_id_from_config(cfg: DealAnalyzerConfig) -> str:
    if str(cfg.deal_analyzer_spreadsheet_id or "").strip():
        return str(cfg.deal_analyzer_spreadsheet_id).strip()
    if str(cfg.deal_analyzer_sheet_url or "").strip():
        return extract_spreadsheet_id(str(cfg.deal_analyzer_sheet_url).strip())
    raise RuntimeError("deal_analyzer_spreadsheet_id/deal_analyzer_sheet_url is not set in config")


def _detect_header_row(matrix: list[list[str]], *, start_row: int = 1, min_nonempty: int = 3) -> int:
    for offset, row in enumerate(matrix):
        nonempty = sum(1 for cell in row if clean_text(cell))
        if nonempty >= min_nonempty:
            return start_row + offset
    return DEFAULT_HEADER_ROW


def _base_identity_key(values: dict[str, Any]) -> str:
    return build_base_key(values)


def _counts_from_values(values: dict[str, Any]) -> tuple[int, int, int]:
    return (
        _parse_int(values.get("sample_size", 0)),
        _parse_int(values.get("deals_count", 0)),
        _parse_int(values.get("calls_count", 0)),
    )


def _sort_key_from_values(values: dict[str, Any]) -> tuple[str, str, str, str]:
    period_start = _safe_date(str(values.get("week_start", "") or values.get("period_start", "")))
    period_end = _safe_date(str(values.get("week_end", "") or values.get("period_end", "")))
    control_day = _safe_date(str(values.get("control_day_date", "")))
    manager = _norm_text(values.get("manager_name", ""))
    return (period_start, period_end, control_day, manager)


def _row_is_key_occupied(row: list[str], key_indexes: list[int]) -> bool:
    for idx in key_indexes:
        if idx < len(row) and clean_text(row[idx]):
            return True
    return False


def _values_by_field(row: list[str], mapped: dict[str, int]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field, idx in mapped.items():
        out[field] = row[idx] if idx < len(row) else ""
    return out


def _project_row_to_headers(row: dict[str, Any], headers: list[str], mapped: dict[str, int]) -> list[str]:
    values = ["" for _ in headers]
    for field, idx in mapped.items():
        if field not in row or idx >= len(values):
            if field == "week_start" and "period_start" in row and idx < len(values):
                val = row.get("period_start")
            elif field == "week_end" and "period_end" in row and idx < len(values):
                val = row.get("period_end")
            else:
                continue
        else:
            val = row.get(field)
        values[idx] = "" if val is None else str(val)
    return values


def _simulate_insert_positions(
    *,
    existing_rows: list[ExistingDailyRow],
    new_rows: list[dict[str, Any]],
    data_start_row: int,
) -> list[dict[str, Any]]:
    placed_existing = [
        {"row_number": item.row_number, "sort_key": item.sort_key, "source": "existing"}
        for item in sorted(existing_rows, key=lambda x: x.row_number)
    ]
    placements: list[dict[str, Any]] = []

    for payload in sorted(new_rows, key=lambda x: x["sort_key"]):
        if not placed_existing:
            insert_row = data_start_row
        else:
            predecessors = [node for node in placed_existing if node["sort_key"] <= payload["sort_key"]]
            if predecessors:
                insert_row = max(int(node["row_number"]) for node in predecessors) + 1
            else:
                insert_row = min(int(node["row_number"]) for node in placed_existing)

        for node in placed_existing:
            if int(node["row_number"]) >= insert_row:
                node["row_number"] = int(node["row_number"]) + 1

        new_node = {
            "row_number": insert_row,
            "sort_key": payload["sort_key"],
            "source": "new",
            "identity_key": payload["identity_key"],
            "base_key": payload.get("base_key", ""),
            "counts": payload["counts"],
            "values": payload["values"],
            "row_payload": payload["row_payload"],
        }
        placed_existing.append(new_node)
        placements.append(new_node)

    return sorted(placements, key=lambda x: int(x["row_number"]))


def _plan_values_only_positions(
    *,
    existing_rows: list[ExistingDailyRow],
    new_rows: list[dict[str, Any]],
    data_start_row: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Plan values-only row placement without structural inserts.

    Returns:
      - placed rows with concrete row numbers
      - structural operations that would require insertDimension
    """
    taken_rows = {int(item.row_number) for item in existing_rows}
    context_items: list[dict[str, Any]] = [
        {"row_number": int(item.row_number), "sort_key": item.sort_key, "source": "existing"}
        for item in existing_rows
    ]
    placed: list[dict[str, Any]] = []
    structural_ops: list[dict[str, Any]] = []

    for payload in sorted(new_rows, key=lambda x: x["sort_key"]):
        sort_key = payload["sort_key"]
        predecessors = [int(item["row_number"]) for item in context_items if item["sort_key"] <= sort_key]
        successors = [int(item["row_number"]) for item in context_items if item["sort_key"] > sort_key]

        start_row = (max(predecessors) + 1) if predecessors else int(data_start_row)
        end_row = (min(successors) - 1) if successors else None

        chosen_row = 0
        if end_row is not None:
            for row_number in range(start_row, end_row + 1):
                if row_number not in taken_rows:
                    chosen_row = row_number
                    break
            if chosen_row == 0:
                structural_ops.append(
                    {
                        "type": "insert_row",
                        "identity_key": str(payload.get("identity_key", "")),
                        "base_key": str(payload.get("base_key", "")),
                        "desired_row": int(start_row),
                        "before_row": int(end_row + 1),
                        "reason": "requires_structural_insert_in_middle",
                    }
                )
                continue
        else:
            chosen_row = int(start_row)
            while chosen_row in taken_rows:
                chosen_row += 1

        taken_rows.add(chosen_row)
        node = {
            "row_number": int(chosen_row),
            "sort_key": sort_key,
            "source": "new",
            "identity_key": payload["identity_key"],
            "base_key": payload.get("base_key", ""),
            "counts": payload["counts"],
            "values": payload["values"],
            "row_payload": payload["row_payload"],
        }
        context_items.append({"row_number": int(chosen_row), "sort_key": sort_key, "source": "new"})
        placed.append(node)

    placed.sort(key=lambda x: int(x["row_number"]))
    return placed, structural_ops


def _group_contiguous_row_items(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda x: int(x.get("row_number", 0) or 0))
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    prev_row = 0
    for item in sorted_rows:
        row_number = int(item.get("row_number", 0) or 0)
        if row_number <= 0:
            continue
        if not current:
            current = [item]
            prev_row = row_number
            continue
        if row_number == prev_row + 1:
            current.append(item)
        else:
            groups.append(current)
            current = [item]
        prev_row = row_number
    if current:
        groups.append(current)
    return groups


def discover_daily_control_sheet(
    *,
    cfg: DealAnalyzerConfig,
    workbook_name: str,
    daily_sheet_name: str,
    source_sheet_name: str,
    logger: Any,
) -> dict[str, Any]:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = _resolve_spreadsheet_id_from_config(cfg)

    sheets = client.list_sheets(spreadsheet_id)
    titles = [str(item.get("title") or "").strip() for item in sheets if str(item.get("title") or "").strip()]
    roks_oap_candidates = [title for title in titles if "рокс" in _norm_text(title) and "оап" in _norm_text(title)]

    daily_resolved = client.resolve_sheet(spreadsheet_id, daily_sheet_name)
    source_resolved = client.resolve_sheet(spreadsheet_id, source_sheet_name)

    pre_header_range = f"'{daily_resolved['title']}'!A1:ZZ20"
    pre_header_rows = client.get_values(spreadsheet_id, pre_header_range)
    detected_header_row = _detect_header_row(pre_header_rows, start_row=1, min_nonempty=3)
    header_range = f"'{daily_resolved['title']}'!A{detected_header_row}:ZZ{detected_header_row}"
    header_rows = client.get_values(spreadsheet_id, header_range)
    headers = [str(x or "").strip() for x in (header_rows[0] if header_rows else [])]

    data_start_row = detected_header_row + 1
    first_data_range = f"'{daily_resolved['title']}'!A{data_start_row}:ZZ{data_start_row + 199}"
    first_data = client.get_values(spreadsheet_id, first_data_range)
    first_non_empty: list[dict[str, Any]] = []
    for idx, row in enumerate(first_data, start=data_start_row):
        if any(clean_text(cell) for cell in row):
            first_non_empty.append({"row_number": idx, "values": [str(cell or "") for cell in row[: min(40, len(row))]]})
        if len(first_non_empty) >= 20:
            break

    data_validations: dict[str, list[str]] = {}
    protected_ranges: list[dict[str, Any]] = []
    frozen_rows = int((daily_resolved.get("gridProperties", {}) if isinstance(daily_resolved.get("gridProperties"), dict) else {}).get("frozenRowCount", 0) or 0)
    discovery_warnings: list[str] = []

    try:
        service = client.build_service()
        meta = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                ranges=[f"'{daily_resolved['title']}'!A{detected_header_row}:ZZ{detected_header_row + 20}"],
                includeGridData=True,
                fields="sheets(properties(sheetId,title,index,gridProperties),protectedRanges,data.rowData.values(formattedValue,dataValidation))",
            )
            .execute()
        )
        if isinstance(meta, dict):
            sheets_meta = meta.get("sheets", [])
            if isinstance(sheets_meta, list) and sheets_meta:
                sheet_meta = sheets_meta[0] if isinstance(sheets_meta[0], dict) else {}
                protected_ranges = sheet_meta.get("protectedRanges", []) if isinstance(sheet_meta.get("protectedRanges"), list) else []
                props = sheet_meta.get("properties", {}) if isinstance(sheet_meta.get("properties"), dict) else {}
                grid_props = props.get("gridProperties", {}) if isinstance(props.get("gridProperties"), dict) else {}
                frozen_rows = int(grid_props.get("frozenRowCount", frozen_rows) or frozen_rows)
                data_blocks = sheet_meta.get("data", []) if isinstance(sheet_meta.get("data"), list) else []
                if data_blocks:
                    row_data = data_blocks[0].get("rowData", []) if isinstance(data_blocks[0], dict) else []
                    if row_data:
                        header_cells = row_data[0].get("values", []) if isinstance(row_data[0], dict) else []
                        for col_idx, cell in enumerate(header_cells, start=1):
                            if not isinstance(cell, dict):
                                continue
                            dv = cell.get("dataValidation", {})
                            if not isinstance(dv, dict):
                                continue
                            condition = dv.get("condition", {})
                            if not isinstance(condition, dict):
                                continue
                            values = condition.get("values", [])
                            allowed: list[str] = []
                            if isinstance(values, list):
                                for item in values:
                                    if isinstance(item, dict):
                                        entered = item.get("userEnteredValue")
                                        text = clean_text(entered)
                                        if text:
                                            allowed.append(text)
                            if allowed:
                                data_validations[_col_letter(col_idx)] = allowed
    except Exception as exc:
        discovery_warnings.append(f"metadata_validation_read_failed:{exc}")

    header_mapping = map_headers(headers, DAILY_FIELD_ALIASES)
    return {
        "ok": True,
        "workbook_name": workbook_name,
        "spreadsheet_id": spreadsheet_id,
        "daily_sheet": {
            "title": str(daily_resolved.get("title") or daily_sheet_name),
            "sheet_id": daily_resolved.get("sheetId"),
            "index": daily_resolved.get("index"),
            "row_count": daily_resolved.get("rowCount"),
            "column_count": daily_resolved.get("columnCount"),
            "frozen_rows": frozen_rows,
        },
        "source_sheet": {
            "title": str(source_resolved.get("title") or source_sheet_name),
            "sheet_id": source_resolved.get("sheetId"),
            "index": source_resolved.get("index"),
        },
        "headers_row_number": detected_header_row,
        "data_start_row": data_start_row,
        "headers": headers,
        "mapped_columns": {field: headers[idx] for field, idx in header_mapping.mapped.items() if idx < len(headers)},
        "unmapped_columns": header_mapping.unmapped_columns,
        "dropdown_data_validation": data_validations,
        "protected_ranges_count": len(protected_ranges),
        "protected_ranges": protected_ranges,
        "first_20_nonempty_rows": first_non_empty,
        "sheets": sheets,
        "roks_oap_candidates": roks_oap_candidates,
        "warnings": discovery_warnings,
    }


def plan_daily_control_write(
    *,
    payload_rows: list[dict[str, Any]],
    headers: list[str],
    existing_rows: list[list[str]],
    data_start_row: int = DEFAULT_DATA_START_ROW,
) -> dict[str, Any]:
    header_mapping = map_headers(headers, DAILY_FIELD_ALIASES)
    mapped = header_mapping.mapped

    required_columns = list(BASE_IDENTITY_FIELDS)
    missing_required = [field for field in required_columns if field not in mapped]
    if missing_required:
        return {
            "ok": False,
            "error": "missing_identity_columns",
            "missing_identity_columns": missing_required,
            "rows_to_insert": [],
            "rows_to_update": [],
            "rows_skipped_existing": [],
            "rows_skipped_stale": [],
            "conflicts": [],
            "structural_changes_required": False,
            "planned_structural_operations": [],
            "existing_rows_detected": 0,
            "append_scan_last_nonempty_row": 0,
            "first_empty_row_after_existing_array": int(data_start_row),
        }
    count_identity_available = all(field in mapped for field in COUNT_IDENTITY_FIELDS)

    key_indexes = [mapped[field] for field in KEY_OCCUPANCY_FIELDS if field in mapped]
    if not key_indexes:
        key_indexes = [mapped[field] for field in BASE_IDENTITY_FIELDS if field in mapped]
    existing_key_rows: list[ExistingDailyRow] = []
    existing_exact_index: dict[str, ExistingDailyRow] = {}
    existing_base_index: dict[str, ExistingDailyRow] = {}
    last_nonempty_row = data_start_row - 1

    for offset, row in enumerate(existing_rows):
        row_number = data_start_row + offset
        if _row_is_key_occupied(row, key_indexes):
            last_nonempty_row = max(last_nonempty_row, row_number)
            values = _values_by_field(row, mapped)
            base_identity_key = _base_identity_key(values)
            exact_identity_key = build_idempotency_key(values) if count_identity_available else base_identity_key
            if not base_identity_key.strip():
                continue
            existing_item = ExistingDailyRow(
                row_number=row_number,
                identity_key=exact_identity_key,
                base_key=base_identity_key,
                sort_key=_sort_key_from_values(values),
                counts=_counts_from_values(values),
                values=[str(x or "") for x in row],
            )
            existing_key_rows.append(existing_item)
            if exact_identity_key.strip():
                existing_exact_index[exact_identity_key] = existing_item
            existing_base_index[base_identity_key] = existing_item

    rows_to_insert_payloads: list[dict[str, Any]] = []
    rows_to_update_payloads: list[dict[str, Any]] = []
    rows_skipped_existing: list[dict[str, Any]] = []
    rows_skipped_stale: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []

    for row in payload_rows:
        if not isinstance(row, dict):
            continue
        base_identity_key = _base_identity_key(row)
        exact_identity_key = build_idempotency_key(row) if count_identity_available else base_identity_key
        if not base_identity_key.strip():
            continue

        counts = _counts_from_values(row)
        existing_exact = existing_exact_index.get(exact_identity_key)
        if existing_exact:
            rows_skipped_existing.append(
                {
                    "identity_key": exact_identity_key,
                    "base_key": base_identity_key,
                    "row_number": existing_exact.row_number,
                    "counts": counts,
                }
            )
            continue

        existing_base = existing_base_index.get(base_identity_key)
        if existing_base:
            if not count_identity_available:
                rows_skipped_existing.append(
                    {
                        "identity_key": exact_identity_key,
                        "base_key": base_identity_key,
                        "row_number": existing_base.row_number,
                        "counts": counts,
                        "note": "count_identity_columns_missing_in_sheet",
                    }
                )
                continue
            relation = classify_count_relation(existing_base.counts, counts)
            if relation in {"same"}:
                rows_skipped_existing.append(
                    {
                        "identity_key": exact_identity_key,
                        "base_key": base_identity_key,
                        "row_number": existing_base.row_number,
                        "counts": counts,
                    }
                )
                continue
            if relation == "bigger":
                rows_to_update_payloads.append(
                    {
                        "identity_key": exact_identity_key,
                        "base_key": base_identity_key,
                        "counts": counts,
                        "old_counts": existing_base.counts,
                        "row_number": existing_base.row_number,
                        "values": _project_row_to_headers(row, headers, mapped),
                        "row_payload": row,
                    }
                )
                continue
            if relation == "smaller":
                rows_skipped_stale.append(
                    {
                        "identity_key": exact_identity_key,
                        "base_key": base_identity_key,
                        "row_number": existing_base.row_number,
                        "old_count": {
                            "sample_size": existing_base.counts[0],
                            "deals_count": existing_base.counts[1],
                            "calls_count": existing_base.counts[2],
                        },
                        "new_count": {
                            "sample_size": counts[0],
                            "deals_count": counts[1],
                            "calls_count": counts[2],
                        },
                        "reason": "stale_counts_smaller_than_existing",
                    }
                )
                continue
            conflicts.append(
                {
                    "identity_key": exact_identity_key,
                    "base_key": base_identity_key,
                    "row_number": existing_base.row_number,
                    "manager_name": str(row.get("manager_name", "")),
                    "control_day_date": str(row.get("control_day_date", "")),
                    "old_count": {
                        "sample_size": existing_base.counts[0],
                        "deals_count": existing_base.counts[1],
                        "calls_count": existing_base.counts[2],
                    },
                    "new_count": {
                        "sample_size": counts[0],
                        "deals_count": counts[1],
                        "calls_count": counts[2],
                    },
                    "reason": "conflict_needs_review",
                }
            )
            continue

        rows_to_insert_payloads.append(
            {
                "identity_key": exact_identity_key,
                "base_key": base_identity_key,
                "counts": counts,
                "sort_key": _sort_key_from_values(row),
                "values": _project_row_to_headers(row, headers, mapped),
                "row_payload": row,
            }
        )

    placements, structural_ops = _plan_values_only_positions(
        existing_rows=existing_key_rows,
        new_rows=rows_to_insert_payloads,
        data_start_row=data_start_row,
    )
    rows_to_insert = [
        {
            "row_number": int(item["row_number"]),
            "identity_key": item["identity_key"],
            "base_key": item.get("base_key", ""),
            "counts": item["counts"],
            "values": item["values"],
            "row_payload": item["row_payload"],
        }
        for item in placements
    ]

    rows_to_update = [
        {
            "row_number": int(item.get("row_number", 0) or 0),
            "identity_key": str(item.get("identity_key", "") or ""),
            "base_key": str(item.get("base_key", "") or ""),
            "counts": item.get("counts", (0, 0, 0)),
            "old_counts": item.get("old_counts", (0, 0, 0)),
            "values": item.get("values", []),
            "row_payload": item.get("row_payload", {}),
        }
        for item in sorted(rows_to_update_payloads, key=lambda x: int(x.get("row_number", 0) or 0))
        if int(item.get("row_number", 0) or 0) > 0
    ]

    return {
        "ok": True,
        "error": "",
        "mapped_indexes": {field: idx for field, idx in mapped.items()},
        "mapped_columns": {field: headers[idx] for field, idx in mapped.items() if idx < len(headers)},
        "unmapped_columns": header_mapping.unmapped_columns,
        "rows_to_insert": rows_to_insert,
        "rows_to_update": rows_to_update,
        "rows_skipped_existing": rows_skipped_existing,
        "rows_skipped_stale": rows_skipped_stale,
        "conflicts": conflicts,
        "structural_changes_required": bool(structural_ops),
        "planned_structural_operations": structural_ops,
        "existing_rows_detected": len(existing_key_rows),
        "append_scan_last_nonempty_row": last_nonempty_row,
        "first_empty_row_after_existing_array": max(int(data_start_row), int(last_nonempty_row) + 1),
        "count_identity_available": count_identity_available,
    }


def build_discovery_markdown(discovery: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    lines.append(f"workbook: {discovery.get('workbook_name', '')}")
    lines.append(f"spreadsheet_id: {discovery.get('spreadsheet_id', '')}")
    daily_sheet = discovery.get("daily_sheet", {}) if isinstance(discovery.get("daily_sheet"), dict) else {}
    lines.append(f"daily_sheet: {daily_sheet.get('title', '')} (sheet_id={daily_sheet.get('sheet_id', '')})")
    source_sheet = discovery.get("source_sheet", {}) if isinstance(discovery.get("source_sheet"), dict) else {}
    lines.append(f"source_sheet: {source_sheet.get('title', '')}")
    lines.append(f"headers_count: {len(discovery.get('headers', []) if isinstance(discovery.get('headers'), list) else [])}")
    lines.append(f"dropdown_columns_count: {len(discovery.get('dropdown_data_validation', {}) if isinstance(discovery.get('dropdown_data_validation'), dict) else {})}")
    lines.append(f"protected_ranges_count: {int(discovery.get('protected_ranges_count', 0) or 0)}")

    mapped = discovery.get("mapped_columns", {}) if isinstance(discovery.get("mapped_columns"), dict) else {}
    if mapped:
        lines.append("")
        lines.append("mapped columns:")
        for key, value in mapped.items():
            lines.append(f"- {key}: {value}")

    warnings = discovery.get("warnings", []) if isinstance(discovery.get("warnings"), list) else []
    if warnings:
        lines.append("")
        lines.append("warnings:")
        for warning in warnings:
            lines.append(f"- {warning}")

    preview = discovery.get("first_20_nonempty_rows", []) if isinstance(discovery.get("first_20_nonempty_rows"), list) else []
    if preview:
        lines.append("")
        lines.append("first non-empty rows:")
        for row in preview[:5]:
            if not isinstance(row, dict):
                continue
            lines.append(f"- row {row.get('row_number')}: {row.get('values', [])}")
    return lines


def _read_protected_ranges_count(client: GoogleSheetsApiClient, *, spreadsheet_id: str, sheet_name: str) -> int:
    try:
        resolved = client.resolve_sheet(spreadsheet_id, sheet_name)
        sheet_id = resolved.get("sheetId")
        if sheet_id is None:
            return 0
        service = client.build_service()
        meta = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId),protectedRanges)",
            )
            .execute()
        )
        sheets = meta.get("sheets", []) if isinstance(meta, dict) else []
        for item in sheets:
            if not isinstance(item, dict):
                continue
            props = item.get("properties", {}) if isinstance(item.get("properties"), dict) else {}
            if int(props.get("sheetId", -1)) != int(sheet_id):
                continue
            ranges = item.get("protectedRanges", [])
            if isinstance(ranges, list):
                return len(ranges)
        return 0
    except Exception:
        return 0


def _writer_plan_markdown(plan: dict[str, Any]) -> list[str]:
    lines = [
        f"mode: {plan.get('mode', '')}",
        f"sheet_name: {plan.get('sheet_name', '')}",
        f"spreadsheet_id: {plan.get('spreadsheet_id', '')}",
        f"write_strategy: {plan.get('write_strategy', '')}",
        f"rows_prepared: {plan.get('rows_prepared', 0)}",
        f"rows_to_insert: {plan.get('rows_to_insert', 0)}",
        f"rows_to_update: {plan.get('rows_to_update', 0)}",
        f"rows_skipped_existing: {plan.get('rows_skipped_existing', 0)}",
        f"rows_skipped_stale: {plan.get('rows_skipped_stale', 0)}",
        f"rows_quarantined: {plan.get('rows_quarantined', 0)}",
        f"conflicts_count: {plan.get('conflicts_count', 0)}",
        f"structural_changes_required: {plan.get('structural_changes_required', False)}",
        f"existing_rows_detected: {plan.get('existing_rows_detected', 0)}",
        f"first_empty_row_after_existing_array: {plan.get('first_empty_row_after_existing_array', 0)}",
        f"protected_ranges_count: {plan.get('protected_ranges_count', 0)}",
        f"strict_preflight: {plan.get('strict_preflight', False)}",
        f"write_allowed: {plan.get('write_allowed', False)}",
        f"block_reason: {plan.get('block_reason', '')}",
        f"update_policy: {plan.get('update_policy', '')}",
    ]

    planned_value_ranges = plan.get("planned_value_ranges", []) if isinstance(plan.get("planned_value_ranges"), list) else []
    if planned_value_ranges:
        lines.append("")
        lines.append("planned_value_ranges:")
        for rng in planned_value_ranges[:50]:
            lines.append(f"- {rng}")

    planned_structural_operations = (
        plan.get("planned_structural_operations", [])
        if isinstance(plan.get("planned_structural_operations"), list)
        else []
    )
    if planned_structural_operations:
        lines.append("")
        lines.append("planned_structural_operations:")
        for item in planned_structural_operations[:50]:
            lines.append(f"- {item}")

    planned_append_ranges = plan.get("planned_append_ranges", []) if isinstance(plan.get("planned_append_ranges"), list) else []
    if planned_append_ranges:
        lines.append("")
        lines.append("planned_append_ranges:")
        for rng in planned_append_ranges[:50]:
            lines.append(f"- {rng}")

    idempotency_keys = plan.get("idempotency_keys", []) if isinstance(plan.get("idempotency_keys"), list) else []
    if idempotency_keys:
        lines.append("")
        lines.append("idempotency_keys:")
        for key in idempotency_keys[:50]:
            lines.append(f"- {key}")

    return lines


def write_daily_control_rows(
    *,
    cfg: DealAnalyzerConfig,
    run_dir: Path,
    daily_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    allow_partial_write: bool,
    quarantine_unrepaired: bool,
    logger: Any,
) -> dict[str, Any]:
    payload_path = run_dir / "daily_control_payload.json"
    if not payload_path.exists():
        raise FileNotFoundError(f"Daily payload not found: {payload_path}")

    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload_rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []

    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = _resolve_spreadsheet_id_from_config(cfg)

    probe_headers_range = f"'{daily_sheet_name}'!A1:{DEFAULT_END_COL}20"
    probe_rows = client.get_values(spreadsheet_id, probe_headers_range)
    detected_header_row = _detect_header_row(probe_rows, start_row=1, min_nonempty=3)
    data_start_row = detected_header_row + 1

    headers_range = f"'{daily_sheet_name}'!A{detected_header_row}:{DEFAULT_END_COL}{detected_header_row}"
    header_rows = client.get_values(spreadsheet_id, headers_range)
    headers = [str(x or "").strip() for x in (header_rows[0] if header_rows else [])]
    if not headers:
        raise RuntimeError(f"Daily sheet header row is empty: {daily_sheet_name} row={detected_header_row}")

    data_range = f"'{daily_sheet_name}'!A{data_start_row}:{DEFAULT_END_COL}"
    existing_rows = client.get_values(spreadsheet_id, data_range)

    plan = plan_daily_control_write(
        payload_rows=[row for row in payload_rows if isinstance(row, dict)],
        headers=headers,
        existing_rows=existing_rows,
        data_start_row=data_start_row,
    )

    conflicts = plan.get("conflicts", []) if isinstance(plan.get("conflicts"), list) else []
    plan_rows_to_insert = plan.get("rows_to_insert", []) if isinstance(plan.get("rows_to_insert"), list) else []
    plan_rows_to_update = plan.get("rows_to_update", []) if isinstance(plan.get("rows_to_update"), list) else []
    rows_skipped_existing = plan.get("rows_skipped_existing", []) if isinstance(plan.get("rows_skipped_existing"), list) else []
    rows_skipped_stale = plan.get("rows_skipped_stale", []) if isinstance(plan.get("rows_skipped_stale"), list) else []

    conflicts_path = run_dir / "daily_control_conflicts.json"
    conflicts_path.write_text(
        json.dumps({"conflicts_count": len(conflicts), "conflicts": conflicts}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    preflight = evaluate_writer_preflight(
        rows=[row for row in payload_rows if isinstance(row, dict)],
        strict_preflight=bool(strict_preflight),
        conflicts_count=len(conflicts),
        duplicate_policy="skip",
        allow_partial_write=bool(allow_partial_write),
        quarantine_unrepaired=bool(quarantine_unrepaired),
    )
    lint_payload = preflight.get("text_lint", {}) if isinstance(preflight.get("text_lint"), dict) else {}
    quarantined_rows = preflight.get("quarantined_rows", []) if isinstance(preflight.get("quarantined_rows"), list) else []
    quality_blocked = not bool(preflight.get("passed", False))

    rows_for_write = preflight.get("rows_for_write", []) if isinstance(preflight.get("rows_for_write"), list) else []
    allowed_base_keys = {build_base_key(row) for row in rows_for_write if isinstance(row, dict)}
    allowed_exact_keys = {build_idempotency_key(row) for row in rows_for_write if isinstance(row, dict)}

    rows_to_insert = [
        item
        for item in plan_rows_to_insert
        if str(item.get("base_key", "") or "") in allowed_base_keys
        or str(item.get("identity_key", "") or "") in allowed_exact_keys
    ]
    rows_to_update = [
        item
        for item in plan_rows_to_update
        if str(item.get("base_key", "") or "") in allowed_base_keys
        or str(item.get("identity_key", "") or "") in allowed_exact_keys
    ]

    mapped_indexes = plan.get("mapped_indexes", {}) if isinstance(plan.get("mapped_indexes"), dict) else {}
    dropdown_validation = _read_dropdown_validation_by_column(
        client,
        spreadsheet_id=spreadsheet_id,
        sheet_name=daily_sheet_name,
        header_row=detected_header_row,
        max_cols=max(1, len(headers)),
    )
    criticality_resolution: list[dict[str, Any]] = []
    criticality_field_idx = int(mapped_indexes.get("criticality", -1))
    criticality_col = _col_letter(criticality_field_idx + 1) if criticality_field_idx >= 0 else ""
    criticality_allowed = dropdown_validation.get(criticality_col, []) if criticality_col else []

    for item in rows_to_insert + rows_to_update:
        if not isinstance(item, dict):
            continue
        row_payload = item.get("row_payload", {}) if isinstance(item.get("row_payload"), dict) else {}
        requested = row_payload.get("criticality")
        recommended_ru, written_value, mode = _resolve_criticality_value_for_write(
            requested=requested,
            allowed_values=criticality_allowed,
        )
        row_payload["criticality"] = written_value
        item["row_payload"] = row_payload
        values = item.get("values", []) if isinstance(item.get("values"), list) else []
        if criticality_field_idx >= 0 and criticality_field_idx < len(values):
            values[criticality_field_idx] = written_value
        item["values"] = values
        criticality_resolution.append(
            {
                "row_number": int(item.get("row_number", 0) or 0),
                "identity_key": str(item.get("identity_key", "") or ""),
                "criticality_requested_value": clean_text(requested),
                "criticality_written_value": clean_text(written_value),
                "recommended_russian_value": recommended_ru,
                "criticality_validation_mode": mode,
                "criticality_dropdown_values": criticality_allowed,
            }
        )

    structural_changes_required = bool(plan.get("structural_changes_required", False))
    planned_structural_operations = (
        plan.get("planned_structural_operations", [])
        if isinstance(plan.get("planned_structural_operations"), list)
        else []
    )

    row_end_col = _col_letter(max(1, len(headers)))
    planned_insert_ranges: list[str] = []
    for group in _group_contiguous_row_items(rows_to_insert):
        if not group:
            continue
        start_row = int(group[0].get("row_number", 0) or 0)
        end_row = int(group[-1].get("row_number", 0) or 0)
        if start_row <= 0 or end_row <= 0:
            continue
        planned_insert_ranges.append(f"'{daily_sheet_name}'!{DEFAULT_START_COL}{start_row}:{row_end_col}{end_row}")
    planned_update_ranges = [
        f"'{daily_sheet_name}'!{DEFAULT_START_COL}{int(item.get('row_number', 0) or 0)}:{row_end_col}{int(item.get('row_number', 0) or 0)}"
        for item in rows_to_update
        if int(item.get("row_number", 0) or 0) > 0
    ]
    planned_append_ranges = list(planned_insert_ranges)
    planned_value_ranges = planned_insert_ranges + planned_update_ranges

    protected_ranges_count = _read_protected_ranges_count(
        client,
        spreadsheet_id=spreadsheet_id,
        sheet_name=daily_sheet_name,
    )

    if not bool(plan.get("ok", False)):
        block_reason = str(plan.get("error", "plan_failed"))
    elif quality_blocked:
        block_reason = str(preflight.get("block_reason") or "quality_preflight_failed")
    elif structural_changes_required:
        block_reason = "requires_structural_insert"
    elif dry_run:
        block_reason = "dry_run_mode"
    elif not bool(cfg.deal_analyzer_write_enabled):
        block_reason = "write_disabled_by_config"
    else:
        block_reason = ""

    write_allowed = bool(
        (not dry_run)
        and bool(cfg.deal_analyzer_write_enabled)
        and bool(plan.get("ok", False))
        and (not quality_blocked)
        and (not structural_changes_required)
    )

    # Normal strategy is values-only. Structural operations are diagnostics only.
    insert_operations: list[dict[str, Any]] = []
    update_operations = [{"row_number": int(item.get("row_number", 0) or 0), "row_count": 1} for item in rows_to_update]

    writer_plan = build_writer_plan_payload(
        mode="dry_run" if dry_run else "real_write",
        sheet_name=daily_sheet_name,
        spreadsheet_id=spreadsheet_id,
        write_strategy="values_only",
        rows_prepared=len(payload_rows),
        rows_to_insert=rows_to_insert,
        rows_to_update=rows_to_update,
        rows_skipped_existing=rows_skipped_existing,
        rows_skipped_stale=rows_skipped_stale,
        rows_quarantined=quarantined_rows,
        conflicts=conflicts,
        structural_changes_required=structural_changes_required,
        planned_value_ranges=planned_value_ranges,
        planned_structural_operations=planned_structural_operations,
        planned_ranges=planned_value_ranges,
        planned_update_ranges=planned_update_ranges,
        planned_append_ranges=planned_append_ranges,
        existing_rows_detected=int(plan.get("existing_rows_detected", 0) or 0),
        first_empty_row_after_existing_array=int(plan.get("first_empty_row_after_existing_array", 0) or 0),
        protected_ranges_count=int(protected_ranges_count or 0),
        strict_preflight=bool(strict_preflight),
        write_allowed=bool(write_allowed),
        block_reason=block_reason,
        insert_operations=insert_operations,
        update_operations=update_operations,
        append_operations=[],
        update_policy="update_if_new_count_is_bigger",
        criticality_resolution=criticality_resolution,
    )
    writer_plan["preflight"] = preflight
    writer_plan["daily_text_lint"] = lint_payload

    writer_plan_path = run_dir / "daily_control_writer_plan.json"
    writer_plan_md_path = run_dir / "daily_control_writer_plan.md"
    writer_plan_path.write_text(json.dumps(writer_plan, ensure_ascii=False, indent=2), encoding="utf-8")
    writer_plan_md_path.write_text(
        "# Daily Control Writer Plan\n\n" + "\n".join(_writer_plan_markdown(writer_plan)).strip() + "\n",
        encoding="utf-8",
    )

    status = {
        "sheet_name": daily_sheet_name,
        "mode": "dry_run" if dry_run else "real_write",
        "strict_preflight": bool(strict_preflight),
        "write_strategy": "values_only",
        "rows_prepared": len(payload_rows),
        "rows_to_insert": len(rows_to_insert),
        "rows_to_update": len(rows_to_update),
        "rows_skipped_existing": len(rows_skipped_existing),
        "rows_skipped_stale": len(rows_skipped_stale),
        "rows_quarantined": len(quarantined_rows),
        "conflicts_count": len(conflicts),
        "structural_changes_required": structural_changes_required,
        "append_scan_enabled": True,
        "header_row_number": detected_header_row,
        "append_scan_last_nonempty_row": int(plan.get("append_scan_last_nonempty_row", 0) or 0),
        "first_empty_row_after_existing_array": int(plan.get("first_empty_row_after_existing_array", 0) or 0),
        "append_scan_start_row_chosen": int(rows_to_insert[0].get("row_number", 0)) if rows_to_insert else 0,
        "existing_rows_detected": int(plan.get("existing_rows_detected", 0) or 0),
        "duplicate_rows_detected": int(len(rows_skipped_existing)),
        "rows_skipped_as_duplicates": int(len(rows_skipped_existing)),
        "duplicate_policy": "skip",
        "planned_ranges": planned_value_ranges,
        "planned_value_ranges": planned_value_ranges,
        "planned_append_ranges": planned_append_ranges,
        "planned_structural_operations": planned_structural_operations,
        "planned_update_ranges": planned_update_ranges,
        "allow_partial_write": bool(allow_partial_write),
        "quarantine_unrepaired": bool(quarantine_unrepaired),
        "write_allowed": bool(write_allowed),
        "block_reason": block_reason,
        "rows_written": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "write_start_row": 0,
        "write_end_row": 0,
        "final_written_range": "",
        "error": "",
        "conflicts_artifact": str(conflicts_path),
        "writer_plan_artifact": str(writer_plan_path),
        "writer_plan_md_artifact": str(writer_plan_md_path),
        "update_policy": "update_if_new_count_is_bigger",
        "criticality_resolution": criticality_resolution,
        "preflight": preflight,
        "daily_text_lint": lint_payload,
    }
    requested_values = sorted(
        {
            str(item.get("criticality_requested_value") or "").strip()
            for item in criticality_resolution
            if str(item.get("criticality_requested_value") or "").strip()
        }
    )
    written_values = sorted(
        {
            str(item.get("criticality_written_value") or "").strip()
            for item in criticality_resolution
            if str(item.get("criticality_written_value") or "").strip()
        }
    )
    modes = sorted(
        {
            str(item.get("criticality_validation_mode") or "").strip()
            for item in criticality_resolution
            if str(item.get("criticality_validation_mode") or "").strip()
        }
    )
    status["criticality_requested_value"] = requested_values[0] if len(requested_values) == 1 else ("mixed" if requested_values else "")
    status["criticality_written_value"] = written_values[0] if len(written_values) == 1 else ("mixed" if written_values else "")
    status["criticality_validation_mode"] = modes[0] if len(modes) == 1 else ("mixed" if modes else "")

    if not bool(plan.get("ok", False)):
        status["error"] = str(plan.get("error", "plan_failed"))
        return status

    if quality_blocked:
        status["error"] = str(preflight.get("block_reason") or "quality_preflight_failed")
        return status

    if structural_changes_required:
        status["error"] = "requires_structural_insert"
        return status

    if dry_run or not bool(cfg.deal_analyzer_write_enabled) or (not rows_to_insert and not rows_to_update):
        return status

    mapped_indexes = plan.get("mapped_indexes", {}) if isinstance(plan.get("mapped_indexes"), dict) else {}
    write_data: list[dict[str, Any]] = []

    update_ranges: list[dict[str, Any]] = []
    for item in rows_to_update:
        row_number = int(item.get("row_number", 0) or 0)
        if row_number <= 0:
            continue
        row_payload = item.get("row_payload", {}) if isinstance(item.get("row_payload"), dict) else {}
        for field, idx in mapped_indexes.items():
            if field not in row_payload:
                continue
            col = _col_letter(int(idx) + 1)
            value = row_payload.get(field)
            update_ranges.append(
                {
                    "range": f"'{daily_sheet_name}'!{col}{row_number}:{col}{row_number}",
                    "values": [["" if value is None else str(value)]],
                }
            )
    if update_ranges:
        write_data.extend(update_ranges)

    for group in _group_contiguous_row_items(rows_to_insert):
        if not group:
            continue
        start_row = int(group[0].get("row_number", 0) or 0)
        end_row = int(group[-1].get("row_number", 0) or 0)
        if start_row <= 0 or end_row <= 0:
            continue
        values_matrix: list[list[str]] = []
        for item in group:
            row_values = item.get("values", []) if isinstance(item.get("values"), list) else []
            values_matrix.append([str(x or "") for x in row_values])
        write_data.append(
            {
                "range": f"'{daily_sheet_name}'!{DEFAULT_START_COL}{start_row}:{row_end_col}{end_row}",
                "values": values_matrix,
            }
        )

    if write_data:
        client.batch_update_values(spreadsheet_id, write_data)

    all_row_numbers = [int(item.get("row_number", 0) or 0) for item in rows_to_insert + rows_to_update if int(item.get("row_number", 0) or 0) > 0]
    start_row = min(all_row_numbers) if all_row_numbers else 0
    end_row = max(all_row_numbers) if all_row_numbers else 0
    status["rows_written"] = len(rows_to_insert) + len(rows_to_update)
    status["rows_inserted"] = len(rows_to_insert)
    status["rows_updated"] = len(rows_to_update)
    status["write_start_row"] = start_row
    status["write_end_row"] = end_row
    if start_row > 0 and end_row > 0:
        status["final_written_range"] = f"{daily_sheet_name}!{DEFAULT_START_COL}{start_row}:{_col_letter(len(headers))}{end_row}"
    return status


def execute_daily_write(
    *,
    cfg: Any,
    run_dir: Path,
    daily_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    allow_partial_write: bool,
    quarantine_unrepaired: bool,
    logger: Any,
) -> dict[str, Any]:
    return write_daily_control_rows(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name=daily_sheet_name,
        dry_run=dry_run,
        strict_preflight=strict_preflight,
        allow_partial_write=allow_partial_write,
        quarantine_unrepaired=quarantine_unrepaired,
        logger=logger,
    )


def should_block_real_write(*, conflicts_count: int, strict_preflight: bool) -> bool:
    return bool(strict_preflight and int(conflicts_count or 0) > 0)
