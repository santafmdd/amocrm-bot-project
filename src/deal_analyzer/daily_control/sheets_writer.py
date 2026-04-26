from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id

from ..config import DealAnalyzerConfig
from .idempotency import build_idempotency_key
from .source_reader import DAILY_FIELD_ALIASES, clean_text, map_headers
from .validation.writer_preflight import evaluate_writer_preflight
from .writer_plan import build_writer_plan_payload


DEFAULT_DATA_START_ROW = 3
DEFAULT_HEADER_ROW = 2
DEFAULT_START_COL = "A"
DEFAULT_END_COL = "CS"

BASE_IDENTITY_FIELDS: tuple[str, ...] = ("period_start", "period_end", "control_day_date", "manager_name")
COUNT_IDENTITY_FIELDS: tuple[str, ...] = ("deals_count", "calls_count")
COUNT_FIELDS: tuple[str, ...] = ("sample_size", "deals_count", "calls_count")


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


def _col_letter(index: int) -> str:
    out = ""
    value = max(1, int(index))
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
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
    return "|".join(str(values.get(field, "")).strip() for field in BASE_IDENTITY_FIELDS)


def _counts_from_values(values: dict[str, Any]) -> tuple[int, int, int]:
    return (
        _parse_int(values.get("sample_size", 0)),
        _parse_int(values.get("deals_count", 0)),
        _parse_int(values.get("calls_count", 0)),
    )


def _sort_key_from_values(values: dict[str, Any]) -> tuple[str, str, str, str]:
    period_start = _safe_date(str(values.get("period_start", "")))
    period_end = _safe_date(str(values.get("period_end", "")))
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
            continue
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
            "counts": payload["counts"],
            "values": payload["values"],
            "row_payload": payload["row_payload"],
        }
        placed_existing.append(new_node)
        placements.append(new_node)

    return sorted(placements, key=lambda x: int(x["row_number"]))


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
            "rows_skipped_existing": [],
            "conflicts": [],
            "existing_rows_detected": 0,
            "append_scan_last_nonempty_row": 0,
        }
    count_identity_available = all(field in mapped for field in COUNT_IDENTITY_FIELDS)

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
    rows_skipped_existing: list[dict[str, Any]] = []
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

    placements = _simulate_insert_positions(
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

    return {
        "ok": True,
        "error": "",
        "mapped_columns": {field: headers[idx] for field, idx in mapped.items() if idx < len(headers)},
        "unmapped_columns": header_mapping.unmapped_columns,
        "rows_to_insert": rows_to_insert,
        "rows_skipped_existing": rows_skipped_existing,
        "conflicts": conflicts,
        "existing_rows_detected": len(existing_key_rows),
        "append_scan_last_nonempty_row": last_nonempty_row,
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
        f"rows_prepared: {plan.get('rows_prepared', 0)}",
        f"rows_to_insert: {plan.get('rows_to_insert', 0)}",
        f"rows_skipped_existing: {plan.get('rows_skipped_existing', 0)}",
        f"conflicts_count: {plan.get('conflicts_count', 0)}",
        f"existing_rows_detected: {plan.get('existing_rows_detected', 0)}",
        f"protected_ranges_count: {plan.get('protected_ranges_count', 0)}",
        f"strict_preflight: {plan.get('strict_preflight', False)}",
        f"write_allowed: {plan.get('write_allowed', False)}",
        f"block_reason: {plan.get('block_reason', '')}",
    ]

    planned_ranges = plan.get("planned_ranges", []) if isinstance(plan.get("planned_ranges"), list) else []
    if planned_ranges:
        lines.append("")
        lines.append("planned_ranges:")
        for rng in planned_ranges[:50]:
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
    rows_to_insert = plan.get("rows_to_insert", []) if isinstance(plan.get("rows_to_insert"), list) else []

    row_end_col = _col_letter(max(1, len(headers)))
    planned_ranges = [
        f"'{daily_sheet_name}'!{DEFAULT_START_COL}{int(item.get('row_number', 0) or 0)}:{row_end_col}{int(item.get('row_number', 0) or 0)}"
        for item in rows_to_insert
        if int(item.get("row_number", 0) or 0) > 0
    ]

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
    )
    lint_payload = preflight.get("text_lint", {}) if isinstance(preflight.get("text_lint"), dict) else {}
    quality_blocked = not bool(preflight.get("passed", False))

    protected_ranges_count = _read_protected_ranges_count(
        client,
        spreadsheet_id=spreadsheet_id,
        sheet_name=daily_sheet_name,
    )

    if not bool(plan.get("ok", False)):
        block_reason = str(plan.get("error", "plan_failed"))
    elif quality_blocked:
        block_reason = str(preflight.get("block_reason") or "quality_preflight_failed")
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
    )

    writer_plan = build_writer_plan_payload(
        mode="dry_run" if dry_run else "real_write",
        sheet_name=daily_sheet_name,
        spreadsheet_id=spreadsheet_id,
        rows_prepared=len(payload_rows),
        rows_to_insert=rows_to_insert,
        rows_skipped_existing=(plan.get("rows_skipped_existing", []) if isinstance(plan.get("rows_skipped_existing"), list) else []),
        conflicts=conflicts,
        planned_ranges=planned_ranges,
        existing_rows_detected=int(plan.get("existing_rows_detected", 0) or 0),
        protected_ranges_count=int(protected_ranges_count or 0),
        strict_preflight=bool(strict_preflight),
        write_allowed=bool(write_allowed),
        block_reason=block_reason,
        insert_operations=[{"row_number": int(item.get("row_number", 0) or 0), "row_count": 1} for item in rows_to_insert],
        append_operations=[{"range": rng, "values_rows": 1} for rng in planned_ranges],
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
        "rows_prepared": len(payload_rows),
        "rows_to_insert": len(rows_to_insert),
        "rows_skipped_existing": len(plan.get("rows_skipped_existing", []) if isinstance(plan.get("rows_skipped_existing"), list) else []),
        "conflicts_count": len(conflicts),
        "append_scan_enabled": True,
        "header_row_number": detected_header_row,
        "append_scan_last_nonempty_row": int(plan.get("append_scan_last_nonempty_row", 0) or 0),
        "append_scan_start_row_chosen": int(rows_to_insert[0].get("row_number", 0)) if rows_to_insert else 0,
        "existing_rows_detected": int(plan.get("existing_rows_detected", 0) or 0),
        "duplicate_rows_detected": int(len(plan.get("rows_skipped_existing", [])) if isinstance(plan.get("rows_skipped_existing"), list) else 0),
        "rows_skipped_as_duplicates": int(len(plan.get("rows_skipped_existing", [])) if isinstance(plan.get("rows_skipped_existing"), list) else 0),
        "duplicate_policy": "skip",
        "planned_ranges": planned_ranges,
        "write_allowed": bool(write_allowed),
        "block_reason": block_reason,
        "rows_written": 0,
        "write_start_row": 0,
        "write_end_row": 0,
        "final_written_range": "",
        "error": "",
        "conflicts_artifact": str(conflicts_path),
        "writer_plan_artifact": str(writer_plan_path),
        "writer_plan_md_artifact": str(writer_plan_md_path),
        "preflight": preflight,
        "daily_text_lint": lint_payload,
    }

    if not bool(plan.get("ok", False)):
        status["error"] = str(plan.get("error", "plan_failed"))
        return status

    if quality_blocked:
        status["error"] = str(preflight.get("block_reason") or "quality_preflight_failed")
        return status

    if dry_run or not bool(cfg.deal_analyzer_write_enabled) or not rows_to_insert:
        return status

    write_ranges: list[dict[str, Any]] = []
    for item in rows_to_insert:
        row_number = int(item.get("row_number", 0) or 0)
        if row_number <= 0:
            continue
        client.insert_rows(
            spreadsheet_id=spreadsheet_id,
            tab_name=daily_sheet_name,
            start_index=row_number - 1,
            row_count=1,
        )
        values = [str(x or "") for x in (item.get("values", []) if isinstance(item.get("values"), list) else [])]
        write_ranges.append({"range": f"'{daily_sheet_name}'!{DEFAULT_START_COL}{row_number}:{row_end_col}{row_number}", "values": [values]})

    if write_ranges:
        client.batch_update_values(spreadsheet_id, write_ranges)

    start_row = min(int(item.get("row_number", 0) or 0) for item in rows_to_insert)
    end_row = max(int(item.get("row_number", 0) or 0) for item in rows_to_insert)
    status["rows_written"] = len(rows_to_insert)
    status["write_start_row"] = start_row
    status["write_end_row"] = end_row
    status["final_written_range"] = f"{daily_sheet_name}!{DEFAULT_START_COL}{start_row}:{_col_letter(len(headers))}{end_row}"
    return status


def execute_daily_write(
    *,
    cfg: Any,
    run_dir: Path,
    daily_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    logger: Any,
) -> dict[str, Any]:
    return write_daily_control_rows(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name=daily_sheet_name,
        dry_run=dry_run,
        strict_preflight=strict_preflight,
        logger=logger,
    )


def should_block_real_write(*, conflicts_count: int, strict_preflight: bool) -> bool:
    return bool(strict_preflight and int(conflicts_count or 0) > 0)
