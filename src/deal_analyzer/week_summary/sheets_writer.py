from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers, parse_date
from ..weekly_shared.writer_utils import col_letter, group_contiguous_row_items, row_is_occupied
from .source_reader import (
    WEEK_SUMMARY_MANAGER_ALIASES,
    WEEK_SUMMARY_PLAN_ALIASES,
    WEEK_SUMMARY_TARGET_ALIASES,
    resolve_spreadsheet_id,
)
from .validation import evaluate_writer_preflight
from .writer_plan import build_writer_plan_payload


DEFAULT_START_COL = "A"
DEFAULT_END_COL = "AZ"
BASE_FIELDS: tuple[str, ...] = ("week_start", "week_end")
COUNT_FIELD = "deals_count"
KEY_OCCUPANCY_FIELDS: tuple[str, ...] = ("week_start", "week_end")


@dataclass(frozen=True)
class ExistingWeekSummaryRow:
    row_number: int
    base_key: str
    exact_key: str
    deals_count: int
    sort_key: tuple[str, str]
    values: list[str]


def _parse_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    digits = "".join(ch for ch in text if ch.isdigit() or ch == "-")
    if not digits:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def _sort_key_from_values(values: dict[str, Any]) -> tuple[str, str]:
    return (
        parse_date(str(values.get("week_start", ""))),
        parse_date(str(values.get("week_end", ""))),
    )


def _values_by_field(row: list[str], mapped: dict[str, int]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field, idx in mapped.items():
        out[field] = row[idx] if idx < len(row) else ""
    return out


def _project_row_to_headers(row: dict[str, Any], headers: list[str], mapped: dict[str, int]) -> list[str]:
    values = ["" for _ in headers]
    for field, idx in mapped.items():
        if idx >= len(values):
            continue
        values[idx] = "" if row.get(field) is None else str(row.get(field))
    return values


def _build_base_key(values: dict[str, Any]) -> str:
    return "|".join([parse_date(str(values.get("week_start", ""))), parse_date(str(values.get("week_end", "")))])


def _build_exact_key(values: dict[str, Any]) -> str:
    return "|".join([_build_base_key(values), clean_text(values.get("deals_count", "0")), clean_text(values.get("brief_report", ""))[:50]])


def discover_week_summary_sheet(
    *,
    cfg: Any,
    workbook_name: str,
    manager_summary_sheet_name: str,
    plan_sheet_name: str,
    target_sheet_name: str,
    logger: Any,
) -> dict[str, Any]:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = resolve_spreadsheet_id(cfg)
    sheets = client.list_sheets(spreadsheet_id)
    titles = [str(item.get("title") or "").strip() for item in sheets if str(item.get("title") or "").strip()]

    manager_resolved = client.resolve_sheet(spreadsheet_id, manager_summary_sheet_name)
    plan_resolved = client.resolve_sheet(spreadsheet_id, plan_sheet_name)
    target_resolved = client.resolve_sheet(spreadsheet_id, target_sheet_name)

    manager_matrix = client.get_values(spreadsheet_id, f"'{manager_resolved['title']}'!A1:{DEFAULT_END_COL}40")
    plan_matrix = client.get_values(spreadsheet_id, f"'{plan_resolved['title']}'!A1:{DEFAULT_END_COL}40")
    target_matrix = client.get_values(spreadsheet_id, f"'{target_resolved['title']}'!A1:{DEFAULT_END_COL}40")

    manager_header_row = detect_header_row(manager_matrix, start_row=1, min_nonempty=3)
    plan_header_row = detect_header_row(plan_matrix, start_row=1, min_nonempty=3)
    target_header_row = detect_header_row(target_matrix, start_row=1, min_nonempty=3)

    manager_headers = [clean_text(x) for x in (manager_matrix[manager_header_row - 1] if manager_matrix else [])]
    plan_headers = [clean_text(x) for x in (plan_matrix[plan_header_row - 1] if plan_matrix else [])]
    target_headers = [clean_text(x) for x in (target_matrix[target_header_row - 1] if target_matrix else [])]

    manager_mapping = map_headers(manager_headers, WEEK_SUMMARY_MANAGER_ALIASES)
    plan_mapping = map_headers(plan_headers, WEEK_SUMMARY_PLAN_ALIASES)
    target_mapping = map_headers(target_headers, WEEK_SUMMARY_TARGET_ALIASES)

    return {
        "workbook_name": workbook_name,
        "spreadsheet_id": spreadsheet_id,
        "sheet_titles": titles,
        "manager_summary_sheet": {
            "title": manager_resolved.get("title", manager_summary_sheet_name),
            "sheet_id": manager_resolved.get("sheetId"),
            "header_row_number": manager_header_row,
        },
        "plan_sheet": {
            "title": plan_resolved.get("title", plan_sheet_name),
            "sheet_id": plan_resolved.get("sheetId"),
            "header_row_number": plan_header_row,
        },
        "target_sheet": {
            "title": target_resolved.get("title", target_sheet_name),
            "sheet_id": target_resolved.get("sheetId"),
            "header_row_number": target_header_row,
        },
        "manager_headers": manager_headers,
        "plan_headers": plan_headers,
        "target_headers": target_headers,
        "manager_mapped_columns": {
            field: manager_headers[idx] for field, idx in manager_mapping.mapped.items() if idx < len(manager_headers)
        },
        "plan_mapped_columns": {field: plan_headers[idx] for field, idx in plan_mapping.mapped.items() if idx < len(plan_headers)},
        "target_mapped_columns": {
            field: target_headers[idx] for field, idx in target_mapping.mapped.items() if idx < len(target_headers)
        },
        "manager_unmapped_columns": manager_mapping.unmapped_columns,
        "plan_unmapped_columns": plan_mapping.unmapped_columns,
        "target_unmapped_columns": target_mapping.unmapped_columns,
    }


def build_discovery_markdown(discovery: dict[str, Any]) -> list[str]:
    lines = [
        f"workbook: {discovery.get('workbook_name', '')}",
        f"spreadsheet_id: {discovery.get('spreadsheet_id', '')}",
        f"manager_summary_sheet: {((discovery.get('manager_summary_sheet') or {}) if isinstance(discovery.get('manager_summary_sheet'), dict) else {}).get('title', '')}",
        f"plan_sheet: {((discovery.get('plan_sheet') or {}) if isinstance(discovery.get('plan_sheet'), dict) else {}).get('title', '')}",
        f"target_sheet: {((discovery.get('target_sheet') or {}) if isinstance(discovery.get('target_sheet'), dict) else {}).get('title', '')}",
    ]
    mapped = discovery.get("target_mapped_columns", {}) if isinstance(discovery.get("target_mapped_columns"), dict) else {}
    if mapped:
        lines.append("")
        lines.append("target mapped columns:")
        for field, value in mapped.items():
            lines.append(f"- {field}: {value}")
    warnings = discovery.get("target_unmapped_columns", []) if isinstance(discovery.get("target_unmapped_columns"), list) else []
    if warnings:
        lines.append("")
        lines.append("target unmapped columns:")
        for item in warnings[:40]:
            lines.append(f"- {item}")
    return lines


def plan_week_summary_write(
    *,
    payload_rows: list[dict[str, Any]],
    headers: list[str],
    existing_rows: list[list[str]],
    data_start_row: int,
) -> dict[str, Any]:
    mapped = map_headers(headers, WEEK_SUMMARY_TARGET_ALIASES).mapped
    missing_required = [field for field in BASE_FIELDS if field not in mapped]
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
            "append_scan_last_nonempty_row": int(data_start_row - 1),
            "first_empty_row_after_existing_array": int(data_start_row),
            "mapped_indexes": mapped,
            "mapped_columns": {},
        }

    key_indexes = [mapped[field] for field in KEY_OCCUPANCY_FIELDS if field in mapped]
    existing_items: list[ExistingWeekSummaryRow] = []
    existing_base_index: dict[str, ExistingWeekSummaryRow] = {}
    last_nonempty_row = data_start_row - 1
    for offset, row in enumerate(existing_rows):
        row_number = data_start_row + offset
        if not row_is_occupied(row, key_indexes):
            continue
        last_nonempty_row = max(last_nonempty_row, row_number)
        values = _values_by_field(row, mapped)
        base_key = _build_base_key(values)
        if not base_key:
            continue
        existing = ExistingWeekSummaryRow(
            row_number=row_number,
            base_key=base_key,
            exact_key=_build_exact_key(values),
            deals_count=_parse_int(values.get("deals_count")),
            sort_key=_sort_key_from_values(values),
            values=[str(item or "") for item in row],
        )
        existing_items.append(existing)
        existing_base_index[base_key] = existing

    rows_to_update: list[dict[str, Any]] = []
    rows_skipped_existing: list[dict[str, Any]] = []
    rows_skipped_stale: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    new_payloads: list[dict[str, Any]] = []

    for row in payload_rows:
        if not isinstance(row, dict):
            continue
        base_key = _build_base_key(row)
        if not base_key:
            continue
        existing = existing_base_index.get(base_key)
        new_count = _parse_int(row.get(COUNT_FIELD)) if COUNT_FIELD in mapped else 0
        if existing is not None:
            exact_key = _build_exact_key(row)
            if existing.exact_key == exact_key:
                rows_skipped_existing.append({"base_key": base_key, "row_number": existing.row_number, "reason": "exact_duplicate"})
                continue
            if COUNT_FIELD in mapped and new_count < existing.deals_count:
                rows_skipped_stale.append(
                    {
                        "base_key": base_key,
                        "row_number": existing.row_number,
                        "old_deals_count": existing.deals_count,
                        "new_deals_count": new_count,
                        "reason": "stale_counts_smaller_than_existing",
                    }
                )
                continue
            rows_to_update.append(
                {
                    "base_key": base_key,
                    "row_number": existing.row_number,
                    "old_deals_count": existing.deals_count,
                    "new_deals_count": new_count,
                    "values": _project_row_to_headers(row, headers, mapped),
                    "row_payload": row,
                }
            )
            continue
        new_payloads.append(
            {
                "base_key": base_key,
                "sort_key": _sort_key_from_values(row),
                "deals_count": new_count,
                "values": _project_row_to_headers(row, headers, mapped),
                "row_payload": row,
            }
        )

    first_empty_row = max(int(data_start_row), int(last_nonempty_row) + 1)
    structural_ops: list[dict[str, Any]] = []
    rows_to_insert: list[dict[str, Any]] = []
    existing_sort_keys = [item.sort_key for item in existing_items]
    next_row = first_empty_row
    for payload in sorted(new_payloads, key=lambda item: item["sort_key"]):
        has_successor = any(existing_key > payload["sort_key"] for existing_key in existing_sort_keys)
        if has_successor:
            structural_ops.append(
                {
                    "type": "insert_row",
                    "base_key": payload["base_key"],
                    "reason": "requires_structural_insert_in_middle",
                }
            )
            continue
        rows_to_insert.append(
            {
                "row_number": next_row,
                "base_key": payload["base_key"],
                "deals_count": payload["deals_count"],
                "values": payload["values"],
                "row_payload": payload["row_payload"],
            }
        )
        next_row += 1

    return {
        "ok": True,
        "error": "",
        "rows_to_insert": rows_to_insert,
        "rows_to_update": rows_to_update,
        "rows_skipped_existing": rows_skipped_existing,
        "rows_skipped_stale": rows_skipped_stale,
        "conflicts": conflicts,
        "structural_changes_required": bool(structural_ops),
        "planned_structural_operations": structural_ops,
        "existing_rows_detected": len(existing_items),
        "append_scan_last_nonempty_row": int(last_nonempty_row),
        "first_empty_row_after_existing_array": int(first_empty_row),
        "mapped_indexes": mapped,
    }


def _writer_plan_markdown(plan: dict[str, Any]) -> list[str]:
    lines = [
        f"mode: {plan.get('mode', '')}",
        f"sheet_name: {plan.get('sheet_name', '')}",
        f"write_strategy: {plan.get('write_strategy', '')}",
        f"rows_prepared: {plan.get('rows_prepared', 0)}",
        f"rows_to_insert: {plan.get('rows_to_insert', 0)}",
        f"rows_to_update: {plan.get('rows_to_update', 0)}",
        f"rows_skipped_existing: {plan.get('rows_skipped_existing', 0)}",
        f"rows_skipped_stale: {plan.get('rows_skipped_stale', 0)}",
        f"rows_quarantined: {plan.get('rows_quarantined', 0)}",
        f"conflicts_count: {plan.get('conflicts_count', 0)}",
        f"structural_changes_required: {plan.get('structural_changes_required', False)}",
        f"write_allowed: {plan.get('write_allowed', False)}",
        f"block_reason: {plan.get('block_reason', '')}",
    ]
    if isinstance(plan.get("planned_value_ranges"), list) and plan.get("planned_value_ranges"):
        lines.append("")
        lines.append("planned_value_ranges:")
        for item in plan.get("planned_value_ranges", [])[:100]:
            lines.append(f"- {item}")
    return lines


def write_week_summary_rows(
    *,
    cfg: Any,
    run_dir: Path,
    target_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    allow_partial_write: bool,
    quarantine_unrepaired: bool,
    logger: Any,
) -> dict[str, Any]:
    payload_path = run_dir / "week_summary_payload.json"
    if not payload_path.exists():
        raise FileNotFoundError(f"Week summary payload not found: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload_rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []

    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = resolve_spreadsheet_id(cfg)

    probe = client.get_values(spreadsheet_id, f"'{target_sheet_name}'!A1:{DEFAULT_END_COL}30")
    header_row_number = detect_header_row(probe, start_row=1, min_nonempty=3)
    data_start_row = header_row_number + 1
    header_rows = client.get_values(spreadsheet_id, f"'{target_sheet_name}'!A{header_row_number}:{DEFAULT_END_COL}{header_row_number}")
    headers = [clean_text(item) for item in (header_rows[0] if header_rows else [])]
    if not headers:
        raise RuntimeError(f"Target sheet header row is empty: {target_sheet_name} row={header_row_number}")
    existing_rows = client.get_values(spreadsheet_id, f"'{target_sheet_name}'!A{data_start_row}:{DEFAULT_END_COL}")

    plan = plan_week_summary_write(
        payload_rows=[row for row in payload_rows if isinstance(row, dict)],
        headers=headers,
        existing_rows=existing_rows,
        data_start_row=data_start_row,
    )

    conflicts = plan.get("conflicts", []) if isinstance(plan.get("conflicts"), list) else []
    rows_to_insert = plan.get("rows_to_insert", []) if isinstance(plan.get("rows_to_insert"), list) else []
    rows_to_update = plan.get("rows_to_update", []) if isinstance(plan.get("rows_to_update"), list) else []
    rows_skipped_existing = plan.get("rows_skipped_existing", []) if isinstance(plan.get("rows_skipped_existing"), list) else []
    rows_skipped_stale = plan.get("rows_skipped_stale", []) if isinstance(plan.get("rows_skipped_stale"), list) else []
    structural_changes_required = bool(plan.get("structural_changes_required", False))
    planned_structural_operations = (
        plan.get("planned_structural_operations", []) if isinstance(plan.get("planned_structural_operations"), list) else []
    )

    preflight = evaluate_writer_preflight(
        rows=[row for row in payload_rows if isinstance(row, dict)],
        strict_preflight=bool(strict_preflight),
        conflicts_count=len(conflicts),
        allow_partial_write=bool(allow_partial_write),
        quarantine_unrepaired=bool(quarantine_unrepaired),
    )
    quality_blocked = not bool(preflight.get("passed", False))
    rows_for_write = preflight.get("rows_for_write", []) if isinstance(preflight.get("rows_for_write"), list) else []
    allowed_keys = {_build_base_key(row) for row in rows_for_write if isinstance(row, dict)}
    rows_to_insert = [item for item in rows_to_insert if str(item.get("base_key", "")) in allowed_keys]
    rows_to_update = [item for item in rows_to_update if str(item.get("base_key", "")) in allowed_keys]
    quarantined_rows = preflight.get("quarantined_rows", []) if isinstance(preflight.get("quarantined_rows"), list) else []

    row_end_col = col_letter(max(1, len(headers)))
    planned_insert_ranges: list[str] = []
    for group in group_contiguous_row_items(rows_to_insert):
        start = int(group[0].get("row_number", 0) or 0)
        end = int(group[-1].get("row_number", 0) or 0)
        if start > 0 and end > 0:
            planned_insert_ranges.append(f"'{target_sheet_name}'!{DEFAULT_START_COL}{start}:{row_end_col}{end}")
    planned_update_ranges = [
        f"'{target_sheet_name}'!{DEFAULT_START_COL}{int(item.get('row_number', 0) or 0)}:{row_end_col}{int(item.get('row_number', 0) or 0)}"
        for item in rows_to_update
        if int(item.get("row_number", 0) or 0) > 0
    ]
    planned_value_ranges = planned_insert_ranges + planned_update_ranges

    if not bool(plan.get("ok", False)):
        block_reason = str(plan.get("error") or "plan_failed")
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

    writer_plan = build_writer_plan_payload(
        mode="dry_run" if dry_run else "real_write",
        sheet_name=target_sheet_name,
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
        planned_append_ranges=planned_insert_ranges,
        existing_rows_detected=int(plan.get("existing_rows_detected", 0) or 0),
        first_empty_row_after_existing_array=int(plan.get("first_empty_row_after_existing_array", 0) or 0),
        protected_ranges_count=0,
        strict_preflight=bool(strict_preflight),
        write_allowed=bool(write_allowed),
        block_reason=block_reason,
        insert_operations=[],
        update_operations=[{"row_number": int(item.get("row_number", 0) or 0), "row_count": 1} for item in rows_to_update],
        append_operations=[],
        update_policy="update_if_new_facts_present",
    )
    writer_plan["preflight"] = preflight
    writer_plan_path = run_dir / "week_summary_writer_plan.json"
    writer_plan_md_path = run_dir / "week_summary_writer_plan.md"
    writer_plan_path.write_text(json.dumps(writer_plan, ensure_ascii=False, indent=2), encoding="utf-8")
    writer_plan_md_path.write_text(
        "# Week Summary Writer Plan\n\n" + "\n".join(_writer_plan_markdown(writer_plan)).strip() + "\n",
        encoding="utf-8",
    )

    status = {
        "sheet_name": target_sheet_name,
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
        "header_row_number": header_row_number,
        "append_scan_last_nonempty_row": int(plan.get("append_scan_last_nonempty_row", 0) or 0),
        "first_empty_row_after_existing_array": int(plan.get("first_empty_row_after_existing_array", 0) or 0),
        "existing_rows_detected": int(plan.get("existing_rows_detected", 0) or 0),
        "planned_value_ranges": planned_value_ranges,
        "planned_append_ranges": planned_insert_ranges,
        "planned_update_ranges": planned_update_ranges,
        "planned_structural_operations": planned_structural_operations,
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
        "writer_plan_artifact": str(writer_plan_path),
        "writer_plan_md_artifact": str(writer_plan_md_path),
        "update_policy": "update_if_new_facts_present",
        "preflight": preflight,
    }

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

    for item in rows_to_update:
        row_number = int(item.get("row_number", 0) or 0)
        row_payload = item.get("row_payload", {}) if isinstance(item.get("row_payload"), dict) else {}
        if row_number <= 0:
            continue
        for field, idx in mapped_indexes.items():
            if field not in row_payload:
                continue
            col = col_letter(int(idx) + 1)
            write_data.append(
                {
                    "range": f"'{target_sheet_name}'!{col}{row_number}:{col}{row_number}",
                    "values": [["" if row_payload.get(field) is None else str(row_payload.get(field))]],
                }
            )

    for group in group_contiguous_row_items(rows_to_insert):
        start_row = int(group[0].get("row_number", 0) or 0)
        end_row = int(group[-1].get("row_number", 0) or 0)
        values_matrix = []
        for item in group:
            values_matrix.append([str(x or "") for x in (item.get("values", []) if isinstance(item.get("values"), list) else [])])
        write_data.append(
            {
                "range": f"'{target_sheet_name}'!{DEFAULT_START_COL}{start_row}:{row_end_col}{end_row}",
                "values": values_matrix,
            }
        )
    if write_data:
        client.batch_update_values(spreadsheet_id, write_data)

    all_rows = [int(item.get("row_number", 0) or 0) for item in rows_to_insert + rows_to_update if int(item.get("row_number", 0) or 0) > 0]
    status["rows_written"] = len(rows_to_insert) + len(rows_to_update)
    status["rows_inserted"] = len(rows_to_insert)
    status["rows_updated"] = len(rows_to_update)
    status["write_start_row"] = min(all_rows) if all_rows else 0
    status["write_end_row"] = max(all_rows) if all_rows else 0
    if all_rows:
        status["final_written_range"] = (
            f"{target_sheet_name}!{DEFAULT_START_COL}{status['write_start_row']}:{col_letter(len(headers))}{status['write_end_row']}"
        )
    return status


def execute_week_summary_write(
    *,
    cfg: Any,
    run_dir: Path,
    target_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    allow_partial_write: bool,
    quarantine_unrepaired: bool,
    logger: Any,
) -> dict[str, Any]:
    return write_week_summary_rows(
        cfg=cfg,
        run_dir=run_dir,
        target_sheet_name=target_sheet_name,
        dry_run=dry_run,
        strict_preflight=strict_preflight,
        allow_partial_write=allow_partial_write,
        quarantine_unrepaired=quarantine_unrepaired,
        logger=logger,
    )
