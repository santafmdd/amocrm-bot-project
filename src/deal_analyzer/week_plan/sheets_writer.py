from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..config import DealAnalyzerConfig
from ..daily_control.source_reader import clean_text, detect_header_row, map_headers, parse_date
from .idempotency import build_base_key, build_exact_key, is_final_status
from .source_reader import WEEK_PLAN_SOURCE_ALIASES, WEEK_PLAN_TARGET_ALIASES, resolve_spreadsheet_id
from .validation import evaluate_writer_preflight
from .writer_plan import build_writer_plan_payload


DEFAULT_START_COL = "A"
DEFAULT_END_COL = "AZ"

BASE_FIELDS: tuple[str, ...] = ("plan_week_start", "plan_week_end", "plan_date", "recipient", "activity_type")
KEY_OCCUPANCY_FIELDS: tuple[str, ...] = (*BASE_FIELDS, "status")
USER_FACING_TEXT_FIELDS: tuple[str, ...] = (
    "what_i_do",
    "task_to_assign",
    "what_to_check",
    "daily_meeting_thesis",
    "expected_quantity_effect",
    "expected_quality_effect",
)
SMART_QUOTES_TRANSLATION = str.maketrans(
    {
        "«": '"',
        "»": '"',
        "“": '"',
        "”": '"',
        "‘": '"',
        "’": '"',
    }
)


def _mapped_columns_debug(headers: list[str], mapped: dict[str, int]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for field, idx in mapped.items():
        header = headers[idx] if 0 <= int(idx) < len(headers) else ""
        out[field] = {"index": int(idx), "header": clean_text(header)}
    return out


def _resolve_identity_mappings(
    *,
    headers: list[str],
    mapped: dict[str, int],
) -> tuple[dict[str, int], dict[str, dict[str, Any]], list[str]]:
    resolved = dict(mapped)
    inferred_columns: dict[str, dict[str, Any]] = {}
    missing_identity = [field for field in BASE_FIELDS if field not in resolved]

    # Safety fallback for "План недели": if A1 header is empty but layout is canonical,
    # treat column A as "План недели с" to avoid false missing_identity_columns blocks.
    if "plan_week_start" in missing_identity:
        col_a_header = clean_text(headers[0]) if headers else ""
        col_b_field = "plan_week_end" in resolved and int(resolved.get("plan_week_end", -1)) == 1
        col_a_free = 0 not in {int(idx) for idx in resolved.values()}
        if headers and (not col_a_header) and col_b_field and col_a_free:
            resolved["plan_week_start"] = 0
            inferred_columns["plan_week_start"] = {
                "index": 0,
                "header": "",
                "reason": "blank_header_column_a_inferred_from_canonical_week_plan_layout",
            }
            missing_identity = [field for field in BASE_FIELDS if field not in resolved]

    return resolved, inferred_columns, missing_identity


@dataclass(frozen=True)
class ExistingPlanRow:
    row_number: int
    base_key: str
    exact_key: str
    status: str
    sort_key: tuple[str, str, str, str]
    values: list[str]


def _col_letter(index: int) -> str:
    out = ""
    value = max(1, int(index))
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
    return out


def _sort_key_from_values(values: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        parse_date(str(values.get("plan_week_start", ""))),
        parse_date(str(values.get("plan_date", ""))),
        clean_text(values.get("recipient", "")).lower(),
        clean_text(values.get("activity_type", "")).lower(),
    )


def _normalize_quotes(text: str) -> str:
    return str(text or "").translate(SMART_QUOTES_TRANSLATION)


def _normalize_row_typography(row: dict[str, Any]) -> dict[str, Any]:
    updated = dict(row)
    for field in USER_FACING_TEXT_FIELDS:
        if field in updated and updated.get(field) is not None:
            updated[field] = _normalize_quotes(str(updated.get(field)))
    return updated


def _extract_condition_values(
    *,
    condition: dict[str, Any],
    client: GoogleSheetsApiClient,
    spreadsheet_id: str,
) -> tuple[list[str], str, list[str]]:
    warnings: list[str] = []
    if not isinstance(condition, dict):
        return [], "", warnings
    condition_type = str(condition.get("type") or "").strip()
    values_raw = condition.get("values", []) if isinstance(condition.get("values"), list) else []
    if condition_type == "ONE_OF_LIST":
        values: list[str] = []
        seen: set[str] = set()
        for item in values_raw:
            if not isinstance(item, dict):
                continue
            value = clean_text(item.get("userEnteredValue", ""))
            if value and value not in seen:
                seen.add(value)
                values.append(value)
        return values, "one_of_list", warnings
    if condition_type == "ONE_OF_RANGE":
        out: list[str] = []
        seen: set[str] = set()
        for item in values_raw:
            if not isinstance(item, dict):
                continue
            ref = str(item.get("userEnteredValue", "") or "").strip()
            if not ref:
                continue
            if ref.startswith("="):
                ref = ref[1:].strip()
            try:
                matrix = client.get_values(spreadsheet_id, ref)
            except Exception as exc:
                warnings.append(f"dropdown_range_read_failed:{ref}:{exc}")
                continue
            for row in matrix:
                if not isinstance(row, list):
                    continue
                for cell in row:
                    value = clean_text(cell)
                    if value and value not in seen:
                        seen.add(value)
                        out.append(value)
        return out, "one_of_range", warnings
    return [], condition_type.lower(), warnings


def _discover_target_dropdown_rules(
    *,
    client: GoogleSheetsApiClient,
    spreadsheet_id: str,
    target_sheet_name: str,
    header_row_number: int,
    target_headers: list[str],
) -> dict[str, Any]:
    data_start_row = int(header_row_number or 1) + 1
    data_end_row = data_start_row + 40
    range_a1 = f"'{target_sheet_name}'!A{data_start_row}:{DEFAULT_END_COL}{data_end_row}"
    warnings: list[str] = []
    by_index: dict[int, dict[str, Any]] = {}
    try:
        service = client.build_service()
        payload = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                ranges=[range_a1],
                includeGridData=True,
                fields="sheets(data(startColumn,rowData(values(dataValidation))))",
            )
            .execute()
        )
    except Exception as exc:
        return {"by_index": {}, "warnings": [f"dropdown_discovery_failed:{exc}"]}

    sheets = payload.get("sheets", []) if isinstance(payload, dict) else []
    for sheet in sheets:
        if not isinstance(sheet, dict):
            continue
        for data in sheet.get("data", []) if isinstance(sheet.get("data"), list) else []:
            if not isinstance(data, dict):
                continue
            start_col = int(data.get("startColumn", 0) or 0)
            for row_data in data.get("rowData", []) if isinstance(data.get("rowData"), list) else []:
                if not isinstance(row_data, dict):
                    continue
                values = row_data.get("values", []) if isinstance(row_data.get("values"), list) else []
                for offset, cell in enumerate(values):
                    if not isinstance(cell, dict):
                        continue
                    dv = cell.get("dataValidation", {})
                    if not isinstance(dv, dict):
                        continue
                    condition = dv.get("condition", {}) if isinstance(dv.get("condition"), dict) else {}
                    allowed, source, dv_warnings = _extract_condition_values(
                        condition=condition,
                        client=client,
                        spreadsheet_id=spreadsheet_id,
                    )
                    warnings.extend(dv_warnings)
                    col_index = start_col + int(offset)
                    current = by_index.get(col_index)
                    if current is None:
                        by_index[col_index] = {
                            "header": target_headers[col_index] if col_index < len(target_headers) else "",
                            "strict": bool(dv.get("strict", False)),
                            "condition_type": source,
                            "allowed_values": list(allowed),
                        }
                        continue
                    merged = list(current.get("allowed_values", []))
                    seen = {clean_text(item).lower() for item in merged}
                    for item in allowed:
                        norm = clean_text(item).lower()
                        if norm and norm not in seen:
                            merged.append(item)
                            seen.add(norm)
                    current["allowed_values"] = merged
                    current["strict"] = bool(current.get("strict", False) or bool(dv.get("strict", False)))
                    if not current.get("condition_type"):
                        current["condition_type"] = source

    for col_index, item in by_index.items():
        if item.get("condition_type") in {"one_of_list", "one_of_range"} and not item.get("allowed_values"):
            warnings.append(f"dropdown_allowed_values_unavailable:col_{col_index + 1}")

    return {"by_index": by_index, "warnings": warnings}


def _match_dropdown_value_with_reason(*, field: str, raw_value: str, allowed_values: list[str]) -> tuple[str, str]:
    raw = clean_text(_normalize_quotes(raw_value))
    if not raw:
        return "", "empty"
    for item in allowed_values:
        if raw == clean_text(item):
            return clean_text(item), "exact"
    raw_norm = raw.lower()
    by_norm = {clean_text(item).lower(): clean_text(item) for item in allowed_values if clean_text(item)}
    if raw_norm in by_norm:
        return by_norm[raw_norm], "casefold"

    if field == "priority":
        aliases = {
            "high": ["high", "высокий"],
            "medium": ["medium", "средний"],
            "low": ["low", "низкий"],
        }
        for canonical, probes in aliases.items():
            if raw_norm in probes:
                for probe in probes:
                    if probe in by_norm:
                        return by_norm[probe], f"priority_alias:{canonical}"
                if canonical in by_norm:
                    return by_norm[canonical], f"priority_alias:{canonical}"
    if field == "status":
        aliases = {
            "запланировано": ["запланировано", "planned", "plan"],
            "в работе": ["в работе", "in progress", "in_progress"],
            "выполнено": ["выполнено", "done", "completed", "complete"],
        }
        for canonical, probes in aliases.items():
            if raw_norm in probes:
                for probe in probes:
                    if probe in by_norm:
                        return by_norm[probe], f"status_alias:{canonical}"
                if canonical in by_norm:
                    return by_norm[canonical], f"status_alias:{canonical}"
    if field == "activity_type":
        aliases = {
            "обучение": ["обучение", "личный разбор", "разбор", "коучинг"],
            "контроль": ["контроль", "задача", "проверка"],
            "операционная": ["операционная", "дейлик", "daily", "оперативка", "отдел"],
            "развитие": ["развитие"],
            "стратегическая": ["стратегическая", "стратегия", "strategy"],
        }
        matched_canonical = ""
        for canonical, probes in aliases.items():
            if raw_norm in probes:
                matched_canonical = canonical
                break
        if matched_canonical:
            preferred = aliases.get(matched_canonical, [])
            for probe in preferred:
                if probe in by_norm:
                    return by_norm[probe], f"activity_alias:{matched_canonical}"
            # Soft fallback across close categories when sheet uses a reduced enum.
            proximity = {
                "обучение": ("развитие", "операционная", "контроль"),
                "контроль": ("операционная", "обучение", "развитие"),
                "операционная": ("контроль", "обучение", "развитие"),
                "развитие": ("обучение", "операционная", "контроль"),
                "стратегическая": ("развитие", "операционная", "контроль"),
            }
            for candidate in proximity.get(matched_canonical, ()):
                probes = aliases.get(candidate, [])
                for probe in probes:
                    if probe in by_norm:
                        return by_norm[probe], f"activity_proximity:{matched_canonical}->{candidate}"
    return "", "no_match"


def _match_dropdown_value(*, field: str, raw_value: str, allowed_values: list[str]) -> str:
    mapped, _ = _match_dropdown_value_with_reason(field=field, raw_value=raw_value, allowed_values=allowed_values)
    return mapped


def _apply_dropdown_mapping_to_rows(
    *,
    rows: list[dict[str, Any]],
    mapped_indexes: dict[str, int],
    dropdown_rules_by_index: dict[int, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    mapped_count = 0
    unmapped_count = 0
    unmapped_examples: list[dict[str, Any]] = []
    activity_type_normalization_rows: list[dict[str, Any]] = []

    field_rules: dict[str, dict[str, Any]] = {}
    for field, idx in mapped_indexes.items():
        if idx in dropdown_rules_by_index:
            field_rules[field] = dropdown_rules_by_index[idx]

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        blocked = False
        for field, rule in field_rules.items():
            if field not in updated:
                continue
            raw_value = clean_text(updated.get(field, ""))
            if not raw_value:
                continue
            allowed_values = [clean_text(item) for item in rule.get("allowed_values", []) if clean_text(item)]
            if not allowed_values:
                continue
            mapped, normalization_reason = _match_dropdown_value_with_reason(
                field=field,
                raw_value=raw_value,
                allowed_values=allowed_values,
            )
            if field == "activity_type":
                activity_type_normalization_rows.append(
                    {
                        "row_index": row_index,
                        "recipient": str(row.get("recipient") or ""),
                        "plan_date": str(row.get("plan_date") or ""),
                        "original_activity_type": raw_value,
                        "normalized_activity_type": mapped or "",
                        "allowed_values": allowed_values[:20],
                        "normalization_reason": normalization_reason,
                        "valid_after_normalization": bool(mapped),
                    }
                )
            if mapped:
                mapped_count += 1
                updated[field] = mapped
                continue
            unmapped_count += 1
            blocked = True
            example = {
                "row_index": row_index,
                "field": field,
                "raw_value": raw_value,
                "allowed_values": allowed_values[:20],
                "reason": "dropdown_value_not_allowed",
            }
            if len(unmapped_examples) < 20:
                unmapped_examples.append(example)
            quarantined.append(
                {
                    "row_index": row_index,
                    "recipient": str(row.get("recipient") or ""),
                    "plan_date": str(row.get("plan_date") or ""),
                    "reason": "dropdown_value_not_allowed",
                    "dropdown_error": example,
                    "row": row,
                }
            )
            break
        if not blocked:
            out_rows.append(updated)

    return (
        out_rows,
        quarantined,
        {
            "dropdown_mapped_count": mapped_count,
            "dropdown_unmapped_count": unmapped_count,
            "dropdown_unmapped_examples": unmapped_examples,
            "dropdown_rules_fields_count": len(field_rules),
            "activity_type_normalization_rows": activity_type_normalization_rows,
        },
    )


def _values_by_field(row: list[str], mapped: dict[str, int]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field, idx in mapped.items():
        out[field] = row[idx] if idx < len(row) else ""
    return out


def _row_is_key_occupied(row: list[str], key_indexes: list[int]) -> bool:
    for idx in key_indexes:
        if idx < len(row) and clean_text(row[idx]):
            return True
    return False


def _project_row_to_headers(row: dict[str, Any], headers: list[str], mapped: dict[str, int]) -> list[str]:
    values = ["" for _ in headers]
    for field, idx in mapped.items():
        if idx >= len(values):
            continue
        values[idx] = "" if row.get(field) is None else str(row.get(field))
    return values


def _group_contiguous_row_items(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda x: int(x.get("row_number", 0) or 0))
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    prev = 0
    for item in sorted_rows:
        row_no = int(item.get("row_number", 0) or 0)
        if row_no <= 0:
            continue
        if not current:
            current = [item]
            prev = row_no
            continue
        if row_no == prev + 1:
            current.append(item)
        else:
            groups.append(current)
            current = [item]
        prev = row_no
    if current:
        groups.append(current)
    return groups


def _compute_manager_week_coverage(
    *,
    rows: list[dict[str, Any]],
    managers_in_scope: list[str],
    expected_workdays: list[str],
) -> dict[str, Any]:
    normalized_expected = sorted({str(item or "").strip() for item in expected_workdays if str(item or "").strip()})
    rows_by_manager: dict[str, list[str]] = {}
    rows_count_by_manager: dict[str, int] = {}
    missing_dates_by_manager: dict[str, list[str]] = {}

    for manager in managers_in_scope:
        manager_name = clean_text(manager)
        if not manager_name:
            continue
        dates = sorted(
            {
                str(item.get("plan_date") or "").strip()
                for item in rows
                if isinstance(item, dict) and clean_text(item.get("recipient") or "") == manager_name
            }
        )
        dates = [item for item in dates if item]
        rows_by_manager[manager_name] = dates
        rows_count_by_manager[manager_name] = len(dates)
        missing = [item for item in normalized_expected if item not in dates]
        if missing:
            missing_dates_by_manager[manager_name] = missing

    return {
        "managers_in_planning_scope": sorted(rows_by_manager.keys()),
        "expected_workdays": normalized_expected,
        "rows_by_manager": rows_by_manager,
        "rows_count_by_manager": rows_count_by_manager,
        "missing_dates_by_manager": missing_dates_by_manager,
        "coverage_complete": not bool(missing_dates_by_manager),
    }


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
        f"write_allowed: {plan.get('write_allowed', False)}",
        f"block_reason: {plan.get('block_reason', '')}",
    ]
    planned_ranges = plan.get("planned_value_ranges", []) if isinstance(plan.get("planned_value_ranges"), list) else []
    if planned_ranges:
        lines.append("")
        lines.append("planned_value_ranges:")
        for rng in planned_ranges[:100]:
            lines.append(f"- {rng}")
    structural = plan.get("planned_structural_operations", []) if isinstance(plan.get("planned_structural_operations"), list) else []
    if structural:
        lines.append("")
        lines.append("planned_structural_operations:")
        for op in structural[:100]:
            lines.append(f"- {op}")
    return lines


def discover_week_plan_sheet(
    *,
    cfg: DealAnalyzerConfig,
    workbook_name: str,
    source_sheet_name: str,
    target_sheet_name: str,
    logger: Any,
) -> dict[str, Any]:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = resolve_spreadsheet_id(cfg)

    sheets = client.list_sheets(spreadsheet_id)
    titles = [str(item.get("title") or "").strip() for item in sheets if str(item.get("title") or "").strip()]

    source_resolved = client.resolve_sheet(spreadsheet_id, source_sheet_name)
    target_resolved = client.resolve_sheet(spreadsheet_id, target_sheet_name)

    source_matrix = client.get_values(spreadsheet_id, f"'{source_resolved['title']}'!A1:{DEFAULT_END_COL}40")
    target_matrix = client.get_values(spreadsheet_id, f"'{target_resolved['title']}'!A1:{DEFAULT_END_COL}40")
    source_header_row = detect_header_row(source_matrix, start_row=1, min_nonempty=5)
    target_header_row = detect_header_row(target_matrix, start_row=1, min_nonempty=3)

    source_headers = [clean_text(x) for x in (source_matrix[source_header_row - 1] if source_matrix else [])]
    target_headers = [clean_text(x) for x in (target_matrix[target_header_row - 1] if target_matrix else [])]

    source_mapping = map_headers(source_headers, WEEK_PLAN_SOURCE_ALIASES)
    target_mapping = map_headers(target_headers, WEEK_PLAN_TARGET_ALIASES)
    dropdown_discovery = _discover_target_dropdown_rules(
        client=client,
        spreadsheet_id=spreadsheet_id,
        target_sheet_name=target_resolved.get("title", target_sheet_name),
        header_row_number=target_header_row,
        target_headers=target_headers,
    )
    dropdown_by_field: dict[str, Any] = {}
    for field, idx in target_mapping.mapped.items():
        rule = (dropdown_discovery.get("by_index", {}) if isinstance(dropdown_discovery.get("by_index"), dict) else {}).get(idx)
        if not isinstance(rule, dict):
            continue
        dropdown_by_field[field] = {
            "header": str(rule.get("header", "")),
            "strict": bool(rule.get("strict", False)),
            "condition_type": str(rule.get("condition_type", "")),
            "allowed_values": [str(item) for item in (rule.get("allowed_values", []) if isinstance(rule.get("allowed_values"), list) else [])],
        }

    return {
        "workbook_name": workbook_name,
        "spreadsheet_id": spreadsheet_id,
        "sheet_titles": titles,
        "source_sheet": {
            "title": source_resolved.get("title", source_sheet_name),
            "sheet_id": source_resolved.get("sheetId"),
            "header_row_number": source_header_row,
        },
        "target_sheet": {
            "title": target_resolved.get("title", target_sheet_name),
            "sheet_id": target_resolved.get("sheetId"),
            "header_row_number": target_header_row,
        },
        "source_headers": source_headers,
        "target_headers": target_headers,
        "source_mapped_columns": {
            field: source_headers[idx]
            for field, idx in source_mapping.mapped.items()
            if idx < len(source_headers)
        },
        "target_mapped_columns": {
            field: target_headers[idx]
            for field, idx in target_mapping.mapped.items()
            if idx < len(target_headers)
        },
        "source_unmapped_columns": source_mapping.unmapped_columns,
        "target_unmapped_columns": target_mapping.unmapped_columns,
        "target_dropdown_values": dropdown_by_field,
        "target_dropdown_warnings": dropdown_discovery.get("warnings", []) if isinstance(dropdown_discovery.get("warnings"), list) else [],
        "source_preview": [
            {"row_number": source_header_row + i + 1, "values": row}
            for i, row in enumerate(source_matrix[source_header_row : source_header_row + 20])
            if any(clean_text(cell) for cell in row)
        ],
        "target_preview": [
            {"row_number": target_header_row + i + 1, "values": row}
            for i, row in enumerate(target_matrix[target_header_row : target_header_row + 20])
            if any(clean_text(cell) for cell in row)
        ],
    }


def build_discovery_markdown(discovery: dict[str, Any]) -> list[str]:
    lines = [
        f"workbook: {discovery.get('workbook_name', '')}",
        f"spreadsheet_id: {discovery.get('spreadsheet_id', '')}",
        f"source_sheet: {((discovery.get('source_sheet') or {}) if isinstance(discovery.get('source_sheet'), dict) else {}).get('title', '')}",
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
    dropdown_values = discovery.get("target_dropdown_values", {}) if isinstance(discovery.get("target_dropdown_values"), dict) else {}
    if dropdown_values:
        lines.append("")
        lines.append("target dropdown values:")
        for field, spec in dropdown_values.items():
            if not isinstance(spec, dict):
                continue
            allowed = spec.get("allowed_values", []) if isinstance(spec.get("allowed_values"), list) else []
            lines.append(f"- {field}: {', '.join([str(x) for x in allowed[:12]])}")
    dropdown_warnings = discovery.get("target_dropdown_warnings", []) if isinstance(discovery.get("target_dropdown_warnings"), list) else []
    if dropdown_warnings:
        lines.append("")
        lines.append("target dropdown warnings:")
        for item in dropdown_warnings[:40]:
            lines.append(f"- {item}")
    return lines


def plan_week_plan_write(
    *,
    payload_rows: list[dict[str, Any]],
    headers: list[str],
    existing_rows: list[list[str]],
    data_start_row: int,
) -> dict[str, Any]:
    mapped_raw = map_headers(headers, WEEK_PLAN_TARGET_ALIASES).mapped
    mapped, inferred_columns, missing_identity = _resolve_identity_mappings(headers=headers, mapped=mapped_raw)
    mapped_columns_debug = _mapped_columns_debug(headers, mapped)
    if missing_identity:
        return {
            "ok": False,
            "error": "missing_identity_columns",
            "missing_identity_columns": missing_identity,
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
            "mapped_columns": mapped_columns_debug,
            "actual_headers": [clean_text(item) for item in headers],
            "inferred_columns": inferred_columns,
        }

    key_indexes = [mapped[field] for field in KEY_OCCUPANCY_FIELDS if field in mapped]
    if not key_indexes:
        key_indexes = [mapped[field] for field in BASE_FIELDS if field in mapped]

    existing_items: list[ExistingPlanRow] = []
    existing_base_index: dict[str, ExistingPlanRow] = {}
    last_nonempty_row = data_start_row - 1

    for offset, row in enumerate(existing_rows):
        row_number = data_start_row + offset
        if not _row_is_key_occupied(row, key_indexes):
            continue
        last_nonempty_row = max(last_nonempty_row, row_number)
        values = _values_by_field(row, mapped)
        base_key = build_base_key(values)
        if not base_key.strip():
            continue
        status_value = clean_text(values.get("status", ""))
        existing = ExistingPlanRow(
            row_number=row_number,
            base_key=base_key,
            exact_key=build_exact_key(values),
            status=status_value,
            sort_key=_sort_key_from_values(values),
            values=[str(x or "") for x in row],
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
        base_key = build_base_key(row)
        if not base_key.strip():
            continue
        exact_key = build_exact_key(row)
        existing = existing_base_index.get(base_key)
        if existing is not None:
            if is_final_status(existing.status):
                rows_skipped_existing.append(
                    {
                        "base_key": base_key,
                        "row_number": existing.row_number,
                        "reason": "status_final_completed",
                        "status": existing.status,
                    }
                )
                continue
            if existing.exact_key == exact_key:
                rows_skipped_existing.append(
                    {
                        "base_key": base_key,
                        "row_number": existing.row_number,
                        "reason": "exact_duplicate",
                    }
                )
                continue
            rows_to_update.append(
                {
                    "base_key": base_key,
                    "identity_key": exact_key,
                    "row_number": existing.row_number,
                    "values": _project_row_to_headers(row, headers, mapped),
                    "row_payload": row,
                }
            )
            continue

        new_payloads.append(
            {
                "base_key": base_key,
                "identity_key": exact_key,
                "sort_key": _sort_key_from_values(row),
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
                "identity_key": payload["identity_key"],
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
        "mapped_columns": mapped_columns_debug,
        "inferred_columns": inferred_columns,
        "actual_headers": [clean_text(item) for item in headers],
    }


def write_week_plan_rows(
    *,
    cfg: DealAnalyzerConfig,
    run_dir: Path,
    target_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    allow_partial_write: bool,
    quarantine_unrepaired: bool,
    logger: Any,
) -> dict[str, Any]:
    payload_path = run_dir / "week_plan_payload.json"
    if not payload_path.exists():
        raise FileNotFoundError(f"Week plan payload not found: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    payload_rows_raw = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    payload_rows = [_normalize_row_typography(row) for row in payload_rows_raw if isinstance(row, dict)]
    manager_coverage_payload = dict(
        payload.get("manager_week_coverage", {})
        if isinstance(payload.get("manager_week_coverage"), dict)
        else {}
    )
    require_full_manager_week_coverage = bool(payload.get("require_full_manager_week_coverage", False))
    manager_week_coverage_incomplete = bool(manager_coverage_payload.get("coverage_incomplete", False))

    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = resolve_spreadsheet_id(cfg)

    probe = client.get_values(spreadsheet_id, f"'{target_sheet_name}'!A1:{DEFAULT_END_COL}30")
    header_row_number = detect_header_row(probe, start_row=1, min_nonempty=3)
    data_start_row = header_row_number + 1
    header_rows = client.get_values(spreadsheet_id, f"'{target_sheet_name}'!A{header_row_number}:{DEFAULT_END_COL}{header_row_number}")
    headers = [clean_text(x) for x in (header_rows[0] if header_rows else [])]
    if not headers:
        raise RuntimeError(f"Target sheet header row is empty: {target_sheet_name} row={header_row_number}")
    existing_rows = client.get_values(spreadsheet_id, f"'{target_sheet_name}'!A{data_start_row}:{DEFAULT_END_COL}")

    mapped_indexes = map_headers(headers, WEEK_PLAN_TARGET_ALIASES).mapped
    discovery_dropdown_by_field: dict[str, Any] = {}
    discovery_path = run_dir / "week_plan_sheet_discovery.json"
    dropdown_warnings: list[str] = []
    if discovery_path.exists():
        try:
            discovery_payload = json.loads(discovery_path.read_text(encoding="utf-8"))
            if isinstance(discovery_payload, dict):
                raw_dropdown = discovery_payload.get("target_dropdown_values", {})
                if isinstance(raw_dropdown, dict):
                    discovery_dropdown_by_field = raw_dropdown
                raw_warnings = discovery_payload.get("target_dropdown_warnings", [])
                if isinstance(raw_warnings, list):
                    dropdown_warnings.extend([str(item) for item in raw_warnings if str(item).strip()])
        except Exception:
            discovery_dropdown_by_field = {}
    dropdown_rules_by_index: dict[int, dict[str, Any]] = {}
    if discovery_dropdown_by_field:
        for field, spec in discovery_dropdown_by_field.items():
            if field not in mapped_indexes or not isinstance(spec, dict):
                continue
            idx = int(mapped_indexes[field])
            dropdown_rules_by_index[idx] = {
                "header": str(spec.get("header", "")),
                "strict": bool(spec.get("strict", False)),
                "condition_type": str(spec.get("condition_type", "")),
                "allowed_values": [str(item) for item in (spec.get("allowed_values", []) if isinstance(spec.get("allowed_values"), list) else [])],
            }
    else:
        fresh_rules = _discover_target_dropdown_rules(
            client=client,
            spreadsheet_id=spreadsheet_id,
            target_sheet_name=target_sheet_name,
            header_row_number=header_row_number,
            target_headers=headers,
        )
        dropdown_rules_by_index = fresh_rules.get("by_index", {}) if isinstance(fresh_rules.get("by_index"), dict) else {}
        if not dropdown_rules_by_index:
            dropdown_warnings.append("dropdown_allowed_values_unavailable")
        raw_warnings = fresh_rules.get("warnings", [])
        if isinstance(raw_warnings, list):
            dropdown_warnings.extend([str(item) for item in raw_warnings if str(item).strip()])

    mapped_rows, dropdown_quarantined_rows, dropdown_diag = _apply_dropdown_mapping_to_rows(
        rows=payload_rows,
        mapped_indexes=mapped_indexes,
        dropdown_rules_by_index=dropdown_rules_by_index,
    )

    plan = plan_week_plan_write(
        payload_rows=mapped_rows,
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
    planned_structural_operations = plan.get("planned_structural_operations", []) if isinstance(plan.get("planned_structural_operations"), list) else []

    preflight = evaluate_writer_preflight(
        rows=mapped_rows,
        strict_preflight=bool(strict_preflight),
        conflicts_count=len(conflicts),
        allow_partial_write=bool(allow_partial_write),
        quarantine_unrepaired=bool(quarantine_unrepaired),
    )
    preflight = dict(preflight or {})
    preflight["dropdown_mapped_count"] = int(dropdown_diag.get("dropdown_mapped_count", 0) or 0)
    preflight["dropdown_unmapped_count"] = int(dropdown_diag.get("dropdown_unmapped_count", 0) or 0)
    preflight["dropdown_unmapped_examples"] = dropdown_diag.get("dropdown_unmapped_examples", [])
    preflight["dropdown_warnings"] = dropdown_warnings
    preflight["manager_week_coverage"] = manager_coverage_payload
    preflight["require_full_manager_week_coverage"] = bool(require_full_manager_week_coverage)
    preflight["manager_week_coverage_incomplete"] = bool(manager_week_coverage_incomplete)
    if payload_rows and (not mapped_rows):
        failed_rules = preflight.get("failed_rules", []) if isinstance(preflight.get("failed_rules"), list) else []
        failed_rules.append({"rule": "no_rows_after_dropdown_mapping", "count": len(dropdown_quarantined_rows)})
        preflight["failed_rules"] = failed_rules
        preflight["passed"] = False
        preflight["block_reason"] = str(preflight.get("block_reason") or "quality_preflight_failed")
    rows_for_write = preflight.get("rows_for_write", []) if isinstance(preflight.get("rows_for_write"), list) else []
    coverage_after_preflight: dict[str, Any] = {}
    managers_in_scope_preflight = manager_coverage_payload.get("managers_in_planning_scope", [])
    expected_workdays_preflight = manager_coverage_payload.get("expected_workdays", [])
    if isinstance(managers_in_scope_preflight, list) and isinstance(expected_workdays_preflight, list):
        coverage_after_preflight = _compute_manager_week_coverage(
            rows=[item for item in rows_for_write if isinstance(item, dict)],
            managers_in_scope=[str(item) for item in managers_in_scope_preflight],
            expected_workdays=[str(item) for item in expected_workdays_preflight],
        )
        manager_week_coverage_incomplete = bool(
            require_full_manager_week_coverage
            and managers_in_scope_preflight
            and not bool(coverage_after_preflight.get("coverage_complete", False))
        )
        manager_coverage_payload["after_preflight"] = coverage_after_preflight
        manager_coverage_payload["coverage_incomplete_after_preflight"] = bool(manager_week_coverage_incomplete)
    preflight["manager_week_coverage_after_preflight"] = coverage_after_preflight
    preflight["manager_week_coverage_incomplete"] = bool(manager_week_coverage_incomplete)
    if require_full_manager_week_coverage and manager_week_coverage_incomplete:
        failed_rules = preflight.get("failed_rules", []) if isinstance(preflight.get("failed_rules"), list) else []
        failed_rules.append(
            {
                "rule": "manager_week_coverage_incomplete_after_preflight",
                "missing_dates_by_manager": coverage_after_preflight.get("missing_dates_by_manager", {}),
            }
        )
        preflight["failed_rules"] = failed_rules
        preflight["passed"] = False
        preflight["block_reason"] = "manager_week_coverage_incomplete_after_preflight"

    quality_blocked = not bool(preflight.get("passed", False))

    allowed_exact_keys = {build_exact_key(row) for row in rows_for_write if isinstance(row, dict)}

    rows_to_insert = [item for item in rows_to_insert if str(item.get("identity_key", "")) in allowed_exact_keys]
    rows_to_update = [item for item in rows_to_update if str(item.get("identity_key", "")) in allowed_exact_keys]
    if rows_to_insert:
        first_row = min(int(item.get("row_number", 0) or 0) for item in rows_to_insert if int(item.get("row_number", 0) or 0) > 0)
        if first_row > 0:
            sorted_inserts = sorted(rows_to_insert, key=lambda item: int(item.get("row_number", 0) or 0))
            for offset, item in enumerate(sorted_inserts):
                item["row_number"] = int(first_row) + int(offset)
            rows_to_insert = sorted_inserts
    quarantined_rows = preflight.get("quarantined_rows", []) if isinstance(preflight.get("quarantined_rows"), list) else []
    if dropdown_quarantined_rows:
        quarantined_rows = [*dropdown_quarantined_rows, *quarantined_rows]

    row_end_col = _col_letter(max(1, len(headers)))
    planned_insert_ranges: list[str] = []
    for group in _group_contiguous_row_items(rows_to_insert):
        if not group:
            continue
        start = int(group[0].get("row_number", 0) or 0)
        end = int(group[-1].get("row_number", 0) or 0)
        if start <= 0 or end <= 0:
            continue
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

    conflicts_path = run_dir / "week_plan_conflicts.json"
    conflicts_path.write_text(
        json.dumps({"conflicts_count": len(conflicts), "conflicts": conflicts}, ensure_ascii=False, indent=2),
        encoding="utf-8",
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
        update_policy="update_non_final_existing_row",
    )
    writer_plan["preflight"] = preflight
    writer_plan["dropdown_mapped_count"] = int(dropdown_diag.get("dropdown_mapped_count", 0) or 0)
    writer_plan["dropdown_unmapped_count"] = int(dropdown_diag.get("dropdown_unmapped_count", 0) or 0)
    writer_plan["dropdown_unmapped_examples"] = dropdown_diag.get("dropdown_unmapped_examples", [])
    writer_plan["dropdown_warnings"] = dropdown_warnings
    writer_plan["manager_week_coverage"] = manager_coverage_payload
    writer_plan["require_full_manager_week_coverage"] = bool(require_full_manager_week_coverage)
    writer_plan["manager_week_coverage_incomplete"] = bool(manager_week_coverage_incomplete)

    activity_type_allowed_values: list[str] = []
    activity_type_idx = mapped_indexes.get("activity_type")
    if isinstance(activity_type_idx, int):
        rule = dropdown_rules_by_index.get(int(activity_type_idx), {})
        if isinstance(rule, dict):
            raw_allowed = rule.get("allowed_values", [])
            if isinstance(raw_allowed, list):
                activity_type_allowed_values = [clean_text(item) for item in raw_allowed if clean_text(item)]
    activity_type_norm_rows = dropdown_diag.get("activity_type_normalization_rows", [])
    activity_type_norm_payload = {
        "allowed_values": activity_type_allowed_values,
        "rows_total": len(activity_type_norm_rows) if isinstance(activity_type_norm_rows, list) else 0,
        "rows": activity_type_norm_rows if isinstance(activity_type_norm_rows, list) else [],
    }
    activity_type_norm_path = run_dir / "week_plan_activity_type_normalization_debug.json"
    activity_type_norm_path.write_text(json.dumps(activity_type_norm_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    writer_plan["activity_type_normalization_artifact"] = str(activity_type_norm_path)
    if not bool(plan.get("ok", False)):
        writer_plan["identity_mapping_error_details"] = {
            "target_sheet": target_sheet_name,
            "header_row_number": int(header_row_number or 1),
            "missing_identity_columns": plan.get("missing_identity_columns", []),
            "mapped_columns": plan.get("mapped_columns", {}),
            "actual_headers": plan.get("actual_headers", [clean_text(item) for item in headers]),
            "inferred_columns": plan.get("inferred_columns", {}),
        }

    writer_plan_path = run_dir / "week_plan_writer_plan.json"
    writer_plan_md_path = run_dir / "week_plan_writer_plan.md"
    writer_plan_path.write_text(json.dumps(writer_plan, ensure_ascii=False, indent=2), encoding="utf-8")
    writer_plan_md_path.write_text(
        "# Week Plan Writer Plan\n\n" + "\n".join(_writer_plan_markdown(writer_plan)).strip() + "\n",
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
        "append_scan_enabled": True,
        "header_row_number": header_row_number,
        "append_scan_last_nonempty_row": int(plan.get("append_scan_last_nonempty_row", 0) or 0),
        "first_empty_row_after_existing_array": int(plan.get("first_empty_row_after_existing_array", 0) or 0),
        "append_scan_start_row_chosen": int(rows_to_insert[0].get("row_number", 0)) if rows_to_insert else 0,
        "existing_rows_detected": int(plan.get("existing_rows_detected", 0) or 0),
        "planned_ranges": planned_value_ranges,
        "planned_value_ranges": planned_value_ranges,
        "planned_append_ranges": planned_insert_ranges,
        "planned_update_ranges": planned_update_ranges,
        "planned_structural_operations": planned_structural_operations,
        "allow_partial_write": bool(allow_partial_write),
        "quarantine_unrepaired": bool(quarantine_unrepaired),
        "dropdown_mapped_count": int(dropdown_diag.get("dropdown_mapped_count", 0) or 0),
        "dropdown_unmapped_count": int(dropdown_diag.get("dropdown_unmapped_count", 0) or 0),
        "dropdown_unmapped_examples": dropdown_diag.get("dropdown_unmapped_examples", []),
        "dropdown_warnings": dropdown_warnings,
        "manager_week_coverage": manager_coverage_payload,
        "require_full_manager_week_coverage": bool(require_full_manager_week_coverage),
        "manager_week_coverage_incomplete": bool(manager_week_coverage_incomplete),
        "activity_type_normalization_artifact": str(activity_type_norm_path),
        "write_allowed": bool(write_allowed),
        "block_reason": block_reason,
        "rows_written": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "write_start_row": 0,
        "write_end_row": 0,
        "final_written_ranges": [],
        "error": "",
        "conflicts_artifact": str(conflicts_path),
        "writer_plan_artifact": str(writer_plan_path),
        "writer_plan_md_artifact": str(writer_plan_md_path),
        "update_policy": "update_non_final_existing_row",
        "preflight": preflight,
    }

    if not bool(plan.get("ok", False)):
        status["error"] = str(plan.get("error", "plan_failed"))
        status["missing_identity_columns"] = plan.get("missing_identity_columns", [])
        status["mapped_columns"] = plan.get("mapped_columns", {})
        status["actual_headers"] = plan.get("actual_headers", [clean_text(item) for item in headers])
        status["inferred_columns"] = plan.get("inferred_columns", {})
        status["target_sheet"] = target_sheet_name
        status["header_row_number"] = int(header_row_number or 1)
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
            col = _col_letter(int(idx) + 1)
            write_data.append(
                {
                    "range": f"'{target_sheet_name}'!{col}{row_number}:{col}{row_number}",
                    "values": [["" if row_payload.get(field) is None else str(row_payload.get(field))]],
                }
            )

    for group in _group_contiguous_row_items(rows_to_insert):
        if not group:
            continue
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
    start_row = min(all_rows) if all_rows else 0
    end_row = max(all_rows) if all_rows else 0
    final_ranges = []
    if start_row > 0 and end_row > 0:
        final_ranges.append(f"{target_sheet_name}!{DEFAULT_START_COL}{start_row}:{_col_letter(len(headers))}{end_row}")

    status["rows_written"] = len(rows_to_insert) + len(rows_to_update)
    status["rows_inserted"] = len(rows_to_insert)
    status["rows_updated"] = len(rows_to_update)
    status["write_start_row"] = start_row
    status["write_end_row"] = end_row
    status["final_written_ranges"] = final_ranges
    return status


def execute_week_plan_write(
    *,
    cfg: DealAnalyzerConfig,
    run_dir: Path,
    target_sheet_name: str,
    dry_run: bool,
    strict_preflight: bool,
    allow_partial_write: bool,
    quarantine_unrepaired: bool,
    logger: Any,
) -> dict[str, Any]:
    return write_week_plan_rows(
        cfg=cfg,
        run_dir=run_dir,
        target_sheet_name=target_sheet_name,
        dry_run=dry_run,
        strict_preflight=strict_preflight,
        allow_partial_write=allow_partial_write,
        quarantine_unrepaired=quarantine_unrepaired,
        logger=logger,
    )
