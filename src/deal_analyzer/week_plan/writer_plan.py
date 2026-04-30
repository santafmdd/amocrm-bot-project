from __future__ import annotations

from typing import Any


def build_writer_plan_payload(
    *,
    mode: str,
    sheet_name: str,
    spreadsheet_id: str,
    write_strategy: str,
    rows_prepared: int,
    rows_to_insert: list[dict[str, Any]],
    rows_to_update: list[dict[str, Any]],
    rows_skipped_existing: list[dict[str, Any]],
    rows_skipped_stale: list[dict[str, Any]],
    rows_quarantined: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
    structural_changes_required: bool,
    planned_value_ranges: list[str],
    planned_structural_operations: list[dict[str, Any]],
    planned_ranges: list[str],
    planned_update_ranges: list[str],
    planned_append_ranges: list[str],
    existing_rows_detected: int,
    first_empty_row_after_existing_array: int,
    protected_ranges_count: int,
    strict_preflight: bool,
    write_allowed: bool,
    block_reason: str,
    insert_operations: list[dict[str, Any]],
    update_operations: list[dict[str, Any]],
    append_operations: list[dict[str, Any]],
    update_policy: str,
) -> dict[str, Any]:
    identity_keys: list[str] = []
    for item in rows_to_insert:
        key = str(item.get("identity_key", "") or "").strip()
        if key:
            identity_keys.append(key)
    for item in rows_to_update:
        key = str(item.get("identity_key", "") or "").strip()
        if key:
            identity_keys.append(key)

    return {
        "mode": mode,
        "sheet_name": sheet_name,
        "spreadsheet_id": spreadsheet_id,
        "write_strategy": str(write_strategy or "values_only"),
        "rows_prepared": int(rows_prepared or 0),
        "rows_to_insert": len(rows_to_insert),
        "rows_to_update": len(rows_to_update),
        "rows_skipped_existing": len(rows_skipped_existing),
        "rows_skipped_stale": len(rows_skipped_stale),
        "rows_quarantined": len(rows_quarantined),
        "conflicts_count": len(conflicts),
        "structural_changes_required": bool(structural_changes_required),
        "planned_value_ranges": planned_value_ranges,
        "planned_structural_operations": planned_structural_operations,
        "planned_ranges": planned_ranges,
        "planned_update_ranges": planned_update_ranges,
        "planned_append_ranges": planned_append_ranges,
        "insert_operations": insert_operations,
        "update_operations": update_operations,
        "append_operations": append_operations,
        "idempotency_keys": identity_keys,
        "existing_rows_detected": int(existing_rows_detected or 0),
        "first_empty_row_after_existing_array": int(first_empty_row_after_existing_array or 0),
        "protected_ranges_count": int(protected_ranges_count or 0),
        "strict_preflight": bool(strict_preflight),
        "write_allowed": bool(write_allowed),
        "block_reason": str(block_reason or ""),
        "update_policy": str(update_policy or ""),
    }
