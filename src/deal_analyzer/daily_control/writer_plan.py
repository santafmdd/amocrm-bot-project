from __future__ import annotations

from typing import Any


def build_writer_plan_payload(
    *,
    mode: str,
    sheet_name: str,
    spreadsheet_id: str,
    rows_prepared: int,
    rows_to_insert: list[dict[str, Any]],
    rows_skipped_existing: list[dict[str, Any]],
    conflicts: list[dict[str, Any]],
    planned_ranges: list[str],
    existing_rows_detected: int,
    protected_ranges_count: int,
    strict_preflight: bool,
    write_allowed: bool,
    block_reason: str,
    insert_operations: list[dict[str, Any]],
    append_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "mode": mode,
        "sheet_name": sheet_name,
        "spreadsheet_id": spreadsheet_id,
        "rows_prepared": int(rows_prepared or 0),
        "rows_to_insert": len(rows_to_insert),
        "rows_skipped_existing": len(rows_skipped_existing),
        "conflicts_count": len(conflicts),
        "insert_operations": insert_operations,
        "append_operations": append_operations,
        "planned_ranges": planned_ranges,
        "idempotency_keys": [str(item.get("identity_key", "")) for item in rows_to_insert if str(item.get("identity_key", ""))],
        "existing_rows_detected": int(existing_rows_detected or 0),
        "protected_ranges_count": int(protected_ranges_count or 0),
        "strict_preflight": bool(strict_preflight),
        "write_allowed": bool(write_allowed),
        "block_reason": str(block_reason or ""),
    }

