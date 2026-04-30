from __future__ import annotations

from typing import Any

from .payload_validator import payload_has_blockers, validate_daily_payload_rows
from .text_lint import lint_daily_text_rows, lint_has_blockers


def _row_blocker_reason(row: dict[str, Any], lint: dict[str, Any], payload_validation: dict[str, Any]) -> str:
    if lint_has_blockers(lint):
        return "text_lint_blocker"
    if payload_has_blockers(payload_validation):
        return "payload_validation_blocker"
    return ""


def evaluate_writer_preflight(
    *,
    rows: list[dict[str, Any]],
    strict_preflight: bool,
    conflicts_count: int,
    duplicate_policy: str,
    allow_partial_write: bool = True,
    quarantine_unrepaired: bool = True,
) -> dict[str, Any]:
    rows_for_write: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []

    lint_before = lint_daily_text_rows(rows)

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            quarantined_rows.append(
                {
                    "row_index": idx,
                    "reason": "row_is_not_dict",
                    "manager_name": "",
                    "control_day_date": "",
                }
            )
            continue
        row_lint = lint_daily_text_rows([row])
        row_payload_validation = validate_daily_payload_rows([row])
        reason = _row_blocker_reason(row, row_lint, row_payload_validation)
        if reason:
            if quarantine_unrepaired:
                quarantined_rows.append(
                    {
                        "row_index": idx,
                        "reason": reason,
                        "manager_name": str(row.get("manager_name") or ""),
                        "control_day_date": str(row.get("control_day_date") or ""),
                        "lint": row_lint,
                        "payload_validator": row_payload_validation,
                    }
                )
                continue
            rows_for_write.append(row)
            continue
        rows_for_write.append(row)

    lint_after = lint_daily_text_rows(rows_for_write)
    payload_validation = validate_daily_payload_rows(rows_for_write)

    failed_rules: list[dict[str, Any]] = []

    if quarantined_rows and not bool(allow_partial_write):
        failed_rules.append({"rule": "row_blockers_present", "count": len(quarantined_rows)})

    if lint_has_blockers(lint_after):
        failed_rules.append(
            {
                "rule": "text_lint_blockers_present",
                "count": int(
                    (lint_after.get("foreign_greeting_count", 0) or 0)
                    + (lint_after.get("foreign_language_count", 0) or 0)
                    + (lint_after.get("chinese_text_count", 0) or 0)
                    + (lint_after.get("markdown_fence_count", 0) or 0)
                ),
            }
        )

    if payload_has_blockers(payload_validation):
        failed_rules.append({"rule": "payload_validation_failed", "count": 1})

    if rows and not rows_for_write:
        failed_rules.append({"rule": "no_rows_after_quarantine", "count": len(quarantined_rows)})

    if bool(strict_preflight) and int(conflicts_count or 0) > 0 and str(duplicate_policy or "skip").strip().lower() != "skip":
        failed_rules.append({"rule": "conflicts_block_write", "count": int(conflicts_count or 0)})

    passed = len(failed_rules) == 0
    return {
        "passed": passed,
        "failed_rules": failed_rules,
        "block_reason": "" if passed else "quality_preflight_failed",
        "rows_for_write_count": len(rows_for_write),
        "rows_quarantined_count": len(quarantined_rows),
        "rows_for_write": rows_for_write,
        "quarantined_rows": quarantined_rows,
        "allow_partial_write": bool(allow_partial_write),
        "quarantine_unrepaired": bool(quarantine_unrepaired),
        "text_lint": lint_after,
        "text_lint_before_quarantine": lint_before,
        "payload_validator": payload_validation,
    }
