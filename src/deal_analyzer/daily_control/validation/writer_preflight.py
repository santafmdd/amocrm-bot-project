from __future__ import annotations

from typing import Any

from .payload_validator import payload_has_blockers, validate_daily_payload_rows
from .text_lint import lint_daily_text_rows, lint_has_blockers


def evaluate_writer_preflight(
    *,
    rows: list[dict[str, Any]],
    strict_preflight: bool,
    conflicts_count: int,
    duplicate_policy: str,
) -> dict[str, Any]:
    lint = lint_daily_text_rows(rows)
    payload_validation = validate_daily_payload_rows(rows)

    failed_rules: list[dict[str, Any]] = []
    if lint_has_blockers(lint):
        failed_rules.append(
            {
                "rule": "text_lint_blockers_present",
                "count": int(
                    (lint.get("foreign_greeting_count", 0) or 0)
                    + (lint.get("foreign_language_count", 0) or 0)
                    + (lint.get("chinese_text_count", 0) or 0)
                    + (lint.get("markdown_fence_count", 0) or 0)
                ),
            }
        )
    if payload_has_blockers(payload_validation):
        failed_rules.append({"rule": "payload_validation_failed", "count": 1})
    if bool(strict_preflight) and int(conflicts_count or 0) > 0 and str(duplicate_policy or "skip").strip().lower() != "skip":
        failed_rules.append({"rule": "conflicts_block_write", "count": int(conflicts_count or 0)})

    passed = len(failed_rules) == 0
    return {
        "passed": passed,
        "failed_rules": failed_rules,
        "block_reason": "" if passed else "quality_preflight_failed",
        "text_lint": lint,
        "payload_validator": payload_validation,
    }
