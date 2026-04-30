from __future__ import annotations

from typing import Any

from ..daily_control.source_reader import clean_text, parse_date
from ..weekly_shared.validation import contains_cjk, has_markdown_fence


WEEK_SUMMARY_NARRATIVE_FIELDS: tuple[str, ...] = (
    "brief_report",
    "quantity_delta",
    "quality_delta",
    "what_failed",
    "focus_next_week",
    "next_week_plan",
    "meeting_message",
    "strategic_accents",
    "risks",
    "manager_report_phrase",
)

REQUIRED_FIELDS: tuple[str, ...] = ("week_start", "week_end", *WEEK_SUMMARY_NARRATIVE_FIELDS)


def lint_week_summary_text_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    foreign_language_count = 0
    chinese_text_count = 0
    markdown_fence_count = 0
    empty_user_fields_count = 0
    problem_examples: list[dict[str, Any]] = []

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        for field in WEEK_SUMMARY_NARRATIVE_FIELDS:
            value = clean_text(row.get(field, ""))
            if not value:
                empty_user_fields_count += 1
                if len(problem_examples) < 20:
                    problem_examples.append(
                        {"row_index": row_index, "field": field, "value": "", "markers": ["empty_user_field"]}
                    )
                continue
            markers: list[str] = []
            if contains_cjk(value):
                chinese_text_count += 1
                markers.append("chinese_text")
            if has_markdown_fence(value):
                markdown_fence_count += 1
                markers.append("markdown_fence")
            if markers and len(problem_examples) < 20:
                problem_examples.append(
                    {"row_index": row_index, "field": field, "value": value[:280], "markers": markers}
                )

    return {
        "foreign_language_count": foreign_language_count,
        "chinese_text_count": chinese_text_count,
        "markdown_fence_count": markdown_fence_count,
        "empty_user_fields_count": empty_user_fields_count,
        "checked_fields_count": len(rows) * len(WEEK_SUMMARY_NARRATIVE_FIELDS),
        "problem_examples": problem_examples,
    }


def lint_has_blockers(lint: dict[str, Any]) -> bool:
    return bool(
        int(lint.get("chinese_text_count", 0) or 0) > 0
        or int(lint.get("markdown_fence_count", 0) or 0) > 0
    )


def validate_week_summary_payload_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_required_count = 0
    invalid_date_count = 0
    duplicate_key_count = 0
    duplicate_keys: list[str] = []
    seen: set[str] = set()

    for row in rows:
        if not isinstance(row, dict):
            missing_required_count += 1
            continue
        if any(not clean_text(row.get(field, "")) for field in REQUIRED_FIELDS):
            missing_required_count += 1
        week_start = parse_date(str(row.get("week_start", "")))
        week_end = parse_date(str(row.get("week_end", "")))
        if not week_start or not week_end:
            invalid_date_count += 1
        key = f"{week_start}|{week_end}"
        if key in seen:
            duplicate_key_count += 1
            if len(duplicate_keys) < 20:
                duplicate_keys.append(key)
        seen.add(key)

    return {
        "rows_total": len(rows),
        "missing_required_count": missing_required_count,
        "invalid_date_count": invalid_date_count,
        "duplicate_key_count": duplicate_key_count,
        "duplicate_keys": duplicate_keys,
    }


def payload_has_blockers(result: dict[str, Any]) -> bool:
    return bool(
        int(result.get("missing_required_count", 0) or 0) > 0
        or int(result.get("invalid_date_count", 0) or 0) > 0
        or int(result.get("duplicate_key_count", 0) or 0) > 0
    )


def evaluate_writer_preflight(
    *,
    rows: list[dict[str, Any]],
    strict_preflight: bool,
    conflicts_count: int,
    allow_partial_write: bool = True,
    quarantine_unrepaired: bool = True,
) -> dict[str, Any]:
    rows_for_write: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []
    lint_before = lint_week_summary_text_rows(rows)

    for idx, row in enumerate(rows):
        row_lint = lint_week_summary_text_rows([row])
        row_validation = validate_week_summary_payload_rows([row])
        if lint_has_blockers(row_lint) or payload_has_blockers(row_validation):
            if quarantine_unrepaired:
                quarantined_rows.append(
                    {
                        "row_index": idx,
                        "reason": "row_preflight_blocker",
                        "text_lint": row_lint,
                        "payload_validator": row_validation,
                    }
                )
                continue
        rows_for_write.append(row)

    lint_after = lint_week_summary_text_rows(rows_for_write)
    payload_validation = validate_week_summary_payload_rows(rows_for_write)
    failed_rules: list[dict[str, Any]] = []
    if quarantined_rows and not bool(allow_partial_write):
        failed_rules.append({"rule": "row_blockers_present", "count": len(quarantined_rows)})
    if lint_has_blockers(lint_after):
        failed_rules.append({"rule": "text_lint_blockers_present", "count": 1})
    if payload_has_blockers(payload_validation):
        failed_rules.append({"rule": "payload_validation_failed", "count": 1})
    if rows and not rows_for_write:
        failed_rules.append({"rule": "no_rows_after_quarantine", "count": len(quarantined_rows)})
    if bool(strict_preflight) and int(conflicts_count or 0) > 0:
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
