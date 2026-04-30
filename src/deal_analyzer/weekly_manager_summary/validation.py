from __future__ import annotations

import re
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date


WEEKLY_NARRATIVE_FIELDS: tuple[str, ...] = (
    "weekly_result",
    "improved",
    "not_improved",
    "repeating_mistakes",
    "training_for_employee",
    "post_training_tasks",
    "manager_actions_next_week",
    "expected_quantity_effect",
    "expected_quality_effect",
    "manager_report_phrase",
    "employee_message",
)

OPTIONAL_NARRATIVE_FIELDS: set[str] = {
    "training_for_employee",
    "post_training_tasks",
}

REQUIRED_FIELDS: tuple[str, ...] = (
    "week_start",
    "week_end",
    "manager_name",
    "manager_role_profile",
    "deals_count",
    "product_focus_week",
    "base_mix_week",
    "weekly_result",
    "improved",
    "not_improved",
    "repeating_mistakes",
    "manager_actions_next_week",
    "expected_quantity_effect",
    "expected_quality_effect",
    "manager_report_phrase",
    "employee_message",
    "avg_score_0_100",
)

ALLOWED_LATIN_TERMS: set[str] = {
    "link",
    "info",
    "plm",
    "crm",
    "amocrm",
    "id",
    "url",
    "http",
    "https",
    "api",
    "json",
    "llm",
    "stt",
    "rightarrow",
    "fact",
    "roks",
    "oap",
    "kpi",
}


def _contains_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def _contains_foreign_greeting(text: str) -> bool:
    return bool(re.search(r"\b(hello|hi|greetings)\b|你好|您好", text or "", re.IGNORECASE))


def _contains_markdown_fence(text: str) -> bool:
    return "```" in str(text or "")


def _foreign_tokens(text: str) -> list[str]:
    tokens = re.findall(r"[A-Za-z]{3,}", str(text or ""))
    out: list[str] = []
    for token in tokens:
        token_norm = token.lower()
        if token_norm in ALLOWED_LATIN_TERMS:
            continue
        out.append(token_norm)
    return out


def lint_weekly_text_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    foreign_language_count = 0
    chinese_text_count = 0
    markdown_fence_count = 0
    empty_user_fields_count = 0
    problem_examples: list[dict[str, Any]] = []

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        for field in WEEKLY_NARRATIVE_FIELDS:
            value = clean_text(row.get(field, ""))
            if not value:
                if field in OPTIONAL_NARRATIVE_FIELDS and str(row.get("training_source") or "") != "week_plan":
                    continue
                empty_user_fields_count += 1
                if len(problem_examples) < 20:
                    problem_examples.append(
                        {
                            "row_index": row_index,
                            "manager_name": str(row.get("manager_name", "")),
                            "field": field,
                            "value": "",
                            "markers": ["empty_user_field"],
                        }
                    )
                continue
            markers: list[str] = []
            if _contains_foreign_greeting(value):
                foreign_language_count += 1
                markers.append("foreign_greeting")
            else:
                unknown_tokens = _foreign_tokens(value)
                if unknown_tokens:
                    foreign_language_count += 1
                    markers.append("foreign_language")
            if _contains_chinese(value):
                chinese_text_count += 1
                markers.append("chinese_text")
            if _contains_markdown_fence(value):
                markdown_fence_count += 1
                markers.append("markdown_fence")
            if markers and len(problem_examples) < 20:
                problem_examples.append(
                    {
                        "row_index": row_index,
                        "manager_name": str(row.get("manager_name", "")),
                        "field": field,
                        "value": value,
                        "markers": markers,
                    }
                )
    return {
        "foreign_language_count": foreign_language_count,
        "chinese_text_count": chinese_text_count,
        "markdown_fence_count": markdown_fence_count,
        "empty_user_fields_count": empty_user_fields_count,
        "checked_fields_count": len(rows) * len(WEEKLY_NARRATIVE_FIELDS),
        "problem_examples": problem_examples,
    }


def lint_has_blockers(lint: dict[str, Any]) -> bool:
    return bool(
        int(lint.get("foreign_language_count", 0) or 0) > 0
        or int(lint.get("chinese_text_count", 0) or 0) > 0
        or int(lint.get("markdown_fence_count", 0) or 0) > 0
    )


def validate_weekly_payload_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_required_count = 0
    missing_required_examples: list[dict[str, Any]] = []
    invalid_date_count = 0
    invalid_score_count = 0
    duplicate_key_count = 0
    duplicate_keys: list[str] = []
    seen_keys: set[str] = set()

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            missing_required_count += 1
            continue
        missing = [field for field in REQUIRED_FIELDS if not clean_text(row.get(field, ""))]
        if missing:
            missing_required_count += 1
            if len(missing_required_examples) < 20:
                missing_required_examples.append(
                    {
                        "row_index": row_index,
                        "manager_name": str(row.get("manager_name", "")),
                        "missing": missing,
                    }
                )
        if not parse_date(str(row.get("week_start", ""))) or not parse_date(str(row.get("week_end", ""))):
            invalid_date_count += 1
        score_raw = clean_text(row.get("avg_score_0_100", ""))
        try:
            score = int(score_raw)
            if score < 0 or score > 100:
                invalid_score_count += 1
        except Exception:
            invalid_score_count += 1
        key = "|".join(
            [
                parse_date(str(row.get("week_start", ""))),
                parse_date(str(row.get("week_end", ""))),
                clean_text(row.get("manager_name", "")),
            ]
        )
        if key.strip():
            if key in seen_keys:
                duplicate_key_count += 1
                if len(duplicate_keys) < 20:
                    duplicate_keys.append(key)
            seen_keys.add(key)

    return {
        "rows_total": len(rows),
        "missing_required_count": missing_required_count,
        "missing_required_examples": missing_required_examples,
        "invalid_date_count": invalid_date_count,
        "invalid_score_count": invalid_score_count,
        "duplicate_key_count": duplicate_key_count,
        "duplicate_keys": duplicate_keys,
    }


def payload_has_blockers(payload_validation: dict[str, Any]) -> bool:
    return bool(
        int(payload_validation.get("missing_required_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_date_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_score_count", 0) or 0) > 0
        or int(payload_validation.get("duplicate_key_count", 0) or 0) > 0
    )


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
    lint_before = lint_weekly_text_rows(rows)

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            quarantined_rows.append({"row_index": idx, "reason": "row_is_not_dict"})
            continue
        row_lint = lint_weekly_text_rows([row])
        row_validation = validate_weekly_payload_rows([row])
        if lint_has_blockers(row_lint) or payload_has_blockers(row_validation):
            if quarantine_unrepaired:
                quarantined_rows.append(
                    {
                        "row_index": idx,
                        "reason": "row_preflight_blocker",
                        "manager_name": str(row.get("manager_name", "")),
                        "week_start": str(row.get("week_start", "")),
                        "week_end": str(row.get("week_end", "")),
                        "text_lint": row_lint,
                        "payload_validator": row_validation,
                    }
                )
                continue
        rows_for_write.append(row)

    lint_after = lint_weekly_text_rows(rows_for_write)
    payload_validation = validate_weekly_payload_rows(rows_for_write)
    failed_rules: list[dict[str, Any]] = []
    if quarantined_rows and not bool(allow_partial_write):
        failed_rules.append({"rule": "row_blockers_present", "count": len(quarantined_rows)})
    if lint_has_blockers(lint_after):
        failed_rules.append({"rule": "text_lint_blockers_present", "count": 1})
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
