from __future__ import annotations

import re
from datetime import date
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date
from .idempotency import build_exact_key


WEEK_PLAN_NARRATIVE_FIELDS: tuple[str, ...] = (
    "what_i_do",
    "task_to_assign",
    "what_to_check",
    "daily_meeting_thesis",
    "expected_quantity_effect",
    "expected_quality_effect",
)

REQUIRED_FIELDS: tuple[str, ...] = (
    "plan_week_start",
    "plan_week_end",
    "plan_date",
    "day_label",
    "recipient",
    "activity_type",
    "priority",
    "what_i_do",
    "task_to_assign",
    "what_to_check",
    "status",
)

ALLOWED_PRIORITY = {
    "high",
    "medium",
    "low",
    "высокий",
    "средний",
    "низкий",
}

ALLOWED_ACTIVITY_TYPES = {
    "дейлик",
    "личный разбор",
    "обучение",
    "контроль",
    "задача",
    "отдел",
    "стратегия",
    "операционная",
    "развитие",
    "стратегическая",
}

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
    "email",
    "mail",
    "json",
    "llm",
    "stt",
    "roks",
    "oap",
    "kpi",
    "lpr",
    "demo",
    "done",
    "tilda",
    "in",
    "out",
    "interest",
    "avg",
    "score",
    "smart",
    "consultative",
    "educational",
    "guided",
    "discovery",
    "hands",
    "client",
    "led",
    "product",
    "walkthrough",
    "problem",
    "based",
    "soft",
    "influence",
    "next",
    "step",
    "commitment",
    "demo",
    "test",
    "invoice",
    "payment",
}

URL_RE = re.compile(r"^https?://", re.IGNORECASE)
LATIN_TOKEN_RE = re.compile(r"[A-Za-z]{3,}")
CJK_RE = re.compile(r"[\u4e00-\u9fff]")
FOREIGN_GREETING_RE = re.compile(r"\b(hello|hi|greetings)\b|你好|您好", re.IGNORECASE)
MARKDOWN_RE = re.compile(r"```")
INLINE_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
LATEX_ARROW_RE = re.compile(r"\\(?:rightarrow|to)", re.IGNORECASE)
ARROW_RE = re.compile(r"(?:->|→|←|↔)")


def _contains_foreign_language(text: str) -> bool:
    scrubbed = INLINE_URL_RE.sub(" ", str(text or ""))
    scrubbed = LATEX_ARROW_RE.sub(" ", scrubbed)
    scrubbed = ARROW_RE.sub(" ", scrubbed)
    scrubbed = scrubbed.replace("$", " ")
    for token in LATIN_TOKEN_RE.findall(scrubbed):
        token_lc = token.lower()
        if token_lc in ALLOWED_LATIN_TERMS:
            continue
        # Abbreviations like LPR should not quarantine otherwise valid Russian rows.
        if token.isupper() and len(token) <= 4:
            continue
        return True
    return False


def lint_week_plan_text_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    foreign_language_count = 0
    chinese_text_count = 0
    markdown_fence_count = 0
    empty_user_fields_count = 0
    problem_examples: list[dict[str, Any]] = []

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        recipient = str(row.get("recipient", ""))
        for field in WEEK_PLAN_NARRATIVE_FIELDS:
            value = clean_text(row.get(field, ""))
            if not value:
                empty_user_fields_count += 1
                if len(problem_examples) < 20:
                    problem_examples.append(
                        {
                            "row_index": row_index,
                            "recipient": recipient,
                            "field": field,
                            "value": "",
                            "markers": ["empty_user_field"],
                        }
                    )
                continue
            markers: list[str] = []
            if FOREIGN_GREETING_RE.search(value):
                foreign_language_count += 1
                markers.append("foreign_greeting")
            elif _contains_foreign_language(value):
                foreign_language_count += 1
                markers.append("foreign_language")
            if CJK_RE.search(value):
                chinese_text_count += 1
                markers.append("chinese_text")
            if MARKDOWN_RE.search(value):
                markdown_fence_count += 1
                markers.append("markdown_fence")
            if markers and len(problem_examples) < 20:
                problem_examples.append(
                    {
                        "row_index": row_index,
                        "recipient": recipient,
                        "field": field,
                        "value": value[:280],
                        "markers": markers,
                    }
                )

    return {
        "foreign_language_count": foreign_language_count,
        "chinese_text_count": chinese_text_count,
        "markdown_fence_count": markdown_fence_count,
        "empty_user_fields_count": empty_user_fields_count,
        "checked_fields_count": len(rows) * len(WEEK_PLAN_NARRATIVE_FIELDS),
        "problem_examples": problem_examples,
    }


def lint_has_blockers(lint: dict[str, Any]) -> bool:
    return bool(
        int(lint.get("foreign_language_count", 0) or 0) > 0
        or int(lint.get("chinese_text_count", 0) or 0) > 0
        or int(lint.get("markdown_fence_count", 0) or 0) > 0
    )


def _parse_iso(value: Any) -> date | None:
    text = parse_date(str(value or ""))
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _is_valid_url(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return True
    return bool(URL_RE.search(text))


def validate_week_plan_payload_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_required_count = 0
    missing_required_examples: list[dict[str, Any]] = []
    invalid_date_count = 0
    invalid_week_range_count = 0
    invalid_priority_count = 0
    invalid_activity_type_count = 0
    invalid_link_count = 0
    duplicate_key_count = 0
    duplicate_keys: list[str] = []
    seen: set[str] = set()

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
                        "recipient": str(row.get("recipient", "")),
                        "missing": missing,
                    }
                )

        week_start = _parse_iso(row.get("plan_week_start"))
        week_end = _parse_iso(row.get("plan_week_end"))
        plan_date = _parse_iso(row.get("plan_date"))
        if week_start is None or week_end is None or plan_date is None:
            invalid_date_count += 1
        else:
            if week_end < week_start or plan_date < week_start or plan_date > week_end:
                invalid_week_range_count += 1

        priority = clean_text(row.get("priority", "")).lower()
        if priority and priority not in ALLOWED_PRIORITY:
            invalid_priority_count += 1

        activity_type = clean_text(row.get("activity_type", "")).lower()
        if activity_type and activity_type not in ALLOWED_ACTIVITY_TYPES:
            invalid_activity_type_count += 1

        if not _is_valid_url(row.get("training_link")) or not _is_valid_url(row.get("post_training_task_link")):
            invalid_link_count += 1

        key = build_exact_key(row)
        if key:
            if key in seen:
                duplicate_key_count += 1
                if len(duplicate_keys) < 20:
                    duplicate_keys.append(key)
            seen.add(key)

    return {
        "rows_total": len(rows),
        "missing_required_count": missing_required_count,
        "missing_required_examples": missing_required_examples,
        "invalid_date_count": invalid_date_count,
        "invalid_week_range_count": invalid_week_range_count,
        "invalid_priority_count": invalid_priority_count,
        "invalid_activity_type_count": invalid_activity_type_count,
        "invalid_link_count": invalid_link_count,
        "duplicate_key_count": duplicate_key_count,
        "duplicate_keys": duplicate_keys,
    }


def payload_has_blockers(payload_validation: dict[str, Any]) -> bool:
    return bool(
        int(payload_validation.get("missing_required_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_date_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_week_range_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_priority_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_activity_type_count", 0) or 0) > 0
        or int(payload_validation.get("invalid_link_count", 0) or 0) > 0
        or int(payload_validation.get("duplicate_key_count", 0) or 0) > 0
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
    rows_empty = len(rows) == 0

    lint_before = lint_week_plan_text_rows(rows)

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            quarantined_rows.append({"row_index": idx, "reason": "row_is_not_dict"})
            continue
        row_lint = lint_week_plan_text_rows([row])
        row_payload_validation = validate_week_plan_payload_rows([row])
        if lint_has_blockers(row_lint) or payload_has_blockers(row_payload_validation):
            if quarantine_unrepaired:
                quarantined_rows.append(
                    {
                        "row_index": idx,
                        "reason": "row_preflight_blocker",
                        "recipient": str(row.get("recipient", "")),
                        "plan_date": str(row.get("plan_date", "")),
                        "text_lint": row_lint,
                        "payload_validator": row_payload_validation,
                    }
                )
                continue
        rows_for_write.append(row)

    lint_after = lint_week_plan_text_rows(rows_for_write)
    payload_validation = validate_week_plan_payload_rows(rows_for_write)

    failed_rules: list[dict[str, Any]] = []
    if rows_empty:
        failed_rules.append({"rule": "rows_empty", "count": 0})
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
    block_reason = ""
    if not passed:
        block_reason = "rows_empty" if rows_empty else "quality_preflight_failed"
    return {
        "passed": passed,
        "failed_rules": failed_rules,
        "block_reason": block_reason,
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
