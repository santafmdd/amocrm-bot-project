from __future__ import annotations

from datetime import date
from typing import Any

from ..idempotency import build_idempotency_key


REQUIRED_FIELDS: tuple[str, ...] = (
    "period_start",
    "period_end",
    "control_day_date",
    "day_label",
    "manager_name",
    "deals_count",
    "calls_count",
    "main_pattern",
    "growth_zones",
    "what_to_fix",
    "what_to_tell_employee",
    "expected_quant_impact",
    "expected_qual_impact",
    "score_0_100",
    "criticality",
)

ALLOWED_CRITICALITY = {"low", "medium", "high"}


def _parse_iso(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def validate_daily_payload_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    missing_required_count = 0
    missing_required_examples: list[dict[str, Any]] = []
    invalid_date_count = 0
    invalid_score_count = 0
    invalid_criticality_count = 0
    duplicate_key_count = 0
    key_counter: dict[str, int] = {}

    if not rows:
        return {
            "rows_total": 0,
            "missing_required_count": 1,
            "missing_required_examples": [{"row_index": -1, "missing": ["rows_empty"]}],
            "duplicate_key_count": 0,
            "duplicate_keys": [],
            "invalid_date_count": 0,
            "invalid_score_count": 0,
            "invalid_criticality_count": 0,
        }

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            missing_required_count += 1
            if len(missing_required_examples) < 10:
                missing_required_examples.append({"row_index": idx, "missing": ["row_is_not_dict"]})
            continue

        missing = [field for field in REQUIRED_FIELDS if str(row.get(field, "")).strip() == ""]
        if missing:
            missing_required_count += 1
            if len(missing_required_examples) < 10:
                missing_required_examples.append({"row_index": idx, "missing": missing})

        if _parse_iso(row.get("period_start")) is None or _parse_iso(row.get("period_end")) is None or _parse_iso(row.get("control_day_date")) is None:
            invalid_date_count += 1

        score = row.get("score_0_100")
        try:
            score_val = int(score)
        except (TypeError, ValueError):
            invalid_score_count += 1
        else:
            if score_val < 0 or score_val > 100:
                invalid_score_count += 1

        criticality = str(row.get("criticality") or "").strip().lower()
        if criticality not in ALLOWED_CRITICALITY:
            invalid_criticality_count += 1

        key = build_idempotency_key(row)
        key_counter[key] = int(key_counter.get(key, 0) or 0) + 1

    duplicate_keys = [key for key, count in key_counter.items() if count > 1 and key.strip()]
    duplicate_key_count = len(duplicate_keys)

    return {
        "rows_total": len(rows),
        "missing_required_count": missing_required_count,
        "missing_required_examples": missing_required_examples,
        "duplicate_key_count": duplicate_key_count,
        "duplicate_keys": duplicate_keys[:20],
        "invalid_date_count": invalid_date_count,
        "invalid_score_count": invalid_score_count,
        "invalid_criticality_count": invalid_criticality_count,
    }


def payload_has_blockers(result: dict[str, Any]) -> bool:
    return (
        int(result.get("missing_required_count", 0) or 0) > 0
        or int(result.get("duplicate_key_count", 0) or 0) > 0
        or int(result.get("invalid_date_count", 0) or 0) > 0
        or int(result.get("invalid_score_count", 0) or 0) > 0
        or int(result.get("invalid_criticality_count", 0) or 0) > 0
    )
