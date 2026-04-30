from __future__ import annotations

import hashlib
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date

FINAL_STATUSES: set[str] = {
    "выполнено",
    "выполнена",
    "завершено",
    "done",
    "completed",
}


def build_base_key(values: dict[str, Any]) -> str:
    return "|".join(
        [
            parse_date(str(values.get("plan_week_start", ""))),
            parse_date(str(values.get("plan_week_end", ""))),
            parse_date(str(values.get("plan_date", ""))),
            clean_text(values.get("recipient", "")),
            clean_text(values.get("activity_type", "")).lower(),
        ]
    )


def short_action_hash(values: dict[str, Any]) -> str:
    payload = "|".join(
        [
            clean_text(values.get("what_i_do", "")),
            clean_text(values.get("task_to_assign", "")),
            clean_text(values.get("what_to_check", "")),
            clean_text(values.get("daily_meeting_thesis", "")),
        ]
    )
    digest = hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()
    return digest[:12]


def build_exact_key(values: dict[str, Any]) -> str:
    return "|".join([build_base_key(values), short_action_hash(values)])


def is_final_status(value: Any) -> bool:
    probe = clean_text(value).lower()
    if not probe:
        return False
    return probe in FINAL_STATUSES
