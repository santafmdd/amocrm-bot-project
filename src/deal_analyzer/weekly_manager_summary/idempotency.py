from __future__ import annotations

from typing import Any

from ..daily_control.source_reader import clean_text, parse_date


def build_base_key(values: dict[str, Any]) -> str:
    return "|".join(
        [
            parse_date(str(values.get("week_start", ""))),
            parse_date(str(values.get("week_end", ""))),
            clean_text(values.get("manager_name", "")),
        ]
    )


def build_exact_key(values: dict[str, Any]) -> str:
    return "|".join(
        [
            build_base_key(values),
            clean_text(values.get("deals_count", "0")),
            clean_text(values.get("calls_count", "0")),
            clean_text(values.get("source_day_count", "0")),
        ]
    )


def classify_count_relation(old_deals: int, new_deals: int) -> str:
    if new_deals > old_deals:
        return "bigger"
    if new_deals < old_deals:
        return "smaller"
    return "same"

