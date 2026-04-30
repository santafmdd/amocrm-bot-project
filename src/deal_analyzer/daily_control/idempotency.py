from __future__ import annotations

from typing import Any

from .models import IdempotencyKey


BASE_IDENTITY_FIELDS: tuple[str, ...] = ("week_start", "week_end", "control_day_date", "manager_name")


def build_base_key(row: dict[str, Any]) -> str:
    week_start = str(row.get("week_start") or row.get("period_start") or "").strip()
    week_end = str(row.get("week_end") or row.get("period_end") or "").strip()
    control_day = str(row.get("control_day_date") or "").strip()
    manager = str(row.get("manager_name") or "").strip()
    return "|".join((week_start, week_end, control_day, manager))


def build_idempotency_key(row: dict[str, Any]) -> str:
    key = IdempotencyKey(
        week_start=str(row.get("week_start") or row.get("period_start") or ""),
        week_end=str(row.get("week_end") or row.get("period_end") or ""),
        control_day_date=str(row.get("control_day_date") or ""),
        manager_name=str(row.get("manager_name") or ""),
        source_deals_count=int(row.get("deals_count") or 0),
        source_calls_count=int(row.get("calls_count") or 0),
    )
    return key.as_string()


def has_conflicting_counts(old_counts: tuple[int, int, int], new_counts: tuple[int, int, int]) -> bool:
    return tuple(int(x or 0) for x in old_counts) != tuple(int(x or 0) for x in new_counts)


def classify_count_relation(old_counts: tuple[int, int, int], new_counts: tuple[int, int, int]) -> str:
    old_sample, old_deals, old_calls = (int(x or 0) for x in old_counts)
    new_sample, new_deals, new_calls = (int(x or 0) for x in new_counts)

    if (new_sample, new_deals, new_calls) == (old_sample, old_deals, old_calls):
        return "same"

    bigger = new_deals >= old_deals and new_calls >= old_calls and (
        new_deals > old_deals or new_calls > old_calls or new_sample > old_sample
    )
    if bigger:
        return "bigger"

    smaller = new_deals <= old_deals and new_calls <= old_calls and (
        new_deals < old_deals or new_calls < old_calls or new_sample < old_sample
    )
    if smaller:
        return "smaller"

    return "weird"
