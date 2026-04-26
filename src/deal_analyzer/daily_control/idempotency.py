from __future__ import annotations

from typing import Any

from .models import IdempotencyKey


def build_idempotency_key(row: dict[str, Any]) -> str:
    key = IdempotencyKey(
        period_start=str(row.get("period_start") or ""),
        period_end=str(row.get("period_end") or ""),
        control_day_date=str(row.get("control_day_date") or ""),
        manager_name=str(row.get("manager_name") or ""),
        source_deals_count=int(row.get("deals_count") or 0),
        source_calls_count=int(row.get("calls_count") or 0),
    )
    return key.as_string()


def has_conflicting_counts(old_counts: tuple[int, int, int], new_counts: tuple[int, int, int]) -> bool:
    return tuple(int(x or 0) for x in old_counts) != tuple(int(x or 0) for x in new_counts)
