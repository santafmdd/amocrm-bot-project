from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from ..daily_control.source_reader import parse_date


def parse_iso_date(value: Any) -> date | None:
    parsed = parse_date(str(value or ""))
    if not parsed:
        return None
    try:
        return date.fromisoformat(parsed)
    except ValueError:
        return None


def week_bounds_monday_sunday(value: Any) -> tuple[str, str]:
    day = parse_iso_date(value)
    if day is None:
        return "", ""
    week_start = day - timedelta(days=day.weekday())
    week_end = week_start + timedelta(days=6)
    return week_start.isoformat(), week_end.isoformat()


def previous_month(year: int, month: int) -> tuple[int, int]:
    if month <= 1:
        return year - 1, 12
    return year, month - 1


def month_end_date(year: int, month: int) -> date:
    if month == 12:
        return date(year + 1, 1, 1) - timedelta(days=1)
    return date(year, month + 1, 1) - timedelta(days=1)


def week_month_majority(week_start: Any, week_end: Any) -> tuple[int, int] | None:
    start = parse_iso_date(week_start)
    end = parse_iso_date(week_end)
    if start is None or end is None:
        return None
    if end < start:
        start, end = end, start
    month_days: dict[tuple[int, int], int] = {}
    cursor = start
    while cursor <= end:
        key = (cursor.year, cursor.month)
        month_days[key] = int(month_days.get(key, 0) or 0) + 1
        cursor += timedelta(days=1)
    if not month_days:
        return None
    ordered = sorted(month_days.items(), key=lambda item: (-item[1], item[0][0], item[0][1]))
    return ordered[0][0]
