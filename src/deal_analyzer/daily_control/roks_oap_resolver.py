from __future__ import annotations

from datetime import date, timedelta
from typing import Any


RUSSIAN_MONTH_TOKENS: dict[int, tuple[str, ...]] = {
    1: ("январ",),
    2: ("феврал",),
    3: ("март",),
    4: ("апрел",),
    5: ("май",),
    6: ("июн",),
    7: ("июл",),
    8: ("август",),
    9: ("сентябр",),
    10: ("октябр",),
    11: ("ноябр",),
    12: ("декабр",),
}


def _norm_text(value: Any) -> str:
    import re

    return " ".join(
        re.sub(r"[^0-9a-zа-яё/ ]+", " ", str(value or "").lower().replace("ё", "е")).split()
    )


def _pick_month_sheet(
    *,
    normalized: list[str],
    original: list[str],
    year: int,
    month: int,
) -> str:
    tokens = RUSSIAN_MONTH_TOKENS.get(month, ())
    for idx, norm in enumerate(normalized):
        if "рокс" not in norm or "оап" not in norm:
            continue
        if str(year) not in norm:
            continue
        if tokens and any(token in norm for token in tokens):
            return original[idx]
    return ""


def resolve_oap_month_sheets(*, sheet_titles: list[str], period_end: date) -> dict[str, Any]:
    titles = [str(x or "").strip() for x in sheet_titles if str(x or "").strip()]
    normalized = [_norm_text(x) for x in titles]
    previous_month_date = period_end.replace(day=1) - timedelta(days=1)

    selected_current = _pick_month_sheet(
        normalized=normalized,
        original=titles,
        year=period_end.year,
        month=period_end.month,
    )
    selected_previous = _pick_month_sheet(
        normalized=normalized,
        original=titles,
        year=previous_month_date.year,
        month=previous_month_date.month,
    )

    warnings: list[str] = []
    if not selected_current:
        warnings.append(f"missing_current_sheet_for_{period_end.year}_{period_end.month:02d}")
    if not selected_previous:
        warnings.append(f"missing_previous_sheet_for_{previous_month_date.year}_{previous_month_date.month:02d}")

    status = "sheets_found" if selected_current and selected_previous else "sheets_not_found"
    return {
        "status": status,
        "selected_current_month_sheet": selected_current,
        "selected_previous_month_sheet": selected_previous,
        "selected": [x for x in (selected_current, selected_previous) if x],
        "candidates": titles,
        "warnings": warnings,
        "target_current": {"year": period_end.year, "month": period_end.month},
        "target_previous": {"year": previous_month_date.year, "month": previous_month_date.month},
    }
