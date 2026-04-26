from __future__ import annotations

import re
from typing import Any


NARRATIVE_FIELDS_DAILY: tuple[str, ...] = (
    "main_pattern",
    "strong_sides",
    "growth_zones",
    "why_it_matters",
    "what_to_reinforce",
    "what_to_fix",
    "what_to_tell_employee",
    "expected_quant_impact",
    "expected_qual_impact",
)

MAX_FIELD_LENGTH = 1400


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def clean_daily_text(value: str, counts: dict[str, int]) -> str:
    before = str(value or "")
    after = _clean_text(before)
    if before != after:
        counts["normalize_whitespace"] = int(counts.get("normalize_whitespace", 0) or 0) + 1
    if len(after) > MAX_FIELD_LENGTH:
        after = after[: MAX_FIELD_LENGTH - 1].rstrip() + "…"
        counts["truncate_max_length"] = int(counts.get("truncate_max_length", 0) or 0) + 1
    return after


def clean_rows(rows: list[dict[str, Any]], *, fields: tuple[str, ...] = NARRATIVE_FIELDS_DAILY) -> tuple[list[dict[str, Any]], dict[str, int]]:
    counts: dict[str, int] = {}
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        for field in fields:
            if field in updated:
                updated[field] = clean_daily_text(str(updated.get(field, "") or ""), counts)
        out.append(updated)
    return out, counts
