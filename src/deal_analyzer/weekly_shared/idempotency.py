from __future__ import annotations

import hashlib
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date


def build_week_key(*, week_start: Any, week_end: Any, manager: Any = "") -> str:
    parts = [parse_date(str(week_start or "")), parse_date(str(week_end or ""))]
    manager_value = clean_text(manager)
    if manager_value:
        parts.append(manager_value)
    return "|".join(parts)


def short_text_hash(*values: Any, size: int = 12) -> str:
    payload = "|".join(clean_text(value) for value in values if clean_text(value))
    if not payload:
        return ""
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()[: max(4, int(size or 12))]
