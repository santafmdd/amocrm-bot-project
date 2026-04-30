from __future__ import annotations

import re
from typing import Any

from ..daily_control.source_reader import clean_text


URL_RE = re.compile(r"^https?://", re.IGNORECASE)
SMART_QUOTES_TRANSLATION = str.maketrans(
    {
        "«": '"',
        "»": '"',
        "“": '"',
        "”": '"',
        "‘": '"',
        "’": '"',
    }
)


def is_valid_url_or_empty(value: Any) -> bool:
    text = clean_text(value)
    if not text:
        return True
    return bool(URL_RE.search(text))


def has_markdown_fence(value: Any) -> bool:
    return "```" in str(value or "")


def contains_cjk(value: Any) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(value or "")))


def normalize_typographic_quotes(value: Any) -> str:
    return str(value or "").translate(SMART_QUOTES_TRANSLATION)


def normalize_row_quotes(rows: list[dict[str, Any]], *, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        for field in fields:
            if field in updated and updated.get(field) is not None:
                updated[field] = normalize_typographic_quotes(updated.get(field))
        out.append(updated)
    return out
