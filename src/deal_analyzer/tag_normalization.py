from __future__ import annotations

from typing import Any

# Controlled deterministic mapping table:
# key = normalized lowercase raw tag value (whitespace-collapsed),
# value = desired canonical form.
TAG_NORMALIZATION_MAP: dict[str, str] = {
    "istock link": "istock.link",
    "istock.link": "istock.link",
    "исток линк": "istock.link",
    "линк": "линк",
    "link": "линк",
    "инфо": "инфо",
    "info": "инфо",
    "машэкспо": "машэкспо",
    "tilda": "tilda",
}


def normalize_tag_key(value: Any) -> str:
    return " ".join(str(value or "").split()).strip().lower()


def sanitize_raw_tag(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def normalize_tag_value(value: Any) -> tuple[str, str, bool]:
    raw_tag = sanitize_raw_tag(value)
    if not raw_tag:
        return "", "", False
    key = normalize_tag_key(raw_tag)
    if key in TAG_NORMALIZATION_MAP:
        return raw_tag, TAG_NORMALIZATION_MAP[key], True
    return raw_tag, raw_tag, False
