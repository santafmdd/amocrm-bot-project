"""Canonical normalization for weekly refusals statuses/reasons."""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Iterable

_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_SOFT_PUNCT = re.compile(r"[\"`]+")
_RE_TRAILING_JUNK = re.compile(r"[.,;:!?/\-\s]+$")
_RE_NON_ALNUM_SPACE = re.compile(r"[^0-9a-z\u0400-\u04ff()./+\-\s]+", flags=re.IGNORECASE)
_NEAR_WORD_SUFFIXES = (
    "\u0441\u0432\u044f",
    "\u0441\u0432\u044f\u0437",
    "\u0432\u044b\u0445\u043e\u0434\u0438\u0442",
)

_REASON_ALIAS_MAP: dict[str, str] = {
    "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f": "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c",
    "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437": "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c",
    "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442": "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c",
    "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c": "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c",
}


def normalize_basic_text(value: str) -> str:
    raw = str(value or "").strip().lower().replace("\u0451", "\u0435")
    raw = _RE_SOFT_PUNCT.sub("", raw)
    raw = _RE_NON_ALNUM_SPACE.sub(" ", raw)
    raw = _RE_MULTI_SPACE.sub(" ", raw).strip()
    raw = _RE_TRAILING_JUNK.sub("", raw).strip()
    return raw


def normalize_group_name(value: str) -> str:
    text = normalize_basic_text(value)
    if not text:
        return ""
    text = re.sub(r"\b\u043f\u0435\u0440\u0432\u0438\u0447\u043d\u044b\u0439\b", "\u043f\u0435\u0440\u0432\u044b\u0439", text)
    text = text.replace("\u043f\u0435\u0440\u0432\u044b\u0439 \u043a\u043e\u043d\u0442\u0430\u043a\u0442 \u043a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f", "\u043f\u0435\u0440\u0432\u044b\u0439 \u043a\u043e\u043d\u0442\u0430\u043a\u0442. \u043a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f")
    text = text.replace("\u0432\u044b\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u043e \u043a \u043f", "\u0432\u044b\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u043e \u043a\u043f")
    return text


def canonicalize_refusal_reason(value: str) -> str:
    reason = normalize_basic_text(value)
    if not reason:
        return ""
    alias_hit = _REASON_ALIAS_MAP.get(reason)
    if alias_hit:
        return alias_hit
    if reason.endswith(_NEAR_WORD_SUFFIXES) and "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b" in reason:
        return "\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c"
    return reason


def canonicalize_before_status(value: str) -> str:
    return canonicalize_refusal_reason(value)


def parse_group_and_reason(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    m = re.match(r"^\(([^)]+)\)\s*(.+)$", raw)
    if m:
        return normalize_group_name(m.group(1)), canonicalize_refusal_reason(m.group(2))
    if "/" in raw:
        left, right = raw.split("/", 1)
        return normalize_group_name(left), canonicalize_refusal_reason(right)
    return "", canonicalize_refusal_reason(raw)


def format_grouped_status(group: str, reason: str) -> str:
    g = normalize_group_name(group)
    r = canonicalize_refusal_reason(reason)
    if g and r:
        return f"({g}) {r}"
    return r


def format_grouped_status_display(group: str, reason: str) -> str:
    """Canonical display label for grouped refusal status used in sheet rows."""
    canonical = format_grouped_status(group, reason)
    g, r = parse_group_and_reason(canonical)
    if g and r:
        return f"({g.capitalize()}) {r.capitalize()}"
    return r.capitalize()


def canonicalize_after_status(value: str) -> str:
    group, reason = parse_group_and_reason(value)
    return format_grouped_status(group, reason)


def conservative_near_match(value: str, candidates: Iterable[str], *, min_ratio: float = 0.96) -> str | None:
    """Very conservative near-match helper; use only inside one stage-group."""
    target = normalize_basic_text(value)
    if not target:
        return None
    best: tuple[str, float] | None = None
    for item in candidates:
        probe = normalize_basic_text(item)
        if not probe or abs(len(probe) - len(target)) > 2:
            continue
        ratio = SequenceMatcher(a=target, b=probe).ratio()
        if ratio >= min_ratio and (best is None or ratio > best[1]):
            best = (item, ratio)
    return best[0] if best else None
