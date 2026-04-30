from __future__ import annotations

import re
from collections import Counter
from typing import Any

from ..daily_control.source_reader import clean_text


def parse_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    match = re.search(r"-?\d+", text)
    if not match:
        return 0
    try:
        return int(match.group(0))
    except Exception:
        return 0


def tokenize_mix_value(value: Any) -> list[tuple[str, int]]:
    text = clean_text(value)
    if not text:
        return []
    tokens = re.split(r"[;,\n\|]+", text)
    out: list[tuple[str, int]] = []
    for token in tokens:
        item = clean_text(token)
        if not item:
            continue
        match = re.match(r"^(.*?)\s+-\s+(\d+)$", item)
        if match:
            label = clean_text(match.group(1))
            count = max(1, parse_int(match.group(2)))
            out.append((label, count))
        else:
            out.append((item, 1))
    return out


def aggregate_mix(values: list[str]) -> str:
    counter: Counter[str] = Counter()
    for value in values:
        for label, count in tokenize_mix_value(value):
            if not label:
                continue
            counter[label] += max(1, int(count or 1))
    if not counter:
        return ""
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))
    return "; ".join(f"{label} - {count}" for label, count in ordered)


def top_labels(values: list[str], *, limit: int = 12) -> list[str]:
    counter: Counter[str] = Counter()
    for value in values:
        for label, count in tokenize_mix_value(value):
            if not label:
                continue
            counter[label] += max(1, int(count or 1))
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))
    return [label for label, _count in ordered[: max(1, int(limit or 12))]]


def build_signal_strength(*, messages: list[str], fixes: list[str], growth: list[str], deals_count: int) -> int:
    # Technical proxy metric only; no scripted analytics meaning.
    non_empty = sum(1 for item in [*messages, *fixes, *growth] if clean_text(item))
    return int(non_empty + max(0, int(deals_count or 0)))
