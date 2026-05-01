from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from .models import EvidenceItem

OBJECTION_PATTERNS: dict[str, tuple[str, ...]] = {
    "дорого": ("дорого", "высокая цена", "бюджет"),
    "не актуально": ("не актуально", "неинтересно", "не сейчас"),
    "нет времени": ("нет времени", "позже", "перезвоните"),
    "уже есть решение": ("уже есть", "работаем с", "другой подрядчик"),
    "отправьте на почту": ("отправьте", "на почту", "скиньте"),
}


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _detect_objections(text: str) -> list[str]:
    probe = _clean(text).lower()
    if not probe:
        return []
    found: list[str] = []
    for name, patterns in OBJECTION_PATTERNS.items():
        if any(pattern in probe for pattern in patterns):
            found.append(name)
    return found


def analyze_objections(
    evidence_items: list[EvidenceItem],
    *,
    top_n: int = 6,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    success_counter: Counter[str] = Counter()
    failure_counter: Counter[str] = Counter()
    success_links: dict[str, list[str]] = defaultdict(list)
    failure_links: dict[str, list[str]] = defaultdict(list)

    for item in evidence_items:
        objections = _detect_objections(item.text)
        if not objections:
            continue
        for objection in objections:
            if item.outcome == "success":
                success_counter[objection] += 1
                if item.evidence_link:
                    success_links[objection].append(item.evidence_link)
            elif item.outcome == "failure":
                failure_counter[objection] += 1
                if item.evidence_link:
                    failure_links[objection].append(item.evidence_link)

    def _pack(counter: Counter[str], links_map: dict[str, list[str]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for objection, count in counter.most_common(max(1, int(top_n or 6))):
            seen: set[str] = set()
            links: list[str] = []
            for link in links_map.get(objection, []):
                cleaned = _clean(link)
                if not cleaned or cleaned in seen:
                    continue
                seen.add(cleaned)
                links.append(cleaned)
            out.append(
                {
                    "objection": objection,
                    "count": int(count),
                    "evidence_links": links[:5],
                }
            )
        return out

    success = _pack(success_counter, success_links)
    failures = _pack(failure_counter, failure_links)
    debug = {
        "objections_success_total": sum(int(item.get("count", 0) or 0) for item in success),
        "objections_failure_total": sum(int(item.get("count", 0) or 0) for item in failures),
        "success": success,
        "failures": failures,
    }
    return success, failures, debug
