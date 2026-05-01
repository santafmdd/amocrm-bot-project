from __future__ import annotations

from collections import Counter
from typing import Any

from .models import EvidenceItem


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _contains_any(text: str, patterns: tuple[str, ...]) -> bool:
    probe = _clean(text).lower()
    return any(pattern in probe for pattern in patterns)


def analyze_behavior_patterns(
    *,
    strengths: list[str],
    growth_zones: list[str],
    evidence_items: list[EvidenceItem],
    top_n: int = 8,
) -> tuple[list[str], list[str], dict[str, Any]]:
    recurring_counter: Counter[str] = Counter()
    pattern_counter: Counter[str] = Counter()

    for text in growth_zones:
        phrase = _clean(text)
        if phrase:
            recurring_counter[phrase] += 1

    for item in evidence_items:
        text = _clean(item.text)
        if not text:
            continue
        probe = text.lower()
        if _contains_any(probe, ("лпр", "лицо, принимающее решение", "принимает решение")):
            pattern_counter["Проваливается в ранней квалификации ЛПР"] += 1
        if _contains_any(probe, ("следующего шага", "следующий шаг", "дата", "время", "зафикс")):
            if item.outcome == "success":
                pattern_counter["Фиксирует следующий шаг, когда держит структуру"] += 1
            elif item.outcome == "failure":
                pattern_counter["Теряет следующий шаг без конкретики даты/времени"] += 1
        if _contains_any(probe, ("возраж", "дорого", "не актуально", "подумать")):
            if item.outcome == "success":
                pattern_counter["Стабильно отрабатывает возражения при структурном диалоге"] += 1
            elif item.outcome == "failure":
                pattern_counter["Теряет управление на возражениях при поверхностной диагностике"] += 1

    patterns = [text for text, _count in pattern_counter.most_common(max(1, int(top_n or 8)))]
    recurring = [text for text, _count in recurring_counter.most_common(max(1, int(top_n or 8)))]
    debug = {
        "pattern_counts": dict(pattern_counter),
        "recurring_mistake_counts": dict(recurring_counter),
        "strengths_total": len(strengths),
        "growth_zones_total": len(growth_zones),
    }
    return patterns, recurring, debug
