from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DailyCaseProfile:
    mode: str
    mode_reason: str
    confidence: str
    allowed_axes: tuple[str, ...]
    banned_topics: tuple[str, ...]
    preferred_modules: tuple[str, ...]


DAILY_CASE_MODES = {
    "negotiation_lpr_analysis",
    "secretary_analysis",
    "redial_discipline_analysis",
    "supplier_inbound_analysis",
    "warm_inbound_analysis",
    "skip_no_meaningful_case",
}


def classify_daily_case(*, role: str, items: list[dict[str, Any]]) -> DailyCaseProfile:
    role_norm = str(role or "").strip().lower()
    text = _items_text(items)
    has_usable = any(_transcript_usable_score(i) >= 2 for i in items if isinstance(i, dict))
    lpr_hits = _count_hits(text, ("лпр", "директор", "собственник", "лицо принима", "руководител"))
    secretary_hits = _count_hits(text, ("секретар", "ресепш", "соедините", "перевед", "почт", "маршрут"))
    supplier_hits = _count_hits(text, ("поставщик", "закуп", "тендер", "кп", "etp", "электронн", "регистрац", "заявк"))
    warm_hits = _count_hits(text, ("демо", "демонстрац", "бриф", "тест", "оплат", "счет", "встреч", "подтвержд"))
    dead_redial = any(
        bool(i.get("repeated_dead_redial_day_flag"))
        or bool(i.get("same_time_redial_pattern_flag"))
        or bool(i.get("numbers_not_fully_covered_flag"))
        or int(i.get("repeated_dead_redial_count", 0) or 0) > 0
        for i in items
        if isinstance(i, dict)
    )

    if dead_redial and not has_usable:
        return _profile_redial("dial_pattern_without_usable_negotiation")
    if has_usable and lpr_hits > 0:
        return _profile_lpr("usable_lpr_call")
    if has_usable and secretary_hits > 0 and lpr_hits == 0:
        return _profile_secretary("secretary_route_call")
    if has_usable and supplier_hits > 0 and warm_hits == 0:
        return _profile_supplier("supplier_inbound_signals")
    if has_usable and warm_hits > 0:
        return _profile_warm("warm_inbound_or_warm_stage_signals")
    if dead_redial:
        return _profile_redial("dial_pattern_detected")
    if has_usable:
        if "телемаркетолог" in role_norm:
            return _profile_secretary("usable_call_without_lpr_for_cold_role")
        return _profile_warm("usable_call_general")

    return DailyCaseProfile(
        mode="skip_no_meaningful_case",
        mode_reason="no_usable_call_and_no_meaningful_dial_pattern",
        confidence="low",
        allowed_axes=("skip",),
        banned_topics=(),
        preferred_modules=(),
    )


def mode_is_writable(profile: DailyCaseProfile) -> bool:
    return profile.mode != "skip_no_meaningful_case"


def mode_prompt_policy(profile: DailyCaseProfile) -> dict[str, Any]:
    return {
        "daily_analysis_mode": profile.mode,
        "mode_reason": profile.mode_reason,
        "mode_confidence": profile.confidence,
        "allowed_axes": list(profile.allowed_axes),
        "banned_topics": list(profile.banned_topics),
        "preferred_modules": list(profile.preferred_modules),
    }


def _profile_lpr(reason: str) -> DailyCaseProfile:
    return DailyCaseProfile(
        mode="negotiation_lpr_analysis",
        mode_reason=reason,
        confidence="high",
        allowed_axes=(
            "этапы переговоров",
            "выход на ЛПР",
            "открытые и уточняющие вопросы",
            "снятие сомнений",
            "закрытие на следующий шаг",
        ),
        banned_topics=(),
        preferred_modules=(
            "модуль выхода на ЛПР",
            "модуль открытых и уточняющих вопросов",
            "модуль фиксации следующего шага",
        ),
    )


def _profile_secretary(reason: str) -> DailyCaseProfile:
    return DailyCaseProfile(
        mode="secretary_analysis",
        mode_reason=reason,
        confidence="medium",
        allowed_axes=(
            "проход секретаря",
            "уточнение маршрута",
            "инфоповод",
            "выход на релевантную роль",
            "следующий шаг по секретарю",
        ),
        banned_topics=(
            "бриф",
            "презентац",
            "демо",
            "демонстрац",
            "боль клиента",
            "бизнес-задач",
            "результат демонстрац",
        ),
        preferred_modules=(
            "модуль захода через инфоповод",
            "модуль «по какому вопросу»",
            "модуль выхода на закупочную роль",
        ),
    )


def _profile_redial(reason: str) -> DailyCaseProfile:
    return DailyCaseProfile(
        mode="redial_discipline_analysis",
        mode_reason=reason,
        confidence="medium",
        allowed_axes=(
            "дисциплина попыток",
            "покрытие номеров",
            "время дозвона",
            "повторы без результата",
        ),
        banned_topics=(
            "бриф",
            "презентац",
            "демо",
            "демонстрац",
            "боль клиента",
            "бизнес-задач",
            "выход на лпр",
            "закрытие на встречу",
        ),
        preferred_modules=(
            "правило лимита повторов по номеру",
            "модуль смены окна дозвона",
            "правило полного покрытия номеров",
        ),
    )


def _profile_supplier(reason: str) -> DailyCaseProfile:
    return DailyCaseProfile(
        mode="supplier_inbound_analysis",
        mode_reason=reason,
        confidence="medium",
        allowed_axes=(
            "входящий интерес",
            "маршрут заявки",
            "квалификация supplier/etp сценария",
            "следующий шаг по входящему",
        ),
        banned_topics=("не заполнен бриф", "не подтверждена презентация"),
        preferred_modules=(
            "модуль входящего supplier-контакта",
            "модуль уточнения маршрута заявки",
            "модуль фиксации следующего шага по входящему",
        ),
    )


def _profile_warm(reason: str) -> DailyCaseProfile:
    return DailyCaseProfile(
        mode="warm_inbound_analysis",
        mode_reason=reason,
        confidence="medium",
        allowed_axes=(
            "подтверждение встречи/демо",
            "результат демо/теста",
            "следующий шаг после встречи",
            "движение к оплате",
        ),
        banned_topics=(),
        preferred_modules=(
            "модуль подтверждения встречи",
            "модуль post-demo следующего шага",
            "модуль фиксации результата в CRM",
        ),
    )


def _count_hits(text: str, markers: tuple[str, ...]) -> int:
    if not text:
        return 0
    low = text.lower()
    return sum(1 for marker in markers if marker in low)


def _items_text(items: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        for key in (
            "call_signal_summary_short",
            "transcript_text_excerpt",
            "status_name",
            "pipeline_name",
            "manager_summary",
            "product_name",
            "call_history_pattern_summary",
        ):
            value = " ".join(str(item.get(key) or "").split()).strip()
            if value:
                chunks.append(value)
        for key in ("tags", "company_tags", "source_values"):
            vals = item.get(key) if isinstance(item.get(key), list) else []
            for value in vals:
                text = " ".join(str(value or "").split()).strip()
                if text:
                    chunks.append(text)
    return " ".join(chunks).strip()


def _transcript_usable_score(item: dict[str, Any]) -> int:
    label = str(item.get("transcript_usability_label") or "").strip().lower()
    if label == "usable":
        return 3
    if label in {"weak", "noisy"}:
        return 1
    excerpt = " ".join(str(item.get("transcript_text_excerpt") or "").split()).strip()
    call_summary = " ".join(str(item.get("call_signal_summary_short") or "").split()).strip()
    if not excerpt and not call_summary:
        return 0
    if len(excerpt) >= 120 or len(call_summary) >= 80:
        return 2
    return 1
