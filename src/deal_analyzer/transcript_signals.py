from __future__ import annotations

from collections import Counter
from typing import Any


def derive_transcript_signals(*, deal: dict[str, Any], snapshot: dict[str, Any] | None) -> dict[str, Any]:
    transcript_texts = _collect_transcript_texts(deal=deal, snapshot=snapshot)
    combined = "\n".join(transcript_texts).strip()
    norm = _norm(combined)

    has_transcript = bool(norm)
    signal_info = _has_any(
        norm,
        (
            "info",
            "инфо",
            "каталог",
            "карточк",
            "контент",
            "фото",
            "описани",
            "ценообраз",
        ),
    )
    signal_link = _has_any(
        norm,
        (
            "link",
            "srm",
            "закуп",
            "поставщ",
            "тендер",
            "кп",
            "коммерческ",
            "сравнен",
            "интеграц",
        ),
    )
    signal_demo = _has_any(norm, ("demo", "демо", "демонстр", "презентац", "показ систем"))
    signal_test = _has_any(norm, ("тест", "пилот", "пробн", "протестир"))
    signal_budget = _has_any(norm, ("бюджет", "дорог", "цена", "стоимост", "оплата", "кп"))
    signal_followup = _has_any(norm, ("перезвон", "связаться", "повторный звонок", "follow up", "фоллоу"))
    signal_objection_price = _has_any(norm, ("дорого", "высокая цена", "не укладыва", "бюджета нет"))
    signal_objection_no_need = _has_any(norm, ("не нужно", "не актуально", "нет потребности", "отложили"))
    signal_objection_not_target = _has_any(norm, ("не целев", "не наш продукт", "свои разработк", "не будут работать в облаке"))
    signal_next_step = _has_any(
        norm,
        (
            "следующий шаг",
            "договорились",
            "вышлю",
            "направлю",
            "подготовим кп",
            "созвон",
            "встреча",
            "перезвон",
        ),
    )
    signal_dmr = _has_any(norm, ("лпр", "лицо принимающее решение", "директор", "собственник", "руководител"))

    excerpt = _make_excerpt(combined)
    summary = _build_signal_summary(
        transcript_available=has_transcript,
        product_info=signal_info,
        product_link=signal_link,
        demo=signal_demo,
        test=signal_test,
        budget=signal_budget,
        next_step=signal_next_step,
        objection_price=signal_objection_price,
        objection_no_need=signal_objection_no_need,
        objection_not_target=signal_objection_not_target,
        decision_maker=signal_dmr,
    )
    return {
        "transcript_available": has_transcript,
        "transcript_text_excerpt": excerpt,
        "call_signal_product_info": signal_info,
        "call_signal_product_link": signal_link,
        "call_signal_demo_discussed": signal_demo,
        "call_signal_test_discussed": signal_test,
        "call_signal_budget_discussed": signal_budget,
        "call_signal_followup_discussed": signal_followup,
        "call_signal_objection_price": signal_objection_price,
        "call_signal_objection_no_need": signal_objection_no_need,
        "call_signal_objection_not_target": signal_objection_not_target,
        "call_signal_next_step_present": signal_next_step,
        "call_signal_decision_maker_reached": signal_dmr,
        "call_signal_summary_short": summary,
    }


def build_call_signal_aggregates(records: list[dict[str, Any]]) -> dict[str, int]:
    transcript_count = 0
    next_step_count = 0
    next_step_not_in_crm_count = 0
    wrong_or_mixed_product_count = 0
    objection_pattern_count = 0

    for item in records:
        if bool(item.get("transcript_available")):
            transcript_count += 1
        if bool(item.get("call_signal_next_step_present")):
            next_step_count += 1
            flags = item.get("risk_flags") if isinstance(item.get("risk_flags"), list) else []
            if any("follow-up" in str(flag).lower() for flag in flags):
                next_step_not_in_crm_count += 1
        if bool(item.get("call_signal_product_link")) and bool(item.get("call_signal_product_info")):
            wrong_or_mixed_product_count += 1
        elif bool(item.get("call_signal_product_link")) and str(item.get("product_hypothesis") or "").lower() == "info":
            wrong_or_mixed_product_count += 1
        elif bool(item.get("call_signal_product_info")) and str(item.get("product_hypothesis") or "").lower() == "link":
            wrong_or_mixed_product_count += 1
        if any(
            bool(item.get(k))
            for k in (
                "call_signal_objection_price",
                "call_signal_objection_no_need",
                "call_signal_objection_not_target",
            )
        ):
            objection_pattern_count += 1
    return {
        "deals_with_transcript": transcript_count,
        "deals_with_next_step_in_call": next_step_count,
        "deals_next_step_in_call_but_missing_followup_in_crm": next_step_not_in_crm_count,
        "deals_with_probable_wrong_or_mixed_product_by_call": wrong_or_mixed_product_count,
        "deals_with_early_objection_pattern": objection_pattern_count,
    }


def _collect_transcript_texts(*, deal: dict[str, Any], snapshot: dict[str, Any] | None) -> list[str]:
    texts: list[str] = []
    for key in ("transcript_text", "call_transcript_text"):
        raw = str(deal.get(key) or "").strip()
        if raw:
            texts.append(raw)

    if isinstance(snapshot, dict):
        transcripts = snapshot.get("transcripts")
        if isinstance(transcripts, list):
            for item in transcripts:
                if not isinstance(item, dict):
                    continue
                text = str(item.get("transcript_text") or item.get("text") or "").strip()
                if text:
                    texts.append(text)

    deduped: list[str] = []
    seen: set[str] = set()
    for text in texts:
        key = _norm(text)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(text)
    return deduped


def _make_excerpt(text: str, limit: int = 240) -> str:
    clean = " ".join(str(text or "").strip().split())
    if len(clean) <= limit:
        return clean
    return clean[: limit - 1].rstrip() + "…"


def _build_signal_summary(
    *,
    transcript_available: bool,
    product_info: bool,
    product_link: bool,
    demo: bool,
    test: bool,
    budget: bool,
    next_step: bool,
    objection_price: bool,
    objection_no_need: bool,
    objection_not_target: bool,
    decision_maker: bool,
) -> str:
    if not transcript_available:
        return ""
    parts: list[str] = []
    if product_info and product_link:
        parts.append("обсуждались INFO и LINK (mixed-сигнал)")
    elif product_info:
        parts.append("преобладает INFO-сигнал")
    elif product_link:
        parts.append("преобладает LINK-сигнал")
    if demo:
        parts.append("обсуждалась презентация/демо")
    if test:
        parts.append("обсуждался тест/пилот")
    if budget:
        parts.append("поднимался вопрос бюджета/цены")
    if next_step:
        parts.append("в разговоре есть следующий шаг")
    if decision_maker:
        parts.append("есть сигнал контакта с ЛПР")
    objections = []
    if objection_price:
        objections.append("цена")
    if objection_no_need:
        objections.append("нет потребности")
    if objection_not_target:
        objections.append("нецелевой кейс")
    if objections:
        parts.append(f"возражения: {', '.join(objections)}")
    if not parts:
        return "есть транскрипт, но явных управленческих сигналов мало"
    return "; ".join(parts[:3])


def _norm(text: Any) -> str:
    out = " ".join(str(text or "").strip().lower().replace("ё", "е").split())
    return out


def _has_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)
