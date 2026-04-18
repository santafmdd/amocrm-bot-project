from __future__ import annotations

from typing import Any

from .config import DealAnalyzerConfig
from .models import DealAnalysis
from .prompt_builder import build_employee_training_message_draft, build_manager_message_draft


def analyze_deal(normalized_deal: dict[str, Any], config: DealAnalyzerConfig) -> DealAnalysis:
    deal = normalized_deal if isinstance(normalized_deal, dict) else {}
    weights = config.score_weights

    product_values = _as_list_str(deal.get("product_values"))
    tags = _as_list_str(deal.get("tags"))
    notes = deal.get("notes_summary_raw") if isinstance(deal.get("notes_summary_raw"), list) else []
    tasks = deal.get("tasks_summary_raw") if isinstance(deal.get("tasks_summary_raw"), list) else []
    notes_texts = _extract_texts(notes)
    tasks_texts = _extract_texts(tasks)
    notes_joined = " ".join(_normalize_text_for_match(x) for x in notes_texts)

    has_presentation = bool(deal.get("presentation_detected") or deal.get("demo_result_text") or deal.get("brief_url"))
    has_brief = bool(_clean_text(deal.get("brief_url")))
    has_demo = bool(_clean_text(deal.get("demo_result_text")))
    has_pain = bool(_clean_text(deal.get("pain_text")))
    has_business_tasks = bool(_clean_text(deal.get("business_tasks_text")))
    has_followup_tasks = len(tasks) > 0
    has_product_fit = len(product_values) > 0
    has_probability = deal.get("probability_value") not in (None, "")
    has_long_call = bool(deal.get("long_call_detected"))
    has_notes_content = len(notes_texts) > 0
    has_tasks_content = len(tasks_texts) > 0
    has_tags = len(tags) > 0
    has_contact_data = _has_contact_data(deal)
    has_company_data = _has_company_data(deal)

    status_norm = _normalize_text_for_match(deal.get("status_name"))
    is_closed_lost = any(x in status_norm for x in ("закрыто", "не реализ", "отказ"))
    has_reasoned_loss = is_closed_lost and _has_reasoned_loss_context(notes_joined)
    has_market_mismatch = has_reasoned_loss and _has_market_mismatch_keywords(notes_joined)

    data_points = [
        has_brief,
        has_demo,
        has_pain,
        has_business_tasks,
        has_product_fit,
        has_probability,
        has_contact_data,
        has_company_data,
        has_notes_content,
        has_tags,
    ]
    completeness_ratio = sum(1 for x in data_points if x) / len(data_points)

    updated_at = _as_int(deal.get("updated_at"))
    created_at = _as_int(deal.get("created_at"))
    has_stage_movement = bool(updated_at and created_at and updated_at > created_at)

    comments_present = bool(_clean_text(deal.get("company_comment")) or _clean_text(deal.get("contact_comment")))
    notes_present = len(notes) > 0
    tasks_present = len(tasks) > 0

    empty_context_with_movement = has_stage_movement and not (notes_present or tasks_present or comments_present)
    has_any_context = any(
        (
            has_notes_content,
            has_tasks_content,
            comments_present,
            has_contact_data,
            has_company_data,
            has_tags,
            has_long_call,
        )
    )

    score = 0
    if has_presentation:
        score += weights.get("presentation", 0)
    if has_brief:
        score += weights.get("brief", 0)
    if has_demo:
        score += weights.get("demo_result", 0)
    if has_pain:
        score += weights.get("pain", 0)
    if has_business_tasks:
        score += weights.get("business_tasks", 0)
    if has_followup_tasks:
        score += weights.get("followup_tasks", 0)
    if has_product_fit:
        score += weights.get("product_fit", 0)
    if has_probability:
        score += weights.get("probability", 0)
    if completeness_ratio >= 0.66:
        score += weights.get("data_completeness", 0)

    if has_notes_content:
        score += 6
    if has_tasks_content:
        score += 4
    if has_contact_data:
        score += 4
    if has_company_data:
        score += 4
    if has_tags:
        score += 2
    if has_long_call:
        score += 4
    if has_reasoned_loss:
        score += 3

    if empty_context_with_movement:
        score -= 8
    if is_closed_lost and not has_reasoned_loss and not has_any_context:
        score -= 10
    if score <= 0 and has_any_context:
        score = 12

    score = max(0, min(100, int(score)))

    strong_sides: list[str] = []
    growth_zones: list[str] = []
    risk_flags: list[str] = []

    _push_flag(has_presentation, strong_sides, growth_zones, "Проведена презентация", "Не подтверждена презентация")
    _push_flag(has_brief, strong_sides, growth_zones, "Заполнен бриф", "Не заполнен бриф")
    _push_flag(has_demo, strong_sides, growth_zones, "Зафиксирован результат демонстрации", "Нет результата демонстрации")
    _push_flag(has_pain, strong_sides, growth_zones, "Зафиксирована боль клиента", "Не зафиксирована боль клиента")
    _push_flag(has_business_tasks, strong_sides, growth_zones, "Записаны бизнес-задачи клиента", "Не заполнены бизнес-задачи клиента")
    _push_flag(has_followup_tasks, strong_sides, growth_zones, "Есть follow-up задачи", "Нет follow-up задач")
    _push_flag(has_product_fit, strong_sides, growth_zones, "Есть подтвержденный product fit", "Не подтвержден product fit")
    _push_flag(has_probability, strong_sides, growth_zones, "Указана вероятность сделки", "Не указана вероятность сделки")
    _push_flag(has_notes_content, strong_sides, growth_zones, "Есть контекст в notes", "Нет внятного контекста в notes")
    _push_flag(has_contact_data, strong_sides, growth_zones, "Заполнены контактные данные", "Неполные контактные данные")
    _push_flag(has_company_data, strong_sides, growth_zones, "Заполнены данные компании", "Неполные данные компании")

    if empty_context_with_movement:
        risk_flags.append("evidence_context: Есть движение по сделке без контекста в notes/tasks/comments")
    if not has_followup_tasks:
        risk_flags.append("process_hygiene: Нет follow-up задач")
    if not has_notes_content and not has_tasks_content:
        risk_flags.append("evidence_context: Нет содержательных notes/tasks")
    if has_reasoned_loss:
        if has_market_mismatch:
            risk_flags.append("qualified_loss: Рыночное несовпадение/нецелевой сценарий")
        else:
            risk_flags.append("qualified_loss: Осознанный отказ/anti-fit с контекстом")
    elif is_closed_lost and not has_any_context:
        risk_flags.append("evidence_context: Закрытая сделка без объяснимого контекста")

    if score < 40:
        risk_flags.append("process_hygiene: Низкая оценка качества ведения сделки")

    presentation_quality_flag = "ok" if has_presentation and has_demo else "needs_attention"
    followup_quality_flag = "ok" if has_followup_tasks else "needs_attention"
    if completeness_ratio >= 0.8:
        data_completeness_flag = "complete"
    elif completeness_ratio >= 0.5 or has_any_context:
        data_completeness_flag = "partial"
    else:
        data_completeness_flag = "poor"

    policy = _select_policy(risk_flags)
    recommended_actions_for_manager = _manager_actions(
        policy=policy,
        has_presentation=has_presentation,
        has_followup_tasks=has_followup_tasks,
        has_probability=has_probability,
        has_pain=has_pain,
        has_notes_content=has_notes_content,
        has_business_tasks=has_business_tasks,
    )
    recommended_training_tasks_for_employee = _employee_training_tasks(
        policy=policy,
        has_brief=has_brief,
        has_demo=has_demo,
        has_pain=has_pain,
        has_business_tasks=has_business_tasks,
        has_notes_content=has_notes_content,
    )

    analysis = DealAnalysis(
        deal_id=_as_int(deal.get("deal_id")),
        amo_lead_id=_as_int(deal.get("amo_lead_id")),
        deal_name=_clean_text(deal.get("deal_name")),
        score_0_100=score,
        strong_sides=strong_sides,
        growth_zones=growth_zones,
        risk_flags=risk_flags,
        presentation_quality_flag=presentation_quality_flag,
        followup_quality_flag=followup_quality_flag,
        data_completeness_flag=data_completeness_flag,
        recommended_actions_for_manager=recommended_actions_for_manager,
        recommended_training_tasks_for_employee=recommended_training_tasks_for_employee,
        manager_message_draft="",
        employee_training_message_draft="",
    )

    manager_draft = build_manager_message_draft(analysis)
    employee_draft = build_employee_training_message_draft(analysis)

    merged = analysis.to_dict()
    merged["manager_message_draft"] = manager_draft
    merged["employee_training_message_draft"] = employee_draft
    return DealAnalysis(**merged)


def _push_flag(ok: bool, strong: list[str], growth: list[str], ok_text: str, fail_text: str) -> None:
    if ok:
        strong.append(ok_text)
    else:
        growth.append(fail_text)


def _manager_actions(
    *,
    policy: str,
    has_presentation: bool,
    has_followup_tasks: bool,
    has_probability: bool,
    has_pain: bool,
    has_notes_content: bool,
    has_business_tasks: bool,
) -> list[str]:
    if policy == "qualified_loss":
        return [
            "Подтвердить и зафиксировать причину anti-fit / market mismatch в карточке сделки.",
            "Пометить кейс как нецелевой или ограниченно перспективный для текущего сегмента.",
            "Исключить сделку из стандартного follow-up pressure path и зафиксировать статус решения.",
            "Передать кейс в сегментный анализ рынка и продуктового позиционирования.",
        ]
    if policy == "evidence_context":
        actions: list[str] = []
        if not has_notes_content:
            actions.append("Дописать содержательные notes по сделке: контекст, причина статуса, следующий шаг.")
        if not has_business_tasks:
            actions.append("Заполнить бизнес-задачу клиента и ожидаемый результат в CRM.")
        if not has_pain:
            actions.append("Зафиксировать боль клиента и критерии успеха, чтобы снять риск потери контекста.")
        actions.append("Проверить полноту evidence-данных перед следующей активностью по сделке.")
        return actions[:4]

    actions: list[str] = []
    if not has_presentation:
        actions.append("Назначить и провести презентацию с фиксацией результата")
    if not has_followup_tasks:
        actions.append("Поставить follow-up задачу с датой и ответственным")
    if not has_probability:
        actions.append("Обновить вероятность сделки по текущему этапу")
    if not has_pain:
        actions.append("Провести короткий разбор потребности и зафиксировать боль клиента")
    return actions or ["Поддерживать текущий темп и качество ведения сделки"]


def _employee_training_tasks(
    *,
    policy: str,
    has_brief: bool,
    has_demo: bool,
    has_pain: bool,
    has_business_tasks: bool,
    has_notes_content: bool,
) -> list[str]:
    if policy == "qualified_loss":
        return [
            "Разбор кейса: как корректно фиксировать anti-fit/market mismatch без лишнего давления на клиента.",
            "Отработать формулировку причины отказа в CRM так, чтобы она была полезна для сегментного анализа.",
            "Сфокусироваться на качестве квалификации и раннем отсеве нецелевых кейсов.",
        ]
    if policy == "evidence_context":
        tasks: list[str] = []
        if not has_notes_content:
            tasks.append("Тренировка: записывать содержательные notes после каждого значимого контакта.")
        if not has_business_tasks:
            tasks.append("Тренировка: формулировка бизнес-задач клиента в измеримом виде.")
        if not has_pain:
            tasks.append("Тренировка: выявление боли клиента и критериев успеха.")
        return tasks or ["Тренировка: поддерживать полноту CRM-контекста по сделке."]

    tasks: list[str] = []
    if not has_brief:
        tasks.append("Тренировка: корректное заполнение брифа")
    if not has_demo:
        tasks.append("Тренировка: фиксация результата демонстрации в CRM")
    if not has_pain:
        tasks.append("Тренировка: выявление боли клиента (SPIN/5 Why)")
    if not has_business_tasks:
        tasks.append("Тренировка: формулировка бизнес-задач клиента")
    return tasks or ["Тренировка не требуется, поддерживать текущий стандарт"]


def _select_policy(risk_flags: list[str]) -> str:
    if any(str(x).startswith("qualified_loss:") for x in risk_flags):
        return "qualified_loss"
    process_count = sum(1 for x in risk_flags if str(x).startswith("process_hygiene:"))
    evidence_count = sum(1 for x in risk_flags if str(x).startswith("evidence_context:"))
    if evidence_count >= process_count and evidence_count > 0:
        return "evidence_context"
    return "process_hygiene"


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _as_list_str(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        txt = _clean_text(item)
        if txt:
            out.append(txt)
    return out


def _as_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_texts(items: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        candidates = (
            item.get("text"),
            item.get("note_text"),
            item.get("body"),
            item.get("result"),
            item.get("content"),
            item.get("name"),
        )
        for candidate in candidates:
            txt = _clean_text(candidate)
            if txt:
                out.append(txt)
                break
    return out


def _normalize_text_for_match(value: Any) -> str:
    return _clean_text(value).lower().replace("ё", "е")


def _has_contact_data(deal: dict[str, Any]) -> bool:
    phones = deal.get("contact_phone")
    emails = deal.get("contact_email")
    name = _clean_text(deal.get("contact_name"))
    if isinstance(phones, list):
        phone_ok = any(_clean_text(x) for x in phones)
    else:
        phone_ok = bool(_clean_text(phones))
    if isinstance(emails, list):
        email_ok = any(_clean_text(x) for x in emails)
    else:
        email_ok = bool(_clean_text(emails))
    return phone_ok or email_ok or bool(name)


def _has_company_data(deal: dict[str, Any]) -> bool:
    company = _clean_text(deal.get("company_name"))
    inn = _clean_text(deal.get("company_inn"))
    return bool(company or inn)


def _has_reasoned_loss_context(notes_norm: str) -> bool:
    if not notes_norm:
        return False
    markers = (
        "отказ",
        "не подошло",
        "анти",
        "свои разработк",
        "не будут работать в облаке",
        "не готовы к облаку",
        "не готовы переходить",
        "дорого",
        "нецелев",
        "рынок не",
    )
    return any(marker in notes_norm for marker in markers)


def _has_market_mismatch_keywords(notes_norm: str) -> bool:
    if not notes_norm:
        return False
    markers = (
        "свои разработк",
        "не будут работать в облаке",
        "не готовы к облаку",
        "нецелев",
        "рынок не",
    )
    return any(marker in notes_norm for marker in markers)
