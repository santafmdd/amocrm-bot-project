from __future__ import annotations

from typing import Any

from .config import DealAnalyzerConfig
from .models import DealAnalysis
from .prompt_builder import build_employee_training_message_draft, build_manager_message_draft


def analyze_deal(normalized_deal: dict[str, Any], config: DealAnalyzerConfig) -> DealAnalysis:
    deal = normalized_deal if isinstance(normalized_deal, dict) else {}
    weights = config.score_weights

    product_values = _as_list_str(deal.get("product_values"))
    notes = deal.get("notes_summary_raw") if isinstance(deal.get("notes_summary_raw"), list) else []
    tasks = deal.get("tasks_summary_raw") if isinstance(deal.get("tasks_summary_raw"), list) else []

    has_presentation = bool(deal.get("presentation_detected") or deal.get("demo_result_text") or deal.get("brief_url"))
    has_brief = bool(_clean_text(deal.get("brief_url")))
    has_demo = bool(_clean_text(deal.get("demo_result_text")))
    has_pain = bool(_clean_text(deal.get("pain_text")))
    has_business_tasks = bool(_clean_text(deal.get("business_tasks_text")))
    has_followup_tasks = len(tasks) > 0
    has_product_fit = len(product_values) > 0
    has_probability = deal.get("probability_value") not in (None, "")

    data_points = [has_brief, has_demo, has_pain, has_business_tasks, has_product_fit, has_probability]
    completeness_ratio = sum(1 for x in data_points if x) / len(data_points)

    updated_at = _as_int(deal.get("updated_at"))
    created_at = _as_int(deal.get("created_at"))
    has_stage_movement = bool(updated_at and created_at and updated_at > created_at)

    comments_present = bool(_clean_text(deal.get("company_comment")) or _clean_text(deal.get("contact_comment")))
    notes_present = len(notes) > 0
    tasks_present = len(tasks) > 0

    empty_context_with_movement = has_stage_movement and not (notes_present or tasks_present or comments_present)

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

    if empty_context_with_movement:
        risk_flags.append("Есть движение по сделке без контекста в notes/tasks/comments")

    if score < 40:
        risk_flags.append("Низкая оценка качества ведения сделки")

    presentation_quality_flag = "ok" if has_presentation and has_demo else "needs_attention"
    followup_quality_flag = "ok" if has_followup_tasks else "needs_attention"
    if completeness_ratio >= 0.8:
        data_completeness_flag = "complete"
    elif completeness_ratio >= 0.5:
        data_completeness_flag = "partial"
    else:
        data_completeness_flag = "poor"

    recommended_actions_for_manager = _manager_actions(
        has_presentation=has_presentation,
        has_followup_tasks=has_followup_tasks,
        has_probability=has_probability,
        has_pain=has_pain,
    )
    recommended_training_tasks_for_employee = _employee_training_tasks(
        has_brief=has_brief,
        has_demo=has_demo,
        has_pain=has_pain,
        has_business_tasks=has_business_tasks,
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


def _manager_actions(*, has_presentation: bool, has_followup_tasks: bool, has_probability: bool, has_pain: bool) -> list[str]:
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


def _employee_training_tasks(*, has_brief: bool, has_demo: bool, has_pain: bool, has_business_tasks: bool) -> list[str]:
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
