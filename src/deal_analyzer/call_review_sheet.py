from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

from .base_mix import resolve_base_mix


CALL_REVIEW_DEFAULT_COLUMNS = [
    "Дата анализа",
    "Дата кейса",
    "Менеджер",
    "Роль",
    "Deal ID",
    "Ссылка на сделку",
    "Сделка",
    "Компания",
    "Продукт / фокус",
    "База / тег",
    "Тип кейса",
    "Прослушанные звонки",
    "Проход секретаря",
    "Контакт с ЛПР",
    "Актуальность и потребность",
    "Презентация встречи",
    "Закрытие на встречу",
    "Отработка возражений",
    "Чистота речи",
    "Работа с CRM",
    "Дисциплина дозвонов",
    "Вход и рамка демо",
    "Выявление контекста перед показом",
    "Показ релевантного сценария",
    "Привязка к процессу клиента",
    "Работа с вопросами и возражениями на демо",
    "Фиксация следующего шага после демо",
    "Комментарий по этапу (демо)",
    "Запуск теста",
    "Критерии успеха теста",
    "Ответственные и сроки по тесту",
    "Сопровождение теста",
    "Сбор обратной связи по тесту",
    "Снятие возражений после теста",
    "Комментарий по этапу (тест)",
    "Повторный контакт по дожиму",
    "Работа с сомнениями и стоп-факторами",
    "Согласование условий / КП",
    "Фиксация решения",
    "Дожим не провисает",
    "Комментарий по этапу (дожим)",
    "Ключевой вывод",
    "Сильная сторона",
    "Зона роста",
    "Почему это важно",
    "Что закрепить",
    "Что исправить",
    "Что донести сотруднику",
    "Эффект количество / неделя",
    "Эффект качество",
    "Оценка 0-100",
    "Критичность",
]


CASE_DISPLAY_MAP = {
    "negotiation_lpr_analysis": "разговор с лпр",
    "secretary_analysis": "секретарь",
    "redial_discipline_analysis": "дисциплина дозвонов",
    "supplier_inbound_analysis": "входящий от поставщика",
    "warm_inbound_analysis": "теплый входящий",
    "presentation_analysis": "презентация",
    "test_analysis": "работа с тестом",
    "dozhim_analysis": "дожим",
}


ROLE_DISPLAY_MAP = {
    "telemarketer": "телемаркетолог",
    "sales_manager": "менеджер по продажам",
    "телемаркетолог": "телемаркетолог",
    "менеджер по продажам": "менеджер по продажам",
}


SECRETARY_TOKENS = (
    "секретар",
    "соедин",
    "маршрут",
    "перевед",
    "переадрес",
    "почт",
)

LPR_TOKENS = (
    "лпр",
    "лицо принима",
    "директор",
    "собственник",
    "руководител",
)

SUPPLIER_TOKENS = (
    "поставщик",
    "supplier",
    "закуп",
    "тендер",
    "площадк",
    "регистрац",
)

WARM_TOKENS = (
    "демо",
    "демонстрац",
    "тест",
    "бриф",
    "кп",
    "коммерческ",
    "оплат",
)


def build_call_review_payload(
    *,
    summary: dict[str, Any],
    period_deal_records: list[dict[str, Any]],
    analysis_shortlist_payload: dict[str, Any],
    base_domain: str,
    manager_allowlist: list[str] | tuple[str, ...] | None = None,
    manager_role_registry: dict[str, str] | None = None,
) -> dict[str, Any]:
    records_by_deal = {
        str(item.get("deal_id") or ""): item
        for item in period_deal_records
        if isinstance(item, dict) and str(item.get("deal_id") or "").strip()
    }
    selected_items = (
        analysis_shortlist_payload.get("selected_items", [])
        if isinstance(analysis_shortlist_payload.get("selected_items"), list)
        else []
    )

    run_analysis_date = _safe_date(summary.get("run_timestamp")) or _safe_date(summary.get("executed_at")) or ""
    allowlist = [str(x or "").strip() for x in (manager_allowlist or []) if str(x or "").strip()]

    rows: list[dict[str, Any]] = []
    debug_rows: list[dict[str, Any]] = []
    skipped_total = 0

    for idx, candidate in enumerate(selected_items):
        if not isinstance(candidate, dict):
            continue
        deal_id = str(candidate.get("deal_id") or "").strip()
        if not deal_id:
            skipped_total += 1
            debug_rows.append({"deal_id": "", "skip_reason": "missing_deal_id", "order": idx + 1})
            continue

        deal = records_by_deal.get(deal_id)
        if not isinstance(deal, dict):
            skipped_total += 1
            debug_rows.append({"deal_id": deal_id, "skip_reason": "deal_not_found_in_period_records", "order": idx + 1})
            continue

        manager_name = _text(deal.get("owner_name"))
        if allowlist and not _manager_allowed(manager_name=manager_name, allowlist=allowlist):
            skipped_total += 1
            debug_rows.append({
                "deal_id": deal_id,
                "manager": manager_name,
                "skip_reason": "manager_not_in_allowlist",
                "order": idx + 1,
            })
            continue

        case_mode = _derive_case_mode(candidate=candidate, deal=deal)
        primary_source = _derive_primary_source(case_mode=case_mode)
        skip_reason = _skip_reason_for_case(case_mode=case_mode, candidate=candidate, deal=deal)
        if skip_reason:
            skipped_total += 1
            debug_rows.append(
                {
                    "deal_id": deal_id,
                    "manager": manager_name,
                    "case_mode": case_mode,
                    "primary_source": primary_source,
                    "skip_reason": skip_reason,
                    "order": idx + 1,
                }
            )
            continue

        row = _build_case_row(
            analysis_date=run_analysis_date,
            candidate=candidate,
            deal=deal,
            case_mode=case_mode,
            primary_source=primary_source,
            base_domain=base_domain,
            manager_role_registry=manager_role_registry,
        )
        rows.append(row)
        debug_rows.append(
            {
                "deal_id": deal_id,
                "manager": manager_name,
                "case_mode": case_mode,
                "primary_source": primary_source,
                "skip_reason": "",
                "llm_ready": bool(deal.get("call_review_llm_ready")),
                "order": idx + 1,
                "selection_reason": str(candidate.get("shortlist_reason") or ""),
            }
        )

    rows.sort(
        key=lambda x: (
            str(x.get("Дата кейса") or ""),
            str(x.get("Менеджер") or ""),
            str(x.get("Deal ID") or ""),
        )
    )
    return {
        "mode": "call_review_sheet",
        "sheet_name": "Разбор звонков",
        "start_cell": "A2",
        "columns": list(CALL_REVIEW_DEFAULT_COLUMNS),
        "rows": rows,
        "rows_count": len(rows),
        "selection_debug": {
            "rows_total": len(rows),
            "rows_skipped": skipped_total,
            "rows_by_case_mode": _count_values(rows, key="__case_mode_internal"),
            "rows_by_primary_source": _count_values(rows, key="__daily_primary_source_internal"),
            "details": debug_rows,
        },
    }


def rows_to_sheet_matrix(*, rows: list[dict[str, Any]], columns: list[str]) -> list[list[Any]]:
    out: list[list[Any]] = []
    for row in rows:
        out.append(_map_row_to_headers(row=row, columns=columns))
    return out


def _map_row_to_headers(*, row: dict[str, Any], columns: list[str]) -> list[Any]:
    values: list[Any] = []
    occurrences: Counter[str] = Counter()
    for col in columns:
        key = " ".join(str(col or "").split()).strip()
        occurrences[key] += 1
        occ = occurrences[key]
        values.append(_value_for_header(row=row, header=key, occurrence=occ))
    return values


def _value_for_header(*, row: dict[str, Any], header: str, occurrence: int) -> Any:
    # Backward-compatible support for old sheets with duplicated "Комментарий по этапу".
    if header == "Здоровается":
        if occurrence == 1:
            return row.get("Здоровается (секретарь)", "")
        return row.get("Здоровается (лпр)", "") or row.get("Здоровается (секретарь)", "")
    if header == "Знакомится":
        if occurrence == 1:
            return row.get("Знакомится (секретарь)", "")
        return row.get("Знакомится (лпр)", "") or row.get("Знакомится (секретарь)", "")
    if header == "Комментарий по этапу":
        comment_map = {
            1: "Комментарий по этапу (секретарь)",
            2: "Комментарий по этапу (лпр)",
            3: "Комментарий по этапу (актуальность)",
            4: "Комментарий по этапу (презентация встречи)",
            5: "Комментарий по этапу (закрытие на встречу)",
            6: "Комментарий по этапу (возражения)",
            7: "Комментарий по этапу (чистота речи)",
            8: "Комментарий по этапу (crm)",
            9: "Комментарий по этапу (дисциплина дозвонов)",
            10: "Комментарий по этапу (демо)",
            11: "Комментарий по этапу (тест)",
            12: "Комментарий по этапу (дожим)",
        }
        return row.get(comment_map.get(occurrence, ""), "")

    alias_map = {
        "Итог / вывод": "Итог по кейсу",
        "Почему важно": "Почему это важно",
        "Сильные стороны": "Сильная сторона",
        "Зоны роста": "Зона роста",
        "Ключевой вывод": "Ключевой вывод",
        "Эффект количество": "Эффект количество / неделя",
        "Эффект количество / неделя": "Эффект количество / неделя",
        "Эффект качество": "Эффект качество",
        "Что донес сотруднику": "Что донести сотруднику",
        "Что донести сотруднику": "Что донести сотруднику",
        "Комментарий по этапу (демо/тест/дожим)": "Комментарий по этапу (демо)",
    }
    if header in row:
        return row.get(header, "")
    src = alias_map.get(header)
    if src:
        return row.get(src, "")
    return ""


def _build_case_row(
    *,
    analysis_date: str,
    candidate: dict[str, Any],
    deal: dict[str, Any],
    case_mode: str,
    primary_source: str,
    base_domain: str,
    manager_role_registry: dict[str, str] | None,
) -> dict[str, Any]:
    manager = _text(deal.get("owner_name"))
    role_internal = _resolve_manager_role(manager_name=manager, registry=manager_role_registry)
    role = _display_role(role_internal)
    deal_id = _text(candidate.get("deal_id") or deal.get("deal_id"))
    deal_name = _text(deal.get("deal_name"))
    company_name = _text(deal.get("company_name"))
    llm_payload = deal.get("call_review_llm_fields") if isinstance(deal.get("call_review_llm_fields"), dict) else {}
    llm_ready = bool(deal.get("call_review_llm_ready")) and bool(llm_payload)
    analysis_date_value = _safe_date(deal.get("call_anchor_date")) or _safe_date(deal.get("call_anchor_timestamp")) or analysis_date
    case_date = (
        _safe_date(deal.get("call_anchor_date"))
        or _safe_date(deal.get("call_anchor_timestamp"))
        or _safe_date(deal.get("updated_at"))
        or _safe_date(deal.get("created_at"))
        or ""
    )
    case_type_display = CASE_DISPLAY_MAP.get(case_mode, "переговорный кейс")

    transcript_score = int(
        candidate.get("transcript_usability_score_final", 0)
        or deal.get("transcript_usability_score_final", 0)
        or 0
    )
    transcript_label = _text(
        candidate.get("transcript_usability_label") or deal.get("transcript_usability_label")
    ).lower()
    call_summary = _text(deal.get("call_signal_summary_short"))
    transcript_excerpt = _text(deal.get("transcript_text_excerpt"))
    combined_text = _combined_case_text(deal=deal, call_summary=call_summary, transcript_excerpt=transcript_excerpt)
    has_next_step = bool(deal.get("call_signal_next_step_present"))
    has_demo = bool(deal.get("call_signal_demo_discussed"))
    has_test = bool(deal.get("call_signal_test_discussed"))
    has_objection = any(
        bool(deal.get(k))
        for k in (
            "call_signal_objection_price",
            "call_signal_objection_no_need",
            "call_signal_objection_not_target",
        )
    )
    redial_flag = bool(deal.get("repeated_dead_redial_day_flag")) or int(deal.get("repeated_dead_redial_count", 0) or 0) > 0
    not_covered = bool(deal.get("numbers_not_fully_covered_flag"))
    same_time = bool(deal.get("same_time_redial_pattern_flag"))
    over_limit = int(deal.get("dial_over_limit_numbers_count", 0) or 0) > 0
    has_secretary_signal = _has_any_token(combined_text, SECRETARY_TOKENS)
    has_lpr_signal = bool(deal.get("call_signal_decision_maker_reached")) or _has_any_token(combined_text, LPR_TOKENS)
    has_supplier_signal = _has_any_token(combined_text, SUPPLIER_TOKENS)
    has_warm_signal = has_demo or has_test or _has_any_token(combined_text, WARM_TOKENS)
    conversation_mode = case_mode in {
        "negotiation_lpr_analysis",
        "supplier_inbound_analysis",
        "warm_inbound_analysis",
        "secretary_analysis",
    }

    sec_comment = ""
    lpr_comment = ""

    open_q = _mark("?" in combined_text or "как " in combined_text or "почему" in combined_text)
    clarify_q = _mark("уточн" in combined_text or "детал" in combined_text)
    problem_q = _mark(
        "боль" in combined_text or "проблем" in combined_text or "узк" in combined_text or "актуальн" in combined_text
    )
    forming_q = _mark("если" in combined_text and "то" in combined_text)
    non_empty_summary = bool(call_summary or transcript_excerpt)
    crm_mismatch = bool(deal.get("crm_vs_call_mismatch"))
    risk_flags = deal.get("risk_flags") if isinstance(deal.get("risk_flags"), list) else []
    followup_gap_flag = any("follow-up" in _text(flag).lower() for flag in risk_flags)
    same_day_repeat = bool(deal.get("same_day_repeat_attempts_flag"))
    different_days_same_time = bool(deal.get("different_days_same_time_flag"))
    different_days_different_time = bool(deal.get("different_days_different_time_flag"))

    base_mix = _resolve_base_mix_for_record(deal)
    product_focus = _resolve_product_focus(deal)
    key_takeaway = _compose_key_takeaway(
        case_mode=case_mode,
        deal=deal,
        has_next_step=has_next_step,
        has_lpr_signal=has_lpr_signal,
        has_secretary_signal=has_secretary_signal,
        has_supplier_signal=has_supplier_signal,
    )
    strong_side = _compose_strong_side(
        case_mode=case_mode,
        deal=deal,
        transcript_label=transcript_label,
        has_lpr_signal=has_lpr_signal,
        has_secretary_signal=has_secretary_signal,
        has_next_step=has_next_step,
        has_objection=has_objection,
    )
    growth_zone = _compose_growth_zone(
        case_mode=case_mode,
        deal=deal,
        transcript_score=transcript_score,
        has_lpr_signal=has_lpr_signal,
        has_next_step=has_next_step,
        has_secretary_signal=has_secretary_signal,
    )
    fix_action = _compose_fix_action(
        case_mode=case_mode,
        deal=deal,
        has_next_step=has_next_step,
        has_lpr_signal=has_lpr_signal,
        has_secretary_signal=has_secretary_signal,
    )
    reinforce = _compose_reinforce(
        case_mode=case_mode,
        deal=deal,
        has_lpr_signal=has_lpr_signal,
        has_secretary_signal=has_secretary_signal,
    )
    why_important = _compose_why_important(case_mode=case_mode, role=role)
    coaching = _compose_coaching(case_mode=case_mode, deal=deal, fix_action=fix_action, reinforce=reinforce)
    expected_quantity = _compose_expected_quantity(case_mode=case_mode, role=role)
    expected_quality = _compose_expected_quality(case_mode=case_mode, role=role)

    # Prefer per-case LLM columns when available; deterministic text is fallback-only.
    if llm_ready:
        key_takeaway = _text(llm_payload.get("key_takeaway")) or key_takeaway
        strong_side = _text(llm_payload.get("strong_sides")) or strong_side
        growth_zone = _text(llm_payload.get("growth_zones")) or growth_zone
        fix_action = _text(llm_payload.get("fix_action")) or fix_action
        reinforce = _text(llm_payload.get("reinforce")) or reinforce
        why_important = _text(llm_payload.get("why_important")) or why_important
        coaching = _text(llm_payload.get("coaching_list")) or coaching
        expected_quantity = _text(llm_payload.get("expected_quantity")) or expected_quantity
        expected_quality = _text(llm_payload.get("expected_quality")) or expected_quality

    sec_comment_llm = _text(llm_payload.get("stage_secretary_comment")) if llm_ready else ""
    lpr_comment_llm = _text(llm_payload.get("stage_lpr_comment")) if llm_ready else ""
    need_comment_llm = _text(llm_payload.get("stage_need_comment")) if llm_ready else ""
    presentation_comment_llm = _text(llm_payload.get("stage_presentation_comment")) if llm_ready else ""
    closing_comment_llm = _text(llm_payload.get("stage_closing_comment")) if llm_ready else ""
    objections_comment_llm = _text(llm_payload.get("stage_objections_comment")) if llm_ready else ""
    speech_comment_llm = _text(llm_payload.get("stage_speech_comment")) if llm_ready else ""
    crm_comment_llm = _text(llm_payload.get("stage_crm_comment")) if llm_ready else ""
    discipline_comment_llm = _text(llm_payload.get("stage_discipline_comment")) if llm_ready else ""
    demo_comment_llm = _text(llm_payload.get("stage_demo_comment")) if llm_ready else ""
    demo_intro_comment_llm = _text(llm_payload.get("stage_demo_intro_comment")) if llm_ready else ""
    demo_context_comment_llm = _text(llm_payload.get("stage_demo_context_comment")) if llm_ready else ""
    demo_relevant_comment_llm = _text(llm_payload.get("stage_demo_relevant_comment")) if llm_ready else ""
    demo_process_comment_llm = _text(llm_payload.get("stage_demo_process_comment")) if llm_ready else ""
    demo_objections_comment_llm = _text(llm_payload.get("stage_demo_objections_comment")) if llm_ready else ""
    demo_next_step_comment_llm = _text(llm_payload.get("stage_demo_next_step_comment")) if llm_ready else ""
    test_launch_comment_llm = _text(llm_payload.get("stage_test_launch_comment")) if llm_ready else ""
    test_criteria_comment_llm = _text(llm_payload.get("stage_test_criteria_comment")) if llm_ready else ""
    test_owners_comment_llm = _text(llm_payload.get("stage_test_owners_comment")) if llm_ready else ""
    test_support_comment_llm = _text(llm_payload.get("stage_test_support_comment")) if llm_ready else ""
    test_feedback_comment_llm = _text(llm_payload.get("stage_test_feedback_comment")) if llm_ready else ""
    test_objections_comment_llm = _text(llm_payload.get("stage_test_objections_comment")) if llm_ready else ""
    test_comment_llm = _text(llm_payload.get("stage_test_comment")) if llm_ready else ""
    dozhim_recontact_comment_llm = _text(llm_payload.get("stage_dozhim_recontact_comment")) if llm_ready else ""
    dozhim_doubts_comment_llm = _text(llm_payload.get("stage_dozhim_doubts_comment")) if llm_ready else ""
    dozhim_terms_comment_llm = _text(llm_payload.get("stage_dozhim_terms_comment")) if llm_ready else ""
    dozhim_decision_comment_llm = _text(llm_payload.get("stage_dozhim_decision_comment")) if llm_ready else ""
    dozhim_flow_comment_llm = _text(llm_payload.get("stage_dozhim_flow_comment")) if llm_ready else ""
    dozhim_comment_llm = _text(llm_payload.get("stage_dozhim_comment")) if llm_ready else ""
    evidence_quote_llm = _text(llm_payload.get("evidence_quote")) if llm_ready else ""

    def _sheet_mark(applicable: bool, positive: bool, evidence_present: bool = False) -> str:
        if not applicable:
            return ""
        if positive:
            return "да"
        if evidence_present:
            return "нет"
        return ""

    is_negotiation_case = case_mode in {
        "negotiation_lpr_analysis",
        "supplier_inbound_analysis",
        "warm_inbound_analysis",
        "secretary_analysis",
    }
    is_presentation_case = case_mode == "presentation_analysis" or (has_demo and case_mode in {"warm_inbound_analysis", "supplier_inbound_analysis"})
    is_test_case = case_mode == "test_analysis" or has_test
    is_dozhim_case = case_mode == "dozhim_analysis" or _has_any_token(combined_text, ("кп", "счет", "коммерческ", "договор", "услов"))
    has_demo_evidence = has_demo or bool(demo_intro_comment_llm or demo_context_comment_llm or demo_relevant_comment_llm)
    has_test_evidence = has_test or bool(test_launch_comment_llm or test_criteria_comment_llm or test_feedback_comment_llm)
    has_dozhim_evidence = bool(dozhim_recontact_comment_llm or dozhim_doubts_comment_llm or dozhim_terms_comment_llm or dozhim_decision_comment_llm)

    demo_stage_comment = _text(
        "; ".join(
            x
            for x in (
                demo_intro_comment_llm,
                demo_context_comment_llm,
                demo_relevant_comment_llm,
                demo_process_comment_llm,
                demo_objections_comment_llm,
                demo_next_step_comment_llm,
                demo_comment_llm,
            )
            if _text(x)
        )
    )
    test_stage_comment = _text(
        "; ".join(
            x
            for x in (
                test_launch_comment_llm,
                test_criteria_comment_llm,
                test_owners_comment_llm,
                test_support_comment_llm,
                test_feedback_comment_llm,
                test_objections_comment_llm,
                test_comment_llm,
            )
            if _text(x)
        )
    )
    dozhim_stage_comment = _text(
        "; ".join(
            x
            for x in (
                dozhim_recontact_comment_llm,
                dozhim_doubts_comment_llm,
                dozhim_terms_comment_llm,
                dozhim_decision_comment_llm,
                dozhim_flow_comment_llm,
                dozhim_comment_llm,
            )
            if _text(x)
        )
    )

    result = {
        "Дата анализа": _safe_date(deal.get("business_window_date")) or analysis_date_value,
        "Дата кейса": case_date,
        "Менеджер": manager,
        "Роль": role,
        "Deal ID": deal_id,
        "Ссылка на сделку": _deal_url(base_domain=base_domain, deal_id=deal_id),
        "Сделка": deal_name,
        "Компания": company_name,
        "Продукт / фокус": product_focus,
        "База / тег": base_mix,
        "Тип кейса": case_type_display,
        "Прослушанные звонки": _listened_calls_text(candidate=candidate, deal=deal),
        "Проход секретаря": _sheet_mark(case_mode == "secretary_analysis", has_secretary_signal, non_empty_summary),
        "Контакт с ЛПР": _sheet_mark(
            case_mode in {"negotiation_lpr_analysis", "supplier_inbound_analysis", "warm_inbound_analysis"},
            has_lpr_signal,
            non_empty_summary,
        ),
        "Актуальность и потребность": _sheet_mark(is_negotiation_case, problem_q == "да" or open_q == "да", non_empty_summary),
        "Презентация встречи": _sheet_mark(is_negotiation_case, has_demo or has_next_step, non_empty_summary),
        "Закрытие на встречу": _sheet_mark(is_negotiation_case, has_next_step, non_empty_summary),
        "Отработка возражений": _sheet_mark(is_negotiation_case, has_objection, has_objection),
        "Чистота речи": _sheet_mark(is_negotiation_case, transcript_label == "usable" and transcript_score >= 2, transcript_score >= 1),
        "Работа с CRM": _sheet_mark(True, not crm_mismatch, True),
        "Дисциплина дозвонов": _sheet_mark(
            case_mode == "redial_discipline_analysis",
            (not redial_flag and not over_limit and not not_covered),
            redial_flag or over_limit or not_covered,
        ),
        "Вход и рамка демо": _sheet_mark(is_presentation_case, has_demo_evidence, has_demo_evidence),
        "Выявление контекста перед показом": _sheet_mark(is_presentation_case, bool(demo_context_comment_llm), has_demo_evidence),
        "Показ релевантного сценария": _sheet_mark(is_presentation_case, bool(demo_relevant_comment_llm), has_demo_evidence),
        "Привязка к процессу клиента": _sheet_mark(is_presentation_case, bool(demo_process_comment_llm), has_demo_evidence),
        "Работа с вопросами и возражениями на демо": _sheet_mark(
            is_presentation_case,
            bool(demo_objections_comment_llm or has_objection),
            has_demo_evidence,
        ),
        "Фиксация следующего шага после демо": _sheet_mark(is_presentation_case, bool(demo_next_step_comment_llm or has_next_step), has_demo_evidence),
        "Комментарий по этапу (демо)": demo_stage_comment,
        "Запуск теста": _sheet_mark(is_test_case, bool(test_launch_comment_llm or has_test_evidence), has_test_evidence),
        "Критерии успеха теста": _sheet_mark(is_test_case, bool(test_criteria_comment_llm), has_test_evidence),
        "Ответственные и сроки по тесту": _sheet_mark(is_test_case, bool(test_owners_comment_llm), has_test_evidence),
        "Сопровождение теста": _sheet_mark(is_test_case, bool(test_support_comment_llm), has_test_evidence),
        "Сбор обратной связи по тесту": _sheet_mark(is_test_case, bool(test_feedback_comment_llm), has_test_evidence),
        "Снятие возражений после теста": _sheet_mark(is_test_case, bool(test_objections_comment_llm), has_test_evidence),
        "Комментарий по этапу (тест)": test_stage_comment,
        "Повторный контакт по дожиму": _sheet_mark(is_dozhim_case, bool(dozhim_recontact_comment_llm or has_dozhim_evidence), has_dozhim_evidence),
        "Работа с сомнениями и стоп-факторами": _sheet_mark(is_dozhim_case, bool(dozhim_doubts_comment_llm), has_dozhim_evidence),
        "Согласование условий / КП": _sheet_mark(is_dozhim_case, bool(dozhim_terms_comment_llm), has_dozhim_evidence),
        "Фиксация решения": _sheet_mark(is_dozhim_case, bool(dozhim_decision_comment_llm), has_dozhim_evidence),
        "Дожим не провисает": _sheet_mark(is_dozhim_case, bool(dozhim_flow_comment_llm or has_next_step), has_dozhim_evidence),
        "Комментарий по этапу (дожим)": dozhim_stage_comment,
        "Ключевой вывод": key_takeaway,
        "Сильная сторона": strong_side,
        "Зона роста": growth_zone,
        "Почему это важно": why_important,
        "Что закрепить": reinforce,
        "Что исправить": fix_action,
        "Что донести сотруднику": coaching,
        "Эффект количество / неделя": expected_quantity,
        "Эффект качество": expected_quality,
        "Оценка 0-100": deal.get("score") or "",
        "Критичность": _criticality_from_score(deal.get("score")),
        # Compatibility fields for old headers and debug access.
        "Итог по кейсу": key_takeaway,
        "Сильные стороны": strong_side,
        "Зоны роста": growth_zone,
        "Эффект количество": expected_quantity,
        "Что донес сотруднику": coaching,
        "Комментарий по этапу (секретарь)": sec_comment_llm,
        "Комментарий по этапу (лпр)": lpr_comment_llm,
        "Комментарий по этапу (актуальность)": need_comment_llm,
        "Комментарий по этапу (презентация встречи)": presentation_comment_llm,
        "Комментарий по этапу (закрытие на встречу)": closing_comment_llm,
        "Комментарий по этапу (возражения)": objections_comment_llm,
        "Комментарий по этапу (чистота речи)": speech_comment_llm,
        "Комментарий по этапу (crm)": crm_comment_llm,
        "Комментарий по этапу (дисциплина дозвонов)": discipline_comment_llm,
        "Комментарий по этапу (демо/тест/дожим)": demo_comment_llm,
        "Здоровается (секретарь)": sec_comment_llm or _sheet_mark(case_mode == "secretary_analysis", has_secretary_signal, non_empty_summary),
        "Здоровается (лпр)": lpr_comment_llm or _sheet_mark(
            case_mode in {"negotiation_lpr_analysis", "supplier_inbound_analysis", "warm_inbound_analysis"},
            has_lpr_signal,
            non_empty_summary,
        ),
        "Знакомится (секретарь)": sec_comment_llm,
        "Знакомится (лпр)": lpr_comment_llm,
        "Доказательства / цитаты": evidence_quote_llm or transcript_excerpt or call_summary,
        "__case_mode_internal": case_mode,
        "__daily_primary_source_internal": primary_source,
        "__selection_reason": _text(candidate.get("shortlist_reason") or ""),
        "__selection_rank_group": int(candidate.get("rank_group", 0) or 0),
        "__selected_for_transcription": bool(candidate.get("selected_for_transcription")),
        "__llm_text_ready": llm_ready,
        "__llm_source": _text(deal.get("call_review_llm_source")),
        "__llm_generation_error": _text(deal.get("call_review_llm_error")),
    }

    # keep both singular/plural headers where some sheets differ
    result.setdefault("Сильная сторона", strong_side)
    result.setdefault("Зона роста", growth_zone)
    result.setdefault("Почему важно", why_important)
    result.setdefault("Эффект количество", expected_quantity)
    result.setdefault("Что донес сотруднику", coaching)

    return result


def _manager_allowed(*, manager_name: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    low = _text(manager_name).lower()
    if not low:
        return False
    for item in allowlist:
        token = _text(item).lower()
        if not token:
            continue
        if token in low or low in token:
            return True
    return False


def _resolve_manager_role(*, manager_name: str, registry: dict[str, str] | None) -> str:
    low = _text(manager_name).lower()
    reg = dict(registry or {})
    if reg:
        for key, role in reg.items():
            token = _text(key).lower()
            if token and (token in low or low == token):
                r = _text(role).lower()
                if r in {"telemarketer", "sales_manager"}:
                    return r
    if "рустам" in low:
        return "telemarketer"
    if "илья" in low:
        return "sales_manager"
    return "sales_manager"


def _derive_case_mode(*, candidate: dict[str, Any], deal: dict[str, Any]) -> str:
    case_type = _text(candidate.get("call_case_type")).lower()
    pool = _text(candidate.get("pool_type")).lower()
    transcript_score = int(
        candidate.get("transcript_usability_score_final", 0)
        or deal.get("transcript_usability_score_final", 0)
        or 0
    )
    combined_text = _combined_case_text(
        deal=deal,
        call_summary=_text(deal.get("call_signal_summary_short")),
        transcript_excerpt=_text(deal.get("transcript_text_excerpt")),
    )
    has_secretary_signal = _has_any_token(combined_text, SECRETARY_TOKENS)
    has_lpr_signal = bool(deal.get("call_signal_decision_maker_reached")) or _has_any_token(combined_text, LPR_TOKENS)
    has_supplier_signal = _has_any_token(combined_text, SUPPLIER_TOKENS)
    has_warm_signal = bool(deal.get("call_signal_demo_discussed")) or bool(deal.get("call_signal_test_discussed")) or _has_any_token(combined_text, WARM_TOKENS)
    has_presentation_signal = bool(deal.get("call_signal_demo_discussed")) or _has_any_token(
        combined_text, ("презентац", "демо", "показ", "экран")
    )
    has_test_signal = bool(deal.get("call_signal_test_discussed")) or _has_any_token(
        combined_text, ("тест", "пилот", "критер", "обратн")
    )
    has_dozhim_signal = _has_any_token(
        combined_text, ("дожим", "кп", "коммерческ", "счет", "услов", "договор", "решени")
    )
    has_discipline_pattern = any(
        (
            bool(deal.get("repeated_dead_redial_day_flag")),
            bool(deal.get("same_time_redial_pattern_flag")),
            bool(deal.get("numbers_not_fully_covered_flag")),
            int(deal.get("repeated_dead_redial_count", 0) or 0) > 0,
            int(deal.get("dial_over_limit_numbers_count", 0) or 0) > 0,
        )
    )

    if case_type == "lpr_conversation" and (has_lpr_signal or transcript_score >= 2):
        return "negotiation_lpr_analysis"
    if case_type == "secretary_case":
        return "secretary_analysis"
    if case_type in {"supplier_inbound", "supplier_case"}:
        return "supplier_inbound_analysis"
    if case_type in {"warm_inbound", "warm_case"}:
        return "warm_inbound_analysis"
    if case_type in {"presentation", "demo"}:
        return "presentation_analysis"
    if case_type in {"test", "pilot"}:
        return "test_analysis"
    if case_type in {"dozhim", "closing"}:
        return "dozhim_analysis"
    if case_type in {"redial_discipline", "discipline_case"}:
        if has_secretary_signal and transcript_score >= 1:
            return "secretary_analysis"
        if has_lpr_signal and transcript_score >= 2:
            return "negotiation_lpr_analysis"
        return "redial_discipline_analysis"
    if case_type == "autoanswer_noise":
        return "skip_no_meaningful_case"

    if pool == "discipline_pool":
        if has_secretary_signal and transcript_score >= 1:
            return "secretary_analysis"
        if has_lpr_signal and transcript_score >= 2:
            return "negotiation_lpr_analysis"
        return "redial_discipline_analysis"

    if has_supplier_signal and transcript_score >= 1:
        return "supplier_inbound_analysis"
    if has_presentation_signal and transcript_score >= 1:
        return "presentation_analysis"
    if has_test_signal and transcript_score >= 1:
        return "test_analysis"
    if has_dozhim_signal and transcript_score >= 1:
        return "dozhim_analysis"
    if has_warm_signal and transcript_score >= 1:
        return "warm_inbound_analysis"
    if has_secretary_signal and not has_lpr_signal and transcript_score >= 1:
        return "secretary_analysis"
    if has_lpr_signal and transcript_score >= 1:
        return "negotiation_lpr_analysis"
    if bool(deal.get("call_signal_next_step_present")) and transcript_score >= 1:
        return "warm_inbound_analysis"
    if has_discipline_pattern:
        return "redial_discipline_analysis"

    return "skip_no_meaningful_case"


def _derive_primary_source(*, case_mode: str) -> str:
    return "discipline_pool" if case_mode == "redial_discipline_analysis" else "conversation_pool"


def _skip_reason_for_case(*, case_mode: str, candidate: dict[str, Any], deal: dict[str, Any]) -> str:
    if case_mode == "skip_no_meaningful_case":
        return "skip_no_meaningful_case"

    transcript_score = int(
        candidate.get("transcript_usability_score_final", 0)
        or deal.get("transcript_usability_score_final", 0)
        or 0
    )
    transcript_label = _text(candidate.get("transcript_usability_label") or deal.get("transcript_usability_label")).lower()
    call_summary = _text(deal.get("call_signal_summary_short"))
    transcript_excerpt = _text(deal.get("transcript_text_excerpt"))
    combined_text = _combined_case_text(deal=deal, call_summary=call_summary, transcript_excerpt=transcript_excerpt)
    autoanswer = _text(candidate.get("call_case_type")).lower() == "autoanswer_noise"
    has_next_step = bool(deal.get("call_signal_next_step_present"))
    has_lpr_signal = bool(deal.get("call_signal_decision_maker_reached")) or _has_any_token(combined_text, LPR_TOKENS)
    has_secretary_signal = _has_any_token(combined_text, SECRETARY_TOKENS)
    has_supplier_signal = _has_any_token(combined_text, SUPPLIER_TOKENS)
    has_warm_signal = bool(deal.get("call_signal_demo_discussed")) or bool(deal.get("call_signal_test_discussed")) or _has_any_token(
        combined_text, WARM_TOKENS
    )
    forced_fallback = bool(candidate.get("forced_fallback"))
    has_any_meaningful_evidence = any(
        (
            transcript_score >= 1,
            bool(call_summary),
            has_next_step,
            has_lpr_signal,
            has_secretary_signal,
            has_supplier_signal,
            has_warm_signal,
        )
    )

    if forced_fallback and not has_any_meaningful_evidence:
        return "forced_fallback_without_meaningful_evidence"

    if case_mode == "redial_discipline_analysis":
        return "discipline_case_disabled_for_call_review_write"

    if str(candidate.get("pool_type") or "").strip().lower() == "discipline_pool":
        return "discipline_pool_not_allowed_for_call_review_write"

    if not bool(deal.get("call_review_llm_ready")):
        return "llm_not_ready_for_call_review_row"

    if autoanswer and transcript_score < 2:
        return "autoanswer_noise_not_meaningful"
    if transcript_label in {"empty", "noisy"} and transcript_score < 2:
        return "noisy_or_empty_transcript_not_allowed_for_write"
    if transcript_label == "weak" and transcript_score < 2 and not call_summary:
        return "weak_or_empty_conversation_evidence"
    if case_mode == "secretary_analysis":
        if transcript_score < 1 and not has_secretary_signal:
            return "secretary_without_meaningful_signal"
        return ""
    if case_mode in {
        "negotiation_lpr_analysis",
        "supplier_inbound_analysis",
        "warm_inbound_analysis",
        "presentation_analysis",
        "test_analysis",
        "dozhim_analysis",
    }:
        if transcript_score < 1 and not has_next_step and not has_lpr_signal and not has_supplier_signal and not has_warm_signal:
            return "conversation_without_minimum_signal"
    if transcript_score <= 0 and not call_summary and not has_lpr_signal and not has_secretary_signal and not has_supplier_signal:
        return "conversation_case_without_meaningful_evidence"

    return ""


def _combined_case_text(*, deal: dict[str, Any], call_summary: str, transcript_excerpt: str) -> str:
    parts = [
        call_summary,
        transcript_excerpt,
        _text(deal.get("manager_insight_short")),
        _text(deal.get("manager_summary")),
        _text(deal.get("deal_name")),
        _text(deal.get("source_name")),
        _text(deal.get("source_url")),
    ]
    return " ".join(p for p in parts if p).lower()


def _has_any_token(text: str, tokens: tuple[str, ...]) -> bool:
    low = str(text or "").lower()
    return any(token in low for token in tokens)


def _display_role(value: str) -> str:
    low = _text(value).lower()
    return ROLE_DISPLAY_MAP.get(low, "менеджер по продажам")


def _stage_mark(applicable: bool, *, positive: bool) -> str:
    if not applicable:
        return "н/п"
    return "да" if positive else "нет"


def _mark(flag: bool) -> str:
    return "да" if flag else "нет"


def _discipline_comment(deal: dict[str, Any]) -> str:
    parts: list[str] = []
    if bool(deal.get("numbers_not_fully_covered_flag")):
        parts.append("не покрыты все номера")
    if bool(deal.get("same_time_redial_pattern_flag")):
        parts.append("повторы в одно и то же время")
    if int(deal.get("repeated_dead_redial_count", 0) or 0) > 0:
        parts.append("повторы пустых дозвонов")
    if int(deal.get("dial_over_limit_numbers_count", 0) or 0) > 0:
        parts.append("больше 2 попыток на номер")
    return "; ".join(parts)


def _listened_calls_text(*, candidate: dict[str, Any], deal: dict[str, Any]) -> str:
    selected = int(candidate.get("selected_call_count", 0) or deal.get("selected_call_count", 0) or 0)
    if selected > 0:
        return str(selected)
    calls_total = int(deal.get("call_candidates_count", 0) or 0)
    if calls_total > 0:
        return str(min(3, calls_total))
    return "1"


def _deal_url(*, base_domain: str, deal_id: str) -> str:
    did = _text(deal_id)
    if not did:
        return ""
    domain = _text(base_domain).rstrip("/")
    if domain:
        return f"{domain}/leads/detail/{did}"
    return did


def _safe_date(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    for marker in ("T", " "):
        if marker in text:
            text = text.split(marker, 1)[0]
            break
    try:
        datetime.fromisoformat(text)
        return text
    except Exception:
        return ""


def _text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _count_values(rows: list[dict[str, Any]], *, key: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        if not isinstance(row, dict):
            continue
        label = _text(row.get(key))
        if label:
            counter[label] += 1
    return dict(counter)


def _resolve_product_focus(record: dict[str, Any]) -> str:
    hyp = _text(record.get("product_hypothesis")).lower()
    if hyp in {"info", "link", "mixed", "unknown"}:
        return {
            "info": "инфо",
            "link": "линк",
            "mixed": "оба",
            "unknown": "до продукта разговор не дошел",
        }[hyp]
    name = _text(record.get("product_name")).lower()
    if "линк" in name or "link" in name:
        return "линк"
    if "инфо" in name or "info" in name:
        return "инфо"
    return "до продукта разговор не дошел"


def _resolve_base_mix_for_record(record: dict[str, Any]) -> str:
    if _text(record.get("base_mix_selected_value")):
        return _text(record.get("base_mix_selected_value"))
    resolved = resolve_base_mix([record])
    return _text(resolved.get("selected_value")) or "солянка"


def _compose_key_takeaway(
    *,
    case_mode: str,
    deal: dict[str, Any],
    has_next_step: bool,
    has_lpr_signal: bool,
    has_secretary_signal: bool,
    has_supplier_signal: bool,
) -> str:
    if case_mode == "redial_discipline_analysis":
        summary = _discipline_comment(deal)
        return summary or "День ушел в пустые наборы, нужно перестроить дисциплину дозвонов."

    if case_mode == "secretary_analysis":
        if has_secretary_signal:
            return "Контакт через секретаря состоялся, дальше нужно дожать понятный маршрут к нужной роли."
        return "Кейс про проход секретаря: фиксируем маршрут и переводим в предметный контакт."

    if case_mode == "supplier_inbound_analysis":
        return "Входящий поставщикский контур есть, важно быстро довести до конкретного шага и срока."

    if case_mode == "warm_inbound_analysis":
        return "Теплый интерес есть, сейчас ключевая задача - зафиксировать следующий шаг без провисания."

    call_summary = _text(deal.get("call_signal_summary_short"))
    insight = _text(deal.get("manager_insight_short"))
    for candidate in (call_summary, insight):
        if candidate:
            return candidate[:320].replace("follow-up", "следующий шаг")
    if has_lpr_signal and has_next_step:
        return "Есть предметный разговор с выходом в следующий шаг. Теперь важно не потерять темп."
    if has_supplier_signal:
        return "Есть входящий интерес по поставщикам, нужно быстро переводить в рабочий шаг."
    return "Есть рабочий кейс, закрепляем следующий шаг и нормальную фиксацию сути разговора."


def _compose_strong_side(
    *,
    case_mode: str,
    deal: dict[str, Any],
    transcript_label: str,
    has_lpr_signal: bool,
    has_secretary_signal: bool,
    has_next_step: bool,
    has_objection: bool,
) -> str:
    parts: list[str] = []
    if case_mode == "secretary_analysis":
        if has_secretary_signal:
            parts.append("аккуратно прошел секретаря и удержал разговор по маршруту")
        if has_next_step:
            parts.append("зафиксировал, куда и когда возвращаемся")
        return "; ".join(parts[:2])
    if case_mode == "redial_discipline_analysis":
        return ""
    if has_lpr_signal:
        parts.append("дошел до нужной роли и удержал предметный разговор")
    if has_next_step:
        parts.append("закрыл разговор в конкретный следующий шаг")
    if has_objection:
        parts.append("не слился на возражении и продолжил диалог")
    if transcript_label == "usable":
        parts.append("речь собранная, без лишнего шума")
    if parts:
        return "; ".join(parts[:2])
    return ""


def _compose_growth_zone(
    *,
    case_mode: str,
    deal: dict[str, Any],
    transcript_score: int,
    has_lpr_signal: bool,
    has_next_step: bool,
    has_secretary_signal: bool,
) -> str:
    if case_mode == "redial_discipline_analysis":
        return _discipline_comment(deal) or "Убрать повторы пустых дозвонов и закрыть покрытие всех номеров."
    if case_mode == "secretary_analysis":
        if not has_secretary_signal:
            return "Четче заходить через инфоповод и просить маршрут на нужную роль."
        if not has_next_step:
            return "После прохода секретаря сразу фиксировать следующий шаг и срок."
        return "Уточнять результат маршрутизации: кому отправили и когда возвращаемся."
    if transcript_score < 2:
        return "Переслушать звонок и выровнять структуру разговора перед следующим касанием."
    if not has_lpr_signal:
        return "Раньше проверять, кто перед нами, и доводить до контакта с решающим."
    if not has_next_step:
        return "В конце разговора всегда фиксировать конкретный следующий шаг с датой."
    return "Глубже раскрывать актуальность клиента, чтобы следующий шаг был устойчивым."


def _compose_fix_action(
    *,
    case_mode: str,
    deal: dict[str, Any],
    has_next_step: bool,
    has_lpr_signal: bool,
    has_secretary_signal: bool,
) -> str:
    if case_mode == "redial_discipline_analysis":
        return "Сменить окно дозвона, ограничить 2 попытки на номер и закрыть покрытие всех номеров."
    if case_mode == "secretary_analysis":
        if not has_secretary_signal:
            return "Скрипт захода через секретаря: инфоповод + прямой запрос маршрута."
        if not has_next_step:
            return "После секретаря сразу ставить следующий шаг с датой и ответственным."
        return "Попросить прямой контакт нужной роли и зафиксировать точку возврата."
    if not has_lpr_signal:
        return "На старте разговора подтверждать роль собеседника и не идти дальше вслепую."
    if not has_next_step:
        return "Финал каждого разговора - конкретный следующий шаг, дата и кто на связи."
    return "Дожать фиксацию результата разговора в одной понятной записи без воды."


def _compose_reinforce(
    *,
    case_mode: str,
    deal: dict[str, Any],
    has_lpr_signal: bool,
    has_secretary_signal: bool,
) -> str:
    coaching_hint = _text(deal.get("coaching_hint_short"))
    if coaching_hint:
        return coaching_hint
    if case_mode == "secretary_analysis" or has_secretary_signal:
        return "Модуль захода через инфоповод и четкий запрос маршрута на нужную роль."
    if case_mode == "redial_discipline_analysis":
        return "Правило дозвона: не больше двух попыток на номер + смена времени в разные дни."
    if has_lpr_signal:
        return "Модуль закрытия на следующий шаг: что делаем, когда и кто подтверждает."
    return "Модуль короткой квалификации и фиксации шага без лишнего давления."


def _compose_why_important(*, case_mode: str, role: str) -> str:
    if case_mode == "redial_discipline_analysis":
        return "Меньше пустых наборов, больше живых контактов. Для сотрудника это прямой прирост рабочих диалогов, для отдела - чище верх воронки."
    if role == "телемаркетолог":
        return "Четкий следующий шаг после разговора дает сотруднику больше выходов на нужную роль и больше назначенных встреч."
    return "Фиксация шага и результата после разговора дает сотруднику больше управляемых сделок и меньше зависаний по воронке."


def _compose_coaching(*, case_mode: str, deal: dict[str, Any], fix_action: str, reinforce: str) -> str:
    if case_mode == "redial_discipline_analysis":
        first = "Разобрали паттерн пустых перезвонов и где теряются попытки."
        second = reinforce or "Дали правило дозвона: не больше двух попыток на номер и смена времени."
        third = fix_action or "В следующих касаниях закрывает покрытие номеров и убирает повторы без результата."
        return f"1) {first}\n2) {second}\n3) {third}"

    if case_mode == "secretary_analysis":
        first = "Разобрали проход секретаря и формулировку инфоповода."
        second = reinforce or "Дали модуль маршрутизации на нужную роль."
        third = fix_action or "В следующих звонках фиксирует маршрут и конкретный шаг возврата."
        return f"1) {first}\n2) {second}\n3) {third}"

    first = "Разобрали этап разговора и где теряется следующий шаг."
    second = reinforce or "Дали рабочий модуль под текущий тип кейса."
    third = fix_action or "В следующих касаниях закрепляет шаг, срок и ответственного."
    return f"1) {first}\n2) {second}\n3) {third}"


def _compose_expected_quantity(*, case_mode: str, role: str) -> str:
    if case_mode == "redial_discipline_analysis":
        return "1-2 пустых перезвона меньше за неделю и +0.2-0.4 живого контакта."
    if case_mode == "secretary_analysis":
        return "+0.2-0.4 выхода на нужную роль в неделю за счет более четкого прохода секретаря."
    if case_mode == "supplier_inbound_analysis":
        return "+0.2-0.4 рабочего шага по входящим в неделю, без провисания после первого контакта."
    if case_mode == "warm_inbound_analysis":
        return "+0.2-0.5 подтвержденного следующего шага в неделю после теплых касаний."
    if role == "телемаркетолог":
        return "+0.2-0.4 встречи в неделю за счет более четкого закрытия на следующий шаг."
    return "+0.2-0.5 подтвержденной встречи в неделю за счет дожима после разговора."


def _compose_expected_quality(*, case_mode: str, role: str) -> str:
    if case_mode == "redial_discipline_analysis":
        return "Меньше шума на верхе воронки: дозвоны станут управляемыми, а база - чище."
    if case_mode == "secretary_analysis":
        return "Лучше качество верхнего этапа: меньше тупиков на секретаре и понятнее маршрут к нужной роли."
    if case_mode == "supplier_inbound_analysis":
        return "Входящие перестанут зависать после первого касания, шаг по сделке станет прозрачнее."
    if case_mode == "warm_inbound_analysis":
        return "Теплый этап станет стабильнее: после контакта будет чаще фиксироваться реальный следующий шаг."
    if role == "телемаркетолог":
        return "Выше качество верхнего этапа: лучше маршрутизация, понятнее следующий шаг, меньше потерь после первого контакта."
    return "Выше управляемость теплого этапа: после разговора чаще фиксируется шаг, меньше зависаний между этапами."


def _criticality_from_score(score: Any) -> str:
    try:
        val = int(score)
    except Exception:
        return "средняя"
    if val < 40:
        return "высокая"
    if val < 70:
        return "средняя"
    return "низкая"
