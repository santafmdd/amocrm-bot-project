from __future__ import annotations

import json
from typing import Any

from .config import DealAnalyzerConfig
from .models import DealAnalysis


REPAIR_JSON_INSTRUCTION = (
    "Верни только валидный JSON-объект без комментариев, markdown и пояснений. "
    "Без текста до/после JSON, без тройных кавычек."
)

HYBRID_SHORT_JSON_INSTRUCTION = (
    "Верни только валидный JSON-объект без markdown/комментариев/объяснений. "
    "Ровно поля: product_hypothesis_llm, loss_reason_short, manager_insight_short, coaching_hint_short, reanimation_reason_short_llm."
)

DAILY_TABLE_JSON_INSTRUCTION = (
    "Верни только валидный JSON-объект без markdown/комментариев/пояснений. "
    "Строго ключи: key_takeaway, strong_sides, growth_zones, why_important, reinforce, fix_action, "
    "coaching_list, expected_quantity, expected_quality."
)

DAILY_RERANK_JSON_INSTRUCTION = (
    "Верни только валидный JSON-объект без markdown/комментариев/пояснений. "
    "Строго формат: {\"ranked\":[{\"deal_id\":\"...\",\"rank\":1,\"reason\":\"...\",\"skip\":false,\"skip_reason\":\"...\","
    "\"call_analysis_viability\":\"high|medium|low\",\"call_analysis_viability_reason\":\"...\"}]}"
)


def build_manager_message_draft(analysis: DealAnalysis) -> str:
    deal_label = analysis.deal_name or f"Сделка {analysis.deal_id}"
    positives = "; ".join(analysis.strong_sides) if analysis.strong_sides else "сильные стороны не зафиксированы"
    risks = "; ".join(analysis.risk_flags) if analysis.risk_flags else "критичные риски не найдены"
    actions = (
        "; ".join(analysis.recommended_actions_for_manager)
        if analysis.recommended_actions_for_manager
        else "действия не указаны"
    )

    return (
        f"Коротко по {deal_label}: итоговый балл {analysis.score_0_100}/100. "
        f"Сильные стороны: {positives}. "
        f"Риски: {risks}. "
        f"Рекомендуемые шаги: {actions}."
    )


def build_employee_training_message_draft(analysis: DealAnalysis) -> str:
    deal_label = analysis.deal_name or f"Сделка {analysis.deal_id}"
    zones = "; ".join(analysis.growth_zones) if analysis.growth_zones else "критичных зон роста не выявлено"
    tasks = (
        "; ".join(analysis.recommended_training_tasks_for_employee)
        if analysis.recommended_training_tasks_for_employee
        else "задачи не назначены"
    )

    return f"Разбор по {deal_label}: зона роста - {zones}. Учебные задачи: {tasks}."


def build_ollama_chat_messages(*, normalized_deal: dict[str, Any], config: DealAnalyzerConfig) -> list[dict[str, str]]:
    system_prompt = _build_system_prompt(config.style_profile_name)
    user_prompt = _build_user_prompt(normalized_deal)
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def append_json_repair_instruction(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    out = list(messages)
    out.append({"role": "user", "content": REPAIR_JSON_INSTRUCTION})
    return out


def append_hybrid_json_repair_instruction(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    out = list(messages)
    out.append({"role": "user", "content": HYBRID_SHORT_JSON_INSTRUCTION})
    return out


def append_daily_table_json_repair_instruction(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    out = list(messages)
    out.append({"role": "user", "content": DAILY_TABLE_JSON_INSTRUCTION})
    return out


def append_daily_rerank_json_repair_instruction(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    out = list(messages)
    out.append({"role": "user", "content": DAILY_RERANK_JSON_INSTRUCTION})
    return out


def build_hybrid_short_messages(*, normalized_deal: dict[str, Any], config: DealAnalyzerConfig) -> list[dict[str, str]]:
    system_prompt = (
        "Ты помогаешь с коротким уточнением анализа сделки. "
        "Не выдумывай факты, используй только входные данные. "
        "Нужен строго JSON без markdown и текста вокруг. "
        "Разрешены только ключи: product_hypothesis_llm, loss_reason_short, manager_insight_short, coaching_hint_short, reanimation_reason_short_llm. "
        "product_hypothesis_llm: одно из info|link|mixed|unknown. "
        "Если данных мало/противоречиво, ставь unknown и коротко укажи ограничение в manager_insight_short. "
        "Каждое поле: короткая строка до 180 символов. "
        f"Профиль стиля: {config.style_profile_name}."
    )
    compact = _compact_payload_for_llm(normalized_deal)
    user_prompt = (
        "Сделка (compact payload):\n"
        f"{json.dumps(compact, ensure_ascii=False, indent=2)}\n\n"
        "Верни короткое уточнение по трем полям JSON."
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def build_daily_table_messages(
    *,
    factual_payload: dict[str, Any],
    config: DealAnalyzerConfig,
    style_source_excerpt: str,
) -> list[dict[str, str]]:
    case_policy = factual_payload.get("case_policy", {}) if isinstance(factual_payload.get("case_policy"), dict) else {}
    mode = str(case_policy.get("daily_analysis_mode") or "general").strip()
    allowed_axes = case_policy.get("allowed_axes", []) if isinstance(case_policy.get("allowed_axes"), list) else []
    banned_topics = case_policy.get("banned_topics", []) if isinstance(case_policy.get("banned_topics"), list) else []
    system_prompt = (
        "Ты формируешь текст ячеек для таблицы ежедневного управленческого контроля продаж. "
        "Пиши живым рабочим русским языком руководителя: коротко, по делу, без бюрократии. "
        "Используй только факты из входных данных, ничего не выдумывай. "
        "Можно выбирать подходящие инструменты продаж (вопросы, модуль следующего шага, квалификация), "
        "если это подтверждается фактурой звонков/CRM. "
        "Если переговорного материала мало, честно пиши про дисциплину набора->дозвона без выдумки. "
        "Верни только JSON-объект без markdown."
    )
    style_hint = style_source_excerpt.strip()
    user_prompt = (
        "Factual payload:\n"
        f"{json.dumps(factual_payload, ensure_ascii=False, indent=2)}\n\n"
        "Сгенерируй поля таблицы строго в JSON-ключах:\n"
        "- key_takeaway\n"
        "- strong_sides\n"
        "- growth_zones\n"
        "- why_important\n"
        "- reinforce\n"
        "- fix_action\n"
        "- coaching_list\n"
        "- expected_quantity\n"
        "- expected_quality\n\n"
        "Контракт формата:\n"
        "1) key_takeaway: 1 короткий абзац.\n"
        "2) strong_sides: только подтверждаемые сильные стороны; если нечего хвалить, пустая строка.\n"
        "3) growth_zones: максимум 2 пункта через '; '.\n"
        "4) why_important: сначала польза сотруднику, потом польза отделу.\n"
        "5) reinforce: конкретный прием/инструмент.\n"
        "6) fix_action: конкретный шаг без фразы 'на ближайший цикл'.\n"
        "7) coaching_list: только нумерованный формат '1) ...\\n2) ...\\n3) ...', без слова 'донес'.\n"
        "8) expected_quantity: только абсолютные значения, без процентов и без обещаний конверсии.\n"
        "9) expected_quality: можно аккуратно описывать влияние на этапы/конверсию как гипотезу.\n\n"
        f"Режим кейса: {mode}\n"
        f"Разрешенные оси разбора: {', '.join(str(x) for x in allowed_axes) if allowed_axes else 'по фактам кейса'}\n"
        f"Запрещенные темы: {', '.join(str(x) for x in banned_topics) if banned_topics else 'нет специальных запретов'}\n"
        "Жесткое правило: не подставляй запрещенные темы в сильные стороны/зоны роста/действия.\n\n"
        "Важно: не скатывайся в техно-термины и канцелярит. Не повторяй одинаковые шаблоны между строками.\n\n"
        "Style source excerpt:\n"
        f"{style_hint or '(style source unavailable)'}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def build_daily_rerank_messages(
    *,
    rerank_payload: dict[str, Any],
    config: DealAnalyzerConfig,
    style_source_excerpt: str,
) -> list[dict[str, str]]:
    system_prompt = (
        "Ты помогаешь ранжировать сделки для ежедневного управленческого разбора. "
        "Выбирай те сделки, где больше пользы для управления: живые сигналы разговора, "
        "достаточный контекст, понятный участок воронки, практическая ценность для разборов. "
        "Слабые/шумные кейсы можно помечать skip=true. "
        "Используй только входные данные, ничего не выдумывай. Верни только JSON."
    )
    user_prompt = (
        "Payload:\n"
        f"{json.dumps(rerank_payload, ensure_ascii=False, indent=2)}\n\n"
        "Верни JSON:\n"
        "{\n"
        '  "ranked": [\n'
        '    {"deal_id":"321", "rank":1, "reason":"коротко", "skip":false, "skip_reason":"", "call_analysis_viability":"high", "call_analysis_viability_reason":"коротко"}\n'
        "  ]\n"
        "}\n\n"
        "Правила:\n"
        "1) Сначала самые информативные и полезные для daily-control.\n"
        "2) Если transcript weak/noisy и CRM тонкий, можно ставить skip=true.\n"
        "3) reason и skip_reason короткие, по делу.\n"
        f"Style source excerpt:\n{style_source_excerpt or '(style source unavailable)'}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _build_system_prompt(style_profile_name: str) -> str:
    return (
        "Ты анализируешь качество ведения сделки. "
        "Пиши по-русски, коротко, живым менеджерским языком без канцелярита. "
        "Не выдумывай факты: используй только входные данные. "
        "Если данных мало, явно укажи это в risk_flags или growth_zones. "
        "Верни строго JSON-объект. Только JSON и ничего больше: "
        "без markdown, без ``` блоков, без комментариев, без пояснений до и после JSON. "
        "Ожидаемые поля: score_0_100, strong_sides, growth_zones, risk_flags, "
        "recommended_actions_for_manager, recommended_training_tasks_for_employee, "
        "manager_message_draft, employee_training_message_draft, "
        "presentation_quality_flag, followup_quality_flag, data_completeness_flag. "
        f"Профиль стиля: {style_profile_name}."
    )


def _build_user_prompt(normalized_deal: dict[str, Any]) -> str:
    compact = _compact_payload_for_llm(normalized_deal)
    payload_json = json.dumps(compact, ensure_ascii=False, indent=2)
    return (
        "Входные данные сделки (normalized payload, compact):\n"
        f"{payload_json}\n\n"
        "Сформируй анализ по указанному контракту. "
        "score_0_100 должен быть целым от 0 до 100. "
        "Списки должны быть массивами строк. "
        "Если поле не подтверждается данными, не выдумывай детали."
    )


def _compact_payload_for_llm(normalized_deal: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "deal_id",
        "amo_lead_id",
        "deal_name",
        "responsible_user_name",
        "pipeline_name",
        "status_name",
        "product_values",
        "source_values",
        "pain_text",
        "business_tasks_text",
        "brief_url",
        "demo_result_text",
        "test_result_text",
        "probability_value",
        "tags",
        "presentation_detected",
        "presentation_detect_reason",
        "long_call_detected",
        "longest_call_duration_seconds",
        "manager_scope_allowed",
    ]
    out = {k: normalized_deal.get(k) for k in keys}
    out["notes_summary_raw"] = _compact_list(normalized_deal.get("notes_summary_raw"), 5)
    out["tasks_summary_raw"] = _compact_list(normalized_deal.get("tasks_summary_raw"), 5)
    out["presentation_link_candidates"] = _compact_list(normalized_deal.get("presentation_link_candidates"), 5)
    out["company_comment"] = _truncate_text(normalized_deal.get("company_comment"), 250)
    out["contact_comment"] = _truncate_text(normalized_deal.get("contact_comment"), 250)
    return out


def _compact_list(value: Any, max_items: int) -> list[Any]:
    if not isinstance(value, list):
        return []
    out: list[Any] = []
    for item in value[:max_items]:
        if isinstance(item, dict):
            compact_item: dict[str, Any] = {}
            for k, v in item.items():
                if isinstance(v, str):
                    compact_item[k] = _truncate_text(v, 180)
                else:
                    compact_item[k] = v
            out.append(compact_item)
        elif isinstance(item, str):
            out.append(_truncate_text(item, 180))
        else:
            out.append(item)
    return out


def _truncate_text(value: Any, limit: int) -> str:
    text = " ".join(str(value or "").strip().split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"
