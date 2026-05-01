from __future__ import annotations

import json
import re
import time
from typing import Any

from src.deal_analyzer.employee_profiles.analyzer import (
    apply_profile_to_row_fields,
    build_employee_profile_context,
    sanitize_employee_text,
)
from src.deal_analyzer.employee_profiles.registry import (
    build_employee_profile_registry,
    resolve_employee_profile,
)
from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError
from src.deal_analyzer.llm_runtime import classify_llm_error

from ..weekly_shared.role_policy import contains_forbidden_upper_funnel_for_sales_manager, resolve_role_policy
from ..weekly_shared.roks_oap import build_manager_metric_interpretation
from .models import WeeklyManagerGroup


WEEKLY_REQUIRED_FIELDS: tuple[str, ...] = (
    "weekly_result",
    "improved",
    "not_improved",
    "repeating_mistakes",
    "manager_actions_next_week",
    "expected_quantity_effect",
    "expected_quality_effect",
    "manager_report_phrase",
    "employee_message",
)


def _safe_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("$\\rightarrow$", " -> ")
    text = text.replace("\\rightarrow", " -> ")
    text = text.replace("$", " ")
    return " ".join(text.replace("\n", " ").replace("\r", " ").split()).strip()


def _join_unique(values: list[str], *, sep: str = "; ") -> str:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = _safe_text(raw)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return sep.join(out)


def _enumerate_join(values: list[str]) -> str:
    unique: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = _safe_text(raw)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(text)
    if not unique:
        return ""
    if len(unique) == 1:
        return unique[0]
    return "; ".join(f"{idx}) {item}" for idx, item in enumerate(unique, start=1))


def _roks_weekly_fact(
    manager_metrics: dict[str, Any],
    *,
    manager_name: str = "",
    manager_role_profile: str = "",
) -> dict[str, Any]:
    weekly_fact = (manager_metrics.get("weekly_fact") or {}) if isinstance(manager_metrics, dict) else {}
    if not isinstance(weekly_fact, dict):
        weekly_fact = {}
    interpretation = weekly_fact.get("metric_interpretation")
    if not isinstance(interpretation, dict):
        interpretation = build_manager_metric_interpretation(
            manager_name=manager_name,
            manager_role_profile=manager_role_profile,
            weekly_fact={
                "interest_fact": weekly_fact.get("interest_fact"),
                "demo_fact": weekly_fact.get("demo_fact"),
                "test_fact": weekly_fact.get("test_fact"),
                "invoice_count_fact": weekly_fact.get("invoice_count_fact"),
                "payment_count_fact": weekly_fact.get("payment_count_fact"),
            },
        )
    return {
        "roks_calls_fact": weekly_fact.get("calls_fact"),
        "roks_lpr_fact": weekly_fact.get("lpr_fact"),
        "roks_interest_fact": weekly_fact.get("interest_fact"),
        "roks_demo_fact": weekly_fact.get("demo_fact"),
        "roks_test_fact": weekly_fact.get("test_fact"),
        "roks_invoice_count_fact": weekly_fact.get("invoice_count_fact"),
        "roks_payment_count_fact": weekly_fact.get("payment_count_fact"),
        "roks_calls_fact_raw_cell": weekly_fact.get("calls_fact_raw_cell", ""),
        "roks_sheet_used": weekly_fact.get("roks_sheet_used", ""),
        "roks_week_index_used": weekly_fact.get("week_index_used"),
        "roks_week_label_used": weekly_fact.get("week_label_used", ""),
        "roks_row_labels_found": list(weekly_fact.get("row_labels_found", []) or []),
        "roks_warnings": list(weekly_fact.get("warnings", []) or []),
        "metric_interpretation": interpretation,
    }


def _sanitize_quantitative_phrase(
    *,
    text: str,
    analyzed_deals_count: int,
    roks_calls_fact: int | float | None,
) -> str:
    out = sanitize_employee_text(_safe_text(text))
    if not out:
        return ""
    if analyzed_deals_count > 0:
        out = re.sub(
            r"\b(пров[её]л[аи]?|сделал[аи]?)\s+\d+\s+сдел(?:к(?:у|и|а)?|ок)\b",
            f"в разбор попало {analyzed_deals_count} сделок",
            out,
            flags=re.IGNORECASE,
        )
    if roks_calls_fact is None:
        out = re.sub(
            r"\b0\s+звонк(?:ов|а|и)?\b",
            "факт по РОКС не подтянулся",
            out,
            flags=re.IGNORECASE,
        )
    out = re.sub(
        r"\b(?:сам\s+)?назначил[аи]?\s+(\d+)\s+демо\b",
        lambda match: f"провел {match.group(1)} демо",
        out,
        flags=re.IGNORECASE,
    )
    return out


def _sanitize_role_scope_phrase(*, text: str, manager_name: str, manager_role_profile: str) -> str:
    out = sanitize_employee_text(_safe_text(text))
    policy = resolve_role_policy(
        manager_name=manager_name,
        manager_role_profile=manager_role_profile,
    )
    blocked, _marker = contains_forbidden_upper_funnel_for_sales_manager(text=out, policy=policy)
    if blocked:
        return (
            "Фокус: теплая/текущая воронка, перевод интереса в демо, дожим теста/счета и контроль "
            "следующего шага в amoCRM; без массового холодного обзвона."
        )
    if str(policy.get("role") or "") == "sales_manager":
        if re.search(r"(давить|продавл\w+|жестк\w+\s+продаж\w+|презент\w+\s+все\s+функц\w+)", out, flags=re.IGNORECASE):
            return (
                "Фокус: consultative demo и guided discovery — вести клиента через его задачу, "
                "дать hands-on действие в сервисе, зафиксировать критерий успеха теста и следующий шаг."
            )
    return out


def _call_llm(
    *,
    model: str,
    base_url: str,
    timeout_seconds: int,
    messages: list[dict[str, str]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    started = time.perf_counter()
    try:
        client = OllamaClient(base_url=base_url, model=model, timeout_seconds=max(1, int(timeout_seconds or 60)))
        parsed = client.chat_json(messages=messages)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        payload = parsed.payload if isinstance(parsed.payload, dict) else None
        return payload, {"ok": bool(payload), "error": "", "elapsed_ms": elapsed_ms, "repair_applied": bool(parsed.repair_applied)}
    except OllamaClientError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return None, {"ok": False, "error": str(exc), "elapsed_ms": elapsed_ms, "repair_applied": False}


def _preflight_model(*, model: str, base_url: str, timeout_seconds: int) -> dict[str, Any]:
    messages = [
        {"role": "system", "content": "Верни строго валидный JSON-объект без markdown."},
        {"role": "user", "content": 'Ответь строго JSON-объектом: {"ok": true}'},
    ]
    payload, meta = _call_llm(
        model=model,
        base_url=base_url,
        timeout_seconds=max(1, int(timeout_seconds or 30)),
        messages=messages,
    )
    prompt_size_chars = sum(len(str(item.get("content") or "")) for item in messages)
    if payload is None:
        return {
            "ok": False,
            "error": str(meta.get("error") or "preflight_payload_empty"),
            "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
            "prompt_size_chars": prompt_size_chars,
        }
    ok_value = payload.get("ok")
    ok = bool(ok_value is True or (isinstance(ok_value, str) and ok_value.strip().lower() == "true"))
    return {
        "ok": ok,
        "error": "" if ok else "preflight_json_missing_ok_true",
        "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
        "prompt_size_chars": prompt_size_chars,
    }


def _runtime_from_config(
    *,
    cfg: Any,
    llm_runtime: dict[str, Any],
    main_model_override: str | None,
    fallback_model_override: str | None,
) -> dict[str, Any]:
    main_model = str(main_model_override or "").strip() or str(
        ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("model")
        or cfg.ollama_model
        or "deepseek-v4-pro:cloud"
    ).strip()
    fallback_model = str(fallback_model_override or "").strip() or str(
        ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("model")
        or cfg.ollama_fallback_model
        or "deepseek-v4-flash:cloud"
    ).strip()
    return {
        "main": {
            "model": main_model,
            "base_url": str(
                ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("base_url")
                or cfg.ollama_base_url
                or "http://127.0.0.1:11434"
            ).strip(),
            "timeout_seconds": int(
                ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("timeout_seconds")
                or cfg.ollama_timeout_seconds
                or 120
            ),
            "preflight_timeout_seconds": int(
                ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("preflight_timeout_seconds")
                or cfg.ollama_preflight_timeout_seconds
                or 20
            ),
        },
        "fallback": {
            "enabled": bool(
                ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("enabled", True)
                if fallback_model
                else False
            ),
            "model": fallback_model,
            "base_url": str(
                ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("base_url")
                or cfg.ollama_fallback_base_url
                or cfg.ollama_base_url
                or "http://127.0.0.1:11434"
            ).strip(),
            "timeout_seconds": int(
                ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("timeout_seconds")
                or cfg.ollama_fallback_timeout_seconds
                or cfg.ollama_timeout_seconds
                or 120
            ),
            "preflight_timeout_seconds": int(
                ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("preflight_timeout_seconds")
                or cfg.ollama_preflight_timeout_seconds
                or 20
            ),
        },
    }


def _build_group_context(
    group: WeeklyManagerGroup,
    roks_snapshot: dict[str, Any],
    *,
    compact: bool = False,
    client_context_by_manager: dict[str, dict[str, Any]] | None = None,
    employee_profiles_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manager_metrics = (
        (roks_snapshot.get("manager_metrics") or {}) if isinstance(roks_snapshot.get("manager_metrics"), dict) else {}
    ).get(group.manager_name, {})
    roks_fact = _roks_weekly_fact(
        manager_metrics if isinstance(manager_metrics, dict) else {},
        manager_name=group.manager_name,
        manager_role_profile=group.manager_role_profile,
    )
    metric_interpretation = (
        roks_fact.get("metric_interpretation", {})
        if isinstance(roks_fact.get("metric_interpretation"), dict)
        else {}
    )
    source_daily_rows = []
    for item in group.source_rows[: (4 if compact else 10)]:
        if not isinstance(item, dict):
            continue
        source_daily_rows.append(
            {
                "date": item.get("control_day_date"),
                "main_pattern": item.get("main_pattern"),
                "strong_sides": item.get("strong_sides"),
                "growth_zones": item.get("growth_zones"),
                "what_to_fix": item.get("what_to_fix"),
                "what_to_tell_employee": item.get("what_to_tell_employee"),
                "score_0_100": item.get("score_0_100"),
            }
        )
    client_context = {}
    if isinstance(client_context_by_manager, dict):
        node = client_context_by_manager.get(_safe_text(group.manager_name).lower())
        if isinstance(node, dict):
            client_context = node
    employee_profile_context = build_employee_profile_context(
        manager_name=group.manager_name,
        manager_role_profile=group.manager_role_profile,
        source_rows=[item for item in group.source_rows if isinstance(item, dict)],
        registry_raw=employee_profiles_registry,
    )
    payload = {
        "period_start": group.period_start,
        "period_end": group.period_end,
        "week_start": group.week_start,
        "week_end": group.week_end,
        "manager_name": group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "source_day_count": group.source_day_count,
        "deals_count": group.deals_count,
        "calls_count": group.calls_count,
        "analyzed_deals_count": int(group.analyzed_deals_count or group.deals_count or 0),
        "analyzed_calls_count": int(group.analyzed_calls_count or group.calls_count or 0),
        "quality_sample_size": int(group.quality_sample_size or group.deals_count or 0),
        "avg_score_0_100": group.avg_score_0_100,
        "deal_links": group.deal_links[:20],
        "product_mix_week": group.product_mix_week,
        "base_mix_week": group.base_mix_week,
        "repeated_growth_zones": group.repeated_growth_zones[:12],
        "repeated_strong_sides": group.repeated_strong_sides[:12],
        "repeated_fix_points": group.repeated_fix_points[:12],
        "repeated_messages": group.repeated_messages[:12],
        "plan_actions_total": int(group.plan_actions_total or 0),
        "plan_done_count": int(group.plan_done_count or 0),
        "plan_in_progress_count": int(group.plan_in_progress_count or 0),
        "plan_postponed_count": int(group.plan_postponed_count or 0),
        "plan_no_status_count": int(group.plan_no_status_count or 0),
        "plan_training_links": group.plan_training_links[:10],
        "plan_post_training_task_links": group.plan_post_training_task_links[:10],
        "plan_training_topics": group.plan_training_topics[:10],
        "plan_training_rows_found_count": int(group.plan_training_rows_found_count or 0),
        "plan_training_rows_used_count": int(group.plan_training_rows_used_count or 0),
        "plan_training_rows_used": group.plan_training_rows_used[:10],
        "unresolved_plan_actions": group.unresolved_plan_actions[:10],
        "training_source_policy": "week_plan_only",
        "source_daily_rows": source_daily_rows,
        "roks_snapshot_status": roks_snapshot.get("status", ""),
        "roks_manager_metrics": manager_metrics,
        "roks_metric_interpretation": metric_interpretation,
        "metrics_context": {
            "roks_facts": {
                "Дозвоны": roks_fact.get("roks_calls_fact"),
                "ЛПР": roks_fact.get("roks_lpr_fact"),
                "Есть интерес": roks_fact.get("roks_interest_fact"),
                "Демо": roks_fact.get("roks_demo_fact"),
                "Тест": roks_fact.get("roks_test_fact"),
                "Счета": roks_fact.get("roks_invoice_count_fact"),
                "Оплаты": roks_fact.get("roks_payment_count_fact"),
                "roks_sheet_used": roks_fact.get("roks_sheet_used", ""),
                "week_label_used": roks_fact.get("roks_week_label_used", ""),
            },
            "metric_interpretation": metric_interpretation,
            "quality_sample": {
                "analyzed_deals_count": int(group.analyzed_deals_count or group.deals_count or 0),
                "analyzed_calls_count": int(group.analyzed_calls_count or group.calls_count or 0),
                "quality_sample_size": int(group.quality_sample_size or group.deals_count or 0),
                "note": "В разбор попадает только выборка для качественного анализа, это не общий объем работы менеджера.",
            },
        },
        "client_list_context": client_context,
        "employee_profile": employee_profile_context,
    }
    if compact:
        payload["context_mode"] = "compact_retry"
    return payload


def _build_llm_messages(context: dict[str, Any], *, repair_mode: bool = False, previous_error: str = "") -> list[dict[str, str]]:
    schema = {
        "weekly_result": "",
        "improved": "",
        "not_improved": "",
        "repeating_mistakes": "",
        "training_for_employee": "",
        "post_training_tasks": "",
        "manager_actions_next_week": "",
        "expected_quantity_effect": "",
        "expected_quality_effect": "",
        "manager_report_phrase": "",
        "employee_message": "",
    }
    system = (
        "Ты руководитель активных продаж. Верни только валидный JSON без markdown. "
        "Пиши только на русском, по фактам входных данных. "
        "Не используй шаблонные фразы и не выдумывай факты. "
        "Запрещены формулировки сравнения со стадиями CRM. "
        "Ссылки на обучение/задачи и тему обучения не придумывай: если в плане нет обучения, training_for_employee и post_training_tasks оставь пустыми. "
        "Не подменяй факт РОКС выборкой из разбора: analyzed_deals_count/quality_sample_size не равны общему объему работы. "
        "Не пиши формулировки 'провел N сделок' или 'сделал N сделок'; используй нейтрально: 'в разбор попало N сделок'. "
        "Если факт по РОКС отсутствует, пиши 'факт по РОКС не подтянулся', а не ноль. "
        "ROKS по менеджерам role-based: это не всегда линейная персональная воронка. "
        "Для профиля демо-исполнителя не пиши 'назначил N демо'; корректно: 'провел N демо', "
        "часть демо может прийти из встреч, назначенных другими менеджерами/источниками. "
        "Для профиля top-funnel не оценивай менеджера по demo/test/invoice/payment как обязательным KPI."
        " Для sales_manager не давай рекомендации в формате массового холодного обзвона/наборов/дозвонов. "
        "Для рекомендаций по демо используй consultative demo стандарт: educational demo, guided discovery, "
        "client-led walkthrough, hands-on demonstration, совместная диагностика; "
        "не 'давить' и не 'показывать все функции подряд'."
        " Если в context есть client_list_context, опирайся на него для sales_manager: рекомендации через теплую/текущую воронку и конкретные клиентские категории."
        " Учитывай employee_profile: direct_accountability = прямой тон без унижения, "
        "expert_to_expert = профессионально, через коммерческий результат и автономию."
    )
    if repair_mode:
        system += " Режим repair: исправь JSON и язык, верни только JSON-объект."
    user_payload = {
        "schema": schema,
        "context": context,
        "repair_reason": previous_error,
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def _validate_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    if not isinstance(payload, dict):
        return False, ["payload_not_object"]
    errors: list[str] = []
    for field in WEEKLY_REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing_field:{field}")
        elif not _safe_text(payload.get(field)):
            errors.append(f"empty_field:{field}")
    return len(errors) == 0, errors


def _build_quarantine_row(group: WeeklyManagerGroup, *, source_run_id: str) -> dict[str, Any]:
    return {
        "week_start": group.week_start,
        "week_end": group.week_end,
        "manager_name": group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "deals_count": group.deals_count,
        "product_focus_week": group.product_mix_week,
        "base_mix_week": group.base_mix_week,
        "weekly_result": "",
        "improved": "",
        "not_improved": "",
        "repeating_mistakes": "",
        "training_for_employee": "",
        "training_link": "",
        "post_training_tasks": "",
        "post_training_tasks_link": "",
        "manager_actions_next_week": "",
        "expected_quantity_effect": "",
        "expected_quality_effect": "",
        "manager_report_phrase": "",
        "employee_message": "",
        "avg_score_0_100": group.avg_score_0_100,
        "training_source": "not_planned",
        "training_rows_found_count": int(group.plan_training_rows_found_count or 0),
        "training_rows_used": [],
        "training_status": "llm_failed",
        "analysis_backend_used": "quarantined_llm_failed",
        "source_run_id": source_run_id,
    }


def _row_from_payload(
    *,
    group: WeeklyManagerGroup,
    payload: dict[str, Any],
    backend: str,
    source_run_id: str,
    roks_snapshot: dict[str, Any],
    employee_profiles_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manager_metrics = (
        (roks_snapshot.get("manager_metrics") or {}) if isinstance(roks_snapshot.get("manager_metrics"), dict) else {}
    ).get(group.manager_name, {})
    roks_fact = _roks_weekly_fact(
        manager_metrics if isinstance(manager_metrics, dict) else {},
        manager_name=group.manager_name,
        manager_role_profile=group.manager_role_profile,
    )
    metric_interpretation = (
        roks_fact.get("metric_interpretation", {})
        if isinstance(roks_fact.get("metric_interpretation"), dict)
        else {}
    )
    analyzed_deals_count = int(group.analyzed_deals_count or group.deals_count or 0)
    analyzed_calls_count = int(group.analyzed_calls_count or group.calls_count or 0)
    quality_sample_size = int(group.quality_sample_size or group.deals_count or 0)
    roks_calls_fact = roks_fact.get("roks_calls_fact")
    planned_training = int(group.plan_training_rows_found_count or 0) > 0
    planned_training_topics: list[str] = []
    for topic in group.plan_training_topics:
        text = _safe_text(topic)
        if text:
            planned_training_topics.append(text)

    planned_training_tasks: list[str] = []
    for row in group.plan_training_rows_used:
        if not isinstance(row, dict):
            continue
        task = _safe_text(row.get("task_to_assign"))
        if task:
            planned_training_tasks.append(task)

    if planned_training:
        training_for_employee = _enumerate_join(planned_training_topics)
        training_link = _join_unique(group.plan_training_links, sep="\n")
        post_training_link = _join_unique(group.plan_post_training_task_links, sep="\n")
        post_training_tasks = _enumerate_join(planned_training_tasks)
        training_source = "week_plan"
        training_status = "обучение запланировано"
        training_rows_used = [item for item in group.plan_training_rows_used[:10] if isinstance(item, dict)]
    else:
        training_for_employee = ""
        training_link = ""
        post_training_link = ""
        post_training_tasks = ""
        training_source = "not_planned"
        training_status = "обучение не планировалось"
        training_rows_used = []

    row = {
        "week_start": group.week_start,
        "week_end": group.week_end,
        "manager_name": group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "deals_count": int(group.deals_count or 0),
        "analyzed_deals_count": analyzed_deals_count,
        "analyzed_calls_count": analyzed_calls_count,
        "quality_sample_size": quality_sample_size,
        "roks_calls_fact": roks_fact.get("roks_calls_fact"),
        "roks_lpr_fact": roks_fact.get("roks_lpr_fact"),
        "roks_interest_fact": roks_fact.get("roks_interest_fact"),
        "roks_demo_fact": roks_fact.get("roks_demo_fact"),
        "roks_test_fact": roks_fact.get("roks_test_fact"),
        "roks_invoice_count_fact": roks_fact.get("roks_invoice_count_fact"),
        "roks_payment_count_fact": roks_fact.get("roks_payment_count_fact"),
        "roks_sheet_used": roks_fact.get("roks_sheet_used", ""),
        "roks_week_index_used": roks_fact.get("roks_week_index_used"),
        "roks_week_label_used": roks_fact.get("roks_week_label_used", ""),
        "roks_row_labels_found": roks_fact.get("roks_row_labels_found", []),
        "roks_warnings": roks_fact.get("roks_warnings", []),
        "metric_interpretation": metric_interpretation,
        "source_generated_interest": metric_interpretation.get("source_generated_interest"),
        "conducted_demo": metric_interpretation.get("conducted_demo"),
        "routed_meetings_possible": metric_interpretation.get("routed_meetings_possible"),
        "downstream_metrics_applicable": metric_interpretation.get("downstream_metrics_applicable"),
        "product_focus_week": group.product_mix_week,
        "base_mix_week": group.base_mix_week,
        "weekly_result": _sanitize_quantitative_phrase(
            text=str(payload.get("weekly_result") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "improved": _sanitize_quantitative_phrase(
            text=str(payload.get("improved") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "not_improved": _sanitize_quantitative_phrase(
            text=str(payload.get("not_improved") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "repeating_mistakes": _sanitize_quantitative_phrase(
            text=str(payload.get("repeating_mistakes") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "training_for_employee": training_for_employee,
        "training_link": training_link,
        "post_training_tasks": post_training_tasks,
        "post_training_tasks_link": post_training_link,
        "manager_actions_next_week": _sanitize_role_scope_phrase(
            text=_sanitize_quantitative_phrase(
                text=str(payload.get("manager_actions_next_week") or ""),
                analyzed_deals_count=analyzed_deals_count,
                roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
            ),
            manager_name=group.manager_name,
            manager_role_profile=group.manager_role_profile,
        ),
        "expected_quantity_effect": _sanitize_quantitative_phrase(
            text=str(payload.get("expected_quantity_effect") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "expected_quality_effect": _sanitize_quantitative_phrase(
            text=str(payload.get("expected_quality_effect") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "manager_report_phrase": _sanitize_quantitative_phrase(
            text=str(payload.get("manager_report_phrase") or ""),
            analyzed_deals_count=analyzed_deals_count,
            roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
        ),
        "employee_message": _sanitize_role_scope_phrase(
            text=_sanitize_quantitative_phrase(
                text=str(payload.get("employee_message") or ""),
                analyzed_deals_count=analyzed_deals_count,
                roks_calls_fact=roks_calls_fact if isinstance(roks_calls_fact, (int, float)) else None,
            ),
            manager_name=group.manager_name,
            manager_role_profile=group.manager_role_profile,
        ),
        "avg_score_0_100": int(group.avg_score_0_100 or 0),
        "training_source": training_source,
        "training_rows_found_count": int(group.plan_training_rows_found_count or 0),
        "training_rows_used": training_rows_used,
        "training_status": training_status,
        "analysis_backend_used": backend,
        "source_run_id": source_run_id,
    }
    profile_registry = build_employee_profile_registry(employee_profiles_registry)
    profile = resolve_employee_profile(
        manager_name=group.manager_name,
        manager_role_profile=group.manager_role_profile,
        registry=profile_registry,
    )
    row, _changes = apply_profile_to_row_fields(
        row=row,
        profile=profile,
        fields=("employee_message", "manager_report_phrase", "manager_actions_next_week"),
        date_hint_field="week_end",
    )
    return row


def analyze_weekly_groups(
    *,
    groups: list[WeeklyManagerGroup],
    cfg: Any,
    roks_snapshot: dict[str, Any],
    llm_runtime: dict[str, Any],
    logger: Any,
    source_run_id: str,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    llm_max_attempts: int = 6,
    client_context_by_manager: dict[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime=llm_runtime,
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
    )
    preflight: dict[str, Any] = {}
    for name in ("main", "fallback"):
        node = runtime.get(name, {}) if isinstance(runtime.get(name), dict) else {}
        if name == "fallback" and not bool(node.get("enabled", False)):
            preflight[name] = {"ok": False, "error": "candidate_disabled"}
            continue
        result = _preflight_model(
            model=str(node.get("model") or ""),
            base_url=str(node.get("base_url") or ""),
            timeout_seconds=int(node.get("preflight_timeout_seconds", 20) or 20),
        )
        preflight[name] = {**result, "error_type": classify_llm_error(str(result.get("error") or ""))}
        if logger is not None:
            if bool(result.get("ok", False)):
                logger.info(
                    "weekly_manager_summary llm preflight ok candidate=%s model=%s elapsed_ms=%s",
                    name,
                    str(node.get("model") or ""),
                    int(result.get("elapsed_ms", 0) or 0),
                )
            else:
                logger.warning(
                    "weekly_manager_summary llm preflight error candidate=%s model=%s elapsed_ms=%s error=%s",
                    name,
                    str(node.get("model") or ""),
                    int(result.get("elapsed_ms", 0) or 0),
                    str(result.get("error") or ""),
                )

    rows: list[dict[str, Any]] = []
    llm_requests: list[dict[str, Any]] = []
    llm_responses: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []
    llm_success_main = 0
    llm_success_main_repair = 0
    llm_success_main_compact_retry = 0
    llm_success_fallback = 0
    llm_success_fallback_repair = 0
    llm_success_fallback_compact_retry = 0
    llm_failed_count = 0
    llm_attempts_total = 0
    employee_profile_context_rows: list[dict[str, Any]] = []
    employee_behavior_marker_rows: list[dict[str, Any]] = []

    for idx, group in enumerate(groups):
        profile_context = build_employee_profile_context(
            manager_name=group.manager_name,
            manager_role_profile=group.manager_role_profile,
            source_rows=[item for item in group.source_rows if isinstance(item, dict)],
            registry_raw=getattr(cfg, "employee_profiles", None),
        )
        employee_profile_context_rows.append(
            {
                "manager_name": group.manager_name,
                "week_start": group.week_start,
                "week_end": group.week_end,
                "communication_style": profile_context.get("communication_style", ""),
                "motivators": profile_context.get("motivators", []),
                "avoid": profile_context.get("avoid", []),
                "profile_source": profile_context.get("profile_source", ""),
            }
        )
        marker_payload = profile_context.get("behavior_markers", {})
        if isinstance(marker_payload, dict):
            employee_behavior_marker_rows.append(
                {
                    "manager_name": group.manager_name,
                    "week_start": group.week_start,
                    "week_end": group.week_end,
                    "repeated_growth_zones": marker_payload.get("repeated_growth_zones", []),
                    "repeated_strong_sides": marker_payload.get("repeated_strong_sides", []),
                    "preferred_behavior_pattern_under_pressure": marker_payload.get(
                        "preferred_behavior_pattern_under_pressure",
                        "",
                    ),
                    "coaching_response_style": marker_payload.get("coaching_response_style", ""),
                }
            )
        full_context = _build_group_context(
            group,
            roks_snapshot,
            compact=False,
            client_context_by_manager=client_context_by_manager,
            employee_profiles_registry=getattr(cfg, "employee_profiles", None),
        )
        compact_context = _build_group_context(
            group,
            roks_snapshot,
            compact=True,
            client_context_by_manager=client_context_by_manager,
            employee_profiles_registry=getattr(cfg, "employee_profiles", None),
        )
        attempts: list[dict[str, Any]] = []
        if bool(preflight.get("main", {}).get("ok", False)):
            attempts.extend(
                [
                    {"stage": "main", "candidate": "main", "repair": False, "context": full_context},
                    {"stage": "main_repair", "candidate": "main", "repair": True, "context": full_context},
                    {"stage": "main_compact_retry", "candidate": "main", "repair": False, "context": compact_context},
                ]
            )
        if bool(preflight.get("fallback", {}).get("ok", False)):
            attempts.extend(
                [
                    {"stage": "fallback", "candidate": "fallback", "repair": False, "context": full_context},
                    {"stage": "fallback_repair", "candidate": "fallback", "repair": True, "context": full_context},
                    {"stage": "fallback_compact_retry", "candidate": "fallback", "repair": False, "context": compact_context},
                ]
            )
        attempts = attempts[: max(1, int(llm_max_attempts or 6))]
        selected_payload: dict[str, Any] | None = None
        selected_backend = ""
        selected_meta: dict[str, Any] = {}
        last_error = "llm_json_invalid"
        last_error_type = "invalid_json"
        row_attempt_trace: list[dict[str, Any]] = []
        max_prompt_size_chars = 0
        last_preview = ""
        models_attempted: list[str] = []

        for attempt in attempts:
            llm_attempts_total += 1
            candidate = str(attempt.get("candidate") or "")
            node = runtime.get(candidate, {}) if isinstance(runtime.get(candidate), dict) else {}
            model = str(node.get("model") or "")
            base_url = str(node.get("base_url") or "")
            timeout = int(node.get("timeout_seconds") or 120)
            if not model or not base_url:
                continue
            models_attempted.append(model)
            messages = _build_llm_messages(
                attempt.get("context") if isinstance(attempt.get("context"), dict) else full_context,
                repair_mode=bool(attempt.get("repair", False)),
                previous_error=last_error,
            )
            prompt_size_chars = sum(len(str(item.get("content") or "")) for item in messages)
            max_prompt_size_chars = max(max_prompt_size_chars, prompt_size_chars)
            payload, meta = _call_llm(
                model=model,
                base_url=base_url,
                timeout_seconds=timeout,
                messages=messages,
            )
            error = str(meta.get("error") or "")
            error_type = classify_llm_error(error)
            preview = ""
            if payload is not None:
                try:
                    preview = json.dumps(payload, ensure_ascii=False)[:500]
                except Exception:
                    preview = str(payload)[:500]
            elif error:
                preview = error[:500]
            if preview:
                last_preview = preview
            trace_item = {
                "stage": str(attempt.get("stage") or ""),
                "model": model,
                "error": error,
                "error_type": error_type,
                "prompt_size_chars": prompt_size_chars,
                "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
                "response_preview": preview,
            }
            row_attempt_trace.append(trace_item)
            llm_responses.append({"row_index": idx, **trace_item, "payload": payload})
            if payload is None:
                last_error = error or "llm_payload_empty"
                last_error_type = classify_llm_error(last_error)
                continue
            ok, validation_errors = _validate_payload(payload)
            if not ok:
                last_error = "invalid_schema:" + ",".join(validation_errors)
                last_error_type = classify_llm_error(last_error)
                row_attempt_trace[-1]["error"] = last_error
                row_attempt_trace[-1]["error_type"] = last_error_type
                continue
            selected_payload = payload
            selected_backend = str(attempt.get("stage") or "")
            selected_meta = meta
            if selected_backend == "main":
                llm_success_main += 1
            elif selected_backend == "main_repair":
                llm_success_main_repair += 1
            elif selected_backend == "main_compact_retry":
                llm_success_main_compact_retry += 1
            elif selected_backend == "fallback":
                llm_success_fallback += 1
            elif selected_backend == "fallback_repair":
                llm_success_fallback_repair += 1
            elif selected_backend == "fallback_compact_retry":
                llm_success_fallback_compact_retry += 1
            break

        if selected_payload is None:
            llm_failed_count += 1
            row = _build_quarantine_row(group, source_run_id=source_run_id)
            rows.append(row)
            quarantined_rows.append(
                {
                    "row_index": idx,
                    "manager_name": group.manager_name,
                    "week_start": group.week_start,
                    "week_end": group.week_end,
                    "reason": last_error,
                    "error_type": last_error_type,
                    "models_attempted": models_attempted,
                    "errors_by_attempt": [
                        {
                            "stage": item.get("stage", ""),
                            "model": item.get("model", ""),
                            "error_type": item.get("error_type", ""),
                            "error": item.get("error", ""),
                        }
                        for item in row_attempt_trace
                    ],
                    "raw_response_preview": str(last_preview)[:500],
                    "prompt_size_chars": int(max_prompt_size_chars or 0),
                    "failure_stage": row_attempt_trace[-1].get("stage", "") if row_attempt_trace else "",
                    "analysis_backend_used": "quarantined_llm_failed",
                }
            )
            llm_requests.append(
                {
                    "row_index": idx,
                    "group_key": f"{group.week_start}|{group.week_end}|{group.manager_name}",
                    "selected_backend": "quarantined_llm_failed",
                    "selected_meta": {"ok": False, "error": last_error},
                    "attempt_trace": row_attempt_trace,
                }
            )
        else:
            row = _row_from_payload(
                group=group,
                payload=selected_payload,
                backend=selected_backend,
                source_run_id=source_run_id,
                roks_snapshot=roks_snapshot,
                employee_profiles_registry=getattr(cfg, "employee_profiles", None),
            )
            rows.append(row)
            llm_requests.append(
                {
                    "row_index": idx,
                    "group_key": f"{group.week_start}|{group.week_end}|{group.manager_name}",
                    "selected_backend": selected_backend,
                    "selected_meta": selected_meta,
                    "attempt_trace": row_attempt_trace,
                }
            )
        if logger is not None:
            logger.info(
                "weekly_manager_summary llm row=%s manager=%s week=%s..%s backend=%s",
                idx,
                group.manager_name,
                group.week_start,
                group.week_end,
                rows[-1].get("analysis_backend_used", ""),
            )

    training_source_counts: dict[str, int] = {}
    training_rows_found_count = 0
    training_rows_used: list[dict[str, Any]] = []
    training_missing_but_generated_count = 0
    training_examples: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        source = str(row.get("training_source") or "not_planned")
        training_source_counts[source] = int(training_source_counts.get(source, 0) or 0) + 1
        training_rows_found_count += int(row.get("training_rows_found_count", 0) or 0)
        used_rows = row.get("training_rows_used", []) if isinstance(row.get("training_rows_used"), list) else []
        for item in used_rows:
            if isinstance(item, dict):
                training_rows_used.append(item)
        if source != "week_plan" and (
            _safe_text(row.get("training_for_employee"))
            or _safe_text(row.get("training_link"))
            or _safe_text(row.get("post_training_tasks_link"))
        ):
            training_missing_but_generated_count += 1
            if len(training_examples) < 5:
                training_examples.append(
                    {
                        "manager_name": row.get("manager_name", ""),
                        "week_start": row.get("week_start", ""),
                        "week_end": row.get("week_end", ""),
                        "training_for_employee": row.get("training_for_employee", ""),
                        "training_link": row.get("training_link", ""),
                        "post_training_tasks_link": row.get("post_training_tasks_link", ""),
                    }
                )

    diagnostics = {
        "llm_runtime": {
            "main": runtime.get("main", {}),
            "fallback": runtime.get("fallback", {}),
            "preflight": preflight,
            "selected": "mixed",
            "reason": "weekly_llm_first",
        },
        "llm_attempts_total": llm_attempts_total,
        "llm_success_main": llm_success_main,
        "llm_success_main_repair": llm_success_main_repair,
        "llm_success_main_compact_retry": llm_success_main_compact_retry,
        "llm_success_fallback": llm_success_fallback,
        "llm_success_fallback_repair": llm_success_fallback_repair,
        "llm_success_fallback_compact_retry": llm_success_fallback_compact_retry,
        "llm_failed_count": llm_failed_count,
        "fallback_used_count": int(
            llm_success_fallback + llm_success_fallback_repair + llm_success_fallback_compact_retry
        ),
        "quarantined_count": len(quarantined_rows),
        "quarantined_rows": quarantined_rows,
        "llm_requests": llm_requests,
        "llm_responses": llm_responses,
        "max_prompt_size_chars_seen": max(
            [int(item.get("prompt_size_chars", 0) or 0) for item in llm_responses if isinstance(item, dict)] or [0]
        ),
        "training_source_counts": training_source_counts,
        "training_rows_found_count": int(training_rows_found_count),
        "training_rows_used": training_rows_used[:20],
        "training_rows_used_count": len(training_rows_used),
        "training_missing_but_generated_count": int(training_missing_but_generated_count),
        "training_missing_but_generated_examples": training_examples,
        "employee_profile_context_rows": employee_profile_context_rows,
        "employee_behavior_marker_rows": employee_behavior_marker_rows,
    }
    return rows, diagnostics
