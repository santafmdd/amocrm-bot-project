from __future__ import annotations

import json
import re
import time
from typing import Any

from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError
from src.deal_analyzer.llm_runtime import classify_llm_error

from .day_grouper import week_bounds_monday_sunday
from .models import DailyControlInputGroup
from .validation.text_lint import lint_daily_text_rows, lint_has_blockers


LLM_REQUIRED_FIELDS: tuple[str, ...] = (
    "date",
    "day_label",
    "manager_name",
    "base_mix",
    "product_mix",
    "main_pattern",
    "strengths",
    "growth_zones",
    "why_it_matters",
    "what_to_fix",
    "what_to_tell_employee",
    "expected_effect_quantity",
    "expected_effect_quality",
    "score_0_100",
    "criticality",
    "training_needed",
    "training_topic",
    "evidence_short",
    "data_limitations",
)


def _build_llm_messages(
    context: dict[str, Any],
    *,
    repair_mode: bool = False,
    previous_error: str = "",
) -> list[dict[str, str]]:
    system = (
        "Ты руководитель активных продаж. Верни строго валидный JSON без markdown. "
        "Пиши пользовательские поля только на русском. "
        "Не придумывай факты, используй только входные данные. "
        "Перед ответом проверь, что в JSON нет китайского текста, английских фраз, иностранных приветствий, markdown и техкомментариев. "
        "Допустимые рабочие исключения только при необходимости: LINK, INFO, PLM, CRM, amoCRM, ROKS, OAP, ID, URL. "
        "Термины API/JSON/LLM/STT в пользовательских полях не используй, заменяй русскими эквивалентами. "
        "Не используй фразу 'Лучше сказать:'. "
        "Не используй канцелярит и scripted-аналитику. "
        "Если данных мало, аккуратно укажи ограничения в data_limitations и сформируй осторожный вывод. "
        "В expected_effect_quantity/expected_effect_quality формулируй управленческую гипотезу, не точный прогноз."
    )
    if repair_mode:
        system += " Режим repair: исправь структуру и язык. Верни только валидный JSON-объект."

    schema = {
        "date": "YYYY-MM-DD",
        "day_label": "понедельник",
        "manager_name": "Имя Фамилия",
        "department": "",
        "base_mix": "",
        "product_mix": "",
        "main_pattern": "",
        "strengths": "",
        "growth_zones": "",
        "why_it_matters": "",
        "what_to_fix": "",
        "what_to_tell_employee": "",
        "expected_effect_quantity": "",
        "expected_effect_quality": "",
        "score_0_100": 0,
        "criticality": "low",
        "training_needed": False,
        "training_topic": "",
        "evidence_short": "",
        "data_limitations": "",
    }

    user_payload = {
        "schema": schema,
        "context": context,
        "repair_reason": previous_error,
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def _normalize_criticality(value: Any, score: int) -> str:
    text = str(value or "").strip().lower()
    if text in {"low", "medium", "high", "critical"}:
        return text
    if text in {"низкая", "низкий"}:
        return "low"
    if text in {"средняя", "средний"}:
        return "medium"
    if text in {"высокая", "высокий"}:
        return "high"
    if text in {"критическая", "критичная", "critical"}:
        return "critical"
    if score <= 19:
        return "critical"
    if score <= 44:
        return "high"
    if score <= 69:
        return "medium"
    return "low"


def _criticality_ru_from_code(value: str) -> str:
    code = str(value or "").strip().lower()
    if code == "critical":
        return "критичная"
    if code == "high":
        return "высокая"
    if code == "medium":
        return "средняя"
    return "низкая"


def _safe_score(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(100, parsed))


def _safe_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


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
        return payload, {
            "ok": bool(payload),
            "error": "",
            "elapsed_ms": elapsed_ms,
            "repair_applied": bool(parsed.repair_applied),
        }
    except OllamaClientError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return None, {
            "ok": False,
            "error": str(exc),
            "elapsed_ms": elapsed_ms,
            "repair_applied": False,
        }


def _preflight_model(*, model: str, base_url: str, timeout_seconds: int) -> dict[str, Any]:
    preflight_messages = [
        {
            "role": "system",
            "content": "Верни строго валидный JSON-объект без markdown.",
        },
        {
            "role": "user",
            "content": 'Ответь строго JSON-объектом: {"ok": true}',
        },
    ]
    payload, meta = _call_llm(
        model=model,
        base_url=base_url,
        timeout_seconds=max(1, int(timeout_seconds or 30)),
        messages=preflight_messages,
    )
    prompt_size_chars = sum(len(str(item.get("content") or "")) for item in preflight_messages)
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
    fallback2_model_override: str | None,
    fallback_timeout_seconds: int | None,
    no_retry_on_rate_limit: bool,
) -> dict[str, Any]:
    def _model_timeout(model_name: str, configured: int, *, is_fallback: bool) -> int:
        timeout = int(configured or 0) if int(configured or 0) > 0 else 120
        if "gemma" in str(model_name or "").lower():
            gemma_cap = int(getattr(cfg, "local_gemma_generation_timeout_sec", 240) or 240)
            timeout = min(timeout, max(60, gemma_cap))
        if is_fallback:
            timeout = max(60, timeout)
        return max(30, timeout)

    main_model = str(main_model_override or "").strip() or str(
        ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("model")
        or cfg.ollama_model
        or "qwen3.5:397b-cloud"
    ).strip()
    fallback_model = str(fallback_model_override or "").strip() or str(
        ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("model")
        or cfg.ollama_fallback_model
        or "deepseek-v3.1:671b-cloud"
    ).strip()
    fallback2_model = str(fallback2_model_override or "").strip() or str(
        ((llm_runtime.get("fallback2") or {}) if isinstance(llm_runtime.get("fallback2"), dict) else {}).get("model")
        or ""
    ).strip()

    main_timeout_cfg = int(
        ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("timeout_seconds")
        or cfg.ollama_timeout_seconds
        or 120
    )
    main = {
        "model": main_model,
        "base_url": str(
            ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("base_url")
            or cfg.ollama_base_url
            or "http://127.0.0.1:11434"
        ).strip(),
        "timeout_seconds": _model_timeout(main_model, main_timeout_cfg, is_fallback=False),
        "preflight_timeout_seconds": int(
            ((llm_runtime.get("main") or {}) if isinstance(llm_runtime.get("main"), dict) else {}).get("preflight_timeout_seconds")
            or cfg.ollama_preflight_timeout_seconds
            or 20
        ),
    }
    fallback_timeout_cfg = int(
        ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("timeout_seconds")
        or (int(fallback_timeout_seconds) if int(fallback_timeout_seconds or 0) > 0 else 0)
        or cfg.ollama_fallback_timeout_seconds
        or cfg.ollama_timeout_seconds
        or 120
    )
    fallback = {
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
        "timeout_seconds": _model_timeout(fallback_model, fallback_timeout_cfg, is_fallback=True),
    }
    fallback2_timeout_cfg = int(
        ((llm_runtime.get("fallback2") or {}) if isinstance(llm_runtime.get("fallback2"), dict) else {}).get("timeout_seconds")
        or (int(fallback_timeout_seconds) if int(fallback_timeout_seconds or 0) > 0 else 0)
        or cfg.ollama_fallback_timeout_seconds
        or cfg.ollama_timeout_seconds
        or 120
    )
    fallback2 = {
        "enabled": bool(fallback2_model),
        "model": fallback2_model,
        "base_url": str(
            ((llm_runtime.get("fallback2") or {}) if isinstance(llm_runtime.get("fallback2"), dict) else {}).get("base_url")
            or cfg.ollama_fallback_base_url
            or cfg.ollama_base_url
            or "http://127.0.0.1:11434"
        ).strip(),
        "timeout_seconds": _model_timeout(fallback2_model, fallback2_timeout_cfg, is_fallback=True),
    }
    candidates: list[dict[str, Any]] = [dict(main, name="main"), dict(fallback, name="fallback"), dict(fallback2, name="fallback2")]
    return {
        "main": main,
        "fallback": fallback,
        "fallback2": fallback2,
        "candidates": candidates,
        "no_retry_on_rate_limit": bool(no_retry_on_rate_limit),
    }


def _compact_source_cases(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in source_rows[:3]:
        compact.append(
            {
                "deal_id": row.get("deal_id"),
                "deal_name": row.get("deal_name"),
                "case_type": row.get("case_type"),
                "listened_calls": row.get("listened_calls"),
                "key_takeaway": _safe_text(row.get("key_takeaway"))[:220],
            }
        )
    return compact


def _build_group_context(group: DailyControlInputGroup, roks_snapshot: dict[str, Any], *, compact: bool = False) -> dict[str, Any]:
    manager_metrics = (
        (roks_snapshot.get("manager_metrics") or {}) if isinstance(roks_snapshot.get("manager_metrics"), dict) else {}
    ).get(group.manager_name, {})

    source_cases = []
    rows = group.source_rows[: (3 if compact else 6)]
    for row in rows:
        source_cases.append(
            {
                "deal_id": row.get("deal_id"),
                "deal_name": row.get("deal_name"),
                "case_type": row.get("case_type"),
                "listened_calls": row.get("listened_calls"),
                "key_takeaway": row.get("key_takeaway"),
                "strong": row.get("strong"),
                "growth": row.get("growth"),
                "fix": row.get("fix"),
            }
        )

    limitations: list[str] = []
    if not group.source_rows:
        limitations.append("нет исходных строк за день")
    if not manager_metrics:
        limitations.append("метрики РОКС ОАП по менеджеру не распарсены")
    if not group.base_mix:
        limitations.append("база/теги не заполнены")

    payload = {
        "period_start": group.period_start,
        "period_end": group.period_end,
        "week_start": group.week_start,
        "week_end": group.week_end,
        "date": group.control_day_date,
        "day_label": group.day_label,
        "manager_name": group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "sample_size": group.sample_size,
        "deals_count": group.deals_count,
        "calls_count": group.calls_count,
        "deal_ids": group.deal_ids,
        "deal_names": group.deal_names,
        "deal_links": group.deal_links,
        "product_mix": group.product_mix,
        "base_mix": group.base_mix,
        "source_insights": group.insights,
        "discipline_signals": group.discipline_signals,
        "roks_manager_metrics": manager_metrics,
        "roks_snapshot_status": roks_snapshot.get("status"),
        "source_cases": source_cases if not compact else _compact_source_cases(group.source_rows),
        "limitations": limitations,
    }
    if compact:
        payload["context_mode"] = "compact_retry"
    return payload


def _payload_has_language_blockers(payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    lint_row = {
        "main_pattern": _safe_text(payload.get("main_pattern")),
        "strong_sides": _safe_text(payload.get("strengths")),
        "growth_zones": _safe_text(payload.get("growth_zones")),
        "why_it_matters": _safe_text(payload.get("why_it_matters")),
        "what_to_fix": _safe_text(payload.get("what_to_fix")),
        "what_to_tell_employee": _safe_text(payload.get("what_to_tell_employee")),
        "expected_quant_impact": _safe_text(payload.get("expected_effect_quantity")),
        "expected_qual_impact": _safe_text(payload.get("expected_effect_quality")),
        "evidence_short": _safe_text(payload.get("evidence_short")),
        "data_limitations": _safe_text(payload.get("data_limitations")),
        "manager_name": _safe_text(payload.get("manager_name")),
        "deal_ids": "",
    }
    lint = lint_daily_text_rows([lint_row])
    return lint_has_blockers(lint), lint


def _validate_llm_payload(payload: dict[str, Any]) -> tuple[bool, list[str], dict[str, Any]]:
    errors: list[str] = []
    if not isinstance(payload, dict):
        return False, ["payload_not_object"], {}
    for field in LLM_REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing_field:{field}")
    score = _safe_score(payload.get("score_0_100"))
    if score < 0 or score > 100:
        errors.append("invalid_score")
    criticality = _normalize_criticality(payload.get("criticality"), score)
    if criticality not in {"low", "medium", "high", "critical"}:
        errors.append("invalid_criticality")
    language_blocked, lint = _payload_has_language_blockers(payload)
    if language_blocked:
        errors.append("language_blockers_present")
    return len(errors) == 0, errors, lint


def _build_quarantine_row(group: DailyControlInputGroup, *, source_run_id: str) -> dict[str, Any]:
    week_start = str(group.week_start or "")
    week_end = str(group.week_end or "")
    return {
        "week_start": week_start,
        "week_end": week_end,
        "period_start": group.period_start,
        "period_end": group.period_end,
        "control_day_date": group.control_day_date,
        "day_label": group.day_label,
        "manager_name": group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "sample_size": group.sample_size,
        "deals_count": group.deals_count,
        "calls_count": group.calls_count,
        "deal_ids": "; ".join(group.deal_ids),
        "deal_links": "; ".join(group.deal_links),
        "product_mix": group.product_mix,
        "base_mix": group.base_mix,
        "main_pattern": "",
        "strong_sides": "",
        "growth_zones": "",
        "why_it_matters": "",
        "what_to_reinforce": "",
        "what_to_fix": "",
        "what_to_tell_employee": "",
        "expected_quant_impact": "",
        "expected_qual_impact": "",
        "score_0_100": 0,
        "criticality": "критичная",
        "analysis_backend_used": "quarantined_llm_failed",
        "source_run_id": source_run_id,
        "training_needed": False,
        "training_topic": "",
        "evidence_short": "",
        "data_limitations": "аналитика не сформирована: llm_json_invalid",
    }


def _row_from_llm_payload(
    *,
    group: DailyControlInputGroup,
    payload: dict[str, Any],
    backend: str,
    source_run_id: str,
) -> dict[str, Any]:
    score = _safe_score(payload.get("score_0_100"))
    criticality_code = _normalize_criticality(payload.get("criticality"), score)
    control_day_date = str(payload.get("date") or group.control_day_date)
    week_start, week_end = week_bounds_monday_sunday(control_day_date)
    if not week_start:
        week_start = str(group.week_start or "")
    if not week_end:
        week_end = str(group.week_end or "")
    return {
        "week_start": week_start,
        "week_end": week_end,
        "period_start": group.period_start,
        "period_end": group.period_end,
        "control_day_date": control_day_date,
        "day_label": str(payload.get("day_label") or group.day_label),
        "manager_name": str(payload.get("manager_name") or group.manager_name),
        "manager_role_profile": group.manager_role_profile,
        "sample_size": group.sample_size,
        "deals_count": group.deals_count,
        "calls_count": group.calls_count,
        "deal_ids": "; ".join(group.deal_ids),
        "deal_links": "; ".join(group.deal_links),
        "product_mix": str(payload.get("product_mix") or group.product_mix),
        "base_mix": str(payload.get("base_mix") or group.base_mix),
        "main_pattern": _safe_text(payload.get("main_pattern")),
        "strong_sides": _safe_text(payload.get("strengths")),
        "growth_zones": _safe_text(payload.get("growth_zones")),
        "why_it_matters": _safe_text(payload.get("why_it_matters")),
        "what_to_reinforce": _safe_text(payload.get("what_to_reinforce") or payload.get("strengths")),
        "what_to_fix": _safe_text(payload.get("what_to_fix")),
        "what_to_tell_employee": _safe_text(payload.get("what_to_tell_employee")),
        "expected_quant_impact": _safe_text(payload.get("expected_effect_quantity")),
        "expected_qual_impact": _safe_text(payload.get("expected_effect_quality")),
        "score_0_100": score,
        "criticality": _criticality_ru_from_code(criticality_code),
        "analysis_backend_used": backend,
        "source_run_id": source_run_id,
        "training_needed": bool(payload.get("training_needed", False)),
        "training_topic": _safe_text(payload.get("training_topic")),
        "evidence_short": _safe_text(payload.get("evidence_short")),
        "data_limitations": _safe_text(payload.get("data_limitations")),
    }


def analyze_daily_packages(
    *,
    packages: list[DailyControlInputGroup],
    cfg: Any,
    roks_snapshot: dict[str, Any],
    llm_runtime: dict[str, Any],
    logger: Any,
    source_run_id: str,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    fallback2_model_override: str | None = None,
    fallback_timeout_seconds: int | None = None,
    no_retry_on_rate_limit: bool = True,
    llm_max_attempts: int = 9,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime=llm_runtime,
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
        fallback2_model_override=fallback2_model_override,
        fallback_timeout_seconds=fallback_timeout_seconds,
        no_retry_on_rate_limit=no_retry_on_rate_limit,
    )
    preflight_candidates: list[dict[str, Any]] = []
    for candidate in runtime.get("candidates", []) if isinstance(runtime.get("candidates"), list) else []:
        if not isinstance(candidate, dict):
            continue
        name = str(candidate.get("name") or "")
        model = str(candidate.get("model") or "")
        base_url = str(candidate.get("base_url") or "")
        enabled_candidate = bool(candidate.get("enabled", True))
        if not enabled_candidate or not model:
            preflight_candidates.append(
                {
                    "name": name,
                    "model": model,
                    "base_url": base_url,
                    "ok": False,
                    "error": "candidate_disabled",
                    "error_type": "unknown",
                    "no_retry_due_to_rate_limit": False,
                    "elapsed_ms": 0,
                }
            )
            continue
        preflight_timeout = int(candidate.get("preflight_timeout_seconds") or getattr(cfg, "ollama_preflight_timeout_seconds", 20) or 20)
        result = _preflight_model(
            model=model,
            base_url=base_url,
            timeout_seconds=preflight_timeout,
        )
        error_text = str(result.get("error") or "")
        error_type = classify_llm_error(error_text)
        if logger is not None:
            if bool(result.get("ok", False)):
                logger.info(
                    "daily_control llm preflight ok candidate=%s model=%s elapsed_ms=%s",
                    name,
                    model,
                    int(result.get("elapsed_ms", 0) or 0),
                )
            else:
                logger.warning(
                    "daily_control llm preflight error candidate=%s model=%s elapsed_ms=%s error=%s",
                    name,
                    model,
                    int(result.get("elapsed_ms", 0) or 0),
                    error_text,
                )
        preflight_candidates.append(
            {
                "name": name,
                "model": model,
                "base_url": base_url,
                "ok": bool(result.get("ok", False)),
                "error": error_text,
                "error_type": error_type,
                "no_retry_due_to_rate_limit": bool(
                    runtime.get("no_retry_on_rate_limit", True) and error_type == "cloud_usage_limit"
                ),
                "elapsed_ms": int(result.get("elapsed_ms", 0) or 0),
                "prompt_size_chars": int(result.get("prompt_size_chars", 0) or 0),
            }
        )
    preflight: dict[str, Any] = {
        "main": {"ok": False, "error": "not_checked", "elapsed_ms": 0},
        "fallback": {"ok": False, "error": "not_checked", "elapsed_ms": 0},
        "fallback2": {"ok": False, "error": "not_checked", "elapsed_ms": 0},
        "candidates": preflight_candidates,
    }
    for item in preflight_candidates:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        if name in {"main", "fallback", "fallback2"}:
            preflight[name] = {
                "ok": bool(item.get("ok", False)),
                "error": str(item.get("error") or ""),
                "elapsed_ms": int(item.get("elapsed_ms", 0) or 0),
                "error_type": str(item.get("error_type") or ""),
                "no_retry_due_to_rate_limit": bool(item.get("no_retry_due_to_rate_limit", False)),
                "prompt_size_chars": int(item.get("prompt_size_chars", 0) or 0),
            }
    selected_preflight = next(
        (item for item in preflight_candidates if isinstance(item, dict) and bool(item.get("ok", False))),
        None,
    )
    if isinstance(selected_preflight, dict):
        runtime["selected"] = str(selected_preflight.get("name") or "none")
        runtime["reason"] = f"{runtime['selected']}_preflight_ok"
    else:
        runtime["selected"] = "none"
        first_error_type = str(preflight_candidates[0].get("error_type") or "") if preflight_candidates else ""
        if first_error_type == "cloud_usage_limit":
            runtime["reason"] = "main_unavailable_rate_limit"
        else:
            runtime["reason"] = "no_live_llm_runtime"

    main_available = bool(preflight["main"].get("ok", False))
    fallback_available = bool(preflight["fallback"].get("ok", False))
    fallback2_available = bool(preflight["fallback2"].get("ok", False))

    rows: list[dict[str, Any]] = []
    llm_requests: list[dict[str, Any]] = []
    llm_responses: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []

    llm_attempts_total = 0
    llm_success_main = 0
    llm_success_main_repair = 0
    llm_success_main_compact_retry = 0
    llm_success_fallback = 0
    llm_success_fallback_repair = 0
    llm_success_fallback_compact_retry = 0
    llm_success_fallback2 = 0
    llm_success_fallback2_repair = 0
    llm_success_fallback2_compact_retry = 0
    llm_json_repair_count = 0
    llm_failed_count = 0

    for idx, group in enumerate(packages):
        full_context = _build_group_context(group, roks_snapshot, compact=False)
        compact_context = _build_group_context(group, roks_snapshot, compact=True)
        request_record = {
            "row_index": idx,
            "group_key": f"{group.control_day_date}|{group.manager_name}",
            "context": full_context,
        }

        selected_backend = ""
        selected_payload: dict[str, Any] | None = None
        selected_meta: dict[str, Any] = {}

        attempt_chain: list[dict[str, Any]] = []
        candidate_order = ["main", "fallback", "fallback2"]
        for candidate_name in candidate_order:
            candidate_cfg = runtime.get(candidate_name, {}) if isinstance(runtime.get(candidate_name), dict) else {}
            candidate_available = bool(preflight.get(candidate_name, {}).get("ok", False)) if isinstance(preflight.get(candidate_name), dict) else False
            if candidate_name == "main" and not main_available:
                continue
            if candidate_name == "fallback" and not fallback_available:
                continue
            if candidate_name == "fallback2" and not fallback2_available:
                continue
            if not candidate_available:
                continue
            model = str(candidate_cfg.get("model") or "")
            base_url = str(candidate_cfg.get("base_url") or "")
            timeout = int(candidate_cfg.get("timeout_seconds") or 120)
            if not model or not base_url:
                continue
            attempt_chain.append(
                {
                    "stage": candidate_name,
                    "candidate": candidate_name,
                    "model": model,
                    "base_url": base_url,
                    "timeout": timeout,
                    "context": full_context,
                    "repair_mode": False,
                    "is_retry": False,
                }
            )
            if candidate_name in {"main", "fallback", "fallback2"}:
                attempt_chain.append(
                    {
                        "stage": f"{candidate_name}_repair",
                        "candidate": candidate_name,
                        "model": model,
                        "base_url": base_url,
                        "timeout": timeout,
                        "context": full_context,
                        "repair_mode": True,
                        "is_retry": True,
                    }
                )
            if candidate_name in {"main", "fallback", "fallback2"}:
                attempt_chain.append(
                    {
                        "stage": f"{candidate_name}_compact_retry",
                        "candidate": candidate_name,
                        "model": model,
                        "base_url": base_url,
                        "timeout": timeout,
                        "context": compact_context,
                        "repair_mode": False,
                        "is_retry": True,
                    }
                )

        max_attempts = max(1, int(llm_max_attempts or 3))
        if len(attempt_chain) > max_attempts:
            trimmed = attempt_chain[:max_attempts]
            has_fallback = any(str(item.get("stage", "")).startswith("fallback") for item in trimmed)
            if (not has_fallback) and any(str(item.get("stage", "")).startswith("fallback") for item in attempt_chain):
                first_fallback = next(
                    (item for item in attempt_chain if str(item.get("stage", "")).startswith("fallback")),
                    None,
                )
                if first_fallback is not None and trimmed:
                    trimmed[-1] = first_fallback
            attempt_chain = trimmed

        row_started = time.monotonic()
        row_hard_timeout_seconds = max(
            2700,
            int((fallback_timeout_seconds or runtime.get("fallback", {}).get("timeout_seconds") or 2400) + 300),
        )
        last_error = ""
        last_error_type = ""
        last_failed_model = ""
        last_failed_base_url = ""
        last_failed_stage = ""
        blocked_models_due_to_rate_limit: set[str] = set()
        row_attempt_details: list[dict[str, Any]] = []
        row_max_prompt_size_chars = 0
        row_last_response_preview = ""
        for attempt in attempt_chain:
            elapsed = int(time.monotonic() - row_started)
            remaining = int(row_hard_timeout_seconds - elapsed)
            if remaining <= 0:
                last_error = "row_hard_timeout_exceeded"
                last_error_type = "timeout"
                break
            attempt_model = str(attempt.get("model") or "")
            if bool(attempt.get("is_retry")) and attempt_model in blocked_models_due_to_rate_limit:
                continue
            llm_attempts_total += 1
            stage = str(attempt.get("stage") or "")
            context = attempt.get("context") if isinstance(attempt.get("context"), dict) else full_context
            messages = _build_llm_messages(
                context,
                repair_mode=bool(attempt.get("repair_mode", False)),
                previous_error=last_error,
            )
            prompt_size_chars = sum(len(str(item.get("content") or "")) for item in messages)
            row_max_prompt_size_chars = max(row_max_prompt_size_chars, prompt_size_chars)
            if stage == "main":
                request_record["main_messages"] = messages
            elif stage == "main_repair":
                request_record["main_repair_messages"] = messages
            elif stage == "main_compact_retry":
                request_record["main_compact_retry_messages"] = messages
            elif stage == "fallback":
                request_record["fallback_messages"] = messages
            elif stage == "fallback_repair":
                request_record["fallback_repair_messages"] = messages
            elif stage == "fallback_compact_retry":
                request_record["fallback_compact_retry_messages"] = messages
            elif stage == "fallback2":
                request_record["fallback2_messages"] = messages
            elif stage == "fallback2_repair":
                request_record["fallback2_repair_messages"] = messages
            elif stage == "fallback2_compact_retry":
                request_record["fallback2_compact_retry_messages"] = messages

            payload, meta = _call_llm(
                model=str(attempt.get("model") or ""),
                base_url=str(attempt.get("base_url") or ""),
                timeout_seconds=max(1, min(int(attempt.get("timeout") or 120), remaining)),
                messages=messages,
            )
            meta_error = str(meta.get("error") or "")
            meta_error_type = classify_llm_error(meta_error)
            response_preview = ""
            if payload is not None:
                try:
                    response_preview = json.dumps(payload, ensure_ascii=False)[:500]
                except Exception:
                    response_preview = str(payload)[:500]
            elif meta_error:
                response_preview = meta_error[:500]
            row_last_response_preview = response_preview or row_last_response_preview
            no_retry_due_to_rate_limit = bool(
                runtime.get("no_retry_on_rate_limit", True) and meta_error_type == "cloud_usage_limit"
            )
            if no_retry_due_to_rate_limit and attempt_model:
                blocked_models_due_to_rate_limit.add(attempt_model)
            if meta_error:
                last_error = meta_error
                last_error_type = meta_error_type
                last_failed_model = attempt_model
                last_failed_base_url = str(attempt.get("base_url") or "")
                last_failed_stage = stage
            llm_responses.append(
                {
                    "row_index": idx,
                    "stage": stage,
                    "candidate": str(attempt.get("candidate") or ""),
                    "model": str(attempt.get("model") or ""),
                    "base_url": str(attempt.get("base_url") or ""),
                    "meta": meta,
                    "error_type": meta_error_type,
                    "no_retry_due_to_rate_limit": no_retry_due_to_rate_limit,
                    "prompt_size_chars": prompt_size_chars,
                    "response_preview": response_preview,
                    "payload": payload,
                }
            )
            row_attempt_details.append(
                {
                    "stage": stage,
                    "candidate": str(attempt.get("candidate") or ""),
                    "model": str(attempt.get("model") or ""),
                    "base_url": str(attempt.get("base_url") or ""),
                    "prompt_size_chars": prompt_size_chars,
                    "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
                    "error": meta_error,
                    "error_type": meta_error_type,
                    "response_preview": response_preview,
                    "no_retry_due_to_rate_limit": no_retry_due_to_rate_limit,
                }
            )

            if payload is None:
                last_error = str(meta.get("error") or "llm_payload_empty")
                last_error_type = classify_llm_error(last_error)
                last_failed_model = attempt_model
                last_failed_base_url = str(attempt.get("base_url") or "")
                last_failed_stage = stage
                continue

            valid, validation_errors, lint = _validate_llm_payload(payload)
            if not valid:
                last_error = "invalid_schema:" + ",".join(validation_errors)
                last_error_type = classify_llm_error(last_error)
                meta = {**meta, "error": last_error, "lint": lint}
                row_attempt_details[-1]["error"] = last_error
                row_attempt_details[-1]["error_type"] = last_error_type
                last_failed_stage = stage
                continue

            selected_payload = payload
            selected_meta = meta
            selected_backend = stage
            if bool(meta.get("repair_applied")):
                llm_json_repair_count += 1
            if stage == "main":
                llm_success_main += 1
            elif stage == "main_repair":
                llm_success_main_repair += 1
            elif stage == "main_compact_retry":
                llm_success_main_compact_retry += 1
            elif stage == "fallback":
                llm_success_fallback += 1
            elif stage == "fallback_repair":
                llm_success_fallback_repair += 1
            elif stage == "fallback_compact_retry":
                llm_success_fallback_compact_retry += 1
            elif stage == "fallback2":
                llm_success_fallback2 += 1
            elif stage == "fallback2_repair":
                llm_success_fallback2_repair += 1
            elif stage == "fallback2_compact_retry":
                llm_success_fallback2_compact_retry += 1
            break

        if selected_payload is None:
            llm_failed_count += 1
            row = _build_quarantine_row(group, source_run_id=source_run_id)
            llm_responses.append(
                {
                    "row_index": idx,
                    "stage": "quarantined_llm_failed",
                    "model": "",
                    "meta": {"ok": False, "error": str(last_error or "llm_json_invalid")},
                    "payload": {},
                }
            )
            selected_backend = "quarantined_llm_failed"
            selected_meta = {"ok": False, "error": str(last_error or "llm_json_invalid")}
            quarantined_rows.append(
                {
                    "row_index": idx,
                    "manager_name": group.manager_name,
                    "control_day_date": group.control_day_date,
                    "reason": str(last_error or "llm_json_invalid"),
                    "error_type": str(last_error_type or classify_llm_error(last_error or "")),
                    "failed_model": last_failed_model,
                    "failed_base_url": last_failed_base_url,
                    "failure_stage": last_failed_stage,
                    "prompt_size_chars": int(row_max_prompt_size_chars or 0),
                    "raw_response_preview": str(row_last_response_preview or "")[:500],
                    "models_attempted": [
                        str(item.get("model") or "")
                        for item in row_attempt_details
                        if isinstance(item, dict) and str(item.get("model") or "")
                    ],
                    "errors_by_attempt": [
                        {
                            "stage": str(item.get("stage") or ""),
                            "model": str(item.get("model") or ""),
                            "error_type": str(item.get("error_type") or ""),
                            "error": str(item.get("error") or ""),
                        }
                        for item in row_attempt_details
                        if isinstance(item, dict)
                    ],
                    "attempt_trace": row_attempt_details,
                    "fallback_reason": ("main_cloud_usage_limit" if str(last_error_type) == "cloud_usage_limit" else ""),
                    "analysis_backend_used": "quarantined_llm_failed",
                }
            )
        else:
            row = _row_from_llm_payload(
                group=group,
                payload=selected_payload,
                backend=selected_backend,
                source_run_id=source_run_id,
            )

        request_record["selected_backend"] = selected_backend
        request_record["selected_meta"] = selected_meta
        llm_requests.append(request_record)

        if logger is not None:
            logger.info(
                "daily_control llm row=%s manager=%s date=%s backend=%s",
                idx,
                group.manager_name,
                group.control_day_date,
                selected_backend,
            )

        rows.append(row)

    diagnostics = {
        "llm_runtime": {
            "main": runtime["main"],
            "fallback": runtime["fallback"],
            "fallback2": runtime.get("fallback2", {}),
            "selected": "mixed",
            "reason": "daily_llm_first",
            "preflight": preflight,
        },
        "llm_attempts_total": llm_attempts_total,
        "llm_success_main": llm_success_main,
        "llm_success_main_repair": llm_success_main_repair,
        "llm_success_main_compact_retry": llm_success_main_compact_retry,
        "llm_success_fallback": llm_success_fallback,
        "llm_success_fallback_repair": llm_success_fallback_repair,
        "llm_success_fallback_compact_retry": llm_success_fallback_compact_retry,
        "llm_success_fallback2": llm_success_fallback2,
        "llm_success_fallback2_repair": llm_success_fallback2_repair,
        "llm_success_fallback2_compact_retry": llm_success_fallback2_compact_retry,
        "llm_json_repair_count": llm_json_repair_count,
        "llm_failed_count": llm_failed_count,
        "fallback_used_count": int(
            llm_success_fallback
            + llm_success_fallback_repair
            + llm_success_fallback_compact_retry
            + llm_success_fallback2
            + llm_success_fallback2_repair
            + llm_success_fallback2_compact_retry
        ),
        "rows_failed_rate_limit": sum(
            1 for row in quarantined_rows if str(row.get("error_type") or "") == "cloud_usage_limit"
        ),
        "rows_failed_timeout": sum(
            1 for row in quarantined_rows if str(row.get("error_type") or "") == "timeout"
        ),
        "rows_failed_invalid_json": sum(
            1 for row in quarantined_rows if str(row.get("error_type") or "") == "invalid_json"
        ),
        "rows_skipped_no_runtime": sum(
            1 for row in quarantined_rows if str(row.get("reason") or "") == "no_runtime"
        ),
        "rows_recovered_by_local_fallback": (
            int(llm_success_fallback + llm_success_fallback_repair)
            if "gpt-oss" in str(runtime.get("fallback", {}).get("model", "")).lower()
            else (
                int(llm_success_fallback2 + llm_success_fallback2_repair)
                if "gpt-oss" in str(runtime.get("fallback2", {}).get("model", "")).lower()
                else 0
            )
        ),
        "quarantined_count": len(quarantined_rows),
        "quarantined_rows": quarantined_rows,
        "llm_requests": llm_requests,
        "llm_responses": llm_responses,
        "max_prompt_size_chars_seen": max(
            [int(item.get("prompt_size_chars", 0) or 0) for item in llm_responses if isinstance(item, dict)] or [0]
        ),
        "top_data_limitations": [
            row.get("data_limitations", "")
            for row in rows
            if str(row.get("data_limitations", "")).strip()
        ][:5],
    }
    return rows, diagnostics
