from __future__ import annotations

import json
import time
from typing import Any

from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError

from .models import DailyControlInputGroup


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


def _build_llm_messages(context: dict[str, Any], *, repair_mode: bool = False, previous_error: str = "") -> list[dict[str, str]]:
    system = (
        "Ты руководитель активных продаж. Верни строго JSON без markdown. "
        "Не придумывай факты, используй только входные данные. "
        "Пиши на русском, без английского и без китайского текста. "
        "Не используй фразу 'Лучше сказать:'. "
        "Если данных мало - честно укажи это в data_limitations. "
        "Для базы/тегов сортируй по частоте от более частых к редким. "
        "Ожидаемый эффект формулируй как управленческую гипотезу, не как точный прогноз."
    )
    if repair_mode:
        system += " Режим repair: исправь JSON, сохрани смысл, верни только валидный объект."

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
    if text in {"low", "medium", "high"}:
        return text
    if text in {"низкая", "низкий"}:
        return "low"
    if text in {"средняя", "средний"}:
        return "medium"
    if text in {"высокая", "высокий", "критическая", "critical"}:
        return "high"
    if score <= 40:
        return "high"
    if score <= 69:
        return "medium"
    return "low"


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
    started = time.perf_counter()
    try:
        client = OllamaClient(base_url=base_url, model=model, timeout_seconds=max(1, int(timeout_seconds or 30)))
        probe = client.preflight(probe_timeout_seconds=max(1, int(timeout_seconds or 30)))
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {
            "ok": bool(probe.ok),
            "error": str(probe.error or ""),
            "elapsed_ms": elapsed_ms,
        }
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return {"ok": False, "error": str(exc), "elapsed_ms": elapsed_ms}


def _runtime_from_config(
    *,
    cfg: Any,
    llm_runtime: dict[str, Any],
    main_model_override: str | None,
    fallback_model_override: str | None,
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
        or "gemma4:31b-cloud"
    ).strip()
    fallback_model = str(fallback_model_override or "").strip() or str(
        ((llm_runtime.get("fallback") or {}) if isinstance(llm_runtime.get("fallback"), dict) else {}).get("model")
        or cfg.ollama_fallback_model
        or "deepseek-v3.1:671b-cloud"
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
    return {"main": main, "fallback": fallback}


def _build_group_context(group: DailyControlInputGroup, roks_snapshot: dict[str, Any]) -> dict[str, Any]:
    manager_metrics = (
        (roks_snapshot.get("manager_metrics") or {}) if isinstance(roks_snapshot.get("manager_metrics"), dict) else {}
    ).get(group.manager_name, {})

    source_cases = []
    for row in group.source_rows[:6]:
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

    return {
        "period_start": group.period_start,
        "period_end": group.period_end,
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
        "source_cases": source_cases,
        "limitations": limitations,
    }


def _validate_llm_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(payload, dict):
        return False, ["payload_not_object"]
    for field in LLM_REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing_field:{field}")
    score = _safe_score(payload.get("score_0_100"))
    if score < 0 or score > 100:
        errors.append("invalid_score")
    criticality = _normalize_criticality(payload.get("criticality"), score)
    if criticality not in {"low", "medium", "high"}:
        errors.append("invalid_criticality")
    return len(errors) == 0, errors


def _build_minimal_fallback_row(group: DailyControlInputGroup, *, source_run_id: str) -> dict[str, Any]:
    marker = "не сформировано: llm_json_invalid"
    return {
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
        "main_pattern": marker,
        "strong_sides": marker,
        "growth_zones": marker,
        "why_it_matters": marker,
        "what_to_reinforce": marker,
        "what_to_fix": marker,
        "what_to_tell_employee": marker,
        "expected_quant_impact": marker,
        "expected_qual_impact": marker,
        "score_0_100": 0,
        "criticality": "high",
        "analysis_backend_used": "deterministic_fallback",
        "source_run_id": source_run_id,
        "training_needed": False,
        "training_topic": "",
        "evidence_short": marker,
        "data_limitations": marker,
    }


def _row_from_llm_payload(
    *,
    group: DailyControlInputGroup,
    payload: dict[str, Any],
    backend: str,
    source_run_id: str,
) -> dict[str, Any]:
    score = _safe_score(payload.get("score_0_100"))
    criticality = _normalize_criticality(payload.get("criticality"), score)
    return {
        "period_start": group.period_start,
        "period_end": group.period_end,
        "control_day_date": str(payload.get("date") or group.control_day_date),
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
        "criticality": criticality,
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
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime=llm_runtime,
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
    )
    preflight = {
        "main": _preflight_model(
            model=runtime["main"]["model"],
            base_url=runtime["main"]["base_url"],
            timeout_seconds=runtime["main"]["preflight_timeout_seconds"],
        ),
        "fallback": {
            "ok": False,
            "error": "fallback_disabled",
            "elapsed_ms": 0,
        },
    }
    if bool(runtime["fallback"].get("enabled")) and str(runtime["fallback"].get("model") or ""):
        preflight["fallback"] = _preflight_model(
            model=runtime["fallback"]["model"],
            base_url=runtime["fallback"]["base_url"],
            timeout_seconds=int(getattr(cfg, "ollama_fallback_preflight_timeout_seconds", 20) or 20),
        )
    main_available = bool(preflight["main"].get("ok", False))
    fallback_available = bool(preflight["fallback"].get("ok", False))

    rows: list[dict[str, Any]] = []
    llm_requests: list[dict[str, Any]] = []
    llm_responses: list[dict[str, Any]] = []

    llm_success_main = 0
    llm_success_fallback = 0
    llm_json_repair_count = 0
    llm_failed_count = 0

    for idx, group in enumerate(packages):
        context = _build_group_context(group, roks_snapshot)
        request_record = {
            "row_index": idx,
            "group_key": f"{group.control_day_date}|{group.manager_name}",
            "context": context,
        }

        selected_backend = ""
        selected_payload: dict[str, Any] | None = None
        selected_meta: dict[str, Any] = {}

        # Main attempt
        if main_available:
            main_messages = _build_llm_messages(context)
            request_record["main_messages"] = main_messages
            payload, meta = _call_llm(
                model=runtime["main"]["model"],
                base_url=runtime["main"]["base_url"],
                timeout_seconds=runtime["main"]["timeout_seconds"],
                messages=main_messages,
            )
            llm_responses.append(
                {
                    "row_index": idx,
                    "stage": "main",
                    "model": runtime["main"]["model"],
                    "meta": meta,
                    "payload": payload,
                }
            )
        else:
            payload, meta = None, {"ok": False, "error": "main_preflight_failed", "elapsed_ms": 0, "repair_applied": False}

        valid = False
        if payload is not None:
            valid, validation_errors = _validate_llm_payload(payload)
            if not valid:
                meta = {**meta, "error": "invalid_schema:" + ",".join(validation_errors)}
            else:
                validation_errors = []
        else:
            validation_errors = []

        if valid:
            selected_backend = "main"
            selected_payload = payload
            selected_meta = meta
            llm_success_main += 1
            if bool(meta.get("repair_applied")):
                llm_json_repair_count += 1
        else:
            # one repair retry on main
            if main_available:
                repair_messages = _build_llm_messages(context, repair_mode=True, previous_error=str(meta.get("error") or ""))
                request_record["main_repair_messages"] = repair_messages
                repair_payload, repair_meta = _call_llm(
                    model=runtime["main"]["model"],
                    base_url=runtime["main"]["base_url"],
                    timeout_seconds=runtime["main"]["timeout_seconds"],
                    messages=repair_messages,
                )
                llm_responses.append(
                    {
                        "row_index": idx,
                        "stage": "main_repair",
                        "model": runtime["main"]["model"],
                        "meta": repair_meta,
                        "payload": repair_payload,
                    }
                )
            else:
                repair_payload, repair_meta = None, {"ok": False, "error": "main_preflight_failed", "elapsed_ms": 0, "repair_applied": False}
            repair_valid = False
            if repair_payload is not None:
                repair_valid, repair_errors = _validate_llm_payload(repair_payload)
                if not repair_valid:
                    repair_meta = {**repair_meta, "error": "invalid_schema:" + ",".join(repair_errors)}
            if repair_valid:
                selected_backend = "main"
                selected_payload = repair_payload
                selected_meta = repair_meta
                llm_success_main += 1
                llm_json_repair_count += 1
            else:
                # fallback
                if bool(runtime["fallback"].get("enabled")) and str(runtime["fallback"].get("model") or "") and fallback_available:
                    fallback_messages = _build_llm_messages(context)
                    request_record["fallback_messages"] = fallback_messages
                    fb_payload, fb_meta = _call_llm(
                        model=runtime["fallback"]["model"],
                        base_url=runtime["fallback"]["base_url"],
                        timeout_seconds=runtime["fallback"]["timeout_seconds"],
                        messages=fallback_messages,
                    )
                    llm_responses.append(
                        {
                            "row_index": idx,
                            "stage": "fallback",
                            "model": runtime["fallback"]["model"],
                            "meta": fb_meta,
                            "payload": fb_payload,
                        }
                    )
                    fb_valid = False
                    if fb_payload is not None:
                        fb_valid, fb_errors = _validate_llm_payload(fb_payload)
                        if not fb_valid:
                            fb_meta = {**fb_meta, "error": "invalid_schema:" + ",".join(fb_errors)}
                    if fb_valid:
                        selected_backend = "fallback"
                        selected_payload = fb_payload
                        selected_meta = fb_meta
                        llm_success_fallback += 1
                        if bool(fb_meta.get("repair_applied")):
                            llm_json_repair_count += 1

        if selected_payload is None:
            llm_failed_count += 1
            row = _build_minimal_fallback_row(group, source_run_id=source_run_id)
            llm_responses.append(
                {
                    "row_index": idx,
                    "stage": "deterministic_fallback",
                    "model": "",
                    "meta": {"ok": False, "error": "llm_json_invalid"},
                    "payload": {},
                }
            )
            selected_backend = "deterministic_fallback"
            selected_meta = {"ok": False, "error": "llm_json_invalid"}
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
            "selected": "mixed",
            "reason": "daily_llm_first",
            "preflight": preflight,
        },
        "llm_success_main": llm_success_main,
        "llm_success_fallback": llm_success_fallback,
        "llm_json_repair_count": llm_json_repair_count,
        "llm_failed_count": llm_failed_count,
        "llm_requests": llm_requests,
        "llm_responses": llm_responses,
        "top_data_limitations": [
            row.get("data_limitations", "")
            for row in rows
            if str(row.get("data_limitations", "")).strip()
        ][:5],
    }
    return rows, diagnostics
