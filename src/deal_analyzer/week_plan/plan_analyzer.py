from __future__ import annotations

import json
import time
from typing import Any

from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError
from src.deal_analyzer.llm_runtime import classify_llm_error

from ..daily_control.source_reader import day_label_from_iso
from ..weekly_shared.roks_oap import build_manager_metric_interpretation
from .idempotency import build_exact_key
from .models import WeekPlanSignalGroup


ITEM_FIELDS: tuple[str, ...] = (
    "date",
    "day",
    "recipient",
    "activity_type",
    "priority",
    "what_i_do",
    "task_to_assign",
    "what_to_check",
    "daily_meeting_thesis",
    "training_link",
    "post_training_task_link",
    "expected_quantity_effect",
    "expected_quality_effect",
    "status",
)


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
    messages = [
        {"role": "system", "content": "Верни строго валидный JSON-объект без markdown."},
        {"role": "user", "content": 'Ответь строго JSON-объектом: {"ok": true}'},
    ]
    payload, meta = _call_llm(
        model=model,
        base_url=base_url,
        timeout_seconds=max(1, int(timeout_seconds or 20)),
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
                or cfg.ollama_fallback_preflight_timeout_seconds
                or cfg.ollama_preflight_timeout_seconds
                or 20
            ),
        },
    }


def _build_group_context(group: WeekPlanSignalGroup, roks_snapshot: dict[str, Any], *, compact: bool = False) -> dict[str, Any]:
    manager_metrics = (
        (roks_snapshot.get("manager_metrics") or {}) if isinstance(roks_snapshot.get("manager_metrics"), dict) else {}
    ).get(group.manager_name, {})
    weekly_fact = (
        (manager_metrics.get("weekly_fact") or {})
        if isinstance(manager_metrics, dict) and isinstance(manager_metrics.get("weekly_fact"), dict)
        else {}
    )
    metric_interpretation = (
        weekly_fact.get("metric_interpretation", {})
        if isinstance(weekly_fact.get("metric_interpretation"), dict)
        else build_manager_metric_interpretation(
            manager_name=group.manager_name,
            manager_role_profile=group.manager_role_profile,
            weekly_fact={
                "interest_fact": weekly_fact.get("interest_fact"),
                "demo_fact": weekly_fact.get("demo_fact"),
                "test_fact": weekly_fact.get("test_fact"),
                "invoice_count_fact": weekly_fact.get("invoice_count_fact"),
                "payment_count_fact": weekly_fact.get("payment_count_fact"),
            },
        )
    )
    source_daily_rows: list[dict[str, Any]] = []
    for item in group.source_rows[: (4 if compact else 12)]:
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
    payload = {
        "period_start": group.period_start,
        "period_end": group.period_end,
        "plan_week_start": group.plan_week_start,
        "plan_week_end": group.plan_week_end,
        "manager_name": group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "source_day_count": group.source_day_count,
        "deals_count": group.deals_count,
        "calls_count": group.calls_count,
        "avg_score_0_100": group.avg_score_0_100,
        "deal_links": group.deal_links[:20],
        "product_mix_week": group.product_mix_week,
        "base_mix_week": group.base_mix_week,
        "repeated_growth_zones": group.repeated_growth_zones[:10],
        "repeated_strong_sides": group.repeated_strong_sides[:10],
        "repeated_fix_points": group.repeated_fix_points[:10],
        "repeated_messages": group.repeated_messages[:10],
        "training_signal_count": group.training_signal_count,
        "criticality_histogram": group.criticality_histogram,
        "source_daily_rows": source_daily_rows,
        "roks_snapshot_status": roks_snapshot.get("status", ""),
        "roks_manager_metrics": manager_metrics,
        "roks_metric_interpretation": metric_interpretation,
    }
    if compact:
        payload["context_mode"] = "compact_retry"
    return payload


def _build_llm_messages(
    context: dict[str, Any],
    *,
    repair_mode: bool = False,
    previous_error: str = "",
    allowed_activity_types: list[str] | None = None,
) -> list[dict[str, str]]:
    schema = {
        "items": [
            {
                "date": "YYYY-MM-DD",
                "day": "понедельник",
                "recipient": "",
                "activity_type": "обучение",
                "priority": "high",
                "what_i_do": "",
                "task_to_assign": "",
                "what_to_check": "",
                "daily_meeting_thesis": "",
                "training_link": "",
                "post_training_task_link": "",
                "expected_quantity_effect": "",
                "expected_quality_effect": "",
                "status": "запланировано",
            }
        ]
    }
    system = (
        "Ты формируешь план недели руководителя продаж. Верни только валидный JSON без markdown. "
        "Пиши только на русском. Не выдумывай факты и ссылки. "
        "Не используй общие формулировки типа 'провести работу' или 'улучшить коммуникацию'. "
        "Каждое действие должно быть проверяемым: что делаю, какую задачу даю, что проверяю. "
        "Разрешенные термины: LINK, INFO, PLM, CRM, amoCRM. "
        "Если ссылки нет, оставь training_link/post_training_task_link пустыми строками. "
        "Учитывай роль-ориентированную интерпретацию РОКС: это не всегда линейная персональная воронка. "
        "Для профиля, который проводит демо, допустимо планировать демо выше self-generated 'есть интерес' "
        "за счет встреч, переданных другими источниками. Для top-funnel профиля делай акцент на верх воронки."
    )
    if repair_mode:
        system += " Режим repair: исправь JSON и язык, верни только JSON-объект со списком items."

    normalized_allowed_activity_types = [
        _safe_text(item).lower()
        for item in (allowed_activity_types or [])
        if _safe_text(item)
    ]
    if not normalized_allowed_activity_types:
        normalized_allowed_activity_types = [
            "дейлик",
            "личный разбор",
            "обучение",
            "контроль",
            "задача",
            "отдел",
            "стратегия",
            "операционная",
            "развитие",
            "стратегическая",
        ]

    user_payload = {
        "schema": schema,
        "context": context,
        "repair_reason": previous_error,
        "activity_types": normalized_allowed_activity_types,
        "activity_type_rule": "Используй только значения из activity_types без новых вариантов.",
        "priority_values": ["high", "medium", "low"],
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def _extract_plan_items(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("items"), list):
        return [item for item in payload.get("items", []) if isinstance(item, dict)]
    if isinstance(payload.get("rows"), list):
        return [item for item in payload.get("rows", []) if isinstance(item, dict)]
    if all(field in payload for field in ITEM_FIELDS):
        return [payload]
    return []


def _normalize_priority(value: Any) -> str:
    probe = _safe_text(value).lower()
    if probe in {"высокий", "high"}:
        return "high"
    if probe in {"средний", "medium"}:
        return "medium"
    if probe in {"низкий", "low"}:
        return "low"
    return ""


def _validate_item(item: dict[str, Any], *, default_recipient: str) -> tuple[bool, list[str], dict[str, Any]]:
    normalized = {
        "date": _safe_text(item.get("date")),
        "day": _safe_text(item.get("day")),
        "recipient": _safe_text(item.get("recipient")) or default_recipient,
        "activity_type": _safe_text(item.get("activity_type")).lower(),
        "priority": _normalize_priority(item.get("priority")),
        "what_i_do": _safe_text(item.get("what_i_do")),
        "task_to_assign": _safe_text(item.get("task_to_assign")),
        "what_to_check": _safe_text(item.get("what_to_check")),
        "daily_meeting_thesis": _safe_text(item.get("daily_meeting_thesis")),
        "training_link": _safe_text(item.get("training_link")),
        "post_training_task_link": _safe_text(item.get("post_training_task_link")),
        "expected_quantity_effect": _safe_text(item.get("expected_quantity_effect")),
        "expected_quality_effect": _safe_text(item.get("expected_quality_effect")),
        "status": _safe_text(item.get("status")) or "запланировано",
    }
    if not normalized["day"] and normalized["date"]:
        normalized["day"] = day_label_from_iso(normalized["date"])

    errors: list[str] = []
    for field in ITEM_FIELDS:
        if field in {"training_link", "post_training_task_link"}:
            continue
        if not normalized.get(field):
            errors.append(f"empty_field:{field}")

    if not normalized["date"]:
        errors.append("empty_field:date")

    return len(errors) == 0, errors, normalized


def _row_from_item(
    *,
    group: WeekPlanSignalGroup,
    item: dict[str, Any],
    backend: str,
    source_run_id: str,
) -> dict[str, Any]:
    row = {
        "plan_week_start": group.plan_week_start,
        "plan_week_end": group.plan_week_end,
        "plan_date": item.get("date", ""),
        "day_label": item.get("day", ""),
        "recipient": item.get("recipient", "") or group.manager_name,
        "manager_role_profile": group.manager_role_profile,
        "activity_type": item.get("activity_type", ""),
        "priority": item.get("priority", ""),
        "what_i_do": item.get("what_i_do", ""),
        "task_to_assign": item.get("task_to_assign", ""),
        "what_to_check": item.get("what_to_check", ""),
        "daily_meeting_thesis": item.get("daily_meeting_thesis", ""),
        "training_link": item.get("training_link", ""),
        "post_training_task_link": item.get("post_training_task_link", ""),
        "expected_quantity_effect": item.get("expected_quantity_effect", ""),
        "expected_quality_effect": item.get("expected_quality_effect", ""),
        "status": item.get("status", "запланировано"),
        "source_deals_count": int(group.deals_count or 0),
        "source_calls_count": int(group.calls_count or 0),
        "source_day_count": int(group.source_day_count or 0),
        "analysis_backend_used": backend,
        "source_run_id": source_run_id,
    }
    row["idempotency_key"] = build_exact_key(row)
    return row


def analyze_week_plan_groups(
    *,
    groups: list[WeekPlanSignalGroup],
    cfg: Any,
    roks_snapshot: dict[str, Any],
    llm_runtime: dict[str, Any],
    logger: Any,
    source_run_id: str,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    llm_max_attempts: int = 6,
    allowed_activity_types: list[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime=llm_runtime,
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
    )

    preflight: dict[str, Any] = {}
    for name in ("main", "fallback"):
        node = runtime.get(name, {})
        if not isinstance(node, dict):
            preflight[name] = {"ok": False, "error": "node_missing"}
            continue
        if name == "fallback" and not bool(node.get("enabled", False)):
            preflight[name] = {"ok": False, "error": "candidate_disabled"}
            continue
        result = _preflight_model(
            model=str(node.get("model") or ""),
            base_url=str(node.get("base_url") or ""),
            timeout_seconds=int(node.get("preflight_timeout_seconds", 20) or 20),
        )
        preflight[name] = {
            **result,
            "error_type": classify_llm_error(str(result.get("error") or "")),
        }
        if logger is not None:
            if bool(result.get("ok", False)):
                logger.info(
                    "week_plan llm preflight ok candidate=%s model=%s elapsed_ms=%s",
                    name,
                    str(node.get("model") or ""),
                    int(result.get("elapsed_ms", 0) or 0),
                )
            else:
                logger.warning(
                    "week_plan llm preflight error candidate=%s model=%s elapsed_ms=%s error=%s",
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

    for idx, group in enumerate(groups):
        full_context = _build_group_context(group, roks_snapshot, compact=False)
        compact_context = _build_group_context(group, roks_snapshot, compact=True)

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

        selected_items: list[dict[str, Any]] = []
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
                allowed_activity_types=allowed_activity_types,
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
            llm_responses.append({"group_index": idx, **trace_item, "payload": payload})

            if payload is None:
                last_error = error or "llm_payload_empty"
                last_error_type = classify_llm_error(last_error)
                continue

            items = _extract_plan_items(payload)
            valid_items: list[dict[str, Any]] = []
            validation_errors: list[str] = []
            for item in items:
                ok, item_errors, normalized_item = _validate_item(item, default_recipient=group.manager_name)
                if ok:
                    valid_items.append(normalized_item)
                else:
                    validation_errors.extend(item_errors)

            if not valid_items:
                last_error = "invalid_schema:" + ",".join(sorted(set(validation_errors or ["empty_items"])))
                last_error_type = classify_llm_error(last_error)
                row_attempt_trace[-1]["error"] = last_error
                row_attempt_trace[-1]["error_type"] = last_error_type
                continue

            selected_items = valid_items
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

        if not selected_items:
            llm_failed_count += 1
            quarantined_rows.append(
                {
                    "group_index": idx,
                    "manager_name": group.manager_name,
                    "plan_week_start": group.plan_week_start,
                    "plan_week_end": group.plan_week_end,
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
                    "group_index": idx,
                    "group_key": f"{group.plan_week_start}|{group.plan_week_end}|{group.manager_name}",
                    "selected_backend": "quarantined_llm_failed",
                    "selected_meta": {"ok": False, "error": last_error},
                    "attempt_trace": row_attempt_trace,
                }
            )
            continue

        produced_rows = [
            _row_from_item(
                group=group,
                item=item,
                backend=selected_backend,
                source_run_id=source_run_id,
            )
            for item in selected_items
        ]
        rows.extend(produced_rows)

        llm_requests.append(
            {
                "group_index": idx,
                "group_key": f"{group.plan_week_start}|{group.plan_week_end}|{group.manager_name}",
                "selected_backend": selected_backend,
                "selected_meta": selected_meta,
                "attempt_trace": row_attempt_trace,
                "produced_rows": len(produced_rows),
            }
        )

        if logger is not None:
            logger.info(
                "week_plan llm group=%s manager=%s week=%s..%s backend=%s rows=%s",
                idx,
                group.manager_name,
                group.plan_week_start,
                group.plan_week_end,
                selected_backend,
                len(produced_rows),
            )

    diagnostics = {
        "llm_runtime": {
            "main": runtime.get("main", {}),
            "fallback": runtime.get("fallback", {}),
            "preflight": preflight,
            "selected": "mixed",
            "reason": "week_plan_llm_first",
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
    }
    return rows, diagnostics
