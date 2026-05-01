from __future__ import annotations

import json
import re
import time
from typing import Any

from src.deal_analyzer.employee_profiles.analyzer import (
    build_employee_profile_context,
    sanitize_employee_text,
)
from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError
from src.deal_analyzer.llm_runtime import classify_llm_error

from .models import WeekSummaryGroup


WEEK_SUMMARY_REQUIRED_FIELDS: tuple[str, ...] = (
    "brief_report",
    "quantity_delta",
    "quality_delta",
    "what_failed",
    "focus_next_week",
    "next_week_plan",
    "meeting_message",
    "strategic_accents",
    "risks",
    "manager_report_phrase",
)


def _safe_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _sanitize_role_based_phrase(value: Any) -> str:
    out = sanitize_employee_text(_safe_text(value))
    out = re.sub(
        r"\b(?:сам\s+)?назначил[аи]?\s+(\d+)\s+демо\b",
        lambda match: f"провел {match.group(1)} демо",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"(массов\w*\s+обзвон|20\s+звонк\w*\s+по\s+баз\w*|прозвон\w*\s+баз\w*|наборы|дозвоны)",
        "фокус на теплой/текущей воронке и контроле следующего шага",
        out,
        flags=re.IGNORECASE,
    )
    out = re.sub(
        r"(давить|продавл\w+|агрессивн\w+\s+продаж\w+|презент\w+\s+все\s+функц\w+)",
        "вести клиента через consultative demo: guided discovery, hands-on действие и следующий шаг",
        out,
        flags=re.IGNORECASE,
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
    payload, meta = _call_llm(model=model, base_url=base_url, timeout_seconds=timeout_seconds, messages=messages)
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


def _build_group_context(
    group: WeekSummaryGroup,
    roks_snapshot: dict[str, Any],
    *,
    compact: bool = False,
    client_priority_summary: dict[str, Any] | None = None,
    employee_profiles_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    employee_profiles_context: dict[str, Any] = {}
    for row in group.source_manager_rows:
        if not isinstance(row, dict):
            continue
        manager_name = _safe_text(row.get("manager_name"))
        if not manager_name:
            continue
        employee_profiles_context[manager_name] = build_employee_profile_context(
            manager_name=manager_name,
            manager_role_profile=_safe_text(row.get("manager_role_profile")),
            source_rows=[row],
            registry_raw=employee_profiles_registry,
        )
    return {
        "period_start": group.period_start,
        "period_end": group.period_end,
        "week_start": group.week_start,
        "week_end": group.week_end,
        "managers_count": group.managers_count,
        "deals_count": group.deals_count,
        "avg_score_0_100": group.avg_score_0_100,
        "planned_actions_total": group.planned_actions_total,
        "done_actions_count": group.done_actions_count,
        "in_progress_actions_count": group.in_progress_actions_count,
        "postponed_actions_count": group.postponed_actions_count,
        "no_status_actions_count": group.no_status_actions_count,
        "training_links": group.training_links[:12],
        "post_training_task_links": group.post_training_task_links[:12],
        "unresolved_actions": group.unresolved_actions[:12],
        "manager_rows": group.source_manager_rows[: (4 if compact else 12)],
        "plan_rows": group.source_plan_rows[: (4 if compact else 12)],
        "roks_snapshot_status": roks_snapshot.get("status", ""),
        "roks_metrics": roks_snapshot.get("manager_metrics", {}),
        "client_list_priority_summary": client_priority_summary or {},
        "employee_profiles": employee_profiles_context,
        "context_mode": "compact_retry" if compact else "normal",
    }


def _build_llm_messages(context: dict[str, Any], *, repair_mode: bool = False, previous_error: str = "") -> list[dict[str, str]]:
    schema = {
        "brief_report": "",
        "quantity_delta": "",
        "quality_delta": "",
        "what_failed": "",
        "focus_next_week": "",
        "next_week_plan": "",
        "meeting_message": "",
        "strategic_accents": "",
        "risks": "",
        "manager_report_phrase": "",
    }
    system = (
        "Ты формируешь свод недели для руководителя продаж. Верни только валидный JSON без markdown. "
        "Пиши только на русском. Не придумывай факты и ссылки. Используй только факты из context. "
        "Не давай рекомендации sales_manager в формате массового холодного обзвона; "
        "для sales_manager фокус на теплой/текущей воронке, демо/тест/счет/оплата и next step. "
        "Рекомендации по демо формулируй через consultative demo / guided discovery / hands-on demonstration, "
        "без давления и без подхода 'показать все функции'."
        " Если в context есть client_list_priority_summary, учитывай эти приоритеты для фокуса sales_manager на теплых клиентах."
        " Если в context есть employee_profiles, учитывай персональные стили коучинга: "
        "direct_accountability — жестко по ответственности без унижения; "
        "expert_to_expert — через профессиональный рост и коммерческий результат."
    )
    system += (
        " ROKS manager funnel is role-based, not always strictly linear per one manager; "
        "do not enforce hard demo<=interest assumptions in per-manager conclusions."
    )
    if repair_mode:
        system += " Режим repair: исправь JSON и язык, верни только JSON-объект."
    user_payload = {"schema": schema, "context": context, "repair_reason": previous_error}
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def _validate_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    if not isinstance(payload, dict):
        return False, ["payload_not_object"]
    errors: list[str] = []
    for field in WEEK_SUMMARY_REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"missing_field:{field}")
        elif not _safe_text(payload.get(field)):
            errors.append(f"empty_field:{field}")
    return len(errors) == 0, errors


def _row_from_payload(*, group: WeekSummaryGroup, payload: dict[str, Any], backend: str, source_run_id: str) -> dict[str, Any]:
    return {
        "week_start": group.week_start,
        "week_end": group.week_end,
        "brief_report": _sanitize_role_based_phrase(payload.get("brief_report")),
        "quantity_delta": _sanitize_role_based_phrase(payload.get("quantity_delta")),
        "quality_delta": _sanitize_role_based_phrase(payload.get("quality_delta")),
        "what_failed": _sanitize_role_based_phrase(payload.get("what_failed")),
        "focus_next_week": _sanitize_role_based_phrase(payload.get("focus_next_week")),
        "next_week_plan": _sanitize_role_based_phrase(payload.get("next_week_plan")),
        "meeting_message": _sanitize_role_based_phrase(payload.get("meeting_message")),
        "strategic_accents": _sanitize_role_based_phrase(payload.get("strategic_accents")),
        "risks": _sanitize_role_based_phrase(payload.get("risks")),
        "manager_report_phrase": _sanitize_role_based_phrase(payload.get("manager_report_phrase")),
        "deals_count": int(group.deals_count or 0),
        "managers_count": int(group.managers_count or 0),
        "planned_actions_total": int(group.planned_actions_total or 0),
        "done_actions_count": int(group.done_actions_count or 0),
        "analysis_backend_used": backend,
        "source_run_id": source_run_id,
    }


def analyze_week_summary_groups(
    *,
    groups: list[WeekSummaryGroup],
    cfg: Any,
    roks_snapshot: dict[str, Any],
    llm_runtime: dict[str, Any],
    logger: Any,
    source_run_id: str,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    llm_max_attempts: int = 6,
    client_priority_summary: dict[str, Any] | None = None,
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
                    "week_summary llm preflight ok candidate=%s model=%s elapsed_ms=%s",
                    name,
                    str(node.get("model") or ""),
                    int(result.get("elapsed_ms", 0) or 0),
                )
            else:
                logger.warning(
                    "week_summary llm preflight error candidate=%s model=%s elapsed_ms=%s error=%s",
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
        full_context = _build_group_context(
            group,
            roks_snapshot,
            compact=False,
            client_priority_summary=client_priority_summary,
            employee_profiles_registry=getattr(cfg, "employee_profiles", None),
        )
        employee_profiles_payload = (
            full_context.get("employee_profiles", {})
            if isinstance(full_context.get("employee_profiles"), dict)
            else {}
        )
        for manager_name, profile_node in employee_profiles_payload.items():
            if not isinstance(profile_node, dict):
                continue
            employee_profile_context_rows.append(
                {
                    "manager_name": manager_name,
                    "week_start": group.week_start,
                    "week_end": group.week_end,
                    "communication_style": profile_node.get("communication_style", ""),
                    "motivators": profile_node.get("motivators", []),
                    "avoid": profile_node.get("avoid", []),
                    "profile_source": profile_node.get("profile_source", ""),
                }
            )
            marker_payload = profile_node.get("behavior_markers", {})
            if isinstance(marker_payload, dict):
                employee_behavior_marker_rows.append(
                    {
                        "manager_name": manager_name,
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
        compact_context = _build_group_context(
            group,
            roks_snapshot,
            compact=True,
            client_priority_summary=client_priority_summary,
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
            payload, meta = _call_llm(model=model, base_url=base_url, timeout_seconds=timeout, messages=messages)
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
            quarantined_rows.append(
                {
                    "row_index": idx,
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
                    "group_key": f"{group.week_start}|{group.week_end}",
                    "selected_backend": "quarantined_llm_failed",
                    "selected_meta": {"ok": False, "error": last_error},
                    "attempt_trace": row_attempt_trace,
                }
            )
            continue

        row = _row_from_payload(group=group, payload=selected_payload, backend=selected_backend, source_run_id=source_run_id)
        rows.append(row)
        llm_requests.append(
            {
                "row_index": idx,
                "group_key": f"{group.week_start}|{group.week_end}",
                "selected_backend": selected_backend,
                "selected_meta": selected_meta,
                "attempt_trace": row_attempt_trace,
            }
        )

    diagnostics = {
        "llm_runtime": {
            "main": runtime.get("main", {}),
            "fallback": runtime.get("fallback", {}),
            "preflight": preflight,
            "selected": "mixed",
            "reason": "week_summary_llm_first",
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
        "employee_profile_context_rows": employee_profile_context_rows,
        "employee_behavior_marker_rows": employee_behavior_marker_rows,
    }
    return rows, diagnostics

