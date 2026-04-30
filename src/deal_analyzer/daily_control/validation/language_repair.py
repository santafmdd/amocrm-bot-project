from __future__ import annotations

import json
import re
import time
from typing import Any

from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError
from src.deal_analyzer.llm_runtime import classify_llm_error

from .text_lint import DAILY_NARRATIVE_FIELDS, lint_daily_text_rows, lint_has_blockers


FOREIGN_GREETING_RE = re.compile(r"(你好|您好|hello\b|hi\b|greetings)", re.IGNORECASE)
MARKDOWN_FENCE_RE = re.compile(r"```")
CONTROL_CHARS_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]")
REQUIRED_NARRATIVE_FIELDS: tuple[str, ...] = (
    "main_pattern",
    "growth_zones",
    "what_to_fix",
    "what_to_tell_employee",
    "expected_quant_impact",
    "expected_qual_impact",
)


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = CONTROL_CHARS_RE.sub("", text)
    text = MARKDOWN_FENCE_RE.sub("", text)
    text = FOREIGN_GREETING_RE.sub("", text)
    text = " ".join(text.replace("\n", " ").split())
    return text.strip()


def _needs_llm_repair(row: dict[str, Any], lint: dict[str, Any]) -> bool:
    marker = "не сформировано: llm_json_invalid"
    for field in DAILY_NARRATIVE_FIELDS:
        if marker in str(row.get(field, "") or ""):
            return True
    for field in REQUIRED_NARRATIVE_FIELDS:
        if not _clean_text(row.get(field, "")):
            return True
    return lint_has_blockers(lint)


def _build_repair_messages(fields_payload: dict[str, str]) -> list[dict[str, str]]:
    system = (
        "Ты редактор управленческих заметок по продажам. "
        "Верни только валидный JSON без markdown: {\"fields\":{...}}. "
        "Сохрани смысл и факты. Перепиши пользовательские поля на русском. "
        "Удали китайский/английский мусор, приветствия и технические служебные вставки. "
        "Допустимые рабочие термины при необходимости: LINK, INFO, PLM, CRM, amoCRM, ROKS, OAP, ID, URL. "
        "Не меняй даты, id, ссылки, числовые значения и имена менеджеров."
    )
    user = json.dumps({"fields": fields_payload}, ensure_ascii=False)
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _repair_with_model(*, model: str, base_url: str, timeout_seconds: int, fields_payload: dict[str, str]) -> tuple[dict[str, str] | None, dict[str, Any]]:
    started = time.perf_counter()
    try:
        client = OllamaClient(base_url=base_url, model=model, timeout_seconds=max(1, int(timeout_seconds or 60)))
        parsed = client.chat_json(messages=_build_repair_messages(fields_payload))
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        payload = parsed.payload if isinstance(parsed.payload, dict) else {}
        fields = payload.get("fields", {}) if isinstance(payload.get("fields"), dict) else {}
        return ({str(k): str(v or "") for k, v in fields.items()} if fields else None), {
            "ok": bool(fields),
            "error": "" if fields else "empty_fields",
            "model": model,
            "elapsed_ms": elapsed_ms,
            "repair_applied": bool(parsed.repair_applied),
        }
    except (OllamaClientError, ValueError, TypeError) as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return None, {
            "ok": False,
            "error": str(exc),
            "model": model,
            "elapsed_ms": elapsed_ms,
            "repair_applied": False,
        }


def _build_attempts(*, cfg: Any, llm_runtime: dict[str, Any] | None, max_attempts: int) -> list[dict[str, Any]]:
    runtime = dict(llm_runtime or {})
    main_node = runtime.get("main", {}) if isinstance(runtime.get("main"), dict) else {}
    fallback_node = runtime.get("fallback", {}) if isinstance(runtime.get("fallback"), dict) else {}

    main_model = str(main_node.get("model") or getattr(cfg, "ollama_model", "") or "qwen3.5:397b-cloud").strip()
    main_base_url = str(main_node.get("base_url") or getattr(cfg, "ollama_base_url", "") or "http://127.0.0.1:11434").strip()
    main_timeout = int(main_node.get("timeout_seconds") or getattr(cfg, "ollama_timeout_seconds", 120) or 120)

    fallback_enabled = bool(fallback_node.get("enabled", True))
    fallback_model = str(
        fallback_node.get("model")
        or getattr(cfg, "ollama_fallback_model", "")
        or "deepseek-v3.1:671b-cloud"
    ).strip()
    fallback_base_url = str(
        fallback_node.get("base_url")
        or getattr(cfg, "ollama_fallback_base_url", "")
        or getattr(cfg, "ollama_base_url", "")
        or "http://127.0.0.1:11434"
    ).strip()
    fallback_timeout = int(
        fallback_node.get("timeout_seconds")
        or getattr(cfg, "ollama_fallback_timeout_seconds", 120)
        or getattr(cfg, "ollama_timeout_seconds", 120)
        or 120
    )
    fallback2_node = runtime.get("fallback2", {}) if isinstance(runtime.get("fallback2"), dict) else {}
    fallback2_enabled = bool(fallback2_node.get("enabled", False))
    fallback2_model = str(fallback2_node.get("model") or "").strip()
    fallback2_base_url = str(
        fallback2_node.get("base_url")
        or getattr(cfg, "ollama_fallback_base_url", "")
        or getattr(cfg, "ollama_base_url", "")
        or "http://127.0.0.1:11434"
    ).strip()
    fallback2_timeout = int(
        fallback2_node.get("timeout_seconds")
        or getattr(cfg, "ollama_fallback_timeout_seconds", 120)
        or getattr(cfg, "ollama_timeout_seconds", 120)
        or 120
    )

    sequence: list[dict[str, Any]] = []
    if main_model and main_base_url:
        sequence.append({"kind": "main", "model": main_model, "base_url": main_base_url, "timeout_seconds": max(30, main_timeout)})
    if fallback_enabled and fallback_model and fallback_base_url:
        sequence.append({"kind": "fallback", "model": fallback_model, "base_url": fallback_base_url, "timeout_seconds": max(30, fallback_timeout)})
    if fallback2_enabled and fallback2_model and fallback2_base_url:
        sequence.append({"kind": "fallback2", "model": fallback2_model, "base_url": fallback2_base_url, "timeout_seconds": max(30, fallback2_timeout)})

    if not sequence:
        return []

    attempts: list[dict[str, Any]] = []
    max_attempts = max(1, int(max_attempts or 1))
    index = 0
    while len(attempts) < max_attempts:
        attempts.append(sequence[index % len(sequence)])
        index += 1
    return attempts


def repair_language_rows(
    *,
    rows: list[dict[str, Any]],
    cfg: Any,
    llm_runtime: dict[str, Any] | None,
    logger: Any,
    max_attempts: int = 3,
    enable_llm_repair: bool = True,
) -> dict[str, Any]:
    repaired_rows: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []

    fields_repaired_count = 0
    rows_repaired_count = 0
    rows_recovered_from_quarantine = 0
    llm_repair_attempts = 0
    llm_repair_success = 0
    llm_repair_failed = 0

    attempts = _build_attempts(cfg=cfg, llm_runtime=llm_runtime, max_attempts=max_attempts)

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        original = dict(row)
        candidate = dict(row)
        row_fields_repaired = 0

        for field in DAILY_NARRATIVE_FIELDS:
            if field not in candidate:
                continue
            before = str(candidate.get(field, "") or "")
            after = _clean_text(before)
            if before != after:
                row_fields_repaired += 1
                fields_repaired_count += 1
            candidate[field] = after

        lint = lint_daily_text_rows([candidate])
        blocker = _needs_llm_repair(candidate, lint)
        repair_trace: list[dict[str, Any]] = []

        if blocker and enable_llm_repair and attempts:
            blocked_models_due_to_rate_limit: set[str] = set()
            for attempt_no, attempt in enumerate(attempts, start=1):
                model_name = str(attempt.get("model") or "")
                if model_name in blocked_models_due_to_rate_limit:
                    continue
                llm_repair_attempts += 1
                payload = {field: str(candidate.get(field, "") or "") for field in DAILY_NARRATIVE_FIELDS if field in candidate}
                repaired_fields, meta = _repair_with_model(
                    model=model_name,
                    base_url=str(attempt.get("base_url") or ""),
                    timeout_seconds=int(attempt.get("timeout_seconds") or 120),
                    fields_payload=payload,
                )
                meta_error = str(meta.get("error") or "")
                meta_error_type = classify_llm_error(meta_error)
                no_retry_due_to_rate_limit = bool(
                    (llm_runtime.get("no_retry_on_rate_limit", True) if isinstance(llm_runtime, dict) else True)
                    and meta_error_type == "cloud_usage_limit"
                )
                if no_retry_due_to_rate_limit and model_name:
                    blocked_models_due_to_rate_limit.add(model_name)
                trace_item = {"attempt": attempt_no, **meta, "kind": attempt.get("kind", "")}
                trace_item["error_type"] = meta_error_type
                trace_item["no_retry_due_to_rate_limit"] = no_retry_due_to_rate_limit
                repair_trace.append(trace_item)
                if not repaired_fields:
                    llm_repair_failed += 1
                    continue

                llm_repair_success += 1
                for field, value in repaired_fields.items():
                    if field in DAILY_NARRATIVE_FIELDS and field in candidate:
                        new_value = _clean_text(value)
                        if str(candidate.get(field, "") or "") != new_value:
                            row_fields_repaired += 1
                            fields_repaired_count += 1
                        candidate[field] = new_value

                lint = lint_daily_text_rows([candidate])
                if not _needs_llm_repair(candidate, lint):
                    break

        final_blocker = _needs_llm_repair(candidate, lint)
        initial_backend = str(original.get("analysis_backend_used") or "")
        if not final_blocker and initial_backend == "quarantined_llm_failed":
            candidate["analysis_backend_used"] = "language_repair_recovered"
            rows_recovered_from_quarantine += 1

        if final_blocker:
            quarantine_reason = "language_blocker_unrepaired"
            if str(candidate.get("analysis_backend_used") or "") == "quarantined_llm_failed":
                quarantine_reason = "llm_failed_before_language_repair"
            quarantined_rows.append(
                {
                    "row_index": row_index,
                    "manager_name": str(candidate.get("manager_name") or ""),
                    "control_day_date": str(candidate.get("control_day_date") or ""),
                    "reason": quarantine_reason,
                    "lint": lint,
                    "repair_trace": repair_trace,
                    "row": candidate,
                }
            )
        else:
            repaired_rows.append(candidate)
            if row_fields_repaired > 0:
                rows_repaired_count += 1

        details.append(
            {
                "row_index": row_index,
                "manager_name": str(original.get("manager_name") or ""),
                "control_day_date": str(original.get("control_day_date") or ""),
                "repaired_fields_count": row_fields_repaired,
                "quarantined": final_blocker,
                "lint_after": lint,
                "repair_trace": repair_trace,
            }
        )

        if logger is not None and final_blocker:
            logger.warning(
                "daily_control language repair quarantined row=%s manager=%s date=%s",
                row_index,
                candidate.get("manager_name", ""),
                candidate.get("control_day_date", ""),
            )

    aggregate_lint = lint_daily_text_rows(repaired_rows)
    metrics = {
        "rows_total": len(rows),
        "rows_repaired_count": rows_repaired_count,
        "rows_quarantined_count": len(quarantined_rows),
        "fields_repaired_count": fields_repaired_count,
        "unrepaired_blockers_count": len(quarantined_rows),
        "rows_recovered_from_quarantine": rows_recovered_from_quarantine,
        "allowed_latin_terms_count": int(aggregate_lint.get("allowed_latin_terms_count", 0) or 0),
        "technical_terms_warning_count": int(aggregate_lint.get("technical_terms_warning_count", 0) or 0),
        "llm_repair_attempts": llm_repair_attempts,
        "llm_repair_success": llm_repair_success,
        "llm_repair_failed": llm_repair_failed,
    }

    return {
        "rows": repaired_rows,
        "quarantined_rows": quarantined_rows,
        "details": details,
        "metrics": metrics,
        "lint_after_repair": aggregate_lint,
    }


def build_language_repair_markdown(payload: dict[str, Any]) -> list[str]:
    metrics = payload.get("metrics", {}) if isinstance(payload.get("metrics"), dict) else {}
    lines = [
        f"rows_total: {metrics.get('rows_total', 0)}",
        f"rows_repaired_count: {metrics.get('rows_repaired_count', 0)}",
        f"rows_recovered_from_quarantine: {metrics.get('rows_recovered_from_quarantine', 0)}",
        f"rows_quarantined_count: {metrics.get('rows_quarantined_count', 0)}",
        f"fields_repaired_count: {metrics.get('fields_repaired_count', 0)}",
        f"unrepaired_blockers_count: {metrics.get('unrepaired_blockers_count', 0)}",
        f"allowed_latin_terms_count: {metrics.get('allowed_latin_terms_count', 0)}",
        f"technical_terms_warning_count: {metrics.get('technical_terms_warning_count', 0)}",
        f"llm_repair_attempts: {metrics.get('llm_repair_attempts', 0)}",
        f"llm_repair_success: {metrics.get('llm_repair_success', 0)}",
        f"llm_repair_failed: {metrics.get('llm_repair_failed', 0)}",
    ]
    quarantined = payload.get("quarantined_rows", []) if isinstance(payload.get("quarantined_rows"), list) else []
    if quarantined:
        lines.append("")
        lines.append("quarantined rows:")
        for item in quarantined[:20]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- row={item.get('row_index')} manager={item.get('manager_name','')} date={item.get('control_day_date','')} reason={item.get('reason','')}"
            )
    return lines
