from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from .call_review_v3_builder import build_call_review_v3_payload
from .cli import (
    _call_review_case_has_meaningful_conversation,
    _call_review_llm_fields_ready,
    _llm_chat_json_with_runtime,
    _maybe_write_call_review_sheet,
    _normalize_call_review_case_mode,
    _prepare_call_review_llm_fields,
    _repair_call_review_llm_fields,
    _sanitize_call_review_llm_fields,
)
from .config import load_deal_analyzer_config
from .llm_runtime import classify_llm_error, resolve_ollama_runtime


SINGLE_PASS_DEFAULT_LIMIT = 5
DEFAULT_SHEET_NAME = "Разбор звонков"
MAX_STYLE_CHARS_DEFAULT = 4000
MAX_REFERENCE_CHARS_DEFAULT = 4000
MAX_TRANSCRIPT_CHARS_DEFAULT = 9000
MAX_PROMPT_CHARS_DEFAULT = 18000
EXPERIMENTAL_REPLAY_WARNING = "EXPERIMENTAL: not recommended for production call review write"

CASE_TYPE_BY_MODE: dict[str, str] = {
    "secretary_analysis": "разговор с секретарем",
    "negotiation_lpr_analysis": "разговор с лпр",
    "warm_case": "теплый входящий",
    "supplier_inbound_analysis": "входящий от поставщика",
    "redial_discipline_analysis": "недозвоны / дисциплина дозвонов",
    "confirm_demo_analysis": "подтверждение презентации",
    "presentation_analysis": "презентация",
    "test_analysis": "работа с тестом",
    "dozhim_analysis": "дожим",
}

STAGE_GROUPS_BY_MODE: dict[str, str] = {
    "secretary_analysis": "secretary,speech,crm,discipline",
    "negotiation_lpr_analysis": "lpr,need,presentation,closing,objections,speech,crm",
    "warm_case": "lpr,need,presentation,closing,objections,speech,crm",
    "supplier_inbound_analysis": "lpr,need,presentation,closing,speech,crm",
    "redial_discipline_analysis": "discipline,crm",
    "confirm_demo_analysis": "confirm_demo,speech,crm",
    "presentation_analysis": "demo,objections,speech,crm",
    "test_analysis": "test,objections,speech,crm",
    "dozhim_analysis": "dozhim,objections,speech,crm",
}


def _read_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_md(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay call review LLM-only pipeline on existing run artifacts "
            "(no STT/download/live refresh)."
        )
    )
    parser.add_argument("--run-dir", required=True, help="Existing analyze-period run directory")
    parser.add_argument(
        "--config",
        default="config/deal_analyzer.call_review.deepseek.realwrite.json",
        help="Deal analyzer config path",
    )
    parser.add_argument("--main-model", default="gemma4:31b-cloud")
    parser.add_argument("--fallback-model", default="gpt-oss:20b")
    parser.add_argument("--fallback2-model", default="deepseek-v3.1:671b-cloud")
    parser.add_argument("--main-timeout", type=int, default=0)
    parser.add_argument("--fallback-timeout", type=int, default=2400)
    parser.add_argument("--fallback2-timeout", type=int, default=0)
    parser.add_argument("--max-style-chars", type=int, default=MAX_STYLE_CHARS_DEFAULT)
    parser.add_argument("--max-reference-chars", type=int, default=MAX_REFERENCE_CHARS_DEFAULT)
    parser.add_argument("--max-transcript-chars", type=int, default=MAX_TRANSCRIPT_CHARS_DEFAULT)
    parser.add_argument("--max-prompt-chars", type=int, default=MAX_PROMPT_CHARS_DEFAULT)
    parser.add_argument("--limit", type=int, default=None, help="Rows to replay (default dry-run safe bound)")
    parser.add_argument("--offset", type=int, default=0, help="Offset in selected items")
    parser.add_argument("--deal-id", action="append", default=[], help="Replay only specific deal_id (can repeat)")
    parser.add_argument("--max-runtime-minutes", type=int, default=0)
    parser.add_argument("--max-llm-calls", type=int, default=0)
    parser.add_argument("--allow-full-run", action="store_true")
    parser.add_argument("--single-pass-json", dest="single_pass_json", action="store_true", default=True)
    parser.add_argument("--no-single-pass-json", dest="single_pass_json", action="store_false")
    parser.add_argument("--stop-on-rate-limit", action="store_true")
    parser.add_argument("--no-retry-on-rate-limit", action="store_true")
    parser.add_argument("--skip-cloud-on-rate-limit", action="store_true")
    parser.add_argument("--allow-partial-write", action="store_true")
    parser.add_argument("--quarantine-failed", action="store_true")
    parser.add_argument("--dry-run", action="store_true", help="Force writer dry-run")
    parser.add_argument("--write", action="store_true", help="Enable real write (disabled by default)")
    parser.add_argument(
        "--allow-experimental-gemma-write",
        action="store_true",
        help="Allow real write when main model is gemma/gemma4 (experimental only)",
    )
    parser.add_argument("--strict-preflight", action="store_true")
    return parser.parse_args()


def _is_cloud_model_name(model: str) -> bool:
    low = str(model or "").strip().lower()
    return bool(low and ("cloud" in low))


def _is_gemma_model_name(model: str) -> bool:
    low = str(model or "").strip().lower()
    return low.startswith("gemma") or "gemma4" in low


def _experimental_gemma_write_block_reason(
    *,
    write_requested: bool,
    main_model: str,
    allow_experimental_gemma_write: bool,
) -> str:
    if bool(write_requested and _is_gemma_model_name(main_model) and not allow_experimental_gemma_write):
        return "experimental_gemma_write_requires_explicit_allow_flag"
    return ""


def _messages_char_count(messages: list[dict[str, str]]) -> int:
    total = 0
    for message in messages:
        if not isinstance(message, dict):
            continue
        total += len(str(message.get("content") or ""))
    return total


def _truncate_chars(value: str, max_chars: int) -> str:
    text = str(value or "").strip()
    limit = max(0, int(max_chars or 0))
    if not text or limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit].rstrip()


def _resolve_call_review_llm_runtime(
    cfg: Any,
    logger: Any,
    *,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    fallback2_model_override: str | None = None,
    fallback_timeout_override: int | None = None,
    no_retry_on_rate_limit: bool = True,
    no_retry_on_context_overflow: bool = False,
) -> dict[str, Any]:
    runtime = resolve_ollama_runtime(
        cfg=cfg,
        enabled=cfg.analyzer_backend in {"hybrid", "ollama"},
        logger=logger,
        log_prefix="call review llm",
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
        fallback2_model_override=fallback2_model_override,
        fallback_timeout_override=fallback_timeout_override,
        no_retry_on_rate_limit=no_retry_on_rate_limit,
    )
    runtime["no_retry_on_context_overflow"] = bool(no_retry_on_context_overflow)
    return runtime


def _preflight_status_by_candidate(runtime: dict[str, Any], candidate: str) -> str:
    preflight = runtime.get("preflight_results", []) if isinstance(runtime.get("preflight_results"), list) else []
    for item in preflight:
        if not isinstance(item, dict):
            continue
        if str(item.get("candidate") or "") != candidate:
            continue
        if bool(item.get("ok")):
            return "ok"
        err_type = str(item.get("error_type") or "unknown")
        return f"failed:{err_type}"
    return "missing"


def _build_runtime_with_blocked_models(
    runtime: dict[str, Any],
    *,
    blocked_models: set[str],
    skip_cloud_on_rate_limit: bool,
) -> tuple[dict[str, Any], int]:
    if not skip_cloud_on_rate_limit or not blocked_models:
        return dict(runtime), 0
    blocked = {str(x).strip().lower() for x in blocked_models if str(x).strip()}
    out = json.loads(json.dumps(runtime, ensure_ascii=False)) if isinstance(runtime, dict) else {}
    removed_count = 0
    for source in ("main", "fallback", "fallback2"):
        cfg = out.get(source, {}) if isinstance(out.get(source), dict) else {}
        model = str(cfg.get("model") or "").strip()
        if model and model.lower() in blocked and _is_cloud_model_name(model):
            cfg["enabled"] = False
            out[source] = cfg
            removed_count += 1
    selected = "none"
    reason = "no_runtime_after_rate_limit"
    for source in ("main", "fallback", "fallback2"):
        cfg = out.get(source, {}) if isinstance(out.get(source), dict) else {}
        if bool(cfg.get("enabled", True)) and str(cfg.get("model") or "").strip():
            selected = source
            reason = "runtime_routed_after_rate_limit"
            break
    out["selected"] = selected
    out["reason"] = reason
    return out, removed_count


def _extract_selected_items(run_dir: Path) -> list[dict[str, Any]]:
    selected_path = run_dir / "call_review_v3" / "selected_anchor_cases.json"
    payload = _read_json(selected_path, default=[])
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("selected_items", "items", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
    shortlist_path = run_dir / "analysis_shortlist.json"
    shortlist = _read_json(shortlist_path, default={})
    if isinstance(shortlist, dict):
        value = shortlist.get("selected_items")
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _expand_deal_ids(raw_values: list[str]) -> set[str]:
    out: set[str] = set()
    for value in raw_values:
        parts = [x.strip() for x in str(value or "").replace(";", ",").split(",")]
        for part in parts:
            if part:
                out.add(part)
    return out


def _resolve_effective_limit(*, limit_arg: int | None, write_requested: bool, allow_full_run: bool) -> int:
    if limit_arg is None:
        if not write_requested:
            return SINGLE_PASS_DEFAULT_LIMIT
        if not allow_full_run:
            raise ValueError("full replay write requires --allow-full-run or explicit --limit")
        return 0
    limit = int(limit_arg)
    if limit < 0:
        raise ValueError("--limit must be >= 0")
    if limit == 0 and not allow_full_run:
        raise ValueError("full replay requires --allow-full-run")
    return limit


def _filter_selected_items(
    selected_items: list[dict[str, Any]],
    *,
    deal_ids: set[str],
    offset: int,
    limit: int,
) -> list[dict[str, Any]]:
    filtered = selected_items
    if deal_ids:
        filtered = [x for x in filtered if str(x.get("deal_id") or "").strip() in deal_ids]
    if offset > 0:
        filtered = filtered[offset:]
    if limit > 0:
        filtered = filtered[:limit]
    return filtered


def _collect_transcript_from_snapshot(snapshot: dict[str, Any]) -> tuple[str, str, int, int, float]:
    transcripts = snapshot.get("transcripts", []) if isinstance(snapshot.get("transcripts"), list) else []
    parts: list[str] = []
    segments_count = 0
    longest_segment_sec = 0.0
    for item in transcripts:
        if not isinstance(item, dict):
            continue
        text = str(item.get("transcript_text") or "").strip()
        if text:
            parts.append(text)
        segments = item.get("transcript_segments")
        if not isinstance(segments, list):
            segments = item.get("segments")
        if isinstance(segments, list):
            segments_count += len(segments)
            for segment in segments:
                if not isinstance(segment, dict):
                    continue
                try:
                    start_sec = float(segment.get("start", 0) or 0.0)
                except Exception:
                    start_sec = 0.0
                try:
                    end_sec = float(segment.get("end", 0) or 0.0)
                except Exception:
                    end_sec = 0.0
                longest_segment_sec = max(longest_segment_sec, max(0.0, end_sec - start_sec))
    full_text = "\n".join(parts).strip()
    excerpt = full_text[:9000].strip()
    return full_text, excerpt, len(full_text), segments_count, longest_segment_sec


def _record_from_deal_artifact(
    *,
    deal_id: str,
    candidate: dict[str, Any],
    deal_artifact_path: Path,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    payload = _read_json(deal_artifact_path, default={})
    if not isinstance(payload, dict):
        return None, {"deal_id": deal_id, "reason": "invalid_deal_artifact_json", "path": str(deal_artifact_path)}
    snapshot = payload.get("snapshot", {}) if isinstance(payload.get("snapshot"), dict) else {}
    analysis = payload.get("analysis", {}) if isinstance(payload.get("analysis"), dict) else {}
    crm = snapshot.get("crm", {}) if isinstance(snapshot.get("crm"), dict) else {}

    transcript_text, transcript_excerpt_from_snapshot, transcript_len, segments_count, longest_segment_sec = _collect_transcript_from_snapshot(
        snapshot
    )
    transcript_excerpt = str(analysis.get("transcript_text_excerpt") or transcript_excerpt_from_snapshot or "").strip()
    if not transcript_text:
        transcript_text = transcript_excerpt
    transcript_len = int(analysis.get("transcript_text_len", 0) or transcript_len or len(transcript_text))

    missing_transcript = not bool(transcript_excerpt.strip() or transcript_text.strip())
    if missing_transcript:
        missing = {
            "deal_id": deal_id,
            "reason": "missing_transcript_text",
            "path": str(deal_artifact_path),
        }
    else:
        missing = None

    record = {
        "deal_id": str(analysis.get("deal_id") or crm.get("deal_id") or deal_id),
        "deal_name": str(analysis.get("deal_name") or crm.get("deal_name") or ""),
        "owner_name": str(crm.get("responsible_user_name") or analysis.get("owner_name") or ""),
        "company_name": str(crm.get("company_name") or ""),
        "status_name": str(crm.get("status_name") or ""),
        "pipeline_name": str(crm.get("pipeline_name") or ""),
        "source_values": crm.get("source_values", []) if isinstance(crm.get("source_values"), list) else [],
        "tags": crm.get("tags", []) if isinstance(crm.get("tags"), list) else [],
        "company_tags": crm.get("company_tags", []) if isinstance(crm.get("company_tags"), list) else [],
        "product_hypothesis": str(analysis.get("product_hypothesis_llm") or analysis.get("product_hypothesis") or ""),
        "transcript_text": transcript_text,
        "transcript_text_excerpt": transcript_excerpt,
        "transcript_text_len": transcript_len,
        "transcript_segments_count": segments_count,
        "transcript_longest_segment_sec": float(longest_segment_sec or 0.0),
        "transcript_usability_label": str(analysis.get("transcript_usability_label") or ""),
        "transcript_usability_score_final": int(analysis.get("transcript_usability_score_final", 0) or 0),
        "call_signal_summary_short": str(analysis.get("call_signal_summary_short") or ""),
        "call_signal_next_step_present": bool(analysis.get("call_signal_next_step_present")),
        "call_signal_decision_maker_reached": bool(analysis.get("call_signal_decision_maker_reached")),
        "call_signal_demo_discussed": bool(analysis.get("call_signal_demo_discussed")),
        "call_signal_objection_price": bool(analysis.get("call_signal_objection_price")),
        "call_signal_objection_no_need": bool(analysis.get("call_signal_objection_no_need")),
        "call_signal_objection_not_target": bool(analysis.get("call_signal_objection_not_target")),
        "crm_consistency_summary": str(analysis.get("crm_consistency_summary") or ""),
        "analysis_confidence": str(analysis.get("analysis_confidence") or ""),
        "selected_call_count": int(candidate.get("selected_call_count", 0) or 0),
        "selected_call_ids": candidate.get("selected_call_ids", []) if isinstance(candidate.get("selected_call_ids"), list) else [],
        "artifact_path": str(deal_artifact_path),
    }
    return record, missing


def _build_period_deal_records(run_dir: Path, selected_items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    missing_transcripts: list[dict[str, Any]] = []
    deals_dir = run_dir / "deals"

    for item in selected_items:
        deal_id = str(item.get("deal_id") or "").strip()
        if not deal_id:
            continue
        deal_path = deals_dir / f"deal_{deal_id}.json"
        if not deal_path.exists():
            missing_transcripts.append({"deal_id": deal_id, "reason": "deal_artifact_missing", "path": str(deal_path)})
            continue
        record, missing = _record_from_deal_artifact(
            deal_id=deal_id,
            candidate=item,
            deal_artifact_path=deal_path,
        )
        if isinstance(record, dict):
            records.append(record)
        if isinstance(missing, dict):
            missing_transcripts.append(missing)
    return records, missing_transcripts


def _strip_markdown_and_think(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    low = value.lower()
    if "<think>" in low:
        value = value.replace("<think>", " ").replace("</think>", " ")
    if value.startswith("```"):
        lines = value.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        value = "\n".join(lines)
    value = " ".join(value.split())
    value = value.replace("Лучше сказать:", "Используй:")
    return value.strip()


def _to_int_score(value: Any, default: int = 55) -> int:
    try:
        score = int(float(value))
    except Exception:
        return default
    return max(0, min(100, score))


def _build_single_pass_messages(
    *,
    record: dict[str, Any],
    candidate: dict[str, Any],
    case_mode: str,
    max_style_chars: int,
    max_reference_chars: int,
    max_transcript_chars: int,
    max_prompt_chars: int,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    case_type_display = CASE_TYPE_BY_MODE.get(case_mode, "разговор с лпр")
    transcript_excerpt_raw = str(record.get("transcript_text_excerpt") or "").strip()
    style_context_raw = " ".join(
        x
        for x in [
            str(record.get("call_signal_summary_short") or "").strip(),
            str(record.get("crm_consistency_summary") or "").strip(),
            str(record.get("analysis_confidence") or "").strip(),
        ]
        if x
    ).strip()
    reference_context_raw = " ".join(
        x
        for x in [
            str(record.get("product_hypothesis") or "").strip(),
            ", ".join(record.get("tags", []) if isinstance(record.get("tags"), list) else []),
            ", ".join(record.get("company_tags", []) if isinstance(record.get("company_tags"), list) else []),
            ", ".join(record.get("source_values", []) if isinstance(record.get("source_values"), list) else []),
        ]
        if x
    ).strip()
    transcript_excerpt = _truncate_chars(transcript_excerpt_raw, max_transcript_chars)
    style_context = _truncate_chars(style_context_raw, max_style_chars)
    reference_context = _truncate_chars(reference_context_raw, max_reference_chars)

    system_prompt = (
        "Ты руководитель активных продаж. Верни строго один JSON-объект без markdown и без текста вне JSON. "
        "Все пользовательские поля только на русском. Не используй <think> и технические пояснения про API/JSON/LLM. "
        "Не используй фразу 'Лучше сказать:'. Если нужен речевой модуль, пиши 'Используй: ...'. "
        "Разрешенные рабочие термины при необходимости: LINK, INFO, PLM, CRM, amoCRM, API, KPI."
    )
    contract = {
        "case_summary": "краткий итог разговора по делу",
        "main_issue": "главная проблема разговора",
        "strong_sides": "сильные стороны",
        "growth_zones": "зоны роста",
        "what_to_fix": "что конкретно исправить",
        "what_to_tell_employee": "что донести сотруднику",
        "better_phrase": "одна фраза в формате 'Используй: ...' или пусто",
        "expected_effect_quantity": "ожидаемый количественный эффект",
        "expected_effect_quality": "ожидаемый качественный эффект",
        "risk_level": "low|medium|high",
        "quality_score_0_100": 0,
        "data_limitations": "ограничения данных",
        "evidence_quote": "одна короткая цитата по делу",
    }

    def _compose_messages() -> list[dict[str, str]]:
        context = {
            "deal_id": str(record.get("deal_id") or ""),
            "deal_name": str(record.get("deal_name") or ""),
            "manager_name": str(record.get("owner_name") or ""),
            "company_name": str(record.get("company_name") or ""),
            "status_name": str(record.get("status_name") or ""),
            "pipeline_name": str(record.get("pipeline_name") or ""),
            "case_mode": case_mode,
            "case_type_display": case_type_display,
            "selected_call_count": int(
                candidate.get("selected_call_count", 0) or record.get("selected_call_count", 0) or 0
            ),
            "selected_call_ids": candidate.get("selected_call_ids", [])
            if isinstance(candidate.get("selected_call_ids"), list)
            else [],
            "style_context": style_context,
            "reference_context": reference_context,
            "transcript_excerpt": transcript_excerpt,
        }
        user_prompt = (
            "Контекст кейса:\n"
            + json.dumps(context, ensure_ascii=False, indent=2)
            + "\n\nВерни JSON строго по схеме:\n"
            + json.dumps(contract, ensure_ascii=False, indent=2)
        )
        return [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

    messages = _compose_messages()
    max_prompt = max(0, int(max_prompt_chars or 0))
    if max_prompt > 0:
        for _ in range(8):
            current_prompt_chars = _messages_char_count(messages)
            if current_prompt_chars <= max_prompt:
                break
            overflow = current_prompt_chars - max_prompt
            reduced = False
            if transcript_excerpt and len(transcript_excerpt) > 1200:
                shrink = min(len(transcript_excerpt) - 1200, max(400, overflow + 200))
                transcript_excerpt = transcript_excerpt[: max(0, len(transcript_excerpt) - shrink)].rstrip()
                reduced = True
            elif style_context and len(style_context) > 700:
                shrink = min(len(style_context) - 700, max(200, overflow))
                style_context = style_context[: max(0, len(style_context) - shrink)].rstrip()
                reduced = True
            elif reference_context and len(reference_context) > 700:
                shrink = min(len(reference_context) - 700, max(200, overflow))
                reference_context = reference_context[: max(0, len(reference_context) - shrink)].rstrip()
                reduced = True
            if not reduced:
                break
            messages = _compose_messages()

    return messages, {
        "style_context_chars_used": len(style_context),
        "reference_context_chars_used": len(reference_context),
        "transcript_chars_used": len(transcript_excerpt),
        "prompt_chars_after_trim": _messages_char_count(messages),
    }


def _build_single_pass_repair_messages(messages: list[dict[str, str]]) -> list[dict[str, str]]:
    repair_instruction = {
        "role": "system",
        "content": (
            "Исправь ответ: верни только валидный JSON-объект по исходной схеме. "
            "Без markdown, без <think>, без текста до/после JSON."
        ),
    }
    return [*messages, repair_instruction]


def _map_single_pass_payload_to_llm_fields(
    *,
    payload: dict[str, Any],
    case_mode: str,
) -> dict[str, Any]:
    case_summary = _strip_markdown_and_think(str(payload.get("case_summary") or payload.get("key_takeaway") or ""))
    main_issue = _strip_markdown_and_think(str(payload.get("main_issue") or payload.get("growth_zones") or ""))
    strong_sides = _strip_markdown_and_think(str(payload.get("strong_sides") or ""))
    growth_zones = _strip_markdown_and_think(str(payload.get("growth_zones") or ""))
    what_to_fix = _strip_markdown_and_think(str(payload.get("what_to_fix") or ""))
    what_to_tell_employee = _strip_markdown_and_think(str(payload.get("what_to_tell_employee") or ""))
    better_phrase = _strip_markdown_and_think(str(payload.get("better_phrase") or ""))
    expected_quantity = _strip_markdown_and_think(str(payload.get("expected_effect_quantity") or payload.get("expected_effect") or ""))
    expected_quality = _strip_markdown_and_think(str(payload.get("expected_effect_quality") or payload.get("expected_effect") or ""))
    data_limitations = _strip_markdown_and_think(str(payload.get("data_limitations") or ""))
    evidence_quote = _strip_markdown_and_think(str(payload.get("evidence_quote") or ""))
    if better_phrase and not better_phrase.lower().startswith("используй:"):
        better_phrase = f"Используй: {better_phrase}"
    why_important = _strip_markdown_and_think(
        str(payload.get("why_it_matters") or payload.get("expected_effect_quality") or "")
    )
    if not why_important:
        why_important = _strip_markdown_and_think(data_limitations or "Нужно точнее фиксировать следующий шаг, чтобы сделки не провисали.")
    coaching_lines: list[str] = []
    if what_to_tell_employee:
        coaching_lines.append(f"1) {what_to_tell_employee}")
    if better_phrase:
        coaching_lines.append(f"2) {better_phrase}")
    coaching = "\n".join(coaching_lines)
    stage_comment = case_summary or main_issue or growth_zones or what_to_fix
    stage_comment = _strip_markdown_and_think(stage_comment)
    out: dict[str, Any] = {
        "primary_case_type": CASE_TYPE_BY_MODE.get(case_mode, "разговор с лпр"),
        "relevant_stage_groups": STAGE_GROUPS_BY_MODE.get(case_mode, "lpr,need,speech,crm"),
        "one_main_issue": main_issue,
        "evidence_by_stage": stage_comment,
        "key_takeaway": case_summary,
        "strong_sides": strong_sides,
        "growth_zones": growth_zones,
        "why_important": why_important,
        "reinforce": strong_sides,
        "fix_action": what_to_fix,
        "coaching_list": coaching,
        "expected_quantity": expected_quantity,
        "expected_quality": expected_quality,
        "evidence_quote": evidence_quote,
        "risk_level": str(payload.get("risk_level") or ""),
        "quality_score_0_100": _to_int_score(payload.get("quality_score_0_100"), default=55),
        "data_limitations": data_limitations,
    }
    if case_mode == "test_analysis":
        out["stage_test_comment"] = stage_comment
        out["stage_test_launch_comment"] = stage_comment
        out["stage_test_criteria_comment"] = stage_comment
    elif case_mode == "presentation_analysis":
        out["stage_demo_comment"] = stage_comment
        out["stage_demo_intro_comment"] = stage_comment
        out["stage_demo_context_comment"] = stage_comment
    elif case_mode == "dozhim_analysis":
        out["stage_dozhim_comment"] = stage_comment
        out["stage_dozhim_recontact_comment"] = stage_comment
        out["stage_dozhim_terms_comment"] = stage_comment
    elif case_mode == "secretary_analysis":
        out["stage_secretary_comment"] = stage_comment
    else:
        out["stage_lpr_comment"] = stage_comment
        out["stage_need_comment"] = stage_comment
        out["stage_speech_comment"] = stage_comment
    return out


def _prepare_single_pass_generation(
    *,
    logger: Any,
    base_runtime: dict[str, Any],
    selected_items: list[dict[str, Any]],
    records_by_deal: dict[str, dict[str, Any]],
    max_runtime_minutes: int,
    max_llm_calls: int,
    stop_on_rate_limit: bool,
    skip_cloud_on_rate_limit: bool,
    quarantine_failed: bool,
    max_style_chars: int,
    max_reference_chars: int,
    max_transcript_chars: int,
    max_prompt_chars: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rows_quarantined: list[dict[str, Any]] = []
    rows_skipped: list[dict[str, Any]] = []
    run_started = time.monotonic()
    blocked_models: set[str] = set()
    rate_limited_models: set[str] = set()
    context_overflow_models: set[str] = set()
    first_rate_limit_at: dict[str, Any] | None = None
    first_context_overflow_at: dict[str, Any] | None = None
    preflight_statuses: dict[str, str] = {
        "main": _preflight_status_by_candidate(base_runtime, "main"),
        "fallback": _preflight_status_by_candidate(base_runtime, "fallback"),
        "fallback2": _preflight_status_by_candidate(base_runtime, "fallback2"),
    }

    preflight = base_runtime.get("preflight_results", []) if isinstance(base_runtime.get("preflight_results"), list) else []
    for item in preflight:
        if not isinstance(item, dict):
            continue
        error_type = str(item.get("error_type") or "")
        model = str(item.get("model") or "").strip()
        if error_type == "cloud_usage_limit" and model and _is_cloud_model_name(model):
            blocked_models.add(model)
            rate_limited_models.add(model)
            if first_rate_limit_at is None:
                first_rate_limit_at = {
                    "stage": "preflight",
                    "candidate": str(item.get("candidate") or ""),
                    "model": model,
                    "error": str(item.get("error") or ""),
                }
        if (
            skip_cloud_on_rate_limit
            and error_type == "context_overflow"
            and model
            and _is_cloud_model_name(model)
        ):
            blocked_models.add(model)
            context_overflow_models.add(model)
            if first_context_overflow_at is None:
                first_context_overflow_at = {
                    "stage": "preflight",
                    "candidate": str(item.get("candidate") or ""),
                    "model": model,
                    "error": str(item.get("error") or ""),
                }

    counts = Counter()
    llm_source_rows = Counter()
    llm_elapsed_by_source: dict[str, list[int]] = {"main": [], "fallback": [], "fallback2": []}
    failed_reasons = Counter()
    skipped_reasons = Counter()

    llm_calls_used = 0
    cloud_calls_used = 0
    cloud_calls_saved_estimate = 0
    rows_routed_to_fallback_after_rate_limit = 0
    rows_routed_to_fallback_after_context_overflow = 0
    rows_skipped_due_to_rate_limit = 0
    rows_skipped_due_to_context_overflow = 0
    max_prompt_size_chars_seen = 0
    style_context_chars_used = 0
    reference_context_chars_used = 0

    for idx, candidate in enumerate(selected_items, start=1):
        total = len(selected_items)
        deal_id = str(candidate.get("deal_id") or "").strip()
        if not deal_id:
            skipped_reasons["missing_deal_id"] += 1
            rows_skipped.append({"row_index": idx, "deal_id": "", "reason": "missing_deal_id"})
            continue

        if max_runtime_minutes > 0:
            elapsed_minutes = (time.monotonic() - run_started) / 60.0
            if elapsed_minutes >= float(max_runtime_minutes):
                remaining = total - idx + 1
                for tail in selected_items[idx - 1 :]:
                    tail_id = str(tail.get("deal_id") or "").strip()
                    rows_skipped.append({"row_index": idx, "deal_id": tail_id, "reason": "max_runtime_minutes_exceeded"})
                skipped_reasons["max_runtime_minutes_exceeded"] += remaining
                break

        if max_llm_calls > 0 and llm_calls_used >= int(max_llm_calls):
            remaining = total - idx + 1
            for tail in selected_items[idx - 1 :]:
                tail_id = str(tail.get("deal_id") or "").strip()
                rows_skipped.append({"row_index": idx, "deal_id": tail_id, "reason": "max_llm_calls_exceeded"})
            skipped_reasons["max_llm_calls_exceeded"] += remaining
            break

        record = records_by_deal.get(deal_id)
        if not isinstance(record, dict):
            rows_skipped.append({"row_index": idx, "deal_id": deal_id, "reason": "record_not_found"})
            skipped_reasons["record_not_found"] += 1
            continue

        case_mode = _normalize_call_review_case_mode(candidate=candidate, record=record)
        record["call_review_case_mode"] = case_mode
        if not _call_review_case_has_meaningful_conversation(case_mode=case_mode, candidate=candidate, record=record):
            rows_skipped.append({"row_index": idx, "deal_id": deal_id, "reason": "not_meaningful_conversation_case", "case_mode": case_mode})
            skipped_reasons["not_meaningful_conversation_case"] += 1
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = "not_meaningful_conversation_case"
            record["call_review_llm_error_category"] = "case_gated"
            continue

        transcript_excerpt = str(record.get("transcript_text_excerpt") or "").strip()
        transcript_chars = int(record.get("transcript_text_len", 0) or len(transcript_excerpt))
        if not transcript_excerpt:
            rows_skipped.append({"row_index": idx, "deal_id": deal_id, "reason": "missing_transcript"})
            skipped_reasons["missing_transcript"] += 1
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = "missing_transcript"
            record["call_review_llm_error_category"] = "missing_transcript"
            continue

        runtime_for_row, removed_count = _build_runtime_with_blocked_models(
            base_runtime,
            blocked_models=blocked_models,
            skip_cloud_on_rate_limit=skip_cloud_on_rate_limit,
        )
        if removed_count > 0:
            cloud_calls_saved_estimate += removed_count

        selected_runtime = str(runtime_for_row.get("selected") or "none")
        if selected_runtime not in {"main", "fallback", "fallback2"}:
            reason = "no_live_runtime_after_model_block" if blocked_models else "no_live_llm_runtime"
            rows_quarantined.append(
                {
                    "row_index": idx,
                    "deal_id": deal_id,
                    "manager": str(record.get("owner_name") or ""),
                    "reason": reason,
                    "error_type": "cloud_usage_limit" if blocked_models else "runtime_unavailable",
                    "failed_model": "",
                    "failed_base_url": "",
                    "fallback_reason": "",
                    "case_mode": case_mode,
                    "transcript_length_chars": transcript_chars,
                }
            )
            failed_reasons[reason] += 1
            if rate_limited_models:
                rows_skipped_due_to_rate_limit += 1
            if context_overflow_models:
                rows_skipped_due_to_context_overflow += 1
            continue

        messages, prompt_meta = _build_single_pass_messages(
            record=record,
            candidate=candidate,
            case_mode=case_mode,
            max_style_chars=max_style_chars,
            max_reference_chars=max_reference_chars,
            max_transcript_chars=max_transcript_chars,
            max_prompt_chars=max_prompt_chars,
        )
        style_context_chars_used = max(style_context_chars_used, int(prompt_meta.get("style_context_chars_used", 0) or 0))
        reference_context_chars_used = max(
            reference_context_chars_used,
            int(prompt_meta.get("reference_context_chars_used", 0) or 0),
        )
        max_prompt_size_chars_seen = max(
            max_prompt_size_chars_seen,
            int(prompt_meta.get("prompt_chars_after_trim", 0) or 0),
        )
        repair_messages = _build_single_pass_repair_messages(messages)
        diagnostics: dict[str, Any] = {}
        payload, source = _llm_chat_json_with_runtime(
            runtime=runtime_for_row,
            messages=messages,
            repair_messages=repair_messages,
            logger=logger,
            log_prefix=f"call_review replay v2 deal={deal_id}",
            diagnostics_out=diagnostics,
        )

        attempts = diagnostics.get("attempts", []) if isinstance(diagnostics.get("attempts"), list) else []
        llm_calls_used += len(attempts)
        new_rate_limit_hit = False
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            attempt_model = str(attempt.get("model") or "").strip()
            attempt_error_type = str(attempt.get("error_type") or "")
            if attempt_model and _is_cloud_model_name(attempt_model):
                cloud_calls_used += 1
            if attempt_error_type == "cloud_usage_limit":
                if attempt_model and _is_cloud_model_name(attempt_model):
                    blocked_models.add(attempt_model)
                    rate_limited_models.add(attempt_model)
                    new_rate_limit_hit = True
                    if first_rate_limit_at is None:
                        first_rate_limit_at = {
                            "stage": "row",
                            "row_index": idx,
                            "deal_id": deal_id,
                            "model": attempt_model,
                            "error": str(attempt.get("error") or ""),
                        }
            if (
                skip_cloud_on_rate_limit
                and attempt_error_type == "context_overflow"
                and attempt_model
                and _is_cloud_model_name(attempt_model)
            ):
                blocked_models.add(attempt_model)
                context_overflow_models.add(attempt_model)
                if first_context_overflow_at is None:
                    first_context_overflow_at = {
                        "stage": "row",
                        "row_index": idx,
                        "deal_id": deal_id,
                        "model": attempt_model,
                        "error": str(attempt.get("error") or ""),
                    }

        llm_elapsed_ms = int(diagnostics.get("llm_elapsed_ms", 0) or 0)
        llm_model = str(diagnostics.get("llm_model") or "")
        llm_source = str(diagnostics.get("llm_source") or source or "")
        max_prompt_size_chars_seen = max(max_prompt_size_chars_seen, int(diagnostics.get("prompt_chars", 0) or 0))

        if not isinstance(payload, dict):
            error_text = str(source or diagnostics.get("fallback_reason") or "llm_generation_failed")
            error_type = str(diagnostics.get("error_type") or classify_llm_error(error_text))
            failed_model = ""
            failed_base_url = ""
            for attempt in reversed(attempts):
                if isinstance(attempt, dict) and not bool(attempt.get("success")):
                    failed_model = str(attempt.get("failed_model") or attempt.get("model") or "")
                    failed_base_url = str(attempt.get("failed_base_url") or "")
                    break
            reason = "llm_json_invalid" if error_type == "invalid_json" else (
                "llm_timeout" if error_type == "timeout" else (
                    "llm_rate_limit" if error_type == "cloud_usage_limit" else (
                        "llm_context_overflow" if error_type == "context_overflow" else "llm_generation_failed"
                    )
                )
            )
            failed_reasons[reason] += 1
            if error_type == "cloud_usage_limit":
                rows_skipped_due_to_rate_limit += 1
            if error_type == "context_overflow":
                rows_skipped_due_to_context_overflow += 1
            rows_quarantined.append(
                {
                    "row_index": idx,
                    "deal_id": deal_id,
                    "manager": str(record.get("owner_name") or ""),
                    "reason": reason,
                    "error_type": error_type,
                    "failed_model": failed_model,
                    "failed_base_url": failed_base_url,
                    "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
                    "case_mode": case_mode,
                    "transcript_length_chars": transcript_chars,
                    "llm_source": llm_source,
                    "llm_elapsed_ms": llm_elapsed_ms,
                    "attempts": attempts,
                }
            )
            status = "quarantine"
        else:
            mapped = _map_single_pass_payload_to_llm_fields(payload=payload, case_mode=case_mode)
            fields = _sanitize_call_review_llm_fields(mapped)
            ready, reason = _call_review_llm_fields_ready(fields=fields, case_mode=case_mode)
            if not ready:
                fields, _ = _repair_call_review_llm_fields(fields=fields, case_mode=case_mode)
                ready, reason = _call_review_llm_fields_ready(fields=fields, case_mode=case_mode)

            if not ready:
                reason_text = str(reason or "validation_failed")
                reason_cat = "missing_required_stage_comment" if reason_text.startswith("llm_missing_stage_") else "validation_failed"
                failed_reasons[reason_cat] += 1
                rows_quarantined.append(
                    {
                        "row_index": idx,
                        "deal_id": deal_id,
                        "manager": str(record.get("owner_name") or ""),
                        "reason": reason_cat,
                        "error_type": "invalid_json" if reason_cat == "validation_failed" else "missing_required_stage_comment",
                        "failed_model": llm_model,
                        "failed_base_url": str((runtime_for_row.get(llm_source, {}) or {}).get("base_url") or ""),
                        "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
                        "case_mode": case_mode,
                        "transcript_length_chars": transcript_chars,
                        "llm_source": llm_source,
                        "llm_elapsed_ms": llm_elapsed_ms,
                        "attempts": attempts,
                    }
                )
                status = "quarantine"
            else:
                record["call_review_llm_ready"] = True
                record["call_review_llm_error"] = ""
                record["call_review_llm_error_category"] = ""
                record["call_review_llm_fields"] = fields
                record["call_review_llm_source"] = llm_source
                record["call_review_llm_runtime_metrics"] = {
                    "llm_source": llm_source,
                    "llm_model": llm_model,
                    "llm_elapsed_ms": llm_elapsed_ms,
                    "prompt_chars": int(diagnostics.get("prompt_chars", 0) or 0),
                    "response_chars": int(diagnostics.get("response_chars", 0) or 0),
                    "timed_out": bool(diagnostics.get("timed_out", False)),
                    "fallback_used": bool(diagnostics.get("fallback_used", False)),
                    "fallback_reason": str(diagnostics.get("fallback_reason") or ""),
                    "fallback_model_used": str(diagnostics.get("fallback_model_used") or ""),
                    "json_valid": bool(diagnostics.get("json_valid", False)),
                    "repair_used": bool(diagnostics.get("repair_used", False)),
                    "attempts": attempts,
                }
                counts["rows_success"] += 1
                llm_source_rows[llm_source or "unknown"] += 1
                if llm_source in llm_elapsed_by_source:
                    llm_elapsed_by_source[llm_source].append(llm_elapsed_ms)
                if rate_limited_models and llm_source in {"fallback", "fallback2"}:
                    rows_routed_to_fallback_after_rate_limit += 1
                if context_overflow_models and llm_source in {"fallback", "fallback2"}:
                    rows_routed_to_fallback_after_context_overflow += 1
                status = "success"

        if stop_on_rate_limit and new_rate_limit_hit:
            remaining = total - idx
            if remaining > 0:
                for tail in selected_items[idx:]:
                    tail_id = str(tail.get("deal_id") or "").strip()
                    rows_skipped.append(
                        {
                            "row_index": idx + 1,
                            "deal_id": tail_id,
                            "reason": "stopped_on_rate_limit",
                        }
                    )
                skipped_reasons["stopped_on_rate_limit"] += remaining
            logger.warning("call_review replay v2 stopped on rate limit at row=%s deal=%s", idx, deal_id)
            logger.info(
                "call_review replay progress row=%s/%s deal=%s model=%s transcript_chars=%s elapsed_ms=%s status=%s success=%s failed=%s quarantine=%s skipped=%s cloud_calls=%s",
                idx,
                total,
                deal_id,
                llm_model or llm_source,
                transcript_chars,
                llm_elapsed_ms,
                status,
                counts["rows_success"],
                sum(failed_reasons.values()),
                len(rows_quarantined),
                len(rows_skipped),
                cloud_calls_used,
            )
            break

        logger.info(
            "call_review replay progress row=%s/%s deal=%s model=%s transcript_chars=%s elapsed_ms=%s status=%s success=%s failed=%s quarantine=%s skipped=%s cloud_calls=%s",
            idx,
            total,
            deal_id,
            llm_model or llm_source,
            transcript_chars,
            llm_elapsed_ms,
            status,
            counts["rows_success"],
            sum(failed_reasons.values()),
            len(rows_quarantined),
            len(rows_skipped),
            cloud_calls_used,
        )

    if quarantine_failed:
        for row in rows_quarantined:
            deal_id = str(row.get("deal_id") or "")
            record = records_by_deal.get(deal_id)
            if isinstance(record, dict):
                record["call_review_llm_ready"] = False
                record["call_review_llm_error"] = str(row.get("reason") or "quarantined")
                record["call_review_llm_error_category"] = str(row.get("error_type") or "quarantined")

    generation = {
        "selected_runtime": str(base_runtime.get("selected") or "none"),
        "preflight_main_status": str(preflight_statuses.get("main") or "missing"),
        "preflight_fallback_status": str(preflight_statuses.get("fallback") or "missing"),
        "preflight_fallback2_status": str(preflight_statuses.get("fallback2") or "missing"),
        "generated_rows": int(counts["rows_success"]),
        "rows_prepared": int(counts["rows_success"]),
        "failed_rows": int(len(rows_quarantined)),
        "skipped_rows": int(len(rows_skipped)),
        "skip_reasons": dict(skipped_reasons),
        "failed_reasons": dict(failed_reasons),
        "llm_sources": dict(llm_source_rows),
        "llm_source_rows": dict(llm_source_rows),
        "main_rows_count": int(llm_source_rows.get("main", 0) or 0),
        "fallback_rows_count": int(llm_source_rows.get("fallback", 0) or 0),
        "fallback2_rows_count": int(llm_source_rows.get("fallback2", 0) or 0),
        "llm_timeout_rows": int(
            sum(1 for row in rows_quarantined if str(row.get("error_type") or "") == "timeout")
        ),
        "main_timeout_count": 0,
        "fallback_timeout_count": 0,
        "fallback2_timeout_count": 0,
        "llm_json_repair_count": int(
            sum(
                1
                for record in records_by_deal.values()
                if isinstance(record, dict)
                and bool(
                    ((record.get("call_review_llm_runtime_metrics") or {}).get("repair_used", False)
                    if isinstance(record.get("call_review_llm_runtime_metrics"), dict)
                    else False)
                )
            )
        ),
        "llm_elapsed_ms_by_source": {
            source: {
                "avg": round(sum(values) / len(values), 2) if values else 0,
                "max": max(values) if values else 0,
                "count": len(values),
            }
            for source, values in llm_elapsed_by_source.items()
        },
        "main_avg_elapsed_ms": (
            round(sum(llm_elapsed_by_source.get("main", [])) / len(llm_elapsed_by_source.get("main", [])), 2)
            if llm_elapsed_by_source.get("main")
            else 0
        ),
        "fallback_avg_elapsed_ms": (
            round(sum(llm_elapsed_by_source.get("fallback", [])) / len(llm_elapsed_by_source.get("fallback", [])), 2)
            if llm_elapsed_by_source.get("fallback")
            else 0
        ),
        "fallback2_avg_elapsed_ms": (
            round(sum(llm_elapsed_by_source.get("fallback2", [])) / len(llm_elapsed_by_source.get("fallback2", [])), 2)
            if llm_elapsed_by_source.get("fallback2")
            else 0
        ),
        "main_model": str((base_runtime.get("main", {}) or {}).get("model") or ""),
        "fallback_model": str((base_runtime.get("fallback", {}) or {}).get("model") or ""),
        "fallback2_model": str((base_runtime.get("fallback2", {}) or {}).get("model") or ""),
        "single_pass_json": True,
        "llm_calls_used": int(llm_calls_used),
        "cloud_calls_used": int(cloud_calls_used),
        "cloud_calls_saved_estimate": int(cloud_calls_saved_estimate),
        "rate_limited_models": sorted(rate_limited_models),
        "context_overflow_models": sorted(context_overflow_models),
        "first_rate_limit_at": first_rate_limit_at or {},
        "first_context_overflow_at": first_context_overflow_at or {},
        "rows_skipped_due_to_rate_limit": int(rows_skipped_due_to_rate_limit),
        "rows_skipped_due_to_context_overflow": int(rows_skipped_due_to_context_overflow),
        "rows_routed_to_fallback_after_rate_limit": int(rows_routed_to_fallback_after_rate_limit),
        "rows_routed_to_fallback_after_context_overflow": int(rows_routed_to_fallback_after_context_overflow),
        "rows_failed_rate_limit": int(
            sum(1 for row in rows_quarantined if str(row.get("error_type") or "") == "cloud_usage_limit")
        ),
        "rows_failed_context_overflow": int(
            sum(1 for row in rows_quarantined if str(row.get("error_type") or "") == "context_overflow")
        ),
        "rows_failed_timeout": int(
            sum(1 for row in rows_quarantined if str(row.get("error_type") or "") == "timeout")
        ),
        "rows_failed_invalid_json": int(
            sum(1 for row in rows_quarantined if str(row.get("error_type") or "") == "invalid_json")
        ),
        "rows_recovered_by_local_fallback": int(llm_source_rows.get("fallback", 0) or 0),
        "max_prompt_size_chars_seen": int(max_prompt_size_chars_seen),
        "style_context_chars_used": int(style_context_chars_used),
        "reference_context_chars_used": int(reference_context_chars_used),
    }

    row_flow_debug = {
        "selected_items": len(selected_items),
        "rows_after_single_pass": int(counts["rows_success"]),
        "rows_quarantined": len(rows_quarantined),
        "rows_skipped": len(rows_skipped),
        "quarantine": rows_quarantined,
        "skipped": rows_skipped,
        "rate_limited_models": sorted(rate_limited_models),
        "context_overflow_models": sorted(context_overflow_models),
        "preflight_statuses": preflight_statuses,
    }

    return generation, rows_quarantined, rows_skipped, row_flow_debug


def _build_examples_md(payload_rows: list[dict[str, Any]], max_examples: int = 3) -> list[str]:
    lines = ["# Call Review LLM Replay V2 Examples", ""]
    for idx, row in enumerate(payload_rows[:max_examples], start=1):
        deal_id = str(row.get("Deal ID") or "")
        manager = str(row.get("Менеджер") or "")
        case_type = str(row.get("Тип кейса") or "")
        key_takeaway = str(row.get("Ключевой вывод") or "")
        fix_action = str(row.get("Что исправить") or "")
        tell_employee = str(row.get("Что донести сотруднику") or "")
        lines.extend(
            [
                f"## {idx}. Deal {deal_id}",
                f"- Менеджер: {manager}",
                f"- Тип кейса: {case_type}",
                f"- Ключевой вывод: {key_takeaway}",
                f"- Что исправить: {fix_action}",
                f"- Что донести сотруднику: {tell_employee}",
                "",
            ]
        )
    if len(payload_rows) == 0:
        lines.append("Нет строк для примеров.")
    return lines


def _finalize_payload_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    finalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out = dict(row)
        for key, value in list(out.items()):
            if not isinstance(value, str):
                continue
            text = value
            if "Лучше сказать:" in text:
                text = text.replace("Лучше сказать:", "Используй:")
            if "Лучше Лучше сказать" in text:
                text = text.replace("Лучше Лучше сказать", "Используй")
            out[key] = text
        finalized.append(out)
    return finalized


def main() -> None:
    args = _parse_args()
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    logger.warning("%s", EXPERIMENTAL_REPLAY_WARNING)

    run_dir = Path(str(args.run_dir)).resolve()
    if not run_dir.exists():
        raise FileNotFoundError(f"run dir not found: {run_dir}")

    write_requested = bool(args.write and not args.dry_run)
    try:
        effective_limit = _resolve_effective_limit(
            limit_arg=args.limit,
            write_requested=write_requested,
            allow_full_run=bool(args.allow_full_run),
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    cfg = load_deal_analyzer_config(str(args.config))
    llm_cfg = replace(
        cfg,
        ollama_model=str(args.main_model or cfg.ollama_model or "gemma4:31b-cloud"),
        ollama_fallback_enabled=True,
        ollama_fallback_model=str(args.fallback_model or cfg.ollama_fallback_model or "gpt-oss:20b"),
        ollama_fallback_timeout_seconds=max(1, int(args.fallback_timeout or cfg.ollama_fallback_timeout_seconds or 2400)),
        call_review_row_hard_timeout_seconds=max(
            2700,
            int(getattr(cfg, "call_review_row_hard_timeout_seconds", 1200) or 1200),
        ),
    )

    selected_items_all = _extract_selected_items(run_dir)
    selected_items = _filter_selected_items(
        selected_items_all,
        deal_ids=_expand_deal_ids(list(args.deal_id or [])),
        offset=max(0, int(args.offset or 0)),
        limit=int(effective_limit),
    )
    if not selected_items:
        raise SystemExit("no selected items after applying filters/limit")

    analysis_shortlist_payload = {"selected_items": selected_items}
    period_deal_records, missing_transcripts = _build_period_deal_records(run_dir, selected_items)
    _write_json(run_dir / "missing_transcripts.json", {"rows": missing_transcripts, "count": len(missing_transcripts)})

    llm_runtime = _resolve_call_review_llm_runtime(
        llm_cfg,
        logger,
        main_model_override=str(args.main_model or "").strip() or None,
        fallback_model_override=str(args.fallback_model or "").strip() or None,
        fallback2_model_override=str(args.fallback2_model or "").strip() or None,
        fallback_timeout_override=(int(args.fallback_timeout or 0) if int(args.fallback_timeout or 0) > 0 else None),
        no_retry_on_rate_limit=bool(args.no_retry_on_rate_limit),
        no_retry_on_context_overflow=bool(args.skip_cloud_on_rate_limit),
    )
    for source, timeout_arg in (
        ("main", int(args.main_timeout or 0)),
        ("fallback", int(args.fallback_timeout or 0)),
        ("fallback2", int(args.fallback2_timeout or 0)),
    ):
        if timeout_arg > 0 and isinstance(llm_runtime.get(source), dict):
            llm_runtime[source]["timeout_seconds"] = int(timeout_arg)
            if isinstance(llm_runtime.get("candidates"), list):
                for candidate in llm_runtime["candidates"]:
                    if isinstance(candidate, dict) and str(candidate.get("name") or "") == source:
                        candidate["timeout_seconds"] = int(timeout_arg)

    records_by_deal = {
        str(item.get("deal_id") or "").strip(): item
        for item in period_deal_records
        if isinstance(item, dict) and str(item.get("deal_id") or "").strip()
    }

    if bool(args.single_pass_json):
        llm_generation, rows_quarantined, rows_skipped, row_flow_debug = _prepare_single_pass_generation(
            logger=logger,
            base_runtime=llm_runtime,
            selected_items=selected_items,
            records_by_deal=records_by_deal,
            max_runtime_minutes=max(0, int(args.max_runtime_minutes or 0)),
            max_llm_calls=max(0, int(args.max_llm_calls or 0)),
            stop_on_rate_limit=bool(args.stop_on_rate_limit),
            skip_cloud_on_rate_limit=bool(args.skip_cloud_on_rate_limit),
            quarantine_failed=bool(args.quarantine_failed),
            max_style_chars=max(0, int(args.max_style_chars or MAX_STYLE_CHARS_DEFAULT)),
            max_reference_chars=max(0, int(args.max_reference_chars or MAX_REFERENCE_CHARS_DEFAULT)),
            max_transcript_chars=max(0, int(args.max_transcript_chars or MAX_TRANSCRIPT_CHARS_DEFAULT)),
            max_prompt_chars=max(0, int(args.max_prompt_chars or MAX_PROMPT_CHARS_DEFAULT)),
        )
    else:
        llm_generation = _prepare_call_review_llm_fields(
            cfg=llm_cfg,
            logger=logger,
            llm_runtime=llm_runtime,
            style_source_excerpt="",
            period_deal_records=period_deal_records,
            analysis_shortlist_payload=analysis_shortlist_payload,
            step_artifacts_root=run_dir / "call_review_llm_replay_step_artifacts",
        )
        rows_quarantined = []
        rows_skipped = []
        row_flow_debug = {
            "selected_items": len(selected_items),
            "rows_after_single_pass": int(llm_generation.get("generated_rows", 0) or 0),
            "rows_quarantined": 0,
            "rows_skipped": int(llm_generation.get("skipped_rows", 0) or 0),
            "quarantine": [],
            "skipped": [],
            "rate_limited_models": [],
        }

    summary_base = _read_json(run_dir / "summary.json", default={})
    call_ledger_all = _read_json(run_dir / "call_review_v3" / "call_ledger_all.json", default=[])
    call_ledger_audit = _read_json(run_dir / "call_review_v3" / "call_ledger_audit.json", default={})
    anchor_shortlist = _read_json(run_dir / "call_review_v3" / "anchor_shortlist.json", default=[])

    call_review_payload = build_call_review_v3_payload(
        summary=summary_base if isinstance(summary_base, dict) else {},
        period_deal_records=period_deal_records,
        analysis_shortlist_payload=analysis_shortlist_payload,
        base_domain=str(llm_cfg.call_base_domain or "").strip() or "https://istock.link",
        manager_allowlist=list(llm_cfg.daily_manager_allowlist or []),
        manager_role_registry=dict(llm_cfg.manager_role_registry or {}),
        run_dir=None,
        call_ledger_all=call_ledger_all if isinstance(call_ledger_all, list) else [],
        call_ledger_audit=call_ledger_audit if isinstance(call_ledger_audit, dict) else {},
        anchor_shortlist=anchor_shortlist if isinstance(anchor_shortlist, list) else [],
        selected_anchor_cases=selected_items,
        abort_stage="",
        abort_error="",
        artifacts_written=[],
        cfg=llm_cfg,
        logger=logger,
    )

    payload_for_writer = {
        "mode": str(call_review_payload.get("mode") or "call_review_sheet"),
        "schema_version": str(call_review_payload.get("schema_version") or "v3"),
        "sheet_name": str(call_review_payload.get("sheet_name") or DEFAULT_SHEET_NAME),
        "start_cell": str(call_review_payload.get("start_cell") or "A2"),
        "columns": call_review_payload.get("columns", []),
        "rows": call_review_payload.get("rows", []),
        "rows_count": int(call_review_payload.get("rows_count", 0) or 0),
    }
    payload_for_writer["rows"] = _finalize_payload_rows(
        payload_for_writer.get("rows", []) if isinstance(payload_for_writer.get("rows"), list) else []
    )
    payload_for_writer["rows_count"] = len(payload_for_writer["rows"])
    preflight = call_review_payload.get("semantic_preflight", {})
    preflight_passed = bool(preflight.get("passed", True) if isinstance(preflight, dict) else True)

    rows_to_write = int(payload_for_writer.get("rows_count", 0) or 0)
    rows_quarantined_count = int(len(rows_quarantined))
    experimental_main_model = str(args.main_model or "")
    experimental_gemma_write_requested = bool(write_requested and _is_gemma_model_name(experimental_main_model))
    experimental_gemma_write_allowed = bool(args.allow_experimental_gemma_write)
    experimental_gemma_block_reason = _experimental_gemma_write_block_reason(
        write_requested=write_requested,
        main_model=experimental_main_model,
        allow_experimental_gemma_write=experimental_gemma_write_allowed,
    )

    write_allowed = write_requested
    block_reason = ""
    if not write_requested:
        write_allowed = False
        block_reason = "dry_run_mode"
    elif experimental_gemma_block_reason:
        write_allowed = False
        block_reason = experimental_gemma_block_reason
    elif rows_to_write <= 0:
        write_allowed = False
        block_reason = "rows_rebuilt_zero"
    elif bool(args.strict_preflight) and not preflight_passed:
        write_allowed = False
        block_reason = "strict_preflight_failed"
    elif rows_quarantined_count > 0 and not bool(args.allow_partial_write):
        write_allowed = False
        block_reason = "rows_quarantined"

    writer_cfg = llm_cfg if write_allowed else replace(llm_cfg, deal_analyzer_write_enabled=False)
    writer_status = _maybe_write_call_review_sheet(
        cfg=writer_cfg,
        logger=logger,
        call_review_payload=payload_for_writer,
    )
    if write_allowed and str(writer_status.get("mode") or "") != "real_write":
        write_allowed = False
        if not block_reason:
            block_reason = "writer_not_in_real_mode"

    writer_plan = {
        "mode": "real_write" if write_requested else "dry_run",
        "write_requested": write_requested,
        "write_allowed": write_allowed,
        "block_reason": block_reason,
        "rows_prepared": rows_to_write,
        "rows_to_write": rows_to_write,
        "rows_quarantined": rows_quarantined_count,
        "rows_skipped": len(rows_skipped),
        "strict_preflight": bool(args.strict_preflight),
        "allow_partial_write": bool(args.allow_partial_write),
        "quarantine_failed": bool(args.quarantine_failed),
        "experimental_warning": EXPERIMENTAL_REPLAY_WARNING,
        "experimental_gemma_write_requested": experimental_gemma_write_requested,
        "experimental_gemma_write_allowed": experimental_gemma_write_allowed,
        "writer_status": writer_status,
    }

    replay_summary = {
        "run_dir": str(run_dir),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "selected_items_total": len(selected_items_all),
        "selected_items_replay": len(selected_items),
        "offset": int(args.offset or 0),
        "limit_effective": int(effective_limit),
        "allow_full_run": bool(args.allow_full_run),
        "period_deal_records_total": len(period_deal_records),
        "missing_transcripts_count": len(missing_transcripts),
        "llm_main_model": str(args.main_model or ""),
        "llm_fallback_model": str(args.fallback_model or ""),
        "llm_fallback2_model": str(args.fallback2_model or ""),
        "experimental_warning": EXPERIMENTAL_REPLAY_WARNING,
        "experimental_gemma_write_requested": experimental_gemma_write_requested,
        "experimental_gemma_write_allowed": experimental_gemma_write_allowed,
        "llm_generation": llm_generation,
        "preflight_main_status": str(llm_generation.get("preflight_main_status") or ""),
        "preflight_fallback_status": str(llm_generation.get("preflight_fallback_status") or ""),
        "preflight_fallback2_status": str(llm_generation.get("preflight_fallback2_status") or ""),
        "rows_available": len(selected_items),
        "rows_rebuilt": rows_to_write,
        "rows_prepared": int(writer_status.get("rows_prepared", 0) or 0),
        "rows_written": int(writer_status.get("rows_written", 0) or 0),
        "rows_quarantined": rows_quarantined_count,
        "rows_skipped": len(rows_skipped),
        "writer_mode": str(writer_status.get("mode") or "dry_run"),
        "semantic_preflight_passed": preflight_passed,
        "failed_rules": preflight.get("failed_rules", []) if isinstance(preflight, dict) else [],
        "warning_rules": preflight.get("warning_rules", []) if isinstance(preflight, dict) else [],
        "strict_preflight": bool(args.strict_preflight),
        "allow_partial_write": bool(args.allow_partial_write),
        "write_requested": write_requested,
        "write_allowed": write_allowed,
        "write_executed": bool(write_allowed and str(writer_status.get("mode") or "") == "real_write"),
        "rows_skipped_no_runtime": int((llm_generation.get("skip_reasons", {}) or {}).get("no_live_llm_runtime", 0) or 0),
        "rows_failed_rate_limit": int(llm_generation.get("rows_failed_rate_limit", 0) or 0),
        "rows_failed_context_overflow": int(llm_generation.get("rows_failed_context_overflow", 0) or 0),
        "rows_failed_timeout": int(llm_generation.get("rows_failed_timeout", 0) or 0),
        "rows_failed_invalid_json": int(llm_generation.get("rows_failed_invalid_json", 0) or 0),
        "rows_recovered_by_local_fallback": int(llm_generation.get("rows_recovered_by_local_fallback", 0) or 0),
        "context_overflow_models": list(llm_generation.get("context_overflow_models", []) or []),
        "rows_routed_to_fallback_after_context_overflow": int(
            llm_generation.get("rows_routed_to_fallback_after_context_overflow", 0) or 0
        ),
        "max_prompt_size_chars_seen": int(llm_generation.get("max_prompt_size_chars_seen", 0) or 0),
        "style_context_chars_used": int(llm_generation.get("style_context_chars_used", 0) or 0),
        "reference_context_chars_used": int(llm_generation.get("reference_context_chars_used", 0) or 0),
    }

    _write_json(run_dir / "call_review_llm_replay_v2_payload.json", payload_for_writer)
    _write_json(run_dir / "call_review_llm_replay_v2_preflight.json", preflight if isinstance(preflight, dict) else {})
    _write_json(
        run_dir / "call_review_llm_replay_v2_quarantine.json",
        {"rows": rows_quarantined, "count": len(rows_quarantined), "skipped": rows_skipped},
    )
    _write_json(run_dir / "call_review_llm_replay_v2_writer_plan.json", writer_plan)
    _write_json(run_dir / "call_review_llm_replay_v2_summary.json", replay_summary)
    _write_json(run_dir / "call_review_llm_replay_v2_runtime_status.json", llm_runtime)
    _write_md(run_dir / "call_review_llm_replay_v2_examples.md", _build_examples_md(payload_for_writer.get("rows", [])))

    _write_json(run_dir / "call_review_llm_replay_payload.json", payload_for_writer)
    _write_json(run_dir / "call_review_llm_replay_preflight.json", preflight if isinstance(preflight, dict) else {})
    _write_json(run_dir / "call_review_llm_replay_writer_plan.json", writer_plan)
    _write_json(run_dir / "call_review_llm_replay_summary.json", replay_summary)
    _write_json(run_dir / "call_review_llm_replay_runtime_status.json", llm_runtime)
    _write_json(run_dir / "call_review_llm_replay_row_flow_debug.json", row_flow_debug)

    print(str(run_dir))


if __name__ == "__main__":
    main()

