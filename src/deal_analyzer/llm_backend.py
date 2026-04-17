from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import DealAnalyzerConfig
from .llm_client import OllamaClient, OllamaClientError
from .models import DealAnalysis
from .prompt_builder import append_json_repair_instruction, build_ollama_chat_messages
from .rules import analyze_deal


@dataclass(frozen=True)
class LlmAnalysisOutcome:
    analysis: DealAnalysis
    backend_used: str
    llm_error: bool
    error_message: str | None
    repaired: bool = False


def analyze_deal_with_ollama(
    *,
    normalized_deal: dict[str, Any],
    config: DealAnalyzerConfig,
    client: OllamaClient | None = None,
) -> DealAnalysis:
    return analyze_deal_with_ollama_outcome(normalized_deal=normalized_deal, config=config, client=client).analysis


def analyze_deal_with_ollama_outcome(
    *,
    normalized_deal: dict[str, Any],
    config: DealAnalyzerConfig,
    client: OllamaClient | None = None,
) -> LlmAnalysisOutcome:
    baseline = analyze_deal(normalized_deal, config)
    ollama = client or OllamaClient(
        base_url=config.ollama_base_url,
        model=config.ollama_model,
        timeout_seconds=config.ollama_timeout_seconds,
    )

    base_messages = build_ollama_chat_messages(normalized_deal=normalized_deal, config=config)

    first_error: OllamaClientError | None = None
    try:
        parsed = ollama.chat_json(messages=base_messages)
        merged = _merge_with_baseline(baseline.to_dict(), parsed.payload)
        merged["analysis_backend_requested"] = config.analyzer_backend
        merged["analysis_backend_used"] = "ollama"
        merged["llm_repair_applied"] = bool(parsed.repair_applied)
        return LlmAnalysisOutcome(
            analysis=DealAnalysis(**merged),
            backend_used="ollama",
            llm_error=False,
            error_message=None,
            repaired=bool(parsed.repair_applied),
        )
    except OllamaClientError as exc:
        first_error = exc

    repair_messages = append_json_repair_instruction(base_messages)
    try:
        parsed = ollama.chat_json(messages=repair_messages)
        merged = _merge_with_baseline(baseline.to_dict(), parsed.payload)
        merged["analysis_backend_requested"] = config.analyzer_backend
        merged["analysis_backend_used"] = "ollama"
        merged["llm_repair_applied"] = True
        return LlmAnalysisOutcome(
            analysis=DealAnalysis(**merged),
            backend_used="ollama",
            llm_error=False,
            error_message=None,
            repaired=True,
        )
    except OllamaClientError as second:
        message = (
            "Ollama failed after retry. "
            f"first={first_error}; second={second}"
        )
        fallback = baseline.to_dict()
        fallback["analysis_backend_requested"] = config.analyzer_backend
        fallback["analysis_backend_used"] = "rules_fallback"
        fallback["llm_repair_applied"] = False
        return LlmAnalysisOutcome(
            analysis=DealAnalysis(**fallback),
            backend_used="rules_fallback",
            llm_error=True,
            error_message=message,
            repaired=False,
        )


def _merge_with_baseline(base: dict[str, Any], llm_payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)

    if "score_0_100" in llm_payload:
        out["score_0_100"] = _coerce_score(llm_payload.get("score_0_100"), out["score_0_100"])

    for key in (
        "strong_sides",
        "growth_zones",
        "risk_flags",
        "recommended_actions_for_manager",
        "recommended_training_tasks_for_employee",
    ):
        if key in llm_payload:
            out[key] = _coerce_str_list(llm_payload.get(key), out[key])

    for key in (
        "presentation_quality_flag",
        "followup_quality_flag",
        "data_completeness_flag",
        "manager_message_draft",
        "employee_training_message_draft",
    ):
        if key in llm_payload:
            out[key] = _coerce_text(llm_payload.get(key), out[key])

    return out


def _coerce_score(value: Any, fallback: int) -> int:
    try:
        score = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(0, min(100, score))


def _coerce_str_list(value: Any, fallback: list[str]) -> list[str]:
    if not isinstance(value, list):
        return fallback
    out: list[str] = []
    for item in value:
        txt = _coerce_text(item, "")
        if txt:
            out.append(txt)
    return out or fallback


def _coerce_text(value: Any, fallback: str) -> str:
    txt = " ".join(str(value or "").strip().split())
    return txt or fallback
