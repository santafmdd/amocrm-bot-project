from __future__ import annotations

from typing import Any

from .config import DealAnalyzerConfig
from .llm_client import OllamaClient


def classify_llm_error(error_text: str) -> str:
    low = str(error_text or "").strip().lower()
    if not low:
        return "unknown"
    if is_cloud_usage_limit_error(low):
        return "cloud_usage_limit"
    if is_context_overflow_error(low):
        return "context_overflow"
    timeout_tokens = (
        "timed out",
        "timeout",
        "time out",
        "read timed out",
        "remote disconnected",
        "remotedisconnected",
        "connection reset",
        "connection aborted",
        "row_hard_timeout_exceeded",
    )
    if any(token in low for token in timeout_tokens):
        return "timeout"
    invalid_json_tokens = (
        "not valid json object",
        "invalid json",
        "json decode",
        "json envelope",
        "message.content",
    )
    if any(token in low for token in invalid_json_tokens):
        return "invalid_json"
    connection_tokens = (
        "connection failed",
        "connection error",
        "name or service not known",
        "nodename nor servname provided",
        "refused",
        "unreachable",
    )
    if any(token in low for token in connection_tokens):
        return "connection_error"
    return "unknown"


def is_cloud_usage_limit_error(error_text: str) -> bool:
    low = str(error_text or "").strip().lower()
    if not low:
        return False
    if "http 429" in low:
        return True
    limit_tokens = ("weekly usage limit", "session limit", "usage limit")
    return any(token in low for token in limit_tokens)


def is_context_overflow_error(error_text: str) -> bool:
    low = str(error_text or "").strip().lower()
    if not low:
        return False
    overflow_tokens = (
        "prompt too long",
        "exceeded max context length",
        "max context length",
        "max context",
        "context window",
        "too many tokens",
        "maximum context length",
        "input is too long",
        "input too long",
    )
    return any(token in low for token in overflow_tokens)


def should_retry_same_model(
    *,
    error_text: str,
    no_retry_on_rate_limit: bool,
    no_retry_on_context_overflow: bool = False,
) -> bool:
    error_type = classify_llm_error(error_text)
    if no_retry_on_rate_limit and error_type == "cloud_usage_limit":
        return False
    if no_retry_on_context_overflow and error_type == "context_overflow":
        return False
    return True


def _is_local_gemma_runtime(*, model: str, base_url: str) -> bool:
    model_norm = str(model or "").strip().lower()
    if not model_norm.startswith("gemma4"):
        return False
    base = str(base_url or "").strip().lower()
    return ("127.0.0.1" in base) or ("localhost" in base)


def _candidate_payload(
    *,
    name: str,
    model: str,
    base_url: str,
    timeout_seconds: int,
    preflight_timeout_seconds: int,
    enabled: bool,
) -> dict[str, Any]:
    return {
        "name": str(name or "").strip(),
        "model": str(model or "").strip(),
        "base_url": str(base_url or "").strip(),
        "timeout_seconds": max(1, int(timeout_seconds or 60)),
        "preflight_timeout_seconds": max(1, int(preflight_timeout_seconds or 12)),
        "enabled": bool(enabled and str(model or "").strip() and str(base_url or "").strip()),
    }


def build_runtime_candidates(
    *,
    cfg: DealAnalyzerConfig,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    fallback2_model_override: str | None = None,
    fallback_timeout_override: int | None = None,
) -> list[dict[str, Any]]:
    main_model = str(main_model_override or cfg.ollama_model or "").strip()
    fallback_model = str(fallback_model_override or cfg.ollama_fallback_model or "").strip()
    fallback2_model = str(fallback2_model_override or "").strip()

    fallback_timeout = int(
        fallback_timeout_override
        if isinstance(fallback_timeout_override, int) and fallback_timeout_override > 0
        else (cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 60)
    )

    candidates = [
        _candidate_payload(
            name="main",
            model=main_model,
            base_url=str(cfg.ollama_base_url or "").strip() or "http://127.0.0.1:11434",
            timeout_seconds=int(cfg.ollama_timeout_seconds or 60),
            preflight_timeout_seconds=int(
                getattr(cfg, "ollama_preflight_timeout_seconds", cfg.ollama_timeout_seconds) or cfg.ollama_timeout_seconds
            ),
            enabled=True,
        ),
        _candidate_payload(
            name="fallback",
            model=fallback_model,
            base_url=str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "").strip() or "http://127.0.0.1:11434",
            timeout_seconds=fallback_timeout,
            preflight_timeout_seconds=int(
                getattr(cfg, "ollama_fallback_preflight_timeout_seconds", fallback_timeout) or fallback_timeout
            ),
            enabled=bool(cfg.ollama_fallback_enabled),
        ),
        _candidate_payload(
            name="fallback2",
            model=fallback2_model,
            base_url=str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "").strip() or "http://127.0.0.1:11434",
            timeout_seconds=fallback_timeout,
            preflight_timeout_seconds=int(
                getattr(cfg, "ollama_fallback_preflight_timeout_seconds", fallback_timeout) or fallback_timeout
            ),
            enabled=bool(fallback2_model),
        ),
    ]
    return candidates


def resolve_ollama_runtime(
    *,
    cfg: DealAnalyzerConfig,
    enabled: bool,
    logger: Any | None,
    log_prefix: str,
    main_model_override: str | None = None,
    fallback_model_override: str | None = None,
    fallback2_model_override: str | None = None,
    fallback_timeout_override: int | None = None,
    no_retry_on_rate_limit: bool = True,
) -> dict[str, Any]:
    candidates = build_runtime_candidates(
        cfg=cfg,
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
        fallback2_model_override=fallback2_model_override,
        fallback_timeout_override=fallback_timeout_override,
    )
    main_candidate = candidates[0]
    fallback_candidate = candidates[1]
    fallback2_candidate = candidates[2]
    runtime = {
        "enabled": bool(enabled),
        "main_ok": False,
        "fallback_ok": False,
        "fallback2_ok": False,
        "main_error": "",
        "fallback_error": "",
        "fallback2_error": "",
        "selected": "none",
        "reason": "",
        "main": {**main_candidate},
        "fallback": {**fallback_candidate},
        "fallback2": {**fallback2_candidate},
        "candidates": [{**candidate} for candidate in candidates],
        "no_retry_on_rate_limit": bool(no_retry_on_rate_limit),
        "preflight_results": [],
    }
    if not runtime["enabled"]:
        runtime["reason"] = "llm_backend_not_requested"
        return runtime

    selected = "none"
    selected_reason = "all_candidates_failed"
    selected_error_type = ""
    preflight_results: list[dict[str, Any]] = []

    for candidate in candidates:
        name = str(candidate.get("name") or "")
        model = str(candidate.get("model") or "")
        base_url = str(candidate.get("base_url") or "")
        timeout_seconds = int(candidate.get("timeout_seconds") or 60)
        preflight_timeout = int(candidate.get("preflight_timeout_seconds") or timeout_seconds)
        enabled_candidate = bool(candidate.get("enabled"))
        if not enabled_candidate:
            preflight_results.append(
                {
                    "candidate": name,
                    "model": model,
                    "base_url": base_url,
                    "ok": False,
                    "error": "candidate_disabled",
                    "error_type": "unknown",
                    "no_retry_due_to_rate_limit": False,
                }
            )
            continue
        if _is_local_gemma_runtime(model=model, base_url=base_url):
            preflight_timeout = max(
                1,
                int(getattr(cfg, "local_gemma_preflight_timeout_sec", preflight_timeout) or preflight_timeout),
            )
        client = OllamaClient(base_url=base_url, model=model, timeout_seconds=timeout_seconds)
        probe = client.preflight(probe_timeout_seconds=preflight_timeout)
        probe_error = str(probe.error or "")
        error_type = classify_llm_error(probe_error)
        soft_ok = _is_soft_preflight_ok(probe_error)
        ok = bool(probe.ok or soft_ok)
        no_retry_due_to_rate_limit = bool(no_retry_on_rate_limit and error_type == "cloud_usage_limit")
        preflight_results.append(
            {
                "candidate": name,
                "model": model,
                "base_url": base_url,
                "ok": ok,
                "error": probe_error,
                "error_type": error_type,
                "soft_ok": bool(soft_ok and not bool(probe.ok)),
                "no_retry_due_to_rate_limit": no_retry_due_to_rate_limit,
            }
        )
        runtime[f"{name}_ok"] = ok
        runtime[f"{name}_error"] = probe_error
        if name in {"main", "fallback", "fallback2"}:
            runtime[name]["enabled"] = enabled_candidate
        if ok and selected == "none":
            selected = name
            if bool(soft_ok and not bool(probe.ok)):
                selected_reason = f"{name}_soft_ok_nonjson"
            else:
                selected_reason = f"{name}_ok"
        if logger is not None:
            if ok:
                logger.info(
                    "%s preflight success: selected_candidate=%s base_url=%s model=%s timeout_seconds=%s",
                    log_prefix,
                    name,
                    base_url,
                    model,
                    timeout_seconds,
                )
            else:
                logger.warning(
                    "%s preflight failed: candidate=%s base_url=%s model=%s reason=%s",
                    log_prefix,
                    name,
                    base_url,
                    model,
                    probe_error,
                )
        if selected == "none":
            selected_error_type = error_type

    runtime["selected"] = selected
    runtime["reason"] = selected_reason if selected != "none" else ("main_unavailable_rate_limit" if selected_error_type == "cloud_usage_limit" else "main_and_fallback_failed")
    runtime["preflight_results"] = preflight_results

    # Backward-compat aliases.
    if selected == "fallback2":
        runtime["fallback_ok"] = bool(runtime.get("fallback_ok") or runtime.get("fallback2_ok"))
    if not runtime.get("fallback_ok"):
        runtime["fallback_ok"] = bool(runtime.get("fallback_ok"))
    return runtime


def _is_soft_preflight_ok(error_text: str) -> bool:
    low = str(error_text or "").strip().lower()
    if not low:
        return False
    return "not valid json object" in low or "invalid json" in low
