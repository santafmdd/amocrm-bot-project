from __future__ import annotations

from typing import Any

from .config import DealAnalyzerConfig
from .llm_client import OllamaClient


def resolve_ollama_runtime(
    *,
    cfg: DealAnalyzerConfig,
    enabled: bool,
    logger: Any | None,
    log_prefix: str,
) -> dict[str, Any]:
    runtime = {
        "enabled": bool(enabled),
        "main_ok": False,
        "fallback_ok": False,
        "main_error": "",
        "fallback_error": "",
        "selected": "none",
        "reason": "",
        "main": {
            "base_url": cfg.ollama_base_url,
            "model": cfg.ollama_model,
            "timeout_seconds": cfg.ollama_timeout_seconds,
        },
        "fallback": {
            "base_url": cfg.ollama_fallback_base_url or cfg.ollama_base_url,
            "model": cfg.ollama_fallback_model or cfg.ollama_model,
            "timeout_seconds": cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds,
            "enabled": bool(cfg.ollama_fallback_enabled),
        },
    }
    if not runtime["enabled"]:
        runtime["reason"] = "llm_backend_not_requested"
        return runtime

    main_client = OllamaClient(
        base_url=cfg.ollama_base_url,
        model=cfg.ollama_model,
        timeout_seconds=cfg.ollama_timeout_seconds,
    )
    main_probe_timeout = min(max(3, int(cfg.ollama_timeout_seconds)), 12)
    main_probe = main_client.preflight(probe_timeout_seconds=main_probe_timeout)
    main_soft_ok = _is_soft_preflight_ok(str(main_probe.error or ""))
    runtime["main_ok"] = bool(main_probe.ok or main_soft_ok)
    runtime["main_error"] = str(main_probe.error or "")
    if runtime["main_ok"]:
        runtime["selected"] = "main"
        runtime["reason"] = "main_soft_ok_nonjson" if main_soft_ok and not bool(main_probe.ok) else "main_ok"
        if logger is not None:
            logger.info(
                "%s preflight success: selected=main base_url=%s model=%s timeout_seconds=%s",
                log_prefix,
                cfg.ollama_base_url,
                cfg.ollama_model,
                cfg.ollama_timeout_seconds,
            )
    else:
        if logger is not None:
            logger.warning(
                "%s preflight failed: candidate=main base_url=%s model=%s reason=%s",
                log_prefix,
                cfg.ollama_base_url,
                cfg.ollama_model,
                runtime["main_error"],
            )

    if not bool(cfg.ollama_fallback_enabled):
        if not runtime["main_ok"]:
            runtime["reason"] = "fallback_disabled"
        return runtime

    fb_base = str(cfg.ollama_fallback_base_url or cfg.ollama_base_url)
    fb_model = str(cfg.ollama_fallback_model or cfg.ollama_model)
    fb_timeout = int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds)
    fb_client = OllamaClient(
        base_url=fb_base,
        model=fb_model,
        timeout_seconds=fb_timeout,
    )
    fb_probe_timeout = min(max(3, fb_timeout), 12)
    fb_probe = fb_client.preflight(probe_timeout_seconds=fb_probe_timeout)
    fb_soft_ok = _is_soft_preflight_ok(str(fb_probe.error or ""))
    runtime["fallback_ok"] = bool(fb_probe.ok or fb_soft_ok)
    runtime["fallback_error"] = str(fb_probe.error or "")
    if runtime["fallback_ok"]:
        if not runtime["main_ok"]:
            runtime["selected"] = "fallback"
            runtime["reason"] = "fallback_soft_ok_nonjson" if fb_soft_ok and not bool(fb_probe.ok) else "fallback_ok"
            if logger is not None:
                logger.warning(
                    "%s failover activated: selected=fallback base_url=%s model=%s timeout_seconds=%s",
                    log_prefix,
                    fb_base,
                    fb_model,
                    fb_timeout,
                )
        elif logger is not None:
            logger.info(
                "%s fallback preflight success: model=%s base_url=%s timeout_seconds=%s (standby)",
                log_prefix,
                fb_model,
                fb_base,
                fb_timeout,
            )
    else:
        if not runtime["main_ok"]:
            runtime["reason"] = "main_and_fallback_failed"
        if logger is not None:
            logger.warning(
                "%s fallback preflight failed: base_url=%s model=%s reason=%s",
                log_prefix,
                fb_base,
                fb_model,
                runtime["fallback_error"],
            )
    return runtime


def _is_soft_preflight_ok(error_text: str) -> bool:
    low = str(error_text or "").strip().lower()
    if not low:
        return False
    return "not valid json object" in low or "invalid json" in low
