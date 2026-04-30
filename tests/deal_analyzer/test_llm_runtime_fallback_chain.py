from __future__ import annotations

from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.llm_runtime import classify_llm_error, resolve_ollama_runtime, should_retry_same_model


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:31b-cloud",
        ollama_timeout_seconds=120,
        ollama_fallback_enabled=True,
        ollama_fallback_base_url="http://127.0.0.1:11434",
        ollama_fallback_model="gpt-oss:20b",
        ollama_fallback_timeout_seconds=2400,
    )


def test_classify_llm_error_cloud_usage_limit() -> None:
    err = "HTTP 429: you have reached your weekly usage limit"
    assert classify_llm_error(err) == "cloud_usage_limit"
    assert should_retry_same_model(error_text=err, no_retry_on_rate_limit=True) is False


def test_classify_llm_error_context_overflow() -> None:
    err = "HTTP 400: prompt too long; exceeded max context length by 65535 tokens"
    assert classify_llm_error(err) == "context_overflow"
    assert (
        should_retry_same_model(
            error_text=err,
            no_retry_on_rate_limit=True,
            no_retry_on_context_overflow=True,
        )
        is False
    )


def test_resolve_runtime_429_main_selects_fallback(monkeypatch) -> None:
    class _FakeProbe:
        def __init__(self, ok: bool, error: str):
            self.ok = ok
            self.error = error

    class _FakeClient:
        def __init__(self, *, base_url: str, model: str, timeout_seconds: int):
            _ = base_url, timeout_seconds
            self.model = model

        def preflight(self, *, probe_timeout_seconds: int = 5):
            _ = probe_timeout_seconds
            if self.model.startswith("gemma4"):
                return _FakeProbe(False, "HTTP 429: weekly usage limit")
            if self.model.startswith("gpt-oss"):
                return _FakeProbe(True, "")
            return _FakeProbe(False, "connection error: refused")

    monkeypatch.setattr("src.deal_analyzer.llm_runtime.OllamaClient", _FakeClient)
    runtime = resolve_ollama_runtime(
        cfg=_cfg(),
        enabled=True,
        logger=None,
        log_prefix="test",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="gpt-oss:20b",
        fallback2_model_override="deepseek-v3.1:671b-cloud",
        no_retry_on_rate_limit=True,
    )
    assert runtime["selected"] == "fallback"
    preflight = runtime.get("preflight_results", [])
    main = next(item for item in preflight if item.get("candidate") == "main")
    assert main.get("error_type") == "cloud_usage_limit"
    assert main.get("no_retry_due_to_rate_limit") is True


def test_resolve_runtime_context_overflow_main_selects_fallback(monkeypatch) -> None:
    class _FakeProbe:
        def __init__(self, ok: bool, error: str):
            self.ok = ok
            self.error = error

    class _FakeClient:
        def __init__(self, *, base_url: str, model: str, timeout_seconds: int):
            _ = base_url, timeout_seconds
            self.model = model

        def preflight(self, *, probe_timeout_seconds: int = 5):
            _ = probe_timeout_seconds
            if self.model.startswith("gemma4"):
                return _FakeProbe(False, "HTTP 400: prompt too long; exceeded max context length")
            if self.model.startswith("gpt-oss"):
                return _FakeProbe(True, "")
            return _FakeProbe(False, "connection error: refused")

    monkeypatch.setattr("src.deal_analyzer.llm_runtime.OllamaClient", _FakeClient)
    runtime = resolve_ollama_runtime(
        cfg=_cfg(),
        enabled=True,
        logger=None,
        log_prefix="test",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="gpt-oss:20b",
        fallback2_model_override="deepseek-v3.1:671b-cloud",
        no_retry_on_rate_limit=True,
    )
    assert runtime["selected"] == "fallback"
    preflight = runtime.get("preflight_results", [])
    main = next(item for item in preflight if item.get("candidate") == "main")
    assert main.get("error_type") == "context_overflow"


def test_resolve_runtime_uses_local_fallback_when_cloud_candidates_limited(monkeypatch) -> None:
    class _FakeProbe:
        def __init__(self, ok: bool, error: str):
            self.ok = ok
            self.error = error

    class _FakeClient:
        def __init__(self, *, base_url: str, model: str, timeout_seconds: int):
            _ = base_url, timeout_seconds
            self.model = model

        def preflight(self, *, probe_timeout_seconds: int = 5):
            _ = probe_timeout_seconds
            if self.model in {"gemma4:31b-cloud", "deepseek-v3.1:671b-cloud"}:
                return _FakeProbe(False, "HTTP 429: session limit")
            if self.model == "gpt-oss:20b":
                return _FakeProbe(True, "")
            return _FakeProbe(False, "unknown")

    monkeypatch.setattr("src.deal_analyzer.llm_runtime.OllamaClient", _FakeClient)
    runtime = resolve_ollama_runtime(
        cfg=_cfg(),
        enabled=True,
        logger=None,
        log_prefix="test",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        fallback2_model_override="gpt-oss:20b",
        no_retry_on_rate_limit=True,
    )
    assert runtime["selected"] == "fallback2"
    assert runtime.get("fallback2_ok") is True
