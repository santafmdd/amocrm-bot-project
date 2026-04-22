from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.cli import _analyze_one_with_isolation, _analyze_period_rows, _run_ollama_preflight
from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.llm_backend import LlmAnalysisOutcome
from src.deal_analyzer.llm_client import OllamaPreflightResult
from src.deal_analyzer.models import DealAnalysis


class _Logger:
    def __init__(self):
        self.warnings: list[str] = []
        self.infos: list[str] = []

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else str(msg))

    def info(self, msg, *args):
        self.infos.append(msg % args if args else str(msg))


def _cfg(timeout_seconds: int = 60) -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={
            "presentation": 20,
            "brief": 10,
            "demo_result": 10,
            "pain": 10,
            "business_tasks": 10,
            "followup_tasks": 10,
            "product_fit": 15,
            "probability": 5,
            "data_completeness": 10,
        },
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:e4b",
        ollama_timeout_seconds=timeout_seconds,
        style_profile_name="manager_ru_v1",
    )


def _analysis(deal_id: int, backend_used: str, repaired: bool = False) -> DealAnalysis:
    return DealAnalysis(
        deal_id=deal_id,
        amo_lead_id=deal_id,
        deal_name=f"Deal {deal_id}",
        score_0_100=50,
        strong_sides=[],
        growth_zones=[],
        risk_flags=[],
        presentation_quality_flag="ok",
        followup_quality_flag="ok",
        data_completeness_flag="partial",
        recommended_actions_for_manager=[],
        recommended_training_tasks_for_employee=[],
        manager_message_draft="",
        employee_training_message_draft="",
        analysis_backend_used=backend_used,
        llm_repair_applied=repaired,
    )


def test_analyze_period_continues_when_one_deal_falls_back_and_counts_repaired():
    rows = [{"deal_id": 1}, {"deal_id": 2}, {"deal_id": 3}]
    logger = _Logger()

    side_effects = [
        LlmAnalysisOutcome(_analysis(1, "ollama"), "ollama", False, None, False),
        LlmAnalysisOutcome(_analysis(2, "rules_fallback"), "rules_fallback", True, "bad json", False),
        LlmAnalysisOutcome(_analysis(3, "ollama", repaired=True), "ollama", False, None, True),
    ]

    with patch("src.deal_analyzer.cli.analyze_deal_with_ollama_outcome", side_effect=side_effects):
        analyses, counts = _analyze_period_rows(rows, _cfg(), logger, backend_override="ollama")

    assert len(analyses) == 3
    assert counts["llm_success_count"] == 2
    assert counts["llm_success_repaired_count"] == 1
    assert counts["llm_fallback_count"] == 1
    assert counts["llm_error_count"] == 1


def test_analyze_deal_and_period_pass_same_timeout_via_config_object():
    logger = _Logger()
    cfg = _cfg(timeout_seconds=42)
    seen_timeouts: list[int] = []

    def _capture(*, normalized_deal, config):
        seen_timeouts.append(config.ollama_timeout_seconds)
        deal_id = int(normalized_deal.get("deal_id") or 0)
        return LlmAnalysisOutcome(_analysis(deal_id, "ollama"), "ollama", False, None, False)

    with patch("src.deal_analyzer.cli.analyze_deal_with_ollama_outcome", side_effect=_capture):
        _analyze_one_with_isolation({"deal_id": 10}, cfg, logger, deal_hint="10", backend_override="ollama")
        _analyze_period_rows([{"deal_id": 11}, {"deal_id": 12}], cfg, logger, backend_override="ollama")

    assert seen_timeouts == [42, 42, 42]


def test_preflight_success_path():
    logger = _Logger()

    class _Client:
        def __init__(self, *, base_url, model, timeout_seconds):
            pass

        def preflight(self, *, probe_timeout_seconds):
            return OllamaPreflightResult(ok=True, error=None)

    with patch("src.deal_analyzer.llm_runtime.OllamaClient", _Client):
        forced_rules = _run_ollama_preflight(_cfg(), logger)

    assert forced_rules is False
    assert any("preflight success" in msg for msg in logger.infos)


def test_preflight_fail_path_switches_to_rules():
    logger = _Logger()

    class _Client:
        def __init__(self, *, base_url, model, timeout_seconds):
            pass

        def preflight(self, *, probe_timeout_seconds):
            return OllamaPreflightResult(ok=False, error="connect failed")

    with patch("src.deal_analyzer.llm_runtime.OllamaClient", _Client):
        forced_rules = _run_ollama_preflight(_cfg(), logger)

    assert forced_rules is True
    assert any("preflight failed" in msg for msg in logger.warnings)


def test_preflight_failover_to_fallback_does_not_force_rules():
    logger = _Logger()
    cfg = DealAnalyzerConfig(**{**_cfg().__dict__, "ollama_fallback_enabled": True, "ollama_fallback_model": "deepseek-v3.1:671b-cloud"})

    class _Client:
        def __init__(self, *, base_url, model, timeout_seconds):
            self.model = model

        def preflight(self, *, probe_timeout_seconds):
            if self.model == "gemma4:e4b":
                return OllamaPreflightResult(ok=False, error="main_down")
            return OllamaPreflightResult(ok=True, error=None)

    with patch("src.deal_analyzer.llm_runtime.OllamaClient", _Client):
        forced_rules = _run_ollama_preflight(cfg, logger)

    assert forced_rules is False


def test_preflight_soft_nonjson_is_treated_as_live_runtime():
    logger = _Logger()

    class _Client:
        def __init__(self, *, base_url, model, timeout_seconds):
            pass

        def preflight(self, *, probe_timeout_seconds):
            return OllamaPreflightResult(ok=False, error="Ollama content is not valid JSON object: hello")

    with patch("src.deal_analyzer.llm_runtime.OllamaClient", _Client):
        forced_rules = _run_ollama_preflight(_cfg(), logger)

    assert forced_rules is False


def test_cli_analyze_period_parses_limit():
    import sys
    from unittest.mock import patch as _patch
    from src.deal_analyzer.cli import _parse_args as _parse

    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "analyze-period",
        "--input",
        "workspace/amocrm_collector/collect_period_latest.json",
        "--limit",
        "7",
    ]
    with _patch.object(sys, "argv", argv):
        args = _parse()
    assert args.command == "analyze-period"
    assert args.limit == 7


def test_hybrid_success_path_uses_hybrid_backend_and_counts_llm_success():
    logger = _Logger()
    cfg = _cfg(timeout_seconds=50)
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "analyzer_backend": "hybrid"})

    outcome = LlmAnalysisOutcome(
        _analysis(21, "hybrid"),
        "hybrid",
        False,
        None,
        False,
    )

    with patch("src.deal_analyzer.cli.analyze_deal_with_hybrid_outcome", return_value=outcome):
        analysis, counts = _analyze_one_with_isolation({"deal_id": 21}, cfg, logger, deal_hint="21", backend_override="hybrid")

    assert analysis["analysis_backend_used"] == "hybrid"
    assert counts["llm_success_count"] == 1
    assert counts["llm_fallback_count"] == 0
    assert counts["llm_error_count"] == 0


def test_hybrid_invalid_json_or_timeout_falls_back_without_crashing():
    logger = _Logger()
    cfg = _cfg(timeout_seconds=50)
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "analyzer_backend": "hybrid"})

    outcome = LlmAnalysisOutcome(
        _analysis(22, "rules_fallback"),
        "rules_fallback",
        True,
        "bad json",
        False,
    )

    with patch("src.deal_analyzer.cli.analyze_deal_with_hybrid_outcome", return_value=outcome):
        analysis, counts = _analyze_one_with_isolation({"deal_id": 22}, cfg, logger, deal_hint="22", backend_override="hybrid")

    assert analysis["analysis_backend_used"] == "rules_fallback"
    assert counts["llm_success_count"] == 0
    assert counts["llm_fallback_count"] == 1
    assert counts["llm_error_count"] == 1


def test_rules_only_path_unchanged_with_hybrid_available():
    logger = _Logger()
    cfg = DealAnalyzerConfig(**{**_cfg().__dict__, "analyzer_backend": "rules"})
    analysis, counts = _analyze_one_with_isolation({"deal_id": 23, "deal_name": "Rules only"}, cfg, logger, deal_hint="23")
    assert analysis["analysis_backend_used"] == "rules"
    assert analysis["analysis_backend_requested"] == "rules"
    assert counts == {
        "llm_success_count": 0,
        "llm_success_repaired_count": 0,
        "llm_fallback_count": 0,
        "llm_error_count": 0,
    }
