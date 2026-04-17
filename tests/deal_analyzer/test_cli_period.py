from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.cli import _analyze_one_with_isolation, _analyze_period_rows
from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.llm_backend import LlmAnalysisOutcome
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


def _analysis(deal_id: int, backend_used: str) -> DealAnalysis:
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
    )


def test_analyze_period_continues_when_one_deal_falls_back():
    rows = [{"deal_id": 1}, {"deal_id": 2}, {"deal_id": 3}]
    logger = _Logger()

    side_effects = [
        LlmAnalysisOutcome(_analysis(1, "ollama"), "ollama", False, None),
        LlmAnalysisOutcome(_analysis(2, "rules_fallback"), "rules_fallback", True, "bad json"),
        LlmAnalysisOutcome(_analysis(3, "ollama"), "ollama", False, None),
    ]

    with patch("src.deal_analyzer.cli.analyze_deal_with_ollama_outcome", side_effect=side_effects):
        analyses, counts = _analyze_period_rows(rows, _cfg(), logger)

    assert len(analyses) == 3
    assert counts["llm_success_count"] == 2
    assert counts["llm_fallback_count"] == 1
    assert counts["llm_error_count"] == 1


def test_analyze_deal_and_period_pass_same_timeout_via_config_object():
    logger = _Logger()
    cfg = _cfg(timeout_seconds=42)
    seen_timeouts: list[int] = []

    def _capture(*, normalized_deal, config):
        seen_timeouts.append(config.ollama_timeout_seconds)
        deal_id = int(normalized_deal.get("deal_id") or 0)
        return LlmAnalysisOutcome(_analysis(deal_id, "ollama"), "ollama", False, None)

    with patch("src.deal_analyzer.cli.analyze_deal_with_ollama_outcome", side_effect=_capture):
        _analyze_one_with_isolation({"deal_id": 10}, cfg, logger, deal_hint="10")
        _analyze_period_rows([{"deal_id": 11}, {"deal_id": 12}], cfg, logger)

    assert seen_timeouts == [42, 42, 42]
