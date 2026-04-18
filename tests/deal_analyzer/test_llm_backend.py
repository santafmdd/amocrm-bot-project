from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.llm_backend import analyze_deal_with_hybrid_outcome, analyze_deal_with_ollama_outcome
from src.deal_analyzer.llm_client import OllamaClientError, ParsedJsonResponse


class _FakeClient:
    def __init__(self, payload, repair_applied: bool = False):
        self.payload = payload
        self.repair_applied = repair_applied

    def chat_json(self, *, messages):
        return ParsedJsonResponse(payload=self.payload, repair_applied=self.repair_applied)


class _RetryThenSuccessClient:
    def __init__(self):
        self.calls = 0

    def chat_json(self, *, messages):
        self.calls += 1
        if self.calls == 1:
            raise OllamaClientError("bad json")
        return ParsedJsonResponse(payload={"score_0_100": 66, "strong_sides": ["Есть контакт"]}, repair_applied=True)


class _AlwaysFailClient:
    def chat_json(self, *, messages):
        raise OllamaClientError("still bad json")


def _cfg() -> DealAnalyzerConfig:
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
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
    )


def test_llm_backend_merges_llm_payload_into_baseline_contract():
    row = {"deal_id": 1, "amo_lead_id": 1, "deal_name": "Demo"}
    llm_payload = {
        "score_0_100": 61,
        "strong_sides": ["Есть контакт"],
        "growth_zones": ["Добавить brief"],
        "risk_flags": ["Мало данных"],
        "recommended_actions_for_manager": ["Проверить next step"],
        "recommended_training_tasks_for_employee": ["Тренинг по квалификации"],
        "manager_message_draft": "Черновик менеджеру",
        "employee_training_message_draft": "Черновик сотруднику",
        "presentation_quality_flag": "needs_attention",
        "followup_quality_flag": "ok",
        "data_completeness_flag": "partial",
    }

    out = analyze_deal_with_ollama_outcome(normalized_deal=row, config=_cfg(), client=_FakeClient(llm_payload))
    assert out.analysis.score_0_100 == 61
    assert out.analysis.strong_sides == ["Есть контакт"]
    assert out.backend_used == "ollama"
    assert out.llm_error is False
    assert out.analysis.analysis_backend_used == "ollama"
    assert out.analysis.llm_repair_applied is False


def test_llm_backend_retries_and_succeeds_on_second_attempt():
    out = analyze_deal_with_ollama_outcome(
        normalized_deal={"deal_id": 2, "deal_name": "Retry Demo"}, config=_cfg(), client=_RetryThenSuccessClient()
    )
    assert out.backend_used == "ollama"
    assert out.llm_error is False
    assert out.analysis.analysis_backend_used == "ollama"
    assert out.analysis.llm_repair_applied is True
    assert out.repaired is True


def test_llm_backend_falls_back_to_rules_after_failed_retry():
    out = analyze_deal_with_ollama_outcome(
        normalized_deal={"deal_id": 3, "deal_name": "Bad JSON"}, config=_cfg(), client=_AlwaysFailClient()
    )
    assert out.backend_used == "rules_fallback"
    assert out.llm_error is True
    assert out.error_message is not None
    assert out.analysis.analysis_backend_used == "rules_fallback"
    assert out.analysis.llm_repair_applied is False


def test_llm_backend_uses_timeout_from_config_when_creating_client():
    cfg = _cfg()
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "ollama_timeout_seconds": 37})

    captured: dict[str, object] = {}

    class _ConstructedClient:
        def __init__(self, *, base_url, model, timeout_seconds):
            captured["base_url"] = base_url
            captured["model"] = model
            captured["timeout_seconds"] = timeout_seconds

        def chat_json(self, *, messages):
            return ParsedJsonResponse(payload={"score_0_100": 70}, repair_applied=False)

    with patch("src.deal_analyzer.llm_backend.OllamaClient", _ConstructedClient):
        out = analyze_deal_with_ollama_outcome(normalized_deal={"deal_id": 4}, config=cfg, client=None)

    assert out.backend_used == "ollama"
    assert captured["timeout_seconds"] == 37
    assert captured["model"] == cfg.ollama_model
    assert captured["base_url"] == cfg.ollama_base_url


def test_hybrid_backend_success_adds_short_fields_without_overriding_rules_payload():
    cfg = _cfg()
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "analyzer_backend": "hybrid"})
    row = {"deal_id": 5, "amo_lead_id": 5, "deal_name": "Hybrid Deal"}
    payload = {
        "loss_reason_short": "Анти-fit по внедрению",
        "manager_insight_short": "Кейс не из целевого сегмента",
        "coaching_hint_short": "Ранний отсев по признакам anti-fit",
    }

    out = analyze_deal_with_hybrid_outcome(normalized_deal=row, config=cfg, client=_FakeClient(payload))
    assert out.backend_used == "hybrid"
    assert out.llm_error is False
    assert out.analysis.analysis_backend_used == "hybrid"
    assert out.analysis.loss_reason_short == "Анти-fit по внедрению"
    assert out.analysis.manager_insight_short == "Кейс не из целевого сегмента"
    assert out.analysis.coaching_hint_short == "Ранний отсев по признакам anti-fit"
    assert out.analysis.llm_fallback is False
    assert out.analysis.llm_error is False


def test_hybrid_backend_invalid_json_or_timeout_falls_back_to_rules_only():
    cfg = _cfg()
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "analyzer_backend": "hybrid"})

    out = analyze_deal_with_hybrid_outcome(
        normalized_deal={"deal_id": 6, "deal_name": "Hybrid Bad"}, config=cfg, client=_AlwaysFailClient()
    )
    assert out.backend_used == "rules_fallback"
    assert out.llm_error is True
    assert out.analysis.analysis_backend_used == "rules_fallback"
    assert out.analysis.loss_reason_short == ""
    assert out.analysis.manager_insight_short == ""
    assert out.analysis.coaching_hint_short == ""
    assert out.analysis.llm_fallback is True
    assert out.analysis.llm_error is True
