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
    row = {
        "deal_id": 1,
        "amo_lead_id": 1,
        "deal_name": "Demo",
        "notes_summary_raw": [{"text": "Тендерный контур и сравнение КП"}],
    }
    llm_payload = {
        "product_hypothesis_llm": "link",
        "loss_reason_short": "Потеря на позднем согласовании",
        "manager_insight_short": "Нужен более ранний контроль критериев закупки",
        "coaching_hint_short": "Отработать сценарий с возражением по тендеру",
        "reanimation_reason_short_llm": "Есть шанс вернуться через короткий follow-up при изменении условий",
    }

    out = analyze_deal_with_ollama_outcome(normalized_deal=row, config=_cfg(), client=_FakeClient(llm_payload))
    baseline = out.analysis.to_dict()
    assert isinstance(baseline["score_0_100"], int)
    assert out.analysis.product_hypothesis_llm == "link"
    assert out.analysis.loss_reason_short == "Потеря на позднем согласовании"
    assert out.analysis.reanimation_reason_short_llm.startswith("Есть шанс")
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
    row = {
        "deal_id": 5,
        "amo_lead_id": 5,
        "deal_name": "Hybrid Deal",
        "notes_summary_raw": [{"text": "Есть контекст по продукту INFO"}],
    }
    payload = {
        "product_hypothesis_llm": "info",
        "loss_reason_short": "Анти-fit по внедрению",
        "manager_insight_short": "Кейс не из целевого сегмента",
        "coaching_hint_short": "Ранний отсев по признакам anti-fit",
        "reanimation_reason_short_llm": "Реанимация нецелесообразна без изменения fit",
    }

    out = analyze_deal_with_hybrid_outcome(normalized_deal=row, config=cfg, client=_FakeClient(payload))
    assert out.backend_used == "hybrid"
    assert out.llm_error is False
    assert out.analysis.analysis_backend_used == "hybrid"
    assert out.analysis.loss_reason_short == "Анти-fit по внедрению"
    assert out.analysis.manager_insight_short == "Кейс не из целевого сегмента"
    assert out.analysis.coaching_hint_short == "Ранний отсев по признакам anti-fit"
    assert out.analysis.product_hypothesis_llm == "info"
    assert out.analysis.reanimation_reason_short_llm == "Реанимация нецелесообразна без изменения fit"
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
    assert out.analysis.product_hypothesis_llm == "unknown"
    assert out.analysis.reanimation_reason_short_llm == ""
    assert out.analysis.llm_fallback is True
    assert out.analysis.llm_error is True


def test_hybrid_does_not_override_rules_score_or_risk_flags():
    cfg = _cfg()
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "analyzer_backend": "hybrid"})
    row = {"deal_id": 7, "deal_name": "Stable rules source", "status_name": "В работе"}
    payload = {
        "loss_reason_short": "Любой текст",
        "manager_insight_short": "Коротко",
        "coaching_hint_short": "Подсказка",
        "product_hypothesis_llm": "mixed",
        "score_0_100": 3,
        "risk_flags": ["fake"],
    }
    out = analyze_deal_with_hybrid_outcome(normalized_deal=row, config=cfg, client=_FakeClient(payload))
    rules_ref = analyze_deal_with_hybrid_outcome(
        normalized_deal=row,
        config=cfg,
        client=_FakeClient(
            {
                "loss_reason_short": "",
                "manager_insight_short": "",
                "coaching_hint_short": "",
                "product_hypothesis_llm": "unknown",
            }
        ),
    )
    assert out.analysis.score_0_100 == rules_ref.analysis.score_0_100
    assert out.analysis.risk_flags == rules_ref.analysis.risk_flags


def test_hybrid_product_hypothesis_llm_is_unknown_on_sparse_context():
    cfg = _cfg()
    cfg = DealAnalyzerConfig(**{**cfg.__dict__, "analyzer_backend": "hybrid"})
    row = {"deal_id": 8, "deal_name": "Sparse context", "notes_summary_raw": [], "tasks_summary_raw": []}
    payload = {
        "product_hypothesis_llm": "info",
        "loss_reason_short": "Недостаточно данных",
        "manager_insight_short": "Пусто",
        "coaching_hint_short": "Пусто",
    }
    out = analyze_deal_with_hybrid_outcome(normalized_deal=row, config=cfg, client=_FakeClient(payload))
    assert out.analysis.product_hypothesis_llm == "unknown"
