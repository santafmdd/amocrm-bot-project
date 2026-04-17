from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.enrichment import EnrichmentContext, build_operator_outputs, enrich_rows


def _cfg(*, client_enabled: bool = True, appointment_enabled: bool = True) -> DealAnalyzerConfig:
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
        analyzer_backend="rules",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:e4b",
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
        client_list_enrich_enabled=client_enabled,
        appointment_list_enrich_enabled=appointment_enabled,
        fields_mapping={"client_list": {}, "appointment_list": {}},
        operator_outputs_enabled=True,
    )


class _Logger:
    def __init__(self):
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, msg, *args):
        self.infos.append(msg % args if args else str(msg))

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else str(msg))


def test_matching_by_phone_and_partial_match():
    rows = [{"deal_id": 1001, "contact_phone": "+7 (999) 123-45-67", "company_name": "Ромашка", "contact_name": "Иван"}]
    ctx = EnrichmentContext(
        client_rows=[{"__row_id": "2", "телефон": "79991234567", "тест начат": "да"}],
        appointment_rows=[],
    )
    logger = _Logger()

    with patch("src.deal_analyzer.enrichment._load_context", return_value=ctx):
        out = enrich_rows(rows, config=_cfg(client_enabled=True, appointment_enabled=True), logger=logger)

    assert out[0]["enrichment_match_status"] == "partial"
    assert out[0]["enrichment_match_source"] == "client_list"
    assert out[0]["matched_client_list_row_id"] == "2"


def test_matching_by_email_and_company_fallback_and_no_match_path():
    logger = _Logger()
    rows = [
        {"deal_id": 2001, "contact_email": "user@example.com", "company_name": "Альфа"},
        {"deal_id": 2002, "company_name": "Бета"},
        {"deal_id": 2003, "company_name": "Гамма"},
    ]
    ctx = EnrichmentContext(
        client_rows=[
            {"__row_id": "3", "email": "user@example.com"},
            {"__row_id": "4", "компания": "Бета"},
        ],
        appointment_rows=[],
    )

    with patch("src.deal_analyzer.enrichment._load_context", return_value=ctx):
        out = enrich_rows(rows, config=_cfg(client_enabled=True, appointment_enabled=False), logger=logger)

    assert out[0]["matched_client_list_row_id"] == "3"
    assert out[1]["matched_client_list_row_id"] == "4"
    assert out[2]["enrichment_match_status"] == "none"


def test_build_operator_outputs_generates_blocks():
    deal = {
        "deal_id": 3001,
        "deal_name": "Сделка тест",
        "pipeline_name": "Привлечение",
        "status_name": "Квалификация",
        "enrichment_match_status": "partial",
        "pain_text": "",
        "business_tasks_text": "",
        "brief_url": "",
    }
    analysis = {
        "risk_flags": ["Мало данных"],
        "growth_zones": ["Добавить pain", "Добавить задачи"],
        "followup_quality_flag": "needs_attention",
    }
    out = build_operator_outputs(deal=deal, analysis=analysis)

    assert out["manager_summary"]
    assert out["employee_coaching"]
    assert isinstance(out["employee_fix_tasks"], list)
    assert 3 <= len(out["employee_fix_tasks"]) <= 7


def test_backward_compatibility_when_enrich_disabled():
    rows = [{"deal_id": 4001, "deal_name": "X"}]
    logger = _Logger()
    out = enrich_rows(rows, config=_cfg(client_enabled=False, appointment_enabled=False), logger=logger)

    assert out[0]["enrichment_match_status"] == "disabled"
    assert out[0]["enrichment_match_source"] == "none"
