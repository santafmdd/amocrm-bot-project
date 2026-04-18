from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.rules import analyze_deal


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
        analyzer_backend="rules",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:e4b",
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
    )


def test_analyze_deal_high_score_when_core_signals_present():
    row = {
        "deal_id": 1,
        "amo_lead_id": 1,
        "deal_name": "Deal",
        "presentation_detected": True,
        "brief_url": "https://docs.google.com/x",
        "demo_result_text": "done",
        "pain_text": "pain",
        "business_tasks_text": "tasks",
        "product_values": ["Product"],
        "probability_value": 75,
        "tasks_summary_raw": [{"id": 1}],
        "notes_summary_raw": [{"id": 2}],
        "created_at": 10,
        "updated_at": 20,
    }

    analysis = analyze_deal(row, _cfg())
    assert analysis.score_0_100 >= 80
    assert analysis.presentation_quality_flag == "ok"
    assert analysis.data_completeness_flag in {"complete", "partial"}


def test_analyze_deal_adds_risk_flag_for_empty_context_with_movement():
    row = {
        "deal_id": 2,
        "amo_lead_id": 2,
        "deal_name": "Deal 2",
        "presentation_detected": False,
        "brief_url": "",
        "demo_result_text": "",
        "pain_text": "",
        "business_tasks_text": "",
        "product_values": [],
        "probability_value": None,
        "tasks_summary_raw": [],
        "notes_summary_raw": [],
        "company_comment": "",
        "contact_comment": "",
        "created_at": 100,
        "updated_at": 200,
    }

    analysis = analyze_deal(row, _cfg())
    assert any("notes/tasks/comments" in x for x in analysis.risk_flags)
    assert analysis.score_0_100 < 40


def test_rules_user_facing_texts_are_readable_and_without_question_marks():
    row = {
        "deal_id": 3,
        "amo_lead_id": 3,
        "deal_name": "Deal 3",
        "presentation_detected": False,
        "brief_url": "",
        "demo_result_text": "",
        "pain_text": "",
        "business_tasks_text": "",
        "product_values": [],
        "probability_value": None,
        "tasks_summary_raw": [],
        "notes_summary_raw": [],
        "company_comment": "",
        "contact_comment": "",
        "created_at": 10,
        "updated_at": 20,
    }

    analysis = analyze_deal(row, _cfg())
    combined = (
        analysis.strong_sides
        + analysis.growth_zones
        + analysis.risk_flags
        + analysis.recommended_actions_for_manager
        + analysis.recommended_training_tasks_for_employee
    )
    assert combined
    assert all("?" not in item for item in combined)
    assert all(str(item).strip() for item in combined)
