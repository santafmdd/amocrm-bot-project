from src.deal_analyzer.models import DealAnalysis


def test_deal_analysis_to_dict_contains_required_keys():
    analysis = DealAnalysis(
        deal_id=1,
        amo_lead_id=1,
        deal_name="Test",
        score_0_100=70,
        strong_sides=["a"],
        growth_zones=["b"],
        risk_flags=["c"],
        presentation_quality_flag="ok",
        followup_quality_flag="needs_attention",
        data_completeness_flag="partial",
        recommended_actions_for_manager=["x"],
        recommended_training_tasks_for_employee=["y"],
        manager_message_draft="m",
        employee_training_message_draft="e",
    )

    payload = analysis.to_dict()
    assert payload["deal_id"] == 1
    assert payload["score_0_100"] == 70
    assert payload["manager_message_draft"] == "m"
    assert "recommended_training_tasks_for_employee" in payload
