from src.deal_analyzer.models import AnalysisRunMetadata, DealAnalysis


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


def test_analysis_run_metadata_public_visibility_toggle():
    metadata = AnalysisRunMetadata(
        executed_at="2026-04-17T09:00:00+00:00",
        period_mode_resolved="previous_workweek",
        period_start="2026-04-06",
        period_end="2026-04-10",
        public_period_label="2026-04-06..2026-04-10",
        as_of_date="2026-04-17",
    )

    pub_hidden = metadata.to_public_dict(include_executed_at=False)
    assert "executed_at" not in pub_hidden

    pub_visible = metadata.to_public_dict(include_executed_at=True)
    assert pub_visible["executed_at"] == "2026-04-17T09:00:00+00:00"
