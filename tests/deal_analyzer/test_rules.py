from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.enrichment import build_operator_outputs
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


def test_empty_closed_deal_stays_low_and_has_evidence_context_risk():
    row = {
        "deal_id": 10,
        "amo_lead_id": 10,
        "deal_name": "Empty closed",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "contact_phone": "",
        "contact_email": "",
        "company_name": "",
        "company_inn": "",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert analysis.score_0_100 <= 20
    assert any(flag.startswith("evidence_context:") for flag in analysis.risk_flags)


def test_closed_deal_with_reasoned_loss_note_marked_as_qualified_loss():
    row = {
        "deal_id": 11,
        "amo_lead_id": 11,
        "deal_name": "Reasoned loss",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [{"text": "Клиент сказал: свои разработки, в облаке работать не будут"}],
        "tasks_summary_raw": [],
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert any(flag.startswith("qualified_loss:") for flag in analysis.risk_flags)
    assert analysis.score_0_100 > 0


def test_closed_deal_with_context_without_demo_not_forced_to_zero():
    row = {
        "deal_id": 12,
        "amo_lead_id": 12,
        "deal_name": "Context no demo",
        "status_name": "Закрыто и не реализовано",
        "demo_result_text": "",
        "brief_url": "",
        "notes_summary_raw": [{"text": "Обсудили бюджет и причины отказа, не подошли сроки внедрения"}],
        "tasks_summary_raw": [{"text": "Повторный контакт через 2 месяца"}],
        "contact_phone": ["+7 900 000 00 00"],
        "company_name": "ООО Тест",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert analysis.score_0_100 > 0
    assert analysis.data_completeness_flag in {"partial", "complete"}


def test_deal_with_notes_tasks_and_contact_data_is_not_zero():
    row = {
        "deal_id": 13,
        "amo_lead_id": 13,
        "deal_name": "Contextful",
        "status_name": "В работе",
        "notes_summary_raw": [{"text": "Уточнили потребность и ограничения"}],
        "tasks_summary_raw": [{"text": "Назначить follow-up звонок"}],
        "contact_phone": ["+7 900 111 22 33"],
        "contact_email": ["client@example.com"],
        "company_name": "ООО Ромашка",
        "company_inn": "7701234567",
        "tags": ["приоритет"],
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert analysis.score_0_100 > 0
    assert not any(flag.startswith("qualified_loss:") for flag in analysis.risk_flags)


def test_policy_qualified_loss_avoids_default_followup_presentation_push():
    row = {
        "deal_id": 14,
        "amo_lead_id": 14,
        "deal_name": "Qualified loss policy",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [{"text": "Отказ, свои разработки, в облаке работать не будут"}],
        "tasks_summary_raw": [],
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert any(flag.startswith("qualified_loss:") for flag in analysis.risk_flags)
    manager_actions_joined = " ".join(analysis.recommended_actions_for_manager).lower()
    employee_tasks_joined = " ".join(analysis.recommended_training_tasks_for_employee).lower()
    assert "назначить и провести презентацию" not in manager_actions_joined
    assert "поставить follow-up задачу" not in manager_actions_joined
    assert "обновить вероятность сделки" not in manager_actions_joined
    assert "anti-fit" in manager_actions_joined or "рыноч" in manager_actions_joined or "нецелевой" in manager_actions_joined
    assert "давление" in employee_tasks_joined or "квалификац" in employee_tasks_joined or "отказа" in employee_tasks_joined


def test_policy_closed_lost_without_context_focuses_on_closeout_classification_not_demo():
    row = {
        "deal_id": 17,
        "amo_lead_id": 17,
        "deal_name": "Closed lost no context",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    manager_actions_joined = " ".join(analysis.recommended_actions_for_manager).lower()
    assert "презентац" not in manager_actions_joined
    assert "follow-up" not in manager_actions_joined
    assert "вероятност" not in manager_actions_joined
    assert "причин" in manager_actions_joined or "класси" in manager_actions_joined or "закры" in manager_actions_joined


def test_policy_evidence_context_prioritizes_crm_evidence_completion():
    row = {
        "deal_id": 15,
        "amo_lead_id": 15,
        "deal_name": "Evidence gap policy",
        "status_name": "В работе",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "contact_phone": "",
        "contact_email": "",
        "company_name": "",
        "company_inn": "",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert any(flag.startswith("evidence_context:") for flag in analysis.risk_flags)
    manager_actions_joined = " ".join(analysis.recommended_actions_for_manager).lower()
    assert "notes" in manager_actions_joined or "контекст" in manager_actions_joined or "crm" in manager_actions_joined


def test_policy_process_hygiene_keeps_followup_recommendations():
    row = {
        "deal_id": 16,
        "amo_lead_id": 16,
        "deal_name": "Process hygiene policy",
        "status_name": "В работе",
        "notes_summary_raw": [{"text": "Контекст зафиксирован, есть интерес клиента"}],
        "tasks_summary_raw": [],
        "contact_phone": ["+7 900 111 22 33"],
        "company_name": "ООО Процесс",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert any(flag.startswith("process_hygiene:") for flag in analysis.risk_flags)
    manager_actions_joined = " ".join(analysis.recommended_actions_for_manager).lower()
    assert "follow-up" in manager_actions_joined or "вероятност" in manager_actions_joined


def test_won_status_returns_safe_handoff_recommendations():
    row = {
        "deal_id": 18,
        "amo_lead_id": 18,
        "deal_name": "Won deal",
        "status_name": "Успешно реализовано",
        "notes_summary_raw": [{"text": "Сделка закрыта успешно"}],
        "tasks_summary_raw": [{"text": "Передача в сопровождение"}],
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    manager_actions_joined = " ".join(analysis.recommended_actions_for_manager).lower()
    assert "handoff" in manager_actions_joined or "post-sale" in manager_actions_joined or "передач" in manager_actions_joined
    assert "поставить follow-up задачу" not in manager_actions_joined


def test_employee_outputs_for_qualified_loss_no_generic_demo_followup_leakage():
    row = {
        "deal_id": 19,
        "amo_lead_id": 19,
        "deal_name": "Qualified loss employee policy",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [{"text": "Клиент отказался: свои разработки, не будут работать в облаке"}],
        "tasks_summary_raw": [],
        "brief_url": "",
        "pain_text": "",
        "business_tasks_text": "",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg()).to_dict()
    outputs = build_operator_outputs(deal=row, analysis=analysis)
    coaching = str(outputs.get("employee_coaching", "")).lower()
    fix_tasks = " ".join(str(x) for x in outputs.get("employee_fix_tasks", [])).lower()
    assert "презентац" not in coaching
    assert "бриф" not in coaching
    assert "презентац" not in fix_tasks
    assert "follow-up" not in fix_tasks
    assert "бриф" not in fix_tasks
    assert any(token in fix_tasks for token in ("anti-fit", "market mismatch", "класси", "нецелев"))


def test_employee_outputs_for_closed_lost_without_qualified_loss_not_demo_path():
    row = {
        "deal_id": 20,
        "amo_lead_id": 20,
        "deal_name": "Closed lost no context employee policy",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "brief_url": "",
        "pain_text": "",
        "business_tasks_text": "",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg()).to_dict()
    outputs = build_operator_outputs(deal=row, analysis=analysis)
    coaching = str(outputs.get("employee_coaching", "")).lower()
    fix_tasks = " ".join(str(x) for x in outputs.get("employee_fix_tasks", [])).lower()
    assert "презентац" not in coaching
    assert "бриф" not in coaching
    assert "презентац" not in fix_tasks
    assert "follow-up" not in fix_tasks
    assert "восстанов" in coaching or "причин" in fix_tasks or "класси" in fix_tasks


def test_employee_outputs_for_active_process_hygiene_keep_followup_guidance():
    row = {
        "deal_id": 21,
        "amo_lead_id": 21,
        "deal_name": "Active process hygiene employee policy",
        "status_name": "В работе",
        "notes_summary_raw": [{"text": "Контекст есть, клиент заинтересован"}],
        "tasks_summary_raw": [],
        "brief_url": "",
        "pain_text": "Есть боль",
        "business_tasks_text": "Есть бизнес задача",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg()).to_dict()
    outputs = build_operator_outputs(deal=row, analysis=analysis)
    fix_tasks = " ".join(str(x) for x in outputs.get("employee_fix_tasks", [])).lower()
    assert "follow-up" in fix_tasks or "бриф" in fix_tasks


def test_closed_lost_with_context_non_qualified_uses_closeout_policy_without_active_leakage():
    row = {
        "deal_id": 32093998,
        "amo_lead_id": 32093998,
        "deal_name": "Closed lost with minimal context",
        "status_name": "Закрыто и не реализовано",
        "notes_summary_raw": [{"text": "Клиент попросил поставить паузу и закрыть кейс, детали причины не зафиксированы."}],
        "tasks_summary_raw": [],
        "brief_url": "",
        "pain_text": "",
        "business_tasks_text": "",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg()).to_dict()
    assert not any(str(x).startswith("qualified_loss:") for x in analysis.get("risk_flags", []))
    outputs = build_operator_outputs(deal=row, analysis=analysis)
    coaching = str(outputs.get("employee_coaching", "")).lower()
    fix_tasks = " ".join(str(x) for x in outputs.get("employee_fix_tasks", [])).lower()
    forbidden = ("боль клиента", "бизнес-задач", "презентац", "бриф", "follow-up", "вероятност")
    assert all(token not in coaching for token in forbidden)
    assert all(token not in fix_tasks for token in forbidden)
    assert any(token in coaching for token in ("потер", "класси", "anti-pattern"))
    assert any(token in fix_tasks for token in ("потер", "closeout", "crm-cleanup", "класси"))


def test_low_confidence_owner_ambiguity_is_marked_and_manager_actions_get_caution():
    row = {
        "deal_id": 31,
        "amo_lead_id": 31,
        "deal_name": "Owner ambiguity sample",
        "status_name": "В работе",
        "responsible_user_name": "Менеджер А",
        "enriched_conducted_by": "Менеджер Б",
        "enriched_appointment_date": "2026-04-10",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "company_name": "ООО Тест",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg())
    assert analysis.owner_ambiguity_flag is True
    assert analysis.analysis_confidence == "low"
    assert any("owner_ambiguity" in str(x) for x in (analysis.data_quality_flags or []))
    actions = " ".join(analysis.recommended_actions_for_manager).lower()
    assert "ограничен качеством crm" in actions or "owner ambiguity" in actions


def test_low_confidence_closed_lost_outputs_are_not_accusatory_or_active_pipeline_default():
    row = {
        "deal_id": 32,
        "amo_lead_id": 32,
        "deal_name": "Low confidence closeout",
        "status_name": "Закрыто и не реализовано",
        "responsible_user_name": "Менеджер А",
        "enriched_assigned_by": "Руководитель",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "company_name": "ООО Контекст",
        "enriched_appointment_date": "2026-04-11",
        "created_at": 10,
        "updated_at": 20,
    }
    analysis = analyze_deal(row, _cfg()).to_dict()
    outputs = build_operator_outputs(deal=row, analysis=analysis)
    text = " ".join(
        [str(outputs.get("employee_coaching", ""))]
        + [str(x) for x in outputs.get("employee_fix_tasks", [])]
    ).lower()
    for bad in ("презентац", "follow-up", "вероятност", "боль клиента", "бизнес-задач"):
        assert bad not in text
    assert any(token in text for token in ("причин", "closeout", "owner", "атриб"))
