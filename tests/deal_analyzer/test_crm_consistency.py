from src.deal_analyzer.crm_consistency import build_crm_consistency_layer


def test_crm_consistency_detects_next_step_mismatch() -> None:
    crm = {
        "status_name": "В работе",
        "pipeline_name": "Продажи",
        "responsible_user_name": "Илья",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
        "product_values": [],
        "contact_phone": "",
        "contact_email": "",
        "company_name": "",
        "company_inn": "",
    }
    analysis = {
        "transcript_available": True,
        "call_signal_summary_short": "Есть следующий шаг, клиент ждёт коммерческое",
        "call_signal_next_step_present": True,
        "call_signal_product_info": True,
    }

    out = build_crm_consistency_layer(crm=crm, analysis=analysis)
    assert "next_step_in_call_missing_in_crm_tasks" in out["crm_vs_call_mismatch"]
    assert "product_signal_in_call_missing_in_crm_product" in out["crm_vs_call_mismatch"]
    assert "crm_context_missing_notes_tasks" in out["crm_hygiene_flags"]
    assert out["crm_consistency_summary"]


def test_crm_consistency_clean_when_context_is_present() -> None:
    crm = {
        "status_name": "В работе",
        "pipeline_name": "Продажи",
        "responsible_user_name": "Рустам",
        "notes_summary_raw": [{"text": "Обсудили следующий шаг"}],
        "tasks_summary_raw": [{"text": "Отправить КП"}],
        "product_values": ["LINK"],
        "contact_phone": "+7 999 000-00-11",
        "contact_email": "a@b.ru",
        "company_name": "ООО Тест",
        "company_inn": "7700000000",
        "tags": ["etp"],
    }
    analysis = {
        "transcript_available": True,
        "call_signal_summary_short": "Разговор предметный",
        "call_signal_next_step_present": True,
        "call_signal_product_link": True,
    }

    out = build_crm_consistency_layer(crm=crm, analysis=analysis)
    assert out["crm_vs_call_mismatch"] == []
    assert "crm_context_missing_notes_tasks" not in out["crm_hygiene_flags"]
    debug = out.get("crm_consistency_debug", {})
    assert debug.get("consistency_score_0_100", 0) >= 70

