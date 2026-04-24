from src.deal_analyzer.call_review_sheet import build_call_review_payload, rows_to_sheet_matrix


def _deal_record() -> dict:
    return {
        "deal_id": "32165731",
        "deal_name": "Тестовая сделка",
        "company_name": "ООО Тест",
        "owner_name": "Рустам Хомидов",
        "updated_at": "2026-04-02T12:00:00+00:00",
        "call_signal_summary_short": "Секретарь дал маршрут на закупки через почту.",
        "transcript_text_excerpt": "Соединили через секретаря, договорились про следующий контакт.",
        "transcript_usability_score_final": 2,
        "transcript_usability_label": "usable",
        "call_signal_next_step_present": True,
        "call_signal_decision_maker_reached": False,
        "call_candidates_count": 2,
        "dial_over_limit_numbers_count": 0,
        "repeated_dead_redial_day_flag": False,
        "same_time_redial_pattern_flag": False,
        "numbers_not_fully_covered_flag": False,
        "crm_vs_call_mismatch": [],
        "score": 57,
        "deal_tags_raw": ["ВПРОК 26", "tilda"],
        "tags": ["ВПРОК 26", "tilda"],
        "company_tags": [],
        "call_anchor_date": "2026-04-02",
        "call_review_llm_ready": True,
        "call_review_llm_source": "main",
        "call_review_llm_fields": {
            "key_takeaway": "Есть рабочий разговор по секретарю, маршрут понятен.",
            "strong_sides": "Четко обозначил цель и удержал маршрут.",
            "growth_zones": "Добавить фиксацию срока обратного контакта.",
            "why_important": "Так меньше пустых звонков и быстрее выход на нужную роль.",
            "reinforce": "Модуль захода через инфоповод.",
            "fix_action": "В конце каждого такого звонка фиксировать точный срок возврата.",
            "coaching_list": "1) Разобрали заход.\n2) Дали модуль маршрута.\n3) На следующем звонке фиксирует срок.",
            "expected_quantity": "+0.2-0.4 выхода на нужную роль в неделю.",
            "expected_quality": "Маршрут к нужной роли станет стабильнее.",
            "stage_secretary_comment": "Секретарь дал рабочий маршрут на закупки.",
            "stage_lpr_comment": "",
            "stage_need_comment": "",
            "stage_presentation_comment": "",
            "stage_closing_comment": "Следующий шаг зафиксирован.",
            "stage_objections_comment": "",
            "stage_speech_comment": "Речь собранная, без лишнего шума.",
            "stage_crm_comment": "CRM фиксирует факт разговора и следующий шаг.",
            "stage_discipline_comment": "",
            "stage_demo_comment": "",
            "evidence_quote": "Договорились о следующем контакте через почту.",
        },
    }


def test_call_review_payload_has_one_case_one_row_and_human_labels() -> None:
    payload = build_call_review_payload(
        summary={"run_timestamp": "2026-04-23T10:00:00+00:00"},
        period_deal_records=[_deal_record()],
        analysis_shortlist_payload={
            "selected_items": [
                {
                    "deal_id": "32165731",
                    "call_case_type": "secretary_case",
                    "pool_type": "conversation_pool",
                    "selected_for_transcription": True,
                    "selected_call_count": 1,
                    "transcript_usability_score_final": 2,
                    "transcript_usability_label": "usable",
                    "shortlist_reason": "priority_1_meaningful_conversation:secretary_case",
                }
            ]
        },
        base_domain="https://officeistockinfo.amocrm.ru",
        manager_allowlist=["Илья", "Рустам"],
        manager_role_registry={"Рустам": "telemarketer", "Илья": "sales_manager"},
    )
    assert payload["rows_count"] == 1
    row = payload["rows"][0]
    assert row["Deal ID"] == "32165731"
    assert row["Ссылка на сделку"].startswith("https://officeistockinfo.amocrm.ru/leads/detail/32165731")
    assert row["Сделка"] == "Тестовая сделка"
    assert row["Компания"] == "ООО Тест"
    assert row["База / тег"] == "ВПРОК 26; tilda"
    assert row["Тип кейса"] == "секретарь"
    assert row["Роль"] == "телемаркетолог"
    assert "warm_case" not in row["Тип кейса"]
    assert "бриф" not in row["Зоны роста"].lower()
    assert "демо" not in row["Зоны роста"].lower()


def test_call_review_payload_skips_weak_noise_case() -> None:
    weak_deal = _deal_record()
    weak_deal["deal_id"] = "32160389"
    weak_deal["owner_name"] = "Илья Бочков"
    weak_deal["call_signal_summary_short"] = ""
    weak_deal["transcript_text_excerpt"] = ""
    weak_deal["transcript_usability_score_final"] = 0
    weak_deal["transcript_usability_label"] = "empty"

    payload = build_call_review_payload(
        summary={"run_timestamp": "2026-04-23T10:00:00+00:00"},
        period_deal_records=[weak_deal],
        analysis_shortlist_payload={
            "selected_items": [
                {
                    "deal_id": "32160389",
                    "call_case_type": "autoanswer_noise",
                    "pool_type": "conversation_pool",
                    "selected_for_transcription": True,
                    "transcript_usability_score_final": 0,
                    "transcript_usability_label": "empty",
                }
            ]
        },
        base_domain="https://officeistockinfo.amocrm.ru",
        manager_allowlist=["Илья", "Рустам"],
    )
    assert payload["rows_count"] == 0
    assert payload["selection_debug"]["rows_skipped"] >= 1


def test_rows_to_sheet_matrix_respects_duplicate_headers() -> None:
    payload = build_call_review_payload(
        summary={"run_timestamp": "2026-04-23T10:00:00+00:00"},
        period_deal_records=[_deal_record()],
        analysis_shortlist_payload={
            "selected_items": [
                {
                    "deal_id": "32165731",
                    "call_case_type": "secretary_case",
                    "pool_type": "conversation_pool",
                    "selected_for_transcription": True,
                    "selected_call_count": 1,
                    "transcript_usability_score_final": 2,
                    "transcript_usability_label": "usable",
                }
            ]
        },
        base_domain="https://officeistockinfo.amocrm.ru",
        manager_allowlist=["Рустам"],
    )
    row = payload["rows"][0]
    columns = [
        "Здоровается",
        "Здоровается",
        "Комментарий по этапу",
        "Комментарий по этапу",
        "Тип кейса",
    ]
    matrix = rows_to_sheet_matrix(rows=[row], columns=columns)
    assert len(matrix) == 1
    assert matrix[0][0] != ""
    assert matrix[0][1] != ""
    assert matrix[0][2] != matrix[0][3]
    assert matrix[0][4] == "секретарь"


def test_call_review_payload_skips_discipline_case_for_battle_write() -> None:
    rec = _deal_record()
    rec["deal_id"] = "777"
    rec["call_review_llm_ready"] = True
    payload = build_call_review_payload(
        summary={"run_timestamp": "2026-04-23T10:00:00+00:00"},
        period_deal_records=[rec],
        analysis_shortlist_payload={
            "selected_items": [
                {
                    "deal_id": "777",
                    "call_case_type": "redial_discipline",
                    "pool_type": "discipline_pool",
                    "selected_for_transcription": False,
                    "transcript_usability_score_final": 0,
                    "transcript_usability_label": "empty",
                }
            ]
        },
        base_domain="https://officeistockinfo.amocrm.ru",
        manager_allowlist=["Рустам"],
    )
    assert payload["rows_count"] == 0
    assert payload["selection_debug"]["rows_skipped"] == 1
