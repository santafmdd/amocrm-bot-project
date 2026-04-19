from src.deal_analyzer.transcript_signals import build_call_signal_aggregates, derive_transcript_signals


def test_derive_transcript_signals_detects_basic_keywords():
    deal = {"deal_id": 1001}
    snapshot = {
        "transcripts": [
            {
                "transcript_status": "ok",
                "transcript_text": (
                    "Обсудили INFO продукт, demo и тест. "
                    "Клиент сказал дорого, но договорились на следующий шаг и КП."
                ),
            }
        ]
    }
    signals = derive_transcript_signals(deal=deal, snapshot=snapshot)
    assert signals["transcript_available"] is True
    assert signals["call_signal_product_info"] is True
    assert signals["call_signal_demo_discussed"] is True
    assert signals["call_signal_test_discussed"] is True
    assert signals["call_signal_budget_discussed"] is True
    assert signals["call_signal_objection_price"] is True
    assert signals["call_signal_next_step_present"] is True
    assert signals["call_signal_summary_short"]


def test_derive_transcript_signals_returns_safe_empty_when_no_text():
    signals = derive_transcript_signals(deal={"deal_id": 7}, snapshot={"transcripts": []})
    assert signals["transcript_available"] is False
    assert signals["transcript_text_excerpt"] == ""
    assert signals["call_signal_summary_short"] == ""
    assert signals["call_signal_objection_not_target"] is False


def test_call_signal_aggregates_count_expected_patterns():
    records = [
        {
            "transcript_available": True,
            "call_signal_next_step_present": True,
            "risk_flags": ["process_hygiene: Нет follow-up задач"],
            "call_signal_product_info": True,
            "call_signal_product_link": True,
            "product_hypothesis": "mixed",
            "call_signal_objection_price": True,
            "call_signal_objection_no_need": False,
            "call_signal_objection_not_target": False,
        },
        {
            "transcript_available": True,
            "call_signal_next_step_present": False,
            "risk_flags": [],
            "call_signal_product_info": False,
            "call_signal_product_link": False,
            "product_hypothesis": "unknown",
            "call_signal_objection_price": False,
            "call_signal_objection_no_need": True,
            "call_signal_objection_not_target": False,
        },
    ]
    agg = build_call_signal_aggregates(records)
    assert agg["deals_with_transcript"] == 2
    assert agg["deals_with_next_step_in_call"] == 1
    assert agg["deals_next_step_in_call_but_missing_followup_in_crm"] == 1
    assert agg["deals_with_probable_wrong_or_mixed_product_by_call"] == 1
    assert agg["deals_with_early_objection_pattern"] == 2

