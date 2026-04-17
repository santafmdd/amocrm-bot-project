import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.parsers.weekly_refusals_parser import normalize_status_text, parse_weekly_refusals_rows


def test_normalize_status_text_trims_and_unifies_yo_spaces() -> None:
    assert normalize_status_text("  Закрыто  и  не   реализовано  ") == "закрыто и не реализовано"
    assert normalize_status_text("Ёжик") == "ежик"


def test_parse_weekly_refusals_rows_aggregates_before_after_and_deals() -> None:
    rows = [
        {
            "status_before": "Первичный контакт",
            "status_after": "Закрыто и не реализовано (дорого)",
            "status_after_loss_reason": "(Верификация) Не дозвониться",
            "deal_id": "101",
            "deal_url": "https://x/amocrm/leads/detail/101",
        },
        {
            "status_before": "  первичный   контакт  ",
            "status_after": "Закрыто и не реализовано (дорого)",
            "status_after_loss_reason": "(Верификация) Не дозвониться",
            "deal_id": "102",
            "deal_url": "https://x/amocrm/leads/detail/102",
        },
        {
            "status_before": "Квалификация",
            "status_after": "Закрыто и не реализовано (не бюджет)",
            "status_after_loss_reason": "(Квалификация) Нецелевой",
            "deal_id": "101",
            "deal_url": "https://x/amocrm/leads/detail/101",
        },
    ]

    parsed = parse_weekly_refusals_rows(
        report_id="weekly_refusals_weekly_2m",
        display_name="Weekly refusals",
        rows=rows,
    )

    before = {item["status"]: int(item["count"]) for item in parsed.aggregated_before_status_counts}
    after = {item["status"]: int(item["count"]) for item in parsed.aggregated_after_status_counts}

    assert before["первичный контакт"] == 2
    assert before["квалификация"] == 1
    assert after["(верификация) не дозвониться"] == 2
    assert after["(квалификация) нецелевой"] == 1
    assert len(parsed.deal_refs) == 2


def test_parse_weekly_refusals_rows_after_falls_back_to_status_after_when_loss_reason_missing() -> None:
    rows = [
        {
            "status_before": "Квалификация",
            "status_after": "Закрыто и не реализовано",
            "status_after_loss_reason": "",
            "deal_id": "1",
            "deal_url": "u1",
        },
        {
            "status_before": "Квалификация",
            "status_after": "Закрыто и не реализовано",
            "status_after_loss_reason": None,
            "deal_id": "2",
            "deal_url": "u2",
        },
    ]
    parsed = parse_weekly_refusals_rows(
        report_id="weekly_refusals_weekly_2m",
        display_name="Weekly refusals",
        rows=rows,
    )
    after = {item["status"]: int(item["count"]) for item in parsed.aggregated_after_status_counts}
    assert after["закрыто и не реализовано"] == 2


def test_parse_weekly_refusals_rows_merges_near_duplicate_loss_reasons() -> None:
    rows = [
        {
            "status_before": "Верификация",
            "status_after": "Закрыто и не реализовано",
            "status_after_loss_reason": "(Верификация) Перестал выходить на свя",
            "deal_id": "11",
            "deal_url": "u11",
        },
        {
            "status_before": "Верификация",
            "status_after": "Закрыто и не реализовано",
            "status_after_loss_reason": "(Верификация) Перестал выходить на связь",
            "deal_id": "12",
            "deal_url": "u12",
        },
    ]
    parsed = parse_weekly_refusals_rows(
        report_id="weekly_refusals_weekly_2m",
        display_name="Weekly refusals",
        rows=rows,
    )
    after = {item["status"]: int(item["count"]) for item in parsed.aggregated_after_status_counts}
    assert after == {"(верификация) перестал выходить на связь": 2}
