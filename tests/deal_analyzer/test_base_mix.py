from src.deal_analyzer.base_mix import build_base_mix_text
from src.deal_analyzer.cli import _merge_deal_company_tags


def test_base_mix_priority_deal_tags_then_company_tags():
    records = [
        {
            "deal_tags_raw": ["металл"],
            "company_tags": ["тендерные"],
            "source_values": ["https://istock.link/"],
            "notes_summary_raw": [{"text": "ОКВЭД 28.1"}],
        }
    ]
    mix = build_base_mix_text(records)
    assert mix.startswith("металл")
    assert "тендерные" not in mix


def test_base_mix_uses_company_tags_when_deal_tags_empty():
    records = [
        {
            "deal_tags_raw": [],
            "tags": [],
            "company_tags": ["машиностроение"],
            "source_values": [],
            "notes_summary_raw": [],
        }
    ]
    mix = build_base_mix_text(records)
    assert mix == "машиностроение"


def test_base_mix_uses_company_tags_on_level_2_not_merged_tags_level_1():
    records = [
        {
            "deal_tags_raw": [],
            "tags": ["company-only-tag"],
            "company_tags": ["company-only-tag"],
            "source_values": [],
        }
    ]
    mix = build_base_mix_text(records)
    assert mix == "company-only-tag"


def test_base_mix_uses_source_form_url_hint_for_32160389_case():
    records = [
        {
            "deal_tags_raw": [],
            "tags": [],
            "company_tags": [],
            "source_values": [],
            "deal_name": "Заявка с формы: Запросить демонстрацию страницы https://istock.link/",
            "notes_summary_raw": [],
        }
    ]
    mix = build_base_mix_text(records).lower()
    assert mix != "солянка"
    assert "istock.link" in mix or "линк" in mix or "входящие формы" in mix


def test_base_mix_does_not_use_stage_or_status_as_source():
    records = [
        {
            "deal_tags_raw": [],
            "tags": [],
            "company_tags": [],
            "source_values": [],
            "status_name": "Первичный контакт",
            "pipeline_name": "Привлечение",
        }
    ]
    mix = build_base_mix_text(records).lower()
    assert mix == "солянка"
    assert "привлечение" not in mix
    assert "первичный" not in mix


def test_company_tag_propagation_exposes_missing_tag_for_32165731_case():
    merged, company, propagated = _merge_deal_company_tags(
        deal_tags=[],
        company_tags=["машэкспо", "инфо"],
    )
    assert set(company) == {"машэкспо", "инфо"}
    assert set(propagated) == {"машэкспо", "инфо"}
    assert set(merged) == {"машэкспо", "инфо"}
