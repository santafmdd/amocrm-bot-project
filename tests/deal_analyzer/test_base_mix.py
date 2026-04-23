from src.deal_analyzer.base_mix import build_base_mix_text, resolve_base_mix
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


def test_base_mix_resolution_debug_selected_from_deal():
    resolution = resolve_base_mix(
        [
            {
                "deal_tags_raw": ["Приоритетный"],
                "company_tags": ["Компания"],
                "source_values": ["site"],
            }
        ]
    )
    assert resolution["selected_source"] == "deal_tags"
    assert resolution["selected_value"] == "Приоритетный"
    assert resolution["fallback_used"] is False
    assert "Приоритетный" in resolution["raw_tags_deal"]


def test_base_mix_resolution_debug_selected_from_company():
    resolution = resolve_base_mix(
        [
            {
                "deal_tags_raw": [],
                "tags": [],
                "company_tags": ["КомпанияТег"],
            }
        ]
    )
    assert resolution["selected_source"] == "company_tags"
    assert resolution["selected_value"] == "КомпанияТег"
    assert resolution["fallback_used"] is False
    assert "КомпанияТег" in resolution["raw_tags_company"]


def test_base_mix_resolution_debug_fallback():
    resolution = resolve_base_mix([{"deal_tags_raw": [], "tags": [], "company_tags": []}])
    assert resolution["selected_source"] == "fallback"
    assert resolution["selected_value"] == "солянка"
    assert resolution["fallback_used"] is True


def test_base_mix_tag_entries_keep_raw_and_normalized_by_mapping():
    resolution = resolve_base_mix(
        [
            {
                "deal_tags_raw": ["ИНФО", "  Исток линк  "],
                "company_tags": [],
            }
        ]
    )
    entries = resolution.get("deal_tag_entries")
    assert isinstance(entries, list)
    assert {"raw_tag": "ИНФО", "normalized_tag": "инфо", "source_of_tag": "deal"} in entries
    assert {"raw_tag": "Исток линк", "normalized_tag": "istock.link", "source_of_tag": "deal"} in entries


def test_base_mix_no_mapping_keeps_raw_value():
    resolution = resolve_base_mix(
        [
            {
                "deal_tags_raw": ["Кастомный Тег Без Маппинга"],
                "company_tags": [],
            }
        ]
    )
    assert resolution["selected_source"] == "deal_tags"
    assert resolution["selected_value"] == "Кастомный Тег Без Маппинга"
    entries = resolution.get("deal_tag_entries")
    assert isinstance(entries, list)
    assert {
        "raw_tag": "Кастомный Тег Без Маппинга",
        "normalized_tag": "Кастомный Тег Без Маппинга",
        "source_of_tag": "deal",
    } in entries
