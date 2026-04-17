from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from src.amocrm_collector.config import (
    build_collector_config_summary,
    collect_collector_config_warnings,
    load_collector_config,
)


def test_load_collector_config_defaults_and_bom_support():
    payload = {
        "base_domain": "test-account.amocrm.ru",
        "manager_ids_include": [1, "2", "x"],
        "manager_ids_exclude": [3, "4"],
        "manager_names_exclude": ["Антон Коломоец", "Гордиенко Кирилл"],
        "pipeline_ids_include": [11, 22],
        "product_field_id": "101",
        "company_comment_field_id": "201",
        "contact_comment_field_id": 202,
        "presentation_link_search": {
            "scan_deal_custom_fields_url": True,
            "regexes": ["docs.google.com"],
        },
        "output_dir": "workspace/amocrm_collector",
    }
    text_with_bom = "\ufeff" + json.dumps(payload, ensure_ascii=False)

    def _fake_read_text(self, encoding=None):
        return text_with_bom.lstrip("\ufeff") if encoding == "utf-8-sig" else text_with_bom

    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", _fake_read_text):
        cfg = load_collector_config("config/amocrm_collector.local.json")

    assert cfg.base_domain == "test-account.amocrm.ru"
    assert cfg.manager_ids_include == [1, 2]
    assert cfg.manager_ids_exclude == [3, 4]
    assert cfg.manager_names_exclude == ["Антон Коломоец", "Гордиенко Кирилл"]
    assert cfg.pipeline_ids_include == [11, 22]
    assert cfg.product_field_id == 101
    assert cfg.company_comment_field_id == 201
    assert cfg.contact_comment_field_id == 202
    assert cfg.presentation_link_search.scan_deal_custom_fields_url is True
    assert cfg.presentation_link_search.regexes == ["docs.google.com"]
    assert cfg.output_dir.name == "amocrm_collector"


def test_load_collector_config_supports_legacy_scan_lead_key():
    payload = {
        "presentation_link_search": {
            "scan_lead_custom_fields_url": False,
            "regexes": ["docs.google.com"],
        }
    }

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.read_text", return_value=json.dumps(payload, ensure_ascii=False)
    ):
        cfg = load_collector_config("config/amocrm_collector.local.json")

    assert cfg.presentation_link_search.scan_deal_custom_fields_url is False


def test_invalid_manager_ids_exclude_raises_config_error():
    payload = {
        "manager_ids_exclude": ["Антон Коломоец", 42],
        "manager_names_exclude": ["Гордиенко Кирилл"],
    }

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.read_text", return_value=json.dumps(payload, ensure_ascii=False)
    ):
        with pytest.raises(RuntimeError, match="manager_ids_exclude"):
            load_collector_config("config/amocrm_collector.local.json")


def test_valid_manager_exclusions_loaded_separately():
    payload = {
        "manager_ids_exclude": [101, "202"],
        "manager_names_exclude": ["Антон Коломоец", "Гордиенко Кирилл"],
    }

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.read_text", return_value=json.dumps(payload, ensure_ascii=False)
    ):
        cfg = load_collector_config("config/amocrm_collector.local.json")

    assert cfg.manager_ids_exclude == [101, 202]
    assert cfg.manager_names_exclude == ["Антон Коломоец", "Гордиенко Кирилл"]


def test_zero_field_ids_emit_warnings_and_summary_marks_zero():
    payload = {
        "product_field_id": 0,
        "source_field_id": 0,
        "pain_field_id": 103,
    }

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.read_text", return_value=json.dumps(payload, ensure_ascii=False)
    ):
        cfg = load_collector_config("config/amocrm_collector.local.json")

    warnings = collect_collector_config_warnings(cfg)
    summary = build_collector_config_summary(cfg)

    assert any("product_field_id=0" in msg for msg in warnings)
    assert any("source_field_id=0" in msg for msg in warnings)
    assert "product_field_id" in summary["field_id_state"]["zero"]
    assert "source_field_id" in summary["field_id_state"]["zero"]
