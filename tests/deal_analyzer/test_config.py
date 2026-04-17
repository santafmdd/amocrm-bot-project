from pathlib import Path
from unittest.mock import patch
from datetime import date

import pytest

from src.deal_analyzer.config import load_deal_analyzer_config, resolve_period


def _cfg_payload(extra: str = "") -> str:
    base = """
{
  "output_dir": "workspace/deal_analyzer",
  "analyzer_backend": "ollama",
  "ollama_base_url": "http://127.0.0.1:11434",
  "ollama_model": "gemma4:e4b",
  "ollama_timeout_seconds": 45,
  "style_profile_name": "manager_ru_v2",
  "period_mode": "smart_manager_default",
  "period_label_mode": "period_only",
  "hide_executed_at_from_public_exports": true,
  "executed_at_visibility": "internal_only",
  "client_list_enrich_enabled": false,
  "appointment_list_enrich_enabled": false,
  "client_list_source_name": "",
  "appointment_list_source_name": ""
}
""".strip()
    return base if not extra else extra


def test_load_deal_analyzer_config_with_ollama_backend_and_period_fields():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")

    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", return_value=_cfg_payload()):
        cfg = load_deal_analyzer_config(str(cfg_path))

    assert cfg.analyzer_backend == "ollama"
    assert cfg.ollama_model == "gemma4:e4b"
    assert cfg.ollama_timeout_seconds == 45
    assert cfg.style_profile_name == "manager_ru_v2"
    assert cfg.period_mode == "smart_manager_default"
    assert cfg.hide_executed_at_from_public_exports is True


def test_load_deal_analyzer_config_rejects_unknown_backend():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.read_text", return_value='{"analyzer_backend":"unknown"}'
    ):
        with pytest.raises(RuntimeError, match="Unsupported analyzer_backend"):
            load_deal_analyzer_config(str(cfg_path))


def test_resolve_period_smart_mode_weekend_goes_current_week_to_date():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")
    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", return_value=_cfg_payload()):
        cfg = load_deal_analyzer_config(str(cfg_path))

    resolved = resolve_period(config=cfg, today=date(2026, 4, 18))  # Saturday
    assert resolved.resolved_mode == "current_week_to_date"
    assert resolved.period_start.isoformat() == "2026-04-13"
    assert resolved.period_end.isoformat() == "2026-04-18"


def test_resolve_period_smart_mode_weekday_goes_previous_workweek():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")
    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", return_value=_cfg_payload()):
        cfg = load_deal_analyzer_config(str(cfg_path))

    resolved = resolve_period(config=cfg, today=date(2026, 4, 17))  # Friday
    assert resolved.resolved_mode == "previous_workweek"
    assert resolved.period_start.isoformat() == "2026-04-06"
    assert resolved.period_end.isoformat() == "2026-04-10"


def test_resolve_period_previous_calendar_week():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")
    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", return_value=_cfg_payload()):
        cfg = load_deal_analyzer_config(str(cfg_path))

    resolved = resolve_period(config=cfg, requested_mode="previous_calendar_week", today=date(2026, 4, 17))
    assert resolved.period_start.isoformat() == "2026-04-06"
    assert resolved.period_end.isoformat() == "2026-04-12"


def test_resolve_period_custom_range_from_cli_override():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")
    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", return_value=_cfg_payload()):
        cfg = load_deal_analyzer_config(str(cfg_path))

    resolved = resolve_period(
        config=cfg,
        requested_mode="custom_range",
        cli_date_from="2026-04-01",
        cli_date_to="2026-04-07",
        today=date(2026, 4, 17),
    )
    assert resolved.period_start.isoformat() == "2026-04-01"
    assert resolved.period_end.isoformat() == "2026-04-07"

def test_resolve_period_custom_range_requires_dates():
    cfg_path = Path("d:/AI_Automation/amocrm_bot/project/config/deal_analyzer.local.json")
    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text", return_value=_cfg_payload()):
        cfg = load_deal_analyzer_config(str(cfg_path))

    with pytest.raises(RuntimeError, match="requires both date_from and date_to"):
        resolve_period(config=cfg, requested_mode="custom_range", today=date(2026, 4, 17))
