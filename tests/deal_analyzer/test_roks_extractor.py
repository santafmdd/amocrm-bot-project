from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.roks_extractor import extract_roks_snapshot


class _Logger:
    def __init__(self):
        self.infos = []
        self.warnings = []

    def info(self, msg, *args):
        self.infos.append(msg % args if args else str(msg))

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else str(msg))


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
        roks_source_url="https://docs.google.com/spreadsheets/d/abc123/edit",
        roks_sheet_name="РОКС 2026",
    )


def test_roks_sanitization_removes_formula_errors():
    logger = _Logger()
    cfg = _cfg()

    with patch("src.deal_analyzer.roks_extractor.extract_spreadsheet_id", return_value="abc123"), patch(
        "src.deal_analyzer.roks_extractor.GoogleSheetsApiClient.list_sheets",
        return_value=[{"title": "РОКС 2026"}],
    ), patch(
        "src.deal_analyzer.roks_extractor.GoogleSheetsApiClient.get_values",
        return_value=[
            ["Метрика", "Значение"],
            ["Конверсия", "#DIV/0!"],
            ["Прогноз", "15"],
        ],
    ):
        result = extract_roks_snapshot(config=cfg, logger=logger, manager="Илья")

    payload = result.to_dict()
    assert payload["ok"] is True
    items = payload["conversion_snapshot"]["items"]
    assert items and items[0]["value"] == ""


def test_roks_missing_source_is_honest_fallback():
    logger = _Logger()
    cfg = DealAnalyzerConfig(**{**_cfg().__dict__, "roks_source_url": ""})

    result = extract_roks_snapshot(config=cfg, logger=logger, manager="Илья")
    payload = result.to_dict()
    assert payload["ok"] is False
    assert payload["warnings"]
