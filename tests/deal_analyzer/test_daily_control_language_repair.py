from __future__ import annotations

from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.daily_control.validation.language_repair import repair_language_rows
from src.deal_analyzer.daily_control.validation.text_lint import lint_daily_text_rows, lint_has_blockers


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:31b-cloud",
        ollama_timeout_seconds=60,
        ollama_fallback_enabled=True,
        ollama_fallback_base_url="http://127.0.0.1:11434",
        ollama_fallback_model="deepseek-v3.1:671b-cloud",
        ollama_fallback_timeout_seconds=60,
    )


def _row(text: str) -> dict:
    return {
        "week_start": "2026-04-20",
        "week_end": "2026-04-26",
        "period_start": "2026-03-30",
        "period_end": "2026-04-24",
        "control_day_date": "2026-04-22",
        "day_label": "среда",
        "manager_name": "Илья Бочков",
        "manager_role_profile": "менеджер по продажам",
        "sample_size": 1,
        "deals_count": 1,
        "calls_count": 1,
        "deal_ids": "32000168",
        "deal_links": "https://example/1",
        "product_mix": "LINK - 1",
        "base_mix": "INFO - 1",
        "main_pattern": text,
        "strong_sides": "Держит контакт",
        "growth_zones": "Нужно фиксировать следующий шаг",
        "why_it_matters": "Без фиксации шага теряется темп",
        "what_to_reinforce": "Структуру разговора",
        "what_to_fix": "Фиксировать дату следующего шага",
        "what_to_tell_employee": "Разобрать 2 кейса на ежедневке",
        "expected_quant_impact": "+1 шаг в неделю",
        "expected_qual_impact": "Выше управляемость follow-up",
        "score_0_100": 50,
        "criticality": "средняя",
    }


def test_allowlisted_terms_are_not_blockers() -> None:
    lint = lint_daily_text_rows([_row("Работаем с LINK и INFO, фиксируем в CRM и amoCRM")])
    assert lint_has_blockers(lint) is False
    assert int(lint.get("allowed_latin_terms_count", 0)) >= 3


def test_technical_terms_are_warnings_not_blockers() -> None:
    lint = lint_daily_text_rows([_row("Проверить API и JSON по интеграции")])
    assert lint_has_blockers(lint) is False
    assert int(lint.get("technical_terms_warning_count", 0)) >= 2


def test_language_repair_quarantines_unrepaired_row_without_llm() -> None:
    repaired = repair_language_rows(
        rows=[_row("你好, clarifying decision-makers and gathering contacts")],
        cfg=_cfg(),
        llm_runtime={},
        logger=None,
        max_attempts=1,
        enable_llm_repair=False,
    )
    assert len(repaired["rows"]) == 0
    assert len(repaired["quarantined_rows"]) == 1


def test_language_repair_uses_llm_and_unblocks_row(monkeypatch) -> None:
    def _fake_repair_with_model(*, model, base_url, timeout_seconds, fields_payload):
        _ = model, base_url, timeout_seconds, fields_payload
        return ({"main_pattern": "Коротко уточнить роль ЛПР и зафиксировать следующий шаг"}, {"ok": True, "error": "", "model": "x", "elapsed_ms": 10, "repair_applied": False})

    monkeypatch.setattr("src.deal_analyzer.daily_control.validation.language_repair._repair_with_model", _fake_repair_with_model)

    repaired = repair_language_rows(
        rows=[_row("hello, clarifying decision-makers and gathering contacts")],
        cfg=_cfg(),
        llm_runtime={},
        logger=None,
        max_attempts=2,
        enable_llm_repair=True,
    )
    assert len(repaired["rows"]) == 1
    assert len(repaired["quarantined_rows"]) == 0
    assert "hello" not in repaired["rows"][0]["main_pattern"].lower()


def test_language_repair_removes_markdown_fence_deterministically() -> None:
    repaired = repair_language_rows(
        rows=[_row("```json привет```")],
        cfg=_cfg(),
        llm_runtime={},
        logger=None,
        max_attempts=1,
        enable_llm_repair=False,
    )
    # fence cleanup should make row writable without blocker
    assert len(repaired["rows"]) == 1
    assert "```" not in repaired["rows"][0]["main_pattern"]
