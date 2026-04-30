from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.daily_control.daily_analyzer import _runtime_from_config, analyze_daily_packages
from src.deal_analyzer.daily_control.day_grouper import aggregate_mix, group_by_manager_day, week_bounds_monday_sunday
from src.deal_analyzer.daily_control.models import DailyControlInputGroup
from src.deal_analyzer.daily_control.roks_oap_parser import parse_roks_oap_snapshot
from src.deal_analyzer.daily_control.sheets_writer import (
    _resolve_criticality_value_for_write,
    plan_daily_control_write,
    write_daily_control_rows,
)
from src.deal_analyzer.daily_control.validation.writer_preflight import evaluate_writer_preflight


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="deepseek-v3.1:671b-cloud",
        ollama_timeout_seconds=60,
        ollama_fallback_enabled=True,
        ollama_fallback_base_url="http://127.0.0.1:11434",
        ollama_fallback_model="deepseek-v3.1:671b-cloud",
        ollama_fallback_timeout_seconds=60,
    )


def _sample_group() -> DailyControlInputGroup:
    return DailyControlInputGroup(
        period_start="2026-03-30",
        period_end="2026-04-24",
        week_start="2026-04-20",
        week_end="2026-04-26",
        control_day_date="2026-04-22",
        day_label="среда",
        manager_name="Илья Бочков",
        manager_role_profile="менеджер по продажам",
        source_rows=[
            {
                "deal_id": "32000168",
                "deal_name": "Сделка А",
                "deal_link": "https://example/1",
                "case_type": "презентация",
                "listened_calls": "2026-04-22 14:01 - 44:44",
                "key_takeaway": "Долго тянул к фиксации следующего шага",
                "strong": "Хорошо держит контакт",
                "growth": "Нужна четкая фиксация следующего шага",
                "fix": "Фиксировать шаг в конце звонка",
                "tell_employee": "После звонка фиксировать шаг и дату",
            }
        ],
        sample_size=1,
        deals_count=1,
        calls_count=1,
        deal_ids=["32000168"],
        deal_names=["Сделка А"],
        deal_links=["https://example/1"],
        product_mix="линк - 1",
        base_mix="tilda - 1",
        insights={"growth_zones": ["Нужна четкая фиксация следующего шага"]},
        discipline_signals={"discipline_case_rows": 0},
    )


def _valid_llm_payload() -> dict:
    return {
        "date": "2026-04-22",
        "day_label": "среда",
        "manager_name": "Илья Бочков",
        "department": "продажи",
        "base_mix": "tilda - 1",
        "product_mix": "линк - 1",
        "main_pattern": "В разговоре проседает фиксация следующего шага.",
        "strengths": "Хорошо держит диалог и не теряет клиента.",
        "growth_zones": "Нужно раньше закрывать на конкретное время следующего касания.",
        "why_it_matters": "Без фиксации шага теряется переход к следующему этапу.",
        "what_to_fix": "Фиксировать следующий шаг и дату в конце каждого звонка.",
        "what_to_tell_employee": "На ежедневке разобрать два звонка и закрепить правило фиксации шага.",
        "expected_effect_quantity": "Ожидаемо +1-2 подтвержденных шага в неделю при том же объеме звонков.",
        "expected_effect_quality": "Должен вырасти переход из разговора в управляемый follow-up.",
        "score_0_100": 62,
        "criticality": "medium",
        "training_needed": True,
        "training_topic": "фиксация следующего шага",
        "evidence_short": "В звонке шаг обсуждается, но дата не закрепляется.",
        "data_limitations": "",
    }


def _row_for_preflight(**overrides):
    row = {
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
        "product_mix": "линк - 1",
        "base_mix": "tilda - 1",
        "main_pattern": "В разговоре проседает фиксация следующего шага.",
        "strong_sides": "Хорошо держит диалог.",
        "growth_zones": "Нужно фиксировать следующий шаг.",
        "why_it_matters": "Без фиксации шага теряем переход.",
        "what_to_reinforce": "Держать структуру диалога.",
        "what_to_fix": "Фиксировать дату следующего шага.",
        "what_to_tell_employee": "Разобрать 2 звонка на ежедневке.",
        "expected_quant_impact": "Ожидаемо +1-2 шага в неделю.",
        "expected_qual_impact": "Переход к следующему этапу станет стабильнее.",
        "score_0_100": 62,
        "criticality": "средняя",
    }
    row.update(overrides)
    return row


def test_llm_first_analyzer_uses_full_group_context(monkeypatch) -> None:
    captured = {"calls": []}

    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        captured["calls"].append({"model": model, "messages": messages, "timeout": timeout_seconds})
        return _valid_llm_payload(), {"ok": True, "error": "", "elapsed_ms": 123, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=3,
    )

    assert len(rows) == 1
    assert rows[0]["analysis_backend_used"] == "main"
    assert diag["llm_success_main"] == 1
    assert captured["calls"][0]["model"] == "gemma4:31b-cloud"
    req = diag["llm_requests"][0]
    assert req["context"]["deal_ids"] == ["32000168"]
    assert req["context"]["base_mix"] == "tilda - 1"
    assert req["context"]["product_mix"] == "линк - 1"


def test_fallback_selected_when_main_invalid_json(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if "gemma" in model:
            return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 99, "repair_applied": False}
        return _valid_llm_payload(), {"ok": True, "error": "", "elapsed_ms": 88, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=5,
    )

    assert rows[0]["analysis_backend_used"] == "fallback"
    assert diag["llm_success_main"] == 0
    assert diag["llm_success_fallback"] == 1
    runtime_diag = diag.get("llm_runtime", {}) if isinstance(diag.get("llm_runtime"), dict) else {}
    fallback_diag = runtime_diag.get("fallback", {}) if isinstance(runtime_diag.get("fallback"), dict) else {}
    assert fallback_diag.get("model") == "deepseek-v3.1:671b-cloud"
    assert diag["quarantined_count"] == 0


def test_fallback_is_reached_with_default_attempt_window(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if "gemma" in model:
            return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 99, "repair_applied": False}
        return _valid_llm_payload(), {"ok": True, "error": "", "elapsed_ms": 88, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=3,
    )

    assert rows[0]["analysis_backend_used"] == "fallback"
    assert diag["llm_success_fallback"] == 1


def test_main_429_moves_to_fallback_without_main_retry(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        calls.append(str(model))
        if str(model).startswith("gemma4"):
            return None, {"ok": False, "error": "HTTP 429: weekly usage limit", "elapsed_ms": 99, "repair_applied": False}
        return _valid_llm_payload(), {"ok": True, "error": "", "elapsed_ms": 88, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="gpt-oss:20b",
        llm_max_attempts=7,
        no_retry_on_rate_limit=True,
    )

    assert rows[0]["analysis_backend_used"] == "fallback"
    assert diag["llm_success_fallback"] == 1
    assert calls.count("gemma4:31b-cloud") == 1


def test_daily_runtime_defaults_to_qwen_when_models_missing() -> None:
    cfg = replace(_cfg(), ollama_model="", ollama_fallback_model="")
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime={},
        main_model_override=None,
        fallback_model_override=None,
        fallback2_model_override=None,
        fallback_timeout_seconds=None,
        no_retry_on_rate_limit=False,
    )
    assert runtime["main"]["model"] == "qwen3.5:397b-cloud"
    assert runtime["fallback"]["model"] == "deepseek-v3.1:671b-cloud"


def test_daily_runtime_cli_override_has_priority() -> None:
    runtime = _runtime_from_config(
        cfg=_cfg(),
        llm_runtime={},
        main_model_override="override-main",
        fallback_model_override="override-fallback",
        fallback2_model_override=None,
        fallback_timeout_seconds=None,
        no_retry_on_rate_limit=False,
    )
    assert runtime["main"]["model"] == "override-main"
    assert runtime["fallback"]["model"] == "override-fallback"


def test_fallback2_used_when_main_rate_limited_and_fallback_invalid(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if str(model).startswith("gemma4"):
            return None, {"ok": False, "error": "HTTP 429: usage limit", "elapsed_ms": 40, "repair_applied": False}
        if str(model).startswith("gpt-oss"):
            return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 40, "repair_applied": False}
        return _valid_llm_payload(), {"ok": True, "error": "", "elapsed_ms": 33, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="gpt-oss:20b",
        fallback2_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=7,
        no_retry_on_rate_limit=True,
    )

    assert rows[0]["analysis_backend_used"] == "fallback2"
    assert diag["llm_success_fallback2"] == 1


def test_timeout_on_main_moves_to_fallback(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if str(model).startswith("gemma4"):
            return None, {"ok": False, "error": "timed out", "elapsed_ms": 120000, "repair_applied": False}
        return _valid_llm_payload(), {"ok": True, "error": "", "elapsed_ms": 50, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="gpt-oss:20b",
        llm_max_attempts=7,
    )

    assert rows[0]["analysis_backend_used"] == "fallback"
    assert diag["llm_success_fallback"] == 1


def test_all_attempts_fail_row_is_quarantined_not_scripted(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 11, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.daily_control.daily_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1},
    )

    rows, diag = analyze_daily_packages(
        packages=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=5,
    )

    row = rows[0]
    assert row["analysis_backend_used"] == "quarantined_llm_failed"
    assert row["main_pattern"] == ""
    assert row["what_to_tell_employee"] == ""
    assert diag["llm_failed_count"] == 1
    assert diag["quarantined_count"] == 1
    quarantined = diag["quarantined_rows"][0]
    assert quarantined["prompt_size_chars"] > 0
    assert isinstance(quarantined["errors_by_attempt"], list)
    assert quarantined["errors_by_attempt"]
    assert "not valid JSON object" in quarantined["raw_response_preview"]


def test_main_preflight_uses_non_empty_json_probe(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        captured["model"] = model
        captured["base_url"] = base_url
        captured["timeout_seconds"] = timeout_seconds
        captured["messages"] = messages
        return {"ok": True}, {"ok": True, "error": "", "elapsed_ms": 12, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.daily_control.daily_analyzer._call_llm", _fake_call_llm)

    from src.deal_analyzer.daily_control.daily_analyzer import _preflight_model

    result = _preflight_model(model="gemma4:31b-cloud", base_url="http://127.0.0.1:11434", timeout_seconds=20)
    assert result["ok"] is True
    assert int(result.get("prompt_size_chars", 0)) > 0
    messages = captured.get("messages", [])
    assert isinstance(messages, list)
    assert messages
    assert "Ответь строго JSON-объектом" in str(messages[-1].get("content", ""))


def test_roks_parser_selects_april_and_march_and_extracts_metrics() -> None:
    sheet_titles = [
        "РОКС ОАП-январь 2026",
        "РОКС ОАП-февраль 2026",
        "РОКС ОАП-март 2026",
        "РОКС ОАП-апрель 2026",
    ]

    matrix = [["" for _ in range(17)] for _ in range(120)]
    matrix[42][0] = "Бочков"
    metric_values = {0: "20", 1: "100", 2: "", 3: "14", 4: "9", 5: "7", 6: "4", 7: "3", 8: "120000", 9: "2", 10: "80000"}
    for offset, value in metric_values.items():
        matrix[42 + offset][3] = value
    matrix[44][5] = "1"
    matrix[44][7] = "2"
    matrix[44][9] = "3"
    matrix[44][11] = "4"
    matrix[44][13] = "5"

    class _FakeClient:
        def list_sheets(self, spreadsheet_id: str):
            _ = spreadsheet_id
            return [{"title": title} for title in sheet_titles]

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "апрель 2026" in rng:
                return matrix
            if "март 2026" in rng:
                return matrix
            return []

    snapshot = parse_roks_oap_snapshot(
        client=_FakeClient(),
        spreadsheet_id="sheet",
        period_end=date(2026, 4, 24),
        manager_allowlist=("Илья Бочков",),
    )

    assert snapshot["selected_current_month_sheet"] == "РОКС ОАП-апрель 2026"
    assert snapshot["selected_previous_month_sheet"] == "РОКС ОАП-март 2026"
    assert snapshot["status"] == "sheets_found_metrics_extracted"
    metrics = snapshot["manager_metrics"]["Илья Бочков"]["current_month"]
    assert metrics["dials"] == 100
    assert metrics["reach"] == 15


def test_idempotency_exact_same_skips_duplicate() -> None:
    headers = ["Неделя с", "Неделя по", "Дата контроля", "Менеджер", "Проанализировано сделок", "Количество звонков"]
    existing_rows = [["2026-04-20", "2026-04-26", "2026-04-22", "Илья Бочков", "3", "5"]]
    payload_rows = [{"week_start": "2026-04-20", "week_end": "2026-04-26", "control_day_date": "2026-04-22", "manager_name": "Илья Бочков", "deals_count": 3, "calls_count": 5}]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert plan["ok"] is True
    assert len(plan["rows_skipped_existing"]) == 1
    assert len(plan["rows_to_insert"]) == 0
    assert len(plan["rows_to_update"]) == 0


def test_idempotency_bigger_counts_updates_existing_row() -> None:
    headers = ["Неделя с", "Неделя по", "Дата контроля", "Менеджер", "Проанализировано сделок", "Количество звонков"]
    existing_rows = [["2026-04-20", "2026-04-26", "2026-04-22", "Илья Бочков", "3", "5"]]
    payload_rows = [{"week_start": "2026-04-20", "week_end": "2026-04-26", "control_day_date": "2026-04-22", "manager_name": "Илья Бочков", "deals_count": 4, "calls_count": 7}]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert len(plan["rows_to_update"]) == 1
    assert len(plan["conflicts"]) == 0
    assert len(plan["rows_to_insert"]) == 0


def test_idempotency_smaller_counts_skips_stale() -> None:
    headers = ["Неделя с", "Неделя по", "Дата контроля", "Менеджер", "Проанализировано сделок", "Количество звонков"]
    existing_rows = [["2026-04-20", "2026-04-26", "2026-04-22", "Илья Бочков", "6", "8"]]
    payload_rows = [{"week_start": "2026-04-20", "week_end": "2026-04-26", "control_day_date": "2026-04-22", "manager_name": "Илья Бочков", "deals_count": 4, "calls_count": 7}]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert len(plan["rows_skipped_stale"]) == 1
    assert len(plan["rows_to_update"]) == 0


def test_idempotency_weird_mismatch_conflict() -> None:
    headers = ["Неделя с", "Неделя по", "Дата контроля", "Менеджер", "Проанализировано сделок", "Количество звонков"]
    existing_rows = [["2026-04-20", "2026-04-26", "2026-04-22", "Илья Бочков", "6", "8"]]
    payload_rows = [{"week_start": "2026-04-20", "week_end": "2026-04-26", "control_day_date": "2026-04-22", "manager_name": "Илья Бочков", "deals_count": 5, "calls_count": 9}]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert len(plan["conflicts"]) == 1
    assert plan["conflicts"][0]["reason"] == "conflict_needs_review"


def test_writer_plan_created_on_dry_run_contains_update_fields(monkeypatch) -> None:
    payload = {"rows": [_row_for_preflight()]}
    run_dir = Path("workspace/tmp_tests/daily_control_writer_test/new_run_v2").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "daily_control_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:CS" in rng:
                return [[
                    "Неделя с", "Неделя по", "Дата контроля", "День", "Менеджер", "Роль менеджера",
                    "Проанализировано сделок", "Количество звонков", "Ключевой вывод", "Сильные стороны",
                    "Зоны роста", "Почему это важно", "Что закрепить", "Что исправить", "Что донес сотруднику",
                    "Ожидаемый эффект - количество", "Ожидаемый эффект - качество", "Оценка 0-100", "Критичность",
                ]]
            return []

        def resolve_sheet(self, spreadsheet_id: str, tab_name: str):
            _ = spreadsheet_id
            return {"title": tab_name, "sheetId": 1}

        def build_service(self):
            raise RuntimeError("no service in unit test")

        def insert_rows(self, **kwargs):
            raise AssertionError("dry-run must not insert")

        def batch_update_values(self, *args, **kwargs):
            raise AssertionError("dry-run must not write")

    monkeypatch.setattr("src.deal_analyzer.daily_control.sheets_writer.GoogleSheetsApiClient", _FakeClient)

    cfg = replace(_cfg(), deal_analyzer_spreadsheet_id="sheet-id", deal_analyzer_write_enabled=True)
    status = write_daily_control_rows(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name="Дневной контроль",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )

    assert status["mode"] == "dry_run"
    assert status["rows_written"] == 0
    assert status["block_reason"] == "dry_run_mode"

    plan_path = run_dir / "daily_control_writer_plan.json"
    assert plan_path.exists()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan.get("write_strategy") == "values_only"
    assert plan.get("structural_changes_required") is False
    assert plan.get("insert_operations") == []
    assert isinstance(plan.get("planned_value_ranges"), list)
    assert "rows_to_update" in plan
    assert "rows_skipped_stale" in plan
    assert "planned_update_ranges" in plan
    assert isinstance(plan.get("planned_ranges"), list)


def test_preflight_allows_partial_write_with_row_quarantine() -> None:
    rows = [
        _row_for_preflight(),
        _row_for_preflight(main_pattern="hello clarifying decision-makers and gathering contacts"),
    ]
    preflight = evaluate_writer_preflight(
        rows=rows,
        strict_preflight=True,
        conflicts_count=0,
        duplicate_policy="skip",
        allow_partial_write=True,
        quarantine_unrepaired=True,
    )
    assert preflight["passed"] is True
    assert preflight["rows_quarantined_count"] == 1
    assert preflight["rows_for_write_count"] == 1


def test_base_and_product_mix_sorted_by_frequency() -> None:
    base_mix = aggregate_mix(["Инглегмаш-2026; tilda", "Инглегмаш-2026", "стройка_линк; Инглегмаш-2026"])
    product_mix = aggregate_mix(["линк; инфо", "линк", "инфо", "линк"])
    assert base_mix.startswith("Инглегмаш-2026 - 3")
    assert product_mix.startswith("линк - 3")


def test_grouping_uses_period_and_manager_filters() -> None:
    headers = ["Дата кейса", "Менеджер", "Роль", "Deal ID", "Прослушанные звонки", "Продукт / фокус", "База / тег"]
    rows = [
        ["2026-04-22", "Илья Бочков", "менеджер по продажам", "1", "2026-04-22 11:00 - 03:20", "линк", "tilda"],
        ["2026-04-25", "Илья Бочков", "менеджер по продажам", "2", "2026-04-25 12:00 - 04:20", "инфо", "expo"],
        ["2026-04-22", "Антон", "менеджер", "3", "2026-04-22 12:00 - 02:00", "линк", "x"],
    ]
    groups, diag = group_by_manager_day(
        headers=headers,
        rows=rows,
        cfg=_cfg(),
        period_start=date(2026, 4, 20),
        period_end=date(2026, 4, 24),
        manager_allowlist=("Илья Бочков", "Рустам Хомидов"),
    )
    assert len(groups) == 1
    assert groups[0].manager_name == "Илья Бочков"
    assert groups[0].deals_count == 1
    assert diag["rows_filtered_out"] == 2


def test_week_bounds_from_control_day_date() -> None:
    week_start, week_end = week_bounds_monday_sunday("2026-04-24")
    assert week_start == "2026-04-20"
    assert week_end == "2026-04-26"


def test_rows_have_per_day_week_not_global_period() -> None:
    headers = ["Дата кейса", "Менеджер", "Роль", "Deal ID", "Прослушанные звонки", "Продукт / фокус", "База / тег"]
    rows = [
        ["2026-03-30", "Илья Бочков", "менеджер по продажам", "1", "2026-03-30 11:00 - 03:20", "линк", "tilda"],
        ["2026-04-06", "Илья Бочков", "менеджер по продажам", "2", "2026-04-06 12:00 - 04:20", "инфо", "expo"],
        ["2026-04-13", "Илья Бочков", "менеджер по продажам", "3", "2026-04-13 12:00 - 04:20", "инфо", "expo"],
        ["2026-04-20", "Илья Бочков", "менеджер по продажам", "4", "2026-04-20 12:00 - 04:20", "инфо", "expo"],
    ]
    groups, _ = group_by_manager_day(
        headers=headers,
        rows=rows,
        cfg=_cfg(),
        period_start=date(2026, 3, 30),
        period_end=date(2026, 4, 24),
        manager_allowlist=("Илья Бочков",),
    )
    by_day = {g.control_day_date: (g.week_start, g.week_end) for g in groups}
    assert by_day["2026-03-30"] == ("2026-03-30", "2026-04-05")
    assert by_day["2026-04-06"] == ("2026-04-06", "2026-04-12")
    assert by_day["2026-04-13"] == ("2026-04-13", "2026-04-19")
    assert by_day["2026-04-20"] == ("2026-04-20", "2026-04-26")


def test_criticality_written_in_russian_when_validation_is_free() -> None:
    recommended, written, mode = _resolve_criticality_value_for_write(requested="medium", allowed_values=[])
    assert recommended == "средняя"
    assert written == "средняя"
    assert mode == "free_input"


def test_criticality_english_fallback_when_dropdown_english_only() -> None:
    recommended, written, mode = _resolve_criticality_value_for_write(
        requested="средняя",
        allowed_values=["low", "medium", "high"],
    )
    assert recommended == "средняя"
    assert written == "medium"
    assert mode == "dropdown_english_fallback"
