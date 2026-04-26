from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.daily_control.daily_analyzer import analyze_daily_packages
from src.deal_analyzer.daily_control.day_grouper import aggregate_mix, group_by_manager_day
from src.deal_analyzer.daily_control.models import DailyControlInputGroup
from src.deal_analyzer.daily_control.roks_oap_parser import parse_roks_oap_snapshot
from src.deal_analyzer.daily_control.sheets_writer import plan_daily_control_write, write_daily_control_rows
from src.deal_analyzer.daily_control.validation.text_lint import lint_daily_text_rows
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
        "criticality": "medium",
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
    )

    assert len(rows) == 1
    assert rows[0]["analysis_backend_used"] == "main"
    assert diag["llm_success_main"] == 1
    assert captured["calls"][0]["model"] == "gemma4:31b-cloud"
    req = diag["llm_requests"][0]
    assert req["context"]["deal_ids"] == ["32000168"]
    assert req["context"]["base_mix"] == "tilda - 1"
    assert req["context"]["product_mix"] == "линк - 1"


def test_fallback_selected_when_main_returns_invalid_json(monkeypatch) -> None:
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
    )

    assert rows[0]["analysis_backend_used"] == "fallback"
    assert diag["llm_success_main"] == 0
    assert diag["llm_success_fallback"] == 1


def test_both_invalid_json_do_not_create_fake_deterministic_analytics(monkeypatch) -> None:
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
    )

    row = rows[0]
    assert row["analysis_backend_used"] == "deterministic_fallback"
    assert row["main_pattern"] == "не сформировано: llm_json_invalid"
    assert diag["llm_failed_count"] == 1


def test_roks_parser_selects_april_and_march_and_extracts_metrics() -> None:
    sheet_titles = [
        "РОКС ОАП-январь 2026",
        "РОКС ОАП-февраль 2026",
        "РОКС ОАП-март 2026",
        "РОКС ОАП-апрель 2026",
    ]

    matrix = [["" for _ in range(17)] for _ in range(120)]
    matrix[42][0] = "Бочков"  # row 43
    # offsets 0..10 starting from row 43
    metric_values = {
        0: "20",  # days
        1: "100",  # dials
        2: "",  # reach -> fallback from weekly facts
        3: "14",  # lpr
        4: "9",  # interest
        5: "7",  # demo
        6: "4",  # test
        7: "3",  # invoice_count
        8: "120000",  # invoice_amount
        9: "2",  # payment_count
        10: "80000",  # payment_amount
    }
    for offset, value in metric_values.items():
        matrix[42 + offset][3] = value  # column D
    # weekly facts for reach: F,H,J,L,N => idx 5,7,9,11,13
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
    assert metrics["reach"] == 15  # 1+2+3+4+5 fallback from weekly fact cols


def test_validation_blocks_foreign_greeting_but_not_style_warning() -> None:
    row_warning = _row_for_preflight(what_to_fix="разобрать выявления ЛПР")
    preflight_warning = evaluate_writer_preflight(
        rows=[row_warning],
        strict_preflight=True,
        conflicts_count=0,
        duplicate_policy="skip",
    )
    assert preflight_warning["passed"] is True
    assert int(preflight_warning["text_lint"].get("bad_grammar_marker_count", 0)) > 0

    row_block = _row_for_preflight(what_to_tell_employee="hello, fixed plan")
    preflight_block = evaluate_writer_preflight(
        rows=[row_block],
        strict_preflight=True,
        conflicts_count=0,
        duplicate_policy="skip",
    )
    assert preflight_block["passed"] is False
    assert any(rule.get("rule") == "text_lint_blockers_present" for rule in preflight_block["failed_rules"])


def test_idempotency_skips_existing_row_with_same_key_and_counts() -> None:
    headers = [
        "Неделя с",
        "Неделя по",
        "Дата контроля",
        "Менеджер",
        "Проанализировано сделок",
        "Количество звонков",
    ]
    existing_rows = [["2026-03-30", "2026-04-24", "2026-04-22", "Илья Бочков", "3", "5"]]
    payload_rows = [
        {
            "period_start": "2026-03-30",
            "period_end": "2026-04-24",
            "control_day_date": "2026-04-22",
            "manager_name": "Илья Бочков",
            "deals_count": 3,
            "calls_count": 5,
        }
    ]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert plan["ok"] is True
    assert len(plan["rows_skipped_existing"]) == 1
    assert len(plan["rows_to_insert"]) == 0


def test_conflict_when_same_day_manager_but_different_counts() -> None:
    headers = [
        "Неделя с",
        "Неделя по",
        "Дата контроля",
        "Менеджер",
        "Проанализировано сделок",
        "Количество звонков",
    ]
    existing_rows = [["2026-03-30", "2026-04-24", "2026-04-22", "Илья Бочков", "3", "5"]]
    payload_rows = [
        {
            "period_start": "2026-03-30",
            "period_end": "2026-04-24",
            "control_day_date": "2026-04-22",
            "manager_name": "Илья Бочков",
            "deals_count": 4,
            "calls_count": 7,
        }
    ]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert len(plan["conflicts"]) == 1
    assert len(plan["rows_to_insert"]) == 0


def test_rows_with_dropdown_only_are_treated_as_empty() -> None:
    headers = [
        "Неделя с",
        "Неделя по",
        "Дата контроля",
        "Менеджер",
        "Комментарий",
        "Проанализировано сделок",
        "Количество звонков",
    ]
    existing_rows = [["", "", "", "", "formula", "", ""]]
    payload_rows = [
        {
            "period_start": "2026-03-30",
            "period_end": "2026-04-24",
            "control_day_date": "2026-04-22",
            "manager_name": "Рустам Хомидов",
            "deals_count": 1,
            "calls_count": 1,
        }
    ]
    plan = plan_daily_control_write(payload_rows=payload_rows, headers=headers, existing_rows=existing_rows)
    assert plan["existing_rows_detected"] == 0
    assert len(plan["rows_to_insert"]) == 1
    assert plan["rows_to_insert"][0]["row_number"] == 3


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


def test_writer_plan_created_on_dry_run(monkeypatch) -> None:
    payload = {
        "rows": [_row_for_preflight()],
    }
    run_dir = Path("workspace/tmp_tests/daily_control_writer_test/new_run").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "daily_control_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:CS" in rng:
                return [[
                    "Неделя с",
                    "Неделя по",
                    "Дата контроля",
                    "День",
                    "Менеджер",
                    "Роль менеджера",
                    "Проанализировано сделок",
                    "Количество звонков",
                    "Ключевой вывод",
                    "Сильные стороны",
                    "Зоны роста",
                    "Почему это важно",
                    "Что закрепить",
                    "Что исправить",
                    "Что донес сотруднику",
                    "Ожидаемый эффект - количество",
                    "Ожидаемый эффект - качество",
                    "Оценка 0-100",
                    "Критичность",
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
        logger=None,
    )

    assert status["mode"] == "dry_run"
    assert status["rows_written"] == 0
    assert status["block_reason"] == "dry_run_mode"

    plan_path = run_dir / "daily_control_writer_plan.json"
    assert plan_path.exists()
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert isinstance(plan.get("planned_ranges"), list)
    assert len(plan.get("planned_ranges", [])) >= 1


def test_quality_block_reason_precedes_dry_run_mode(monkeypatch) -> None:
    payload = {
        "rows": [_row_for_preflight(what_to_tell_employee="hello team")],
    }
    run_dir = Path("workspace/tmp_tests/daily_control_writer_test/new_run_quality").resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "daily_control_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:CS" in rng:
                return [[
                    "Неделя с",
                    "Неделя по",
                    "Дата контроля",
                    "День",
                    "Менеджер",
                    "Роль менеджера",
                    "Проанализировано сделок",
                    "Количество звонков",
                    "Ключевой вывод",
                    "Сильные стороны",
                    "Зоны роста",
                    "Почему это важно",
                    "Что закрепить",
                    "Что исправить",
                    "Что донес сотруднику",
                    "Ожидаемый эффект - количество",
                    "Ожидаемый эффект - качество",
                    "Оценка 0-100",
                    "Критичность",
                ]]
            return []

        def resolve_sheet(self, spreadsheet_id: str, tab_name: str):
            _ = spreadsheet_id
            return {"title": tab_name, "sheetId": 1}

        def build_service(self):
            raise RuntimeError("no service in unit test")

    monkeypatch.setattr("src.deal_analyzer.daily_control.sheets_writer.GoogleSheetsApiClient", _FakeClient)

    cfg = replace(_cfg(), deal_analyzer_spreadsheet_id="sheet-id", deal_analyzer_write_enabled=True)
    status = write_daily_control_rows(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name="Дневной контроль",
        dry_run=True,
        strict_preflight=True,
        logger=None,
    )

    assert status["error"] == "quality_preflight_failed"
    assert status["block_reason"] == "quality_preflight_failed"


def test_daily_text_lint_detects_english_blocker() -> None:
    lint = lint_daily_text_rows([
        _row_for_preflight(main_pattern="Clarifying decision-makers and gathering contacts")
    ])
    assert int(lint.get("foreign_language_count", 0)) > 0
