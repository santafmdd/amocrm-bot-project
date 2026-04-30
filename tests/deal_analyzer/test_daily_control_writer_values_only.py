from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.daily_control.sheets_writer import write_daily_control_rows


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


def _headers() -> list[str]:
    return [
        "Неделя с",
        "Неделя по",
        "Дата контроля",
        "День",
        "Менеджер",
        "Роль менеджера",
        "Проанализировано сделок",
        "Ссылки на сделки",
        "Продукт / фокус",
        "База микс",
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
    ]


def _row_for_day(day: date, manager: str, *, deals_count: int = 1, calls_count: int = 1) -> dict:
    week_start = day - timedelta(days=day.weekday())
    week_end = week_start + timedelta(days=6)
    return {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "period_start": "2026-03-30",
        "period_end": "2026-04-24",
        "control_day_date": day.isoformat(),
        "day_label": "понедельник",
        "manager_name": manager,
        "manager_role_profile": "менеджер по продажам",
        "sample_size": deals_count,
        "deals_count": deals_count,
        "calls_count": calls_count,
        "deal_ids": "32000168",
        "deal_links": "https://example/1",
        "product_mix": "линк - 1",
        "base_mix": "tilda - 1",
        "main_pattern": "Нужно стабильнее фиксировать следующий шаг.",
        "strong_sides": "Хороший контакт с клиентом.",
        "growth_zones": "Слабая фиксация следующего шага.",
        "why_it_matters": "Без фиксации шага падает управляемость воронки.",
        "what_to_reinforce": "Короткий и уверенный вход в диалог.",
        "what_to_fix": "В конце звонка фиксировать дату и формат следующего шага.",
        "what_to_tell_employee": "Разобрать 2 звонка и закрепить правило фиксации шага.",
        "expected_quant_impact": "Ожидаемо +1-2 управляемых шага в неделю.",
        "expected_qual_impact": "Стабилизируется переход к следующему этапу.",
        "score_0_100": 62,
        "criticality": "средняя",
    }


def _write_payload(run_dir: Path, rows: list[dict]) -> None:
    payload = {"rows": rows}
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "daily_control_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_values_only_empty_sheet_plans_contiguous_range_and_no_insert_ops(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/daily_control_values_only/empty_sheet").resolve()
    rows = []
    start = date(2026, 3, 30)
    for i in range(31):
        manager = "Илья Бочков" if i % 2 == 0 else "Рустам Хомидов"
        rows.append(_row_for_day(start + timedelta(days=i), manager))
    _write_payload(run_dir, rows)

    class _FakeClient:
        insert_calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:CS" in rng:
                return [_headers()]
            return []

        def resolve_sheet(self, spreadsheet_id: str, tab_name: str):
            _ = spreadsheet_id
            return {"title": tab_name, "sheetId": 1}

        def build_service(self):
            raise RuntimeError("not needed in unit test")

        def insert_rows(self, **kwargs):
            _ = kwargs
            _FakeClient.insert_calls += 1
            raise AssertionError("values-only writer must not call insert_rows")

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            raise AssertionError("dry-run must not write values")

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
    assert status["write_strategy"] == "values_only"
    assert status["rows_to_insert"] == 31
    assert status["rows_to_update"] == 0
    assert status["structural_changes_required"] is False
    assert status["block_reason"] == "dry_run_mode"
    assert status["rows_written"] == 0
    assert status["planned_value_ranges"][:1] == ["'Дневной контроль'!A2:U32"]
    assert _FakeClient.insert_calls == 0

    plan = json.loads((run_dir / "daily_control_writer_plan.json").read_text(encoding="utf-8"))
    assert plan["write_strategy"] == "values_only"
    assert plan["structural_changes_required"] is False
    assert plan["insert_operations"] == []
    assert plan["planned_value_ranges"][:1] == ["'Дневной контроль'!A2:U32"]


def test_values_only_real_write_does_not_call_insert_rows(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/daily_control_values_only/real_values_only").resolve()
    rows = [_row_for_day(date(2026, 4, 20), "Илья Бочков"), _row_for_day(date(2026, 4, 21), "Рустам Хомидов")]
    _write_payload(run_dir, rows)

    class _FakeClient:
        insert_calls = 0
        batch_calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:CS" in rng:
                return [_headers()]
            return []

        def resolve_sheet(self, spreadsheet_id: str, tab_name: str):
            _ = spreadsheet_id
            return {"title": tab_name, "sheetId": 1}

        def build_service(self):
            raise RuntimeError("not needed in unit test")

        def insert_rows(self, **kwargs):
            _ = kwargs
            _FakeClient.insert_calls += 1
            raise AssertionError("values-only writer must not call insert_rows")

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            _FakeClient.batch_calls += 1
            return {}

    monkeypatch.setattr("src.deal_analyzer.daily_control.sheets_writer.GoogleSheetsApiClient", _FakeClient)

    cfg = replace(_cfg(), deal_analyzer_spreadsheet_id="sheet-id", deal_analyzer_write_enabled=True)
    status = write_daily_control_rows(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name="Дневной контроль",
        dry_run=False,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )

    assert status["mode"] == "real_write"
    assert status["write_strategy"] == "values_only"
    assert status["rows_written"] == 2
    assert status["rows_inserted"] == 2
    assert status["rows_updated"] == 0
    assert status["final_written_range"] == "Дневной контроль!A2:U3"
    assert _FakeClient.insert_calls == 0
    assert _FakeClient.batch_calls >= 1


def test_structural_insert_required_blocks_real_write(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/daily_control_values_only/needs_structural_insert").resolve()
    rows = [_row_for_day(date(2026, 4, 11), "Илья Бочков", deals_count=4, calls_count=7)]
    _write_payload(run_dir, rows)

    class _FakeClient:
        insert_calls = 0
        batch_calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:CS" in rng:
                return [_headers()]
            return [
                ["2026-04-06", "2026-04-12", "2026-04-10", "пятница", "Илья Бочков", "менеджер по продажам", "3", "https://example/1"],
                ["2026-04-06", "2026-04-12", "2026-04-12", "воскресенье", "Илья Бочков", "менеджер по продажам", "3", "https://example/1"],
            ]

        def resolve_sheet(self, spreadsheet_id: str, tab_name: str):
            _ = spreadsheet_id
            return {"title": tab_name, "sheetId": 1}

        def build_service(self):
            raise RuntimeError("not needed in unit test")

        def insert_rows(self, **kwargs):
            _ = kwargs
            _FakeClient.insert_calls += 1
            raise AssertionError("insert_rows must be blocked in normal mode")

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            _FakeClient.batch_calls += 1
            return {}

    monkeypatch.setattr("src.deal_analyzer.daily_control.sheets_writer.GoogleSheetsApiClient", _FakeClient)

    cfg = replace(_cfg(), deal_analyzer_spreadsheet_id="sheet-id", deal_analyzer_write_enabled=True)
    status = write_daily_control_rows(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name="Дневной контроль",
        dry_run=False,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )

    assert status["write_strategy"] == "values_only"
    assert status["structural_changes_required"] is True
    assert status["write_allowed"] is False
    assert status["block_reason"] == "requires_structural_insert"
    assert status["error"] == "requires_structural_insert"
    assert status["rows_written"] == 0
    assert _FakeClient.insert_calls == 0
    assert _FakeClient.batch_calls == 0

    plan = json.loads((run_dir / "daily_control_writer_plan.json").read_text(encoding="utf-8"))
    assert plan["structural_changes_required"] is True
    assert plan["planned_structural_operations"]
