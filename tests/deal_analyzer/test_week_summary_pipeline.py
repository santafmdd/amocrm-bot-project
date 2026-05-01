from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.week_summary import cli as week_summary_cli
from src.deal_analyzer.week_summary.aggregator import build_week_summary_groups
from src.deal_analyzer.week_summary.analyzer import _runtime_from_config, _sanitize_role_based_phrase, analyze_week_summary_groups
from src.deal_analyzer.week_summary.models import WeekSummaryGroup
from src.deal_analyzer.week_summary.sheets_writer import plan_week_summary_write, write_week_summary_rows


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
        deal_analyzer_spreadsheet_id="sheet",
        deal_analyzer_write_enabled=True,
    )


def _manager_headers() -> list[str]:
    return ["Неделя с", "Неделя по", "Менеджер", "Проанализировано сделок", "Средняя оценка 0-100", "Итог недели"]


def _plan_headers() -> list[str]:
    return ["План недели с", "План недели по", "Статус", "Ссылка на обучение / материал", "Ссылка на задачи после обучения", "Что делаю"]


def _target_headers() -> list[str]:
    return [
        "Неделя с",
        "Неделя по",
        "Краткий отчет за прошлую неделю",
        "Что изменилось количественно",
        "Что изменилось качественно",
        "Что не сработало",
        "Фокус следующей недели",
        "План следующей недели",
        "Что говорю на еженедельном собрании",
        "Около-стратегические акценты",
        "Риски",
        "Формулировка для руководителя",
        "Проанализировано сделок",
    ]


def _payload_row(**overrides):
    row = {
        "week_start": "2026-04-27",
        "week_end": "2026-05-03",
        "brief_report": "Краткий итог недели по отделу.",
        "quantity_delta": "По количеству держим темп.",
        "quality_delta": "Качество выросло по фиксации шага.",
        "what_failed": "Часть задач без статуса.",
        "focus_next_week": "Фокус на фиксации следующего шага.",
        "next_week_plan": "Дожимаем дисциплину CRM по шагам.",
        "meeting_message": "На собрании фиксируем дедлайны.",
        "strategic_accents": "Сохраняем темп по лидогенерации.",
        "risks": "Риск переносов задач без контроля.",
        "manager_report_phrase": "Неделя стабильная, но контроль статусов обязателен.",
        "deals_count": 12,
    }
    row.update(overrides)
    return row


def _sample_group() -> WeekSummaryGroup:
    return WeekSummaryGroup(
        period_start="2026-04-27",
        period_end="2026-05-03",
        week_start="2026-04-27",
        week_end="2026-05-03",
        source_manager_rows=[{"manager_name": "Илья Бочков", "deals_count": 5}],
        source_plan_rows=[{"status": "в работе", "what_i_do": "Разобрать 2 звонка"}],
        managers_count=2,
        deals_count=12,
        avg_score_0_100=68,
        planned_actions_total=4,
        done_actions_count=1,
        in_progress_actions_count=2,
        postponed_actions_count=1,
        no_status_actions_count=0,
        training_links=["https://train/link"],
        post_training_task_links=["https://tasks/link"],
        unresolved_actions=["Разобрать 2 звонка"],
    )


def test_week_summary_aggregation_from_manager_and_plan() -> None:
    manager_rows = [
        ["2026-04-27", "2026-05-03", "Илья Бочков", "5", "70", "Итог"],
        ["2026-04-27", "2026-05-03", "Рустам Хомидов", "7", "66", "Итог"],
    ]
    plan_rows = [
        ["2026-04-27", "2026-05-03", "выполнено", "https://train/link", "https://tasks/link", "Разобрать кейс"],
        ["2026-04-27", "2026-05-03", "в работе", "", "", "Проверить CRM"],
    ]
    groups, diag, plan_fact = build_week_summary_groups(
        manager_headers=_manager_headers(),
        manager_rows=manager_rows,
        plan_headers=_plan_headers(),
        plan_rows=plan_rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
    )
    assert diag["groups_count"] == 1
    assert groups[0].deals_count == 12
    assert groups[0].planned_actions_total == 2
    assert len(plan_fact) == 2


def test_week_summary_runtime_defaults_to_deepseek_v4_when_models_missing() -> None:
    cfg = DealAnalyzerConfig(**{**_cfg().__dict__, "ollama_model": "", "ollama_fallback_model": ""})
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime={},
        main_model_override=None,
        fallback_model_override=None,
    )
    assert runtime["main"]["model"] == "deepseek-v4-pro:cloud"
    assert runtime["fallback"]["model"] == "deepseek-v4-flash:cloud"


def test_week_summary_runtime_cli_override_has_priority() -> None:
    runtime = _runtime_from_config(
        cfg=_cfg(),
        llm_runtime={},
        main_model_override="override-main",
        fallback_model_override="override-fallback",
    )
    assert runtime["main"]["model"] == "override-main"
    assert runtime["fallback"]["model"] == "override-fallback"


def test_weekly_summary_sales_manager_recommendations_do_not_push_cold_calls() -> None:
    sanitized = _sanitize_role_based_phrase("Массовый обзвон и 20 звонков по базе как план дня.")
    assert "массовый обзвон" not in sanitized.lower()
    assert "фокус на теплой/текущей воронке" in sanitized.lower()


def test_week_summary_writer_skip_existing() -> None:
    existing = [["2026-04-27", "2026-05-03", "Краткий итог недели по отделу.", "", "", "", "", "", "", "", "", "", "12"]]
    plan = plan_week_summary_write(
        payload_rows=[_payload_row()],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_skipped_existing"]) == 1


def test_week_summary_writer_update_when_new_facts() -> None:
    existing = [["2026-04-27", "2026-05-03", "Старый итог.", "", "", "", "", "", "", "", "", "", "8"]]
    plan = plan_week_summary_write(
        payload_rows=[_payload_row(deals_count=12)],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_to_update"]) == 1


def test_week_summary_no_structural_insert_in_normal_path() -> None:
    existing = [["2026-04-20", "2026-04-26", "Итог", "", "", "", "", "", "", "", "", "", "10"]]
    plan = plan_week_summary_write(
        payload_rows=[_payload_row(week_start="2026-04-27", week_end="2026-05-03")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert plan["structural_changes_required"] is False
    assert plan["planned_structural_operations"] == []


def test_week_summary_llm_invalid_json_then_fallback_success(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if "gemma" in model:
            return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 9, "repair_applied": False}
        return {
            "brief_report": "Краткий итог",
            "quantity_delta": "Количество стабильно",
            "quality_delta": "Качество выросло",
            "what_failed": "Слабый контроль статусов",
            "focus_next_week": "Фокус на фиксации шага",
            "next_week_plan": "Дожать дисциплину CRM",
            "meeting_message": "Фиксируем дедлайны на неделю",
            "strategic_accents": "Не просаживать лидогенерацию",
            "risks": "Перенос задач",
            "manager_report_phrase": "Итог недели рабочий",
        }, {"ok": True, "error": "", "elapsed_ms": 9, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.week_summary.analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.week_summary.analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )
    rows, diag = analyze_week_summary_groups(
        groups=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    assert len(rows) == 1
    assert rows[0]["analysis_backend_used"].startswith("fallback")
    assert diag["llm_success_fallback"] >= 1


def test_week_summary_llm_full_failure_to_quarantine(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 9, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.week_summary.analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.week_summary.analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )
    rows, diag = analyze_week_summary_groups(
        groups=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    assert rows == []
    assert diag["quarantined_count"] == 1
    assert diag["quarantined_rows"][0]["raw_response_preview"]


def test_week_summary_sanitizes_assigned_demo_phrase(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "brief_report": "Бочков назначил 18 демо за неделю.",
            "quantity_delta": "Назначил 18 демо и удержал темп.",
            "quality_delta": "Качество выросло.",
            "what_failed": "Нужен контроль статусов.",
            "focus_next_week": "Фиксация следующего шага.",
            "next_week_plan": "Контроль переходов.",
            "meeting_message": "Назначил 18 демо.",
            "strategic_accents": "Не просаживать верх воронки.",
            "risks": "Перенос задач.",
            "manager_report_phrase": "Назначил 18 демо.",
        }, {"ok": True, "error": "", "elapsed_ms": 9, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.week_summary.analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.week_summary.analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )
    rows, _diag = analyze_week_summary_groups(
        groups=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    row = rows[0]
    assert "назначил 18 демо" not in row["brief_report"].lower()
    assert "провел 18 демо" in row["brief_report"].lower()


def test_week_summary_writer_dry_run_no_sheet_mutation(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/week_summary") / f"run_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "week_summary_payload.json").write_text(
        json.dumps({"rows": [_payload_row()]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    class FakeClient:
        calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [_target_headers()]
            if "A1:AZ1" in rng:
                return [_target_headers()]
            return []

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            FakeClient.calls += 1

    monkeypatch.setattr("src.deal_analyzer.week_summary.sheets_writer.GoogleSheetsApiClient", FakeClient)
    status = write_week_summary_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="Свод недели",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["mode"] == "dry_run"
    assert status["rows_written"] == 0
    assert FakeClient.calls == 0


def _target_headers_without_count() -> list[str]:
    return [item for item in _target_headers() if item != "Проанализировано сделок"]


def test_week_summary_writer_identity_without_deals_count_column() -> None:
    plan = plan_week_summary_write(
        payload_rows=[_payload_row()],
        headers=_target_headers_without_count(),
        existing_rows=[],
        data_start_row=2,
    )
    assert plan["ok"] is True
    assert plan["error"] == ""
    assert len(plan["rows_to_insert"]) == 1


def test_week_summary_fallback_from_daily_when_manager_summary_empty() -> None:
    groups, diag, _ = build_week_summary_groups(
        manager_headers=[],
        manager_rows=[],
        plan_headers=_plan_headers(),
        plan_rows=[["2026-04-27", "2026-05-03", "в работе", "", "", "Проверить CRM"]],
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        daily_headers=["week_start", "week_end", "manager_name", "deals_count", "score_0_100", "main_pattern"],
        daily_rows=[["2026-04-27", "2026-05-03", "Илья Бочков", "4", "70", "Закрывать следующий шаг датой и временем"]],
    )
    assert diag["daily_fallback_applied"] is True
    assert len(groups) == 1
    assert groups[0].deals_count == 4


def test_week_summary_cli_accepts_daily_sheet_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "build",
            "--config",
            "config/deal_analyzer.call_review.deepseek.realwrite.json",
            "--period-start",
            "2026-04-27",
            "--period-end",
            "2026-05-03",
            "--daily-sheet",
            "Дневной контроль",
            "--plan-sheet",
            "План недели",
            "--manager-summary-sheet",
            "Недельный свод менеджеров",
            "--target-sheet",
            "Свод недели",
        ],
    )
    args = week_summary_cli._parse_args()
    assert args.daily_sheet == "Дневной контроль"


def test_week_summary_cli_defaults_use_utf8_sheet_names(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "discover",
            "--config",
            "config/deal_analyzer.call_review.deepseek.realwrite.json",
        ],
    )
    args = week_summary_cli._parse_args()
    assert args.manager_summary_sheet == "Недельный свод менеджеров"
    assert args.plan_sheet == "План недели"
    assert args.target_sheet == "Свод недели"

def test_weekly_summary_sales_manager_demo_recommendations_not_aggressive() -> None:
    sanitized = _sanitize_role_based_phrase(
        "На демо нужно давить и презентовать все функции подряд, чтобы продавить решение."
    ).lower()
    assert "давить" not in sanitized
    assert "презентовать все функции" not in sanitized
    assert "consultative demo" in sanitized
    assert "guided discovery" in sanitized
