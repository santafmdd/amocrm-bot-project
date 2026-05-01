from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.weekly_manager_summary import cli as weekly_manager_cli
from src.deal_analyzer.weekly_manager_summary.models import WeeklyManagerGroup
from src.deal_analyzer.weekly_manager_summary.sheets_writer import plan_weekly_manager_write, write_weekly_manager_rows
from src.deal_analyzer.weekly_manager_summary.week_grouper import aggregate_mix, group_daily_rows_by_week_manager
from src.deal_analyzer.weekly_manager_summary.weekly_analyzer import (
    _runtime_from_config,
    _sanitize_role_scope_phrase,
    analyze_weekly_groups,
)


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


def _source_headers() -> list[str]:
    return [
        "week_start",
        "week_end",
        "control_day_date",
        "day_label",
        "manager_name",
        "manager_role_profile",
        "deals_count",
        "calls_count",
        "deal_links",
        "product_mix",
        "base_mix",
        "main_pattern",
        "strong_sides",
        "growth_zones",
        "why_it_matters",
        "what_to_reinforce",
        "what_to_fix",
        "what_to_tell_employee",
        "expected_quant_impact",
        "expected_qual_impact",
        "score_0_100",
        "criticality",
    ]


def _plan_headers() -> list[str]:
    return [
        "plan_week_start",
        "plan_week_end",
        "plan_date",
        "day_label",
        "recipient",
        "activity_type",
        "what_i_do",
        "status",
        "training_link",
        "post_training_task_link",
    ]


def _target_headers() -> list[str]:
    return [
        "week_start",
        "week_end",
        "manager_name",
        "manager_role_profile",
        "deals_count",
        "product_focus_week",
        "base_mix_week",
        "weekly_result",
        "improved",
        "not_improved",
        "repeating_mistakes",
        "training_for_employee",
        "training_link",
        "post_training_tasks",
        "post_training_tasks_link",
        "manager_actions_next_week",
        "expected_quantity_effect",
        "expected_quality_effect",
        "manager_report_phrase",
        "employee_message",
        "avg_score_0_100",
    ]


def _payload_row(**overrides):
    row = {
        "week_start": "2026-04-27",
        "week_end": "2026-05-03",
        "manager_name": "Ilya Bochkov",
        "manager_role_profile": "manager",
        "deals_count": 5,
        "calls_count": 8,
        "source_day_count": 3,
        "product_focus_week": "info - 3; link - 1",
        "base_mix_week": "Inglemash-2026 - 4; tilda - 1",
        "weekly_result": "By week manager keeps pace but closes next-step weakly.",
        "improved": "Call opening became shorter and clearer.",
        "not_improved": "Part of calls still has no fixed next step date.",
        "repeating_mistakes": "Delays fixation to the end and loses control point.",
        "training_for_employee": "Plan training around closing to a concrete slot.",
        "training_link": "",
        "post_training_tasks": "After training run 10 calls by checklist.",
        "post_training_tasks_link": "",
        "manager_actions_next_week": "Daily review of 2 calls focused on close.",
        "expected_quantity_effect": "Expected +1..2 controlled next steps per week.",
        "expected_quality_effect": "Stabilize transition to next stage after calls.",
        "manager_report_phrase": "Progress is visible but close discipline is required.",
        "employee_message": "Fix date/time of next step in every conversation.",
        "avg_score_0_100": 64,
    }
    row.update(overrides)
    return row


def _sample_group() -> WeeklyManagerGroup:
    return WeeklyManagerGroup(
        period_start="2026-04-27",
        period_end="2026-05-03",
        week_start="2026-04-27",
        week_end="2026-05-03",
        manager_name="Ilya Bochkov",
        manager_role_profile="manager",
        source_rows=[
            {
                "control_day_date": "2026-04-27",
                "main_pattern": "Close step is weak",
                "strong_sides": "Good rapport",
                "growth_zones": "Fix next step",
                "what_to_fix": "Close to date",
                "what_to_tell_employee": "Review 2 calls",
                "score_0_100": 62,
            }
        ],
        source_day_count=1,
        deals_count=5,
        calls_count=8,
        avg_score_0_100=62,
        deal_links=["https://example/1"],
        product_mix_week="info - 2",
        base_mix_week="Inglemash-2026 - 2",
        repeated_growth_zones=["Fix next step"],
        repeated_strong_sides=["Good rapport"],
        repeated_fix_points=["Close to date"],
        repeated_messages=["Review 2 calls"],
        plan_actions_total=2,
        plan_done_count=1,
        plan_in_progress_count=1,
        plan_postponed_count=0,
        plan_no_status_count=0,
        plan_training_topics=["Plan-driven training topic"],
        plan_training_rows_found_count=1,
        plan_training_rows_used_count=1,
        plan_training_rows_used=[
            {
                "plan_date": "2026-04-28",
                "activity_type": "обучение",
                "topic": "Plan-driven training topic",
                "training_link": "https://train/link",
                "post_training_task_link": "https://tasks/link",
                "task_to_assign": "10 role-play calls",
            }
        ],
        plan_training_links=["https://train/link"],
        plan_post_training_task_links=["https://tasks/link"],
        unresolved_plan_actions=["Review two calls on next-step fixation"],
    )


def test_grouping_daily_rows_by_week_and_manager_with_plan_fact() -> None:
    rows = [
        ["2026-04-27", "2026-05-03", "2026-04-27", "monday", "Ilya Bochkov", "manager", "2", "3", "l1", "info", "A", "k", "s", "g", "w", "", "f", "t", "", "", "70", "medium"],
        ["2026-04-27", "2026-05-03", "2026-04-28", "tuesday", "Ilya Bochkov", "manager", "3", "5", "l2", "link", "A", "k", "s", "g", "w", "", "f", "t", "", "", "60", "medium"],
    ]
    plan_rows = [
        ["2026-04-27", "2026-05-03", "2026-04-28", "tuesday", "Ilya Bochkov", "обучение", "Review 2 calls", "in_progress", "https://train/link", "https://tasks/link"],
    ]
    groups, diag, plan_fact_rows = group_daily_rows_by_week_manager(
        headers=_source_headers(),
        rows=rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        manager_allowlist=("Ilya Bochkov",),
        plan_headers=_plan_headers(),
        plan_rows=plan_rows,
    )
    assert diag["groups_count"] == 1
    assert groups[0].plan_actions_total == 1
    assert groups[0].plan_in_progress_count == 1
    assert groups[0].plan_training_links == ["https://train/link"]
    assert groups[0].plan_training_rows_found_count == 1
    assert groups[0].plan_training_topics == ["Review 2 calls"]
    assert len(plan_fact_rows) == 1
    assert "Ilya Bochkov" in diag["managers_in_daily_control"]
    assert "Ilya Bochkov" in diag["managers_in_groups"]


def test_manager_coverage_debug_skipped_allowlist_reason() -> None:
    rows = [
        ["2026-04-27", "2026-05-03", "2026-04-27", "monday", "Ilya Bochkov", "manager", "2", "3", "l1", "info", "A", "k", "s", "g", "w", "", "f", "t", "", "", "70", "medium"],
        ["2026-04-27", "2026-05-03", "2026-04-28", "tuesday", "Rustam Khomidov", "telemarketer", "2", "3", "l2", "info", "A", "k", "s", "g", "w", "", "f", "t", "", "", "65", "medium"],
    ]
    _groups, diag, _plan_fact_rows = group_daily_rows_by_week_manager(
        headers=_source_headers(),
        rows=rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        manager_allowlist=("Ilya Bochkov",),
        plan_headers=_plan_headers(),
        plan_rows=[],
    )
    assert "Ilya Bochkov" in diag["managers_in_daily_control"]
    assert "Rustam Khomidov" in diag["managers_in_daily_control"]
    assert "Ilya Bochkov" in diag["managers_in_groups"]
    assert any(item.get("reason") == "manager_outside_allowlist" for item in diag["managers_skipped_with_reason"])


def test_weekly_manager_runtime_defaults_to_deepseek_v4_when_models_missing() -> None:
    cfg = DealAnalyzerConfig(**{**_cfg().__dict__, "ollama_model": "", "ollama_fallback_model": ""})
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime={},
        main_model_override=None,
        fallback_model_override=None,
    )
    assert runtime["main"]["model"] == "deepseek-v4-pro:cloud"
    assert runtime["fallback"]["model"] == "deepseek-v4-flash:cloud"


def test_weekly_manager_runtime_cli_override_has_priority() -> None:
    runtime = _runtime_from_config(
        cfg=_cfg(),
        llm_runtime={},
        main_model_override="override-main",
        fallback_model_override="override-fallback",
    )
    assert runtime["main"]["model"] == "override-main"
    assert runtime["fallback"]["model"] == "override-fallback"


def test_base_mix_sorted_by_frequency() -> None:
    assert aggregate_mix(["Inglemash-2026", "tilda", "Inglemash-2026"]) == "Inglemash-2026 - 2; tilda - 1"


def test_product_mix_sorted_and_aggregated() -> None:
    assert aggregate_mix(["info - 2", "info - 1", "link"]) == "info - 3; link - 1"


def test_weekly_manager_sales_manager_recommendations_do_not_push_cold_calls() -> None:
    sanitized = _sanitize_role_scope_phrase(
        text="Делаем массовый обзвон и 20 звонков по базе для поиска ЛПР.",
        manager_name="Илья Бочков",
        manager_role_profile="менеджер по продажам",
    )
    assert "массовый обзвон" not in sanitized.lower()
    assert "теплая/текущая воронка" in sanitized.lower()


def test_existing_row_skip_when_count_same() -> None:
    existing = [["2026-04-27", "2026-05-03", "Ilya Bochkov", "manager", "5"]]
    plan = plan_weekly_manager_write(
        payload_rows=[_payload_row(deals_count=5)],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_skipped_existing"]) == 1
    assert len(plan["rows_to_update"]) == 0


def test_existing_row_update_when_count_grows() -> None:
    existing = [["2026-04-27", "2026-05-03", "Ilya Bochkov", "manager", "5"]]
    plan = plan_weekly_manager_write(
        payload_rows=[_payload_row(deals_count=6)],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_to_update"]) == 1
    assert len(plan["rows_to_insert"]) == 0


def test_append_first_empty_row_after_existing_array() -> None:
    existing = [
        ["2026-04-20", "2026-04-26", "Ilya Bochkov", "manager", "5"],
        ["2026-04-20", "2026-04-26", "Rustam Khomidov", "telemarketer", "4"],
    ]
    plan = plan_weekly_manager_write(
        payload_rows=[_payload_row(manager_name="New Manager", week_start="2026-04-27", week_end="2026-05-03")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert plan["first_empty_row_after_existing_array"] == 4
    assert plan["rows_to_insert"][0]["row_number"] == 4


def test_no_structural_insert_in_normal_path() -> None:
    existing = [["2026-04-20", "2026-04-26", "Ilya Bochkov", "manager", "5"]]
    plan = plan_weekly_manager_write(
        payload_rows=[_payload_row(week_start="2026-04-27", week_end="2026-05-03", manager_name="Rustam Khomidov")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert plan["structural_changes_required"] is False
    assert plan["planned_structural_operations"] == []


def test_structural_insert_required_for_middle_insertion() -> None:
    existing = [["2026-05-04", "2026-05-10", "Ilya Bochkov", "manager", "5"]]
    plan = plan_weekly_manager_write(
        payload_rows=[_payload_row(week_start="2026-04-27", week_end="2026-05-03", manager_name="Rustam Khomidov")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert plan["structural_changes_required"] is True
    assert len(plan["planned_structural_operations"]) == 1


def test_llm_invalid_json_then_fallback_success(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if "gemma" in model:
            return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 11, "repair_applied": False}
        return {
            "weekly_result": "Result",
            "improved": "Improved",
            "not_improved": "Not improved",
            "repeating_mistakes": "Repeat",
            "training_for_employee": "Should be overridden by plan source",
            "post_training_tasks": "Tasks",
            "manager_actions_next_week": "Actions",
            "expected_quantity_effect": "Quantity",
            "expected_quality_effect": "Quality",
            "manager_report_phrase": "Leader phrase",
            "employee_message": "Employee note",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 88},
    )
    rows, diag = analyze_weekly_groups(
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
    assert rows[0]["analysis_backend_used"].startswith("fallback")
    assert diag["llm_success_fallback"] >= 1
    assert rows[0]["training_source"] == "week_plan"
    assert rows[0]["training_for_employee"] == "Plan-driven training topic"
    assert rows[0]["training_link"] == "https://train/link"
    assert rows[0]["post_training_tasks_link"] == "https://tasks/link"


def test_training_not_generated_when_plan_has_no_training(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "weekly_result": "Result",
            "improved": "Improved",
            "not_improved": "Not improved",
            "repeating_mistakes": "Repeat",
            "training_for_employee": "must be dropped",
            "post_training_tasks": "must be dropped",
            "manager_actions_next_week": "Actions",
            "expected_quantity_effect": "Quantity",
            "expected_quality_effect": "Quality",
            "manager_report_phrase": "Leader phrase",
            "employee_message": "Employee note",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 88},
    )
    group = _sample_group()
    group.plan_training_topics = []
    group.plan_training_rows_found_count = 0
    group.plan_training_rows_used_count = 0
    group.plan_training_rows_used = []
    group.plan_training_links = []
    group.plan_post_training_task_links = []
    rows, diag = analyze_weekly_groups(
        groups=[group],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    assert rows[0]["training_source"] == "not_planned"
    assert rows[0]["training_for_employee"] == ""
    assert rows[0]["training_link"] == ""
    assert rows[0]["post_training_tasks_link"] == ""
    assert diag["training_missing_but_generated_count"] == 0


def test_training_rows_aggregated_from_week_plan(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "weekly_result": "Result",
            "improved": "Improved",
            "not_improved": "Not improved",
            "repeating_mistakes": "Repeat",
            "training_for_employee": "should be replaced from plan",
            "post_training_tasks": "should be replaced from plan",
            "manager_actions_next_week": "Actions",
            "expected_quantity_effect": "Quantity",
            "expected_quality_effect": "Quality",
            "manager_report_phrase": "Leader phrase",
            "employee_message": "Employee note",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 88},
    )

    group = _sample_group()
    group.plan_training_rows_found_count = 4
    group.plan_training_rows_used_count = 4
    group.plan_training_topics = [
        "Глубинное интервью и вопросы почему",
        "Выявление ЛПР и обход секретаря",
        "Замена отправлю материалы на назначение демо",
        "Итоговый разбор по чек-листу ЛПР/Боли/Следующий шаг",
    ]
    group.plan_training_rows_used = [
        {"task_to_assign": "Провести 10 звонков по новой технике"},
        {"task_to_assign": "Зафиксировать следующий шаг в каждом звонке"},
        {"task_to_assign": "Отправить 3 записи на проверку"},
        {"task_to_assign": "Подтвердить ЛПР минимум в 5 кейсах"},
    ]
    group.plan_training_links = ["https://train/1", "https://train/2", "https://train/1"]
    group.plan_post_training_task_links = ["https://task/1", "https://task/2"]

    rows, diag = analyze_weekly_groups(
        groups=[group],
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
    assert row["training_source"] == "week_plan"
    assert "1) Глубинное интервью и вопросы почему" in row["training_for_employee"]
    assert "4) Итоговый разбор по чек-листу ЛПР/Боли/Следующий шаг" in row["training_for_employee"]
    assert "1) Провести 10 звонков по новой технике" in row["post_training_tasks"]
    assert "4) Подтвердить ЛПР минимум в 5 кейсах" in row["post_training_tasks"]
    assert row["training_link"] == "https://train/1\nhttps://train/2"
    assert row["post_training_tasks_link"] == "https://task/1\nhttps://task/2"
    assert diag["training_missing_but_generated_count"] == 0


def test_analyzed_sample_not_rendered_as_completed_deals(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "weekly_result": "Менеджер провел 15 сделок за неделю.",
            "improved": "Сделал 15 сделок и закрепил результат.",
            "not_improved": "0 звонков в РОКС.",
            "repeating_mistakes": "Повтор по фиксации следующего шага.",
            "training_for_employee": "",
            "post_training_tasks": "",
            "manager_actions_next_week": "Провел 15 сделок и держит темп.",
            "expected_quantity_effect": "Плюс 2 сделки.",
            "expected_quality_effect": "Стабилизация структуры звонка.",
            "manager_report_phrase": "Провел 15 сделок.",
            "employee_message": "Продолжаем тем же курсом.",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )

    group = _sample_group()
    group.deals_count = 15
    group.analyzed_deals_count = 15
    rows, _diag = analyze_weekly_groups(
        groups=[group],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_extracted", "manager_metrics": {"Ilya Bochkov": {"weekly_fact": {"calls_fact": None}}}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    row = rows[0]
    assert "провел 15 сделок" not in row["weekly_result"].lower()
    assert "сделал 15 сделок" not in row["improved"].lower()
    assert "в разбор попало 15 сделок" in row["weekly_result"].lower() or "в разбор попало 15 сделок" in row["improved"].lower()
    assert "0 звонков" not in row["not_improved"].lower()
    assert "факт по РОКС не подтянулся".lower() in row["not_improved"].lower()


def test_roks_facts_exposed_in_row(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "weekly_result": "Итог по неделе.",
            "improved": "Улучшилось.",
            "not_improved": "Нужно дожать следующий шаг.",
            "repeating_mistakes": "Откладывание фиксации шага.",
            "training_for_employee": "",
            "post_training_tasks": "",
            "manager_actions_next_week": "Разобрать 2 кейса.",
            "expected_quantity_effect": "Рост дозвонов.",
            "expected_quality_effect": "Лучше структура звонка.",
            "manager_report_phrase": "Идем по плану.",
            "employee_message": "Фиксируй следующий шаг в каждом диалоге.",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )
    rows, _diag = analyze_weekly_groups(
        groups=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={
            "status": "sheets_found_metrics_extracted",
            "manager_metrics": {
                "Ilya Bochkov": {
                    "weekly_fact": {
                        "calls_fact": 131,
                        "lpr_fact": 31,
                        "interest_fact": 5,
                        "demo_fact": 2,
                        "test_fact": 1,
                        "invoice_count_fact": 1,
                        "payment_count_fact": 1,
                        "roks_sheet_used": "РОКС ОАП-апрель 2026",
                        "week_index_used": 2,
                        "week_label_used": "2 НЕДЕЛЯ",
                    }
                }
            },
        },
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    row = rows[0]
    assert row["roks_calls_fact"] == 131
    assert row["roks_lpr_fact"] == 31
    assert row["roks_interest_fact"] == 5


def test_role_based_roks_interpretation_fields_exposed(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "weekly_result": "Результат по неделе.",
            "improved": "Улучшилось.",
            "not_improved": "Нужно дожать следующий шаг.",
            "repeating_mistakes": "Повтор фиксации следующего шага.",
            "training_for_employee": "",
            "post_training_tasks": "",
            "manager_actions_next_week": "Разобрать 2 кейса.",
            "expected_quantity_effect": "Рост дозвонов.",
            "expected_quality_effect": "Лучше структура звонка.",
            "manager_report_phrase": "Идем по плану.",
            "employee_message": "Фиксируй следующий шаг в каждом диалоге.",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )

    group = _sample_group()
    group.manager_name = "Rustam Khomidov"
    group.manager_role_profile = "telemarketer"
    rows, _diag = analyze_weekly_groups(
        groups=[group],
        cfg=_cfg(),
        roks_snapshot={
            "status": "sheets_found_metrics_extracted",
            "manager_metrics": {
                "Rustam Khomidov": {
                    "weekly_fact": {
                        "calls_fact": 131,
                        "lpr_fact": 31,
                        "interest_fact": 24,
                        "demo_fact": 0,
                        "test_fact": 0,
                        "invoice_count_fact": 0,
                        "payment_count_fact": 0,
                        "roks_sheet_used": "РОКС ОАП-апрель 2026",
                        "week_index_used": 2,
                        "week_label_used": "2 НЕДЕЛЯ",
                    }
                }
            },
        },
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )
    row = rows[0]
    assert row["source_generated_interest"] == 24
    assert row["conducted_demo"] == 0
    assert row["downstream_metrics_applicable"] is False
    assert row["routed_meetings_possible"] is True


def test_demo_phrase_is_not_rendered_as_self_assigned(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return {
            "weekly_result": "Бочков назначил 18 демо за неделю.",
            "improved": "Назначил 18 демо и держит темп.",
            "not_improved": "Слабая фиксация следующего шага.",
            "repeating_mistakes": "Повторяется перенос фиксации.",
            "training_for_employee": "",
            "post_training_tasks": "",
            "manager_actions_next_week": "Назначил 18 демо и продолжает.",
            "expected_quantity_effect": "Стабильный поток демо.",
            "expected_quality_effect": "Лучше структура звонка.",
            "manager_report_phrase": "Назначил 18 демо.",
            "employee_message": "Закрепить фиксацию шага.",
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 64},
    )
    row = analyze_weekly_groups(
        groups=[_sample_group()],
        cfg=_cfg(),
        roks_snapshot={"status": "sheets_found_metrics_unparsed", "manager_metrics": {}},
        llm_runtime={},
        logger=None,
        source_run_id="run1",
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="deepseek-v3.1:671b-cloud",
        llm_max_attempts=6,
    )[0][0]
    assert "назначил 18 демо" not in row["weekly_result"].lower()
    assert "провел 18 демо" in row["weekly_result"].lower()


def test_llm_full_failure_goes_to_quarantine_with_preview(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = model, base_url, timeout_seconds, messages
        return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 11, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.weekly_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.weekly_manager_summary.weekly_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 88},
    )
    rows, diag = analyze_weekly_groups(
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
    assert rows[0]["analysis_backend_used"] == "quarantined_llm_failed"
    assert diag["quarantined_count"] == 1
    assert diag["quarantined_rows"][0]["raw_response_preview"]


def test_writer_dry_run_does_not_write(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/weekly_manager_summary") / f"run_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "weekly_manager_payload.json").write_text(
        json.dumps({"rows": [_payload_row()]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    class FakeClient:
        calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:U30" in rng:
                return [_target_headers()]
            if "A1:U1" in rng:
                return [_target_headers()]
            return []

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            FakeClient.calls += 1

    monkeypatch.setattr("src.deal_analyzer.weekly_manager_summary.sheets_writer.GoogleSheetsApiClient", FakeClient)
    status = write_weekly_manager_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="Weekly manager summary",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["mode"] == "dry_run"
    assert FakeClient.calls == 0
    assert status["rows_written"] == 0


def test_writer_payload_missing_returns_block_status() -> None:
    run_dir = Path("workspace/tmp_tests/weekly_manager_summary") / f"run_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    status = write_weekly_manager_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="Недельный свод менеджеров",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["write_allowed"] is False
    assert status["block_reason"] == "payload_missing"
    assert "Weekly payload not found:" in status["error"]
    assert status["run_dir"] == str(run_dir)
    assert status["expected_payload_path"].endswith("weekly_manager_payload.json")


def test_cli_accepts_daily_sheet_alias(monkeypatch) -> None:
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
            "--target-sheet",
            "Недельный свод менеджеров",
        ],
    )
    args = weekly_manager_cli._parse_args()
    assert args.daily_sheet == "Дневной контроль"
