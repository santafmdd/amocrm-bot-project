from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from src.deal_analyzer.week_summary.aggregator import build_week_summary_groups
from src.deal_analyzer.weekly_manager_summary.week_grouper import group_daily_rows_by_week_manager
from src.deal_analyzer.weekly_shared.date_utils import week_bounds_monday_sunday, week_month_majority
from src.deal_analyzer.weekly_shared.pipeline_cli import _manager_rows_table, _plan_rows_table, _resolve_cycle_periods
from src.deal_analyzer.weekly_shared import roks_oap
from src.deal_analyzer.weekly_shared.roks_oap import resolve_weekly_roks_selection
from src.deal_analyzer.weekly_shared.role_policy import demo_quality_checklist, resolve_role_policy
from src.deal_analyzer.weekly_shared.validation import normalize_row_quotes, normalize_typographic_quotes


def test_week_bounds_monday_sunday() -> None:
    week_start, week_end = week_bounds_monday_sunday("2026-04-24")
    assert week_start == "2026-04-20"
    assert week_end == "2026-04-26"


def test_week_month_majority_on_month_boundary() -> None:
    majority = week_month_majority("2026-04-27", "2026-05-03")
    assert majority == (2026, 4)


def test_roks_selection_falls_back_to_available_month() -> None:
    selection = resolve_weekly_roks_selection(
        sheet_titles=["РОКС ОАП-апрель 2026", "РОКС ОАП-март 2026"],
        week_start="2026-04-27",
        week_end="2026-05-03",
    )
    assert selection["selected_current_month_sheet"] == "РОКС ОАП-апрель 2026"
    assert selection["selected_previous_month_sheet"] == "РОКС ОАП-март 2026"
    assert selection["selection_reason"] in {"majority_month", "majority_month_missing_fallback_to_available_month"}


def test_roks_weekly_fact_reads_dials_when_plan_empty() -> None:
    matrix: list[list[str]] = [
        ["Рустам Хомидов", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        ["", "Дозвоны", "", "", "", "", "", "131", "", "", "", "", "", ""],
        ["", "ЛПР", "", "", "", "", "", "31", "", "", "", "", "", ""],
        ["", "Есть интерес", "", "", "", "", "", "5", "", "", "", "", "", ""],
    ]
    parsed, warnings = roks_oap._extract_weekly_fact_metrics(  # noqa: SLF001
        matrix=matrix,
        manager_allowlist=("Рустам Хомидов",),
        week_index=2,
    )
    rustam = parsed["Рустам Хомидов"]
    assert rustam["calls_fact_value"] == 131
    assert rustam["lpr_fact_value"] == 31
    assert rustam["interest_fact_value"] == 5
    assert rustam["calls_fact_raw_cell"] == "131"
    assert warnings == [] or all("missing" not in str(item) for item in warnings)


def test_pipeline_period_split_resolution() -> None:
    out = _resolve_cycle_periods(
        SimpleNamespace(
            signal_start="2026-04-20",
            signal_end="2026-04-26",
            plan_week_start="2026-04-27",
            plan_week_end="2026-05-03",
            period_start="",
            period_end="",
        )
    )
    assert out["signal_start"].isoformat() == "2026-04-20"
    assert out["plan_week_start"].isoformat() == "2026-04-27"
    assert out["period_warnings"] == []


def test_quote_normalization_helpers() -> None:
    assert normalize_typographic_quotes("«тест»") == '"тест"'
    rows = normalize_row_quotes([{"weekly_result": "“Результат”", "other": "ok"}], fields=("weekly_result",))
    assert rows[0]["weekly_result"] == '"Результат"'
    assert rows[0]["other"] == "ok"


def test_integrated_in_memory_cycle_tables() -> None:
    plan_rows_payload = [
        {
            "plan_week_start": "2026-04-27",
            "plan_week_end": "2026-05-03",
            "plan_date": "2026-04-28",
            "day_label": "вторник",
            "recipient": "Илья Бочков",
            "activity_type": "обучение",
            "what_i_do": "Разобрать 2 кейса по фиксации следующего шага.",
            "status": "в работе",
            "training_link": "https://train/link",
            "post_training_task_link": "https://tasks/link",
            "task_to_assign": "Отработать 10 звонков.",
        }
    ]
    plan_headers, plan_rows = _plan_rows_table(plan_rows_payload)

    daily_headers = [
        "Неделя с",
        "Неделя по",
        "Дата контроля",
        "День",
        "Менеджер",
        "Роль менеджера",
        "Проанализировано сделок",
        "Количество звонков",
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
    daily_rows = [
        [
            "2026-04-27",
            "2026-05-03",
            "2026-04-27",
            "понедельник",
            "Илья Бочков",
            "менеджер",
            "3",
            "4",
            "l1",
            "инфо",
            "A",
            "k",
            "s",
            "g",
            "w",
            "",
            "f",
            "t",
            "",
            "",
            "68",
            "средняя",
        ]
    ]
    manager_groups, _diag, _plan_fact_rows = group_daily_rows_by_week_manager(
        headers=daily_headers,
        rows=daily_rows,
        period_start=date.fromisoformat("2026-04-27"),
        period_end=date.fromisoformat("2026-05-03"),
        manager_allowlist=("Илья Бочков",),
        plan_headers=plan_headers,
        plan_rows=plan_rows,
    )
    manager_payload = [
        {
            "week_start": "2026-04-27",
            "week_end": "2026-05-03",
            "manager_name": manager_groups[0].manager_name,
            "manager_role_profile": manager_groups[0].manager_role_profile,
            "deals_count": manager_groups[0].deals_count,
            "avg_score_0_100": manager_groups[0].avg_score_0_100,
            "weekly_result": "Итог",
            "improved": "Улучшилось",
            "not_improved": "Не улучшилось",
            "repeating_mistakes": "Повтор",
            "training_for_employee": "Обучение",
            "training_link": "https://train/link",
            "post_training_tasks": "Задачи",
            "post_training_tasks_link": "https://tasks/link",
            "manager_actions_next_week": "Действия",
            "expected_quantity_effect": "Количество",
            "expected_quality_effect": "Качество",
            "manager_report_phrase": "Фраза",
            "employee_message": "Сообщение",
        }
    ]
    manager_headers, manager_rows = _manager_rows_table(manager_payload)

    week_groups, week_diag, _plan_fact = build_week_summary_groups(
        manager_headers=manager_headers,
        manager_rows=manager_rows,
        plan_headers=plan_headers,
        plan_rows=plan_rows,
        period_start=date.fromisoformat("2026-04-27"),
        period_end=date.fromisoformat("2026-05-03"),
        daily_headers=daily_headers,
        daily_rows=daily_rows,
    )
    assert len(week_groups) == 1
    assert week_diag["groups_count"] == 1


def test_roks_interpretation_allows_demo_above_interest_for_bochkov() -> None:
    interpretation = roks_oap.build_manager_metric_interpretation(
        manager_name="Ilya Bochkov",
        manager_role_profile="manager",
        weekly_fact={
            "interest_fact": 10,
            "demo_fact": 18,
            "test_fact": 6,
            "invoice_count_fact": 3,
            "payment_count_fact": 2,
        },
    )
    assert interpretation["conducted_demo"] == 18
    assert interpretation["source_generated_interest"] == 10
    assert interpretation["downstream_metrics_applicable"] is True
    assert interpretation["routed_meetings_possible"] is True
    assert "demo_gt_interest_role_allowed" in interpretation["warnings"]


def test_roks_interpretation_allows_top_funnel_with_zero_downstream_for_khomidov() -> None:
    interpretation = roks_oap.build_manager_metric_interpretation(
        manager_name="Rustam Khomidov",
        manager_role_profile="telemarketer",
        weekly_fact={
            "interest_fact": 24,
            "demo_fact": 0,
            "test_fact": 0,
            "invoice_count_fact": 0,
            "payment_count_fact": 0,
        },
    )
    assert interpretation["source_generated_interest"] == 24
    assert interpretation["conducted_demo"] == 0
    assert interpretation["downstream_metrics_applicable"] is False
    assert interpretation["routed_meetings_possible"] is True
    assert "downstream_zero_role_allowed" in interpretation["warnings"]


def test_role_policy_config_override() -> None:
    policy = resolve_role_policy(
        manager_name="Илья Бочков",
        manager_role_profile="менеджер по продажам",
        role_policy_registry={
            "Илья Бочков": {
                "role": "sales_manager",
                "primary_funnel_scope": ["interest_to_demo", "demo_to_test"],
                "restricted_funnel_scope": ["cold_calling", "mass_lpr_discovery"],
                "max_upper_funnel_tasks_per_week": 0,
            }
        },
    )
    assert policy["role"] == "sales_manager"
    assert policy["allowed_primary_funnel_focus"] == ["interest_to_demo", "demo_to_test"]
    assert "cold_calling" in policy["restricted_upper_funnel"]
    assert policy["max_upper_funnel_tasks_per_week"] == 0


def test_demo_checklist_schema() -> None:
    policy = resolve_role_policy(
        manager_name="Илья Бочков",
        manager_role_profile="менеджер по продажам",
    )
    checklist = demo_quality_checklist(policy)
    assert isinstance(checklist, list)
    assert len(checklist) >= 6
    required = {
        "была ли выявлена задача клиента до показа",
        "было ли hands-on действие клиента",
        "показаны ли только релевантные функции",
        "был ли вопрос после каждого смыслового блока",
        "зафиксирован ли критерий успеха теста",
        "назначен ли следующий шаг",
    }
    assert required.issubset(set(checklist))
    assert policy.get("demo_methodology") == [
        "educational_demo",
        "guided_discovery",
        "client_hands_on",
        "soft_influence",
        "problem_based_demo",
        "next_step_commitment",
    ]
