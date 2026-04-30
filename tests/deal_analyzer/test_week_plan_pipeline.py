from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.week_plan.cli import (
    _build_bootstrap_rows,
    _compute_manager_week_coverage,
    _expand_missing_manager_week_rows,
    _resolve_signal_and_plan_periods,
)
from src.deal_analyzer.week_plan.models import WeekPlanSignalGroup
from src.deal_analyzer.week_plan.plan_analyzer import (
    _build_group_context,
    _runtime_from_config,
    _validate_item,
    analyze_week_plan_groups,
)
from src.deal_analyzer.week_plan.sheets_writer import _apply_dropdown_mapping_to_rows, plan_week_plan_write, write_week_plan_rows
from src.deal_analyzer.week_plan.source_reader import WEEK_PLAN_SOURCE_ALIASES, WEEK_PLAN_TARGET_ALIASES
from src.deal_analyzer.week_plan.validation import (
    evaluate_writer_preflight,
    lint_week_plan_text_rows,
    validate_week_plan_payload_rows,
)
from src.deal_analyzer.week_plan.weekly_signal_builder import aggregate_mix, group_daily_rows_into_week_signals
from src.deal_analyzer.daily_control.source_reader import map_headers


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


def _target_headers() -> list[str]:
    return [
        "План недели с",
        "План недели по",
        "Дата",
        "День",
        "Адресат",
        "Роль менеджера",
        "Тип активности",
        "Приоритет",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
        "Ожидаемый эффект - количество",
        "Ожидаемый эффект - качество",
        "Статус",
    ]


def _payload_row(**overrides):
    row = {
        "plan_week_start": "2026-04-27",
        "plan_week_end": "2026-05-03",
        "plan_date": "2026-04-27",
        "day_label": "понедельник",
        "recipient": "Рустам Хомидов",
        "manager_role_profile": "телемаркетолог",
        "activity_type": "обучение",
        "priority": "high",
        "what_i_do": "Разбираю 2 звонка по фиксации следующего шага.",
        "task_to_assign": "Сделать 5 отработок с фиксацией даты следующего контакта.",
        "what_to_check": "Наличие даты и времени следующего шага в карточке.",
        "daily_meeting_thesis": "Не завершать разговор без управляемого шага.",
        "training_link": "",
        "post_training_task_link": "",
        "expected_quantity_effect": "Ожидаемо +1-2 управляемых шага в неделю.",
        "expected_quality_effect": "Стабильнее переход из контакта в следующий этап.",
        "status": "запланировано",
        "source_deals_count": 3,
        "source_calls_count": 3,
    }
    row.update(overrides)
    return row


def _sample_group() -> WeekPlanSignalGroup:
    return WeekPlanSignalGroup(
        period_start="2026-04-27",
        period_end="2026-05-03",
        plan_week_start="2026-04-27",
        plan_week_end="2026-05-03",
        manager_name="Рустам Хомидов",
        manager_role_profile="телемаркетолог",
        source_rows=[
            {
                "control_day_date": "2026-04-27",
                "main_pattern": "Теряет инициативу в финале.",
                "strong_sides": "Уверенный заход в контакт.",
                "growth_zones": "Фиксация следующего шага.",
                "what_to_fix": "Закрывать на конкретный слот.",
                "what_to_tell_employee": "Запланировали обучение по закрытию на дату.",
                "score_0_100": 62,
            }
        ],
        source_day_count=1,
        deals_count=3,
        calls_count=3,
        avg_score_0_100=62,
        deal_links=["https://example/1"],
        product_mix_week="инфо - 2",
        base_mix_week="Инглегмаш-2026 - 2",
        repeated_growth_zones=["Фиксация следующего шага"],
        repeated_strong_sides=["Уверенный заход"],
        repeated_fix_points=["Закрывать на слот"],
        repeated_messages=["Запланировали обучение"],
        training_signal_count=1,
        criticality_histogram={"средняя": 1},
    )


def test_discovery_mapping_aliases_present() -> None:
    source_map = map_headers(_source_headers(), WEEK_PLAN_SOURCE_ALIASES).mapped
    target_map = map_headers(_target_headers(), WEEK_PLAN_TARGET_ALIASES).mapped
    assert "manager_name" in source_map
    assert "what_to_tell_employee" in source_map
    assert "recipient" in target_map
    assert "what_i_do" in target_map


def test_grouping_signals_from_daily_control() -> None:
    rows = [
        [
            "2026-04-27",
            "2026-05-03",
            "2026-04-27",
            "понедельник",
            "Рустам Хомидов",
            "телемаркетолог",
            "2",
            "2",
            "https://deal/1",
            "инфо",
            "Инглегмаш-2026",
            "k",
            "s",
            "g",
            "w",
            "",
            "f",
            "Запланировали обучение по закрытию.",
            "",
            "",
            "70",
            "средняя",
        ]
    ]
    groups, diag = group_daily_rows_into_week_signals(
        headers=_source_headers(),
        rows=rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        manager_allowlist=("Рустам Хомидов",),
    )
    assert diag["groups_count"] == 1
    assert groups[0].manager_name == "Рустам Хомидов"


def test_week_plan_context_allows_demo_above_interest_for_demo_executor_role() -> None:
    group = _sample_group()
    group.manager_name = "Ilya Bochkov"
    group.manager_role_profile = "manager"
    context = _build_group_context(
        group,
        {
            "status": "sheets_found_metrics_extracted",
            "manager_metrics": {
                "Ilya Bochkov": {
                    "weekly_fact": {
                        "interest_fact": 10,
                        "demo_fact": 18,
                        "test_fact": 5,
                        "invoice_count_fact": 2,
                        "payment_count_fact": 1,
                    }
                }
            },
        },
        compact=False,
    )
    interpretation = context.get("roks_metric_interpretation", {})
    assert interpretation.get("source_generated_interest") == 10
    assert interpretation.get("conducted_demo") == 18
    assert interpretation.get("downstream_metrics_applicable") is True


def test_signals_count_not_zero_for_nonempty_problem_source() -> None:
    rows = [
        [
            "2026-04-27",
            "2026-05-03",
            "2026-04-27",
            "понедельник",
            "Рустам Хомидов",
            "телемаркетолог",
            "3",
            "3",
            "https://deal/1",
            "инфо",
            "Инглегмаш-2026",
            "Ключевой вывод",
            "Сильные стороны",
            "Выявление потребностей",
            "Почему важно",
            "",
            "Фиксация следующего шага",
            "Разобрать фиксацию следующего шага",
            "",
            "",
            "64",
            "средняя",
        ]
    ]
    _groups, diag = group_daily_rows_into_week_signals(
        headers=_source_headers(),
        rows=rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        manager_allowlist=("Рустам Хомидов",),
    )
    assert diag["source_rows_total"] == 1
    assert diag["signals_count"] > 0


def test_manager_coverage_debug_fields_in_signal_builder() -> None:
    rows = [
        [
            "2026-04-27",
            "2026-05-03",
            "2026-04-27",
            "понедельник",
            "Рустам Хомидов",
            "телемаркетолог",
            "1",
            "1",
            "",
            "инфо",
            "Инглегмаш-2026",
            "k",
            "s",
            "g",
            "w",
            "",
            "f",
            "Запланировали обучение по выявлению потребности",
            "",
            "",
            "70",
            "средняя",
        ],
        [
            "2026-04-27",
            "2026-05-03",
            "2026-04-28",
            "вторник",
            "Илья Бочков",
            "менеджер",
            "1",
            "1",
            "",
            "инфо",
            "Инглегмаш-2026",
            "k",
            "s",
            "g",
            "w",
            "",
            "f",
            "Контроль по следующему шагу",
            "",
            "",
            "65",
            "средняя",
        ],
    ]
    _groups, diag = group_daily_rows_into_week_signals(
        headers=_source_headers(),
        rows=rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        manager_allowlist=("Рустам Хомидов",),
    )
    assert "Рустам Хомидов" in diag["managers_in_daily_control"]
    assert "Илья Бочков" in diag["managers_in_daily_control"]
    assert "Рустам Хомидов" in diag["managers_in_groups"]
    assert any(item.get("reason") == "manager_outside_allowlist" for item in diag["managers_skipped_with_reason"])


def test_week_plan_runtime_defaults_to_deepseek_v4_when_models_missing() -> None:
    cfg = DealAnalyzerConfig(**{**_cfg().__dict__, "ollama_model": "", "ollama_fallback_model": ""})
    runtime = _runtime_from_config(
        cfg=cfg,
        llm_runtime={},
        main_model_override=None,
        fallback_model_override=None,
    )
    assert runtime["main"]["model"] == "deepseek-v4-pro:cloud"
    assert runtime["fallback"]["model"] == "deepseek-v4-flash:cloud"


def test_week_plan_runtime_cli_override_has_priority() -> None:
    runtime = _runtime_from_config(
        cfg=_cfg(),
        llm_runtime={},
        main_model_override="override-main",
        fallback_model_override="override-fallback",
    )
    assert runtime["main"]["model"] == "override-main"
    assert runtime["fallback"]["model"] == "override-fallback"


def test_signal_and_plan_period_split_from_new_args() -> None:
    args = SimpleNamespace(
        signal_start="2026-04-20",
        signal_end="2026-04-26",
        plan_week_start="2026-04-27",
        plan_week_end="2026-05-03",
        period_start="",
        period_end="",
    )
    out = _resolve_signal_and_plan_periods(args)
    assert out["signal_start"].isoformat() == "2026-04-20"
    assert out["signal_end"].isoformat() == "2026-04-26"
    assert out["plan_week_start"].isoformat() == "2026-04-27"
    assert out["plan_week_end"].isoformat() == "2026-05-03"
    assert out["period_warnings"] == []


def test_legacy_period_back_compat_warning() -> None:
    args = SimpleNamespace(
        signal_start="",
        signal_end="",
        plan_week_start="",
        plan_week_end="",
        period_start="2026-04-27",
        period_end="2026-05-03",
    )
    out = _resolve_signal_and_plan_periods(args)
    assert out["signal_start"].isoformat() == "2026-04-27"
    assert out["plan_week_start"].isoformat() == "2026-04-27"
    assert "legacy_period_used_for_signal_and_plan" in out["period_warnings"]


def test_first_week_with_bootstrap_if_empty_prepares_rows() -> None:
    rows = _build_bootstrap_rows(
        managers=["Илья Бочков", "Рустам Хомидов"],
        role_by_manager={"Илья Бочков": "менеджер", "Рустам Хомидов": "телемаркетолог"},
        plan_week_start=date(2026, 3, 30),
        plan_week_end=date(2026, 4, 3),
        source_run_id="run_bootstrap",
    )
    assert len(rows) == 10
    assert any(str(item.get("recipient")) == "Илья Бочков" for item in rows)
    assert any(str(item.get("recipient")) == "Рустам Хомидов" for item in rows)
    assert all(str(item.get("plan_week_start")) == "2026-03-30" for item in rows)
    assert all(str(item.get("plan_week_end")) == "2026-04-03" for item in rows)


def test_first_week_without_bootstrap_if_empty_blocks_on_rows_empty() -> None:
    preflight = evaluate_writer_preflight(
        rows=[],
        strict_preflight=True,
        conflicts_count=0,
        allow_partial_write=True,
        quarantine_unrepaired=True,
    )
    assert preflight["passed"] is False
    assert preflight["block_reason"] == "rows_empty"
    assert any(str(item.get("rule")) == "rows_empty" for item in preflight["failed_rules"])


def test_bootstrap_rows_do_not_claim_historical_call_findings() -> None:
    rows = _build_bootstrap_rows(
        managers=["Рустам Хомидов"],
        role_by_manager={},
        plan_week_start=date(2026, 3, 30),
        plan_week_end=date(2026, 4, 3),
        source_run_id="run_bootstrap",
    )
    forbidden = ("по звонкам увидели", "по дневному контролю выявлено")
    fields = ("what_i_do", "task_to_assign", "what_to_check", "daily_meeting_thesis")
    for row in rows:
        for field in fields:
            value = str(row.get(field, "")).lower()
            assert all(marker not in value for marker in forbidden)


def test_dropdown_mapping_and_quote_normalization() -> None:
    rows = [
        _payload_row(
            priority="high",
            status="planned",
            day_label="Понедельник",
            what_i_do='Разбор «сложного» кейса и “фиксации шага”',
            recipient="Рустам Хомидов",
            activity_type="обучение",
        )
    ]
    mapped_rows, quarantined, diag = _apply_dropdown_mapping_to_rows(
        rows=rows,
        mapped_indexes={
            "day_label": 3,
            "recipient": 4,
            "activity_type": 6,
            "priority": 7,
            "status": 16,
        },
        dropdown_rules_by_index={
            3: {"allowed_values": ["понедельник"]},
            4: {"allowed_values": ["Рустам Хомидов"]},
            6: {"allowed_values": ["обучение"]},
            7: {"allowed_values": ["высокий", "средний", "низкий"]},
            16: {"allowed_values": ["запланировано", "в работе", "выполнено"]},
        },
    )
    assert len(quarantined) == 0
    assert len(mapped_rows) == 1
    assert mapped_rows[0]["priority"] == "высокий"
    assert mapped_rows[0]["status"] == "запланировано"
    assert "«" in rows[0]["what_i_do"]
    assert diag["dropdown_mapped_count"] >= 2


def test_dropdown_unmapped_value_goes_to_quarantine() -> None:
    rows = [_payload_row(priority="super-high", recipient="Рустам Хомидов", activity_type="обучение")]
    mapped_rows, quarantined, diag = _apply_dropdown_mapping_to_rows(
        rows=rows,
        mapped_indexes={"recipient": 4, "activity_type": 6, "priority": 7},
        dropdown_rules_by_index={
            4: {"allowed_values": ["Рустам Хомидов"]},
            6: {"allowed_values": ["обучение"]},
            7: {"allowed_values": ["высокий", "средний", "низкий"]},
        },
    )
    assert mapped_rows == []
    assert len(quarantined) == 1
    assert quarantined[0]["reason"] == "dropdown_value_not_allowed"
    assert diag["dropdown_unmapped_count"] == 1


def test_activity_type_dropdown_mapping_for_personal_review() -> None:
    rows = [_payload_row(activity_type="личный разбор", recipient="Рустам Хомидов", priority="high", status="запланировано")]
    mapped_rows, quarantined, diag = _apply_dropdown_mapping_to_rows(
        rows=rows,
        mapped_indexes={"recipient": 4, "activity_type": 6, "priority": 7, "status": 16},
        dropdown_rules_by_index={
            4: {"allowed_values": ["Рустам Хомидов"]},
            6: {"allowed_values": ["операционная", "контроль", "обучение", "развитие", "стратегическая"]},
            7: {"allowed_values": ["высокий", "средний", "низкий"]},
            16: {"allowed_values": ["запланировано", "в работе", "выполнено"]},
        },
    )
    assert len(quarantined) == 0
    assert len(mapped_rows) == 1
    assert mapped_rows[0]["activity_type"] in {"обучение", "развитие", "операционная", "контроль"}
    assert diag["dropdown_unmapped_count"] == 0


def test_activity_type_dropdown_mapping_daily_and_strategy_aliases() -> None:
    rows = [
        _payload_row(activity_type="дейлик", recipient="Рустам Хомидов"),
        _payload_row(activity_type="стратегия", recipient="Рустам Хомидов", plan_date="2026-04-28", day_label="вторник"),
    ]
    mapped_rows, quarantined, diag = _apply_dropdown_mapping_to_rows(
        rows=rows,
        mapped_indexes={"recipient": 4, "activity_type": 6},
        dropdown_rules_by_index={
            4: {"allowed_values": ["Рустам Хомидов"]},
            6: {"allowed_values": ["операционная", "контроль", "обучение", "развитие", "стратегическая"]},
        },
    )
    assert len(quarantined) == 0
    assert len(mapped_rows) == 2
    assert mapped_rows[0]["activity_type"] == "операционная"
    assert mapped_rows[1]["activity_type"] == "стратегическая"
    assert diag["dropdown_unmapped_count"] == 0


def test_week_plan_lint_allows_urls_and_known_business_tokens() -> None:
    rows = [
        _payload_row(
            what_i_do="Разбор сделок https://officeistockinfo.amocrm.ru/leads/detail/32063140",
            expected_quantity_effect="Рост конверсии в demo_done и больше LPR в неделю",
        )
    ]
    lint = lint_week_plan_text_rows(rows)
    assert lint["foreign_language_count"] == 0


def test_week_plan_lint_allows_metric_terms_and_latex_arrow_marker() -> None:
    rows = [
        _payload_row(
            task_to_assign="Заменить формулировку на шаблон «Факт $\\rightarrow$ Польза».",
            what_i_do="Сверяю показатели LPR и Interest за неделю.",
            what_to_check="Проверяю динамику avg_score относительно базовых 65.",
        )
    ]
    lint = lint_week_plan_text_rows(rows)
    assert lint["foreign_language_count"] == 0


def test_week_plan_lint_allows_tilda_product_token() -> None:
    rows = [
        _payload_row(
            what_i_do="Проверяю сделки из базы tilda на предмет корректной квалификации ЛПР.",
        )
    ]
    lint = lint_week_plan_text_rows(rows)
    assert lint["foreign_language_count"] == 0


def test_week_plan_lint_allows_email_token() -> None:
    rows = [
        _payload_row(
            what_i_do="Тренинг по переводу клиента с e-mail на живой диалог.",
        )
    ]
    lint = lint_week_plan_text_rows(rows)
    assert lint["foreign_language_count"] == 0


def test_week_plan_lint_allows_smart_token() -> None:
    rows = [
        _payload_row(
            task_to_assign="Для каждой сделки зафиксируй конкретный результат по SMART и дату следующего шага.",
        )
    ]
    lint = lint_week_plan_text_rows(rows)
    assert lint["foreign_language_count"] == 0


def test_priority_mapping_and_default_status_in_item_validation() -> None:
    ok, errors, normalized = _validate_item(
        {
            "date": "2026-04-27",
            "day": "понедельник",
            "recipient": "Рустам Хомидов",
            "activity_type": "обучение",
            "priority": "высокий",
            "what_i_do": "Разбор звонка по фиксации следующего шага.",
            "task_to_assign": "Сделать 5 отработок.",
            "what_to_check": "Наличие следующего шага в карточке.",
            "daily_meeting_thesis": "Не завершать без управляемого шага.",
            "training_link": "",
            "post_training_task_link": "",
            "expected_quantity_effect": "Больше управляемых шагов.",
            "expected_quality_effect": "Стабильный переход в следующий этап.",
            "status": "",
        },
        default_recipient="Рустам Хомидов",
    )
    assert ok is True
    assert errors == []
    assert normalized["priority"] == "high"
    assert normalized["status"] == "запланировано"


def test_training_signal_detection_from_phrase() -> None:
    rows = [
        [
            "2026-04-27",
            "2026-05-03",
            "2026-04-27",
            "понедельник",
            "Рустам Хомидов",
            "телемаркетолог",
            "1",
            "1",
            "",
            "инфо",
            "Инглегмаш-2026",
            "k",
            "s",
            "g",
            "w",
            "",
            "f",
            "Запланировали обучение по выявлению потребности",
            "",
            "",
            "70",
            "средняя",
        ]
    ]
    groups, _ = group_daily_rows_into_week_signals(
        headers=_source_headers(),
        rows=rows,
        period_start=date(2026, 4, 27),
        period_end=date(2026, 5, 3),
        manager_allowlist=("Рустам Хомидов",),
    )
    assert groups[0].training_signal_count > 0


def test_llm_invalid_json_then_fallback_success(monkeypatch) -> None:
    def _fake_call_llm(*, model, base_url, timeout_seconds, messages):
        _ = base_url, timeout_seconds, messages
        if "gemma" in model:
            return None, {"ok": False, "error": "not valid JSON object", "elapsed_ms": 11, "repair_applied": False}
        return {
            "items": [
                {
                    "date": "2026-04-27",
                    "day": "понедельник",
                    "recipient": "Рустам Хомидов",
                    "activity_type": "обучение",
                    "priority": "high",
                    "what_i_do": "Разбираю кейс по закрытию на шаг.",
                    "task_to_assign": "Сделать 5 фиксаций следующего шага.",
                    "what_to_check": "Есть ли дата и время следующего контакта.",
                    "daily_meeting_thesis": "Не завершать без управляемого шага.",
                    "training_link": "",
                    "post_training_task_link": "",
                    "expected_quantity_effect": "Ожидаемо +1-2 шага.",
                    "expected_quality_effect": "Стабильнее переход в следующий этап.",
                    "status": "запланировано",
                }
            ]
        }, {"ok": True, "error": "", "elapsed_ms": 10, "repair_applied": False}

    monkeypatch.setattr("src.deal_analyzer.week_plan.plan_analyzer._call_llm", _fake_call_llm)
    monkeypatch.setattr(
        "src.deal_analyzer.week_plan.plan_analyzer._preflight_model",
        lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 88},
    )

    rows, diag = analyze_week_plan_groups(
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
    assert len(rows) >= 1
    assert rows[0]["analysis_backend_used"].startswith("fallback")
    assert diag["llm_success_fallback"] >= 1


def test_no_invented_links_validation() -> None:
    result = validate_week_plan_payload_rows([_payload_row(training_link="not_a_link")])
    assert result["invalid_link_count"] == 1


def test_empty_training_links_allowed_by_validation() -> None:
    result = validate_week_plan_payload_rows([_payload_row(training_link="", post_training_task_link="")])
    assert result["invalid_link_count"] == 0


def test_values_only_writer_plan_no_structural_insert_normal() -> None:
    existing = [["2026-04-20", "2026-04-26", "2026-04-20", "понедельник", "Илья Бочков", "менеджер", "контроль", "high", "x", "y", "z", "t", "", "", "q", "w", "запланировано"]]
    plan = plan_week_plan_write(
        payload_rows=[_payload_row(plan_week_start="2026-04-27", plan_week_end="2026-05-03", recipient="Рустам Хомидов")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert plan["structural_changes_required"] is False
    assert plan["planned_structural_operations"] == []


def test_skip_existing_by_idempotency_key() -> None:
    existing = [[
        "2026-04-27",
        "2026-05-03",
        "2026-04-27",
        "понедельник",
        "Рустам Хомидов",
        "телемаркетолог",
        "обучение",
        "high",
        "Разбираю 2 звонка по фиксации следующего шага.",
        "Сделать 5 отработок с фиксацией даты следующего контакта.",
        "Наличие даты и времени следующего шага в карточке.",
        "Не завершать разговор без управляемого шага.",
        "",
        "",
        "Ожидаемо +1-2 управляемых шага в неделю.",
        "Стабильнее переход из контакта в следующий этап.",
        "запланировано",
    ]]
    plan = plan_week_plan_write(
        payload_rows=[_payload_row()],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_skipped_existing"]) == 1
    assert len(plan["rows_to_update"]) == 0


def test_update_non_final_existing_row() -> None:
    existing = [[
        "2026-04-27",
        "2026-05-03",
        "2026-04-27",
        "понедельник",
        "Рустам Хомидов",
        "телемаркетолог",
        "обучение",
        "high",
        "Старый текст",
        "Старая задача",
        "Старая проверка",
        "Старый тезис",
        "",
        "",
        "Старый эффект",
        "Старое качество",
        "запланировано",
    ]]
    plan = plan_week_plan_write(
        payload_rows=[_payload_row(what_i_do="Новый текст")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_to_update"]) == 1


def test_do_not_update_completed_row() -> None:
    existing = [[
        "2026-04-27",
        "2026-05-03",
        "2026-04-27",
        "понедельник",
        "Рустам Хомидов",
        "телемаркетолог",
        "обучение",
        "high",
        "Старый текст",
        "Старая задача",
        "Старая проверка",
        "Старый тезис",
        "",
        "",
        "Старый эффект",
        "Старое качество",
        "выполнено",
    ]]
    plan = plan_week_plan_write(
        payload_rows=[_payload_row(what_i_do="Новый текст")],
        headers=_target_headers(),
        existing_rows=existing,
        data_start_row=2,
    )
    assert len(plan["rows_to_update"]) == 0
    assert len(plan["rows_skipped_existing"]) == 1


def test_dry_run_does_not_change_google_sheets(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/week_plan") / f"run_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "week_plan_payload.json").write_text(
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

    monkeypatch.setattr("src.deal_analyzer.week_plan.sheets_writer.GoogleSheetsApiClient", FakeClient)
    status = write_week_plan_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="План недели",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["mode"] == "dry_run"
    assert FakeClient.calls == 0
    assert status["rows_written"] == 0


def test_writer_preflight_blocks_empty_payload_not_successful_zero_write() -> None:
    preflight = evaluate_writer_preflight(
        rows=[],
        strict_preflight=True,
        conflicts_count=0,
        allow_partial_write=True,
        quarantine_unrepaired=True,
    )
    assert preflight["rows_for_write_count"] == 0
    assert preflight["passed"] is False
    assert preflight["block_reason"] == "rows_empty"


def test_manager_week_coverage_repair_expands_missing_workdays() -> None:
    rows = [
        _payload_row(recipient="Илья Бочков", plan_date="2026-04-13", day_label="понедельник"),
        _payload_row(recipient="Рустам Хомидов", plan_date="2026-04-13", day_label="понедельник"),
        _payload_row(recipient="Рустам Хомидов", plan_date="2026-04-14", day_label="вторник"),
        _payload_row(recipient="Рустам Хомидов", plan_date="2026-04-15", day_label="среда"),
        _payload_row(recipient="Рустам Хомидов", plan_date="2026-04-16", day_label="четверг"),
        _payload_row(recipient="Рустам Хомидов", plan_date="2026-04-17", day_label="пятница"),
    ]
    expected_workdays = ["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]
    before = _compute_manager_week_coverage(
        rows=rows,
        managers_in_scope=["Илья Бочков", "Рустам Хомидов"],
        expected_workdays=expected_workdays,
    )
    assert before["coverage_complete"] is False
    assert before["missing_dates_by_manager"]["Илья Бочков"] == ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]

    repaired_rows, added = _expand_missing_manager_week_rows(
        rows=rows,
        missing_dates_by_manager=before["missing_dates_by_manager"],
    )
    assert added == 4
    after = _compute_manager_week_coverage(
        rows=repaired_rows,
        managers_in_scope=["Илья Бочков", "Рустам Хомидов"],
        expected_workdays=expected_workdays,
    )
    assert after["coverage_complete"] is True
    assert len(after["rows_by_manager"]["Илья Бочков"]) == 5


def test_writer_blocks_when_manager_week_coverage_incomplete(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/week_plan") / f"run_cov_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "week_plan_payload.json").write_text(
        json.dumps(
            {
                "rows": [
                    _payload_row(
                        recipient="Илья Бочков",
                        plan_week_start="2026-04-13",
                        plan_week_end="2026-04-17",
                        plan_date="2026-04-13",
                        day_label="понедельник",
                    )
                ],
                "require_full_manager_week_coverage": True,
                "manager_week_coverage": {
                    "coverage_incomplete": True,
                    "managers_in_planning_scope": ["Илья Бочков"],
                    "expected_workdays": ["2026-04-13", "2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"],
                    "after_payload_validation": {
                        "missing_dates_by_manager": {
                            "Илья Бочков": ["2026-04-14", "2026-04-15", "2026-04-16", "2026-04-17"]
                        }
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [_target_headers()]
            if "A1:AZ1" in rng:
                return [_target_headers()]
            return []

        def build_service(self):
            class _Service:
                def spreadsheets(self):
                    class _Sheets:
                        def get(self, **kwargs):
                            _ = kwargs

                            class _Exec:
                                def execute(self):
                                    return {"sheets": []}

                            return _Exec()

                    return _Sheets()

            return _Service()

        def batch_update_values(self, spreadsheet_id: str, data):
            raise AssertionError("batch_update_values should not be called when coverage is incomplete")

    monkeypatch.setattr("src.deal_analyzer.week_plan.sheets_writer.GoogleSheetsApiClient", FakeClient)
    status = write_week_plan_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="План недели",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["write_allowed"] is False
    assert status["block_reason"] == "manager_week_coverage_incomplete_after_preflight"


def test_writer_compacts_planned_ranges_after_quarantine_filter(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/week_plan") / f"run_compact_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload_rows = [
        _payload_row(
            recipient="Илья Бочков",
            plan_week_start="2026-04-13",
            plan_week_end="2026-04-17",
            plan_date="2026-04-13",
            day_label="понедельник",
            activity_type="обучение",
        ),
        _payload_row(
            recipient="Рустам Хомидов",
            plan_week_start="2026-04-13",
            plan_week_end="2026-04-17",
            plan_date="2026-04-13",
            day_label="понедельник",
            activity_type="неизвестный тип",
        ),
        _payload_row(
            recipient="Илья Бочков",
            plan_week_start="2026-04-13",
            plan_week_end="2026-04-17",
            plan_date="2026-04-14",
            day_label="вторник",
            activity_type="обучение",
        ),
    ]
    (run_dir / "week_plan_payload.json").write_text(
        json.dumps(
            {
                "rows": payload_rows,
                "require_full_manager_week_coverage": False,
                "manager_week_coverage": {
                    "managers_in_planning_scope": ["Илья Бочков"],
                    "expected_workdays": ["2026-04-13", "2026-04-14"],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [_target_headers()]
            if "A1:AZ1" in rng:
                return [_target_headers()]
            return []

        def build_service(self):
            class _Service:
                def spreadsheets(self):
                    class _Sheets:
                        def get(self, **kwargs):
                            _ = kwargs

                            class _Exec:
                                def execute(self):
                                    return {
                                        "sheets": [
                                            {
                                                "data": [
                                                    {
                                                        "startColumn": 6,
                                                        "rowData": [
                                                            {
                                                                "values": [
                                                                    {
                                                                        "dataValidation": {
                                                                            "strict": True,
                                                                            "condition": {
                                                                                "type": "ONE_OF_LIST",
                                                                                "values": [
                                                                                    {"userEnteredValue": "операционная"},
                                                                                    {"userEnteredValue": "контроль"},
                                                                                    {"userEnteredValue": "обучение"},
                                                                                    {"userEnteredValue": "развитие"},
                                                                                    {"userEnteredValue": "стратегическая"},
                                                                                ],
                                                                            },
                                                                        }
                                                                    }
                                                                ]
                                                            }
                                                        ],
                                                    }
                                                ]
                                            }
                                        ]
                                    }

                            return _Exec()

                    return _Sheets()

            return _Service()

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            raise AssertionError("dry-run should not write")

    monkeypatch.setattr("src.deal_analyzer.week_plan.sheets_writer.GoogleSheetsApiClient", FakeClient)
    status = write_week_plan_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="План недели",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["rows_to_insert"] == 2
    assert status["rows_quarantined"] >= 1
    planned_ranges = status.get("planned_ranges", [])
    assert isinstance(planned_ranges, list) and len(planned_ranges) == 1
    assert planned_ranges[0].endswith("A2:Q3")


def test_plan_week_identity_inferred_when_a1_header_blank() -> None:
    headers = [
        "",
        "План недели по",
        "Дата",
        "День",
        "Адресат",
        "Тип активности",
        "Приоритет",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
        "Ожидаемый эффект - количество",
        "Ожидаемый эффект - качество",
        "Статус",
    ]
    rows = [
        _payload_row(
            plan_week_start="2026-04-13",
            plan_week_end="2026-04-17",
            plan_date=f"2026-04-{13 + idx:02d}",
            day_label=("понедельник", "вторник", "среда", "четверг", "пятница")[idx],
            recipient="Рустам Хомидов",
        )
        for idx in range(5)
    ]
    plan = plan_week_plan_write(
        payload_rows=rows,
        headers=headers,
        existing_rows=[],
        data_start_row=2,
    )
    assert plan["ok"] is True
    assert len(plan["rows_to_insert"]) == 5
    assert plan["rows_to_insert"][0]["row_number"] == 2
    assert plan["mapped_indexes"]["plan_week_start"] == 0
    assert plan.get("inferred_columns", {}).get("plan_week_start", {}).get("reason", "").startswith("blank_header_column_a")


def test_missing_identity_header_has_detailed_diagnostics() -> None:
    headers = [
        "Не та колонка",
        "План недели по",
        "Дата",
        "День",
        "Адресат",
        "Тип активности",
        "Приоритет",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
        "Ожидаемый эффект - количество",
        "Ожидаемый эффект - качество",
        "Статус",
    ]
    plan = plan_week_plan_write(
        payload_rows=[_payload_row()],
        headers=headers,
        existing_rows=[],
        data_start_row=2,
    )
    assert plan["ok"] is False
    assert plan["error"] == "missing_identity_columns"
    assert "plan_week_start" in plan["missing_identity_columns"]
    assert "actual_headers" in plan
    assert "mapped_columns" in plan
    assert plan["mapped_columns"]["plan_week_end"]["index"] == 1


def test_writer_dry_run_header_only_russian_sheet_plans_contiguous_range(monkeypatch) -> None:
    run_dir = Path("workspace/tmp_tests/week_plan") / f"run_header_only_{uuid.uuid4().hex[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload_rows = [
        _payload_row(
            plan_week_start="2026-04-13",
            plan_week_end="2026-04-17",
            plan_date=f"2026-04-{13 + idx:02d}",
            day_label=("понедельник", "вторник", "среда", "четверг", "пятница")[idx],
            recipient="Илья Бочков" if idx < 2 else "Рустам Хомидов",
            activity_type="обучение",
        )
        for idx in range(5)
    ] + [
        _payload_row(
            plan_week_start="2026-04-13",
            plan_week_end="2026-04-17",
            plan_date=f"2026-04-{13 + idx:02d}",
            day_label=("понедельник", "вторник", "среда", "четверг", "пятница")[idx],
            recipient="Рустам Хомидов",
            activity_type="контроль",
            what_i_do=f"Контроль {idx}",
        )
        for idx in range(5)
    ]
    (run_dir / "week_plan_payload.json").write_text(
        json.dumps({"rows": payload_rows, "require_full_manager_week_coverage": False}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    headers = [
        "План недели с",
        "План недели по",
        "Дата",
        "День",
        "Адресат",
        "Тип активности",
        "Приоритет",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
        "Ожидаемый эффект - количество",
        "Ожидаемый эффект - качество",
        "Статус",
    ]

    class FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [headers]
            if "A1:AZ1" in rng:
                return [headers]
            return []

        def build_service(self):
            class _Service:
                def spreadsheets(self):
                    class _Sheets:
                        def get(self, **kwargs):
                            _ = kwargs

                            class _Exec:
                                def execute(self):
                                    return {"sheets": []}

                            return _Exec()

                    return _Sheets()

            return _Service()

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            raise AssertionError("dry-run should not write")

    monkeypatch.setattr("src.deal_analyzer.week_plan.sheets_writer.GoogleSheetsApiClient", FakeClient)
    status = write_week_plan_rows(
        cfg=_cfg(),
        run_dir=run_dir,
        target_sheet_name="План недели",
        dry_run=True,
        strict_preflight=True,
        allow_partial_write=True,
        quarantine_unrepaired=True,
        logger=None,
    )
    assert status["error"] == ""
    assert status["rows_prepared"] == 10
    assert status["rows_to_insert"] == 10
    assert status["rows_quarantined"] == 0
    assert status["block_reason"] == "dry_run_mode"
    assert len(status["planned_ranges"]) == 1
    assert status["planned_ranges"][0].endswith("A2:P11")
