from __future__ import annotations

import json

from src.deal_analyzer.employee_profiles.analyzer import apply_profile_to_row_fields
from src.deal_analyzer.employee_profiles.registry import (
    build_employee_profile_registry,
    resolve_employee_profile,
)
from src.deal_analyzer.training_materials.models import TrainingCandidate
from src.deal_analyzer.training_materials.training_analyzer import _build_messages
from src.deal_analyzer.week_plan.models import WeekPlanSignalGroup
from src.deal_analyzer.week_plan.plan_analyzer import _build_group_context as _build_week_plan_group_context
from src.deal_analyzer.week_summary.analyzer import _build_group_context as _build_week_summary_group_context
from src.deal_analyzer.week_summary.models import WeekSummaryGroup


def test_employee_profile_registry_defaults() -> None:
    registry = build_employee_profile_registry(None)
    rustam = resolve_employee_profile(
        manager_name="Рустам Хомидов",
        manager_role_profile="телемаркетолог",
        registry=registry,
    )
    ilya = resolve_employee_profile(
        manager_name="Илья Бочков",
        manager_role_profile="менеджер по продажам",
        registry=registry,
    )
    assert rustam.communication_style == "direct_accountability"
    assert ilya.communication_style == "expert_to_expert"


def test_rustam_direct_accountability_tone_without_insults() -> None:
    profile = resolve_employee_profile(
        manager_name="Рустам Хомидов",
        manager_role_profile="телемаркетолог",
        registry=build_employee_profile_registry(None),
    )
    row, _changes = apply_profile_to_row_fields(
        row={
            "control_day_date": "2026-04-29",
            "what_to_tell_employee": "Ты тупой, соберись и зафиксируй следующий шаг.",
        },
        profile=profile,
        fields=("what_to_tell_employee",),
        date_hint_field="control_day_date",
    )
    text = str(row.get("what_to_tell_employee") or "").lower()
    assert "туп" not in text
    assert "контроль срока" in text


def test_ilya_expert_to_expert_tone() -> None:
    profile = resolve_employee_profile(
        manager_name="Илья Бочков",
        manager_role_profile="менеджер по продажам",
        registry=build_employee_profile_registry(None),
    )
    row, _changes = apply_profile_to_row_fields(
        row={
            "week_end": "2026-04-17",
            "employee_message": "Проверь работу со сделками и обнови коммуникацию.",
        },
        profile=profile,
        fields=("employee_message",),
        date_hint_field="week_end",
    )
    text = str(row.get("employee_message") or "").lower()
    assert "коммерческий фокус" in text


def test_profiles_used_in_training_materials() -> None:
    candidate = TrainingCandidate(
        row_number=12,
        plan_week_start="2026-04-13",
        plan_week_end="2026-04-17",
        plan_date="2026-04-14",
        recipient="Илья Бочков",
        manager_role_profile="менеджер по продажам",
        activity_type="обучение",
        status="запланировано",
        what_i_do="Разобрать переход demo -> test",
        task_to_assign="Отработать 10 кейсов фиксации следующего шага",
        what_to_check="Есть дата теста и следующий контакт",
        daily_meeting_thesis="Фокус на дожиме warm-пайплайна",
        expected_quantity_effect="Рост переходов demo -> test",
        expected_quality_effect="Стабильный next step",
        training_link="",
        post_training_task_link="",
        topic_hash="h1",
        idempotency_key="k1",
    )
    messages = _build_messages(
        candidate=candidate,
        snippets=[],
        repair_mode=False,
        previous_error="",
        compact=False,
    )
    payload = json.loads(str(messages[1].get("content") or "{}"))
    profile = (
        payload.get("context", {}).get("employee_profile", {})
        if isinstance(payload.get("context"), dict)
        else {}
    )
    assert profile.get("communication_style") == "expert_to_expert"


def test_profiles_used_in_week_plan() -> None:
    group = WeekPlanSignalGroup(
        period_start="2026-04-06",
        period_end="2026-04-10",
        plan_week_start="2026-04-13",
        plan_week_end="2026-04-17",
        manager_name="Рустам Хомидов",
        manager_role_profile="телемаркетолог",
        source_rows=[
            {
                "control_day_date": "2026-04-10",
                "growth_zones": "фиксация следующего шага",
                "strong_sides": "уверенный вход в разговор",
            }
        ],
    )
    context = _build_week_plan_group_context(
        group,
        {"status": "ok", "manager_metrics": {}},
        compact=False,
    )
    profile = context.get("employee_profile", {}) if isinstance(context.get("employee_profile"), dict) else {}
    assert profile.get("communication_style") == "direct_accountability"


def test_profiles_used_in_weekly_summary() -> None:
    group = WeekSummaryGroup(
        period_start="2026-04-13",
        period_end="2026-04-17",
        week_start="2026-04-13",
        week_end="2026-04-17",
        source_manager_rows=[
            {
                "manager_name": "Илья Бочков",
                "manager_role_profile": "менеджер по продажам",
            }
        ],
    )
    context = _build_week_summary_group_context(
        group,
        {"status": "ok", "manager_metrics": {}},
        compact=False,
        client_priority_summary={},
    )
    profiles = context.get("employee_profiles", {}) if isinstance(context.get("employee_profiles"), dict) else {}
    ilya_profile = profiles.get("Илья Бочков", {}) if isinstance(profiles.get("Илья Бочков", {}), dict) else {}
    assert ilya_profile.get("communication_style") == "expert_to_expert"

