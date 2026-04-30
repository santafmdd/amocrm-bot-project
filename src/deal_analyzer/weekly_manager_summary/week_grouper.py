from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

from ..daily_control.source_reader import clean_text, map_headers, parse_date, pick_by_mapping
from ..weekly_shared.date_utils import week_bounds_monday_sunday
from ..weekly_shared.signal_builder import aggregate_mix, parse_int, top_labels
from ..weekly_shared.week_plan_reader import build_plan_fact_index
from .models import WeeklyManagerGroup
from .source_reader import WEEKLY_SOURCE_ALIASES


def _manager_allowed(manager_name: str, allowlist: tuple[str, ...] | None) -> bool:
    if not allowlist:
        return True
    probe = clean_text(manager_name).lower()
    if not probe:
        return False
    for item in allowlist:
        check = clean_text(item).lower()
        if not check:
            continue
        if probe == check or probe in check or check in probe:
            return True
    return False


def group_daily_rows_by_week_manager(
    *,
    headers: list[str],
    rows: list[list[str]],
    period_start: date,
    period_end: date,
    manager_allowlist: tuple[str, ...] | None = None,
    plan_headers: list[str] | None = None,
    plan_rows: list[list[str]] | None = None,
) -> tuple[list[WeeklyManagerGroup], dict[str, Any], list[dict[str, Any]]]:
    mapped = map_headers(headers, WEEKLY_SOURCE_ALIASES).mapped
    manager_idx = int(mapped.get("manager_name", -1))
    groups_raw: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    managers_in_daily_control: set[str] = set()
    managers_in_groups: set[str] = set()
    managers_skipped_with_reason: list[dict[str, Any]] = []
    diagnostics: dict[str, Any] = {
        "source_rows_total": len(rows),
        "rows_filtered_out": 0,
        "rows_missing_date": 0,
        "rows_outside_period": 0,
        "rows_outside_manager_allowlist": 0,
        "header_mapping": {field: headers[idx] for field, idx in mapped.items() if idx < len(headers)},
    }

    for row_index, raw in enumerate(rows):
        if not isinstance(raw, list):
            diagnostics["rows_filtered_out"] = int(diagnostics.get("rows_filtered_out", 0) or 0) + 1
            if len(managers_skipped_with_reason) < 200:
                managers_skipped_with_reason.append(
                    {
                        "row_index": row_index,
                        "manager_name": "",
                        "reason": "row_is_not_list",
                    }
                )
            continue

        manager_name = pick_by_mapping(raw, mapped, "manager_name")
        if manager_idx >= 0 and manager_idx < len(raw):
            manager_raw = clean_text(raw[manager_idx])
            if manager_raw:
                managers_in_daily_control.add(manager_raw)
        if not _manager_allowed(manager_name, manager_allowlist):
            diagnostics["rows_filtered_out"] = int(diagnostics.get("rows_filtered_out", 0) or 0) + 1
            diagnostics["rows_outside_manager_allowlist"] = int(diagnostics.get("rows_outside_manager_allowlist", 0) or 0) + 1
            if len(managers_skipped_with_reason) < 200:
                managers_skipped_with_reason.append(
                    {
                        "row_index": row_index,
                        "manager_name": clean_text(manager_name),
                        "reason": "manager_outside_allowlist",
                    }
                )
            continue

        control_day = parse_date(pick_by_mapping(raw, mapped, "control_day_date"))
        week_start = parse_date(pick_by_mapping(raw, mapped, "week_start"))
        week_end = parse_date(pick_by_mapping(raw, mapped, "week_end"))
        if not week_start or not week_end:
            calc_start, calc_end = week_bounds_monday_sunday(control_day)
            week_start = week_start or calc_start
            week_end = week_end or calc_end

        if not control_day:
            diagnostics["rows_filtered_out"] = int(diagnostics.get("rows_filtered_out", 0) or 0) + 1
            diagnostics["rows_missing_date"] = int(diagnostics.get("rows_missing_date", 0) or 0) + 1
            if len(managers_skipped_with_reason) < 200:
                managers_skipped_with_reason.append(
                    {
                        "row_index": row_index,
                        "manager_name": clean_text(manager_name),
                        "reason": "missing_or_invalid_control_day_date",
                    }
                )
            continue
        try:
            control_date = date.fromisoformat(control_day)
        except ValueError:
            diagnostics["rows_filtered_out"] = int(diagnostics.get("rows_filtered_out", 0) or 0) + 1
            diagnostics["rows_missing_date"] = int(diagnostics.get("rows_missing_date", 0) or 0) + 1
            if len(managers_skipped_with_reason) < 200:
                managers_skipped_with_reason.append(
                    {
                        "row_index": row_index,
                        "manager_name": clean_text(manager_name),
                        "reason": "invalid_control_day_date",
                    }
                )
            continue
        if control_date < period_start or control_date > period_end:
            diagnostics["rows_filtered_out"] = int(diagnostics.get("rows_filtered_out", 0) or 0) + 1
            diagnostics["rows_outside_period"] = int(diagnostics.get("rows_outside_period", 0) or 0) + 1
            if len(managers_skipped_with_reason) < 200:
                managers_skipped_with_reason.append(
                    {
                        "row_index": row_index,
                        "manager_name": clean_text(manager_name),
                        "reason": "outside_period",
                    }
                )
            continue

        managers_in_groups.add(clean_text(manager_name))
        groups_raw[(week_start, week_end, manager_name)].append(
            {
                "week_start": week_start,
                "week_end": week_end,
                "control_day_date": control_day,
                "day_label": pick_by_mapping(raw, mapped, "day_label"),
                "manager_name": manager_name,
                "manager_role_profile": pick_by_mapping(raw, mapped, "manager_role_profile"),
                "deals_count": parse_int(pick_by_mapping(raw, mapped, "deals_count")),
                "calls_count": parse_int(pick_by_mapping(raw, mapped, "calls_count")),
                "deal_links": pick_by_mapping(raw, mapped, "deal_links"),
                "product_mix": pick_by_mapping(raw, mapped, "product_mix"),
                "base_mix": pick_by_mapping(raw, mapped, "base_mix"),
                "main_pattern": pick_by_mapping(raw, mapped, "main_pattern"),
                "strong_sides": pick_by_mapping(raw, mapped, "strong_sides"),
                "growth_zones": pick_by_mapping(raw, mapped, "growth_zones"),
                "why_it_matters": pick_by_mapping(raw, mapped, "why_it_matters"),
                "what_to_reinforce": pick_by_mapping(raw, mapped, "what_to_reinforce"),
                "what_to_fix": pick_by_mapping(raw, mapped, "what_to_fix"),
                "what_to_tell_employee": pick_by_mapping(raw, mapped, "what_to_tell_employee"),
                "expected_quant_impact": pick_by_mapping(raw, mapped, "expected_quant_impact"),
                "expected_qual_impact": pick_by_mapping(raw, mapped, "expected_qual_impact"),
                "score_0_100": parse_int(pick_by_mapping(raw, mapped, "score_0_100")),
                "criticality": pick_by_mapping(raw, mapped, "criticality"),
            }
        )

    plan_fact_index = build_plan_fact_index(headers=plan_headers or [], rows=plan_rows or [])
    plan_fact_rows: list[dict[str, Any]] = []
    groups: list[WeeklyManagerGroup] = []
    for (week_start, week_end, manager_name), daily_rows in sorted(
        groups_raw.items(),
        key=lambda item: (item[0][0], item[0][2].lower()),
    ):
        deal_links: list[str] = []
        seen_links: set[str] = set()
        product_values: list[str] = []
        base_values: list[str] = []
        growth_values: list[str] = []
        strong_values: list[str] = []
        fix_values: list[str] = []
        tell_values: list[str] = []
        score_values: list[int] = []
        deals_count = 0
        calls_count = 0
        role = ""
        for row in daily_rows:
            role = role or clean_text(row.get("manager_role_profile", ""))
            deals_count += int(row.get("deals_count", 0) or 0)
            calls_count += int(row.get("calls_count", 0) or 0)
            score = int(row.get("score_0_100", 0) or 0)
            if score > 0:
                score_values.append(score)
            for field, collector in (
                ("product_mix", product_values),
                ("base_mix", base_values),
                ("growth_zones", growth_values),
                ("strong_sides", strong_values),
                ("what_to_fix", fix_values),
                ("what_to_tell_employee", tell_values),
            ):
                value = clean_text(row.get(field, ""))
                if value:
                    collector.append(value)
            for link in [clean_text(item) for item in str(row.get("deal_links", "")).split(";")]:
                if link and link not in seen_links:
                    seen_links.add(link)
                    deal_links.append(link)

        plan_key = f"{week_start}|{week_end}|{clean_text(manager_name).lower()}"
        plan_fact = plan_fact_index.get(plan_key, {})
        if plan_fact:
            plan_fact_rows.append({"key": plan_key, **plan_fact})
        avg_score = round(sum(score_values) / len(score_values)) if score_values else 0
        groups.append(
            WeeklyManagerGroup(
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                week_start=week_start,
                week_end=week_end,
                manager_name=manager_name,
                manager_role_profile=role,
                source_rows=daily_rows,
                source_day_count=len(daily_rows),
                deals_count=deals_count,
                calls_count=calls_count,
                avg_score_0_100=int(avg_score),
                deal_links=deal_links,
                product_mix_week=aggregate_mix(product_values),
                base_mix_week=aggregate_mix(base_values),
                repeated_growth_zones=top_labels(growth_values),
                repeated_strong_sides=top_labels(strong_values),
                repeated_fix_points=top_labels(fix_values),
                repeated_messages=top_labels(tell_values),
                plan_actions_total=int(plan_fact.get("plan_actions_total", 0) or 0),
                plan_done_count=int(plan_fact.get("plan_done_count", 0) or 0),
                plan_in_progress_count=int(plan_fact.get("plan_in_progress_count", 0) or 0),
                plan_postponed_count=int(plan_fact.get("plan_postponed_count", 0) or 0),
                plan_no_status_count=int(plan_fact.get("plan_no_status_count", 0) or 0),
                plan_training_links=list(plan_fact.get("plan_training_links", []) or []),
                plan_post_training_task_links=list(plan_fact.get("plan_post_training_task_links", []) or []),
                plan_training_topics=list(plan_fact.get("plan_training_topics", []) or []),
                plan_training_rows_found_count=int(plan_fact.get("training_rows_found_count", 0) or 0),
                plan_training_rows_used_count=int(plan_fact.get("training_rows_used_count", 0) or 0),
                plan_training_rows_used=list(plan_fact.get("training_rows_used", []) or []),
                unresolved_plan_actions=list(plan_fact.get("unresolved_actions", []) or []),
                analyzed_deals_count=int(deals_count),
                analyzed_calls_count=int(calls_count),
                quality_sample_size=int(deals_count),
            )
        )

    diagnostics["groups_count"] = len(groups)
    diagnostics["plan_fact_rows_detected"] = len(plan_fact_rows)
    diagnostics["managers_in_daily_control"] = sorted([name for name in managers_in_daily_control if name])
    diagnostics["managers_in_groups"] = sorted([name for name in managers_in_groups if name])
    diagnostics["managers_skipped_with_reason"] = managers_skipped_with_reason
    return groups, diagnostics, plan_fact_rows
