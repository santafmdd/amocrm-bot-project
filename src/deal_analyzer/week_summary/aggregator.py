from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date
from ..weekly_shared.daily_control_reader import map_daily_control_headers
from ..weekly_shared.date_utils import week_bounds_monday_sunday
from .models import WeekSummaryGroup
from .source_reader import map_manager_headers, map_plan_headers


FALLBACK_MANAGER_HEADERS: list[str] = [
    "Неделя с",
    "Неделя по",
    "Менеджер",
    "Проанализировано сделок",
    "Средняя оценка 0-100",
    "Итог недели",
]


def _parse_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    digits = "".join(ch for ch in text if ch.isdigit() or ch == "-")
    if not digits:
        return 0
    try:
        return int(digits)
    except Exception:
        return 0


def _status_bucket(status: str) -> str:
    probe = clean_text(status).lower()
    if probe in {"выполнено", "выполнена", "done", "completed"}:
        return "done"
    if probe in {"в работе", "в процессе", "in_progress", "in progress"}:
        return "in_progress"
    if probe in {"перенесено", "перенесена", "postponed"}:
        return "postponed"
    return "no_status"


def _build_manager_rows_from_daily(
    *,
    daily_headers: list[str],
    daily_rows: list[list[str]],
    period_start: date,
    period_end: date,
) -> tuple[list[list[str]], dict[str, Any]]:
    mapped_daily = map_daily_control_headers(daily_headers)
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    total_source_rows = 0

    for row in daily_rows:
        if not isinstance(row, list):
            continue
        total_source_rows += 1
        week_start = parse_date(row[mapped_daily["week_start"]] if mapped_daily.get("week_start", -1) < len(row) else "")
        week_end = parse_date(row[mapped_daily["week_end"]] if mapped_daily.get("week_end", -1) < len(row) else "")

        if not week_start:
            control_day = parse_date(
                row[mapped_daily["control_day_date"]] if mapped_daily.get("control_day_date", -1) < len(row) else ""
            )
            if control_day:
                week_start, week_end = week_bounds_monday_sunday(control_day)

        if not week_start or not week_end:
            continue
        try:
            week_start_dt = date.fromisoformat(week_start)
        except ValueError:
            continue
        if week_start_dt < period_start or week_start_dt > period_end:
            continue

        manager_name = clean_text(row[mapped_daily["manager_name"]] if mapped_daily.get("manager_name", -1) < len(row) else "")
        if not manager_name:
            continue

        key = (week_start, week_end, manager_name)
        state = by_key.setdefault(
            key,
            {
                "deals_count": 0,
                "scores": [],
                "patterns": [],
            },
        )
        state["deals_count"] += _parse_int(
            row[mapped_daily["deals_count"]] if mapped_daily.get("deals_count", -1) < len(row) else ""
        )
        score = _parse_int(row[mapped_daily["score_0_100"]] if mapped_daily.get("score_0_100", -1) < len(row) else "")
        if score > 0:
            state["scores"].append(score)
        pattern = clean_text(row[mapped_daily["main_pattern"]] if mapped_daily.get("main_pattern", -1) < len(row) else "")
        if pattern and pattern not in state["patterns"]:
            state["patterns"].append(pattern)

    rows: list[list[str]] = []
    for (week_start, week_end, manager_name), state in sorted(by_key.items(), key=lambda item: (item[0][0], item[0][2])):
        scores = state.get("scores", [])
        avg_score = round(sum(scores) / len(scores)) if scores else 0
        summary = "; ".join(state.get("patterns", [])[:2])
        rows.append(
            [
                week_start,
                week_end,
                manager_name,
                str(int(state.get("deals_count", 0) or 0)),
                str(int(avg_score)),
                summary,
            ]
        )

    diagnostics = {
        "daily_source_rows_total": total_source_rows,
        "daily_fallback_rows_count": len(rows),
        "daily_fallback_applied": bool(rows),
    }
    return rows, diagnostics


def _collect_manager_by_week(
    *,
    manager_rows: list[list[str]],
    mapped_manager: dict[str, int],
    period_start: date,
    period_end: date,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    manager_by_week: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    required = ("week_start", "week_end", "manager_name")
    if any(field not in mapped_manager for field in required):
        return manager_by_week
    for row in manager_rows:
        if not isinstance(row, list):
            continue
        week_start = parse_date(row[mapped_manager["week_start"]] if mapped_manager.get("week_start", -1) < len(row) else "")
        week_end = parse_date(row[mapped_manager["week_end"]] if mapped_manager.get("week_end", -1) < len(row) else "")
        if not week_start or not week_end:
            continue
        try:
            week_start_dt = date.fromisoformat(week_start)
        except ValueError:
            continue
        if week_start_dt < period_start or week_start_dt > period_end:
            continue

        manager_by_week[(week_start, week_end)].append(
            {
                "week_start": week_start,
                "week_end": week_end,
                "manager_name": clean_text(
                    row[mapped_manager["manager_name"]] if mapped_manager.get("manager_name", -1) < len(row) else ""
                ),
                "deals_count": _parse_int(
                    row[mapped_manager["deals_count"]] if mapped_manager.get("deals_count", -1) < len(row) else ""
                ),
                "avg_score_0_100": _parse_int(
                    row[mapped_manager["avg_score_0_100"]] if mapped_manager.get("avg_score_0_100", -1) < len(row) else ""
                ),
                "weekly_result": clean_text(
                    row[mapped_manager["weekly_result"]] if mapped_manager.get("weekly_result", -1) < len(row) else ""
                ),
            }
        )
    return manager_by_week


def build_week_summary_groups(
    *,
    manager_headers: list[str],
    manager_rows: list[list[str]],
    plan_headers: list[str],
    plan_rows: list[list[str]],
    period_start: date,
    period_end: date,
    daily_headers: list[str] | None = None,
    daily_rows: list[list[str]] | None = None,
) -> tuple[list[WeekSummaryGroup], dict[str, Any], list[dict[str, Any]]]:
    mapped_manager = map_manager_headers(manager_headers)
    mapped_plan = map_plan_headers(plan_headers)

    manager_by_week = _collect_manager_by_week(
        manager_rows=manager_rows,
        mapped_manager=mapped_manager,
        period_start=period_start,
        period_end=period_end,
    )

    fallback_diag: dict[str, Any] = {"daily_fallback_applied": False}
    if not manager_by_week and isinstance(daily_headers, list) and isinstance(daily_rows, list) and daily_rows:
        fallback_rows, fallback_diag = _build_manager_rows_from_daily(
            daily_headers=daily_headers,
            daily_rows=daily_rows,
            period_start=period_start,
            period_end=period_end,
        )
        if fallback_rows:
            mapped_fallback = map_manager_headers(FALLBACK_MANAGER_HEADERS)
            manager_by_week = _collect_manager_by_week(
                manager_rows=fallback_rows,
                mapped_manager=mapped_fallback,
                period_start=period_start,
                period_end=period_end,
            )

    plan_by_week: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    if "plan_week_start" in mapped_plan and "plan_week_end" in mapped_plan:
        for row in plan_rows:
            if not isinstance(row, list):
                continue
            week_start = parse_date(
                row[mapped_plan["plan_week_start"]] if mapped_plan.get("plan_week_start", -1) < len(row) else ""
            )
            week_end = parse_date(row[mapped_plan["plan_week_end"]] if mapped_plan.get("plan_week_end", -1) < len(row) else "")
            if not week_start or not week_end:
                continue
            plan_by_week[(week_start, week_end)].append(
                {
                    "week_start": week_start,
                    "week_end": week_end,
                    "status": clean_text(row[mapped_plan["status"]] if mapped_plan.get("status", -1) < len(row) else ""),
                    "training_link": clean_text(
                        row[mapped_plan["training_link"]] if mapped_plan.get("training_link", -1) < len(row) else ""
                    ),
                    "post_training_task_link": clean_text(
                        row[mapped_plan["post_training_task_link"]]
                        if mapped_plan.get("post_training_task_link", -1) < len(row)
                        else ""
                    ),
                    "what_i_do": clean_text(row[mapped_plan["what_i_do"]] if mapped_plan.get("what_i_do", -1) < len(row) else ""),
                }
            )

    groups: list[WeekSummaryGroup] = []
    plan_fact_rows: list[dict[str, Any]] = []
    for (week_start, week_end), manager_items in sorted(manager_by_week.items(), key=lambda item: item[0][0]):
        plan_items = plan_by_week.get((week_start, week_end), [])
        deals_total = sum(int(item.get("deals_count", 0) or 0) for item in manager_items)
        score_values = [int(item.get("avg_score_0_100", 0) or 0) for item in manager_items if int(item.get("avg_score_0_100", 0) or 0) > 0]
        avg_score = round(sum(score_values) / len(score_values)) if score_values else 0

        done = 0
        in_progress = 0
        postponed = 0
        no_status = 0
        training_links: list[str] = []
        post_training_links: list[str] = []
        unresolved_actions: list[str] = []
        for item in plan_items:
            bucket = _status_bucket(item.get("status", ""))
            if bucket == "done":
                done += 1
            elif bucket == "in_progress":
                in_progress += 1
            elif bucket == "postponed":
                postponed += 1
            else:
                no_status += 1

            link = clean_text(item.get("training_link", ""))
            if link and link not in training_links:
                training_links.append(link)
            post_link = clean_text(item.get("post_training_task_link", ""))
            if post_link and post_link not in post_training_links:
                post_training_links.append(post_link)
            if bucket != "done":
                action = clean_text(item.get("what_i_do", ""))
                if action:
                    unresolved_actions.append(action)
            plan_fact_rows.append(
                {
                    "week_start": week_start,
                    "week_end": week_end,
                    "status": item.get("status", ""),
                    "training_link": link,
                    "post_training_task_link": post_link,
                    "what_i_do": clean_text(item.get("what_i_do", "")),
                }
            )

        groups.append(
            WeekSummaryGroup(
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                week_start=week_start,
                week_end=week_end,
                source_manager_rows=manager_items,
                source_plan_rows=plan_items,
                managers_count=len({clean_text(item.get("manager_name", "")) for item in manager_items if clean_text(item.get("manager_name", ""))}),
                deals_count=deals_total,
                avg_score_0_100=int(avg_score),
                planned_actions_total=len(plan_items),
                done_actions_count=done,
                in_progress_actions_count=in_progress,
                postponed_actions_count=postponed,
                no_status_actions_count=no_status,
                training_links=training_links,
                post_training_task_links=post_training_links,
                unresolved_actions=unresolved_actions[:20],
            )
        )

    diagnostics = {
        "manager_rows_total": len(manager_rows),
        "plan_rows_total": len(plan_rows),
        "groups_count": len(groups),
        "weeks_detected": [f"{group.week_start}|{group.week_end}" for group in groups],
        **fallback_diag,
    }
    return groups, diagnostics, plan_fact_rows
