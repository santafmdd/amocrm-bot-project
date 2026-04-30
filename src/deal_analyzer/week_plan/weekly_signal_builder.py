from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import date, timedelta
from typing import Any

from ..daily_control.source_reader import clean_text, map_headers, parse_date, pick_by_mapping
from .models import WeekPlanSignalGroup
from .source_reader import WEEK_PLAN_SOURCE_ALIASES


_ACTIVITY_MARKERS: tuple[tuple[str, str], ...] = (
    ("обуч", "training"),
    ("разбор", "review"),
    ("дейлик", "daily"),
    ("контрол", "control"),
    ("задач", "task"),
)


def _parse_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    match = re.search(r"-?\d+", text)
    if not match:
        return 0
    try:
        return int(match.group(0))
    except Exception:
        return 0


def week_bounds_monday_sunday(iso_day: str) -> tuple[str, str]:
    parsed = parse_date(iso_day)
    if not parsed:
        return "", ""
    try:
        control_day = date.fromisoformat(parsed)
    except ValueError:
        return "", ""
    week_start = control_day - timedelta(days=control_day.weekday())
    week_end = week_start + timedelta(days=6)
    return week_start.isoformat(), week_end.isoformat()


def _tokenize_mix_value(value: str) -> list[tuple[str, int]]:
    text = clean_text(value)
    if not text:
        return []
    tokens = re.split(r"[;,\n\|]+", text)
    out: list[tuple[str, int]] = []
    for token in tokens:
        item = clean_text(token)
        if not item:
            continue
        match = re.match(r"^(.*?)\s+-\s+(\d+)$", item)
        if match:
            label = clean_text(match.group(1))
            count = _parse_int(match.group(2))
            out.append((label, max(1, count)))
        else:
            out.append((item, 1))
    return out


def aggregate_mix(values: list[str]) -> str:
    counter: Counter[str] = Counter()
    for value in values:
        for label, count in _tokenize_mix_value(value):
            if not label:
                continue
            counter[label] += max(1, int(count or 1))
    if not counter:
        return ""
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))
    return "; ".join(f"{label} - {count}" for label, count in ordered)


def _extract_list(values: list[str], *, limit: int = 12) -> list[str]:
    counter: Counter[str] = Counter()
    for value in values:
        for token in _tokenize_mix_value(value):
            label, count = token
            if not label:
                continue
            counter[label] += max(1, int(count or 1))
    if not counter:
        return []
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))
    return [label for label, _count in ordered[: max(1, int(limit or 12))]]


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


def _training_signal_count(messages: list[str], fixes: list[str], growth: list[str]) -> int:
    text_parts = [*messages, *fixes, *growth]
    count = 0
    for part in text_parts:
        low = clean_text(part).lower()
        if not low:
            continue
        if "заплан" in low and "обуч" in low:
            count += 1
            continue
        if "обуч" in low and any(marker in low for marker in ("разбор", "навык", "скрипт", "техника")):
            count += 1
    return count


def _criticality_histogram(values: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        key = clean_text(value).lower()
        if not key:
            continue
        out[key] = int(out.get(key, 0) or 0) + 1
    return out


def _activity_hints(messages: list[str], fixes: list[str], growth: list[str]) -> list[str]:
    text = " ".join([*messages, *fixes, *growth]).lower()
    hints: list[str] = []
    for marker, label in _ACTIVITY_MARKERS:
        if marker in text:
            hints.append(label)
    return sorted(set(hints))


def group_daily_rows_into_week_signals(
    *,
    headers: list[str],
    rows: list[list[str]],
    period_start: date,
    period_end: date,
    manager_allowlist: tuple[str, ...] | None = None,
    plan_week_start_override: str = "",
    plan_week_end_override: str = "",
) -> tuple[list[WeekPlanSignalGroup], dict[str, Any]]:
    mapped = map_headers(headers, WEEK_PLAN_SOURCE_ALIASES).mapped
    manager_idx = int(mapped.get("manager_name", -1))
    groups_raw: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    signal_rows_count = 0
    managers_in_daily_control: set[str] = set()
    managers_in_signal_period: set[str] = set()
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

        signal_rows_count += 1
        managers_in_signal_period.add(clean_text(manager_name))
        managers_in_groups.add(clean_text(manager_name))
        group_week_start = plan_week_start_override or week_start
        group_week_end = plan_week_end_override or week_end
        groups_raw[(group_week_start, group_week_end, manager_name)].append(
            {
                "week_start": group_week_start,
                "week_end": group_week_end,
                "control_day_date": control_day,
                "day_label": pick_by_mapping(raw, mapped, "day_label"),
                "manager_name": manager_name,
                "manager_role_profile": pick_by_mapping(raw, mapped, "manager_role_profile"),
                "deals_count": _parse_int(pick_by_mapping(raw, mapped, "deals_count")),
                "calls_count": _parse_int(pick_by_mapping(raw, mapped, "calls_count")),
                "deal_ids": pick_by_mapping(raw, mapped, "deal_ids"),
                "deal_links": pick_by_mapping(raw, mapped, "deal_links"),
                "product_mix": pick_by_mapping(raw, mapped, "product_mix"),
                "base_mix": pick_by_mapping(raw, mapped, "base_mix"),
                "main_pattern": pick_by_mapping(raw, mapped, "main_pattern"),
                "strong_sides": pick_by_mapping(raw, mapped, "strong_sides"),
                "growth_zones": pick_by_mapping(raw, mapped, "growth_zones"),
                "what_to_fix": pick_by_mapping(raw, mapped, "what_to_fix"),
                "what_to_tell_employee": pick_by_mapping(raw, mapped, "what_to_tell_employee"),
                "expected_quant_impact": pick_by_mapping(raw, mapped, "expected_quant_impact"),
                "expected_qual_impact": pick_by_mapping(raw, mapped, "expected_qual_impact"),
                "score_0_100": _parse_int(pick_by_mapping(raw, mapped, "score_0_100")),
                "criticality": pick_by_mapping(raw, mapped, "criticality"),
            }
        )

    groups: list[WeekPlanSignalGroup] = []
    total_training_signals = 0
    total_activity_hints = 0
    total_signal_strength = 0
    for (week_start, week_end, manager_name), daily_rows in sorted(
        groups_raw.items(),
        key=lambda item: (item[0][0], item[0][2].lower()),
    ):
        role = ""
        deals_count = 0
        calls_count = 0
        score_values: list[int] = []
        deal_links: list[str] = []
        seen_links: set[str] = set()
        product_values: list[str] = []
        base_values: list[str] = []
        growth_values: list[str] = []
        strong_values: list[str] = []
        fix_values: list[str] = []
        tell_values: list[str] = []
        criticality_values: list[str] = []

        for row in daily_rows:
            role = role or clean_text(row.get("manager_role_profile", ""))
            deals_count += int(row.get("deals_count", 0) or 0)
            calls_count += int(row.get("calls_count", 0) or 0)
            score = int(row.get("score_0_100", 0) or 0)
            if score > 0:
                score_values.append(score)
            criticality_value = clean_text(row.get("criticality", ""))
            if criticality_value:
                criticality_values.append(criticality_value)

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

        avg_score = round(sum(score_values) / len(score_values)) if score_values else 0
        training_signal_count = _training_signal_count(tell_values, fix_values, growth_values)
        total_training_signals += training_signal_count
        activity_hints = _activity_hints(tell_values, fix_values, growth_values)
        total_activity_hints += len(activity_hints)
        signal_strength = (
            len(growth_values)
            + len(fix_values)
            + len(tell_values)
            + training_signal_count
            + len(activity_hints)
            + (1 if deals_count > 0 else 0)
        )
        total_signal_strength += max(0, int(signal_strength))

        groups.append(
            WeekPlanSignalGroup(
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                plan_week_start=week_start,
                plan_week_end=week_end,
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
                repeated_growth_zones=_extract_list(growth_values),
                repeated_strong_sides=_extract_list(strong_values),
                repeated_fix_points=_extract_list(fix_values),
                repeated_messages=_extract_list(tell_values),
                training_signal_count=training_signal_count,
                criticality_histogram=_criticality_histogram(criticality_values),
            )
        )

    diagnostics["groups_count"] = len(groups)
    diagnostics["signal_rows_count"] = int(signal_rows_count or 0)
    diagnostics["signals_count"] = int(total_signal_strength)
    diagnostics["training_signals_count"] = int(total_training_signals)
    diagnostics["activity_hints_count"] = int(total_activity_hints)
    diagnostics["managers_in_daily_control"] = sorted([name for name in managers_in_daily_control if name])
    diagnostics["managers_in_signal_period"] = sorted([name for name in managers_in_signal_period if name])
    diagnostics["managers_in_groups"] = sorted([name for name in managers_in_groups if name])
    diagnostics["managers_skipped_with_reason"] = managers_skipped_with_reason
    return groups, diagnostics
