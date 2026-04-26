from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import date
from typing import Any

from .models import DailyControlInputGroup
from .source_reader import (
    SOURCE_FIELD_ALIASES,
    clean_text,
    day_label_from_iso,
    extract_date_from_listened_calls,
    map_headers,
    parse_date,
    parse_listened_calls_count,
    pick_by_mapping,
)


def _split_mix_tokens(value: str) -> list[str]:
    text = clean_text(value)
    if not text:
        return []
    chunks = re.split(r"[;,\n\|]+", text)
    out: list[str] = []
    for chunk in chunks:
        token = clean_text(chunk)
        if token:
            out.append(token)
    return out


def aggregate_mix(values: list[str]) -> str:
    counter: Counter[str] = Counter()
    for value in values:
        for token in _split_mix_tokens(value):
            counter[token] += 1
    if not counter:
        return ""
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0].lower()))
    return "; ".join(f"{token} - {count}" for token, count in ordered)


def _manager_allowed(manager_name: str, allowlist: tuple[str, ...] | None) -> bool:
    if not allowlist:
        return True
    name = clean_text(manager_name).lower()
    if not name:
        return False
    for item in allowlist:
        probe = clean_text(item).lower()
        if not probe:
            continue
        if name == probe or probe in name or name in probe:
            return True
    return False


def group_by_manager_day(
    *,
    headers: list[str],
    rows: list[list[str]],
    cfg: Any,
    period_start: date,
    period_end: date,
    manager_allowlist: tuple[str, ...] | None = None,
) -> tuple[list[DailyControlInputGroup], dict[str, Any]]:
    _ = cfg
    header_mapping = map_headers(headers, SOURCE_FIELD_ALIASES).mapped

    grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    calls_confidence: Counter[str] = Counter()
    diagnostics: dict[str, Any] = {
        "source_rows_total": len(rows),
        "rows_filtered_out": 0,
        "rows_missing_date": 0,
        "rows_outside_period": 0,
        "rows_outside_manager_allowlist": 0,
        "header_mapping": {field: headers[idx] for field, idx in header_mapping.items() if idx < len(headers)},
    }

    for raw in rows:
        if not isinstance(raw, list):
            diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"] or 0) + 1
            continue

        case_date = parse_date(pick_by_mapping(raw, header_mapping, "case_date"))
        if not case_date:
            case_date = extract_date_from_listened_calls(pick_by_mapping(raw, header_mapping, "listened_calls"))
        if not case_date:
            diagnostics["rows_missing_date"] = int(diagnostics["rows_missing_date"] or 0) + 1
            diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"] or 0) + 1
            continue

        try:
            case_dt = date.fromisoformat(case_date)
        except ValueError:
            diagnostics["rows_missing_date"] = int(diagnostics["rows_missing_date"] or 0) + 1
            diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"] or 0) + 1
            continue

        if case_dt < period_start or case_dt > period_end:
            diagnostics["rows_outside_period"] = int(diagnostics["rows_outside_period"] or 0) + 1
            diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"] or 0) + 1
            continue

        manager_name = pick_by_mapping(raw, header_mapping, "manager")
        if not _manager_allowed(manager_name, manager_allowlist):
            diagnostics["rows_outside_manager_allowlist"] = int(diagnostics["rows_outside_manager_allowlist"] or 0) + 1
            diagnostics["rows_filtered_out"] = int(diagnostics["rows_filtered_out"] or 0) + 1
            continue

        listened = pick_by_mapping(raw, header_mapping, "listened_calls")
        calls_count, confidence = parse_listened_calls_count(listened)
        calls_confidence[confidence] += 1

        entry = {
            "case_date": case_date,
            "manager_name": manager_name,
            "manager_role_profile": pick_by_mapping(raw, header_mapping, "role"),
            "deal_id": pick_by_mapping(raw, header_mapping, "deal_id"),
            "deal_link": pick_by_mapping(raw, header_mapping, "deal_link"),
            "deal_name": pick_by_mapping(raw, header_mapping, "deal_name"),
            "company": pick_by_mapping(raw, header_mapping, "company"),
            "product_focus": pick_by_mapping(raw, header_mapping, "product_focus"),
            "base_tag": pick_by_mapping(raw, header_mapping, "base_tag"),
            "case_type": pick_by_mapping(raw, header_mapping, "case_type"),
            "listened_calls": listened,
            "calls_count": calls_count,
            "calls_count_confidence": confidence,
            "key_takeaway": pick_by_mapping(raw, header_mapping, "key_takeaway"),
            "strong": pick_by_mapping(raw, header_mapping, "strong"),
            "growth": pick_by_mapping(raw, header_mapping, "growth"),
            "why_important": pick_by_mapping(raw, header_mapping, "why_important"),
            "reinforce": pick_by_mapping(raw, header_mapping, "reinforce"),
            "fix": pick_by_mapping(raw, header_mapping, "fix"),
            "tell_employee": pick_by_mapping(raw, header_mapping, "tell_employee"),
            "expected_qty": pick_by_mapping(raw, header_mapping, "expected_qty"),
            "expected_quality": pick_by_mapping(raw, header_mapping, "expected_quality"),
            "score": pick_by_mapping(raw, header_mapping, "score"),
            "criticality": pick_by_mapping(raw, header_mapping, "criticality"),
            "row_raw": [str(x or "") for x in raw],
        }

        grouped_rows[(case_date, manager_name)].append(entry)

    packages: list[DailyControlInputGroup] = []
    for (control_day_date, manager_name), source_rows in sorted(grouped_rows.items(), key=lambda item: (item[0][0], item[0][1].lower())):
        deal_ids_unique: list[str] = []
        deal_names_unique: list[str] = []
        deal_links_unique: list[str] = []
        seen_deals: set[str] = set()
        seen_names: set[str] = set()
        seen_links: set[str] = set()

        calls_count = 0
        product_values: list[str] = []
        base_values: list[str] = []

        insights: dict[str, list[str]] = {
            "key_takeaways": [],
            "strong_sides": [],
            "growth_zones": [],
            "what_to_fix": [],
            "what_to_tell_employee": [],
            "why_it_matters": [],
        }

        discipline_signals = {
            "discipline_case_rows": 0,
            "short_call_rows": 0,
        }

        manager_role = ""
        for row in source_rows:
            manager_role = manager_role or clean_text(row.get("manager_role_profile", ""))
            deal_id = clean_text(row.get("deal_id", ""))
            if deal_id and deal_id not in seen_deals:
                seen_deals.add(deal_id)
                deal_ids_unique.append(deal_id)

            deal_name = clean_text(row.get("deal_name", ""))
            if deal_name and deal_name not in seen_names:
                seen_names.add(deal_name)
                deal_names_unique.append(deal_name)

            deal_link = clean_text(row.get("deal_link", ""))
            if deal_link and deal_link not in seen_links:
                seen_links.add(deal_link)
                deal_links_unique.append(deal_link)

            calls_count += int(row.get("calls_count", 0) or 0)

            product_focus = clean_text(row.get("product_focus", ""))
            if product_focus:
                product_values.append(product_focus)
            base_tag = clean_text(row.get("base_tag", ""))
            if base_tag:
                base_values.append(base_tag)

            for key, src_field in (
                ("key_takeaways", "key_takeaway"),
                ("strong_sides", "strong"),
                ("growth_zones", "growth"),
                ("what_to_fix", "fix"),
                ("what_to_tell_employee", "tell_employee"),
                ("why_it_matters", "why_important"),
            ):
                value = clean_text(row.get(src_field, ""))
                if value:
                    insights[key].append(value)

            case_type = clean_text(row.get("case_type", "")).lower()
            if "дисципл" in case_type or "недозвон" in case_type:
                discipline_signals["discipline_case_rows"] = int(discipline_signals["discipline_case_rows"] or 0) + 1
            if int(row.get("calls_count", 0) or 0) == 0 and clean_text(row.get("listened_calls", "")):
                discipline_signals["short_call_rows"] = int(discipline_signals["short_call_rows"] or 0) + 1

        calls_count_effective = calls_count if calls_count > 0 else len(source_rows)

        package = DailyControlInputGroup(
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            control_day_date=control_day_date,
            day_label=day_label_from_iso(control_day_date),
            manager_name=manager_name,
            manager_role_profile=manager_role,
            source_rows=source_rows,
            sample_size=len(source_rows),
            deals_count=len(deal_ids_unique),
            calls_count=calls_count_effective,
            deal_ids=deal_ids_unique,
            deal_names=deal_names_unique,
            deal_links=deal_links_unique,
            product_mix=aggregate_mix(product_values),
            base_mix=aggregate_mix(base_values),
            insights=insights,
            discipline_signals=discipline_signals,
        )
        packages.append(package)

    diagnostics["groups_total"] = len(packages)
    diagnostics["calls_count_confidence_distribution"] = dict(calls_confidence)
    return packages, diagnostics
