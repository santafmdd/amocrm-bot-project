from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any

from ..daily_control.roks_oap_parser import parse_roks_oap_snapshot
from .date_utils import month_end_date, parse_iso_date, previous_month, week_month_majority


RUS_MONTH_TOKENS: dict[int, tuple[str, ...]] = {
    1: ("январ",),
    2: ("феврал",),
    3: ("март",),
    4: ("апрел",),
    5: ("май",),
    6: ("июн",),
    7: ("июл",),
    8: ("август",),
    9: ("сентябр",),
    10: ("октябр",),
    11: ("ноябр",),
    12: ("декабр",),
}

# Legacy hints keep parser robust when manager block labels are not parsed from header rows.
BLOCK_ROW_HINTS: dict[str, int] = {
    "отдел": 3,
    "гордиенко": 23,
    "бочков": 43,
    "хомидов": 63,
}

WEEKLY_FACT_COLS = [5, 7, 9, 11, 13]  # F/H/J/L/N

METRIC_LABEL_HINTS: dict[str, tuple[str, ...]] = {
    "dials": ("дозвон",),
    "lpr": ("лпр",),
    "interest": ("интерес", "есть интерес"),
    "demo_done": ("демо", "встреч", "презентац"),
    "test": ("тест",),
    "invoice_count": ("счет", "счёт"),
    "payment_count": ("оплат",),
}


def _norm(value: Any) -> str:
    text = str(value or "").lower().replace("ё", "е")
    text = re.sub(r"[^0-9a-zа-я/ ]+", " ", text)
    return " ".join(text.split())


def _parse_number(value: Any) -> float | None:
    text = str(value or "").strip().replace(" ", "").replace(",", ".")
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _number_to_json(value: float | None) -> int | float | None:
    if value is None:
        return None
    if abs(value - round(value)) < 1e-9:
        return int(round(value))
    return round(value, 4)


def _find_sheet(titles_norm: list[str], titles: list[str], *, year: int, month: int) -> str:
    tokens = RUS_MONTH_TOKENS.get(month, ())
    for idx, norm in enumerate(titles_norm):
        if "рокс" not in norm or "оап" not in norm:
            continue
        if str(year) not in norm:
            continue
        if tokens and any(token in norm for token in tokens):
            return titles[idx]
    return ""


def _manager_surname_key(manager_name: str) -> str:
    parts = [part for part in _norm(manager_name).split() if part]
    if not parts:
        return ""
    return parts[-1]


def _role_profile_key(role_profile: str) -> str:
    return _norm(role_profile or "")


def _looks_like_bochkov(manager_name: str, role_profile: str) -> bool:
    manager_key = _manager_surname_key(manager_name)
    role_key = _role_profile_key(role_profile)
    return (
        "bochkov" in manager_key
        or "бочков" in manager_key
        or "closer" in role_key
        or "demo" in role_key
    )


def _looks_like_khomidov_top_funnel(manager_name: str, role_profile: str) -> bool:
    manager_key = _manager_surname_key(manager_name)
    role_key = _role_profile_key(role_profile)
    return (
        "khomidov" in manager_key
        or "homidov" in manager_key
        or "хомидов" in manager_key
        or "telemarketer" in role_key
        or "телемаркет" in role_key
        or "cold" in role_key
        or "top funnel" in role_key
    )


def _is_empty_metric(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def build_manager_metric_interpretation(
    *,
    manager_name: str,
    manager_role_profile: str = "",
    weekly_fact: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fact = weekly_fact if isinstance(weekly_fact, dict) else {}
    interest_fact = fact.get("interest_fact")
    demo_fact = fact.get("demo_fact")
    test_fact = fact.get("test_fact")
    invoice_fact = fact.get("invoice_count_fact")
    payment_fact = fact.get("payment_count_fact")
    is_bochkov_profile = _looks_like_bochkov(manager_name, manager_role_profile)
    is_top_funnel_profile = _looks_like_khomidov_top_funnel(manager_name, manager_role_profile)

    manager_role = str(manager_role_profile or "").strip()
    if not manager_role:
        if is_bochkov_profile:
            manager_role = "demo_executor_mixed_sources"
        elif is_top_funnel_profile:
            manager_role = "top_funnel_generator"
        else:
            manager_role = "general_manager"

    downstream_applicable = not is_top_funnel_profile
    routed_meetings_possible = bool(is_bochkov_profile or is_top_funnel_profile)
    warnings: list[str] = []
    notes: list[str] = []

    if is_bochkov_profile:
        notes.append(
            "Р”Р»СЏ РїСЂРѕС„РёР»СЏ Бочкова РґРµРјРѕ РјРѕР¶РµС‚ Р±С‹С‚СЊ РІС‹С€Рµ self-generated 'Р•СЃС‚СЊ РёРЅС‚РµСЂРµСЃ', "
            "РїРѕС‚РѕРјСѓ С‡С‚Рѕ РѕРЅ РїСЂРѕРІРѕРґРёС‚ РІСЃС‚СЂРµС‡Рё РёР· РїРµСЂРµРґР°РЅРЅРѕРіРѕ РїРѕС‚РѕРєР°."
        )
        if isinstance(demo_fact, (int, float)) and isinstance(interest_fact, (int, float)) and demo_fact > interest_fact:
            warnings.append("demo_gt_interest_role_allowed")

    if is_top_funnel_profile:
        notes.append(
            "Р”Р»СЏ РїСЂРѕС„РёР»СЏ Хомидова РєР»СЋС‡РµРІС‹Рµ KPI РІРµСЂС…РЅРµР№ РІРѕСЂРѕРЅРєРё: РґРѕР·РІРѕРЅС‹/Р›РџР /РµСЃС‚СЊ РёРЅС‚РµСЂРµСЃ. "
            "Downstream СЌС‚Р°РїС‹ demo/test/invoice/payment РјРѕРіСѓС‚ Р±С‹С‚СЊ РЅРµРїСЂРёРјРµРЅРёРјС‹ РґР»СЏ Р»РёС‡РЅРѕР№ РѕС†РµРЅРєРё."
        )
        if (
            isinstance(interest_fact, (int, float))
            and interest_fact > 0
            and all(_is_empty_metric(value) or float(value) == 0.0 for value in (demo_fact, test_fact, invoice_fact, payment_fact))
        ):
            warnings.append("downstream_zero_role_allowed")

    return {
        "manager_role_profile": manager_role,
        "source_generated_interest": interest_fact,
        "conducted_demo": demo_fact,
        "routed_meetings_possible": routed_meetings_possible,
        "downstream_metrics_applicable": downstream_applicable,
        "notes": notes,
        "warnings": warnings,
    }


def _detect_block_rows(matrix: list[list[str]]) -> dict[str, int]:
    detected: dict[str, int] = {}
    for idx, row in enumerate(matrix, start=1):
        probe = _norm(" ".join([str(row[0] if len(row) > 0 else ""), str(row[1] if len(row) > 1 else "")]))
        if not probe:
            continue
        for key in BLOCK_ROW_HINTS:
            if key in probe and key not in detected:
                detected[key] = idx
    return detected


def _week_index_for_month(*, week_start: str, year: int, month: int) -> int:
    week_start_dt = parse_iso_date(week_start)
    if week_start_dt is None:
        return 1
    first_day = date(year, month, 1)
    first_week_start = first_day - timedelta(days=first_day.weekday())
    offset_days = (week_start_dt - first_week_start).days
    index = (offset_days // 7) + 1
    return max(1, min(5, index))


def _match_metric_key(label_norm: str) -> str | None:
    if not label_norm:
        return None
    for metric_key, hints in METRIC_LABEL_HINTS.items():
        for hint in hints:
            if hint in label_norm:
                return metric_key
    return None


def _find_manager_block_start(
    *,
    matrix: list[list[str]],
    block_rows: dict[str, int],
    manager_name: str,
) -> tuple[int | None, list[str]]:
    warnings: list[str] = []
    surname_key = _manager_surname_key(manager_name)
    if not surname_key:
        return None, [f"manager_name_empty:{manager_name}"]

    start_row = block_rows.get(surname_key)
    if start_row:
        return int(start_row), warnings

    # Fallback 1: scan first two columns.
    for idx, row in enumerate(matrix, start=1):
        probe = _norm(" ".join([str(row[0] if len(row) > 0 else ""), str(row[1] if len(row) > 1 else "")]))
        if surname_key in probe:
            warnings.append(f"manager_block_scanned:{manager_name}:{idx}")
            return idx, warnings

    # Fallback 2: static row hints.
    for hint, row_no in BLOCK_ROW_HINTS.items():
        if hint in surname_key:
            warnings.append(f"manager_block_row_hint:{manager_name}:{row_no}")
            return row_no, warnings

    warnings.append(f"manager_block_missing:{manager_name}")
    return None, warnings


def _extract_weekly_fact_metrics(
    *,
    matrix: list[list[str]],
    manager_allowlist: tuple[str, ...],
    week_index: int,
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    warnings: list[str] = []
    block_rows = _detect_block_rows(matrix)
    parsed: dict[str, dict[str, Any]] = {}
    week_col_idx = WEEKLY_FACT_COLS[max(0, min(len(WEEKLY_FACT_COLS) - 1, week_index - 1))]
    sorted_block_rows = sorted(set(block_rows.values()))

    for manager_name in manager_allowlist:
        start_row, manager_warnings = _find_manager_block_start(
            matrix=matrix,
            block_rows=block_rows,
            manager_name=manager_name,
        )
        warnings.extend(manager_warnings)
        if start_row is None:
            interpretation = build_manager_metric_interpretation(
                manager_name=manager_name,
                manager_role_profile="",
                weekly_fact={
                    "interest_fact": None,
                    "demo_fact": None,
                    "test_fact": None,
                    "invoice_count_fact": None,
                    "payment_count_fact": None,
                },
            )
            parsed[manager_name] = {
                "status": "manager_block_missing",
                "manager_block_row": None,
                "row_labels_found": [],
                "week_index_used": int(week_index),
                "week_label_used": f"{week_index} НЕДЕЛЯ",
                "calls_fact_raw_cell": "",
                "calls_fact_value": None,
                "lpr_fact_value": None,
                "interest_fact_value": None,
                "demo_fact_value": None,
                "test_fact_value": None,
                "invoice_count_fact": None,
                "payment_count_fact": None,
                "metric_interpretation": interpretation,
                "warnings": manager_warnings,
            }
            continue

        block_end = len(matrix) + 1
        for row_no in sorted_block_rows:
            if row_no > start_row:
                block_end = row_no
                break
        scan_until = min(len(matrix), max(start_row, block_end - 1), start_row + 25)

        row_labels_found: list[str] = []
        metric_values: dict[str, int | float | None] = {}
        metric_raw_cells: dict[str, str] = {}
        row_label_by_metric: dict[str, str] = {}

        for row_no in range(start_row, scan_until + 1):
            row = matrix[row_no - 1] if 0 <= row_no - 1 < len(matrix) else []
            label = str(row[1] if len(row) > 1 else row[0] if row else "").strip()
            label_norm = _norm(label)
            metric_key = _match_metric_key(label_norm)
            if not metric_key:
                continue
            if label and label not in row_labels_found:
                row_labels_found.append(label)
            if metric_key not in row_label_by_metric:
                row_label_by_metric[metric_key] = label
            raw_cell = str(row[week_col_idx] if len(row) > week_col_idx else "").strip()
            parsed_value = _number_to_json(_parse_number(raw_cell))
            prev_value = metric_values.get(metric_key)
            # Keep first non-empty parsed value; do not replace valid value with empty one.
            if metric_key not in metric_values or (prev_value is None and parsed_value is not None):
                metric_values[metric_key] = parsed_value
                metric_raw_cells[metric_key] = raw_cell

        current_warnings: list[str] = []
        calls_value = metric_values.get("dials")
        if calls_value is None:
            current_warnings.append("calls_fact_missing")
        interpretation = build_manager_metric_interpretation(
            manager_name=manager_name,
            manager_role_profile="",
            weekly_fact={
                "interest_fact": metric_values.get("interest"),
                "demo_fact": metric_values.get("demo_done"),
                "test_fact": metric_values.get("test"),
                "invoice_count_fact": metric_values.get("invoice_count"),
                "payment_count_fact": metric_values.get("payment_count"),
            },
        )

        parsed[manager_name] = {
            "status": "ok"
            if any(metric_values.get(key) is not None for key in ("dials", "lpr", "interest", "demo_done", "test", "invoice_count", "payment_count"))
            else "metrics_missing",
            "manager_block_row": int(start_row),
            "row_labels_found": row_labels_found,
            "week_index_used": int(week_index),
            "week_label_used": f"{week_index} НЕДЕЛЯ",
            "calls_fact_raw_cell": metric_raw_cells.get("dials", ""),
            "calls_fact_value": metric_values.get("dials"),
            "lpr_fact_value": metric_values.get("lpr"),
            "interest_fact_value": metric_values.get("interest"),
            "demo_fact_value": metric_values.get("demo_done"),
            "test_fact_value": metric_values.get("test"),
            "invoice_count_fact": metric_values.get("invoice_count"),
            "payment_count_fact": metric_values.get("payment_count"),
            "metric_interpretation": interpretation,
            "row_label_by_metric": row_label_by_metric,
            "warnings": [*manager_warnings, *current_warnings],
        }
    return parsed, warnings


def resolve_weekly_roks_selection(*, sheet_titles: list[str], week_start: str, week_end: str) -> dict[str, Any]:
    titles = [str(item or "").strip() for item in sheet_titles if str(item or "").strip()]
    titles_norm = [_norm(item) for item in titles]
    majority = week_month_majority(week_start, week_end)
    warnings: list[str] = []
    if majority is None:
        return {
            "selected_current_month_sheet": "",
            "selected_previous_month_sheet": "",
            "selection_reason": "invalid_week_range",
            "candidates": titles,
            "warnings": ["invalid_week_range"],
        }

    target_year, target_month = majority
    target_previous_year, target_previous_month = previous_month(target_year, target_month)
    selected_current = _find_sheet(titles_norm, titles, year=target_year, month=target_month)
    selection_reason = "majority_month"

    if not selected_current:
        selection_reason = "majority_month_missing_fallback_to_available_month"
        probe_year, probe_month = target_previous_year, target_previous_month
        for _ in range(12):
            selected_current = _find_sheet(titles_norm, titles, year=probe_year, month=probe_month)
            if selected_current:
                target_year, target_month = probe_year, probe_month
                target_previous_year, target_previous_month = previous_month(target_year, target_month)
                warnings.append("current_month_missing_used_previous_available")
                break
            probe_year, probe_month = previous_month(probe_year, probe_month)

    selected_previous = _find_sheet(
        titles_norm,
        titles,
        year=target_previous_year,
        month=target_previous_month,
    )
    if not selected_current:
        warnings.append("missing_current_month_sheet")
    if not selected_previous:
        warnings.append("missing_previous_month_sheet")

    return {
        "selected_current_month_sheet": selected_current,
        "selected_previous_month_sheet": selected_previous,
        "selection_reason": selection_reason,
        "candidates": titles,
        "warnings": warnings,
        "target_current": {"year": target_year, "month": target_month},
        "target_previous": {"year": target_previous_year, "month": target_previous_month},
    }


def build_weekly_roks_oap_snapshot(
    *,
    client: Any,
    spreadsheet_id: str,
    week_start: str,
    week_end: str,
    manager_allowlist: tuple[str, ...],
) -> dict[str, Any]:
    try:
        sheets = client.list_sheets(spreadsheet_id)
        titles = [str(item.get("title") or "").strip() for item in sheets if str(item.get("title") or "").strip()]
    except Exception as exc:
        return {
            "status": "access_error",
            "parse_status": "access_error",
            "selection_reason": "list_sheets_failed",
            "selected_current_month_sheet": "",
            "selected_previous_month_sheet": "",
            "candidates": [],
            "warnings": [f"list_sheets_failed:{exc}"],
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
            "weekly_metrics_by_manager": {},
        }

    selection = resolve_weekly_roks_selection(sheet_titles=titles, week_start=week_start, week_end=week_end)
    target_current = selection.get("target_current", {}) if isinstance(selection.get("target_current"), dict) else {}
    year = int(target_current.get("year", 0) or 0)
    month = int(target_current.get("month", 0) or 0)
    if year <= 0 or month <= 0:
        return {
            "status": "sheets_not_found",
            "parse_status": "sheets_not_found",
            **selection,
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
            "weekly_metrics_by_manager": {},
        }

    effective_period_end = month_end_date(year, month)
    snapshot = parse_roks_oap_snapshot(
        client=client,
        spreadsheet_id=spreadsheet_id,
        period_end=effective_period_end,
        manager_allowlist=manager_allowlist,
    )
    snapshot = dict(snapshot or {})
    snapshot["selection_reason"] = selection.get("selection_reason", "")
    snapshot["candidates"] = selection.get("candidates", [])
    snapshot["warnings"] = [*list(snapshot.get("warnings") or []), *list(selection.get("warnings") or [])]
    snapshot["selected_current_month_sheet"] = str(selection.get("selected_current_month_sheet") or "")
    snapshot["selected_previous_month_sheet"] = str(selection.get("selected_previous_month_sheet") or "")
    snapshot["week_start"] = week_start
    snapshot["week_end"] = week_end

    week_index = _week_index_for_month(week_start=week_start, year=year, month=month)
    snapshot["week_index_used"] = int(week_index)
    snapshot["week_label_used"] = f"{week_index} НЕДЕЛЯ"

    current_sheet = str(snapshot.get("selected_current_month_sheet") or "")
    weekly_metrics_by_manager: dict[str, dict[str, Any]] = {}
    if current_sheet:
        try:
            matrix = client.get_values(spreadsheet_id, f"'{current_sheet}'!A1:Q160")
            weekly_metrics_by_manager, weekly_warnings = _extract_weekly_fact_metrics(
                matrix=matrix,
                manager_allowlist=manager_allowlist,
                week_index=week_index,
            )
            snapshot["warnings"] = [*list(snapshot.get("warnings") or []), *weekly_warnings]
        except Exception as exc:
            snapshot["warnings"] = [*list(snapshot.get("warnings") or []), f"weekly_sheet_read_failed:{exc}"]
    else:
        snapshot["warnings"] = [*list(snapshot.get("warnings") or []), "weekly_current_sheet_missing"]

    manager_metrics = snapshot.get("manager_metrics", {}) if isinstance(snapshot.get("manager_metrics"), dict) else {}
    for manager_name in manager_allowlist:
        manager_state = manager_metrics.get(manager_name)
        if not isinstance(manager_state, dict):
            manager_state = {}
            manager_metrics[manager_name] = manager_state
        weekly_state = weekly_metrics_by_manager.get(manager_name, {})
        manager_role_profile = str(manager_state.get("manager_role_profile") or "")
        interpretation = build_manager_metric_interpretation(
            manager_name=manager_name,
            manager_role_profile=manager_role_profile,
            weekly_fact={
                "interest_fact": weekly_state.get("interest_fact_value"),
                "demo_fact": weekly_state.get("demo_fact_value"),
                "test_fact": weekly_state.get("test_fact_value"),
                "invoice_count_fact": weekly_state.get("invoice_count_fact"),
                "payment_count_fact": weekly_state.get("payment_count_fact"),
            },
        )
        manager_state["weekly_fact"] = {
            "calls_fact": weekly_state.get("calls_fact_value"),
            "lpr_fact": weekly_state.get("lpr_fact_value"),
            "interest_fact": weekly_state.get("interest_fact_value"),
            "demo_fact": weekly_state.get("demo_fact_value"),
            "test_fact": weekly_state.get("test_fact_value"),
            "invoice_count_fact": weekly_state.get("invoice_count_fact"),
            "payment_count_fact": weekly_state.get("payment_count_fact"),
            "week_index_used": weekly_state.get("week_index_used"),
            "week_label_used": weekly_state.get("week_label_used"),
            "calls_fact_raw_cell": weekly_state.get("calls_fact_raw_cell", ""),
            "row_labels_found": weekly_state.get("row_labels_found", []),
            "row_label_by_metric": weekly_state.get("row_label_by_metric", {}),
            "manager_block_row": weekly_state.get("manager_block_row"),
            "warnings": weekly_state.get("warnings", []),
            "metric_interpretation": weekly_state.get("metric_interpretation", interpretation),
            "roks_sheet_used": current_sheet,
        }
    snapshot["manager_metrics"] = manager_metrics
    snapshot["weekly_metrics_by_manager"] = weekly_metrics_by_manager
    return snapshot

