"""CLI entrypoint for profile-driven analytics capture flow."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.analytics.scenario_executor import ScenarioExecutor
from src.browser.analytics_flow import AnalyticsFlow, AnalyticsFlowInput
from src.browser.events_flow import EventsFlow, EventsFlowInput
from src.browser.amo_reader import AmoAnalyticsReader
from src.browser.models import AnalyticsSnapshot, SourceKind, TabMode
from src.browser.session import BrowserSession, load_browser_settings
from src.config import load_config
from src.config_loader import load_report_profiles, load_table_mappings
from src.logger import setup_logging
from src.safety import ensure_inside_root, ensure_project_structure
from src.utils.compiled_artifacts import find_latest_compiled_artifact
from src.writers.compiler import (
    compile_profile_analytics_result,
    compile_stage_pivot,
    save_compiled_result_json,
    save_stage_pivot_json,
)
from src.writers.google_sheets_layout_ui_writer import GoogleSheetsUILayoutWriter
from src.writers.google_sheets_api_layout_discovery import GoogleSheetsApiLayoutInspector
from src.writers.google_sheets_api_layout_writer import GoogleSheetsApiLayoutWriter
from src.writers.google_sheets_ui_writer import GoogleSheetsUIWriter
from src.writers.layout_dsl_routing import execution_input_to_dict, parse_dsl_execution_inputs
from src.writers.models import WriterDestinationConfig
from src.parsers.weekly_refusals_parser import parse_weekly_refusals_rows
from src.writers.weekly_refusals_block_writer import WeeklyRefusalsBlockWriter

@dataclass(frozen=True)
class RuntimeOptions:
    google_auth_mode: str
    weekly_period_strategy_override: str | None = None
    weekly_period_mode_override: str | None = None
    weekly_date_from_override: str | None = None
    weekly_date_to_override: str | None = None


def _clean_optional_text(value: str | None) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def _build_runtime_options(args: argparse.Namespace, effective_google_auth_mode: str) -> RuntimeOptions:
    return RuntimeOptions(
        google_auth_mode=effective_google_auth_mode,
        weekly_period_strategy_override=_clean_optional_text(getattr(args, "weekly_period_strategy", None)),
        weekly_period_mode_override=_clean_optional_text(getattr(args, "weekly_period_mode", None)),
        weekly_date_from_override=_clean_optional_text(getattr(args, "weekly_date_from", None)),
        weekly_date_to_override=_clean_optional_text(getattr(args, "weekly_date_to", None)),
    )

def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}

def _normalize_google_auth_mode(raw: str | None) -> str:
    value = str(raw or "").strip().lower()
    if value in {"auto", "cache_only", "interactive_bootstrap"}:
        return value
    return "auto"


def _resolve_google_auth_mode(cli_value: str | None) -> str:
    if str(cli_value or "").strip():
        return _normalize_google_auth_mode(cli_value)
    return _normalize_google_auth_mode(os.getenv("GOOGLE_API_AUTH_MODE", "auto"))


def _apply_google_auth_mode_override(cli_value: str | None) -> str:
    mode = _resolve_google_auth_mode(cli_value)
    os.environ["GOOGLE_API_AUTH_MODE"] = mode
    return mode


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run profile-driven analytics flow: open analytics, set filter, capture all/active/closed. "
            "Read/report safe mode only."
        )
    )
    parser.add_argument("--report-id", required=True, help="Report profile id from config/report_profiles.yaml")
    parser.add_argument(
        "--tag-selection-mode",
        choices=["script", "agent_assisted", "external_agent"],
        default=None,
        help="Tag selection mode for source_kind=tag: script (default), agent_assisted, or external_agent.",
    )
    parser.add_argument(
        "--browser-backend",
        choices=["playwright_local", "openclaw_cdp"],
        default=None,
        help="Browser backend: launch local Playwright Chromium or connect to OpenClaw CDP.",
    )
    parser.add_argument(
        "--wait-for-enter",
        action="store_true",
        help="Pause before starting flow so user can login manually in browser, then press Enter.",
    )
    parser.add_argument(
        "--external-agent-bridge-cmd",
        default=None,
        help=(
            "Optional command for external-agent bridge. "
            "If set, command is executed after handoff prep in tag external_agent mode."
        ),
    )
    parser.add_argument(
        "--external-agent-bridge-timeout-sec",
        type=int,
        default=None,
        help="Timeout for external-agent bridge command in seconds (default: 180).",
    )
    parser.add_argument(
        "--writer-debug-cell-nav",
        action="store_true",
        help=(
            "Run Google Sheets writer navigation debug only (A1->B2->A1), "
            "without clearing or writing TSV data."
        ),
    )
    parser.add_argument(
        "--writer-debug-cell-nav-only",
        action="store_true",
        help=(
            "Run only Google Sheets writer cell navigation debug flow (A1->B2->A1) "
            "without running analytics capture."
        ),
    )
    parser.add_argument(
        "--writer-layout-dry-run",
        action="store_true",
        help=(
            "Dry-run mode for layout writer: resolve blocks/headers/stage mapping and planned writes "
            "without writing values to sheet."
        ),
    )
    parser.add_argument(
        "--writer-layout-grid-inspector-only",
        action="store_true",
        help=(
            "Run isolated Google Sheets layout grid inspector (visible-area diagnostics only) "
            "without analytics flow, discovery scan, or writing."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-inspector-only",
        action="store_true",
        help=(
            "Run isolated Google Sheets API read-only layout discovery inspector "
            "without browser discovery/write and without amoCRM execution."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-write",
        action="store_true",
        help=(
            "Use Google Sheets API layout write path (batch update by discovered anchors) "
            "instead of browser layout writer."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-dry-run",
        action="store_true",
        help=(
            "Dry-run for Google Sheets API layout write path: build anchor/stage plan and artifacts "
            "without actual batch updates."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-preferred",
        action="store_true",
        help=(
            "Preferred production routing for google_sheets_layout_ui: "
            "run browser capture + Google Sheets API layout write. "
            "UI layout writer stays fallback/debug."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-fallback-to-ui",
        action="store_true",
        help=(
            "When API-preferred write fails, fallback to browser UI layout writer "
            "instead of hard-failing."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-write-from-latest-compiled",
        action="store_true",
        help=(
            "Run isolated Google Sheets API layout write path from latest exports/compiled artifacts "
            "without browser/amoCRM flow."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-target-dsl-row",
        type=int,
        default=None,
        help="Optional target anchor selector for API layout writer: exact DSL row number.",
    )
    parser.add_argument(
        "--writer-layout-api-target-dsl-text-contains",
        default=None,
        help="Optional target anchor selector for API layout writer: substring match in DSL text.",
    )
    parser.add_argument(
        "--writer-layout-api-target-dsl-cell",
        default=None,
        help="Optional target anchor selector for API layout writer: exact DSL command cell (e.g. A1, F14).",
    )
    parser.add_argument(
        "--writer-layout-api-batch-from-sheet-dsl-dry-run",
        action="store_true",
        help=(
            "Batch mode: read DSL anchors from sheet, parse source mapping for each block, "
            "log execution plan, without amoCRM capture and without sheet writes."
        ),
    )
    parser.add_argument(
        "--writer-layout-api-batch-from-sheet-dsl",
        action="store_true",
        help=(
            "Batch mode: read DSL anchors from sheet, execute per-block amoCRM scenarios, "
            "build per-block compiled result, and write each block via API writer."
        ),
    )
    parser.add_argument(
        "--execution-from-sheet-dsl",
        action="store_true",
        help=(
            "Build analytics execution input from a discovered Google Sheets DSL anchor "
            "instead of static report_profile filter_values."
        ),
    )
    parser.add_argument(
        "--google-auth-mode",
        choices=["auto", "cache_only", "interactive_bootstrap"],
        default=None,
        help=(
            "Google API auth mode override for current process. "
            "Use cache_only in normal runtime to avoid interactive system-browser OAuth popups."
        ),
    )
    parser.add_argument(
        "--execution-source-target-id",
        default=None,
        help=(
            "Optional table_mappings target_id used only as DSL source for --execution-from-sheet-dsl. "
            "Writer destination remains report.output.target_id."
        ),
    )
    parser.add_argument(
        "--weekly-period-strategy",
        choices=["current_week", "previous_week", "auto_weekly", "monday_current_else_previous", "manual_range"],
        default=None,
        help="Override weekly refusals period strategy for this run.",
    )
    parser.add_argument(
        "--weekly-period-mode",
        default=None,
        help="Direct period mode override for weekly refusals (for manual_range/runtime experiments).",
    )
    parser.add_argument(
        "--weekly-date-from",
        default=None,
        help="Override weekly refusals date_from for current run.",
    )
    parser.add_argument(
        "--weekly-date-to",
        default=None,
        help="Override weekly refusals date_to for current run.",
    )
    return parser


def _wait_for_login_ready() -> None:
    print()
    print("Profile flow preparation:")
    print("1) If needed, login to amoCRM in browser window.")
    print("2) Return to terminal.")
    input("Press Enter to start profile flow... ")


def _normalize_tabs(raw_tabs: list[str]) -> list[TabMode]:
    tabs: list[TabMode] = []
    for item in raw_tabs:
        value = str(item).strip().lower()
        if value in ("all", "active", "closed"):
            tabs.append(value)  # type: ignore[arg-type]
    if not tabs:
        return ["all", "active", "closed"]

    unique: list[TabMode] = []
    seen: set[str] = set()
    for tab in tabs:
        if tab in seen:
            continue
        seen.add(tab)
        unique.append(tab)
    return unique


def _parse_dsl_cell_ref(cell: str | None) -> tuple[int, int] | None:
    raw = str(cell or "").strip().upper()
    if not raw:
        return None
    import re

    m = re.match(r"^([A-Z]+)(\d+)$", raw)
    if not m:
        return None
    col_s, row_s = m.groups()
    row = int(row_s)
    col = 0
    for ch in col_s:
        col = col * 26 + (ord(ch) - ord("A") + 1)
    if row <= 0 or col <= 0:
        return None
    return row, col


def _anchor_sort_key(anchor: dict[str, Any]) -> tuple[int, int]:
    return (int(anchor.get("dsl_row", 0) or 0), int(anchor.get("dsl_col", 0) or 0))


def _anchor_dsl_cell(anchor: dict[str, Any]) -> str:
    row = int(anchor.get("dsl_row", 0) or 0)
    col = int(anchor.get("dsl_col", 0) or 0)
    if row <= 0 or col <= 0:
        return ""
    label = ""
    x = col
    while x > 0:
        x, rem = divmod(x - 1, 26)
        label = chr(ord("A") + rem) + label
    return f"{label}{row}"


def _select_execution_anchor_from_discovery(
    anchors: list[dict[str, Any]],
    *,
    target_dsl_row: int | None,
    target_dsl_text_contains: str | None,
    target_dsl_cell: str | None = None,
) -> dict[str, Any] | None:
    rows = [a for a in anchors if isinstance(a, dict)]
    if not rows:
        return None

    rows.sort(key=_anchor_sort_key)

    parsed_cell = _parse_dsl_cell_ref(target_dsl_cell)
    if parsed_cell is not None:
        row_n, col_n = parsed_cell
        matched = [a for a in rows if int(a.get("dsl_row", 0) or 0) == row_n and int(a.get("dsl_col", 0) or 0) == col_n]
        if target_dsl_text_contains:
            needle = str(target_dsl_text_contains).strip().lower()
            matched = [a for a in matched if needle in str(a.get("dsl_text", "")).lower()]
        if matched:
            return matched[0]

    if target_dsl_row is not None:
        matched = [a for a in rows if int(a.get("dsl_row", 0) or 0) == int(target_dsl_row)]
        if target_dsl_text_contains:
            needle = str(target_dsl_text_contains).strip().lower()
            matched = [a for a in matched if needle in str(a.get("dsl_text", "")).lower()]
        if matched:
            return matched[0]

    if target_dsl_text_contains:
        needle = str(target_dsl_text_contains).strip().lower()
        matched = [a for a in rows if needle in str(a.get("dsl_text", "")).lower()]
        if matched:
            return matched[0]

    return rows[0]


def _map_dsl_source_to_flow(input_source_kind: str, operator: str) -> tuple[SourceKind, str]:
    kind = str(input_source_kind or "").strip().lower()
    op = str(operator or "=").strip()
    if kind == "tag":
        return "tag", "="
    if kind in {"utm_exact", "utm_source"}:
        return "utm_source", "="
    if kind == "utm_prefix":
        return "utm_source", "^="
    raise RuntimeError(f"Unsupported DSL execution source_kind={input_source_kind}")


def _resolve_execution_override_from_sheet_dsl(
    *,
    config,
    logger,
    execution_destination: WriterDestinationConfig,
    execution_target_id: str,
    writer_target_id: str,
    writer_destination: WriterDestinationConfig,
    default_tabs: list[TabMode],
    target_dsl_row: int | None,
    target_dsl_text_contains: str | None,
    target_dsl_cell: str | None = None,
) -> dict[str, Any]:
    logger.info("execution_source=sheet_dsl")
    logger.info("execution_input_target_id=%s", execution_target_id)
    logger.info("execution_input_tab_name=%s", execution_destination.tab_name)
    logger.info("writer_target_id=%s", writer_target_id)
    logger.info("writer_tab_name=%s", writer_destination.tab_name)

    inspector = GoogleSheetsApiLayoutInspector(project_root=config.project_root, logger=logger)
    discovery = inspector.inspect(destination=execution_destination)
    anchors = discovery.get("anchors", []) if isinstance(discovery, dict) else []
    if not anchors:
        raise RuntimeError("execution-from-sheet-dsl: no anchors found in discovery")

    selected_anchor = _select_execution_anchor_from_discovery(
        anchors=anchors,
        target_dsl_row=target_dsl_row,
        target_dsl_text_contains=target_dsl_text_contains,
        target_dsl_cell=target_dsl_cell,
    )
    if selected_anchor is None:
        raise RuntimeError("execution-from-sheet-dsl: could not select anchor")

    dsl_row = int(selected_anchor.get("dsl_row", 0) or 0)
    dsl_text = str(selected_anchor.get("dsl_text", "") or "").strip()
    if not dsl_text:
        raise RuntimeError(f"execution-from-sheet-dsl: selected anchor dsl_row={dsl_row} has empty dsl_text")

    _block_cfg, execution_inputs = parse_dsl_execution_inputs(dsl_text)
    if not execution_inputs:
        raise RuntimeError(f"execution-from-sheet-dsl: no execution inputs parsed for dsl_row={dsl_row}")

    # Runtime currently executes one source filter scenario/value per run.
    # If DSL row contains multiple source scenarios/values, we select the first supported one.
    selected_input = None
    for item in execution_inputs:
        if str(item.filter_value or "").strip() and str(item.source_kind or "").strip() in {"tag", "utm_exact", "utm_prefix", "utm_source"}:
            selected_input = item
            break

    if selected_input is None:
        raise RuntimeError(f"execution-from-sheet-dsl: no supported input parsed for dsl_row={dsl_row}")

    source_kind, filter_operator = _map_dsl_source_to_flow(selected_input.source_kind, selected_input.filter_operator)
    logger.info("dsl_execution_mode=single_value_first_supported")
    override_tabs = _normalize_tabs(selected_input.tabs) if selected_input.tabs else list(default_tabs)

    return {
        "dsl_row": dsl_row,
        "dsl_col": int(selected_anchor.get("dsl_col", 0) or 0),
        "dsl_cell": _anchor_dsl_cell(selected_anchor),
        "dsl_text": dsl_text,
        "source_kind": source_kind,
        "filter_values": [str(selected_input.filter_value).strip()],
        "tabs": override_tabs,
        "filter_operator": filter_operator,
    }




def _resolve_weekly_period_mode(current_date: date | None = None) -> str:
    day = current_date or date.today()
    # Monday=0 ... Sunday=6
    return "\u0417\u0430 \u044d\u0442\u0443 \u043d\u0435\u0434\u0435\u043b\u044e" if day.weekday() == 0 else "\u0417\u0430 \u043f\u0440\u043e\u0448\u043b\u0443\u044e \u043d\u0435\u0434\u0435\u043b\u044e"


def _build_weekly_refusals_flow_input(report, runtime_options: RuntimeOptions | None = None) -> EventsFlowInput:
    filters = dict(getattr(report, "filters", {}) or {})
    date_cfg = dict(filters.get("date", {}) or {})
    options = runtime_options or RuntimeOptions(google_auth_mode="auto")

    pipeline_name = str(filters.get("pipeline", "")).strip()
    if not pipeline_name:
        raise RuntimeError("Weekly refusals profile requires filters.pipeline")

    status_after = str(filters.get("status_after", "")).strip() or "\u0417\u0430\u043a\u0440\u044b\u0442\u043e \u0438 \u043d\u0435 \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e"
    status_before = str(filters.get("status_before", "")).strip()
    status_before_values_raw = filters.get("status_before_values", [])
    status_before_values = [str(v).strip() for v in (status_before_values_raw or []) if str(v).strip()]

    date_mode = str(filters.get("date_mode", date_cfg.get("mode", "\u0421\u043e\u0437\u0434\u0430\u043d\u044b"))).strip() or "\u0421\u043e\u0437\u0434\u0430\u043d\u044b"
    period_strategy_raw = str(options.weekly_period_strategy_override or filters.get("period_strategy", "monday_current_else_previous")).strip() or "monday_current_else_previous"

    explicit_period_override = str(options.weekly_period_mode_override or filters.get("period_mode_override", "")).strip()
    configured_period_mode = str(filters.get("period_mode", date_cfg.get("period", ""))).strip()
    mode = str(filters.get("mode", "")).strip().lower()

    if explicit_period_override:
        period_mode = explicit_period_override
        period_resolution = "explicit_override"
        period_strategy_resolved = "manual_range"
    else:
        strategy_norm = period_strategy_raw.strip().lower()
        if strategy_norm in {"", "auto", "auto_weekly"}:
            strategy_norm = "monday_current_else_previous"

        if strategy_norm == "current_week":
            period_mode = "\u0417\u0430 \u044d\u0442\u0443 \u043d\u0435\u0434\u0435\u043b\u044e"
            period_strategy_resolved = "current_week"
            period_resolution = "strategy_current_week"
        elif strategy_norm == "previous_week":
            period_mode = "\u0417\u0430 \u043f\u0440\u043e\u0448\u043b\u0443\u044e \u043d\u0435\u0434\u0435\u043b\u044e"
            period_strategy_resolved = "previous_week"
            period_resolution = "strategy_previous_week"
        elif strategy_norm == "manual_range":
            period_mode = configured_period_mode or "\u0417\u0430 \u043f\u0435\u0440\u0438\u043e\u0434"
            period_strategy_resolved = "manual_range"
            period_resolution = "strategy_manual_range"
        elif strategy_norm == "monday_current_else_previous":
            period_mode = _resolve_weekly_period_mode()
            period_strategy_resolved = "monday_current_else_previous"
            period_resolution = "strategy_monday_current_else_previous"
        elif mode == "weekly" and configured_period_mode.lower() in {"", "auto", "auto_weekly"}:
            period_mode = _resolve_weekly_period_mode()
            period_strategy_resolved = "monday_current_else_previous"
            period_resolution = "weekly_fallback_strategy"
        else:
            period_mode = configured_period_mode or "\u0417\u0430 \u043f\u0440\u043e\u0448\u043b\u0443\u044e \u043d\u0435\u0434\u0435\u043b\u044e"
            period_strategy_resolved = "profile_config"
            period_resolution = "profile_config"

    date_from = str(options.weekly_date_from_override or filters.get("date_from", date_cfg.get("from", ""))).strip()
    date_to = str(options.weekly_date_to_override or filters.get("date_to", date_cfg.get("to", ""))).strip()

    entity_kind = str(filters.get("entity_kind", "\u0421\u0434\u0435\u043b\u043a\u0438")).strip() or "\u0421\u0434\u0435\u043b\u043a\u0438"
    event_type = str(filters.get("event_type", "\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")).strip() or "\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"

    managers_raw = filters.get("managers", [])
    managers = [str(v).strip() for v in (managers_raw or []) if str(v).strip()]

    filter_mode = str(filters.get("filter_mode", "ui_controls")).strip() or "ui_controls"
    saved_preset_name = str(filters.get("saved_preset_name", "")).strip()
    saved_preset_exact_match = _as_bool(filters.get("saved_preset_exact_match", False))

    return EventsFlowInput(
        report_id=str(report.id),
        pipeline_name=pipeline_name,
        date_mode=date_mode,
        period_mode=period_mode,
        date_from=date_from,
        date_to=date_to,
        status_before=status_before,
        status_before_values=status_before_values,
        status_after=status_after,
        entity_kind=entity_kind,
        event_type=event_type,
        period_strategy=f"raw={period_strategy_raw}|resolved={period_strategy_resolved}|mode={period_resolution}",
        managers=managers,
        filter_mode=filter_mode,
        saved_preset_name=saved_preset_name,
        saved_preset_exact_match=saved_preset_exact_match,
    )


def _run_weekly_refusals_profile(
    *,
    config,
    settings,
    logger,
    report,
    destination: WriterDestinationConfig,
    wait_for_enter: bool,
    weekly_dry_run: bool,
    runtime_options: RuntimeOptions | None = None,
) -> None:
    logger.info("weekly refusals flow started: report_id=%s", report.id)
    logger.info("weekly_refusals_runner_version=parsed_to_dict_fix_v1")
    flow_input = _build_weekly_refusals_flow_input(report, runtime_options=runtime_options)
    strategy_parts = str(flow_input.period_strategy or "").split("|")
    strategy_raw = ""
    strategy_resolved = ""
    strategy_mode = ""
    for part in strategy_parts:
        if part.startswith("raw="):
            strategy_raw = part.split("=", 1)[1]
        elif part.startswith("resolved="):
            strategy_resolved = part.split("=", 1)[1]
        elif part.startswith("mode="):
            strategy_mode = part.split("=", 1)[1]
    logger.info(
        "weekly period strategy resolved: report_id=%s raw_strategy=%s resolved_strategy=%s resolution_mode=%s resolved_period_mode=%s",
        report.id,
        strategy_raw,
        strategy_resolved,
        strategy_mode,
        flow_input.period_mode,
    )

    with BrowserSession(settings) as session:
        page = session.new_page()
        if wait_for_enter:
            logger.info("Wait-for-enter mode enabled before weekly refusals flow start.")
            _wait_for_login_ready()

        events_flow = EventsFlow(settings=settings, project_root=config.project_root)
        rows = events_flow.run_capture(page=page, flow_input=flow_input)

    parsed = parse_weekly_refusals_rows(
        report_id=report.id,
        display_name=report.display_name,
        rows=rows,
    )

    compiled_dir = ensure_inside_root(config.exports_dir / "compiled", config.project_root)
    compiled_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    compiled_path = ensure_inside_root(
        compiled_dir / f"weekly_refusals_{report.id}_{stamp}.json",
        config.project_root,
    )
    parsed_payload = parsed.to_dict()
    parsed_payload["mode"] = str((report.filters or {}).get("mode", "weekly") or "weekly").strip().lower() or "weekly"
    parsed_payload["cumulative_write_strategy"] = str(
        (report.filters or {}).get("cumulative_write_strategy", "recompute_from_source") or "recompute_from_source"
    ).strip().lower() or "recompute_from_source"
    parsed_payload["period_mode"] = str(flow_input.period_mode or "")
    parsed_payload["date_mode"] = str(flow_input.date_mode or "")
    parsed_payload["date_from"] = str(flow_input.date_from or "")
    parsed_payload["date_to"] = str(flow_input.date_to or "")
    parsed_payload["period_key"] = (
        f"{report.id}|{parsed_payload['period_mode']}|{parsed_payload['date_from']}|{parsed_payload['date_to']}"
    )
    parsed_payload["writer_mode_semantics"] = (
        "cumulative_add_existing_values"
        if parsed_payload["mode"] == "cumulative" and parsed_payload["cumulative_write_strategy"] in {"add", "add_existing_values"}
        else "recompute_from_source"
    )
    compiled_path.write_text(json.dumps(parsed_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(
        "weekly refusals artifact saved: %s mode=%s semantics=%s cumulative_strategy=%s period_mode=%s period_key=%s",
        compiled_path,
        parsed_payload["mode"],
        parsed_payload["writer_mode_semantics"],
        parsed_payload["cumulative_write_strategy"],
        flow_input.period_mode,
        parsed_payload["period_key"],
    )

    writer = WeeklyRefusalsBlockWriter(
        project_root=config.project_root,
        exports_dir=config.exports_dir,
        logger=logger,
    )
    try:
        write_result = writer.write_block(
            destination=destination,
            parsed_result=parsed_payload,
            dry_run=bool(weekly_dry_run),
        )
        logger.info(
            "weekly refusals write result: dry_run=%s planned_updates=%s updated_cells=%s summary=%s",
            str(write_result.dry_run).lower(),
            write_result.planned_updates,
            write_result.updated_cells,
            write_result.summary_path,
        )
    except RuntimeError as exc:
        message = str(exc)
        if "Weekly refusals anchor not found" in message:
            logger.error("weekly refusals block skipped: report_id=%s reason=%s", report.id, message)
            return
        raise


def _resolve_source_and_values(filters: dict[str, object]) -> tuple[SourceKind, list[str]]:
    # New profile format (preferred).
    filter_source = str(filters.get("filter_source", "")).strip().lower()
    filter_values_raw = filters.get("filter_values", [])

    # Legacy fallback format.
    if not filter_source:
        filter_source = str(filters.get("type", "")).strip().lower()
    if not filter_values_raw:
        filter_values_raw = filters.get("include", [])

    if filter_source not in ("tag", "utm_source"):
        raise RuntimeError(
            "Unsupported filter source in report profile. "
            "Use filter_source=tag or filter_source=utm_source."
        )

    values = [str(v).strip() for v in (filter_values_raw or []) if str(v).strip()]
    # Current report-profile execution keeps all provided values and passes them to UI handlers.
    # Explicit boolean semantics (OR/AND) are not configured by this layer and depend on CRM widget behavior.
    if not values:
        raise RuntimeError("Report profile filter_values is empty. Add at least one value.")

    return filter_source, values  # type: ignore[return-value]


def _export_snapshot(reader: AmoAnalyticsReader, logger, snapshot: AnalyticsSnapshot, exported_tabs: list[str]) -> None:
    json_path, csv_path = reader.export_snapshot(snapshot)
    exported_tabs.append(snapshot.tab_mode)
    logger.info(
        "Saved tab=%s stages=%s total_count=%s parse_method=%s",
        snapshot.tab_mode,
        len(snapshot.stages),
        snapshot.total_count,
        snapshot.parse_method,
    )
    logger.info(
        "Exports for tab=%s: json=%s csv=%s screenshot=%s",
        snapshot.tab_mode,
        json_path,
        csv_path,
        snapshot.screenshot_path,
    )


def _resolve_writer_destination(report, table_mappings: dict, logger) -> WriterDestinationConfig:
    target_id = str(report.output.get("target_id", "")).strip()
    mapping = table_mappings.get(target_id)
    if mapping is None:
        raise RuntimeError(
            f"Writer destination mapping not found for target_id={target_id}. "
            "Configure config/table_mappings.yaml."
        )

    sheet_url = str(mapping.sheet_url or os.getenv("GOOGLE_SHEETS_TEST_URL", "")).strip()
    tab_name = str(mapping.tab_name or mapping.target_sheet_name or "analytics_writer_test").strip()
    start_cell = str(mapping.start_cell or "A1").strip() or "A1"
    write_mode = str(mapping.write_mode or "overwrite_tab").strip() or "overwrite_tab"

    if not sheet_url:
        raise RuntimeError(
            "Writer destination sheet_url is empty. Set table_mappings.yaml:sheet_url or "
            "env GOOGLE_SHEETS_TEST_URL."
        )

    logger.info(
        "writer destination resolved: target_id=%s destination_kind=%s sheet_url_present=%s tab_name=%s write_mode=%s start_cell=%s",
        target_id,
        str(mapping.kind or "google_sheets_ui"),
        str(bool(sheet_url)).lower(),
        tab_name,
        write_mode,
        start_cell,
    )

    return WriterDestinationConfig(
        sheet_url=sheet_url,
        tab_name=tab_name,
        write_mode=write_mode,
        start_cell=start_cell,
        kind=str(mapping.kind or "google_sheets_ui"),
        target_id=target_id,
        layout_config=dict(getattr(mapping, "layout", {}) or {}),
    )



def _resolve_execution_input_destination(
    report,
    table_mappings: dict,
    logger,
    override_target_id: str | None = None,
) -> tuple[str, WriterDestinationConfig]:
    configured_target_id = str((getattr(report, "execution_input", {}) or {}).get("target_id", "")).strip()
    target_id = str(override_target_id or "").strip() or configured_target_id

    if not target_id:
        writer_target_id = str(report.output.get("target_id", "")).strip()
        logger.warning(
            "execution_input.target_id is not set; fallback to writer destination target_id=%s",
            writer_target_id,
        )
        target_id = writer_target_id

    mapping = table_mappings.get(target_id)
    if mapping is None:
        raise RuntimeError(
            f"Execution input destination mapping not found for target_id={target_id}. "
            "Configure report_profiles.yaml:execution_input.target_id or --execution-source-target-id."
        )

    sheet_url = str(mapping.sheet_url or os.getenv("GOOGLE_SHEETS_TEST_URL", "")).strip()
    tab_name = str(mapping.tab_name or mapping.target_sheet_name or "analytics_writer_test").strip()
    start_cell = str(mapping.start_cell or "A1").strip() or "A1"
    write_mode = str(mapping.write_mode or "overwrite_tab").strip() or "overwrite_tab"

    if not sheet_url:
        raise RuntimeError(
            "Execution input destination sheet_url is empty. Set table_mappings.yaml:sheet_url or "
            "env GOOGLE_SHEETS_TEST_URL."
        )

    logger.info(
        "execution input destination resolved: target_id=%s destination_kind=%s sheet_url_present=%s tab_name=%s write_mode=%s start_cell=%s",
        target_id,
        str(mapping.kind or "google_sheets_ui"),
        str(bool(sheet_url)).lower(),
        tab_name,
        write_mode,
        start_cell,
    )

    return target_id, WriterDestinationConfig(
        sheet_url=sheet_url,
        tab_name=tab_name,
        write_mode=write_mode,
        start_cell=start_cell,
        kind=str(mapping.kind or "google_sheets_ui"),
        target_id=target_id,
        layout_config=dict(getattr(mapping, "layout", {}) or {}),
    )



def _load_compiled_result_from_json(path: Path) -> Any:
    data = json.loads(path.read_text(encoding="utf-8"))
    generated_raw = str(data.get("generated_at", "")).strip()
    try:
        generated_at = datetime.fromisoformat(generated_raw) if generated_raw else datetime.now()
    except Exception:
        generated_at = datetime.now()

    from src.writers.models import CompiledProfileAnalyticsResult

    return CompiledProfileAnalyticsResult(
        report_id=str(data.get("report_id", "")).strip(),
        display_name=str(data.get("display_name", "")).strip(),
        generated_at=generated_at,
        source_kind=str(data.get("source_kind", "")).strip(),
        filter_values=[str(v) for v in (data.get("filter_values", []) or [])],
        tabs=[str(v) for v in (data.get("tabs", []) or [])],
        top_cards_by_tab=dict(data.get("top_cards_by_tab", {}) or {}),
        stages_by_tab=dict(data.get("stages_by_tab", {}) or {}),
        totals_by_tab={k: int(v or 0) for k, v in dict(data.get("totals_by_tab", {}) or {}).items()},
    )


def _build_compiled_result_from_pivot(report, source_kind: str, filter_values: list[str], pivot: dict[str, Any]):
    from src.writers.models import CompiledProfileAnalyticsResult

    stages_by_tab: dict[str, list[dict[str, Any]]] = {"all": [], "active": [], "closed": []}
    totals_by_tab: dict[str, int] = {"all": 0, "active": 0, "closed": 0}

    ordered = list(pivot.items())
    for idx, (stage_name, vals) in enumerate(ordered, start=1):
        for tab in ("all", "active", "closed"):
            count = int((vals or {}).get(tab, 0) or 0)
            stages_by_tab[tab].append(
                {
                    "tab": tab,
                    "stage_index": idx,
                    "stage_name": stage_name,
                    "deals_count": count,
                    "budget_text": "",
                    "raw_line": "",
                }
            )
            totals_by_tab[tab] += count

    return CompiledProfileAnalyticsResult(
        report_id=report.id,
        display_name=report.display_name,
        generated_at=datetime.now(),
        source_kind=source_kind,
        filter_values=filter_values,
        tabs=["all", "active", "closed"],
        top_cards_by_tab={"all": [], "active": [], "closed": []},
        stages_by_tab=stages_by_tab,
        totals_by_tab=totals_by_tab,
    )


def _log_latest_api_layout_summary(logger, exports_dir: Path) -> None:
    debug_dir = exports_dir / "debug"
    if not debug_dir.exists():
        return
    files = [p for p in debug_dir.glob("layout_api_write_summary_*.json") if p.is_file()]
    if not files:
        return
    latest = max(files, key=lambda p: p.stat().st_mtime)
    try:
        payload = json.loads(latest.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not read latest layout_api_write_summary: %s error=%s", latest, exc)
        return

    logger.info("latest_layout_api_write_summary=%s", latest)
    logger.info("selected_anchor=%s", payload.get("anchor"))
    logger.info("target_selector=%s", payload.get("target_selector"))
    logger.info("next_anchor_dsl_row=%s", payload.get("next_anchor_dsl_row"))
    logger.info("hard_row_upper_bound=%s", payload.get("hard_row_upper_bound"))
    logger.info("stage_rows_selected_count=%s", payload.get("stage_rows_selected_count"))
    logger.info("stop_reason=%s", payload.get("stop_reason"))


def _run_api_layout_writer_from_latest_compiled(
    *,
    config,
    logger,
    report,
    destination,
    source_kind: str,
    filter_values: list[str],
    dry_run: bool,
    target_dsl_row: int | None = None,
    target_dsl_text_contains: str | None = None,
    target_dsl_cell: str | None = None,
) -> None:
    compiled_stage_pivot_path = find_latest_compiled_artifact(
        exports_dir=config.exports_dir,
        pattern="compiled_stage_pivot_*.json",
        report_id=report.id,
    )
    if compiled_stage_pivot_path is None:
        raise RuntimeError("No compiled_stage_pivot_*.json found in exports/compiled")

    compiled_profile_path = find_latest_compiled_artifact(
        exports_dir=config.exports_dir,
        pattern="compiled_profile_*.json",
        report_id=report.id,
    )

    logger.info("compiled_stage_pivot_path=%s", compiled_stage_pivot_path)
    logger.info("compiled_profile_path=%s", compiled_profile_path if compiled_profile_path else "")

    pivot = json.loads(compiled_stage_pivot_path.read_text(encoding="utf-8"))
    if not isinstance(pivot, dict):
        raise RuntimeError("Invalid compiled stage pivot JSON format")

    if compiled_profile_path is not None:
        compiled_result = _load_compiled_result_from_json(compiled_profile_path)
    else:
        compiled_result = _build_compiled_result_from_pivot(
            report=report,
            source_kind=source_kind,
            filter_values=filter_values,
            pivot=pivot,
        )

    logger.info(
        "isolated api writer target_selector input: dsl_row=%s dsl_cell=%s dsl_text_contains=%s",
        target_dsl_row,
        str(target_dsl_cell or ""),
        str(target_dsl_text_contains or ""),
    )

    api_writer = GoogleSheetsApiLayoutWriter(project_root=config.project_root, logger=logger)
    api_writer.write_profile_analytics_result(
        compiled_result=compiled_result,
        destination=destination,
        dry_run=dry_run,
        target_dsl_row=target_dsl_row,
        target_dsl_text_contains=target_dsl_text_contains,
        target_dsl_cell=target_dsl_cell,
    )
    _log_latest_api_layout_summary(logger, config.exports_dir)


def _resolve_layout_api_routing(args, destination: WriterDestinationConfig) -> dict[str, bool]:
    layout_cfg = destination.layout_config or {}
    api_preferred = bool(args.writer_layout_api_preferred or _as_bool(layout_cfg.get("api_preferred")))
    api_dry_run = bool(args.writer_layout_api_dry_run)
    api_explicit_write = bool(args.writer_layout_api_write)
    api_write_enabled = bool(api_explicit_write or api_dry_run or api_preferred)
    api_fallback_to_ui = bool(
        args.writer_layout_api_fallback_to_ui or _as_bool(layout_cfg.get("api_fallback_to_ui"))
    )
    return {
        "api_preferred": api_preferred,
        "api_dry_run": api_dry_run,
        "api_explicit_write": api_explicit_write,
        "api_write_enabled": api_write_enabled,
        "api_fallback_to_ui": api_fallback_to_ui,
    }


def _run_layout_writer_with_routing(
    *,
    logger,
    config,
    page,
    flow,
    tabs: list[TabMode],
    report,
    compiled_result,
    destination: WriterDestinationConfig,
    layout_dry_run: bool,
    api_write_enabled: bool,
    api_preferred: bool,
    api_dry_run: bool,
    api_fallback_to_ui: bool,
    target_dsl_row: int | None,
    target_dsl_text_contains: str | None,
    target_dsl_cell: str | None = None,
    api_writer_factory=GoogleSheetsApiLayoutWriter,
    ui_writer_factory=GoogleSheetsUILayoutWriter,
) -> tuple[str, bool]:
    fallback_used = False
    if not api_write_enabled:
        writer = ui_writer_factory(project_root=config.project_root)
        scenario_executor = ScenarioExecutor(
            flow=flow,
            project_root=config.project_root,
            tabs=tabs,
            report_id=report.id,
        )
        writer.write_profile_analytics_result(
            page=page,
            compiled_result=compiled_result,
            destination=destination,
            dry_run=layout_dry_run,
            scenario_executor=scenario_executor,
        )
        return ("layout_ui", fallback_used)

    writer_mode = "api_preferred" if api_preferred else "api_opt_in"
    logger.info("writer mode selected = %s", writer_mode)
    logger.info("api discovery start")
    try:
        api_writer = api_writer_factory(project_root=config.project_root, logger=logger)
        api_writer.write_profile_analytics_result(
            compiled_result=compiled_result,
            destination=destination,
            dry_run=api_dry_run,
            target_dsl_row=target_dsl_row,
            target_dsl_text_contains=target_dsl_text_contains,
            target_dsl_cell=target_dsl_cell,
        )
        logger.info("api discovery finish")
        logger.info("api write success")
        return (writer_mode, fallback_used)
    except Exception as exc:
        logger.error("api write fail: %s", exc)
        if not (api_preferred and api_fallback_to_ui):
            raise
        logger.warning("api preferred failed; fallback to UI layout writer enabled")
        fallback_used = True
        ui_writer = ui_writer_factory(project_root=config.project_root)
        scenario_executor = ScenarioExecutor(
            flow=flow,
            project_root=config.project_root,
            tabs=tabs,
            report_id=report.id,
        )
        ui_writer.write_profile_analytics_result(
            page=page,
            compiled_result=compiled_result,
            destination=destination,
            dry_run=layout_dry_run,
            scenario_executor=scenario_executor,
        )
        return ("layout_ui_fallback", fallback_used)



def _get_latest_api_layout_summary(exports_dir: Path) -> tuple[Path | None, dict[str, Any] | None]:
    debug_dir = exports_dir / "debug"
    if not debug_dir.exists():
        return None, None
    files = [p for p in debug_dir.glob("layout_api_write_summary_*.json") if p.is_file()]
    if not files:
        return None, None
    latest = max(files, key=lambda p: p.stat().st_mtime)
    try:
        payload = json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return latest, None
    return latest, payload if isinstance(payload, dict) else None


def _run_api_layout_batch_from_sheet_dsl(
    *,
    config,
    logger,
    page,
    flow,
    report,
    tabs: list[TabMode],
    destination: WriterDestinationConfig,
    source_kind: str,
    filter_values: list[str],
    dry_run: bool,
    target_dsl_row: int | None = None,
    target_dsl_text_contains: str | None = None,
    target_dsl_cell: str | None = None,
) -> None:
    if destination.kind != "google_sheets_layout_ui":
        raise RuntimeError("batch-from-sheet-dsl requires destination kind google_sheets_layout_ui")

    inspector = GoogleSheetsApiLayoutInspector(project_root=config.project_root, logger=logger)
    discovery = inspector.inspect(destination=destination)
    anchors = discovery.get("anchors", []) if isinstance(discovery, dict) else []
    if not anchors:
        raise RuntimeError("Batch DSL mode: no anchors found by API discovery")

    sorted_anchors = sorted(
        [a for a in anchors if isinstance(a, dict)],
        key=_anchor_sort_key,
    )

    if target_dsl_row is not None or (target_dsl_text_contains or "").strip() or (target_dsl_cell or "").strip():
        selected = _select_execution_anchor_from_discovery(
            anchors=sorted_anchors,
            target_dsl_row=target_dsl_row,
            target_dsl_text_contains=target_dsl_text_contains,
            target_dsl_cell=target_dsl_cell,
        )
        if selected is None:
            raise RuntimeError("Batch DSL mode: target selector did not match any anchor")
        sorted_anchors = [selected]

    logger.info(
        "api batch from sheet dsl start: anchors=%s dry_run=%s",
        len(sorted_anchors),
        str(bool(dry_run)).lower(),
    )

    scenario_executor = ScenarioExecutor(
        flow=flow,
        project_root=config.project_root,
        tabs=tabs,
        report_id=report.id,
    )
    api_writer = GoogleSheetsApiLayoutWriter(project_root=config.project_root, logger=logger)

    summary_rows: list[dict[str, Any]] = []
    successes = 0

    for anchor in sorted_anchors:
        dsl_row = int(anchor.get("dsl_row", 0) or 0)
        dsl_col = int(anchor.get("dsl_col", 0) or 0)
        dsl_cell = _anchor_dsl_cell(anchor)
        dsl_text = str(anchor.get("dsl_text", "") or "").strip()
        logger.info("batch anchor start: dsl_row=%s dsl_col=%s dsl_cell=%s dsl_text=%s", dsl_row, dsl_col, dsl_cell, dsl_text)

        row_summary: dict[str, Any] = {
            "dsl_row": dsl_row,
            "dsl_col": dsl_col,
            "dsl_cell": dsl_cell,
            "dsl_text": dsl_text,
            "anchor": anchor,
            "dry_run": bool(dry_run),
        }

        try:
            block_config, execution_inputs = parse_dsl_execution_inputs(dsl_text)
        except Exception as exc:
            row_summary["status"] = "parse_error"
            row_summary["error"] = str(exc)
            logger.error("batch anchor parse failed: dsl_row=%s error=%s", dsl_row, exc)
            summary_rows.append(row_summary)
            continue

        row_summary["parsed_execution_inputs"] = [execution_input_to_dict(item) for item in execution_inputs]
        for item in execution_inputs:
            logger.info(
                "parsed dsl input: dsl_row=%s source_kind=%s filter_field=%s operator=%s filter_value=%s pipeline=%s period=%s date_mode=%s",
                dsl_row,
                item.source_kind,
                item.filter_field,
                item.filter_operator,
                item.filter_value,
                item.pipeline_name,
                item.period,
                item.date_mode,
            )

        try:
            block_result = scenario_executor.execute_block_scenarios(page=page, block_config=block_config)
            if block_result.best_compiled_result is None:
                row_summary["status"] = "execution_failed"
                row_summary["error"] = "no successful scenario"
                logger.error("batch anchor execution failed: dsl_row=%s no successful scenario", dsl_row)
                summary_rows.append(row_summary)
                continue

            compiled_result = block_result.best_compiled_result
            logger.info(
                "execution source_kind: dsl_row=%s source_kind=%s",
                dsl_row,
                compiled_result.source_kind,
            )

            compiled_json_path = save_compiled_result_json(
                compiled_result=compiled_result,
                exports_dir=config.exports_dir,
                project_root=config.project_root,
            )
            stage_aliases = destination.layout_config.get("stage_aliases", {}) if destination.layout_config else {}
            pivot = compile_stage_pivot(compiled_result=compiled_result, stage_aliases=stage_aliases)
            pivot_path = save_stage_pivot_json(
                pivot=pivot,
                report_id=compiled_result.report_id,
                exports_dir=config.exports_dir,
                project_root=config.project_root,
            )
            logger.info(
                "compiled artifact path for this block: dsl_row=%s compiled_profile=%s compiled_stage_pivot=%s",
                dsl_row,
                compiled_json_path,
                pivot_path,
            )

            response = api_writer.write_profile_analytics_result(
                compiled_result=compiled_result,
                destination=destination,
                dry_run=bool(dry_run),
                target_dsl_row=dsl_row,
                target_dsl_cell=dsl_cell,
            )
            latest_summary_path, latest_summary_payload = _get_latest_api_layout_summary(config.exports_dir)
            _log_latest_api_layout_summary(logger, config.exports_dir)

            row_summary["compiled_profile_path"] = str(compiled_json_path)
            row_summary["compiled_stage_pivot_path"] = str(pivot_path)
            row_summary["layout_api_write_summary_path"] = str(latest_summary_path) if latest_summary_path else ""
            row_summary["selected_block_boundaries"] = {
                "next_anchor_dsl_row": (latest_summary_payload or {}).get("next_anchor_dsl_row") if latest_summary_payload else None,
                "hard_row_upper_bound": (latest_summary_payload or {}).get("hard_row_upper_bound") if latest_summary_payload else None,
                "rows_considered_range": (latest_summary_payload or {}).get("rows_considered_range") if latest_summary_payload else None,
                "stage_rows_selected_count": (latest_summary_payload or {}).get("stage_rows_selected_count") if latest_summary_payload else None,
                "stop_reason": (latest_summary_payload or {}).get("stop_reason") if latest_summary_payload else None,
            }

            if dry_run:
                row_summary["status"] = "dry_run_planned"
                row_summary["planned_updates"] = int(response.get("planned_updates", 0) or 0)
                row_summary["updated_cells_count"] = 0
                logger.info(
                    "batch anchor dry-run success: dsl_row=%s planned_updates=%s",
                    dsl_row,
                    row_summary["planned_updates"],
                )
            else:
                row_summary["status"] = "written"
                row_summary["updated_cells_count"] = int(response.get("totalUpdatedCells", 0) or 0)
                logger.info(
                    "batch anchor write success: dsl_row=%s updated_cells=%s",
                    dsl_row,
                    row_summary["updated_cells_count"],
                )
            successes += 1
        except Exception as exc:
            row_summary["status"] = "write_failed"
            row_summary["error"] = str(exc)
            logger.error("batch anchor failed: dsl_row=%s error=%s", dsl_row, exc)

        summary_rows.append(row_summary)

    debug_dir = config.exports_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = debug_dir / f"layout_api_batch_from_sheet_dsl_summary_{stamp}.json"
    summary_payload = {
        "report_id": report.id,
        "source_kind": source_kind,
        "filter_values": filter_values,
        "anchors_total": len(sorted_anchors),
        "successes": successes,
        "dry_run": bool(dry_run),
        "rows": summary_rows,
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("api batch from sheet dsl summary saved: %s", summary_path)

def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    effective_google_auth_mode = _apply_google_auth_mode_override(args.google_auth_mode)
    runtime_options = _build_runtime_options(args, effective_google_auth_mode)

    config = load_config()
    ensure_inside_root(Path(os.getcwd()), config.project_root)
    ensure_project_structure(config)

    logger = setup_logging(config.logs_dir, level=os.getenv("LOG_LEVEL", "INFO"))
    logger.info("google_auth_mode_effective=%s", effective_google_auth_mode)
    logger.info("runtime_options=%s", runtime_options)
    settings = load_browser_settings(config, browser_backend_override=args.browser_backend)

    report_profiles = load_report_profiles(config)
    table_mappings = load_table_mappings(config)
    report = report_profiles.get(args.report_id)
    if report is None:
        raise RuntimeError(f"Report profile not found: {args.report_id}")

    page_type = str(report.source.get("page_type", "")).strip()

    destination = _resolve_writer_destination(report, table_mappings, logger)

    if page_type == "events_list":
        _run_weekly_refusals_profile(
            config=config,
            settings=settings,
            logger=logger,
            report=report,
            destination=destination,
            wait_for_enter=bool(args.wait_for_enter),
            weekly_dry_run=bool(args.writer_layout_api_dry_run or args.writer_layout_dry_run),
            runtime_options=runtime_options,
        )
        return

    if page_type != "analytics_sales":
        raise RuntimeError(
            f"Unsupported source.page_type={page_type}. "
            "This entrypoint currently supports analytics_sales and events_list."
        )

    source_kind, filter_values = _resolve_source_and_values(report.filters)
    tabs = _normalize_tabs(report.tabs)
    filter_operator = "="
    execution_source = "report_profile"

    writer_target_id = str(report.output.get("target_id", "")).strip()

    if bool(args.execution_from_sheet_dsl):
        execution_target_id, execution_destination = _resolve_execution_input_destination(
            report=report,
            table_mappings=table_mappings,
            logger=logger,
            override_target_id=args.execution_source_target_id,
        )
        override = _resolve_execution_override_from_sheet_dsl(
            config=config,
            logger=logger,
            execution_destination=execution_destination,
            execution_target_id=execution_target_id,
            writer_target_id=writer_target_id,
            writer_destination=destination,
            default_tabs=tabs,
            target_dsl_row=args.writer_layout_api_target_dsl_row,
            target_dsl_text_contains=args.writer_layout_api_target_dsl_text_contains,
            target_dsl_cell=args.writer_layout_api_target_dsl_cell,
        )
        execution_source = "sheet_dsl"
        source_kind = override["source_kind"]
        filter_values = list(override["filter_values"])
        tabs = list(override["tabs"])
        filter_operator = str(override.get("filter_operator", "=") or "=")

        logger.info("execution_source=%s", execution_source)
        logger.info("dsl_row=%s", override.get("dsl_row"))
        logger.info("dsl_col=%s", override.get("dsl_col"))
        logger.info("dsl_cell=%s", override.get("dsl_cell"))
        logger.info("dsl_text=%s", override.get("dsl_text"))
        logger.info("overridden_source_kind=%s", source_kind)
        logger.info("overridden_filter_values=%s", filter_values)
        logger.info("overridden_tabs=%s", tabs)
        logger.info("overridden_filter_operator=%s", filter_operator)
    else:
        logger.info("execution_source=%s", execution_source)

    resolved_tag_mode = (args.tag_selection_mode or os.getenv("TAG_SELECTION_MODE", "script")).strip().lower()
    if resolved_tag_mode not in {"script", "agent_assisted", "external_agent"}:
        resolved_tag_mode = "script"

    flow_input = AnalyticsFlowInput(
        report_id=report.id,
        source_kind=source_kind,
        filter_values=filter_values,
        tabs=tabs,
        tag_selection_mode=resolved_tag_mode,
        filter_operator=filter_operator,
    )

    logger.info("Profile flow started report_id=%s display_name=%s", report.id, report.display_name)
    logger.info("Profile meta: page_type=%s compare_sources=%s enabled=%s", page_type, report.compare_sources, report.enabled)
    if not report.enabled:
        logger.warning("Report profile is disabled in YAML, but run will continue for manual testing.")
    logger.info("Profile resolved: source_kind=%s filter_values=%s tabs=%s", source_kind, filter_values, tabs)
    logger.info("Tag selection mode: %s", resolved_tag_mode)
    logger.info("Browser backend: %s", settings.browser_backend)

    api_routing = _resolve_layout_api_routing(args, destination)
    api_layout_write_enabled = api_routing["api_write_enabled"]
    batch_from_sheet_dsl = bool(args.writer_layout_api_batch_from_sheet_dsl)
    batch_from_sheet_dsl_dry_run = bool(args.writer_layout_api_batch_from_sheet_dsl_dry_run)
    if batch_from_sheet_dsl and batch_from_sheet_dsl_dry_run:
        raise RuntimeError(
            "Use only one mode: --writer-layout-api-batch-from-sheet-dsl or "
            "--writer-layout-api-batch-from-sheet-dsl-dry-run"
        )

    if args.writer_layout_api_write_from_latest_compiled:
        logger.info(
            "writer-layout-api-write-from-latest-compiled mode enabled. "
            "Browser/OpenClaw and amoCRM flow are skipped."
        )
        if destination.kind != "google_sheets_layout_ui":
            raise RuntimeError(
                "writer-layout-api-write-from-latest-compiled requires destination kind google_sheets_layout_ui"
            )
        try:
            _run_api_layout_writer_from_latest_compiled(
                config=config,
                logger=logger,
                report=report,
                destination=destination,
                source_kind=source_kind,
                filter_values=filter_values,
                dry_run=bool(args.writer_layout_api_dry_run),
                target_dsl_row=args.writer_layout_api_target_dsl_row,
                target_dsl_text_contains=args.writer_layout_api_target_dsl_text_contains,
                target_dsl_cell=args.writer_layout_api_target_dsl_cell,
            )
            logger.info("isolated API layout writer run finished successfully")
        except Exception as exc:
            logger.error("isolated API layout writer run failed: %s", exc)
        return

    if args.writer_layout_api_inspector_only:
        logger.info("Writer layout API-inspector-only mode enabled. Browser/analytics flow is skipped.")
        if destination.kind != "google_sheets_layout_ui":
            logger.error(
                "writer-layout-api-inspector-only requires destination kind google_sheets_layout_ui; got=%s",
                destination.kind,
            )
            return
        try:
            inspector = GoogleSheetsApiLayoutInspector(project_root=config.project_root, logger=logger)
            inspector.inspect(destination=destination)
            logger.info("writer layout API inspector finished successfully")
        except Exception as exc:
            logger.error("writer layout API inspector failed: %s", exc)
        return

    with BrowserSession(settings) as session:
        page = session.new_page()

        if args.wait_for_enter:
            logger.info("Wait-for-enter mode enabled before profile flow start.")
            _wait_for_login_ready()

        reader = AmoAnalyticsReader(settings=settings, project_root=config.project_root)
        layout_mode = destination.kind == "google_sheets_layout_ui"

        if args.writer_layout_grid_inspector_only:
            logger.info("Writer layout grid-inspector-only mode enabled. Analytics flow is skipped.")
            if destination.kind != "google_sheets_layout_ui":
                logger.error(
                    "writer-layout-grid-inspector-only requires destination kind google_sheets_layout_ui; got=%s",
                    destination.kind,
                )
                return
            try:
                layout_writer = GoogleSheetsUILayoutWriter(project_root=config.project_root)
                layout_writer.debug_inspect_visible_grid(page=page, destination=destination)
                logger.info("writer layout grid inspector finished successfully")
            except Exception as exc:
                logger.error("writer layout grid inspector failed: %s", exc)
            return

        if args.writer_debug_cell_nav_only:
            logger.info("Writer debug cell-nav-only mode enabled. Analytics flow is skipped.")
            try:
                writer = GoogleSheetsUIWriter()
                # Compiled payload is not used in debug-navigation mode.
                compiled_result = compile_profile_analytics_result(
                    report=report,
                    source_kind=source_kind,
                    filter_values=filter_values,
                    snapshots=[],
                )
                writer.write_profile_analytics_result(
                    page=page,
                    compiled_result=compiled_result,
                    destination=destination,
                    debug_navigation_only=True,
                )
            except Exception as exc:
                logger.error("writer debug cell-nav-only failed: %s", exc)
            return


        if settings.browser_backend == "openclaw_cdp":
            logger.info("Smoke check for openclaw_cdp: opening analytics page")
            reader.open_analytics_page(page)
            smoke_path = settings.screenshots_dir / "openclaw_cdp_smoke_opened.png"
            page.screenshot(path=str(smoke_path), full_page=True)
            logger.info("Smoke check success via openclaw_cdp, screenshot=%s page_url=%s", smoke_path, page.url)
        bridge_cmd = (
            args.external_agent_bridge_cmd
            or os.getenv("EXTERNAL_AGENT_BRIDGE_CMD", "").strip()
            or None
        )
        bridge_timeout_sec = args.external_agent_bridge_timeout_sec
        if bridge_timeout_sec is None:
            try:
                bridge_timeout_sec = int(os.getenv("EXTERNAL_AGENT_BRIDGE_TIMEOUT_SEC", "180"))
            except ValueError:
                bridge_timeout_sec = 180

        if bridge_cmd:
            logger.info("External agent bridge command configured.")
        logger.info("External agent bridge timeout sec: %s", bridge_timeout_sec)

        flow = AnalyticsFlow(
            reader=reader,
            project_root=config.project_root,
            tag_selection_mode=resolved_tag_mode,
            external_agent_bridge_cmd=bridge_cmd,
            external_agent_bridge_timeout_sec=bridge_timeout_sec,
        )

        if batch_from_sheet_dsl or batch_from_sheet_dsl_dry_run:
            if destination.kind != "google_sheets_layout_ui":
                raise RuntimeError(
                    "writer-layout-api-batch-from-sheet-dsl requires destination kind google_sheets_layout_ui"
                )
            _run_api_layout_batch_from_sheet_dsl(
                config=config,
                logger=logger,
                page=page,
                flow=flow,
                report=report,
                tabs=tabs,
                destination=destination,
                source_kind=source_kind,
                filter_values=filter_values,
                dry_run=batch_from_sheet_dsl_dry_run and not batch_from_sheet_dsl,
                target_dsl_row=args.writer_layout_api_target_dsl_row,
                target_dsl_text_contains=args.writer_layout_api_target_dsl_text_contains,
                target_dsl_cell=args.writer_layout_api_target_dsl_cell,
            )
            return

        exported_tabs: list[str] = []
        if layout_mode and not api_layout_write_enabled:
            logger.info(
                "layout mode detected: baseline analytics run is skipped; Google Sheets DSL drives per-block scenario execution"
            )
            snapshots: list[AnalyticsSnapshot] = []
            compiled_result = compile_profile_analytics_result(
                report=report,
                source_kind=source_kind,
                filter_values=filter_values,
                snapshots=snapshots,
            )
        else:
            if layout_mode and api_layout_write_enabled:
                logger.info(
                    "layout API write mode enabled: running baseline analytics capture to build compiled result for API write path"
                )
            snapshots = flow.run_profile_capture(page=page, profile=flow_input)

            for snapshot in snapshots:
                _export_snapshot(reader, logger, snapshot, exported_tabs)

            compiled_result = compile_profile_analytics_result(
                report=report,
                source_kind=source_kind,
                filter_values=filter_values,
                snapshots=snapshots,
            )
            logger.info("compiled result created: tabs=%s totals_by_tab=%s", compiled_result.tabs, compiled_result.totals_by_tab)
            logger.info("compiled result built = true")

            compiled_json_path = save_compiled_result_json(
                compiled_result=compiled_result,
                exports_dir=config.exports_dir,
                project_root=config.project_root,
            )
            logger.info("compiled result saved: %s", compiled_json_path)

        try:
            stage_aliases = destination.layout_config.get("stage_aliases", {}) if destination.layout_config else {}
            pivot = compile_stage_pivot(compiled_result=compiled_result, stage_aliases=stage_aliases)
            pivot_path = save_stage_pivot_json(
                pivot=pivot,
                report_id=compiled_result.report_id,
                exports_dir=config.exports_dir,
                project_root=config.project_root,
            )
            logger.info("compiled stage pivot saved: %s", pivot_path)

            logger.info("writer runtime mode selected: %s", destination.kind)

            if destination.kind == "google_sheets_layout_ui":
                mode_used, fallback_used = _run_layout_writer_with_routing(
                    logger=logger,
                    config=config,
                    page=page,
                    flow=flow,
                    tabs=tabs,
                    report=report,
                    compiled_result=compiled_result,
                    destination=destination,
                    layout_dry_run=args.writer_layout_dry_run,
                    api_write_enabled=api_layout_write_enabled,
                    api_preferred=api_routing["api_preferred"],
                    api_dry_run=api_routing["api_dry_run"],
                    api_fallback_to_ui=api_routing["api_fallback_to_ui"],
                    target_dsl_row=args.writer_layout_api_target_dsl_row,
                    target_dsl_text_contains=args.writer_layout_api_target_dsl_text_contains,
                    target_dsl_cell=args.writer_layout_api_target_dsl_cell,
                )
                logger.info("writer mode final = %s", mode_used)
                logger.info("fallback used = %s", str(fallback_used).lower())
            else:
                writer = GoogleSheetsUIWriter()
                writer.write_profile_analytics_result(
                    page=page,
                    compiled_result=compiled_result,
                    destination=destination,
                    debug_navigation_only=args.writer_debug_cell_nav,
                )
        except Exception as exc:
            logger.error("writer mvp failed: %s", exc)

        logger.info("Profile flow finished. successful_tabs=%s/%s", len(snapshots), len(tabs))
        logger.info("Exported tabs: %s", exported_tabs)


if __name__ == "__main__":
    main()














