from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from src.deal_analyzer.client_list.normalizer import (
    build_header_mapping as build_client_list_header_mapping,
)
from src.deal_analyzer.client_list.normalizer import normalize_client_rows
from src.deal_analyzer.client_list.prioritizer import (
    build_manager_client_context,
    build_priority_summary as build_client_priority_summary,
)
from src.deal_analyzer.client_list.reader import (
    discover_client_list_sheet,
    read_client_list_sheet,
)
from src.deal_analyzer.config import DealAnalyzerConfig, load_deal_analyzer_config
from src.deal_analyzer.progress import ProgressReporter
from src.deal_analyzer.daily_control.style.deterministic_cleaner import clean_rows
from src.deal_analyzer.weekly_manager_summary.artifacts import write_json, write_markdown
from src.deal_analyzer.weekly_manager_summary.roks_enrichment import build_roks_oap_snapshot
from src.deal_analyzer.weekly_manager_summary.sheets_writer import (
    build_discovery_markdown,
    discover_weekly_manager_sheet,
    execute_weekly_write,
)
from src.deal_analyzer.weekly_manager_summary.source_reader import read_daily_control_source, resolve_spreadsheet_id
from src.deal_analyzer.weekly_manager_summary.validation import (
    lint_weekly_text_rows,
    payload_has_blockers,
    validate_weekly_payload_rows,
)
from src.deal_analyzer.weekly_manager_summary.week_grouper import group_daily_rows_by_week_manager
from src.deal_analyzer.weekly_manager_summary.weekly_analyzer import analyze_weekly_groups
from src.deal_analyzer.weekly_shared.week_plan_reader import read_week_plan_rows
from src.deal_analyzer.weekly_shared.validation import normalize_row_quotes


def _parse_iso_date(value: str, *, field: str) -> datetime:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except Exception as exc:
        raise RuntimeError(f"Invalid {field}: {value}. Expected YYYY-MM-DD") from exc


def _new_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "weekly_manager_summary" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _manager_allowlist(cfg: DealAnalyzerConfig, cli_values: list[str] | None) -> tuple[str, ...]:
    if cli_values:
        values = tuple(str(x).strip() for x in cli_values if str(x).strip())
        if values:
            return values
    cfg_values = tuple(str(x).strip() for x in (cfg.daily_manager_allowlist or ()) if str(x).strip())
    if cfg_values:
        return cfg_values
    return ("Илья Бочков", "Рустам Хомидов")


def _load_client_context_by_manager(
    *,
    cfg: DealAnalyzerConfig,
    logger: Any,
    manager_names: list[str],
    period_start: str,
    period_end: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    if not bool(getattr(cfg, "client_list_enabled", False)):
        return (
            {"status": "disabled", "warnings": ["client_list_disabled"]},
            {"rows_total": 0, "categories": {}, "top_rows": []},
            {},
        )
    try:
        discovery = discover_client_list_sheet(cfg=cfg, logger=logger)
        snapshot = read_client_list_sheet(cfg=cfg, logger=logger)
        mapping = build_client_list_header_mapping(snapshot.headers, cfg=cfg)
        rows, _rejected = normalize_client_rows(
            headers=snapshot.headers,
            rows=snapshot.rows,
            mapping=mapping,
            header_row_number=snapshot.header_row_number,
        )
        priority = build_client_priority_summary(rows)
        context_by_manager: dict[str, dict[str, Any]] = {}
        for manager_name in manager_names:
            manager = str(manager_name or "").strip()
            if not manager:
                continue
            node = build_manager_client_context(
                rows=rows,
                manager_name=manager,
                period_start=period_start,
                period_end=period_end,
                manager_role_registry=getattr(cfg, "manager_role_registry", None),
                role_policy_registry=getattr(cfg, "role_policy_registry", None),
            )
            context_by_manager[manager.lower()] = node.__dict__
        return discovery, priority, context_by_manager
    except Exception as exc:
        return (
            {"status": "read_error", "warnings": ["client_list_read_failed"], "error": str(exc)},
            {"rows_total": 0, "categories": {}, "top_rows": [], "error": str(exc)},
            {},
        )


def _payload_row_validation_rejections(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        validation = validate_weekly_payload_rows([row])
        if payload_has_blockers(validation):
            rejected_rows.append(
                {
                    "row_index": idx,
                    "manager_name": str(row.get("manager_name") or ""),
                    "week_start": str(row.get("week_start") or ""),
                    "week_end": str(row.get("week_end") or ""),
                    "reason": "payload_validator_blocker",
                    "payload_validator": validation,
                    "row": row,
                }
            )
            continue
        valid_rows.append(row)
    return valid_rows, rejected_rows


def _build_quality_review(rows: list[dict[str, Any]], *, limit: int = 10) -> dict[str, Any]:
    lint = lint_weekly_text_rows(rows)
    by_row: dict[int, dict[str, Any]] = {}
    for item in lint.get("problem_examples", []) if isinstance(lint.get("problem_examples"), list) else []:
        if not isinstance(item, dict):
            continue
        row_idx = int(item.get("row_index", -1))
        if row_idx < 0:
            continue
        current = by_row.setdefault(
            row_idx,
            {
                "row_index": row_idx,
                "manager_name": item.get("manager_name", ""),
                "markers": set(),
                "fields": set(),
                "examples": [],
            },
        )
        for marker in item.get("markers", []) if isinstance(item.get("markers"), list) else []:
            current["markers"].add(str(marker))
        current["fields"].add(str(item.get("field", "")))
        if len(current["examples"]) < 2:
            current["examples"].append(str(item.get("value", "")))

    problem_rows = []
    for row in by_row.values():
        row["markers"] = sorted(list(row["markers"]))
        row["fields"] = sorted(list(row["fields"]))
        problem_rows.append(row)
    problem_rows.sort(key=lambda item: len(item.get("markers", [])), reverse=True)
    return {
        "rows_total": len(rows),
        "problem_rows_total": len(problem_rows),
        "problem_rows": problem_rows[: max(1, int(limit or 10))],
        "text_lint": lint,
    }


def _summary_markdown_lines(summary: dict[str, Any]) -> list[str]:
    return [
        f"groups_count: {summary.get('groups_count', 0)}",
        f"rows_prepared: {summary.get('rows_prepared', 0)}",
        f"rows_after_llm_analyzer: {summary.get('rows_after_llm_analyzer', 0)}",
        f"rows_after_payload_validator: {summary.get('rows_after_payload_validator', 0)}",
        f"rows_in_writer_payload: {summary.get('rows_in_writer_payload', 0)}",
        f"rows_quarantined: {summary.get('rows_quarantined', 0)}",
        f"rows_to_insert: {summary.get('rows_to_insert', 0)}",
        f"rows_to_update: {summary.get('rows_to_update', 0)}",
        f"rows_skipped_existing: {summary.get('rows_skipped_existing', 0)}",
        f"conflicts_count: {summary.get('conflicts_count', 0)}",
        f"llm_main_model: {summary.get('llm_main_model', '')}",
        f"llm_fallback_model: {summary.get('llm_fallback_model', '')}",
        f"llm_success_main: {summary.get('llm_success_main', 0)}",
        f"llm_success_fallback: {summary.get('llm_success_fallback', 0)}",
        f"llm_failed_count: {summary.get('llm_failed_count', 0)}",
        f"fallback_used_count: {summary.get('fallback_used_count', 0)}",
        f"training_source_counts: {summary.get('training_source_counts', {})}",
        f"training_rows_found_count: {summary.get('training_rows_found_count', 0)}",
        f"training_rows_used_count: {summary.get('training_rows_used_count', 0)}",
        f"training_missing_but_generated_count: {summary.get('training_missing_but_generated_count', 0)}",
        f"roks_oap_snapshot_status: {summary.get('roks_oap_snapshot_status', '')}",
        f"selected_current_month_sheet: {summary.get('selected_current_month_sheet', '')}",
        f"selected_previous_month_sheet: {summary.get('selected_previous_month_sheet', '')}",
        f"writer mode: {summary.get('writer_mode', '')}",
        f"write_allowed: {summary.get('write_allowed', False)}",
        f"block_reason: {summary.get('block_reason', '')}",
    ]


def _build_roks_metrics_debug(
    *,
    groups: list[Any],
    roks_snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    weekly_metrics = (
        roks_snapshot.get("weekly_metrics_by_manager", {})
        if isinstance(roks_snapshot.get("weekly_metrics_by_manager"), dict)
        else {}
    )
    roks_sheet_used = str(roks_snapshot.get("selected_current_month_sheet") or "")
    week_index_used = roks_snapshot.get("week_index_used")
    week_label_used = str(roks_snapshot.get("week_label_used") or "")
    out: list[dict[str, Any]] = []
    for group in groups:
        manager_name = str(getattr(group, "manager_name", "") or "")
        weekly_state = weekly_metrics.get(manager_name, {}) if isinstance(weekly_metrics, dict) else {}
        if not isinstance(weekly_state, dict):
            weekly_state = {}
        metric_interpretation = (
            weekly_state.get("metric_interpretation", {})
            if isinstance(weekly_state.get("metric_interpretation"), dict)
            else {}
        )
        out.append(
            {
                "manager_name": manager_name,
                "period_start": str(getattr(group, "period_start", "") or ""),
                "period_end": str(getattr(group, "period_end", "") or ""),
                "week_start": str(getattr(group, "week_start", "") or ""),
                "week_end": str(getattr(group, "week_end", "") or ""),
                "roks_sheet_used": str(weekly_state.get("roks_sheet_used") or roks_sheet_used),
                "week_index_used": weekly_state.get("week_index_used", week_index_used),
                "week_label_used": str(weekly_state.get("week_label_used") or week_label_used),
                "row_labels_found": list(weekly_state.get("row_labels_found", []) or []),
                "calls_fact_raw_cell": str(weekly_state.get("calls_fact_raw_cell") or ""),
                "calls_fact_value": weekly_state.get("calls_fact_value"),
                "lpr_fact_value": weekly_state.get("lpr_fact_value"),
                "interest_fact_value": weekly_state.get("interest_fact_value"),
                "demo_fact_value": weekly_state.get("demo_fact_value"),
                "test_fact_value": weekly_state.get("test_fact_value"),
                "invoice_count_fact_value": weekly_state.get("invoice_count_fact"),
                "payment_count_fact_value": weekly_state.get("payment_count_fact"),
                "metric_interpretation": metric_interpretation,
                "manager_role_profile": metric_interpretation.get("manager_role_profile", ""),
                "source_generated_interest": metric_interpretation.get("source_generated_interest"),
                "conducted_demo": metric_interpretation.get("conducted_demo"),
                "routed_meetings_possible": metric_interpretation.get("routed_meetings_possible"),
                "downstream_metrics_applicable": metric_interpretation.get("downstream_metrics_applicable"),
                "analyzed_deals_count": int(getattr(group, "analyzed_deals_count", 0) or 0),
                "analyzed_calls_count": int(getattr(group, "analyzed_calls_count", 0) or 0),
                "quality_sample_size": int(getattr(group, "quality_sample_size", 0) or 0),
                "warnings": list(weekly_state.get("warnings", []) or []),
            }
        )
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly manager summary CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover source/target sheets for weekly manager summary")
    discover.add_argument("--config", required=True)
    discover.add_argument("--workbook", default="РОКС 2026")
    discover.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    discover.add_argument("--plan-sheet", default="План недели")
    discover.add_argument("--target-sheet", default="Недельный свод менеджеров")

    build = sub.add_parser("build", help="Build weekly manager summary payload from daily control")
    build.add_argument("--config", required=True)
    build.add_argument("--period-start", required=True)
    build.add_argument("--period-end", required=True)
    build.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    build.add_argument("--plan-sheet", default="План недели")
    build.add_argument("--target-sheet", default="Недельный свод менеджеров")
    build.add_argument("--manager", dest="managers", action="append", default=None)
    build.add_argument("--main-model", default="")
    build.add_argument("--fallback-model", default="")
    build.add_argument("--llm-max-attempts", type=int, default=6)
    build.add_argument("--limit", type=int, default=0)
    build.add_argument("--dry-run", action="store_true")

    write = sub.add_parser("write", help="Write prepared weekly payload to target sheet")
    write.add_argument("--config", required=True)
    write.add_argument("--run-dir", required=True)
    write.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    write.add_argument("--plan-sheet", default="План недели")
    write.add_argument("--target-sheet", default="Недельный свод менеджеров")
    write.add_argument("--dry-run", action="store_true")
    write.add_argument("--write", action="store_true")
    write.add_argument("--strict-preflight", action="store_true")
    write.add_argument("--allow-partial-write", dest="allow_partial_write", action="store_true")
    write.add_argument("--no-allow-partial-write", dest="allow_partial_write", action="store_false")
    write.add_argument("--quarantine-unrepaired", dest="quarantine_unrepaired", action="store_true")
    write.add_argument("--no-quarantine-unrepaired", dest="quarantine_unrepaired", action="store_false")
    write.set_defaults(allow_partial_write=True, quarantine_unrepaired=True)
    return parser.parse_args()


def _run_discover(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)
    discovery = discover_weekly_manager_sheet(
        cfg=cfg,
        workbook_name=str(args.workbook or "РОКС 2026"),
        source_sheet_name=str(args.daily_sheet or "Дневной контроль"),
        target_sheet_name=str(args.target_sheet or "Недельный свод менеджеров"),
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        logger=logger,
    )
    write_json(run_dir / "weekly_manager_sheet_discovery.json", discovery)
    write_markdown(run_dir / "weekly_manager_sheet_discovery.md", title="Weekly Manager Discovery", lines=build_discovery_markdown(discovery))
    print(str(run_dir))


def _run_build(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)
    progress = ProgressReporter(
        process="weekly_manager_summary",
        run_dir=run_dir,
        heartbeat_seconds=int(getattr(cfg, "progress_heartbeat_seconds", 30) or 30),
        logger=logger,
        step_name="init",
        total=0,
    )

    period_start = _parse_iso_date(str(args.period_start), field="period_start").date()
    period_end = _parse_iso_date(str(args.period_end), field="period_end").date()
    if period_end < period_start:
        raise RuntimeError("period_end must be >= period_start")
    progress.update(
        step_name="period_resolved",
        current=0,
        total=0,
        current_item={"stage": "period", "date": f"{period_start.isoformat()}..{period_end.isoformat()}"},
    )

    discovery = discover_weekly_manager_sheet(
        cfg=cfg,
        workbook_name="РОКС 2026",
        source_sheet_name=str(args.daily_sheet or "Дневной контроль"),
        target_sheet_name=str(args.target_sheet or "Недельный свод менеджеров"),
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        logger=logger,
    )
    write_json(run_dir / "weekly_manager_sheet_discovery.json", discovery)
    write_markdown(run_dir / "weekly_manager_sheet_discovery.md", title="Weekly Manager Discovery", lines=build_discovery_markdown(discovery))
    progress.update(step_name="sheet_discovery", current=0, total=0, current_item={"stage": "discover"})

    spreadsheet_id = resolve_spreadsheet_id(cfg)
    source_sheet_name = (
        (discovery.get("source_sheet", {}) if isinstance(discovery.get("source_sheet"), dict) else {}).get("title")
        or str(args.daily_sheet or "Дневной контроль")
    )
    plan_sheet_name = (
        (discovery.get("plan_sheet", {}) if isinstance(discovery.get("plan_sheet"), dict) else {}).get("title")
        or str(args.plan_sheet or "План недели")
    )
    snapshot = read_daily_control_source(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        source_sheet_name=source_sheet_name,
        logger=logger,
    )
    plan_snapshot = read_week_plan_rows(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=plan_sheet_name,
        logger=logger,
    )
    managers = _manager_allowlist(cfg, args.managers)
    groups, grouping_diag, plan_fact_rows = group_daily_rows_by_week_manager(
        headers=snapshot.headers,
        rows=snapshot.rows,
        period_start=period_start,
        period_end=period_end,
        manager_allowlist=managers,
        plan_headers=plan_snapshot.headers,
        plan_rows=plan_snapshot.rows,
    )
    groups_total_before_limit = len(groups)
    if int(args.limit or 0) > 0:
        groups = groups[: int(args.limit)]
    grouping_diag = dict(grouping_diag or {})
    grouping_diag["groups_total_before_limit"] = groups_total_before_limit
    grouping_diag["groups_total_after_limit"] = len(groups)
    grouping_diag["groups_limit_applied"] = int(args.limit or 0)
    progress.update(
        step_name="groups_built",
        current=0,
        total=len(groups),
        current_item={"stage": "grouping", "groups": len(groups)},
    )
    client_discovery, client_priority, client_context_by_manager = _load_client_context_by_manager(
        cfg=cfg,
        logger=logger,
        manager_names=[
            str(getattr(group, "manager_name", "") or "")
            for group in groups
            if str(getattr(group, "manager_name", "") or "").strip()
        ],
        period_start=period_start.isoformat(),
        period_end=period_end.isoformat(),
    )

    app_root = Path(cfg.config_path).resolve().parents[1]
    sheet_client = None
    try:
        from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

        sheet_client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    except Exception:
        sheet_client = None
    if sheet_client is None:
        roks_snapshot = {
            "status": "access_error",
            "parse_status": "access_error",
            "warnings": ["google_sheets_client_init_failed"],
            "selected_current_month_sheet": "",
            "selected_previous_month_sheet": "",
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
        }
    else:
        roks_snapshot = build_roks_oap_snapshot(
            client=sheet_client,
            spreadsheet_id=spreadsheet_id,
            week_start=period_start.isoformat(),
            week_end=period_end.isoformat(),
            manager_allowlist=managers,
        )

    llm_runtime = {
        "main": {
            "model": str(args.main_model or "").strip() or "deepseek-v4-pro:cloud",
            "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
        "fallback": {
            "enabled": True,
            "model": str(args.fallback_model or "").strip() or "deepseek-v4-flash:cloud",
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
    }

    rows, llm_diag = analyze_weekly_groups(
        groups=groups,
        cfg=cfg,
        roks_snapshot=roks_snapshot,
        client_context_by_manager=client_context_by_manager,
        llm_runtime=llm_runtime,
        logger=logger,
        source_run_id=run_dir.name,
        main_model_override=str(args.main_model or "").strip() or None,
        fallback_model_override=str(args.fallback_model or "").strip() or None,
        llm_max_attempts=int(args.llm_max_attempts or 6),
    )
    progress.update(
        step_name="llm_completed",
        current=len(rows),
        total=max(len(groups), len(rows)),
        current_item={"stage": "llm", "model": str(args.main_model or "") or "default"},
    )
    rows_cleaned, _cleanup_counts = clean_rows(
        rows,
        fields=(
            "weekly_result",
            "improved",
            "not_improved",
            "repeating_mistakes",
            "training_for_employee",
            "post_training_tasks",
            "manager_actions_next_week",
            "expected_quantity_effect",
            "expected_quality_effect",
            "manager_report_phrase",
            "employee_message",
        ),
    )
    rows_cleaned = normalize_row_quotes(
        rows_cleaned,
        fields=(
            "weekly_result",
            "improved",
            "not_improved",
            "repeating_mistakes",
            "training_for_employee",
            "post_training_tasks",
            "manager_actions_next_week",
            "expected_quantity_effect",
            "expected_quality_effect",
            "manager_report_phrase",
            "employee_message",
        ),
    )
    writer_rows, payload_validator_rejected = _payload_row_validation_rejections(rows_cleaned)
    llm_quarantined = llm_diag.get("quarantined_rows", []) if isinstance(llm_diag.get("quarantined_rows"), list) else []
    quarantined_rows = [*llm_quarantined, *payload_validator_rejected]
    managers_in_daily_control = grouping_diag.get("managers_in_daily_control", [])
    managers_in_groups = grouping_diag.get("managers_in_groups", [])
    managers_in_payload = sorted(
        {
            str(item.get("manager_name") or "").strip()
            for item in writer_rows
            if isinstance(item, dict) and str(item.get("manager_name") or "").strip()
        }
    )
    managers_skipped_with_reason = list(grouping_diag.get("managers_skipped_with_reason", []) or [])
    for item in llm_quarantined:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("manager_name") or "").strip(),
                "reason": str(item.get("reason") or "llm_failed"),
                "stage": "llm_analyzer",
            }
        )
    for item in payload_validator_rejected:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("manager_name") or "").strip(),
                "reason": str(item.get("reason") or "payload_validator_blocker"),
                "stage": "payload_validator",
            }
        )

    payload = {
        "mode": "weekly_manager_summary",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "source_sheet": source_sheet_name,
        "plan_sheet": plan_sheet_name,
        "target_sheet": str(args.target_sheet or "Недельный свод менеджеров"),
        "rows": writer_rows,
        "rows_count": len(writer_rows),
        "rows_prepared": len(rows),
        "rows_quarantined": len(quarantined_rows),
        "llm_runtime": llm_diag.get("llm_runtime", {}),
    }
    input_groups_payload = {
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "groups_count": len(groups),
        "groups": [group.__dict__ for group in groups],
        "grouping_diagnostics": grouping_diag,
    }
    quality_review = _build_quality_review(writer_rows, limit=10)

    row_flow_filtered: list[dict[str, Any]] = []
    for item in llm_quarantined:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "llm_analyzer",
                    "row_index": item.get("row_index"),
                    "manager_name": item.get("manager_name", ""),
                    "week_start": item.get("week_start", ""),
                    "week_end": item.get("week_end", ""),
                    "reason": item.get("reason", "llm_failed"),
                    "error_type": item.get("error_type", ""),
                    "models_attempted": item.get("models_attempted", []),
                    "errors_by_attempt": item.get("errors_by_attempt", []),
                    "raw_response_preview": item.get("raw_response_preview", ""),
                    "prompt_size_chars": item.get("prompt_size_chars", 0),
                }
            )
    for item in payload_validator_rejected:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "payload_validator",
                    "row_index": item.get("row_index"),
                    "manager_name": item.get("manager_name", ""),
                    "week_start": item.get("week_start", ""),
                    "week_end": item.get("week_end", ""),
                    "reason": item.get("reason", "payload_validator_blocker"),
                }
            )

    row_flow_debug = {
        "groups_count": len(groups),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_weekly_payload": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "quarantine_count": len(quarantined_rows),
        "rejected_rows_count": len(row_flow_filtered),
        "filtered_rows": row_flow_filtered,
        "managers_in_daily_control": managers_in_daily_control,
        "managers_in_groups": managers_in_groups,
        "managers_in_payload": managers_in_payload,
        "managers_skipped_with_reason": managers_skipped_with_reason,
        "training_source_counts": llm_diag.get("training_source_counts", {}),
        "training_rows_found_count": int(llm_diag.get("training_rows_found_count", 0) or 0),
        "training_rows_used": llm_diag.get("training_rows_used", []),
        "training_rows_used_count": int(llm_diag.get("training_rows_used_count", 0) or 0),
        "training_missing_but_generated_count": int(llm_diag.get("training_missing_but_generated_count", 0) or 0),
        "training_missing_but_generated_examples": llm_diag.get("training_missing_but_generated_examples", []),
    }
    roks_metrics_debug = _build_roks_metrics_debug(groups=groups, roks_snapshot=roks_snapshot)
    metric_sources_debug = {
        "kpi_source_policy": "roks_facts",
        "quality_sample_source_policy": "daily_control_and_call_review_sample",
        "groups_count": len(groups),
        "groups": [
            {
                "manager_name": item.get("manager_name", ""),
                "week_start": item.get("week_start", ""),
                "week_end": item.get("week_end", ""),
                "sources": {
                    "roks_calls_fact": "ROKS_OAP",
                    "roks_lpr_fact": "ROKS_OAP",
                    "roks_interest_fact": "ROKS_OAP",
                    "roks_demo_fact": "ROKS_OAP",
                    "roks_test_fact": "ROKS_OAP",
                    "roks_invoice_count_fact": "ROKS_OAP",
                    "roks_payment_count_fact": "ROKS_OAP",
                    "analyzed_deals_count": "daily_control_quality_sample",
                    "analyzed_calls_count": "daily_control_quality_sample",
                    "quality_sample_size": "daily_control_quality_sample",
                    "metric_interpretation": "ROKS_role_profile_policy",
                },
                "warnings": item.get("warnings", []),
            }
            for item in roks_metrics_debug
        ],
    }

    write_json(run_dir / "weekly_manager_source_rows.json", {"headers": snapshot.headers, "rows": snapshot.rows})
    write_json(run_dir / "client_list_discovery.json", client_discovery)
    write_json(run_dir / "client_list_priority_summary.json", client_priority)
    write_json(
        run_dir / "weekly_manager_client_context_debug.json",
        {"context_by_manager": client_context_by_manager, "status": client_discovery.get("status", "")},
    )
    write_json(run_dir / "weekly_manager_plan_fact_rows.json", plan_fact_rows)
    write_json(run_dir / "weekly_manager_input_groups.json", input_groups_payload)
    write_json(run_dir / "roks_oap_snapshot.json", roks_snapshot)
    write_json(run_dir / "weekly_manager_roks_metrics_debug.json", roks_metrics_debug)
    write_json(run_dir / "weekly_manager_metric_sources_debug.json", metric_sources_debug)
    write_json(run_dir / "weekly_manager_llm_requests.json", llm_diag.get("llm_requests", []))
    write_json(run_dir / "weekly_manager_llm_responses.json", llm_diag.get("llm_responses", []))
    write_json(
        run_dir / "employee_profile_context_debug.json",
        {
            "rows_total": len(llm_diag.get("employee_profile_context_rows", []) if isinstance(llm_diag.get("employee_profile_context_rows"), list) else []),
            "rows": llm_diag.get("employee_profile_context_rows", []),
        },
    )
    write_json(
        run_dir / "employee_behavior_markers.json",
        {
            "rows_total": len(llm_diag.get("employee_behavior_marker_rows", []) if isinstance(llm_diag.get("employee_behavior_marker_rows"), list) else []),
            "rows": llm_diag.get("employee_behavior_marker_rows", []),
        },
    )
    write_json(run_dir / "weekly_manager_payload.json", payload)
    write_json(run_dir / "weekly_manager_quarantine.json", {"rows_quarantined": len(quarantined_rows), "rows": quarantined_rows})
    write_json(run_dir / "weekly_manager_row_flow_debug.json", row_flow_debug)
    write_json(run_dir / "weekly_manager_quality_review.json", quality_review)
    write_json(
        run_dir / "weekly_manager_training_source_debug.json",
        {
            "training_source_counts": llm_diag.get("training_source_counts", {}),
            "training_rows_found_count": int(llm_diag.get("training_rows_found_count", 0) or 0),
            "training_rows_used_count": int(llm_diag.get("training_rows_used_count", 0) or 0),
            "training_rows_used": llm_diag.get("training_rows_used", []),
            "training_missing_but_generated_count": int(llm_diag.get("training_missing_but_generated_count", 0) or 0),
            "examples": llm_diag.get("training_missing_but_generated_examples", []),
        },
    )

    summary = {
        "run_id": run_dir.name,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "source_sheet": source_sheet_name,
        "plan_sheet": plan_sheet_name,
        "target_sheet": str(args.target_sheet or "Недельный свод менеджеров"),
        "groups_count": len(groups),
        "rows_prepared": len(rows),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "rows_quarantined": len(quarantined_rows),
        "managers_in_daily_control": managers_in_daily_control,
        "managers_in_groups": managers_in_groups,
        "managers_in_payload": managers_in_payload,
        "managers_skipped_with_reason": managers_skipped_with_reason,
        "training_source_counts": llm_diag.get("training_source_counts", {}),
        "training_rows_found_count": int(llm_diag.get("training_rows_found_count", 0) or 0),
        "training_rows_used_count": int(llm_diag.get("training_rows_used_count", 0) or 0),
        "training_missing_but_generated_count": int(llm_diag.get("training_missing_but_generated_count", 0) or 0),
        "training_rows_used": llm_diag.get("training_rows_used", []),
        "client_list_status": client_discovery.get("status", ""),
        "employee_profile_context_rows": len(llm_diag.get("employee_profile_context_rows", []))
        if isinstance(llm_diag.get("employee_profile_context_rows"), list)
        else 0,
        "employee_behavior_markers_rows": len(llm_diag.get("employee_behavior_marker_rows", []))
        if isinstance(llm_diag.get("employee_behavior_marker_rows"), list)
        else 0,
        "rows_to_insert": 0,
        "rows_to_update": 0,
        "rows_skipped_existing": 0,
        "rows_skipped_stale": 0,
        "conflicts_count": 0,
        "llm_main_model": (llm_diag.get("llm_runtime", {}).get("main", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("main", {}), dict) else {}).get("model", ""),
        "llm_fallback_model": (llm_diag.get("llm_runtime", {}).get("fallback", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("fallback", {}), dict) else {}).get("model", ""),
        "llm_attempts_total": llm_diag.get("llm_attempts_total", 0),
        "llm_success_main": llm_diag.get("llm_success_main", 0),
        "llm_success_main_repair": llm_diag.get("llm_success_main_repair", 0),
        "llm_success_main_compact_retry": llm_diag.get("llm_success_main_compact_retry", 0),
        "llm_success_fallback": llm_diag.get("llm_success_fallback", 0),
        "llm_success_fallback_repair": llm_diag.get("llm_success_fallback_repair", 0),
        "llm_success_fallback_compact_retry": llm_diag.get("llm_success_fallback_compact_retry", 0),
        "llm_failed_count": llm_diag.get("llm_failed_count", 0),
        "fallback_used_count": llm_diag.get("fallback_used_count", 0),
        "roks_oap_snapshot_status": roks_snapshot.get("status", ""),
        "selected_current_month_sheet": roks_snapshot.get("selected_current_month_sheet", ""),
        "selected_previous_month_sheet": roks_snapshot.get("selected_previous_month_sheet", ""),
        "roks_week_index_used": roks_snapshot.get("week_index_used"),
        "roks_week_label_used": roks_snapshot.get("week_label_used", ""),
        "writer_mode": "dry_run",
        "write_allowed": False,
        "block_reason": "dry_run_build_only",
    }
    write_json(run_dir / "summary.json", summary)
    write_markdown(run_dir / "summary.md", title="Weekly Manager Summary", lines=_summary_markdown_lines(summary))
    progress.update(
        step_name="artifacts_written",
        current=len(writer_rows),
        total=max(len(rows), len(writer_rows)),
        current_item={"stage": "artifacts", "rows": len(writer_rows)},
    )
    progress.finish(status="completed", step_name="build_completed")
    print(str(run_dir))


def _run_write(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = Path(str(args.run_dir)).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    dry_run = not bool(args.write and not args.dry_run)

    status = execute_weekly_write(
        cfg=cfg,
        run_dir=run_dir,
        target_sheet_name=str(args.target_sheet or "Недельный свод менеджеров"),
        dry_run=dry_run,
        strict_preflight=bool(args.strict_preflight),
        allow_partial_write=bool(args.allow_partial_write),
        quarantine_unrepaired=bool(args.quarantine_unrepaired),
        logger=logger,
    )
    write_json(run_dir / "weekly_manager_writer_status.json", status)
    summary_path = run_dir / "summary.json"
    summary = {}
    if summary_path.exists():
        try:
            data = json.loads(summary_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                summary = data
        except Exception:
            summary = {}
    summary.update(
        {
            "writer_mode": status.get("mode", "dry_run"),
            "write_strategy": status.get("write_strategy", "values_only"),
            "rows_to_insert": status.get("rows_to_insert", 0),
            "rows_to_update": status.get("rows_to_update", 0),
            "rows_skipped_existing": status.get("rows_skipped_existing", 0),
            "rows_skipped_stale": status.get("rows_skipped_stale", 0),
            "rows_quarantined": status.get("rows_quarantined", 0),
            "conflicts_count": status.get("conflicts_count", 0),
            "write_allowed": status.get("write_allowed", False),
            "block_reason": status.get("block_reason", ""),
        }
    )
    write_json(summary_path, summary)
    write_markdown(run_dir / "summary.md", title="Weekly Manager Summary", lines=_summary_markdown_lines(summary))
    if str(status.get("block_reason") or "") == "payload_missing":
        print("Сначала выполните build и используйте run-dir, где есть weekly_manager_payload.json")
    print(json.dumps(status, ensure_ascii=False, indent=2))


def main() -> None:
    args = _parse_args()
    if args.command == "discover":
        _run_discover(args)
        return
    if args.command == "build":
        _run_build(args)
        return
    if args.command == "write":
        _run_write(args)
        return
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
