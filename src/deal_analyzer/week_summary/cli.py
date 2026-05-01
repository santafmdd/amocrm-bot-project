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
    build_priority_summary as build_client_priority_summary,
)
from src.deal_analyzer.client_list.reader import read_client_list_sheet
from src.deal_analyzer.config import load_deal_analyzer_config
from src.deal_analyzer.progress import ProgressReporter
from .aggregator import build_week_summary_groups
from .analyzer import analyze_week_summary_groups
from .artifacts import write_json, write_markdown
from .roks_enrichment import build_roks_oap_snapshot
from .sheets_writer import build_discovery_markdown, discover_week_summary_sheet, execute_week_summary_write
from .source_reader import read_sheet_snapshot, resolve_spreadsheet_id
from ..weekly_shared.daily_control_reader import read_daily_control_rows
from ..weekly_shared.validation import normalize_row_quotes
from .validation import (
    evaluate_writer_preflight,
    lint_has_blockers,
    lint_week_summary_text_rows,
    payload_has_blockers,
    validate_week_summary_payload_rows,
)


def _parse_iso_date(value: str, *, field: str) -> datetime:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except Exception as exc:
        raise RuntimeError(f"Invalid {field}: {value}. Expected YYYY-MM-DD") from exc


def _new_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "week_summary" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


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
        f"roks_oap_snapshot_status: {summary.get('roks_oap_snapshot_status', '')}",
        f"selected_current_month_sheet: {summary.get('selected_current_month_sheet', '')}",
        f"selected_previous_month_sheet: {summary.get('selected_previous_month_sheet', '')}",
        f"writer mode: {summary.get('writer_mode', '')}",
        f"write_allowed: {summary.get('write_allowed', False)}",
        f"block_reason: {summary.get('block_reason', '')}",
    ]


def _payload_row_validation_rejections(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        lint = lint_week_summary_text_rows([row])
        payload_validation = validate_week_summary_payload_rows([row])
        if lint_has_blockers(lint) or payload_has_blockers(payload_validation):
            rejected_rows.append(
                {
                    "row_index": idx,
                    "week_start": str(row.get("week_start") or ""),
                    "week_end": str(row.get("week_end") or ""),
                    "reason": "payload_validator_blocker",
                    "text_lint": lint,
                    "payload_validator": payload_validation,
                    "row": row,
                }
            )
            continue
        valid_rows.append(row)
    return valid_rows, rejected_rows


def _load_client_priority_summary(cfg: Any, logger: Any) -> dict[str, Any]:
    if not bool(getattr(cfg, "client_list_enabled", False)):
        return {"status": "disabled", "rows_total": 0, "categories": {}, "top_rows": []}
    try:
        snapshot = read_client_list_sheet(cfg=cfg, logger=logger)
        mapping = build_client_list_header_mapping(snapshot.headers, cfg=cfg)
        rows, _rejected = normalize_client_rows(
            headers=snapshot.headers,
            rows=snapshot.rows,
            mapping=mapping,
            header_row_number=snapshot.header_row_number,
        )
        summary = build_client_priority_summary(rows)
        summary["status"] = "ok"
        return summary
    except Exception as exc:
        return {"status": "read_error", "rows_total": 0, "categories": {}, "top_rows": [], "error": str(exc)}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Week summary CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover source/target sheets for week summary")
    discover.add_argument("--config", required=True)
    discover.add_argument("--workbook", default="РОКС 2026")
    discover.add_argument("--manager-summary-sheet", default="Недельный свод менеджеров")
    discover.add_argument("--plan-sheet", default="План недели")
    discover.add_argument("--target-sheet", default="Свод недели")

    build = sub.add_parser("build", help="Build week summary payload")
    build.add_argument("--config", required=True)
    build.add_argument("--period-start", required=True)
    build.add_argument("--period-end", required=True)
    build.add_argument("--manager-summary-sheet", default="Недельный свод менеджеров")
    build.add_argument("--plan-sheet", default="План недели")
    build.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    build.add_argument("--target-sheet", default="Свод недели")
    build.add_argument("--main-model", default="")
    build.add_argument("--fallback-model", default="")
    build.add_argument("--llm-max-attempts", type=int, default=6)
    build.add_argument("--limit", type=int, default=0)
    build.add_argument("--dry-run", action="store_true")

    write = sub.add_parser("write", help="Write prepared week summary payload to target sheet")
    write.add_argument("--config", required=True)
    write.add_argument("--run-dir", required=True)
    write.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    write.add_argument("--plan-sheet", default="План недели")
    write.add_argument("--manager-summary-sheet", default="Недельный свод менеджеров")
    write.add_argument("--target-sheet", default="Свод недели")
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
    discovery = discover_week_summary_sheet(
        cfg=cfg,
        workbook_name=str(args.workbook or "РОКС 2026"),
        manager_summary_sheet_name=str(args.manager_summary_sheet or "Недельный свод менеджеров"),
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        target_sheet_name=str(args.target_sheet or "Свод недели"),
        logger=logger,
    )
    write_json(run_dir / "week_summary_sheet_discovery.json", discovery)
    write_markdown(run_dir / "week_summary_sheet_discovery.md", title="Week Summary Discovery", lines=build_discovery_markdown(discovery))
    print(str(run_dir))


def _run_build(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)
    progress = ProgressReporter(
        process="week_summary",
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

    discovery = discover_week_summary_sheet(
        cfg=cfg,
        workbook_name="РОКС 2026",
        manager_summary_sheet_name=str(args.manager_summary_sheet or "Недельный свод менеджеров"),
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        target_sheet_name=str(args.target_sheet or "Свод недели"),
        logger=logger,
    )
    write_json(run_dir / "week_summary_sheet_discovery.json", discovery)
    write_markdown(run_dir / "week_summary_sheet_discovery.md", title="Week Summary Discovery", lines=build_discovery_markdown(discovery))
    progress.update(step_name="sheet_discovery", current=0, total=0, current_item={"stage": "discover"})

    spreadsheet_id = resolve_spreadsheet_id(cfg)
    manager_sheet_name = (
        (discovery.get("manager_summary_sheet", {}) if isinstance(discovery.get("manager_summary_sheet"), dict) else {}).get("title")
        or str(args.manager_summary_sheet or "Недельный свод менеджеров")
    )
    plan_sheet_name = (
        (discovery.get("plan_sheet", {}) if isinstance(discovery.get("plan_sheet"), dict) else {}).get("title")
        or str(args.plan_sheet or "План недели")
    )
    manager_snapshot = read_sheet_snapshot(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=manager_sheet_name,
        logger=logger,
    )
    plan_snapshot = read_sheet_snapshot(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=plan_sheet_name,
        logger=logger,
    )
    daily_sheet_name = str(args.daily_sheet or "Дневной контроль")
    daily_snapshot = read_daily_control_rows(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=daily_sheet_name,
        logger=logger,
    )

    groups, grouping_diag, plan_fact_rows = build_week_summary_groups(
        manager_headers=manager_snapshot.headers,
        manager_rows=manager_snapshot.rows,
        plan_headers=plan_snapshot.headers,
        plan_rows=plan_snapshot.rows,
        period_start=period_start,
        period_end=period_end,
        daily_headers=daily_snapshot.headers,
        daily_rows=daily_snapshot.rows,
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
            manager_allowlist=tuple(str(x).strip() for x in (cfg.daily_manager_allowlist or ()) if str(x).strip()),
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
            "preflight_timeout_seconds": int(cfg.ollama_fallback_preflight_timeout_seconds or cfg.ollama_preflight_timeout_seconds or 20),
        },
    }
    client_priority_summary = _load_client_priority_summary(cfg, logger)

    rows, llm_diag = analyze_week_summary_groups(
        groups=groups,
        cfg=cfg,
        roks_snapshot=roks_snapshot,
        client_priority_summary=client_priority_summary,
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
    rows = normalize_row_quotes(
        rows,
        fields=(
            "brief_report",
            "quantity_delta",
            "quality_delta",
            "what_failed",
            "focus_next_week",
            "next_week_plan",
            "meeting_message",
            "strategic_accents",
            "risks",
            "manager_report_phrase",
        ),
    )

    writer_rows, payload_validator_rejected = _payload_row_validation_rejections(rows)
    llm_quarantined = llm_diag.get("quarantined_rows", []) if isinstance(llm_diag.get("quarantined_rows"), list) else []
    quarantined_rows = [*llm_quarantined, *payload_validator_rejected]

    row_flow_filtered: list[dict[str, Any]] = []
    for item in llm_quarantined:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "llm_analyzer",
                    "row_index": item.get("row_index"),
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
                    "week_start": item.get("week_start", ""),
                    "week_end": item.get("week_end", ""),
                    "reason": item.get("reason", "payload_validator_blocker"),
                }
            )

    row_flow_debug = {
        "groups_count": len(groups),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_week_summary_payload": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "quarantine_count": len(quarantined_rows),
        "rejected_rows_count": len(row_flow_filtered),
        "filtered_rows": row_flow_filtered,
    }

    payload = {
        "mode": "week_summary",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "manager_summary_sheet": manager_sheet_name,
        "plan_sheet": plan_sheet_name,
        "target_sheet": str(args.target_sheet or "Свод недели"),
        "rows": writer_rows,
        "rows_count": len(writer_rows),
        "rows_prepared": len(rows),
        "rows_quarantined": len(quarantined_rows),
        "llm_runtime": llm_diag.get("llm_runtime", {}),
        "client_list_priority_summary": client_priority_summary,
    }

    preflight_quality = evaluate_writer_preflight(
        rows=writer_rows,
        strict_preflight=True,
        conflicts_count=0,
        allow_partial_write=True,
        quarantine_unrepaired=True,
    )

    write_json(run_dir / "week_summary_manager_rows.json", {"headers": manager_snapshot.headers, "rows": manager_snapshot.rows})
    write_json(run_dir / "week_summary_plan_fact_rows.json", plan_fact_rows)
    write_json(run_dir / "week_summary_daily_fallback_rows.json", {"headers": daily_snapshot.headers, "rows": daily_snapshot.rows})
    write_json(run_dir / "client_list_priority_summary.json", client_priority_summary)
    write_json(run_dir / "roks_oap_snapshot.json", roks_snapshot)
    write_json(run_dir / "week_summary_llm_requests.json", llm_diag.get("llm_requests", []))
    write_json(run_dir / "week_summary_llm_responses.json", llm_diag.get("llm_responses", []))
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
    write_json(run_dir / "week_summary_payload.json", payload)
    write_json(run_dir / "week_summary_quarantine.json", {"rows_quarantined": len(quarantined_rows), "rows": quarantined_rows})
    write_json(run_dir / "week_summary_row_flow_debug.json", row_flow_debug)
    write_json(run_dir / "week_summary_quality_review.json", {"preflight": preflight_quality})

    summary = {
        "run_id": run_dir.name,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "manager_summary_sheet": manager_sheet_name,
        "plan_sheet": plan_sheet_name,
        "daily_sheet": daily_sheet_name,
        "target_sheet": str(args.target_sheet or "Свод недели"),
        "manager_rows_total": int(grouping_diag.get("manager_rows_total", 0) or 0),
        "plan_rows_total": int(grouping_diag.get("plan_rows_total", 0) or 0),
        "daily_source_rows_total": int(grouping_diag.get("daily_source_rows_total", 0) or 0),
        "daily_fallback_applied": bool(grouping_diag.get("daily_fallback_applied", False)),
        "groups_count": len(groups),
        "rows_prepared": len(rows),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "rows_quarantined": len(quarantined_rows),
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
        "client_list_status": client_priority_summary.get("status", ""),
        "client_list_rows_total": int(client_priority_summary.get("rows_total", 0) or 0),
        "employee_profile_context_rows": len(llm_diag.get("employee_profile_context_rows", []))
        if isinstance(llm_diag.get("employee_profile_context_rows"), list)
        else 0,
        "employee_behavior_markers_rows": len(llm_diag.get("employee_behavior_marker_rows", []))
        if isinstance(llm_diag.get("employee_behavior_marker_rows"), list)
        else 0,
        "writer_mode": "dry_run",
        "write_allowed": False,
        "block_reason": "dry_run_build_only",
    }
    write_json(run_dir / "summary.json", summary)
    write_markdown(run_dir / "summary.md", title="Week Summary", lines=_summary_markdown_lines(summary))
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

    status = execute_week_summary_write(
        cfg=cfg,
        run_dir=run_dir,
        target_sheet_name=str(args.target_sheet or "Свод недели"),
        dry_run=dry_run,
        strict_preflight=bool(args.strict_preflight),
        allow_partial_write=bool(args.allow_partial_write),
        quarantine_unrepaired=bool(args.quarantine_unrepaired),
        logger=logger,
    )
    write_json(run_dir / "week_summary_writer_status.json", status)
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
    write_markdown(run_dir / "summary.md", title="Week Summary", lines=_summary_markdown_lines(summary))
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

