from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from src.deal_analyzer.config import DealAnalyzerConfig, load_deal_analyzer_config
from src.deal_analyzer.daily_control.style.deterministic_cleaner import clean_rows
from src.deal_analyzer.progress import ProgressReporter
from src.deal_analyzer.week_plan.plan_analyzer import analyze_week_plan_groups
from src.deal_analyzer.week_plan.roks_enrichment import build_roks_oap_snapshot
from src.deal_analyzer.week_plan.sheets_writer import (
    build_discovery_markdown as build_week_plan_discovery_markdown,
    discover_week_plan_sheet,
    execute_week_plan_write,
)
from src.deal_analyzer.week_plan.source_reader import read_daily_control_source, resolve_spreadsheet_id
from src.deal_analyzer.week_plan.validation import (
    lint_has_blockers as week_plan_lint_has_blockers,
    lint_week_plan_text_rows,
    payload_has_blockers as week_plan_payload_has_blockers,
    validate_week_plan_payload_rows,
)
from src.deal_analyzer.week_plan.weekly_signal_builder import group_daily_rows_into_week_signals
from src.deal_analyzer.week_summary.aggregator import build_week_summary_groups
from src.deal_analyzer.week_summary.analyzer import analyze_week_summary_groups
from src.deal_analyzer.week_summary.sheets_writer import (
    build_discovery_markdown as build_week_summary_discovery_markdown,
    discover_week_summary_sheet,
    execute_week_summary_write,
)
from src.deal_analyzer.week_summary.validation import (
    lint_has_blockers as week_summary_lint_has_blockers,
    lint_week_summary_text_rows,
    payload_has_blockers as week_summary_payload_has_blockers,
    validate_week_summary_payload_rows,
)
from src.deal_analyzer.weekly_manager_summary.sheets_writer import (
    build_discovery_markdown as build_weekly_manager_discovery_markdown,
    discover_weekly_manager_sheet,
    execute_weekly_write,
)
from src.deal_analyzer.weekly_manager_summary.validation import (
    payload_has_blockers as weekly_manager_payload_has_blockers,
    validate_weekly_payload_rows,
)
from src.deal_analyzer.weekly_manager_summary.week_grouper import group_daily_rows_by_week_manager
from src.deal_analyzer.weekly_manager_summary.weekly_analyzer import analyze_weekly_groups
from src.deal_analyzer.weekly_shared.artifacts import write_json, write_markdown


PLAN_HEADERS: tuple[str, ...] = (
    "План недели с",
    "План недели по",
    "Дата",
    "День",
    "Адресат",
    "Тип активности",
    "Что делаю",
    "Статус",
    "Ссылка на обучение / материал",
    "Ссылка на задачи после обучения",
    "Какую задачу даю",
)

MANAGER_HEADERS: tuple[str, ...] = (
    "Неделя с",
    "Неделя по",
    "Менеджер",
    "Роль менеджера",
    "Проанализировано сделок",
    "Средняя оценка 0-100",
    "Итог недели",
    "Что улучшилось",
    "Что не улучшилось",
    "Повторяющиеся ошибки",
    "Обучение сотруднику",
    "Ссылка на обучение",
    "Задачи после обучения",
    "Ссылка на задачи после обучения",
    "Мои действия на следующую неделю",
    "Ожидаемый эффект - количество",
    "Ожидаемый эффект - качество",
    "Формулировка для руководителя",
    "Сообщение сотруднику",
)


def _parse_iso_date(value: str, *, field: str):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception as exc:
        raise RuntimeError(f"Invalid {field}: {value}. Expected YYYY-MM-DD") from exc


def _resolve_cycle_periods(args: argparse.Namespace) -> dict[str, Any]:
    signal_start_raw = str(getattr(args, "signal_start", "") or "").strip()
    signal_end_raw = str(getattr(args, "signal_end", "") or "").strip()
    plan_start_raw = str(getattr(args, "plan_week_start", "") or "").strip()
    plan_end_raw = str(getattr(args, "plan_week_end", "") or "").strip()
    legacy_start_raw = str(getattr(args, "period_start", "") or "").strip()
    legacy_end_raw = str(getattr(args, "period_end", "") or "").strip()
    warnings: list[str] = []
    has_new = any([signal_start_raw, signal_end_raw, plan_start_raw, plan_end_raw])
    if has_new:
        missing: list[str] = []
        if not signal_start_raw:
            missing.append("signal_start")
        if not signal_end_raw:
            missing.append("signal_end")
        if not plan_start_raw:
            missing.append("plan_week_start")
        if not plan_end_raw:
            missing.append("plan_week_end")
        if missing:
            raise RuntimeError(f"Missing required period args: {', '.join(missing)}")
        signal_start = _parse_iso_date(signal_start_raw, field="signal_start")
        signal_end = _parse_iso_date(signal_end_raw, field="signal_end")
        plan_week_start = _parse_iso_date(plan_start_raw, field="plan_week_start")
        plan_week_end = _parse_iso_date(plan_end_raw, field="plan_week_end")
    else:
        if not legacy_start_raw or not legacy_end_raw:
            raise RuntimeError(
                "Provide either --signal-start/--signal-end + --plan-week-start/--plan-week-end "
                "or legacy --period-start/--period-end."
            )
        signal_start = _parse_iso_date(legacy_start_raw, field="period_start")
        signal_end = _parse_iso_date(legacy_end_raw, field="period_end")
        plan_week_start = signal_start
        plan_week_end = signal_end
        warnings.append("legacy_period_used_for_signal_and_plan")
    if signal_end < signal_start:
        raise RuntimeError("signal_end must be >= signal_start")
    if plan_week_end < plan_week_start:
        raise RuntimeError("plan_week_end must be >= plan_week_start")
    return {
        "signal_start": signal_start,
        "signal_end": signal_end,
        "plan_week_start": plan_week_start,
        "plan_week_end": plan_week_end,
        "period_warnings": warnings,
    }


def _new_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "weekly_cycle" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _manager_allowlist(cfg: DealAnalyzerConfig) -> tuple[str, ...]:
    cfg_values = tuple(str(x).strip() for x in (cfg.daily_manager_allowlist or ()) if str(x).strip())
    return cfg_values or ("Илья Бочков", "Рустам Хомидов")


def _runtime(cfg: DealAnalyzerConfig, *, main_model: str, fallback_model: str) -> dict[str, Any]:
    return {
        "main": {
            "model": main_model,
            "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
        "fallback": {
            "enabled": True,
            "model": fallback_model,
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(
                cfg.ollama_fallback_preflight_timeout_seconds or cfg.ollama_preflight_timeout_seconds or 20
            ),
        },
    }


def _reject_week_plan_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ok, rejected = [], []
    for idx, row in enumerate(rows):
        lint = lint_week_plan_text_rows([row])
        payload_validation = validate_week_plan_payload_rows([row])
        if week_plan_lint_has_blockers(lint) or week_plan_payload_has_blockers(payload_validation):
            rejected.append({
                "row_index": idx,
                "recipient": str(row.get("recipient") or ""),
                "plan_date": str(row.get("plan_date") or ""),
                "reason": "payload_validator_blocker",
                "text_lint": lint,
                "payload_validator": payload_validation,
                "row": row,
            })
            continue
        ok.append(row)
    return ok, rejected


def _reject_weekly_manager_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ok, rejected = [], []
    for idx, row in enumerate(rows):
        payload_validation = validate_weekly_payload_rows([row])
        if weekly_manager_payload_has_blockers(payload_validation):
            rejected.append({
                "row_index": idx,
                "manager_name": str(row.get("manager_name") or ""),
                "week_start": str(row.get("week_start") or ""),
                "week_end": str(row.get("week_end") or ""),
                "reason": "payload_validator_blocker",
                "payload_validator": payload_validation,
                "row": row,
            })
            continue
        ok.append(row)
    return ok, rejected


def _reject_week_summary_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ok, rejected = [], []
    for idx, row in enumerate(rows):
        lint = lint_week_summary_text_rows([row])
        payload_validation = validate_week_summary_payload_rows([row])
        if week_summary_lint_has_blockers(lint) or week_summary_payload_has_blockers(payload_validation):
            rejected.append({
                "row_index": idx,
                "week_start": str(row.get("week_start") or ""),
                "week_end": str(row.get("week_end") or ""),
                "reason": "payload_validator_blocker",
                "text_lint": lint,
                "payload_validator": payload_validation,
                "row": row,
            })
            continue
        ok.append(row)
    return ok, rejected


def _plan_rows_table(rows: list[dict[str, Any]]) -> tuple[list[str], list[list[str]]]:
    out: list[list[str]] = []
    for row in rows:
        out.append([
            str(row.get("plan_week_start", "") or ""),
            str(row.get("plan_week_end", "") or ""),
            str(row.get("plan_date", "") or ""),
            str(row.get("day_label", "") or ""),
            str(row.get("recipient", "") or ""),
            str(row.get("activity_type", "") or ""),
            str(row.get("what_i_do", "") or ""),
            str(row.get("status", "") or ""),
            str(row.get("training_link", "") or ""),
            str(row.get("post_training_task_link", "") or ""),
            str(row.get("task_to_assign", "") or ""),
        ])
    return list(PLAN_HEADERS), out


def _manager_rows_table(rows: list[dict[str, Any]]) -> tuple[list[str], list[list[str]]]:
    out: list[list[str]] = []
    for row in rows:
        out.append([
            str(row.get("week_start", "") or ""),
            str(row.get("week_end", "") or ""),
            str(row.get("manager_name", "") or ""),
            str(row.get("manager_role_profile", "") or ""),
            str(row.get("deals_count", "") or ""),
            str(row.get("avg_score_0_100", "") or ""),
            str(row.get("weekly_result", "") or ""),
            str(row.get("improved", "") or ""),
            str(row.get("not_improved", "") or ""),
            str(row.get("repeating_mistakes", "") or ""),
            str(row.get("training_for_employee", "") or ""),
            str(row.get("training_link", "") or ""),
            str(row.get("post_training_tasks", "") or ""),
            str(row.get("post_training_tasks_link", "") or ""),
            str(row.get("manager_actions_next_week", "") or ""),
            str(row.get("expected_quantity_effect", "") or ""),
            str(row.get("expected_quality_effect", "") or ""),
            str(row.get("manager_report_phrase", "") or ""),
            str(row.get("employee_message", "") or ""),
        ])
    return list(MANAGER_HEADERS), out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Weekly integrated pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    cmd = sub.add_parser("build-cycle", help="Run week_plan -> weekly_manager_summary -> week_summary in dry-run")
    cmd.add_argument("--config", required=True)
    cmd.add_argument("--period-start", default="")
    cmd.add_argument("--period-end", default="")
    cmd.add_argument("--signal-start", default="")
    cmd.add_argument("--signal-end", default="")
    cmd.add_argument("--plan-week-start", default="")
    cmd.add_argument("--plan-week-end", default="")
    cmd.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    cmd.add_argument("--plan-sheet", default="План недели")
    cmd.add_argument("--manager-summary-sheet", default="Недельный свод менеджеров")
    cmd.add_argument("--week-summary-sheet", default="Свод недели")
    cmd.add_argument("--main-model", default="deepseek-v4-pro:cloud")
    cmd.add_argument("--fallback-model", default="deepseek-v4-flash:cloud")
    cmd.add_argument("--llm-max-attempts", type=int, default=6)
    cmd.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _run_build_cycle(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)
    progress = ProgressReporter(
        process="weekly_cycle",
        run_dir=run_dir,
        heartbeat_seconds=int(getattr(cfg, "progress_heartbeat_seconds", 30) or 30),
        logger=logger,
        step_name="init",
        total=3,
    )

    periods = _resolve_cycle_periods(args)
    signal_start = periods["signal_start"]
    signal_end = periods["signal_end"]
    plan_week_start = periods["plan_week_start"]
    plan_week_end = periods["plan_week_end"]
    period_warnings = list(periods.get("period_warnings", []) or [])
    progress.update(
        step_name="period_resolved",
        current=0,
        total=3,
        current_item={"stage": "period", "date": f"{plan_week_start.isoformat()}..{plan_week_end.isoformat()}"},
    )

    main_model = str(args.main_model or "").strip() or "deepseek-v4-pro:cloud"
    fallback_model = str(args.fallback_model or "").strip() or "deepseek-v4-flash:cloud"
    runtime = _runtime(cfg, main_model=main_model, fallback_model=fallback_model)

    week_plan_discovery = discover_week_plan_sheet(cfg=cfg, workbook_name="РОКС 2026", source_sheet_name=str(args.daily_sheet), target_sheet_name=str(args.plan_sheet), logger=logger)
    weekly_manager_discovery = discover_weekly_manager_sheet(cfg=cfg, workbook_name="РОКС 2026", source_sheet_name=str(args.daily_sheet), target_sheet_name=str(args.manager_summary_sheet), plan_sheet_name=str(args.plan_sheet), logger=logger)
    week_summary_discovery = discover_week_summary_sheet(cfg=cfg, workbook_name="РОКС 2026", manager_summary_sheet_name=str(args.manager_summary_sheet), plan_sheet_name=str(args.plan_sheet), target_sheet_name=str(args.week_summary_sheet), logger=logger)
    write_json(run_dir / "week_plan_sheet_discovery.json", week_plan_discovery)
    write_markdown(run_dir / "week_plan_sheet_discovery.md", title="Week Plan Discovery", lines=build_week_plan_discovery_markdown(week_plan_discovery))
    write_json(run_dir / "weekly_manager_sheet_discovery.json", weekly_manager_discovery)
    write_markdown(run_dir / "weekly_manager_sheet_discovery.md", title="Weekly Manager Discovery", lines=build_weekly_manager_discovery_markdown(weekly_manager_discovery))
    write_json(run_dir / "week_summary_sheet_discovery.json", week_summary_discovery)
    write_markdown(run_dir / "week_summary_sheet_discovery.md", title="Week Summary Discovery", lines=build_week_summary_discovery_markdown(week_summary_discovery))
    progress.update(step_name="discover_completed", current=0, total=3, current_item={"stage": "discover"})

    spreadsheet_id = resolve_spreadsheet_id(cfg)
    source_sheet_name = ((week_plan_discovery.get("source_sheet") or {}) if isinstance(week_plan_discovery.get("source_sheet"), dict) else {}).get("title") or str(args.daily_sheet)
    plan_sheet_name = ((week_plan_discovery.get("target_sheet") or {}) if isinstance(week_plan_discovery.get("target_sheet"), dict) else {}).get("title") or str(args.plan_sheet)
    manager_sheet_name = ((weekly_manager_discovery.get("target_sheet") or {}) if isinstance(weekly_manager_discovery.get("target_sheet"), dict) else {}).get("title") or str(args.manager_summary_sheet)
    week_summary_sheet_name = ((week_summary_discovery.get("target_sheet") or {}) if isinstance(week_summary_discovery.get("target_sheet"), dict) else {}).get("title") or str(args.week_summary_sheet)

    daily_snapshot = read_daily_control_source(cfg=cfg, spreadsheet_id=spreadsheet_id, source_sheet_name=source_sheet_name, logger=logger)
    write_json(run_dir / "week_plan_source_rows.json", {"headers": daily_snapshot.headers, "rows": daily_snapshot.rows})
    progress.update(
        step_name="source_read_completed",
        current=0,
        total=3,
        current_item={"stage": "source_read", "rows": len(daily_snapshot.rows)},
    )

    managers = _manager_allowlist(cfg)
    try:
        from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

        app_root = Path(cfg.config_path).resolve().parents[1]
        sheet_client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
        roks_snapshot = build_roks_oap_snapshot(
            client=sheet_client,
            spreadsheet_id=spreadsheet_id,
            week_start=plan_week_start.isoformat(),
            week_end=plan_week_end.isoformat(),
            manager_allowlist=managers,
        )
    except Exception:
        roks_snapshot = {
            "status": "access_error",
            "parse_status": "access_error",
            "warnings": ["google_sheets_client_init_failed"],
            "selected_current_month_sheet": "",
            "selected_previous_month_sheet": "",
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
        }
    write_json(run_dir / "roks_oap_snapshot.json", roks_snapshot)

    week_plan_groups, week_plan_grouping_diag = group_daily_rows_into_week_signals(
        headers=daily_snapshot.headers,
        rows=daily_snapshot.rows,
        period_start=signal_start,
        period_end=signal_end,
        manager_allowlist=managers,
        plan_week_start_override=plan_week_start.isoformat(),
        plan_week_end_override=plan_week_end.isoformat(),
    )
    week_plan_rows_raw, week_plan_llm_diag = analyze_week_plan_groups(groups=week_plan_groups, cfg=cfg, roks_snapshot=roks_snapshot, llm_runtime=runtime, logger=logger, source_run_id=run_dir.name, main_model_override=main_model, fallback_model_override=fallback_model, llm_max_attempts=int(args.llm_max_attempts or 6))
    week_plan_rows, _cleanup = clean_rows(week_plan_rows_raw, fields=("what_i_do", "task_to_assign", "what_to_check", "daily_meeting_thesis", "expected_quantity_effect", "expected_quality_effect"))
    week_plan_writer_rows, week_plan_rejected = _reject_week_plan_rows(week_plan_rows)
    week_plan_llm_quarantine = week_plan_llm_diag.get("quarantined_rows", []) if isinstance(week_plan_llm_diag.get("quarantined_rows"), list) else []
    week_plan_quarantine_rows = [*week_plan_llm_quarantine, *week_plan_rejected]
    week_plan_payload = {
        "mode": "week_plan",
        "period_start": signal_start.isoformat(),
        "period_end": signal_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "source_sheet": source_sheet_name,
        "target_sheet": plan_sheet_name,
        "rows": week_plan_writer_rows,
        "rows_count": len(week_plan_writer_rows),
        "rows_prepared": len(week_plan_rows_raw),
        "rows_quarantined": len(week_plan_quarantine_rows),
        "llm_runtime": week_plan_llm_diag.get("llm_runtime", {}),
    }
    write_json(run_dir / "week_plan_payload.json", week_plan_payload)
    write_json(run_dir / "week_plan_quarantine.json", {"rows_quarantined": len(week_plan_quarantine_rows), "rows": week_plan_quarantine_rows})
    week_plan_managers_in_payload = sorted(
        {
            str(x.get("recipient") or "").strip()
            for x in week_plan_writer_rows
            if isinstance(x, dict) and str(x.get("recipient") or "").strip()
        }
    )
    write_json(
        run_dir / "week_plan_row_flow_debug.json",
        {
            "groups_count": len(week_plan_groups),
            "rows_after_llm_analyzer": len(week_plan_rows_raw),
            "rows_after_payload_validator": len(week_plan_writer_rows),
            "rows_in_writer_payload": len(week_plan_writer_rows),
            "quarantine_count": len(week_plan_quarantine_rows),
            "filtered_rows": week_plan_quarantine_rows,
            "managers_in_daily_control": week_plan_grouping_diag.get("managers_in_daily_control", []),
            "managers_in_groups": week_plan_grouping_diag.get("managers_in_groups", []),
            "managers_in_payload": week_plan_managers_in_payload,
            "managers_in_plan_payload": week_plan_managers_in_payload,
            "managers_skipped_with_reason": week_plan_grouping_diag.get("managers_skipped_with_reason", []),
        },
    )
    write_json(run_dir / "week_plan_quality_review.json", {"text_lint": lint_week_plan_text_rows(week_plan_writer_rows)})
    progress.update(
        step_name="week_plan_completed",
        current=1,
        total=3,
        current_item={"stage": "week_plan", "rows": len(week_plan_writer_rows)},
    )

    week_plan_status = execute_week_plan_write(cfg=cfg, run_dir=run_dir, target_sheet_name=plan_sheet_name, dry_run=True, strict_preflight=True, allow_partial_write=True, quarantine_unrepaired=True, logger=logger)
    write_json(run_dir / "week_plan_writer_status.json", week_plan_status)

    plan_headers, plan_rows = _plan_rows_table(week_plan_writer_rows)
    week_manager_groups, week_manager_grouping_diag, week_manager_plan_fact_rows = group_daily_rows_by_week_manager(
        headers=daily_snapshot.headers,
        rows=daily_snapshot.rows,
        period_start=plan_week_start,
        period_end=plan_week_end,
        manager_allowlist=managers,
        plan_headers=plan_headers,
        plan_rows=plan_rows,
    )
    week_manager_rows_raw, week_manager_llm_diag = analyze_weekly_groups(groups=week_manager_groups, cfg=cfg, roks_snapshot=roks_snapshot, llm_runtime=runtime, logger=logger, source_run_id=run_dir.name, main_model_override=main_model, fallback_model_override=fallback_model, llm_max_attempts=int(args.llm_max_attempts or 6))
    week_manager_rows, _ = clean_rows(week_manager_rows_raw, fields=("weekly_result", "improved", "not_improved", "repeating_mistakes", "training_for_employee", "post_training_tasks", "manager_actions_next_week", "expected_quantity_effect", "expected_quality_effect", "manager_report_phrase", "employee_message"))
    week_manager_writer_rows, week_manager_rejected = _reject_weekly_manager_rows(week_manager_rows)
    week_manager_llm_quarantine = week_manager_llm_diag.get("quarantined_rows", []) if isinstance(week_manager_llm_diag.get("quarantined_rows"), list) else []
    week_manager_quarantine_rows = [*week_manager_llm_quarantine, *week_manager_rejected]
    weekly_manager_payload = {
        "mode": "weekly_manager_summary",
        "period_start": plan_week_start.isoformat(),
        "period_end": plan_week_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "source_sheet": source_sheet_name,
        "plan_sheet": plan_sheet_name,
        "target_sheet": manager_sheet_name,
        "rows": week_manager_writer_rows,
        "rows_count": len(week_manager_writer_rows),
        "rows_prepared": len(week_manager_rows_raw),
        "rows_quarantined": len(week_manager_quarantine_rows),
        "llm_runtime": week_manager_llm_diag.get("llm_runtime", {}),
    }
    write_json(run_dir / "weekly_manager_plan_fact_rows.json", week_manager_plan_fact_rows)
    write_json(run_dir / "weekly_manager_payload.json", weekly_manager_payload)
    write_json(run_dir / "weekly_manager_quarantine.json", {"rows_quarantined": len(week_manager_quarantine_rows), "rows": week_manager_quarantine_rows})
    week_manager_managers_in_payload = sorted(
        {
            str(x.get("manager_name") or "").strip()
            for x in week_manager_writer_rows
            if isinstance(x, dict) and str(x.get("manager_name") or "").strip()
        }
    )
    write_json(
        run_dir / "weekly_manager_row_flow_debug.json",
        {
            "groups_count": len(week_manager_groups),
            "rows_after_llm_analyzer": len(week_manager_rows_raw),
            "rows_after_payload_validator": len(week_manager_writer_rows),
            "rows_in_writer_payload": len(week_manager_writer_rows),
            "quarantine_count": len(week_manager_quarantine_rows),
            "filtered_rows": week_manager_quarantine_rows,
            "managers_in_daily_control": week_manager_grouping_diag.get("managers_in_daily_control", []),
            "managers_in_groups": week_manager_grouping_diag.get("managers_in_groups", []),
            "managers_in_payload": week_manager_managers_in_payload,
            "managers_skipped_with_reason": week_manager_grouping_diag.get("managers_skipped_with_reason", []),
        },
    )

    weekly_manager_status = execute_weekly_write(cfg=cfg, run_dir=run_dir, target_sheet_name=manager_sheet_name, dry_run=True, strict_preflight=True, allow_partial_write=True, quarantine_unrepaired=True, logger=logger)
    write_json(run_dir / "weekly_manager_writer_status.json", weekly_manager_status)
    progress.update(
        step_name="weekly_manager_completed",
        current=2,
        total=3,
        current_item={"stage": "weekly_manager", "rows": len(week_manager_writer_rows)},
    )

    manager_headers, manager_rows = _manager_rows_table(week_manager_writer_rows)
    week_summary_groups, week_summary_grouping_diag, week_summary_plan_fact_rows = build_week_summary_groups(
        manager_headers=manager_headers,
        manager_rows=manager_rows,
        plan_headers=plan_headers,
        plan_rows=plan_rows,
        period_start=plan_week_start,
        period_end=plan_week_end,
        daily_headers=daily_snapshot.headers,
        daily_rows=daily_snapshot.rows,
    )
    week_summary_rows_raw, week_summary_llm_diag = analyze_week_summary_groups(groups=week_summary_groups, cfg=cfg, roks_snapshot=roks_snapshot, llm_runtime=runtime, logger=logger, source_run_id=run_dir.name, main_model_override=main_model, fallback_model_override=fallback_model, llm_max_attempts=int(args.llm_max_attempts or 6))
    week_summary_writer_rows, week_summary_rejected = _reject_week_summary_rows(week_summary_rows_raw)
    week_summary_llm_quarantine = week_summary_llm_diag.get("quarantined_rows", []) if isinstance(week_summary_llm_diag.get("quarantined_rows"), list) else []
    week_summary_quarantine_rows = [*week_summary_llm_quarantine, *week_summary_rejected]
    week_summary_payload = {
        "mode": "week_summary",
        "period_start": plan_week_start.isoformat(),
        "period_end": plan_week_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "manager_summary_sheet": manager_sheet_name,
        "plan_sheet": plan_sheet_name,
        "target_sheet": week_summary_sheet_name,
        "rows": week_summary_writer_rows,
        "rows_count": len(week_summary_writer_rows),
        "rows_prepared": len(week_summary_rows_raw),
        "rows_quarantined": len(week_summary_quarantine_rows),
        "llm_runtime": week_summary_llm_diag.get("llm_runtime", {}),
    }
    write_json(run_dir / "week_summary_plan_fact_rows.json", week_summary_plan_fact_rows)
    write_json(run_dir / "week_summary_payload.json", week_summary_payload)
    write_json(run_dir / "week_summary_quarantine.json", {"rows_quarantined": len(week_summary_quarantine_rows), "rows": week_summary_quarantine_rows})
    write_json(run_dir / "week_summary_row_flow_debug.json", {"groups_count": len(week_summary_groups), "rows_after_llm_analyzer": len(week_summary_rows_raw), "rows_after_payload_validator": len(week_summary_writer_rows), "rows_in_writer_payload": len(week_summary_writer_rows), "quarantine_count": len(week_summary_quarantine_rows), "filtered_rows": week_summary_quarantine_rows})

    week_summary_status = execute_week_summary_write(cfg=cfg, run_dir=run_dir, target_sheet_name=week_summary_sheet_name, dry_run=True, strict_preflight=True, allow_partial_write=True, quarantine_unrepaired=True, logger=logger)
    write_json(run_dir / "week_summary_writer_status.json", week_summary_status)

    write_json(
        run_dir / "weekly_cycle_row_flow_debug.json",
        {
            "week_plan": {
                "groups_count": len(week_plan_groups),
                "rows_prepared": len(week_plan_rows_raw),
                "rows_in_writer_payload": len(week_plan_writer_rows),
                "rows_quarantined": len(week_plan_quarantine_rows),
                "managers_in_daily_control": week_plan_grouping_diag.get("managers_in_daily_control", []),
                "managers_in_signal_period": week_plan_grouping_diag.get("managers_in_signal_period", []),
                "managers_in_groups": week_plan_grouping_diag.get("managers_in_groups", []),
                "managers_in_payload": week_plan_managers_in_payload,
                "managers_in_plan_payload": week_plan_managers_in_payload,
                "managers_skipped_with_reason": week_plan_grouping_diag.get("managers_skipped_with_reason", []),
            },
            "weekly_manager_summary": {
                "groups_count": len(week_manager_groups),
                "rows_prepared": len(week_manager_rows_raw),
                "rows_in_writer_payload": len(week_manager_writer_rows),
                "rows_quarantined": len(week_manager_quarantine_rows),
                "managers_in_daily_control": week_manager_grouping_diag.get("managers_in_daily_control", []),
                "managers_in_groups": week_manager_grouping_diag.get("managers_in_groups", []),
                "managers_in_payload": week_manager_managers_in_payload,
                "managers_skipped_with_reason": week_manager_grouping_diag.get("managers_skipped_with_reason", []),
            },
            "week_summary": {
                "groups_count": len(week_summary_groups),
                "rows_prepared": len(week_summary_rows_raw),
                "rows_in_writer_payload": len(week_summary_writer_rows),
                "rows_quarantined": len(week_summary_quarantine_rows),
            },
        },
    )

    cycle_summary = {
        "run_id": run_dir.name,
        "period_start": plan_week_start.isoformat(),
        "period_end": plan_week_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "period_warnings": period_warnings,
        "week_plan": {
            "rows_prepared": len(week_plan_rows_raw),
            "rows_in_writer_payload": len(week_plan_writer_rows),
            "rows_quarantined": len(week_plan_quarantine_rows),
            "write_strategy": week_plan_status.get("write_strategy", ""),
            "structural_changes_required": week_plan_status.get("structural_changes_required", False),
            "managers_in_signal_period": week_plan_grouping_diag.get("managers_in_signal_period", []),
            "managers_in_payload": week_plan_managers_in_payload,
            "managers_in_plan_payload": week_plan_managers_in_payload,
        },
        "weekly_manager_summary": {
            "rows_prepared": len(week_manager_rows_raw),
            "rows_in_writer_payload": len(week_manager_writer_rows),
            "rows_quarantined": len(week_manager_quarantine_rows),
            "write_strategy": weekly_manager_status.get("write_strategy", ""),
            "structural_changes_required": weekly_manager_status.get("structural_changes_required", False),
            "managers_in_groups": week_manager_grouping_diag.get("managers_in_groups", []),
            "managers_in_payload": week_manager_managers_in_payload,
        },
        "week_summary": {
            "rows_prepared": len(week_summary_rows_raw),
            "rows_in_writer_payload": len(week_summary_writer_rows),
            "rows_quarantined": len(week_summary_quarantine_rows),
            "write_strategy": week_summary_status.get("write_strategy", ""),
            "structural_changes_required": week_summary_status.get("structural_changes_required", False),
        },
        "roks_oap_status": roks_snapshot.get("status", ""),
        "selected_current_month_sheet": roks_snapshot.get("selected_current_month_sheet", ""),
        "selected_previous_month_sheet": roks_snapshot.get("selected_previous_month_sheet", ""),
    }
    write_json(run_dir / "weekly_cycle_summary.json", cycle_summary)
    write_markdown(run_dir / "weekly_cycle_summary.md", title="Weekly Cycle Summary", lines=[
        f"run_id: {cycle_summary.get('run_id', '')}",
        f"signal_period: {signal_start.isoformat()}..{signal_end.isoformat()}",
        f"plan_week: {plan_week_start.isoformat()}..{plan_week_end.isoformat()}",
        f"roks_oap_status: {cycle_summary.get('roks_oap_status', '')}",
        f"week_plan rows_in_writer_payload: {(cycle_summary.get('week_plan') or {}).get('rows_in_writer_payload', 0)}",
        f"weekly_manager_summary rows_in_writer_payload: {(cycle_summary.get('weekly_manager_summary') or {}).get('rows_in_writer_payload', 0)}",
        f"week_summary rows_in_writer_payload: {(cycle_summary.get('week_summary') or {}).get('rows_in_writer_payload', 0)}",
    ])
    progress.update(
        step_name="week_summary_completed",
        current=3,
        total=3,
        current_item={"stage": "week_summary", "rows": len(week_summary_writer_rows)},
    )
    progress.finish(status="completed", step_name="build_cycle_completed")
    print(str(run_dir))


def main() -> None:
    args = _parse_args()
    if args.command == "build-cycle":
        _run_build_cycle(args)
        return
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
