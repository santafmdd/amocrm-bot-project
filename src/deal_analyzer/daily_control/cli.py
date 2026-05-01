from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from src.deal_analyzer.config import DealAnalyzerConfig, load_deal_analyzer_config
from src.deal_analyzer.progress import ProgressReporter
from src.deal_analyzer.daily_control.artifacts import write_json, write_markdown
from src.deal_analyzer.daily_control.daily_analyzer import analyze_daily_packages
from src.deal_analyzer.daily_control.day_grouper import group_by_manager_day
from src.deal_analyzer.daily_control.roks_oap_parser import parse_roks_oap_snapshot
from src.deal_analyzer.daily_control.sheets_writer import (
    build_discovery_markdown,
    discover_daily_control_sheet,
    execute_daily_write,
)
from src.deal_analyzer.daily_control.source_reader import read_call_review_source
from src.deal_analyzer.daily_control.style.deterministic_cleaner import NARRATIVE_FIELDS_DAILY, clean_rows
from src.deal_analyzer.daily_control.style.llm_rewriter import rewrite_rows_with_llm
from src.deal_analyzer.daily_control.style.rewrite_guard import validate_rewrite_row
from src.deal_analyzer.daily_control.style.style_metrics import build_style_metrics
from src.deal_analyzer.daily_control.validation.language_repair import (
    build_language_repair_markdown,
    repair_language_rows,
)
from src.deal_analyzer.daily_control.validation.payload_validator import (
    payload_has_blockers,
    validate_daily_payload_rows,
)
from src.deal_analyzer.daily_control.validation.text_lint import lint_daily_text_rows


def _parse_iso_date(value: str, *, field: str) -> datetime:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except Exception as exc:
        raise RuntimeError(f"Invalid {field}: {value}. Expected YYYY-MM-DD") from exc


def _new_daily_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "daily_control" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily control pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover workbook and daily sheet structure")
    discover.add_argument("--config", required=True, help="Path to deal analyzer config")
    discover.add_argument("--workbook", default="РОКС 2026", help="Workbook display name")
    discover.add_argument("--daily-sheet", default="Дневной контроль", help="Daily control sheet name")
    discover.add_argument("--source-sheet", default="Разбор звонков", help="Source call review sheet name")

    build = sub.add_parser("build", help="Build daily control payload from call review sheet")
    build.add_argument("--config", required=True, help="Path to deal analyzer config")
    build.add_argument("--period-start", required=True, help="YYYY-MM-DD")
    build.add_argument("--period-end", required=True, help="YYYY-MM-DD")
    build.add_argument("--source-sheet", default="Разбор звонков", help="Source call review sheet name")
    build.add_argument("--daily-sheet", default="Дневной контроль", help="Target daily control sheet name")
    build.add_argument("--manager", dest="managers", action="append", default=None)
    build.add_argument("--dry-run", action="store_true")
    build.add_argument("--main-model", default="", help="Override daily-control main model")
    build.add_argument("--fallback-model", default="", help="Override daily-control fallback model")
    build.add_argument("--fallback2-model", default="", help="Optional second fallback model")
    build.add_argument("--fallback-timeout", type=int, default=0, help="Fallback timeout seconds override")
    build.add_argument("--no-retry-on-rate-limit", action="store_true", help="Do not retry the same model after HTTP429 usage-limit errors")
    build.add_argument("--llm-max-attempts", type=int, default=9, help="Max LLM attempts per manager-day")
    build.add_argument("--limit", type=int, default=0, help="Limit manager-day groups for bounded dry-run (0 = all)")
    build.add_argument("--retry-from-run-dir", default="", help="Reuse daily_control_input_groups.json from existing run dir and retry quarantined rows")
    build.add_argument("--style-editor-llm", action="store_true")
    build.add_argument("--style-editor-model", default="")
    build.add_argument("--style-editor-limit", type=int, default=0)
    build.add_argument("--style-editor-timeout", type=int, default=0)

    write = sub.add_parser("write", help="Write prepared daily control payload into sheet")
    write.add_argument("--config", required=True, help="Path to deal analyzer config")
    write.add_argument("--run-dir", required=True, help="Path to daily_control run dir")
    write.add_argument("--daily-sheet", default="Дневной контроль", help="Target daily control sheet name")
    write.add_argument("--dry-run", action="store_true", help="Plan write only (default)")
    write.add_argument("--write", action="store_true", help="Execute real write")
    write.add_argument("--strict-preflight", action="store_true", help="Block write when conflicts are detected")
    write.add_argument("--allow-partial-write", dest="allow_partial_write", action="store_true", help="Allow writing rows without blockers while quarantining bad rows")
    write.add_argument("--no-allow-partial-write", dest="allow_partial_write", action="store_false", help="Block whole batch when any row has blockers")
    write.add_argument("--quarantine-unrepaired", dest="quarantine_unrepaired", action="store_true", help="Quarantine unrepaired rows instead of batch blocking")
    write.add_argument("--no-quarantine-unrepaired", dest="quarantine_unrepaired", action="store_false", help="Do not quarantine unrepaired rows")
    write.set_defaults(allow_partial_write=True, quarantine_unrepaired=True)

    return parser.parse_args()


def _manager_allowlist(cfg: DealAnalyzerConfig, cli_values: list[str] | None) -> tuple[str, ...]:
    if cli_values:
        items = tuple(str(x).strip() for x in cli_values if str(x).strip())
        if items:
            return items
    cfg_values = tuple(str(x).strip() for x in (cfg.daily_manager_allowlist or ()) if str(x).strip())
    if cfg_values:
        return cfg_values
    return ("Илья Бочков", "Рустам Хомидов")


def _style_editor_dir(project_root: Path, run_id: str) -> Path:
    path = project_root / "workspace" / "style_editor" / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run_style_editor(
    *,
    rows: list[dict[str, Any]],
    run_id: str,
    project_root: Path,
    cfg: DealAnalyzerConfig,
    enable_llm: bool,
    model_override: str,
    timeout_override: int,
    row_limit: int,
    logger: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    cleaned_rows, cleanup_counts = clean_rows(rows, fields=NARRATIVE_FIELDS_DAILY)

    style_dir = _style_editor_dir(project_root, run_id)
    rejected_rewrites: list[dict[str, Any]] = []
    llm_rows_used = 0
    llm_rows_failed = 0
    llm_rows_by_model: dict[str, int] = {}

    final_rows = [dict(r) for r in cleaned_rows]
    llm_enabled = bool(enable_llm)

    if llm_enabled:
        target_model = str(model_override or "").strip() or str(cfg.ollama_model or "").strip()
        target_timeout = int(timeout_override or 0) if int(timeout_override or 0) > 0 else int(cfg.ollama_timeout_seconds or 120)
        limit = len(final_rows)
        if int(row_limit or 0) > 0:
            limit = min(limit, int(row_limit))

        for start in range(0, limit, 2):
            batch = []
            for row_index in range(start, min(start + 2, limit)):
                batch.append({
                    "row_index": row_index,
                    "fields": {field: str(final_rows[row_index].get(field, "") or "") for field in NARRATIVE_FIELDS_DAILY if field in final_rows[row_index]},
                })

            rewritten_batch, debug = rewrite_rows_with_llm(
                base_url=str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
                model=target_model,
                timeout_seconds=target_timeout,
                mode="daily_control",
                rows=batch,
                fields=NARRATIVE_FIELDS_DAILY,
            )

            if not bool(debug.get("ok", False)):
                llm_rows_failed += len(batch)
                if logger is not None:
                    logger.warning("daily style editor llm batch failed: %s", debug.get("error", ""))
                continue

            llm_rows_used += len(batch)
            llm_rows_by_model[target_model] = int(llm_rows_by_model.get(target_model, 0) or 0) + len(batch)

            by_index: dict[int, dict[str, Any]] = {}
            for item in rewritten_batch:
                if not isinstance(item, dict):
                    continue
                idx = int(item.get("row_index", -1))
                fields_payload = item.get("fields", {}) if isinstance(item.get("fields"), dict) else {}
                by_index[idx] = fields_payload

            for idx, fields_payload in by_index.items():
                if idx < 0 or idx >= len(final_rows):
                    continue
                candidate = dict(final_rows[idx])
                for field in NARRATIVE_FIELDS_DAILY:
                    if field in fields_payload:
                        candidate[field] = str(fields_payload.get(field, "") or "").strip()
                ok, errors = validate_rewrite_row(
                    original=final_rows[idx],
                    candidate=candidate,
                    narrative_fields=NARRATIVE_FIELDS_DAILY,
                )
                if ok:
                    final_rows[idx] = candidate
                else:
                    rejected_rewrites.append({"row_index": idx, "errors": errors})

    metrics = build_style_metrics(
        llm_enabled=llm_enabled,
        llm_rows_used=llm_rows_used,
        llm_rows_failed=llm_rows_failed,
        llm_rows_by_model=llm_rows_by_model,
        rejected_rewrites_count=len(rejected_rewrites),
        cleanup_counts=cleanup_counts,
    )

    style_input = {"rows": rows, "mode": "daily_control"}
    style_output = {"rows": final_rows, "mode": "daily_control"}

    diff_lines = ["# Style Editor Diff", ""]
    for idx, (before, after) in enumerate(zip(rows, final_rows, strict=False)):
        if not isinstance(before, dict) or not isinstance(after, dict):
            continue
        for field in NARRATIVE_FIELDS_DAILY:
            b = str(before.get(field, "") or "")
            a = str(after.get(field, "") or "")
            if b != a:
                diff_lines.append(f"- row={idx} field={field}")
                diff_lines.append(f"  before: {b[:280]}")
                diff_lines.append(f"  after: {a[:280]}")

    write_json(style_dir / "style_editor_input.json", style_input)
    write_json(style_dir / "style_editor_output.json", style_output)
    write_json(style_dir / "style_editor_metrics.json", metrics)
    write_json(style_dir / "rejected_rewrites.json", {"rejected": rejected_rewrites})
    (style_dir / "style_editor_diff.md").write_text("\n".join(diff_lines).strip() + "\n", encoding="utf-8")

    return final_rows, {"metrics": metrics, "style_dir": str(style_dir), "rejected_rewrites": rejected_rewrites}


def _build_quality_review(rows: list[dict[str, Any]], *, limit: int = 10) -> dict[str, Any]:
    lint = lint_daily_text_rows(rows)
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
                "deal_ids": item.get("deal_ids", ""),
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


def _payload_row_validation_rejections(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        validation = validate_daily_payload_rows([row])
        if payload_has_blockers(validation):
            missing_examples = (
                validation.get("missing_required_examples", [])
                if isinstance(validation.get("missing_required_examples"), list)
                else []
            )
            missing_fields = (
                missing_examples[0].get("missing", [])
                if missing_examples and isinstance(missing_examples[0], dict)
                else []
            )
            reason_parts: list[str] = []
            if missing_fields:
                reason_parts.append(f"missing_required:{','.join(str(x) for x in missing_fields)}")
            if int(validation.get("invalid_date_count", 0) or 0) > 0:
                reason_parts.append("invalid_date")
            if int(validation.get("invalid_score_count", 0) or 0) > 0:
                reason_parts.append("invalid_score")
            if int(validation.get("invalid_criticality_count", 0) or 0) > 0:
                reason_parts.append("invalid_criticality")
            if int(validation.get("duplicate_key_count", 0) or 0) > 0:
                reason_parts.append("duplicate_key")
            rejected_rows.append(
                {
                    "row_index": idx,
                    "manager_name": str(row.get("manager_name") or ""),
                    "control_day_date": str(row.get("control_day_date") or ""),
                    "reason": "payload_validator_blocker" + (f":{'|'.join(reason_parts)}" if reason_parts else ""),
                    "payload_validator": validation,
                    "row": row,
                }
            )
            continue
        valid_rows.append(row)
    return valid_rows, rejected_rows


def _summary_markdown_lines(summary: dict[str, Any]) -> list[str]:
    lines = [
        f"groups_count: {summary.get('groups_count', 0)}",
        f"rows_after_llm_analyzer: {summary.get('rows_after_llm_analyzer', 0)}",
        f"rows_after_language_repair: {summary.get('rows_after_language_repair', 0)}",
        f"rows_after_payload_validator: {summary.get('rows_after_payload_validator', 0)}",
        f"rows_prepared: {summary.get('rows_prepared', 0)}",
        f"rows_in_writer_payload: {summary.get('rows_in_writer_payload', summary.get('rows_prepared', 0))}",
        f"rows_quarantined: {summary.get('rows_quarantined', 0)}",
        f"rows_to_insert: {summary.get('rows_to_insert', 0)}",
        f"rows_to_update: {summary.get('rows_to_update', 0)}",
        f"rows_skipped_existing: {summary.get('rows_skipped_existing', 0)}",
        f"rows_skipped_stale: {summary.get('rows_skipped_stale', 0)}",
        f"conflicts_count: {summary.get('conflicts_count', 0)}",
        f"llm_main_model: {summary.get('llm_main_model', '')}",
        f"llm_fallback_model: {summary.get('llm_fallback_model', '')}",
        f"llm_fallback2_model: {summary.get('llm_fallback2_model', '')}",
        f"llm_attempts_total: {summary.get('llm_attempts_total', 0)}",
        f"llm_success_main: {summary.get('llm_success_main', 0)}",
        f"llm_success_main_repair: {summary.get('llm_success_main_repair', 0)}",
        f"llm_success_main_compact_retry: {summary.get('llm_success_main_compact_retry', 0)}",
        f"llm_success_fallback: {summary.get('llm_success_fallback', 0)}",
        f"llm_success_fallback_repair: {summary.get('llm_success_fallback_repair', 0)}",
        f"llm_success_fallback_compact_retry: {summary.get('llm_success_fallback_compact_retry', 0)}",
        f"llm_success_fallback2: {summary.get('llm_success_fallback2', 0)}",
        f"llm_success_fallback2_repair: {summary.get('llm_success_fallback2_repair', 0)}",
        f"llm_success_fallback2_compact_retry: {summary.get('llm_success_fallback2_compact_retry', 0)}",
        f"llm_json_repair_count: {summary.get('llm_json_repair_count', 0)}",
        f"llm_failed_count: {summary.get('llm_failed_count', 0)}",
        f"fallback_used_count: {summary.get('fallback_used_count', 0)}",
        f"roks_oap_snapshot_status: {summary.get('roks_oap_snapshot_status', '')}",
        f"selected_current_month_sheet: {summary.get('selected_current_month_sheet', '')}",
        f"selected_previous_month_sheet: {summary.get('selected_previous_month_sheet', '')}",
        f"writer mode: {summary.get('writer_mode', 'dry_run')}",
        f"write_allowed: {summary.get('write_allowed', False)}",
        f"block_reason: {summary.get('block_reason', '')}",
    ]

    limitations = summary.get("top_data_limitations", []) if isinstance(summary.get("top_data_limitations"), list) else []
    if limitations:
        lines.append("")
        lines.append("top 5 data limitations:")
        for item in limitations[:5]:
            lines.append(f"- {item}")
    return lines


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _group_from_payload(item: dict[str, Any]) -> Any:
    from src.deal_analyzer.daily_control.models import DailyControlInputGroup

    return DailyControlInputGroup(
        period_start=str(item.get("period_start") or ""),
        period_end=str(item.get("period_end") or ""),
        week_start=str(item.get("week_start") or ""),
        week_end=str(item.get("week_end") or ""),
        control_day_date=str(item.get("control_day_date") or ""),
        day_label=str(item.get("day_label") or ""),
        manager_name=str(item.get("manager_name") or ""),
        manager_role_profile=str(item.get("manager_role_profile") or ""),
        source_rows=(item.get("source_rows") if isinstance(item.get("source_rows"), list) else []),
        sample_size=int(item.get("sample_size") or 0),
        deals_count=int(item.get("deals_count") or 0),
        calls_count=int(item.get("calls_count") or 0),
        deal_ids=[str(x) for x in (item.get("deal_ids") or [])],
        deal_names=[str(x) for x in (item.get("deal_names") or [])],
        deal_links=[str(x) for x in (item.get("deal_links") or [])],
        product_mix=str(item.get("product_mix") or ""),
        base_mix=str(item.get("base_mix") or ""),
        insights=(item.get("insights") if isinstance(item.get("insights"), dict) else {}),
        discipline_signals=(item.get("discipline_signals") if isinstance(item.get("discipline_signals"), dict) else {}),
    )


def _load_retry_groups(
    *,
    retry_from_run_dir: Path,
    logger: Any,
) -> tuple[list[Any], dict[str, Any], dict[str, Any], str]:
    input_groups_payload = _load_json_dict(retry_from_run_dir / "daily_control_input_groups.json")
    groups_raw = (
        input_groups_payload.get("groups")
        if isinstance(input_groups_payload.get("groups"), list)
        else []
    )
    all_groups = [_group_from_payload(item) for item in groups_raw if isinstance(item, dict)]

    quarantine_payload = _load_json_dict(retry_from_run_dir / "daily_control_quarantine.json")
    quarantined_rows = (
        quarantine_payload.get("rows")
        if isinstance(quarantine_payload.get("rows"), list)
        else []
    )
    quarantine_keys = {
        f"{str(item.get('control_day_date') or '')}|{str(item.get('manager_name') or '')}"
        for item in quarantined_rows
        if isinstance(item, dict)
    }
    if quarantine_keys:
        groups = [g for g in all_groups if f"{g.control_day_date}|{g.manager_name}" in quarantine_keys]
    else:
        groups = list(all_groups)

    grouping_diag = (
        input_groups_payload.get("grouping_diagnostics")
        if isinstance(input_groups_payload.get("grouping_diagnostics"), dict)
        else {}
    )
    grouping_diag = dict(grouping_diag)
    grouping_diag["retry_source_run_id"] = retry_from_run_dir.name
    grouping_diag["retry_source_groups_total"] = len(all_groups)
    grouping_diag["retry_source_quarantine_keys_total"] = len(quarantine_keys)
    grouping_diag["retry_groups_selected_total"] = len(groups)

    roks_snapshot = _load_json_dict(retry_from_run_dir / "roks_oap_snapshot.json")
    if not roks_snapshot:
        roks_snapshot = {
            "status": "sheets_not_found",
            "parse_status": "sheets_not_found",
            "warnings": ["retry_run_roks_snapshot_missing"],
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
        }
    payload = _load_json_dict(retry_from_run_dir / "daily_control_payload.json")
    source_sheet_name = str(payload.get("source_sheet") or "Разбор звонков")
    if logger is not None:
        logger.info(
            "daily_control retry-from-run loaded source_run=%s groups=%s quarantine_keys=%s selected=%s",
            retry_from_run_dir.name,
            len(all_groups),
            len(quarantine_keys),
            len(groups),
        )
    return groups, grouping_diag, roks_snapshot, source_sheet_name


def _run_discover(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_daily_run_dir(app_cfg.project_root)

    discovery = discover_daily_control_sheet(
        cfg=cfg,
        workbook_name=str(args.workbook or "РОКС 2026"),
        daily_sheet_name=str(args.daily_sheet or cfg.deal_analyzer_daily_sheet_name or "Дневной контроль"),
        source_sheet_name=str(args.source_sheet or cfg.deal_analyzer_call_review_sheet_name or "Разбор звонков"),
        logger=logger,
    )
    write_json(run_dir / "daily_control_sheet_discovery.json", discovery)
    write_markdown(
        run_dir / "daily_control_sheet_discovery.md",
        title="Daily Control Discovery",
        lines=build_discovery_markdown(discovery),
    )
    print(str(run_dir))


def _run_build(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_daily_run_dir(app_cfg.project_root)
    progress = ProgressReporter(
        process="daily_control",
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

    retry_from_run_dir = Path(str(args.retry_from_run_dir or "")).resolve() if str(args.retry_from_run_dir or "").strip() else None
    managers = _manager_allowlist(cfg, args.managers)

    if retry_from_run_dir is not None:
        progress.update(step_name="load_retry_groups", current=0, total=0, current_item={"stage": "retry_source"})
        groups, grouping_diag, roks_snapshot, source_sheet_name = _load_retry_groups(
            retry_from_run_dir=retry_from_run_dir,
            logger=logger,
        )
        groups_all_count = len(groups)
        if int(args.limit or 0) > 0:
            groups = groups[: int(args.limit)]
        grouping_diag = dict(grouping_diag or {})
        grouping_diag["groups_total_before_limit"] = int(groups_all_count)
        grouping_diag["groups_total_after_limit"] = int(len(groups))
        grouping_diag["groups_limit_applied"] = int(args.limit or 0)
        write_json(
            run_dir / "daily_control_sheet_discovery.json",
            {
                "status": "retry_from_run_dir",
                "retry_source_run_id": retry_from_run_dir.name,
                "source_sheet_name": source_sheet_name,
            },
        )
        write_markdown(
            run_dir / "daily_control_sheet_discovery.md",
            title="Daily Control Discovery",
            lines=[
                "status: retry_from_run_dir",
                f"retry_source_run_id: {retry_from_run_dir.name}",
                f"source_sheet_name: {source_sheet_name}",
            ],
        )
    else:
        progress.update(step_name="sheet_discovery", current=0, total=0, current_item={"stage": "discover"})
        discovery = discover_daily_control_sheet(
            cfg=cfg,
            workbook_name="РОКС 2026",
            daily_sheet_name=str(args.daily_sheet or cfg.deal_analyzer_daily_sheet_name or "Дневной контроль"),
            source_sheet_name=str(args.source_sheet or cfg.deal_analyzer_call_review_sheet_name or "Разбор звонков"),
            logger=logger,
        )
        write_json(run_dir / "daily_control_sheet_discovery.json", discovery)
        write_markdown(
            run_dir / "daily_control_sheet_discovery.md",
            title="Daily Control Discovery",
            lines=build_discovery_markdown(discovery),
        )

        spreadsheet_id = str(discovery.get("spreadsheet_id") or "")
        source_sheet_name = (
            (discovery.get("source_sheet", {}) if isinstance(discovery.get("source_sheet"), dict) else {}).get("title")
            or str(args.source_sheet or cfg.deal_analyzer_call_review_sheet_name or "Разбор звонков")
        )

        source_snapshot = read_call_review_source(
            cfg=cfg,
            spreadsheet_id=spreadsheet_id,
            source_sheet_name=source_sheet_name,
            logger=logger,
        )
        progress.update(
            step_name="source_read_completed",
            current=0,
            total=0,
            current_item={"stage": "source_read", "rows": len(source_snapshot.rows)},
        )

        groups, grouping_diag = group_by_manager_day(
            headers=source_snapshot.headers,
            rows=source_snapshot.rows,
            cfg=cfg,
            period_start=period_start,
            period_end=period_end,
            manager_allowlist=managers,
        )
        groups_all_count = len(groups)
        if int(args.limit or 0) > 0:
            groups = groups[: int(args.limit)]
        grouping_diag = dict(grouping_diag or {})
        grouping_diag["groups_total_before_limit"] = int(groups_all_count)
        grouping_diag["groups_total_after_limit"] = int(len(groups))
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
            roks_snapshot = parse_roks_oap_snapshot(
                client=sheet_client,
                spreadsheet_id=spreadsheet_id,
                period_end=period_end,
                manager_allowlist=managers,
            )

    llm_runtime = {
        "main": {
            "model": str(args.main_model or "").strip() or "qwen3.5:397b-cloud",
            "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
        "fallback": {
            "enabled": True,
            "model": str(args.fallback_model or "").strip() or "deepseek-v3.1:671b-cloud",
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(args.fallback_timeout or 0) if int(args.fallback_timeout or 0) > 0 else int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 120),
        },
        "fallback2": {
            "enabled": bool(str(args.fallback2_model or "").strip()),
            "model": str(args.fallback2_model or "").strip() or "",
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(args.fallback_timeout or 0) if int(args.fallback_timeout or 0) > 0 else int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 120),
        },
        "no_retry_on_rate_limit": bool(args.no_retry_on_rate_limit),
    }

    rows, llm_diag = analyze_daily_packages(
        packages=groups,
        cfg=cfg,
        roks_snapshot=roks_snapshot,
        llm_runtime=llm_runtime,
        logger=logger,
        source_run_id=run_dir.name,
        main_model_override=str(args.main_model or "").strip() or None,
        fallback_model_override=str(args.fallback_model or "").strip() or None,
        fallback2_model_override=str(args.fallback2_model or "").strip() or None,
        fallback_timeout_seconds=(int(args.fallback_timeout or 0) if int(args.fallback_timeout or 0) > 0 else None),
        no_retry_on_rate_limit=bool(args.no_retry_on_rate_limit),
        llm_max_attempts=int(args.llm_max_attempts or 3),
    )
    progress.update(
        step_name="llm_completed",
        current=len(rows),
        total=max(len(groups), len(rows)),
        current_item={"stage": "llm", "model": str(args.main_model or "") or "default"},
    )
    llm_runtime_status = llm_diag.get("llm_runtime", {}) if isinstance(llm_diag.get("llm_runtime"), dict) else {}
    fallback_node = llm_runtime_status.get("fallback", {}) if isinstance(llm_runtime_status.get("fallback"), dict) else {}
    preflight = llm_runtime_status.get("preflight", {}) if isinstance(llm_runtime_status.get("preflight"), dict) else {}
    fallback_preflight = preflight.get("fallback", {}) if isinstance(preflight.get("fallback"), dict) else {}
    fallback_model_name = str(fallback_node.get("model") or "")
    if "gpt-oss:20b" in fallback_model_name.lower() and not bool(fallback_preflight.get("ok", False)):
        logger.warning("local fallback gpt-oss:20b unavailable; run: ollama pull gpt-oss:20b")

    styled_rows, style_debug = _run_style_editor(
        rows=rows,
        run_id=run_dir.name,
        project_root=app_cfg.project_root,
        cfg=cfg,
        enable_llm=bool(args.style_editor_llm),
        model_override=str(args.style_editor_model or "").strip(),
        timeout_override=int(args.style_editor_timeout or 0),
        row_limit=int(args.style_editor_limit or 0),
        logger=logger,
    )

    language_repair = repair_language_rows(
        rows=styled_rows,
        cfg=cfg,
        llm_runtime=(llm_diag.get("llm_runtime", {}) if isinstance(llm_diag.get("llm_runtime"), dict) else {}),
        logger=logger,
        max_attempts=max(1, int(args.llm_max_attempts or 3)),
        enable_llm_repair=True,
    )
    rows_after_language_repair = language_repair.get("rows", []) if isinstance(language_repair.get("rows"), list) else []
    language_quarantined_rows = (
        language_repair.get("quarantined_rows", [])
        if isinstance(language_repair.get("quarantined_rows"), list)
        else []
    )
    writer_rows, payload_validator_rejected_rows = _payload_row_validation_rejections(rows_after_language_repair)
    quarantined_rows = [*language_quarantined_rows, *payload_validator_rejected_rows]

    payload = {
        "mode": "daily_control",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "source_sheet": source_sheet_name,
        "rows": writer_rows,
        "rows_count": len(writer_rows),
        "rows_prepared": len(styled_rows),
        "rows_quarantined": len(quarantined_rows),
        "llm_runtime": llm_diag.get("llm_runtime", {}),
    }

    input_groups_payload = {
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "groups_total": len(groups),
        "groups": [group.__dict__ for group in groups],
        "grouping_diagnostics": grouping_diag,
    }

    quality_review = _build_quality_review(writer_rows, limit=10)

    llm_quarantined_rows = (
        llm_diag.get("quarantined_rows", []) if isinstance(llm_diag.get("quarantined_rows"), list) else []
    )
    row_flow_filtered: list[dict[str, Any]] = []
    for item in llm_quarantined_rows:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "llm_analyzer",
                    "row_index": item.get("row_index"),
                    "manager_name": item.get("manager_name", ""),
                    "control_day_date": item.get("control_day_date", ""),
                    "reason": item.get("reason", "llm_failed"),
                    "error_type": item.get("error_type", ""),
                    "failed_model": item.get("failed_model", ""),
                    "failed_base_url": item.get("failed_base_url", ""),
                    "fallback_reason": item.get("fallback_reason", ""),
                    "failure_stage": item.get("failure_stage", ""),
                    "prompt_size_chars": item.get("prompt_size_chars", 0),
                    "raw_response_preview": item.get("raw_response_preview", ""),
                    "models_attempted": item.get("models_attempted", []),
                    "errors_by_attempt": item.get("errors_by_attempt", []),
                }
            )
    for item in language_quarantined_rows:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "language_repair",
                    "row_index": item.get("row_index"),
                    "manager_name": item.get("manager_name", ""),
                    "control_day_date": item.get("control_day_date", ""),
                    "reason": item.get("reason", "language_blocker_unrepaired"),
                    "repair_trace": item.get("repair_trace", []),
                }
            )
    for item in payload_validator_rejected_rows:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "payload_validator",
                    "row_index": item.get("row_index"),
                    "manager_name": item.get("manager_name", ""),
                    "control_day_date": item.get("control_day_date", ""),
                    "reason": item.get("reason", "payload_validator_blocker"),
                }
            )

    row_flow_debug = {
        "groups_count": len(groups),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_style_editor": len(styled_rows),
        "rows_after_language_repair": len(rows_after_language_repair),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_daily_control_payload": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "quarantine_count": len(quarantined_rows),
        "rejected_rows_count": len(row_flow_filtered),
        "filtered_rows": row_flow_filtered,
    }

    write_json(run_dir / "daily_control_input_groups.json", input_groups_payload)
    write_json(run_dir / "daily_control_llm_requests.json", llm_diag.get("llm_requests", []))
    write_json(run_dir / "daily_control_llm_responses.json", llm_diag.get("llm_responses", []))
    write_json(run_dir / "daily_control_llm_runtime_status.json", llm_runtime_status)
    write_json(run_dir / "daily_control_payload.json", payload)
    write_json(run_dir / "daily_control_row_flow_debug.json", row_flow_debug)
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
    write_json(run_dir / "daily_control_language_repair.json", language_repair)
    write_markdown(
        run_dir / "daily_control_language_repair.md",
        title="Daily Control Language Repair",
        lines=build_language_repair_markdown(language_repair),
    )
    write_json(
        run_dir / "daily_control_quarantine.json",
        {"rows_quarantined": len(quarantined_rows), "rows": quarantined_rows},
    )
    write_json(run_dir / "daily_control_quality_review.json", quality_review)
    write_json(run_dir / "roks_oap_snapshot.json", roks_snapshot)
    write_json(run_dir / "daily_control_style_editor.json", style_debug)

    llm_runtime_payload = llm_diag.get("llm_runtime", {}) if isinstance(llm_diag.get("llm_runtime"), dict) else {}
    llm_preflight_payload = llm_runtime_payload.get("preflight", {}) if isinstance(llm_runtime_payload.get("preflight"), dict) else {}
    preflight_main_status = llm_preflight_payload.get("main", {}) if isinstance(llm_preflight_payload.get("main"), dict) else {}
    preflight_fallback_status = llm_preflight_payload.get("fallback", {}) if isinstance(llm_preflight_payload.get("fallback"), dict) else {}
    preflight_fallback2_status = llm_preflight_payload.get("fallback2", {}) if isinstance(llm_preflight_payload.get("fallback2"), dict) else {}

    summary = {
        "run_id": run_dir.name,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "source_sheet": source_sheet_name,
        "daily_sheet": str(args.daily_sheet or cfg.deal_analyzer_daily_sheet_name or "Дневной контроль"),
        "rows_prepared": len(styled_rows),
        "groups_count": len(groups),
        "groups_total_before_limit": groups_all_count,
        "groups_total_after_limit": len(groups),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_language_repair": len(rows_after_language_repair),
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
        "llm_fallback2_model": (llm_diag.get("llm_runtime", {}).get("fallback2", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("fallback2", {}), dict) else {}).get("model", ""),
        "llm_attempts_total": llm_diag.get("llm_attempts_total", 0),
        "llm_success_main": llm_diag.get("llm_success_main", 0),
        "llm_success_main_repair": llm_diag.get("llm_success_main_repair", 0),
        "llm_success_main_compact_retry": llm_diag.get("llm_success_main_compact_retry", 0),
        "llm_success_fallback": llm_diag.get("llm_success_fallback", 0),
        "llm_success_fallback_repair": llm_diag.get("llm_success_fallback_repair", 0),
        "llm_success_fallback_compact_retry": llm_diag.get("llm_success_fallback_compact_retry", 0),
        "llm_success_fallback2": llm_diag.get("llm_success_fallback2", 0),
        "llm_success_fallback2_repair": llm_diag.get("llm_success_fallback2_repair", 0),
        "llm_success_fallback2_compact_retry": llm_diag.get("llm_success_fallback2_compact_retry", 0),
        "llm_json_repair_count": llm_diag.get("llm_json_repair_count", 0),
        "llm_failed_count": llm_diag.get("llm_failed_count", 0),
        "fallback_used_count": llm_diag.get("fallback_used_count", 0),
        "rows_recovered_by_local_fallback": llm_diag.get("rows_recovered_by_local_fallback", 0),
        "max_prompt_size_chars_seen": llm_diag.get("max_prompt_size_chars_seen", 0),
        "preflight_main_status": preflight_main_status,
        "preflight_fallback_status": preflight_fallback_status,
        "preflight_fallback2_status": preflight_fallback2_status,
        "roks_oap_snapshot_status": roks_snapshot.get("status", ""),
        "selected_current_month_sheet": roks_snapshot.get("selected_current_month_sheet", ""),
        "selected_previous_month_sheet": roks_snapshot.get("selected_previous_month_sheet", ""),
        "writer_mode": "dry_run",
        "write_allowed": False,
        "block_reason": "dry_run_build_only",
        "retry_source_run_id": grouping_diag.get("retry_source_run_id", ""),
        "top_data_limitations": llm_diag.get("top_data_limitations", []),
        "style_editor": style_debug.get("metrics", {}),
        "quality_review": {
            "rows_total": quality_review.get("rows_total", 0),
            "problem_rows_total": quality_review.get("problem_rows_total", 0),
        },
        "employee_profile_context_rows": len(llm_diag.get("employee_profile_context_rows", []))
        if isinstance(llm_diag.get("employee_profile_context_rows"), list)
        else 0,
        "employee_behavior_markers_rows": len(llm_diag.get("employee_behavior_marker_rows", []))
        if isinstance(llm_diag.get("employee_behavior_marker_rows"), list)
        else 0,
    }
    write_json(run_dir / "summary.json", summary)
    write_markdown(run_dir / "summary.md", title="Daily Control Summary", lines=_summary_markdown_lines(summary))
    progress.update(
        step_name="artifacts_written",
        current=len(writer_rows),
        total=max(len(groups), len(writer_rows)),
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
    status = execute_daily_write(
        cfg=cfg,
        run_dir=run_dir,
        daily_sheet_name=str(args.daily_sheet or cfg.deal_analyzer_daily_sheet_name or "Дневной контроль"),
        dry_run=dry_run,
        strict_preflight=bool(args.strict_preflight),
        allow_partial_write=bool(args.allow_partial_write),
        quarantine_unrepaired=bool(args.quarantine_unrepaired),
        logger=logger,
    )

    write_json(run_dir / "daily_control_writer_status.json", status)

    summary_path = run_dir / "summary.json"
    summary = {}
    if summary_path.exists():
        try:
            loaded = json.loads(summary_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                summary = loaded
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
            "structural_changes_required": status.get("structural_changes_required", False),
            "write_allowed": status.get("write_allowed", False),
            "block_reason": status.get("block_reason", ""),
        }
    )
    write_json(summary_path, summary)
    write_markdown(run_dir / "summary.md", title="Daily Control Summary", lines=_summary_markdown_lines(summary))

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
