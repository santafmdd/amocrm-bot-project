from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from src.deal_analyzer.config import DealAnalyzerConfig, load_deal_analyzer_config
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


def _summary_markdown_lines(summary: dict[str, Any]) -> list[str]:
    lines = [
        f"rows_prepared: {summary.get('rows_prepared', 0)}",
        f"rows_to_insert: {summary.get('rows_to_insert', 0)}",
        f"rows_skipped_existing: {summary.get('rows_skipped_existing', 0)}",
        f"conflicts_count: {summary.get('conflicts_count', 0)}",
        f"llm_main_model: {summary.get('llm_main_model', '')}",
        f"llm_fallback_model: {summary.get('llm_fallback_model', '')}",
        f"llm_success_main: {summary.get('llm_success_main', 0)}",
        f"llm_success_fallback: {summary.get('llm_success_fallback', 0)}",
        f"llm_json_repair_count: {summary.get('llm_json_repair_count', 0)}",
        f"llm_failed_count: {summary.get('llm_failed_count', 0)}",
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

    period_start = _parse_iso_date(str(args.period_start), field="period_start").date()
    period_end = _parse_iso_date(str(args.period_end), field="period_end").date()
    if period_end < period_start:
        raise RuntimeError("period_end must be >= period_start")

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

    managers = _manager_allowlist(cfg, args.managers)
    groups, grouping_diag = group_by_manager_day(
        headers=source_snapshot.headers,
        rows=source_snapshot.rows,
        cfg=cfg,
        period_start=period_start,
        period_end=period_end,
        manager_allowlist=managers,
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
            "model": str(args.main_model or "").strip() or "gemma4:31b-cloud",
            "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
        "fallback": {
            "enabled": True,
            "model": str(args.fallback_model or "").strip() or "deepseek-v3.1:671b-cloud",
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 120),
        },
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
    )

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

    payload = {
        "mode": "daily_control",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "source_sheet": source_sheet_name,
        "rows": styled_rows,
        "rows_count": len(styled_rows),
        "llm_runtime": llm_diag.get("llm_runtime", {}),
    }

    input_groups_payload = {
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "groups_total": len(groups),
        "groups": [group.__dict__ for group in groups],
        "grouping_diagnostics": grouping_diag,
    }

    quality_review = _build_quality_review(styled_rows, limit=10)

    write_json(run_dir / "daily_control_input_groups.json", input_groups_payload)
    write_json(run_dir / "daily_control_llm_requests.json", llm_diag.get("llm_requests", []))
    write_json(run_dir / "daily_control_llm_responses.json", llm_diag.get("llm_responses", []))
    write_json(run_dir / "daily_control_payload.json", payload)
    write_json(run_dir / "daily_control_quality_review.json", quality_review)
    write_json(run_dir / "roks_oap_snapshot.json", roks_snapshot)
    write_json(run_dir / "daily_control_style_editor.json", style_debug)

    summary = {
        "run_id": run_dir.name,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "source_sheet": source_sheet_name,
        "daily_sheet": str(args.daily_sheet or cfg.deal_analyzer_daily_sheet_name or "Дневной контроль"),
        "rows_prepared": len(styled_rows),
        "rows_to_insert": 0,
        "rows_skipped_existing": 0,
        "conflicts_count": 0,
        "llm_main_model": (llm_diag.get("llm_runtime", {}).get("main", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("main", {}), dict) else {}).get("model", ""),
        "llm_fallback_model": (llm_diag.get("llm_runtime", {}).get("fallback", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("fallback", {}), dict) else {}).get("model", ""),
        "llm_success_main": llm_diag.get("llm_success_main", 0),
        "llm_success_fallback": llm_diag.get("llm_success_fallback", 0),
        "llm_json_repair_count": llm_diag.get("llm_json_repair_count", 0),
        "llm_failed_count": llm_diag.get("llm_failed_count", 0),
        "roks_oap_snapshot_status": roks_snapshot.get("status", ""),
        "selected_current_month_sheet": roks_snapshot.get("selected_current_month_sheet", ""),
        "selected_previous_month_sheet": roks_snapshot.get("selected_previous_month_sheet", ""),
        "writer_mode": "dry_run",
        "write_allowed": False,
        "block_reason": "dry_run_build_only",
        "top_data_limitations": llm_diag.get("top_data_limitations", []),
        "style_editor": style_debug.get("metrics", {}),
        "quality_review": {
            "rows_total": quality_review.get("rows_total", 0),
            "problem_rows_total": quality_review.get("problem_rows_total", 0),
        },
    }
    write_json(run_dir / "summary.json", summary)
    write_markdown(run_dir / "summary.md", title="Daily Control Summary", lines=_summary_markdown_lines(summary))

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
            "rows_to_insert": status.get("rows_to_insert", 0),
            "rows_skipped_existing": status.get("rows_skipped_existing", 0),
            "conflicts_count": status.get("conflicts_count", 0),
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
