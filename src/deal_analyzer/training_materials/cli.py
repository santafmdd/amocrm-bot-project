from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.integrations.google_sheets_api_client import AUTH_MODE_AUTO, AUTH_MODE_INTERACTIVE_BOOTSTRAP
from src.logger import setup_logging

from ..client_list.normalizer import (
    build_header_mapping as build_client_list_header_mapping,
)
from ..client_list.normalizer import normalize_client_rows
from ..client_list.prioritizer import build_manager_client_context
from ..client_list.reader import read_client_list_sheet
from ..config import load_deal_analyzer_config
from ..progress import ProgressReporter
from .artifacts import write_json, write_markdown
from .docs_writer import (
    ensure_training_materials_oauth_scopes,
    materialize_docs_for_write,
    prepare_local_docs,
    training_materials_required_scopes,
)
from .sheets_link_writer import execute_links_write
from .source_collector import collect_source_snippets, collect_training_candidates, serialize_sources
from .task_writer import build_post_training_task_payload, summarize_task_payload
from .training_analyzer import analyze_training_candidates
from .validation import review_task_quality, review_training_quality, validate_draft_row


def _new_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "training_materials" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _build_initial_progress(*, rows_candidates_total: int = 0) -> dict[str, Any]:
    started_at = _now_iso()
    return {
        "current_stage": "started",
        "current_candidate_index": -1,
        "current_row_number": 0,
        "current_recipient": "",
        "current_model": "",
        "started_at": started_at,
        "last_update_at": started_at,
        "elapsed_seconds": 0,
        "rows_candidates_total": int(rows_candidates_total or 0),
        "rows_started": 0,
        "rows_completed": 0,
        "rows_prepared": 0,
        "rows_quarantined": 0,
        "llm_attempts_total": 0,
    }


def _update_progress(progress: dict[str, Any], progress_path: Path, *, stage: str, started_ts: float, **updates: Any) -> None:
    progress["current_stage"] = str(stage or progress.get("current_stage") or "")
    progress["last_update_at"] = _now_iso()
    progress["elapsed_seconds"] = int(max(0, time.time() - started_ts))
    for key, value in updates.items():
        progress[key] = value
    write_json(progress_path, progress)


def _extract_resume_row_key(item: dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ""
    key = str(item.get("idempotency_key") or "").strip()
    if key:
        return key
    row_number = int(item.get("row_number", 0) or 0)
    recipient = str(item.get("recipient") or "").strip().lower()
    plan_date = str(item.get("plan_date") or "").strip()
    if row_number > 0:
        return f"row:{row_number}"
    if recipient and plan_date:
        return f"{recipient}|{plan_date}"
    return ""


def _load_json(path: Path, *, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _build_client_context_by_manager_for_training(
    *,
    cfg: Any,
    logger: Any,
    week_start: str,
    week_end: str,
    manager_names: list[str],
) -> dict[str, dict[str, Any]]:
    if not bool(getattr(cfg, "client_list_enabled", False)):
        return {}
    try:
        snapshot = read_client_list_sheet(cfg=cfg, logger=logger)
        mapping = build_client_list_header_mapping(snapshot.headers, cfg=cfg)
        rows, _rejected = normalize_client_rows(
            headers=snapshot.headers,
            rows=snapshot.rows,
            mapping=mapping,
            header_row_number=snapshot.header_row_number,
        )
        out: dict[str, dict[str, Any]] = {}
        for manager_name in manager_names:
            name = str(manager_name or "").strip()
            if not name:
                continue
            context = build_manager_client_context(
                rows=rows,
                manager_name=name,
                period_start=week_start,
                period_end=week_end,
                manager_role_registry=getattr(cfg, "manager_role_registry", None),
                role_policy_registry=getattr(cfg, "role_policy_registry", None),
            )
            out[name.lower()] = context.__dict__
        return out
    except Exception:
        return {}
    return data


def _load_rows_file(path: Path) -> list[dict[str, Any]]:
    payload = _load_json(path, default={})
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    return [item for item in rows if isinstance(item, dict)]


def _flush_incremental_artifacts(
    *,
    run_dir: Path,
    payload_rows: list[dict[str, Any]],
    quarantined_rows: list[dict[str, Any]],
    llm_requests: list[dict[str, Any]],
    llm_responses: list[dict[str, Any]],
    quality_rows: list[dict[str, Any]],
    static_payload: dict[str, Any],
    rows_training_candidates: int,
    rows_docs_prepared: int,
    rows_links_to_write: int,
    rows_links_ready_to_write: int,
) -> None:
    payload = {
        **dict(static_payload),
        "rows": payload_rows,
        "rows_count": len(payload_rows),
        "rows_training_candidates": int(rows_training_candidates or 0),
        "rows_docs_prepared": int(rows_docs_prepared or 0),
        "rows_links_to_write": int(rows_links_to_write or 0),
        "rows_links_ready_to_write": int(rows_links_ready_to_write or 0),
        "rows_quarantined": len(quarantined_rows),
    }
    write_json(run_dir / "training_materials_payload.json", payload)
    write_json(run_dir / "training_materials_quarantine.json", {"rows_quarantined": len(quarantined_rows), "rows": quarantined_rows})
    write_json(run_dir / "training_materials_llm_requests.json", llm_requests)
    write_json(run_dir / "training_materials_llm_responses.json", llm_responses)
    write_json(
        run_dir / "training_materials_quality_review.json",
        {
            "rows_total": len(quality_rows),
            "rows_passed": sum(1 for item in quality_rows if bool(item.get("quality_passed", False))),
            "rows_failed": sum(1 for item in quality_rows if not bool(item.get("quality_passed", False))),
            "rows": quality_rows,
        },
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Training materials CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover training candidates in week plan")
    discover.add_argument("--config", required=True)
    discover.add_argument("--plan-sheet", default="План недели")
    discover.add_argument("--force-reauth", action="store_true")

    build = sub.add_parser("build", help="Build training materials payload")
    build.add_argument("--config", required=True)
    build.add_argument("--plan-sheet", default="План недели")
    build.add_argument("--daily-sheet", default="Дневной контроль")
    build.add_argument("--call-review-sheet", default="Разбор звонков")
    build.add_argument("--week-start", required=True)
    build.add_argument("--week-end", required=True)
    build.add_argument("--main-model", default="")
    build.add_argument("--fallback-model", default="")
    build.add_argument("--model-pool", default="", help="Comma-separated per-row model pool")
    build.add_argument(
        "--require-external-sources",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Require external internet sources for each training candidate",
    )
    build.add_argument(
        "--allow-no-external-sources",
        action="store_true",
        help="Allow build without external sources (keeps warning in summary)",
    )
    build.add_argument("--external-search-provider", default="auto")
    build.add_argument("--external-search-limit", type=int, default=5)
    build.add_argument("--external-source-min-count", type=int, default=2)
    build.add_argument("--limit", type=int, default=None)
    build.add_argument("--offset", type=int, default=0)
    build.add_argument("--manager", default="")
    build.add_argument("--plan-date", default="")
    build.add_argument("--max-runtime-minutes", type=int, default=0)
    build.add_argument("--max-llm-calls", type=int, default=0)
    build.add_argument("--main-timeout", type=int, default=0)
    build.add_argument("--fallback-timeout", type=int, default=0)
    build.add_argument("--allow-template-fallback", action="store_true")
    build.add_argument("--allow-full-run", action="store_true")
    build.add_argument("--run-dir", default="")
    build.add_argument("--resume-run-dir", default="")
    build.add_argument("--retry-failed-from-run-dir", default="")
    build.add_argument("--resume", action="store_true")
    build.add_argument("--dry-run", action="store_true")
    build.add_argument("--force-reauth", action="store_true")

    write = sub.add_parser("write", help="Write generated training links back to week plan")
    write.add_argument("--config", required=True)
    write.add_argument("--run-dir", required=True)
    write.add_argument("--plan-sheet", default="План недели")
    write.add_argument("--dry-run", action="store_true")
    write.add_argument("--write", action="store_true")
    write.add_argument("--strict-preflight", action="store_true")
    write.add_argument("--overwrite-links", action="store_true")
    write.add_argument("--force-regenerate-links", action="store_true")
    write.add_argument("--force-reauth", action="store_true")

    benchmark = sub.add_parser("benchmark-models", help="Benchmark candidate generation quality across models")
    benchmark.add_argument("--config", required=True)
    benchmark.add_argument("--models", required=True, help="Comma-separated models to probe")
    benchmark.add_argument("--sample-week-start", required=True)
    benchmark.add_argument("--sample-week-end", required=True)
    benchmark.add_argument("--plan-sheet", default="План недели")
    benchmark.add_argument("--daily-sheet", default="Дневной контроль")
    benchmark.add_argument("--call-review-sheet", default="Разбор звонков")
    benchmark.add_argument("--limit", type=int, default=1)
    benchmark.add_argument("--force-reauth", action="store_true")

    return parser.parse_args()


def _resolve_build_block_reason(
    *,
    rows_training_candidates: int,
    rows_docs_prepared: int,
    llm_failed_count: int,
    source_coverage_failed_rows: int = 0,
) -> str:
    if int(source_coverage_failed_rows or 0) > 0:
        return "source_coverage_failed"
    if int(rows_training_candidates or 0) > 0 and int(rows_docs_prepared or 0) == 0 and int(llm_failed_count or 0) > 0:
        return "llm_generation_failed"
    if int(rows_training_candidates or 0) == 0:
        return "rows_empty"
    if int(rows_docs_prepared or 0) == 0:
        return "rows_empty"
    return "dry_run_mode"


def _extract_generation_failures_rows(llm_quarantine: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "idempotency_key": str(item.get("idempotency_key") or ""),
            "row_number": int(item.get("row_number", 0) or 0),
            "recipient": str(item.get("recipient") or ""),
            "plan_date": str(item.get("plan_date") or ""),
            "training_topic": str(item.get("training_topic") or ""),
            "main_model": str(item.get("main_model") or ""),
            "fallback_model": str(item.get("fallback_model") or ""),
            "main_error": str(item.get("main_error") or ""),
            "fallback_error": str(item.get("fallback_error") or ""),
            "final_reason": str(item.get("final_reason") or item.get("reason") or ""),
            "error_type": str(item.get("error_type") or ""),
        }
        for item in llm_quarantine
        if isinstance(item, dict)
    ]


def _llm_error_summary_by_type(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        key = str(item.get("error_type") or "unknown_llm_error").strip() or "unknown_llm_error"
        counts[key] = int(counts.get(key, 0) or 0) + 1
    return counts


def _load_retry_failed_keys(source_run_dir: Path) -> set[str]:
    keys: set[str] = set()
    for row in _load_generation_failures_rows(source_run_dir):
        key = _extract_resume_row_key(row)
        if key:
            keys.add(key)
    if keys:
        return keys
    for row in _load_rows_file(source_run_dir / "training_materials_quarantine.json"):
        key = _extract_resume_row_key(row)
        if key:
            keys.add(key)
    return keys


def _build_retry_command_suggestion(
    *,
    config_path: str,
    run_dir: Path,
    plan_sheet: str,
    daily_sheet: str,
    call_review_sheet: str,
    week_start: str,
    week_end: str,
    main_model: str,
    fallback_model: str,
) -> str:
    return (
        "python -m src.deal_analyzer.training_materials.cli build "
        f'--config "{config_path}" '
        f'--plan-sheet "{plan_sheet}" '
        f'--daily-sheet "{daily_sheet}" '
        f'--call-review-sheet "{call_review_sheet}" '
        f"--week-start {week_start} --week-end {week_end} "
        f'--main-model "{main_model}" --fallback-model "{fallback_model}" '
        f'--retry-failed-from-run-dir "{run_dir}" --dry-run'
    )


def _parse_model_pool(value: str) -> list[str]:
    items: list[str] = []
    for raw in str(value or "").split(","):
        model = str(raw or "").strip()
        if not model:
            continue
        if model not in items:
            items.append(model)
    return items


def _coverage_debug_row(
    *,
    candidate: Any,
    coverage: Any,
    external_source_min_count: int,
    require_external_sources: bool,
    allow_no_external_sources: bool,
) -> dict[str, Any]:
    style_used = int(getattr(coverage, "style_sources_used", 0) or 0)
    speech_used = int(getattr(coverage, "speech_sources_used", 0) or 0)
    product_used = int(getattr(coverage, "product_sources_used", 0) or 0)
    external_used = bool(getattr(coverage, "external_sources_used", False))
    external_count = int(getattr(coverage, "external_sources_count", 0) or 0)
    external_status = str(getattr(coverage, "external_search_status", "") or "")
    fetch_errors = list(getattr(coverage, "external_source_fetch_errors", []) or [])
    warnings = list(getattr(coverage, "warnings", []) or [])

    reasons: list[str] = []
    if style_used <= 0:
        reasons.append("style_sources_missing")
    if speech_used <= 0:
        reasons.append("speech_sources_missing")
    if product_used <= 0:
        reasons.append("product_sources_missing")

    external_required_now = bool(require_external_sources) and not bool(allow_no_external_sources)
    if external_required_now:
        if not external_used:
            reasons.append("external_sources_unavailable")
        if external_count < max(1, int(external_source_min_count or 1)):
            reasons.append(f"external_sources_below_min:{external_count}")

    source_coverage_passed = len(reasons) == 0
    return {
        "idempotency_key": str(getattr(candidate, "idempotency_key", "") or ""),
        "row_number": int(getattr(candidate, "row_number", 0) or 0),
        "recipient": str(getattr(candidate, "recipient", "") or ""),
        "plan_date": str(getattr(candidate, "plan_date", "") or ""),
        "style_sources_used": style_used,
        "speech_sources_used": speech_used,
        "product_sources_used": product_used,
        "external_sources_used": external_used,
        "external_sources_count": external_count,
        "external_search_status": external_status,
        "external_source_titles": list(getattr(coverage, "external_source_titles", []) or []),
        "external_source_urls": list(getattr(coverage, "external_source_urls", []) or []),
        "external_source_fetch_errors": fetch_errors,
        "warnings": warnings,
        "source_coverage_passed": source_coverage_passed,
        "source_coverage_fail_reasons": reasons,
        "require_external_sources": bool(require_external_sources),
        "allow_no_external_sources": bool(allow_no_external_sources),
        "external_source_min_count": max(1, int(external_source_min_count or 1)),
    }


def _load_generation_failures_rows(run_dir: Path) -> list[dict[str, Any]]:
    path = run_dir / "training_materials_generation_failures.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    return [item for item in rows if isinstance(item, dict)]


def _llm_error_examples_from_generation_failures(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for item in rows[:10]:
        examples.append(
            {
                "row_number": int(item.get("row_number", 0) or 0),
                "recipient": str(item.get("recipient") or ""),
                "plan_date": str(item.get("plan_date") or ""),
                "error_type": str(item.get("error_type") or ""),
                "reason": str(item.get("final_reason") or item.get("main_error") or item.get("fallback_error") or ""),
            }
        )
    return examples


def _quarantined_rows_from_generation_failures(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    quarantined_rows: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        quarantined_rows.append(
            {
                "row_number": int(item.get("row_number", 0) or 0),
                "recipient": str(item.get("recipient") or ""),
                "plan_date": str(item.get("plan_date") or ""),
                "reason": str(item.get("final_reason") or item.get("reason") or item.get("error_type") or ""),
                "error_type": str(item.get("error_type") or ""),
                "quality_fail_reasons": item.get("quality_fail_reasons", []),
                "quality_metrics": item.get("quality_metrics", {}),
            }
        )
    return quarantined_rows


def _merge_status_with_generation_failures(
    *,
    status: dict[str, Any],
    generation_failures: list[dict[str, Any]],
) -> dict[str, Any]:
    if not generation_failures:
        return status
    summary_by_type = _llm_error_summary_by_type(generation_failures)
    quarantined_rows = _quarantined_rows_from_generation_failures(generation_failures)
    block_reason = str(status.get("block_reason") or "")
    status["rows_quarantined"] = max(int(status.get("rows_quarantined", 0) or 0), len(generation_failures))
    if not isinstance(status.get("quarantined_rows"), list) or not status.get("quarantined_rows"):
        status["quarantined_rows"] = quarantined_rows
    if not isinstance(status.get("llm_error_examples"), list):
        status["llm_error_examples"] = _llm_error_examples_from_generation_failures(generation_failures)
    if not isinstance(status.get("llm_error_summary_by_type"), dict):
        status["llm_error_summary_by_type"] = summary_by_type
    if block_reason in {"rows_empty", "nothing_to_write"}:
        status["write_allowed"] = False
        status["block_reason"] = "llm_generation_failed"
    rows_written = int(status.get("rows_written", 0) or 0)
    if rows_written > 0 and int(status.get("rows_quarantined", 0) or 0) > 0:
        status["status"] = "partial_success"
    elif rows_written <= 0 and int(status.get("rows_quarantined", 0) or 0) > 0:
        status["status"] = "failed"
    return status


def _summary_lines(summary: dict[str, Any]) -> list[str]:
    return [
        f"full_run_allowed: {summary.get('full_run_allowed', False)}",
        f"effective_limit: {summary.get('effective_limit', 0)}",
        f"rows_training_candidates: {summary.get('rows_training_candidates', 0)}",
        f"rows_docs_prepared: {summary.get('rows_docs_prepared', 0)}",
        f"rows_links_to_write: {summary.get('rows_links_to_write', 0)}",
        f"rows_links_ready_to_write: {summary.get('rows_links_ready_to_write', 0)}",
        f"docs_creation_mode: {summary.get('docs_creation_mode', '')}",
        f"rows_docs_created: {summary.get('rows_docs_created', 0)}",
        f"rows_task_docs_created: {summary.get('rows_task_docs_created', 0)}",
        f"docs_creation_errors_count: {summary.get('docs_creation_errors_count', 0)}",
        f"rows_skipped_existing_links: {summary.get('rows_skipped_existing_links', 0)}",
        f"rows_quarantined: {summary.get('rows_quarantined', 0)}",
        f"llm_attempts_main: {summary.get('llm_attempts_main', 0)}",
        f"llm_attempts_fallback: {summary.get('llm_attempts_fallback', 0)}",
        f"fallback_used_count: {summary.get('fallback_used_count', 0)}",
        f"quality_repairs_used: {summary.get('quality_repairs_used', 0)}",
        f"targeted_repairs_used: {summary.get('targeted_repairs_used', 0)}",
        f"rows_passed_after_repair: {summary.get('rows_passed_after_repair', 0)}",
        f"quality_rows_total: {summary.get('quality_rows_total', 0)}",
        f"quality_rows_passed: {summary.get('quality_rows_passed', 0)}",
        f"quality_rows_failed: {summary.get('quality_rows_failed', 0)}",
        f"style_sources_used: {summary.get('style_sources_used', 0)}",
        f"speech_sources_used: {summary.get('speech_sources_used', 0)}",
        f"product_sources_used: {summary.get('product_sources_used', 0)}",
        f"external_sources_used: {summary.get('external_sources_used', False)}",
        f"external_sources_count: {summary.get('external_sources_count', 0)}",
        f"external_search_status: {summary.get('external_search_status', '')}",
        f"external_sources_status: {summary.get('external_sources_status', '')}",
        f"external_source_min_count: {summary.get('external_source_min_count', 0)}",
        f"require_external_sources: {summary.get('require_external_sources', True)}",
        f"allow_no_external_sources: {summary.get('allow_no_external_sources', False)}",
        f"source_coverage_passed: {summary.get('source_coverage_passed', False)}",
        f"source_coverage_failed_rows: {summary.get('source_coverage_failed_rows', 0)}",
        f"docs_api_status: {summary.get('docs_api_status', '')}",
        f"drive_api_status: {summary.get('drive_api_status', '')}",
        f"docs_api_error_type: {summary.get('docs_api_error_type', '')}",
        f"action_required: {summary.get('action_required', '')}",
        f"docs_api_error_message: {summary.get('docs_api_error_message', '')}",
        f"scope_mismatch_detected: {summary.get('scope_mismatch_detected', False)}",
        f"reauth_required: {summary.get('reauth_required', False)}",
        f"reauth_instruction: {summary.get('reauth_instruction', '')}",
        f"block_reason: {summary.get('block_reason', '')}",
        f"llm_error_examples: {len(summary.get('llm_error_examples', []) if isinstance(summary.get('llm_error_examples', []), list) else [])}",
        f"llm_error_summary_by_type: {summary.get('llm_error_summary_by_type', {})}",
        f"model_failures_by_type: {summary.get('model_failures_by_type', {})}",
    ]


def _run_discover(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)

    api_caps = ensure_training_materials_oauth_scopes(
        project_root=app_cfg.project_root,
        logger=logger,
        force_reauth=bool(args.force_reauth),
    )
    if bool(api_caps.get("reauth_required", False)):
        logger.warning(
            "training_materials oauth scope mismatch: %s (token=%s)",
            str(api_caps.get("reauth_instruction") or "Удалите token.json и пройдите OAuth заново"),
            str(api_caps.get("token_file") or ""),
        )
    candidates, diag = collect_training_candidates(
        cfg=cfg,
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        week_start="",
        week_end="",
        manager="",
        plan_date="",
        limit=0,
        logger=logger,
        scopes=training_materials_required_scopes(),
        auth_mode=AUTH_MODE_INTERACTIVE_BOOTSTRAP if bool(args.force_reauth) else AUTH_MODE_AUTO,
    )

    write_json(run_dir / "training_materials_discovery.json", {"candidate_diagnostics": diag, "api_capabilities": api_caps})
    write_json(run_dir / "training_materials_candidate_debug.json", diag)
    write_markdown(
        run_dir / "training_materials_discovery.md",
        title="Training Materials Discovery",
        lines=[
            f"plan_sheet: {args.plan_sheet}",
            f"rows_training_candidates: {diag.get('rows_training_candidates', 0)}",
            f"docs_api_available: {api_caps.get('docs_api_available', False)}",
            f"missing_scopes: {api_caps.get('missing_scopes', [])}",
            f"scope_mismatch_detected: {api_caps.get('scope_mismatch_detected', False)}",
            f"reauth_instruction: {api_caps.get('reauth_instruction', '')}",
        ],
    )
    write_json(run_dir / "training_materials_candidates.json", [asdict(item) for item in candidates])
    print(str(run_dir))


def _run_build_legacy(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    explicit_resume_dir = str(getattr(args, "resume_run_dir", "") or "").strip()
    explicit_run_dir = str(getattr(args, "run_dir", "") or "").strip()
    if explicit_run_dir:
        run_dir = Path(explicit_run_dir).resolve()
    elif explicit_resume_dir:
        run_dir = Path(explicit_resume_dir).resolve()
    else:
        run_dir = _new_run_dir(app_cfg.project_root)
    run_dir.mkdir(parents=True, exist_ok=True)
    started_ts = time.time()
    progress_reporter = ProgressReporter(
        process="training_materials",
        run_dir=run_dir,
        heartbeat_seconds=int(getattr(cfg, "progress_heartbeat_seconds", 30) or 30),
        logger=logger,
        step_name="init",
        total=0,
    )

    week_start = str(args.week_start or "").strip()
    week_end = str(args.week_end or "").strip()
    if not week_start or not week_end:
        raise RuntimeError("week_start and week_end are required")
    resume_enabled = bool(getattr(args, "resume", False) or explicit_resume_dir)
    resume_source_dir = Path(explicit_resume_dir).resolve() if explicit_resume_dir else run_dir
    retry_failed_source_dir = Path(str(getattr(args, "retry_failed_from_run_dir", "") or "").strip()).resolve() if str(getattr(args, "retry_failed_from_run_dir", "") or "").strip() else None

    progress_path = run_dir / "training_materials_progress.json"
    summary_path = run_dir / "summary.json"
    runtime_status_path = run_dir / "training_materials_runtime_status.json"
    progress = _load_json(progress_path, default={}) if resume_enabled else {}
    if not isinstance(progress, dict) or not progress:
        progress = _build_initial_progress(rows_candidates_total=0)
    payload_rows_resume = _load_rows_file(resume_source_dir / "training_materials_payload.json") if resume_enabled else []
    quarantine_rows_resume = _load_rows_file(resume_source_dir / "training_materials_quarantine.json") if resume_enabled else []
    llm_requests_resume = _load_json(resume_source_dir / "training_materials_llm_requests.json", default=[]) if resume_enabled else []
    llm_responses_resume = _load_json(resume_source_dir / "training_materials_llm_responses.json", default=[]) if resume_enabled else []
    quality_rows_resume = _load_rows_file(resume_source_dir / "training_materials_quality_review.json") if resume_enabled else []
    if not isinstance(llm_requests_resume, list):
        llm_requests_resume = []
    if not isinstance(llm_responses_resume, list):
        llm_responses_resume = []
    progress["rows_prepared"] = max(int(progress.get("rows_prepared", 0) or 0), len(payload_rows_resume))
    progress["rows_quarantined"] = max(int(progress.get("rows_quarantined", 0) or 0), len(quarantine_rows_resume))
    progress["rows_completed"] = max(int(progress.get("rows_completed", 0) or 0), len(payload_rows_resume) + len(quarantine_rows_resume))
    progress["rows_started"] = max(int(progress.get("rows_started", 0) or 0), int(progress.get("rows_completed", 0) or 0))
    progress["llm_attempts_total"] = max(int(progress.get("llm_attempts_total", 0) or 0), len(llm_requests_resume))
    write_json(progress_path, progress)
    write_json(
        summary_path,
        {
            "run_id": run_dir.name,
            "status": "started",
            "week_start": week_start,
            "week_end": week_end,
            "rows_training_candidates": 0,
            "rows_docs_prepared": 0,
            "rows_links_to_write": 0,
            "rows_links_ready_to_write": 0,
            "rows_quarantined": 0,
            "write_strategy": "values_only",
            "structural_changes_required": False,
            "block_reason": "in_progress",
        },
    )
    write_json(
        runtime_status_path,
        {
            "status": "started",
            "started_at": str(progress.get("started_at") or ""),
            "run_dir": str(run_dir),
            "api_capabilities": {},
            "llm": {},
        },
    )
    write_json(run_dir / "training_materials_candidate_debug.json", {"status": "started", "rows_training_candidates": 0, "rows_skipped": []})
    write_json(run_dir / "training_materials_payload.json", {"mode": "training_materials", "rows": payload_rows_resume, "rows_count": len(payload_rows_resume)})
    write_json(run_dir / "training_materials_quarantine.json", {"rows_quarantined": len(quarantine_rows_resume), "rows": quarantine_rows_resume})
    write_json(run_dir / "training_materials_llm_requests.json", llm_requests_resume)
    write_json(run_dir / "training_materials_llm_responses.json", llm_responses_resume)
    write_json(
        run_dir / "training_materials_quality_review.json",
        {
            "rows_total": len(quality_rows_resume),
            "rows_passed": sum(1 for item in quality_rows_resume if bool(item.get("quality_passed", False))),
            "rows_failed": sum(1 for item in quality_rows_resume if not bool(item.get("quality_passed", False))),
            "rows": quality_rows_resume,
        },
    )

    api_caps = ensure_training_materials_oauth_scopes(
        project_root=app_cfg.project_root,
        logger=logger,
        force_reauth=bool(args.force_reauth),
    )
    progress_reporter.update(
        step_name="source_read_started",
        current=0,
        total=0,
        current_item={"stage": "source_read", "date": f"{week_start}..{week_end}"},
        details={"run_id": run_dir.name},
    )
    if bool(api_caps.get("reauth_required", False)):
        logger.warning(
            "training_materials oauth scope mismatch: %s (token=%s)",
            str(api_caps.get("reauth_instruction") or "Удалите token.json и пройдите OAuth заново"),
            str(api_caps.get("token_file") or ""),
        )
    _update_progress(progress, progress_path, stage="source_read_started", started_ts=started_ts)
    write_json(runtime_status_path, {"status": "source_read_started", "run_dir": str(run_dir), "api_capabilities": api_caps, "llm": {}})
    requested_limit = int(args.limit or 0) if args.limit is not None else 0
    if requested_limit <= 0 and bool(args.dry_run) and not bool(args.allow_full_run):
        requested_limit = 2
    full_run_allowed = bool(args.allow_full_run)
    effective_limit = int(requested_limit if requested_limit > 0 else 0)
    candidates, diag = collect_training_candidates(
        cfg=cfg,
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        week_start=week_start,
        week_end=week_end,
        manager=str(args.manager or ""),
        plan_date=str(args.plan_date or ""),
        limit=0,
        logger=logger,
        scopes=training_materials_required_scopes(),
        auth_mode=AUTH_MODE_INTERACTIVE_BOOTSTRAP if bool(args.force_reauth) else AUTH_MODE_AUTO,
    )
    offset = max(0, int(args.offset or 0))
    if offset > 0:
        candidates = candidates[offset:]
    if requested_limit > 0:
        candidates = candidates[:requested_limit]
    if resume_enabled:
        processed_keys: set[str] = set()
        for item in _load_rows_file(resume_source_dir / "training_materials_payload.json"):
            key = _extract_resume_row_key(item)
            if key:
                processed_keys.add(key)
        for item in _load_rows_file(resume_source_dir / "training_materials_quarantine.json"):
            key = _extract_resume_row_key(item)
            if key:
                processed_keys.add(key)
        if processed_keys:
            candidates = [item for item in candidates if _extract_resume_row_key(asdict(item)) not in processed_keys]
    retry_failed_keys: set[str] = set()
    if retry_failed_source_dir is not None:
        retry_failed_keys = _load_retry_failed_keys(retry_failed_source_dir)
        if retry_failed_keys:
            candidates = [item for item in candidates if _extract_resume_row_key(asdict(item)) in retry_failed_keys]
        else:
            candidates = []
    rows_training_candidates_total = len(candidates)
    progress["rows_candidates_total"] = len(candidates)
    _update_progress(progress, progress_path, stage="source_read_completed", started_ts=started_ts, rows_candidates_total=len(candidates))
    progress_reporter.update(
        step_name="source_read_completed",
        current=0,
        total=len(candidates),
        current_item={"stage": "source_read", "rows": len(candidates)},
    )
    client_context_by_manager = _build_client_context_by_manager_for_training(
        cfg=cfg,
        logger=logger,
        week_start=week_start,
        week_end=week_end,
        manager_names=sorted({str(item.recipient or "").strip() for item in candidates if str(item.recipient or "").strip()}),
    )

    require_external_sources = bool(getattr(args, "require_external_sources", True))
    allow_no_external_sources = bool(getattr(args, "allow_no_external_sources", False))
    external_search_provider = str(getattr(args, "external_search_provider", "auto") or "auto")
    external_search_limit = max(1, int(getattr(args, "external_search_limit", 5) or 5))
    external_source_min_count = max(1, int(getattr(args, "external_source_min_count", 2) or 2))

    snippets_by_key: dict[str, list[Any]] = {}
    snippets_payload: list[dict[str, Any]] = []
    source_coverage_debug_rows: list[dict[str, Any]] = []
    style_sources_used = 0
    speech_sources_used = 0
    product_sources_used = 0
    external_sources_used = False
    external_sources_count = 0
    external_search_status = "external_search_unavailable"
    external_source_titles: list[str] = []
    external_source_urls: list[str] = []
    external_source_fetch_errors: list[str] = []
    coverage_warnings: list[str] = []
    source_coverage_failed_rows: list[dict[str, Any]] = []
    eligible_candidates: list[Any] = []

    for candidate in candidates:
        candidate_client_context = client_context_by_manager.get(str(candidate.recipient or "").strip().lower(), {})
        snippets, coverage = collect_source_snippets(
            cfg=cfg,
            training_topic=candidate.what_i_do,
            project_root=app_cfg.project_root,
            candidate=candidate,
            client_list_context=candidate_client_context,
            external_search_provider=external_search_provider,
            external_search_limit=external_search_limit,
        )
        row_debug = _coverage_debug_row(
            candidate=candidate,
            coverage=coverage,
            external_source_min_count=external_source_min_count,
            require_external_sources=require_external_sources,
            allow_no_external_sources=allow_no_external_sources,
        )
        source_coverage_debug_rows.append(row_debug)
        if not bool(row_debug.get("source_coverage_passed", False)):
            source_coverage_failed_rows.append(row_debug)
            continue

        eligible_candidates.append(candidate)
        snippets_by_key[candidate.idempotency_key] = snippets
        snippets_payload.append(
            {
                "idempotency_key": candidate.idempotency_key,
                "row_number": candidate.row_number,
                "recipient": candidate.recipient,
                "snippets": serialize_sources(snippets),
                "coverage": asdict(coverage),
            }
        )
        style_sources_used = max(style_sources_used, int(coverage.style_sources_used or 0))
        speech_sources_used = max(speech_sources_used, int(coverage.speech_sources_used or 0))
        product_sources_used = max(product_sources_used, int(coverage.product_sources_used or 0))
        external_sources_used = external_sources_used or bool(coverage.external_sources_used)
        external_sources_count = max(external_sources_count, int(getattr(coverage, "external_sources_count", 0) or 0))
        external_search_status = str(coverage.external_search_status or external_search_status)
        for title in list(getattr(coverage, "external_source_titles", []) or []):
            if title and title not in external_source_titles:
                external_source_titles.append(str(title))
        for url in list(getattr(coverage, "external_source_urls", []) or []):
            if url and url not in external_source_urls:
                external_source_urls.append(str(url))
        for err in list(getattr(coverage, "external_source_fetch_errors", []) or []):
            if err and err not in external_source_fetch_errors:
                external_source_fetch_errors.append(str(err))
        for warning in coverage.warnings:
            if warning not in coverage_warnings:
                coverage_warnings.append(str(warning))

    source_coverage_quarantine: list[dict[str, Any]] = []
    for row in source_coverage_failed_rows:
        source_coverage_quarantine.append(
            {
                "idempotency_key": str(row.get("idempotency_key") or ""),
                "row_number": int(row.get("row_number", 0) or 0),
                "recipient": str(row.get("recipient") or ""),
                "plan_date": str(row.get("plan_date") or ""),
                "reason": "source_coverage_failed",
                "errors": list(row.get("source_coverage_fail_reasons", []) if isinstance(row.get("source_coverage_fail_reasons"), list) else []),
                "coverage": row,
            }
        )

    candidates = eligible_candidates
    progress["rows_candidates_total"] = len(candidates)
    _update_progress(progress, progress_path, stage="source_read_completed", started_ts=started_ts, rows_candidates_total=len(candidates))
    progress_reporter.update(
        step_name="candidate_filtering_completed",
        current=0,
        total=len(candidates),
        current_item={"stage": "candidate_filtering", "rows": len(candidates)},
    )

    llm_requests: list[dict[str, Any]] = [dict(item) for item in llm_requests_resume if isinstance(item, dict)]
    llm_responses: list[dict[str, Any]] = [dict(item) for item in llm_responses_resume if isinstance(item, dict)]
    llm_quarantine_live: list[dict[str, Any]] = [dict(item) for item in quarantine_rows_resume if isinstance(item, dict)]
    llm_diag: dict[str, Any] = {}

    def _on_progress(event: dict[str, Any]) -> None:
        stage = str(event.get("stage") or "")
        updates = {
            "current_candidate_index": int(event.get("candidate_index", progress.get("current_candidate_index", -1)) or -1),
            "current_row_number": int(event.get("row_number", progress.get("current_row_number", 0)) or 0),
            "current_recipient": str(event.get("recipient") or progress.get("current_recipient") or ""),
            "current_model": str(event.get("model") or progress.get("current_model") or ""),
        }
        if stage == "candidate_started":
            progress["rows_started"] = int(progress.get("rows_started", 0) or 0) + 1
            updates["rows_started"] = progress["rows_started"]
        if stage == "llm_attempt_started":
            progress["llm_attempts_total"] = int(event.get("llm_attempts_total", progress.get("llm_attempts_total", 0)) or 0)
            updates["llm_attempts_total"] = progress["llm_attempts_total"]
        _update_progress(progress, progress_path, stage=stage, started_ts=started_ts, **updates)
        progress_reporter.update(
            step_name=stage or "running",
            current=int(progress.get("rows_completed", 0) or 0),
            total=int(progress.get("rows_candidates_total", 0) or 0),
            current_item={
                "recipient": str(updates.get("current_recipient") or ""),
                "row_number": int(updates.get("current_row_number", 0) or 0),
                "model": str(updates.get("current_model") or ""),
                "stage": stage,
            },
            details={"llm_attempts_total": int(progress.get("llm_attempts_total", 0) or 0)},
            log=stage in {"candidate_started", "candidate_prepared", "candidate_quarantined", "build_completed"},
        )

    def _on_llm_request(item: dict[str, Any]) -> None:
        llm_requests.append(item)
        write_json(run_dir / "training_materials_llm_requests.json", llm_requests)

    def _on_llm_response(item: dict[str, Any]) -> None:
        llm_responses.append(item)
        write_json(run_dir / "training_materials_llm_responses.json", llm_responses)

    def _on_candidate_quarantine(item: dict[str, Any]) -> None:
        candidate_key = _extract_resume_row_key(item)
        if candidate_key:
            for existing in llm_quarantine_live:
                if _extract_resume_row_key(existing) == candidate_key:
                    return
        llm_quarantine_live.append(dict(item))
        write_json(run_dir / "training_materials_quarantine.json", {"rows_quarantined": len(llm_quarantine_live), "rows": llm_quarantine_live})
        progress["rows_quarantined"] = int(progress.get("rows_quarantined", 0) or 0) + 1
        progress["rows_completed"] = int(progress.get("rows_completed", 0) or 0) + 1
        _update_progress(
            progress,
            progress_path,
            stage="candidate_quarantined",
            started_ts=started_ts,
            rows_completed=progress["rows_completed"],
            rows_quarantined=progress["rows_quarantined"],
        )

    _max_runtime_seconds = max(0, int(args.max_runtime_minutes or 0)) * 60
    model_pool = _parse_model_pool(str(getattr(args, "model_pool", "") or ""))
    try:
        drafts, llm_quarantine, llm_diag = analyze_training_candidates(
            candidates=candidates,
            snippets_by_key=snippets_by_key,
            cfg=cfg,
            logger=logger,
            main_model_override=str(args.main_model or ""),
            fallback_model_override=str(args.fallback_model or ""),
            model_pool_override=model_pool,
            llm_max_attempts=6,
            allow_template_fallback=bool(args.allow_template_fallback),
            max_runtime_seconds=_max_runtime_seconds,
            max_llm_calls=max(0, int(args.max_llm_calls or 0)),
            main_timeout_override=max(0, int(args.main_timeout or 0)),
            fallback_timeout_override=max(0, int(args.fallback_timeout or 0)),
            network_retry_attempts_main=3,
            network_retry_attempts_fallback=2,
            enable_backoff_sleep=True,
            on_progress=_on_progress,
            on_llm_request=_on_llm_request,
            on_llm_response=_on_llm_response,
            on_candidate_quarantine=_on_candidate_quarantine,
        )
    except KeyboardInterrupt:
        write_json(run_dir / "training_materials_runtime_status.json", {"status": "interrupted", "api_capabilities": api_caps, "llm": llm_diag})
        write_json(
            run_dir / "summary.json",
            {
                "run_id": run_dir.name,
                "status": "interrupted",
                "week_start": week_start,
                "week_end": week_end,
                "rows_training_candidates": rows_training_candidates_total,
                "rows_docs_prepared": int(progress.get("rows_prepared", 0) or 0),
                "rows_quarantined": int(progress.get("rows_quarantined", 0) or 0),
                "block_reason": "interrupted",
                "write_strategy": "values_only",
                "structural_changes_required": False,
            },
        )
        _update_progress(progress, progress_path, stage="build_interrupted", started_ts=started_ts)
        progress_reporter.finish(status="interrupted", step_name="build_interrupted", error="KeyboardInterrupt")
        print(str(run_dir))
        return

    valid_drafts = []
    validation_quarantine: list[dict[str, Any]] = []
    quality_review_rows: list[dict[str, Any]] = [dict(item) for item in quality_rows_resume if isinstance(item, dict)]
    for draft in drafts:
        _update_progress(
            progress,
            progress_path,
            stage="quality_check_started",
            started_ts=started_ts,
            current_row_number=int(draft.candidate.row_number or 0),
            current_recipient=str(draft.candidate.recipient or ""),
        )
        training_quality = draft.quality_metrics.get("training", {}) if isinstance(draft.quality_metrics, dict) else {}
        task_quality = draft.quality_metrics.get("task", {}) if isinstance(draft.quality_metrics, dict) else {}
        if not isinstance(training_quality, dict) or not training_quality:
            training_quality = review_training_quality(draft.training_material)
        if not isinstance(task_quality, dict) or not task_quality:
            task_quality = review_task_quality(draft.task_material)

        quality_passed = bool(training_quality.get("quality_passed", False)) and bool(task_quality.get("quality_passed", False))
        quality_fail_reasons = [
            *list(training_quality.get("quality_fail_reasons", []) if isinstance(training_quality.get("quality_fail_reasons", []), list) else []),
            *list(task_quality.get("quality_fail_reasons", []) if isinstance(task_quality.get("quality_fail_reasons", []), list) else []),
        ]
        quality_review_rows.append(
            {
                "row_number": draft.candidate.row_number,
                "doc_title": draft.training_title,
                "recipient": draft.candidate.recipient,
                "plan_date": draft.candidate.plan_date,
                "training_chars": int(training_quality.get("training_chars", 0) or 0),
                "task_chars": int(task_quality.get("task_chars", 0) or 0),
                "sections_count": int(training_quality.get("sections_count", 0) or 0),
                "speech_modules_count": int(training_quality.get("speech_modules_count", 0) or 0),
                "checklist_items_count": int(training_quality.get("checklist_items_count", 0) or 0),
                "foreign_words_count": int(training_quality.get("foreign_words_count", 0) or 0),
                "foreign_words_examples": training_quality.get("foreign_words_examples", []),
                "foreign_words_warning_examples": training_quality.get("foreign_words_warning_examples", []),
                "task_foreign_words_count": int(task_quality.get("task_foreign_words_count", 0) or 0),
                "task_foreign_words_examples": task_quality.get("task_foreign_words_examples", []),
                "task_foreign_words_warning_examples": task_quality.get("task_foreign_words_warning_examples", []),
                "quality_passed": quality_passed,
                "quality_fail_reasons": quality_fail_reasons,
            }
        )

        ok, errors = validate_draft_row(
            {
                "training_title": draft.training_title,
                "training_material": draft.training_material,
                "task_title": draft.task_title,
                "task_material": draft.task_material,
            }
        )
        if not ok:
            validation_quarantine.append(
                {
                    "idempotency_key": draft.candidate.idempotency_key,
                    "row_number": draft.candidate.row_number,
                    "recipient": draft.candidate.recipient,
                    "plan_date": draft.candidate.plan_date,
                    "reason": "draft_validation_failed",
                    "errors": errors,
                    "quality_fail_reasons": quality_fail_reasons,
                }
            )
            write_json(
                run_dir / "training_materials_quarantine.json",
                {
                    "rows_quarantined": len(source_coverage_quarantine) + len(llm_quarantine_live) + len(validation_quarantine),
                    "rows": [*source_coverage_quarantine, *llm_quarantine_live, *validation_quarantine],
                },
            )
            progress["rows_quarantined"] = int(progress.get("rows_quarantined", 0) or 0) + 1
            progress["rows_completed"] = int(progress.get("rows_completed", 0) or 0) + 1
            write_json(
                run_dir / "training_materials_quality_review.json",
                {
                    "rows_total": len(quality_review_rows),
                    "rows_passed": sum(1 for item in quality_review_rows if bool(item.get("quality_passed", False))),
                    "rows_failed": sum(1 for item in quality_review_rows if not bool(item.get("quality_passed", False))),
                    "rows": quality_review_rows,
                },
            )
            _update_progress(
                progress,
                progress_path,
                stage="candidate_quarantined",
                started_ts=started_ts,
                rows_completed=progress["rows_completed"],
                rows_quarantined=progress["rows_quarantined"],
            )
            continue
        valid_drafts.append(draft)
        write_json(
            run_dir / "training_materials_quality_review.json",
            {
                "rows_total": len(quality_review_rows),
                "rows_passed": sum(1 for item in quality_review_rows if bool(item.get("quality_passed", False))),
                "rows_failed": sum(1 for item in quality_review_rows if not bool(item.get("quality_passed", False))),
                "rows": quality_review_rows,
            },
        )
        _update_progress(progress, progress_path, stage="quality_check_finished", started_ts=started_ts)

    local_docs = prepare_local_docs(drafts=valid_drafts, run_dir=run_dir)
    task_payload = build_post_training_task_payload(drafts=valid_drafts)

    rows_payload: list[dict[str, Any]] = [dict(item) for item in payload_rows_resume if isinstance(item, dict)]
    by_key = {item.candidate.idempotency_key: item for item in valid_drafts}
    rows_links_to_write = 0
    rows_links_ready_to_write = 0
    payload_seen_keys: set[str] = set()
    for row in rows_payload:
        key = str(row.get("idempotency_key") or "")
        if key:
            payload_seen_keys.add(key)
        missing_existing_training = not str(row.get("existing_training_link") or "").strip()
        missing_existing_task = not str(row.get("existing_post_training_task_link") or "").strip()
        if missing_existing_training or missing_existing_task:
            rows_links_to_write += 1
        if str(row.get("training_link") or "").strip() or str(row.get("post_training_task_link") or "").strip():
            rows_links_ready_to_write += 1
    for item in local_docs:
        key = str(item.get("idempotency_key") or "")
        if key in payload_seen_keys:
            continue
        draft = by_key.get(key)
        if draft is None:
            continue
        row = {
            "row_number": int(item.get("row_number", 0) or 0),
            "idempotency_key": key,
            "plan_week_start": draft.candidate.plan_week_start,
            "plan_week_end": draft.candidate.plan_week_end,
            "recipient": draft.candidate.recipient,
            "plan_date": draft.candidate.plan_date,
            "activity_type": draft.candidate.activity_type,
            "topic_hash": draft.candidate.topic_hash,
            "training_topic": draft.candidate.what_i_do,
            "training_title": draft.training_title,
            "training_material": draft.training_material,
            "task_title": draft.task_title,
            "task_material": draft.task_material,
            "training_doc_local_path": item.get("training_doc_local_path", ""),
            "task_doc_local_path": item.get("task_doc_local_path", ""),
            "training_link": item.get("training_link", ""),
            "post_training_task_link": item.get("post_training_task_link", ""),
            "existing_training_link": draft.candidate.training_link,
            "existing_post_training_task_link": draft.candidate.post_training_task_link,
            "analysis_backend_used": draft.analysis_backend_used,
            "quality_metrics": {
                "training_chars": int((draft.quality_metrics.get("training", {}) if isinstance(draft.quality_metrics, dict) else {}).get("training_chars", 0) or 0),
                "task_chars": int((draft.quality_metrics.get("task", {}) if isinstance(draft.quality_metrics, dict) else {}).get("task_chars", 0) or 0),
            },
        }
        missing_existing_training = not str(row.get("existing_training_link") or "").strip()
        missing_existing_task = not str(row.get("existing_post_training_task_link") or "").strip()
        if missing_existing_training or missing_existing_task:
            rows_links_to_write += 1
        if str(row.get("training_link") or "").strip() or str(row.get("post_training_task_link") or "").strip():
            rows_links_ready_to_write += 1
        rows_payload.append(row)
        if key:
            payload_seen_keys.add(key)
        progress["rows_prepared"] = int(progress.get("rows_prepared", 0) or 0) + 1
        progress["rows_completed"] = int(progress.get("rows_completed", 0) or 0) + 1
        write_json(
            run_dir / "training_materials_payload.json",
            {
                "mode": "training_materials",
                "week_start": week_start,
                "week_end": week_end,
                "full_run_allowed": full_run_allowed,
                "effective_limit": effective_limit,
                "plan_sheet": str(args.plan_sheet or "План недели"),
                "docs_creation_mode": "dry_run",
                "rows": rows_payload,
                "rows_count": len(rows_payload),
                "rows_training_candidates": rows_training_candidates_total,
                "rows_docs_prepared": len(rows_payload),
                "rows_links_to_write": rows_links_to_write,
                "rows_links_ready_to_write": rows_links_ready_to_write,
                "rows_skipped_existing_links": int(diag.get("rows_skipped_existing_links", 0) or 0),
                "rows_quarantined": int(progress.get("rows_quarantined", 0) or 0),
                "docs_api_status": str(api_caps.get("status") or "docs_api_unavailable"),
            },
        )
        _update_progress(
            progress,
            progress_path,
            stage="candidate_prepared",
            started_ts=started_ts,
            rows_prepared=progress["rows_prepared"],
            rows_completed=progress["rows_completed"],
        )

    llm_quarantine_effective = llm_quarantine_live if llm_quarantine_live else llm_quarantine
    quarantined_rows = [*source_coverage_quarantine, *llm_quarantine_effective, *validation_quarantine]
    generation_failures = _extract_generation_failures_rows(llm_quarantine_effective)
    llm_failed_count = int(llm_diag.get("llm_failed_count", 0) or 0)
    stopped_reason = str(llm_diag.get("stopped_reason") or "").strip()
    llm_error_summary_by_type = llm_diag.get("llm_error_summary_by_type", {}) if isinstance(llm_diag.get("llm_error_summary_by_type", {}), dict) else {}
    build_block_reason = _resolve_build_block_reason(
        rows_training_candidates=rows_training_candidates_total,
        rows_docs_prepared=len(rows_payload),
        llm_failed_count=llm_failed_count,
        source_coverage_failed_rows=len(source_coverage_failed_rows),
    )
    source_coverage_passed = len(source_coverage_failed_rows) == 0
    external_source_fail_rows = [
        item
        for item in source_coverage_failed_rows
        if any(str(reason).startswith("external_") for reason in item.get("source_coverage_fail_reasons", []))
    ]
    if bool(require_external_sources) and not bool(allow_no_external_sources) and len(external_source_fail_rows) > 0:
        build_block_reason = "external_sources_unavailable"
    elif len(source_coverage_failed_rows) > 0 and build_block_reason == "source_coverage_failed":
        build_block_reason = "source_coverage_failed"
    if stopped_reason:
        build_block_reason = stopped_reason
    llm_error_examples = llm_diag.get("llm_error_examples", []) if isinstance(llm_diag.get("llm_error_examples", []), list) else []
    network_errors_total = sum(
        int(llm_error_summary_by_type.get(name, 0) or 0)
        for name in ("ollama_dns_failure", "ollama_network_failure", "ollama_timeout", "ollama_http_5xx")
    )
    action_required = ""
    if build_block_reason == "external_sources_unavailable":
        action_required = (
            "Configure training_materials_external_sources_file or TRAINING_EXTERNAL_CURATED_URLS, "
            "or run with --allow-no-external-sources."
        )
    elif build_block_reason == "source_coverage_failed":
        action_required = (
            "Проверьте source coverage (style/speech/product/external). "
            "Для external fallback настройте training_materials_external_sources_file."
        )
    elif int(llm_error_summary_by_type.get("ollama_dns_failure", 0) or 0) > 0:
        action_required = "Проверить DNS/интернет: Resolve-DnsName ollama.com; Test-NetConnection ollama.com -Port 443"
        if build_block_reason in {"llm_generation_failed", "rows_empty"}:
            build_block_reason = "network_or_ollama_cloud_unavailable"
    elif network_errors_total > 0 and build_block_reason == "llm_generation_failed":
        action_required = "Проверить сеть и доступность Ollama Cloud endpoint."

    payload = {
        "mode": "training_materials",
        "week_start": week_start,
        "week_end": week_end,
        "full_run_allowed": full_run_allowed,
        "effective_limit": effective_limit,
        "plan_sheet": str(args.plan_sheet or "План недели"),
        "docs_creation_mode": "dry_run",
        "rows": rows_payload,
        "rows_count": len(rows_payload),
        "rows_training_candidates": rows_training_candidates_total,
        "rows_docs_prepared": len(rows_payload),
        "rows_links_to_write": rows_links_to_write,
        "rows_links_ready_to_write": rows_links_ready_to_write,
        "rows_skipped_existing_links": int(diag.get("rows_skipped_existing_links", 0) or 0),
        "rows_quarantined": len(quarantined_rows),
        "docs_api_status": str(api_caps.get("status") or "docs_api_unavailable"),
        "external_sources_used": external_sources_used,
        "external_sources_count": int(external_sources_count or 0),
        "external_search_status": external_search_status,
        "external_sources_status": external_search_status,
        "external_source_titles": external_source_titles[:25],
        "external_source_urls": external_source_urls[:25],
        "external_source_fetch_errors": external_source_fetch_errors[:25],
        "require_external_sources": require_external_sources,
        "allow_no_external_sources": allow_no_external_sources,
        "external_source_min_count": external_source_min_count,
        "source_coverage_passed": source_coverage_passed,
        "source_coverage_failed_rows": len(source_coverage_failed_rows),
        "llm_runtime": llm_diag.get("llm_runtime", {}),
    }

    writer_plan = {
        "mode": "dry_run" if bool(args.dry_run) or True else "real_write",
        "write_strategy": "values_only",
        "structural_changes_required": False,
        "planned_structural_operations": [],
        "rows_links_to_write": rows_links_to_write,
        "rows_links_ready_to_write": rows_links_ready_to_write,
        "rows_skipped_existing_links": int(diag.get("rows_skipped_existing_links", 0) or 0),
        "rows_quarantined": len(quarantined_rows),
        "docs_creation_mode": "dry_run",
        "write_allowed": False,
        "block_reason": build_block_reason,
        "llm_error_summary_by_type": llm_error_summary_by_type,
        "llm_error_examples": llm_error_examples,
    }
    writer_status = {
        "mode": "dry_run",
        "write_strategy": "values_only",
        "structural_changes_required": False,
        "rows_links_to_write": rows_links_to_write,
        "rows_links_ready_to_write": rows_links_ready_to_write,
        "rows_skipped_existing_links": int(diag.get("rows_skipped_existing_links", 0) or 0),
        "rows_quarantined": len(quarantined_rows),
        "docs_creation_mode": "dry_run",
        "write_allowed": False,
        "block_reason": build_block_reason,
        "rows_written": 0,
        "llm_error_summary_by_type": llm_error_summary_by_type,
        "llm_error_examples": llm_error_examples,
    }

    summary_status = "completed"
    if len(valid_drafts) > 0 and len(quarantined_rows) > 0:
        summary_status = "partial_success"
    elif len(valid_drafts) == 0 and len(quarantined_rows) > 0:
        summary_status = "failed"

    summary = {
        "run_id": run_dir.name,
        "status": summary_status,
        "plan_sheet": str(args.plan_sheet or "План недели"),
        "daily_sheet": str(args.daily_sheet or "Дневной контроль"),
        "call_review_sheet": str(args.call_review_sheet or "Разбор звонков"),
        "week_start": week_start,
        "week_end": week_end,
        "full_run_allowed": full_run_allowed,
        "effective_limit": effective_limit,
        "rows_training_candidates": rows_training_candidates_total,
        "plan_rows_total": int(diag.get("plan_rows_total", 0) or 0),
        "plan_rows_in_week_by_exact_key": int(diag.get("plan_rows_in_week_by_exact_key", 0) or 0),
        "plan_rows_in_week_by_start_only": int(diag.get("plan_rows_in_week_by_start_only", 0) or 0),
        "plan_rows_training_activity_total": int(diag.get("plan_rows_training_activity_total", 0) or 0),
        "plan_rows_training_activity_in_period": int(diag.get("plan_rows_training_activity_in_period", 0) or 0),
        "rows_docs_prepared": len(valid_drafts),
        "rows_links_to_write": rows_links_to_write,
        "rows_links_ready_to_write": rows_links_ready_to_write,
        "rows_skipped_existing_links": int(diag.get("rows_skipped_existing_links", 0) or 0),
        "rows_quarantined": len(quarantined_rows),
        "quality_rows_total": len(quality_review_rows),
        "quality_rows_passed": sum(1 for item in quality_review_rows if bool(item.get("quality_passed", False))),
        "quality_rows_failed": sum(1 for item in quality_review_rows if not bool(item.get("quality_passed", False))),
        "docs_creation_mode": "dry_run",
        "rows_docs_created": 0,
        "rows_task_docs_created": 0,
        "docs_creation_errors_count": 0,
        "style_sources_used": style_sources_used,
        "speech_sources_used": speech_sources_used,
        "product_sources_used": product_sources_used,
        "external_sources_used": external_sources_used,
        "external_sources_count": int(external_sources_count or 0),
        "external_search_status": external_search_status,
        "external_sources_status": external_search_status,
        "external_source_titles": external_source_titles[:25],
        "external_source_urls": external_source_urls[:25],
        "external_source_fetch_errors": external_source_fetch_errors[:25],
        "require_external_sources": require_external_sources,
        "allow_no_external_sources": allow_no_external_sources,
        "external_source_min_count": external_source_min_count,
        "source_coverage_passed": source_coverage_passed,
        "source_coverage_failed_rows": len(source_coverage_failed_rows),
        "source_coverage_failed_examples": source_coverage_failed_rows[:10],
        "source_coverage_warnings": coverage_warnings,
        "docs_api_status": str(api_caps.get("status") or "docs_api_unavailable"),
        "docs_api_available": bool(api_caps.get("docs_api_available", False)),
        "missing_scopes": api_caps.get("missing_scopes", []),
        "scope_mismatch_detected": bool(api_caps.get("scope_mismatch_detected", False)),
        "reauth_required": bool(api_caps.get("reauth_required", False)),
        "reauth_instruction": str(api_caps.get("reauth_instruction") or ""),
        "llm_main_model": (llm_diag.get("llm_runtime", {}).get("main", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("main", {}), dict) else {}).get("model", ""),
        "llm_fallback_model": (llm_diag.get("llm_runtime", {}).get("fallback", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("fallback", {}), dict) else {}).get("model", ""),
        "llm_model_pool_requested": llm_diag.get("llm_model_pool_requested", model_pool),
        "llm_model_pool_effective": llm_diag.get("llm_model_pool_effective", []),
        "llm_attempts_main": int(llm_diag.get("llm_attempts_main", 0) or 0),
        "llm_attempts_fallback": int(llm_diag.get("llm_attempts_fallback", 0) or 0),
        "llm_success_main": int(llm_diag.get("llm_success_main", 0) or 0),
        "llm_success_fallback": int(llm_diag.get("llm_success_fallback", 0) or 0),
        "llm_failed_main": int(llm_diag.get("llm_failed_main", 0) or 0),
        "llm_failed_fallback": int(llm_diag.get("llm_failed_fallback", 0) or 0),
        "fallback_used_count": int(llm_diag.get("fallback_used_count", 0) or 0),
        "fallback_not_attempted_reasons": llm_diag.get("fallback_not_attempted_reasons", []),
        "llm_failed_count": llm_failed_count,
        "llm_error_summary_by_type": llm_error_summary_by_type,
        "llm_error_examples": llm_error_examples,
        "model_used_by_row": llm_diag.get("model_used_by_row", []),
        "model_failures_by_type": llm_diag.get("model_failures_by_type", {}),
        "employee_profile_context_rows": len(llm_diag.get("employee_profile_context_rows", []))
        if isinstance(llm_diag.get("employee_profile_context_rows"), list)
        else 0,
        "employee_behavior_markers_rows": len(llm_diag.get("employee_behavior_marker_rows", []))
        if isinstance(llm_diag.get("employee_behavior_marker_rows"), list)
        else 0,
        "quality_repairs_used": int(llm_diag.get("quality_repairs_used", 0) or 0),
        "targeted_repairs_used": int(llm_diag.get("targeted_repairs_used", 0) or 0),
        "rows_passed_after_repair": int(llm_diag.get("rows_passed_after_repair", 0) or 0),
        "llm_stopped_reason": stopped_reason,
        "action_required": action_required,
        "write_strategy": "values_only",
        "structural_changes_required": False,
        "block_reason": build_block_reason,
        "write_allowed": False,
        "resume_source_run_dir": str(resume_source_dir) if resume_enabled else "",
        "retry_failed_from_run_dir": str(retry_failed_source_dir) if retry_failed_source_dir is not None else "",
        "retry_failed_rows_targeted": len(retry_failed_keys),
    }

    write_json(run_dir / "training_materials_discovery.json", {"candidate_diagnostics": diag, "api_capabilities": api_caps})
    write_json(run_dir / "training_materials_candidate_debug.json", diag)
    write_json(
        run_dir / "training_materials_source_coverage.json",
        {
            "style_sources_used": style_sources_used,
            "speech_sources_used": speech_sources_used,
            "product_sources_used": product_sources_used,
            "external_sources_used": external_sources_used,
            "external_sources_count": int(external_sources_count or 0),
            "external_search_status": external_search_status,
            "external_sources_status": external_search_status,
            "external_source_titles": external_source_titles[:25],
            "external_source_urls": external_source_urls[:25],
            "external_source_fetch_errors": external_source_fetch_errors[:25],
            "require_external_sources": require_external_sources,
            "allow_no_external_sources": allow_no_external_sources,
            "external_source_min_count": external_source_min_count,
            "source_coverage_passed": source_coverage_passed,
            "source_coverage_failed_rows": len(source_coverage_failed_rows),
            "source_coverage_failed_examples": source_coverage_failed_rows[:10],
            "warnings": coverage_warnings,
        },
    )
    write_json(
        run_dir / "training_materials_external_sources_debug.json",
        {
            "provider": external_search_provider,
            "external_search_limit": external_search_limit,
            "external_source_min_count": external_source_min_count,
            "rows_total": rows_training_candidates_total,
            "rows_eligible_after_source_coverage": len(candidates),
            "rows_failed_source_coverage": len(source_coverage_failed_rows),
            "rows": source_coverage_debug_rows,
        },
    )
    write_json(run_dir / "training_materials_sources.json", snippets_payload)
    write_json(run_dir / "training_materials_candidates.json", [asdict(item) for item in candidates])
    write_json(run_dir / "training_materials_llm_requests.json", llm_requests)
    write_json(run_dir / "training_materials_llm_responses.json", llm_responses)
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
    write_json(
        run_dir / "training_materials_quality_review.json",
        {
            "rows_total": len(quality_review_rows),
            "rows_passed": sum(1 for item in quality_review_rows if bool(item.get("quality_passed", False))),
            "rows_failed": sum(1 for item in quality_review_rows if not bool(item.get("quality_passed", False))),
            "rows": quality_review_rows,
        },
    )
    write_json(run_dir / "training_materials_payload.json", payload)
    write_json(run_dir / "training_materials_task_payload.json", {"rows": task_payload, "summary": summarize_task_payload(task_payload)})
    write_json(run_dir / "training_materials_quarantine.json", {"rows_quarantined": len(quarantined_rows), "rows": quarantined_rows})
    write_json(
        run_dir / "training_materials_generation_failures.json",
        {
            "rows_total": len(generation_failures),
            "rows": generation_failures,
        },
    )
    write_json(run_dir / "training_materials_writer_plan.json", writer_plan)
    write_json(run_dir / "training_materials_writer_status.json", writer_status)
    write_json(
        run_dir / "training_materials_runtime_status.json",
        {"status": "build_completed", "api_capabilities": api_caps, "llm": llm_diag},
    )
    write_json(run_dir / "summary.json", summary)
    _update_progress(progress, progress_path, stage="build_completed", started_ts=started_ts)
    progress_reporter.update(
        step_name="artifacts_written",
        current=len(valid_drafts),
        total=max(rows_training_candidates_total, len(valid_drafts)),
        current_item={"stage": "artifacts", "rows": len(valid_drafts)},
        details={"rows_quarantined": len(quarantined_rows)},
    )
    progress_reporter.finish(status=summary_status, step_name="build_completed")
    write_markdown(run_dir / "summary.md", title="Training Materials", lines=_summary_lines(summary))
    print(str(run_dir))


def _run_build(args: argparse.Namespace) -> None:
    _run_build_legacy(args)


def _run_write(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = Path(str(args.run_dir)).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    progress_reporter = ProgressReporter(
        process="training_materials_write",
        run_dir=run_dir,
        heartbeat_seconds=int(getattr(cfg, "progress_heartbeat_seconds", 30) or 30),
        logger=logger,
        step_name="init",
        total=0,
    )

    payload_path = run_dir / "training_materials_payload.json"
    if not payload_path.exists():
        progress_reporter.finish(status="failed", step_name="payload_missing", error=f"payload_missing:{payload_path}")
        raise FileNotFoundError(f"training payload not found: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    generation_failures = _load_generation_failures_rows(run_dir)
    llm_error_summary_by_type = _llm_error_summary_by_type(generation_failures)

    write_enabled = bool(args.write and not args.dry_run)
    dry_run = not write_enabled
    overwrite_links = bool(args.overwrite_links or getattr(args, "force_regenerate_links", False))
    progress_reporter.update(
        step_name="payload_loaded",
        current=0,
        total=len(rows),
        current_item={"stage": "payload_loaded", "rows": len(rows)},
    )

    api_caps = ensure_training_materials_oauth_scopes(
        project_root=app_cfg.project_root,
        logger=logger,
        force_reauth=bool(args.force_reauth),
    )

    docs_stats: dict[str, Any] = {
        "docs_creation_mode": "write" if write_enabled else "dry_run",
        "rows_docs_created": 0,
        "rows_task_docs_created": 0,
        "rows_links_ready_to_write": 0,
        "docs_creation_errors_count": 0,
        "docs_creation_error_examples": [],
        "rows_docs_reused_from_artifact": 0,
    }
    materialized_rows = [dict(item) for item in rows if isinstance(item, dict)]

    if not materialized_rows and generation_failures:
        summary_payload = _load_json(run_dir / "summary.json", default={})
        week_start = str((summary_payload if isinstance(summary_payload, dict) else {}).get("week_start") or "")
        week_end = str((summary_payload if isinstance(summary_payload, dict) else {}).get("week_end") or "")
        retry_command = _build_retry_command_suggestion(
            config_path=str(args.config),
            run_dir=run_dir,
            plan_sheet=str(args.plan_sheet or "План недели"),
            daily_sheet=str((summary_payload if isinstance(summary_payload, dict) else {}).get("daily_sheet") or "Дневной контроль"),
            call_review_sheet=str((summary_payload if isinstance(summary_payload, dict) else {}).get("call_review_sheet") or "Разбор звонков"),
            week_start=week_start,
            week_end=week_end,
            main_model=str((summary_payload if isinstance(summary_payload, dict) else {}).get("llm_main_model") or "qwen3.5:397b-cloud"),
            fallback_model=str((summary_payload if isinstance(summary_payload, dict) else {}).get("llm_fallback_model") or "deepseek-v3.1:671b-cloud"),
        )
        status = {
            "mode": "real_write" if write_enabled else "dry_run",
            "write_strategy": "values_only",
            "structural_changes_required": False,
            "planned_structural_operations": [],
            "write_allowed": False,
            "block_reason": "llm_generation_failed",
            "rows_links_to_write": 0,
            "rows_links_ready_to_write": 0,
            "rows_skipped_existing_links": 0,
            "rows_missing_generated_links": 0,
            "missing_generated_links_examples": [],
            "rows_quarantined": len(generation_failures),
            "quarantined_rows": [],
            "planned_value_ranges": [],
            "rows_written": 0,
            "rows_docs_created": 0,
            "rows_task_docs_created": 0,
            "docs_creation_errors_count": 0,
            "docs_creation_error_examples": [],
            "docs_creation_mode": "dry_run" if dry_run else "write",
            "docs_api_status": str(api_caps.get("status") or ""),
            "drive_api_status": str(api_caps.get("drive_api_status") or ""),
            "docs_api_error_type": "",
            "action_required": "",
            "docs_api_error_message": "",
            "llm_error_examples": _llm_error_examples_from_generation_failures(generation_failures),
            "llm_error_summary_by_type": llm_error_summary_by_type,
            "retry_command_suggestion": retry_command,
        }
        write_json(run_dir / "training_materials_writer_status.json", status)
        summary_path = run_dir / "summary.json"
        summary = _load_json(summary_path, default={})
        if not isinstance(summary, dict):
            summary = {}
        summary.update(
            {
                "write_strategy": "values_only",
                "structural_changes_required": False,
                "write_allowed": False,
                "block_reason": "llm_generation_failed",
                "llm_error_examples": status["llm_error_examples"],
                "llm_error_summary_by_type": llm_error_summary_by_type,
                "retry_command_suggestion": retry_command,
            }
        )
        write_json(summary_path, summary)
        write_markdown(run_dir / "summary.md", title="Training Materials", lines=_summary_lines(summary))
        progress_reporter.finish(status="failed", step_name="llm_generation_failed")
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return

    if write_enabled and str(api_caps.get("status") or "") != "ok":
        docs_stats["docs_creation_mode"] = "write_failed_scope"
        docs_stats["docs_creation_errors_count"] = len(materialized_rows)
        docs_stats["docs_creation_error_examples"] = [
            {
                "reason": "docs_api_unavailable",
                "status": str(api_caps.get("status") or "docs_api_unavailable"),
                "missing_scopes": api_caps.get("missing_scopes", []),
                "reauth_instruction": str(api_caps.get("reauth_instruction") or ""),
            }
        ]
    else:
        materialized_rows, docs_stats = materialize_docs_for_write(
            cfg=cfg,
            run_dir=run_dir,
            payload_rows=materialized_rows,
            write_enabled=write_enabled,
            overwrite_links=overwrite_links,
            logger=logger,
        )

    payload["rows"] = materialized_rows
    payload["rows_count"] = len(materialized_rows)
    payload["rows_links_ready_to_write"] = int(docs_stats.get("rows_links_ready_to_write", 0) or 0)
    payload["docs_creation_mode"] = str(docs_stats.get("docs_creation_mode") or ("write" if write_enabled else "dry_run"))
    write_json(payload_path, payload)
    progress_reporter.update(
        step_name="docs_materialized",
        current=int(docs_stats.get("rows_links_ready_to_write", 0) or 0),
        total=len(materialized_rows),
        current_item={"stage": "docs_materialized", "rows": len(materialized_rows)},
    )

    if write_enabled and str(api_caps.get("status") or "") != "ok":
        status = {
            "mode": "real_write",
            "write_strategy": "values_only",
            "structural_changes_required": False,
            "planned_structural_operations": [],
            "write_allowed": False,
            "block_reason": "docs_api_unavailable",
            "rows_links_to_write": 0,
            "rows_links_ready_to_write": int(docs_stats.get("rows_links_ready_to_write", 0) or 0),
            "rows_skipped_existing_links": 0,
            "rows_missing_generated_links": len(materialized_rows),
            "missing_generated_links_examples": [],
            "rows_quarantined": 0,
            "quarantined_rows": [],
            "planned_value_ranges": [],
            "rows_written": 0,
            "rows_docs_created": int(docs_stats.get("rows_docs_created", 0) or 0),
            "rows_task_docs_created": int(docs_stats.get("rows_task_docs_created", 0) or 0),
            "docs_creation_errors_count": int(docs_stats.get("docs_creation_errors_count", 0) or 0),
            "docs_creation_error_examples": docs_stats.get("docs_creation_error_examples", []),
            "docs_api_status": str(docs_stats.get("docs_api_status") or api_caps.get("status") or "docs_api_unavailable"),
            "drive_api_status": str(docs_stats.get("drive_api_status") or "unknown"),
            "docs_api_error_type": str(docs_stats.get("docs_api_error_type") or "docs_api_unavailable"),
            "action_required": str(docs_stats.get("action_required") or api_caps.get("reauth_instruction") or ""),
            "docs_api_error_message": str(docs_stats.get("docs_api_error_message") or "Google Docs/Drive API unavailable"),
            "llm_error_examples": _llm_error_examples_from_generation_failures(generation_failures),
            "llm_error_summary_by_type": llm_error_summary_by_type,
            "retry_command_suggestion": "",
        }
    else:
        status = execute_links_write(
            cfg=cfg,
            run_dir=run_dir,
            plan_sheet_name=str(args.plan_sheet or "План недели"),
            dry_run=dry_run,
            write=bool(args.write),
            overwrite_links=overwrite_links,
            strict_preflight=bool(args.strict_preflight),
            logger=logger,
            force_reauth=bool(args.force_reauth),
        )
        status["rows_docs_created"] = int(docs_stats.get("rows_docs_created", 0) or 0)
        status["rows_task_docs_created"] = int(docs_stats.get("rows_task_docs_created", 0) or 0)
        status["rows_links_ready_to_write"] = int(docs_stats.get("rows_links_ready_to_write", 0) or 0)
        status["docs_creation_errors_count"] = int(docs_stats.get("docs_creation_errors_count", 0) or 0)
        status["docs_creation_error_examples"] = docs_stats.get("docs_creation_error_examples", [])
        status["docs_creation_mode"] = str(docs_stats.get("docs_creation_mode") or ("write" if write_enabled else "dry_run"))
        runtime_docs_status = str(docs_stats.get("docs_api_status") or "").strip()
        runtime_drive_status = str(docs_stats.get("drive_api_status") or "").strip()
        status["docs_api_status"] = runtime_docs_status or str(api_caps.get("status") or "docs_api_unavailable")
        status["drive_api_status"] = runtime_drive_status or "unknown"
        status["docs_api_error_type"] = str(docs_stats.get("docs_api_error_type") or "")
        status["action_required"] = str(docs_stats.get("action_required") or "")
        status["docs_api_error_message"] = str(docs_stats.get("docs_api_error_message") or "")
        status["llm_error_summary_by_type"] = llm_error_summary_by_type
        status["retry_command_suggestion"] = ""
    status = _merge_status_with_generation_failures(status=status, generation_failures=generation_failures)
    if not isinstance(status.get("llm_error_examples"), list):
        status["llm_error_examples"] = []
    if not isinstance(status.get("llm_error_summary_by_type"), dict):
        status["llm_error_summary_by_type"] = llm_error_summary_by_type

    write_json(run_dir / "training_materials_writer_status.json", status)
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
            "write_strategy": status.get("write_strategy", "values_only"),
            "structural_changes_required": status.get("structural_changes_required", False),
            "rows_links_to_write": status.get("rows_links_to_write", 0),
            "rows_links_ready_to_write": status.get("rows_links_ready_to_write", 0),
            "rows_skipped_existing_links": status.get("rows_skipped_existing_links", 0),
            "rows_missing_generated_links": status.get("rows_missing_generated_links", 0),
            "rows_quarantined": status.get("rows_quarantined", 0),
            "rows_docs_created": status.get("rows_docs_created", 0),
            "rows_task_docs_created": status.get("rows_task_docs_created", 0),
            "docs_creation_errors_count": status.get("docs_creation_errors_count", 0),
            "docs_creation_error_examples": status.get("docs_creation_error_examples", []),
            "docs_creation_mode": status.get("docs_creation_mode", "write" if write_enabled else "dry_run"),
            "docs_api_status": status.get("docs_api_status", summary.get("docs_api_status", "")),
            "drive_api_status": status.get("drive_api_status", summary.get("drive_api_status", "")),
            "docs_api_error_type": status.get("docs_api_error_type", summary.get("docs_api_error_type", "")),
            "action_required": status.get("action_required", summary.get("action_required", "")),
            "docs_api_error_message": status.get("docs_api_error_message", summary.get("docs_api_error_message", "")),
            "write_allowed": status.get("write_allowed", False),
            "block_reason": status.get("block_reason", ""),
            "status": status.get("status", summary.get("status", "")),
            "llm_error_examples": status.get("llm_error_examples", summary.get("llm_error_examples", [])),
            "llm_error_summary_by_type": status.get("llm_error_summary_by_type", summary.get("llm_error_summary_by_type", {})),
        }
    )
    write_json(summary_path, summary)
    write_markdown(run_dir / "summary.md", title="Training Materials", lines=_summary_lines(summary))
    progress_reporter.finish(
        status=str(status.get("status") or ("completed" if bool(status.get("write_allowed", False)) else "completed")),
        step_name="write_completed",
    )
    print(json.dumps(status, ensure_ascii=False, indent=2))


def _run_benchmark_models(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)
    models = _parse_model_pool(str(args.models or ""))
    if not models:
        raise RuntimeError("No models provided for benchmark")

    api_caps = ensure_training_materials_oauth_scopes(
        project_root=app_cfg.project_root,
        logger=logger,
        force_reauth=bool(args.force_reauth),
    )
    candidates, diag = collect_training_candidates(
        cfg=cfg,
        plan_sheet_name=str(args.plan_sheet or "План недели"),
        week_start=str(args.sample_week_start or ""),
        week_end=str(args.sample_week_end or ""),
        manager="",
        plan_date="",
        limit=0,
        logger=logger,
        scopes=training_materials_required_scopes(),
        auth_mode=AUTH_MODE_INTERACTIVE_BOOTSTRAP if bool(args.force_reauth) else AUTH_MODE_AUTO,
    )
    limit = max(1, int(args.limit or 1))
    candidates = candidates[:limit]
    snippets_by_key: dict[str, list[Any]] = {}
    for candidate in candidates:
        snippets, _coverage = collect_source_snippets(
            cfg=cfg,
            training_topic=candidate.what_i_do,
            project_root=app_cfg.project_root,
            candidate=candidate,
        )
        snippets_by_key[candidate.idempotency_key] = snippets

    results: list[dict[str, Any]] = []
    for model in models:
        started = time.perf_counter()
        drafts, quarantined, llm_diag = analyze_training_candidates(
            candidates=candidates,
            snippets_by_key=snippets_by_key,
            cfg=cfg,
            logger=logger,
            main_model_override=model,
            fallback_model_override="",
            model_pool_override=[model],
            llm_max_attempts=3,
            allow_template_fallback=False,
            network_retry_attempts_main=1,
            network_retry_attempts_fallback=1,
            enable_backoff_sleep=False,
        )
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        first_training_quality = (
            (drafts[0].quality_metrics.get("training", {}) if drafts and isinstance(drafts[0].quality_metrics, dict) else {})
            if drafts
            else {}
        )
        first_task_quality = (
            (drafts[0].quality_metrics.get("task", {}) if drafts and isinstance(drafts[0].quality_metrics, dict) else {})
            if drafts
            else {}
        )
        preflight_main = llm_diag.get("preflight", {}).get("main", {}) if isinstance(llm_diag.get("preflight", {}), dict) else {}
        results.append(
            {
                "model": model,
                "available": bool(preflight_main.get("ok", False)),
                "latency_ms": elapsed_ms,
                "valid_json": bool(len(drafts) > 0),
                "training_chars": int(first_training_quality.get("training_chars", 0) or 0),
                "speech_modules_count": int(first_training_quality.get("speech_modules_count", 0) or 0),
                "checklist_items_count": int(first_training_quality.get("checklist_items_count", 0) or 0),
                "quality_passed": bool(
                    first_training_quality.get("quality_passed", False) and first_task_quality.get("quality_passed", False)
                ),
                "rows_passed": int(len(drafts)),
                "rows_quarantined": int(len(quarantined)),
                "llm_failed_count": int(llm_diag.get("llm_failed_count", 0) or 0),
                "llm_error_summary_by_type": llm_diag.get("llm_error_summary_by_type", {}),
            }
        )

    payload = {
        "status": "completed",
        "sample_week_start": str(args.sample_week_start or ""),
        "sample_week_end": str(args.sample_week_end or ""),
        "rows_sampled": len(candidates),
        "models": models,
        "results": results,
        "candidate_diagnostics": diag,
        "api_capabilities": api_caps,
    }
    write_json(run_dir / "training_materials_benchmark.json", payload)
    write_json(run_dir / "summary.json", payload)
    write_markdown(
        run_dir / "summary.md",
        title="Training Materials Benchmark",
        lines=[
            f"sample_week: {args.sample_week_start}..{args.sample_week_end}",
            f"rows_sampled: {len(candidates)}",
            f"models: {', '.join(models)}",
            f"results_count: {len(results)}",
        ],
    )
    print(str(run_dir))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


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
    if args.command == "benchmark-models":
        _run_benchmark_models(args)
        return
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
