from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.config import load_config
from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id
from src.logger import setup_logging
from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.state_store import load_auth_state
from src.amocrm_collector.client import AmoCollectorClient
from src.ops_storage.config import build_janitor_config_from_analyzer
from src.ops_storage.janitor import run_janitor_clean, run_janitor_report
from src.safety import ensure_inside_root

from .call_downloader import CallDownloader
from .call_evidence import build_call_summary, call_evidence_to_dicts
from .call_review_sheet import (
    CALL_REVIEW_DEFAULT_COLUMNS,
    build_call_review_payload,
    rows_to_sheet_matrix,
)
from .base_mix import (
    build_base_mix_text as build_base_mix_text_priority,
    collect_raw_tag_values,
    normalize_tag_values,
    resolve_base_mix as resolve_base_mix_priority,
)
from .config import DealAnalyzerConfig, load_deal_analyzer_config, resolve_period
from .daily_case_modes import (
    classify_daily_case,
    get_role_scope_policy,
    mode_is_writable,
    mode_prompt_policy,
)
from .daily_multistep import BLOCK_KEYS as DAILY_BLOCK_KEYS, assemble_writer_columns, parse_blocks_markdown, validate_blocks
from .crm_consistency import build_crm_consistency_layer
from .enrichment import build_operator_outputs, enrich_rows
from .exporters import (
    analyzer_output_dir,
    build_markdown_report,
    write_analysis_csv,
    write_json_export,
    write_markdown_export,
)
from .llm_backend import analyze_deal_with_hybrid_outcome, analyze_deal_with_ollama_outcome
from .llm_client import OllamaClient
from .llm_runtime import resolve_ollama_runtime
from .models import AnalysisRunMetadata
from .prompt_builder import (
    append_call_review_case_json_repair_instruction,
    append_daily_rerank_json_repair_instruction,
    append_daily_table_json_repair_instruction,
    build_call_review_case_messages,
    build_call_review_effect_messages,
    build_call_review_free_form_messages,
    build_call_review_style_json_messages,
    build_daily_rerank_messages,
    build_daily_table_messages,
)
from .reference_stack import build_daily_reference_stack, build_reference_prompt_section
from .roks_extractor import extract_roks_snapshot
from .rules import analyze_deal
from .snapshot_builder import build_deal_snapshot, build_period_snapshots
from .transcript_signals import build_call_signal_aggregates, derive_transcript_signals
from .transcription import transcribe_call_evidence

MSK_TZ = timezone(timedelta(hours=3))

DAILY_CONTROL_COLUMNS = [
    "Неделя с",
    "Неделя по",
    "Дата контроля",
    "День",
    "Менеджер",
    "Роль менеджера",
    "Проанализировано сделок",
    "Ссылки на сделки",
    "Продукт / фокус",
    "База микс",
    "Ключевой вывод",
    "Сильные стороны",
    "Зоны роста",
    "Почему это важно",
    "Что закрепить",
    "Что исправить",
    "Что донес сотруднику",
    "Ожидаемый эффект - количество",
    "Ожидаемый эффект - качество",
    "Оценка 0-100",
    "Критичность",
]

DAILY_TEXT_COLUMN_KEYS = (
    "Ключевой вывод",
    "Сильные стороны",
    "Зоны роста",
    "Почему это важно",
    "Что закрепить",
    "Что исправить",
    "Что донес сотруднику",
    "Ожидаемый эффект - количество",
    "Ожидаемый эффект - качество",
)

WEEKLY_MANAGER_COLUMNS = [
    "Неделя с",
    "Неделя по",
    "Менеджер",
    "Роль менеджера",
    "Проанализировано сделок",
    "Продукт / фокус недели",
    "База микс недели",
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
    "Средняя оценка 0-100",
]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deal analyzer CLI (rules + hybrid + Ollama backends)")
    parser.add_argument("--config", required=True, help="Path to analyzer config JSON")
    parser.add_argument("--no-latest", action="store_true", help="Disable latest copy outputs")

    sub = parser.add_subparsers(dest="command", required=True)

    one = sub.add_parser("analyze-deal", help="Analyze one deal from collector JSON")
    one.add_argument("--input", required=True, help="Path to collector deal JSON")

    analyze_snapshot = sub.add_parser(
        "analyze-snapshot",
        help="Vertical slice: build/use snapshot for one deal and save analysis JSON artifact",
    )
    analyze_snapshot.add_argument("--input", required=True, help="Path to collector JSON or prepared snapshot JSON")
    analyze_snapshot.add_argument("--deal-id", default="", help="Deal identifier to select from period input")

    period = sub.add_parser("analyze-period", help="Analyze period payload from collector JSON")
    period.add_argument("--input", required=True, help="Path to collector period JSON")
    period.add_argument(
        "--period-mode",
        choices=[
            "smart_manager_default",
            "current_week_to_date",
            "previous_calendar_week",
            "previous_workweek",
            "custom_range",
        ],
        default=None,
        help="Optional period mode override",
    )
    period.add_argument("--date-from", default=None, help="YYYY-MM-DD (required for custom_range)")
    period.add_argument("--date-to", default=None, help="YYYY-MM-DD (required for custom_range)")
    period.add_argument("--limit", type=int, default=None, help="Optional max deals to analyze from period payload")
    period.add_argument("--owner-contains", default=None, help="Optional case-insensitive owner filter for meeting queue")
    period.add_argument("--product-contains", default=None, help="Optional case-insensitive product filter for meeting queue")
    period.add_argument("--status-contains", default=None, help="Optional case-insensitive status/stage filter for meeting queue")
    period.add_argument("--exclude-low-confidence", action="store_true", help="Exclude low-confidence records from meeting queue")
    period.add_argument("--discussion-limit", type=int, default=10, help="Max records in meeting queue artifacts (default: 10)")

    weekly = sub.add_parser("analyze-weekly", help="Build weekly management layer artifacts from period payload")
    weekly.add_argument("--input", required=True, help="Path to collector period JSON")
    weekly.add_argument("--week-start", default=None, help="Optional week start YYYY-MM-DD")
    weekly.add_argument("--week-end", default=None, help="Optional week end YYYY-MM-DD")
    weekly.add_argument("--limit", type=int, default=None, help="Optional max deals to analyze from period payload")
    weekly.add_argument("--manager-contains", default=None, help="Optional case-insensitive owner filter for weekly outputs")
    weekly.add_argument("--discussion-limit", type=int, default=10, help="Max deals in weekly discussion focus (default: 10)")

    enrich_one = sub.add_parser("enrich-deal", help="Build read-only enriched snapshot for one deal")
    enrich_one.add_argument("--input", required=True, help="Path to collector deal JSON")

    enrich_period = sub.add_parser("enrich-period", help="Build read-only enriched snapshots for period payload")
    enrich_period.add_argument("--input", required=True, help="Path to collector period JSON")

    roks = sub.add_parser("roks-snapshot", help="Read-only KPI/context snapshot from ROKS workbook")
    roks.add_argument("--manager", default="", help="Manager display name for manager scope")
    roks.add_argument("--team", action="store_true", help="Team-level snapshot")

    collect_calls = sub.add_parser("collect-calls", help="Collect call evidence (read-only)")
    collect_calls.add_argument("--input", required=True, help="Path to collector deal/period JSON")

    transcribe_deal = sub.add_parser("transcribe-deal", help="Transcribe calls for one deal input")
    transcribe_deal.add_argument("--input", required=True, help="Path to collector deal JSON")

    transcribe_period = sub.add_parser("transcribe-period", help="Transcribe calls for period input")
    transcribe_period.add_argument("--input", required=True, help="Path to collector period JSON")

    call_snapshot = sub.add_parser("build-call-snapshot", help="Build read-only snapshot with call evidence/transcripts")
    call_snapshot.add_argument("--input", required=True, help="Path to collector deal/period JSON")

    sub.add_parser("janitor-report", help="Storage janitor report (no deletion)")

    janitor_clean = sub.add_parser("janitor-clean", help="Storage janitor cleanup")
    janitor_clean.add_argument("--dry-run", action="store_true", help="Preview cleanup candidates")
    janitor_clean.add_argument("--apply", action="store_true", help="Apply deletion for candidates")

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_deal_analyzer_config(args.config)
    output_dir = analyzer_output_dir(cfg.output_dir)
    write_latest = not bool(args.no_latest)

    logger.info(
        "deal analyzer start: backend=%s ollama_base_url=%s ollama_model=%s period_mode=%s",
        cfg.analyzer_backend,
        cfg.ollama_base_url,
        cfg.ollama_model,
        cfg.period_mode,
    )

    if args.command == "roks-snapshot":
        _run_roks_snapshot(cfg, output_dir, write_latest, logger, manager=str(args.manager or "").strip(), team=bool(args.team))
        return

    if args.command == "janitor-report":
        _run_janitor_report(cfg, app, logger)
        return

    if args.command == "janitor-clean":
        _run_janitor_clean(cfg, app, logger, dry_run=bool(getattr(args, "dry_run", False)), apply=bool(getattr(args, "apply", False)))
        return

    input_path = ensure_inside_root(Path(args.input).resolve(), app.project_root)
    payload = _load_json(input_path)

    if args.command == "analyze-deal":
        _run_analyze_deal(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "analyze-snapshot":
        _run_analyze_snapshot(
            cfg,
            output_dir,
            payload,
            input_path.name,
            write_latest,
            logger,
            deal_id=str(getattr(args, "deal_id", "") or "").strip(),
        )
        return

    if args.command == "analyze-period":
        _run_analyze_period(
            cfg,
            output_dir,
            payload,
            input_path.name,
            write_latest,
            logger,
            period_mode=args.period_mode,
            date_from=args.date_from,
            date_to=args.date_to,
            limit=args.limit,
            owner_contains=args.owner_contains,
            product_contains=args.product_contains,
            status_contains=args.status_contains,
            exclude_low_confidence=bool(args.exclude_low_confidence),
            discussion_limit=args.discussion_limit,
        )
        return

    if args.command == "analyze-weekly":
        _run_analyze_weekly(
            cfg,
            output_dir,
            payload,
            input_path.name,
            logger,
            week_start=args.week_start,
            week_end=args.week_end,
            limit=args.limit,
            manager_contains=args.manager_contains,
            discussion_limit=args.discussion_limit,
        )
        return

    if args.command == "enrich-deal":
        _run_enrich_deal(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "enrich-period":
        _run_enrich_period(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "collect-calls":
        _run_collect_calls(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "transcribe-deal":
        _run_transcribe_deal(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "transcribe-period":
        _run_transcribe_period(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    if args.command == "build-call-snapshot":
        _run_build_call_snapshot(cfg, output_dir, payload, input_path.name, write_latest, logger)
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


def _run_janitor_report(cfg: DealAnalyzerConfig, app, logger) -> None:
    janitor_cfg = build_janitor_config_from_analyzer(analyzer_config=cfg, app_config=app)
    if not janitor_cfg.enabled:
        logger.warning("janitor disabled in config (janitor_enabled=false)")
    result = run_janitor_report(config=janitor_cfg, logger=logger)
    summary = result.report_payload.get("summary", {}) if isinstance(result.report_payload.get("summary"), dict) else {}
    logger.info(
        "janitor-report: total=%s reclaimable=%s deletable_files=%s json=%s md=%s",
        summary.get("total_size_human", ""),
        summary.get("reclaimable_human", ""),
        summary.get("deletable_files", 0),
        result.report_json,
        result.report_md,
    )


def _run_janitor_clean(cfg: DealAnalyzerConfig, app, logger, *, dry_run: bool, apply: bool) -> None:
    janitor_cfg = build_janitor_config_from_analyzer(analyzer_config=cfg, app_config=app)
    if not janitor_cfg.enabled:
        logger.warning("janitor disabled in config (janitor_enabled=false)")
    if dry_run and apply:
        raise RuntimeError("janitor-clean: use only one flag: --dry-run or --apply")
    result = run_janitor_clean(config=janitor_cfg, logger=logger, apply=apply, dry_run_override=True if dry_run else None)
    logger.info(
        "janitor-clean finished: mode=%s deleted_files=%s deleted_bytes=%s report_json=%s",
        result.mode,
        result.deleted_files,
        result.deleted_bytes,
        result.report_json,
    )


def _run_enrich_deal(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    normalized = _extract_single_normalized(payload)
    snapshot = build_deal_snapshot(normalized_deal=normalized, config=cfg, logger=logger)

    export_payload = {
        "command": "enrich-deal",
        "source": source_name,
        "snapshot": snapshot,
    }
    json_out = write_json_export(output_dir=output_dir, name="enrich_deal", payload=export_payload, write_latest=write_latest)

    md = _build_snapshot_markdown(title="Deal Enrichment Snapshot", snapshot=snapshot)
    md_out = write_markdown_export(output_dir=output_dir, name="enrich_deal", markdown=md, write_latest=write_latest)

    csv_rows = [snapshot.get("crm", {})]
    csv_out = write_analysis_csv(output_dir=output_dir, name="enrich_deal", rows=csv_rows, write_latest=write_latest)

    logger.info("enrich-deal success: json=%s md=%s csv=%s", json_out.timestamped, md_out.timestamped, csv_out.timestamped)


def _run_enrich_period(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    normalized_rows = _extract_period_normalized(payload)
    snapshot = build_period_snapshots(normalized_deals=normalized_rows, config=cfg, logger=logger)

    export_payload = {
        "command": "enrich-period",
        "source": source_name,
        "snapshot": snapshot,
    }
    json_out = write_json_export(output_dir=output_dir, name="enrich_period", payload=export_payload, write_latest=write_latest)

    md = _build_snapshot_markdown(title="Period Enrichment Snapshot", snapshot=snapshot)
    md_out = write_markdown_export(output_dir=output_dir, name="enrich_period", markdown=md, write_latest=write_latest)

    csv_rows = [item.get("crm", {}) for item in snapshot.get("items", []) if isinstance(item, dict)]
    csv_out = write_analysis_csv(output_dir=output_dir, name="enrich_period", rows=csv_rows, write_latest=write_latest)

    logger.info(
        "enrich-period success: deals=%s json=%s md=%s csv=%s",
        snapshot.get("deals_total", 0),
        json_out.timestamped,
        md_out.timestamped,
        csv_out.timestamped,
    )


def _run_roks_snapshot(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    write_latest: bool,
    logger,
    *,
    manager: str,
    team: bool,
) -> None:
    snapshot = extract_roks_snapshot(config=cfg, logger=logger, manager=manager or None, team=team)
    payload = {
        "command": "roks-snapshot",
        "snapshot": snapshot.to_dict(),
    }
    name = "roks_snapshot_team" if team and not manager else "roks_snapshot_manager"
    json_out = write_json_export(output_dir=output_dir, name=name, payload=payload, write_latest=write_latest)

    md = _build_roks_markdown(snapshot.to_dict())
    md_out = write_markdown_export(output_dir=output_dir, name=name, markdown=md, write_latest=write_latest)

    logger.info("roks-snapshot success: ok=%s json=%s md=%s", snapshot.ok, json_out.timestamped, md_out.timestamped)


def _run_collect_calls(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    try:
        deals = _extract_period_normalized(payload)
    except Exception:
        deals = []
    if not deals:
        deals = [_extract_single_normalized(payload)]

    downloader = CallDownloader(config=cfg, logger=logger)
    results = []
    for deal in deals:
        deal_id = str(deal.get("deal_id") or deal.get("amo_lead_id") or "")
        raw_bundle = _extract_raw_bundle_for_deal(payload, deal_id)
        result = downloader.collect_deal_calls(deal=deal, raw_bundle=raw_bundle)
        results.append(result.to_dict())

    export_payload = {"command": "collect-calls", "source": source_name, "results": results}
    json_out = write_json_export(output_dir=output_dir, name="collect_calls", payload=export_payload, write_latest=write_latest)
    md = _build_calls_markdown(results=results, title="Call Evidence Collection")
    md_out = write_markdown_export(output_dir=output_dir, name="collect_calls", markdown=md, write_latest=write_latest)

    csv_rows = []
    for item in results:
        summary = item.get("call_summary", {}) if isinstance(item.get("call_summary"), dict) else {}
        csv_rows.append(
            {
                "deal_id": item.get("deal_id", ""),
                "call_source": item.get("source_used", ""),
                "calls_total": summary.get("calls_total", 0),
                "missing_recording_calls": summary.get("missing_recording_calls", 0),
                "longest_call_duration_seconds": summary.get("longest_call_duration_seconds", 0),
                "call_warnings": "; ".join(str(x) for x in item.get("warnings", [])),
            }
        )
    csv_out = write_analysis_csv(output_dir=output_dir, name="collect_calls", rows=csv_rows, write_latest=write_latest)
    logger.info("collect-calls success: deals=%s json=%s md=%s csv=%s", len(results), json_out.timestamped, md_out.timestamped, csv_out.timestamped)


def _run_transcribe_deal(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    normalized = _extract_single_normalized(payload)
    deal_id = str(normalized.get("deal_id") or normalized.get("amo_lead_id") or "")
    downloader = CallDownloader(config=cfg, logger=logger)
    raw_bundle = _extract_raw_bundle_for_deal(payload, deal_id)
    call_result = downloader.collect_deal_calls(deal=normalized, raw_bundle=raw_bundle)
    call_dicts = call_evidence_to_dicts(call_result.calls)
    transcripts = transcribe_call_evidence(calls=call_dicts, config=cfg, logger=logger)

    export_payload = {
        "command": "transcribe-deal",
        "source": source_name,
        "deal_id": deal_id,
        "calls": call_dicts,
        "call_summary": build_call_summary(call_result.calls),
        "transcripts": transcripts,
        "warnings": call_result.warnings,
    }
    json_out = write_json_export(output_dir=output_dir, name="transcribe_deal", payload=export_payload, write_latest=write_latest)
    md_out = write_markdown_export(
        output_dir=output_dir,
        name="transcribe_deal",
        markdown=_build_transcripts_markdown(title="Deal Transcription", payload=export_payload),
        write_latest=write_latest,
    )
    csv_rows = [
        {
            "deal_id": deal_id,
            "calls_total": len(call_dicts),
            "transcripts_total": len(transcripts),
            "transcription_backend": cfg.transcription_backend,
            "call_collection_mode": cfg.call_collection_mode,
        }
    ]
    csv_out = write_analysis_csv(output_dir=output_dir, name="transcribe_deal", rows=csv_rows, write_latest=write_latest)
    logger.info("transcribe-deal success: deal=%s calls=%s transcripts=%s json=%s", deal_id, len(call_dicts), len(transcripts), json_out.timestamped)


def _run_transcribe_period(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    try:
        deals = _extract_period_normalized(payload)
    except Exception:
        deals = [_extract_single_normalized(payload)]
    downloader = CallDownloader(config=cfg, logger=logger)
    items: list[dict[str, Any]] = []
    for deal in deals:
        deal_id = str(deal.get("deal_id") or deal.get("amo_lead_id") or "")
        raw_bundle = _extract_raw_bundle_for_deal(payload, deal_id)
        call_result = downloader.collect_deal_calls(deal=deal, raw_bundle=raw_bundle)
        call_dicts = call_evidence_to_dicts(call_result.calls)
        transcripts = transcribe_call_evidence(calls=call_dicts, config=cfg, logger=logger)
        items.append(
            {
                "deal_id": deal_id,
                "calls": call_dicts,
                "call_summary": build_call_summary(call_result.calls),
                "transcripts": transcripts,
                "warnings": call_result.warnings,
            }
        )

    export_payload = {"command": "transcribe-period", "source": source_name, "items": items, "deals_total": len(items)}
    json_out = write_json_export(output_dir=output_dir, name="transcribe_period", payload=export_payload, write_latest=write_latest)
    md_out = write_markdown_export(
        output_dir=output_dir,
        name="transcribe_period",
        markdown=_build_transcripts_markdown(title="Period Transcription", payload=export_payload),
        write_latest=write_latest,
    )
    csv_rows = [
        {
            "deal_id": item.get("deal_id", ""),
            "calls_total": (item.get("call_summary") or {}).get("calls_total", 0),
            "transcripts_total": len(item.get("transcripts", [])),
            "transcription_backend": cfg.transcription_backend,
            "call_collection_mode": cfg.call_collection_mode,
        }
        for item in items
    ]
    csv_out = write_analysis_csv(output_dir=output_dir, name="transcribe_period", rows=csv_rows, write_latest=write_latest)
    logger.info("transcribe-period success: deals=%s json=%s md=%s csv=%s", len(items), json_out.timestamped, md_out.timestamped, csv_out.timestamped)


def _run_build_call_snapshot(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    is_period = False
    try:
        deals = _extract_period_normalized(payload)
        is_period = len(deals) > 1 or isinstance(payload, list) or (isinstance(payload, dict) and "normalized_deals" in payload)
    except Exception:
        deals = []

    if is_period:
        raw_map = _extract_raw_bundles_map(payload)
        snapshot = build_period_snapshots(normalized_deals=deals, config=cfg, logger=logger, raw_bundles_by_deal=raw_map)
        name = "call_snapshot_period"
        md = _build_snapshot_markdown(title="Period Call Snapshot", snapshot=snapshot)
    else:
        normalized = _extract_single_normalized(payload)
        did = str(normalized.get("deal_id") or normalized.get("amo_lead_id") or "")
        raw_bundle = _extract_raw_bundle_for_deal(payload, did)
        snapshot = build_deal_snapshot(normalized_deal=normalized, config=cfg, logger=logger, raw_bundle=raw_bundle)
        name = "call_snapshot_deal"
        md = _build_snapshot_markdown(title="Deal Call Snapshot", snapshot=snapshot)

    export_payload = {"command": "build-call-snapshot", "source": source_name, "snapshot": snapshot}
    json_out = write_json_export(output_dir=output_dir, name=name, payload=export_payload, write_latest=write_latest)
    md_out = write_markdown_export(output_dir=output_dir, name=name, markdown=md, write_latest=write_latest)
    logger.info("build-call-snapshot success: json=%s md=%s", json_out.timestamped, md_out.timestamped)


def _run_analyze_deal(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
) -> None:
    normalized = _extract_single_normalized(payload)
    enriched_rows = _maybe_enrich_rows([normalized], cfg, logger)
    normalized = enriched_rows[0]

    analysis, llm_counts = _analyze_one_with_isolation(
        normalized,
        cfg,
        logger,
        deal_hint="single",
        backend_override=cfg.analyzer_backend,
    )

    executed_at = datetime.now(timezone.utc).isoformat()
    metadata = AnalysisRunMetadata(
        executed_at=executed_at,
        period_mode_resolved="single_deal",
        period_start="",
        period_end="",
        public_period_label="single_deal",
        as_of_date=datetime.now().date().isoformat(),
        llm_success_count=llm_counts["llm_success_count"],
        llm_success_repaired_count=llm_counts["llm_success_repaired_count"],
        llm_fallback_count=llm_counts["llm_fallback_count"],
        llm_error_count=llm_counts["llm_error_count"],
        backend_requested=cfg.analyzer_backend,
        backend_effective_summary=_build_backend_effective_summary(llm_counts, cfg.analyzer_backend),
    )
    public_meta = _public_metadata(cfg, metadata)

    export_payload = {
        "command": "analyze-deal",
        "source": source_name,
        "backend": cfg.analyzer_backend,
        "metadata": public_meta,
        "analysis": analysis,
    }

    json_out = write_json_export(output_dir=output_dir, name="analyze_deal", payload=export_payload, write_latest=write_latest)
    md = build_markdown_report(title="Deal Analyzer / Single Deal", analyses=[analysis], report_metadata=public_meta)
    md_out = write_markdown_export(output_dir=output_dir, name="analyze_deal", markdown=md, write_latest=write_latest)

    csv_rows = [_attach_metadata(analysis, public_meta)]
    csv_out = write_analysis_csv(
        output_dir=output_dir,
        name="analyze_deal",
        rows=csv_rows,
        write_latest=write_latest,
        include_executed_at="executed_at" in public_meta,
    )

    logger.info(
        "analyze-deal success: backend=%s deal_id=%s score=%s analysis_backend_used=%s json=%s md=%s csv=%s",
        cfg.analyzer_backend,
        analysis.get("deal_id"),
        analysis.get("score_0_100"),
        analysis.get("analysis_backend_used"),
        json_out.timestamped,
        md_out.timestamped,
        csv_out.timestamped,
    )


def _run_analyze_snapshot(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
    *,
    deal_id: str,
) -> None:
    snapshot_source = "prepared_snapshot"
    if deal_id:
        normalized = _find_normalized_by_deal_id(payload, deal_id)
        raw_bundle = _extract_raw_bundle_for_deal(payload, deal_id)
        snapshot = build_deal_snapshot(normalized_deal=normalized, config=cfg, logger=logger, raw_bundle=raw_bundle)
        snapshot_source = "built_from_deal_id"
    else:
        prepared_snapshot = _extract_prepared_snapshot(payload)
        if prepared_snapshot is not None:
            snapshot = prepared_snapshot
        else:
            normalized = _extract_single_normalized(payload)
            raw_bundle = _extract_raw_bundle_for_deal(
                payload,
                str(normalized.get("deal_id") or normalized.get("amo_lead_id") or ""),
            )
            snapshot = build_deal_snapshot(normalized_deal=normalized, config=cfg, logger=logger, raw_bundle=raw_bundle)
            snapshot_source = "built_from_single_input"

    crm = snapshot.get("crm") if isinstance(snapshot, dict) and isinstance(snapshot.get("crm"), dict) else {}
    if not crm:
        raise RuntimeError("analyze-snapshot requires snapshot.crm payload")

    analysis, llm_counts = _analyze_one_with_isolation(
        crm,
        cfg,
        logger,
        deal_hint=str(crm.get("deal_id") or crm.get("amo_lead_id") or "snapshot"),
        backend_override=cfg.analyzer_backend,
    )
    analysis = _attach_enrichment_and_operator_outputs(
        analysis,
        crm,
        cfg,
        snapshot=snapshot if isinstance(snapshot, dict) else None,
    )
    analysis.update(_derive_product_hypothesis(analysis=analysis, deal=crm, snapshot=snapshot if isinstance(snapshot, dict) else None))
    executed_at = datetime.now(timezone.utc).isoformat()
    metadata = AnalysisRunMetadata(
        executed_at=executed_at,
        period_mode_resolved="snapshot_single",
        period_start="",
        period_end="",
        public_period_label="snapshot_single",
        as_of_date=datetime.now().date().isoformat(),
        llm_success_count=llm_counts["llm_success_count"],
        llm_success_repaired_count=llm_counts["llm_success_repaired_count"],
        llm_fallback_count=llm_counts["llm_fallback_count"],
        llm_error_count=llm_counts["llm_error_count"],
        backend_requested=cfg.analyzer_backend,
        backend_effective_summary=_build_backend_effective_summary(llm_counts, cfg.analyzer_backend),
    )
    public_meta = _public_metadata(cfg, metadata)
    export_payload = {
        "command": "analyze-snapshot",
        "source": source_name,
        "snapshot_source": snapshot_source,
        "backend_requested": cfg.analyzer_backend,
        "backend_used": analysis.get("analysis_backend_used", ""),
        "metadata": public_meta,
        "snapshot": snapshot,
        "analysis": analysis,
    }
    json_out = write_json_export(
        output_dir=output_dir,
        name="analyze_snapshot",
        payload=export_payload,
        write_latest=write_latest,
    )
    logger.info(
        "analyze-snapshot success: source=%s backend_requested=%s backend_used=%s deal_id=%s json=%s",
        snapshot_source,
        cfg.analyzer_backend,
        analysis.get("analysis_backend_used", ""),
        analysis.get("deal_id"),
        json_out.timestamped,
    )


def _run_analyze_period(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    write_latest: bool,
    logger,
    *,
    period_mode: str | None,
    date_from: str | None,
    date_to: str | None,
    limit: int | None = None,
    owner_contains: str | None = None,
    product_contains: str | None = None,
    status_contains: str | None = None,
    exclude_low_confidence: bool = False,
    discussion_limit: int = 10,
) -> None:
    resolved = resolve_period(
        config=cfg,
        requested_mode=period_mode,
        cli_date_from=date_from,
        cli_date_to=date_to,
    )

    logger.info(
        "deal analyzer period resolved: requested_mode=%s resolved_mode=%s start=%s end=%s as_of=%s",
        resolved.requested_mode,
        resolved.resolved_mode,
        resolved.period_start.isoformat(),
        resolved.period_end.isoformat(),
        resolved.as_of_date.isoformat(),
    )
    logger.info(
        "call collection mode effective: mode=%s call_backend=%s transcription_backend=%s",
        cfg.call_collection_mode,
        cfg.call_backend,
        cfg.transcription_backend,
    )

    preflight_forced_rules = False
    effective_backend = cfg.analyzer_backend
    analysis_cfg = cfg
    analysis_llm_runtime = resolve_ollama_runtime(
        cfg=cfg,
        enabled=cfg.analyzer_backend in {"ollama", "hybrid"},
        logger=logger,
        log_prefix="analysis llm",
    )
    if cfg.analyzer_backend in {"ollama", "hybrid"}:
        selected = str(analysis_llm_runtime.get("selected") or "none")
        if selected == "fallback":
            fb = analysis_llm_runtime.get("fallback", {}) if isinstance(analysis_llm_runtime.get("fallback"), dict) else {}
            analysis_cfg = replace(
                cfg,
                ollama_base_url=str(fb.get("base_url") or cfg.ollama_base_url),
                ollama_model=str(fb.get("model") or cfg.ollama_model),
                ollama_timeout_seconds=int(fb.get("timeout_seconds") or cfg.ollama_timeout_seconds),
            )
        if selected == "none":
            preflight_forced_rules = True
            effective_backend = "rules"

    normalized_rows_input = _extract_period_normalized(payload)
    raw_bundles_input = _extract_raw_bundles_map(payload)
    normalized_rows_all, raw_bundles_by_deal, refresh_diag = _try_live_refresh_period_rows(
        cfg=cfg,
        logger=logger,
        resolved=resolved,
        fallback_rows=normalized_rows_input,
        fallback_raw_bundles=raw_bundles_input,
    )
    logger.info(
        "period source resolved: mode=%s api_refresh_success=%s fallback_used=%s rows_final=%s error=%s",
        refresh_diag.get("mode", ""),
        refresh_diag.get("api_refresh_success", False),
        refresh_diag.get("fallback_used", True),
        refresh_diag.get("rows_final", len(normalized_rows_all)),
        refresh_diag.get("error", ""),
    )
    normalized_rows_ranked = sorted(
        normalized_rows_all,
        key=_period_deal_priority_key,
    )
    run_started_at = datetime.now(timezone.utc)
    run_dir, deals_dir = _prepare_period_run_dirs(output_dir=output_dir, run_started_at=run_started_at)
    try:
        call_pool_debug = _collect_call_pool_debug(
            cfg=analysis_cfg,
            logger=logger,
            rows=normalized_rows_ranked,
            raw_bundles_by_deal=raw_bundles_by_deal,
        )
    except Exception as exc:
        logger.warning("call pool pre-limit pass failed: error=%s", exc)
        call_pool_debug = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
            "deals_total_before_limit": len(normalized_rows_ranked),
            "deals_with_any_calls": 0,
            "deals_with_recordings": 0,
            "deals_with_long_calls": 0,
            "deals_with_only_short_calls": 0,
            "deals_with_autoanswer_pattern": 0,
            "deals_with_redial_pattern": 0,
            "items": [],
        }
    _write_json_path(run_dir / "call_pool_debug.json", call_pool_debug)
    (run_dir / "call_pool_debug.md").write_text(
        _build_call_pool_debug_markdown(call_pool_debug=call_pool_debug),
        encoding="utf-8",
    )
    company_tag_propagation_plan = _build_company_tag_propagation_dry_run_plan(rows=normalized_rows_ranked)
    company_tag_propagation_plan_path = run_dir / "company_tag_propagation_dry_run.json"
    _write_json_path(company_tag_propagation_plan_path, company_tag_propagation_plan)
    company_tag_propagation_plan_md_path = run_dir / "company_tag_propagation_dry_run.md"
    company_tag_propagation_plan_md_path.write_text(
        _build_company_tag_propagation_dry_run_markdown(company_tag_propagation_plan),
        encoding="utf-8",
    )
    conversation_pool_payload, discipline_pool_payload, pool_aggregates = _build_call_pool_artifacts(
        call_pool_debug=call_pool_debug
    )
    _write_json_path(run_dir / "conversation_pool.json", conversation_pool_payload)
    (run_dir / "conversation_pool.md").write_text(
        _build_pool_markdown(
            title="Conversation Pool",
            pool_items=conversation_pool_payload.get("items", []),
            total=conversation_pool_payload.get("total", 0),
        ),
        encoding="utf-8",
    )
    _write_json_path(run_dir / "discipline_pool.json", discipline_pool_payload)
    (run_dir / "discipline_pool.md").write_text(
        _build_pool_markdown(
            title="Discipline Pool",
            pool_items=discipline_pool_payload.get("items", []),
            total=discipline_pool_payload.get("total", 0),
        ),
        encoding="utf-8",
    )
    discipline_report_payload = _build_discipline_report_payload(
        discipline_pool_payload=discipline_pool_payload,
    )
    _write_json_path(run_dir / "discipline_report.json", discipline_report_payload)
    (run_dir / "discipline_report.md").write_text(
        _build_discipline_report_markdown(discipline_report=discipline_report_payload),
        encoding="utf-8",
    )
    transcription_shortlist_payload = _build_transcription_shortlist_payload(
        conversation_pool_payload=conversation_pool_payload,
        discipline_pool_payload=discipline_pool_payload,
    )
    shortlist_items_raw = (
        transcription_shortlist_payload.get("items", [])
        if isinstance(transcription_shortlist_payload.get("items"), list)
        else []
    )
    shortlist_items_annotated, dropped_open_window_items = _annotate_shortlist_business_windows(
        shortlist_items=[x for x in shortlist_items_raw if isinstance(x, dict)],
        run_started_at_utc=run_started_at,
    )
    transcription_shortlist_payload["items"] = shortlist_items_annotated
    transcription_shortlist_payload["business_window_open_date"] = _open_business_window_date(run_started_at).isoformat()
    transcription_shortlist_payload["business_window_items_open_bucket"] = int(dropped_open_window_items)
    _write_json_path(run_dir / "transcription_shortlist.json", transcription_shortlist_payload)
    (run_dir / "transcription_shortlist.md").write_text(
        _build_transcription_shortlist_markdown(transcription_shortlist_payload=transcription_shortlist_payload),
        encoding="utf-8",
    )
    logger.info(
        "call pool pre-limit summary: deals_total=%s deals_with_any_calls=%s deals_with_recordings=%s deals_with_long_calls=%s deals_with_only_short_calls=%s deals_with_autoanswer_pattern=%s deals_with_redial_pattern=%s",
        call_pool_debug.get("deals_total_before_limit", 0),
        call_pool_debug.get("deals_with_any_calls", 0),
        call_pool_debug.get("deals_with_recordings", 0),
        call_pool_debug.get("deals_with_long_calls", 0),
        call_pool_debug.get("deals_with_only_short_calls", 0),
        call_pool_debug.get("deals_with_autoanswer_pattern", 0),
        call_pool_debug.get("deals_with_redial_pattern", 0),
    )
    shortlist_items = shortlist_items_annotated
    analysis_shortlist_payload = _build_analysis_shortlist_payload(
        shortlist_items=[x for x in shortlist_items if isinstance(x, dict)],
        normalized_rows_ranked=normalized_rows_ranked,
        limit=limit,
    )
    selected_items_raw = (
        analysis_shortlist_payload.get("selected_items", [])
        if isinstance(analysis_shortlist_payload.get("selected_items"), list)
        else []
    )
    selected_items_filtered, selected_dropped_by_window = _filter_selected_by_business_window(
        selected_items=[x for x in selected_items_raw if isinstance(x, dict)]
    )
    analysis_shortlist_payload["selected_items"] = selected_items_filtered
    analysis_shortlist_payload["business_window_filter"] = {
        "open_window_date": _open_business_window_date(run_started_at).isoformat(),
        "selected_items_before_filter": len(selected_items_raw),
        "selected_items_after_filter": len(selected_items_filtered),
        "selected_items_dropped_open_bucket": int(selected_dropped_by_window),
    }
    _write_json_path(run_dir / "analysis_shortlist.json", analysis_shortlist_payload)
    (run_dir / "analysis_shortlist.md").write_text(
        _build_analysis_shortlist_markdown(payload=analysis_shortlist_payload),
        encoding="utf-8",
    )
    selected_deal_ids = [
        str(x.get("deal_id") or "").strip()
        for x in (
            analysis_shortlist_payload.get("selected_items", [])
            if isinstance(analysis_shortlist_payload.get("selected_items"), list)
            else []
        )
        if isinstance(x, dict)
    ]
    selected_deal_ids = [x for x in selected_deal_ids if x]
    selected_by_deal: dict[str, dict[str, Any]] = {
        str(x.get("deal_id") or "").strip(): x
        for x in (
            analysis_shortlist_payload.get("selected_items", [])
            if isinstance(analysis_shortlist_payload.get("selected_items"), list)
            else []
        )
        if isinstance(x, dict) and str(x.get("deal_id") or "").strip()
    }
    selected_shortlist_rows = [
        x
        for x in shortlist_items
        if isinstance(x, dict) and str(x.get("deal_id") or "").strip() in set(selected_deal_ids)
    ]
    calls_selected_for_stt = sum(int(x.get("selected_call_count", 0) or 0) for x in selected_shortlist_rows)
    calls_filtered_noise_for_stt = sum(int(x.get("filtered_noise_calls_count", 0) or 0) for x in selected_shortlist_rows)
    row_by_id = {
        str(row.get("deal_id") or row.get("amo_lead_id") or "").strip(): row
        for row in normalized_rows_ranked
        if isinstance(row, dict)
    }
    normalized_rows = [row_by_id[did] for did in selected_deal_ids if did in row_by_id]
    if not normalized_rows:
        logger.warning("analysis shortlist is empty after ranking; no deals selected for analyze-period")
    shortlist_by_deal = {
        str(x.get("deal_id") or "").strip(): x
        for x in shortlist_items
        if isinstance(x, dict)
    }
    summary_shortlist = analysis_shortlist_payload.get("selected_items", []) if isinstance(analysis_shortlist_payload.get("selected_items"), list) else []
    logger.info(
        "analysis shortlist resolved: conversation_pool_total=%s shortlist_total=%s deals_selected_for_analysis=%s deals_selected_for_stt=%s calls_selected_for_stt=%s calls_filtered_noise=%s",
        conversation_pool_payload.get("total", 0),
        len(shortlist_items),
        len(summary_shortlist),
        len(selected_deal_ids),
        calls_selected_for_stt,
        calls_filtered_noise_for_stt,
    )

    analyses: list[dict[str, Any]] = []
    llm_counts = {
        "llm_success_count": 0,
        "llm_success_repaired_count": 0,
        "llm_fallback_count": 0,
        "llm_error_count": 0,
    }
    deals_failed = 0
    deal_artifact_paths: list[str] = []
    period_deal_records: list[dict[str, Any]] = []
    transcription_impact_rows: list[dict[str, Any]] = []

    for idx, row in enumerate(normalized_rows):
        deal_hint = str(row.get("deal_id") or row.get("amo_lead_id") or idx)
        try:
            shortlist_meta = shortlist_by_deal.get(str(deal_hint).strip(), {})
            analysis_meta = selected_by_deal.get(str(deal_hint).strip(), {})
            selected_call_ids = {
                str(x).strip()
                for x in (
                    shortlist_meta.get("selected_call_ids", [])
                    if isinstance(shortlist_meta.get("selected_call_ids"), list)
                    else []
                )
                if str(x).strip()
            }
            try:
                snapshot = build_deal_snapshot(
                    normalized_deal=row,
                    config=analysis_cfg,
                    logger=logger,
                    raw_bundle=raw_bundles_by_deal.get(deal_hint),
                    selected_call_ids=selected_call_ids,
                    transcription_selection_reason=str(shortlist_meta.get("transcription_selection_reason") or ""),
                )
            except TypeError:
                # Backward-compatible call path for tests/older helpers with legacy signature.
                snapshot = build_deal_snapshot(
                    normalized_deal=row,
                    config=analysis_cfg,
                    logger=logger,
                    raw_bundle=raw_bundles_by_deal.get(deal_hint),
                )
            crm = snapshot.get("crm") if isinstance(snapshot.get("crm"), dict) else row
            call_retry_candidate_score = _daily_candidate_retry_score(snapshot if isinstance(snapshot, dict) else {})
            allow_quality_retry = bool(
                getattr(analysis_cfg, "whisper_quality_retry_enabled", False)
                and (
                    not bool(getattr(analysis_cfg, "whisper_quality_retry_only_for_daily_candidates", True))
                    or call_retry_candidate_score >= 40
                    or idx < 3
                )
            )
            retry_info = _maybe_retry_transcript_quality_for_deal(
                snapshot=snapshot if isinstance(snapshot, dict) else {},
                crm=crm,
                cfg=analysis_cfg,
                logger=logger,
                allow_retry_for_deal=allow_quality_retry,
            )
            analysis, row_counts = _analyze_one_with_isolation(
                crm,
                analysis_cfg,
                logger,
                deal_hint=deal_hint,
                backend_override=effective_backend,
            )
            without_transcript_view = _extract_transcription_compare_view(analysis)
            analysis = _attach_enrichment_and_operator_outputs(
                analysis,
                crm,
                analysis_cfg,
                snapshot=snapshot if isinstance(snapshot, dict) else None,
            )
            with_transcript_view = _extract_transcription_compare_view(analysis)
            analysis.update(
                _derive_product_hypothesis(
                    analysis=analysis,
                    deal=crm,
                    snapshot=snapshot if isinstance(snapshot, dict) else None,
                )
            )
            with_transcript_view = _extract_transcription_compare_view(analysis)
            crm_consistency = build_crm_consistency_layer(crm=crm, analysis=analysis)
            analysis.update(crm_consistency)
            analyses.append(analysis)
            llm_counts["llm_success_count"] += row_counts["llm_success_count"]
            llm_counts["llm_success_repaired_count"] += row_counts["llm_success_repaired_count"]
            llm_counts["llm_fallback_count"] += row_counts["llm_fallback_count"]
            llm_counts["llm_error_count"] += row_counts["llm_error_count"]

            per_deal_payload = {
                "run_timestamp": run_started_at.isoformat(),
                "source": source_name,
                "backend_requested": cfg.analyzer_backend,
                "backend_used": analysis.get("analysis_backend_used", ""),
                "deal_id": analysis.get("deal_id") or crm.get("deal_id"),
                "amo_lead_id": analysis.get("amo_lead_id") or crm.get("amo_lead_id"),
                "snapshot_warnings": snapshot.get("warnings", []) if isinstance(snapshot.get("warnings"), list) else [],
                "snapshot": snapshot,
                "analysis": analysis,
            }
            deal_artifact = deals_dir / _deal_artifact_filename(analysis=analysis, index=idx)
            _write_json_path(deal_artifact, per_deal_payload)
            deal_artifact_paths.append(str(deal_artifact))
            transcription_impact_rows.append(
                _build_transcription_impact_row(
                    deal_id=analysis.get("deal_id") or crm.get("deal_id"),
                    deal_name=analysis.get("deal_name") or crm.get("deal_name") or "",
                    owner_name=crm.get("responsible_user_name") or "",
                    status_or_stage=_compose_status_stage(
                        status_name=crm.get("status_name"),
                        pipeline_name=crm.get("pipeline_name"),
                    ),
                    score=analysis.get("score_0_100"),
                    without_view=without_transcript_view,
                    with_view=with_transcript_view,
                    analysis=analysis,
                    snapshot=snapshot if isinstance(snapshot, dict) else {},
                    artifact_path=str(deal_artifact),
                )
            )
            transcript_items = (
                snapshot.get("transcripts", [])
                if isinstance(snapshot.get("transcripts"), list)
                else []
            )
            transcript_err_counts = _transcript_error_counters(transcript_items)
            call_history_pattern = _derive_call_history_pattern(snapshot if isinstance(snapshot, dict) else {})
            deal_tags = crm.get("tags") if isinstance(crm.get("tags"), list) else []
            company_tags = crm.get("company_tags") if isinstance(crm.get("company_tags"), list) else []
            raw_deal_tags = collect_raw_tag_values(deal_tags)
            merged_tags, normalized_company_tags, propagated_company_tags = _merge_deal_company_tags(
                deal_tags=deal_tags,
                company_tags=company_tags,
            )
            if propagated_company_tags:
                logger.info(
                    "deal tag propagation applied: deal=%s added_tags=%s",
                    analysis.get("deal_id") or crm.get("deal_id"),
                    "; ".join(propagated_company_tags),
                )
            dial_discipline = _build_dial_discipline_signals(
                snapshot if isinstance(snapshot, dict) else {},
                status_name=str(crm.get("status_name") or ""),
            )
            anchor_call_timestamp = str(shortlist_meta.get("anchor_call_timestamp") or "").strip()
            if not anchor_call_timestamp:
                anchor_call_timestamp = _extract_anchor_call_timestamp(
                    snapshot=snapshot if isinstance(snapshot, dict) else {},
                    selected_call_ids=list(
                        shortlist_meta.get("selected_call_ids", [])
                        if isinstance(shortlist_meta.get("selected_call_ids"), list)
                        else []
                    ),
                )
            period_deal_records.append(
                {
                    "deal_id": analysis.get("deal_id") or crm.get("deal_id"),
                    "deal_name": analysis.get("deal_name") or crm.get("deal_name") or "",
                    "owner_name": crm.get("responsible_user_name") or "",
                    "product_name": (
                        _to_product_name(crm.get("product_values"))
                        or _to_product_name(crm.get("product_name"))
                        or _to_product_name(crm.get("source_values"))
                    ),
                    "source_values": crm.get("source_values") if isinstance(crm.get("source_values"), list) else [],
                    "deal_tags_raw": raw_deal_tags,
                    "tags": merged_tags,
                    "company_tags": sorted(normalized_company_tags),
                    "propagated_company_tags": propagated_company_tags,
                    "company_name": str(crm.get("company_name") or ""),
                    "source_name": str(crm.get("source_name") or ""),
                    "source_url": str(crm.get("source_url") or ""),
                    "status_name": crm.get("status_name") or "",
                    "pipeline_name": crm.get("pipeline_name") or "",
                    "created_at": crm.get("created_at") or "",
                    "updated_at": crm.get("updated_at") or "",
                    "status_or_stage": _compose_status_stage(
                        status_name=crm.get("status_name"),
                        pipeline_name=crm.get("pipeline_name"),
                    ),
                    "score": analysis.get("score_0_100"),
                    "risk_flags": analysis.get("risk_flags") if isinstance(analysis.get("risk_flags"), list) else [],
                    "data_quality_flags": analysis.get("data_quality_flags") if isinstance(analysis.get("data_quality_flags"), list) else [],
                    "owner_ambiguity_flag": bool(analysis.get("owner_ambiguity_flag")),
                    "crm_hygiene_confidence": str(analysis.get("crm_hygiene_confidence") or ""),
                    "analysis_confidence": str(analysis.get("analysis_confidence") or ""),
                    "strong_sides": analysis.get("strong_sides") if isinstance(analysis.get("strong_sides"), list) else [],
                    "growth_zones": analysis.get("growth_zones") if isinstance(analysis.get("growth_zones"), list) else [],
                    "manager_insight_short": analysis.get("manager_insight_short", ""),
                    "manager_summary": analysis.get("manager_summary", ""),
                    "employee_coaching": str(analysis.get("employee_coaching") or ""),
                    "product_hypothesis_llm": str(analysis.get("product_hypothesis_llm") or "unknown"),
                    "reanimation_reason_short_llm": str(analysis.get("reanimation_reason_short_llm") or ""),
                    "reanimation_potential": str(analysis.get("reanimation_potential") or "none"),
                    "reanimation_reason_short": str(analysis.get("reanimation_reason_short") or ""),
                    "reanimation_next_step": str(analysis.get("reanimation_next_step") or ""),
                    "reanimation_risk_note": str(analysis.get("reanimation_risk_note") or ""),
                    "product_hypothesis": str(analysis.get("product_hypothesis") or "unknown"),
                    "product_hypothesis_confidence": str(analysis.get("product_hypothesis_confidence") or "low"),
                    "product_hypothesis_sources": analysis.get("product_hypothesis_sources")
                    if isinstance(analysis.get("product_hypothesis_sources"), list)
                    else [],
                    "product_hypothesis_reason_short": str(analysis.get("product_hypothesis_reason_short") or ""),
                    "transcript_available": bool(analysis.get("transcript_available")),
                    "transcript_text_excerpt": str(analysis.get("transcript_text_excerpt") or ""),
                    "transcript_source": str(analysis.get("transcript_source") or ""),
                    "transcript_error": str(analysis.get("transcript_error") or ""),
                    "transcript_text_len": int(analysis.get("transcript_text_len", 0) or 0),
                    "transcript_nonempty_ratio": float(analysis.get("transcript_nonempty_ratio", 0.0) or 0.0),
                    "transcript_noise_score": int(analysis.get("transcript_noise_score", 0) or 0),
                    "transcript_repeat_score": int(analysis.get("transcript_repeat_score", 0) or 0),
                    "transcript_signal_score": int(analysis.get("transcript_signal_score", 0) or 0),
                    "transcript_usability_score_final": int(analysis.get("transcript_usability_score_final", 0) or 0),
                    "transcript_usability_label": str(analysis.get("transcript_usability_label") or "empty"),
                    "call_history_pattern_dead_redials": bool(call_history_pattern.get("call_history_pattern_dead_redials")),
                    "call_history_pattern_score": int(call_history_pattern.get("call_history_pattern_score", 0) or 0),
                    "call_history_pattern_label": str(call_history_pattern.get("call_history_pattern_label") or "none"),
                    "call_history_pattern_summary": str(call_history_pattern.get("call_history_pattern_summary") or ""),
                    "dial_unique_phones_count": int(dial_discipline.get("dial_unique_phones_count", 0) or 0),
                    "dial_attempts_total": int(dial_discipline.get("dial_attempts_total", 0) or 0),
                    "dial_over_limit_numbers_count": int(dial_discipline.get("dial_over_limit_numbers_count", 0) or 0),
                    "repeated_dead_redial_count": int(dial_discipline.get("repeated_dead_redial_count", 0) or 0),
                    "repeated_dead_redial_day_flag": bool(dial_discipline.get("repeated_dead_redial_day_flag")),
                    "same_time_redial_pattern_flag": bool(dial_discipline.get("same_time_redial_pattern_flag")),
                    "numbers_not_fully_covered_flag": bool(dial_discipline.get("numbers_not_fully_covered_flag")),
                    "dial_discipline_pattern_label": str(dial_discipline.get("dial_discipline_pattern_label") or "none"),
                    "transcript_quality_retry_used": bool(retry_info.get("used")),
                    "transcript_quality_retry_improved": bool(retry_info.get("improved")),
                    "transcript_quality_retry_model": str(retry_info.get("retry_model") or ""),
                    "transcript_quality_retry_reason": str(retry_info.get("reason") or ""),
                    "call_signal_summary_short": str(analysis.get("call_signal_summary_short") or ""),
                    "call_signal_product_info": bool(analysis.get("call_signal_product_info")),
                    "call_signal_product_link": bool(analysis.get("call_signal_product_link")),
                    "call_signal_demo_discussed": bool(analysis.get("call_signal_demo_discussed")),
                    "call_signal_test_discussed": bool(analysis.get("call_signal_test_discussed")),
                    "call_signal_budget_discussed": bool(analysis.get("call_signal_budget_discussed")),
                    "call_signal_followup_discussed": bool(analysis.get("call_signal_followup_discussed")),
                    "call_signal_objection_price": bool(analysis.get("call_signal_objection_price")),
                    "call_signal_objection_no_need": bool(analysis.get("call_signal_objection_no_need")),
                    "call_signal_objection_not_target": bool(analysis.get("call_signal_objection_not_target")),
                    "call_signal_next_step_present": bool(analysis.get("call_signal_next_step_present")),
                    "call_signal_decision_maker_reached": bool(analysis.get("call_signal_decision_maker_reached")),
                    "crm_consistency_summary": str(analysis.get("crm_consistency_summary") or ""),
                    "crm_hygiene_flags": analysis.get("crm_hygiene_flags")
                    if isinstance(analysis.get("crm_hygiene_flags"), list)
                    else [],
                    "crm_vs_call_mismatch": analysis.get("crm_vs_call_mismatch")
                    if isinstance(analysis.get("crm_vs_call_mismatch"), list)
                    else [],
                    "crm_consistency_debug": analysis.get("crm_consistency_debug")
                    if isinstance(analysis.get("crm_consistency_debug"), dict)
                    else {},
                    "notes_summary_raw": crm.get("notes_summary_raw") if isinstance(crm.get("notes_summary_raw"), list) else [],
                    "tasks_summary_raw": crm.get("tasks_summary_raw") if isinstance(crm.get("tasks_summary_raw"), list) else [],
                    "company_comment": str(crm.get("company_comment") or ""),
                    "contact_comment": str(crm.get("contact_comment") or ""),
                    "call_source_used": str(snapshot.get("call_evidence", {}).get("source_used", ""))
                    if isinstance(snapshot.get("call_evidence"), dict)
                    else "",
                    "call_candidates_count": len(snapshot.get("call_evidence", {}).get("items", []))
                    if isinstance(snapshot.get("call_evidence"), dict)
                    and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                    else 0,
                    "recording_url_count": sum(
                        1
                        for call in (
                            snapshot.get("call_evidence", {}).get("items", [])
                            if isinstance(snapshot.get("call_evidence"), dict)
                            and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                            else []
                        )
                        if isinstance(call, dict) and str(call.get("recording_url") or "").strip()
                    ),
                    "audio_downloaded_count": sum(
                        1
                        for call in (
                            snapshot.get("call_evidence", {}).get("items", [])
                            if isinstance(snapshot.get("call_evidence"), dict)
                            and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                            else []
                        )
                        if isinstance(call, dict) and str(call.get("audio_download_status") or "").strip().lower() == "downloaded"
                    ),
                    "audio_cached_count": sum(
                        1
                        for call in (
                            snapshot.get("call_evidence", {}).get("items", [])
                            if isinstance(snapshot.get("call_evidence"), dict)
                            and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                            else []
                        )
                        if isinstance(call, dict)
                        and str(call.get("audio_download_status") or "").strip().lower() in {"cached", "local_exists", "resolved_file_url"}
                    ),
                    "audio_failed_count": sum(
                        1
                        for call in (
                            snapshot.get("call_evidence", {}).get("items", [])
                            if isinstance(snapshot.get("call_evidence"), dict)
                            and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                            else []
                        )
                        if isinstance(call, dict) and str(call.get("audio_download_status") or "").strip().lower() == "failed"
                    ),
                    "transcription_attempted_count": sum(
                        1
                        for t in (
                            snapshot.get("transcripts", [])
                            if isinstance(snapshot.get("transcripts"), list)
                            else []
                        )
                        if isinstance(t, dict) and str(t.get("transcript_status") or "").strip().lower() != "disabled"
                    ),
                    "transcription_success_count": sum(
                        1
                        for t in (
                            snapshot.get("transcripts", [])
                            if isinstance(snapshot.get("transcripts"), list)
                            else []
                        )
                        if isinstance(t, dict) and str(t.get("transcript_status") or "").strip().lower() in {"ok", "cached"}
                    ),
                    "transcription_failed_count": sum(
                        1
                        for t in (
                            transcript_items
                        )
                        if isinstance(t, dict)
                        and str(t.get("transcript_status") or "").strip().lower() not in {"ok", "cached", "disabled"}
                    ),
                    "transcription_missing_audio_count": transcript_err_counts.get("missing_audio", 0),
                    "transcription_backend_config_failed_count": transcript_err_counts.get("backend_config", 0),
                    "call_evidence_items_count": len(snapshot.get("call_evidence", {}).get("items", []))
                    if isinstance(snapshot.get("call_evidence"), dict)
                    and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                    else 0,
                    "call_evidence_calls_total": int(snapshot.get("call_evidence", {}).get("summary", {}).get("calls_total", 0) or 0)
                    if isinstance(snapshot.get("call_evidence"), dict)
                    and isinstance(snapshot.get("call_evidence", {}).get("summary"), dict)
                    else 0,
                    "selected_for_transcription": bool(shortlist_meta.get("selected_for_transcription")),
                    "transcription_selection_reason": str(shortlist_meta.get("transcription_selection_reason") or ""),
                    "selected_call_ids": shortlist_meta.get("selected_call_ids")
                    if isinstance(shortlist_meta.get("selected_call_ids"), list)
                    else [],
                    "selected_call_count": int(shortlist_meta.get("selected_call_count", 0) or 0),
                    "anchor_call_id": str(shortlist_meta.get("anchor_call_id") or ""),
                    "business_window_date": str(shortlist_meta.get("business_window_date") or ""),
                    "business_window_closed": bool(shortlist_meta.get("business_window_closed")),
                    "call_anchor_timestamp": anchor_call_timestamp,
                    "call_anchor_date": _date_only_from_iso(anchor_call_timestamp),
                    "analysis_shortlist_rank_group": int(analysis_meta.get("rank_group", 0) or 0),
                    "analysis_shortlist_reason": str(analysis_meta.get("shortlist_reason") or ""),
                    "analysis_shortlist_forced_fallback": bool(analysis_meta.get("forced_fallback")),
                    "has_audio_path": bool(
                        any(
                            isinstance(call, dict) and str(call.get("audio_path") or "").strip()
                            for call in (
                                snapshot.get("call_evidence", {}).get("items", [])
                                if isinstance(snapshot.get("call_evidence"), dict)
                                and isinstance(snapshot.get("call_evidence", {}).get("items"), list)
                                else []
                            )
                        )
                    ),
                    "has_transcript_text": bool(
                        any(
                            isinstance(t, dict) and str(t.get("transcript_text") or "").strip()
                            for t in (
                                snapshot.get("transcripts", [])
                                if isinstance(snapshot.get("transcripts"), list)
                                else []
                            )
                        )
                    ),
                    "warnings": per_deal_payload.get("snapshot_warnings", []),
                    "artifact_path": str(deal_artifact),
                }
            )
        except Exception as exc:
            deals_failed += 1
            logger.warning("analyze-period per-deal failed: deal=%s error=%s", deal_hint, exc)
            failed_artifact = deals_dir / f"deal_{idx + 1}_failed.json"
            _write_json_path(
                failed_artifact,
                {
                    "run_timestamp": run_started_at.isoformat(),
                    "source": source_name,
                    "backend_requested": cfg.analyzer_backend,
                    "deal_hint": deal_hint,
                    "error": str(exc),
                },
            )
            deal_artifact_paths.append(str(failed_artifact))
            period_deal_records.append(
                {
                    "deal_id": deal_hint,
                    "deal_name": "",
                    "owner_name": "",
                    "product_name": "",
                    "status_name": "",
                    "pipeline_name": "",
                    "created_at": "",
                    "updated_at": "",
                    "status_or_stage": "",
                    "score": None,
                    "risk_flags": ["analysis_failed"],
                    "data_quality_flags": [],
                    "owner_ambiguity_flag": False,
                    "crm_hygiene_confidence": "",
                    "analysis_confidence": "",
                    "strong_sides": [],
                    "growth_zones": [],
                    "manager_insight_short": "",
                    "manager_summary": "",
                    "employee_coaching": "",
                    "product_hypothesis_llm": "unknown",
                    "reanimation_reason_short_llm": "",
                    "reanimation_potential": "none",
                    "reanimation_reason_short": "",
                    "reanimation_next_step": "",
                    "reanimation_risk_note": "",
                    "product_hypothesis": "unknown",
                    "product_hypothesis_confidence": "low",
                    "product_hypothesis_sources": [],
                    "product_hypothesis_reason_short": "",
                    "transcript_available": False,
                    "transcript_text_excerpt": "",
                    "transcript_source": "",
                    "transcript_error": "",
                    "transcript_text_len": 0,
                    "transcript_nonempty_ratio": 0.0,
                    "transcript_noise_score": 0,
                    "transcript_repeat_score": 0,
                    "transcript_signal_score": 0,
                    "transcript_usability_score_final": 0,
                    "transcript_usability_label": "empty",
                    "call_history_pattern_dead_redials": False,
                    "call_history_pattern_score": 0,
                    "call_history_pattern_label": "none",
                    "call_history_pattern_summary": "",
                    "transcript_quality_retry_used": False,
                    "transcript_quality_retry_improved": False,
                    "transcript_quality_retry_model": "",
                    "transcript_quality_retry_reason": "",
                    "call_signal_summary_short": "",
                    "call_signal_product_info": False,
                    "call_signal_product_link": False,
                    "call_signal_demo_discussed": False,
                    "call_signal_test_discussed": False,
                    "call_signal_budget_discussed": False,
                    "call_signal_followup_discussed": False,
                    "call_signal_objection_price": False,
                    "call_signal_objection_no_need": False,
                    "call_signal_objection_not_target": False,
                    "call_signal_next_step_present": False,
                    "call_signal_decision_maker_reached": False,
                    "crm_consistency_summary": "",
                    "crm_hygiene_flags": [],
                    "crm_vs_call_mismatch": [],
                    "crm_consistency_debug": {},
                    "call_source_used": "analysis_failed",
                    "call_candidates_count": 0,
                    "recording_url_count": 0,
                    "audio_downloaded_count": 0,
                    "audio_cached_count": 0,
                    "audio_failed_count": 0,
                    "transcription_attempted_count": 0,
                    "transcription_success_count": 0,
                    "transcription_failed_count": 0,
                    "transcription_missing_audio_count": 0,
                    "transcription_backend_config_failed_count": 0,
                    "call_evidence_items_count": 0,
                    "call_evidence_calls_total": 0,
                    "selected_for_transcription": False,
                    "transcription_selection_reason": "analysis_failed",
                    "selected_call_ids": [],
                    "selected_call_count": 0,
                    "has_audio_path": False,
                    "has_transcript_text": False,
                    "warnings": [str(exc)],
                    "artifact_path": str(failed_artifact),
                }
            )

    effective_summary = _build_backend_effective_summary(llm_counts, cfg.analyzer_backend, preflight_forced_rules)
    metadata = AnalysisRunMetadata(
        executed_at=datetime.now(timezone.utc).isoformat(),
        period_mode_resolved=resolved.resolved_mode,
        period_start=resolved.period_start.isoformat(),
        period_end=resolved.period_end.isoformat(),
        public_period_label=resolved.public_period_label(cfg.period_label_mode),
        as_of_date=resolved.as_of_date.isoformat(),
        llm_success_count=llm_counts["llm_success_count"],
        llm_success_repaired_count=llm_counts["llm_success_repaired_count"],
        llm_fallback_count=llm_counts["llm_fallback_count"],
        llm_error_count=llm_counts["llm_error_count"],
        backend_requested=cfg.analyzer_backend,
        backend_effective_summary=effective_summary,
    )
    public_meta = _public_metadata(cfg, metadata)

    export_payload = {
        "command": "analyze-period",
        "source": source_name,
        "backend": cfg.analyzer_backend,
        "metadata": public_meta,
        "deals_total": len(analyses),
        "analyses": analyses,
    }

    json_out = write_json_export(output_dir=output_dir, name="analyze_period", payload=export_payload, write_latest=write_latest)
    md = build_markdown_report(title="Deal Analyzer / Period", analyses=analyses, report_metadata=public_meta)
    md_out = write_markdown_export(output_dir=output_dir, name="analyze_period", markdown=md, write_latest=write_latest)

    csv_rows = [_attach_metadata(item, public_meta) for item in analyses]
    csv_out = write_analysis_csv(
        output_dir=output_dir,
        name="analyze_period",
        rows=csv_rows,
        write_latest=write_latest,
        include_executed_at="executed_at" in public_meta,
    )

    summary_payload = _build_period_run_summary(
        run_started_at=run_started_at,
        backend_requested=cfg.analyzer_backend,
        call_collection_mode=cfg.call_collection_mode,
        analyses=analyses,
        deal_artifact_paths=deal_artifact_paths,
        total_deals_seen=len(normalized_rows_all),
        total_deals_analyzed=len(analyses),
        deals_failed=deals_failed,
        limit=limit,
        period_deal_records=period_deal_records,
        call_pool_debug=call_pool_debug,
        call_pool_aggregates=pool_aggregates,
        transcription_shortlist_diagnostics={
            "conversation_pool_total": int(conversation_pool_payload.get("total", 0) or 0),
            "analysis_shortlist_total": int(analysis_shortlist_payload.get("total_selected", 0) or 0),
            "analysis_shortlist_candidates_total": int(analysis_shortlist_payload.get("total_candidates", 0) or 0),
            "analysis_shortlist_limit_applied": analysis_shortlist_payload.get("limit_applied_to_shortlist"),
            "business_window_open_date": str(transcription_shortlist_payload.get("business_window_open_date") or ""),
            "business_window_items_open_bucket": int(
                transcription_shortlist_payload.get("business_window_items_open_bucket", 0) or 0
            ),
            "analysis_shortlist_selected_dropped_open_bucket": int(
                (
                    analysis_shortlist_payload.get("business_window_filter", {})
                    if isinstance(analysis_shortlist_payload.get("business_window_filter"), dict)
                    else {}
                ).get("selected_items_dropped_open_bucket", 0)
                or 0
            ),
            "analysis_shortlist_forced_fallback_total": sum(
                1
                for x in (
                    analysis_shortlist_payload.get("selected_items", [])
                    if isinstance(analysis_shortlist_payload.get("selected_items"), list)
                    else []
                )
                if isinstance(x, dict) and bool(x.get("forced_fallback"))
            ),
            "deals_selected_for_stt": len(selected_deal_ids),
            "calls_selected_for_stt": int(calls_selected_for_stt or 0),
            "calls_filtered_noise": int(calls_filtered_noise_for_stt or 0),
            "calls_filtered_short_no_answer_autoanswer": int(calls_filtered_noise_for_stt or 0),
        },
        discipline_report_summary=discipline_report_payload.get("summary") if isinstance(discipline_report_payload, dict) else {},
    )
    summary_payload["period_start"] = resolved.period_start.isoformat()
    summary_payload["period_end"] = resolved.period_end.isoformat()
    summary_payload["as_of_date"] = resolved.as_of_date.isoformat()
    summary_payload["call_business_windows"] = {
        "timezone": "+03:00",
        "cutoff_local_time": "15:00",
        "open_window_date": str(transcription_shortlist_payload.get("business_window_open_date") or ""),
        "shortlist_items_open_bucket": int(transcription_shortlist_payload.get("business_window_items_open_bucket", 0) or 0),
        "selected_items_dropped_open_bucket": int(
            (
                analysis_shortlist_payload.get("business_window_filter", {})
                if isinstance(analysis_shortlist_payload.get("business_window_filter"), dict)
                else {}
            ).get("selected_items_dropped_open_bucket", 0)
            or 0
        ),
    }
    summary_payload["live_refresh"] = refresh_diag
    summary_payload["company_tag_propagation_dry_run"] = {
        "artifact_json": str(company_tag_propagation_plan_path),
        "artifact_md": str(company_tag_propagation_plan_md_path),
        "rows_total": int(company_tag_propagation_plan.get("rows_total", 0) or 0),
        "safe_to_propagate_total": int(company_tag_propagation_plan.get("safe_to_propagate_total", 0) or 0),
        "unsafe_total": int(company_tag_propagation_plan.get("unsafe_total", 0) or 0),
    }
    summary_path = run_dir / "summary.json"
    _write_json_path(summary_path, summary_payload)
    call_diag = summary_payload.get("call_runtime_diagnostics", {}) if isinstance(summary_payload.get("call_runtime_diagnostics"), dict) else {}
    logger.info(
        "call runtime diagnostics: mode=%s deals_with_call_candidates=%s deals_with_recording_url=%s audio_downloaded=%s audio_cached=%s audio_failed=%s transcription_attempted=%s transcription_success=%s transcription_failed=%s transcription_failed_missing_audio=%s transcription_failed_backend_config=%s transcript_quality_retry_used=%s transcript_quality_retry_improved=%s",
        call_diag.get("call_collection_mode_effective", cfg.call_collection_mode),
        call_diag.get("deals_with_call_candidates", 0),
        call_diag.get("deals_with_recording_url", 0),
        call_diag.get("audio_downloaded", 0),
        call_diag.get("audio_cached", 0),
        call_diag.get("audio_failed", 0),
        call_diag.get("transcription_attempted", 0),
        call_diag.get("transcription_success", 0),
        call_diag.get("transcription_failed", 0),
        call_diag.get("transcription_failed_missing_audio", 0),
        call_diag.get("transcription_failed_backend_config", 0),
        call_diag.get("transcript_quality_retry_used", 0),
        call_diag.get("transcript_quality_retry_improved", 0),
    )
    top_risks_payload = _build_top_risks_payload(period_deal_records=period_deal_records)
    top_risks_path = run_dir / "top_risks.json"
    _write_json_path(top_risks_path, top_risks_payload)
    meeting_queue_payload = _build_meeting_queue(
        period_deal_records=period_deal_records,
        owner_contains=owner_contains,
        product_contains=product_contains,
        status_contains=status_contains,
        exclude_low_confidence=exclude_low_confidence,
        discussion_limit=discussion_limit,
    )
    meeting_queue_path = run_dir / "meeting_queue.json"
    _write_json_path(meeting_queue_path, meeting_queue_payload)
    meeting_queue_md_path = run_dir / "meeting_queue.md"
    meeting_queue_md_path.write_text(
        _build_meeting_queue_markdown(
            queue_items=meeting_queue_payload,
            owner_contains=owner_contains,
            product_contains=product_contains,
            status_contains=status_contains,
            exclude_low_confidence=exclude_low_confidence,
            discussion_limit=discussion_limit,
        ),
        encoding="utf-8",
    )
    summary_md_path = run_dir / "summary.md"
    summary_md_path.write_text(
        _build_period_summary_markdown(
            summary=summary_payload,
            period_deal_records=period_deal_records,
        ),
        encoding="utf-8",
    )
    manager_brief_path = run_dir / "manager_brief.md"
    manager_brief_path.write_text(
        _build_manager_brief_markdown(
            summary=summary_payload,
            period_deal_records=period_deal_records,
        ),
        encoding="utf-8",
    )
    transcription_impact_payload = _build_transcription_impact_payload(transcription_impact_rows=transcription_impact_rows)
    transcription_impact_json_path = run_dir / "transcription_impact.json"
    _write_json_path(transcription_impact_json_path, transcription_impact_payload)
    transcription_impact_md_path = run_dir / "transcription_impact.md"
    transcription_impact_md_path.write_text(
        _build_transcription_impact_markdown(transcription_impact_rows=transcription_impact_rows),
        encoding="utf-8",
    )
    sheets_dry_run_payload_path = run_dir / "meeting_queue_sheets_dry_run.json"
    sheets_dry_run_payload = _build_meeting_queue_sheets_dry_run_payload(queue_items=meeting_queue_payload)
    _write_json_path(
        sheets_dry_run_payload_path,
        sheets_dry_run_payload,
    )
    style_source_excerpt = _load_daily_style_source_excerpt(logger=logger, cfg=analysis_cfg)
    daily_llm_runtime = _resolve_daily_llm_runtime(analysis_cfg, logger)
    summary_payload["daily_llm_runtime"] = {
        "enabled": bool(daily_llm_runtime.get("enabled")),
        "selected": str(daily_llm_runtime.get("selected") or "none"),
        "reason": str(daily_llm_runtime.get("reason") or ""),
        "main_model": str((daily_llm_runtime.get("main") or {}).get("model") or ""),
        "main_base_url": str((daily_llm_runtime.get("main") or {}).get("base_url") or ""),
        "fallback_enabled": bool((daily_llm_runtime.get("fallback") or {}).get("enabled")),
        "fallback_model": str((daily_llm_runtime.get("fallback") or {}).get("model") or ""),
        "fallback_base_url": str((daily_llm_runtime.get("fallback") or {}).get("base_url") or ""),
        "main_ok": bool(daily_llm_runtime.get("main_ok")),
        "fallback_ok": bool(daily_llm_runtime.get("fallback_ok")),
        "main_error": str(daily_llm_runtime.get("main_error") or ""),
        "fallback_error": str(daily_llm_runtime.get("fallback_error") or ""),
    }
    summary_payload["analysis_llm_runtime"] = {
        "selected": str(analysis_llm_runtime.get("selected") or "none"),
        "reason": str(analysis_llm_runtime.get("reason") or ""),
        "main_ok": bool(analysis_llm_runtime.get("main_ok")),
        "fallback_ok": bool(analysis_llm_runtime.get("fallback_ok")),
        "main_model": str((analysis_llm_runtime.get("main") or {}).get("model") or ""),
        "fallback_model": str((analysis_llm_runtime.get("fallback") or {}).get("model") or ""),
    }
    daily_control_payload = {
        "mode": "legacy_daily_disabled_for_call_review_v2",
        "sheet_name": str(getattr(cfg, "deal_analyzer_daily_sheet_name", "") or ""),
        "start_cell": str(getattr(cfg, "deal_analyzer_daily_start_cell", "A2") or "A2"),
        "columns": list(DAILY_CONTROL_COLUMNS),
        "rows": [],
        "selection_debug_summary": {
            "reason": "active_path_uses_call_review_writer_only",
            "daily_rows_from_conversation_pool": 0,
            "daily_rows_from_discipline_pool": 0,
            "daily_rows_skipped_crm_only": 0,
            "daily_rows_with_real_transcript": 0,
            "daily_rows_with_only_discipline_signals": 0,
        },
    }
    daily_control_payload_path = run_dir / "daily_control_sheet_payload.json"
    _write_json_path(daily_control_payload_path, daily_control_payload)
    daily_selection_debug_path = run_dir / "daily_selection_debug.json"
    _write_json_path(
        daily_selection_debug_path,
        {
            "summary": daily_control_payload.get("selection_debug_summary", {}),
            "rows": [],
        },
    )
    reference_stack_debug_path = run_dir / "daily_reference_stack_debug.json"
    _write_json_path(
        reference_stack_debug_path,
        {
            "rows_total": 0,
            "external_retrieval_rows": 0,
            "rows_with_internal_required_ok": 0,
            "rows_with_role_required_ok": 0,
            "rows_with_product_required_ok": 0,
            "rows": [],
            "reason": "legacy_daily_reference_stack_disabled_in_call_review_v2",
        },
    )
    summary_payload["daily_rows_total"] = 0
    summary_payload["daily_rows_llm_ready"] = 0
    summary_payload["daily_rows_skipped_weak_input"] = 0
    summary_payload["daily_rows_from_conversation_pool"] = 0
    summary_payload["daily_rows_from_discipline_pool"] = 0
    summary_payload["daily_rows_skipped_crm_only"] = 0
    summary_payload["daily_rows_with_real_transcript"] = 0
    summary_payload["daily_rows_with_only_discipline_signals"] = 0
    summary_payload["daily_reference_stack"] = {
        "rows_total": 0,
        "rows_with_references": 0,
        "external_retrieval_rows": 0,
        "rows_with_internal_required_ok": 0,
        "rows_with_role_required_ok": 0,
        "rows_with_product_required_ok": 0,
        "diagnostics_path": str(reference_stack_debug_path),
        "reason": "legacy_daily_reference_stack_disabled_in_call_review_v2",
    }
    summary_payload["daily_multistep_pipeline"] = {
        "mode": "disabled_in_call_review_v2",
        "reason": "active_path_uses_call_review_multistep_only",
    }
    call_review_llm_generation = _prepare_call_review_llm_fields(
        cfg=analysis_cfg,
        logger=logger,
        llm_runtime=daily_llm_runtime,
        style_source_excerpt=style_source_excerpt,
        period_deal_records=period_deal_records,
        analysis_shortlist_payload=analysis_shortlist_payload,
        step_artifacts_root=run_dir / "call_review_step_artifacts",
    )
    summary_payload["call_review_llm_generation"] = call_review_llm_generation
    call_review_payload = build_call_review_payload(
        summary=summary_payload,
        period_deal_records=period_deal_records,
        analysis_shortlist_payload=analysis_shortlist_payload,
        base_domain=_resolve_amo_base_domain_for_links(cfg=cfg),
        manager_allowlist=list(getattr(cfg, "daily_manager_allowlist", ()) or ()),
        manager_role_registry=dict(getattr(cfg, "manager_role_registry", {}) or {}),
    )
    call_review_payload_path = run_dir / "call_review_sheet_payload.json"
    _write_json_path(call_review_payload_path, call_review_payload)
    summary_payload["call_review_rows_total"] = int(call_review_payload.get("rows_count", 0) or 0)
    summary_payload["call_review_selection_debug"] = (
        call_review_payload.get("selection_debug", {})
        if isinstance(call_review_payload.get("selection_debug"), dict)
        else {}
    )
    write_cfg = analysis_cfg
    if not bool(daily_llm_runtime.get("selected") in {"main", "fallback"}):
        write_cfg = replace(analysis_cfg, deal_analyzer_write_enabled=False)
        logger.warning(
            "call review writer forced to dry_run: reason=no_live_llm_runtime selected=%s runtime_reason=%s",
            str(daily_llm_runtime.get("selected") or "none"),
            str(daily_llm_runtime.get("reason") or ""),
        )
    call_review_writer_status = _maybe_write_call_review_sheet(
        cfg=write_cfg,
        logger=logger,
        call_review_payload=call_review_payload,
    )
    if analysis_cfg.deal_analyzer_write_enabled and not write_cfg.deal_analyzer_write_enabled:
        call_review_writer_status["error"] = "write_forced_dry_run_no_live_llm"
    summary_payload["call_review_writer"] = call_review_writer_status
    summary_payload["daily_control_writer"] = {
        "enabled": False,
        "mode": "inactive_for_analyze_period",
        "sheet_name": "",
        "start_cell": "",
        "rows_prepared": 0,
        "rows_written": 0,
        "error": "",
    }
    summary_payload["meeting_queue_writer"] = {
        "enabled": False,
        "mode": "inactive_for_analyze_period",
        "sheet_name": "",
        "start_cell": "",
        "rows_prepared": 0,
        "rows_written": 0,
        "error": "",
    }
    _write_json_path(summary_path, summary_payload)
    summary_md_path.write_text(
        _build_period_summary_markdown(
            summary=summary_payload,
            period_deal_records=period_deal_records,
        ),
        encoding="utf-8",
    )
    manager_brief_path.write_text(
        _build_manager_brief_markdown(
            summary=summary_payload,
            period_deal_records=period_deal_records,
        ),
        encoding="utf-8",
    )

    logger.info(
        "analyze-period success: backend=%s deals_seen=%s deals_analyzed=%s deals_failed=%s llm_success=%s llm_success_repaired=%s llm_fallback=%s llm_error=%s effective=%s json=%s md=%s csv=%s run_summary=%s run_md=%s top_risks=%s manager_brief=%s meeting_queue_json=%s meeting_queue_md=%s transcription_impact_md=%s queue_sheets_dry_run=%s daily_sheet_payload=%s call_review_sheet_payload=%s",
        cfg.analyzer_backend,
        len(normalized_rows_all),
        len(analyses),
        deals_failed,
        llm_counts["llm_success_count"],
        llm_counts["llm_success_repaired_count"],
        llm_counts["llm_fallback_count"],
        llm_counts["llm_error_count"],
        effective_summary,
        json_out.timestamped,
        md_out.timestamped,
        csv_out.timestamped,
        summary_path,
        summary_md_path,
        top_risks_path,
        manager_brief_path,
        meeting_queue_path,
        meeting_queue_md_path,
        transcription_impact_md_path,
        sheets_dry_run_payload_path,
        daily_control_payload_path,
        call_review_payload_path,
    )


def _run_analyze_weekly(
    cfg: DealAnalyzerConfig,
    output_dir: Path,
    payload: dict[str, Any] | list[Any],
    source_name: str,
    logger,
    *,
    week_start: str | None,
    week_end: str | None,
    limit: int | None,
    manager_contains: str | None,
    discussion_limit: int,
) -> None:
    normalized_rows_all = _extract_period_normalized(payload)
    normalized_rows = normalized_rows_all[: max(0, int(limit))] if isinstance(limit, int) and limit >= 0 else normalized_rows_all
    raw_bundles_by_deal = _extract_raw_bundles_map(payload)
    run_started_at = datetime.now(timezone.utc)

    weekly_dir = output_dir / "weekly_runs" / run_started_at.strftime("%Y%m%d_%H%M%S")
    weekly_dir.mkdir(parents=True, exist_ok=True)

    analyses: list[dict[str, Any]] = []
    llm_counts = {
        "llm_success_count": 0,
        "llm_success_repaired_count": 0,
        "llm_fallback_count": 0,
        "llm_error_count": 0,
    }
    deals_failed = 0
    records: list[dict[str, Any]] = []
    for idx, row in enumerate(normalized_rows):
        deal_hint = str(row.get("deal_id") or row.get("amo_lead_id") or idx)
        try:
            snapshot = build_deal_snapshot(
                normalized_deal=row,
                config=cfg,
                logger=logger,
                raw_bundle=raw_bundles_by_deal.get(deal_hint),
            )
            crm = snapshot.get("crm") if isinstance(snapshot.get("crm"), dict) else row
            analysis, row_counts = _analyze_one_with_isolation(
                crm,
                cfg,
                logger,
                deal_hint=deal_hint,
                backend_override=cfg.analyzer_backend,
            )
            analysis = _attach_enrichment_and_operator_outputs(
                analysis,
                crm,
                cfg,
                snapshot=snapshot if isinstance(snapshot, dict) else None,
            )
            analysis.update(
                _derive_product_hypothesis(
                    analysis=analysis,
                    deal=crm,
                    snapshot=snapshot if isinstance(snapshot, dict) else None,
                )
            )
            analyses.append(analysis)
            llm_counts["llm_success_count"] += row_counts["llm_success_count"]
            llm_counts["llm_success_repaired_count"] += row_counts["llm_success_repaired_count"]
            llm_counts["llm_fallback_count"] += row_counts["llm_fallback_count"]
            llm_counts["llm_error_count"] += row_counts["llm_error_count"]

            records.append(
                {
                    "deal_id": analysis.get("deal_id") or crm.get("deal_id"),
                    "deal_name": analysis.get("deal_name") or crm.get("deal_name") or "",
                    "owner_name": crm.get("responsible_user_name") or "",
                    "product_name": (
                        _to_product_name(crm.get("product_values"))
                        or _to_product_name(crm.get("product_name"))
                        or _to_product_name(crm.get("source_values"))
                    ),
                    "status_name": crm.get("status_name") or "",
                    "pipeline_name": crm.get("pipeline_name") or "",
                    "status_or_stage": _compose_status_stage(
                        status_name=crm.get("status_name"),
                        pipeline_name=crm.get("pipeline_name"),
                    ),
                    "score": analysis.get("score_0_100"),
                    "risk_flags": analysis.get("risk_flags") if isinstance(analysis.get("risk_flags"), list) else [],
                    "data_quality_flags": analysis.get("data_quality_flags") if isinstance(analysis.get("data_quality_flags"), list) else [],
                    "analysis_confidence": str(analysis.get("analysis_confidence") or ""),
                    "owner_ambiguity_flag": bool(analysis.get("owner_ambiguity_flag")),
                    "crm_hygiene_confidence": str(analysis.get("crm_hygiene_confidence") or ""),
                    "strong_sides": analysis.get("strong_sides") if isinstance(analysis.get("strong_sides"), list) else [],
                    "growth_zones": analysis.get("growth_zones") if isinstance(analysis.get("growth_zones"), list) else [],
                    "manager_summary": str(analysis.get("manager_summary") or ""),
                    "manager_insight_short": str(analysis.get("manager_insight_short") or ""),
                    "employee_fix_tasks": analysis.get("employee_fix_tasks") if isinstance(analysis.get("employee_fix_tasks"), list) else [],
                    "employee_coaching": str(analysis.get("employee_coaching") or ""),
                    "reanimation_potential": str(analysis.get("reanimation_potential") or "none"),
                    "reanimation_reason_short": str(analysis.get("reanimation_reason_short") or ""),
                    "product_hypothesis": str(analysis.get("product_hypothesis") or "unknown"),
                    "product_hypothesis_confidence": str(analysis.get("product_hypothesis_confidence") or "low"),
                    "transcript_available": bool(analysis.get("transcript_available")),
                    "transcript_source": str(analysis.get("transcript_source") or ""),
                    "transcript_error": str(analysis.get("transcript_error") or ""),
                    "call_signal_summary_short": str(analysis.get("call_signal_summary_short") or ""),
                    "call_signal_product_info": bool(analysis.get("call_signal_product_info")),
                    "call_signal_product_link": bool(analysis.get("call_signal_product_link")),
                    "call_signal_next_step_present": bool(analysis.get("call_signal_next_step_present")),
                    "call_signal_objection_price": bool(analysis.get("call_signal_objection_price")),
                    "call_signal_objection_no_need": bool(analysis.get("call_signal_objection_no_need")),
                    "call_signal_objection_not_target": bool(analysis.get("call_signal_objection_not_target")),
                    "warnings": snapshot.get("warnings", []) if isinstance(snapshot.get("warnings"), list) else [],
                }
            )
        except Exception as exc:
            deals_failed += 1
            logger.warning("analyze-weekly per-deal failed: deal=%s error=%s", deal_hint, exc)
            records.append(
                {
                    "deal_id": deal_hint,
                    "deal_name": "",
                    "owner_name": "",
                    "product_name": "",
                    "status_name": "",
                    "pipeline_name": "",
                    "status_or_stage": "",
                    "score": None,
                    "risk_flags": ["analysis_failed"],
                    "data_quality_flags": [],
                    "analysis_confidence": "",
                    "owner_ambiguity_flag": False,
                    "crm_hygiene_confidence": "",
                    "strong_sides": [],
                    "growth_zones": [],
                    "manager_summary": "",
                    "manager_insight_short": "",
                    "employee_fix_tasks": [],
                    "employee_coaching": "",
                    "reanimation_potential": "none",
                    "reanimation_reason_short": "",
                    "product_hypothesis": "unknown",
                    "product_hypothesis_confidence": "low",
                    "transcript_available": False,
                    "transcript_source": "",
                    "transcript_error": "",
                    "call_signal_summary_short": "",
                    "call_signal_product_info": False,
                    "call_signal_product_link": False,
                    "call_signal_next_step_present": False,
                    "call_signal_objection_price": False,
                    "call_signal_objection_no_need": False,
                    "call_signal_objection_not_target": False,
                    "warnings": [str(exc)],
                }
            )

    base_records = [item for item in records if item.get("score") is not None]
    manager_filter = str(manager_contains or "").strip().lower()
    if manager_filter:
        base_records = [item for item in base_records if manager_filter in str(item.get("owner_name") or "").lower()]

    rustam_records = [item for item in base_records if "рустам" in str(item.get("owner_name") or "").lower()]
    ilya_records = [item for item in base_records if "илья" in str(item.get("owner_name") or "").lower()]
    meeting_queue = _build_meeting_queue(
        period_deal_records=base_records,
        owner_contains=None,
        product_contains=None,
        status_contains=None,
        exclude_low_confidence=False,
        discussion_limit=discussion_limit,
    )

    rustam_path = weekly_dir / "rustam_weekly.md"
    ilya_path = weekly_dir / "ilya_weekly.md"
    brief_path = weekly_dir / "weekly_meeting_brief.md"
    plan_path = weekly_dir / "next_week_plan.md"
    summary_path = weekly_dir / "summary.json"

    rustam_path.write_text(
        _build_manager_weekly_markdown(
            manager_name="Рустам",
            role_focus="Холодный этап: выход на ЛПР, квалификация, интерес, назначение встречи.",
            records=rustam_records,
        ),
        encoding="utf-8",
    )
    ilya_path.write_text(
        _build_manager_weekly_markdown(
            manager_name="Илья",
            role_focus="Теплый этап: демо, тест, follow-up, счет, оплата.",
            records=ilya_records,
        ),
        encoding="utf-8",
    )
    brief_path.write_text(
        _build_weekly_meeting_brief_markdown(
            records=base_records,
            meeting_queue=meeting_queue,
            manager_contains=manager_contains,
        ),
        encoding="utf-8",
    )
    plan_path.write_text(
        _build_next_week_plan_markdown(
            rustam_records=rustam_records,
            ilya_records=ilya_records,
            meeting_queue=meeting_queue,
        ),
        encoding="utf-8",
    )

    summary_payload = {
        "run_timestamp": run_started_at.isoformat(),
        "source": source_name,
        "week_start": week_start or "",
        "week_end": week_end or "",
        "backend_requested": cfg.analyzer_backend,
        "backend_effective_summary": _build_backend_effective_summary(llm_counts, cfg.analyzer_backend),
        "analysis_backend_used_counts": dict(Counter(str(x.get("analysis_backend_used") or "unknown") for x in analyses)),
        "llm_success": llm_counts["llm_success_count"],
        "llm_success_repaired": llm_counts["llm_success_repaired_count"],
        "llm_fallback": llm_counts["llm_fallback_count"],
        "llm_error": llm_counts["llm_error_count"],
        "total_deals_seen": len(normalized_rows_all),
        "total_deals_analyzed": len(base_records),
        "deals_failed": deals_failed,
        "manager_filter": manager_contains or "",
        "discussion_limit": discussion_limit,
        "call_signal_aggregates": build_call_signal_aggregates(base_records),
        "rustam_deals": len(rustam_records),
        "ilya_deals": len(ilya_records),
        "queue_count": len(meeting_queue),
        "output_files": {
            "rustam_weekly": str(rustam_path),
            "ilya_weekly": str(ilya_path),
            "weekly_meeting_brief": str(brief_path),
            "next_week_plan": str(plan_path),
        },
    }
    weekly_sheet_payload = _build_weekly_manager_sheet_payload(
        run_timestamp=run_started_at,
        week_start=week_start or "",
        week_end=week_end or "",
        rustam_records=rustam_records,
        ilya_records=ilya_records,
    )
    weekly_payload_path = weekly_dir / "weekly_manager_sheet_payload.json"
    _write_json_path(weekly_payload_path, weekly_sheet_payload)
    weekly_writer_status = _maybe_write_weekly_manager_sheet(
        cfg=cfg,
        logger=logger,
        weekly_payload=weekly_sheet_payload,
    )
    summary_payload["weekly_manager_writer"] = weekly_writer_status
    _write_json_path(summary_path, summary_payload)
    logger.info(
        "analyze-weekly success: deals_seen=%s deals_analyzed=%s failed=%s rustam=%s ilya=%s llm_success=%s llm_fallback=%s weekly_dir=%s weekly_sheet_payload=%s",
        len(normalized_rows_all),
        len(base_records),
        deals_failed,
        len(rustam_records),
        len(ilya_records),
        llm_counts["llm_success_count"],
        llm_counts["llm_fallback_count"],
        weekly_dir,
        weekly_payload_path,
    )


def _run_ollama_preflight(cfg: DealAnalyzerConfig, logger) -> bool:
    runtime = resolve_ollama_runtime(
        cfg=cfg,
        enabled=cfg.analyzer_backend in {"hybrid", "ollama"},
        logger=logger,
        log_prefix="ollama",
    )
    return str(runtime.get("selected") or "none") == "none"


def _resolve_daily_llm_runtime(cfg: DealAnalyzerConfig, logger) -> dict[str, Any]:
    return resolve_ollama_runtime(
        cfg=cfg,
        enabled=cfg.analyzer_backend in {"hybrid", "ollama"},
        logger=logger,
        log_prefix="daily llm",
    )


def _make_llm_client_from_runtime(runtime: dict[str, Any]) -> OllamaClient | None:
    selected = str(runtime.get("selected") or "")
    if selected == "main":
        main = runtime.get("main", {}) if isinstance(runtime.get("main"), dict) else {}
        return OllamaClient(
            base_url=str(main.get("base_url") or ""),
            model=str(main.get("model") or ""),
            timeout_seconds=int(main.get("timeout_seconds") or 60),
        )
    if selected == "fallback":
        fb = runtime.get("fallback", {}) if isinstance(runtime.get("fallback"), dict) else {}
        return OllamaClient(
            base_url=str(fb.get("base_url") or ""),
            model=str(fb.get("model") or ""),
            timeout_seconds=int(fb.get("timeout_seconds") or 60),
        )
    return None


def _llm_chat_json_with_runtime(
    *,
    runtime: dict[str, Any],
    messages: list[dict[str, str]],
    repair_messages: list[dict[str, str]] | None = None,
    logger: Any | None = None,
    log_prefix: str = "daily llm",
) -> tuple[dict[str, Any], str] | tuple[None, str]:
    selected = str(runtime.get("selected") or "none")
    order: list[str] = []
    if selected in {"main", "fallback"}:
        order.append(selected)
    if selected != "main" and bool(runtime.get("main_ok")):
        order.append("main")
    if selected != "fallback" and bool(runtime.get("fallback_ok")):
        order.append("fallback")
    if not order:
        return None, "no_runtime"

    last_error = "unknown_error"
    for idx, source in enumerate(order):
        try:
            client = _make_llm_client_from_runtime({**runtime, "selected": source})
            if client is None:
                continue
            parsed = client.chat_json(messages=messages)
            if isinstance(parsed.payload, dict):
                return parsed.payload, source
        except Exception as exc:
            last_error = str(exc)
            if logger is not None:
                logger.warning("%s failed on source=%s attempt=%s error=%s", log_prefix, source, idx + 1, exc)
            if repair_messages is not None:
                try:
                    client = _make_llm_client_from_runtime({**runtime, "selected": source})
                    if client is None:
                        continue
                    parsed = client.chat_json(messages=repair_messages)
                    if isinstance(parsed.payload, dict):
                        return parsed.payload, source
                except Exception as exc2:
                    last_error = str(exc2)
                    if logger is not None:
                        logger.warning("%s repair failed on source=%s error=%s", log_prefix, source, exc2)
                    continue
    return None, last_error


def _llm_chat_text_with_runtime(
    *,
    runtime: dict[str, Any],
    messages: list[dict[str, str]],
    logger: Any | None = None,
    log_prefix: str = "daily llm text",
) -> tuple[str | None, str]:
    selected = str(runtime.get("selected") or "none")
    order: list[str] = []
    if selected in {"main", "fallback"}:
        order.append(selected)
    if selected != "main" and bool(runtime.get("main_ok")):
        order.append("main")
    if selected != "fallback" and bool(runtime.get("fallback_ok")):
        order.append("fallback")
    if not order:
        return None, "no_runtime"

    last_error = "unknown_error"
    for idx, source in enumerate(order):
        cfg = runtime.get(source, {}) if isinstance(runtime.get(source), dict) else {}
        base_url = str(cfg.get("base_url") or "").strip()
        model = str(cfg.get("model") or "").strip()
        timeout_seconds = int(cfg.get("timeout_seconds") or 60)
        if not base_url or not model:
            continue
        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        endpoint = f"{base_url.rstrip('/')}/api/chat"
        req = Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
            envelope = json.loads(raw)
            content = envelope.get("message", {}).get("content") if isinstance(envelope, dict) else ""
            text = str(content or "").strip()
            if text:
                return text, source
            last_error = "empty_content"
        except HTTPError as exc:
            preview = ""
            try:
                preview = exc.read().decode("utf-8", errors="replace")
            except Exception:
                preview = ""
            last_error = f"http_{exc.code}:{preview[:200]}"
            if logger is not None:
                logger.warning("%s failed on source=%s attempt=%s error=%s", log_prefix, source, idx + 1, last_error)
        except (URLError, TimeoutError, json.JSONDecodeError, RuntimeError) as exc:
            last_error = str(exc)
            if logger is not None:
                logger.warning("%s failed on source=%s attempt=%s error=%s", log_prefix, source, idx + 1, exc)
    return None, last_error


def _date_only_from_iso(value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""
    if "T" in text:
        text = text.split("T", 1)[0]
    elif " " in text:
        text = text.split(" ", 1)[0]
    try:
        datetime.fromisoformat(text)
        return text
    except Exception:
        return ""


def _extract_anchor_call_timestamp(*, snapshot: dict[str, Any], selected_call_ids: list[str]) -> str:
    call_evidence = snapshot.get("call_evidence", {}) if isinstance(snapshot.get("call_evidence"), dict) else {}
    calls = call_evidence.get("items", []) if isinstance(call_evidence.get("items"), list) else []
    selected = {str(x).strip() for x in (selected_call_ids or []) if str(x).strip()}
    if selected:
        for call in calls:
            if not isinstance(call, dict):
                continue
            call_id = str(call.get("call_id") or "").strip()
            if call_id in selected:
                ts = str(call.get("timestamp") or "").strip()
                if ts:
                    return ts
    for call in calls:
        if not isinstance(call, dict):
            continue
        ts = str(call.get("timestamp") or "").strip()
        if ts:
            return ts
    return ""


def _parse_utc_ts(value: Any) -> datetime | None:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except Exception:
            return None
    try:
        raw = text[:-1] + "+00:00" if text.endswith("Z") else text
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _next_workday(day: datetime.date) -> datetime.date:
    cur = day + timedelta(days=1)
    while cur.weekday() >= 5:
        cur = cur + timedelta(days=1)
    return cur


def _business_window_date_for_call(ts_utc: datetime) -> datetime.date:
    local = ts_utc.astimezone(MSK_TZ)
    if (local.hour, local.minute, local.second) < (15, 0, 0):
        return local.date()
    return _next_workday(local.date())


def _open_business_window_date(run_started_at_utc: datetime) -> datetime.date:
    local = run_started_at_utc.astimezone(MSK_TZ)
    if (local.hour, local.minute, local.second) < (15, 0, 0):
        return local.date()
    return _next_workday(local.date())


def _candidate_anchor_call(candidate: dict[str, Any]) -> dict[str, Any] | None:
    calls = candidate.get("call_items", []) if isinstance(candidate.get("call_items"), list) else []
    selected_ids = {
        str(x).strip()
        for x in (candidate.get("selected_call_ids", []) if isinstance(candidate.get("selected_call_ids"), list) else [])
        if str(x).strip()
    }
    scored: list[tuple[int, dict[str, Any]]] = []
    for call in calls:
        if not isinstance(call, dict):
            continue
        call_id = str(call.get("call_id") or "").strip()
        if selected_ids and call_id and call_id not in selected_ids:
            continue
        duration = int(call.get("duration_seconds", 0) or 0)
        score = duration
        if str(call.get("recording_url") or "").strip():
            score += 120
        if str(call.get("direction") or "").strip().lower() == "outbound":
            score += 20
        ts = _parse_utc_ts(call.get("timestamp"))
        if ts is not None:
            score += 10
        scored.append((score, call))
    if not scored:
        return None
    scored.sort(
        key=lambda x: (
            -x[0],
            str((x[1] or {}).get("timestamp") or ""),
            str((x[1] or {}).get("call_id") or ""),
        )
    )
    return scored[0][1]


def _annotate_shortlist_business_windows(
    *,
    shortlist_items: list[dict[str, Any]],
    run_started_at_utc: datetime,
) -> tuple[list[dict[str, Any]], int]:
    open_window_date = _open_business_window_date(run_started_at_utc)
    out: list[dict[str, Any]] = []
    dropped_open_bucket = 0
    for item in shortlist_items:
        candidate = dict(item)
        anchor = _candidate_anchor_call(candidate)
        anchor_ts = _parse_utc_ts(anchor.get("timestamp") if isinstance(anchor, dict) else "")
        if anchor is None:
            anchor_ts = _parse_utc_ts(candidate.get("updated_at") or candidate.get("created_at"))
        business_date = _business_window_date_for_call(anchor_ts) if anchor_ts is not None else None
        candidate["anchor_call_id"] = str(anchor.get("call_id") or "") if isinstance(anchor, dict) else ""
        candidate["anchor_call_timestamp"] = anchor_ts.isoformat() if anchor_ts is not None else ""
        candidate["business_window_date"] = business_date.isoformat() if business_date is not None else ""
        candidate["business_window_open_date"] = open_window_date.isoformat()
        # Backward-safe behavior: if we cannot resolve anchor datetime, do not drop the deal.
        window_closed = True if business_date is None else bool(business_date < open_window_date)
        candidate["business_window_closed"] = window_closed
        if not window_closed and candidate.get("business_window_date"):
            dropped_open_bucket += 1
        out.append(candidate)
    return out, dropped_open_bucket


def _filter_selected_by_business_window(
    *,
    selected_items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    out: list[dict[str, Any]] = []
    dropped = 0
    for item in selected_items:
        if not isinstance(item, dict):
            continue
        if "business_window_closed" not in item:
            out.append(item)
            continue
        if bool(item.get("business_window_closed")):
            out.append(item)
            continue
        dropped += 1
    return out, dropped


def _normalize_call_review_case_mode(*, candidate: dict[str, Any], record: dict[str, Any]) -> str:
    raw = str(candidate.get("call_case_type") or "").strip().lower()
    if raw == "lpr_conversation":
        return "negotiation_lpr_analysis"
    if raw == "secretary_case":
        return "secretary_analysis"
    if raw in {"supplier_inbound", "supplier_case"}:
        return "supplier_inbound_analysis"
    if raw in {"warm_inbound", "warm_case"}:
        return "warm_inbound_analysis"
    if raw in {"presentation", "demo"}:
        return "presentation_analysis"
    if raw in {"test", "pilot"}:
        return "test_analysis"
    if raw in {"dozhim", "closing"}:
        return "dozhim_analysis"
    if raw in {"redial_discipline", "autoanswer_noise"}:
        return "redial_discipline_analysis"
    role_signal = str(record.get("call_role_signal") or "").strip().lower()
    if role_signal == "secretary":
        return "secretary_analysis"
    if role_signal == "supplier":
        return "supplier_inbound_analysis"
    if bool(record.get("call_signal_decision_maker_reached")):
        return "negotiation_lpr_analysis"
    combined = " ".join(
        str(record.get(k) or "").lower()
        for k in ("call_signal_summary_short", "transcript_text_excerpt", "manager_summary")
    )
    if any(token in combined for token in ("презентац", "демо", "показ")):
        return "presentation_analysis"
    if any(token in combined for token in ("тест", "пилот", "критер")):
        return "test_analysis"
    if any(token in combined for token in ("дожим", "кп", "коммерческ", "договор", "счет")):
        return "dozhim_analysis"
    return "skip_no_meaningful_case"


def _call_review_case_has_meaningful_conversation(*, case_mode: str, candidate: dict[str, Any], record: dict[str, Any]) -> bool:
    if case_mode not in {
        "negotiation_lpr_analysis",
        "secretary_analysis",
        "supplier_inbound_analysis",
        "warm_inbound_analysis",
        "presentation_analysis",
        "test_analysis",
        "dozhim_analysis",
    }:
        return False
    if str(candidate.get("pool_type") or "").strip().lower() != "conversation_pool":
        return False
    if not bool(candidate.get("selected_for_transcription")):
        return False
    label = str(candidate.get("transcript_usability_label") or record.get("transcript_usability_label") or "").strip().lower()
    score = int(
        candidate.get("transcript_usability_score_final", 0)
        or record.get("transcript_usability_score_final", 0)
        or 0
    )
    if label in {"empty", "noisy"} and score < 2:
        return False
    call_summary = " ".join(str(record.get("call_signal_summary_short") or "").split()).strip()
    excerpt = " ".join(str(record.get("transcript_text_excerpt") or "").split()).strip()
    has_text = len(call_summary) >= 20 or len(excerpt) >= 40
    if not has_text and score < 2:
        return False
    if case_mode == "secretary_analysis":
        return score >= 1 or bool(call_summary)
    has_next_step = bool(record.get("call_signal_next_step_present"))
    has_lpr = bool(record.get("call_signal_decision_maker_reached"))
    return bool(score >= 2 or has_next_step or has_lpr or has_text)


def _sanitize_call_review_llm_fields(payload: dict[str, Any]) -> dict[str, str]:
    def _to_text(value: Any) -> str:
        if isinstance(value, (list, tuple)):
            parts = [" ".join(str(x or "").split()).strip() for x in value]
            return "; ".join([p for p in parts if p]).strip()
        if isinstance(value, dict):
            parts = []
            for key, item in value.items():
                k = " ".join(str(key or "").split()).strip()
                v = " ".join(str(item or "").split()).strip()
                if k and v:
                    parts.append(f"{k}: {v}")
                elif v:
                    parts.append(v)
            return "; ".join(parts).strip()
        return " ".join(str(value or "").split()).strip()

    def _s(key: str, limit: int = 900) -> str:
        text = _to_text(payload.get(key))
        return text[:limit]

    def _coaching_text(value: Any, limit: int = 900) -> str:
        if isinstance(value, (list, tuple)):
            items = [" ".join(str(x or "").split()).strip() for x in value]
            non_empty = [x for x in items if x]
            if non_empty:
                lines = [f"{idx + 1}) {item}" for idx, item in enumerate(non_empty)]
                return "\n".join(lines)[:limit]
        text = _to_text(value)
        return text[:limit]

    out = {
        "key_takeaway": _s("key_takeaway", 420),
        "strong_sides": _s("strong_sides", 420),
        "growth_zones": _s("growth_zones", 420),
        "why_important": _s("why_important", 420),
        "reinforce": _s("reinforce", 320),
        "fix_action": _s("fix_action", 320),
        "coaching_list": _coaching_text(payload.get("coaching_list"), 900),
        "expected_quantity": _s("expected_quantity", 220),
        "expected_quality": _s("expected_quality", 320),
        "evidence_quote": _s("evidence_quote", 450),
        "stage_secretary_comment": _s("stage_secretary_comment", 320),
        "stage_lpr_comment": _s("stage_lpr_comment", 320),
        "stage_need_comment": _s("stage_need_comment", 320),
        "stage_presentation_comment": _s("stage_presentation_comment", 320),
        "stage_closing_comment": _s("stage_closing_comment", 320),
        "stage_objections_comment": _s("stage_objections_comment", 320),
        "stage_speech_comment": _s("stage_speech_comment", 320),
        "stage_crm_comment": _s("stage_crm_comment", 320),
        "stage_discipline_comment": _s("stage_discipline_comment", 320),
        "stage_demo_comment": _s("stage_demo_comment", 320),
        "stage_demo_intro_comment": _s("stage_demo_intro_comment", 320),
        "stage_demo_context_comment": _s("stage_demo_context_comment", 320),
        "stage_demo_relevant_comment": _s("stage_demo_relevant_comment", 320),
        "stage_demo_process_comment": _s("stage_demo_process_comment", 320),
        "stage_demo_objections_comment": _s("stage_demo_objections_comment", 320),
        "stage_demo_next_step_comment": _s("stage_demo_next_step_comment", 320),
        "stage_test_launch_comment": _s("stage_test_launch_comment", 320),
        "stage_test_criteria_comment": _s("stage_test_criteria_comment", 320),
        "stage_test_owners_comment": _s("stage_test_owners_comment", 320),
        "stage_test_support_comment": _s("stage_test_support_comment", 320),
        "stage_test_feedback_comment": _s("stage_test_feedback_comment", 320),
        "stage_test_objections_comment": _s("stage_test_objections_comment", 320),
        "stage_test_comment": _s("stage_test_comment", 320),
        "stage_dozhim_recontact_comment": _s("stage_dozhim_recontact_comment", 320),
        "stage_dozhim_doubts_comment": _s("stage_dozhim_doubts_comment", 320),
        "stage_dozhim_terms_comment": _s("stage_dozhim_terms_comment", 320),
        "stage_dozhim_decision_comment": _s("stage_dozhim_decision_comment", 320),
        "stage_dozhim_flow_comment": _s("stage_dozhim_flow_comment", 320),
        "stage_dozhim_comment": _s("stage_dozhim_comment", 320),
    }
    if not out["fix_action"] and out["growth_zones"]:
        out["fix_action"] = out["growth_zones"][:320]
    if not out["reinforce"] and out["strong_sides"]:
        out["reinforce"] = out["strong_sides"][:320]
    if not out["coaching_list"] and out["fix_action"]:
        out["coaching_list"] = f"1) {out['fix_action']}"[:900]
    return out


def _call_review_llm_fields_ready(*, fields: dict[str, str], case_mode: str) -> tuple[bool, str]:
    required_core = (
        "key_takeaway",
        "strong_sides",
        "growth_zones",
        "fix_action",
        "why_important",
        "coaching_list",
        "expected_quantity",
        "expected_quality",
    )
    for key in required_core:
        if not str(fields.get(key) or "").strip():
            return False, f"llm_missing_{key}"

    secretary_stage = str(fields.get("stage_secretary_comment") or "").strip()
    lpr_stage = str(fields.get("stage_lpr_comment") or "").strip()
    need_stage = str(fields.get("stage_need_comment") or "").strip()
    presentation_stage = str(fields.get("stage_presentation_comment") or "").strip()
    closing_stage = str(fields.get("stage_closing_comment") or "").strip()
    objections_stage = str(fields.get("stage_objections_comment") or "").strip()
    speech_stage = str(fields.get("stage_speech_comment") or "").strip()

    if case_mode == "secretary_analysis":
        if not secretary_stage:
            return False, "llm_missing_stage_secretary_comment"
        if not need_stage:
            return False, "llm_missing_stage_need_comment"
        return True, ""

    if case_mode == "presentation_analysis":
        if not (
            str(fields.get("stage_demo_intro_comment") or "").strip()
            or str(fields.get("stage_demo_context_comment") or "").strip()
            or str(fields.get("stage_demo_relevant_comment") or "").strip()
            or str(fields.get("stage_demo_comment") or "").strip()
        ):
            return False, "llm_missing_stage_demo_comment"
        return True, ""

    if case_mode == "test_analysis":
        if not (
            str(fields.get("stage_test_launch_comment") or "").strip()
            or str(fields.get("stage_test_criteria_comment") or "").strip()
            or str(fields.get("stage_test_comment") or "").strip()
        ):
            return False, "llm_missing_stage_test_comment"
        return True, ""

    if case_mode == "dozhim_analysis":
        if not (
            str(fields.get("stage_dozhim_recontact_comment") or "").strip()
            or str(fields.get("stage_dozhim_terms_comment") or "").strip()
            or str(fields.get("stage_dozhim_comment") or "").strip()
        ):
            return False, "llm_missing_stage_dozhim_comment"
        return True, ""

    conversation_stage_count = sum(
        1
        for value in (
            lpr_stage,
            need_stage,
            presentation_stage,
            closing_stage,
            objections_stage,
            speech_stage,
        )
        if value
    )
    if conversation_stage_count < 1:
        return False, "llm_missing_stage_comment_coverage"
    return True, ""


def _prepare_call_review_llm_fields(
    *,
    cfg: DealAnalyzerConfig,
    logger: Any,
    llm_runtime: dict[str, Any] | None,
    style_source_excerpt: str,
    period_deal_records: list[dict[str, Any]],
    analysis_shortlist_payload: dict[str, Any],
    step_artifacts_root: Path | None = None,
) -> dict[str, Any]:
    runtime = llm_runtime or {}
    selected_runtime = str(runtime.get("selected") or "none")
    selected_items = (
        analysis_shortlist_payload.get("selected_items", [])
        if isinstance(analysis_shortlist_payload.get("selected_items"), list)
        else []
    )
    by_deal: dict[str, dict[str, Any]] = {}
    for item in selected_items:
        if not isinstance(item, dict):
            continue
        did = str(item.get("deal_id") or "").strip()
        if did:
            by_deal[did] = item

    generated = 0
    failed = 0
    skipped = 0
    skipped_reasons: Counter[str] = Counter()
    llm_sources: Counter[str] = Counter()
    failed_steps: Counter[str] = Counter()
    step_artifacts_written = 0

    if step_artifacts_root is not None:
        try:
            step_artifacts_root.mkdir(parents=True, exist_ok=True)
        except Exception:
            step_artifacts_root = None

    for record in period_deal_records:
        if not isinstance(record, dict):
            continue
        did = str(record.get("deal_id") or "").strip()
        if not did:
            continue
        candidate = by_deal.get(did, {})
        case_mode = _normalize_call_review_case_mode(candidate=candidate, record=record)
        record["call_review_case_mode"] = case_mode
        if not _call_review_case_has_meaningful_conversation(case_mode=case_mode, candidate=candidate, record=record):
            reason = "not_meaningful_conversation_case"
            if case_mode == "redial_discipline_analysis":
                reason = "discipline_case_disabled"
            elif case_mode == "skip_no_meaningful_case":
                reason = "skip_no_meaningful_case"
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = reason
            skipped += 1
            skipped_reasons[reason] += 1
            continue

        if selected_runtime not in {"main", "fallback"}:
            reason = "no_live_llm_runtime"
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = reason
            skipped += 1
            skipped_reasons[reason] += 1
            continue

        artifacts: dict[str, str] = {}
        source_by_step: dict[str, str] = {}
        row_artifacts_dir = None
        if step_artifacts_root is not None:
            row_artifacts_dir = step_artifacts_root / _safe_slug(f"deal_{did}_{case_mode}")[:120]

        role_display = _manager_role_label(str(record.get("owner_name") or ""), cfg=cfg)
        base_mix = build_base_mix_text_priority([record])
        factual_payload = {
            "deal_id": did,
            "deal_name": str(record.get("deal_name") or ""),
            "company_name": str(record.get("company_name") or ""),
            "manager_name": str(record.get("owner_name") or ""),
            "role": role_display,
            "case_mode": case_mode,
            "product_focus": str(record.get("product_hypothesis") or ""),
            "base_mix": str(base_mix or ""),
            "status": str(record.get("status_name") or ""),
            "pipeline": str(record.get("pipeline_name") or ""),
            "call_summary": str(record.get("call_signal_summary_short") or ""),
            "transcript_excerpt": str(record.get("transcript_text_excerpt") or ""),
            "transcript_label": str(record.get("transcript_usability_label") or ""),
            "transcript_score": int(record.get("transcript_usability_score_final", 0) or 0),
            "call_signal_next_step_present": bool(record.get("call_signal_next_step_present")),
            "call_signal_decision_maker_reached": bool(record.get("call_signal_decision_maker_reached")),
            "call_signal_demo_discussed": bool(record.get("call_signal_demo_discussed")),
            "call_signal_objection_price": bool(record.get("call_signal_objection_price")),
            "call_signal_objection_no_need": bool(record.get("call_signal_objection_no_need")),
            "call_signal_objection_not_target": bool(record.get("call_signal_objection_not_target")),
            "crm_consistency_summary": str(record.get("crm_consistency_summary") or ""),
            "analysis_confidence": str(record.get("analysis_confidence") or ""),
            "selected_call_count": int(candidate.get("selected_call_count", 0) or record.get("selected_call_count", 0) or 0),
        }
        reference_stack = build_daily_reference_stack(cfg=cfg, factual_payload=factual_payload, logger=logger)
        factual_payload["reference_block"] = build_reference_prompt_section(reference_stack)

        if row_artifacts_dir is not None:
            artifacts["01_factual_payload"] = _write_step_artifact(row_artifacts_dir / "01_factual_payload.json", factual_payload)
            step_artifacts_written += 1

        free_messages = build_call_review_free_form_messages(
            factual_payload=factual_payload,
            style_source_excerpt=style_source_excerpt,
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        free_text, free_source = _llm_chat_text_with_runtime(
            runtime=runtime,
            messages=free_messages,
            logger=logger,
            log_prefix=f"call review step_free deal={did}",
        )
        if not free_text:
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = "free_form_generation_failed"
            if row_artifacts_dir is not None:
                artifacts["02_free_form_error"] = _write_step_artifact(
                    row_artifacts_dir / "02_free_form_error.json",
                    {"error": free_source or "free_form_generation_failed"},
                )
                step_artifacts_written += 1
            failed += 1
            failed_steps["free_form"] += 1
            skipped_reasons["free_form_generation_failed"] += 1
            continue
        source_by_step["free_form"] = str(free_source or "")
        if row_artifacts_dir is not None:
            artifacts["02_free_form"] = _write_step_artifact(row_artifacts_dir / "02_free_form.md", free_text)
            step_artifacts_written += 1

        effect_messages = build_call_review_effect_messages(
            factual_payload=factual_payload,
            free_form_text=str(free_text),
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        effect_text, effect_source = _llm_chat_text_with_runtime(
            runtime=runtime,
            messages=effect_messages,
            logger=logger,
            log_prefix=f"call review step_effect deal={did}",
        )
        if not effect_text:
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = "effect_layer_generation_failed"
            if row_artifacts_dir is not None:
                artifacts["03_effect_error"] = _write_step_artifact(
                    row_artifacts_dir / "03_effect_error.json",
                    {"error": effect_source or "effect_layer_generation_failed"},
                )
                step_artifacts_written += 1
            failed += 1
            failed_steps["effect_layer"] += 1
            skipped_reasons["effect_layer_generation_failed"] += 1
            continue
        source_by_step["effect_layer"] = str(effect_source or "")
        if row_artifacts_dir is not None:
            artifacts["03_effect_layer"] = _write_step_artifact(row_artifacts_dir / "03_effect_layer.md", effect_text)
            step_artifacts_written += 1

        factual_payload_structured = dict(factual_payload)
        factual_payload_structured["free_analysis_text"] = str(free_text)
        factual_payload_structured["effect_layer_text"] = str(effect_text)
        messages = build_call_review_case_messages(
            factual_payload=factual_payload_structured,
            style_source_excerpt=style_source_excerpt,
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        repaired = append_call_review_case_json_repair_instruction(messages)
        payload, source = _llm_chat_json_with_runtime(
            runtime=runtime,
            messages=messages,
            repair_messages=repaired,
            logger=logger,
            log_prefix=f"call review step_structured deal={did}",
        )
        if not isinstance(payload, dict):
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = "structured_generation_failed"
            if row_artifacts_dir is not None:
                artifacts["04_structured_error"] = _write_step_artifact(
                    row_artifacts_dir / "04_structured_error.json",
                    {"error": source or "structured_generation_failed"},
                )
                step_artifacts_written += 1
            failed += 1
            failed_steps["structured_json"] += 1
            skipped_reasons["structured_generation_failed"] += 1
            continue
        source_by_step["structured_json"] = str(source or "")
        fields = _sanitize_call_review_llm_fields(payload)
        if row_artifacts_dir is not None:
            artifacts["04_structured_json"] = _write_step_artifact(row_artifacts_dir / "04_structured_json.json", fields)
            step_artifacts_written += 1

        style_messages = build_call_review_style_json_messages(
            structured_json_payload=fields,
            style_source_excerpt=style_source_excerpt,
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        styled_payload, styled_source = _llm_chat_json_with_runtime(
            runtime=runtime,
            messages=style_messages,
            repair_messages=append_call_review_case_json_repair_instruction(style_messages),
            logger=logger,
            log_prefix=f"call review step_style deal={did}",
        )
        styled_fields = fields
        if isinstance(styled_payload, dict):
            styled_fields = _sanitize_call_review_llm_fields(styled_payload)
            source_by_step["style_rewrite"] = str(styled_source or "")
            if row_artifacts_dir is not None:
                artifacts["05_style_json"] = _write_step_artifact(row_artifacts_dir / "05_style_json.json", styled_fields)
                step_artifacts_written += 1

        ready, reason = _call_review_llm_fields_ready(fields=styled_fields, case_mode=case_mode)
        if not ready:
            record["call_review_llm_ready"] = False
            record["call_review_llm_error"] = reason or "llm_fields_incomplete"
            if row_artifacts_dir is not None:
                artifacts["06_validation_error"] = _write_step_artifact(
                    row_artifacts_dir / "06_validation_error.json",
                    {"error": reason or "llm_fields_incomplete", "fields": styled_fields},
                )
                step_artifacts_written += 1
            failed += 1
            failed_steps["validation"] += 1
            skipped_reasons[reason or "llm_fields_incomplete"] += 1
            continue

        if row_artifacts_dir is not None:
            artifacts["06_final_assemble"] = _write_step_artifact(
                row_artifacts_dir / "06_final_assemble.json",
                {"source_of_truth": "styled_blocks", "assembler_only": True, "fields": styled_fields},
            )
            step_artifacts_written += 1

        record["call_review_llm_ready"] = True
        record["call_review_llm_source"] = str(source_by_step.get("style_rewrite") or source_by_step.get("structured_json") or source or "")
        record["call_review_llm_error"] = ""
        record["call_review_llm_fields"] = styled_fields
        record["call_review_llm_provenance"] = {
            "source": str(source or ""),
            "source_by_step": source_by_step,
            "step_artifacts": artifacts,
            "source_of_truth": "styled_blocks",
            "assembler_only": True,
            "style_layer_applied": True,
            "reference_sources_count": int(reference_stack.get("prompt_snippets_count", 0) or 0),
            "reference_required_layers": (
                reference_stack.get("required_layers", {})
                if isinstance(reference_stack.get("required_layers"), dict)
                else {}
            ),
            "reference_sources_used": [
                str(x.get("source") or "")
                for x in (reference_stack.get("prompt_snippets", []) if isinstance(reference_stack.get("prompt_snippets"), list) else [])
                if isinstance(x, dict) and str(x.get("source") or "").strip()
            ],
        }
        generated += 1
        llm_sources[str(source_by_step.get("style_rewrite") or source_by_step.get("structured_json") or source or "unknown")] += 1

    return {
        "selected_runtime": selected_runtime,
        "generated_rows": generated,
        "failed_rows": failed,
        "skipped_rows": skipped,
        "skip_reasons": dict(skipped_reasons),
        "llm_sources": dict(llm_sources),
        "failed_steps": dict(failed_steps),
        "step_artifacts_written": int(step_artifacts_written),
        "step_artifacts_root": str(step_artifacts_root) if step_artifacts_root is not None else "",
    }


def _analyze_period_rows(
    normalized_rows: list[dict[str, Any]], cfg: DealAnalyzerConfig, logger, *, backend_override: str
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    analyses: list[dict[str, Any]] = []
    counts = {
        "llm_success_count": 0,
        "llm_success_repaired_count": 0,
        "llm_fallback_count": 0,
        "llm_error_count": 0,
    }

    for idx, row in enumerate(normalized_rows):
        deal_hint = str(row.get("deal_id") or row.get("amo_lead_id") or idx)
        analysis, row_counts = _analyze_one_with_isolation(
            row,
            cfg,
            logger,
            deal_hint=deal_hint,
            backend_override=backend_override,
        )
        analyses.append(analysis)
        counts["llm_success_count"] += row_counts["llm_success_count"]
        counts["llm_success_repaired_count"] += row_counts["llm_success_repaired_count"]
        counts["llm_fallback_count"] += row_counts["llm_fallback_count"]
        counts["llm_error_count"] += row_counts["llm_error_count"]

    return analyses, counts


def _analyze_one_with_isolation(
    normalized: dict[str, Any],
    cfg: DealAnalyzerConfig,
    logger,
    *,
    deal_hint: str,
    backend_override: str | None = None,
    snapshot: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, int]]:
    counts = {
        "llm_success_count": 0,
        "llm_success_repaired_count": 0,
        "llm_fallback_count": 0,
        "llm_error_count": 0,
    }

    effective_backend = backend_override or cfg.analyzer_backend

    if effective_backend == "rules":
        analysis = analyze_deal(normalized, cfg).to_dict()
        analysis["analysis_backend_requested"] = cfg.analyzer_backend
        analysis["analysis_backend_used"] = "rules"
        analysis["llm_repair_applied"] = False
        analysis["backend"] = cfg.analyzer_backend
        analysis = _attach_enrichment_and_operator_outputs(analysis, normalized, cfg, snapshot=snapshot)
        logger.info("llm overlay skipped: deal=%s backend=rules", deal_hint)
        return analysis, counts

    if effective_backend not in {"ollama", "hybrid"}:
        raise RuntimeError(f"Unsupported analyzer backend: {effective_backend}")

    logger.info(
        "%s analyze call: deal=%s model=%s base_url=%s timeout_seconds=%s",
        effective_backend,
        deal_hint,
        cfg.ollama_model,
        cfg.ollama_base_url,
        cfg.ollama_timeout_seconds,
    )

    try:
        if effective_backend == "hybrid":
            outcome = analyze_deal_with_hybrid_outcome(normalized_deal=normalized, config=cfg)
        else:
            outcome = analyze_deal_with_ollama_outcome(normalized_deal=normalized, config=cfg)
        analysis = outcome.analysis.to_dict()
        analysis["analysis_backend_requested"] = cfg.analyzer_backend
        analysis["backend"] = cfg.analyzer_backend
        if outcome.backend_used in {"ollama", "hybrid"}:
            counts["llm_success_count"] += 1
            if outcome.repaired:
                counts["llm_success_repaired_count"] += 1
        else:
            counts["llm_fallback_count"] += 1
        if outcome.llm_error:
            counts["llm_error_count"] += 1
            logger.warning("%s fallback used: deal=%s reason=%s", effective_backend, deal_hint, outcome.error_message)
        analysis = _attach_enrichment_and_operator_outputs(analysis, normalized, cfg, snapshot=snapshot)
        overlay_fields = _llm_overlay_fields_filled(analysis)
        logger.info(
            "llm overlay result: deal=%s backend_requested=%s backend_used=%s overlay_fields_filled=%s overlay_keys=%s",
            deal_hint,
            cfg.analyzer_backend,
            analysis.get("analysis_backend_used", ""),
            len(overlay_fields),
            ",".join(overlay_fields) if overlay_fields else "-",
        )
        return analysis, counts
    except Exception as exc:  # hard isolation for batch path
        logger.warning("%s analyze failed, fallback to rules: deal=%s error=%s", effective_backend, deal_hint, exc)
        fallback = analyze_deal(normalized, cfg).to_dict()
        fallback["analysis_backend_requested"] = cfg.analyzer_backend
        fallback["analysis_backend_used"] = "rules_fallback"
        fallback["llm_repair_applied"] = False
        fallback["llm_error"] = True
        fallback["llm_fallback"] = True
        fallback["backend"] = cfg.analyzer_backend
        fallback = _attach_enrichment_and_operator_outputs(fallback, normalized, cfg, snapshot=snapshot)
        counts["llm_fallback_count"] += 1
        counts["llm_error_count"] += 1
        logger.warning("llm overlay fallback to rules: deal=%s backend_requested=%s", deal_hint, cfg.analyzer_backend)
        return fallback, counts


def _attach_enrichment_and_operator_outputs(
    analysis: dict[str, Any],
    normalized_deal: dict[str, Any],
    cfg: DealAnalyzerConfig,
    *,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = dict(analysis)
    enrichment_keys = (
        "enrichment_match_status",
        "enrichment_match_source",
        "enrichment_confidence",
        "matched_client_list_row_id",
        "matched_appointment_row_id",
        "matched_client_row_ref",
        "matched_appointment_row_ref",
        "enriched_test_started",
        "enriched_test_completed",
        "enriched_test_status",
        "enriched_test_comments",
        "enriched_appointment_date",
        "enriched_assigned_by",
        "enriched_conducted_by",
        "enriched_meeting_status",
        "enriched_transfer_cancel_flag",
    )
    for key in enrichment_keys:
        out[key] = normalized_deal.get(key, out.get(key, ""))

    if cfg.operator_outputs_enabled:
        out.update(build_operator_outputs(deal=normalized_deal, analysis=out))
    else:
        out.setdefault("manager_summary", "")
        out.setdefault("employee_coaching", "")
        out.setdefault("employee_fix_tasks", [])
    out.update(derive_transcript_signals(deal=normalized_deal, snapshot=snapshot))
    out.update(_derive_transcript_meta(snapshot=snapshot))
    out.update(_derive_reanimation_fields(analysis=out, deal=normalized_deal))
    out.update(_derive_product_hypothesis(analysis=out, deal=normalized_deal, snapshot=snapshot))
    out = _apply_transcript_signal_overlays(analysis=out, deal=normalized_deal)
    llm_reanimation_reason = " ".join(str(out.get("reanimation_reason_short_llm") or "").strip().split())
    if llm_reanimation_reason:
        out["reanimation_reason_short"] = llm_reanimation_reason
    out.setdefault("product_hypothesis_llm", "unknown")
    out.setdefault("reanimation_reason_short_llm", "")
    return out


def _build_backend_effective_summary(
    llm_counts: dict[str, int], backend_requested: str, preflight_forced_rules: bool = False
) -> str:
    if backend_requested == "rules":
        return "rules_only"
    if backend_requested == "hybrid":
        if preflight_forced_rules:
            return "hybrid_preflight_failed_rules_only"
        success = int(llm_counts.get("llm_success_count", 0))
        fallback = int(llm_counts.get("llm_fallback_count", 0))
        if success > 0 and fallback == 0:
            return "hybrid_with_llm_for_all"
        if success > 0 and fallback > 0:
            return "hybrid_with_partial_llm_fallback"
        return "hybrid_requested_rules_only_fallback"
    if preflight_forced_rules:
        return "ollama_preflight_failed_all_rules_fallback"
    success = int(llm_counts.get("llm_success_count", 0))
    fallback = int(llm_counts.get("llm_fallback_count", 0))
    if success > 0 and fallback == 0:
        return "ollama_only"
    if success > 0 and fallback > 0:
        return "ollama_with_partial_rules_fallback"
    return "ollama_requested_all_rules_fallback"


def _public_metadata(cfg: DealAnalyzerConfig, metadata: AnalysisRunMetadata) -> dict[str, Any]:
    include_executed = cfg.executed_at_visibility == "public" and not cfg.hide_executed_at_from_public_exports
    return metadata.to_public_dict(include_executed_at=include_executed)


def _attach_metadata(analysis: dict[str, Any], public_meta: dict[str, Any]) -> dict[str, Any]:
    row = dict(analysis)
    row.update(public_meta)
    return row


def _maybe_enrich_rows(rows: list[dict[str, Any]], cfg: DealAnalyzerConfig, logger) -> list[dict[str, Any]]:
    if not rows:
        return []
    try:
        return enrich_rows(rows, config=cfg, logger=logger)
    except Exception as exc:
        logger.warning("deal analyzer enrich failed, continuing without enrich: %s", exc)
        out: list[dict[str, Any]] = []
        for row in rows:
            cloned = dict(row)
            cloned.setdefault("enrichment_match_status", "error")
            cloned.setdefault("enrichment_match_source", "none")
            cloned.setdefault("enrichment_confidence", 0.0)
            cloned.setdefault("matched_client_list_row_id", "")
            cloned.setdefault("matched_appointment_row_id", "")
            cloned.setdefault("matched_client_row_ref", "")
            cloned.setdefault("matched_appointment_row_ref", "")
            out.append(cloned)
        return out


def _extract_single_normalized(payload: dict[str, Any] | list[Any]) -> dict[str, Any]:
    if isinstance(payload, dict):
        if isinstance(payload.get("normalized"), dict):
            return payload["normalized"]
        if isinstance(payload.get("analysis"), dict):
            return payload["analysis"]
        if _looks_like_normalized_row(payload):
            return payload
    raise RuntimeError("analyze-deal input does not contain normalized deal payload")


def _extract_period_normalized(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("normalized_deals")
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
        rows = payload.get("analyses")
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    raise RuntimeError("analyze-period input does not contain normalized deals list")


def _find_normalized_by_deal_id(payload: dict[str, Any] | list[Any], deal_id: str) -> dict[str, Any]:
    wanted = str(deal_id).strip()
    if not wanted:
        raise RuntimeError("analyze-snapshot --deal-id is empty")
    try:
        rows = _extract_period_normalized(payload)
    except RuntimeError:
        rows = []
    for row in rows:
        if str(row.get("deal_id") or "").strip() == wanted or str(row.get("amo_lead_id") or "").strip() == wanted:
            return row

    one = _extract_single_normalized(payload)
    if str(one.get("deal_id") or "").strip() == wanted or str(one.get("amo_lead_id") or "").strip() == wanted:
        return one
    raise RuntimeError(f"analyze-snapshot deal not found in input: deal_id={wanted}")


def _extract_prepared_snapshot(payload: dict[str, Any] | list[Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    snapshot = payload.get("snapshot")
    if isinstance(snapshot, dict) and isinstance(snapshot.get("crm"), dict):
        return snapshot
    if isinstance(payload.get("crm"), dict) and (
        "snapshot_generated_at" in payload or "call_evidence" in payload or "roks_context" in payload
    ):
        return payload
    return None


def _looks_like_normalized_row(payload: dict[str, Any]) -> bool:
    return any(key in payload for key in ("deal_id", "amo_lead_id", "deal_name", "presentation_detected"))


def _extract_raw_bundle_for_deal(payload: dict[str, Any] | list[Any], deal_id: str) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("raw_bundle"), dict):
        return payload.get("raw_bundle")
    raw_map = _extract_raw_bundles_map(payload)
    return raw_map.get(str(deal_id or ""))


def _extract_raw_bundles_map(payload: dict[str, Any] | list[Any]) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    candidates = payload.get("raw_bundles")
    if isinstance(candidates, dict):
        return {str(k): v for k, v in candidates.items() if isinstance(v, dict)}
    return {}


def _try_live_refresh_period_rows(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    resolved,
    fallback_rows: list[dict[str, Any]],
    fallback_raw_bundles: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, Any]]:
    diag: dict[str, Any] = {
        "mode": "fallback_input_json",
        "api_first_attempted": False,
        "api_refresh_success": False,
        "fallback_used": True,
        "rows_from_api": 0,
        "rows_final": len(fallback_rows),
        "error": "",
    }
    if not bool(getattr(cfg, "period_live_refresh_enabled", True)):
        diag["error"] = "period_live_refresh_disabled"
        logger.info("live refresh skipped: reason=period_live_refresh_disabled")
        return fallback_rows, fallback_raw_bundles, diag
    if str(getattr(cfg, "call_collection_mode", "") or "").strip().lower() == "disabled":
        diag["error"] = "call_collection_mode_disabled"
        logger.info("live refresh skipped: reason=call_collection_mode_disabled")
        return fallback_rows, fallback_raw_bundles, diag

    try:
        auth_cfg = load_amocrm_auth_config(getattr(cfg, "amocrm_auth_config_path", None))
        auth_state = load_auth_state(auth_cfg.state_path)
        access_token = str(getattr(auth_state, "access_token", "") or "").strip()
        base_domain = str(getattr(cfg, "call_base_domain", "") or "").strip() or str(getattr(auth_cfg, "base_domain", "") or "").strip()
        if not base_domain:
            raise RuntimeError("missing_base_domain")
        if not access_token:
            raise RuntimeError("missing_access_token")

        diag["api_first_attempted"] = True
        client = AmoCollectorClient(base_domain=base_domain, access_token=access_token)
        date_from_unix = int(
            datetime(
                resolved.period_start.year,
                resolved.period_start.month,
                resolved.period_start.day,
                0,
                0,
                0,
                tzinfo=timezone.utc,
            ).timestamp()
        )
        date_to_unix = int(
            datetime(
                resolved.period_end.year,
                resolved.period_end.month,
                resolved.period_end.day,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            ).timestamp()
        )

        fetched: list[dict[str, Any]] = []
        page = 1
        users_cache = client.get_users_cache()
        status_cache = client.get_status_cache()
        company_tags_cache: dict[int, list[str]] = {}
        pipeline_names: dict[int, str] = {
            int(p.get("id")): str(p.get("name") or "").strip()
            for p in client.get_pipelines_cache()
            if isinstance(p, dict) and isinstance(p.get("id"), int)
        }
        while True:
            batch = client.get_leads_by_period(
                date_from_unix=date_from_unix,
                date_to_unix=date_to_unix,
                page=page,
                limit=250,
            )
            if not batch:
                break
            fetched.extend(batch)
            if len(batch) < 250:
                break
            page += 1

        existing_by_id: dict[str, dict[str, Any]] = {
            str(row.get("deal_id") or row.get("amo_lead_id") or ""): dict(row)
            for row in fallback_rows
            if isinstance(row, dict)
        }
        refreshed: list[dict[str, Any]] = []
        for lead in fetched:
            did = str(lead.get("id") or "").strip()
            if not did:
                continue
            base = existing_by_id.get(did, {"deal_id": int(did), "amo_lead_id": int(did)})
            pipeline_id = int(lead.get("pipeline_id")) if isinstance(lead.get("pipeline_id"), int) else None
            status_id = int(lead.get("status_id")) if isinstance(lead.get("status_id"), int) else None
            responsible_user_id = int(lead.get("responsible_user_id")) if isinstance(lead.get("responsible_user_id"), int) else None
            responsible_name = ""
            if isinstance(responsible_user_id, int):
                responsible_name = str(users_cache.get(responsible_user_id, {}).get("name") or "").strip()
            status_name = ""
            if isinstance(pipeline_id, int) and isinstance(status_id, int):
                status_name = str(status_cache.get((pipeline_id, status_id), {}).get("name") or "").strip()
            if not status_name:
                status_name = str(base.get("status_name") or "").strip()
            lead_tags = _extract_embedded_tag_names(lead)
            company_tags = _fetch_company_tags_for_lead(
                client=client,
                lead_id=int(did),
                company_tags_cache=company_tags_cache,
            )
            existing_tags = base.get("tags") if isinstance(base.get("tags"), list) else []
            existing_company_tags = base.get("company_tags") if isinstance(base.get("company_tags"), list) else []
            resolved_company_tags = normalize_tag_values(company_tags or existing_company_tags)
            company_tags_source = "api_tags" if company_tags else ("existing_row_company_tags" if existing_company_tags else "none")

            base.update(
                {
                    "deal_id": int(did),
                    "amo_lead_id": int(did),
                    "deal_name": str(lead.get("name") or base.get("deal_name") or "").strip(),
                    "created_at": lead.get("created_at") or base.get("created_at") or "",
                    "updated_at": lead.get("updated_at") or base.get("updated_at") or "",
                    "pipeline_id": pipeline_id if pipeline_id is not None else base.get("pipeline_id"),
                    "pipeline_name": pipeline_names.get(pipeline_id, str(base.get("pipeline_name") or "")),
                    "status_id": status_id if status_id is not None else base.get("status_id"),
                    "status_name": status_name,
                    "responsible_user_id": responsible_user_id if responsible_user_id is not None else base.get("responsible_user_id"),
                    "responsible_user_name": responsible_name or str(base.get("responsible_user_name") or "").strip(),
                    "tags": normalize_tag_values(lead_tags or existing_tags),
                    "company_tags": resolved_company_tags,
                    "company_tags_source": company_tags_source,
                    "company_id": str(base.get("company_id") or base.get("amo_company_id") or "").strip(),
                }
            )
            refreshed.append(base)

        if refreshed:
            diag["mode"] = "live_refresh_amocrm_api"
            diag["api_refresh_success"] = True
            diag["fallback_used"] = False
            diag["rows_from_api"] = len(refreshed)
            diag["rows_final"] = len(refreshed)
            logger.info(
                "live refresh success: source=amocrm_api period=%s..%s rows=%s",
                resolved.period_start.isoformat(),
                resolved.period_end.isoformat(),
                len(refreshed),
            )
            return refreshed, fallback_raw_bundles, diag

        diag["error"] = "live_refresh_empty_batch"
        logger.warning(
            "live refresh fallback: reason=empty_batch period=%s..%s",
            resolved.period_start.isoformat(),
            resolved.period_end.isoformat(),
        )
    except Exception as exc:
        diag["error"] = str(exc)
        logger.warning("live refresh fallback: reason=api_failed error=%s", exc)

    diag["rows_final"] = len(fallback_rows)
    return fallback_rows, fallback_raw_bundles, diag


def _maybe_retry_transcript_quality_for_deal(
    *,
    snapshot: dict[str, Any],
    crm: dict[str, Any],
    cfg: DealAnalyzerConfig,
    logger,
    allow_retry_for_deal: bool,
) -> dict[str, Any]:
    retry_info = {
        "used": False,
        "improved": False,
        "reason": "",
        "before_label": "",
        "after_label": "",
        "before_score": 0,
        "after_score": 0,
        "calls_retried": 0,
        "retry_model": "",
    }
    if not allow_retry_for_deal or not bool(getattr(cfg, "whisper_quality_retry_enabled", False)):
        return retry_info
    try:
        before = derive_transcript_signals(deal=crm, snapshot=snapshot)
        before_label = str(before.get("transcript_usability_label") or "")
        before_score = int(before.get("transcript_usability_score_final", 0) or 0)
        retry_info["before_label"] = before_label
        retry_info["before_score"] = before_score
        if before_label not in {"weak", "noisy", "empty"}:
            retry_info["reason"] = "already_usable"
            return retry_info

        call_items = []
        if isinstance(snapshot.get("call_evidence"), dict):
            items = snapshot.get("call_evidence", {}).get("items")
            if isinstance(items, list):
                call_items = [x for x in items if isinstance(x, dict)]
        candidates = [
            c
            for c in call_items
            if str(c.get("audio_path") or "").strip() or str(c.get("recording_url") or "").strip()
        ]
        if not candidates:
            retry_info["reason"] = "no_audio_candidates"
            return retry_info
        candidates.sort(key=lambda x: int(x.get("duration_seconds", 0) or 0), reverse=True)
        shortlist = candidates[:3]
        retry_info["used"] = True
        retry_info["calls_retried"] = len(shortlist)
        retry_info["retry_model"] = str(getattr(cfg, "whisper_quality_retry_model_name", "") or "")
        logger.info(
            "transcript quality retry: deal=%s calls=%s model=%s",
            crm.get("deal_id") or crm.get("amo_lead_id") or "",
            len(shortlist),
            retry_info["retry_model"],
        )
        retry_cfg = replace(
            cfg,
            whisper_model_name=str(getattr(cfg, "whisper_quality_retry_model_name", "") or getattr(cfg, "whisper_model_name", "")),
            transcription_timeout_seconds=int(
                getattr(cfg, "whisper_quality_retry_timeout_seconds", getattr(cfg, "transcription_timeout_seconds", 60))
                or getattr(cfg, "transcription_timeout_seconds", 60)
            ),
        )
        retried = transcribe_call_evidence(calls=shortlist, config=retry_cfg, logger=logger)
        current_transcripts = snapshot.get("transcripts") if isinstance(snapshot.get("transcripts"), list) else []
        by_call = {
            str(t.get("call_id") or ""): t
            for t in current_transcripts
            if isinstance(t, dict) and str(t.get("call_id") or "").strip()
        }
        for item in retried:
            if not isinstance(item, dict):
                continue
            cid = str(item.get("call_id") or "").strip()
            if not cid:
                continue
            prev = by_call.get(cid, {})
            prev_text_len = len(str(prev.get("transcript_text") or ""))
            cur_text_len = len(str(item.get("transcript_text") or ""))
            if str(item.get("transcript_status") or "") in {"ok", "cached"} and cur_text_len >= prev_text_len:
                by_call[cid] = item
        snapshot["transcripts"] = list(by_call.values()) if by_call else retried
        after = derive_transcript_signals(deal=crm, snapshot=snapshot)
        after_label = str(after.get("transcript_usability_label") or "")
        after_score = int(after.get("transcript_usability_score_final", 0) or 0)
        retry_info["after_label"] = after_label
        retry_info["after_score"] = after_score
        retry_info["improved"] = (after_score > before_score) or (
            before_label in {"empty", "weak", "noisy"} and after_label == "usable"
        )
        retry_info["reason"] = "improved" if retry_info["improved"] else "not_improved"
        logger.info(
            "transcript quality retry result: deal=%s improved=%s before=%s/%s after=%s/%s",
            crm.get("deal_id") or crm.get("amo_lead_id") or "",
            retry_info["improved"],
            before_label,
            before_score,
            after_label,
            after_score,
        )
        return retry_info
    except Exception as exc:
        retry_info["reason"] = "retry_failed"
        logger.warning(
            "transcript quality retry failed: deal=%s error=%s",
            crm.get("deal_id") or crm.get("amo_lead_id") or "",
            exc,
        )
        return retry_info


def _period_deal_priority_key(row: dict[str, Any]) -> tuple[int, int, int, int, str]:
    call_priority = 0
    if bool(row.get("long_call_detected")) or int(row.get("longest_call_duration_seconds", 0) or 0) > 0:
        call_priority = 2
    elif str(row.get("brief_url") or "").strip() or str(row.get("demo_result_text") or "").strip():
        call_priority = 1

    context_priority = 0
    if isinstance(row.get("notes_summary_raw"), list) and row.get("notes_summary_raw"):
        context_priority += 1
    if isinstance(row.get("tasks_summary_raw"), list) and row.get("tasks_summary_raw"):
        context_priority += 1
    if str(row.get("company_comment") or "").strip() or str(row.get("contact_comment") or "").strip():
        context_priority += 1

    updated_score = 0
    updated_raw = row.get("updated_at")
    if isinstance(updated_raw, (int, float)):
        updated_score = int(updated_raw)
    else:
        text = str(updated_raw or "").strip()
        if text.isdigit():
            updated_score = int(text)

    did = str(row.get("deal_id") or row.get("amo_lead_id") or "")
    return (-call_priority, -context_priority, -updated_score, 0 if did else 1, did)


def _prepare_period_run_dirs(*, output_dir: Path, run_started_at: datetime) -> tuple[Path, Path]:
    run_id = run_started_at.strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / "period_runs" / run_id
    deals_dir = run_dir / "deals"
    deals_dir.mkdir(parents=True, exist_ok=True)
    return run_dir, deals_dir


def _deal_artifact_filename(*, analysis: dict[str, Any], index: int) -> str:
    deal_raw = analysis.get("deal_id") or analysis.get("amo_lead_id") or f"idx_{index + 1}"
    deal_text = "".join(ch if ch.isalnum() else "_" for ch in str(deal_raw))
    deal_text = deal_text.strip("_") or f"idx_{index + 1}"
    return f"deal_{deal_text}.json"


def _write_json_path(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_period_run_summary(
    *,
    run_started_at: datetime,
    backend_requested: str,
    call_collection_mode: str,
    analyses: list[dict[str, Any]],
    deal_artifact_paths: list[str],
    total_deals_seen: int,
    total_deals_analyzed: int,
    deals_failed: int,
    limit: int | None,
    period_deal_records: list[dict[str, Any]],
    call_pool_debug: dict[str, Any] | None = None,
    call_pool_aggregates: dict[str, Any] | None = None,
    transcription_shortlist_diagnostics: dict[str, Any] | None = None,
    discipline_report_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    backend_used_counts = Counter(str(item.get("analysis_backend_used") or "unknown") for item in analyses)
    score_values = [int(item.get("score_0_100")) for item in analyses if isinstance(item.get("score_0_100"), int)]
    risk_counter: Counter[str] = Counter()
    for item in analyses:
        flags = item.get("risk_flags")
        if isinstance(flags, list):
            for flag in flags:
                text = str(flag).strip()
                if text:
                    risk_counter[text] += 1
    analysis_confidence_counter: Counter[str] = Counter()
    owner_ambiguity_count = 0
    for item in analyses:
        confidence = str(item.get("analysis_confidence") or "").strip().lower() or "unknown"
        analysis_confidence_counter[confidence] += 1
        if bool(item.get("owner_ambiguity_flag")):
            owner_ambiguity_count += 1

    avg_score = round(sum(score_values) / len(score_values), 2) if score_values else None
    analysis_backend_used = (
        next(iter(backend_used_counts))
        if len(backend_used_counts) == 1
        else "mixed"
    )
    llm_overlay_deals = sum(1 for item in analyses if _llm_overlay_fields_filled(item))
    call_aggregates = build_call_signal_aggregates(period_deal_records)
    transcript_runtime_diagnostics = _build_transcript_runtime_diagnostics(period_deal_records)
    call_runtime_diagnostics = _build_call_runtime_diagnostics(
        period_deal_records,
        call_collection_mode=call_collection_mode,
    )
    call_pool_debug = call_pool_debug if isinstance(call_pool_debug, dict) else {}
    call_pool_aggregates = call_pool_aggregates if isinstance(call_pool_aggregates, dict) else {}
    transcription_shortlist_diagnostics = (
        transcription_shortlist_diagnostics
        if isinstance(transcription_shortlist_diagnostics, dict)
        else {}
    )
    discipline_report_summary = discipline_report_summary if isinstance(discipline_report_summary, dict) else {}
    return {
        "run_timestamp": run_started_at.isoformat(),
        "backend_requested": backend_requested,
        "analysis_backend_used": analysis_backend_used,
        "analysis_backend_used_counts": dict(backend_used_counts),
        "total_deals_seen": total_deals_seen,
        "total_deals_analyzed": total_deals_analyzed,
        "deals_failed": deals_failed,
        "limit": limit,
        "artifact_paths": deal_artifact_paths,
        "score_aggregates": {
            "min": min(score_values) if score_values else None,
            "max": max(score_values) if score_values else None,
            "avg": avg_score,
        },
        "risk_flags_counts": dict(risk_counter),
        "analysis_confidence_counts": dict(analysis_confidence_counter),
        "owner_ambiguity_deals": owner_ambiguity_count,
        "llm_overlay_deals": llm_overlay_deals,
        "call_signal_aggregates": call_aggregates,
        "transcript_runtime_diagnostics": transcript_runtime_diagnostics,
        "call_runtime_diagnostics": call_runtime_diagnostics,
        "deals_total_before_limit": int(call_pool_debug.get("deals_total_before_limit", 0) or 0),
        "deals_with_any_calls": int(call_pool_debug.get("deals_with_any_calls", 0) or 0),
        "deals_with_recordings": int(call_pool_debug.get("deals_with_recordings", 0) or 0),
        "deals_with_long_calls": int(call_pool_debug.get("deals_with_long_calls", 0) or 0),
        "deals_with_only_short_calls": int(call_pool_debug.get("deals_with_only_short_calls", 0) or 0),
        "deals_with_autoanswer_pattern": int(call_pool_debug.get("deals_with_autoanswer_pattern", 0) or 0),
        "deals_with_redial_pattern": int(call_pool_debug.get("deals_with_redial_pattern", 0) or 0),
        "conversation_pool_total": int(call_pool_aggregates.get("conversation_pool_total", 0) or 0),
        "discipline_pool_total": int(call_pool_aggregates.get("discipline_pool_total", 0) or 0),
        "lpr_conversation_total": int(call_pool_aggregates.get("lpr_conversation_total", 0) or 0),
        "secretary_case_total": int(call_pool_aggregates.get("secretary_case_total", 0) or 0),
        "supplier_inbound_total": int(call_pool_aggregates.get("supplier_inbound_total", 0) or 0),
        "warm_inbound_total": int(call_pool_aggregates.get("warm_inbound_total", 0) or 0),
        "redial_discipline_total": int(call_pool_aggregates.get("redial_discipline_total", 0) or 0),
        "autoanswer_noise_total": int(call_pool_aggregates.get("autoanswer_noise_total", 0) or 0),
        "transcription_shortlist_diagnostics": transcription_shortlist_diagnostics,
        "discipline_report_summary": discipline_report_summary,
    }


def _build_transcript_runtime_diagnostics(period_deal_records: list[dict[str, Any]]) -> dict[str, Any]:
    deals = [x for x in period_deal_records if isinstance(x, dict)]
    deals_with_any_call_evidence = sum(
        1
        for x in deals
        if int(x.get("call_evidence_calls_total", 0) or 0) > 0
        or int(x.get("call_evidence_items_count", 0) or 0) > 0
    )
    deals_with_audio_path = sum(1 for x in deals if bool(x.get("has_audio_path")))
    deals_with_transcript_text = sum(1 for x in deals if bool(x.get("has_transcript_text")))
    deals_with_transcript_excerpt = sum(1 for x in deals if str(x.get("transcript_text_excerpt") or "").strip())
    deals_with_nonempty_call_signal_summary = sum(1 for x in deals if str(x.get("call_signal_summary_short") or "").strip())
    deals_with_transcription_error = sum(1 for x in deals if str(x.get("transcript_error") or "").strip())
    labels = [str(x.get("transcript_usability_label") or "").strip().lower() for x in deals]
    transcriptions_usable = sum(1 for label in labels if label == "usable")
    transcriptions_weak = sum(1 for label in labels if label == "weak")
    transcriptions_noisy = sum(1 for label in labels if label == "noisy")
    transcriptions_empty = sum(1 for label in labels if label == "empty")
    deals_with_usable_transcript = sum(
        1 for x in deals if str(x.get("transcript_usability_label") or "").strip().lower() == "usable"
    )
    transcript_layer_effective = bool(
        deals_with_transcript_excerpt > 0 or deals_with_nonempty_call_signal_summary > 0
    )
    return {
        "deals_with_any_call_evidence": deals_with_any_call_evidence,
        "deals_with_audio_path": deals_with_audio_path,
        "deals_with_transcript_text": deals_with_transcript_text,
        "deals_with_transcript_excerpt": deals_with_transcript_excerpt,
        "deals_with_nonempty_call_signal_summary": deals_with_nonempty_call_signal_summary,
        "deals_with_transcription_error": deals_with_transcription_error,
        "transcriptions_usable": transcriptions_usable,
        "transcriptions_weak": transcriptions_weak,
        "transcriptions_noisy": transcriptions_noisy,
        "transcriptions_empty": transcriptions_empty,
        "deals_with_usable_transcript": deals_with_usable_transcript,
        "transcript_layer_effective": transcript_layer_effective,
    }


def _transcript_error_counters(transcripts: list[dict[str, Any]]) -> dict[str, int]:
    missing_audio = 0
    backend_config = 0
    for item in transcripts:
        if not isinstance(item, dict):
            continue
        status = str(item.get("transcript_status") or "").strip().lower()
        err = str(item.get("transcript_error") or "").strip().lower()
        if status in {"missing_audio_file", "missing_recording"} or "audio_path_not_found" in err:
            missing_audio += 1
        if status in {"backend_unavailable", "not_configured"} or "import_failed" in err or "model" in err:
            backend_config += 1
    return {
        "missing_audio": missing_audio,
        "backend_config": backend_config,
    }


def _derive_call_history_pattern(snapshot: dict[str, Any]) -> dict[str, Any]:
    calls = []
    if isinstance(snapshot.get("call_evidence"), dict):
        items = snapshot.get("call_evidence", {}).get("items")
        if isinstance(items, list):
            calls = [x for x in items if isinstance(x, dict)]
    if not calls:
        return {
            "call_history_pattern_dead_redials": False,
            "call_history_pattern_score": 0,
            "call_history_pattern_label": "none",
            "call_history_pattern_summary": "",
        }
    outbound = [c for c in calls if str(c.get("direction") or "").lower() == "outbound"]
    short_or_empty = [
        c
        for c in outbound
        if int(c.get("duration_seconds", 0) or 0) <= 20
        or "missing_recording" in (c.get("quality_flags") or [])
    ]
    dead_redials = len(outbound) >= 3 and len(short_or_empty) >= max(2, int(len(outbound) * 0.7))
    pattern_score = min(100, len(short_or_empty) * 18 + (20 if dead_redials else 0))
    label = "dead_redials" if dead_redials else ("weak_attempts" if short_or_empty else "none")
    summary = ""
    if dead_redials:
        summary = "повторяются пустые перезвоны/недозвоны, полезного контакта мало"
    elif short_or_empty:
        summary = "много коротких или пустых касаний без содержательного разговора"
    return {
        "call_history_pattern_dead_redials": dead_redials,
        "call_history_pattern_score": pattern_score,
        "call_history_pattern_label": label,
        "call_history_pattern_summary": summary,
    }


def _normalize_phone_last7(raw: Any) -> str:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(digits) < 7:
        return ""
    return digits[-7:]


def _is_attempt_like_status(status_value: str) -> bool:
    low = str(status_value or "").strip().lower()
    if not low:
        return False
    markers = (
        "no_answer",
        "busy",
        "voicemail",
        "autoanswer",
        "auto_answer",
        "auto",
        "автоответ",
        "недозвон",
        "ring",
        "secretary",
        "секрет",
        "lpr",
        "липр",
        "connected",
        "answered",
        "answer",
        "drop",
        "hang",
    )
    return any(marker in low for marker in markers)


def _collect_known_phone_candidates(payload: Any) -> set[str]:
    phones: set[str] = set()
    stack: list[Any] = [payload]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            for key, item in value.items():
                key_low = str(key or "").strip().lower()
                if isinstance(item, (dict, list, tuple)):
                    stack.append(item)
                    continue
                if "phone" not in key_low and key_low not in {"contact", "mobile", "tel"}:
                    continue
                phone7 = _normalize_phone_last7(item)
                if phone7:
                    phones.add(phone7)
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, (dict, list, tuple)):
                    stack.append(item)
                    continue
                phone7 = _normalize_phone_last7(item)
                if phone7:
                    phones.add(phone7)
    return phones


def _extract_hhmm_and_day(ts: str) -> tuple[str, str]:
    value = str(ts or "").strip()
    if not value:
        return "", ""
    m_hhmm = re.search(r"[T\s](\d{2}:\d{2})", value)
    m_day = re.search(r"(\d{4}-\d{2}-\d{2})", value)
    return (m_hhmm.group(1) if m_hhmm else "", m_day.group(1) if m_day else "")


def _build_dial_discipline_signals(snapshot: dict[str, Any], *, status_name: str = "") -> dict[str, Any]:
    call_evidence = snapshot.get("call_evidence", {}) if isinstance(snapshot, dict) else {}
    items = call_evidence.get("items", []) if isinstance(call_evidence, dict) and isinstance(call_evidence.get("items"), list) else []
    normalized_deal = snapshot.get("normalized_deal", {}) if isinstance(snapshot, dict) else {}
    known_phones = _collect_known_phone_candidates(normalized_deal if isinstance(normalized_deal, dict) else {})
    attempts_by_phone: dict[str, list[dict[str, Any]]] = {}
    same_time_counter: Counter[str] = Counter()
    same_day_counter: Counter[str] = Counter()
    diff_day_same_time_counter: Counter[str] = Counter()
    times_by_phone: dict[str, set[str]] = {}
    days_by_phone: dict[str, set[str]] = {}
    secretary_touch_count = 0
    autoanswer_attempt_count = 0
    no_answer_attempt_count = 0
    short_drop_attempt_count = 0
    unknown_phone_attempts_count = 0
    for raw in items:
        if not isinstance(raw, dict):
            continue
        phone_raw = raw.get("phone") or raw.get("phone_number") or raw.get("contact_phone") or ""
        phone7 = _normalize_phone_last7(phone_raw)
        status_low = str(raw.get("status") or raw.get("result") or raw.get("disposition") or "").strip().lower()
        if not _is_attempt_like_status(status_low):
            duration = int(raw.get("duration_seconds", 0) or 0)
            if duration <= 2:
                status_low = "short_drop"
            else:
                continue
        duration_seconds = int(raw.get("duration_seconds", 0) or 0)
        if any(x in status_low for x in ("autoanswer", "auto_answer", "auto", "автоответ", "voicemail")):
            autoanswer_attempt_count += 1
        if any(x in status_low for x in ("no_answer", "busy", "недозвон", "ring")):
            no_answer_attempt_count += 1
        if duration_seconds <= 2 or any(x in status_low for x in ("short_drop", "drop", "hang")):
            short_drop_attempt_count += 1
        if not phone7:
            unknown_phone_attempts_count += 1
            continue
        direction = str(raw.get("direction") or "").strip().lower()
        ts = str(raw.get("timestamp") or raw.get("created_at") or "").strip()
        hhmm, day = _extract_hhmm_and_day(ts)
        entry = {
            "status": status_low,
            "direction": direction,
            "hhmm": hhmm,
            "day": day,
            "duration_seconds": duration_seconds,
        }
        attempts_by_phone.setdefault(phone7, []).append(entry)
        known_phones.add(phone7)
        if "секрет" in status_low or "secretary" in status_low:
            secretary_touch_count += 1
        if hhmm:
            same_time_counter[f"{phone7}:{hhmm}"] += 1
            times_by_phone.setdefault(phone7, set()).add(hhmm)
        if day:
            same_day_counter[f"{phone7}:{day}"] += 1
            days_by_phone.setdefault(phone7, set()).add(day)
            if hhmm:
                diff_day_same_time_counter[f"{phone7}:{hhmm}"] += 1

    unique_phones = len(attempts_by_phone)
    repeated_dead_redial_count = 0
    over_limit_numbers = 0
    attempts_per_phone: dict[str, int] = {}
    for phone_key, attempts in attempts_by_phone.items():
        attempts_per_phone[phone_key] = len(attempts)
        if len(attempts) > 2:
            over_limit_numbers += 1
        emptyish = 0
        for entry in attempts:
            st = str(entry.get("status") or "")
            if any(x in st for x in ("no_answer", "busy", "auto", "voicemail", "недозвон", "автоответ")):
                emptyish += 1
        if len(attempts) > 2 and emptyish >= max(2, len(attempts) - 1):
            repeated_dead_redial_count += 1

    same_time_redial_pattern_flag = any(v >= 2 for v in same_time_counter.values())
    same_day_repeat_attempts_flag = any(v >= 2 for v in same_day_counter.values())
    different_days_same_time_flag = False
    different_days_different_time_flag = False
    for phone7, phone_days in days_by_phone.items():
        if len(phone_days) >= 2:
            if any(k.startswith(f"{phone7}:") and v >= 2 for k, v in diff_day_same_time_counter.items()):
                different_days_same_time_flag = True
            if len(times_by_phone.get(phone7, set())) >= 2:
                different_days_different_time_flag = True

    status_low = str(status_name or "").lower()
    is_closed = any(x in status_low for x in ("закрыто", "не реализ", "успешно реализ"))
    known_unique_phones = len(known_phones)
    attempted_phones = sorted(attempts_by_phone.keys())
    not_attempted_phones = sorted([x for x in known_phones if x not in attempts_by_phone])
    if known_unique_phones > 0 and unique_phones > 0:
        numbers_not_fully_covered_flag = len(not_attempted_phones) > 0
    else:
        numbers_not_fully_covered_flag = False
    numbers_coverage_unknown_flag = bool(known_unique_phones > 0 and unique_phones == 0)
    if is_closed and known_unique_phones == 0:
        numbers_not_fully_covered_flag = unique_phones == 0 and unknown_phone_attempts_count == 0

    emptyish_attempts_total = autoanswer_attempt_count + no_answer_attempt_count + short_drop_attempt_count
    attempts_by_day: Counter[str] = Counter()
    for attempts in attempts_by_phone.values():
        for entry in attempts:
            day = str(entry.get("day") or "")
            if day:
                attempts_by_day[day] += 1
    massive_empty_attempts_day_count = sum(
        1
        for day, day_total in attempts_by_day.items()
        if day_total >= 10 and emptyish_attempts_total >= day_total
    )
    massive_empty_attempts_day_flag = massive_empty_attempts_day_count > 0

    red_flag = (
        repeated_dead_redial_count > 0
        or over_limit_numbers > 0
        or same_time_redial_pattern_flag
        or massive_empty_attempts_day_flag
    )
    active_low_attempts_guard = (not is_closed) and sum(len(v) for v in attempts_by_phone.values()) <= 2 and not red_flag
    return {
        "dial_unique_phones_count": unique_phones,
        "dial_known_unique_phones_count": known_unique_phones,
        "dial_attempts_total": sum(len(v) for v in attempts_by_phone.values()),
        "dial_unknown_phone_attempts_count": int(unknown_phone_attempts_count),
        "dial_attempted_phones": attempted_phones,
        "dial_not_attempted_phones": not_attempted_phones,
        "dial_attempts_per_phone": attempts_per_phone,
        "dial_autoanswer_attempts_count": autoanswer_attempt_count,
        "dial_no_answer_attempts_count": no_answer_attempt_count,
        "dial_short_drop_attempts_count": short_drop_attempt_count,
        "dial_secretary_touch_count": secretary_touch_count,
        "dial_over_limit_numbers_count": over_limit_numbers,
        "repeated_dead_redial_count": repeated_dead_redial_count,
        "repeated_dead_redial_day_flag": bool(repeated_dead_redial_count > 0),
        "same_time_redial_pattern_flag": bool(same_time_redial_pattern_flag),
        "same_day_repeat_attempts_flag": bool(same_day_repeat_attempts_flag),
        "different_days_same_time_flag": bool(different_days_same_time_flag),
        "different_days_different_time_flag": bool(different_days_different_time_flag),
        "massive_empty_attempts_day_count": int(massive_empty_attempts_day_count),
        "massive_empty_attempts_day_flag": bool(massive_empty_attempts_day_flag),
        "numbers_not_fully_covered_flag": bool(numbers_not_fully_covered_flag),
        "numbers_coverage_unknown_flag": bool(numbers_coverage_unknown_flag),
        "dial_redial_suspicion_flag": bool(over_limit_numbers > 0 or repeated_dead_redial_count > 0 or same_time_redial_pattern_flag),
        "active_low_attempts_guard": bool(active_low_attempts_guard),
        "dial_discipline_pattern_label": "red_flag" if red_flag else ("normal" if unique_phones > 0 else "none"),
    }


def _merge_deal_company_tags(*, deal_tags: list[Any], company_tags: list[Any]) -> tuple[list[str], list[str], list[str]]:
    normalized_deal_tags = set(normalize_tag_values(deal_tags))
    normalized_company_tags = set(normalize_tag_values(company_tags))
    propagated_company_tags = sorted(x for x in normalized_company_tags if x not in normalized_deal_tags)
    merged_tags = sorted(normalized_deal_tags.union(normalized_company_tags))
    return merged_tags, sorted(normalized_company_tags), propagated_company_tags


def _extract_company_id(row: dict[str, Any]) -> str:
    candidates: list[Any] = [
        row.get("company_id"),
        row.get("amo_company_id"),
        row.get("company"),
    ]
    company_ids = row.get("company_ids")
    if isinstance(company_ids, list) and company_ids:
        candidates.append(company_ids[0])
    links = row.get("entity_links")
    if isinstance(links, list):
        for link in links:
            if not isinstance(link, dict):
                continue
            entity_type = str(link.get("to_entity_type") or "").strip().lower()
            if entity_type == "companies":
                candidates.append(link.get("to_entity_id"))
                break
    for raw in candidates:
        text = str(raw or "").strip()
        if not text:
            continue
        digits = re.sub(r"\D+", "", text)
        return digits or text
    return ""


def _build_company_tag_propagation_dry_run_plan(*, rows: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    safe_count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        deal_id = str(row.get("deal_id") or row.get("amo_lead_id") or "").strip()
        company_id = _extract_company_id(row)
        deal_tags_raw = row.get("tags") if isinstance(row.get("tags"), list) else []
        company_tags_raw = row.get("company_tags") if isinstance(row.get("company_tags"), list) else []
        deal_tags = collect_raw_tag_values(deal_tags_raw)
        company_tags = collect_raw_tag_values(company_tags_raw)
        company_tag_source = str(row.get("company_tags_source") or "api_tags").strip().lower() or "api_tags"

        proposed_tags_to_add: list[str] = []
        safe_to_propagate = False
        reason = ""

        if deal_tags:
            reason = "deal_has_own_tags"
        elif not company_tags:
            reason = "company_has_no_tags"
        elif company_tag_source not in {"api_tags", "existing_row_company_tags"}:
            reason = "company_tag_source_not_trusted"
        elif len(company_tags) > 1:
            reason = "company_has_multiple_tags_conflict"
        else:
            proposed_tags_to_add = [company_tags[0]]
            safe_to_propagate = True
            reason = "single_company_tag_safe_to_propagate"

        if safe_to_propagate:
            safe_count += 1

        items.append(
            {
                "deal_id": deal_id,
                "company_id": company_id,
                "company_tags": company_tags,
                "deal_tags": deal_tags,
                "proposed_tags_to_add": proposed_tags_to_add,
                "safe_to_propagate": safe_to_propagate,
                "reason": reason,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_total": len(items),
        "safe_to_propagate_total": safe_count,
        "unsafe_total": max(0, len(items) - safe_count),
        "items": items,
    }


def _build_company_tag_propagation_dry_run_markdown(payload: dict[str, Any]) -> str:
    rows = payload.get("items", []) if isinstance(payload.get("items"), list) else []
    lines = [
        "# Company Tag Propagation Dry-Run",
        "",
        f"- rows_total: {int(payload.get('rows_total', 0) or 0)}",
        f"- safe_to_propagate_total: {int(payload.get('safe_to_propagate_total', 0) or 0)}",
        f"- unsafe_total: {int(payload.get('unsafe_total', 0) or 0)}",
        "",
        "## Top Safe Proposals",
    ]
    safe_rows = [x for x in rows if isinstance(x, dict) and bool(x.get("safe_to_propagate"))]
    if not safe_rows:
        lines.append("- no safe proposals")
        return "\n".join(lines) + "\n"
    for row in safe_rows[:20]:
        lines.append(
            "- deal_id={deal} company_id={company} add={add} reason={reason}".format(
                deal=str(row.get("deal_id") or ""),
                company=str(row.get("company_id") or ""),
                add=", ".join(row.get("proposed_tags_to_add", []) if isinstance(row.get("proposed_tags_to_add"), list) else []),
                reason=str(row.get("reason") or ""),
            )
        )
    return "\n".join(lines) + "\n"


def _extract_embedded_tag_names(entity: dict[str, Any]) -> list[str]:
    embedded = entity.get("_embedded", {}) if isinstance(entity, dict) else {}
    tags = embedded.get("tags", []) if isinstance(embedded, dict) and isinstance(embedded.get("tags"), list) else []
    out: list[str] = []
    for tag in tags:
        if not isinstance(tag, dict):
            continue
        name = " ".join(str(tag.get("name") or "").split()).strip()
        if name:
            out.append(name)
    return normalize_tag_values(out)


def _fetch_company_tags_for_lead(
    *,
    client: AmoCollectorClient,
    lead_id: int,
    company_tags_cache: dict[int, list[str]],
) -> list[str]:
    tags_out: list[str] = []
    try:
        links = client.get_lead_links(int(lead_id))
    except Exception:
        return []
    company_ids = [
        int(link.get("to_entity_id"))
        for link in links
        if isinstance(link, dict)
        and str(link.get("to_entity_type") or "").strip().lower() == "companies"
        and isinstance(link.get("to_entity_id"), int)
    ]
    for company_id in sorted(set(company_ids)):
        if company_id in company_tags_cache:
            tags_out.extend(company_tags_cache[company_id])
            continue
        company_tags: list[str] = []
        try:
            companies = client.get_companies_by_ids([company_id])
            company = companies[0] if companies else {}
            company_tags = _extract_embedded_tag_names(company if isinstance(company, dict) else {})
        except Exception:
            company_tags = []
        company_tags_cache[company_id] = company_tags
        tags_out.extend(company_tags)
    return normalize_tag_values(tags_out)


def _daily_candidate_retry_score(snapshot: dict[str, Any]) -> int:
    """Estimate call-rich value to decide if expensive transcript quality retry is worth it."""
    call_evidence = snapshot.get("call_evidence", {}) if isinstance(snapshot, dict) else {}
    calls = call_evidence.get("items", []) if isinstance(call_evidence, dict) and isinstance(call_evidence.get("items"), list) else []
    score = 0
    for call in calls:
        if not isinstance(call, dict):
            continue
        duration = int(call.get("duration_seconds", 0) or 0)
        direction = str(call.get("direction") or "").strip().lower()
        rec_url = str(call.get("recording_url") or "").strip()
        if duration >= 40:
            score += 8
        elif duration >= 15:
            score += 4
        if direction in {"outbound", "inbound"}:
            score += 2
        if rec_url:
            score += 6
    if len(calls) >= 3:
        score += 8
    elif len(calls) == 2:
        score += 4
    return min(100, score)


def _collect_call_pool_debug(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    rows: list[dict[str, Any]],
    raw_bundles_by_deal: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    downloader = CallDownloader(config=cfg, logger=logger)
    results_by_deal = downloader.collect_period_calls(
        deals=rows,
        raw_bundles_by_deal=raw_bundles_by_deal,
        resolve_audio=False,
    )
    items: list[dict[str, Any]] = []
    for row in rows:
        deal_id = str(row.get("deal_id") or row.get("amo_lead_id") or "").strip()
        result = results_by_deal.get(deal_id)
        calls = result.calls if result is not None else []
        call_dicts = call_evidence_to_dicts(calls)
        dial = _build_dial_discipline_signals(
            {"call_evidence": {"items": call_dicts}, "normalized_deal": row},
            status_name=str(row.get("status_name") or ""),
        )
        durations = [max(0, int(c.duration_seconds)) for c in calls]
        short_calls = sum(1 for x in durations if 0 <= x <= 20)
        medium_calls = sum(1 for x in durations if 21 <= x <= 60)
        long_calls = sum(1 for x in durations if x >= 61)
        rec_count = sum(1 for c in calls if str(c.recording_url or "").strip())
        audio_path_count = sum(1 for c in calls if str(c.audio_path or "").strip())
        raw_deal_tags = row.get("tags") if isinstance(row.get("tags"), list) else []
        raw_company_tags = row.get("company_tags") if isinstance(row.get("company_tags"), list) else []
        merged_runtime_tags, normalized_company_tags, propagated_company_tags = _merge_deal_company_tags(
            deal_tags=raw_deal_tags,
            company_tags=raw_company_tags,
        )
        if raw_deal_tags:
            runtime_tag_source = "deal_tags"
        elif normalized_company_tags:
            runtime_tag_source = "company_tags"
        else:
            runtime_tag_source = "none"
        item = {
            "deal_id": deal_id,
            "deal_name": str(row.get("deal_name") or "").strip(),
            "owner_name": str(row.get("responsible_user_name") or "").strip(),
            "tags": raw_deal_tags,
            "company_tags": raw_company_tags,
            "runtime_effective_tags": merged_runtime_tags,
            "runtime_tag_source": runtime_tag_source,
            "runtime_company_tag_promoted": bool((not raw_deal_tags) and normalized_company_tags),
            "runtime_propagated_company_tags": propagated_company_tags,
            "status_name": str(row.get("status_name") or "").strip(),
            "pipeline_name": str(row.get("pipeline_name") or "").strip(),
            "updated_at": row.get("updated_at") or "",
            "created_at": row.get("created_at") or "",
            "calls_total": len(calls),
            "outbound_calls": sum(1 for c in calls if str(c.direction or "").strip().lower() == "outbound"),
            "inbound_calls": sum(1 for c in calls if str(c.direction or "").strip().lower() == "inbound"),
            "max_duration_seconds": max(durations) if durations else 0,
            "total_duration_seconds": sum(durations),
            "recording_url_count": rec_count,
            "audio_path_count": audio_path_count,
            "short_calls_0_20_count": short_calls,
            "medium_calls_21_60_count": medium_calls,
            "long_calls_61_plus_count": long_calls,
            "no_answer_like_count": sum(1 for c in call_dicts if _is_no_answer_like_call(c)),
            "autoanswer_like_count": sum(1 for c in call_dicts if _is_autoanswer_like_call(c)),
            "repeated_dead_redial_count": int(dial.get("repeated_dead_redial_count", 0) or 0),
            "same_time_redial_pattern_flag": bool(dial.get("same_time_redial_pattern_flag")),
            "same_day_repeat_attempts_flag": bool(dial.get("same_day_repeat_attempts_flag")),
            "different_days_same_time_flag": bool(dial.get("different_days_same_time_flag")),
            "different_days_different_time_flag": bool(dial.get("different_days_different_time_flag")),
            "unique_phone_count": int(dial.get("dial_unique_phones_count", 0) or 0),
            "known_unique_phone_count": int(dial.get("dial_known_unique_phones_count", 0) or 0),
            "attempted_phones": list(dial.get("dial_attempted_phones") or []),
            "not_attempted_phones": list(dial.get("dial_not_attempted_phones") or []),
            "attempts_per_phone": dict(dial.get("dial_attempts_per_phone") or {}),
            "secretary_touch_count": int(dial.get("dial_secretary_touch_count", 0) or 0),
            "autoanswer_attempts_count": int(dial.get("dial_autoanswer_attempts_count", 0) or 0),
            "no_answer_attempts_count": int(dial.get("dial_no_answer_attempts_count", 0) or 0),
            "short_drop_attempts_count": int(dial.get("dial_short_drop_attempts_count", 0) or 0),
            "unknown_phone_attempts_count": int(dial.get("dial_unknown_phone_attempts_count", 0) or 0),
            "massive_empty_attempts_day_count": int(dial.get("massive_empty_attempts_day_count", 0) or 0),
            "massive_empty_attempts_day_flag": bool(dial.get("massive_empty_attempts_day_flag")),
            "dial_redial_suspicion_flag": bool(dial.get("dial_redial_suspicion_flag")),
            "active_low_attempts_guard": bool(dial.get("active_low_attempts_guard")),
            "numbers_not_fully_covered_flag": bool(dial.get("numbers_not_fully_covered_flag")),
            "numbers_coverage_unknown_flag": bool(dial.get("numbers_coverage_unknown_flag")),
            "call_source_used": str(result.source_used if result is not None else "none"),
            "warnings": list(result.warnings) if result is not None else [],
            "_text_hints": _call_pool_text_hints(row),
            "call_items": [
                {
                    "call_id": str(c.get("call_id") or ""),
                    "timestamp": str(c.get("timestamp") or ""),
                    "duration_seconds": int(c.get("duration_seconds", 0) or 0),
                    "direction": str(c.get("direction") or ""),
                    "recording_url": str(c.get("recording_url") or ""),
                    "audio_path": str(c.get("audio_path") or ""),
                    "phone": str(c.get("phone") or c.get("phone_number") or c.get("contact_phone") or ""),
                    "status": str(c.get("status") or ""),
                    "result": str(c.get("result") or ""),
                    "disposition": str(c.get("disposition") or ""),
                    "quality_flags": c.get("quality_flags") if isinstance(c.get("quality_flags"), list) else [],
                }
                for c in call_dicts
                if isinstance(c, dict)
            ],
        }
        call_case_type = _classify_call_case_type(item)
        pool_type, pool_reason, pool_priority_score = _assign_pool_for_item(
            item=item,
            call_case_type=call_case_type,
        )
        item["call_case_type"] = call_case_type
        item["pool_type"] = pool_type
        item["pool_reason"] = pool_reason
        item["pool_priority_score"] = pool_priority_score
        items.append(item)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "deals_total_before_limit": len(items),
        "deals_with_any_calls": sum(1 for x in items if int(x.get("calls_total", 0) or 0) > 0),
        "deals_with_recordings": sum(1 for x in items if int(x.get("recording_url_count", 0) or 0) > 0),
        "deals_with_long_calls": sum(1 for x in items if int(x.get("long_calls_61_plus_count", 0) or 0) > 0),
        "deals_with_only_short_calls": sum(
            1
            for x in items
            if int(x.get("calls_total", 0) or 0) > 0
            and int(x.get("short_calls_0_20_count", 0) or 0) == int(x.get("calls_total", 0) or 0)
        ),
        "deals_with_autoanswer_pattern": sum(1 for x in items if int(x.get("autoanswer_like_count", 0) or 0) > 0),
        "deals_with_redial_pattern": sum(
            1
            for x in items
            if int(x.get("repeated_dead_redial_count", 0) or 0) > 0
            or bool(x.get("same_time_redial_pattern_flag"))
        ),
        "items": items,
    }


def _call_pool_text_hints(row: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key in ("status_name", "pipeline_name", "source_name", "source_url", "deal_name"):
        value = " ".join(str(row.get(key) or "").split()).strip()
        if value:
            chunks.append(value.lower())
    for key in ("tags", "company_tags", "source_values"):
        values = row.get(key)
        if isinstance(values, list):
            for v in values:
                text = " ".join(str(v or "").split()).strip()
                if text:
                    chunks.append(text.lower())
    return " | ".join(chunks)


def _classify_call_case_type(item: dict[str, Any]) -> str:
    calls_total = int(item.get("calls_total", 0) or 0)
    short_calls = int(item.get("short_calls_0_20_count", 0) or 0)
    medium_calls = int(item.get("medium_calls_21_60_count", 0) or 0)
    long_calls = int(item.get("long_calls_61_plus_count", 0) or 0)
    no_answer_like = int(item.get("no_answer_like_count", 0) or 0)
    autoanswer_like = int(item.get("autoanswer_like_count", 0) or 0)
    repeated_dead_redial = int(item.get("repeated_dead_redial_count", 0) or 0)
    same_time_repeat = bool(item.get("same_time_redial_pattern_flag"))
    not_covered = bool(item.get("numbers_not_fully_covered_flag"))
    massive_empty_day = bool(item.get("massive_empty_attempts_day_flag"))
    text_hints = str(item.get("_text_hints") or "").lower()
    call_items = item.get("call_items", []) if isinstance(item.get("call_items"), list) else []
    call_blob_parts: list[str] = []
    for call in call_items:
        if not isinstance(call, dict):
            continue
        call_blob_parts.extend(
            [
                str(call.get("status") or ""),
                str(call.get("result") or ""),
                str(call.get("disposition") or ""),
            ]
        )
    call_blob = " ".join(call_blob_parts).lower()
    has_secretary_touch = any(token in call_blob for token in ("секрет", "secretary", "ресепш", "switchboard"))
    has_lpr_touch = any(token in call_blob for token in ("липр", "лпр", "decision_maker", "директор", "собственник"))
    has_supplier_touch = any(token in call_blob for token in ("поставщик", "supplier", "закуп", "тендер", "etp"))
    has_nontrivial_conversation = (long_calls + medium_calls) > 0
    is_mostly_short_noise = calls_total > 0 and short_calls >= calls_total and (no_answer_like + autoanswer_like) >= max(1, calls_total - 1)

    if calls_total <= 0:
        return "unknown"
    if is_mostly_short_noise:
        return "autoanswer_noise" if autoanswer_like > 0 else "redial_discipline"
    strong_discipline_pattern = (
        repeated_dead_redial > 0
        or same_time_repeat
        or massive_empty_day
        or (not_covered and short_calls == calls_total and calls_total >= 3)
    )
    if strong_discipline_pattern:
        return "redial_discipline"
    if has_secretary_touch and not has_lpr_touch:
        return "secretary_case"
    if has_supplier_touch and has_nontrivial_conversation:
        return "supplier_inbound"
    if has_lpr_touch and has_nontrivial_conversation:
        return "lpr_conversation"
    if autoanswer_like > 0 and long_calls == 0 and medium_calls == 0:
        return "autoanswer_noise"
    if any(token in text_hints for token in ("секрет", "ресепш", "соедините", "переадрес", "почт")) and (has_nontrivial_conversation or int(item.get("recording_url_count", 0) or 0) > 0):
        return "secretary_case"
    if any(token in text_hints for token in ("поставщик", "supplier", "закуп", "тендер", "etp", "площадк", "регистрац")) and has_nontrivial_conversation:
        return "supplier_inbound"
    if any(token in text_hints for token in ("демо", "демонстрац", "презентац", "тест", "бриф", "встреч")) and has_nontrivial_conversation:
        return "warm_inbound"
    if has_nontrivial_conversation and no_answer_like < calls_total:
        return "lpr_conversation"
    if short_calls == calls_total:
        return "autoanswer_noise" if autoanswer_like > 0 else "redial_discipline"
    if not_covered and (long_calls > 0 or medium_calls > 0):
        return "lpr_conversation"
    if not_covered:
        return "redial_discipline"
    return "unknown"


def _freshness_score_for_item(item: dict[str, Any]) -> int:
    dt = _parse_record_activity_dt(item)
    if not dt:
        return 0
    now_utc = datetime.now(timezone.utc)
    delta_days = max(0, (now_utc.date() - dt.date()).days)
    if delta_days <= 1:
        return 3
    if delta_days <= 3:
        return 2
    if delta_days <= 7:
        return 1
    return 0


def _assign_pool_for_item(*, item: dict[str, Any], call_case_type: str) -> tuple[str, str, int]:
    calls_total = int(item.get("calls_total", 0) or 0)
    recording_count = int(item.get("recording_url_count", 0) or 0)
    max_duration = int(item.get("max_duration_seconds", 0) or 0)
    long_calls = int(item.get("long_calls_61_plus_count", 0) or 0)
    medium_calls = int(item.get("medium_calls_21_60_count", 0) or 0)
    short_calls = int(item.get("short_calls_0_20_count", 0) or 0)
    no_answer_like = int(item.get("no_answer_like_count", 0) or 0)
    autoanswer_like = int(item.get("autoanswer_like_count", 0) or 0)
    repeated_dead_redial = int(item.get("repeated_dead_redial_count", 0) or 0)
    same_time_repeat = bool(item.get("same_time_redial_pattern_flag"))
    not_covered = bool(item.get("numbers_not_fully_covered_flag"))
    massive_empty_day = bool(item.get("massive_empty_attempts_day_flag"))
    active_low_attempts_guard = bool(item.get("active_low_attempts_guard"))
    freshness = _freshness_score_for_item(item)
    has_nontrivial_conversation = (long_calls + medium_calls) > 0
    mostly_short_noise = calls_total > 0 and short_calls >= calls_total and (no_answer_like + autoanswer_like) >= max(1, calls_total - 1)
    conversation_viable = has_nontrivial_conversation and not mostly_short_noise

    conversation_quality = 0
    if call_case_type == "lpr_conversation":
        conversation_quality = 55
    elif call_case_type == "secretary_case":
        conversation_quality = 48
    elif call_case_type == "supplier_inbound":
        conversation_quality = 45
    elif call_case_type == "warm_inbound":
        conversation_quality = 43
    elif long_calls > 0 or medium_calls > 0:
        conversation_quality = 30
    recording_bonus = 12 if recording_count > 0 else 0
    duration_bonus = 8 if max_duration >= 61 else (4 if max_duration >= 21 else 0)
    management_bonus = 0
    if any(
        bool(item.get(k))
        for k in (
            "same_time_redial_pattern_flag",
            "numbers_not_fully_covered_flag",
        )
    ):
        management_bonus += 2
    conversation_score = conversation_quality + recording_bonus + duration_bonus + management_bonus + freshness

    discipline_quality = 0
    if call_case_type == "redial_discipline":
        discipline_quality = 40
    elif call_case_type == "autoanswer_noise":
        discipline_quality = 24
    elif short_calls > 0 and (no_answer_like + autoanswer_like) >= max(1, short_calls):
        discipline_quality = 26
    discipline_bonus = min(15, repeated_dead_redial * 5)
    discipline_bonus += 6 if same_time_repeat else 0
    discipline_bonus += 5 if not_covered else 0
    discipline_bonus += 8 if massive_empty_day else 0
    if calls_total > 0 and short_calls == calls_total:
        discipline_bonus += 4
    discipline_score = discipline_quality + discipline_bonus + freshness

    if active_low_attempts_guard and conversation_viable and conversation_score >= max(35, discipline_score + 1):
        reason = f"{call_case_type}; active_low_attempts_guard"
        return "conversation_pool", reason, int(conversation_score)
    if (
        call_case_type in {"lpr_conversation", "secretary_case", "supplier_inbound", "warm_inbound"}
        and conversation_viable
        and conversation_score >= max(35, discipline_score)
    ):
        reason = f"{call_case_type}; conversation_case_priority rec={recording_count}; dur={max_duration}s"
        return "conversation_pool", reason, int(conversation_score)
    if conversation_viable and conversation_score >= max(42, discipline_score + 4):
        reason = f"{call_case_type}; rec={recording_count}; dur={max_duration}s"
        return "conversation_pool", reason, int(conversation_score)
    if discipline_score >= 36:
        reason = (
            f"{call_case_type}; short={short_calls}; no_answer={no_answer_like}; auto={autoanswer_like}; "
            f"redial={repeated_dead_redial}; massive_empty={int(massive_empty_day)}"
        )
        return "discipline_pool", reason, int(discipline_score)
    return "none", "low_signal_for_both_pools", 0


def _build_call_pool_artifacts(*, call_pool_debug: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    items = [x for x in (call_pool_debug.get("items") if isinstance(call_pool_debug.get("items"), list) else []) if isinstance(x, dict)]
    conv_all = [dict(x) for x in items if str(x.get("pool_type") or "") == "conversation_pool"]
    disc_all = [dict(x) for x in items if str(x.get("pool_type") or "") == "discipline_pool"]

    conv_all.sort(
        key=lambda x: (
            -int(x.get("pool_priority_score", 0) or 0),
            -int(x.get("recording_url_count", 0) or 0),
            -int(x.get("max_duration_seconds", 0) or 0),
            str(x.get("deal_id") or ""),
        )
    )
    disc_all.sort(
        key=lambda x: (
            -int(x.get("pool_priority_score", 0) or 0),
            -int(x.get("repeated_dead_redial_count", 0) or 0),
            -int(x.get("short_calls_0_20_count", 0) or 0),
            str(x.get("deal_id") or ""),
        )
    )

    call_case_counts: Counter[str] = Counter(str(x.get("call_case_type") or "unknown") for x in items)
    aggregates = {
        "conversation_pool_total": len(conv_all),
        "discipline_pool_total": len(disc_all),
        "lpr_conversation_total": int(call_case_counts.get("lpr_conversation", 0)),
        "secretary_case_total": int(call_case_counts.get("secretary_case", 0)),
        "supplier_inbound_total": int(call_case_counts.get("supplier_inbound", 0)),
        "warm_inbound_total": int(call_case_counts.get("warm_inbound", 0)),
        "redial_discipline_total": int(call_case_counts.get("redial_discipline", 0)),
        "autoanswer_noise_total": int(call_case_counts.get("autoanswer_noise", 0)),
    }
    conv_payload = {
        "total": len(conv_all),
        "items": conv_all,
    }
    disc_payload = {
        "total": len(disc_all),
        "items": disc_all,
    }
    return conv_payload, disc_payload, aggregates


def _build_pool_markdown(*, title: str, pool_items: list[dict[str, Any]], total: int) -> str:
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(f"- total: {int(total or 0)}")
    lines.append("")
    if not pool_items:
        lines.append("- empty")
    else:
        for item in pool_items:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- deal={deal} owner={owner} case={case_type} score={score} reason={reason} calls={calls} rec={rec} long={long_calls} short={short_calls} redial={redial}".format(
                    deal=str(item.get("deal_id") or ""),
                    owner=str(item.get("owner_name") or ""),
                    case_type=str(item.get("call_case_type") or "unknown"),
                    score=int(item.get("pool_priority_score", 0) or 0),
                    reason=str(item.get("pool_reason") or ""),
                    calls=int(item.get("calls_total", 0) or 0),
                    rec=int(item.get("recording_url_count", 0) or 0),
                    long_calls=int(item.get("long_calls_61_plus_count", 0) or 0),
                    short_calls=int(item.get("short_calls_0_20_count", 0) or 0),
                    redial=int(item.get("repeated_dead_redial_count", 0) or 0),
                )
            )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_discipline_report_payload(*, discipline_pool_payload: dict[str, Any]) -> dict[str, Any]:
    items = [
        x
        for x in (discipline_pool_payload.get("items") if isinstance(discipline_pool_payload.get("items"), list) else [])
        if isinstance(x, dict)
    ]
    report_items: list[dict[str, Any]] = []
    over_2_attempts_count = 0
    not_covered_count = 0
    short_cluster_count = 0
    same_time_pattern_count = 0
    for item in items:
        call_items = item.get("call_items", []) if isinstance(item.get("call_items"), list) else []
        attempts_per_phone = (
            dict(item.get("attempts_per_phone"))
            if isinstance(item.get("attempts_per_phone"), dict)
            else {}
        )
        if not attempts_per_phone:
            for call in call_items:
                if not isinstance(call, dict):
                    continue
                phone7 = _normalize_phone_last7(call.get("phone") or call.get("phone_number") or call.get("contact_phone") or "")
                key = phone7 or "__unknown__"
                attempts_per_phone[key] = attempts_per_phone.get(key, 0) + 1
        phones_over_2_attempts = sorted([k for k, v in attempts_per_phone.items() if v > 2 and k != "__unknown__"])
        unique_phone_count = int(item.get("unique_phone_count", 0) or 0)
        if unique_phone_count <= 0:
            unique_phone_count = len([k for k in attempts_per_phone if k != "__unknown__"])
        known_unique_phone_count = int(item.get("known_unique_phone_count", 0) or 0)
        not_attempted_phones = (
            [str(x) for x in item.get("not_attempted_phones", []) if str(x or "").strip()]
            if isinstance(item.get("not_attempted_phones"), list)
            else []
        )
        attempted_phones = (
            [str(x) for x in item.get("attempted_phones", []) if str(x or "").strip()]
            if isinstance(item.get("attempted_phones"), list)
            else sorted([k for k in attempts_per_phone if k != "__unknown__"])
        )
        attempts_total = int(item.get("calls_total", 0) or 0)
        short_cluster_flag = attempts_total > 0 and int(item.get("short_calls_0_20_count", 0) or 0) >= max(2, attempts_total - 1)
        autoanswer_cluster_flag = int(item.get("autoanswer_like_count", 0) or 0) >= 2
        same_time_flag = bool(item.get("same_time_redial_pattern_flag"))
        same_day_repeat_attempts_flag = bool(item.get("same_day_repeat_attempts_flag"))
        different_days_same_time_flag = bool(item.get("different_days_same_time_flag"))
        different_days_different_time_flag = bool(item.get("different_days_different_time_flag"))
        not_covered_flag = bool(item.get("numbers_not_fully_covered_flag"))
        repeated_dead_redial_count = int(item.get("repeated_dead_redial_count", 0) or 0)
        massive_empty_attempts_day_flag = bool(item.get("massive_empty_attempts_day_flag"))
        active_low_attempts_guard = bool(item.get("active_low_attempts_guard"))
        status_low = str(item.get("status_name") or "").strip().lower()
        is_closed = any(x in status_low for x in ("закрыто", "не реализ", "успешно реализ"))

        risk_score = 0
        risk_score += min(3, len(phones_over_2_attempts))
        risk_score += 2 if same_time_flag else 0
        risk_score += 1 if same_day_repeat_attempts_flag else 0
        risk_score += 1 if different_days_same_time_flag else 0
        risk_score += 1 if different_days_different_time_flag else 0
        risk_score += 2 if not_covered_flag else 0
        risk_score += 2 if short_cluster_flag else 0
        risk_score += 1 if autoanswer_cluster_flag else 0
        risk_score += 2 if repeated_dead_redial_count > 0 else 0
        risk_score += 2 if massive_empty_attempts_day_flag else 0
        if active_low_attempts_guard:
            risk_score = max(0, risk_score - 2)
        if is_closed and (not_covered_flag or len(phones_over_2_attempts) > 0):
            risk_score += 2
        if risk_score >= 7:
            risk_level = "high"
        elif risk_score >= 4:
            risk_level = "medium"
        else:
            risk_level = "low"

        summary_parts: list[str] = []
        if phones_over_2_attempts:
            summary_parts.append("дрочат один и тот же номер")
        if not_covered_flag:
            summary_parts.append("не покрыты все номера")
        if short_cluster_flag:
            summary_parts.append("день ушел в короткие/пустые наборы")
        if same_time_flag:
            summary_parts.append("наборы в одно и то же время")
        if same_day_repeat_attempts_flag:
            summary_parts.append("повторы подряд в один день")
        if different_days_same_time_flag:
            summary_parts.append("в разные дни звонят в одно и то же время")
        if different_days_different_time_flag:
            summary_parts.append("в разные дни пробуют разное время")
        if autoanswer_cluster_flag:
            summary_parts.append("много автоответчиков")
        if massive_empty_attempts_day_flag:
            summary_parts.append("массово пустые попытки за день")
        if is_closed and not_covered_flag:
            summary_parts.append("сделка закрыта, но обзвон неполный")
        if is_closed and phones_over_2_attempts:
            summary_parts.append("сделка закрыта, а по номеру были лишние повторы")
        if active_low_attempts_guard:
            summary_parts.append("активная сделка, пока рано делать жесткий вывод")
        if not summary_parts:
            summary_parts.append("дисциплина в норме")

        row = {
            "deal_id": str(item.get("deal_id") or ""),
            "deal_name": str(item.get("deal_name") or ""),
            "owner_name": str(item.get("owner_name") or ""),
            "call_case_type": str(item.get("call_case_type") or "unknown"),
            "unique_phone_count": unique_phone_count,
            "known_unique_phone_count": known_unique_phone_count,
            "attempts_total": attempts_total,
            "attempts_per_phone": attempts_per_phone,
            "attempted_phones": attempted_phones,
            "not_attempted_phones": not_attempted_phones,
            "phones_over_2_attempts": phones_over_2_attempts,
            "repeated_dead_redial_count": repeated_dead_redial_count,
            "same_time_redial_pattern_flag": same_time_flag,
            "same_day_repeat_attempts_flag": same_day_repeat_attempts_flag,
            "different_days_same_time_flag": different_days_same_time_flag,
            "different_days_different_time_flag": different_days_different_time_flag,
            "numbers_not_fully_covered_flag": not_covered_flag,
            "short_call_cluster_flag": short_cluster_flag,
            "autoanswer_cluster_flag": autoanswer_cluster_flag,
            "massive_empty_attempts_day_flag": massive_empty_attempts_day_flag,
            "active_low_attempts_guard": active_low_attempts_guard,
            "discipline_summary_short": "; ".join(summary_parts),
            "discipline_risk_level": risk_level,
            "pool_priority_score": int(item.get("pool_priority_score", 0) or 0),
            "pool_reason": str(item.get("pool_reason") or ""),
        }
        report_items.append(row)

        if phones_over_2_attempts:
            over_2_attempts_count += 1
        if not_covered_flag:
            not_covered_count += 1
        if short_cluster_flag:
            short_cluster_count += 1
        if same_time_flag:
            same_time_pattern_count += 1

    report_items.sort(
        key=lambda x: (
            {"high": 0, "medium": 1, "low": 2}.get(str(x.get("discipline_risk_level") or "low"), 3),
            -int(x.get("pool_priority_score", 0) or 0),
            str(x.get("deal_id") or ""),
        )
    )
    return {
        "summary": {
            "discipline_pool_total": len(report_items),
            "deals_over_2_attempts": over_2_attempts_count,
            "deals_numbers_not_fully_covered": not_covered_count,
            "deals_short_call_cluster": short_cluster_count,
            "deals_same_time_repeat_pattern": same_time_pattern_count,
        },
        "items": report_items,
    }


def _build_discipline_report_markdown(*, discipline_report: dict[str, Any]) -> str:
    summary = discipline_report.get("summary", {}) if isinstance(discipline_report.get("summary"), dict) else {}
    items = discipline_report.get("items", []) if isinstance(discipline_report.get("items"), list) else []
    lines: list[str] = []
    lines.append("# Discipline Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- discipline_pool_total: {int(summary.get('discipline_pool_total', 0) or 0)}")
    lines.append(f"- deals_over_2_attempts: {int(summary.get('deals_over_2_attempts', 0) or 0)}")
    lines.append(f"- deals_numbers_not_fully_covered: {int(summary.get('deals_numbers_not_fully_covered', 0) or 0)}")
    lines.append(f"- deals_short_call_cluster: {int(summary.get('deals_short_call_cluster', 0) or 0)}")
    lines.append(f"- deals_same_time_repeat_pattern: {int(summary.get('deals_same_time_repeat_pattern', 0) or 0)}")
    lines.append("")

    lines.append("## Сделки, где дрочат один номер")
    over_2 = [x for x in items if isinstance(x, dict) and isinstance(x.get("phones_over_2_attempts"), list) and x.get("phones_over_2_attempts")]
    if not over_2:
        lines.append("- нет")
    else:
        for x in over_2[:30]:
            lines.append(f"- deal={x.get('deal_id')} phones={', '.join(x.get('phones_over_2_attempts', []))}")
    lines.append("")

    lines.append("## Сделки, где не покрыты все номера")
    not_covered = [x for x in items if isinstance(x, dict) and bool(x.get("numbers_not_fully_covered_flag"))]
    if not not_covered:
        lines.append("- нет")
    else:
        for x in not_covered[:30]:
            lines.append(f"- deal={x.get('deal_id')} attempts_total={x.get('attempts_total')}")
    lines.append("")

    lines.append("## Сделки, где день ушел в пустые наборы")
    short_cluster = [x for x in items if isinstance(x, dict) and bool(x.get("short_call_cluster_flag"))]
    if not short_cluster:
        lines.append("- нет")
    else:
        for x in short_cluster[:30]:
            lines.append(f"- deal={x.get('deal_id')} short_cluster=true summary={x.get('discipline_summary_short','')}")
    lines.append("")

    lines.append("## Сделки, где звонят в одно и то же время")
    same_time = [x for x in items if isinstance(x, dict) and bool(x.get("same_time_redial_pattern_flag"))]
    if not same_time:
        lines.append("- нет")
    else:
        for x in same_time[:30]:
            lines.append(f"- deal={x.get('deal_id')} same_time_pattern=true")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_transcription_shortlist_payload(
    *,
    conversation_pool_payload: dict[str, Any],
    discipline_pool_payload: dict[str, Any],
) -> dict[str, Any]:
    conv_items = [
        x
        for x in (conversation_pool_payload.get("items") if isinstance(conversation_pool_payload.get("items"), list) else [])
        if isinstance(x, dict)
    ]
    disc_items = [
        x
        for x in (discipline_pool_payload.get("items") if isinstance(discipline_pool_payload.get("items"), list) else [])
        if isinstance(x, dict)
    ]
    out: list[dict[str, Any]] = []
    calls_selected_total = 0
    calls_filtered_noise_total = 0

    for item in conv_items:
        calls = item.get("call_items", []) if isinstance(item.get("call_items"), list) else []
        selected_ids, reason, filtered_noise = _select_call_ids_for_transcription(calls)
        payload = dict(item)
        payload["selected_for_transcription"] = bool(selected_ids)
        payload["transcription_selection_reason"] = reason
        payload["selected_call_ids"] = selected_ids
        payload["selected_call_count"] = len(selected_ids)
        payload["filtered_noise_calls_count"] = filtered_noise
        out.append(payload)
        calls_selected_total += len(selected_ids)
        calls_filtered_noise_total += filtered_noise

    for item in disc_items:
        payload = dict(item)
        payload["selected_for_transcription"] = False
        payload["transcription_selection_reason"] = "discipline_pool_not_in_main_stt"
        payload["selected_call_ids"] = []
        payload["selected_call_count"] = 0
        payload["filtered_noise_calls_count"] = int(item.get("calls_total", 0) or 0)
        out.append(payload)
        calls_filtered_noise_total += int(item.get("calls_total", 0) or 0)

    out.sort(
        key=lambda x: (
            0 if bool(x.get("selected_for_transcription")) else 1,
            -int(x.get("pool_priority_score", 0) or 0),
            str(x.get("deal_id") or ""),
        )
    )
    return {
        "items": out,
        "conversation_pool_total": len(conv_items),
        "discipline_pool_total": len(disc_items),
        "deals_selected_for_transcription_total": sum(1 for x in out if bool(x.get("selected_for_transcription"))),
        "calls_selected_total": calls_selected_total,
        "calls_filtered_noise_total": calls_filtered_noise_total,
    }


def _build_analysis_shortlist_payload(
    *,
    shortlist_items: list[dict[str, Any]],
    normalized_rows_ranked: list[dict[str, Any]],
    limit: int | None,
) -> dict[str, Any]:
    target = max(0, int(limit)) if isinstance(limit, int) and limit >= 0 else None
    ranked: list[dict[str, Any]] = []
    for item in shortlist_items:
        if not isinstance(item, dict):
            continue
        did = str(item.get("deal_id") or "").strip()
        if not did:
            continue
        rank_group, reason, forced = _analysis_shortlist_rank_meta(item)
        ranked.append(
            {
                "deal_id": did,
                "rank_group": rank_group,
                "shortlist_reason": reason,
                "forced_fallback": bool(forced),
                "pool_type": str(item.get("pool_type") or "none"),
                "call_case_type": str(item.get("call_case_type") or "unknown"),
                "pool_priority_score": int(item.get("pool_priority_score", 0) or 0),
                "selected_for_transcription": bool(item.get("selected_for_transcription")),
                "selected_call_count": int(item.get("selected_call_count", 0) or 0),
            }
        )
    ranked.sort(
        key=lambda x: (
            int(x.get("rank_group", 99)),
            -int(x.get("pool_priority_score", 0)),
            str(x.get("deal_id") or ""),
        )
    )
    meaningful = [x for x in ranked if not bool(x.get("forced_fallback"))]
    selected_base = meaningful if meaningful else list(ranked)
    if not selected_base:
        # Controlled compatibility fallback for analysis-only path when call pool is empty.
        fallback_ids = [
            str(row.get("deal_id") or row.get("amo_lead_id") or "").strip()
            for row in normalized_rows_ranked
            if isinstance(row, dict)
        ]
        selected_base = [
            {
                "deal_id": did,
                "rank_group": 4,
                "shortlist_reason": "forced_fallback_no_call_signal",
                "forced_fallback": True,
                "pool_type": "none",
                "call_case_type": "unknown",
                "pool_priority_score": 0,
                "selected_for_transcription": False,
                "selected_call_count": 0,
            }
            for did in fallback_ids
            if did
        ]
    selected = selected_base[:target] if target is not None else list(selected_base)
    forced_candidates_total = max(0, len(ranked) - len(meaningful))
    if not ranked:
        forced_candidates_total = len(selected_base)
    return {
        "total_candidates": len(ranked),
        "total_meaningful_candidates": len(meaningful),
        "total_forced_fallback_candidates": forced_candidates_total,
        "total_selected": len(selected),
        "limit_applied_to_shortlist": target,
        "raw_pool_total_before_shortlist": len(
            [
                row
                for row in normalized_rows_ranked
                if isinstance(row, dict) and str(row.get("deal_id") or row.get("amo_lead_id") or "").strip()
            ]
        ),
        "selected_items": selected,
    }


def _analysis_shortlist_rank_meta(item: dict[str, Any]) -> tuple[int, str, bool]:
    pool = str(item.get("pool_type") or "none")
    case_type = str(item.get("call_case_type") or "unknown")
    selected_for_stt = bool(item.get("selected_for_transcription"))
    calls_total = int(item.get("calls_total", 0) or 0)
    short_calls = int(item.get("short_calls_0_20_count", 0) or 0)
    autoanswer_like = int(item.get("autoanswer_like_count", 0) or 0)
    if pool == "conversation_pool" and selected_for_stt and case_type in {"lpr_conversation", "warm_inbound", "supplier_inbound"}:
        return 1, f"priority_1_meaningful_conversation:{case_type}", False
    if pool == "conversation_pool" and selected_for_stt and case_type in {"secretary_case", "lpr_conversation"}:
        return 2, f"priority_2_secretary_or_lpr_potential:{case_type}", False
    if pool == "conversation_pool" and selected_for_stt and case_type in {"supplier_inbound", "warm_inbound"}:
        return 2, f"priority_2_meaningful_inbound:{case_type}", False
    if pool == "discipline_pool" and case_type == "redial_discipline":
        return 3, "priority_3_redial_discipline_pattern", False
    if calls_total > 0 and short_calls == calls_total and autoanswer_like > 0:
        return 5, "priority_5_autoanswer_noise_forced_fallback_only", True
    return 4, "priority_4_forced_fallback_other", True


def _build_analysis_shortlist_markdown(*, payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Analysis Shortlist")
    lines.append("")
    lines.append(f"- total_candidates: {int(payload.get('total_candidates', 0) or 0)}")
    lines.append(f"- total_meaningful_candidates: {int(payload.get('total_meaningful_candidates', 0) or 0)}")
    lines.append(f"- total_forced_fallback_candidates: {int(payload.get('total_forced_fallback_candidates', 0) or 0)}")
    lines.append(f"- total_selected: {int(payload.get('total_selected', 0) or 0)}")
    lines.append(f"- limit_applied_to_shortlist: {payload.get('limit_applied_to_shortlist')}")
    lines.append(f"- raw_pool_total_before_shortlist: {int(payload.get('raw_pool_total_before_shortlist', 0) or 0)}")
    bw = payload.get("business_window_filter", {}) if isinstance(payload.get("business_window_filter"), dict) else {}
    lines.append(f"- business_window_open_date: {str(bw.get('open_window_date') or '')}")
    lines.append(f"- selected_items_before_window_filter: {int(bw.get('selected_items_before_filter', 0) or 0)}")
    lines.append(f"- selected_items_after_window_filter: {int(bw.get('selected_items_after_filter', 0) or 0)}")
    lines.append(f"- selected_items_dropped_open_bucket: {int(bw.get('selected_items_dropped_open_bucket', 0) or 0)}")
    lines.append("")
    lines.append("## Selected Deals")
    items = payload.get("selected_items", []) if isinstance(payload.get("selected_items"), list) else []
    if not items:
        lines.append("- empty")
    else:
        for item in items:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- deal={deal} group={group} pool={pool} case={case_type} score={score} stt={stt} calls_for_stt={stt_calls} forced={forced} window={window} closed={closed} reason={reason}".format(
                    deal=str(item.get("deal_id") or ""),
                    group=int(item.get("rank_group", 0) or 0),
                    pool=str(item.get("pool_type") or ""),
                    case_type=str(item.get("call_case_type") or ""),
                    score=int(item.get("pool_priority_score", 0) or 0),
                    stt=bool(item.get("selected_for_transcription")),
                    stt_calls=int(item.get("selected_call_count", 0) or 0),
                    forced=bool(item.get("forced_fallback")),
                    window=str(item.get("business_window_date") or ""),
                    closed=bool(item.get("business_window_closed")),
                    reason=str(item.get("shortlist_reason") or ""),
                )
            )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _select_call_ids_for_transcription(calls: list[dict[str, Any]]) -> tuple[list[str], str, int]:
    if not calls:
        return [], "no_calls", 0
    scored: list[tuple[int, datetime | None, dict[str, Any]]] = []
    filtered_noise = 0
    for raw in calls:
        if not isinstance(raw, dict):
            continue
        call_id = str(raw.get("call_id") or "").strip()
        if not call_id:
            continue
        duration = int(raw.get("duration_seconds", 0) or 0)
        if duration <= 20 or _is_no_answer_like_call(raw) or _is_autoanswer_like_call(raw):
            filtered_noise += 1
            continue
        score = 0
        if duration >= 120:
            score += 20
        elif duration >= 61:
            score += 14
        elif duration >= 21:
            score += 8
        if str(raw.get("recording_url") or "").strip():
            score += 8
        direction = str(raw.get("direction") or "").strip().lower()
        if direction == "outbound":
            score += 3
        if direction == "inbound":
            score += 2
        ts = _parse_utc_ts(raw.get("timestamp"))
        if ts is not None:
            score += 2
        scored.append((score, ts, raw))

    if not scored:
        return [], "no_meaningful_calls_after_noise_filter", filtered_noise
    scored.sort(
        key=lambda x: (
            -x[0],
            str((x[1] or datetime(1970, 1, 1, tzinfo=timezone.utc)).isoformat()),
            str((x[2] or {}).get("call_id") or ""),
        )
    )
    primary = scored[0][2]
    selected = [str(primary.get("call_id") or "").strip()]
    primary_ts = _parse_utc_ts(primary.get("timestamp"))
    extra_added = 0
    for _, ts, call in scored[1:]:
        if extra_added >= 2:
            break
        cid = str(call.get("call_id") or "").strip()
        if not cid or cid in selected:
            continue
        # Keep only previous related calls (no future leakage across next bucket/day).
        if primary_ts is not None and ts is not None and ts > primary_ts:
            continue
        same_direction = str(call.get("direction") or "").strip().lower() == str(primary.get("direction") or "").strip().lower()
        close_in_time = bool(primary_ts is not None and ts is not None and (primary_ts - ts) <= timedelta(days=3))
        same_day = bool(primary_ts is not None and ts is not None and primary_ts.date() == ts.date())
        if same_direction or close_in_time or same_day:
            selected.append(cid)
            extra_added += 1
    return selected, "anchor_plus_previous_related_calls_only", filtered_noise


def _build_transcription_shortlist_markdown(*, transcription_shortlist_payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Transcription Shortlist")
    lines.append("")
    lines.append(f"- business_window_open_date: {str(transcription_shortlist_payload.get('business_window_open_date') or '')}")
    lines.append(
        f"- business_window_items_open_bucket: {int(transcription_shortlist_payload.get('business_window_items_open_bucket', 0) or 0)}"
    )
    lines.append(f"- conversation_pool_total: {int(transcription_shortlist_payload.get('conversation_pool_total', 0) or 0)}")
    lines.append(f"- discipline_pool_total: {int(transcription_shortlist_payload.get('discipline_pool_total', 0) or 0)}")
    lines.append(
        f"- deals_selected_for_transcription_total: {int(transcription_shortlist_payload.get('deals_selected_for_transcription_total', 0) or 0)}"
    )
    lines.append(f"- calls_selected_total: {int(transcription_shortlist_payload.get('calls_selected_total', 0) or 0)}")
    lines.append(
        f"- calls_filtered_noise_total: {int(transcription_shortlist_payload.get('calls_filtered_noise_total', 0) or 0)}"
    )
    lines.append("")
    lines.append("## Deals")
    items = transcription_shortlist_payload.get("items", []) if isinstance(transcription_shortlist_payload.get("items"), list) else []
    if not items:
        lines.append("- empty")
    else:
        for item in items:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- deal={deal} pool={pool} case={case_type} selected={selected} selected_calls={count} reason={reason}".format(
                    deal=str(item.get("deal_id") or ""),
                    pool=str(item.get("pool_type") or "none"),
                    case_type=str(item.get("call_case_type") or "unknown"),
                    selected=bool(item.get("selected_for_transcription")),
                    count=int(item.get("selected_call_count", 0) or 0),
                    reason=str(item.get("transcription_selection_reason") or ""),
                )
            )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _is_no_answer_like_call(call: dict[str, Any]) -> bool:
    text_parts = [
        str(call.get("status") or ""),
        str(call.get("result") or ""),
        str(call.get("disposition") or ""),
        " ".join(str(x) for x in (call.get("quality_flags") if isinstance(call.get("quality_flags"), list) else []) if str(x).strip()),
    ]
    haystack = " ".join(text_parts).strip().lower()
    if any(token in haystack for token in ("no_answer", "не дозвон", "недозвон", "busy", "занято", "unanswered", "missed")):
        return True
    try:
        duration = int(call.get("duration_seconds", 0) or 0)
    except (TypeError, ValueError):
        duration = 0
    return duration <= 3


def _is_autoanswer_like_call(call: dict[str, Any]) -> bool:
    text_parts = [
        str(call.get("status") or ""),
        str(call.get("result") or ""),
        str(call.get("disposition") or ""),
        " ".join(str(x) for x in (call.get("quality_flags") if isinstance(call.get("quality_flags"), list) else []) if str(x).strip()),
    ]
    haystack = " ".join(text_parts).strip().lower()
    return any(token in haystack for token in ("auto", "voicemail", "автоответ", "автоответчик", "robot"))


def _build_call_pool_debug_markdown(*, call_pool_debug: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Call Pool Debug (Pre-limit)")
    lines.append("")
    lines.append("## Aggregates")
    lines.append(f"- deals_total_before_limit: {int(call_pool_debug.get('deals_total_before_limit', 0) or 0)}")
    lines.append(f"- deals_with_any_calls: {int(call_pool_debug.get('deals_with_any_calls', 0) or 0)}")
    lines.append(f"- deals_with_recordings: {int(call_pool_debug.get('deals_with_recordings', 0) or 0)}")
    lines.append(f"- deals_with_long_calls: {int(call_pool_debug.get('deals_with_long_calls', 0) or 0)}")
    lines.append(f"- deals_with_only_short_calls: {int(call_pool_debug.get('deals_with_only_short_calls', 0) or 0)}")
    lines.append(f"- deals_with_autoanswer_pattern: {int(call_pool_debug.get('deals_with_autoanswer_pattern', 0) or 0)}")
    lines.append(f"- deals_with_redial_pattern: {int(call_pool_debug.get('deals_with_redial_pattern', 0) or 0)}")
    lines.append("")
    lines.append("## Deals")
    items = call_pool_debug.get("items", []) if isinstance(call_pool_debug.get("items"), list) else []
    if not items:
        lines.append("- no deals")
    else:
        for item in items:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- deal={deal} owner={owner} status={status} pipeline={pipeline} calls={calls} out={out} in={inn} max_dur={max_dur}s rec={rec} redial={redial} autoanswer={auto} unique_phones={phones} source={source} pool={pool} case={case_type} pool_score={pool_score}".format(
                    deal=str(item.get("deal_id") or ""),
                    owner=str(item.get("owner_name") or ""),
                    status=str(item.get("status_name") or ""),
                    pipeline=str(item.get("pipeline_name") or ""),
                    calls=int(item.get("calls_total", 0) or 0),
                    out=int(item.get("outbound_calls", 0) or 0),
                    inn=int(item.get("inbound_calls", 0) or 0),
                    max_dur=int(item.get("max_duration_seconds", 0) or 0),
                    rec=int(item.get("recording_url_count", 0) or 0),
                    redial=int(item.get("repeated_dead_redial_count", 0) or 0),
                    auto=int(item.get("autoanswer_like_count", 0) or 0),
                    phones=int(item.get("unique_phone_count", 0) or 0),
                    source=str(item.get("call_source_used") or ""),
                    pool=str(item.get("pool_type") or "none"),
                    case_type=str(item.get("call_case_type") or "unknown"),
                    pool_score=int(item.get("pool_priority_score", 0) or 0),
                )
            )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_call_runtime_diagnostics(
    period_deal_records: list[dict[str, Any]],
    *,
    call_collection_mode: str,
) -> dict[str, Any]:
    deals = [x for x in period_deal_records if isinstance(x, dict)]
    mode_counter: Counter[str] = Counter(
        str(x.get("call_source_used") or "").strip().lower() or "unknown"
        for x in deals
    )
    retry_reason_counter: Counter[str] = Counter(
        str(x.get("transcript_quality_retry_reason") or "").strip().lower()
        for x in deals
        if str(x.get("transcript_quality_retry_reason") or "").strip()
    )
    return {
        "call_collection_mode_effective": str(call_collection_mode or "").strip().lower() or "unknown",
        "call_source_used_counts": dict(mode_counter),
        "deals_with_call_candidates": sum(1 for x in deals if int(x.get("call_candidates_count", 0) or 0) > 0),
        "deals_with_recording_url": sum(1 for x in deals if int(x.get("recording_url_count", 0) or 0) > 0),
        "audio_downloaded": sum(int(x.get("audio_downloaded_count", 0) or 0) for x in deals),
        "audio_cached": sum(int(x.get("audio_cached_count", 0) or 0) for x in deals),
        "audio_failed": sum(int(x.get("audio_failed_count", 0) or 0) for x in deals),
        "transcription_attempted": sum(int(x.get("transcription_attempted_count", 0) or 0) for x in deals),
        "transcription_success": sum(int(x.get("transcription_success_count", 0) or 0) for x in deals),
        "transcription_failed": sum(int(x.get("transcription_failed_count", 0) or 0) for x in deals),
        "transcription_failed_missing_audio": sum(int(x.get("transcription_missing_audio_count", 0) or 0) for x in deals),
        "transcription_failed_backend_config": sum(int(x.get("transcription_backend_config_failed_count", 0) or 0) for x in deals),
        "transcript_quality_retry_used": sum(1 for x in deals if bool(x.get("transcript_quality_retry_used"))),
        "transcript_quality_retry_improved": sum(1 for x in deals if bool(x.get("transcript_quality_retry_improved"))),
        "transcript_quality_retry_reason_counts": dict(retry_reason_counter),
    }


def _build_top_risks_payload(*, period_deal_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = sorted(
        period_deal_records,
        key=lambda x: (
            len(x.get("risk_flags") if isinstance(x.get("risk_flags"), list) else []),
            -int(x.get("score")) if isinstance(x.get("score"), int) else 9999,
        ),
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for item in ranked:
        flags = item.get("risk_flags") if isinstance(item.get("risk_flags"), list) else []
        out.append(
            {
                "deal_id": item.get("deal_id"),
                "deal_name": item.get("deal_name", ""),
                "score": item.get("score"),
                "top_risk_flags": [str(x) for x in flags[:3]],
                "warnings": item.get("warnings", []) if isinstance(item.get("warnings"), list) else [],
                "artifact_path": item.get("artifact_path", ""),
            }
        )
    return out


def _build_period_summary_markdown(*, summary: dict[str, Any], period_deal_records: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append(f"# Analyze Period Run Summary ({summary.get('run_timestamp', '')})")
    lines.append("")
    lines.append("## Data Quality / Interpretation Confidence")
    confidence_counts = summary.get("analysis_confidence_counts", {}) if isinstance(summary.get("analysis_confidence_counts"), dict) else {}
    if confidence_counts:
        for key, val in sorted(confidence_counts.items(), key=lambda x: str(x[0])):
            lines.append(f"- analysis_confidence[{key}]: {val}")
    owner_ambiguity_deals = int(summary.get("owner_ambiguity_deals", 0) or 0)
    low_confidence_deals = sum(1 for item in period_deal_records if str(item.get("analysis_confidence") or "").lower() == "low")
    lines.append(f"- owner_ambiguity_deals: {owner_ambiguity_deals}")
    lines.append(f"- low_confidence_deals: {low_confidence_deals}")
    if low_confidence_deals > 0:
        lines.append("- Note: часть выводов ограничена качеством CRM-данных и/или owner ambiguity.")
    lines.append("")
    product_hypothesis_counts = Counter(
        str(item.get("product_hypothesis") or "unknown").strip().lower() or "unknown"
        for item in period_deal_records
    )
    call_aggregates = build_call_signal_aggregates(period_deal_records)
    lines.append("## Product Hypothesis (Batch)")
    lines.append(f"- info: {product_hypothesis_counts.get('info', 0)}")
    lines.append(f"- link: {product_hypothesis_counts.get('link', 0)}")
    lines.append(f"- mixed: {product_hypothesis_counts.get('mixed', 0)}")
    lines.append(f"- unknown: {product_hypothesis_counts.get('unknown', 0)}")
    lines.append("- CRM product и product hypothesis могут различаться: гипотеза строится из совокупности сигналов.")
    lines.append("")
    call_aggregates = summary.get("call_signal_aggregates", {}) if isinstance(summary.get("call_signal_aggregates"), dict) else {}
    lines.append("## Call-Aware Signals")
    lines.append(f"- Сделок с транскриптом: {call_aggregates.get('deals_with_transcript', 0)}")
    lines.append(f"- Где в разговоре есть next-step: {call_aggregates.get('deals_with_next_step_in_call', 0)}")
    lines.append(
        "- Где next-step в разговоре не отражен в CRM follow-up: "
        f"{call_aggregates.get('deals_next_step_in_call_but_missing_followup_in_crm', 0)}"
    )
    lines.append(
        "- Где по звонку вероятен wrong/mixed product: "
        f"{call_aggregates.get('deals_with_probable_wrong_or_mixed_product_by_call', 0)}"
    )
    lines.append(
        "- Где по звонку виден ранний objection pattern: "
        f"{call_aggregates.get('deals_with_early_objection_pattern', 0)}"
    )
    lines.append("")
    transcript_diag = summary.get("transcript_runtime_diagnostics", {}) if isinstance(summary.get("transcript_runtime_diagnostics"), dict) else {}
    lines.append("## Проверка транскрибации")
    lines.append(f"- Сделок дошло до call evidence: {transcript_diag.get('deals_with_any_call_evidence', 0)}")
    lines.append(f"- Сделок дошло до audio: {transcript_diag.get('deals_with_audio_path', 0)}")
    lines.append(f"- Сделок реально получили transcript: {transcript_diag.get('deals_with_transcript_text', 0)}")
    lines.append(f"- Сделок реально дали смысл в анализе: {transcript_diag.get('deals_with_nonempty_call_signal_summary', 0)}")
    lines.append(f"- Usable transcript: {transcript_diag.get('transcriptions_usable', 0)}")
    lines.append(f"- Weak transcript: {transcript_diag.get('transcriptions_weak', 0)}")
    lines.append(f"- Noisy transcript: {transcript_diag.get('transcriptions_noisy', 0)}")
    lines.append(f"- Empty transcript: {transcript_diag.get('transcriptions_empty', 0)}")
    lines.append(f"- Сделок с usable transcript: {transcript_diag.get('deals_with_usable_transcript', 0)}")
    lines.append(f"- Usable transcript: {transcript_diag.get('transcriptions_usable', 0)}")
    lines.append(f"- Weak transcript: {transcript_diag.get('transcriptions_weak', 0)}")
    lines.append(f"- Noisy transcript: {transcript_diag.get('transcriptions_noisy', 0)}")
    lines.append(f"- Empty transcript: {transcript_diag.get('transcriptions_empty', 0)}")
    lines.append(f"- Сделок с usable transcript: {transcript_diag.get('deals_with_usable_transcript', 0)}")
    if bool(transcript_diag.get("transcript_layer_effective")):
        lines.append("- Вывод: транскрибация реально участвует в анализе.")
    else:
        lines.append("- Вывод: в этом запуске транскрибация фактически не повлияла на анализ.")
    lines.append("")
    ref_diag = summary.get("daily_reference_stack", {}) if isinstance(summary.get("daily_reference_stack"), dict) else {}
    lines.append("## Reference Stack")
    lines.append(f"- rows_total: {ref_diag.get('rows_total', 0)}")
    lines.append(f"- rows_with_references: {ref_diag.get('rows_with_references', 0)}")
    lines.append(f"- external_retrieval_rows: {ref_diag.get('external_retrieval_rows', 0)}")
    lines.append(f"- rows_with_internal_required_ok: {ref_diag.get('rows_with_internal_required_ok', 0)}")
    lines.append(f"- rows_with_role_required_ok: {ref_diag.get('rows_with_role_required_ok', 0)}")
    lines.append(f"- rows_with_product_required_ok: {ref_diag.get('rows_with_product_required_ok', 0)}")
    if str(ref_diag.get("diagnostics_path") or "").strip():
        lines.append(f"- diagnostics: {ref_diag.get('diagnostics_path')}")
    lines.append("")
    call_diag = summary.get("call_runtime_diagnostics", {}) if isinstance(summary.get("call_runtime_diagnostics"), dict) else {}
    lines.append("## E2E проверка звонков")
    lines.append(f"- Найдено звонков: {call_diag.get('deals_with_call_candidates', 0)} сделок-кандидатов")
    lines.append(f"- Найдено записей: {call_diag.get('deals_with_recording_url', 0)} сделок с recording_url")
    lines.append(f"- Скачано: {call_diag.get('audio_downloaded', 0)}")
    lines.append(f"- Расшифровано: {call_diag.get('transcription_success', 0)}")
    lines.append(f"- Не дошло до текста из-за отсутствия аудио: {call_diag.get('transcription_failed_missing_audio', 0)}")
    lines.append(f"- Ошибки модели/бэкенда: {call_diag.get('transcription_failed_backend_config', 0)}")
    lines.append(f"- Retry качества транскрипта: {call_diag.get('transcript_quality_retry_used', 0)}")
    lines.append(f"- Retry дал улучшение: {call_diag.get('transcript_quality_retry_improved', 0)}")
    if int(call_diag.get("transcription_success", 0) or 0) > 0:
        lines.append("- Итог: call-layer реально работает.")
    else:
        lines.append("- Итог: call-layer не дошел до транскрипции.")
    lines.append("")
    lines.append("## Pre-limit Call Pools")
    lines.append(f"- conversation_pool_total: {summary.get('conversation_pool_total', 0)}")
    lines.append(f"- discipline_pool_total: {summary.get('discipline_pool_total', 0)}")
    lines.append(f"- lpr_conversation_total: {summary.get('lpr_conversation_total', 0)}")
    lines.append(f"- secretary_case_total: {summary.get('secretary_case_total', 0)}")
    lines.append(f"- supplier_inbound_total: {summary.get('supplier_inbound_total', 0)}")
    lines.append(f"- warm_inbound_total: {summary.get('warm_inbound_total', 0)}")
    lines.append(f"- redial_discipline_total: {summary.get('redial_discipline_total', 0)}")
    lines.append(f"- autoanswer_noise_total: {summary.get('autoanswer_noise_total', 0)}")
    lines.append("")
    discipline_summary = (
        summary.get("discipline_report_summary", {})
        if isinstance(summary.get("discipline_report_summary"), dict)
        else {}
    )
    lines.append("## negotiation_analysis")
    lines.append(
        "- conversation_pool={conv} | lpr={lpr} | secretary={sec} | supplier={sup} | warm={warm}".format(
            conv=int(summary.get("conversation_pool_total", 0) or 0),
            lpr=int(summary.get("lpr_conversation_total", 0) or 0),
            sec=int(summary.get("secretary_case_total", 0) or 0),
            sup=int(summary.get("supplier_inbound_total", 0) or 0),
            warm=int(summary.get("warm_inbound_total", 0) or 0),
        )
    )
    lines.append("")
    lines.append("## discipline_analysis")
    lines.append(
        "- discipline_pool={pool} | over_2_attempts={over2} | not_covered={nc} | short_cluster={sc} | same_time={st}".format(
            pool=int(summary.get("discipline_pool_total", 0) or 0),
            over2=int(discipline_summary.get("deals_over_2_attempts", 0) or 0),
            nc=int(discipline_summary.get("deals_numbers_not_fully_covered", 0) or 0),
            sc=int(discipline_summary.get("deals_short_call_cluster", 0) or 0),
            st=int(discipline_summary.get("deals_same_time_repeat_pattern", 0) or 0),
        )
    )
    lines.append("")
    shortlist_diag = summary.get("transcription_shortlist_diagnostics", {}) if isinstance(summary.get("transcription_shortlist_diagnostics"), dict) else {}
    lines.append("## Transcription Shortlist")
    lines.append(f"- conversation_pool_total: {shortlist_diag.get('conversation_pool_total', 0)}")
    lines.append(f"- analysis_shortlist_candidates_total: {shortlist_diag.get('analysis_shortlist_candidates_total', 0)}")
    lines.append(f"- analysis_shortlist_total: {shortlist_diag.get('analysis_shortlist_total', 0)}")
    lines.append(f"- analysis_shortlist_limit_applied: {shortlist_diag.get('analysis_shortlist_limit_applied')}")
    lines.append(f"- analysis_shortlist_forced_fallback_total: {shortlist_diag.get('analysis_shortlist_forced_fallback_total', 0)}")
    lines.append(f"- deals_selected_for_stt: {shortlist_diag.get('deals_selected_for_stt', 0)}")
    lines.append(f"- calls_selected_for_stt: {shortlist_diag.get('calls_selected_for_stt', 0)}")
    lines.append(
        f"- calls_filtered_short_no_answer_autoanswer: {shortlist_diag.get('calls_filtered_short_no_answer_autoanswer', 0)}"
    )
    lines.append("")
    lines.append("## Run Info")
    lines.append(f"- Backend requested: {summary.get('backend_requested', '')}")
    lines.append(f"- Backend used: {summary.get('analysis_backend_used', '')}")
    lines.append(f"- LLM overlay deals: {summary.get('llm_overlay_deals', 0)}")
    lines.append(f"- Deals seen: {summary.get('total_deals_seen', 0)}")
    lines.append(f"- Deals analyzed: {summary.get('total_deals_analyzed', 0)}")
    lines.append(f"- Deals failed: {summary.get('deals_failed', 0)}")
    live_refresh = summary.get("live_refresh", {}) if isinstance(summary.get("live_refresh"), dict) else {}
    if live_refresh:
        lines.append(
            "- Live refresh: "
            f"mode={live_refresh.get('mode','')} api_refresh_success={live_refresh.get('api_refresh_success', False)} "
            f"rows_final={live_refresh.get('rows_final', 0)}"
        )
        if str(live_refresh.get("error") or "").strip():
            lines.append(f"- Live refresh fallback reason: {live_refresh.get('error')}")
    cr_writer = summary.get("call_review_writer", {}) if isinstance(summary.get("call_review_writer"), dict) else {}
    lines.append(
        "- Call review writer: mode={mode} rows={rows} target={sheet}:{cell}".format(
            mode=cr_writer.get("mode", "dry_run"),
            rows=cr_writer.get("rows_written", 0),
            sheet=cr_writer.get("sheet_name", "") or "-",
            cell=cr_writer.get("start_cell", "") or "-",
        )
    )
    lines.append("")

    agg = summary.get("score_aggregates", {}) if isinstance(summary.get("score_aggregates"), dict) else {}
    lines.append("## Score Aggregates")
    lines.append(f"- Min: {agg.get('min')}")
    lines.append(f"- Max: {agg.get('max')}")
    lines.append(f"- Avg: {agg.get('avg')}")
    lines.append("")

    lines.append("## Top Risk Flags")
    risk_counts = summary.get("risk_flags_counts", {}) if isinstance(summary.get("risk_flags_counts"), dict) else {}
    qualified_items = [(k, v) for k, v in risk_counts.items() if str(k).startswith("qualified_loss:")]
    regular_items = [(k, v) for k, v in risk_counts.items() if not str(k).startswith("qualified_loss:")]
    if not regular_items:
        lines.append("- none")
    else:
        for risk, count in sorted(regular_items, key=lambda x: x[1], reverse=True):
            lines.append(f"- {risk}: {count}")
    lines.append("")
    meeting_focus = _build_weekly_meeting_focus(
        top_risk_patterns=sorted(regular_items, key=lambda x: x[1], reverse=True),
        period_deal_records=period_deal_records,
    )
    lines.append("## Weekly Meeting Focus")
    lines.append("### Что просело сильнее всего")
    if not meeting_focus["drops"]:
        lines.append("- Нет явного доминирующего провала в текущем срезе.")
    else:
        for item in meeting_focus["drops"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("### Что можно исправить за 1 неделю")
    for item in meeting_focus["one_week_actions"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### Что нельзя интерпретировать уверенно из-за качества CRM")
    if not meeting_focus["limits"]:
        lines.append("- Существенных ограничений интерпретации не зафиксировано.")
    else:
        for item in meeting_focus["limits"]:
            lines.append(f"- {item}")
    lines.append("")

    short_insights = [
        str(item.get("manager_insight_short") or "").strip()
        for item in period_deal_records
        if isinstance(item, dict)
    ]
    short_insights = [x for x in short_insights if x]
    if short_insights:
        lines.append("## Hybrid LLM Insights (Short)")
        for insight, count in Counter(short_insights).most_common(5):
            lines.append(f"- {insight} ({count})")
        lines.append("")
    lines.append("## Qualified Loss / Market Mismatch")
    if not qualified_items:
        lines.append("- none")
    else:
        for risk, count in sorted(qualified_items, key=lambda x: x[1], reverse=True):
            lines.append(f"- {risk}: {count}")
    lines.append("")

    ranked_risky = sorted(
        period_deal_records,
        key=lambda x: (
            len(x.get("risk_flags") if isinstance(x.get("risk_flags"), list) else []),
            -int(x.get("score")) if isinstance(x.get("score"), int) else 9999,
        ),
        reverse=True,
    )[:10]
    lines.append("## Top 10 Most Risky Deals")
    if not ranked_risky:
        lines.append("- none")
    for item in ranked_risky:
        warnings = item.get("warnings") if isinstance(item.get("warnings"), list) else []
        warn_note = " [warnings]" if warnings else ""
        lines.append(
            f"- deal={item.get('deal_id')} score={item.get('score')} risks={len(item.get('risk_flags', []))}{warn_note} name={item.get('deal_name', '')}"
        )
    lines.append("")

    non_loss_scores = [
        item for item in period_deal_records if not _is_loss_like_record(item)
    ]
    top_scores = sorted(
        non_loss_scores,
        key=lambda x: int(x.get("score")) if isinstance(x.get("score"), int) else -1,
        reverse=True,
    )[:10]
    lines.append("## Top 10 Highest Score Deals")
    if not top_scores:
        closed_loss_fallback = sorted(
            [item for item in period_deal_records if _is_loss_like_record(item)],
            key=lambda x: int(x.get("score")) if isinstance(x.get("score"), int) else -1,
            reverse=True,
        )[:10]
        lines.append("- Нет открытых/рабочих сделок для секции потенциала.")
        if closed_loss_fallback:
            lines.append("- Лучшие из закрытых (fallback):")
            for item in closed_loss_fallback:
                warnings = item.get("warnings") if isinstance(item.get("warnings"), list) else []
                warn_note = " [warnings]" if warnings else ""
                lines.append(
                    f"  - deal={item.get('deal_id')} score={item.get('score')}{warn_note} name={item.get('deal_name', '')}"
                )
    for item in top_scores:
        warnings = item.get("warnings") if isinstance(item.get("warnings"), list) else []
        warn_note = " [warnings]" if warnings else ""
        lines.append(
            f"- deal={item.get('deal_id')} score={item.get('score')}{warn_note} name={item.get('deal_name', '')}"
        )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_manager_brief_markdown(*, summary: dict[str, Any], period_deal_records: list[dict[str, Any]]) -> str:
    risk_counts = summary.get("risk_flags_counts", {}) if isinstance(summary.get("risk_flags_counts"), dict) else {}
    qualified_items = [(k, v) for k, v in risk_counts.items() if str(k).startswith("qualified_loss:")]
    regular_items = [(k, v) for k, v in risk_counts.items() if not str(k).startswith("qualified_loss:")]
    top_risk_patterns = sorted(regular_items, key=lambda x: x[1], reverse=True)[:5]
    top_qualified = sorted(qualified_items, key=lambda x: x[1], reverse=True)[:5]
    warnings_count = sum(
        1
        for item in period_deal_records
        if isinstance(item.get("warnings"), list) and len(item.get("warnings")) > 0
    )
    owner_ambiguity_count = sum(1 for item in period_deal_records if bool(item.get("owner_ambiguity_flag")))
    low_confidence_count = sum(1 for item in period_deal_records if str(item.get("analysis_confidence") or "").lower() == "low")
    queue_reason_counts = Counter(_derive_queue_reason(item) for item in period_deal_records if item.get("score") is not None)
    closed_lost_records = [item for item in period_deal_records if _is_loss_like_record(item)]
    product_hypothesis_counts = Counter(
        str(item.get("product_hypothesis") or "unknown").strip().lower() or "unknown"
        for item in period_deal_records
    )
    call_aggregates = build_call_signal_aggregates(period_deal_records)
    reanimation_counts = Counter(str(item.get("reanimation_potential") or "none") for item in closed_lost_records)
    reanimation_interesting = sorted(
        closed_lost_records,
        key=lambda x: (
            _reanimation_priority(str(x.get("reanimation_potential") or "none")),
            int(x.get("score")) if isinstance(x.get("score"), int) else 999,
            -_risk_count(x),
        ),
    )[:5]
    attention_deals = sorted(
        period_deal_records,
        key=lambda x: (
            len(x.get("risk_flags") if isinstance(x.get("risk_flags"), list) else []),
            -(int(x.get("score")) if isinstance(x.get("score"), int) else 0),
        ),
        reverse=True,
    )[:5]
    non_loss_candidates = [
        item for item in period_deal_records if not _is_loss_like_record(item)
    ]
    best_potential = sorted(
        non_loss_candidates,
        key=lambda x: (
            int(x.get("score")) if isinstance(x.get("score"), int) else -1,
            -len(x.get("risk_flags") if isinstance(x.get("risk_flags"), list) else []),
        ),
        reverse=True,
    )[:5]

    actions = _build_manager_brief_actions(
        top_risk_patterns=top_risk_patterns,
        summary=summary,
        period_deal_records=period_deal_records,
    )
    meeting_focus = _build_weekly_meeting_focus(
        top_risk_patterns=sorted(regular_items, key=lambda x: x[1], reverse=True),
        period_deal_records=period_deal_records,
    )
    short_insights = [
        str(item.get("manager_insight_short") or "").strip()
        for item in period_deal_records
        if isinstance(item, dict)
    ]
    short_insights = [x for x in short_insights if x]

    lines: list[str] = []
    lines.append("# Manager Brief")
    lines.append("")
    lines.append("## Период и запуск")
    lines.append(f"- Run timestamp: {summary.get('run_timestamp', '')}")
    lines.append(f"- Backend requested: {summary.get('backend_requested', '')}")
    lines.append(f"- Backend used: {summary.get('analysis_backend_used', '')}")
    lines.append(f"- LLM overlay deals: {summary.get('llm_overlay_deals', 0)}")
    lines.append("")
    lines.append("## Объем")
    lines.append(f"- Просмотрено сделок: {summary.get('total_deals_seen', 0)}")
    lines.append(f"- Проанализировано: {summary.get('total_deals_analyzed', 0)}")
    lines.append(f"- Упало: {summary.get('deals_failed', 0)}")
    lines.append(f"- Owner ambiguity: {owner_ambiguity_count}")
    lines.append(f"- Низкая надежность интерпретации: {low_confidence_count}")
    lines.append("")
    lines.append("## Разбиение queue по категориям")
    lines.append(f"- Живые риски: {queue_reason_counts.get('active_risk', 0)}")
    lines.append(f"- Проверка передачи: {queue_reason_counts.get('won_handoff_check', 0)}")
    lines.append(f"- Низкая надежность / ручная проверка: {queue_reason_counts.get('low_confidence_needs_manual_check', 0)}")
    lines.append(f"- Qualified loss паттерны: {queue_reason_counts.get('qualified_loss_for_pattern_review', 0)}")
    lines.append(f"- Закрытые потери на cleanup-разбор: {queue_reason_counts.get('closed_lost_cleanup_review', 0)}")
    lines.append("")
    lines.append("## Гипотеза по продуктам в разборе")
    lines.append(f"- info: {product_hypothesis_counts.get('info', 0)}")
    lines.append(f"- link: {product_hypothesis_counts.get('link', 0)}")
    lines.append(f"- mixed: {product_hypothesis_counts.get('mixed', 0)}")
    lines.append(f"- unknown: {product_hypothesis_counts.get('unknown', 0)}")
    lines.append("- Это hypothesis layer: может отличаться от CRM-поля продукта при неполной фиксации.")
    lines.append("")
    lines.append("## Call-aware срез")
    lines.append(f"- Сделок с транскриптом: {call_aggregates.get('deals_with_transcript', 0)}")
    lines.append(f"- В разговоре есть следующий шаг: {call_aggregates.get('deals_with_next_step_in_call', 0)}")
    lines.append(
        "- Next-step в разговоре, но без CRM follow-up: "
        f"{call_aggregates.get('deals_next_step_in_call_but_missing_followup_in_crm', 0)}"
    )
    lines.append(
        "- Вероятный wrong/mixed product по звонку: "
        f"{call_aggregates.get('deals_with_probable_wrong_or_mixed_product_by_call', 0)}"
    )
    lines.append(
        "- Ранние objection-паттерны в разговоре: "
        f"{call_aggregates.get('deals_with_early_objection_pattern', 0)}"
    )
    lines.append("")
    discipline_summary = (
        summary.get("discipline_report_summary", {})
        if isinstance(summary.get("discipline_report_summary"), dict)
        else {}
    )
    lines.append("## negotiation_analysis")
    lines.append(
        "- conversation_pool={conv} | lpr={lpr} | secretary={sec} | supplier={sup} | warm={warm}".format(
            conv=int(summary.get("conversation_pool_total", 0) or 0),
            lpr=int(summary.get("lpr_conversation_total", 0) or 0),
            sec=int(summary.get("secretary_case_total", 0) or 0),
            sup=int(summary.get("supplier_inbound_total", 0) or 0),
            warm=int(summary.get("warm_inbound_total", 0) or 0),
        )
    )
    lines.append("")
    lines.append("## discipline_analysis")
    lines.append(
        "- discipline_pool={pool} | over_2_attempts={over2} | not_covered={nc} | short_cluster={sc} | same_time={st}".format(
            pool=int(summary.get("discipline_pool_total", 0) or 0),
            over2=int(discipline_summary.get("deals_over_2_attempts", 0) or 0),
            nc=int(discipline_summary.get("deals_numbers_not_fully_covered", 0) or 0),
            sc=int(discipline_summary.get("deals_short_call_cluster", 0) or 0),
            st=int(discipline_summary.get("deals_same_time_repeat_pattern", 0) or 0),
        )
    )
    lines.append("")
    transcript_diag = summary.get("transcript_runtime_diagnostics", {}) if isinstance(summary.get("transcript_runtime_diagnostics"), dict) else {}
    lines.append("## Проверка транскрибации")
    lines.append(f"- Сделок дошло до call evidence: {transcript_diag.get('deals_with_any_call_evidence', 0)}")
    lines.append(f"- Сделок дошло до audio: {transcript_diag.get('deals_with_audio_path', 0)}")
    lines.append(f"- Сделок реально получили transcript: {transcript_diag.get('deals_with_transcript_text', 0)}")
    lines.append(f"- Сделок реально дали смысл в анализе: {transcript_diag.get('deals_with_nonempty_call_signal_summary', 0)}")
    if bool(transcript_diag.get("transcript_layer_effective")):
        lines.append("- Вывод: транскрибация реально участвует в анализе.")
    else:
        lines.append("- Вывод: в этом запуске транскрибация фактически не повлияла на анализ.")
    lines.append("")
    lines.append("## Потенциал реанимации закрытых потерь")
    lines.append(f"- high: {reanimation_counts.get('high', 0)}")
    lines.append(f"- medium: {reanimation_counts.get('medium', 0)}")
    lines.append(f"- low: {reanimation_counts.get('low', 0)}")
    lines.append(f"- none: {reanimation_counts.get('none', 0)}")
    if reanimation_interesting:
        lines.append("- Топ кейсов для повторного захода:")
        for item in reanimation_interesting:
            lines.append(
                "  - deal={deal} | potential={potential} | score={score} | {reason}".format(
                    deal=item.get("deal_id"),
                    potential=item.get("reanimation_potential", "none"),
                    score=item.get("score"),
                    reason=str(item.get("reanimation_reason_short") or "").strip() or "без комментария",
                )
            )
    lines.append("")
    lines.append("## 5 главных риск-паттернов")
    if not top_risk_patterns:
        lines.append("- Нет доминирующих риск-паттернов")
    else:
        for risk, count in top_risk_patterns:
            lines.append(f"- {risk}: {count}")
    lines.append("")
    lines.append("## Qualified loss / market mismatch")
    if not top_qualified:
        lines.append("- Нет явных qualified loss паттернов")
    else:
        for risk, count in top_qualified:
            lines.append(f"- {risk}: {count}")
    lines.append("")
    lines.append("## Что просело сильнее всего")
    if not meeting_focus["drops"]:
        lines.append("- Нет явного доминирующего провала в текущем срезе.")
    else:
        for item in meeting_focus["drops"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## Что можно исправить за 1 неделю")
    for item in meeting_focus["one_week_actions"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## Что нельзя интерпретировать уверенно из-за качества CRM")
    if not meeting_focus["limits"]:
        lines.append("- Существенных ограничений интерпретации не зафиксировано.")
    else:
        for item in meeting_focus["limits"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## 5 сделок, требующих внимания")
    if not attention_deals:
        lines.append("- Нет данных")
    else:
        for item in attention_deals:
            warn = " (есть warnings)" if isinstance(item.get("warnings"), list) and item.get("warnings") else ""
            lines.append(
                f"- deal={item.get('deal_id')} | score={item.get('score')} | risks={len(item.get('risk_flags', []))}{warn} | {item.get('deal_name', '')}"
            )
    lines.append("")
    lines.append("## 5 сделок с лучшим потенциалом")
    if not best_potential:
        closed_loss_fallback = sorted(
            [item for item in period_deal_records if _is_loss_like_record(item)],
            key=lambda x: int(x.get("score")) if isinstance(x.get("score"), int) else -1,
            reverse=True,
        )[:5]
        lines.append("- Нет открытых/рабочих сделок для блока потенциала.")
        if closed_loss_fallback:
            lines.append("- Лучшие из проанализированных, но закрытых:")
            for item in closed_loss_fallback:
                warn = " (есть warnings)" if isinstance(item.get("warnings"), list) and item.get("warnings") else ""
                lines.append(
                    f"  - deal={item.get('deal_id')} | score={item.get('score')} | risks={len(item.get('risk_flags', []))}{warn} | {item.get('deal_name', '')}"
                )
    else:
        for item in best_potential:
            warn = " (есть warnings)" if isinstance(item.get("warnings"), list) and item.get("warnings") else ""
            lines.append(
                f"- deal={item.get('deal_id')} | score={item.get('score')} | risks={len(item.get('risk_flags', []))}{warn} | {item.get('deal_name', '')}"
            )
    lines.append("")
    lines.append("## Что делать дальше")
    for action in actions:
        lines.append(f"- {action}")
    if low_confidence_count > 0 or owner_ambiguity_count > 0:
        lines.append("- Перед персональными выводами по low-confidence кейсам подтвердить фактического ведущего и полноту CRM-контекста.")
    lines.append("")
    if short_insights:
        lines.append("## Короткие управленческие инсайты (LLM, опционально)")
        for insight, count in Counter(short_insights).most_common(5):
            lines.append(f"- {insight} ({count})")
        lines.append("")
    if warnings_count > 0:
        lines.append(f"_Технические предупреждения snapshot: {warnings_count} сделок (детали в deals/*.json и top_risks.json)._")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_manager_brief_actions(
    *,
    top_risk_patterns: list[tuple[str, int]],
    summary: dict[str, Any],
    period_deal_records: list[dict[str, Any]],
) -> list[str]:
    actions: list[str] = []
    deals_failed = int(summary.get("deals_failed", 0) or 0)
    analyzed_records = [x for x in period_deal_records if x.get("score") is not None]
    all_loss_batch = bool(analyzed_records) and all(_is_loss_like_record(x) for x in analyzed_records)
    if all_loss_batch:
        return [
            "Провести cleanup closed-lost: валидировать причину потери в каждой сделке и убрать пустые формулировки.",
            "Сгруппировать потери по типам (anti-fit, market mismatch, цена, сроки) и обновить сегментную классификацию.",
            "Исключить all-loss кейсы из стандартного pipeline pressure path, оставить только корректный closeout-контур.",
            "Подготовить короткий coaching-разбор по топ-3 причинам потерь для команды.",
            "Зафиксировать корректные критерии раннего отсева нецелевых сделок на этапе квалификации.",
        ]
    if deals_failed > 0:
        actions.append("Разобрать упавшие сделки из artifacts и закрыть причины падения пайплайна анализа.")
    if top_risk_patterns:
        actions.append("Провести короткий разбор топ-рисков с менеджерами по проблемным сделкам.")
    actions.append("Проверить, что по приоритетным сделкам зафиксирован следующий шаг и срок в CRM.")
    actions.append("Назначить follow-up по сделкам из списка внимания с контролем результата.")
    actions.append("Отдельно прогнать 1:1 коучинг по повторяющимся рискам из отчета.")
    if int(summary.get("total_deals_analyzed", 0) or 0) > 0:
        actions.append("Использовать сделки с лучшим потенциалом как эталоны для команды.")
    return actions[:7]


def _build_manager_weekly_markdown(*, manager_name: str, role_focus: str, records: list[dict[str, Any]]) -> str:
    lines: list[str] = [f"# {manager_name} Weekly", ""]
    lines.append(f"- Фокус роли: {role_focus}")
    lines.append(f"- Сделок в разборе: {len(records)}")
    lines.append("")

    if not records:
        lines.append("## Что было хорошо за неделю")
        lines.append("- Недостаточно данных по этому менеджеру в выбранном срезе.")
        lines.append("")
        lines.append("## Что просело")
        lines.append("- Недостаточно данных для уверенного вывода.")
        lines.append("")
        lines.append("## Повторяющиеся ошибки")
        lines.append("- Нет стабильного паттерна: данных мало.")
        lines.append("")
        lines.append("## Что дать в работу на следующую неделю")
        lines.append("- Заполнить базовый CRM-контекст по ключевым сделкам (notes, next-step, причина статуса).")
        lines.append("")
        lines.append("## Что сказать лично менеджеру")
        lines.append("- Начинаем с дисциплины фиксации фактов в CRM, затем переходим к точечной тактике.")
        lines.append("")
        lines.append("## Ожидаемый эффект")
        lines.append("- Повышение надежности интерпретации и более точный управленческий разбор.")
        lines.append("")
        lines.append("## Что нельзя трактовать слишком уверенно из-за качества CRM")
        lines.append("- По текущему срезу выводы ограничены отсутствием достаточных данных.")
        lines.append("")
        return "\n".join(lines).strip() + "\n"

    risk_counts: Counter[str] = Counter()
    score_values = [int(x.get("score")) for x in records if isinstance(x.get("score"), int)]
    low_conf = sum(1 for x in records if _is_low_confidence_record(x))
    for item in records:
        for flag in item.get("risk_flags", []) if isinstance(item.get("risk_flags"), list) else []:
            text = str(flag).strip()
            if text:
                risk_counts[text] += 1
    repeated = [k for k, _ in risk_counts.most_common(5)]
    strong_cases = sorted(
        [x for x in records if isinstance(x.get("score"), int)],
        key=lambda x: int(x.get("score")),
        reverse=True,
    )[:3]

    lines.append("## Что было хорошо за неделю")
    if strong_cases:
        for item in strong_cases:
            lines.append(
                f"- {item.get('deal_name') or item.get('deal_id')}: score={item.get('score')}, статус={item.get('status_or_stage') or '-'}."
            )
    else:
        lines.append("- Явных позитивных кейсов не зафиксировано.")
    lines.append("")

    lines.append("## Что просело")
    if not repeated:
        lines.append("- Критичные провалы не доминируют в текущем срезе.")
    else:
        for risk in repeated[:3]:
            lines.append(f"- {risk}")
    lines.append("")

    lines.append("## Повторяющиеся ошибки")
    if not repeated:
        lines.append("- Повторяемые ошибки не выражены.")
    else:
        for risk in repeated[:5]:
            lines.append(f"- {risk}")
    lines.append("")

    lines.append("## Что дать в работу на следующую неделю")
    if "Рустам" in manager_name:
        lines.append("- По активным сделкам до этапа встречи: фиксировать next-step и дату касания в тот же день.")
        lines.append("- Уточнять ЛПР/критерии закупки до перехода в следующий этап.")
        lines.append("- При low-confidence кейсах сначала восстановить контекст, потом давать жесткие выводы.")
    else:
        lines.append("- По сделкам после интереса: закрыть демо/тест результат короткими фактами в CRM.")
        lines.append("- На этапах счет/оплата контролировать конкретный follow-up с дедлайном.")
        lines.append("- По закрытым потерям: разделять qualified-loss и cleanup-кейсы без давления.")
    lines.append("")

    lines.append("## Что сказать лично менеджеру")
    if "Рустам" in manager_name:
        lines.append("- Твоя зона влияния — качество квалификации и выход на встречу; держи ритм и фиксируй факты сразу.")
    else:
        lines.append("- Твоя зона влияния — качество дожима после демо/теста; меньше общих формулировок, больше конкретики в CRM.")
    lines.append("")

    lines.append("## Ожидаемый эффект")
    lines.append("- Снижение доли шумных low-confidence кейсов и более управляемая очередь рисков.")
    lines.append("- Больше кейсов с понятной причиной статуса и следующим действием.")
    lines.append("")

    lines.append("## Что нельзя трактовать слишком уверенно из-за качества CRM")
    if low_conf == 0:
        lines.append("- Критических ограничений интерпретации по этому менеджеру не выявлено.")
    else:
        lines.append(f"- Low-confidence/owner-ambiguity кейсы: {low_conf}.")
        lines.append("- По этим сделкам сначала проверять фактического ведущего и полноту CRM-фактов.")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_weekly_meeting_brief_markdown(
    *,
    records: list[dict[str, Any]],
    meeting_queue: list[dict[str, Any]],
    manager_contains: str | None,
) -> str:
    lines: list[str] = ["# Weekly Meeting Brief", ""]
    lines.append(f"- Менеджерский фильтр: {str(manager_contains or '').strip() or '-'}")
    lines.append(f"- Сделок в weekly срезе: {len(records)}")
    lines.append(f"- Сделок в очереди обсуждения: {len(meeting_queue)}")
    lines.append("")

    risk_counts: Counter[str] = Counter()
    queue_reason_counts: Counter[str] = Counter()
    for item in records:
        for flag in item.get("risk_flags", []) if isinstance(item.get("risk_flags"), list) else []:
            text = str(flag).strip()
            if text:
                risk_counts[text] += 1
        queue_reason_counts[_derive_queue_reason(item)] += 1
    call_aggregates = build_call_signal_aggregates(records)

    lines.append("## Краткий итог недели")
    if not records:
        lines.append("- Нет данных в weekly-срезе.")
    else:
        avg_score = round(sum(int(x.get("score")) for x in records if isinstance(x.get("score"), int)) / max(1, len(records)), 2)
        lines.append(f"- Средний score по срезу: {avg_score}")
        lines.append(f"- Живые риски: {queue_reason_counts.get('active_risk', 0)}")
        lines.append(f"- Qualified loss паттерны: {queue_reason_counts.get('qualified_loss_for_pattern_review', 0)}")
    lines.append("")

    lines.append("## Что изменилось по команде")
    lines.append(f"- Рустам: {sum(1 for x in records if 'рустам' in str(x.get('owner_name') or '').lower())} сделок в срезе.")
    lines.append(f"- Илья: {sum(1 for x in records if 'илья' in str(x.get('owner_name') or '').lower())} сделок в срезе.")
    lines.append("")
    lines.append("## Call-aware сигналы недели")
    lines.append(f"- Сделок с транскриптом: {call_aggregates.get('deals_with_transcript', 0)}")
    lines.append(
        "- Где next-step в разговоре не отражен в CRM: "
        f"{call_aggregates.get('deals_next_step_in_call_but_missing_followup_in_crm', 0)}"
    )
    lines.append(
        "- Где по звонку вероятен wrong/mixed product: "
        f"{call_aggregates.get('deals_with_probable_wrong_or_mixed_product_by_call', 0)}"
    )
    lines.append(
        "- Где рано всплывают objection-паттерны: "
        f"{call_aggregates.get('deals_with_early_objection_pattern', 0)}"
    )
    lines.append("")

    lines.append("## Где узкое место в воронке")
    if not risk_counts:
        lines.append("- Узкое место не выражено: данных мало.")
    else:
        for risk, count in risk_counts.most_common(3):
            lines.append(f"- {risk} ({count})")
    lines.append("")

    lines.append("## Что говорить на собрании")
    lines.append("- Разделить живые риски и шумные кейсы с низкой надежностью интерпретации.")
    lines.append("- По активным сделкам требовать конкретный next-step и дедлайн.")
    lines.append("- По закрытым потерям: отдельно qualified-loss и cleanup-разбор.")
    lines.append("")

    lines.append("## 3-5 главных управленческих акцентов")
    accents = [
        "Не смешивать low-confidence кейсы с доказанными процессными ошибками.",
        "Сначала исправить дисциплину CRM-фактов, затем усиливать pressure на воронку.",
        "Рустам: усилить качество квалификации и переход к встрече.",
        "Илья: усилить фиксацию результатов демо/теста и follow-up после них.",
        "Closed-lost анализировать как паттерны и cleanup, а не как единый пул.",
    ]
    for item in accents[:5]:
        lines.append(f"- {item}")
    lines.append("")

    lines.append("## Что нельзя интерпретировать уверенно")
    low_conf = sum(1 for x in records if _is_low_confidence_record(x))
    if low_conf == 0:
        lines.append("- Существенных ограничений интерпретации не выявлено.")
    else:
        lines.append(f"- Low-confidence/owner-ambiguity кейсы: {low_conf}.")
        lines.append("- По ним сначала ручная проверка контекста, потом персональные выводы.")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_next_week_plan_markdown(
    *,
    rustam_records: list[dict[str, Any]],
    ilya_records: list[dict[str, Any]],
    meeting_queue: list[dict[str, Any]],
) -> str:
    lines: list[str] = ["# Next Week Plan", ""]
    top_queue = meeting_queue[:5]
    focus_text = ", ".join(str(x.get("deal_id")) for x in top_queue if x.get("deal_id")) or "-"

    days = [
        ("Monday", "Проверить качество входа в неделю по активным рискам."),
        ("Tuesday", "Сфокусироваться на квалификации и фиксации причин статусов."),
        ("Wednesday", "Промежуточный контроль демо/тест/follow-up."),
        ("Thursday", "Разобрать закрытые потери: qualified-loss vs cleanup."),
        ("Friday", "Подвести неделю и зафиксировать next-step на следующую."),
    ]

    for day, theme in days:
        lines.append(f"## {day}")
        lines.append(f"- Что проверить: {theme}")
        lines.append("- Что сказать на ежедневке: работаем от фактов в CRM, без общих формулировок.")
        if day in {"Monday", "Tuesday"}:
            lines.append("- Какую задачу дать: Рустаму — по каждому приоритетному кейсу зафиксировать ЛПР/next-step/дату контакта.")
            lines.append("- По кому фокус: Рустам.")
            lines.append("- Ожидаемый эффект: меньше зависаний на ранних этапах и меньше шумных потерь.")
        elif day in {"Wednesday", "Thursday"}:
            lines.append("- Какую задачу дать: Илье — по warm-кейсам фиксировать результат демо/теста и follow-up с дедлайном.")
            lines.append("- По кому фокус: Илья.")
            lines.append("- Ожидаемый эффект: рост управляемости на этапах после интереса.")
        else:
            lines.append("- Какую задачу дать: закрыть пробелы в low-confidence кейсах и согласовать приоритеты на следующую неделю.")
            lines.append("- По кому фокус: оба, с разделением зон ответственности.")
            lines.append("- Ожидаемый эффект: более точный weekly briefing без ложной строгости.")
        lines.append(f"- Фокус-сделки: {focus_text}")
        lines.append("")

    lines.append("## Примечание")
    lines.append("- План рабочий и гибкий: если появляются новые факты, корректируем фокус внутри дня.")
    lines.append(f"- Рустам в weekly-срезе: {len(rustam_records)} сделок, Илья: {len(ilya_records)} сделок.")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_weekly_meeting_focus(
    *,
    top_risk_patterns: list[tuple[str, int]],
    period_deal_records: list[dict[str, Any]],
) -> dict[str, list[str]]:
    drops: list[str] = []
    for risk, count in top_risk_patterns[:3]:
        drops.append(f"{risk} ({count})")

    one_week_actions: list[str] = []
    for risk, _count in top_risk_patterns:
        key = str(risk).lower()
        if "process_hygiene" in key and "follow-up" in key:
            one_week_actions.append("Ввести ежедневный контроль follow-up: дедлайн + ответственный + expected outcome.")
        elif "evidence_context" in key:
            one_week_actions.append("За неделю закрыть CRM-пробелы по notes/tasks для top-risk сделок (короткий чек-лист полноты).")
        elif "qualified_loss" in key:
            one_week_actions.append("Сверить закрытые потери по anti-fit/market mismatch и убрать их из pressure-пайплайна.")
    if not one_week_actions:
        one_week_actions.append("Сфокусироваться на 5 сделках внимания и зафиксировать по каждой следующий шаг на 7 дней.")
    one_week_actions = _dedup_preserve(one_week_actions)[:5]

    low_confidence_count = sum(1 for item in period_deal_records if str(item.get("analysis_confidence") or "").lower() == "low")
    owner_ambiguity_count = sum(1 for item in period_deal_records if bool(item.get("owner_ambiguity_flag")))
    closed_lost_noise_count = sum(
        1
        for item in period_deal_records
        if _is_loss_like_record(item)
        and (
            str(item.get("analysis_confidence") or "").lower() == "low"
            or bool(item.get("owner_ambiguity_flag"))
            or (isinstance(item.get("warnings"), list) and len(item.get("warnings")) > 0)
        )
    )
    limits: list[str] = []
    if low_confidence_count > 0:
        limits.append(f"Низкая надежность интерпретации: {low_confidence_count} сделок.")
    if owner_ambiguity_count > 0:
        limits.append(f"Owner ambiguity: {owner_ambiguity_count} сделок (нужна проверка фактического ведущего).")
    if closed_lost_noise_count > 0:
        limits.append(f"Closed-lost noise: {closed_lost_noise_count} сделок требуют аккуратного closeout review без обвинительных выводов.")

    return {"drops": drops, "one_week_actions": one_week_actions, "limits": limits}


def _dedup_preserve(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _llm_overlay_fields_filled(item: dict[str, Any]) -> list[str]:
    filled: list[str] = []
    if str(item.get("product_hypothesis_llm") or "").strip().lower() in {"info", "link", "mixed"}:
        filled.append("product_hypothesis_llm")
    for key in ("loss_reason_short", "manager_insight_short", "coaching_hint_short", "reanimation_reason_short_llm"):
        if " ".join(str(item.get(key) or "").strip().split()):
            filled.append(key)
    return filled


def _is_loss_like_record(item: dict[str, Any]) -> bool:
    flags = item.get("risk_flags") if isinstance(item.get("risk_flags"), list) else []
    if any(str(flag).startswith("qualified_loss:") for flag in flags):
        return True
    status_name = str(item.get("status_name") or "").lower()
    return ("закрыто" in status_name and "не реализ" in status_name) or ("закрыто" in status_name and "отказ" in status_name)


def _apply_transcript_signal_overlays(*, analysis: dict[str, Any], deal: dict[str, Any]) -> dict[str, Any]:
    out = dict(analysis)
    risk_flags = out.get("risk_flags") if isinstance(out.get("risk_flags"), list) else []
    normalized = [_normalize_flag_text(x) for x in risk_flags]
    transcript_available = bool(out.get("transcript_available"))
    if not transcript_available:
        out["risk_flags"] = _dedup_preserve([str(x) for x in risk_flags])
        return out

    has_next_step_call = bool(out.get("call_signal_next_step_present"))
    if has_next_step_call:
        removed = []
        keep = []
        for raw, norm in zip(risk_flags, normalized):
            if "нет follow-up задач" in norm or "missing follow-up" in norm:
                removed.append(str(raw))
                continue
            keep.append(str(raw))
        if removed:
            keep.append("evidence_context: По звонку есть next-step, но он не зафиксирован как CRM follow-up.")
            risk_flags = keep
            normalized = [_normalize_flag_text(x) for x in risk_flags]

    status_name = str(deal.get("status_name") or out.get("status_name") or "").lower()
    is_closed_lost = ("закрыто" in status_name and "не реализ" in status_name) or ("закрыто" in status_name and "отказ" in status_name)
    has_qualified_loss = any(str(flag).startswith("qualified_loss:") for flag in risk_flags)
    if is_closed_lost and not has_qualified_loss:
        if bool(out.get("call_signal_objection_not_target")):
            risk_flags.append("qualified_loss: По звонку есть признаки нецелевого кейса/anti-fit.")
        elif bool(out.get("call_signal_objection_no_need")):
            risk_flags.append("qualified_loss: По звонку клиент фиксирует отсутствие текущей потребности.")

    if bool(out.get("call_signal_product_info")) and bool(out.get("call_signal_product_link")):
        if str(out.get("product_hypothesis") or "unknown").lower() == "unknown":
            out["product_hypothesis"] = "mixed"
            out["product_hypothesis_confidence"] = "low"
            out["product_hypothesis_reason_short"] = "По разговору есть сигналы и INFO, и LINK: требуется ручная валидация."
    elif bool(out.get("call_signal_product_info")) and str(out.get("product_hypothesis") or "unknown").lower() == "unknown":
        out["product_hypothesis"] = "info"
        out["product_hypothesis_confidence"] = "medium"
        out["product_hypothesis_reason_short"] = "Гипотеза INFO усилена сигналами разговора."
    elif bool(out.get("call_signal_product_link")) and str(out.get("product_hypothesis") or "unknown").lower() == "unknown":
        out["product_hypothesis"] = "link"
        out["product_hypothesis_confidence"] = "medium"
        out["product_hypothesis_reason_short"] = "Гипотеза LINK усилена сигналами разговора."

    if bool(out.get("owner_ambiguity_flag")) or str(out.get("analysis_confidence") or "").lower() == "low":
        conf = str(out.get("product_hypothesis_confidence") or "low").lower()
        if conf == "high":
            out["product_hypothesis_confidence"] = "medium"
        elif conf == "medium":
            out["product_hypothesis_confidence"] = "low"

    if is_closed_lost and has_next_step_call:
        potential = str(out.get("reanimation_potential") or "none").lower()
        if potential in {"none", "low"}:
            out["reanimation_potential"] = "medium"
            out["reanimation_reason_short"] = (
                "По разговору зафиксирован следующий шаг, но кейс закрыт: возможна аккуратная реанимация после ручной валидации."
            )
            out["reanimation_next_step"] = (
                "Сверить фактический срыв next-step и сделать короткий re-qualification контакт с одним конкретным действием."
            )
            out["reanimation_risk_note"] = "Риск повторного закрытия без фиксации причин и ответственного."

    out["risk_flags"] = _dedup_preserve([str(x) for x in risk_flags if str(x).strip()])
    return out


def _normalize_flag_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _derive_transcript_meta(*, snapshot: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {
            "transcript_source": "",
            "transcript_error": "",
        }
    transcripts = snapshot.get("transcripts")
    if not isinstance(transcripts, list) or not transcripts:
        return {
            "transcript_source": "",
            "transcript_error": "",
        }
    source = ""
    error = ""
    for item in transcripts:
        if not isinstance(item, dict):
            continue
        text = str(item.get("transcript_text") or "").strip()
        status = str(item.get("transcript_status") or "").strip().lower()
        if text and status in {"ok", "cached"}:
            source = str(item.get("transcript_source") or item.get("transcript_backend") or "").strip()
            break
    if not source:
        first = transcripts[0] if isinstance(transcripts[0], dict) else {}
        source = str(first.get("transcript_source") or first.get("transcript_backend") or "").strip()
    for item in transcripts:
        if not isinstance(item, dict):
            continue
        status = str(item.get("transcript_status") or "").strip().lower()
        err = str(item.get("transcript_error") or "").strip()
        if status not in {"ok", "cached", "disabled"} and err:
            error = err
            break
    return {
        "transcript_source": source,
        "transcript_error": error,
    }


def _derive_reanimation_fields(*, analysis: dict[str, Any], deal: dict[str, Any]) -> dict[str, str]:
    status_name = str(deal.get("status_name") or analysis.get("status_name") or "").strip()
    status_norm = status_name.lower()
    is_closed_lost = ("закрыто" in status_norm and "не реализ" in status_norm) or ("закрыто" in status_norm and "отказ" in status_norm)
    if not is_closed_lost:
        return {
            "reanimation_potential": "none",
            "reanimation_reason_short": "Сделка не в статусе closed-lost: реанимация не применима.",
            "reanimation_next_step": "",
            "reanimation_risk_note": "",
        }

    risk_flags = analysis.get("risk_flags") if isinstance(analysis.get("risk_flags"), list) else []
    data_quality_flags = analysis.get("data_quality_flags") if isinstance(analysis.get("data_quality_flags"), list) else []
    confidence_low = str(analysis.get("analysis_confidence") or "").strip().lower() == "low"
    owner_ambiguity = bool(analysis.get("owner_ambiguity_flag"))
    has_qualified_loss = any(str(flag).startswith("qualified_loss:") for flag in risk_flags)
    has_market_mismatch = any(
        token in str(flag).lower() for flag in risk_flags for token in ("market mismatch", "рыноч", "anti-fit", "нецелев")
    )
    has_followup_gap = any("follow-up" in str(flag).lower() for flag in risk_flags)
    has_reason_gap = any(
        str(flag).lower() in {"closed_lost_without_documented_reason", "crm_context_missing_with_stage_movement"}
        for flag in data_quality_flags
    ) or any("нет содержательных notes/tasks" in str(flag).lower() for flag in risk_flags)

    if has_qualified_loss and has_market_mismatch:
        return {
            "reanimation_potential": "none",
            "reanimation_reason_short": "Зафиксирован qualified-loss с признаками anti-fit/market mismatch: повторный заход обычно нецелесообразен.",
            "reanimation_next_step": "Закрыть кейс в cleanup-контуре и использовать как сегментный паттерн потерь.",
            "reanimation_risk_note": "Риск ложного дожима и потери ресурса на нецелевой кейс.",
        }
    if has_qualified_loss:
        return {
            "reanimation_potential": "low",
            "reanimation_reason_short": "Qualified-loss зафиксирован: ограниченный потенциал возврата только при новых вводных.",
            "reanimation_next_step": "Проверить, появились ли новые факты/изменения у клиента перед повторным контактом.",
            "reanimation_risk_note": "Высокий риск повторного отказа без изменения условий кейса.",
        }

    if confidence_low or owner_ambiguity:
        potential = "medium" if (has_followup_gap or has_reason_gap) else "low"
        return {
            "reanimation_potential": potential,
            "reanimation_reason_short": "Вывод ограничен качеством CRM-данных: потенциал реанимации оценен консервативно.",
            "reanimation_next_step": "Сначала подтвердить фактического владельца и причину потери, затем решать о повторном заходе.",
            "reanimation_risk_note": "Риск ошибочной реанимации из-за неполной/неточной атрибуции в CRM.",
        }

    if has_followup_gap and has_reason_gap:
        return {
            "reanimation_potential": "high",
            "reanimation_reason_short": "Потеря похожа на операционный срыв (нет next-step и нет качественной фиксации причины).",
            "reanimation_next_step": "Сделать короткий re-qualification контакт с новым четким next-step и дедлайном.",
            "reanimation_risk_note": "Без дисциплины follow-up сделка снова уйдет в закрытую потерю.",
        }
    if has_followup_gap or has_reason_gap:
        return {
            "reanimation_potential": "medium",
            "reanimation_reason_short": "Есть признаки, что сделка могла закрыться из-за тайминга/недожима, а не жесткого anti-fit.",
            "reanimation_next_step": "Проверить актуальность потребности и согласовать один конкретный следующий шаг.",
            "reanimation_risk_note": "При отсутствии нового триггера у клиента повторный контакт может быть холодным.",
        }

    return {
        "reanimation_potential": "low",
        "reanimation_reason_short": "Явных операционных сигналов для быстрого возврата не обнаружено.",
        "reanimation_next_step": "Оставить в closeout-review и пересмотреть при появлении новых вводных.",
        "reanimation_risk_note": "Риск неэффективной траты ресурса на кейс без подтвержденного окна возврата.",
    }


def _derive_product_hypothesis(
    *,
    analysis: dict[str, Any],
    deal: dict[str, Any],
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    info_keywords = (
        "инфо",
        "каталог",
        "карточк",
        "контент",
        "описани",
        "фото",
        "ценообраз",
    )
    link_keywords = (
        "link",
        "srm",
        "закуп",
        "поставщ",
        "тендер",
        "кп",
        "коммерческ",
        "сравнен",
        "интеграц",
    )

    product_label = _to_product_name(deal.get("product_values") or deal.get("product_name") or "")
    sources: list[str] = []
    info_score = 0
    link_score = 0

    if product_label:
        norm = product_label.lower()
        if any(token in norm for token in info_keywords):
            info_score += 3
            sources.append("crm_product_field")
        if any(token in norm for token in link_keywords):
            link_score += 3
            sources.append("crm_product_field")

    text_signals = _collect_product_signal_texts(deal=deal, snapshot=snapshot)
    for source_name, text in text_signals:
        text_norm = text.lower()
        info_hits = sum(1 for token in info_keywords if token in text_norm)
        link_hits = sum(1 for token in link_keywords if token in text_norm)
        if info_hits:
            info_score += min(info_hits, 2)
            sources.append(source_name)
        if link_hits:
            link_score += min(link_hits, 2)
            sources.append(source_name)

    data_quality_low = str(analysis.get("analysis_confidence") or "").strip().lower() == "low" or bool(
        analysis.get("owner_ambiguity_flag")
    )

    unique_sources = _dedup_preserve(sources)
    if info_score == 0 and link_score == 0:
        return {
            "product_hypothesis": "unknown",
            "product_hypothesis_confidence": "low",
            "product_hypothesis_sources": unique_sources,
            "product_hypothesis_reason_short": "Недостаточно сигналов в CRM/комментариях/звонках для продуктовой гипотезы.",
        }

    if info_score > 0 and link_score > 0:
        confidence = "medium" if abs(info_score - link_score) >= 2 and len(unique_sources) >= 2 else "low"
        if data_quality_low:
            confidence = "low"
        return {
            "product_hypothesis": "mixed",
            "product_hypothesis_confidence": confidence,
            "product_hypothesis_sources": unique_sources,
            "product_hypothesis_reason_short": "Есть сигналы сразу по INFO и LINK: требуется ручная продуктовая валидация.",
        }

    hypothesis = "info" if info_score > link_score else "link"
    dominant_score = max(info_score, link_score)
    confidence = "high" if dominant_score >= 4 and len(unique_sources) >= 2 else "medium"
    if not product_label and confidence == "high":
        confidence = "medium"
    if data_quality_low and confidence == "high":
        confidence = "medium"
    if data_quality_low and confidence == "medium":
        confidence = "low"

    if product_label:
        reason = f"CRM продукт и текстовые сигналы указывают на {hypothesis.upper()}."
    else:
        reason = f"Гипотеза {hypothesis.upper()} построена по заметкам/звонкам без явного CRM-поля продукта."
    if data_quality_low:
        reason += " Уверенность снижена из-за качества CRM/атрибуции."

    return {
        "product_hypothesis": hypothesis,
        "product_hypothesis_confidence": confidence,
        "product_hypothesis_sources": unique_sources,
        "product_hypothesis_reason_short": reason,
    }


def _collect_product_signal_texts(*, deal: dict[str, Any], snapshot: dict[str, Any] | None) -> list[tuple[str, str]]:
    signals: list[tuple[str, str]] = []

    for key in ("notes_summary_raw", "tasks_summary_raw"):
        signals.extend(_extract_text_signals(source=key, value=deal.get(key)))
    for key in ("company_comment", "contact_comment"):
        text = str(deal.get(key) or "").strip()
        if text:
            signals.append((key, text))
    for key in ("tags", "source_values", "product_values", "status_name", "pipeline_name"):
        text = _to_product_name(deal.get(key))
        if text:
            signals.append((key, text))

    if isinstance(snapshot, dict):
        transcripts = snapshot.get("transcripts")
        if isinstance(transcripts, list):
            for item in transcripts:
                if isinstance(item, dict):
                    transcript_text = str(item.get("transcript_text") or item.get("text") or "").strip()
                    if transcript_text:
                        signals.append(("transcript", transcript_text))

    return signals


def _extract_text_signals(*, source: str, value: Any) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if isinstance(value, str):
        text = value.strip()
        if text:
            out.append((source, text))
        return out
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                text = str(item.get("text") or item.get("note") or item.get("result") or "").strip()
                if text:
                    out.append((source, text))
            else:
                text = str(item or "").strip()
                if text:
                    out.append((source, text))
    return out


def _to_product_name(value: Any) -> str:
    if isinstance(value, list):
        texts = [str(x).strip() for x in value if str(x).strip()]
        if texts:
            return ", ".join(texts[:3])
    if isinstance(value, str):
        return value.strip()
    return str(value or "").strip()


def _compose_status_stage(*, status_name: Any, pipeline_name: Any) -> str:
    status = str(status_name or "").strip()
    pipeline = str(pipeline_name or "").strip()
    if status and pipeline:
        return f"{pipeline} / {status}"
    return status or pipeline


def _is_won_like_record(item: dict[str, Any]) -> bool:
    status_name = str(item.get("status_name") or "").lower()
    return "успешно реализ" in status_name or "выигран" in status_name or "оплата" in status_name


def _is_active_record(item: dict[str, Any]) -> bool:
    if _is_loss_like_record(item) or _is_won_like_record(item):
        return False
    return bool(str(item.get("status_name") or "").strip())


def _is_low_confidence_record(item: dict[str, Any]) -> bool:
    analysis_confidence = str(item.get("analysis_confidence") or "").strip().lower()
    hygiene = str(item.get("crm_hygiene_confidence") or "").strip().lower()
    quality_flags = item.get("data_quality_flags") if isinstance(item.get("data_quality_flags"), list) else []
    has_owner_ambiguity_flag = bool(item.get("owner_ambiguity_flag")) or any(
        str(x).lower().startswith("owner_ambiguity") for x in quality_flags
    )
    return analysis_confidence == "low" or hygiene == "low" or has_owner_ambiguity_flag


def _risk_count(item: dict[str, Any]) -> int:
    flags = item.get("risk_flags")
    return len(flags) if isinstance(flags, list) else 0


def _is_high_risk_record(item: dict[str, Any]) -> bool:
    score = item.get("score")
    score_value = int(score) if isinstance(score, int) else 999
    return _risk_count(item) >= 2 or score_value <= 35


def _derive_queue_reason(item: dict[str, Any]) -> str:
    flags = item.get("risk_flags") if isinstance(item.get("risk_flags"), list) else []
    has_qualified_loss = any(str(flag).startswith("qualified_loss:") for flag in flags)
    if _is_won_like_record(item):
        return "won_handoff_check"
    if _is_low_confidence_record(item):
        return "low_confidence_needs_manual_check"
    if has_qualified_loss:
        return "qualified_loss_for_pattern_review"
    if _is_loss_like_record(item):
        return "closed_lost_cleanup_review"
    if _is_active_record(item) and _is_high_risk_record(item):
        return "active_risk"
    return "active_risk"


def _queue_priority(reason: str) -> int:
    priorities = {
        "active_risk": 0,
        "won_handoff_check": 1,
        "low_confidence_needs_manual_check": 2,
        "qualified_loss_for_pattern_review": 3,
        "closed_lost_cleanup_review": 4,
    }
    return priorities.get(reason, 9)


def _queue_reason_label(reason: str) -> str:
    labels = {
        "active_risk": "живой риск",
        "won_handoff_check": "проверка передачи",
        "low_confidence_needs_manual_check": "ручная проверка из-за качества CRM",
        "qualified_loss_for_pattern_review": "паттерн осознанной потери",
        "closed_lost_cleanup_review": "закрытая потеря на cleanup-разбор",
    }
    return labels.get(reason, reason)


def _queue_reason_human(reason: str) -> str:
    labels = {
        "active_risk": "нужен ближайший следующий шаг",
        "low_confidence_needs_manual_check": "перепроверить на свежую голову, переслушать звонок, уточнить по CRM и сверить фактического ведущего сделки",
        "qualified_loss_for_pattern_review": "проверить, это разовая потеря или повторяющийся паттерн",
        "closed_lost_cleanup_review": "уточнить причину потери и решить, есть ли смысл аккуратно вернуться",
        "won_handoff_check": "проверить передачу после победы",
    }
    return labels.get(reason, reason)


def _reanimation_priority(value: str) -> int:
    priorities = {"high": 0, "medium": 1, "low": 2, "none": 3}
    return priorities.get(str(value or "").strip().lower(), 9)


def _build_meeting_queue(
    *,
    period_deal_records: list[dict[str, Any]],
    owner_contains: str | None,
    product_contains: str | None,
    status_contains: str | None,
    exclude_low_confidence: bool,
    discussion_limit: int,
) -> list[dict[str, Any]]:
    owner_filter = str(owner_contains or "").strip().lower()
    product_filter = str(product_contains or "").strip().lower()
    status_filter = str(status_contains or "").strip().lower()
    items: list[dict[str, Any]] = []
    for record in period_deal_records:
        if record.get("score") is None:
            continue
        owner_name = str(record.get("owner_name") or "").strip()
        product_name = str(record.get("product_name") or "").strip()
        status_display = str(record.get("status_or_stage") or record.get("status_name") or "").strip()
        if owner_filter and owner_filter not in owner_name.lower():
            continue
        if product_filter and product_filter not in product_name.lower():
            continue
        if status_filter and status_filter not in status_display.lower():
            continue
        if exclude_low_confidence and _is_low_confidence_record(record):
            continue

        reason = _derive_queue_reason(record)
        risks = record.get("risk_flags") if isinstance(record.get("risk_flags"), list) else []
        manager_one_liner = str(record.get("manager_insight_short") or "").strip() or str(record.get("manager_summary") or "").strip()
        if not manager_one_liner:
            manager_one_liner = "Нужен короткий ручной комментарий руководителя по кейсу."

        items.append(
            {
                "deal_id": record.get("deal_id"),
                "deal_name": record.get("deal_name", ""),
                "owner_name": owner_name,
                "product_name": product_name,
                "status_or_stage": status_display,
                "score_0_100": record.get("score"),
                "analysis_confidence": str(record.get("analysis_confidence") or ""),
                "owner_ambiguity_flag": bool(record.get("owner_ambiguity_flag")),
                "top_risk_flags": [str(x) for x in risks[:5]],
                "manager_one_liner": manager_one_liner,
                "why_in_queue": reason,
                "why_in_queue_human": _queue_reason_human(reason),
                "product_hypothesis_llm": str(record.get("product_hypothesis_llm") or "unknown"),
                "reanimation_potential": str(record.get("reanimation_potential") or "none"),
                "reanimation_reason_short": str(record.get("reanimation_reason_short") or ""),
                "reanimation_reason_short_llm": str(record.get("reanimation_reason_short_llm") or ""),
                "reanimation_next_step": str(record.get("reanimation_next_step") or ""),
                "product_hypothesis": str(record.get("product_hypothesis") or "unknown"),
                "product_hypothesis_confidence": str(record.get("product_hypothesis_confidence") or "low"),
                "product_hypothesis_sources": record.get("product_hypothesis_sources")
                if isinstance(record.get("product_hypothesis_sources"), list)
                else [],
                "product_hypothesis_reason_short": str(record.get("product_hypothesis_reason_short") or ""),
                "transcript_available": bool(record.get("transcript_available")),
                "transcript_text_excerpt": str(record.get("transcript_text_excerpt") or ""),
                "transcript_source": str(record.get("transcript_source") or ""),
                "transcript_error": str(record.get("transcript_error") or ""),
                "call_signal_summary_short": str(record.get("call_signal_summary_short") or ""),
                "call_signal_next_step_present": bool(record.get("call_signal_next_step_present")),
                "call_signal_objection_price": bool(record.get("call_signal_objection_price")),
                "call_signal_objection_no_need": bool(record.get("call_signal_objection_no_need")),
                "call_signal_objection_not_target": bool(record.get("call_signal_objection_not_target")),
                "artifact_path": record.get("artifact_path", ""),
            }
        )

    items.sort(
        key=lambda x: (
            _queue_priority(str(x.get("why_in_queue") or "")),
            int(x.get("score_0_100")) if isinstance(x.get("score_0_100"), int) else 999,
            -len(x.get("top_risk_flags") if isinstance(x.get("top_risk_flags"), list) else []),
            str(x.get("deal_id") or ""),
        )
    )
    limit = max(0, int(discussion_limit)) if isinstance(discussion_limit, int) else 10
    return items[:limit]


def _build_meeting_queue_markdown(
    *,
    queue_items: list[dict[str, Any]],
    owner_contains: str | None,
    product_contains: str | None,
    status_contains: str | None,
    exclude_low_confidence: bool,
    discussion_limit: int,
) -> str:
    lines: list[str] = []
    lines.append("# Meeting Queue")
    lines.append("")
    lines.append("## Фильтры запуска")
    lines.append(f"- owner_contains: {str(owner_contains or '').strip() or '-'}")
    lines.append(f"- product_contains: {str(product_contains or '').strip() or '-'}")
    lines.append(f"- status_contains: {str(status_contains or '').strip() or '-'}")
    lines.append(f"- exclude_low_confidence: {bool(exclude_low_confidence)}")
    lines.append(f"- discussion_limit: {discussion_limit}")
    lines.append("")
    lines.append("## Что смотреть в первую очередь")
    if not queue_items:
        lines.append("- Очередь пуста после применения фильтров.")
    else:
        for item in queue_items[:5]:
            reason = str(item.get("why_in_queue") or "")
            lines.append(
                f"- deal={item.get('deal_id')} [{_queue_reason_label(reason)}] score={item.get('score_0_100')}"
            )
    lines.append("")
    lines.append("### Пояснение по группам")
    lines.append("- живые риски: активные сделки с операционным риском, где еще можно повлиять на исход.")
    lines.append("- проверка передачи: выигранные сделки, где важна корректность handoff.")
    lines.append("- ручная проверка из-за качества CRM: кейсы с низкой надежностью интерпретации/owner ambiguity.")
    lines.append("- паттерн осознанной потери: qualified loss для повторяющихся рыночных/fit-сигналов.")
    lines.append("- закрытая потеря на cleanup-разбор: closed-lost кейсы без qualified-loss, где нужен closeout cleanup.")
    lines.append("")
    lines.append("## Сделки для разбора")
    if not queue_items:
        lines.append("- Нет сделок для обсуждения.")
    else:
        for item in queue_items:
            risks = item.get("top_risk_flags") if isinstance(item.get("top_risk_flags"), list) else []
            lines.append(f"### Deal {item.get('deal_id')} — {item.get('deal_name', '')}")
            lines.append(f"- Владелец: {item.get('owner_name', '') or '-'}")
            lines.append(f"- CRM продукт: {item.get('product_name', '') or '-'}")
            lines.append(f"- Гипотеза продукта: {item.get('product_hypothesis', 'unknown')}")
            lines.append(f"- LLM-гипотеза продукта: {item.get('product_hypothesis_llm', 'unknown')}")
            lines.append(f"- Уверенность гипотезы: {item.get('product_hypothesis_confidence', 'low')}")
            lines.append(f"- Почему: {item.get('product_hypothesis_reason_short', '') or '-'}")
            lines.append(f"- Stage/Status: {item.get('status_or_stage', '') or '-'}")
            lines.append(f"- Score: {item.get('score_0_100')}")
            lines.append(f"- Confidence: {item.get('analysis_confidence', '') or '-'}")
            reason = str(item.get("why_in_queue") or "")
            lines.append(f"- Почему в очереди: {item.get('why_in_queue_human', '') or _queue_reason_human(reason)}")
            lines.append(f"- Технический код очереди: {reason} ({_queue_reason_label(reason)})")
            lines.append(f"- Manager one-liner: {item.get('manager_one_liner', '')}")
            lines.append(f"- Risks: {', '.join(str(x) for x in risks) if risks else '-'}")
            if bool(item.get("transcript_available")):
                lines.append(f"- По звонку видно: {item.get('call_signal_summary_short', '') or 'есть транскрипт, но сигналов мало.'}")
            elif str(item.get("transcript_error") or "").strip():
                lines.append(f"- По звонку видно: транскрипт недоступен ({item.get('transcript_error')})")
            else:
                lines.append("- По звонку видно: данных нет.")
            if _queue_item_is_closed_lost(item):
                lines.append(f"- Потенциал реанимации: {item.get('reanimation_potential', 'none')}")
                lines.append(f"- Почему: {item.get('reanimation_reason_short', '') or '-'}")
                lines.append(f"- Следующий шаг: {item.get('reanimation_next_step', '') or '-'}")
            lines.append(f"- Artifact: {item.get('artifact_path', '')}")
            lines.append("")
    lines.append("## Что не стоит интерпретировать слишком уверенно")
    low_conf = [x for x in queue_items if str(x.get("analysis_confidence") or "").lower() == "low"]
    owner_amb = [x for x in queue_items if bool(x.get("owner_ambiguity_flag"))]
    if not low_conf and not owner_amb:
        lines.append("- Существенных ограничений интерпретации в выбранной очереди не обнаружено.")
    else:
        if low_conf:
            lines.append(f"- Низкая надежность интерпретации: {len(low_conf)} сделок.")
        if owner_amb:
            lines.append(f"- Owner ambiguity: {len(owner_amb)} сделок.")
        lines.append("- По этим кейсам сначала подтвердить фактического ведущего и полноту CRM-фактов, затем делать персональные выводы.")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _queue_item_is_closed_lost(item: dict[str, Any]) -> bool:
    status_or_stage = str(item.get("status_or_stage") or "").lower()
    reason = str(item.get("why_in_queue") or "")
    return (
        reason in {"qualified_loss_for_pattern_review", "closed_lost_cleanup_review"}
        or ("закрыто" in status_or_stage and ("не реализ" in status_or_stage or "отказ" in status_or_stage))
    )


def _extract_transcription_compare_view(analysis: dict[str, Any]) -> dict[str, Any]:
    risk_flags = analysis.get("risk_flags") if isinstance(analysis.get("risk_flags"), list) else []
    manager_one_liner = str(analysis.get("manager_insight_short") or "").strip() or str(analysis.get("manager_summary") or "").strip()
    next_step = str(analysis.get("reanimation_next_step") or "").strip()
    if not next_step:
        actions = analysis.get("recommended_actions_for_manager")
        if isinstance(actions, list):
            for action in actions:
                text = str(action or "").strip()
                if text:
                    next_step = text
                    break
    return {
        "product_hypothesis": str(analysis.get("product_hypothesis") or "unknown"),
        "product_hypothesis_confidence": str(analysis.get("product_hypothesis_confidence") or "low"),
        "product_hypothesis_reason_short": str(analysis.get("product_hypothesis_reason_short") or ""),
        "call_signal_summary_short": str(analysis.get("call_signal_summary_short") or ""),
        "reanimation_potential": str(analysis.get("reanimation_potential") or "none"),
        "reanimation_reason_short": str(analysis.get("reanimation_reason_short") or ""),
        "top_risk_flags": [str(x) for x in risk_flags[:3]],
        "key_risk": str(risk_flags[0]) if risk_flags else "",
        "manager_one_liner": manager_one_liner,
        "manager_summary": str(analysis.get("manager_summary") or ""),
        "employee_coaching": str(analysis.get("employee_coaching") or ""),
        "employee_fix_tasks": [str(x) for x in (analysis.get("employee_fix_tasks") if isinstance(analysis.get("employee_fix_tasks"), list) else [])],
        "next_step": next_step,
    }


def _build_transcription_impact_row(
    *,
    deal_id: Any,
    deal_name: str,
    owner_name: str,
    status_or_stage: str,
    score: Any,
    without_view: dict[str, Any],
    with_view: dict[str, Any],
    analysis: dict[str, Any],
    snapshot: dict[str, Any],
    artifact_path: str,
) -> dict[str, Any]:
    changed_fields: list[str] = []
    meaningful_keys = (
        "product_hypothesis",
        "product_hypothesis_confidence",
        "product_hypothesis_reason_short",
        "call_signal_summary_short",
        "reanimation_potential",
        "reanimation_reason_short",
        "manager_summary",
        "employee_coaching",
        "employee_fix_tasks",
    )
    for key in meaningful_keys:
        if (without_view.get(key) or "") != (with_view.get(key) or ""):
            changed_fields.append(key)

    transcripts = snapshot.get("transcripts") if isinstance(snapshot.get("transcripts"), list) else []
    transcript_available = bool(analysis.get("transcript_available")) or bool(transcripts)
    transcript_errors = [str(t.get("transcript_error") or "").strip() for t in transcripts if isinstance(t, dict) and str(t.get("transcript_error") or "").strip()]
    transcript_text_len = max((len(str(t.get("transcript_text") or "")) for t in transcripts if isinstance(t, dict)), default=0)
    transcript_noisy = bool(transcript_available and (transcript_errors or transcript_text_len < 120))

    if not transcript_available:
        impact_bucket = "no_transcript"
    elif transcript_noisy:
        impact_bucket = "transcript_suspicious_or_noisy"
    elif changed_fields:
        impact_bucket = "transcript_added_meaning"
    else:
        impact_bucket = "transcript_no_change"

    return {
        "deal_id": deal_id,
        "deal_name": deal_name,
        "owner_name": owner_name,
        "status_or_stage": status_or_stage,
        "score_0_100": score,
        "transcript_available": transcript_available,
        "transcript_noisy": transcript_noisy,
        "transcript_error": str(analysis.get("transcript_error") or ""),
        "without_transcript_aware": without_view,
        "with_transcript_aware": with_view,
        "changed_fields": changed_fields,
        "changed": bool(changed_fields),
        "impact_bucket": impact_bucket,
        "why_in_queue": str(analysis.get("why_in_queue") or ""),
        "baseline_summary": str(without_view.get("manager_summary") or without_view.get("manager_one_liner") or "").strip(),
        "transcript_summary": str(with_view.get("manager_summary") or with_view.get("manager_one_liner") or "").strip(),
        "transcript_excerpt": str(analysis.get("transcript_text_excerpt") or "").strip(),
        "artifact_path": artifact_path,
    }


def _build_transcription_impact_markdown(*, transcription_impact_rows: list[dict[str, Any]]) -> str:
    rows = [x for x in transcription_impact_rows if isinstance(x, dict)]
    added = [x for x in rows if x.get("impact_bucket") == "transcript_added_meaning"]
    no_change = [x for x in rows if x.get("impact_bucket") == "transcript_no_change"]
    noisy = [x for x in rows if x.get("impact_bucket") == "transcript_suspicious_or_noisy"]
    call_focus = sorted(
        [x for x in rows if x.get("impact_bucket") in {"transcript_added_meaning", "transcript_suspicious_or_noisy"}],
        key=lambda i: (
            0 if str(i.get("impact_bucket")) == "transcript_added_meaning" else 1,
            int(i.get("score_0_100")) if isinstance(i.get("score_0_100"), int) else 999,
            -len(i.get("changed_fields") if isinstance(i.get("changed_fields"), list) else []),
        ),
    )[:10]

    lines: list[str] = []
    lines.append("# Transcription Impact")
    lines.append("")
    lines.append(f"- Deals compared: {len(rows)}")
    lines.append(f"- Где звонок добавил смысл: {len(added)}")
    lines.append(f"- Где звонок ничего не изменил: {len(no_change)}")
    lines.append(f"- Где транскрипт сомнительный/шумный: {len(noisy)}")
    lines.append("")

    lines.append("## Где звонок реально добавил смысл")
    if not added:
        lines.append("- Не найдено в этом запуске.")
    else:
        for item in added[:20]:
            lines.append(
                f"- deal={item.get('deal_id')} score={item.get('score_0_100')} changed={', '.join(item.get('changed_fields') or []) or '-'}"
            )
    lines.append("")

    lines.append("## Где звонок ничего не изменил")
    if not no_change:
        lines.append("- Не найдено в этом запуске.")
    else:
        for item in no_change[:20]:
            lines.append(f"- deal={item.get('deal_id')} score={item.get('score_0_100')}")
    lines.append("")

    lines.append("## Где транскрипт сомнительный/шумный")
    if not noisy:
        lines.append("- Не найдено в этом запуске.")
    else:
        for item in noisy[:20]:
            err = str(item.get("transcript_error") or "").strip()
            lines.append(f"- deal={item.get('deal_id')} reason={err or 'short/low-signal transcript'}")
    lines.append("")

    lines.append("## Топ-10 сделок для собрания именно по звонкам")
    if not call_focus:
        lines.append("- Нет кандидатов с call-impact в этом запуске.")
    else:
        for item in call_focus:
            with_view = item.get("with_transcript_aware") if isinstance(item.get("with_transcript_aware"), dict) else {}
            lines.append(
                f"- deal={item.get('deal_id')} score={item.get('score_0_100')} impact={item.get('impact_bucket')} summary={with_view.get('call_signal_summary_short','') or '-'}"
            )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_transcription_impact_payload(*, transcription_impact_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [x for x in transcription_impact_rows if isinstance(x, dict)]
    deals_with_transcript = sum(1 for x in rows if bool(x.get("transcript_available")))
    changed_deals = [x for x in rows if bool(x.get("changed"))]
    changed_payload: list[dict[str, Any]] = []
    for item in changed_deals:
        changed_payload.append(
            {
                "deal_id": item.get("deal_id"),
                "deal_name": item.get("deal_name", ""),
                "owner_name": item.get("owner_name", ""),
                "baseline_summary": item.get("baseline_summary", ""),
                "transcript_summary": item.get("transcript_summary", ""),
                "changed_fields": item.get("changed_fields", []) if isinstance(item.get("changed_fields"), list) else [],
                "transcript_excerpt": item.get("transcript_excerpt", ""),
                "artifact_path": item.get("artifact_path", ""),
            }
        )
    return {
        "total_deals_analyzed": len(rows),
        "deals_with_transcript": deals_with_transcript,
        "deals_changed_by_transcript": len(changed_payload),
        "deals_unchanged_by_transcript": max(0, len(rows) - len(changed_payload)),
        "changed_deals": changed_payload,
    }


def _negotiation_signal_presence_score(items: list[dict[str, Any]]) -> int:
    if not items:
        return 0
    hit_count = 0
    for item in items:
        has_summary = bool(str(item.get("call_signal_summary_short") or "").strip())
        has_excerpt = bool(str(item.get("transcript_text_excerpt") or "").strip())
        role_signal = _call_role_signal(item)
        if has_summary or has_excerpt or role_signal in {"lpr", "secretary", "history_pattern"}:
            hit_count += 1
    return round((hit_count / max(1, len(items))) * 100)


def _crm_only_bias_flag(items: list[dict[str, Any]]) -> bool:
    if not items:
        return True
    negotiation_hits = _negotiation_signal_presence_score(items)
    transcript_usable_hits = sum(1 for x in items if str(x.get("transcript_usability_label") or "").strip().lower() == "usable")
    return negotiation_hits < 35 and transcript_usable_hits == 0


def _daily_package_quality_label(*, items: list[dict[str, Any]], forced_fallback: bool) -> str:
    if not items:
        return "weak"
    usable_hits = sum(1 for x in items if str(x.get("transcript_usability_label") or "").strip().lower() == "usable")
    evidence = round(sum(_evidence_richness_score(x) for x in items) / max(1, len(items)), 1)
    if forced_fallback and usable_hits == 0 and evidence < 5:
        return "weak"
    if usable_hits >= max(1, len(items) // 2) and evidence >= 6:
        return "strong"
    if usable_hits >= 1 or evidence >= 5:
        return "acceptable"
    return "thin"


def _build_text_generation_source_per_column(
    *,
    llm_columns: dict[str, str],
    fallback_columns: dict[str, str],
) -> dict[str, str]:
    out: dict[str, str] = {}
    style_applied = bool(llm_columns.get("_style_layer_applied"))
    llm_content_applied = bool(llm_columns.get("_content_layer_applied"))
    llm_ready = bool(llm_columns.get("_llm_text_ready"))
    for key in DAILY_TEXT_COLUMN_KEYS:
        llm_value = " ".join(str(llm_columns.get(key) or "").split()).strip()
        if llm_ready and llm_value:
            out[key] = "llm_style_rewrite" if style_applied else ("llm_content" if llm_content_applied else "llm")
        else:
            out[key] = "rules_fallback"
    return out


def _build_daily_control_sheet_payload(
    *,
    summary: dict[str, Any],
    period_deal_records: list[dict[str, Any]],
    amo_base_domain: str = "",
    manager_allowlist: list[str] | tuple[str, ...] | None = None,
    cfg: DealAnalyzerConfig | None = None,
    logger: Any | None = None,
    backend_effective: str | None = None,
    style_source_excerpt: str = "",
    llm_runtime: dict[str, Any] | None = None,
    daily_step_artifacts_dir: Path | None = None,
) -> dict[str, Any]:
    records = [x for x in period_deal_records if isinstance(x, dict) and x.get("score") is not None]
    allowlist = _resolve_daily_manager_allowlist(manager_allowlist)
    allowset = {x.lower() for x in allowlist}
    grouped: dict[str, list[dict[str, Any]]] = {}
    grouped_unfiltered: dict[str, list[dict[str, Any]]] = {}
    for item in records:
        manager = _normalize_manager_for_dropdown(" ".join(str(item.get("owner_name") or "").strip().split()))
        if not manager:
            manager = "Не указан"
        grouped_unfiltered.setdefault(manager, []).append(item)
        if manager.lower() not in allowset:
            continue
        grouped.setdefault(manager, []).append(item)
    if not grouped and grouped_unfiltered and set(grouped_unfiltered.keys()) == {"Не указан"}:
        grouped = grouped_unfiltered
        if logger is not None:
            logger.warning(
                "daily manager allowlist produced empty set; fallback to unfiltered managers for call review rows",
            )

    period_start = str(summary.get("period_start") or "")
    period_end = str(summary.get("period_end") or "")
    run_date = str(summary.get("run_timestamp") or "").split("T", 1)[0]
    control_days = _resolve_daily_control_days(
        period_start=period_start,
        period_end=period_end,
        records=records,
        run_date=run_date,
    )
    stage_priority_weights, stage_weight_source = _resolve_stage_priority_weights(summary=summary)
    if logger is not None:
        logger.info(
            "daily ranking stage weights: source=%s weights_count=%s",
            stage_weight_source,
            len(stage_priority_weights),
        )

    rows: list[dict[str, Any]] = []
    step_failures: list[dict[str, Any]] = []
    step_artifact_paths: list[str] = []
    daily_rows_from_conversation_pool = 0
    daily_rows_from_discipline_pool = 0
    daily_rows_with_real_transcript = 0
    daily_rows_with_only_discipline_signals = 0
    daily_rows_skipped_crm_only_total = 0
    effect_forecast_fallback_logged = False
    used_deal_ids_by_manager: dict[str, set[str]] = {m: set() for m in grouped}
    for control_day in control_days:
        for manager, manager_records in sorted(grouped.items(), key=lambda x: _manager_sort_key(x[0], allowlist=allowlist)):
            role = _manager_role_label(manager, cfg=cfg)
            used_deal_ids = used_deal_ids_by_manager.setdefault(manager, set())
            dynamic_target = _daily_package_target(
                manager_records=manager_records,
                control_day=control_day,
            )
            package_items = _select_daily_package_records(
                manager_records=manager_records,
                control_day=control_day,
                package_target=dynamic_target,
                carryover_days=7,
                exclude_deal_ids=used_deal_ids,
                stage_priority_weights=stage_priority_weights,
                cfg=cfg,
                logger=logger,
                backend_effective=backend_effective,
                manager=manager,
                role=role,
                style_source_excerpt=style_source_excerpt,
                llm_runtime=llm_runtime,
            )
            if not package_items:
                package_items = _select_daily_package_records_relaxed(
                    manager_records=manager_records,
                    package_target=max(3, dynamic_target // 2),
                    exclude_deal_ids=used_deal_ids,
                    stage_priority_weights=stage_priority_weights,
                )
            if not package_items:
                continue
            for item in package_items:
                did = str(item.get("deal_id") or "").strip()
                if did:
                    used_deal_ids.add(did)

            scores = [int(x.get("score")) for x in package_items if isinstance(x.get("score"), int)]
            avg_score = _daily_weighted_score(package_items) if package_items else (round(sum(scores) / len(scores)) if scores else None)
            links = _build_daily_deal_links(items=package_items, base_domain=amo_base_domain)
            top_risks_raw = _collect_top_risk_flags(package_items, limit=5)
            strong = _collect_short_list(package_items, "strong_sides", limit=3)
            growth = _collect_short_list(package_items, "growth_zones", limit=5)
            filtered_growth, role_note = _filter_growth_for_role(
                role=role,
                growth=growth,
                top_risks=top_risks_raw,
                items=package_items,
            )
            manager_msgs = _collect_short_text(package_items, "manager_summary", limit=2)
            employee_msgs = _collect_short_text(package_items, "employee_coaching", limit=1)
            focus = _top_product_focus(package_items)
            base_mix_resolution = _resolve_base_mix(package_items)
            quality_mix = str(base_mix_resolution.get("selected_value") or _build_base_mix_text(package_items))
            selection_reason = _daily_selection_reason(package_items)
            primary_source = _derive_daily_primary_source(package_items)
            daily_case_type = _derive_daily_case_type(package_items, primary_source=primary_source)
            case_profile = classify_daily_case(role=role, items=package_items)
            role_scope_policy = get_role_scope_policy(role=role, items=package_items)
            case_policy = mode_prompt_policy(case_profile)
            base_allowed_axes = case_policy.get("allowed_axes", []) if isinstance(case_policy.get("allowed_axes"), list) else []
            base_banned_topics = case_policy.get("banned_topics", []) if isinstance(case_policy.get("banned_topics"), list) else []
            role_allowed_topics = role_scope_policy.get("role_allowed_topics", []) if isinstance(role_scope_policy.get("role_allowed_topics"), list) else []
            role_blocked_topics = role_scope_policy.get("role_blocked_topics", []) if isinstance(role_scope_policy.get("role_blocked_topics"), list) else []
            merged_allowed_axes = [str(x) for x in base_allowed_axes if str(x).strip()]
            merged_allowed_axes.extend(str(x) for x in role_allowed_topics if str(x).strip() and str(x) not in merged_allowed_axes)
            merged_banned_topics = [str(x) for x in base_banned_topics if str(x).strip()]
            for topic in role_blocked_topics:
                topic_text = str(topic or "").strip()
                if topic_text and topic_text not in merged_banned_topics:
                    merged_banned_topics.append(topic_text)
            case_policy["allowed_axes"] = merged_allowed_axes
            case_policy["banned_topics"] = merged_banned_topics
            case_policy["role_scope_applied"] = bool(role_scope_policy.get("role_scope_applied"))
            case_policy["role_allowed_topics"] = role_allowed_topics
            case_policy["role_blocked_topics"] = role_blocked_topics
            case_policy["role_scope_conflict_flag"] = bool(role_scope_policy.get("role_scope_conflict_flag"))
            analysis_mode = case_profile.mode
            repeated_dead_redial_count = sum(int(x.get("repeated_dead_redial_count", 0) or 0) for x in package_items if isinstance(x, dict))
            repeated_dead_redial_day_flag = bool(any(bool(x.get("repeated_dead_redial_day_flag")) for x in package_items if isinstance(x, dict)))
            same_time_redial_pattern_flag = bool(any(bool(x.get("same_time_redial_pattern_flag")) for x in package_items if isinstance(x, dict)))
            numbers_not_fully_covered_flag = bool(any(bool(x.get("numbers_not_fully_covered_flag")) for x in package_items if isinstance(x, dict)))
            has_usable_negotiation = any(_transcript_usability_score(x) >= 2 for x in package_items if isinstance(x, dict))
            has_meaningful_dial_pattern = repeated_dead_redial_day_flag or same_time_redial_pattern_flag or numbers_not_fully_covered_flag
            allow_thin_for_local_debug = cfg is None and backend_effective is None
            if not has_usable_negotiation and not has_meaningful_dial_pattern and not allow_thin_for_local_debug:
                continue
            if not mode_is_writable(case_profile) and not allow_thin_for_local_debug:
                continue
            transcript_usability_score = round(
                sum(_transcript_usability_score(x) for x in package_items) / max(1, len(package_items))
            )
            evidence_richness_score = round(
                sum(_evidence_richness_score(x) for x in package_items) / max(1, len(package_items))
            )
            funnel_relevance_score = round(
                sum(_funnel_relevance_score(x, stage_priority_weights=stage_priority_weights) for x in package_items)
                / max(1, len(package_items)),
                2,
            )
            management_value_score = round(
                sum(_management_value_rank(x) for x in package_items) / max(1, len(package_items)),
                2,
            )
            stage_priority_weight_value = round(
                sum(
                    _stage_priority_weight_value_for_item(
                        x,
                        stage_priority_weights=stage_priority_weights,
                    )
                    for x in package_items
                )
                / max(1, len(package_items)),
                3,
            )
            best_rank = min(
                (int(x.get("_daily_selection_rank", 9999)) for x in package_items if isinstance(x, dict)),
                default=9999,
            )
            skipped_candidates_debug = (
                package_items[0].get("_daily_skipped_candidates", [])
                if package_items and isinstance(package_items[0], dict)
                else []
            )
            excluded_crm_only_cases_count = sum(
                1
                for x in (skipped_candidates_debug if isinstance(skipped_candidates_debug, list) else [])
                if isinstance(x, dict)
                and str(x.get("skip_for_daily_reason") or "").strip() in {
                    "crm_only_filler_blocked_due_to_call_cases",
                    "weak_transcript_and_thin_crm",
                }
            )
            selection_reason_v2 = _daily_selection_reason_v2(
                primary_source=primary_source,
                case_type=daily_case_type,
                base_reason=selection_reason,
                excluded_crm_only_cases_count=excluded_crm_only_cases_count,
            )
            fallback_key_takeaway = _daily_user_text(
                _build_daily_key_takeaway(
                    manager=manager,
                    role=role,
                    items=package_items,
                    manager_msgs=manager_msgs,
                    growth=filtered_growth,
                )
            )
            criticality = _score_to_criticality(avg_score, risk_count=sum(len(x.get("risk_flags", [])) for x in package_items))
            fallback_strong_text = _daily_user_text(_build_daily_strong_sides(items=package_items, strong=strong))
            growth_compact = _daily_growth_compact(filtered_growth)
            fallback_reinforce = _daily_user_text(_build_daily_reinforce(items=package_items, role=role, strong=strong))
            fallback_fix = _daily_user_text(_build_daily_fix_action(items=package_items, role=role, growth=growth_compact))
            fallback_why_important = _daily_user_text(
                _build_daily_why_important(role=role, items=package_items, role_note=role_note)
            )
            forecast = _build_daily_effect_forecast(
                items=package_items,
                role=role,
                avg_score=avg_score,
                criticality=criticality,
            )
            forced_fallback = any(
                str(x.get("skip_for_daily_reason") or "").strip() in {"fallback_fill", "weak_transcript_and_thin_crm"}
                for x in package_items
                if isinstance(x, dict)
            )
            negotiation_signal_score = _negotiation_signal_presence_score(package_items)
            crm_only_bias = _crm_only_bias_flag(package_items)
            package_quality = _daily_package_quality_label(items=package_items, forced_fallback=forced_fallback)
            if logger is not None and forecast.get("source") != "roks" and not effect_forecast_fallback_logged:
                logger.info("daily effect forecast source=fallback (ROKS metrics unavailable)")
                effect_forecast_fallback_logged = True
            fallback_expected_qty = _daily_user_text(str(forecast.get("quantity_text") or ""))
            fallback_expected_quality = _daily_user_text(str(forecast.get("quality_text") or ""))
            fallback_coaching_text = _daily_user_text(
                _build_daily_coaching_list(
                    items=package_items,
                    role=role,
                    growth=growth_compact,
                    employee_msgs=employee_msgs,
                )
            )
            fallback_columns = {
                "Ключевой вывод": fallback_key_takeaway,
                "Сильные стороны": fallback_strong_text,
                "Зоны роста": _daily_user_text("; ".join(str(x) for x in growth_compact)),
                "Почему это важно": fallback_why_important,
                "Что закрепить": fallback_reinforce,
                "Что исправить": fallback_fix,
                "Что донес сотруднику": fallback_coaching_text,
                "Ожидаемый эффект - количество": fallback_expected_qty,
                "Ожидаемый эффект - качество": fallback_expected_quality,
            }
            reference_stack_diag: dict[str, Any] = {}
            factual_payload: dict[str, Any] = {}
            llm_required = cfg is not None and cfg.analyzer_backend in {"hybrid", "ollama"}
            llm_text_columns: dict[str, Any] = {}
            multistep = None
            if llm_required:
                factual_payload = _build_daily_table_factual_payload(
                    cfg=cfg,
                    logger=logger,
                    manager=manager,
                    role=role,
                    control_day=control_day,
                    period_start=period_start,
                    period_end=period_end,
                    package_items=package_items,
                    links=links,
                    focus=focus,
                    base_mix=quality_mix,
                    avg_score=avg_score,
                    criticality=criticality,
                    selection_reason=selection_reason,
                    growth_candidates=growth_compact,
                    fallback_columns=fallback_columns,
                    effect_forecast=forecast,
                    case_policy=case_policy,
                )
                reference_stack_diag = (
                    factual_payload.get("reference_stack", {})
                    if isinstance(factual_payload.get("reference_stack"), dict)
                    else {}
                )
                if logger is not None:
                    required = (
                        reference_stack_diag.get("required_layers", {})
                        if isinstance(reference_stack_diag.get("required_layers"), dict)
                        else {}
                    )
                    logger.info(
                        "daily reference runtime: manager=%s day=%s snippets=%s internal_ok=%s role_ok=%s product_ok=%s external_used=%s",
                        manager,
                        control_day,
                        int(reference_stack_diag.get("prompt_snippets_count", 0) or 0),
                        bool((required.get("internal_references", {}) if isinstance(required.get("internal_references"), dict) else {}).get("ok")),
                        bool((required.get("role_context", {}) if isinstance(required.get("role_context"), dict) else {}).get("ok")),
                        bool((required.get("product_reference_urls", {}) if isinstance(required.get("product_reference_urls"), dict) else {}).get("ok")),
                        bool(
                            (reference_stack_diag.get("external_retrieval", {}) if isinstance(reference_stack_diag.get("external_retrieval"), dict) else {}).get("used")
                        ),
                    )
                multistep = _run_daily_multistep_pipeline(
                    cfg=cfg,
                    logger=logger,
                    llm_runtime=llm_runtime,
                    manager=manager,
                    control_day=control_day,
                    package_items=package_items,
                    factual_payload=factual_payload,
                    effect_forecast=forecast,
                    style_source_excerpt=style_source_excerpt,
                    case_policy=case_policy,
                    debug_root=daily_step_artifacts_dir,
                )
                if multistep.get("ok"):
                    llm_text_columns = dict(multistep.get("columns") or {})
                    llm_text_columns["_llm_text_ready"] = True
                    llm_text_columns["_style_layer_applied"] = True
                    llm_text_columns["_content_layer_applied"] = True
                    llm_text_columns["_pipeline_source_of_truth"] = str(multistep.get("source_of_truth") or "styled_blocks")
                    llm_text_columns["_pipeline_assembler_only"] = bool(multistep.get("assembler_only"))
                    artifacts = multistep.get("step_artifacts") if isinstance(multistep.get("step_artifacts"), dict) else {}
                    step_artifact_paths.extend(str(v) for v in artifacts.values() if str(v).strip())
                else:
                    fail = {
                        "manager": manager,
                        "control_day": control_day,
                        "failed_step": str(multistep.get("failed_step") or "unknown"),
                        "error": str(multistep.get("error") or ""),
                        "deal_ids": [x.get("deal_id") for x in package_items if isinstance(x, dict)],
                        "step_artifacts": multistep.get("step_artifacts") if isinstance(multistep.get("step_artifacts"), dict) else {},
                    }
                    step_failures.append(fail)
                    if logger is not None:
                        logger.warning(
                            "daily multistep pipeline failed: manager=%s day=%s step=%s error=%s",
                            manager,
                            control_day,
                            fail["failed_step"],
                            fail["error"],
                        )
                    continue
            else:
                factual_payload = _build_daily_table_factual_payload(
                    cfg=cfg,
                    logger=logger,
                    manager=manager,
                    role=role,
                    control_day=control_day,
                    period_start=period_start,
                    period_end=period_end,
                    package_items=package_items,
                    links=links,
                    focus=focus,
                    base_mix=quality_mix,
                    avg_score=avg_score,
                    criticality=criticality,
                    selection_reason=selection_reason,
                    growth_candidates=growth_compact,
                    fallback_columns=fallback_columns,
                    effect_forecast=forecast,
                    case_policy=case_policy,
                )
                # Legacy rules mode for local/tests only.
                llm_text_columns = _generate_daily_table_text_columns(
                    cfg=cfg,
                    logger=logger,
                    backend_effective=backend_effective,
                    manager=manager,
                    role=role,
                    control_day=control_day,
                    period_start=period_start,
                    period_end=period_end,
                    package_items=package_items,
                    links=links,
                    focus=focus,
                    base_mix=quality_mix,
                    avg_score=avg_score,
                    criticality=criticality,
                    selection_reason=selection_reason,
                    growth_candidates=growth_compact,
                    fallback_columns=fallback_columns,
                    effect_forecast=forecast,
                    style_source_excerpt=style_source_excerpt,
                    llm_runtime=llm_runtime,
                    case_policy=case_policy,
                    factual_payload_override=factual_payload,
                )
                reference_stack_diag = factual_payload.get("reference_stack", {}) if isinstance(factual_payload.get("reference_stack"), dict) else {}

            text_generation_map = _build_text_generation_source_per_column(
                llm_columns=llm_text_columns,
                fallback_columns=fallback_columns,
            )

            llm_ready = bool(llm_text_columns.get("_llm_text_ready"))
            if llm_required and not llm_ready:
                continue
            key_takeaway_text = llm_text_columns.get("Ключевой вывод", "") if llm_ready else fallback_key_takeaway
            strong_text = llm_text_columns.get("Сильные стороны", "") if llm_ready else fallback_strong_text
            growth_text = llm_text_columns.get("Зоны роста", "") if llm_ready else _daily_user_text("; ".join(str(x) for x in growth_compact))
            why_important_text = llm_text_columns.get("Почему это важно", "") if llm_ready else fallback_why_important
            reinforce_text = llm_text_columns.get("Что закрепить", "") if llm_ready else fallback_reinforce
            fix_text = llm_text_columns.get("Что исправить", "") if llm_ready else fallback_fix
            coaching_text = llm_text_columns.get("Что донес сотруднику", "") if llm_ready else fallback_coaching_text
            expected_qty_text = llm_text_columns.get("Ожидаемый эффект - количество", "") if llm_ready else fallback_expected_qty
            expected_quality_text = llm_text_columns.get("Ожидаемый эффект - качество", "") if llm_ready else fallback_expected_quality

            rows.append(
                {
                    "Неделя с": period_start,
                    "Неделя по": period_end,
                    "Дата контроля": control_day,
                    "День": _weekday_ru_from_iso(control_day),
                    "Менеджер": _normalize_manager_for_dropdown(manager),
                    "Роль менеджера": role,
                    "Проанализировано сделок": len(package_items),
                    "Ссылки на сделки": links,
                    "Продукт / фокус": focus,
                    "База микс": quality_mix,
                    "Ключевой вывод": key_takeaway_text,
                    "Сильные стороны": strong_text,
                    "Зоны роста": growth_text,
                    "Почему это важно": why_important_text,
                    "Что закрепить": reinforce_text,
                    "Что исправить": fix_text,
                    "Что донес сотруднику": coaching_text,
                    "Ожидаемый эффект - количество": expected_qty_text,
                    "Ожидаемый эффект - качество": expected_quality_text,
                    "Оценка 0-100": avg_score if avg_score is not None else "",
                    "Критичность": criticality,
                    "selection_reason": selection_reason,
                    "base_mix_selected_source": str(base_mix_resolution.get("selected_source") or ""),
                    "base_mix_selected_value": str(base_mix_resolution.get("selected_value") or ""),
                    "base_mix_fallback_used": bool(base_mix_resolution.get("fallback_used")),
                    "base_mix_raw_tags_deal": (
                        base_mix_resolution.get("raw_tags_deal", [])
                        if isinstance(base_mix_resolution.get("raw_tags_deal"), list)
                        else []
                    ),
                    "base_mix_raw_tags_company": (
                        base_mix_resolution.get("raw_tags_company", [])
                        if isinstance(base_mix_resolution.get("raw_tags_company"), list)
                        else []
                    ),
                    "base_mix_deal_tag_entries": (
                        base_mix_resolution.get("deal_tag_entries", [])
                        if isinstance(base_mix_resolution.get("deal_tag_entries"), list)
                        else []
                    ),
                    "base_mix_company_tag_entries": (
                        base_mix_resolution.get("company_tag_entries", [])
                        if isinstance(base_mix_resolution.get("company_tag_entries"), list)
                        else []
                    ),
                    "daily_primary_source": primary_source,
                    "daily_case_type": daily_case_type,
                    "daily_analysis_mode": (
                        "discipline_analysis"
                        if primary_source == "discipline_pool" or analysis_mode == "redial_discipline_analysis"
                        else analysis_mode
                    ),
                    "daily_analysis_mode_reason": case_profile.mode_reason,
                    "daily_analysis_mode_confidence": case_profile.confidence,
                    "daily_selection_reason": selection_reason,
                    "daily_selection_reason_v2": selection_reason_v2,
                    "excluded_crm_only_cases_count": excluded_crm_only_cases_count,
                    "daily_package_quality_label": package_quality,
                    "daily_package_has_forced_fallback": bool(forced_fallback),
                    "negotiation_signal_presence_score": negotiation_signal_score,
                    "crm_only_bias_flag": bool(crm_only_bias),
                    "transcript_usability_score": transcript_usability_score,
                    "evidence_richness_score": evidence_richness_score,
                    "funnel_relevance_score": funnel_relevance_score,
                    "management_value_score": management_value_score,
                    "daily_selection_rank": best_rank if best_rank != 9999 else "",
                    "stage_priority_weight_source": stage_weight_source,
                    "stage_priority_weight_value": stage_priority_weight_value,
                    "effect_forecast_source": str(forecast.get("source") or ""),
                    "effect_problem_stage": str(forecast.get("stage_focus") or ""),
                    "effect_downstream_stages": ", ".join(
                        str(x) for x in (forecast.get("downstream_stages") or []) if str(x).strip()
                    ),
                    "repeated_dead_redial_count": repeated_dead_redial_count,
                    "repeated_dead_redial_day_flag": repeated_dead_redial_day_flag,
                    "same_time_redial_pattern_flag": same_time_redial_pattern_flag,
                    "numbers_not_fully_covered_flag": numbers_not_fully_covered_flag,
                    "style_layer_applied": bool(llm_text_columns.get("_style_layer_applied")),
                    "llm_text_ready": llm_ready,
                    "daily_multistep_source_of_truth": str(llm_text_columns.get("_pipeline_source_of_truth") or ""),
                    "daily_multistep_assembler_only": bool(llm_text_columns.get("_pipeline_assembler_only")),
                    "text_generation_source_per_column": text_generation_map,
                    "reference_sources_used": ", ".join(
                        str(x.get("source") or "")
                        for x in (reference_stack_diag.get("prompt_snippets", []) if isinstance(reference_stack_diag.get("prompt_snippets"), list) else [])
                        if isinstance(x, dict) and str(x.get("source") or "").strip()
                    ),
                    "reference_sources_count": int(reference_stack_diag.get("prompt_snippets_count", 0) or 0),
                    "reference_required_layers": (
                        reference_stack_diag.get("required_layers", {})
                        if isinstance(reference_stack_diag.get("required_layers"), dict)
                        else {}
                    ),
                    "external_retrieval_used": bool(
                        (reference_stack_diag.get("external_retrieval", {}) if isinstance(reference_stack_diag.get("external_retrieval"), dict) else {}).get("used")
                    ),
                    "external_retrieval_snippets_count": len(
                        (reference_stack_diag.get("external_retrieval", {}) if isinstance(reference_stack_diag.get("external_retrieval"), dict) else {}).get("snippets", [])
                        if isinstance(
                            (reference_stack_diag.get("external_retrieval", {}) if isinstance(reference_stack_diag.get("external_retrieval"), dict) else {}).get("snippets", []),
                            list,
                        )
                        else []
                    ),
                    "reference_stack_diagnostics": reference_stack_diag,
                    "role_scope_applied": bool(
                        llm_text_columns.get("_role_scope_applied", case_policy.get("role_scope_applied", False))
                    ),
                    "role_blocked_topics": str(
                        llm_text_columns.get(
                            "_role_blocked_topics",
                            ", ".join(str(x) for x in (case_policy.get("role_blocked_topics", []) if isinstance(case_policy.get("role_blocked_topics"), list) else [])),
                        )
                    ),
                    "role_allowed_topics": str(
                        llm_text_columns.get(
                            "_role_allowed_topics",
                            ", ".join(str(x) for x in (case_policy.get("role_allowed_topics", []) if isinstance(case_policy.get("role_allowed_topics"), list) else [])),
                        )
                    ),
                    "role_scope_conflict_flag": bool(
                        llm_text_columns.get("_role_scope_conflict_flag", case_policy.get("role_scope_conflict_flag", False))
                    ),
                    "transcript_quality_retry_used": any(
                        bool(x.get("transcript_quality_retry_used"))
                        for x in package_items
                        if isinstance(x, dict)
                    ),
                    "transcript_quality_retry_improved": any(
                        bool(x.get("transcript_quality_retry_improved"))
                        for x in package_items
                        if isinstance(x, dict)
                    ),
                    "transcript_quality_retry_reason": next(
                        (
                            str(x.get("transcript_quality_retry_reason") or "")
                            for x in package_items
                            if isinstance(x, dict) and str(x.get("transcript_quality_retry_reason") or "").strip()
                        ),
                        "",
                    ),
                    "selection_candidates_debug": [
                        {
                            "deal_id": c.get("deal_id", ""),
                            "daily_selection_rank": c.get("_daily_selection_rank", ""),
                            "daily_selection_reason": c.get("_daily_selection_reason", ""),
                            "llm_daily_rank": c.get("llm_daily_rank", ""),
                            "llm_daily_rank_reason": c.get("llm_daily_rank_reason", ""),
                            "llm_call_analysis_viability": c.get("llm_call_analysis_viability", ""),
                            "llm_call_analysis_viability_reason": c.get("llm_call_analysis_viability_reason", ""),
                            "skip_for_daily_reason": c.get("skip_for_daily_reason", ""),
                            "transcript_usability_label": c.get("transcript_usability_label", ""),
                            "transcript_usability_score_final": c.get("transcript_usability_score_final", ""),
                            "daily_tier": c.get("_daily_tier", ""),
                            "call_role_signal": c.get("_call_role_signal", ""),
                            "autoanswer_flag": c.get("_autoanswer_flag", False),
                            "redial_flag": c.get("_redial_flag", False),
                            "usable_flag": c.get("_usable_flag", False),
                            "evidence_richness_score": c.get("_evidence_richness_score", ""),
                            "funnel_relevance_score": c.get("_funnel_relevance_score", ""),
                            "management_value_score": c.get("_management_value_score", ""),
                            "stage_priority_weight_value": c.get("_stage_priority_weight_value", ""),
                        }
                        for c in package_items
                        if isinstance(c, dict)
                    ],
                    "skipped_candidates_debug": skipped_candidates_debug,
                }
            )
            if primary_source == "conversation_pool":
                daily_rows_from_conversation_pool += 1
            elif primary_source == "discipline_pool":
                daily_rows_from_discipline_pool += 1
            if int(transcript_usability_score or 0) >= 2:
                daily_rows_with_real_transcript += 1
            if primary_source == "discipline_pool" and int(transcript_usability_score or 0) < 2:
                daily_rows_with_only_discipline_signals += 1
            daily_rows_skipped_crm_only_total += int(excluded_crm_only_cases_count or 0)

    rows = _apply_daily_text_antirepeat(rows)

    return {
        "mode": "daily_control",
        "sheet_name": "Разбор звонков",
        "start_cell": "A2",
        "columns": list(DAILY_CONTROL_COLUMNS),
        "rows": rows,
        "rows_count": len(rows),
        "daily_multistep_pipeline": {
            "enabled": True,
            "step_failures_count": len(step_failures),
            "step_failures": step_failures,
            "step_artifacts_count": len(step_artifact_paths),
            "step_artifacts": step_artifact_paths,
        },
        "selection_debug_summary": {
            "daily_rows_from_conversation_pool": daily_rows_from_conversation_pool,
            "daily_rows_from_discipline_pool": daily_rows_from_discipline_pool,
            "daily_rows_skipped_crm_only": daily_rows_skipped_crm_only_total,
            "daily_rows_with_real_transcript": daily_rows_with_real_transcript,
            "daily_rows_with_only_discipline_signals": daily_rows_with_only_discipline_signals,
        },
    }


def _load_daily_style_source_excerpt(*, logger: Any | None, cfg: DealAnalyzerConfig | None = None) -> str:
    app = load_config()
    paths: list[Path] = []
    paths.append(app.project_root / "docs" / "мой паттерн общения.txt")
    telegram_root = app.project_root / "docs" / "style_sources" / "telegram_ilya"
    if telegram_root.exists():
        for suffix in ("*.txt", "*.md", "*.html"):
            paths.extend(sorted(telegram_root.rglob(suffix)))

    loaded_paths: list[str] = []
    chunks: list[str] = []
    seen: set[str] = set()
    for path in paths:
        p = str(path.resolve())
        if p in seen or not path.exists():
            continue
        seen.add(p)
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            try:
                text = path.read_text(encoding="utf-8-sig")
            except Exception as exc:
                if logger is not None:
                    logger.warning("daily style/reference source unreadable: path=%s error=%s", path, exc)
                continue
        compact = " ".join(text.split()).strip()
        if not compact:
            continue
        loaded_paths.append(str(path))
        chunks.append(compact[:1800])

    if logger is not None:
        logger.info("daily style sources loaded: count=%s paths=%s", len(loaded_paths), "; ".join(loaded_paths[:12]))
    if not chunks:
        return ""
    return " ".join(chunks)[:5000]


def _safe_slug(value: str) -> str:
    text = "".join(ch if ch.isalnum() else "_" for ch in str(value or ""))
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def _write_step_artifact(path: Path, payload: dict[str, Any] | list[Any] | str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _build_daily_primary_messages(
    *,
    factual_payload: dict[str, Any],
    style_mode: str = "mild",
) -> list[dict[str, str]]:
    style_line = (
        "Стиль: живой и рабочий, можно чуть жестче, но без личных оскорблений."
        if str(style_mode or "mild") == "work_rude"
        else "Стиль: живой, деловой, разговорный, без бюрократии."
    )
    reference_block = build_reference_prompt_section(
        factual_payload.get("reference_stack", {}) if isinstance(factual_payload.get("reference_stack"), dict) else {}
    )
    system = (
        "Ты сильный руководитель продаж. Разбери кейс живым рабочим языком, без канцелярита. "
        "Опирайся только на факты из входа и обязательные референсы. Не выдумывай. "
        f"{style_line}"
    )
    user = (
        "Сделай свободный разбор кейса (без JSON), коротко и предметно:\n"
        "1) что реально произошло в коммуникации,\n"
        "2) что было хорошо,\n"
        "3) где недожали,\n"
        "4) какой конкретный рычаг правки.\n\n"
        f"FACTS:\n{json.dumps(factual_payload, ensure_ascii=False, indent=2)}\n\n"
        f"REFERENCE_STACK:\n{reference_block or '(no references loaded)'}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _build_daily_effect_messages(
    *,
    factual_payload: dict[str, Any],
    free_text: str,
    effect_forecast: dict[str, Any],
    style_mode: str = "mild",
) -> list[dict[str, str]]:
    style_line = (
        "Пиши плотнее и предметнее, допускается умеренно жесткая рабочая лексика."
        if str(style_mode or "mild") == "work_rude"
        else "Пиши коротко и по делу живым рабочим языком."
    )
    reference_block = build_reference_prompt_section(
        factual_payload.get("reference_stack", {}) if isinstance(factual_payload.get("reference_stack"), dict) else {}
    )
    system = (
        "Ты дополняешь готовый разбор управленческим смыслом. Не переписывай разбор заново, "
        "не меняй факты, не выдумывай. "
        f"{style_line}"
    )
    user = (
        "Добавь второй слой:\n"
        "- зачем сотруднику закрыть эти зоны,\n"
        "- что станет лучше по качеству,\n"
        "- какой осторожный эффект в количестве (в штуках за неделю).\n\n"
        f"FREE_ANALYSIS:\n{free_text}\n\n"
        f"FORECAST_HINT:\n{json.dumps(effect_forecast, ensure_ascii=False, indent=2)}\n\n"
        f"FACTS:\n{json.dumps(factual_payload, ensure_ascii=False, indent=2)}\n\n"
        f"REFERENCE_STACK:\n{reference_block or '(no references loaded)'}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _build_daily_block_split_messages(
    *,
    free_text: str,
    effect_text: str,
    case_policy: dict[str, Any],
) -> list[dict[str, str]]:
    blocked = case_policy.get("blocked_topics", []) if isinstance(case_policy, dict) else []
    allowed = case_policy.get("allowed_topics", []) if isinstance(case_policy, dict) else []
    required_headers = "\n".join(f"### {k}" for k in DAILY_BLOCK_KEYS)
    system = (
        "Разложи готовый смысл по блокам. Не анализируй заново, не добавляй новые сущности, "
        "не уходи в CRM hygiene по умолчанию."
    )
    user = (
        "Нужен markdown только с такими заголовками и в таком формате:\n"
        f"{required_headers}\n\n"
        "Жесткие правила:\n"
        "- сохранить смысл из входного анализа;\n"
        "- не добавлять новые факты;\n"
        f"- запрещенные темы: {', '.join(str(x) for x in blocked) or '-'};\n"
        f"- разрешенные акценты: {', '.join(str(x) for x in allowed) or '-'}.\n\n"
        f"PRIMARY_ANALYSIS:\n{free_text}\n\nEFFECT_LAYER:\n{effect_text}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _build_daily_style_messages(
    *,
    blocks_markdown: str,
    style_source_excerpt: str,
    style_mode: str = "mild",
) -> list[dict[str, str]]:
    style_hint = style_source_excerpt[:3000] if style_source_excerpt else "(style source unavailable)"
    style_line = (
        "Разрешен умеренно жесткий рабочий тон (например: завис, просрал шаг, проебал момент), "
        "но без оскорблений личности и токсичного буллинга."
        if str(style_mode or "mild") == "work_rude"
        else "Тон спокойный рабочий, живой и разговорный."
    )
    system = (
        "Ты редактор управленческого текста. Сделай только style rewrite под рабочий живой лексикон. "
        "Не меняй факты и смысл, не добавляй новые сущности. "
        f"{style_line}"
    )
    user = (
        "Перепиши стиль блоков ниже, сохрани те же заголовки `### key` и тот же смысл.\n"
        "Без JSON, только markdown блоки.\n\n"
        f"Style source:\n{style_hint}\n\n"
        f"BLOCKS:\n{blocks_markdown}"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def _run_daily_multistep_pipeline(
    *,
    cfg: DealAnalyzerConfig | None,
    logger: Any | None,
    llm_runtime: dict[str, Any] | None,
    manager: str,
    control_day: str,
    package_items: list[dict[str, Any]],
    factual_payload: dict[str, Any],
    effect_forecast: dict[str, Any],
    style_source_excerpt: str,
    case_policy: dict[str, Any],
    debug_root: Path | None,
) -> dict[str, Any]:
    runtime = llm_runtime or {}
    if cfg is None or not debug_root:
        return {"ok": False, "failed_step": "pipeline_not_configured", "error": "missing_cfg_or_debug_root"}
    selected = str(runtime.get("selected") or "none")
    if selected not in {"main", "fallback"}:
        return {"ok": False, "failed_step": "runtime", "error": f"selected_runtime={selected}"}

    manager_slug = _safe_slug(manager)
    day_slug = _safe_slug(control_day)
    run_key = f"{control_day}_{manager}_{len(package_items)}"
    run_slug = _safe_slug(run_key)[:80]
    row_dir = debug_root / f"{day_slug}__{manager_slug}__{run_slug}"
    step_artifacts: dict[str, str] = {}
    source_by_step: dict[str, str] = {}

    try:
        # step 1: candidate selection
        step1 = {
            "step": "candidate_selection",
            "manager": manager,
            "control_day": control_day,
            "selected_count": len(package_items),
            "selected_deals": [
                {
                    "deal_id": item.get("deal_id"),
                    "deal_name": item.get("deal_name", ""),
                    "daily_selection_rank": item.get("_daily_selection_rank", ""),
                    "daily_selection_reason": item.get("_daily_selection_reason", ""),
                    "skip_for_daily_reason": item.get("skip_for_daily_reason", ""),
                    "transcript_usability_label": item.get("transcript_usability_label", ""),
                }
                for item in package_items
                if isinstance(item, dict)
            ],
            "skipped_candidates": (
                package_items[0].get("_daily_skipped_candidates", [])
                if package_items and isinstance(package_items[0], dict)
                else []
            ),
        }
        step_artifacts["1_candidate_selection"] = _write_step_artifact(row_dir / "01_candidate_selection.json", step1)

        # step 2: primary free-form analysis
        messages_step2 = _build_daily_primary_messages(
            factual_payload=factual_payload,
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        text2, source2 = _llm_chat_text_with_runtime(
            runtime=runtime,
            messages=messages_step2,
            logger=logger,
            log_prefix=f"daily step2 primary manager={manager} day={control_day}",
        )
        if not text2:
            return {"ok": False, "failed_step": "primary_free_form", "error": source2, "step_artifacts": step_artifacts}
        source_by_step["primary_free_form"] = source2
        step_artifacts["2_primary_free_form"] = _write_step_artifact(row_dir / "02_primary_free_form.md", text2)

        # step 3: effect/motivation layer
        messages_step3 = _build_daily_effect_messages(
            factual_payload=factual_payload,
            free_text=text2,
            effect_forecast=effect_forecast,
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        text3, source3 = _llm_chat_text_with_runtime(
            runtime=runtime,
            messages=messages_step3,
            logger=logger,
            log_prefix=f"daily step3 effect manager={manager} day={control_day}",
        )
        if not text3:
            return {"ok": False, "failed_step": "effect_layer", "error": source3, "step_artifacts": step_artifacts}
        source_by_step["effect_layer"] = source3
        step_artifacts["3_effect_layer"] = _write_step_artifact(row_dir / "03_effect_layer.md", text3)

        # step 4: block split
        messages_step4 = _build_daily_block_split_messages(
            free_text=text2,
            effect_text=text3,
            case_policy=case_policy,
        )
        text4, source4 = _llm_chat_text_with_runtime(
            runtime=runtime,
            messages=messages_step4,
            logger=logger,
            log_prefix=f"daily step4 block_split manager={manager} day={control_day}",
        )
        if not text4:
            return {"ok": False, "failed_step": "block_split", "error": source4, "step_artifacts": step_artifacts}
        source_by_step["block_split"] = source4
        step_artifacts["4_block_split_md"] = _write_step_artifact(row_dir / "04_block_split.md", text4)
        blocks = parse_blocks_markdown(text4)
        missing_step4 = validate_blocks(blocks)
        step_artifacts["4_block_split_json"] = _write_step_artifact(
            row_dir / "04_block_split.json",
            {"blocks": blocks, "missing_blocks": missing_step4},
        )
        if missing_step4:
            return {
                "ok": False,
                "failed_step": "block_split_validation",
                "error": f"missing_blocks={','.join(missing_step4)}",
                "step_artifacts": step_artifacts,
            }

        # step 5: style rewrite
        blocks_md = "\n\n".join([f"### {k}\n{blocks.get(k, '')}" for k in DAILY_BLOCK_KEYS]).strip()
        messages_step5 = _build_daily_style_messages(
            blocks_markdown=blocks_md,
            style_source_excerpt=style_source_excerpt,
            style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
        )
        text5, source5 = _llm_chat_text_with_runtime(
            runtime=runtime,
            messages=messages_step5,
            logger=logger,
            log_prefix=f"daily step5 style manager={manager} day={control_day}",
        )
        if not text5:
            return {"ok": False, "failed_step": "style_rewrite", "error": source5, "step_artifacts": step_artifacts}
        source_by_step["style_rewrite"] = source5
        step_artifacts["5_style_rewrite_md"] = _write_step_artifact(row_dir / "05_style_rewrite.md", text5)
        styled_blocks = parse_blocks_markdown(text5)
        missing_step5 = validate_blocks(styled_blocks)
        step_artifacts["5_style_rewrite_json"] = _write_step_artifact(
            row_dir / "05_style_rewrite.json",
            {"styled_blocks": styled_blocks, "missing_blocks": missing_step5},
        )
        if missing_step5:
            return {
                "ok": False,
                "failed_step": "style_validation",
                "error": f"missing_blocks={','.join(missing_step5)}",
                "step_artifacts": step_artifacts,
            }

        # step 6: final JSON assembler
        columns = assemble_writer_columns(styled_blocks)
        step6 = {
            "source_of_truth": "styled_blocks",
            "assembler_only": True,
            "columns": columns,
            "blocks": styled_blocks,
        }
        step_artifacts["6_final_assemble"] = _write_step_artifact(row_dir / "06_final_assemble.json", step6)

        # step 7: writer is handled outside, here we only mark readiness
        step_artifacts["7_writer_ready"] = _write_step_artifact(
            row_dir / "07_writer_ready.json",
            {
                "ready": True,
                "source_of_truth": "styled_blocks",
                "assembler_only": True,
            },
        )
        return {
            "ok": True,
            "columns": columns,
            "step_artifacts": step_artifacts,
            "source_by_step": source_by_step,
            "source_of_truth": "styled_blocks",
            "assembler_only": True,
        }
    except Exception as exc:
        return {
            "ok": False,
            "failed_step": "unexpected",
            "error": str(exc),
            "step_artifacts": step_artifacts,
            "source_by_step": source_by_step,
        }


def _generate_daily_table_text_columns(
    *,
    cfg: DealAnalyzerConfig | None,
    logger: Any | None,
    backend_effective: str | None,
    manager: str,
    role: str,
    control_day: str,
    period_start: str,
    period_end: str,
    package_items: list[dict[str, Any]],
    links: str,
    focus: str,
    base_mix: str,
    avg_score: int | None,
    criticality: str,
    selection_reason: str,
    growth_candidates: list[str],
    fallback_columns: dict[str, str],
    effect_forecast: dict[str, Any],
    style_source_excerpt: str,
    llm_runtime: dict[str, Any] | None = None,
    case_policy: dict[str, Any] | None = None,
    factual_payload_override: dict[str, Any] | None = None,
) -> dict[str, str]:
    if cfg is None:
        out = dict(fallback_columns)
        out["_llm_text_ready"] = False
        return out
    if (backend_effective or cfg.analyzer_backend) not in {"ollama", "hybrid"}:
        out = dict(fallback_columns)
        out["_llm_text_ready"] = False
        return out
    runtime = llm_runtime or {}
    client = _make_llm_client_from_runtime(runtime)
    if client is None:
        out = dict(fallback_columns)
        out["_llm_text_ready"] = False
        return out

    factual_payload = factual_payload_override
    if not isinstance(factual_payload, dict):
        factual_payload = _build_daily_table_factual_payload(
            cfg=cfg,
            logger=logger,
            manager=manager,
            role=role,
            control_day=control_day,
            period_start=period_start,
            period_end=period_end,
            package_items=package_items,
            links=links,
            focus=focus,
            base_mix=base_mix,
            avg_score=avg_score,
            criticality=criticality,
            selection_reason=selection_reason,
            growth_candidates=growth_candidates,
            fallback_columns=fallback_columns,
            effect_forecast=effect_forecast,
            case_policy=case_policy or {},
        )
    messages = build_daily_table_messages(
        factual_payload=factual_payload,
        config=cfg,
        style_source_excerpt=style_source_excerpt,
        style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
    )
    payload, used_source = _llm_chat_json_with_runtime(
        runtime=runtime,
        messages=messages,
        repair_messages=append_daily_table_json_repair_instruction(messages),
        logger=logger,
        log_prefix=f"daily llm text generation manager={manager} day={control_day}",
    )
    if payload is None:
        out = dict(fallback_columns)
        out["_llm_text_ready"] = False
        return out
    content_applied = True
    if logger is not None:
        logger.info(
            "daily llm text generated: manager=%s day=%s backend=%s source=%s",
            manager,
            control_day,
            backend_effective or cfg.analyzer_backend,
            used_source,
        )

    mapped = _sanitize_daily_llm_columns(
        payload=payload if isinstance(payload, dict) else {},
        fallback=fallback_columns,
        role=role,
        case_policy=case_policy or {},
    )
    rewritten, style_applied = _style_rewrite_daily_columns(
        cfg=cfg,
        logger=logger,
        role=role,
        columns=mapped,
        style_source_excerpt=style_source_excerpt,
        llm_runtime=runtime,
        style_mode=str(getattr(cfg, "daily_style_mode", "mild") or "mild"),
    )
    if style_applied and logger is not None:
        logger.info("daily llm style pass applied: manager=%s day=%s", manager, control_day)
    rewritten["_style_layer_applied"] = style_applied
    rewritten = _fill_missing_daily_llm_columns(
        cfg=cfg,
        logger=logger,
        llm_runtime=runtime,
        manager=manager,
        control_day=control_day,
        role=role,
        factual_payload=factual_payload,
        columns=rewritten,
    )
    llm_ready = _daily_llm_columns_ready(rewritten)
    rewritten["_content_layer_applied"] = bool(content_applied and llm_ready)
    rewritten["_llm_text_ready"] = bool(llm_ready)
    if logger is not None and not llm_ready:
        logger.warning(
            "daily llm text rejected: manager=%s day=%s reason=incomplete_columns",
            manager,
            control_day,
        )
    return rewritten


def _fill_missing_daily_llm_columns(
    *,
    cfg: DealAnalyzerConfig,
    logger: Any | None,
    llm_runtime: dict[str, Any] | None,
    manager: str,
    control_day: str,
    role: str,
    factual_payload: dict[str, Any],
    columns: dict[str, Any],
) -> dict[str, Any]:
    out = dict(columns)
    missing_keys = [key for key in DAILY_TEXT_COLUMN_KEYS if not " ".join(str(out.get(key) or "").split()).strip()]
    if not missing_keys:
        return out
    client = _make_llm_client_from_runtime(llm_runtime or {})
    if client is None:
        return out
    prompt_payload = {
        "role": role,
        "missing_keys": missing_keys,
        "current_columns": {key: str(out.get(key) or "") for key in DAILY_TEXT_COLUMN_KEYS},
        "facts": factual_payload,
    }
    messages = [
        {
            "role": "system",
            "content": (
                "Ты дополняешь только недостающие колонки daily-контроля. "
                "Не выдумывай факты, не меняй уже заполненные колонки. "
                "Верни только JSON-объект с ключами из missing_keys."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Input:\n{json.dumps(prompt_payload, ensure_ascii=False, indent=2)}\n\n"
                "Верни JSON только с недостающими ключами. "
                "Пиши коротко, живо, по делу."
            ),
        },
    ]
    payload, _ = _llm_chat_json_with_runtime(
        runtime=llm_runtime or {},
        messages=messages,
        repair_messages=append_daily_table_json_repair_instruction(messages),
        logger=logger,
        log_prefix=f"daily llm missing-columns fill manager={manager} day={control_day}",
    )
    if not isinstance(payload, dict):
        return out
    for key in missing_keys:
        value = " ".join(str(payload.get(key) or "").split()).strip()
        if value:
            out[key] = _daily_user_text(value)
    return out


def _style_rewrite_daily_columns(
    *,
    cfg: DealAnalyzerConfig,
    logger: Any | None,
    role: str,
    columns: dict[str, str],
    style_source_excerpt: str,
    llm_runtime: dict[str, Any] | None = None,
    style_mode: str = "mild",
) -> tuple[dict[str, str], bool]:
    if cfg.analyzer_backend not in {"hybrid", "ollama"}:
        return dict(columns), False
    if not style_source_excerpt.strip():
        return dict(columns), False
    client = _make_llm_client_from_runtime(llm_runtime or {})
    if client is None:
        return dict(columns), False
    payload = {
        "role": role,
        "columns": columns,
    }
    system_prompt = (
        "Ты делаешь только стилевой rewrite для управленческого daily-контроля. "
        "Не меняй факты и смысл, не добавляй новые выводы. "
        "Убери канцелярит, сделай живо и коротко. "
        f"Режим стиля: {style_mode}. "
        "В режиме work_rude допустима умеренно жесткая рабочая лексика без перехода на личности. "
        "Верни только JSON."
    )
    user_prompt = (
        "Перепиши стиль полей под живую управленческую речь.\n"
        f"Input:\n{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        "Верни JSON с теми же ключами колонок. Без новых ключей и без текста вокруг JSON.\n\n"
        f"Style reference:\n{style_source_excerpt[:2200]}"
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    payload, _ = _llm_chat_json_with_runtime(
        runtime=llm_runtime or {},
        messages=messages,
        logger=logger,
        log_prefix="daily llm style pass",
    )
    try:
        if not isinstance(payload, dict):
            return dict(columns), False
        out = dict(columns)
        for key in DAILY_TEXT_COLUMN_KEYS:
            if key in payload:
                out[key] = _daily_user_text(str(payload.get(key) or out.get(key, "")))
        return out, True
    except Exception as exc:
        if logger is not None:
            logger.warning("daily llm style pass skipped: error=%s", exc)
        return dict(columns), False


def _build_daily_table_factual_payload(
    *,
    cfg: DealAnalyzerConfig | None,
    logger: Any | None = None,
    manager: str,
    role: str,
    control_day: str,
    period_start: str,
    period_end: str,
    package_items: list[dict[str, Any]],
    links: str,
    focus: str,
    base_mix: str,
    avg_score: int | None,
    criticality: str,
    selection_reason: str,
    growth_candidates: list[str],
    fallback_columns: dict[str, str],
    effect_forecast: dict[str, Any],
    case_policy: dict[str, Any],
) -> dict[str, Any]:
    short_deals: list[dict[str, Any]] = []
    for item in package_items[:12]:
        short_deals.append(
            {
                "deal_id": item.get("deal_id"),
                "status": item.get("status_name", ""),
                "pipeline": item.get("pipeline_name", ""),
                "risk_flags": (item.get("risk_flags", []) if isinstance(item.get("risk_flags"), list) else [])[:4],
                "call_summary": str(item.get("call_signal_summary_short") or ""),
                "transcript_excerpt": str(item.get("transcript_text_excerpt") or "")[:280],
                "manager_summary": str(item.get("manager_summary") or "")[:220],
                "employee_coaching": str(item.get("employee_coaching") or "")[:220],
            }
        )
    role_scope = get_role_scope_policy(role=role, items=package_items)
    role_allowed_topics = role_scope.get("role_allowed_topics", []) if isinstance(role_scope.get("role_allowed_topics"), list) else []
    role_blocked_topics = role_scope.get("role_blocked_topics", []) if isinstance(role_scope.get("role_blocked_topics"), list) else []
    base_case_policy = dict(case_policy or {})
    merged_allowed_axes = [
        str(x) for x in (base_case_policy.get("allowed_axes", []) if isinstance(base_case_policy.get("allowed_axes"), list) else []) if str(x).strip()
    ]
    merged_allowed_axes.extend(str(x) for x in role_allowed_topics if str(x).strip() and str(x) not in merged_allowed_axes)
    merged_banned_topics = [
        str(x) for x in (base_case_policy.get("banned_topics", []) if isinstance(base_case_policy.get("banned_topics"), list) else []) if str(x).strip()
    ]
    for topic in role_blocked_topics:
        topic_text = str(topic or "").strip()
        if topic_text and topic_text not in merged_banned_topics:
            merged_banned_topics.append(topic_text)
    merged_case_policy = dict(base_case_policy)
    merged_case_policy["allowed_axes"] = merged_allowed_axes
    merged_case_policy["banned_topics"] = merged_banned_topics
    merged_case_policy["role_scope_applied"] = bool(role_scope.get("role_scope_applied"))
    merged_case_policy["role_allowed_topics"] = role_allowed_topics
    merged_case_policy["role_blocked_topics"] = role_blocked_topics
    merged_case_policy["role_scope_conflict_flag"] = bool(role_scope.get("role_scope_conflict_flag"))
    sales_toolbox_modules = (
        list(getattr(cfg, "sales_module_references", ()) or [])
        if cfg is not None and getattr(cfg, "sales_module_references", ())
        else [
            "проход секретаря и выход на ЛПР",
            "формирующие вопросы по боли и задаче",
            "фиксация следующего шага с датой",
            "подтверждение встречи/демо",
            "короткий post-call summary в CRM",
        ]
    )
    product_reference_urls = dict(getattr(cfg, "product_reference_urls", {}) or {}) if cfg is not None else {}
    payload = {
        "manager_name": manager,
        "role": role,
        "control_day": control_day,
        "period_start": period_start,
        "period_end": period_end,
        "deal_links": links,
        "product_focus": focus,
        "base_mix": base_mix,
        "score_0_100": avg_score,
        "criticality": criticality,
        "selection_reason": selection_reason,
        "deals": short_deals,
        "growth_candidates": growth_candidates,
        "effect_forecast_facts": effect_forecast,
        "case_policy": merged_case_policy,
        "role_forbidden_topics": role_blocked_topics,
        "role_allowed_topics": role_allowed_topics,
        "role_scope_applied": bool(role_scope.get("role_scope_applied")),
        "role_scope_conflict_flag": bool(role_scope.get("role_scope_conflict_flag")),
        "sales_toolbox_modules": sales_toolbox_modules,
        "product_reference_urls": product_reference_urls,
        "fallback_reference": fallback_columns,
        "data_confidence_hint": _daily_confidence_hint(package_items),
        "style_mode": str(getattr(cfg, "daily_style_mode", "mild") or "mild") if cfg is not None else "mild",
    }
    payload["reference_stack"] = build_daily_reference_stack(
        cfg=cfg,
        factual_payload=payload,
        logger=logger,
    )
    return payload


def _daily_confidence_hint(items: list[dict[str, Any]]) -> str:
    low = sum(1 for x in items if _is_low_confidence_record(x))
    if not items:
        return "low"
    if low >= max(1, len(items) // 2):
        return "low"
    return "normal"


def _daily_analysis_mode(items: list[dict[str, Any]]) -> str:
    usable = any(_transcript_usability_score(x) >= 2 for x in items if isinstance(x, dict))
    dial = any(
        bool(x.get("repeated_dead_redial_day_flag"))
        or bool(x.get("same_time_redial_pattern_flag"))
        or bool(x.get("numbers_not_fully_covered_flag"))
        for x in items
        if isinstance(x, dict)
    )
    if usable:
        return "negotiation_analysis"
    if dial:
        return "dial_discipline_analysis"
    return "none"


def _sanitize_daily_llm_columns(
    *,
    payload: dict[str, Any],
    fallback: dict[str, str],
    role: str,
    case_policy: dict[str, Any] | None = None,
) -> dict[str, str]:
    mapped = {
        "Ключевой вывод": _daily_user_text(str(payload.get("key_takeaway") or "")),
        "Сильные стороны": _daily_user_text(str(payload.get("strong_sides") or "")),
        "Зоны роста": _daily_user_text(str(payload.get("growth_zones") or "")),
        "Почему это важно": _daily_user_text(str(payload.get("why_important") or "")),
        "Что закрепить": _daily_user_text(str(payload.get("reinforce") or "")),
        "Что исправить": _daily_user_text(str(payload.get("fix_action") or "")),
        "Что донес сотруднику": _daily_user_text(str(payload.get("coaching_list") or "")),
        "Ожидаемый эффект - количество": _daily_user_text(
            str(payload.get("expected_quantity") or "")
        ),
        "Ожидаемый эффект - качество": _daily_user_text(
            str(payload.get("expected_quality") or "")
        ),
    }
    mapped["Ключевой вывод"] = _strip_forbidden_daily_phrases(mapped["Ключевой вывод"])
    mapped["Почему это важно"] = _strip_forbidden_daily_phrases(mapped["Почему это важно"])
    mapped["Что закрепить"] = _strip_forbidden_daily_phrases(mapped["Что закрепить"])
    mapped["Что исправить"] = _strip_forbidden_daily_phrases(mapped["Что исправить"]).replace("на ближайший цикл", "")

    growth = _sanitize_daily_growth(role=role, value=mapped["Зоны роста"], fallback="")
    mapped["Зоны роста"] = growth
    if mapped["Что исправить"].strip() == growth.strip():
        mapped["Что исправить"] = ""

    mapped["Что донес сотруднику"] = _sanitize_daily_coaching_text(
        value=mapped["Что донес сотруднику"],
        fallback=str(fallback.get("Что донес сотруднику") or ""),
    )
    mapped["Ожидаемый эффект - количество"] = _sanitize_daily_expected_quantity(
        value=mapped["Ожидаемый эффект - количество"],
        fallback=str(fallback.get("Ожидаемый эффект - количество") or ""),
    )
    mapped["Ожидаемый эффект - качество"] = _sanitize_daily_expected_quality(
        value=mapped["Ожидаемый эффект - качество"],
        fallback=str(fallback.get("Ожидаемый эффект - качество") or ""),
    )
    banned_topics = case_policy.get("banned_topics", []) if isinstance(case_policy, dict) else []
    if banned_topics:
        # apply explicit policy bans verbatim (for stricter mode-specific contract)
        for key in (
            "Ключевой вывод",
            "Сильные стороны",
            "Зоны роста",
            "Почему это важно",
            "Что закрепить",
            "Что исправить",
            "Что донес сотруднику",
        ):
            text = " ".join(str(mapped.get(key) or "").split()).strip()
            low = text.lower()
            for token in banned_topics:
                token_low = str(token or "").strip().lower()
                if token_low and token_low in low:
                    text = re.sub(re.escape(token_low), "", text, flags=re.IGNORECASE)
                    low = text.lower()
            mapped[key] = " ".join(text.split()).strip(" .;,-")
    mapped, role_scope_debug = _apply_role_scope_to_daily_texts(
        role=role,
        mapped=mapped,
        case_policy=case_policy if isinstance(case_policy, dict) else {},
    )
    mapped["_role_scope_applied"] = bool(role_scope_debug.get("role_scope_applied"))
    mapped["_role_blocked_topics"] = role_scope_debug.get("role_blocked_topics", "")
    mapped["_role_allowed_topics"] = role_scope_debug.get("role_allowed_topics", "")
    mapped["_role_scope_conflict_flag"] = bool(role_scope_debug.get("role_scope_conflict_flag"))
    return mapped


def _apply_role_scope_to_daily_texts(
    *,
    role: str,
    mapped: dict[str, str],
    case_policy: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    out = dict(mapped)
    role_scope_applied = bool(case_policy.get("role_scope_applied", False))
    role_blocked_topics = [
        str(x).strip() for x in (case_policy.get("role_blocked_topics", []) if isinstance(case_policy.get("role_blocked_topics"), list) else []) if str(x).strip()
    ]
    role_allowed_topics = [
        str(x).strip() for x in (case_policy.get("role_allowed_topics", []) if isinstance(case_policy.get("role_allowed_topics"), list) else []) if str(x).strip()
    ]
    role_scope_conflict_flag = bool(case_policy.get("role_scope_conflict_flag", False))
    if not role_scope_applied:
        return out, {
            "role_scope_applied": False,
            "role_blocked_topics": "",
            "role_allowed_topics": "",
            "role_scope_conflict_flag": False,
        }

    for key in (
        "Сильные стороны",
        "Зоны роста",
        "Что исправить",
        "Что донес сотруднику",
        "Почему это важно",
        "Ожидаемый эффект - количество",
        "Ожидаемый эффект - качество",
    ):
        text = " ".join(str(out.get(key) or "").split()).strip()
        if not text:
            continue
        lowered = text.lower()
        for token in role_blocked_topics:
            token_low = token.lower()
            if token_low and token_low in lowered:
                text = re.sub(re.escape(token_low), "", text, flags=re.IGNORECASE)
                lowered = text.lower()
        out[key] = " ".join(text.split()).strip(" .;,-")

    if "телемаркетолог" in str(role).lower():
        # For cold role, aggressively protect from warm-stage leakage unless explicit conflict override is set.
        if not role_scope_conflict_flag:
            warm_markers = ("презентац", "демо", "демонстрац", "бриф", "тест", "кп", "счет", "оплат")
            for key in ("Сильные стороны", "Зоны роста", "Что исправить", "Что донес сотруднику"):
                text = " ".join(str(out.get(key) or "").split()).strip()
                low = text.lower()
                if any(m in low for m in warm_markers):
                    for marker in warm_markers:
                        if marker in low:
                            text = re.sub(marker + r"\w*", "", text, flags=re.IGNORECASE)
                            low = text.lower()
                out[key] = " ".join(text.split()).strip(" .;,-")

    return out, {
        "role_scope_applied": True,
        "role_blocked_topics": ", ".join(role_blocked_topics),
        "role_allowed_topics": ", ".join(role_allowed_topics),
        "role_scope_conflict_flag": role_scope_conflict_flag,
    }


def _strip_forbidden_daily_phrases(value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    replacements = {
        "qualified loss": "не наш кейс",
        "anti-fit": "не наш кейс",
        "market mismatch": "не наш кейс",
        "owner ambiguity": "по этой сделке выводы пока предварительные",
        "owner attribution": "по этой сделке выводы пока предварительные",
        "атрибуция owner": "выводы пока предварительные",
        "factual layer": "фактические сигналы из звонков и crm",
        "fact pattern": "повторяющийся рабочий паттерн",
        "process hygiene": "рабочая дисциплина по сделке",
        "evidence context": "контекст звонков и crm",
        "follow-up": "следующий шаг",
        "closeout": "закрытие причины отказа",
        "ограниченная надежность выводов": "выводы пока предварительные",
        "контекст последних касаний": "что было зафиксировано в CRM",
        "проверить фактуру в crm": "перепроверить по CRM",
        "сверить кто по факту вел": "уточнить фактическое ведение",
    }
    low = text.lower()
    for old, new in replacements.items():
        if old in low:
            pattern = re.compile(re.escape(old), flags=re.IGNORECASE)
            text = pattern.sub(new, text)
            low = text.lower()
    return text


def _sanitize_daily_growth(*, role: str, value: str, fallback: str) -> str:
    parts = [x.strip(" .;-") for x in re.split(r"[;\n]+", value or "") if x.strip(" .;-")]
    out: list[str] = []
    warm_markers = ("презентац", "демо", "демонстрац", "бриф", "тест")
    for part in parts:
        low = part.lower()
        if "телемаркетолог" in str(role).lower() and any(m in low for m in warm_markers):
            continue
        if part not in out:
            out.append(part)
    if not out:
        out = [x.strip() for x in str(fallback or "").split(";") if x.strip()]
    return "; ".join(out[:2])


def _sanitize_daily_coaching_text(*, value: str, fallback: str) -> str:
    text = str(value or "").replace("Донес:", "").replace("донес:", "").strip()
    lines = [x.strip(" -") for x in text.splitlines() if x.strip()]
    numbered = [x for x in lines if re.match(r"^[1-3]\)", x)]
    if len(numbered) >= 3:
        return "\n".join(numbered[:3])
    chunks = re.split(r"[.;]+", text)
    chunks = [x.strip(" -") for x in chunks if x.strip()]
    if len(chunks) < 3:
        fallback_lines = [x.strip() for x in str(fallback or "").splitlines() if x.strip()]
        fb_num = [x for x in fallback_lines if re.match(r"^[1-3]\)", x)]
        if len(fb_num) >= 3:
            return "\n".join(fb_num[:3])
    built: list[str] = []
    for idx, chunk in enumerate(chunks[:3], start=1):
        chunk_clean = re.sub(r"^[1-3]\)\s*", "", chunk).strip()
        built.append(f"{idx}) {chunk_clean}")
    while len(built) < 3:
        built.append(f"{len(built)+1}) Уточнить по 2-3 свежим звонкам и закрепить в работе.")
    return "\n".join(built)


def _sanitize_daily_expected_quantity(*, value: str, fallback: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return fallback
    low = text.lower()
    if "%" in low or "конверси" in low:
        return fallback
    if "этап" in low and not any(ch.isdigit() for ch in low):
        return fallback
    if re.fullmatch(r"[+\-]?\d+(?:-\d+)?", low):
        return fallback
    if re.search(r"\+?0(?:[.,]0+)?\b", low):
        return fallback
    if not any(token in low for token in ("+1", "+2", "1-2", "-1", "1 дополнитель")) and not any(
        ch.isdigit() for ch in low
    ):
        return fallback
    return text


def _sanitize_daily_expected_quality(*, value: str, fallback: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return fallback
    return _strip_forbidden_daily_phrases(text)


def _daily_llm_columns_ready(columns: dict[str, Any]) -> bool:
    # J-Q must come from live LLM output for real write; partial/empty output is treated as not ready.
    required = (
        "Ключевой вывод",
        "Сильные стороны",
        "Зоны роста",
        "Почему это важно",
        "Что закрепить",
        "Что исправить",
        "Что донес сотруднику",
        "Ожидаемый эффект - количество",
        "Ожидаемый эффект - качество",
    )
    for key in required:
        if not " ".join(str(columns.get(key) or "").split()).strip():
            return False
    return True


def _apply_daily_text_antirepeat(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    by_manager: dict[str, list[int]] = {}
    for idx, row in enumerate(rows):
        manager = " ".join(str(row.get("Менеджер") or "").split()).strip().lower()
        by_manager.setdefault(manager, []).append(idx)

    for _, idxs in by_manager.items():
        prev_norm: dict[str, str] = {}
        for pos in idxs:
            row = rows[pos]
            day = str(row.get("День") or "").strip()
            focus = str(row.get("Продукт / фокус") or "").strip()
            for key in ("Ключевой вывод", "Сильные стороны", "Зоны роста", "Почему это важно", "Что исправить"):
                text = " ".join(str(row.get(key) or "").split()).strip()
                norm = re.sub(r"\s+", " ", text).strip().lower()
                if not text:
                    continue
                if prev_norm.get(key) == norm:
                    if key == "Ключевой вывод":
                        row[key] = f"{text} Акцент {day.lower()}: {focus or 'разбор по живым кейсам'}."
                    elif key == "Сильные стороны":
                        row[key] = f"{text} В этом дне лучше всего сработало: {focus or 'связка вопрос -> следующий шаг'}."
                    elif key == "Зоны роста":
                        row[key] = f"{text}; отдельный фокус дня: не терять ритм по дозвонам."
                    elif key == "Почему это важно":
                        row[key] = f"{text} Это даст спокойнее вести день и быстрее видеть, где сделка реально движется."
                    elif key == "Что исправить":
                        row[key] = f"{text} Сегодня акцент: один четкий следующий шаг по каждому живому кейсу."
                    norm = re.sub(r"\s+", " ", str(row.get(key) or "")).strip().lower()
                prev_norm[key] = norm
            rows[pos] = row
    return rows


def _build_weekly_manager_sheet_payload(
    *,
    run_timestamp: datetime,
    week_start: str,
    week_end: str,
    rustam_records: list[dict[str, Any]],
    ilya_records: list[dict[str, Any]],
) -> dict[str, Any]:
    control_date = run_timestamp.date().isoformat()
    day_label = _weekday_ru_from_iso(control_date)
    rows = [
        _build_weekly_manager_row_dict(
            manager_name="Рустам",
            role_focus="Холодный этап",
            records=rustam_records,
            week_start=week_start,
            week_end=week_end,
            control_date=control_date,
            day_label=day_label,
        ),
        _build_weekly_manager_row_dict(
            manager_name="Илья",
            role_focus="Теплый этап",
            records=ilya_records,
            week_start=week_start,
            week_end=week_end,
            control_date=control_date,
            day_label=day_label,
        ),
    ]
    return {
        "mode": "weekly_manager_summary",
        "sheet_name": "Недельный свод менеджеров",
        "start_cell": "A2",
        "columns": list(WEEKLY_MANAGER_COLUMNS),
        "rows": rows,
        "rows_count": len(rows),
    }


def _build_weekly_manager_row_dict(
    *,
    manager_name: str,
    role_focus: str,
    records: list[dict[str, Any]],
    week_start: str,
    week_end: str,
    control_date: str,
    day_label: str,
) -> dict[str, Any]:
    analyzed = len(records)
    scores = [int(x.get("score")) for x in records if isinstance(x.get("score"), int)]
    avg_score = round(sum(scores) / len(scores)) if scores else ""
    top_risks = _collect_top_risk_flags(records, limit=3)
    strong = _collect_short_list(records, "strong_sides", limit=3)
    growth = _collect_short_list(records, "growth_zones", limit=3)
    manager_note = _humanize_work_text(_collect_short_text(records, "manager_summary", limit=1))
    employee_note = _humanize_work_text(_collect_short_text(records, "employee_coaching", limit=1))
    repeated_errors = _humanize_work_text("; ".join(str(x) for x in top_risks))
    training_tasks = _humanize_work_text(_collect_short_tasks(records, field="employee_fix_tasks", limit=3))
    improved = _humanize_work_text("; ".join(str(x) for x in strong))
    not_improved = _humanize_work_text("; ".join(str(x) for x in growth) if growth else repeated_errors)
    manager_next_week = _humanize_work_text(_weekly_manager_actions_line(records=records, manager_name=manager_name))
    return {
        "Неделя с": week_start,
        "Неделя по": week_end,
        "Менеджер": manager_name,
        "Роль менеджера": role_focus,
        "Проанализировано сделок": analyzed,
        "Продукт / фокус недели": _top_product_focus(records),
        "База микс недели": _build_base_mix_text(records),
        "Итог недели": manager_note or _humanize_work_text(top_risks[0] if top_risks else ""),
        "Что улучшилось": improved,
        "Что не улучшилось": not_improved,
        "Повторяющиеся ошибки": repeated_errors,
        "Обучение сотруднику": employee_note,
        "Ссылка на обучение": "",
        "Задачи после обучения": training_tasks,
        "Ссылка на задачи после обучения": "",
        "Мои действия на следующую неделю": manager_next_week,
        "Ожидаемый эффект - количество": _expected_quantity_text(
            avg_score=avg_score if isinstance(avg_score, int) else None,
            deals=analyzed,
            role=role_focus,
        ),
        "Ожидаемый эффект - качество": _expected_quality_text(
            criticality=_score_to_criticality(avg_score if isinstance(avg_score, int) else None, risk_count=len(top_risks)),
            role=role_focus,
        ),
        "Формулировка для руководителя": manager_note,
        "Сообщение сотруднику": employee_note,
        "Средняя оценка 0-100": avg_score,
    }


def _collect_top_risk_flags(records: list[dict[str, Any]], *, limit: int) -> list[str]:
    risk_counts: Counter[str] = Counter()
    for item in records:
        flags = item.get("risk_flags") if isinstance(item.get("risk_flags"), list) else []
        for flag in flags:
            text = str(flag).strip()
            if text:
                risk_counts[text] += 1
    return [k for k, _ in risk_counts.most_common(max(1, int(limit)))]


def _collect_short_list(records: list[dict[str, Any]], field: str, *, limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in records:
        values = item.get(field) if isinstance(item.get(field), list) else []
        for value in values:
            text = " ".join(str(value or "").split()).strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= max(1, int(limit)):
                return out
    return out


def _collect_short_text(records: list[dict[str, Any]], field: str, *, limit: int) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for item in records:
        text = " ".join(str(item.get(field) or "").split()).strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        parts.append(text)
        if len(parts) >= max(1, int(limit)):
            break
    return " ".join(parts)


def _collect_short_tasks(records: list[dict[str, Any]], *, field: str, limit: int) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for item in records:
        values = item.get(field) if isinstance(item.get(field), list) else []
        for value in values:
            text = " ".join(str(value or "").split()).strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= max(1, int(limit)):
                return _humanize_work_text("; ".join(out))
    return _humanize_work_text("; ".join(out))


def _weekly_manager_actions_line(*, records: list[dict[str, Any]], manager_name: str) -> str:
    low_conf = sum(1 for x in records if _is_low_confidence_record(x))
    has_losses = any(_is_loss_like_record(x) for x in records)
    if "рустам" in str(manager_name).lower():
        base = "Сверить, кто по факту ведет ключевые сделки, и дожать следующий шаг по активным кейсам."
    else:
        base = "Переслушать спорные звонки, сверить их с CRM и закрыть провалы в фиксации результата."
    if low_conf > 0:
        base += " По шумным кейсам сначала перепроверить на свежую голову."
    if has_losses:
        base += " По потерям отдельно подсветить причину, а не оставлять в каше."
    return _humanize_work_text(base)


def _top_product_focus(records: list[dict[str, Any]]) -> str:
    info_hits = 0
    link_hits = 0
    unknown_hits = 0
    for item in records:
        hypothesis = str(item.get("product_hypothesis") or "").strip().lower()
        if hypothesis == "info":
            info_hits += 1
            continue
        if hypothesis == "link":
            link_hits += 1
            continue
        if hypothesis == "mixed":
            info_hits += 1
            link_hits += 1
            continue

        text = " ".join(str(item.get("product_name") or "").split()).strip().lower()
        if any(token in text for token in ("info", "инфо")):
            info_hits += 1
        elif any(token in text for token in ("link", "линк", "закуп", "тендер")):
            link_hits += 1
        else:
            # role/speaker heuristics from call context when CRM product is empty
            call_text = " ".join(
                str(item.get(k) or "").lower()
                for k in ("call_signal_summary_short", "transcript_text_excerpt", "manager_summary")
            )
            if any(t in call_text for t in ("снабжен", "закуп", "тендер", "кп", "поставщик")):
                link_hits += 1
            elif any(t in call_text for t in ("конструктор", "техдир", "технолог", "производств", "plm", "инфо")):
                info_hits += 1
            elif any(t in call_text for t in ("оба", "и инфо", "и линк", "смешан")):
                info_hits += 1
                link_hits += 1
            else:
                unknown_hits += 1

    if info_hits and link_hits:
        return "оба"
    if info_hits:
        return "инфо"
    if link_hits:
        return "линк"
    if unknown_hits > 0:
        return "до продукта разговор не дошел"
    return "до продукта разговор не дошел"


def _build_base_mix_text(records: list[dict[str, Any]]) -> str:
    return build_base_mix_text_priority(records)


def _resolve_base_mix(records: list[dict[str, Any]]) -> dict[str, Any]:
    return resolve_base_mix_priority(records)


def _manager_role_label(manager_name: str, *, cfg: DealAnalyzerConfig | None = None) -> str:
    name = " ".join(str(manager_name or "").split()).strip()
    low = name.lower()
    registry = (
        dict(getattr(cfg, "manager_role_registry", {}) or {})
        if cfg is not None and isinstance(getattr(cfg, "manager_role_registry", None), dict)
        else {"Рустам": "telemarketer", "Илья": "sales_manager"}
    )
    role_code = ""
    for manager_key, code in registry.items():
        key_low = " ".join(str(manager_key or "").split()).strip().lower()
        if key_low and key_low in low:
            role_code = str(code or "").strip().lower()
            break
    if not role_code:
        if "рустам" in low:
            role_code = "telemarketer"
        elif "илья" in low:
            role_code = "sales_manager"
        else:
            role_code = "sales_manager"
    if role_code == "telemarketer":
        return "телемаркетолог"
    return "менеджер по продажам"


def _parse_record_activity_dt(record: dict[str, Any]) -> datetime | None:
    for key in ("updated_at", "created_at"):
        raw = record.get(key)
        if raw in (None, ""):
            continue
        if isinstance(raw, (int, float)):
            try:
                return datetime.fromtimestamp(float(raw), tz=timezone.utc)
            except Exception:
                continue
        text = str(raw).strip()
        if not text:
            continue
        if text.isdigit():
            try:
                return datetime.fromtimestamp(float(text), tz=timezone.utc)
            except Exception:
                pass
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None


def _resolve_daily_control_days(
    *,
    period_start: str,
    period_end: str,
    records: list[dict[str, Any]],
    run_date: str,
) -> list[str]:
    try:
        start = datetime.fromisoformat(period_start).date()
        end = datetime.fromisoformat(period_end).date()
    except Exception:
        fallback = str(run_date or "").strip()
        if fallback:
            return [fallback]
        return []
    if end < start:
        start, end = end, start

    out: list[str] = []
    has_saturday_activity = False
    saturday_set: set[str] = set()
    for item in records:
        dt = _parse_record_activity_dt(item)
        if not dt:
            continue
        day = dt.date()
        if day.weekday() == 5:
            saturday_set.add(day.isoformat())
            has_saturday_activity = True

    cur = start
    while cur <= end:
        wd = cur.weekday()
        if wd <= 4:
            out.append(cur.isoformat())
        elif wd == 5 and has_saturday_activity and cur.isoformat() in saturday_set:
            out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _daily_package_target(*, manager_records: list[dict[str, Any]], control_day: str) -> int:
    """Adaptive daily package size: can exceed 6 for high-volume days."""
    try:
        day = datetime.fromisoformat(control_day).date()
    except Exception:
        return 6
    same_day = 0
    call_weighted = 0
    for item in manager_records:
        dt = _parse_record_activity_dt(item)
        if dt and dt.date() == day:
            same_day += 1
            if _transcript_usability_score(item) >= 2:
                call_weighted += 2
            elif int(item.get("call_history_pattern_score", 0) or 0) >= 45:
                call_weighted += 1
    base = max(4, min(12, same_day // 2 + call_weighted // 2))
    if same_day >= 14:
        return max(base, 8)
    if same_day <= 2:
        return min(base, 4)
    return base


def _select_daily_package_records(
    *,
    manager_records: list[dict[str, Any]],
    control_day: str,
    package_target: int,
    carryover_days: int,
    exclude_deal_ids: set[str] | None = None,
    stage_priority_weights: dict[str, float] | None = None,
    cfg: DealAnalyzerConfig | None = None,
    logger: Any | None = None,
    backend_effective: str | None = None,
    manager: str = "",
    role: str = "",
    style_source_excerpt: str = "",
    llm_runtime: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    try:
        day = datetime.fromisoformat(control_day).date()
    except Exception:
        return manager_records[: max(1, package_target)]

    cutoff = datetime(day.year, day.month, day.day, 14, 0, 0, tzinfo=timezone.utc)
    carry_floor = cutoff.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=max(0, int(carryover_days)))

    ranked: list[tuple[tuple[int, float, int, int, int, int, str], dict[str, Any], str]] = []
    excluded = exclude_deal_ids or set()
    for item in manager_records:
        did = str(item.get("deal_id") or "").strip()
        if did and did in excluded:
            continue
        dt = _parse_record_activity_dt(item)
        if dt and dt > cutoff:
            continue
        if dt and dt < carry_floor:
            continue
        freshness = _freshness_rank_for_day(dt=dt, control_day=day)
        transcript_score = _transcript_usability_score(item)
        evidence_score = _evidence_richness_score(item)
        funnel_score = _funnel_relevance_score(item, stage_priority_weights=stage_priority_weights)
        mgmt = _management_value_rank(item)
        carry_penalty = 0 if (dt and dt.date() == day) else 1
        tie_id = did
        tier, tier_reason = _daily_candidate_tier(item, transcript_score=transcript_score, evidence_score=evidence_score)
        key = (tier, -transcript_score, -evidence_score, -funnel_score, -mgmt, -freshness, carry_penalty, tie_id)
        ranked.append((key, item, tier_reason))

    ranked.sort(key=lambda x: x[0])
    has_real_conversation_in_pool = False
    for _, item, _ in ranked:
        transcript_score = _transcript_usability_score(item)
        role_signal = _call_role_signal(item)
        if _is_real_conversation_candidate(item, role_signal=role_signal, transcript_score=transcript_score):
            has_real_conversation_in_pool = True
            break
    has_negotiation_candidates_in_pool = any(
        int(_daily_candidate_tier(item, transcript_score=_transcript_usability_score(item), evidence_score=_evidence_richness_score(item))[0]) <= 2
        for _, item, _ in ranked
    )

    prefiltered: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for idx, (_, item, tier_reason) in enumerate(ranked, start=1):
        transcript_label = str(item.get("transcript_usability_label") or "").strip().lower()
        transcript_score = _transcript_usability_score(item)
        evidence_score = _evidence_richness_score(item)
        management_score = _management_value_rank(item)
        funnel_score = _funnel_relevance_score(item, stage_priority_weights=stage_priority_weights)
        role_signal = _call_role_signal(item)
        tier_num, _ = _daily_candidate_tier(
            item,
            transcript_score=transcript_score,
            evidence_score=evidence_score,
        )
        autoanswer_flag = _is_autoanswer_like_candidate(item)
        redial_flag = bool(item.get("repeated_dead_redial_day_flag")) or int(item.get("repeated_dead_redial_count", 0) or 0) > 0
        usable_flag = _is_real_conversation_candidate(item, role_signal=role_signal, transcript_score=transcript_score)
        skip_reason = ""
        if (
            transcript_label in {"weak", "noisy", "empty"}
            and evidence_score < 5
            and role_signal not in {"history_pattern", "secretary", "supplier_inbound", "warm_inbound"}
            and int(item.get("call_history_pattern_score", 0) or 0) < 45
        ):
            skip_reason = "weak_transcript_and_thin_crm"
        if not skip_reason and has_negotiation_candidates_in_pool and tier_num >= 6:
            skip_reason = "crm_only_filler_blocked_due_to_call_cases"
        if not skip_reason and has_negotiation_candidates_in_pool and tier_num in {3, 4, 5}:
            skip_reason = "discipline_deferred_due_to_negotiation_cases"
        if not skip_reason and has_real_conversation_in_pool and autoanswer_flag and tier_num >= 5:
            skip_reason = "autoanswer_low_priority_when_real_conversation_exists"
        enriched = dict(item)
        enriched["_daily_selection_rank"] = idx
        enriched["_daily_selection_reason"] = tier_reason
        enriched["_daily_tier"] = tier_num
        enriched["_call_role_signal"] = role_signal
        enriched["_autoanswer_flag"] = bool(autoanswer_flag)
        enriched["_redial_flag"] = bool(redial_flag)
        enriched["_usable_flag"] = bool(usable_flag)
        enriched["_transcript_usability_score"] = transcript_score
        enriched["_evidence_richness_score"] = evidence_score
        enriched["_funnel_relevance_score"] = funnel_score
        enriched["_management_value_score"] = management_score
        enriched["_stage_priority_weight_value"] = _stage_priority_weight_value_for_item(
            item,
            stage_priority_weights=stage_priority_weights,
        )
        enriched["skip_for_daily_reason"] = skip_reason
        if skip_reason:
            rejected.append(enriched)
            continue
        prefiltered.append(enriched)

    if prefiltered:
        shortlist_limit = min(12, max(8, max(1, int(package_target)) * 2))
        shortlist = prefiltered[:shortlist_limit]
        rerank_map = _llm_daily_rerank_candidates(
            cfg=cfg,
            logger=logger,
            backend_effective=backend_effective,
            manager=manager,
            role=role,
            control_day=control_day,
            candidates=shortlist,
            style_source_excerpt=style_source_excerpt,
            llm_runtime=llm_runtime,
        )
        if rerank_map:
            for candidate in prefiltered:
                did = str(candidate.get("deal_id") or "").strip()
                meta = rerank_map.get(did, {})
                llm_rank = int(meta.get("rank", 0) or 0)
                candidate["llm_daily_rank"] = llm_rank if llm_rank > 0 else ""
                candidate["llm_daily_rank_reason"] = str(meta.get("reason") or "")
                candidate["llm_call_analysis_viability"] = str(meta.get("call_analysis_viability") or "")
                candidate["llm_call_analysis_viability_reason"] = str(meta.get("call_analysis_viability_reason") or "")
                if bool(meta.get("skip")) and not str(candidate.get("skip_for_daily_reason") or "").strip():
                    candidate["skip_for_daily_reason"] = str(meta.get("skip_reason") or "llm_rerank_skip")
            prefiltered = [x for x in prefiltered if not str(x.get("skip_for_daily_reason") or "").strip()]
            prefiltered.sort(
                key=lambda x: (
                    int(x.get("llm_daily_rank", 0) or 0) if int(x.get("llm_daily_rank", 0) or 0) > 0 else 9999,
                    int(x.get("_daily_selection_rank", 9999)),
                )
            )
        else:
            for candidate in prefiltered:
                candidate.setdefault("llm_daily_rank", "")
                candidate.setdefault("llm_daily_rank_reason", "")
                candidate.setdefault("llm_call_analysis_viability", "")
                candidate.setdefault("llm_call_analysis_viability_reason", "")

    selected: list[dict[str, Any]] = []
    tier_selected: dict[int, int] = {}
    seen: set[str] = set()
    for item in prefiltered:
        did = str(item.get("deal_id") or "")
        if did and did in seen:
            continue
        tier_num, _ = _daily_candidate_tier(
            item,
            transcript_score=_transcript_usability_score(item),
            evidence_score=_evidence_richness_score(item),
        )
        if has_negotiation_candidates_in_pool and tier_num >= 3:
            continue
        # Autoanswer/no-answer tiers are only honest fallback when richer modes are unavailable.
        if tier_num >= 5 and any(v > 0 for k, v in tier_selected.items() if k <= 4):
            continue
        if did:
            seen.add(did)
        selected.append(dict(item))
        tier_selected[tier_num] = tier_selected.get(tier_num, 0) + 1
        if len(selected) >= max(1, int(package_target)):
            break
    for item in rejected:
        if len(selected) >= max(1, int(package_target)):
            break
        # Honest fallback: only when we still have nothing and there is at least some meaningful call-history pattern.
        if selected:
            break
        if has_real_conversation_in_pool:
            continue
        tier_num = int(item.get("_daily_tier", 9) or 9)
        if tier_num > 4:
            continue
        if int(item.get("call_history_pattern_score", 0) or 0) < 45 and _call_role_signal(item) not in {"secretary", "history_pattern", "supplier_inbound", "warm_inbound"}:
            continue
        thin = dict(item)
        thin["skip_for_daily_reason"] = str(thin.get("skip_for_daily_reason") or "fallback_fill")
        selected.append(thin)
    if selected and rejected:
        selected[0]["_daily_skipped_candidates"] = [
            {
                "deal_id": str(x.get("deal_id") or ""),
                "skip_for_daily_reason": str(x.get("skip_for_daily_reason") or ""),
                "transcript_usability_label": str(x.get("transcript_usability_label") or ""),
                "transcript_usability_score_final": int(x.get("transcript_usability_score_final", 0) or 0),
                "evidence_richness_score": int(x.get("_evidence_richness_score", 0) or 0),
                "daily_tier": int(x.get("_daily_tier", 0) or 0),
                "call_role_signal": str(x.get("_call_role_signal", "") or ""),
                "autoanswer_flag": bool(x.get("_autoanswer_flag")),
                "redial_flag": bool(x.get("_redial_flag")),
                "usable_flag": bool(x.get("_usable_flag")),
                "llm_daily_rank": x.get("llm_daily_rank", ""),
                "llm_daily_rank_reason": str(x.get("llm_daily_rank_reason") or ""),
            }
            for x in rejected[:20]
            if isinstance(x, dict)
        ]
    return selected


def _select_daily_package_records_relaxed(
    *,
    manager_records: list[dict[str, Any]],
    package_target: int,
    exclude_deal_ids: set[str] | None = None,
    stage_priority_weights: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    excluded = exclude_deal_ids or set()
    ranked: list[tuple[tuple[int, float, int, int, str], dict[str, Any], str]] = []
    for item in manager_records:
        did = str(item.get("deal_id") or "").strip()
        if did and did in excluded:
            continue
        transcript_score = _transcript_usability_score(item)
        evidence = _evidence_richness_score(item)
        funnel = _funnel_relevance_score(item, stage_priority_weights=stage_priority_weights)
        mgmt = _management_value_rank(item)
        score_val = int(item.get("score")) if isinstance(item.get("score"), int) else 50
        tier, tier_reason = _daily_candidate_tier(item, transcript_score=transcript_score, evidence_score=evidence)
        ranked.append(((tier, -funnel, -evidence, -mgmt, did), item, tier_reason))
    ranked.sort(key=lambda x: x[0])
    selected: list[dict[str, Any]] = []
    for idx, (_, item, tier_reason) in enumerate(ranked, start=1):
        enriched = dict(item)
        enriched["_daily_selection_rank"] = idx
        enriched["_daily_selection_reason"] = tier_reason
        enriched["_transcript_usability_score"] = _transcript_usability_score(item)
        enriched["_evidence_richness_score"] = _evidence_richness_score(item)
        enriched["_funnel_relevance_score"] = _funnel_relevance_score(item, stage_priority_weights=stage_priority_weights)
        enriched["_management_value_score"] = _management_value_rank(item)
        enriched["_stage_priority_weight_value"] = _stage_priority_weight_value_for_item(
            item,
            stage_priority_weights=stage_priority_weights,
        )
        enriched["skip_for_daily_reason"] = ""
        enriched["llm_daily_rank"] = ""
        enriched["llm_daily_rank_reason"] = ""
        selected.append(enriched)
        if len(selected) >= max(1, int(package_target)):
            break
    return selected


def _llm_daily_rerank_candidates(
    *,
    cfg: DealAnalyzerConfig | None,
    logger: Any | None,
    backend_effective: str | None,
    manager: str,
    role: str,
    control_day: str,
    candidates: list[dict[str, Any]],
    style_source_excerpt: str,
    llm_runtime: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    if cfg is None:
        return {}
    if (backend_effective or cfg.analyzer_backend) not in {"ollama", "hybrid"}:
        return {}
    if len(candidates) < 2:
        return {}
    if _make_llm_client_from_runtime(llm_runtime or {}) is None:
        return {}

    shortlist = candidates[:12]
    rerank_payload = {
        "manager": manager,
        "role": role,
        "control_day": control_day,
        "candidates": [
            {
                "deal_id": str(item.get("deal_id") or ""),
                "status": str(item.get("status_name") or ""),
                "pipeline": str(item.get("pipeline_name") or ""),
                "transcript_usability_label": str(item.get("transcript_usability_label") or ""),
                "transcript_usability_score": int(item.get("_transcript_usability_score", 0) or 0),
                "evidence_richness_score": int(item.get("_evidence_richness_score", 0) or 0),
                "funnel_relevance_score": float(item.get("_funnel_relevance_score", 0) or 0),
                "management_value_score": int(item.get("_management_value_score", 0) or 0),
                "call_summary": str(item.get("call_signal_summary_short") or "")[:220],
                "transcript_excerpt": str(item.get("transcript_text_excerpt") or "")[:320],
                "manager_summary": str(item.get("manager_summary") or "")[:200],
                "growth_zones": (item.get("growth_zones", []) if isinstance(item.get("growth_zones"), list) else [])[:2],
                "risk_flags": (item.get("risk_flags", []) if isinstance(item.get("risk_flags"), list) else [])[:3],
            }
            for item in shortlist
            if str(item.get("deal_id") or "").strip()
        ],
    }
    if len(rerank_payload["candidates"]) < 2:
        return {}

    messages = build_daily_rerank_messages(
        rerank_payload=rerank_payload,
        config=cfg,
        style_source_excerpt=style_source_excerpt,
    )
    if logger is not None:
        logger.info(
            "daily llm rerank call: manager=%s day=%s candidates_total=%s candidates_for_llm=%s",
            manager,
            control_day,
            len(candidates),
            len(rerank_payload["candidates"]),
        )
    payload_raw, _ = _llm_chat_json_with_runtime(
        runtime=llm_runtime or {},
        messages=messages,
        repair_messages=append_daily_rerank_json_repair_instruction(messages),
        logger=logger,
        log_prefix=f"daily llm rerank manager={manager} day={control_day}",
    )
    if not isinstance(payload_raw, dict):
        return {}
    payload = payload_raw

    ranked_rows = payload.get("ranked") if isinstance(payload, dict) else None
    if not isinstance(ranked_rows, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in ranked_rows:
        if not isinstance(row, dict):
            continue
        did = str(row.get("deal_id") or "").strip()
        if not did:
            continue
        try:
            rank = int(row.get("rank", 0) or 0)
        except Exception:
            rank = 0
        result[did] = {
            "rank": rank,
            "reason": str(row.get("reason") or "").strip(),
            "skip": bool(row.get("skip")),
            "skip_reason": str(row.get("skip_reason") or "").strip(),
            "call_analysis_viability": str(row.get("call_analysis_viability") or "").strip().lower(),
            "call_analysis_viability_reason": str(row.get("call_analysis_viability_reason") or "").strip(),
        }
    return result


def _build_daily_deal_links(*, items: list[dict[str, Any]], base_domain: str) -> str:
    links: list[str] = []
    root = str(base_domain or "").strip().rstrip("/")
    seen: set[str] = set()
    for item in items:
        did = str(item.get("deal_id") or "").strip()
        if not did:
            continue
        if root:
            url = f"{root}/leads/detail/{did}"
        else:
            url = did
        if url in seen:
            continue
        seen.add(url)
        links.append(url)
    return "\n".join(links)


def _freshness_rank_for_day(*, dt: datetime | None, control_day) -> int:
    if not dt:
        return 0
    d = dt.date()
    if d == control_day:
        return 3
    delta = (control_day - d).days
    if delta == 1:
        return 2
    if 2 <= delta <= 7:
        return 1
    return 0


def _evidence_rank(item: dict[str, Any]) -> int:
    rank = 0
    if bool(item.get("transcript_available")):
        rank += 2
    if str(item.get("call_signal_summary_short") or "").strip():
        rank += 2
    if str(item.get("manager_summary") or "").strip():
        rank += 1
    if str(item.get("employee_coaching") or "").strip():
        rank += 1
    return rank


def _management_value_rank(item: dict[str, Any]) -> int:
    flags = item.get("risk_flags") if isinstance(item.get("risk_flags"), list) else []
    rank = len(flags)
    if any(str(flag).startswith("qualified_loss:") for flag in flags):
        rank += 2
    if bool(item.get("owner_ambiguity_flag")):
        rank += 1
    if str(item.get("analysis_confidence") or "").lower() == "low":
        rank += 1
    return rank


def _transcript_usability_score(item: dict[str, Any]) -> int:
    if not isinstance(item, dict):
        return 0
    if _is_autoanswer_like_candidate(item):
        return 0
    score_from_signal = item.get("transcript_usability_score_final")
    try:
        if score_from_signal is not None:
            score_val = int(score_from_signal)
            if score_val > 0:
                if score_val >= 65:
                    return 3
                if score_val >= 35:
                    return 2
                return 1
    except Exception:
        pass
    label = str(item.get("transcript_usability_label") or "").strip().lower()
    if label == "usable":
        if _is_autoanswer_like_candidate(item):
            return 1
        return 3
    if label in {"weak", "noisy"}:
        return 1
    if label == "empty":
        return 0
    excerpt = " ".join(str(item.get("transcript_text_excerpt") or "").split()).strip()
    call_summary = " ".join(str(item.get("call_signal_summary_short") or "").split()).strip()
    available = bool(item.get("transcript_available"))
    if not available and not excerpt and not call_summary:
        return 0
    text = (excerpt + " " + call_summary).lower()
    if _is_autoanswer_like_candidate(item):
        return 0
    noise_markers = ("шум", "неразборчив", "обрыв", "тишин", "пусто", "непонят")
    if any(marker in text for marker in noise_markers):
        return 1
    if len(excerpt) >= 120 or len(call_summary) >= 60:
        return 3
    return 2


def _is_autoanswer_like_candidate(item: dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    text_parts = [
        str(item.get("transcript_text_excerpt") or ""),
        str(item.get("call_signal_summary_short") or ""),
        str(item.get("call_history_pattern_summary") or ""),
        str(item.get("status_name") or ""),
    ]
    text = " ".join(" ".join(x.split()).strip().lower() for x in text_parts if isinstance(x, str))
    autoanswer_markers = (
        "абонент сейчас не может ответить",
        "пожалуйста, дождитесь ответа оператора",
        "записать свое сообщение",
        "оставьте сообщение",
        "оставить сообщение",
        "после звукового сигнала",
        "автоответ",
        "автоответчик",
        "voicemail",
        "no_answer",
        "not available",
        "занят",
        "гудки",
        "перезвонить позже",
    )
    if any(marker in text for marker in autoanswer_markers):
        positive_conversation_markers = (
            "лпр",
            "директор",
            "собственник",
            "встреч",
            "демо",
            "демонстрац",
            "тест",
            "закуп",
            "поставщик",
            "презентац",
        )
        if any(marker in text for marker in positive_conversation_markers):
            return False
        return True
    transcript_label = str(item.get("transcript_usability_label") or "").strip().lower()
    duration = int(item.get("longest_call_duration_seconds", 0) or 0)
    has_any_transcript_text = bool(str(item.get("transcript_text_excerpt") or "").strip())
    has_call_summary = bool(str(item.get("call_signal_summary_short") or "").strip())
    dead_call_markers = ("абонент", "не может ответить", "сигнал", "сообщени", "оператор", "voicemail", "auto")
    if (
        (has_any_transcript_text or has_call_summary)
        and duration <= 30
        and transcript_label in {"weak", "noisy", "empty"}
        and any(marker in text for marker in dead_call_markers)
        and "следующ" not in text
    ):
        return True
    return False


def _is_real_conversation_candidate(item: dict[str, Any], *, role_signal: str, transcript_score: int) -> bool:
    if transcript_score < 2:
        return False
    if _is_autoanswer_like_candidate(item):
        return False
    return role_signal in {"lpr", "secretary", "supplier_inbound", "warm_inbound"}


def _evidence_richness_score(item: dict[str, Any]) -> int:
    score = 0
    if _transcript_usability_score(item) >= 2:
        score += 4
    notes = item.get("notes_summary_raw") if isinstance(item.get("notes_summary_raw"), list) else []
    tasks = item.get("tasks_summary_raw") if isinstance(item.get("tasks_summary_raw"), list) else []
    comments = int(bool(str(item.get("company_comment") or "").strip())) + int(bool(str(item.get("contact_comment") or "").strip()))
    if notes:
        score += min(3, len(notes))
    if tasks:
        score += min(2, len(tasks))
    if comments:
        score += comments
    if isinstance(item.get("tags"), list) and item.get("tags"):
        score += 1
    if str(item.get("status_name") or "").strip():
        score += 1
    return score


def _resolve_stage_priority_weights(*, summary: dict[str, Any]) -> tuple[dict[str, float], str]:
    roks = summary.get("roks_stage_priority_weights")
    if isinstance(roks, dict) and roks:
        out: dict[str, float] = {}
        for key, value in roks.items():
            try:
                out[str(key).strip().lower()] = max(0.1, float(value))
            except Exception:
                continue
        if out:
            return out, "roks"
    return {}, "neutral_fallback"


def _funnel_relevance_score(item: dict[str, Any], *, stage_priority_weights: dict[str, float] | None = None) -> float:
    base = 1.0
    status = " ".join(str(item.get("status_name") or "").split()).strip().lower()
    pipeline = " ".join(str(item.get("pipeline_name") or "").split()).strip().lower()
    hints = [
        ("лпр", 1.2),
        ("встреч", 1.2),
        ("демонстрац", 1.1),
        ("бриф", 1.1),
        ("тест", 1.1),
        ("закрыто", 0.95),
    ]
    for token, coeff in hints:
        if token in status or token in pipeline:
            base *= coeff
    weights = stage_priority_weights or {}
    for key, weight in weights.items():
        if key and (key in status or key in pipeline):
            base *= max(0.1, float(weight))
    return round(base, 3)


def _stage_priority_weight_value_for_item(
    item: dict[str, Any],
    *,
    stage_priority_weights: dict[str, float] | None = None,
) -> float:
    weights = stage_priority_weights or {}
    if not weights:
        return 1.0
    status = " ".join(str(item.get("status_name") or "").split()).strip().lower()
    pipeline = " ".join(str(item.get("pipeline_name") or "").split()).strip().lower()
    matched: list[float] = []
    for key, weight in weights.items():
        if key and (key in status or key in pipeline):
            try:
                matched.append(max(0.1, float(weight)))
            except Exception:
                continue
    return round(max(matched) if matched else 1.0, 3)


def _daily_candidate_tier(item: dict[str, Any], *, transcript_score: int, evidence_score: int) -> tuple[int, str]:
    role_signal = _call_role_signal(item)
    autoanswer_like = _is_autoanswer_like_candidate(item)
    has_call = (
        bool(item.get("transcript_available"))
        or bool(str(item.get("call_signal_summary_short") or "").strip())
        or int(item.get("call_candidates_count") or 0) > 0
    )
    if role_signal == "lpr" and has_call and transcript_score >= 2 and evidence_score >= 4 and not autoanswer_like:
        return 0, "has_call_priority"
    if role_signal == "secretary" and has_call and transcript_score >= 1 and evidence_score >= 3 and not autoanswer_like:
        return 1, "secretary_fallback_priority"
    if role_signal in {"supplier_inbound", "warm_inbound"} and has_call and transcript_score >= 2 and evidence_score >= 3 and not autoanswer_like:
        return 2, "warm_supplier_priority"
    if role_signal == "history_pattern" and int(item.get("call_history_pattern_score", 0) or 0) >= 45:
        return 3, "call_history_pattern_priority"
    if bool(item.get("repeated_dead_redial_day_flag")) or int(item.get("repeated_dead_redial_count", 0) or 0) > 0:
        return 4, "dial_discipline_priority"
    if has_call and transcript_score >= 2 and evidence_score >= 5 and not autoanswer_like:
        return 2, "has_call_priority"
    if autoanswer_like:
        return 5, "autoanswer_low_priority"
    if evidence_score >= 5:
        return 6, "rich_context_priority"
    return 7, "fallback_fill"


def _call_role_signal(item: dict[str, Any]) -> str:
    text = " ".join(
        str(item.get(k) or "").strip().lower()
        for k in ("call_signal_summary_short", "transcript_text_excerpt", "manager_summary")
    )
    if _is_autoanswer_like_candidate(item):
        return "autoanswer"
    if any(token in text for token in ("лпр", "директор", "собственник", "руководител", "лицо принима")):
        return "lpr"
    if any(
        token in text
        for token in (
            "секретар",
            "ресепш",
            "переадрес",
            "соедините",
            "на почту",
            "почту направ",
            "как связаться",
            "снабжением связаться",
            "отдела снабжения нет",
        )
    ):
        return "secretary"
    if any(token in text for token in ("поставщик", "закуп", "тендер", "кп", "supplier", "etp", "регистрац", "площадк")):
        return "supplier_inbound"
    if any(token in text for token in ("демо", "демонстрац", "бриф", "тест", "встреч", "презентац", "оплат", "счет")):
        return "warm_inbound"
    if bool(item.get("call_history_pattern_dead_redials")) or int(item.get("call_history_pattern_score", 0) or 0) >= 45:
        return "history_pattern"
    if bool(item.get("repeated_dead_redial_day_flag")):
        return "history_pattern"
    return "none"


def _filter_growth_for_role(
    *,
    role: str,
    growth: list[str],
    top_risks: list[str],
    items: list[dict[str, Any]],
) -> tuple[list[str], str]:
    warm_markers = (
        "презентац",
        "демо",
        "бриф",
        "тест",
        "счет",
        "оплат",
    )
    role_norm = str(role or "").lower()
    if "телемаркетолог" not in role_norm:
        base = growth or top_risks
        return ([_humanize_work_text(x) for x in base[:3]] or ["Дожать следующий шаг и не терять ритм по активным сделкам."], "")

    explicit_warm_evidence = any(
        bool(item.get("call_signal_demo_discussed"))
        or bool(item.get("call_signal_test_discussed"))
        or any(marker in str(item.get("status_or_stage") or "").lower() for marker in warm_markers)
        for item in items
    )
    raw = growth or top_risks
    if explicit_warm_evidence:
        return ([_humanize_work_text(x) for x in raw[:3]] or ["Дожать следующий шаг и закрыть зависание по активным кейсам."], "")

    filtered = []
    for text in raw:
        low = str(text or "").lower()
        if any(marker in low for marker in warm_markers):
            continue
        filtered.append(_humanize_work_text(text))
    if filtered:
        return filtered[:3], ""
    return ["Подтянуть квалификацию и дожать назначение встречи.", "Проверить, что после звонка фиксируется конкретный следующий шаг."], "warm_signals_out_of_scope"


def _build_daily_why_important(*, role: str, items: list[dict[str, Any]], role_note: str) -> str:
    if role_note == "warm_signals_out_of_scope":
        return "Если это не разделить по ролям, сотрудник теряет фокус и звонки идут вхолостую."
    low_conf = sum(1 for x in items if _is_low_confidence_record(x))
    if low_conf > 0:
        return "Сотруднику проще вести сделки, когда в CRM не каша. Отделу это дает ровную воронку без провалов."
    if "телемаркетолог" in str(role).lower():
        return "Сотруднику легче дожимать контакт, когда после звонка сразу понятен следующий шаг. Для отдела это плюс к конверсии в встречу."
    return "Сотруднику проще держать темп, когда каждая встреча закрыта по результату. Для отдела это меньше зависаний на теплом этапе."


def _build_daily_effect_forecast(
    *,
    items: list[dict[str, Any]],
    role: str,
    avg_score: int | None,
    criticality: str,
) -> dict[str, Any]:
    stage_focus = _detect_daily_problem_stage(items=items, role=role)
    roks = _extract_roks_conversion_rates(items=items)
    base_delta = _estimate_base_absolute_delta(role=role, avg_score=avg_score, stage_focus=stage_focus)
    downstream_stages = _effect_downstream_stages(stage_focus=stage_focus)
    conversions = roks if roks else _fallback_conversion_rates(stage_focus=stage_focus)
    if roks:
        quantity = _compose_quantity_from_cascade(stage_focus=stage_focus, base_delta=base_delta, conversions=conversions)
        quality = _compose_quality_from_cascade(
            stage_focus=stage_focus,
            criticality=criticality,
            role=role,
            with_roks=True,
        )
        return {
            "source": "roks",
            "stage_focus": stage_focus,
            "base_delta": base_delta,
            "quantity_text": quantity,
            "quality_text": quality,
            "conversions_used": conversions,
            "downstream_stages": downstream_stages,
        }

    fallback_qty = _compose_quantity_from_cascade(stage_focus=stage_focus, base_delta=base_delta, conversions=conversions)
    fallback_quality = _compose_quality_from_cascade(
        stage_focus=stage_focus,
        criticality=criticality,
        role=role,
        with_roks=False,
    )
    return {
        "source": "fallback",
        "stage_focus": stage_focus,
            "base_delta": base_delta,
            "quantity_text": fallback_qty,
            "quality_text": fallback_quality or _expected_quality_text(criticality=criticality, role=role),
            "conversions_used": conversions,
            "downstream_stages": downstream_stages,
        }


def _effect_downstream_stages(*, stage_focus: str) -> list[str]:
    if stage_focus in {"lpr", "qualification"}:
        return ["meeting", "meeting_to_next"]
    if stage_focus in {"meeting", "meeting_to_next"}:
        return ["next_step", "progress"]
    if stage_focus == "next_step":
        return ["progress"]
    return ["progress"]


def _fallback_conversion_rates(*, stage_focus: str) -> dict[str, float]:
    if stage_focus in {"lpr", "qualification"}:
        return {"lpr_to_meeting": 0.32, "meeting_to_next": 0.36}
    if stage_focus in {"meeting", "meeting_to_next"}:
        return {"meeting_to_next": 0.38, "next_to_progress": 0.48}
    return {"next_to_progress": 0.5}


def _extract_roks_conversion_rates(*, items: list[dict[str, Any]]) -> dict[str, float]:
    for item in items:
        rates = item.get("roks_conversion_rates")
        if not isinstance(rates, dict):
            continue
        out: dict[str, float] = {}
        for key, value in rates.items():
            try:
                out[str(key)] = max(0.0, min(1.0, float(value)))
            except Exception:
                continue
        if out:
            return out
    return {}


def _detect_daily_problem_stage(*, items: list[dict[str, Any]], role: str) -> str:
    role_norm = str(role or "").lower()
    if "телемаркетолог" in role_norm:
        if any("лпр" in str(x).lower() for i in items for x in (i.get("growth_zones") or [])):
            return "lpr"
        if any("встреч" in str(x).lower() for i in items for x in (i.get("growth_zones") or [])):
            return "meeting"
        return "qualification"
    if any("следующ" in str(x).lower() for i in items for x in (i.get("growth_zones") or [])):
        return "next_step"
    if any("демо" in str(x).lower() or "презентац" in str(x).lower() for i in items for x in (i.get("growth_zones") or [])):
        return "meeting_to_next"
    return "meeting_to_next"


def _estimate_base_absolute_delta(*, role: str, avg_score: int | None, stage_focus: str) -> int:
    role_norm = str(role or "").lower()
    if avg_score is None:
        return 1
    if "телемаркетолог" in role_norm:
        if avg_score < 40:
            return 2
        if avg_score < 70:
            return 1
        return 1
    if avg_score < 40:
        return 1
    if stage_focus == "next_step":
        return 2 if avg_score < 70 else 1
    return 1


def _compose_quantity_from_cascade(*, stage_focus: str, base_delta: int, conversions: dict[str, float]) -> str:
    base = max(0.1, float(base_delta))
    downstream = 0.1
    if stage_focus in {"lpr", "qualification"}:
        lpr_to_meeting = conversions.get("lpr_to_meeting", conversions.get("lpr_meeting", 0.35))
        downstream = max(0.1, round(base * lpr_to_meeting, 1))
        return f"+{base:.1f} качественных ЛПР в неделю; каскадом это около +{downstream:.1f} встреч в работу."
    if stage_focus in {"meeting", "meeting_to_next"}:
        meeting_to_next = conversions.get("meeting_to_next", conversions.get("meeting_next", 0.4))
        downstream = max(0.1, round(base * meeting_to_next, 1))
        return f"+{base:.1f} подтвержденных встреч в неделю; из них примерно +{downstream:.1f} перейдут в следующий шаг."
    if stage_focus == "next_step":
        next_to_progress = conversions.get("next_to_progress", conversions.get("next_progress", 0.5))
        downstream = max(0.1, round(base * next_to_progress, 1))
        return f"На {base:.1f} сделки в неделю меньше зависаний; это даст около +{downstream:.1f} сделок с понятным движением вниз."
    return f"+{base:.1f} дополнительного рабочего шага в неделю."


def _compose_quality_from_cascade(*, stage_focus: str, criticality: str, role: str, with_roks: bool) -> str:
    role_norm = str(role or "").lower()
    source_hint = "по текущим метрикам" if with_roks else "пока по консервативной оценке"
    if "телемаркетолог" in role_norm:
        if stage_focus in {"lpr", "qualification"}:
            return (
                f"Качество квалификации станет чище ({source_hint}): сильнее отсеем слабых ЛПР. "
                "Краткосрочно ЛПР→встреча может просесть, но встречи станут более рабочими."
            )
        return (
            f"Фиксация после звонка станет ровнее ({source_hint}); меньше пустых переходов и меньше шумных переводов вниз."
        )
    if stage_focus in {"meeting", "meeting_to_next", "next_step"}:
        return (
            f"Станет стабильнее связка встреча→следующий шаг ({source_hint}): меньше потерянных договоренностей и яснее управляемость после встречи."
        )
    if criticality == "высокая":
        return f"Сначала выровняем базовую дисциплину этапа ({source_hint}), затем закрепим эффект на соседних этапах."
    return f"Качество этапа будет расти постепенно ({source_hint}) без завышенных ожиданий."


def _expected_quantity_text(*, avg_score: int | None, deals: int, role: str) -> str:
    if deals <= 0:
        return ""
    role_norm = str(role or "").lower()
    if "телемаркетолог" in role_norm:
        if avg_score is None:
            return "1 дополнительный следующий шаг в работе за неделю."
        if avg_score < 40:
            return "+1-2 качественных ЛПР за неделю."
        if avg_score < 70:
            return "+1 встреча в неделю."
        return "1-2 сделки меньше будут зависать в работе."
    if avg_score is None:
        return "1 дополнительный следующий шаг в работе за неделю."
    if avg_score < 40:
        return "-1 потеря в неделю за счет раннего дожима."
    if avg_score < 70:
        return "+1 подтвержденная встреча в неделю."
    return "1-2 сделки меньше будут зависать в работе."


def _expected_quality_text(*, criticality: str, role: str) -> str:
    role_norm = str(role or "").lower()
    if criticality == "высокая":
        if "телемаркетолог" in role_norm:
            return "Квалификация станет чище: меньше шумных ЛПР, боль и бизнес-задача будут фиксироваться внятнее."
        return "После встречи будет понятнее, что делать дальше: меньше потерянных договоренностей, стабильнее перевод в следующий шаг."
    if criticality == "средняя":
        if "телемаркетолог" in role_norm:
            return "Фиксация разговора станет аккуратнее: меньше пустых карточек, легче отделить реальный интерес от шума."
        return "Качество фиксации после встречи станет ровнее; перевод в следующий этап может вырасти, но это пока рабочая гипотеза."
    return "Сохраним рабочий ритм и ровное качество ведения сделок."


def _weekday_ru_from_iso(value: str) -> str:
    names = {
        0: "Понедельник",
        1: "Вторник",
        2: "Среда",
        3: "Четверг",
        4: "Пятница",
        5: "Суббота",
        6: "Воскресенье",
    }
    try:
        dt = datetime.fromisoformat(str(value)).date()
    except Exception:
        return ""
    return names.get(dt.weekday(), "")


def _score_to_criticality(avg_score: int | None, *, risk_count: int) -> str:
    if avg_score is None:
        return ""
    if avg_score < 35 or risk_count >= 8:
        return "высокая"
    if avg_score < 65 or risk_count >= 4:
        return "средняя"
    return "низкая"


def _humanize_work_text(value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""
    replacements = {
        "нужен ручной контроль": "перепроверить на свежую голову",
        "требуется дополнительная верификация": "переслушать звонок и сверить с CRM",
        "необходимо провести follow-up": "дожать следующий шаг",
        "follow-up": "следующий шаг",
        "требуется дальнейшая проработка": "нормально зафиксировать причину потери и решить, как двигаться дальше",
        "это влияет на предсказуемость недели": "если это не дожать, воронка дальше опять поедет вслепую",
        "qualified loss": "не наш кейс",
        "anti-fit": "не наш кейс",
        "market mismatch": "не тот сценарий",
        "owner attribution": "кто по факту вел сделку",
        "owner ambiguity": "неясно, кто по факту вел сделку",
        "owner": "ответственный",
        "closeout cleanup": "нормально закрыть причину отказа и дочистить CRM",
        "closeout-cleanup": "нормально закрыть причину отказа и дочистить CRM",
        "closeout": "закрытие причины отказа",
        "pressure": "лишнее давление",
        "notes": "комментарии",
        "атрибуция owner": "кто по факту вел сделку",
        "проверить фактуру в crm": "переслушать звонок и сверить с CRM",
        "factual layer": "фактура",
        "fact pattern": "картина по сделке",
        "process hygiene": "дисциплина процесса",
        "evidence context": "контекст разговора",
        "ограниченная надежность выводов": "выводы пока предварительные",
        "ограниченная надежность": "предварительно",
        "подтвердить фактического ведущего": "сверить, кто по факту вел сделку",
        "контекст последних касаний": "что было в последних касаниях",
        "необходимо": "нужно",
        "требуется": "нужно",
    }
    lowered = text.lower()
    for old, new in replacements.items():
        if old in lowered:
            text = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
            lowered = text.lower()
    return text


def _daily_user_text(value: str) -> str:
    text = _humanize_work_text(value)
    rewrite = {
        "кейс не про дожим": "здесь не давим, сначала нормально разбираем причину",
        "приоритет — корректная фиксация причины потери и нормально закрыть причину отказа и дочистить crm": "приоритет — нормально закрыть причину отказа и дочистить CRM",
        "интерпретация ограничена качеством crm-данных": "здесь выводы ограничены, сначала перепроверить фактуру в CRM",
        "без pressure следующего шага": "без лишнего давления по сделке",
        "без лишнее давление следующий шаг": "без лишнего дожима",
        "закрытая потеря кейс": "закрытая потеря",
        "корректная закрытие причины отказа-классификация": "нормально зафиксировать причину потери",
        "closed-lost": "закрытая потеря",
        "process_hygiene:": "дисциплина процесса:",
        "disabled": "выключено",
        "anti-pattern": "повторяющийся сбой",
        "атрибуцию ответственный": "кто по факту вел сделку",
        "проверить фактуру в crm": "переслушать звонок и сверить с CRM",
        "дисциплина процесса:": "",
        "контекст разговора:": "",
    }
    lowered = text.lower()
    for old, new in rewrite.items():
        if old in lowered:
            text = re.sub(re.escape(old), new, text, flags=re.IGNORECASE)
            lowered = text.lower()
    text = re.sub(r"закрытая потеря кейс", "закрытая потеря", text, flags=re.IGNORECASE)
    text = re.sub(r"корректн\w*\s+закрыти\w+\s+причин\w+\s+отказ\w+-классификаци\w+", "нормально зафиксировать причину потери", text, flags=re.IGNORECASE)
    text = re.sub(r"нужна\s+нормально\s+зафиксировать", "нужно нормально зафиксировать", text, flags=re.IGNORECASE)
    text = re.sub(r"перепереслушать", "переслушать", text, flags=re.IGNORECASE)
    return text


def _resolve_daily_manager_allowlist(values: list[str] | tuple[str, ...] | None) -> list[str]:
    values = list(values or ["Илья", "Рустам"])
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        name = _normalize_manager_for_dropdown(str(raw or "").strip())
        if not name:
            continue
        low = name.lower()
        if low in seen:
            continue
        seen.add(low)
        normalized.append(name)
    return normalized or ["Илья", "Рустам"]


def _daily_selection_reason(items: list[dict[str, Any]]) -> str:
    reasons = [str(x.get("_daily_selection_reason") or "").strip() for x in items if isinstance(x, dict)]
    if reasons:
        if "has_call_priority" in reasons:
            return "has_call_priority"
        if "rich_context_priority" in reasons:
            return "rich_context_priority"
        return reasons[0]
    if any(bool(x.get("transcript_available")) or str(x.get("call_signal_summary_short") or "").strip() for x in items):
        return "has_call_priority"
    if any(
        isinstance(x.get("notes_summary_raw"), list) and x.get("notes_summary_raw")
        or isinstance(x.get("tasks_summary_raw"), list) and x.get("tasks_summary_raw")
        or str(x.get("manager_summary") or "").strip()
        for x in items
    ):
        return "rich_context_priority"
    return "fallback_fill"


def _derive_daily_primary_source(items: list[dict[str, Any]]) -> str:
    tiers = [
        int(x.get("_daily_tier", 9) or 9)
        for x in items
        if isinstance(x, dict)
    ]
    if any(t <= 2 for t in tiers):
        return "conversation_pool"
    if any(t in {3, 4, 5} for t in tiers):
        return "discipline_pool"
    return "conversation_pool"


def _derive_daily_case_type(items: list[dict[str, Any]], *, primary_source: str) -> str:
    signals = [
        str(x.get("_call_role_signal") or _call_role_signal(x) or "").strip()
        for x in items
        if isinstance(x, dict)
    ]
    if primary_source == "discipline_pool":
        if any(s in {"history_pattern", "autoanswer"} for s in signals):
            return "redial_discipline"
        return "discipline_case"
    if any(s == "lpr" for s in signals):
        return "lpr_conversation"
    if any(s == "secretary" for s in signals):
        return "secretary_case"
    if any(s == "supplier_inbound" for s in signals):
        return "supplier_case"
    if any(s == "warm_inbound" for s in signals):
        return "warm_case"
    return "conversation_case"


def _daily_selection_reason_v2(
    *,
    primary_source: str,
    case_type: str,
    base_reason: str,
    excluded_crm_only_cases_count: int,
) -> str:
    return (
        f"{primary_source}:{case_type}:{base_reason}:excluded_crm_only={int(excluded_crm_only_cases_count or 0)}"
    )


def _daily_growth_compact(growth: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for g in growth:
        text = _daily_user_text(g)
        key = text.lower().strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= 2:
            break
    return out


def _build_daily_key_takeaway(
    *,
    manager: str,
    role: str,
    items: list[dict[str, Any]],
    manager_msgs: str,
    growth: list[str],
) -> str:
    with_call = any(bool(x.get("transcript_available")) or str(x.get("call_signal_summary_short") or "").strip() for x in items)
    low_conf = sum(1 for x in items if _is_low_confidence_record(x))
    if with_call:
        core = manager_msgs or "По звонкам видно живой материал: есть за что зацепиться и что дожать."
    else:
        core = manager_msgs or "По CRM видно только часть картины, поэтому выводы держим рабочими и без перегибов."
    if low_conf > 0:
        tail = "По части сделок выводы предварительные, работаем только от зафиксированных фактов."
    elif growth:
        tail = f"Главный рычаг на ближайший цикл: {growth[0]}."
    else:
        tail = "Держим ритм и не отпускаем следующий шаг после каждого касания."
    return f"{core} {tail}"


def _build_daily_strong_sides(*, items: list[dict[str, Any]], strong: list[str]) -> str:
    with_call = any(bool(x.get("transcript_available")) for x in items)
    call_signal = any(str(x.get("call_signal_summary_short") or "").strip() for x in items)
    if with_call or call_signal:
        return "По разговору есть рабочие моменты: держит контакт и доводит до понятного следующего шага."
    if strong:
        return "; ".join(str(x) for x in strong[:2])
    has_crm_fixation = any(str(x.get("manager_summary") or "").strip() for x in items)
    if has_crm_fixation:
        return "Хорошо фиксирует суть сделки и не теряет ход в CRM."
    return ""


def _build_daily_reinforce(*, items: list[dict[str, Any]], role: str, strong: list[str]) -> str:
    role_norm = str(role or "").lower()
    if strong:
        return f"Закрепить: {strong[0]}"
    if "телемаркетолог" in role_norm:
        return "Закрепить модуль выхода на ЛПР и короткую фиксацию следующего шага сразу после звонка."
    return "Закрепить модуль закрытия встречи: результат, следующий шаг и срок сразу в CRM."


def _build_daily_fix_action(*, items: list[dict[str, Any]], role: str, growth: list[str]) -> str:
    if growth:
        main = growth[0]
        return f"Починить в работе: {main.lower()}"
    role_norm = str(role or "").lower()
    if "телемаркетолог" in role_norm:
        return "Дожать назначение и сразу фиксировать понятный следующий шаг."
    return "Закрывать итог встречи и следующий шаг в тот же день."


def _build_daily_coaching_list(
    *,
    items: list[dict[str, Any]],
    role: str,
    growth: list[str],
    employee_msgs: str,
) -> str:
    role_norm = str(role or "").lower()
    line1 = "разобрали 2-3 кейса дня и где именно утекает шаг."
    if "телемаркетолог" in role_norm:
        line2 = "дали модуль: выход на ЛПР + назначение встречи без провисания."
    else:
        line2 = "дали модуль: итог встречи + четкий следующий шаг с датой."
    if growth:
        line3 = f"в следующих звонках пробует: {growth[0].lower()}."
    elif employee_msgs:
        line3 = employee_msgs.strip().rstrip(".") + "."
    else:
        line3 = "если не выровняется за цикл, выносим в полноценное обучение."
    return "1) " + line1 + "\n2) " + line2 + "\n3) " + line3


def _normalize_manager_for_dropdown(value: str) -> str:
    text = _repair_common_mojibake(" ".join(str(value or "").split()).strip())
    low = text.lower()
    if "илья" in low:
        return "Илья"
    if "рустам" in low:
        return "Рустам"
    return text


def _repair_common_mojibake(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return raw
    # Handles common UTF-8-as-CP1251 mojibake fragments like "РР»СЊСЏ".
    if "Р" not in raw and "С" not in raw:
        return raw
    try:
        repaired = raw.encode("latin1", errors="strict").decode("utf-8", errors="strict")
        repaired = " ".join(repaired.split()).strip()
        return repaired or raw
    except Exception:
        return raw


def _manager_sort_key(manager_name: str, *, allowlist: list[str] | None = None) -> tuple[int, str]:
    text = str(manager_name or "").strip()
    low = text.lower()
    if allowlist:
        for idx, name in enumerate(allowlist):
            if low == str(name).strip().lower():
                return (idx, text)
    if "илья" in low:
        return (0, text)
    if "рустам" in low:
        return (1, text)
    return (2, text)


def _daily_weighted_score(items: list[dict[str, Any]]) -> int | None:
    if not items:
        return None
    scores: list[float] = []
    for item in items:
        negotiation = 40.0
        if bool(item.get("transcript_available")):
            negotiation += 20.0
        if str(item.get("call_signal_summary_short") or "").strip():
            negotiation += 15.0
        if bool(item.get("call_signal_next_step_present")):
            negotiation += 10.0
        if bool(item.get("call_signal_decision_maker_reached")):
            negotiation += 5.0
        if bool(item.get("call_signal_objection_not_target")):
            negotiation -= 10.0
        negotiation = max(0.0, min(100.0, negotiation))

        crm = 30.0
        confidence = str(item.get("analysis_confidence") or "").strip().lower()
        if confidence == "high":
            crm += 35.0
        elif confidence == "medium":
            crm += 20.0
        else:
            crm += 8.0
        if isinstance(item.get("risk_flags"), list) and item.get("risk_flags"):
            crm += 7.0
        crm = max(0.0, min(100.0, crm))

        total = (negotiation * 0.8) + (crm * 0.2)
        scores.append(total)
    return int(round(sum(scores) / len(scores)))


def _build_meeting_queue_sheets_dry_run_payload(*, queue_items: list[dict[str, Any]]) -> dict[str, Any]:
    columns = [
        "deal_id",
        "deal_name",
        "owner_name",
        "status_or_stage",
        "score_0_100",
        "analysis_confidence",
        "why_in_queue_human",
        "call_signal_summary_short",
        "reanimation_potential",
        "reanimation_next_step",
        "artifact_path",
    ]
    rows: list[dict[str, Any]] = []
    for item in queue_items:
        if not isinstance(item, dict):
            continue
        row = {col: item.get(col, "") for col in columns}
        row["why_in_queue_human"] = item.get("why_in_queue_human", "") or _queue_reason_human(str(item.get("why_in_queue") or ""))
        rows.append(row)
    return {
        "mode": "dry_run",
        "writer_scope": "deal_analyzer_only",
        "target_hint": "meeting_queue_call_aware_review",
        "sheet_name": "",
        "start_cell": "",
        "columns": columns,
        "rows": rows,
        "rows_count": len(rows),
        "note": "Fill sheet_name/start_cell to enable real table write in next step.",
    }


def _maybe_write_call_review_sheet(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    call_review_payload: dict[str, Any],
) -> dict[str, Any]:
    rows = call_review_payload.get("rows", []) if isinstance(call_review_payload.get("rows"), list) else []
    rows_dict = [x for x in rows if isinstance(x, dict)]
    payload_sheet_name = str(call_review_payload.get("sheet_name") or "").strip() or "Разбор звонков"
    payload_start_cell = str(call_review_payload.get("start_cell") or "").strip() or "A2"

    sheet_name = str(getattr(cfg, "deal_analyzer_call_review_sheet_name", "") or "").strip()
    if not sheet_name:
        sheet_name = payload_sheet_name

    start_cell = str(getattr(cfg, "deal_analyzer_call_review_start_cell", "") or "").strip()
    if not start_cell:
        start_cell = payload_start_cell

    columns = (
        call_review_payload.get("columns", [])
        if isinstance(call_review_payload.get("columns"), list)
        else list(CALL_REVIEW_DEFAULT_COLUMNS)
    )
    values_rows = rows_to_sheet_matrix(rows=rows_dict, columns=columns)

    if bool(getattr(cfg, "deal_analyzer_write_enabled", False)):
        try:
            spreadsheet_id = _resolve_spreadsheet_id_from_config(cfg=cfg)
            if spreadsheet_id and sheet_name and start_cell:
                app_cfg = load_config()
                client = GoogleSheetsApiClient(project_root=app_cfg.project_root, logger=logger)
                def _load_sheet_contract(target_sheet_name: str) -> tuple[list[str], dict[int, list[str]]]:
                    header_columns_local = _read_sheet_header_columns(
                        client=client,
                        spreadsheet_id=spreadsheet_id,
                        sheet_name=target_sheet_name,
                        start_cell=start_cell,
                    )
                    dropdown_values_local = _read_sheet_dropdown_values_for_data_row(
                        client=client,
                        spreadsheet_id=spreadsheet_id,
                        sheet_name=target_sheet_name,
                        start_cell=start_cell,
                        columns=header_columns_local,
                    )
                    return header_columns_local, dropdown_values_local

                try:
                    header_columns, dropdown_values_by_col = _load_sheet_contract(sheet_name)
                except Exception:
                    # Fallback for broken config tab names: use payload default tab.
                    if sheet_name != payload_sheet_name:
                        logger.warning(
                            "call review writer tab fallback: from=%s to=%s",
                            sheet_name,
                            payload_sheet_name,
                        )
                        sheet_name = payload_sheet_name
                        header_columns, dropdown_values_by_col = _load_sheet_contract(sheet_name)
                    else:
                        raise

                if not header_columns:
                    logger.warning(
                        "call review writer blocked: reason=header_read_empty sheet_name=%s start_cell=%s",
                        sheet_name,
                        start_cell,
                    )
                    return {
                        "enabled": True,
                        "mode": "dry_run",
                        "write_mode": "append",
                        "sheet_name": sheet_name,
                        "start_cell": start_cell,
                        "rows_prepared": len(rows_dict),
                        "rows_written": 0,
                        "write_start_row": 0,
                        "write_end_row": 0,
                        "error": "header_read_empty",
                    }
                columns = header_columns
                values_rows = rows_to_sheet_matrix(rows=rows_dict, columns=columns)
                values_rows = _normalize_rows_by_dropdown_values(
                    rows=values_rows,
                    columns=columns,
                    dropdown_values_by_col=dropdown_values_by_col,
                )
        except Exception as exc:
            logger.warning("call review writer blocked: reason=header_fetch_failed error=%s", exc)
            return {
                "enabled": True,
                "mode": "dry_run",
                "write_mode": "append",
                "sheet_name": sheet_name,
                "start_cell": start_cell,
                "rows_prepared": len(rows_dict),
                "rows_written": 0,
                "write_start_row": 0,
                "write_end_row": 0,
                "error": f"header_fetch_failed:{exc}",
            }

    return _maybe_write_rows_to_sheet(
        cfg=cfg,
        logger=logger,
        values_rows=values_rows,
        sheet_name=sheet_name,
        start_cell=start_cell,
        writer_tag="call_review_writer",
        append_mode=not bool(getattr(cfg, "deal_analyzer_overwrite_mode", False)),
    )


def _maybe_write_daily_control_sheet(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    daily_payload: dict[str, Any],
) -> dict[str, Any]:
    rows = daily_payload.get("rows", []) if isinstance(daily_payload.get("rows"), list) else []
    payload_columns = daily_payload.get("columns", []) if isinstance(daily_payload.get("columns"), list) else []
    rows_dict = [x for x in rows if isinstance(x, dict)]
    llm_ready_rows = [row for row in rows_dict if bool(row.get("llm_text_ready"))]
    case_rows = _expand_daily_rows_to_case_rows(
        rows=llm_ready_rows,
        base_domain=_resolve_amo_base_domain_for_links(cfg=cfg),
    )

    sheet_name = str(getattr(cfg, "deal_analyzer_daily_sheet_name", "") or "").strip()
    if not sheet_name:
        sheet_name = str(getattr(cfg, "deal_analyzer_sheet_name", "") or "").strip()
    if not sheet_name:
        sheet_name = str(daily_payload.get("sheet_name") or "").strip() or "Разбор звонков"
    if sheet_name.strip().lower() == "дневной контроль":
        sheet_name = "Разбор звонков"

    start_cell = str(getattr(cfg, "deal_analyzer_daily_start_cell", "") or "").strip()
    if not start_cell:
        start_cell = str(getattr(cfg, "deal_analyzer_start_cell", "") or "").strip()
    if not start_cell:
        start_cell = str(daily_payload.get("start_cell") or "").strip() or "A2"

    columns = payload_columns
    values_rows = [[row.get(col, "") for col in columns] for row in case_rows]

    if bool(getattr(cfg, "deal_analyzer_write_enabled", False)):
        try:
            spreadsheet_id = _resolve_spreadsheet_id_from_config(cfg=cfg)
            if spreadsheet_id and sheet_name and start_cell:
                app_cfg = load_config()
                client = GoogleSheetsApiClient(project_root=app_cfg.project_root, logger=logger)
                header_columns = _read_sheet_header_columns(
                    client=client,
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name,
                    start_cell=start_cell,
                )
                if header_columns:
                    columns = header_columns
                    adapted_rows = _adapt_case_rows_to_sheet_columns(rows=case_rows, columns=columns)
                    values_rows = [[row.get(col, "") for col in columns] for row in adapted_rows]
        except Exception as exc:
            logger.warning("daily call review header read failed; fallback to payload columns: error=%s", exc)

    return _maybe_write_rows_to_sheet(
        cfg=cfg,
        logger=logger,
        values_rows=values_rows,
        sheet_name=sheet_name,
        start_cell=start_cell,
        writer_tag="daily_call_review_writer",
        append_mode=not bool(getattr(cfg, "deal_analyzer_overwrite_mode", False)),
    )


def _candidate_is_meaningful_for_call_review(candidate: dict[str, Any]) -> bool:
    if not isinstance(candidate, dict):
        return False
    skip_reason = str(candidate.get("skip_for_daily_reason") or "").strip().lower()
    if skip_reason in {"weak_transcript_and_thin_crm", "crm_only_filler_blocked_due_to_call_cases", "noise_short_or_autoanswer"}:
        return False
    daily_tier = str(candidate.get("daily_tier") or "").strip().lower()
    if daily_tier in {"priority_6_noise", "priority_7_forced_fallback"}:
        return False
    usable_label = str(candidate.get("transcript_usability_label") or "").strip().lower()
    usable_score = int(candidate.get("transcript_usability_score_final", 0) or 0)
    has_usable = usable_label == "usable" or usable_score >= 2 or bool(candidate.get("usable_flag"))
    has_discipline = bool(candidate.get("redial_flag")) or bool(candidate.get("same_time_redial_pattern_flag")) or bool(candidate.get("numbers_not_fully_covered_flag"))
    call_role_signal = str(candidate.get("call_role_signal") or "").strip().lower()
    has_secretary_or_lpr = call_role_signal in {"secretary", "lpr", "supplier"}
    if has_usable or has_discipline or has_secretary_or_lpr:
        return True
    # Conservative fallback: if candidate was selected and is not explicitly noisy, allow case row.
    return True


def _build_single_deal_link(*, base_domain: str, deal_id: str) -> str:
    did = str(deal_id or "").strip()
    if not did:
        return ""
    if base_domain:
        return f"{base_domain.rstrip('/')}/leads/detail/{did}"
    return did


def _expand_daily_rows_to_case_rows(*, rows: list[dict[str, Any]], base_domain: str) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        candidates = row.get("selection_candidates_debug", [])
        if not isinstance(candidates, list):
            candidates = []
        meaningful_candidates = [c for c in candidates if isinstance(c, dict) and _candidate_is_meaningful_for_call_review(c)]
        if not meaningful_candidates:
            continue
        for candidate in meaningful_candidates:
            deal_id = str(candidate.get("deal_id") or "").strip()
            out = dict(row)
            out["Проанализировано сделок"] = 1
            if deal_id:
                out["Ссылки на сделки"] = _build_single_deal_link(base_domain=base_domain, deal_id=deal_id)
                out["deal_id"] = deal_id
            out["daily_selection_reason_v2"] = str(
                out.get("daily_selection_reason_v2")
                or candidate.get("daily_selection_reason")
                or out.get("daily_selection_reason")
                or ""
            )
            out["daily_selection_rank"] = candidate.get("daily_selection_rank", out.get("daily_selection_rank", ""))
            out["llm_daily_rank"] = candidate.get("llm_daily_rank", "")
            out["llm_daily_rank_reason"] = candidate.get("llm_daily_rank_reason", "")
            out["skip_for_daily_reason"] = candidate.get("skip_for_daily_reason", "")
            out["transcript_usability_score"] = candidate.get(
                "transcript_usability_score_final", out.get("transcript_usability_score", "")
            )
            out["transcript_usability_label"] = candidate.get("transcript_usability_label", "")
            out["daily_case_type"] = out.get("daily_case_type") or candidate.get("call_role_signal", "")
            expanded.append(out)
    return expanded


def _adapt_case_rows_to_sheet_columns(*, rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
    alias_map = {
        "Дата анализа": "Дата контроля",
        "Дата кейса": "Дата контроля",
        "Роль": "Роль менеджера",
        "Deal ID": "deal_id",
        "Ссылка на сделку": "Ссылки на сделки",
        "Сделка": "Ключевой вывод",
        "Компания": "База микс",
        "База / тег": "База микс",
        "Тип кейса": "daily_case_type",
        "Прослушанные звонки": "Проанализировано сделок",
        "Итог / вывод": "Ключевой вывод",
        "Сильные стороны": "Сильные стороны",
        "Зоны роста": "Зоны роста",
        "Что исправить": "Что исправить",
        "Что закрепить": "Что закрепить",
        "Почему важно": "Почему это важно",
        "Почему это важно": "Почему это важно",
        "Следующий шаг": "Ожидаемый эффект - количество",
    }
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        out: dict[str, Any] = {}
        for col in columns:
            if col in row:
                out[col] = row.get(col, "")
                continue
            src_key = alias_map.get(col, "")
            if src_key:
                out[col] = row.get(src_key, "")
                continue
            out[col] = ""
        out_rows.append(out)
    return out_rows


def _maybe_write_weekly_manager_sheet(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    weekly_payload: dict[str, Any],
) -> dict[str, Any]:
    rows = weekly_payload.get("rows", []) if isinstance(weekly_payload.get("rows"), list) else []
    rows_dict = [x for x in rows if isinstance(x, dict)]
    columns = weekly_payload.get("columns", []) if isinstance(weekly_payload.get("columns"), list) else list(WEEKLY_MANAGER_COLUMNS)
    values_rows = [[row.get(col, "") for col in columns] for row in rows_dict]
    default_sheet_name = str(weekly_payload.get("sheet_name") or "").strip() or "Недельный свод менеджеров"
    default_start_cell = str(weekly_payload.get("start_cell") or "").strip() or "A2"

    sheet_name = str(getattr(cfg, "deal_analyzer_weekly_sheet_name", "") or "").strip() or default_sheet_name
    start_cell = str(getattr(cfg, "deal_analyzer_weekly_start_cell", "") or "").strip() or default_start_cell

    status = {
        "enabled": bool(getattr(cfg, "deal_analyzer_write_enabled", False)),
        "mode": "dry_run",
        "sheet_name": sheet_name,
        "start_cell": start_cell,
        "rows_prepared": len(rows_dict),
        "rows_written": 0,
        "error": "",
    }
    if not status["enabled"]:
        logger.info(
            "weekly manager writer disabled: mode=dry_run rows_prepared=%s sheet_name=%s start_cell=%s",
            status["rows_prepared"],
            sheet_name or "<empty>",
            start_cell or "<empty>",
        )
        return status

    return _maybe_write_rows_to_sheet(
        cfg=cfg,
        logger=logger,
        values_rows=values_rows,
        sheet_name=sheet_name,
        start_cell=start_cell,
        writer_tag="weekly_manager_writer",
        append_mode=False,
    )


def _resolve_spreadsheet_id_from_config(*, cfg: DealAnalyzerConfig) -> str:
    spreadsheet_id = str(getattr(cfg, "deal_analyzer_spreadsheet_id", "") or "").strip()
    if spreadsheet_id:
        return spreadsheet_id
    sheet_url = str(getattr(cfg, "deal_analyzer_sheet_url", "") or "").strip()
    if not sheet_url:
        return ""
    try:
        return extract_spreadsheet_id(sheet_url)
    except Exception:
        return ""


def _resolve_amo_base_domain_for_links(*, cfg: DealAnalyzerConfig) -> str:
    candidates: list[str] = []
    direct = str(getattr(cfg, "call_base_domain", "") or "").strip()
    if direct:
        candidates.append(direct)
    try:
        auth_cfg = load_amocrm_auth_config(getattr(cfg, "amocrm_auth_config_path", None))
        auth_domain = str(getattr(auth_cfg, "base_domain", "") or "").strip()
        if auth_domain:
            candidates.append(auth_domain)
    except Exception:
        pass

    for raw in candidates:
        text = str(raw).strip().rstrip("/")
        if not text:
            continue
        if not text.startswith(("http://", "https://")):
            text = f"https://{text}"
        return text
    return ""


def _maybe_write_rows_to_sheet(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    values_rows: list[list[Any]],
    sheet_name: str,
    start_cell: str,
    writer_tag: str,
    spreadsheet_id_override: str = "",
    append_mode: bool = False,
) -> dict[str, Any]:
    write_enabled = bool(getattr(cfg, "deal_analyzer_write_enabled", False))
    rows_prepared = len(values_rows)
    cols_total = max((len(r) for r in values_rows), default=0)
    status = {
        "enabled": write_enabled,
        "mode": "dry_run",
        "write_mode": "append" if append_mode else "overwrite",
        "sheet_name": sheet_name,
        "start_cell": start_cell,
        "rows_prepared": rows_prepared,
        "rows_written": 0,
        "write_start_row": 0,
        "write_end_row": 0,
        "error": "",
    }
    if not write_enabled:
        logger.info(
            "%s disabled: mode=dry_run rows_prepared=%s sheet_name=%s start_cell=%s",
            writer_tag,
            rows_prepared,
            sheet_name or "<empty>",
            start_cell or "<empty>",
        )
        return status
    if not sheet_name or not start_cell:
        status["error"] = "write_skipped_target_not_set"
        logger.warning(
            "%s skipped: reason=target_not_set sheet_name=%s start_cell=%s rows_prepared=%s",
            writer_tag,
            sheet_name or "<empty>",
            start_cell or "<empty>",
            rows_prepared,
        )
        return status

    spreadsheet_id = str(spreadsheet_id_override or "").strip() or _resolve_spreadsheet_id_from_config(cfg=cfg)
    if not spreadsheet_id:
        status["error"] = "write_skipped_spreadsheet_not_set"
        logger.warning("%s skipped: reason=spreadsheet_not_set", writer_tag)
        return status

    if cols_total <= 0:
        cols_total = 1
        values_rows = []

    try:
        app_cfg = load_config()
        client = GoogleSheetsApiClient(project_root=app_cfg.project_root, logger=logger)
        existing_rows_to_clear = _detect_existing_rows_to_clear(
            client=client,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_cell=start_cell,
            cols=cols_total,
            scan_rows=max(max(rows_prepared, 1), 500),
            logger=logger,
        )
        write_start_cell = start_cell
        clear_rows = max(rows_prepared, existing_rows_to_clear)
        if append_mode:
            start_col, start_row = _parse_a1_cell(start_cell)
            write_row = start_row + max(0, existing_rows_to_clear)
            write_start_cell = f"{start_col}{write_row}"
            clear_rows = 0
        logger.info(
            "%s started: spreadsheet_id=%s sheet_name=%s start_cell=%s write_start_cell=%s rows_prepared=%s cols=%s clear_rows=%s write_mode=%s",
            writer_tag,
            spreadsheet_id,
            sheet_name,
            start_cell,
            write_start_cell,
            rows_prepared,
            cols_total,
            clear_rows,
            "append" if append_mode else "overwrite",
        )
        if clear_rows > 0:
            clear_range = _build_target_a1_range(start_cell=start_cell, rows=clear_rows, cols=cols_total)
            tabbed_clear_range = client.build_tab_a1_range(tab_title=sheet_name, range_suffix=clear_range)
            empty_values = [["" for _ in range(cols_total)] for _ in range(clear_rows)]
            client.batch_update_values(
                spreadsheet_id=spreadsheet_id,
                data=[{"range": tabbed_clear_range, "values": empty_values}],
            )
        if rows_prepared > 0:
            write_range = _build_target_a1_range(start_cell=write_start_cell, rows=rows_prepared, cols=cols_total)
            tabbed_write_range = client.build_tab_a1_range(tab_title=sheet_name, range_suffix=write_range)
            normalized_rows = [list(r) + [""] * max(0, cols_total - len(r)) for r in values_rows]
            client.batch_update_values(
                spreadsheet_id=spreadsheet_id,
                data=[{"range": tabbed_write_range, "values": normalized_rows}],
            )
        status["mode"] = "real_write"
        status["rows_written"] = rows_prepared
        _, status["write_start_row"] = _parse_a1_cell(write_start_cell)
        status["write_end_row"] = status["write_start_row"] + max(0, rows_prepared) - 1 if rows_prepared > 0 else status["write_start_row"]
        logger.info(
            "%s completed: mode=real_write rows_prepared=%s rows_written=%s sheet=%s start_cell=%s write_start_row=%s write_end_row=%s",
            writer_tag,
            rows_prepared,
            status["rows_written"],
            sheet_name,
            write_start_cell,
            status["write_start_row"],
            status["write_end_row"],
        )
        return status
    except Exception as exc:
        status["error"] = str(exc)
        logger.warning(
            "%s failed: spreadsheet_id=%s sheet=%s start_cell=%s rows_prepared=%s error=%s",
            writer_tag,
            spreadsheet_id,
            sheet_name,
            start_cell,
            rows_prepared,
            exc,
        )
        return status


def _maybe_write_meeting_queue_sheet(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    dry_run_payload: dict[str, Any],
) -> dict[str, Any]:
    rows = dry_run_payload.get("rows", []) if isinstance(dry_run_payload.get("rows"), list) else []
    columns = dry_run_payload.get("columns", []) if isinstance(dry_run_payload.get("columns"), list) else []
    rows_prepared = len(rows)
    sheet_name = str(getattr(cfg, "deal_analyzer_sheet_name", "") or "").strip()
    start_cell = str(getattr(cfg, "deal_analyzer_start_cell", "") or "").strip()
    write_enabled = bool(getattr(cfg, "deal_analyzer_write_enabled", False))

    status = {
        "enabled": write_enabled,
        "mode": "dry_run",
        "sheet_name": sheet_name,
        "start_cell": start_cell,
        "rows_prepared": rows_prepared,
        "rows_written": 0,
        "error": "",
    }
    if not write_enabled:
        logger.info(
            "meeting queue writer disabled: mode=dry_run rows_prepared=%s spreadsheet_id=%s sheet_name=%s start_cell=%s",
            rows_prepared,
            str(getattr(cfg, "deal_analyzer_spreadsheet_id", "") or "").strip() or "<empty>",
            sheet_name or "<empty>",
            start_cell or "<empty>",
        )
        return status
    if not sheet_name or not start_cell:
        status["error"] = "write_skipped_target_not_set"
        logger.warning(
            "meeting queue write skipped: reason=target_not_set sheet_name=%s start_cell=%s rows_prepared=%s",
            sheet_name or "<empty>",
            start_cell or "<empty>",
            rows_prepared,
        )
        return status

    spreadsheet_id = str(getattr(cfg, "deal_analyzer_spreadsheet_id", "") or "").strip()
    sheet_url = str(getattr(cfg, "deal_analyzer_sheet_url", "") or "").strip()
    if not spreadsheet_id and sheet_url:
        try:
            spreadsheet_id = extract_spreadsheet_id(sheet_url)
        except Exception as exc:
            status["error"] = f"invalid_sheet_url:{exc}"
            logger.warning("meeting queue write skipped: reason=invalid_sheet_url error=%s", exc)
            return status
    if not spreadsheet_id:
        status["error"] = "write_skipped_spreadsheet_not_set"
        logger.warning(
            "meeting queue write skipped: reason=spreadsheet_not_set rows_prepared=%s sheet_name=%s start_cell=%s",
            rows_prepared,
            sheet_name or "<empty>",
            start_cell or "<empty>",
        )
        return status

    try:
        app_cfg = load_config()
        client = GoogleSheetsApiClient(project_root=app_cfg.project_root, logger=logger)
        values_matrix: list[list[Any]] = []
        values_matrix.append([str(col) for col in columns])
        for row in rows:
            if not isinstance(row, dict):
                continue
            values_matrix.append([row.get(col, "") for col in columns])
        rows_total = len(values_matrix)
        cols_total = len(columns)
        if rows_total <= 0 or cols_total <= 0:
            logger.info("meeting queue write skipped: reason=no_values rows_prepared=%s", rows_prepared)
            return status
        existing_rows_to_clear = _detect_existing_rows_to_clear(
            client=client,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            start_cell=start_cell,
            cols=cols_total,
            scan_rows=max(rows_total, 500),
            logger=logger,
        )
        clear_rows = max(rows_total, existing_rows_to_clear)
        clear_range = _build_target_a1_range(start_cell=start_cell, rows=clear_rows, cols=cols_total)
        tabbed_range = client.build_tab_a1_range(tab_title=sheet_name, range_suffix=clear_range)
        empty_values = [["" for _ in range(cols_total)] for _ in range(clear_rows)]
        logger.info(
            "meeting queue write started: spreadsheet_id=%s sheet_name=%s start_cell=%s rows_prepared=%s clear_rows=%s cols=%s",
            spreadsheet_id,
            sheet_name,
            start_cell,
            rows_prepared,
            clear_rows,
            cols_total,
        )
        client.batch_update_values(
            spreadsheet_id=spreadsheet_id,
            data=[{"range": tabbed_range, "values": empty_values}],
        )
        client.batch_update_values(
            spreadsheet_id=spreadsheet_id,
            data=[{"range": tabbed_range, "values": values_matrix}],
        )
        status["mode"] = "real_write"
        status["rows_written"] = max(0, rows_total - 1)
        logger.info(
            "meeting queue write completed: mode=real_write spreadsheet_id=%s sheet=%s start_cell=%s rows_prepared=%s rows_written=%s",
            spreadsheet_id,
            sheet_name,
            start_cell,
            rows_prepared,
            status["rows_written"],
        )
        return status
    except Exception as exc:
        status["error"] = str(exc)
        logger.warning(
            "meeting queue write failed: spreadsheet_id=%s sheet=%s start_cell=%s rows_prepared=%s error=%s",
            spreadsheet_id,
            sheet_name,
            start_cell,
            rows_prepared,
            exc,
        )
        return status


def _build_target_a1_range(*, start_cell: str, rows: int, cols: int) -> str:
    col_letters, row_number = _parse_a1_cell(start_cell)
    start_col = _column_letters_to_number(col_letters)
    end_col = start_col + max(0, cols) - 1
    end_row = row_number + max(0, rows) - 1
    return f"{col_letters.upper()}{row_number}:{_number_to_column_letters(end_col)}{end_row}"


def _detect_existing_rows_to_clear(
    *,
    client: GoogleSheetsApiClient,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    cols: int,
    scan_rows: int,
    logger,
) -> int:
    try:
        scan_rows_safe = max(1, int(scan_rows))
        scan_range_suffix = _build_target_a1_range(start_cell=start_cell, rows=scan_rows_safe, cols=cols)
        tabbed_scan_range = client.build_tab_a1_range(tab_title=sheet_name, range_suffix=scan_range_suffix)
        matrix = client.get_values(spreadsheet_id=spreadsheet_id, range_a1=tabbed_scan_range)
        used_rows = 0
        for idx, row in enumerate(matrix):
            row_cells = row if isinstance(row, list) else []
            if any(str(cell).strip() for cell in row_cells):
                used_rows = idx + 1
        return used_rows
    except Exception as exc:
        logger.warning("meeting queue clear scan failed, fallback to payload-size clear: error=%s", exc)
        return 0


def _read_sheet_header_columns(
    *,
    client: GoogleSheetsApiClient,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    max_cols: int = 80,
) -> list[str]:
    start_col, start_row = _parse_a1_cell(start_cell)
    start_col_n = _column_letters_to_number(start_col)
    end_col = _number_to_column_letters(start_col_n + max(1, int(max_cols)) - 1)
    header_suffix = f"{start_col}{start_row}:{end_col}{start_row}"
    tabbed = client.build_tab_a1_range(tab_title=sheet_name, range_suffix=header_suffix)
    matrix = client.get_values(spreadsheet_id=spreadsheet_id, range_a1=tabbed)
    row = matrix[0] if matrix and isinstance(matrix[0], list) else []
    columns = [str(x).strip() for x in row if str(x).strip()]
    return columns


def _read_sheet_dropdown_values_for_data_row(
    *,
    client: GoogleSheetsApiClient,
    spreadsheet_id: str,
    sheet_name: str,
    start_cell: str,
    columns: list[str],
) -> dict[int, list[str]]:
    if not columns:
        return {}
    try:
        start_col, start_row = _parse_a1_cell(start_cell)
        start_col_n = _column_letters_to_number(start_col)
        end_col = _number_to_column_letters(start_col_n + max(1, len(columns)) - 1)
        range_suffix = f"{start_col}{start_row}:{end_col}{start_row}"
        tabbed_range = client.build_tab_a1_range(tab_title=sheet_name, range_suffix=range_suffix)
        service = client.build_service()
        payload = (
            service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                ranges=[tabbed_range],
                includeGridData=True,
                fields="sheets(data(rowData(values(dataValidation(condition(values(userEnteredValue)))))))",
            )
            .execute()
        )
        sheets = payload.get("sheets", []) if isinstance(payload, dict) else []
        first_sheet = sheets[0] if sheets and isinstance(sheets[0], dict) else {}
        data_blocks = first_sheet.get("data", []) if isinstance(first_sheet.get("data"), list) else []
        first_block = data_blocks[0] if data_blocks and isinstance(data_blocks[0], dict) else {}
        row_data = first_block.get("rowData", []) if isinstance(first_block.get("rowData"), list) else []
        first_row = row_data[0] if row_data and isinstance(row_data[0], dict) else {}
        cells = first_row.get("values", []) if isinstance(first_row.get("values"), list) else []
        out: dict[int, list[str]] = {}
        for idx, cell in enumerate(cells[: len(columns)]):
            if not isinstance(cell, dict):
                continue
            dv = cell.get("dataValidation", {}) if isinstance(cell.get("dataValidation"), dict) else {}
            cond = dv.get("condition", {}) if isinstance(dv.get("condition"), dict) else {}
            values = cond.get("values", []) if isinstance(cond.get("values"), list) else []
            allowed: list[str] = []
            for raw in values:
                if not isinstance(raw, dict):
                    continue
                val = " ".join(str(raw.get("userEnteredValue") or "").split()).strip()
                if val:
                    allowed.append(val)
            if allowed:
                out[idx] = allowed
        return out
    except Exception:
        return {}


def _match_dropdown_value(value: str, allowed: list[str]) -> str:
    raw = " ".join(str(value or "").split()).strip()
    if not raw:
        return ""
    allowed_clean = [" ".join(str(x or "").split()).strip() for x in allowed if " ".join(str(x or "").split()).strip()]
    if not allowed_clean:
        return raw
    low_map = {x.lower(): x for x in allowed_clean}
    if raw.lower() in low_map:
        return low_map[raw.lower()]

    synonyms = {
        "yes": {"да", "yes", "ok", "выполнено", "сделано", "true"},
        "no": {"нет", "no", "не выполнено", "false"},
        "na": {"н/п", "не применимо", "na", "n/a", "-", "пусто"},
    }
    group = ""
    low_raw = raw.lower()
    for key, variants in synonyms.items():
        if low_raw in variants:
            group = key
            break
    if group:
        for allowed_value in allowed_clean:
            if allowed_value.lower() in synonyms[group]:
                return allowed_value

    # Last conservative fallback: if option contains raw token, keep that option.
    for allowed_value in allowed_clean:
        if low_raw in allowed_value.lower() or allowed_value.lower() in low_raw:
            return allowed_value
    return ""


def _normalize_rows_by_dropdown_values(
    *,
    rows: list[list[Any]],
    columns: list[str],
    dropdown_values_by_col: dict[int, list[str]],
) -> list[list[Any]]:
    if not rows or not columns or not dropdown_values_by_col:
        return rows
    out: list[list[Any]] = []
    for row in rows:
        values = list(row)
        for col_idx, allowed_values in dropdown_values_by_col.items():
            if col_idx < 0 or col_idx >= len(values):
                continue
            normalized = _match_dropdown_value(str(values[col_idx] or ""), allowed_values)
            values[col_idx] = normalized
        out.append(values)
    return out


def _parse_a1_cell(value: str) -> tuple[str, int]:
    text = str(value or "").strip().upper()
    m = re.match(r"^([A-Z]+)([0-9]+)$", text)
    if not m:
        raise RuntimeError(f"Invalid A1 start cell: {value!r}")
    return m.group(1), int(m.group(2))


def _column_letters_to_number(col: str) -> int:
    n = 0
    for ch in col.upper():
        n = (n * 26) + (ord(ch) - ord("A") + 1)
    return n


def _number_to_column_letters(index: int) -> str:
    if index <= 0:
        raise RuntimeError(f"Invalid column index: {index}")
    out = ""
    n = index
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


def _build_calls_markdown(*, results: list[dict[str, Any]], title: str) -> str:
    lines = [f"# {title}", ""]
    lines.append(f"Deals: {len(results)}")
    lines.append("")
    for item in results:
        summary = item.get("call_summary", {}) if isinstance(item.get("call_summary"), dict) else {}
        lines.append(f"- Deal {item.get('deal_id', '')}: source={item.get('source_used', '')}, calls={summary.get('calls_total', 0)}, missing_recording={summary.get('missing_recording_calls', 0)}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_transcripts_markdown(*, title: str, payload: dict[str, Any]) -> str:
    lines = [f"# {title}", ""]
    if "deal_id" in payload:
        lines.append(f"Deal: {payload.get('deal_id')}")
        lines.append(f"Calls: {len(payload.get('calls', []))}")
        lines.append(f"Transcripts: {len(payload.get('transcripts', []))}")
    else:
        items = payload.get("items", []) if isinstance(payload.get("items"), list) else []
        lines.append(f"Deals: {len(items)}")
        for item in items[:20]:
            lines.append(f"- Deal {item.get('deal_id', '')}: transcripts={len(item.get('transcripts', []))}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _load_json(path: Path) -> dict[str, Any] | list[Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, (dict, list)):
        raise RuntimeError(f"Unsupported JSON root in input: {path}")
    return data


def _build_snapshot_markdown(*, title: str, snapshot: dict[str, Any]) -> str:
    lines = [f"# {title}", ""]
    lines.append(f"Generated at: {snapshot.get('snapshot_generated_at', '')}")
    if "deals_total" in snapshot:
        lines.append(f"Deals total: {snapshot.get('deals_total')}")
    lines.append("")

    if "crm" in snapshot:
        crm = snapshot.get("crm", {}) if isinstance(snapshot.get("crm"), dict) else {}
        lines.append(f"Deal: {crm.get('deal_id') or crm.get('amo_lead_id')}")
        lines.append(f"Name: {crm.get('deal_name', '')}")
        lines.append(f"Enrichment status: {crm.get('enrichment_match_status', '')}")
        call_summary = (snapshot.get("call_evidence") or {}).get("summary", {}) if isinstance(snapshot.get("call_evidence"), dict) else {}
        lines.append(f"Calls total: {call_summary.get('calls_total', 0)}")
        lines.append(f"Missing recording calls: {call_summary.get('missing_recording_calls', 0)}")
    elif isinstance(snapshot.get("items"), list):
        for item in snapshot.get("items", [])[:10]:
            crm = item.get("crm", {}) if isinstance(item, dict) else {}
            lines.append(
                f"- Deal {crm.get('deal_id') or crm.get('amo_lead_id')}: enrichment={crm.get('enrichment_match_status', '')}"
            )

    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _build_roks_markdown(snapshot: dict[str, Any]) -> str:
    lines = ["# ROKS Snapshot", ""]
    lines.append(f"Scope: {snapshot.get('scope', '')}")
    lines.append(f"Manager: {snapshot.get('manager', '')}")
    lines.append(f"Sheet: {snapshot.get('sheet_title', '')}")
    lines.append(f"OK: {snapshot.get('ok', False)}")
    warnings = snapshot.get("warnings", []) if isinstance(snapshot.get("warnings"), list) else []
    if warnings:
        lines.append(f"Warnings: {'; '.join(str(x) for x in warnings)}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


if __name__ == "__main__":
    main()
