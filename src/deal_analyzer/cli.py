from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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
from .config import DealAnalyzerConfig, load_deal_analyzer_config, resolve_period
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
from .models import AnalysisRunMetadata
from .prompt_builder import (
    append_daily_rerank_json_repair_instruction,
    append_daily_table_json_repair_instruction,
    build_daily_rerank_messages,
    build_daily_table_messages,
)
from .roks_extractor import extract_roks_snapshot
from .rules import analyze_deal
from .snapshot_builder import build_deal_snapshot, build_period_snapshots
from .transcript_signals import build_call_signal_aggregates, derive_transcript_signals
from .transcription import transcribe_call_evidence

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
    if cfg.analyzer_backend in {"ollama", "hybrid"}:
        preflight_forced_rules = _run_ollama_preflight(cfg, logger)
        if preflight_forced_rules:
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
    normalized_rows = (
        normalized_rows_ranked[: max(0, int(limit))]
        if isinstance(limit, int) and limit >= 0
        else normalized_rows_ranked
    )
    run_started_at = datetime.now(timezone.utc)
    run_dir, deals_dir = _prepare_period_run_dirs(output_dir=output_dir, run_started_at=run_started_at)

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
            snapshot = build_deal_snapshot(
                normalized_deal=row,
                config=cfg,
                logger=logger,
                raw_bundle=raw_bundles_by_deal.get(deal_hint),
            )
            crm = snapshot.get("crm") if isinstance(snapshot.get("crm"), dict) else row
            call_retry_candidate_score = _daily_candidate_retry_score(snapshot if isinstance(snapshot, dict) else {})
            allow_quality_retry = bool(
                getattr(cfg, "whisper_quality_retry_enabled", False)
                and (
                    not bool(getattr(cfg, "whisper_quality_retry_only_for_daily_candidates", True))
                    or call_retry_candidate_score >= 40
                    or idx < 3
                )
            )
            retry_info = _maybe_retry_transcript_quality_for_deal(
                snapshot=snapshot if isinstance(snapshot, dict) else {},
                crm=crm,
                cfg=cfg,
                logger=logger,
                allow_retry_for_deal=allow_quality_retry,
            )
            analysis, row_counts = _analyze_one_with_isolation(
                crm,
                cfg,
                logger,
                deal_hint=deal_hint,
                backend_override=effective_backend,
            )
            without_transcript_view = _extract_transcription_compare_view(analysis)
            analysis = _attach_enrichment_and_operator_outputs(
                analysis,
                crm,
                cfg,
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
                    "tags": merged_tags,
                    "company_tags": sorted(normalized_company_tags),
                    "propagated_company_tags": propagated_company_tags,
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
    )
    summary_payload["period_start"] = resolved.period_start.isoformat()
    summary_payload["period_end"] = resolved.period_end.isoformat()
    summary_payload["as_of_date"] = resolved.as_of_date.isoformat()
    summary_payload["live_refresh"] = refresh_diag
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
    style_source_excerpt = _load_daily_style_source_excerpt(logger=logger, cfg=cfg)
    daily_llm_runtime = _resolve_daily_llm_runtime(cfg, logger)
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
    daily_control_payload = _build_daily_control_sheet_payload(
        summary=summary_payload,
        period_deal_records=period_deal_records,
        amo_base_domain=_resolve_amo_base_domain_for_links(cfg=cfg),
        manager_allowlist=list(getattr(cfg, "daily_manager_allowlist", ()) or ()),
        cfg=cfg,
        logger=logger,
        backend_effective=effective_backend,
        style_source_excerpt=style_source_excerpt,
        llm_runtime=daily_llm_runtime,
    )
    daily_rows = daily_control_payload.get("rows", []) if isinstance(daily_control_payload.get("rows"), list) else []
    summary_payload["daily_rows_total"] = len(daily_rows)
    summary_payload["daily_rows_llm_ready"] = sum(1 for r in daily_rows if isinstance(r, dict) and bool(r.get("llm_text_ready")))
    summary_payload["daily_rows_skipped_weak_input"] = sum(
        1
        for r in daily_rows
        if isinstance(r, dict)
        and str(r.get("daily_package_quality_label") or "") in {"weak", "thin"}
        and int(r.get("negotiation_signal_presence_score", 0) or 0) <= 20
    )
    daily_control_payload_path = run_dir / "daily_control_sheet_payload.json"
    _write_json_path(daily_control_payload_path, daily_control_payload)
    write_cfg = cfg
    if not bool(daily_llm_runtime.get("selected") in {"main", "fallback"}):
        write_cfg = replace(cfg, deal_analyzer_write_enabled=False)
        logger.warning(
            "daily writer forced to dry_run: reason=no_live_llm_runtime selected=%s runtime_reason=%s",
            str(daily_llm_runtime.get("selected") or "none"),
            str(daily_llm_runtime.get("reason") or ""),
        )
    meeting_queue_writer_status = _maybe_write_daily_control_sheet(
        cfg=write_cfg,
        logger=logger,
        daily_payload=daily_control_payload,
    )
    if cfg.deal_analyzer_write_enabled and not write_cfg.deal_analyzer_write_enabled:
        meeting_queue_writer_status["error"] = "write_forced_dry_run_no_live_llm"
    summary_payload["daily_control_writer"] = meeting_queue_writer_status
    summary_payload["meeting_queue_writer"] = meeting_queue_writer_status
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
        "analyze-period success: backend=%s deals_seen=%s deals_analyzed=%s deals_failed=%s llm_success=%s llm_success_repaired=%s llm_fallback=%s llm_error=%s effective=%s json=%s md=%s csv=%s run_summary=%s run_md=%s top_risks=%s manager_brief=%s meeting_queue_json=%s meeting_queue_md=%s transcription_impact_md=%s queue_sheets_dry_run=%s daily_sheet_payload=%s",
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
    client = OllamaClient(
        base_url=cfg.ollama_base_url,
        model=cfg.ollama_model,
        timeout_seconds=cfg.ollama_timeout_seconds,
    )
    probe_timeout = min(max(3, cfg.ollama_timeout_seconds), 12)
    result = client.preflight(probe_timeout_seconds=probe_timeout)
    if result.ok:
        logger.info(
            "ollama preflight success: base_url=%s model=%s timeout_seconds=%s probe_timeout_seconds=%s",
            cfg.ollama_base_url,
            cfg.ollama_model,
            cfg.ollama_timeout_seconds,
            probe_timeout,
        )
        return False

    logger.warning(
        "ollama preflight failed: base_url=%s model=%s timeout_seconds=%s probe_timeout_seconds=%s reason=%s; switching LLM layer to rules fallback",
        cfg.ollama_base_url,
        cfg.ollama_model,
        cfg.ollama_timeout_seconds,
        probe_timeout,
        result.error,
    )
    return True


def _resolve_daily_llm_runtime(cfg: DealAnalyzerConfig, logger) -> dict[str, Any]:
    runtime = {
        "enabled": cfg.analyzer_backend in {"hybrid", "ollama"},
        "main_ok": False,
        "fallback_ok": False,
        "main_error": "",
        "fallback_error": "",
        "selected": "none",
        "reason": "",
        "main": {
            "base_url": cfg.ollama_base_url,
            "model": cfg.ollama_model,
            "timeout_seconds": cfg.ollama_timeout_seconds,
        },
        "fallback": {
            "base_url": cfg.ollama_fallback_base_url or cfg.ollama_base_url,
            "model": cfg.ollama_fallback_model or cfg.ollama_model,
            "timeout_seconds": cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds,
            "enabled": bool(cfg.ollama_fallback_enabled),
        },
    }
    if not runtime["enabled"]:
        runtime["reason"] = "llm_backend_not_requested"
        return runtime

    main_client = OllamaClient(
        base_url=cfg.ollama_base_url,
        model=cfg.ollama_model,
        timeout_seconds=cfg.ollama_timeout_seconds,
    )
    main_probe_timeout = min(max(3, int(cfg.ollama_timeout_seconds)), 12)
    main_probe = main_client.preflight(probe_timeout_seconds=main_probe_timeout)
    runtime["main_ok"] = bool(main_probe.ok)
    runtime["main_error"] = str(main_probe.error or "")
    if runtime["main_ok"]:
        runtime["selected"] = "main"
        runtime["reason"] = "main_ok"
        logger.info(
            "daily llm preflight success: selected=main base_url=%s model=%s timeout_seconds=%s",
            cfg.ollama_base_url,
            cfg.ollama_model,
            cfg.ollama_timeout_seconds,
        )

    logger.warning(
        "daily llm preflight failed: candidate=main base_url=%s model=%s reason=%s",
        cfg.ollama_base_url,
        cfg.ollama_model,
        runtime["main_error"],
    )
    if not bool(cfg.ollama_fallback_enabled):
        if not runtime["main_ok"]:
            runtime["reason"] = "fallback_disabled"
        return runtime

    fb_base = str(cfg.ollama_fallback_base_url or cfg.ollama_base_url)
    fb_model = str(cfg.ollama_fallback_model or cfg.ollama_model)
    fb_timeout = int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds)
    fb_client = OllamaClient(
        base_url=fb_base,
        model=fb_model,
        timeout_seconds=fb_timeout,
    )
    fb_probe_timeout = min(max(3, fb_timeout), 12)
    fb_probe = fb_client.preflight(probe_timeout_seconds=fb_probe_timeout)
    runtime["fallback_ok"] = bool(fb_probe.ok)
    runtime["fallback_error"] = str(fb_probe.error or "")
    if runtime["fallback_ok"]:
        if not runtime["main_ok"]:
            runtime["selected"] = "fallback"
            runtime["reason"] = "fallback_ok"
            logger.warning(
                "daily llm failover activated: selected=fallback base_url=%s model=%s timeout_seconds=%s",
                fb_base,
                fb_model,
                fb_timeout,
            )
        else:
            logger.info(
                "daily llm fallback preflight success: model=%s base_url=%s timeout_seconds=%s (standby failover ready)",
                fb_model,
                fb_base,
                fb_timeout,
            )
    else:
        if not runtime["main_ok"]:
            runtime["reason"] = "main_and_fallback_failed"
        logger.warning(
            "daily llm fallback preflight failed: base_url=%s model=%s reason=%s",
            fb_base,
            fb_model,
            runtime["fallback_error"],
        )
    return runtime


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


def _build_dial_discipline_signals(snapshot: dict[str, Any], *, status_name: str = "") -> dict[str, Any]:
    call_evidence = snapshot.get("call_evidence", {}) if isinstance(snapshot, dict) else {}
    items = call_evidence.get("items", []) if isinstance(call_evidence, dict) and isinstance(call_evidence.get("items"), list) else []
    attempts_by_phone: dict[str, list[dict[str, Any]]] = {}
    same_time_counter: Counter[str] = Counter()
    for raw in items:
        if not isinstance(raw, dict):
            continue
        phone_raw = raw.get("phone") or raw.get("phone_number") or raw.get("contact_phone") or ""
        phone7 = _normalize_phone_last7(phone_raw)
        if not phone7:
            continue
        status_low = str(raw.get("status") or raw.get("result") or raw.get("disposition") or "").strip().lower()
        direction = str(raw.get("direction") or "").strip().lower()
        ts = str(raw.get("timestamp") or raw.get("created_at") or "").strip()
        hhmm = ""
        if ts:
            m = re.search(r"T(\d{2}:\d{2})", ts)
            if m:
                hhmm = m.group(1)
        entry = {
            "status": status_low,
            "direction": direction,
            "hhmm": hhmm,
        }
        attempts_by_phone.setdefault(phone7, []).append(entry)
        if hhmm:
            same_time_counter[f"{phone7}:{hhmm}"] += 1

    unique_phones = len(attempts_by_phone)
    repeated_dead_redial_count = 0
    over_limit_numbers = 0
    for _, attempts in attempts_by_phone.items():
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
    status_low = str(status_name or "").lower()
    is_closed = any(x in status_low for x in ("закрыто", "не реализ", "успешно реализ"))
    if is_closed:
        numbers_not_fully_covered_flag = any(len(v) == 0 for v in attempts_by_phone.values()) if attempts_by_phone else False
    else:
        numbers_not_fully_covered_flag = unique_phones > 1 and any(len(v) == 1 for v in attempts_by_phone.values())

    red_flag = repeated_dead_redial_count > 0 or over_limit_numbers > 0
    return {
        "dial_unique_phones_count": unique_phones,
        "dial_attempts_total": sum(len(v) for v in attempts_by_phone.values()),
        "dial_over_limit_numbers_count": over_limit_numbers,
        "repeated_dead_redial_count": repeated_dead_redial_count,
        "repeated_dead_redial_day_flag": bool(repeated_dead_redial_count > 0),
        "same_time_redial_pattern_flag": bool(same_time_redial_pattern_flag),
        "numbers_not_fully_covered_flag": bool(numbers_not_fully_covered_flag),
        "dial_discipline_pattern_label": "red_flag" if red_flag else ("normal" if unique_phones > 0 else "none"),
    }


def _merge_deal_company_tags(*, deal_tags: list[Any], company_tags: list[Any]) -> tuple[list[str], list[str], list[str]]:
    normalized_deal_tags = {str(x).strip() for x in deal_tags if str(x).strip()}
    normalized_company_tags = {str(x).strip() for x in company_tags if str(x).strip()}
    propagated_company_tags = sorted(x for x in normalized_company_tags if x not in normalized_deal_tags)
    merged_tags = sorted(normalized_deal_tags.union(normalized_company_tags))
    return merged_tags, sorted(normalized_company_tags), propagated_company_tags


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
    mq_writer = summary.get("meeting_queue_writer", {}) if isinstance(summary.get("meeting_queue_writer"), dict) else {}
    lines.append(
        "- Meeting queue writer: mode={mode} rows={rows} target={sheet}:{cell}".format(
            mode=mq_writer.get("mode", "dry_run"),
            rows=mq_writer.get("rows_written", 0),
            sheet=mq_writer.get("sheet_name", "") or "-",
            cell=mq_writer.get("start_cell", "") or "-",
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
) -> dict[str, Any]:
    records = [x for x in period_deal_records if isinstance(x, dict) and x.get("score") is not None]
    allowlist = _resolve_daily_manager_allowlist(manager_allowlist)
    allowset = {x.lower() for x in allowlist}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in records:
        manager = _normalize_manager_for_dropdown(" ".join(str(item.get("owner_name") or "").strip().split()))
        if not manager:
            manager = "Не указан"
        if manager.lower() not in allowset:
            continue
        grouped.setdefault(manager, []).append(item)

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
    effect_forecast_fallback_logged = False
    used_deal_ids_by_manager: dict[str, set[str]] = {m: set() for m in grouped}
    for control_day in control_days:
        for manager, manager_records in sorted(grouped.items(), key=lambda x: _manager_sort_key(x[0], allowlist=allowlist)):
            role = _manager_role_label(manager)
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
            quality_mix = _build_base_mix_text(package_items)
            selection_reason = _daily_selection_reason(package_items)
            analysis_mode = _daily_analysis_mode(package_items)
            repeated_dead_redial_count = sum(int(x.get("repeated_dead_redial_count", 0) or 0) for x in package_items if isinstance(x, dict))
            repeated_dead_redial_day_flag = bool(any(bool(x.get("repeated_dead_redial_day_flag")) for x in package_items if isinstance(x, dict)))
            same_time_redial_pattern_flag = bool(any(bool(x.get("same_time_redial_pattern_flag")) for x in package_items if isinstance(x, dict)))
            numbers_not_fully_covered_flag = bool(any(bool(x.get("numbers_not_fully_covered_flag")) for x in package_items if isinstance(x, dict)))
            has_usable_negotiation = any(_transcript_usability_score(x) >= 2 for x in package_items if isinstance(x, dict))
            has_meaningful_dial_pattern = repeated_dead_redial_day_flag or same_time_redial_pattern_flag or numbers_not_fully_covered_flag
            allow_thin_for_local_debug = cfg is None and backend_effective is None
            if not has_usable_negotiation and not has_meaningful_dial_pattern and not allow_thin_for_local_debug:
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
            )
            text_generation_map = _build_text_generation_source_per_column(
                llm_columns=llm_text_columns,
                fallback_columns=fallback_columns,
            )

            llm_ready = bool(llm_text_columns.get("_llm_text_ready"))
            llm_required = cfg is not None and (backend_effective or cfg.analyzer_backend) in {"hybrid", "ollama"}
            if llm_required and not llm_ready:
                continue
            key_takeaway_text = llm_text_columns.get("Ключевой вывод", "") if llm_ready else fallback_key_takeaway
            strong_text = llm_text_columns.get("Сильные стороны", "") if llm_ready else fallback_strong_text
            growth_text = (
                llm_text_columns.get("Зоны роста", "")
                if llm_ready
                else _daily_user_text("; ".join(str(x) for x in growth_compact))
            )
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
                    "daily_analysis_mode": analysis_mode,
                    "daily_selection_reason": selection_reason,
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
                    "text_generation_source_per_column": text_generation_map,
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
                            "evidence_richness_score": c.get("_evidence_richness_score", ""),
                            "funnel_relevance_score": c.get("_funnel_relevance_score", ""),
                            "management_value_score": c.get("_management_value_score", ""),
                            "stage_priority_weight_value": c.get("_stage_priority_weight_value", ""),
                        }
                        for c in package_items
                        if isinstance(c, dict)
                    ],
                    "skipped_candidates_debug": (
                        package_items[0].get("_daily_skipped_candidates", [])
                        if package_items and isinstance(package_items[0], dict)
                        else []
                    ),
                }
            )

    rows = _apply_daily_text_antirepeat(rows)

    return {
        "mode": "daily_control",
        "sheet_name": "Дневной контроль",
        "start_cell": "A2",
        "columns": list(DAILY_CONTROL_COLUMNS),
        "rows": rows,
        "rows_count": len(rows),
    }


def _load_daily_style_source_excerpt(*, logger: Any | None, cfg: DealAnalyzerConfig | None = None) -> str:
    app = load_config()
    paths: list[Path] = []
    paths.append(app.project_root / "docs" / "мой паттерн общения.txt")
    telegram_root = app.project_root / "docs" / "style_sources" / "telegram_ilya"
    if telegram_root.exists():
        for suffix in ("*.txt", "*.md", "*.html"):
            paths.extend(sorted(telegram_root.rglob(suffix)))
    sales_defaults = [
        app.project_root / "docs" / "sales_context" / "scripts" / "link_base.md",
        app.project_root / "docs" / "sales_context" / "scripts" / "info_plm_base.md",
        app.project_root / "docs" / "sales_context" / "scripts" / "info_plm_light_industry.md",
    ]
    paths.extend(sales_defaults)
    if cfg is not None:
        refs = getattr(cfg, "sales_module_references", ()) or ()
        for raw in refs:
            candidate = Path(str(raw or "").strip())
            if not str(candidate):
                continue
            if not candidate.is_absolute():
                candidate = (app.project_root / candidate).resolve()
            if candidate.is_dir():
                for suffix in ("*.txt", "*.md", "*.html"):
                    paths.extend(sorted(candidate.rglob(suffix)))
            elif candidate.exists():
                paths.append(candidate)

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
        logger.info("daily style/reference sources loaded: count=%s paths=%s", len(loaded_paths), "; ".join(loaded_paths[:12]))
    if not chunks:
        return ""
    return " ".join(chunks)[:5000]


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

    factual_payload = _build_daily_table_factual_payload(
        cfg=cfg,
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
    )
    messages = build_daily_table_messages(
        factual_payload=factual_payload,
        config=cfg,
        style_source_excerpt=style_source_excerpt,
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
    )
    rewritten, style_applied = _style_rewrite_daily_columns(
        cfg=cfg,
        logger=logger,
        role=role,
        columns=mapped,
        style_source_excerpt=style_source_excerpt,
        llm_runtime=runtime,
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
        "Убери канцелярит, сделай живо и коротко. Верни только JSON."
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
    role_forbidden_topics = (
        [
            "презентация",
            "демонстрация",
            "бриф",
            "тест",
        ]
        if "телемаркетолог" in str(role).lower()
        else []
    )
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
    return {
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
        "role_forbidden_topics": role_forbidden_topics,
        "sales_toolbox_modules": sales_toolbox_modules,
        "product_reference_urls": product_reference_urls,
        "fallback_reference": fallback_columns,
        "data_confidence_hint": _daily_confidence_hint(package_items),
    }


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


def _sanitize_daily_llm_columns(*, payload: dict[str, Any], fallback: dict[str, str], role: str) -> dict[str, str]:
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
        fallback="",
    )
    mapped["Ожидаемый эффект - количество"] = _sanitize_daily_expected_quantity(
        value=mapped["Ожидаемый эффект - количество"],
        fallback="",
    )
    mapped["Ожидаемый эффект - качество"] = _sanitize_daily_expected_quality(
        value=mapped["Ожидаемый эффект - качество"],
        fallback="",
    )
    return mapped


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
    segments: Counter[str] = Counter()
    for item in records:
        # 1) deal tags
        for key in ("tags",):
            values = item.get(key)
            if not isinstance(values, list):
                continue
            for raw in values:
                text = " ".join(str(raw or "").split()).strip()
                if text:
                    segments[text] += 1
        # 2) company tags
        for key in ("company_tags",):
            values = item.get(key)
            if not isinstance(values, list):
                continue
            for raw in values:
                text = " ".join(str(raw or "").split()).strip()
                if text:
                    segments[text] += 1
        # 3) source / utm / pipeline hints
        for key in ("source_values",):
            values = item.get(key)
            if not isinstance(values, list):
                continue
            for raw in values:
                text = " ".join(str(raw or "").split()).strip()
                if text:
                    segments[text] += 1
        # 4) comments / notes / okved hints
        notes = item.get("notes_summary_raw") if isinstance(item.get("notes_summary_raw"), list) else []
        for note in notes:
            if not isinstance(note, dict):
                continue
            raw = str(note.get("text") or "").strip().lower()
            if "оквэд" in raw:
                segments["ОКВЭД/сегмент из комментариев"] += 1
            if "тендер" in raw:
                segments["тендерные"] += 1
            if "закуп" in raw:
                segments["закупки"] += 1
        for key in ("company_comment", "contact_comment"):
            raw_comment = str(item.get(key) or "").strip().lower()
            if "оквэд" in raw_comment:
                segments["ОКВЭД/сегмент из комментариев"] += 1
            if "тендер" in raw_comment:
                segments["тендерные"] += 1
            if "закуп" in raw_comment:
                segments["закупки"] += 1
    if segments:
        top = [name for name, _ in segments.most_common(3)]
        return "; ".join(top)
    return "солянка"


def _manager_role_label(manager_name: str) -> str:
    text = str(manager_name or "").lower()
    if "рустам" in text:
        return "телемаркетолог"
    if "илья" in text:
        return "менеджер по продажам"
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
        key = (tier, -funnel_score, -evidence_score, -mgmt, -freshness, carry_penalty, tie_id)
        ranked.append((key, item, tier_reason))

    ranked.sort(key=lambda x: x[0])
    prefiltered: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for idx, (_, item, tier_reason) in enumerate(ranked, start=1):
        transcript_label = str(item.get("transcript_usability_label") or "").strip().lower()
        transcript_score = _transcript_usability_score(item)
        evidence_score = _evidence_richness_score(item)
        management_score = _management_value_rank(item)
        funnel_score = _funnel_relevance_score(item, stage_priority_weights=stage_priority_weights)
        role_signal = _call_role_signal(item)
        skip_reason = ""
        if (
            transcript_label in {"weak", "noisy", "empty"}
            and evidence_score < 5
            and role_signal not in {"history_pattern", "secretary"}
            and int(item.get("call_history_pattern_score", 0) or 0) < 45
        ):
            skip_reason = "weak_transcript_and_thin_crm"
        enriched = dict(item)
        enriched["_daily_selection_rank"] = idx
        enriched["_daily_selection_reason"] = tier_reason
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
    tier_selected: dict[int, int] = {0: 0, 1: 0, 2: 0}
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
        # Weak tier is used only as honest fallback when no richer candidates are available.
        if tier_num == 2 and (tier_selected[0] > 0 or tier_selected[1] > 0):
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
        if int(item.get("call_history_pattern_score", 0) or 0) < 45 and _call_role_signal(item) not in {"secretary", "history_pattern"}:
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
    noise_markers = ("шум", "неразборчив", "обрыв", "тишин", "пусто", "непонят")
    if any(marker in text for marker in noise_markers):
        return 1
    if len(excerpt) >= 120 or len(call_summary) >= 60:
        return 3
    return 2


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
    has_call = (
        bool(item.get("transcript_available"))
        or bool(str(item.get("call_signal_summary_short") or "").strip())
        or int(item.get("call_candidates_count") or 0) > 0
    )
    if role_signal == "lpr" and has_call and transcript_score >= 1 and evidence_score >= 4:
        return 0, "has_call_priority"
    if role_signal == "secretary" and has_call and transcript_score >= 1 and evidence_score >= 3:
        return 1, "secretary_fallback_priority"
    if role_signal == "history_pattern" and int(item.get("call_history_pattern_score", 0) or 0) >= 45:
        return 1, "call_history_pattern_priority"
    if bool(item.get("repeated_dead_redial_day_flag")) or int(item.get("repeated_dead_redial_count", 0) or 0) > 0:
        return 1, "dial_discipline_priority"
    if has_call and transcript_score >= 1 and evidence_score >= 5:
        return 0, "has_call_priority"
    if evidence_score >= 5:
        return 1, "rich_context_priority"
    return 2, "fallback_fill"


def _call_role_signal(item: dict[str, Any]) -> str:
    text = " ".join(
        str(item.get(k) or "").strip().lower()
        for k in ("call_signal_summary_short", "transcript_text_excerpt", "manager_summary")
    )
    if any(token in text for token in ("лпр", "директор", "собственник", "руководител", "лицо принима")):
        return "lpr"
    if any(token in text for token in ("секретар", "ресепш", "переадрес", "соедините")):
        return "secretary"
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
    downstream = 0
    if stage_focus in {"lpr", "qualification"}:
        lpr_to_meeting = conversions.get("lpr_to_meeting", conversions.get("lpr_meeting", 0.35))
        downstream = max(0, round(base_delta * lpr_to_meeting))
        return f"+{base_delta} качественных ЛПР за неделю, это даст примерно +{downstream} встреч в работу."
    if stage_focus in {"meeting", "meeting_to_next"}:
        meeting_to_next = conversions.get("meeting_to_next", conversions.get("meeting_next", 0.4))
        downstream = max(0, round(base_delta * meeting_to_next))
        return f"+{base_delta} подтвержденная встреча в неделю, из них примерно +{downstream} перейдут в следующий шаг."
    if stage_focus == "next_step":
        next_to_progress = conversions.get("next_to_progress", conversions.get("next_progress", 0.5))
        downstream = max(0, round(base_delta * next_to_progress))
        return f"{base_delta}-2 сделки меньше будут зависать; это даст примерно +{downstream} сделок с понятным движением вниз."
    return f"+{base_delta} дополнительный рабочий шаг в неделю."


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


def _maybe_write_daily_control_sheet(
    *,
    cfg: DealAnalyzerConfig,
    logger,
    daily_payload: dict[str, Any],
) -> dict[str, Any]:
    rows = daily_payload.get("rows", []) if isinstance(daily_payload.get("rows"), list) else []
    columns = daily_payload.get("columns", []) if isinstance(daily_payload.get("columns"), list) else []
    rows_dict = [x for x in rows if isinstance(x, dict)]
    llm_ready_rows = [row for row in rows_dict if bool(row.get("llm_text_ready"))]
    values_rows = [[row.get(col, "") for col in columns] for row in llm_ready_rows]

    sheet_name = str(getattr(cfg, "deal_analyzer_daily_sheet_name", "") or "").strip()
    if not sheet_name:
        sheet_name = str(getattr(cfg, "deal_analyzer_sheet_name", "") or "").strip()
    if not sheet_name:
        sheet_name = str(daily_payload.get("sheet_name") or "").strip() or "Дневной контроль"

    start_cell = str(getattr(cfg, "deal_analyzer_daily_start_cell", "") or "").strip()
    if not start_cell:
        start_cell = str(getattr(cfg, "deal_analyzer_start_cell", "") or "").strip()
    if not start_cell:
        start_cell = str(daily_payload.get("start_cell") or "").strip() or "A2"

    return _maybe_write_rows_to_sheet(
        cfg=cfg,
        logger=logger,
        values_rows=values_rows,
        sheet_name=sheet_name,
        start_cell=start_cell,
        writer_tag="daily_control_writer",
        append_mode=not bool(getattr(cfg, "deal_analyzer_overwrite_mode", False)),
    )


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
