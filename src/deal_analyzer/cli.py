from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging
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
from .llm_backend import analyze_deal_with_ollama_outcome
from .llm_client import OllamaClient
from .models import AnalysisRunMetadata
from .roks_extractor import extract_roks_snapshot
from .rules import analyze_deal
from .snapshot_builder import build_deal_snapshot, build_period_snapshots
from .transcription import transcribe_call_evidence


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deal analyzer CLI (rules + Ollama backends)")
    parser.add_argument("--config", required=True, help="Path to analyzer config JSON")
    parser.add_argument("--no-latest", action="store_true", help="Disable latest copy outputs")

    sub = parser.add_subparsers(dest="command", required=True)

    one = sub.add_parser("analyze-deal", help="Analyze one deal from collector JSON")
    one.add_argument("--input", required=True, help="Path to collector deal JSON")

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

    preflight_forced_rules = False
    effective_backend = cfg.analyzer_backend
    if cfg.analyzer_backend == "ollama":
        preflight_forced_rules = _run_ollama_preflight(cfg, logger)
        if preflight_forced_rules:
            effective_backend = "rules"

    normalized_rows = _extract_period_normalized(payload)
    normalized_rows = _maybe_enrich_rows(normalized_rows, cfg, logger)
    analyses, llm_counts = _analyze_period_rows(normalized_rows, cfg, logger, backend_override=effective_backend)

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

    logger.info(
        "analyze-period success: backend=%s deals=%s llm_success=%s llm_success_repaired=%s llm_fallback=%s llm_error=%s effective=%s json=%s md=%s csv=%s",
        cfg.analyzer_backend,
        len(analyses),
        llm_counts["llm_success_count"],
        llm_counts["llm_success_repaired_count"],
        llm_counts["llm_fallback_count"],
        llm_counts["llm_error_count"],
        effective_summary,
        json_out.timestamped,
        md_out.timestamped,
        csv_out.timestamped,
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
        "ollama preflight failed: base_url=%s model=%s timeout_seconds=%s probe_timeout_seconds=%s reason=%s; switching entire period to rules fallback",
        cfg.ollama_base_url,
        cfg.ollama_model,
        cfg.ollama_timeout_seconds,
        probe_timeout,
        result.error,
    )
    return True


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
        analysis = _attach_enrichment_and_operator_outputs(analysis, normalized, cfg)
        return analysis, counts

    if effective_backend != "ollama":
        raise RuntimeError(f"Unsupported analyzer backend: {effective_backend}")

    logger.info(
        "ollama analyze call: deal=%s model=%s base_url=%s timeout_seconds=%s",
        deal_hint,
        cfg.ollama_model,
        cfg.ollama_base_url,
        cfg.ollama_timeout_seconds,
    )

    try:
        outcome = analyze_deal_with_ollama_outcome(normalized_deal=normalized, config=cfg)
        analysis = outcome.analysis.to_dict()
        analysis["analysis_backend_requested"] = "ollama"
        analysis["backend"] = cfg.analyzer_backend
        if outcome.backend_used == "ollama":
            counts["llm_success_count"] += 1
            if outcome.repaired:
                counts["llm_success_repaired_count"] += 1
        else:
            counts["llm_fallback_count"] += 1
        if outcome.llm_error:
            counts["llm_error_count"] += 1
            logger.warning("ollama fallback used: deal=%s reason=%s", deal_hint, outcome.error_message)
        analysis = _attach_enrichment_and_operator_outputs(analysis, normalized, cfg)
        return analysis, counts
    except Exception as exc:  # hard isolation for batch path
        logger.warning("ollama analyze failed, fallback to rules: deal=%s error=%s", deal_hint, exc)
        fallback = analyze_deal(normalized, cfg).to_dict()
        fallback["analysis_backend_requested"] = "ollama"
        fallback["analysis_backend_used"] = "rules_fallback"
        fallback["llm_repair_applied"] = False
        fallback["backend"] = cfg.analyzer_backend
        fallback = _attach_enrichment_and_operator_outputs(fallback, normalized, cfg)
        counts["llm_fallback_count"] += 1
        counts["llm_error_count"] += 1
        return fallback, counts


def _attach_enrichment_and_operator_outputs(
    analysis: dict[str, Any], normalized_deal: dict[str, Any], cfg: DealAnalyzerConfig
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
    return out


def _build_backend_effective_summary(
    llm_counts: dict[str, int], backend_requested: str, preflight_forced_rules: bool = False
) -> str:
    if backend_requested == "rules":
        return "rules_only"
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
