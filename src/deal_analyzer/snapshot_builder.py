from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

from .call_downloader import CallDownloader
from .call_evidence import build_call_summary, call_evidence_to_dicts
from .enrichment import enrich_rows
from .roks_extractor import extract_roks_snapshot
from .transcription import transcribe_call_evidence


def build_deal_snapshot(
    *,
    normalized_deal: dict[str, Any],
    config,
    logger,
    raw_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot_warnings: list[str] = []
    enriched = _safe_enrich_one(normalized_deal=normalized_deal, config=config, logger=logger, warnings=snapshot_warnings)
    quality = _snapshot_quality(enriched)
    enriched.setdefault("data_quality_flags", quality["data_quality_flags"])
    enriched.setdefault("owner_ambiguity_flag", quality["owner_ambiguity_flag"])
    enriched.setdefault("crm_hygiene_confidence", quality["crm_hygiene_confidence"])
    manager = str(enriched.get("responsible_user_name") or "").strip()
    roks = _safe_extract_roks_snapshot(config=config, logger=logger, manager=manager or None, team=not bool(manager), warnings=snapshot_warnings)

    call_downloader = CallDownloader(config=config, logger=logger)
    call_result = _safe_collect_deal_calls(call_downloader=call_downloader, deal=enriched, raw_bundle=raw_bundle, logger=logger, warnings=snapshot_warnings)
    call_dicts = call_evidence_to_dicts(call_result.calls)
    transcripts = _safe_transcribe_call_evidence(calls=call_dicts, config=config, logger=logger, warnings=snapshot_warnings)

    return {
        "snapshot_generated_at": datetime.now(timezone.utc).isoformat(),
        "deal_id": enriched.get("deal_id"),
        "amo_lead_id": enriched.get("amo_lead_id"),
        "crm": enriched,
        "data_quality": quality,
        "enrichment": {
            "match_status": enriched.get("enrichment_match_status", "none"),
            "match_source": enriched.get("enrichment_match_source", "none"),
            "match_confidence": enriched.get("enrichment_confidence", 0.0),
            "matched_client_row_ref": enriched.get("matched_client_row_ref") or enriched.get("matched_client_list_row_id", ""),
            "matched_appointment_row_ref": enriched.get("matched_appointment_row_ref") or enriched.get("matched_appointment_row_id", ""),
        },
        "call_evidence": {
            "source_used": call_result.source_used,
            "warnings": call_result.warnings,
            "items": call_dicts,
            "summary": build_call_summary(call_result.calls),
        },
        "transcripts": transcripts,
        "call_derived_summary": _build_call_derived_summary(call_result.calls, transcripts),
        "roks_context": roks.to_dict(),
        "warnings": snapshot_warnings,
    }


def build_period_snapshots(
    *,
    normalized_deals: list[dict[str, Any]],
    config,
    logger,
    raw_bundles_by_deal: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    snapshot_warnings: list[str] = []
    enriched_rows = _safe_enrich_rows(normalized_deals=normalized_deals, config=config, logger=logger, warnings=snapshot_warnings)
    managers = sorted({str(row.get("responsible_user_name") or "").strip() for row in enriched_rows if str(row.get("responsible_user_name") or "").strip()})

    roks_team = _safe_extract_roks_snapshot(config=config, logger=logger, team=True, warnings=snapshot_warnings)
    roks_by_manager: dict[str, dict[str, Any]] = {}
    for manager in managers:
        roks_by_manager[manager] = _safe_extract_roks_snapshot(
            config=config,
            logger=logger,
            manager=manager,
            team=False,
            warnings=snapshot_warnings,
        ).to_dict()

    call_downloader = CallDownloader(config=config, logger=logger)
    raw_bundles_by_deal = raw_bundles_by_deal or {}

    items: list[dict[str, Any]] = []
    for row in enriched_rows:
        quality = _snapshot_quality(row)
        row.setdefault("data_quality_flags", quality["data_quality_flags"])
        row.setdefault("owner_ambiguity_flag", quality["owner_ambiguity_flag"])
        row.setdefault("crm_hygiene_confidence", quality["crm_hygiene_confidence"])
        manager_name = str(row.get("responsible_user_name") or "").strip()
        deal_id = str(row.get("deal_id") or row.get("amo_lead_id") or "")
        call_result = _safe_collect_deal_calls(
            call_downloader=call_downloader,
            deal=row,
            raw_bundle=raw_bundles_by_deal.get(deal_id),
            logger=logger,
            warnings=snapshot_warnings,
        )
        call_dicts = call_evidence_to_dicts(call_result.calls)
        transcripts = _safe_transcribe_call_evidence(calls=call_dicts, config=config, logger=logger, warnings=snapshot_warnings)

        items.append(
            {
                "deal_id": row.get("deal_id"),
                "amo_lead_id": row.get("amo_lead_id"),
                "crm": row,
                "data_quality": quality,
                "enrichment": {
                    "match_status": row.get("enrichment_match_status", "none"),
                    "match_source": row.get("enrichment_match_source", "none"),
                    "match_confidence": row.get("enrichment_confidence", 0.0),
                    "matched_client_row_ref": row.get("matched_client_row_ref") or row.get("matched_client_list_row_id", ""),
                    "matched_appointment_row_ref": row.get("matched_appointment_row_ref") or row.get("matched_appointment_row_id", ""),
                },
                "call_evidence": {
                    "source_used": call_result.source_used,
                    "warnings": call_result.warnings,
                    "items": call_dicts,
                    "summary": build_call_summary(call_result.calls),
                },
                "transcripts": transcripts,
                "call_derived_summary": _build_call_derived_summary(call_result.calls, transcripts),
                "roks_context": roks_by_manager.get(manager_name, roks_team.to_dict()),
                "warnings": [],
            }
        )

    return {
        "snapshot_generated_at": datetime.now(timezone.utc).isoformat(),
        "deals_total": len(items),
        "managers": managers,
        "roks_team_context": roks_team.to_dict(),
        "items": items,
        "warnings": snapshot_warnings,
    }


def _build_call_derived_summary(calls, transcripts: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(calls)
    longest = max((int(c.duration_seconds) for c in calls), default=0)
    with_transcript = sum(1 for item in transcripts if str(item.get("transcript_status") or "") in {"ok", "cached"})
    return {
        "calls_total": total,
        "longest_call_duration_seconds": longest,
        "calls_with_transcript": with_transcript,
        "primary_factual_anchor_available": bool(total > 0),
    }


def _safe_enrich_one(*, normalized_deal: dict[str, Any], config, logger, warnings: list[str]) -> dict[str, Any]:
    try:
        rows = enrich_rows([dict(normalized_deal)], config=config, logger=logger)
        if rows:
            return rows[0]
    except Exception as exc:
        logger.warning("snapshot enrich failed for deal: deal_id=%s error=%s", normalized_deal.get("deal_id") or normalized_deal.get("amo_lead_id") or "", exc)
        warnings.append(f"enrichment_failed:{exc}")
    fallback = dict(normalized_deal)
    fallback["enrichment_match_status"] = "error"
    fallback["enrichment_match_source"] = "error"
    fallback["enrichment_confidence"] = 0.0
    return fallback


def _safe_enrich_rows(*, normalized_deals: list[dict[str, Any]], config, logger, warnings: list[str]) -> list[dict[str, Any]]:
    try:
        rows = enrich_rows([dict(row) for row in normalized_deals], config=config, logger=logger)
        if rows:
            return rows
    except Exception as exc:
        logger.warning("snapshot enrich failed for period: deals=%s error=%s", len(normalized_deals), exc)
        warnings.append(f"enrichment_failed:{exc}")
    fallback_rows: list[dict[str, Any]] = []
    for row in normalized_deals:
        item = dict(row)
        item["enrichment_match_status"] = "error"
        item["enrichment_match_source"] = "error"
        item["enrichment_confidence"] = 0.0
        fallback_rows.append(item)
    return fallback_rows


def _safe_extract_roks_snapshot(*, config, logger, warnings: list[str], manager: str | None = None, team: bool = False):
    try:
        return extract_roks_snapshot(config=config, logger=logger, manager=manager, team=team)
    except Exception as exc:
        scope = "team" if team else "manager"
        error_text = str(exc)
        logger.warning("snapshot roks extraction failed: scope=%s manager=%s error=%s", scope, manager or "", exc)
        warnings.append(f"roks_failed:{scope}:{exc}")
        return SimpleNamespace(
            to_dict=lambda: {
                "ok": False,
                "scope": scope,
                "manager": manager or "",
                "source_url": "",
                "sheet_title": "",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "employee_month_context": {},
                "team_month_context": {},
                "weekly_context": {},
                "conversion_snapshot": {},
                "forecast_residual": {},
                "warnings": [f"roks_extraction_failed:{error_text}"],
                "error": error_text,
            }
        )


def _safe_collect_deal_calls(*, call_downloader, deal: dict[str, Any], raw_bundle: dict[str, Any] | None, logger, warnings: list[str]):
    try:
        return call_downloader.collect_deal_calls(deal=deal, raw_bundle=raw_bundle)
    except Exception as exc:
        deal_id = deal.get("deal_id") or deal.get("amo_lead_id") or ""
        logger.warning("snapshot call collection failed: deal_id=%s error=%s", deal_id, exc)
        warnings.append(f"call_collection_failed:{deal_id}:{exc}")
        return SimpleNamespace(
            calls=[],
            warnings=[f"call_collection_failed:{exc}"],
            source_used="error",
        )


def _safe_transcribe_call_evidence(*, calls: list[dict[str, Any]], config, logger, warnings: list[str]) -> list[dict[str, Any]]:
    try:
        return transcribe_call_evidence(calls=calls, config=config, logger=logger)
    except Exception as exc:
        logger.warning("snapshot transcription failed: calls=%s error=%s", len(calls), exc)
        warnings.append(f"transcription_failed:{exc}")
        return []


def _snapshot_quality(deal: dict[str, Any]) -> dict[str, Any]:
    flags = deal.get("data_quality_flags")
    if not isinstance(flags, list):
        flags = []
    normalized_flags: list[str] = []
    seen: set[str] = set()
    for item in flags:
        text = " ".join(str(item or "").strip().split())
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized_flags.append(text)

    owner_ambiguity_flag = bool(deal.get("owner_ambiguity_flag")) or any(
        str(x).lower().startswith("owner_ambiguity") for x in normalized_flags
    )
    crm_hygiene_confidence = str(deal.get("crm_hygiene_confidence") or "").strip().lower()
    if crm_hygiene_confidence not in {"high", "medium", "low"}:
        if any(
            x in {
                "crm_context_missing_with_stage_movement",
                "crm_context_sparse_with_activity_signals",
                "closed_lost_without_documented_reason",
                "owner_missing_in_crm",
            }
            for x in normalized_flags
        ):
            crm_hygiene_confidence = "low"
        elif normalized_flags:
            crm_hygiene_confidence = "medium"
        else:
            crm_hygiene_confidence = "high"

    return {
        "data_quality_flags": normalized_flags,
        "owner_ambiguity_flag": owner_ambiguity_flag,
        "crm_hygiene_confidence": crm_hygiene_confidence,
    }
