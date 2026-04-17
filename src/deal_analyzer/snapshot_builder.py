from __future__ import annotations

from datetime import datetime, timezone
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
    enriched = enrich_rows([dict(normalized_deal)], config=config, logger=logger)[0]
    manager = str(enriched.get("responsible_user_name") or "").strip()
    roks = extract_roks_snapshot(config=config, logger=logger, manager=manager or None, team=not bool(manager))

    call_downloader = CallDownloader(config=config, logger=logger)
    call_result = call_downloader.collect_deal_calls(deal=enriched, raw_bundle=raw_bundle)
    call_dicts = call_evidence_to_dicts(call_result.calls)
    transcripts = transcribe_call_evidence(calls=call_dicts, config=config, logger=logger)

    return {
        "snapshot_generated_at": datetime.now(timezone.utc).isoformat(),
        "deal_id": enriched.get("deal_id"),
        "amo_lead_id": enriched.get("amo_lead_id"),
        "crm": enriched,
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
    }


def build_period_snapshots(
    *,
    normalized_deals: list[dict[str, Any]],
    config,
    logger,
    raw_bundles_by_deal: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    enriched_rows = enrich_rows([dict(row) for row in normalized_deals], config=config, logger=logger)
    managers = sorted({str(row.get("responsible_user_name") or "").strip() for row in enriched_rows if str(row.get("responsible_user_name") or "").strip()})

    roks_team = extract_roks_snapshot(config=config, logger=logger, team=True)
    roks_by_manager: dict[str, dict[str, Any]] = {}
    for manager in managers:
        roks_by_manager[manager] = extract_roks_snapshot(config=config, logger=logger, manager=manager, team=False).to_dict()

    call_downloader = CallDownloader(config=config, logger=logger)
    raw_bundles_by_deal = raw_bundles_by_deal or {}

    items: list[dict[str, Any]] = []
    for row in enriched_rows:
        manager_name = str(row.get("responsible_user_name") or "").strip()
        deal_id = str(row.get("deal_id") or row.get("amo_lead_id") or "")
        call_result = call_downloader.collect_deal_calls(deal=row, raw_bundle=raw_bundles_by_deal.get(deal_id))
        call_dicts = call_evidence_to_dicts(call_result.calls)
        transcripts = transcribe_call_evidence(calls=call_dicts, config=config, logger=logger)

        items.append(
            {
                "deal_id": row.get("deal_id"),
                "amo_lead_id": row.get("amo_lead_id"),
                "crm": row,
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
            }
        )

    return {
        "snapshot_generated_at": datetime.now(timezone.utc).isoformat(),
        "deals_total": len(items),
        "managers": managers,
        "roks_team_context": roks_team.to_dict(),
        "items": items,
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
