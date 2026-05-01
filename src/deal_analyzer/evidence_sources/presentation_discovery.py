from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.deal_analyzer.call_downloader import CallDownloader

from .amocrm_events_reader import collect_presentation_related_lead_ids
from .artifacts import write_evidence_artifacts
from .google_drive_links import collect_links_from_mapping, extract_google_drive_links_from_text
from .models import GoogleDriveLink, PresentationDiscoveryOptions, PresentationEvidenceItem
from .presentation_transcriber import transcribe_presentation_link

_PRESENTATION_TOKENS = (
    "демо",
    "демонстрац",
    "презентац",
    "показ",
    "тест",
    "счет",
    "оплат",
)


def discover_presentation_evidence(
    *,
    cfg,
    logger,
    rows: list[dict[str, Any]],
    raw_bundles_by_deal: dict[str, dict[str, Any]],
    call_ledger_all: list[dict[str, Any]],
    period_start: date,
    period_end: date,
    options: PresentationDiscoveryOptions,
    run_dir: Path,
    progress_reporter: Any | None = None,
) -> dict[str, Any]:
    debug: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "include_presentations": bool(options.include_presentations),
        "period_start": period_start.isoformat() if isinstance(period_start, date) else "",
        "period_end": period_end.isoformat() if isinstance(period_end, date) else "",
        "status": "disabled",
        "events_selection": {},
        "rows_total": len(rows),
        "rows_candidate_total": 0,
        "warnings": [],
    }
    if not bool(options.include_presentations):
        artifacts = write_evidence_artifacts(
            run_dir=run_dir,
            presentation_discovery_debug=debug,
            presentation_links_found=[],
            presentation_transcription_status=[],
            evidence_items=[],
        )
        return {
            "deal_updates": {},
            "evidence_items": [],
            "presentation_links_found": [],
            "presentation_transcription_status": [],
            "artifacts": artifacts,
            "summary": {
                "presentation_links_found_count": 0,
                "presentation_transcription_ok_count": 0,
                "presentation_transcription_failed_count": 0,
                "presentation_evidence_items_count": 0,
            },
        }

    downloader = CallDownloader(config=cfg, logger=logger)
    client, base_domain, _token, auth_error = downloader._make_api_client()  # type: ignore[attr-defined]
    if client is None:
        debug["warnings"].append(f"amocrm_client_unavailable:{auth_error}")

    event_deal_ids: set[str] = set()
    events_debug: dict[str, Any] = {}
    if client is not None:
        try:
            event_deal_ids, events_debug = collect_presentation_related_lead_ids(
                client=client,
                period_start=period_start,
                period_end=period_end,
                logger=logger,
            )
        except Exception as exc:
            debug["warnings"].append(f"events_reader_failed:{exc}")
            event_deal_ids = set()
            events_debug = {"error": str(exc)}
    debug["events_selection"] = events_debug

    rows_by_deal = {
        str(row.get("deal_id") or row.get("amo_lead_id") or "").strip(): row
        for row in rows
        if isinstance(row, dict) and str(row.get("deal_id") or row.get("amo_lead_id") or "").strip()
    }
    call_rows_by_deal: dict[str, list[dict[str, Any]]] = {}
    for item in call_ledger_all:
        if not isinstance(item, dict):
            continue
        deal_id = str(item.get("deal_id") or "").strip()
        if not deal_id:
            continue
        call_rows_by_deal.setdefault(deal_id, []).append(item)

    candidate_deal_ids = set(event_deal_ids)
    for deal_id, row in rows_by_deal.items():
        blob = _row_text_blob(row)
        if any(token in blob for token in _PRESENTATION_TOKENS):
            candidate_deal_ids.add(deal_id)
    for deal_id in call_rows_by_deal.keys():
        if deal_id in rows_by_deal:
            candidate_deal_ids.add(deal_id)

    debug["rows_candidate_total"] = len(candidate_deal_ids)
    debug["status"] = "ok"
    links_found: list[dict[str, Any]] = []
    tx_status: list[dict[str, Any]] = []
    evidence_items: list[dict[str, Any]] = []
    deal_updates: dict[str, dict[str, Any]] = {}
    max_files = max(1, int(options.max_presentation_files_per_run or 20))
    tx_attempts = 0

    for deal_id in sorted(candidate_deal_ids):
        if progress_reporter is not None:
            try:
                progress_reporter.update(
                    step_name="presentation_candidate_processing",
                    current=len(evidence_items),
                    total=max(1, len(candidate_deal_ids)),
                    current_item={"deal_id": deal_id, "stage": "presentation_discovery"},
                    log=False,
                )
            except Exception:
                pass
        row = rows_by_deal.get(deal_id, {})
        raw_bundle = raw_bundles_by_deal.get(deal_id, {}) if isinstance(raw_bundles_by_deal, dict) else {}
        row_links = _collect_links_for_deal(
            row=row,
            raw_bundle=raw_bundle,
            link_fields=str(options.presentation_link_fields or "auto"),
        )
        links_found.extend(
            [dict(x.to_dict(), deal_id=deal_id) for x in row_links]
        )
        local_evidence: list[PresentationEvidenceItem] = []

        for call in call_rows_by_deal.get(deal_id, []):
            local_evidence.append(
                PresentationEvidenceItem(
                    deal_id=deal_id,
                    evidence_type="phone_call",
                    source_location=str(call.get("source_location") or ""),
                    entity_type=str(call.get("entity_type") or "lead"),
                    call_id=str(call.get("call_id") or ""),
                    contact_id=str(call.get("contact_id") or ""),
                    contact_url=str(call.get("contact_url") or ""),
                    note_excerpt=str(call.get("deal_name") or ""),
                )
            )

        for link in row_links:
            local_evidence.append(
                PresentationEvidenceItem(
                    deal_id=deal_id,
                    evidence_type="comment_link",
                    source_location=str(link.source_field),
                    link_url=str(link.url),
                    link_kind=str(link.kind),
                    note_excerpt=str(link.source_excerpt),
                )
            )
            if link.kind in {"video", "audio", "unknown"} and tx_attempts < max_files:
                tx_attempts += 1
                if bool(options.presentation_transcribe_missing):
                    tx = transcribe_presentation_link(link=link, config=cfg, logger=logger)
                else:
                    tx = {
                        "status": "transcription_skipped_by_flag",
                        "error": "",
                        "transcript_text": "",
                        "transcript_chars": 0,
                        "backend": "",
                        "cache_hit": False,
                    }
                tx_row = {
                    "deal_id": deal_id,
                    "url": link.url,
                    "file_id": link.file_id,
                    "kind": link.kind,
                    "status": str(tx.get("status") or ""),
                    "error": str(tx.get("error") or ""),
                    "transcript_chars": int(tx.get("transcript_chars", 0) or 0),
                    "backend": str(tx.get("backend") or ""),
                    "cache_hit": bool(tx.get("cache_hit")),
                }
                tx_status.append(tx_row)
                if progress_reporter is not None:
                    try:
                        progress_reporter.update(
                            step_name="presentation_transcription",
                            current=len(tx_status),
                            total=max(1, max_files),
                            current_item={"deal_id": deal_id, "stage": "presentation_transcription"},
                            log=False,
                        )
                    except Exception:
                        pass
                if int(tx.get("transcript_chars", 0) or 0) > 0:
                    local_evidence.append(
                        PresentationEvidenceItem(
                            deal_id=deal_id,
                            evidence_type="presentation_transcript",
                            source_location="presentation_link",
                            link_url=str(link.url),
                            link_kind=str(link.kind),
                            transcript_text=str(tx.get("transcript_text") or ""),
                            transcript_status=str(tx.get("status") or ""),
                            transcript_error=str(tx.get("error") or ""),
                            transcript_chars=int(tx.get("transcript_chars", 0) or 0),
                        )
                    )
                else:
                    local_evidence.append(
                        PresentationEvidenceItem(
                            deal_id=deal_id,
                            evidence_type="presentation_video",
                            source_location="presentation_link",
                            link_url=str(link.url),
                            link_kind=str(link.kind),
                            transcript_status=str(tx.get("status") or ""),
                            transcript_error=str(tx.get("error") or ""),
                        )
                    )

        note_blob = _manual_note_blob(row=row, raw_bundle=raw_bundle)
        if note_blob:
            local_evidence.append(
                PresentationEvidenceItem(
                    deal_id=deal_id,
                    evidence_type="manual_note",
                    source_location="crm_notes_or_comments",
                    note_excerpt=note_blob[:450],
                )
            )

        demo_fields = _derive_demo_fields(local_evidence=local_evidence, note_blob=note_blob)
        for item in local_evidence:
            item_dict = item.to_dict()
            item_dict.update(demo_fields)
            evidence_items.append(item_dict)

        deal_updates[deal_id] = {
            "presentation_link_candidates": [x.to_dict() for x in row_links],
            "evidence_items": [dict(x.to_dict(), **demo_fields) for x in local_evidence],
            **demo_fields,
        }

    artifacts = write_evidence_artifacts(
        run_dir=run_dir,
        presentation_discovery_debug=debug,
        presentation_links_found=links_found,
        presentation_transcription_status=tx_status,
        evidence_items=evidence_items,
    )
    return {
        "deal_updates": deal_updates,
        "evidence_items": evidence_items,
        "presentation_links_found": links_found,
        "presentation_transcription_status": tx_status,
        "artifacts": artifacts,
        "summary": {
            "presentation_links_found_count": len(links_found),
            "presentation_transcription_ok_count": sum(
                1
                for x in tx_status
                if str(x.get("status") or "").strip().lower() in {"ok", "cached"}
                and int(x.get("transcript_chars", 0) or 0) > 0
            ),
            "presentation_transcription_failed_count": sum(
                1
                for x in tx_status
                if str(x.get("status") or "").strip().lower()
                not in {"ok", "cached", "transcription_skipped_by_flag"}
            ),
            "presentation_evidence_items_count": len(evidence_items),
            "presentation_candidates_total": len(candidate_deal_ids),
        },
    }


def _collect_links_for_deal(
    *,
    row: dict[str, Any],
    raw_bundle: dict[str, Any],
    link_fields: str = "auto",
) -> list[GoogleDriveLink]:
    out: list[GoogleDriveLink] = []
    if isinstance(row, dict):
        selected_fields = (
            [x.strip() for x in str(link_fields or "").split(",") if x.strip()]
            if str(link_fields or "").strip().lower() != "auto"
            else (
                "company_comment",
                "contact_comment",
                "notes_summary_raw",
                "tasks_summary_raw",
                "brief_url",
                "demo_result_text",
            )
        )
        out.extend(
            collect_links_from_mapping(
                record=row,
                fields=selected_fields,
            )
        )
    if isinstance(raw_bundle, dict):
        notes = raw_bundle.get("notes") if isinstance(raw_bundle.get("notes"), list) else []
        for idx, note in enumerate(notes, start=1):
            if not isinstance(note, dict):
                continue
            text_blob = _to_note_text(note)
            out.extend(
                extract_google_drive_links_from_text(
                    text=text_blob,
                    source_field=f"raw_bundle.notes[{idx}]",
                )
            )
        lead = raw_bundle.get("lead") if isinstance(raw_bundle.get("lead"), dict) else {}
        cf_values = lead.get("custom_fields_values") if isinstance(lead.get("custom_fields_values"), list) else []
        for idx, cf in enumerate(cf_values, start=1):
            if not isinstance(cf, dict):
                continue
            text_blob = _to_note_text(cf)
            out.extend(
                extract_google_drive_links_from_text(
                    text=text_blob,
                    source_field=f"raw_bundle.custom_fields_values[{idx}]",
                )
            )
    unique: dict[tuple[str, str], GoogleDriveLink] = {}
    for link in out:
        unique[(link.url, link.source_field)] = link
    return list(unique.values())


def _to_note_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        chunks: list[str] = []
        for key, raw in value.items():
            if isinstance(raw, list):
                chunks.append(f"{key}: " + ", ".join(str(x) for x in raw))
            elif isinstance(raw, dict):
                chunks.append(f"{key}: " + " ".join(f"{k2}={v2}" for k2, v2 in raw.items()))
            else:
                chunks.append(f"{key}: {raw}")
        return "\n".join(chunks)
    if isinstance(value, list):
        return "\n".join(_to_note_text(x) for x in value)
    return str(value or "")


def _row_text_blob(row: dict[str, Any]) -> str:
    chunks = [
        str(row.get("status_name") or ""),
        str(row.get("pipeline_name") or ""),
        str(row.get("deal_name") or ""),
        str(row.get("company_comment") or ""),
        str(row.get("contact_comment") or ""),
        str(row.get("manager_summary") or ""),
        str(row.get("call_signal_summary_short") or ""),
    ]
    for key in ("notes_summary_raw", "tasks_summary_raw"):
        values = row.get(key)
        if isinstance(values, list):
            chunks.extend(str(x) for x in values)
    return " ".join(chunks).lower()


def _manual_note_blob(*, row: dict[str, Any], raw_bundle: dict[str, Any]) -> str:
    chunks: list[str] = []
    for key in ("company_comment", "contact_comment", "manager_summary", "employee_coaching"):
        value = str(row.get(key) or "").strip()
        if value:
            chunks.append(value)
    notes = raw_bundle.get("notes") if isinstance(raw_bundle.get("notes"), list) else []
    for note in notes[:8]:
        if isinstance(note, dict):
            chunks.append(_to_note_text(note))
    return " ".join(chunks).strip()


def _derive_demo_fields(
    *,
    local_evidence: list[PresentationEvidenceItem],
    note_blob: str,
) -> dict[str, Any]:
    text_parts = [str(note_blob or "")]
    for item in local_evidence:
        text_parts.append(str(item.note_excerpt or ""))
        text_parts.append(str(item.transcript_text or ""))
    text = " ".join(text_parts).lower()
    aggressive_tokens = ("жмите", "покупайте", "срочно", "без вариантов", "обязаны")
    educational_tokens = (
        "давайте вместе",
        "покажу как",
        "сделайте сами",
        "совместно",
        "обучающ",
        "совместная диагностика",
    )
    hands_on_tokens = ("откройте", "нажмите", "введите", "попробуйте", "сделайте")
    discovery_tokens = ("как у вас сейчас", "узкое место", "что мешает", "почему", "где теряете")
    next_step_tokens = ("следующий шаг", "договорились", "дата", "время", "тест", "счет")

    has_aggressive = any(token in text for token in aggressive_tokens)
    has_educational = any(token in text for token in educational_tokens)
    has_hands_on = any(token in text for token in hands_on_tokens)
    has_discovery = any(token in text for token in discovery_tokens)
    has_next_step = any(token in text for token in next_step_tokens)

    if has_aggressive and not has_educational:
        demo_format = "aggressive_pitch"
    elif has_educational or has_hands_on:
        demo_format = "educational_demo"
    else:
        demo_format = "unclear"

    score = 45
    if demo_format == "educational_demo":
        score += 20
    if has_hands_on:
        score += 15
    if has_discovery:
        score += 10
    if has_next_step:
        score += 10
    if has_aggressive and not has_educational:
        score -= 20
    score = max(0, min(100, score))

    hint = (
        "После каждого смыслового блока фиксируй вывод клиента и следующий шаг с датой."
        if score < 75
        else "Сохраняй формат guided discovery: клиент делает действие сам, ты управляешь вопросами."
    )
    return {
        "demo_format_detected": demo_format,
        "client_hands_on_detected": "yes" if has_hands_on else "unknown",
        "problem_discovery_before_demo": "yes" if has_discovery else "unknown",
        "next_step_fixed_after_demo": "yes" if has_next_step else "unknown",
        "demo_quality_score_0_100": int(score),
        "demo_coaching_hint": hint,
    }
