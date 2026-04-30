from __future__ import annotations

import hashlib
from datetime import date, datetime, time, timedelta, timezone
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from typing import Any

from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.state_store import load_auth_state
from src.amocrm_collector.client import AmoCollectorClient, ApiRequestError
from src.config import load_config

from .call_evidence import (
    CallEvidence,
    build_call_summary,
    deduplicate_calls,
    extract_calls_from_normalized_deal,
    extract_calls_from_notes,
)


@dataclass(frozen=True)
class CallCollectionResult:
    deal_id: str
    calls: list[CallEvidence]
    warnings: list[str]
    source_used: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "deal_id": self.deal_id,
            "source_used": self.source_used,
            "warnings": list(self.warnings),
            "calls": [c.to_dict() for c in self.calls],
            "call_summary": build_call_summary(self.calls),
        }


class CallDownloader:
    def __init__(self, *, config, logger) -> None:
        self.config = config
        self.logger = logger
        app_cfg = load_config()
        cache_hint = str(getattr(self.config, "audio_cache_dir", "workspace/deal_analyzer/audio_cache") or "workspace/deal_analyzer/audio_cache")
        self.audio_cache_dir = (app_cfg.project_root / cache_hint).resolve()
        self.audio_cache_dir.mkdir(parents=True, exist_ok=True)
        self._auth_header_token: str | None = None

    def collect_deal_calls(
        self,
        *,
        deal: dict[str, Any],
        raw_bundle: dict[str, Any] | None = None,
        resolve_audio: bool = True,
        audio_call_ids: set[str] | None = None,
    ) -> CallCollectionResult:
        deal_id = str(deal.get("deal_id") or deal.get("amo_lead_id") or "")
        mode = str(getattr(self.config, "call_collection_mode", "disabled") or "disabled").strip().lower()
        warnings: list[str] = []
        candidates: list[CallEvidence] = []
        source_used = "none"

        if mode == "disabled":
            return CallCollectionResult(deal_id=deal_id, calls=[], warnings=["call_collection_disabled"], source_used="disabled")

        if mode in {"api_first", "api_only"}:
            api_calls, api_warning = self._collect_from_api(deal_id=deal_id)
            if api_warning:
                warnings.append(api_warning)
            if api_calls:
                candidates.extend(api_calls)
                source_used = "amocrm_api"

        if (mode in {"api_first", "raw_fallback", "raw_only"} and not candidates and isinstance(raw_bundle, dict)):
            notes = raw_bundle.get("notes") if isinstance(raw_bundle.get("notes"), list) else []
            users_cache = raw_bundle.get("users_cache") if isinstance(raw_bundle.get("users_cache"), dict) else {}
            raw_calls = extract_calls_from_notes(
                notes=notes,
                deal_id=deal_id,
                users_cache=users_cache,
                source_location="collector_raw_bundle:notes",
            )
            if raw_calls:
                candidates.extend(raw_calls)
                source_used = "collector_raw_bundle"

        if not candidates:
            fallback_calls = extract_calls_from_normalized_deal(deal=deal)
            if fallback_calls:
                candidates.extend(fallback_calls)
                source_used = "normalized_fallback"

        deduped = deduplicate_calls(candidates)
        if resolve_audio:
            deduped = [self._resolve_call_audio(call, audio_call_ids=audio_call_ids) for call in deduped]
        return CallCollectionResult(deal_id=deal_id, calls=deduped, warnings=warnings, source_used=source_used)

    def collect_period_calls(
        self,
        *,
        deals: list[dict[str, Any]],
        raw_bundles_by_deal: dict[str, dict[str, Any]] | None = None,
        resolve_audio: bool = True,
        audio_call_ids_by_deal: dict[str, set[str]] | None = None,
    ) -> dict[str, CallCollectionResult]:
        out: dict[str, CallCollectionResult] = {}
        raw_bundles_by_deal = raw_bundles_by_deal or {}
        audio_call_ids_by_deal = audio_call_ids_by_deal or {}
        for deal in deals:
            deal_id = str(deal.get("deal_id") or deal.get("amo_lead_id") or "")
            out[deal_id] = self.collect_deal_calls(
                deal=deal,
                raw_bundle=raw_bundles_by_deal.get(deal_id),
                resolve_audio=resolve_audio,
                audio_call_ids=audio_call_ids_by_deal.get(deal_id),
            )
        return out

    def collect_period_calls_call_first(
        self,
        *,
        period_start: date,
        period_end: date,
    ) -> tuple[list[CallEvidence], dict[str, Any]]:
        """Collect call notes from global call source for the period before deal-based filtering."""
        client, base_domain, token, auth_error = self._make_api_client()
        base_domain_clean = str(base_domain or "").strip().rstrip("/")
        if base_domain_clean and not base_domain_clean.startswith(("http://", "https://")):
            base_domain_clean = f"https://{base_domain_clean}"
        audit: dict[str, Any] = {
            "source_mode": "deal_first_fallback",
            "base_domain": base_domain,
            "global_source_attempts": [],
            "contacts_source_attempts": [],
            "events_fallback_attempts": [],
            "calls_seen_from_global_source": 0,
            "calls_missing_deal_id": 0,
            "contacts_notes_requests_count": 0,
            "contact_calls_seen": 0,
            "contact_calls_linked_to_deal": 0,
            "contact_calls_without_deal": 0,
            "contact_calls_written_as_contact_only": 0,
            "contact_call_resolution_debug": [],
            "deals_resolved_from_calls": 0,
            "deals_failed_to_resolve": 0,
            "auth_error": auth_error or "",
        }
        if client is None:
            return [], audit

        users_cache = client.get_users_cache()
        calls_by_key: dict[str, CallEvidence] = {}
        missing_deal_id = 0
        contact_calls_seen = 0
        contact_calls_linked_to_deal = 0
        contact_calls_without_deal = 0
        contact_links_cache: dict[str, list[int]] = {}
        lead_snapshot_cache: dict[int, dict[str, Any] | None] = {}
        contact_call_resolution_debug: list[dict[str, Any]] = []
        period_from_unix, period_to_unix = self._period_unix_bounds(period_start=period_start, period_end=period_end)

        def _append_call(call: CallEvidence) -> None:
            key = self._call_dedup_key(call)
            current = calls_by_key.get(key)
            if current is None:
                calls_by_key[key] = call
                return
            # Required precedence: if same call exists in lead and contact sources, keep lead.
            current_is_lead = str(getattr(current, "entity_type", "lead") or "lead").strip().lower() == "lead"
            new_is_lead = str(getattr(call, "entity_type", "lead") or "lead").strip().lower() == "lead"
            if new_is_lead and not current_is_lead:
                calls_by_key[key] = call
                return
            if current_is_lead and not new_is_lead:
                return
            if bool(call.recording_url) and not bool(current.recording_url):
                calls_by_key[key] = call
                return
            if int(call.duration_seconds or 0) > int(current.duration_seconds or 0):
                calls_by_key[key] = call

        def _resolve_contact_linked_leads(contact_id_text: str) -> list[int]:
            cached = contact_links_cache.get(contact_id_text)
            if cached is not None:
                return list(cached)
            out: list[int] = []
            if not contact_id_text.isdigit():
                contact_links_cache[contact_id_text] = out
                return out
            try:
                links = client.get_contact_links(int(contact_id_text))
            except Exception:
                links = []
            for link in links:
                if not isinstance(link, dict):
                    continue
                to_type = str(link.get("to_entity_type") or "").strip().lower()
                if to_type != "leads":
                    continue
                to_id = link.get("to_entity_id")
                if isinstance(to_id, int) and to_id > 0:
                    out.append(to_id)
                else:
                    text = str(to_id or "").strip()
                    if text.isdigit():
                        out.append(int(text))
            unique_sorted = sorted(set(out))
            contact_links_cache[contact_id_text] = unique_sorted
            return list(unique_sorted)

        def _load_lead_snapshot(lead_id: int) -> dict[str, Any] | None:
            cached = lead_snapshot_cache.get(int(lead_id))
            if int(lead_id) in lead_snapshot_cache:
                return cached
            snapshot: dict[str, Any] | None = None
            try:
                loaded = client.get_lead(int(lead_id))
                snapshot = loaded if isinstance(loaded, dict) else None
            except Exception:
                snapshot = None
            lead_snapshot_cache[int(lead_id)] = snapshot
            return snapshot

        def _select_best_lead_for_contact(*, lead_ids: list[int], note: dict[str, Any]) -> tuple[str, str]:
            params = note.get("params") if isinstance(note.get("params"), dict) else {}
            lead_hint = ""
            for key in ("lead_id", "deal_id", "entity_id"):
                value = params.get(key)
                if isinstance(value, int) and value > 0:
                    lead_hint = str(value)
                    break
                text = str(value or "").strip()
                if text.isdigit():
                    lead_hint = text
                    break
            if lead_hint and any(int(lead_hint) == int(x) for x in lead_ids):
                return lead_hint, "lead_hint_from_note_params"
            if not lead_ids:
                return "", "no_linked_leads"
            best_lead_id = ""
            best_rank: tuple[int, int, int] | None = None
            for lead_id in lead_ids:
                snapshot = _load_lead_snapshot(lead_id)
                if not isinstance(snapshot, dict):
                    rank = (0, 0, 0)
                else:
                    status_id_raw = snapshot.get("status_id")
                    status_id = int(status_id_raw) if isinstance(status_id_raw, int) else 0
                    is_open = 1 if self._is_probably_open_lead_status(status_id) else 0
                    updated = int(snapshot.get("updated_at") or 0)
                    created = int(snapshot.get("created_at") or 0)
                    rank = (is_open, updated, created)
                if best_rank is None or rank > best_rank:
                    best_rank = rank
                    best_lead_id = str(lead_id)
            if best_lead_id:
                return best_lead_id, "best_linked_lead_by_open_status_and_recency"
            return str(lead_ids[0]), "linked_lead_fallback_first"

        def _append_calls_from_notes(*, notes: list[dict[str, Any]], source_location: str) -> None:
            nonlocal missing_deal_id
            for note in notes:
                if not isinstance(note, dict):
                    continue
                deal_id = self._resolve_note_deal_id(note)
                if not deal_id:
                    missing_deal_id += 1
                    continue
                extracted = extract_calls_from_notes(
                    notes=[note],
                    deal_id=deal_id,
                    users_cache=users_cache,
                    source_location=source_location,
                )
                for call in extracted:
                    _append_call(call)

        def _append_calls_from_contact_notes(*, notes: list[dict[str, Any]], source_location: str) -> None:
            nonlocal contact_calls_seen, contact_calls_linked_to_deal, contact_calls_without_deal
            for note in notes:
                if not isinstance(note, dict):
                    continue
                note_type = str(note.get("note_type") or "").strip().lower()
                if note_type not in {"call_in", "call_out"}:
                    continue
                contact_id = self._resolve_note_contact_id(note)
                if not contact_id:
                    continue
                linked_lead_ids = _resolve_contact_linked_leads(contact_id)
                selected_deal_id, resolution_reason = _select_best_lead_for_contact(
                    lead_ids=linked_lead_ids,
                    note=note,
                )
                if selected_deal_id:
                    entity_type = "lead"
                    final_deal_id = selected_deal_id
                    contact_calls_linked_to_deal += 1
                else:
                    entity_type = "contact_only"
                    final_deal_id = f"contact_{contact_id}"
                    contact_calls_without_deal += 1

                contact_url = (
                    f"{base_domain_clean}/contacts/detail/{contact_id}"
                    if base_domain_clean and contact_id
                    else ""
                )
                extracted = extract_calls_from_notes(
                    notes=[note],
                    deal_id=final_deal_id,
                    users_cache=users_cache,
                    source_location=source_location,
                    entity_type=entity_type,
                    contact_id=contact_id,
                    contact_url=contact_url,
                    resolution_reason=resolution_reason,
                )
                for call in extracted:
                    contact_calls_seen += 1
                    contact_call_resolution_debug.append(
                        {
                            "contact_id": str(contact_id),
                            "call_id": str(call.call_id or ""),
                            "linked_leads_found": [int(x) for x in linked_lead_ids],
                            "selected_deal_id": str(selected_deal_id or ""),
                            "final_entity_type": entity_type,
                            "resolution_reason": str(resolution_reason or ""),
                        }
                    )
                    _append_call(call)

        # Primary path: /api/v4/leads/notes with period filters and call note types.
        note_variants: list[dict[str, Any]] = [
            {"label": "notes_call_in", "note_type": "call_in"},
            {"label": "notes_call_out", "note_type": "call_out"},
            {"label": "notes_without_note_type_filter", "note_type": None},
        ]
        for variant in note_variants:
            total_items = 0
            page_requests = 0
            success = False
            last_error = ""
            last_request = ""
            for page in range(1, 101):
                params: dict[str, Any] = {
                    "limit": 250,
                    "page": page,
                    "filter[created_at][from]": period_from_unix,
                    "filter[created_at][to]": period_to_unix,
                }
                note_type = variant.get("note_type")
                if isinstance(note_type, str) and note_type:
                    params["filter[note_type]"] = note_type
                try:
                    notes_page, meta, request_path = client.get_leads_notes_page(params=params)
                    page_requests += 1
                    last_request = request_path
                    page_count = len(notes_page)
                    total_items += page_count
                    _append_calls_from_notes(
                        notes=notes_page,
                        source_location=f"amocrm_api:global_notes:{variant['label']}",
                    )
                    success = True
                    if page_count < 250:
                        break
                except ApiRequestError as exc:
                    last_error = str(exc)
                    last_request = exc.path
                    break
                except Exception as exc:
                    last_error = str(exc)
                    break
            audit["global_source_attempts"].append(
                {
                    "label": str(variant.get("label") or ""),
                    "endpoint": "/api/v4/leads/notes",
                    "request_path_last": last_request,
                    "page_requests": int(page_requests),
                    "response_items_total": int(total_items),
                    "ok": bool(success),
                    "error": str(last_error or ""),
                }
            )

        # Secondary path: /api/v4/contacts/notes with same period filters and call note types.
        for variant in note_variants:
            total_items = 0
            page_requests = 0
            success = False
            last_error = ""
            last_request = ""
            for page in range(1, 101):
                params = {
                    "limit": 250,
                    "page": page,
                    "filter[created_at][from]": period_from_unix,
                    "filter[created_at][to]": period_to_unix,
                }
                note_type = variant.get("note_type")
                if isinstance(note_type, str) and note_type:
                    params["filter[note_type]"] = note_type
                try:
                    notes_page, meta, request_path = client.get_contacts_notes_page(params=params)
                    page_requests += 1
                    last_request = request_path
                    page_count = len(notes_page)
                    total_items += page_count
                    _append_calls_from_contact_notes(
                        notes=notes_page,
                        source_location="amocrm_api:contacts_notes",
                    )
                    success = True
                    if page_count < 250:
                        break
                except ApiRequestError as exc:
                    last_error = str(exc)
                    last_request = exc.path
                    break
                except Exception as exc:
                    last_error = str(exc)
                    break
            audit["contacts_notes_requests_count"] = int(audit.get("contacts_notes_requests_count", 0) or 0) + int(page_requests)
            audit["contacts_source_attempts"].append(
                {
                    "label": str(variant.get("label") or ""),
                    "endpoint": "/api/v4/contacts/notes",
                    "request_path_last": last_request,
                    "page_requests": int(page_requests),
                    "response_items_total": int(total_items),
                    "ok": bool(success),
                    "error": str(last_error or ""),
                }
            )

        # Fallback path: events search -> resolve lead_ids -> per-deal notes.
        if not calls_by_key:
            event_type_variants = (
                "incoming_call",
                "outgoing_call",
                "call_in",
                "call_out",
                "",
            )
            lead_ids_from_events: set[int] = set()
            for event_type in event_type_variants:
                total_items = 0
                page_requests = 0
                success = False
                last_error = ""
                last_request = ""
                for page in range(1, 51):
                    params: dict[str, Any] = {
                        "limit": 250,
                        "page": page,
                        "filter[created_at][from]": period_from_unix,
                        "filter[created_at][to]": period_to_unix,
                    }
                    if event_type:
                        params["filter[type]"] = event_type
                    try:
                        events_page, meta, request_path = client.get_events_page(params=params)
                        page_requests += 1
                        last_request = request_path
                        page_count = len(events_page)
                        total_items += page_count
                        for event in events_page:
                            lead_id = self._resolve_event_deal_id(event)
                            if isinstance(lead_id, int) and lead_id > 0:
                                lead_ids_from_events.add(lead_id)
                        success = True
                        if page_count < 250:
                            break
                    except ApiRequestError as exc:
                        last_error = str(exc)
                        last_request = exc.path
                        break
                    except Exception as exc:
                        last_error = str(exc)
                        break
                audit["events_fallback_attempts"].append(
                    {
                        "event_type": event_type or "no_type_filter",
                        "endpoint": "/api/v4/events",
                        "request_path_last": last_request,
                        "page_requests": int(page_requests),
                        "response_items_total": int(total_items),
                        "ok": bool(success),
                        "error": str(last_error or ""),
                    }
                )

            if lead_ids_from_events:
                for lead_id in sorted(lead_ids_from_events):
                    try:
                        notes = client.get_notes_by_lead(lead_id)
                    except Exception:
                        continue
                    scoped = [
                        n
                        for n in notes
                        if isinstance(n, dict)
                        and str(n.get("note_type") or "").strip().lower() in {"call_in", "call_out"}
                        and period_from_unix <= int(n.get("created_at") or 0) <= period_to_unix
                    ]
                    _append_calls_from_notes(
                        notes=scoped,
                        source_location="amocrm_api:events_fallback_notes_by_lead",
                    )

        deduped = deduplicate_calls(list(calls_by_key.values()))
        audit["source_mode"] = "call_first" if deduped else "deal_first_fallback"
        audit["calls_seen_from_global_source"] = len(deduped)
        audit["calls_missing_deal_id"] = int(missing_deal_id)
        audit["contact_calls_seen"] = int(contact_calls_seen)
        audit["contact_calls_linked_to_deal"] = int(contact_calls_linked_to_deal)
        audit["contact_calls_without_deal"] = int(contact_calls_without_deal)
        audit["contact_calls_written_as_contact_only"] = int(
            sum(1 for call in deduped if str(getattr(call, "entity_type", "lead") or "lead").strip().lower() == "contact_only")
        )
        audit["contact_call_resolution_debug"] = contact_call_resolution_debug
        return deduped, audit

    def _resolve_call_audio(self, call: CallEvidence, *, audio_call_ids: set[str] | None = None) -> CallEvidence:
        if isinstance(audio_call_ids, set) and audio_call_ids and str(call.call_id or "").strip() not in audio_call_ids:
            return replace(
                call,
                audio_download_status="not_selected_for_transcription",
                audio_download_error="",
            )
        if isinstance(audio_call_ids, set) and not audio_call_ids:
            return replace(
                call,
                audio_download_status="not_selected_for_transcription",
                audio_download_error="",
            )
        local_audio_path = str(call.audio_path or "").strip()
        if local_audio_path:
            local_path = Path(local_audio_path)
            if local_path.exists() and local_path.is_file():
                return replace(
                    call,
                    audio_path=str(local_path),
                    audio_download_status="local_exists",
                    audio_download_error="",
                )

        source_url = self._pick_recording_url(call)
        if not source_url:
            return replace(
                call,
                audio_path="",
                audio_source_url="",
                audio_download_status="missing_url",
                audio_download_error="",
            )

        if source_url.startswith("file://"):
            local_file = Path(source_url[7:])
            if local_file.exists() and local_file.is_file():
                return replace(
                    call,
                    audio_path=str(local_file),
                    audio_source_url=source_url,
                    audio_download_status="resolved_file_url",
                    audio_download_error="",
                )
            return replace(
                call,
                audio_path="",
                audio_source_url=source_url,
                audio_download_status="failed",
                audio_download_error="file_url_not_found",
            )

        target_path = self._build_target_audio_path(call=call, source_url=source_url)
        if target_path.exists() and target_path.is_file():
            return replace(
                call,
                audio_path=str(target_path),
                audio_source_url=source_url,
                audio_download_status="cached",
                audio_download_error="",
            )

        try:
            self._download_file(source_url=source_url, target_path=target_path)
            return replace(
                call,
                audio_path=str(target_path),
                audio_source_url=source_url,
                audio_download_status="downloaded",
                audio_download_error="",
            )
        except Exception as exc:
            self.logger.warning(
                "call audio download failed: deal_id=%s call_id=%s url=%s error=%s",
                call.deal_id,
                call.call_id,
                source_url,
                exc,
            )
            return replace(
                call,
                audio_path="",
                audio_source_url=source_url,
                audio_download_status="failed",
                audio_download_error=str(exc),
            )

    def _collect_from_api(self, *, deal_id: str) -> tuple[list[CallEvidence], str | None]:
        if not deal_id.isdigit():
            return [], "invalid_deal_id_for_api_calls"

        base_domain = str(getattr(self.config, "call_base_domain", "") or "").strip()
        if not base_domain:
            try:
                auth_cfg = load_amocrm_auth_config(getattr(self.config, "amocrm_auth_config_path", None))
                base_domain = auth_cfg.base_domain
                state = load_auth_state(auth_cfg.state_path)
                token = state.access_token
            except Exception as exc:
                return [], f"auth_config_load_failed:{exc}"
        else:
            try:
                auth_cfg = load_amocrm_auth_config(getattr(self.config, "amocrm_auth_config_path", None))
                state = load_auth_state(auth_cfg.state_path)
                token = state.access_token
            except Exception as exc:
                return [], f"auth_state_load_failed:{exc}"

        if not token:
            return [], "missing_access_token"
        if not base_domain:
            return [], "missing_base_domain"

        try:
            client = AmoCollectorClient(base_domain=base_domain, access_token=token)
            notes = client.get_notes_by_lead(int(deal_id))
            users_cache = client.get_users_cache()
            calls = extract_calls_from_notes(
                notes=notes,
                deal_id=deal_id,
                users_cache=users_cache,
                source_location="amocrm_api:notes",
            )
            return calls, None
        except Exception as exc:
            return [], f"api_call_collection_failed:{exc}"

    def _make_api_client(self) -> tuple[AmoCollectorClient | None, str, str, str]:
        base_domain = str(getattr(self.config, "call_base_domain", "") or "").strip()
        token = ""
        auth_error = ""
        if not base_domain:
            try:
                auth_cfg = load_amocrm_auth_config(getattr(self.config, "amocrm_auth_config_path", None))
                base_domain = auth_cfg.base_domain
                state = load_auth_state(auth_cfg.state_path)
                token = str(state.access_token or "")
            except Exception as exc:
                auth_error = f"auth_config_load_failed:{exc}"
        else:
            try:
                auth_cfg = load_amocrm_auth_config(getattr(self.config, "amocrm_auth_config_path", None))
                state = load_auth_state(auth_cfg.state_path)
                token = str(state.access_token or "")
            except Exception as exc:
                auth_error = f"auth_state_load_failed:{exc}"
        if not base_domain:
            return None, "", token, auth_error or "missing_base_domain"
        if not token:
            return None, base_domain, "", auth_error or "missing_access_token"
        try:
            client = AmoCollectorClient(base_domain=base_domain, access_token=token)
            return client, base_domain, token, auth_error
        except Exception as exc:
            return None, base_domain, token, f"client_init_failed:{exc}"

    @staticmethod
    def _period_unix_bounds(*, period_start: date, period_end: date) -> tuple[int, int]:
        # Cover period edges and business-window overlap from previous workday 15:00 MSK.
        start_day = period_start
        while start_day.weekday() >= 5:
            start_day = start_day - timedelta(days=1)
        prev_day = start_day - timedelta(days=1)
        while prev_day.weekday() >= 5:
            prev_day = prev_day - timedelta(days=1)
        start_msk = datetime.combine(prev_day, time(15, 0, 0), tzinfo=timezone(timedelta(hours=3)))
        end_msk = datetime.combine(period_end, time(23, 59, 59), tzinfo=timezone(timedelta(hours=3)))
        return int(start_msk.timestamp()), int(end_msk.timestamp())

    @staticmethod
    def _resolve_note_deal_id(note: dict[str, Any]) -> str:
        for key in ("entity_id", "lead_id", "element_id"):
            value = note.get(key)
            if isinstance(value, int):
                return str(value)
            text = str(value or "").strip()
            if text.isdigit():
                return text
        params = note.get("params") if isinstance(note.get("params"), dict) else {}
        for key in ("lead_id", "entity_id", "lead", "deal_id"):
            value = params.get(key)
            if isinstance(value, int):
                return str(value)
            text = str(value or "").strip()
            if text.isdigit():
                return text
        return ""

    @staticmethod
    def _resolve_note_contact_id(note: dict[str, Any]) -> str:
        for key in ("entity_id", "contact_id", "element_id"):
            value = note.get(key)
            if isinstance(value, int):
                return str(value)
            text = str(value or "").strip()
            if text.isdigit():
                return text
        params = note.get("params") if isinstance(note.get("params"), dict) else {}
        for key in ("contact_id", "entity_id", "contact"):
            value = params.get(key)
            if isinstance(value, int):
                return str(value)
            text = str(value or "").strip()
            if text.isdigit():
                return text
        return ""

    @staticmethod
    def _is_probably_open_lead_status(status_id: int) -> bool:
        if status_id <= 0:
            return False
        # Common amoCRM terminal statuses.
        if status_id in {142, 143}:
            return False
        return True

    @staticmethod
    def _resolve_event_deal_id(event: dict[str, Any]) -> int | None:
        for key in ("entity_id", "lead_id", "object_id", "entity"):
            value = event.get(key)
            if isinstance(value, int) and value > 0:
                return value
            text = str(value or "").strip()
            if text.isdigit():
                parsed = int(text)
                if parsed > 0:
                    return parsed
        for container_key in ("value_after", "value_before", "created_by", "custom_fields_values"):
            node = event.get(container_key)
            if isinstance(node, dict):
                for key in ("lead_id", "entity_id", "deal_id", "id"):
                    value = node.get(key)
                    if isinstance(value, int) and value > 0:
                        return value
                    text = str(value or "").strip()
                    if text.isdigit():
                        parsed = int(text)
                        if parsed > 0:
                            return parsed
            if isinstance(node, list):
                for item in node:
                    if not isinstance(item, dict):
                        continue
                    for key in ("lead_id", "entity_id", "deal_id", "id"):
                        value = item.get(key)
                        if isinstance(value, int) and value > 0:
                            return value
                        text = str(value or "").strip()
                        if text.isdigit():
                            parsed = int(text)
                            if parsed > 0:
                                return parsed
        return None

    @staticmethod
    def _call_dedup_key(call: CallEvidence) -> str:
        if str(call.call_id or "").strip():
            return f"id:{str(call.call_id).strip()}"
        if str(call.recording_url or "").strip():
            return f"rec:{str(call.recording_url).strip().lower()}"
        return f"mix:{call.deal_id}:{call.timestamp}:{call.duration_seconds}:{call.direction}"

    def _pick_recording_url(self, call: CallEvidence) -> str:
        return str(call.recording_url or "").strip()

    def _build_target_audio_path(self, *, call: CallEvidence, source_url: str) -> Path:
        parsed = urlparse(source_url)
        suffix = Path(parsed.path).suffix.lower()
        if not suffix or len(suffix) > 8:
            suffix = ".mp3"
        raw_hash = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:12]
        safe_deal = str(call.deal_id or "deal").strip() or "deal"
        safe_call = str(call.call_id or "call").strip() or "call"
        filename = f"deal_{safe_deal}__call_{safe_call}__{raw_hash}{suffix}"
        return self.audio_cache_dir / filename

    def _download_file(self, *, source_url: str, target_path: Path) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        headers = {"User-Agent": "amocrm-bot/deal-analyzer"}
        maybe_token = self._resolve_token_for_url(source_url)
        if maybe_token:
            headers["Authorization"] = f"Bearer {maybe_token}"
        request = Request(source_url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=30) as response:
                data = response.read()
        except URLError as exc:
            raise RuntimeError(f"download_request_failed:{exc}") from exc
        if not data:
            raise RuntimeError("download_empty_body")
        target_path.write_bytes(data)

    def _resolve_token_for_url(self, source_url: str) -> str | None:
        if self._auth_header_token is not None:
            return self._auth_header_token
        url_host = (urlparse(source_url).hostname or "").lower()
        if not url_host:
            self._auth_header_token = ""
            return None
        try:
            auth_cfg = load_amocrm_auth_config(getattr(self.config, "amocrm_auth_config_path", None))
            state = load_auth_state(auth_cfg.state_path)
            domain_host = (urlparse(auth_cfg.base_domain).hostname or "").lower()
            if domain_host and (url_host == domain_host or url_host.endswith(f".{domain_host}")):
                self._auth_header_token = str(state.access_token or "")
            else:
                self._auth_header_token = ""
        except Exception:
            self._auth_header_token = ""
        return self._auth_header_token or None
