from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from typing import Any

from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.state_store import load_auth_state
from src.amocrm_collector.client import AmoCollectorClient
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
