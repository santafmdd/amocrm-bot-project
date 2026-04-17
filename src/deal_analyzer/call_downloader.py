from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.state_store import load_auth_state
from src.amocrm_collector.client import AmoCollectorClient

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

    def collect_deal_calls(self, *, deal: dict[str, Any], raw_bundle: dict[str, Any] | None = None) -> CallCollectionResult:
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
        return CallCollectionResult(deal_id=deal_id, calls=deduped, warnings=warnings, source_used=source_used)

    def collect_period_calls(
        self,
        *,
        deals: list[dict[str, Any]],
        raw_bundles_by_deal: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, CallCollectionResult]:
        out: dict[str, CallCollectionResult] = {}
        raw_bundles_by_deal = raw_bundles_by_deal or {}
        for deal in deals:
            deal_id = str(deal.get("deal_id") or deal.get("amo_lead_id") or "")
            out[deal_id] = self.collect_deal_calls(deal=deal, raw_bundle=raw_bundles_by_deal.get(deal_id))
        return out

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
