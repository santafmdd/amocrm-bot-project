from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class CallEvidence:
    call_id: str
    deal_id: str
    manager_id: str
    manager_name: str
    timestamp: str
    duration_seconds: int
    direction: str
    source_location: str
    recording_url: str
    recording_ref: str
    quality_flags: list[str]
    missing_recording: bool
    audio_path: str = ""
    audio_source_url: str = ""
    audio_download_status: str = "not_attempted"
    audio_download_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def extract_calls_from_notes(
    *,
    notes: list[dict[str, Any]],
    deal_id: int | str,
    users_cache: dict[int, dict[str, Any]] | None = None,
    source_location: str = "amocrm_api:notes",
) -> list[CallEvidence]:
    out: list[CallEvidence] = []
    users_cache = users_cache or {}
    deal_text = str(deal_id)

    for note in notes:
        note_type = str(note.get("note_type") or "").strip().lower()
        if note_type not in {"call_in", "call_out"}:
            continue

        params = note.get("params") if isinstance(note.get("params"), dict) else {}
        manager_id = str(note.get("responsible_user_id") or note.get("created_by") or "")
        manager_name = ""
        if manager_id.isdigit():
            user = users_cache.get(int(manager_id), {})
            manager_name = str(user.get("name") or "").strip()

        duration = _safe_int(params.get("duration") or note.get("duration") or 0)
        link = str(params.get("link") or "").strip()
        uniq = str(params.get("uniq") or note.get("id") or "").strip()
        ts_unix = _safe_int(note.get("created_at") or note.get("updated_at") or 0)

        quality_flags: list[str] = []
        if duration <= 0:
            quality_flags.append("zero_duration")
        if not link:
            quality_flags.append("missing_recording")

        out.append(
            CallEvidence(
                call_id=uniq or str(note.get("id") or ""),
                deal_id=deal_text,
                manager_id=manager_id,
                manager_name=manager_name,
                timestamp=_to_iso(ts_unix),
                duration_seconds=max(0, duration),
                direction="inbound" if note_type == "call_in" else "outbound",
                source_location=source_location,
                recording_url=link,
                recording_ref=uniq,
                quality_flags=quality_flags,
                missing_recording=not bool(link),
            )
        )

    return out


def extract_calls_from_normalized_deal(*, deal: dict[str, Any], source_location: str = "normalized_fallback") -> list[CallEvidence]:
    """Fallback evidence path when API/raw bundle notes are unavailable."""
    duration = _safe_int(deal.get("longest_call_duration_seconds") or 0)
    if duration <= 0:
        return []

    reasons = deal.get("presentation_detect_reason") if isinstance(deal.get("presentation_detect_reason"), list) else []
    flags = ["fallback_from_normalized"]
    if not any(str(x).startswith("long_call") for x in reasons):
        flags.append("duration_unverified")

    return [
        CallEvidence(
            call_id=f"fallback-{deal.get('deal_id') or deal.get('amo_lead_id')}",
            deal_id=str(deal.get("deal_id") or deal.get("amo_lead_id") or ""),
            manager_id=str(deal.get("responsible_user_id") or ""),
            manager_name=str(deal.get("responsible_user_name") or ""),
            timestamp="",
            duration_seconds=duration,
            direction="unknown",
            source_location=source_location,
            recording_url="",
            recording_ref="",
            quality_flags=flags,
            missing_recording=True,
        )
    ]


def deduplicate_calls(calls: list[CallEvidence]) -> list[CallEvidence]:
    dedup: dict[str, CallEvidence] = {}
    for call in calls:
        key = _dedup_key(call)
        current = dedup.get(key)
        if current is None:
            dedup[key] = call
            continue
        if _rank(call) > _rank(current):
            dedup[key] = call
    return sorted(dedup.values(), key=lambda c: (c.timestamp, c.call_id))


def build_call_summary(calls: list[CallEvidence]) -> dict[str, Any]:
    inbound = sum(1 for c in calls if c.direction == "inbound")
    outbound = sum(1 for c in calls if c.direction == "outbound")
    missing_recording = sum(1 for c in calls if c.missing_recording)
    longest = max((c.duration_seconds for c in calls), default=0)
    return {
        "calls_total": len(calls),
        "inbound_calls": inbound,
        "outbound_calls": outbound,
        "missing_recording_calls": missing_recording,
        "longest_call_duration_seconds": longest,
        "evidence_ready": bool(calls),
    }


def call_evidence_to_dicts(calls: list[CallEvidence]) -> list[dict[str, Any]]:
    return [call.to_dict() for call in calls]


def _dedup_key(call: CallEvidence) -> str:
    if call.call_id:
        return f"id:{call.call_id}"
    if call.recording_url:
        return f"rec:{call.recording_url.lower()}"
    return f"mix:{call.deal_id}:{call.timestamp}:{call.duration_seconds}:{call.direction}"


def _rank(call: CallEvidence) -> tuple[int, int]:
    return (0 if call.missing_recording else 1, max(0, call.duration_seconds))


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_iso(unix_ts: int) -> str:
    if unix_ts <= 0:
        return ""
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()
