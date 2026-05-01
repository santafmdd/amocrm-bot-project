from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from src.config import load_config
from src.deal_analyzer.transcription import transcribe_call_evidence

from .models import GoogleDriveLink


def transcribe_presentation_link(
    *,
    link: GoogleDriveLink,
    config,
    logger,
) -> dict[str, Any]:
    app_cfg = load_config()
    cache_root = (app_cfg.project_root / "workspace" / "cache" / "presentations").resolve()
    cache_root.mkdir(parents=True, exist_ok=True)
    transcript_cache = cache_root / "transcripts"
    transcript_cache.mkdir(parents=True, exist_ok=True)

    key = _presentation_key(link)
    cached_path = transcript_cache / f"{key}.json"
    if cached_path.exists():
        try:
            payload = json.loads(cached_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                payload["cache_hit"] = True
                return payload
        except Exception:
            pass

    if link.kind not in {"video", "audio", "unknown"}:
        payload = {
            "status": "not_transcribed_non_media",
            "error": "",
            "transcript_text": "",
            "transcript_chars": 0,
            "backend": "",
            "cache_hit": False,
        }
        _write_json(cached_path, payload)
        return payload

    calls = [
        {
            "call_id": f"presentation_{key}",
            "deal_id": "",
            "recording_url": str(link.url),
            "audio_path": "",
            "source_location": "presentation_link",
        }
    ]
    try:
        items = transcribe_call_evidence(calls=calls, config=config, logger=logger)
    except Exception as exc:
        payload = {
            "status": "stt_backend_error",
            "error": str(exc),
            "transcript_text": "",
            "transcript_chars": 0,
            "backend": "",
            "cache_hit": False,
        }
        _write_json(cached_path, payload)
        return payload

    item = items[0] if isinstance(items, list) and items else {}
    text = str(item.get("transcript_text") or "")
    status_raw = str(item.get("transcript_status") or "").strip().lower()
    transcript_chars = int(item.get("transcript_chars", 0) or len(text))
    payload = {
        "status": status_raw or "unknown",
        "error": str(item.get("transcript_error") or ""),
        "transcript_text": text,
        "transcript_chars": transcript_chars,
        "backend": str(item.get("transcript_backend") or ""),
        "cache_hit": False,
    }
    _write_json(cached_path, payload)
    return payload


def _presentation_key(link: GoogleDriveLink) -> str:
    raw = str(link.file_id or link.url or "")
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

