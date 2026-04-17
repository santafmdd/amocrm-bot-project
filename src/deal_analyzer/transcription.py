from __future__ import annotations

from pathlib import Path
from typing import Any

from src.config import load_config

from .transcript_cache import TranscriptCache
from .transcription_backends import TranscriptArtifact, create_transcription_backend


def transcribe_call_evidence(
    *,
    calls: list[dict[str, Any]],
    config,
    logger,
) -> list[dict[str, Any]]:
    app_cfg = load_config()
    cache_hint = str(getattr(config, "transcription_cache_dir", "workspace/deal_analyzer/transcripts_cache") or "workspace/deal_analyzer/transcripts_cache")
    cache_dir = (app_cfg.project_root / cache_hint).resolve()
    cache = TranscriptCache(cache_dir=cache_dir)
    backend = create_transcription_backend(backend_name=getattr(config, "transcription_backend", "disabled"), logger=logger)

    out: list[dict[str, Any]] = []
    for call in calls:
        key = cache.make_key(call)
        cached = cache.get(key)
        if cached:
            item = dict(cached)
            item.setdefault("transcript_cache_key", key)
            item.setdefault("call_id", call.get("call_id", ""))
            item.setdefault("deal_id", call.get("deal_id", ""))
            item["transcript_status"] = "cached"
            out.append(item)
            continue

        artifact: TranscriptArtifact
        try:
            artifact = backend.transcribe(call=call, cache_key=key)
        except Exception as exc:
            logger.warning("transcription backend failed: backend=%s call_id=%s error=%s", backend.name, call.get("call_id", ""), exc)
            fallback = {
                "call_id": call.get("call_id", ""),
                "deal_id": call.get("deal_id", ""),
                "transcript_text": "",
                "transcript_status": "backend_error",
                "transcript_backend": backend.name,
                "transcript_language": "",
                "transcript_confidence": None,
                "transcript_created_at": "",
                "transcript_cache_key": key,
            }
            cache.set(key, fallback)
            out.append(fallback)
            continue

        payload = {
            "call_id": call.get("call_id", ""),
            "deal_id": call.get("deal_id", ""),
            **artifact.to_dict(),
        }
        cache.set(key, payload)
        out.append(payload)

    return out
