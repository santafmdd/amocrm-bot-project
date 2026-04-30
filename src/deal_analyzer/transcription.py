from __future__ import annotations

from typing import Any

from src.config import load_config

from .transcript_cache import TranscriptCache
from .transcription_backends import TranscriptArtifact, create_transcription_backend

_MIN_USABLE_TRANSCRIPT_CHARS = 20


def transcribe_call_evidence(
    *,
    calls: list[dict[str, Any]],
    config,
    logger,
) -> list[dict[str, Any]]:
    app_cfg = load_config()
    cache_hint = str(
        getattr(config, "transcription_cache_dir", "workspace/deal_analyzer/transcripts_cache")
        or "workspace/deal_analyzer/transcripts_cache"
    )
    cache_dir = (app_cfg.project_root / cache_hint).resolve()
    cache = TranscriptCache(cache_dir=cache_dir)
    backend = create_transcription_backend(
        backend_name=getattr(config, "transcription_backend", "disabled"),
        logger=logger,
        config=config,
    )

    out: list[dict[str, Any]] = []
    for call in calls:
        key = cache.make_key(call)
        cached = cache.get(key)
        if cached:
            item = dict(cached)
            _normalize_transcript_payload(item=item, call=call, cache_key=key)
            item["transcript_cache_hit"] = True
            status = str(item.get("transcript_status") or "").strip().lower()
            if bool(item.get("transcript_usable")) and status in {"ok", "cached"}:
                item["transcript_status"] = "cached"
                out.append(item)
                continue

            retry = _transcribe_once(backend=backend, call=call, cache_key=key, logger=logger)
            if retry is not None:
                retry["transcript_retry_attempted"] = True
                retry["transcript_retry_reason"] = "cached_transcript_unusable"
                retry["transcript_cache_hit"] = True
                retry["transcript_cache_ignored"] = True
                if bool(retry.get("transcript_usable")):
                    cache.set(key, retry)
                    out.append(retry)
                    continue
                retry["transcript_status"] = "empty_transcript_after_stt"
                if not str(retry.get("transcript_error") or "").strip():
                    retry["transcript_error"] = "empty_transcript_after_retry"
                cache.set(key, retry)
                out.append(retry)
                continue

            item["transcript_retry_attempted"] = True
            item["transcript_retry_reason"] = "cached_transcript_unusable_retry_failed"
            item["transcript_status"] = "empty_transcript_after_stt"
            if not str(item.get("transcript_error") or "").strip():
                item["transcript_error"] = "cached_transcript_unusable_and_retry_failed"
            out.append(item)
            continue

        logger.info(
            "transcription attempted: backend=%s model=%s deal=%s call=%s source=%s",
            getattr(backend, "name", ""),
            getattr(backend, "model_name", ""),
            call.get("deal_id", ""),
            call.get("call_id", ""),
            call.get("recording_url", "") or call.get("audio_path", "") or "",
        )

        payload = _transcribe_once(backend=backend, call=call, cache_key=key, logger=logger)
        if payload is None:
            fallback: dict[str, Any] = {
                "call_id": call.get("call_id", ""),
                "deal_id": call.get("deal_id", ""),
                "transcript_text": "",
                "transcript_status": "backend_error",
                "transcript_backend": getattr(backend, "name", ""),
                "transcript_language": "",
                "transcript_confidence": None,
                "transcript_created_at": "",
                "transcript_cache_key": key,
                "transcript_duration_sec": None,
                "transcript_segments": [],
                "transcript_source": "",
                "transcript_error": "backend_exception",
            }
            _normalize_transcript_payload(item=fallback, call=call, cache_key=key)
            out.append(fallback)
            continue

        if (
            not bool(payload.get("transcript_usable"))
            and str(payload.get("transcript_status") or "").strip().lower() in {"ok", "cached", "empty_transcript"}
        ):
            retry = _transcribe_once(backend=backend, call=call, cache_key=key, logger=logger)
            payload["transcript_retry_attempted"] = True
            payload["transcript_retry_reason"] = "empty_transcript_after_stt"
            if retry is not None:
                retry["transcript_retry_attempted"] = True
                retry["transcript_retry_reason"] = "empty_transcript_after_stt"
                payload = retry
            if not bool(payload.get("transcript_usable")):
                payload["transcript_status"] = "empty_transcript_after_stt"
                if not str(payload.get("transcript_error") or "").strip():
                    payload["transcript_error"] = "empty_transcript_after_retry"
            _normalize_transcript_payload(item=payload, call=call, cache_key=key)

        if bool(payload.get("transcript_usable")) and str(payload.get("transcript_status") or "") in {"ok", "cached"}:
            logger.info(
                "transcription success: backend=%s model=%s deal=%s call=%s language=%s chars=%s",
                payload.get("transcript_backend", ""),
                getattr(backend, "model_name", ""),
                payload.get("deal_id", ""),
                payload.get("call_id", ""),
                payload.get("transcript_language", ""),
                int(payload.get("transcript_chars", 0) or 0),
            )
        else:
            logger.warning(
                "transcription not usable: backend=%s model=%s deal=%s call=%s status=%s chars=%s error=%s",
                payload.get("transcript_backend", ""),
                getattr(backend, "model_name", ""),
                payload.get("deal_id", ""),
                payload.get("call_id", ""),
                payload.get("transcript_status", ""),
                int(payload.get("transcript_chars", 0) or 0),
                payload.get("transcript_error", ""),
            )

        cache.set(key, payload)
        out.append(payload)

    return out


def _transcribe_once(
    *,
    backend,
    call: dict[str, Any],
    cache_key: str,
    logger,
) -> dict[str, Any] | None:
    artifact: TranscriptArtifact
    try:
        artifact = backend.transcribe(call=call, cache_key=cache_key)
    except Exception as exc:
        logger.warning(
            "transcription backend failed: backend=%s call_id=%s error=%s",
            getattr(backend, "name", ""),
            call.get("call_id", ""),
            exc,
        )
        return None
    payload: dict[str, Any] = {
        "call_id": call.get("call_id", ""),
        "deal_id": call.get("deal_id", ""),
        **artifact.to_dict(),
    }
    _normalize_transcript_payload(item=payload, call=call, cache_key=cache_key)
    return payload


def _normalize_transcript_payload(
    *,
    item: dict[str, Any],
    call: dict[str, Any],
    cache_key: str,
) -> None:
    item.setdefault("transcript_cache_key", cache_key)
    item.setdefault("call_id", call.get("call_id", ""))
    item.setdefault("deal_id", call.get("deal_id", ""))
    text = str(item.get("transcript_text") or "").strip()
    status = str(item.get("transcript_status") or "").strip().lower()
    chars = len(text)
    usable = bool(chars >= _MIN_USABLE_TRANSCRIPT_CHARS and status in {"ok", "cached"})
    item["transcript_chars"] = chars
    item["transcript_usable"] = usable
    if status in {"ok", "cached"} and not usable:
        item["transcript_status"] = "empty_transcript"
        if not str(item.get("transcript_error") or "").strip():
            item["transcript_error"] = "empty_transcript_text"
