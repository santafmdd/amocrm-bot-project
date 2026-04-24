from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class TranscriptArtifact:
    transcript_text: str
    transcript_status: str
    transcript_backend: str
    transcript_language: str
    transcript_confidence: float | None
    transcript_created_at: str
    transcript_cache_key: str
    transcript_duration_sec: float | None = None
    transcript_segments: list[dict[str, Any]] | None = None
    transcript_source: str = ""
    transcript_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "transcript_text": self.transcript_text,
            "transcript_status": self.transcript_status,
            "transcript_backend": self.transcript_backend,
            "transcript_language": self.transcript_language,
            "transcript_confidence": self.transcript_confidence,
            "transcript_created_at": self.transcript_created_at,
            "transcript_cache_key": self.transcript_cache_key,
            "transcript_duration_sec": self.transcript_duration_sec,
            "transcript_segments": self.transcript_segments or [],
            "transcript_source": self.transcript_source,
            "transcript_error": self.transcript_error,
        }


class TranscriptionBackend(Protocol):
    name: str

    def transcribe(self, *, call: dict[str, Any], cache_key: str) -> TranscriptArtifact:
        ...


class DisabledTranscriptionBackend:
    name = "disabled"

    def transcribe(self, *, call: dict[str, Any], cache_key: str) -> TranscriptArtifact:
        return TranscriptArtifact(
            transcript_text="",
            transcript_status="disabled",
            transcript_backend=self.name,
            transcript_language="",
            transcript_confidence=None,
            transcript_created_at=_now_iso(),
            transcript_cache_key=cache_key,
        )


class MockTranscriptionBackend:
    name = "mock"

    def transcribe(self, *, call: dict[str, Any], cache_key: str) -> TranscriptArtifact:
        did = call.get("deal_id") or "-"
        cid = call.get("call_id") or "-"
        duration = int(call.get("duration_seconds") or 0)
        text = f"Mock transcript for deal {did}, call {cid}, duration {duration}s."
        return TranscriptArtifact(
            transcript_text=text,
            transcript_status="ok",
            transcript_backend=self.name,
            transcript_language="ru",
            transcript_confidence=0.99,
            transcript_created_at=_now_iso(),
            transcript_cache_key=cache_key,
        )


class LocalPlaceholderTranscriptionBackend:
    name = "local_placeholder"

    def transcribe(self, *, call: dict[str, Any], cache_key: str) -> TranscriptArtifact:
        if not call.get("recording_url"):
            return TranscriptArtifact(
                transcript_text="",
                transcript_status="missing_recording",
                transcript_backend=self.name,
                transcript_language="",
                transcript_confidence=None,
                transcript_created_at=_now_iso(),
                transcript_cache_key=cache_key,
            )
        return TranscriptArtifact(
            transcript_text="",
            transcript_status="not_implemented",
            transcript_backend=self.name,
            transcript_language="",
            transcript_confidence=None,
            transcript_created_at=_now_iso(),
            transcript_cache_key=cache_key,
        )


class CloudPlaceholderTranscriptionBackend:
    name = "cloud_placeholder"

    def transcribe(self, *, call: dict[str, Any], cache_key: str) -> TranscriptArtifact:
        if not call.get("recording_url"):
            status = "missing_recording"
        else:
            status = "not_configured"
        return TranscriptArtifact(
            transcript_text="",
            transcript_status=status,
            transcript_backend=self.name,
            transcript_language="",
            transcript_confidence=None,
            transcript_created_at=_now_iso(),
            transcript_cache_key=cache_key,
        )


class FasterWhisperTranscriptionBackend:
    name = "faster_whisper"

    def __init__(self, *, logger, model_name: str, device: str, compute_type: str, language: str | None) -> None:
        self.logger = logger
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type
        self.language = language
        self._model: Any | None = None

    def transcribe(self, *, call: dict[str, Any], cache_key: str) -> TranscriptArtifact:
        audio_path = _resolve_call_audio_path(call)
        if audio_path is None:
            return TranscriptArtifact(
                transcript_text="",
                transcript_status="missing_audio_file",
                transcript_backend=self.name,
                transcript_language="",
                transcript_confidence=None,
                transcript_created_at=_now_iso(),
                transcript_cache_key=cache_key,
                transcript_duration_sec=None,
                transcript_segments=[],
                transcript_source="",
                transcript_error="audio_path_not_found",
            )

        try:
            from faster_whisper import WhisperModel  # type: ignore
        except Exception as exc:
            return TranscriptArtifact(
                transcript_text="",
                transcript_status="backend_unavailable",
                transcript_backend=self.name,
                transcript_language="",
                transcript_confidence=None,
                transcript_created_at=_now_iso(),
                transcript_cache_key=cache_key,
                transcript_duration_sec=None,
                transcript_segments=[],
                transcript_source=str(audio_path),
                transcript_error=f"faster_whisper_import_failed:{exc}",
            )

        if self._model is None:
            self._model = WhisperModel(
                self.model_name,
                device=self.device,
                compute_type=self.compute_type,
            )

        segments_iter, info = self._model.transcribe(
            str(audio_path),
            language=self.language or None,
            vad_filter=True,
            beam_size=1,
        )
        segments: list[dict[str, Any]] = []
        texts: list[str] = []
        for seg in segments_iter:
            seg_text = str(getattr(seg, "text", "") or "").strip()
            if seg_text:
                texts.append(seg_text)
            segments.append(
                {
                    "start": float(getattr(seg, "start", 0.0) or 0.0),
                    "end": float(getattr(seg, "end", 0.0) or 0.0),
                    "text": seg_text,
                }
            )
        text = " ".join(texts).strip()
        language = str(getattr(info, "language", "") or "")
        language_prob = getattr(info, "language_probability", None)
        confidence = float(language_prob) if isinstance(language_prob, (int, float)) else None
        duration = getattr(info, "duration", None)
        duration_sec = float(duration) if isinstance(duration, (int, float)) else None
        return TranscriptArtifact(
            transcript_text=text,
            transcript_status="ok" if text else "empty_transcript",
            transcript_backend=self.name,
            transcript_language=language,
            transcript_confidence=confidence,
            transcript_created_at=_now_iso(),
            transcript_cache_key=cache_key,
            transcript_duration_sec=duration_sec,
            transcript_segments=segments,
            transcript_source=str(audio_path),
            transcript_error="" if text else "empty_transcript_text",
        )


def create_transcription_backend(*, backend_name: str, logger, config=None) -> TranscriptionBackend:
    name = str(backend_name or "disabled").strip().lower()
    if name == "disabled":
        return DisabledTranscriptionBackend()
    if name == "mock":
        return MockTranscriptionBackend()
    if name == "local_placeholder":
        return LocalPlaceholderTranscriptionBackend()
    if name == "cloud_placeholder":
        return CloudPlaceholderTranscriptionBackend()
    if name == "faster_whisper":
        try:
            import faster_whisper  # type: ignore # noqa: F401
        except Exception as exc:
            logger.warning(
                "faster-whisper is not installed or unavailable; transcription backend will run with controlled fallback. error=%s",
                exc,
            )
        model_name = str(
            getattr(config, "whisper_model_name", "")
            or getattr(config, "transcription_model", "")
            or "large-v3-turbo"
        ).strip()
        device = _resolve_whisper_device(str(getattr(config, "whisper_device", "auto") or "auto"))
        compute_type = _resolve_whisper_compute_type(
            str(getattr(config, "whisper_compute_type", "auto") or "auto"),
            device=device,
        )
        language_raw = str(getattr(config, "transcription_language", "") or "").strip().lower()
        language = language_raw or None
        return FasterWhisperTranscriptionBackend(
            logger=logger,
            model_name=model_name,
            device=device,
            compute_type=compute_type,
            language=language,
        )

    logger.warning("unknown transcription backend '%s', fallback to disabled", name)
    return DisabledTranscriptionBackend()


def _resolve_call_audio_path(call: dict[str, Any]) -> Path | None:
    for key in ("audio_path", "local_audio_path", "recording_path", "file_path"):
        raw = str(call.get(key) or "").strip()
        if raw:
            candidate = Path(raw).expanduser()
            if candidate.exists() and candidate.is_file():
                return candidate

    recording_url = str(call.get("recording_url") or "").strip()
    if recording_url.startswith("file://"):
        candidate = Path(recording_url[7:])
        if candidate.exists() and candidate.is_file():
            return candidate

    if recording_url and "://" not in recording_url:
        candidate = Path(recording_url).expanduser()
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _resolve_whisper_device(raw_device: str) -> str:
    value = str(raw_device or "auto").strip().lower()
    if value in {"cuda", "cpu"}:
        return value
    try:
        import torch  # type: ignore

        if bool(torch.cuda.is_available()):
            return "cuda"
    except Exception:
        pass
    return "cpu"


def _resolve_whisper_compute_type(raw_compute_type: str, *, device: str) -> str:
    value = str(raw_compute_type or "auto").strip().lower()
    if value != "auto":
        return value
    return "float16" if device == "cuda" else "int8"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
