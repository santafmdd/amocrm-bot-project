from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "transcript_text": self.transcript_text,
            "transcript_status": self.transcript_status,
            "transcript_backend": self.transcript_backend,
            "transcript_language": self.transcript_language,
            "transcript_confidence": self.transcript_confidence,
            "transcript_created_at": self.transcript_created_at,
            "transcript_cache_key": self.transcript_cache_key,
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


def create_transcription_backend(*, backend_name: str, logger) -> TranscriptionBackend:
    name = str(backend_name or "disabled").strip().lower()
    if name == "disabled":
        return DisabledTranscriptionBackend()
    if name == "mock":
        return MockTranscriptionBackend()
    if name == "local_placeholder":
        return LocalPlaceholderTranscriptionBackend()
    if name == "cloud_placeholder":
        return CloudPlaceholderTranscriptionBackend()

    logger.warning("unknown transcription backend '%s', fallback to disabled", name)
    return DisabledTranscriptionBackend()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
