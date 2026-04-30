from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.transcription import transcribe_call_evidence


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


class _FailingBackend:
    name = "mock_failing"

    def __init__(self) -> None:
        self.calls = 0

    def transcribe(self, *, call, cache_key):
        self.calls += 1
        raise RuntimeError("backend down")


class _FakeCache:
    def __init__(self, *_, **__):
        self._store = {}

    def make_key(self, call):
        return f"k:{call.get('call_id')}"

    def get(self, key):
        return self._store.get(key)

    def set(self, key, payload):
        self._store[key] = payload


class _Artifact:
    def __init__(self, payload: dict) -> None:
        self._payload = dict(payload)

    def to_dict(self) -> dict:
        return dict(self._payload)


class _EmptyBackend:
    name = "mock_empty"

    def __init__(self) -> None:
        self.calls = 0

    def transcribe(self, *, call, cache_key):
        self.calls += 1
        return _Artifact(
            {
                "transcript_text": "",
                "transcript_status": "ok",
                "transcript_backend": self.name,
                "transcript_language": "ru",
                "transcript_confidence": 0.31,
                "transcript_created_at": "2026-04-27T13:00:00+00:00",
                "transcript_cache_key": cache_key,
                "transcript_duration_sec": None,
                "transcript_segments": [],
                "transcript_source": "audio_path",
                "transcript_error": "",
            }
        )


class _NonEmptyBackend:
    name = "mock_nonempty"

    def __init__(self) -> None:
        self.calls = 0

    def transcribe(self, *, call, cache_key):
        self.calls += 1
        return _Artifact(
            {
                "transcript_text": (
                    "Обсудили текущий процесс клиента и договорились про следующий шаг на этой неделе."
                ),
                "transcript_status": "ok",
                "transcript_backend": self.name,
                "transcript_language": "ru",
                "transcript_confidence": 0.62,
                "transcript_created_at": "2026-04-27T13:00:00+00:00",
                "transcript_cache_key": cache_key,
                "transcript_duration_sec": 160.0,
                "transcript_segments": [{"start": 0.0, "end": 20.0, "text": "Обсудили"}],
                "transcript_source": "audio_path",
                "transcript_error": "",
            }
        )


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="rules",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:e4b",
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
        transcription_backend="mock",
    )


def test_backend_error_is_not_cached_and_backend_is_retried():
    backend = _FailingBackend()
    logger = _Logger()
    calls = [{"call_id": "c1", "deal_id": "d1"}]
    fake_cache = _FakeCache()

    with patch("src.deal_analyzer.transcription.create_transcription_backend", return_value=backend), patch(
        "src.deal_analyzer.transcription.TranscriptCache", return_value=fake_cache
    ), patch(
        "src.deal_analyzer.transcription.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ):
        first = transcribe_call_evidence(calls=calls, config=_cfg(), logger=logger)
        second = transcribe_call_evidence(calls=calls, config=_cfg(), logger=logger)

    assert first[0]["transcript_status"] == "backend_error"
    assert second[0]["transcript_status"] == "backend_error"
    assert backend.calls == 2


def test_empty_transcript_is_retried_and_marked_empty_after_stt() -> None:
    backend = _EmptyBackend()
    logger = _Logger()
    calls = [{"call_id": "c-empty", "deal_id": "d-empty", "audio_path": "x.wav"}]
    fake_cache = _FakeCache()

    with patch("src.deal_analyzer.transcription.create_transcription_backend", return_value=backend), patch(
        "src.deal_analyzer.transcription.TranscriptCache", return_value=fake_cache
    ), patch(
        "src.deal_analyzer.transcription.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ):
        result = transcribe_call_evidence(calls=calls, config=_cfg(), logger=logger)

    assert backend.calls == 2
    item = result[0]
    assert item["transcript_status"] == "empty_transcript_after_stt"
    assert item["transcript_usable"] is False
    assert int(item.get("transcript_chars", 0) or 0) == 0
    assert bool(item.get("transcript_retry_attempted")) is True


def test_cached_disabled_empty_transcript_is_ignored_and_retranscribed() -> None:
    backend = _NonEmptyBackend()
    logger = _Logger()
    calls = [{"call_id": "c-cache", "deal_id": "d-cache", "audio_path": "x.wav"}]
    fake_cache = _FakeCache()
    fake_cache._store["k:c-cache"] = {
        "call_id": "c-cache",
        "deal_id": "d-cache",
        "transcript_text": "",
        "transcript_status": "disabled",
        "transcript_backend": "disabled",
        "transcript_language": "",
        "transcript_confidence": None,
        "transcript_created_at": "2026-04-27T12:00:00+00:00",
        "transcript_cache_key": "k:c-cache",
        "transcript_duration_sec": None,
        "transcript_segments": [],
        "transcript_source": "",
        "transcript_error": "",
    }

    with patch("src.deal_analyzer.transcription.create_transcription_backend", return_value=backend), patch(
        "src.deal_analyzer.transcription.TranscriptCache", return_value=fake_cache
    ), patch(
        "src.deal_analyzer.transcription.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ):
        result = transcribe_call_evidence(calls=calls, config=_cfg(), logger=logger)

    assert backend.calls == 1
    item = result[0]
    assert item["transcript_usable"] is True
    assert int(item.get("transcript_chars", 0) or 0) > 20
    assert bool(item.get("transcript_cache_hit")) is True
    assert bool(item.get("transcript_cache_ignored")) is True
