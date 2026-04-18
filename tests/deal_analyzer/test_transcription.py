from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.transcription import transcribe_call_evidence


class _Logger:
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
