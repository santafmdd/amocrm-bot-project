from __future__ import annotations

import types
import shutil
from pathlib import Path

from src.deal_analyzer.transcription_backends import FasterWhisperTranscriptionBackend
from src.config import load_config


class _Seg:
    def __init__(self, start: float, end: float, text: str) -> None:
        self.start = start
        self.end = end
        self.text = text


class _Info:
    def __init__(self) -> None:
        self.language = "ru"
        self.language_probability = 0.88
        self.duration = 12.5


class _FakeWhisperModel:
    def __init__(self, model_name: str, device: str, compute_type: str) -> None:
        self.model_name = model_name
        self.device = device
        self.compute_type = compute_type

    def transcribe(self, audio: str, language=None, vad_filter=True, beam_size=1):
        assert audio
        return iter([_Seg(0.0, 1.1, "Привет"), _Seg(1.1, 2.4, "Обсудили демо")]), _Info()


def _test_dir(name: str) -> Path:
    app = load_config()
    root = app.project_root / "workspace" / "tmp_tests" / "deal_analyzer" / name
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_faster_whisper_backend_missing_audio_returns_controlled_status():
    backend = FasterWhisperTranscriptionBackend(
        logger=None,
        model_name="whisper-large-v3-turbo",
        device="cpu",
        compute_type="int8",
        language="ru",
    )
    artifact = backend.transcribe(call={"call_id": "c1", "deal_id": "d1"}, cache_key="k1")
    assert artifact.transcript_status == "missing_audio_file"
    assert artifact.transcript_error == "audio_path_not_found"


def test_faster_whisper_backend_success_with_mocked_module(monkeypatch):
    fake_module = types.SimpleNamespace(WhisperModel=_FakeWhisperModel)
    monkeypatch.setitem(__import__("sys").modules, "faster_whisper", fake_module)

    audio = _test_dir("fw_tx_backend") / "call1.wav"
    audio.write_bytes(b"fake")
    backend = FasterWhisperTranscriptionBackend(
        logger=None,
        model_name="whisper-large-v3-turbo",
        device="cpu",
        compute_type="int8",
        language="ru",
    )
    artifact = backend.transcribe(
        call={"call_id": "c1", "deal_id": "d1", "audio_path": str(audio)},
        cache_key="k2",
    )
    assert artifact.transcript_status == "ok"
    assert "Привет" in artifact.transcript_text
    assert artifact.transcript_language == "ru"
    assert artifact.transcript_duration_sec == 12.5
    assert artifact.transcript_source == str(audio)
    assert artifact.transcript_error == ""
    assert isinstance(artifact.transcript_segments, list)
    assert len(artifact.transcript_segments) == 2
