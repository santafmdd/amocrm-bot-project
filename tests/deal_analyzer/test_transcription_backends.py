from src.deal_analyzer.transcription_backends import (
    DisabledTranscriptionBackend,
    FasterWhisperTranscriptionBackend,
    MockTranscriptionBackend,
    create_transcription_backend,
)


class _Logger:
    def warning(self, *_args, **_kwargs):
        return None


def test_backend_routing_disabled_and_mock():
    logger = _Logger()
    b1 = create_transcription_backend(backend_name="disabled", logger=logger, config=None)
    b2 = create_transcription_backend(backend_name="mock", logger=logger, config=None)
    assert isinstance(b1, DisabledTranscriptionBackend)
    assert isinstance(b2, MockTranscriptionBackend)


def test_backend_routing_unknown_fallbacks_to_disabled():
    logger = _Logger()
    backend = create_transcription_backend(backend_name="unknown_backend", logger=logger, config=None)
    assert isinstance(backend, DisabledTranscriptionBackend)


def test_backend_routing_faster_whisper():
    logger = _Logger()
    cfg = type(
        "Cfg",
        (),
        {
            "whisper_model_name": "whisper-large-v3-turbo",
            "transcription_model": "",
            "whisper_device": "cpu",
            "whisper_compute_type": "int8",
            "transcription_language": "ru",
        },
    )()
    backend = create_transcription_backend(backend_name="faster_whisper", logger=logger, config=cfg)
    assert isinstance(backend, FasterWhisperTranscriptionBackend)
    assert backend.model_name == "whisper-large-v3-turbo"
