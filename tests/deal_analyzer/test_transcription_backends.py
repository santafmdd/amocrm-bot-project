from src.deal_analyzer.transcription_backends import (
    DisabledTranscriptionBackend,
    MockTranscriptionBackend,
    create_transcription_backend,
)


class _Logger:
    def warning(self, *_args, **_kwargs):
        return None


def test_backend_routing_disabled_and_mock():
    logger = _Logger()
    b1 = create_transcription_backend(backend_name="disabled", logger=logger)
    b2 = create_transcription_backend(backend_name="mock", logger=logger)
    assert isinstance(b1, DisabledTranscriptionBackend)
    assert isinstance(b2, MockTranscriptionBackend)


def test_backend_routing_unknown_fallbacks_to_disabled():
    logger = _Logger()
    backend = create_transcription_backend(backend_name="unknown_backend", logger=logger)
    assert isinstance(backend, DisabledTranscriptionBackend)
