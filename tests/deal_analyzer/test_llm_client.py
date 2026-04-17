import json
from unittest.mock import patch

import pytest

from src.deal_analyzer.llm_client import (
    OllamaClient,
    OllamaClientError,
    ParsedJsonResponse,
    parse_json_response,
)


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_parse_json_response_valid_json():
    parsed = parse_json_response('{"score_0_100": 77}')
    assert isinstance(parsed, ParsedJsonResponse)
    assert parsed.payload["score_0_100"] == 77
    assert parsed.repair_applied is False


def test_parse_json_response_extracts_json_with_code_fence():
    text = "```json\n{\"score_0_100\": 55}\n```"
    parsed = parse_json_response(text)
    assert parsed.payload["score_0_100"] == 55
    assert parsed.repair_applied is True


def test_parse_json_response_extracts_trimmed_json_with_noise_before_after():
    text = "note: draft\n{\"score_0_100\": 65}\nend"
    parsed = parse_json_response(text)
    assert parsed.payload["score_0_100"] == 65
    assert parsed.repair_applied is True


def test_ollama_client_chat_json_success():
    envelope = {"message": {"content": json.dumps({"score_0_100": 77})}}
    client = OllamaClient(base_url="http://127.0.0.1:11434", model="gemma4:e4b", timeout_seconds=10)

    with patch("src.deal_analyzer.llm_client.urlopen", return_value=_FakeResponse(envelope)):
        out = client.chat_json(messages=[{"role": "user", "content": "x"}])

    assert out.payload["score_0_100"] == 77
    assert out.repair_applied is False


def test_ollama_client_raises_on_invalid_content_json():
    envelope = {"message": {"content": "not-json"}}
    client = OllamaClient(base_url="http://127.0.0.1:11434", model="gemma4:e4b", timeout_seconds=10)

    with patch("src.deal_analyzer.llm_client.urlopen", return_value=_FakeResponse(envelope)):
        with pytest.raises(OllamaClientError, match="not valid JSON object"):
            client.chat_json(messages=[{"role": "user", "content": "x"}])


def test_ollama_preflight_success():
    envelope = {"message": {"content": "{}"}}
    client = OllamaClient(base_url="http://127.0.0.1:11434", model="gemma4:e4b", timeout_seconds=10)

    with patch("src.deal_analyzer.llm_client.urlopen", return_value=_FakeResponse(envelope)):
        result = client.preflight(probe_timeout_seconds=4)

    assert result.ok is True
    assert result.error is None


def test_ollama_preflight_fail_on_invalid_content():
    envelope = {"message": {"content": "broken"}}
    client = OllamaClient(base_url="http://127.0.0.1:11434", model="gemma4:e4b", timeout_seconds=10)

    with patch("src.deal_analyzer.llm_client.urlopen", return_value=_FakeResponse(envelope)):
        result = client.preflight(probe_timeout_seconds=4)

    assert result.ok is False
    assert "not valid JSON" in (result.error or "")
