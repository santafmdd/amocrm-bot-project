from pathlib import Path

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.prompt_builder import build_ollama_chat_messages


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:e4b",
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
    )


def test_build_ollama_chat_messages_contains_deal_payload_and_contract():
    messages = build_ollama_chat_messages(
        normalized_deal={"deal_id": 1, "deal_name": "Demo"},
        config=_cfg(),
    )
    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert "JSON" in messages[0]["content"]
    assert messages[1]["role"] == "user"
    assert "deal_name" in messages[1]["content"]
