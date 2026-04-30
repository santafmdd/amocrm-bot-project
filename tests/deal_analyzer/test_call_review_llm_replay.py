from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.deal_analyzer.call_review_llm_replay import (
    _build_runtime_with_blocked_models,
    _build_single_pass_messages,
    _experimental_gemma_write_block_reason,
    _filter_selected_items,
    _map_single_pass_payload_to_llm_fields,
    _resolve_call_review_llm_runtime,
    _resolve_effective_limit,
    _strip_markdown_and_think,
)
from src.deal_analyzer.config import DealAnalyzerConfig


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={},
        analyzer_backend="ollama",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:31b-cloud",
        ollama_timeout_seconds=120,
        ollama_fallback_enabled=True,
        ollama_fallback_base_url="http://127.0.0.1:11434",
        ollama_fallback_model="gpt-oss:20b",
        ollama_fallback_timeout_seconds=2400,
    )


def test_resolve_effective_limit_defaults_to_safe_bound_for_dry_run() -> None:
    limit = _resolve_effective_limit(limit_arg=None, write_requested=False, allow_full_run=False)
    assert limit == 5


def test_resolve_effective_limit_blocks_full_without_allow_flag() -> None:
    with pytest.raises(ValueError):
        _resolve_effective_limit(limit_arg=0, write_requested=False, allow_full_run=False)


def test_filter_selected_items_by_deal_offset_and_limit() -> None:
    items = [
        {"deal_id": "1"},
        {"deal_id": "2"},
        {"deal_id": "3"},
        {"deal_id": "4"},
    ]
    filtered = _filter_selected_items(items, deal_ids={"2", "3", "4"}, offset=1, limit=1)
    assert [x["deal_id"] for x in filtered] == ["3"]


def test_build_runtime_with_blocked_models_disables_cloud_main() -> None:
    runtime = {
        "selected": "main",
        "main": {"model": "gemma4:31b-cloud", "enabled": True},
        "fallback": {"model": "gpt-oss:20b", "enabled": True},
        "fallback2": {"model": "deepseek-v3.1:671b-cloud", "enabled": True},
    }
    patched, removed = _build_runtime_with_blocked_models(
        runtime,
        blocked_models={"gemma4:31b-cloud"},
        skip_cloud_on_rate_limit=True,
    )
    assert removed == 1
    assert patched["main"]["enabled"] is False
    assert patched["selected"] == "fallback"


def test_single_pass_payload_mapping_sets_stage_and_phrase_format() -> None:
    payload = {
        "case_summary": "Итог",
        "main_issue": "Проблема",
        "strong_sides": "Сильные стороны",
        "growth_zones": "Зоны роста",
        "what_to_fix": "Что исправить",
        "what_to_tell_employee": "Что донести",
        "better_phrase": "Сначала уточним рамку теста",
        "expected_effect_quantity": "+1-2 шага",
        "expected_effect_quality": "Выше управляемость",
        "quality_score_0_100": 77,
    }
    fields = _map_single_pass_payload_to_llm_fields(payload=payload, case_mode="test_analysis")
    assert fields["stage_test_comment"] == "Итог"
    assert fields["primary_case_type"] == "работа с тестом"
    assert fields["coaching_list"].startswith("1) Что донести")
    assert "Используй:" in fields["coaching_list"]
    assert fields["quality_score_0_100"] == 77


def test_strip_markdown_and_think_cleanup() -> None:
    text = "```json\n<think>hidden</think> Лучше сказать: Текст\n```"
    cleaned = _strip_markdown_and_think(text)
    assert "<think>" not in cleaned
    assert "Лучше сказать:" not in cleaned
    assert "Используй:" in cleaned


def test_resolve_call_review_runtime_uses_call_review_preflight_prefix(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve(**kwargs):
        captured.update(kwargs)
        return {"selected": "fallback", "preflight_results": []}

    monkeypatch.setattr("src.deal_analyzer.call_review_llm_replay.resolve_ollama_runtime", _fake_resolve)
    runtime = _resolve_call_review_llm_runtime(
        _cfg(),
        logger=None,
        main_model_override="gemma4:31b-cloud",
        fallback_model_override="gpt-oss:20b",
        no_retry_on_context_overflow=True,
    )
    assert captured.get("log_prefix") == "call review llm"
    assert runtime.get("selected") == "fallback"
    assert runtime.get("no_retry_on_context_overflow") is True


def test_prompt_size_limiter_trims_large_contexts() -> None:
    record = {
        "deal_id": "1",
        "deal_name": "Deal",
        "owner_name": "Илья",
        "company_name": "Company",
        "status_name": "Status",
        "pipeline_name": "Pipeline",
        "transcript_text_excerpt": "тест " * 20000,
        "call_signal_summary_short": "style " * 2000,
        "crm_consistency_summary": "consistency " * 1000,
        "analysis_confidence": "high",
        "product_hypothesis": "product " * 1000,
        "tags": ["tag1", "tag2"] * 200,
        "company_tags": ["ctag"] * 200,
        "source_values": ["src"] * 200,
    }
    candidate = {"selected_call_count": 2, "selected_call_ids": ["c1", "c2"]}
    messages, meta = _build_single_pass_messages(
        record=record,
        candidate=candidate,
        case_mode="warm_case",
        max_style_chars=1200,
        max_reference_chars=1300,
        max_transcript_chars=2500,
        max_prompt_chars=7000,
    )
    assert meta["style_context_chars_used"] <= 1200
    assert meta["reference_context_chars_used"] <= 1300
    assert meta["transcript_chars_used"] <= 2500
    assert meta["prompt_chars_after_trim"] <= 7000
    assert len(messages) == 2


def test_write_with_gemma_requires_explicit_allow_flag() -> None:
    block = _experimental_gemma_write_block_reason(
        write_requested=True,
        main_model="gemma4:31b-cloud",
        allow_experimental_gemma_write=False,
    )
    assert block == "experimental_gemma_write_requires_explicit_allow_flag"


def test_dry_run_with_gemma_does_not_require_allow_flag() -> None:
    block = _experimental_gemma_write_block_reason(
        write_requested=False,
        main_model="gemma4:31b-cloud",
        allow_experimental_gemma_write=False,
    )
    assert block == ""


def test_production_call_review_configs_have_no_gemma_main() -> None:
    cfg_paths = [
        Path("config/deal_analyzer.call_review.deepseek.realwrite.json"),
        Path("config/deal_analyzer.example.json"),
    ]
    for cfg_path in cfg_paths:
        payload = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
        main_model = str(payload.get("ollama_model") or "").lower()
        assert "gemma" not in main_model, f"unexpected gemma main model in {cfg_path}"
