from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.snapshot_builder import build_deal_snapshot


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


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
        call_collection_mode="disabled",
        transcription_backend="disabled",
    )


def test_snapshot_includes_call_evidence_and_transcripts_contract():
    cfg = _cfg()
    logger = _Logger()
    deal = {"deal_id": 1001, "amo_lead_id": 1001, "deal_name": "Test", "responsible_user_name": "????"}

    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", return_value=[{**deal, "enrichment_match_status": "none"}]), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.CallDownloader") as downloader_cls, patch(
        "src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]
    ):
        roks.return_value.to_dict.return_value = {"ok": True}
        downloader = downloader_cls.return_value
        downloader.collect_deal_calls.return_value.calls = []
        downloader.collect_deal_calls.return_value.source_used = "disabled"
        downloader.collect_deal_calls.return_value.warnings = ["call_collection_disabled"]

        snapshot = build_deal_snapshot(normalized_deal=deal, config=cfg, logger=logger)

    assert "call_evidence" in snapshot
    assert "transcripts" in snapshot
    assert "call_derived_summary" in snapshot
    assert isinstance(snapshot["call_evidence"]["items"], list)
