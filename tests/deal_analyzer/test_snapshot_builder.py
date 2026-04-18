from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.snapshot_builder import build_deal_snapshot, build_period_snapshots


def _cfg() -> DealAnalyzerConfig:
    return DealAnalyzerConfig(
        config_path=Path("config/deal_analyzer.local.json"),
        output_dir=Path("workspace/deal_analyzer"),
        score_weights={
            "presentation": 20,
            "brief": 10,
            "demo_result": 10,
            "pain": 10,
            "business_tasks": 10,
            "followup_tasks": 10,
            "product_fit": 15,
            "probability": 5,
            "data_completeness": 10,
        },
        analyzer_backend="rules",
        ollama_base_url="http://127.0.0.1:11434",
        ollama_model="gemma4:e4b",
        ollama_timeout_seconds=60,
        style_profile_name="manager_ru_v1",
        fields_mapping={"client_list": {}, "appointment_list": {}},
    )


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


def test_build_deal_snapshot_contract():
    row = {"deal_id": 1, "amo_lead_id": 1, "deal_name": "Demo", "responsible_user_name": "Илья"}

    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", return_value=[{**row, "enrichment_match_status": "none"}]), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]):
        roks.return_value.to_dict.return_value = {"ok": True}
        snap = build_deal_snapshot(normalized_deal=row, config=_cfg(), logger=_Logger())

    assert "crm" in snap
    assert "enrichment" in snap
    assert "roks_context" in snap
    assert snap["crm"]["deal_id"] == 1


def test_build_period_snapshots_contract_and_manager_split():
    rows = [
        {"deal_id": 1, "responsible_user_name": "Илья", "enrichment_match_status": "partial"},
        {"deal_id": 2, "responsible_user_name": "Рустам", "enrichment_match_status": "none"},
    ]

    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", return_value=rows), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]):
        roks.return_value.to_dict.return_value = {"ok": True}
        snap = build_period_snapshots(normalized_deals=rows, config=_cfg(), logger=_Logger())

    assert snap["deals_total"] == 2
    assert len(snap["items"]) == 2
    assert "roks_team_context" in snap


def test_build_deal_snapshot_survives_enrich_failure():
    row = {"deal_id": 10, "amo_lead_id": 10, "deal_name": "Demo"}
    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", side_effect=RuntimeError("enrich boom")), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]):
        roks.return_value.to_dict.return_value = {"ok": True}
        snap = build_deal_snapshot(normalized_deal=row, config=_cfg(), logger=_Logger())
    assert snap["crm"]["deal_id"] == 10
    assert snap["crm"]["enrichment_match_status"] == "error"
    assert any("enrichment_failed" in w for w in snap["warnings"])


def test_build_deal_snapshot_survives_roks_failure():
    row = {"deal_id": 11, "amo_lead_id": 11, "deal_name": "Demo"}
    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", return_value=[{**row, "enrichment_match_status": "none"}]), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot", side_effect=RuntimeError("roks boom")
    ), patch("src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]):
        snap = build_deal_snapshot(normalized_deal=row, config=_cfg(), logger=_Logger())
    assert snap["roks_context"]["ok"] is False
    assert any("roks_failed" in w for w in snap["warnings"])


def test_build_deal_snapshot_survives_call_collection_failure():
    row = {"deal_id": 12, "amo_lead_id": 12, "deal_name": "Demo"}
    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", return_value=[{**row, "enrichment_match_status": "none"}]), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.CallDownloader") as downloader_cls, patch(
        "src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]
    ):
        roks.return_value.to_dict.return_value = {"ok": True}
        downloader_cls.return_value.collect_deal_calls.side_effect = RuntimeError("call boom")
        snap = build_deal_snapshot(normalized_deal=row, config=_cfg(), logger=_Logger())
    assert snap["call_evidence"]["items"] == []
    assert snap["call_evidence"]["source_used"] == "error"
    assert any("call_collection_failed" in w for w in snap["warnings"])


def test_build_deal_snapshot_survives_transcription_failure():
    row = {"deal_id": 13, "amo_lead_id": 13, "deal_name": "Demo"}
    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", return_value=[{**row, "enrichment_match_status": "none"}]), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.transcribe_call_evidence", side_effect=RuntimeError("tx boom")):
        roks.return_value.to_dict.return_value = {"ok": True}
        snap = build_deal_snapshot(normalized_deal=row, config=_cfg(), logger=_Logger())
    assert snap["transcripts"] == []
    assert any("transcription_failed" in w for w in snap["warnings"])


def test_build_period_snapshots_survives_enrich_failure():
    rows = [{"deal_id": 21, "amo_lead_id": 21, "responsible_user_name": "Илья"}]
    with patch("src.deal_analyzer.snapshot_builder.enrich_rows", side_effect=RuntimeError("enrich boom")), patch(
        "src.deal_analyzer.snapshot_builder.extract_roks_snapshot"
    ) as roks, patch("src.deal_analyzer.snapshot_builder.transcribe_call_evidence", return_value=[]):
        roks.return_value.to_dict.return_value = {"ok": True}
        snap = build_period_snapshots(normalized_deals=rows, config=_cfg(), logger=_Logger())
    assert snap["deals_total"] == 1
    assert snap["items"][0]["crm"]["enrichment_match_status"] == "error"
    assert any("enrichment_failed" in w for w in snap["warnings"])
