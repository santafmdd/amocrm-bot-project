import sys
from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.cli import _parse_args, _run_analyze_snapshot
from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.exporters import ExportFileSet


class _Logger:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, msg, *args):
        self.infos.append(msg % args if args else str(msg))

    def warning(self, msg, *args):
        self.warnings.append(msg % args if args else str(msg))


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
    )


def test_cli_parses_analyze_snapshot_command():
    argv = [
        "prog",
        "--config",
        "config/deal_analyzer.local.json",
        "analyze-snapshot",
        "--input",
        "workspace/deal_analyzer/call_snapshot_deal_latest.json",
        "--deal-id",
        "31913530",
    ]
    with patch.object(sys, "argv", argv):
        args = _parse_args()
    assert args.command == "analyze-snapshot"
    assert args.deal_id == "31913530"


def test_run_analyze_snapshot_saves_json_artifact_and_handles_snapshot_warnings():
    cfg = _cfg()
    logger = _Logger()
    payload = {
        "snapshot": {
            "snapshot_generated_at": "2026-04-18T10:00:00+00:00",
            "crm": {"deal_id": 101, "amo_lead_id": 101, "deal_name": "Deal 101"},
            "warnings": ["enrichment_failed:test"],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [],
            "roks_context": {"ok": False},
        }
    }
    captured: dict = {}

    def _capture_write_json_export(*, output_dir, name, payload, write_latest):
        captured["name"] = name
        captured["payload"] = payload
        return ExportFileSet(
            timestamped=Path("workspace/deal_analyzer/analyze_snapshot_20260418_100000.json"),
            latest=Path("workspace/deal_analyzer/analyze_snapshot_latest.json"),
        )

    with patch("src.deal_analyzer.cli.write_json_export", side_effect=_capture_write_json_export):
        _run_analyze_snapshot(
            cfg,
            Path("workspace/deal_analyzer"),
            payload,
            "call_snapshot_deal_latest.json",
            True,
            logger,
            deal_id="",
        )

    assert captured["name"] == "analyze_snapshot"
    out = captured["payload"]
    assert out["command"] == "analyze-snapshot"
    assert out["snapshot"]["warnings"] == ["enrichment_failed:test"]
    analysis = out["analysis"]
    required = {
        "deal_id",
        "amo_lead_id",
        "deal_name",
        "score_0_100",
        "strong_sides",
        "growth_zones",
        "risk_flags",
        "recommended_actions_for_manager",
        "recommended_training_tasks_for_employee",
        "manager_message_draft",
        "employee_training_message_draft",
    }
    assert required.issubset(set(analysis.keys()))
