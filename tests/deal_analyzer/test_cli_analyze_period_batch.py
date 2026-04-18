import json
import shutil
from pathlib import Path
from unittest.mock import patch

from src.config import load_config
from src.deal_analyzer.cli import _run_analyze_period
from src.deal_analyzer.config import DealAnalyzerConfig


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


def _snapshot_for_deal(deal_id: int, *, warnings=None):
    return {
        "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
        "crm": {"deal_id": deal_id, "amo_lead_id": deal_id, "deal_name": f"Deal {deal_id}"},
        "warnings": list(warnings or []),
        "call_evidence": {"items": [], "summary": {"calls_total": 0}},
        "transcripts": [],
        "roks_context": {"ok": True},
    }


def _analysis_for_deal(deal_id: int, *, backend_used="rules", score=50):
    return (
        {
            "deal_id": deal_id,
            "amo_lead_id": deal_id,
            "deal_name": f"Deal {deal_id}",
            "score_0_100": score,
            "strong_sides": [],
            "growth_zones": [],
            "risk_flags": ["risk_a"] if score < 60 else [],
            "presentation_quality_flag": "ok",
            "followup_quality_flag": "ok",
            "data_completeness_flag": "partial",
            "recommended_actions_for_manager": [],
            "recommended_training_tasks_for_employee": [],
            "manager_message_draft": "",
            "employee_training_message_draft": "",
            "analysis_backend_requested": "rules",
            "analysis_backend_used": backend_used,
            "llm_repair_applied": False,
            "backend": "rules",
        },
        {
            "llm_success_count": 0,
            "llm_success_repaired_count": 0,
            "llm_fallback_count": 0,
            "llm_error_count": 0,
        },
    )


def _fresh_output_dir(name: str) -> Path:
    app = load_config()
    root = app.project_root / "workspace" / "tmp_tests" / "deal_analyzer" / name
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_analyze_period_creates_run_dir_and_summary_json():
    output_dir = _fresh_output_dir("period_batch_1")
    payload = {"normalized_deals": [{"deal_id": 1}, {"deal_id": 2}]}
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return _snapshot_for_deal(int(normalized_deal["deal_id"]))

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        return _analysis_for_deal(int(normalized["deal_id"]), score=40 + int(normalized["deal_id"]))

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation", side_effect=_fake_analyze
    ):
        _run_analyze_period(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            True,
            logger,
            period_mode=None,
            date_from=None,
            date_to=None,
            limit=None,
        )

    run_root = output_dir / "period_runs"
    run_dirs = [p for p in run_root.iterdir() if p.is_dir()]
    assert len(run_dirs) == 1
    summary_path = run_dirs[0] / "summary.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["total_deals_seen"] == 2
    assert summary["total_deals_analyzed"] == 2
    assert summary["deals_failed"] == 0
    assert len(summary["artifact_paths"]) == 2


def test_analyze_period_limit_is_applied():
    output_dir = _fresh_output_dir("period_batch_2")
    payload = {"normalized_deals": [{"deal_id": 1}, {"deal_id": 2}, {"deal_id": 3}]}
    logger = _Logger()

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"]))), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"])),
    ):
        _run_analyze_period(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            True,
            logger,
            period_mode=None,
            date_from=None,
            date_to=None,
            limit=1,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_deals_seen"] == 3
    assert summary["total_deals_analyzed"] == 1
    assert summary["limit"] == 1


def test_analyze_period_partial_snapshot_warnings_do_not_fail_batch():
    output_dir = _fresh_output_dir("period_batch_3")
    payload = {"normalized_deals": [{"deal_id": 1}, {"deal_id": 2}]}
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        if int(normalized_deal["deal_id"]) == 2:
            return _snapshot_for_deal(2, warnings=["transcription_failed:test"])
        return _snapshot_for_deal(1)

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"])),
    ):
        _run_analyze_period(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            True,
            logger,
            period_mode=None,
            date_from=None,
            date_to=None,
            limit=None,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    deals_dir = run_dir / "deals"
    artifacts = sorted(deals_dir.glob("deal_*.json"))
    assert len(artifacts) == 2
    second = json.loads(artifacts[1].read_text(encoding="utf-8"))
    assert second["snapshot_warnings"] == ["transcription_failed:test"]
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["deals_failed"] == 0


def test_analyze_period_summary_counts_failed_deals():
    output_dir = _fresh_output_dir("period_batch_4")
    payload = {"normalized_deals": [{"deal_id": 1}, {"deal_id": 2}]}
    logger = _Logger()

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        if int(normalized["deal_id"]) == 2:
            raise RuntimeError("analysis failed")
        return _analysis_for_deal(1, score=80)

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"]))), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation", side_effect=_fake_analyze
    ):
        _run_analyze_period(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            True,
            logger,
            period_mode=None,
            date_from=None,
            date_to=None,
            limit=None,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_deals_seen"] == 2
    assert summary["total_deals_analyzed"] == 1
    assert summary["deals_failed"] == 1
