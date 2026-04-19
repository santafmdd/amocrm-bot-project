import json
import shutil
from pathlib import Path
from unittest.mock import patch

from src.config import load_config
from src.deal_analyzer.cli import _run_analyze_weekly
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


def _fresh_output_dir(name: str) -> Path:
    app = load_config()
    root = app.project_root / "workspace" / "tmp_tests" / "deal_analyzer" / name
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_analyze_weekly_creates_required_artifacts():
    output_dir = _fresh_output_dir("weekly_artifacts")
    payload = {
        "normalized_deals": [
            {"deal_id": 1, "deal_name": "R Deal", "responsible_user_name": "Рустам Хомидов", "status_name": "В работе", "pipeline_name": "Привлечение"},
            {"deal_id": 2, "deal_name": "I Deal", "responsible_user_name": "Илья Бочков", "status_name": "Закрыто и не реализовано", "pipeline_name": "Привлечение"},
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-19T10:00:00+00:00",
            "crm": dict(normalized_deal),
            "warnings": [],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [],
            "roks_context": {"ok": True},
        }

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        deal_id = int(normalized.get("deal_id"))
        return (
            {
                "deal_id": deal_id,
                "amo_lead_id": deal_id,
                "deal_name": normalized.get("deal_name", ""),
                "score_0_100": 70 if deal_id == 1 else 25,
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": ["process_hygiene: missing follow-up"] if deal_id == 1 else ["qualified_loss: market mismatch"],
                "presentation_quality_flag": "ok",
                "followup_quality_flag": "needs_attention",
                "data_completeness_flag": "partial",
                "recommended_actions_for_manager": [],
                "recommended_training_tasks_for_employee": [],
                "manager_message_draft": "",
                "employee_training_message_draft": "",
                "analysis_backend_requested": "rules",
                "analysis_backend_used": "rules",
                "llm_repair_applied": False,
                "backend": "rules",
                "analysis_confidence": "high",
                "crm_hygiene_confidence": "high",
                "owner_ambiguity_flag": False,
                "manager_summary": "summary",
                "manager_insight_short": "insight",
                "reanimation_potential": "none" if deal_id == 1 else "low",
                "reanimation_reason_short": "reason",
                "product_hypothesis": "unknown",
                "product_hypothesis_confidence": "low",
            },
            {"llm_success_count": 0, "llm_success_repaired_count": 0, "llm_fallback_count": 0, "llm_error_count": 0},
        )

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation", side_effect=_fake_analyze
    ):
        _run_analyze_weekly(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            logger,
            week_start="2026-04-13",
            week_end="2026-04-19",
            limit=None,
            manager_contains=None,
            discussion_limit=10,
        )

    weekly_root = output_dir / "weekly_runs"
    runs = [p for p in weekly_root.iterdir() if p.is_dir()]
    assert len(runs) == 1
    run_dir = runs[0]
    assert (run_dir / "rustam_weekly.md").exists()
    assert (run_dir / "ilya_weekly.md").exists()
    assert (run_dir / "weekly_meeting_brief.md").exists()
    assert (run_dir / "next_week_plan.md").exists()
    assert (run_dir / "summary.json").exists()
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_deals_seen"] == 2
    assert summary["total_deals_analyzed"] == 2
    assert "output_files" in summary


def test_analyze_weekly_manager_filter_applies_to_summary_counts():
    output_dir = _fresh_output_dir("weekly_filter")
    payload = {
        "normalized_deals": [
            {"deal_id": 1, "deal_name": "R Deal", "responsible_user_name": "Рустам Хомидов", "status_name": "В работе"},
            {"deal_id": 2, "deal_name": "I Deal", "responsible_user_name": "Илья Бочков", "status_name": "В работе"},
        ]
    }
    logger = _Logger()

    with patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: {"crm": dict(kw["normalized_deal"]), "warnings": []},
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: (
            {
                "deal_id": int(normalized["deal_id"]),
                "amo_lead_id": int(normalized["deal_id"]),
                "deal_name": normalized.get("deal_name", ""),
                "score_0_100": 40,
                "risk_flags": ["process_hygiene: missing follow-up"],
                "analysis_confidence": "high",
                "owner_ambiguity_flag": False,
                "crm_hygiene_confidence": "high",
                "analysis_backend_used": "rules",
            },
            {"llm_success_count": 0, "llm_success_repaired_count": 0, "llm_fallback_count": 0, "llm_error_count": 0},
        ),
    ):
        _run_analyze_weekly(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            logger,
            week_start=None,
            week_end=None,
            limit=None,
            manager_contains="рустам",
            discussion_limit=10,
        )

    run_dir = next((output_dir / "weekly_runs").iterdir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["total_deals_seen"] == 2
    assert summary["total_deals_analyzed"] == 1
    assert summary["rustam_deals"] == 1
    assert summary["ilya_deals"] == 0


def test_analyze_weekly_mentions_confidence_limits_in_outputs():
    output_dir = _fresh_output_dir("weekly_limits")
    payload = {"normalized_deals": [{"deal_id": 1, "deal_name": "R Deal", "responsible_user_name": "Рустам Хомидов", "status_name": "Закрыто и не реализовано"}]}
    logger = _Logger()

    with patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: {"crm": dict(kw["normalized_deal"]), "warnings": []},
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: (
            {
                "deal_id": int(normalized["deal_id"]),
                "amo_lead_id": int(normalized["deal_id"]),
                "deal_name": normalized.get("deal_name", ""),
                "score_0_100": 20,
                "risk_flags": ["evidence_context: missing notes"],
                "analysis_confidence": "low",
                "owner_ambiguity_flag": True,
                "crm_hygiene_confidence": "low",
                "analysis_backend_used": "rules",
            },
            {"llm_success_count": 0, "llm_success_repaired_count": 0, "llm_fallback_count": 0, "llm_error_count": 0},
        ),
    ):
        _run_analyze_weekly(
            _cfg(),
            output_dir,
            payload,
            "period.json",
            logger,
            week_start=None,
            week_end=None,
            limit=None,
            manager_contains=None,
            discussion_limit=10,
        )

    run_dir = next((output_dir / "weekly_runs").iterdir())
    rustam_md = (run_dir / "rustam_weekly.md").read_text(encoding="utf-8").lower()
    brief_md = (run_dir / "weekly_meeting_brief.md").read_text(encoding="utf-8").lower()
    assert "нельзя трактовать слишком уверенно" in rustam_md
    assert "что нельзя интерпретировать уверенно" in brief_md
