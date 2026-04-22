import json
import shutil
from pathlib import Path
from types import SimpleNamespace
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
            {"deal_id": 1, "deal_name": "R Deal", "responsible_user_name": "Р СѓСЃС‚Р°Рј РҐРѕРјРёРґРѕРІ", "status_name": "Р’ СЂР°Р±РѕС‚Рµ", "pipeline_name": "РџСЂРёРІР»РµС‡РµРЅРёРµ"},
            {"deal_id": 2, "deal_name": "I Deal", "responsible_user_name": "РР»СЊСЏ Р‘РѕС‡РєРѕРІ", "status_name": "Р—Р°РєСЂС‹С‚Рѕ Рё РЅРµ СЂРµР°Р»РёР·РѕРІР°РЅРѕ", "pipeline_name": "РџСЂРёРІР»РµС‡РµРЅРёРµ"},
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
            {"deal_id": 1, "deal_name": "R Deal", "responsible_user_name": "\u0420\u0443\u0441\u0442\u0430\u043c \u0425\u043e\u043c\u0438\u0434\u043e\u0432", "status_name": "\u0412 \u0440\u0430\u0431\u043e\u0442\u0435"},
            {"deal_id": 2, "deal_name": "I Deal", "responsible_user_name": "\u0418\u043b\u044c\u044f \u0411\u043e\u0447\u043a\u043e\u0432", "status_name": "\u0412 \u0440\u0430\u0431\u043e\u0442\u0435"},
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
            manager_contains="\u0440\u0443\u0441\u0442\u0430\u043c",
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
    payload = {
        "normalized_deals": [
            {
                "deal_id": 1,
                "deal_name": "R Deal",
                "responsible_user_name": "\u0420\u0443\u0441\u0442\u0430\u043c \u0425\u043e\u043c\u0438\u0434\u043e\u0432",
                "status_name": "\u0417\u0430\u043a\u0440\u044b\u0442\u043e \u0438 \u043d\u0435 \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e",
            }
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

def test_analyze_weekly_real_write_uses_header_mapping_and_a2():
    output_dir = _fresh_output_dir("weekly_real_write_header_map")
    payload = {
        "normalized_deals": [
            {"deal_id": 1, "deal_name": "R Deal", "responsible_user_name": "Рустам", "status_name": "В работе", "pipeline_name": "Привлечение"},
            {"deal_id": 2, "deal_name": "I Deal", "responsible_user_name": "Илья", "status_name": "В работе", "pipeline_name": "Привлечение"},
        ]
    }
    logger = _Logger()
    base_cfg = _cfg()
    cfg = base_cfg.__class__(
        **{
            **base_cfg.__dict__,
            "deal_analyzer_write_enabled": True,
            "deal_analyzer_spreadsheet_id": "sheet123",
            "deal_analyzer_weekly_sheet_name": "Недельный свод менеджеров",
            "deal_analyzer_weekly_start_cell": "A2",
        }
    )

    class _FakeSheetsClient:
        def __init__(self, project_root, logger):
            self.calls = []

        def build_tab_a1_range(self, *, tab_title, range_suffix):
            return f"'{tab_title}'!{range_suffix}"

        def get_values(self, spreadsheet_id, range_a1):
            if range_a1.endswith("A1:ZZ1"):
                return [[
                    "Неделя с",
                    "Неделя по",
                    "Дата контроля",
                    "Менеджер",
                    "Роль менеджера",
                    "Проанализировано сделок",
                    "Ключевой вывод",
                    "Оценка 0-100",
                ]]
            return []

        def batch_update_values(self, spreadsheet_id, data):
            self.calls.append((spreadsheet_id, data))
            return {"ok": True}

    fake_client = _FakeSheetsClient(None, None)

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
                "score_0_100": 60,
                "risk_flags": [],
                "analysis_confidence": "high",
                "owner_ambiguity_flag": False,
                "crm_hygiene_confidence": "high",
                "analysis_backend_used": "rules",
                "manager_summary": "Нормальный рабочий темп, держим фокус.",
            },
            {"llm_success_count": 0, "llm_success_repaired_count": 0, "llm_fallback_count": 0, "llm_error_count": 0},
        )

    with patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation", side_effect=_fake_analyze
    ):
        _run_analyze_weekly(
            cfg,
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

    run_dir = next((output_dir / "weekly_runs").iterdir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    writer = summary.get("weekly_manager_writer", {})
    assert writer.get("mode") == "real_write"
    assert writer.get("rows_written") == 2
    assert len(fake_client.calls) == 2
    assert "A2:" in fake_client.calls[0][1][0]["range"]
    assert "A2:" in fake_client.calls[1][1][0]["range"]
    written_values = fake_client.calls[1][1][0]["values"]
    assert len(written_values) == 2
    assert len(written_values[0]) == 21

