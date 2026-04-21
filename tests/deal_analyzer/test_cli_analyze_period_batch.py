import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config import load_config
from src.deal_analyzer.cli import (
    _build_daily_control_sheet_payload,
    _build_transcription_impact_row,
    _derive_product_hypothesis,
    _expected_quality_text,
    _expected_quantity_text,
    _run_analyze_period,
)
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
        period_live_refresh_enabled=False,
    )


def _snapshot_for_deal(deal_id: int, *, warnings=None, status_name: str = "Р’ СЂР°Р±РѕС‚Рµ"):
    return {
        "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
        "crm": {
            "deal_id": deal_id,
            "amo_lead_id": deal_id,
            "deal_name": f"Deal {deal_id}",
            "status_name": status_name,
            "responsible_user_name": "Илья",
        },
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
            "risk_flags": ["qualified_loss: market mismatch"] if score < 45 else (["process_hygiene: missing follow-up"] if score < 60 else []),
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
            "data_quality_flags": ["crm_context_sparse_with_activity_signals"] if score < 60 else [],
            "owner_ambiguity_flag": score < 45,
            "crm_hygiene_confidence": "low" if score < 60 else "high",
            "analysis_confidence": "low" if score < 60 else "high",
            "transcript_available": score < 60,
            "transcript_text_excerpt": "Обсуждали следующий шаг и бюджет" if score < 60 else "",
            "call_signal_summary_short": "в разговоре есть следующий шаг; поднимался вопрос бюджета" if score < 60 else "",
            "call_signal_product_info": score < 60,
            "call_signal_product_link": False,
            "call_signal_demo_discussed": False,
            "call_signal_test_discussed": False,
            "call_signal_budget_discussed": score < 60,
            "call_signal_followup_discussed": score < 60,
            "call_signal_objection_price": score < 60,
            "call_signal_objection_no_need": False,
            "call_signal_objection_not_target": False,
            "call_signal_next_step_present": score < 60,
            "call_signal_decision_maker_reached": False,
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
    summary_md_path = run_dirs[0] / "summary.md"
    top_risks_path = run_dirs[0] / "top_risks.json"
    manager_brief_path = run_dirs[0] / "manager_brief.md"
    meeting_queue_json_path = run_dirs[0] / "meeting_queue.json"
    meeting_queue_md_path = run_dirs[0] / "meeting_queue.md"
    transcription_impact_md_path = run_dirs[0] / "transcription_impact.md"
    transcription_impact_json_path = run_dirs[0] / "transcription_impact.json"
    queue_sheets_dry_run_path = run_dirs[0] / "meeting_queue_sheets_dry_run.json"
    assert summary_path.exists()
    assert summary_md_path.exists()
    assert top_risks_path.exists()
    assert manager_brief_path.exists()
    assert meeting_queue_json_path.exists()
    assert meeting_queue_md_path.exists()
    assert transcription_impact_md_path.exists()
    assert transcription_impact_json_path.exists()
    assert queue_sheets_dry_run_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["total_deals_seen"] == 2
    assert summary["total_deals_analyzed"] == 2
    assert summary["deals_failed"] == 0
    assert "analysis_confidence_counts" in summary
    assert "owner_ambiguity_deals" in summary
    assert "call_signal_aggregates" in summary
    assert "call_runtime_diagnostics" in summary
    assert "transcript_runtime_diagnostics" in summary
    assert "meeting_queue_writer" in summary
    call_diag = summary["call_runtime_diagnostics"]
    assert "call_collection_mode_effective" in call_diag
    assert "deals_with_call_candidates" in call_diag
    assert "deals_with_recording_url" in call_diag
    assert "audio_downloaded" in call_diag
    assert "audio_cached" in call_diag
    assert "audio_failed" in call_diag
    assert "transcription_attempted" in call_diag
    assert "transcription_success" in call_diag
    assert "transcription_failed" in call_diag
    assert "transcription_failed_missing_audio" in call_diag
    assert "transcription_failed_backend_config" in call_diag
    tx_diag = summary["transcript_runtime_diagnostics"]
    assert "deals_with_any_call_evidence" in tx_diag
    assert "deals_with_audio_path" in tx_diag
    assert "deals_with_transcript_text" in tx_diag
    assert "deals_with_transcript_excerpt" in tx_diag
    assert "deals_with_nonempty_call_signal_summary" in tx_diag
    assert "deals_with_transcription_error" in tx_diag
    assert "transcript_layer_effective" in tx_diag
    assert len(summary["artifact_paths"]) == 2
    md = summary_md_path.read_text(encoding="utf-8")
    assert "## Run Info" in md
    assert "## Score Aggregates" in md
    assert "## Top Risk Flags" in md
    assert "## Data Quality / Interpretation Confidence" in md
    assert "## Call-Aware Signals" in md
    assert "## Проверка транскрибации" in md
    assert "## E2E проверка звонков" in md
    assert "## Weekly Meeting Focus" in md
    assert "### Что просело сильнее всего" in md
    assert "### Что можно исправить за 1 неделю" in md
    assert "### Что нельзя интерпретировать уверенно из-за качества CRM" in md
    assert "## Qualified Loss / Market Mismatch" in md
    assert "## Top 10 Most Risky Deals" in md
    assert "## Top 10 Highest Score Deals" in md
    assert "Meeting queue writer:" in md
    top_risks = json.loads(top_risks_path.read_text(encoding="utf-8"))
    assert isinstance(top_risks, list)
    assert len(top_risks) == 2
    assert "deal_id" in top_risks[0]
    assert "top_risk_flags" in top_risks[0]
    assert "artifact_path" in top_risks[0]
    queue = json.loads(meeting_queue_json_path.read_text(encoding="utf-8"))
    assert isinstance(queue, list)
    if queue:
        sample = queue[0]
        for key in (
            "deal_id",
            "deal_name",
            "owner_name",
            "product_name",
            "status_or_stage",
            "score_0_100",
            "analysis_confidence",
            "owner_ambiguity_flag",
            "top_risk_flags",
            "manager_one_liner",
            "why_in_queue",
            "why_in_queue_human",
            "transcript_available",
            "call_signal_summary_short",
            "transcript_source",
            "transcript_error",
            "artifact_path",
        ):
            assert key in sample
    queue_md = meeting_queue_md_path.read_text(encoding="utf-8")
    assert "# Meeting Queue" in queue_md
    assert "## Фильтры запуска" in queue_md
    assert "## Сделки для разбора" in queue_md
    assert "### Пояснение по группам" in queue_md
    assert "По звонку видно:" in queue_md
    tx_impact_md = transcription_impact_md_path.read_text(encoding="utf-8")
    assert "# Transcription Impact" in tx_impact_md
    assert "## Где звонок реально добавил смысл" in tx_impact_md
    assert "## Где звонок ничего не изменил" in tx_impact_md
    assert "## Где транскрипт сомнительный/шумный" in tx_impact_md
    assert "## Топ-10 сделок для собрания именно по звонкам" in tx_impact_md
    tx_impact_json = json.loads(transcription_impact_json_path.read_text(encoding="utf-8"))
    assert isinstance(tx_impact_json, dict)
    assert "total_deals_analyzed" in tx_impact_json
    assert "deals_changed_by_transcript" in tx_impact_json
    assert "changed_deals" in tx_impact_json
    if tx_impact_json.get("changed_deals"):
        sample_changed = tx_impact_json["changed_deals"][0]
        assert "deal_id" in sample_changed
        assert "baseline_summary" in sample_changed
        assert "transcript_summary" in sample_changed
        assert "changed_fields" in sample_changed
        assert "transcript_excerpt" in sample_changed
    queue_dry_run = json.loads(queue_sheets_dry_run_path.read_text(encoding="utf-8"))
    assert queue_dry_run["mode"] == "dry_run"
    assert queue_dry_run["writer_scope"] == "deal_analyzer_only"
    assert "columns" in queue_dry_run and "rows" in queue_dry_run
    assert "why_in_queue_human" in queue_dry_run["columns"]
    assert "why_in_queue_technical" not in queue_dry_run["columns"]
    if queue_dry_run["rows"]:
        first_row = queue_dry_run["rows"][0]
        assert first_row.get("why_in_queue_human")
    brief = manager_brief_path.read_text(encoding="utf-8")
    assert "# Manager Brief" in brief
    assert "Backend requested:" in brief
    assert "Backend used:" in brief
    assert "Owner ambiguity:" in brief
    assert "Низкая надежность интерпретации:" in brief
    assert "## Call-aware срез" in brief
    assert "## Проверка транскрибации" in brief
    assert "## Что просело сильнее всего" in brief
    assert "## Что можно исправить за 1 неделю" in brief
    assert "## Что нельзя интерпретировать уверенно из-за качества CRM" in brief
    assert "## Qualified loss / market mismatch" in brief
    assert "## 5 сделок, требующих внимания" in brief or "## 5 ñäåëîê, òðåáóþùèõ âíèìàíèÿ" in brief
    assert "## Что делать дальше" in brief or "## ×òî äåëàòü äàëüøå" in brief


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
    top_risks = json.loads((run_dir / "top_risks.json").read_text(encoding="utf-8"))
    queue = json.loads((run_dir / "meeting_queue.json").read_text(encoding="utf-8"))
    md = (run_dir / "summary.md").read_text(encoding="utf-8")
    brief = (run_dir / "manager_brief.md").read_text(encoding="utf-8")
    assert summary["deals_failed"] == 0
    assert len(top_risks) == 2
    assert isinstance(queue, list)
    assert any(item.get("warnings") for item in top_risks)
    assert "[warnings]" in md
    assert "snapshot" in brief.lower()


def test_call_runtime_diagnostics_pipeline_stays_alive_on_audio_and_transcription_failures():
    output_dir = _fresh_output_dir("period_batch_call_runtime_failures")
    payload = {"normalized_deals": [{"deal_id": 1}, {"deal_id": 2}]}
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        deal_id = int(normalized_deal["deal_id"])
        if deal_id == 1:
            return {
                "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
                "crm": {"deal_id": 1, "amo_lead_id": 1, "deal_name": "Deal 1", "status_name": "В работе"},
                "warnings": [],
                "call_evidence": {
                    "source_used": "api_first",
                    "warnings": [],
                    "items": [
                        {
                            "call_id": "c1",
                            "deal_id": "1",
                            "recording_url": "https://example.test/r1.mp3",
                            "audio_path": "",
                            "audio_download_status": "failed",
                        }
                    ],
                    "summary": {"calls_total": 1},
                },
                "transcripts": [
                    {"call_id": "c1", "transcript_status": "backend_error", "transcript_text": "", "transcript_error": "boom"}
                ],
                "roks_context": {"ok": True},
            }
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
            "crm": {"deal_id": 2, "amo_lead_id": 2, "deal_name": "Deal 2", "status_name": "В работе"},
            "warnings": [],
            "call_evidence": {
                "source_used": "api_first",
                "warnings": [],
                "items": [
                    {
                        "call_id": "c2",
                        "deal_id": "2",
                        "recording_url": "https://example.test/r2.mp3",
                        "audio_path": "D:/tmp/audio2.mp3",
                        "audio_download_status": "downloaded",
                    }
                ],
                "summary": {"calls_total": 1},
            },
            "transcripts": [
                {"call_id": "c2", "transcript_status": "ok", "transcript_text": "Короткий транскрипт", "transcript_error": ""}
            ],
            "roks_context": {"ok": True},
        }

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"]), score=55),
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
    call_diag = summary["call_runtime_diagnostics"]
    assert summary["deals_failed"] == 0
    assert call_diag["deals_with_call_candidates"] == 2
    assert call_diag["deals_with_recording_url"] == 2
    assert call_diag["audio_downloaded"] == 1
    assert call_diag["audio_failed"] == 1
    assert call_diag["transcription_attempted"] == 2
    assert call_diag["transcription_success"] == 1
    assert call_diag["transcription_failed"] == 1


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
    brief = (run_dir / "manager_brief.md").read_text(encoding="utf-8")
    assert summary["total_deals_seen"] == 2
    assert summary["total_deals_analyzed"] == 1
    assert summary["deals_failed"] == 1
    assert ("- Deals failed: 1" in brief) or ("- Упало: 1" in brief)


def test_manager_brief_and_summary_safe_fallback_when_all_deals_closed_loss():
    output_dir = _fresh_output_dir("period_batch_all_loss")
    payload = {"normalized_deals": [{"deal_id": 1}, {"deal_id": 2}]}
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return _snapshot_for_deal(int(normalized_deal["deal_id"]), status_name="Р—Р°РєСЂС‹С‚Рѕ Рё РЅРµ СЂРµР°Р»РёР·РѕРІР°РЅРѕ")

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        return (
            {
                "deal_id": int(normalized["deal_id"]),
                "amo_lead_id": int(normalized["deal_id"]),
                "deal_name": f"Deal {normalized['deal_id']}",
                "score_0_100": 55,
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": ["qualified_loss: Р С‹РЅРѕС‡РЅРѕРµ РЅРµСЃРѕРІРїР°РґРµРЅРёРµ/РЅРµС†РµР»РµРІРѕР№ СЃС†РµРЅР°СЂРёР№"],
                "presentation_quality_flag": "needs_attention",
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
            },
            {
                "llm_success_count": 0,
                "llm_success_repaired_count": 0,
                "llm_fallback_count": 0,
                "llm_error_count": 0,
            },
        )

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

    run_dir = next((output_dir / "period_runs").iterdir())
    summary_md = (run_dir / "summary.md").read_text(encoding="utf-8")
    brief_md = (run_dir / "manager_brief.md").read_text(encoding="utf-8")
    assert "fallback" in summary_md.lower()
    assert "closed-lost" in brief_md.lower()
    assert "cleanup closed-lost" in brief_md.lower()
    assert "pipeline pressure path" in brief_md
    assert "поставить follow-up задачу" not in brief_md.lower()
    assert "презентац" not in brief_md.lower()
    assert "## Что нельзя интерпретировать уверенно из-за качества CRM" in brief_md


def test_period_artifact_closed_lost_with_context_non_qualified_has_no_employee_active_leakage():
    output_dir = _fresh_output_dir("period_batch_closed_lost_context_non_qualified")
    payload = {
        "normalized_deals": [
            {
                "deal_id": 32093998,
                "amo_lead_id": 32093998,
                "deal_name": "Deal 32093998",
                "status_name": "Закрыто и не реализовано",
                "notes_summary_raw": [{"text": "Клиент отказался после обсуждения сроков"}],
                "tasks_summary_raw": [],
                "brief_url": "",
                "pain_text": "",
                "business_tasks_text": "",
                "created_at": 10,
                "updated_at": 20,
            }
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
            "crm": dict(normalized_deal),
            "warnings": [],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [],
            "roks_context": {"ok": True},
        }

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot):
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
    deal_artifact = run_dir / "deals" / "deal_32093998.json"
    data = json.loads(deal_artifact.read_text(encoding="utf-8"))
    analysis = data.get("analysis", {})
    coaching = str(analysis.get("employee_coaching", "")).lower()
    fix_tasks = " ".join(str(x) for x in analysis.get("employee_fix_tasks", [])).lower()
    forbidden = ("боль клиента", "бизнес-задач", "презентац", "бриф", "follow-up", "вероятност")
    assert all(token not in coaching for token in forbidden)
    assert all(token not in fix_tasks for token in forbidden)


def test_meeting_queue_filters_and_exclude_low_confidence():
    output_dir = _fresh_output_dir("period_batch_meeting_queue_filters")
    payload = {
        "normalized_deals": [
            {
                "deal_id": 1,
                "deal_name": "Deal one",
                "responsible_user_name": "Илья",
                "product_values": ["ИНФО"],
                "status_name": "В работе",
                "pipeline_name": "Привлечение",
            },
            {
                "deal_id": 2,
                "deal_name": "Deal two",
                "responsible_user_name": "Рустам",
                "product_values": ["ДРУГОЙ"],
                "status_name": "В работе",
                "pipeline_name": "Привлечение",
            },
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
            "crm": dict(normalized_deal),
            "warnings": [],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [],
            "roks_context": {"ok": True},
        }

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        deal_id = int(normalized.get("deal_id"))
        is_low = deal_id == 1
        return (
            {
                "deal_id": deal_id,
                "amo_lead_id": deal_id,
                "deal_name": normalized.get("deal_name", ""),
                "score_0_100": 30 if is_low else 60,
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": ["process_hygiene: missing follow-up"],
                "presentation_quality_flag": "needs_attention",
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
                "analysis_confidence": "low" if is_low else "high",
                "crm_hygiene_confidence": "low" if is_low else "high",
                "owner_ambiguity_flag": False,
                "manager_insight_short": f"insight {deal_id}",
                "manager_summary": f"summary {deal_id}",
            },
            {
                "llm_success_count": 0,
                "llm_success_repaired_count": 0,
                "llm_fallback_count": 0,
                "llm_error_count": 0,
            },
        )

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
            owner_contains="Руст",
            product_contains="друг",
            status_contains="работе",
            exclude_low_confidence=True,
            discussion_limit=10,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    queue = json.loads((run_dir / "meeting_queue.json").read_text(encoding="utf-8"))
    assert len(queue) == 1
    assert queue[0]["deal_id"] == 2


def test_meeting_queue_categories_and_sorting_semantics():
    output_dir = _fresh_output_dir("period_batch_meeting_queue_categories")
    payload = {
        "normalized_deals": [
            {"deal_id": 1, "deal_name": "active", "responsible_user_name": "A", "status_name": "В работе", "pipeline_name": "P", "product_values": ["X"]},
            {"deal_id": 2, "deal_name": "won", "responsible_user_name": "A", "status_name": "Успешно реализовано", "pipeline_name": "P", "product_values": ["X"]},
            {"deal_id": 3, "deal_name": "low confidence", "responsible_user_name": "A", "status_name": "В работе", "pipeline_name": "P", "product_values": ["X"]},
            {"deal_id": 4, "deal_name": "qualified loss", "responsible_user_name": "A", "status_name": "Закрыто и не реализовано", "pipeline_name": "P", "product_values": ["X"]},
            {"deal_id": 5, "deal_name": "closed lost cleanup", "responsible_user_name": "A", "status_name": "Закрыто и не реализовано", "pipeline_name": "P", "product_values": ["X"]},
            {"deal_id": 6, "deal_name": "closed lost low confidence", "responsible_user_name": "A", "status_name": "Закрыто и не реализовано", "pipeline_name": "P", "product_values": ["X"]},
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
            "crm": dict(normalized_deal),
            "warnings": [],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [],
            "roks_context": {"ok": True},
        }

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        deal_id = int(normalized.get("deal_id"))
        if deal_id == 1:
            risk_flags = ["process_hygiene: missing follow-up", "evidence_context: missing notes"]
            score = 20
            confidence = "high"
            owner_ambiguity = False
        elif deal_id == 2:
            risk_flags = []
            score = 70
            confidence = "high"
            owner_ambiguity = False
        elif deal_id == 3:
            risk_flags = ["process_hygiene: missing follow-up"]
            score = 55
            confidence = "low"
            owner_ambiguity = True
        elif deal_id == 4:
            risk_flags = ["qualified_loss: market mismatch"]
            score = 40
            confidence = "high"
            owner_ambiguity = False
        elif deal_id == 6:
            risk_flags = ["evidence_context: missing notes", "process_hygiene: missing follow-up"]
            score = 25
            confidence = "low"
            owner_ambiguity = True
        else:
            risk_flags = ["evidence_context: no reason captured"]
            score = 35
            confidence = "high"
            owner_ambiguity = False
        return (
            {
                "deal_id": deal_id,
                "amo_lead_id": deal_id,
                "deal_name": normalized.get("deal_name", ""),
                "score_0_100": score,
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": risk_flags,
                "presentation_quality_flag": "needs_attention",
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
                "analysis_confidence": confidence,
                "crm_hygiene_confidence": confidence,
                "owner_ambiguity_flag": owner_ambiguity,
                "manager_insight_short": f"insight {deal_id}",
                "manager_summary": f"summary {deal_id}",
                "reanimation_potential": (
                    "none"
                    if deal_id in {1, 2, 3, 4}
                    else ("medium" if deal_id == 6 else "low")
                ),
                "reanimation_reason_short": (
                    "Вывод ограничен качеством CRM-данных."
                    if deal_id == 6
                    else ("Qualified-loss anti-fit." if deal_id == 4 else "Closeout cleanup.")
                ),
                "reanimation_next_step": "next step",
                "reanimation_risk_note": "risk",
            },
            {"llm_success_count": 0, "llm_success_repaired_count": 0, "llm_fallback_count": 0, "llm_error_count": 0},
        )

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
            discussion_limit=10,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    queue = json.loads((run_dir / "meeting_queue.json").read_text(encoding="utf-8"))
    by_id = {int(item["deal_id"]): item for item in queue}
    assert by_id[1]["why_in_queue"] == "active_risk"
    assert by_id[2]["why_in_queue"] == "won_handoff_check"
    assert by_id[3]["why_in_queue"] == "low_confidence_needs_manual_check"
    assert by_id[4]["why_in_queue"] == "qualified_loss_for_pattern_review"
    assert by_id[5]["why_in_queue"] == "closed_lost_cleanup_review"
    assert by_id[6]["why_in_queue"] == "low_confidence_needs_manual_check"
    assert by_id[5]["why_in_queue"] != "active_risk"
    assert by_id[4]["reanimation_potential"] in {"none", "low"}
    assert by_id[4]["reanimation_potential"] != "high"
    assert by_id[3]["reanimation_potential"] == "none"
    assert by_id[6]["reanimation_potential"] in {"low", "medium"}
    assert by_id[6]["reanimation_reason_short"].lower().find("качеств") >= 0
    assert "reanimation_next_step" in by_id[5]
    ordered_ids = [int(item["deal_id"]) for item in queue]
    assert ordered_ids == [1, 2, 6, 3, 4, 5]

    queue_md = (run_dir / "meeting_queue.md").read_text(encoding="utf-8")
    assert "Потенциал реанимации" in queue_md
    assert "Почему:" in queue_md
    assert "Следующий шаг:" in queue_md

    brief = (run_dir / "manager_brief.md").read_text(encoding="utf-8")
    assert "## Разбиение queue по категориям" in brief
    assert "Живые риски:" in brief
    assert "Проверка передачи:" in brief
    assert "Низкая надежность / ручная проверка:" in brief
    assert "Qualified loss паттерны:" in brief
    assert "Закрытые потери на cleanup-разбор:" in brief
    assert "## Потенциал реанимации закрытых потерь" in brief


def test_product_hypothesis_layer_does_not_override_crm_product():
    deal = {
        "deal_id": 901,
        "product_values": ["INFO модуль"],
        "notes_summary_raw": [{"text": "Обсуждаем выгрузку карточек и контента"}],
    }
    analysis = {"analysis_confidence": "high", "owner_ambiguity_flag": False}
    result = _derive_product_hypothesis(analysis=analysis, deal=deal, snapshot={"transcripts": []})
    assert deal["product_values"] == ["INFO модуль"]
    assert result["product_hypothesis"] == "info"
    assert result["product_hypothesis_confidence"] in {"medium", "high"}
    assert result["product_hypothesis_reason_short"]


def test_product_hypothesis_low_confidence_is_capped():
    deal = {
        "deal_id": 902,
        "product_values": [],
        "notes_summary_raw": [{"text": "Клиент просит SRM и работу с поставщиками"}],
    }
    analysis = {"analysis_confidence": "low", "owner_ambiguity_flag": True}
    result = _derive_product_hypothesis(analysis=analysis, deal=deal, snapshot={"transcripts": []})
    assert result["product_hypothesis"] == "link"
    assert result["product_hypothesis_confidence"] in {"low", "medium"}
    assert result["product_hypothesis_confidence"] != "high"


def test_meeting_queue_sheets_dry_run_uses_human_reason():
    output_dir = _fresh_output_dir("period_batch_queue_human_reason")
    payload = {
        "normalized_deals": [
            {
                "deal_id": 11,
                "deal_name": "Deal eleven",
                "responsible_user_name": "Илья",
                "product_values": ["ИНФО"],
                "status_name": "В работе",
                "pipeline_name": "Привлечение",
            }
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
            "crm": dict(normalized_deal),
            "warnings": [],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [],
            "roks_context": {"ok": True},
        }

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        return (
            {
                "deal_id": int(normalized.get("deal_id")),
                "amo_lead_id": int(normalized.get("deal_id")),
                "deal_name": normalized.get("deal_name", ""),
                "score_0_100": 22,
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": ["process_hygiene: missing follow-up"],
                "presentation_quality_flag": "needs_attention",
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
                "manager_insight_short": "insight 11",
                "manager_summary": "summary 11",
            },
            {"llm_success_count": 0, "llm_success_repaired_count": 0, "llm_fallback_count": 0, "llm_error_count": 0},
        )

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

    run_dir = next((output_dir / "period_runs").iterdir())
    queue_dry_run = json.loads((run_dir / "meeting_queue_sheets_dry_run.json").read_text(encoding="utf-8"))
    assert queue_dry_run["rows_count"] == 1
    row = queue_dry_run["rows"][0]
    assert row["why_in_queue_human"] == "нужен ближайший следующий шаг"
    assert "why_in_queue_technical" not in row


def test_transcription_compare_diff_only_for_meaningful_fields():
    row = _build_transcription_impact_row(
        deal_id=1,
        deal_name="Deal 1",
        owner_name="Owner",
        status_or_stage="В работе",
        score=55,
        without_view={
            "product_hypothesis": "unknown",
            "product_hypothesis_confidence": "low",
            "product_hypothesis_reason_short": "",
            "call_signal_summary_short": "",
            "reanimation_potential": "none",
            "reanimation_reason_short": "",
            "manager_summary": "A",
            "employee_coaching": "X",
            "employee_fix_tasks": ["1"],
            "top_risk_flags": ["a"],
        },
        with_view={
            "product_hypothesis": "info",
            "product_hypothesis_confidence": "medium",
            "product_hypothesis_reason_short": "reason",
            "call_signal_summary_short": "call",
            "reanimation_potential": "low",
            "reanimation_reason_short": "r",
            "manager_summary": "B",
            "employee_coaching": "Y",
            "employee_fix_tasks": ["2"],
            "top_risk_flags": ["DIFFERENT_BUT_NOT_MEANINGFUL"],
        },
        analysis={
            "transcript_available": True,
            "transcript_text_excerpt": "excerpt",
            "transcript_error": "",
            "why_in_queue": "active_risk",
        },
        snapshot={"transcripts": [{"transcript_text": "long enough text for stable interpretation " * 5}]},
        artifact_path="artifact.json",
    )
    assert row["changed"] is True
    assert "top_risk_flags" not in row["changed_fields"]
    assert "product_hypothesis" in row["changed_fields"]
    assert "manager_summary" in row["changed_fields"]


def test_product_hypothesis_mixed_and_unknown():
    mixed_deal = {
        "deal_id": 903,
        "notes_summary_raw": [{"text": "Нужны карточки INFO и интеграция с тендерными закупками"}],
        "product_values": [],
    }
    mixed = _derive_product_hypothesis(
        analysis={"analysis_confidence": "medium", "owner_ambiguity_flag": False},
        deal=mixed_deal,
        snapshot={"transcripts": [{"transcript_text": "Обсудили INFO и SRM контур"}]},
    )
    assert mixed["product_hypothesis"] == "mixed"

    unknown = _derive_product_hypothesis(
        analysis={"analysis_confidence": "high", "owner_ambiguity_flag": False},
        deal={"deal_id": 904, "notes_summary_raw": [], "tasks_summary_raw": [], "product_values": []},
        snapshot={"transcripts": []},
    )
    assert unknown["product_hypothesis"] == "unknown"
    assert unknown["product_hypothesis_confidence"] == "low"


def test_period_artifacts_include_product_hypothesis_fields():
    output_dir = _fresh_output_dir("period_batch_product_hypothesis_artifacts")
    payload = {
        "normalized_deals": [
            {
                "deal_id": 1,
                "deal_name": "Deal one",
                "responsible_user_name": "Илья",
                "product_values": ["INFO"],
                "status_name": "В работе",
                "pipeline_name": "Привлечение",
                "notes_summary_raw": [{"text": "Нужна витрина и контент"}],
            }
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
            "crm": dict(normalized_deal),
            "warnings": [],
            "call_evidence": {"items": [], "summary": {"calls_total": 0}},
            "transcripts": [{"transcript_text": "Подтверждаем продукт INFO"}],
            "roks_context": {"ok": True},
        }

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot):
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
            discussion_limit=10,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    queue = json.loads((run_dir / "meeting_queue.json").read_text(encoding="utf-8"))
    assert queue
    assert "product_hypothesis" in queue[0]
    assert "product_hypothesis_confidence" in queue[0]
    assert "product_hypothesis_reason_short" in queue[0]

    queue_md = (run_dir / "meeting_queue.md").read_text(encoding="utf-8")
    assert "CRM продукт:" in queue_md
    assert "Гипотеза продукта:" in queue_md
    assert "Уверенность гипотезы:" in queue_md

    brief = (run_dir / "manager_brief.md").read_text(encoding="utf-8")
    assert "## Гипотеза по продуктам в разборе" in brief
    assert "- info:" in brief


def test_period_summary_reports_real_llm_overlay_usage():
    output_dir = _fresh_output_dir("period_batch_llm_overlay_summary")
    payload = {
        "normalized_deals": [
            {"deal_id": 1001, "deal_name": "Deal one", "status_name": "В работе", "pipeline_name": "P"},
            {"deal_id": 1002, "deal_name": "Deal two", "status_name": "В работе", "pipeline_name": "P"},
        ]
    }
    logger = _Logger()

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle):
        return {
            "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
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
                "score_0_100": 50,
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": [],
                "presentation_quality_flag": "ok",
                "followup_quality_flag": "ok",
                "data_completeness_flag": "partial",
                "recommended_actions_for_manager": [],
                "recommended_training_tasks_for_employee": [],
                "manager_message_draft": "",
                "employee_training_message_draft": "",
                "analysis_backend_requested": "hybrid",
                "analysis_backend_used": "hybrid",
                "llm_repair_applied": False,
                "backend": "hybrid",
                "analysis_confidence": "high",
                "crm_hygiene_confidence": "high",
                "owner_ambiguity_flag": False,
                "loss_reason_short": "Коротко",
                "manager_insight_short": "Инсайт",
                "coaching_hint_short": "Коучинг",
                "product_hypothesis_llm": "link" if deal_id == 1001 else "unknown",
                "reanimation_reason_short_llm": "Причина LLM" if deal_id == 1001 else "",
            },
            {
                "llm_success_count": 1,
                "llm_success_repaired_count": 0,
                "llm_fallback_count": 0,
                "llm_error_count": 0,
            },
        )

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
            discussion_limit=10,
        )

    run_dir = next((output_dir / "period_runs").iterdir())
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["analysis_backend_used"] == "hybrid"
    assert summary["llm_overlay_deals"] == 2


def test_meeting_queue_writer_real_write_path_via_mock():
    output_dir = _fresh_output_dir("period_batch_writer_real_write")
    payload = {"normalized_deals": [{"deal_id": 1}]}
    logger = _Logger()
    base_cfg = _cfg()
    cfg = base_cfg.__class__(
        **{
            **base_cfg.__dict__,
            "deal_analyzer_write_enabled": True,
            "deal_analyzer_spreadsheet_id": "sheet123",
            "deal_analyzer_sheet_name": "analytics_writer_test",
            "deal_analyzer_start_cell": "A1",
        }
    )

    class _FakeSheetsClient:
        def __init__(self, project_root, logger):
            self.calls = []

        def build_tab_a1_range(self, *, tab_title, range_suffix):
            return f"'{tab_title}'!{range_suffix}"

        def get_values(self, spreadsheet_id, range_a1):
            return []

        def batch_update_values(self, spreadsheet_id, data):
            self.calls.append((spreadsheet_id, data))
            return {"ok": True}

    fake_client = _FakeSheetsClient(None, None)

    with patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"])),
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"]), score=55),
    ):
        _run_analyze_period(
            cfg,
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
    writer = summary.get("meeting_queue_writer", {})
    assert writer.get("enabled") is True
    assert writer.get("mode") == "real_write"
    assert writer.get("rows_prepared", 0) >= 1
    assert writer.get("rows_written") == writer.get("rows_prepared")
    assert writer.get("write_mode") == "append"
    assert len(fake_client.calls) == 1


def test_meeting_queue_writer_safe_skip_when_target_missing():
    output_dir = _fresh_output_dir("period_batch_writer_safe_skip")
    payload = {"normalized_deals": [{"deal_id": 1}]}
    logger = _Logger()
    base_cfg = _cfg()
    cfg = base_cfg.__class__(
        **{
            **base_cfg.__dict__,
            "deal_analyzer_write_enabled": True,
            "deal_analyzer_spreadsheet_id": "",
            "deal_analyzer_sheet_name": "",
            "deal_analyzer_start_cell": "",
        }
    )

    with patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"])),
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"]), score=55),
    ):
        _run_analyze_period(
            cfg,
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
    writer = summary.get("meeting_queue_writer", {})
    assert writer.get("enabled") is True
    assert writer.get("mode") == "dry_run"
    assert writer.get("rows_prepared", 0) >= 1
    assert writer.get("rows_written") == 0
    assert writer.get("error")


def test_meeting_queue_writer_appends_below_existing_rows():
    output_dir = _fresh_output_dir("period_batch_writer_tail_clear")
    payload = {"normalized_deals": [{"deal_id": 1}]}
    logger = _Logger()
    base_cfg = _cfg()
    cfg = base_cfg.__class__(
        **{
            **base_cfg.__dict__,
            "deal_analyzer_write_enabled": True,
            "deal_analyzer_spreadsheet_id": "sheet123",
            "deal_analyzer_sheet_name": "analytics_writer_test",
            "deal_analyzer_start_cell": "A1",
        }
    )

    class _FakeSheetsClient:
        def __init__(self, project_root, logger):
            self.calls = []

        def build_tab_a1_range(self, *, tab_title, range_suffix):
            return f"'{tab_title}'!{range_suffix}"

        def get_values(self, spreadsheet_id, range_a1):
            # Existing block has 5 non-empty rows (header + 4 data rows)
            return [["h1"], ["r1"], ["r2"], ["r3"], ["r4"]]

        def batch_update_values(self, spreadsheet_id, data):
            self.calls.append((spreadsheet_id, data))
            return {"ok": True}

    fake_client = _FakeSheetsClient(None, None)

    with patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"])),
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"]), score=55),
    ):
        _run_analyze_period(
            cfg,
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
    writer = summary.get("meeting_queue_writer", {})
    assert writer.get("mode") == "real_write"
    assert writer.get("write_mode") == "append"
    assert writer.get("write_start_row", 0) >= 6


def test_daily_control_payload_uses_business_columns_only():
    summary = {
        "period_start": "2026-04-14",
        "period_end": "2026-04-20",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 101,
            "owner_name": "Рустам",
            "product_name": "ИНФО",
            "score": 55,
            "risk_flags": ["process_hygiene: missing follow-up"],
            "manager_summary": "Нужно дожать фиксацию следующего шага.",
            "growth_zones": ["Не всегда фиксируется следующий шаг"],
            "strong_sides": ["Есть нормальный контакт с ЛПР"],
            "employee_coaching": "Переслушать звонок и сверить с CRM.",
            "status_name": "В работе",
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    assert payload["sheet_name"] == "Дневной контроль"
    assert payload["start_cell"] == "A2"
    assert "deal_id" not in payload["columns"]
    assert "artifact_path" not in payload["columns"]
    assert payload["columns"][0] == "Неделя с"
    assert payload["columns"][-1] == "Критичность"
    row = payload["rows"][0]
    assert "Ссылки на сделки" in row
    assert "Что донес сотруднику" in row
    assert isinstance(row["Оценка 0-100"], int)
    assert 0 <= row["Оценка 0-100"] <= 100
    joined = " ".join(
        str(row.get(k) or "")
        for k in ("Ключевой вывод", "Сильные стороны", "Зоны роста", "Почему это важно", "Что закрепить", "Что исправить", "Что донес сотруднику")
    ).lower()
    for bad in ("qualified loss", "anti-fit", "owner", "closeout", "follow-up"):
        assert bad not in joined


def test_daily_control_writer_starts_from_a2_and_does_not_write_header_row():
    output_dir = _fresh_output_dir("period_batch_daily_writer_a2")
    payload = {"normalized_deals": [{"deal_id": 1}]}
    logger = _Logger()
    base_cfg = _cfg()
    cfg = base_cfg.__class__(
        **{
            **base_cfg.__dict__,
            "deal_analyzer_write_enabled": True,
            "deal_analyzer_spreadsheet_id": "sheet123",
            "deal_analyzer_daily_sheet_name": "Дневной контроль",
            "deal_analyzer_daily_start_cell": "A2",
        }
    )

    class _FakeSheetsClient:
        def __init__(self, project_root, logger):
            self.calls = []

        def build_tab_a1_range(self, *, tab_title, range_suffix):
            return f"'{tab_title}'!{range_suffix}"

        def get_values(self, spreadsheet_id, range_a1):
            return []

        def batch_update_values(self, spreadsheet_id, data):
            self.calls.append((spreadsheet_id, data))
            return {"ok": True}

    fake_client = _FakeSheetsClient(None, None)
    with patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"])),
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"]), score=55),
    ):
        _run_analyze_period(
            cfg,
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

    assert len(fake_client.calls) == 1
    write_range = fake_client.calls[0][1][0]["range"]
    assert "A2:" in write_range
    written_values = fake_client.calls[0][1][0]["values"]
    assert len(written_values) >= 1


def test_daily_control_writer_uses_append_mode_by_default():
    output_dir = _fresh_output_dir("period_batch_daily_writer_tail")
    payload = {"normalized_deals": [{"deal_id": 1}]}
    logger = _Logger()
    base_cfg = _cfg()
    cfg = base_cfg.__class__(
        **{
            **base_cfg.__dict__,
            "deal_analyzer_write_enabled": True,
            "deal_analyzer_spreadsheet_id": "sheet123",
            "deal_analyzer_daily_sheet_name": "Дневной контроль",
            "deal_analyzer_daily_start_cell": "A2",
        }
    )

    class _FakeSheetsClient:
        def __init__(self, project_root, logger):
            self.calls = []

        def build_tab_a1_range(self, *, tab_title, range_suffix):
            return f"'{tab_title}'!{range_suffix}"

        def get_values(self, spreadsheet_id, range_a1):
            return [["row1"], ["row2"], ["row3"], ["row4"]]

        def batch_update_values(self, spreadsheet_id, data):
            self.calls.append((spreadsheet_id, data))
            return {"ok": True}

    fake_client = _FakeSheetsClient(None, None)
    with patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli.build_deal_snapshot",
        side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"])),
    ), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"]), score=55),
    ):
        _run_analyze_period(
            cfg,
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
    writer = summary.get("daily_control_writer", {})
    assert writer.get("write_mode") == "append"
    assert writer.get("rows_written", 0) >= 1
    assert writer.get("write_start_row", 0) >= 6


def test_daily_control_payload_uses_weekday_packages_not_runtime_sunday():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 101,
            "owner_name": "Рустам",
            "product_name": "ИНФО",
            "score": 55,
            "risk_flags": ["process_hygiene: missing follow-up"],
            "manager_summary": "Дожать следующий шаг.",
            "growth_zones": ["Не всегда фиксируется следующий шаг"],
            "strong_sides": ["Есть контакт с ЛПР"],
            "employee_coaching": "Переслушать звонок и сверить с CRM.",
            "status_name": "В работе",
            "updated_at": "2026-04-17T11:00:00+00:00",
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    dates = [row.get("Дата контроля") for row in payload.get("rows", [])]
    days = [str(row.get("День") or "") for row in payload.get("rows", [])]
    assert "2026-04-19" not in dates
    assert "Воскресенье" not in days
    assert payload.get("rows_count", 0) >= 1


def test_daily_control_role_aware_filters_warm_growth_for_rustam():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 202,
            "owner_name": "Рустам",
            "product_name": "ИНФО",
            "score": 42,
            "risk_flags": ["demo_missing", "brief_missing", "process_hygiene: missing follow-up"],
            "growth_zones": ["Не подтверждена презентация", "Не заполнен бриф", "Не фиксируется следующий шаг"],
            "strong_sides": ["Вышел на ЛПР"],
            "manager_summary": "Есть движение, но провисает фиксация следующего шага.",
            "employee_coaching": "Сверить звонок с CRM и дожать следующий шаг.",
            "status_name": "В работе",
            "updated_at": "2026-04-16T11:00:00+00:00",
            "call_signal_demo_discussed": False,
            "call_signal_test_discussed": False,
            "status_or_stage": "В работе / Привлечение",
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    assert payload.get("rows")
    growth = str(payload["rows"][0].get("Зоны роста") or "").lower()
    role = str(payload["rows"][0].get("Роль менеджера") or "").lower()
    assert role == "телемаркетолог"
    assert "презентац" not in growth
    assert "бриф" not in growth


def test_daily_control_links_are_full_urls_with_newlines():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 301,
            "owner_name": "Илья",
            "score": 58,
            "risk_flags": ["process_hygiene: missing follow-up"],
            "growth_zones": ["Не всегда фиксируется следующий шаг"],
            "strong_sides": ["Есть контакт с ЛПР"],
            "manager_summary": "Дожать следующий шаг и не терять темп.",
            "employee_coaching": "Сверить звонок и CRM.",
            "status_name": "В работе",
        },
        {
            "deal_id": 302,
            "owner_name": "Илья",
            "score": 57,
            "risk_flags": ["process_hygiene: missing follow-up"],
            "growth_zones": ["Не всегда фиксируется следующий шаг"],
            "strong_sides": ["Есть контакт с ЛПР"],
            "manager_summary": "Дожать следующий шаг и не терять темп.",
            "employee_coaching": "Сверить звонок и CRM.",
            "status_name": "В работе",
        },
    ]
    payload = _build_daily_control_sheet_payload(
        summary=summary,
        period_deal_records=records,
        amo_base_domain="https://example.amocrm.ru",
    )
    assert payload.get("rows")
    links = str(payload["rows"][0].get("Ссылки на сделки") or "")
    assert "https://example.amocrm.ru/leads/detail/301" in links
    assert "https://example.amocrm.ru/leads/detail/302" in links
    assert "\n" in links


def test_daily_control_spreads_rows_across_multiple_weekdays_when_enough_material():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = []
    for idx in range(1, 9):
        records.append(
            {
                "deal_id": 500 + idx,
                "owner_name": "Рустам",
                "score": 45 + idx,
                "risk_flags": ["process_hygiene: missing follow-up"],
                "growth_zones": ["Не всегда фиксируется следующий шаг"],
                "strong_sides": ["Есть контакт с ЛПР"],
                "manager_summary": "Дожать следующий шаг и не терять темп.",
                "employee_coaching": "Перепроверить на свежую голову и сверить с CRM.",
                "status_name": "В работе",
                "updated_at": "",
            }
        )
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    rows = payload.get("rows", [])
    dates = [str(row.get("Дата контроля") or "") for row in rows]
    assert len(rows) >= 2
    assert len(set(dates)) >= 2


def test_daily_control_default_allowlist_excludes_non_target_managers():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {"deal_id": 801, "owner_name": "Илья", "score": 60, "status_name": "В работе"},
        {"deal_id": 802, "owner_name": "Рустам", "score": 61, "status_name": "В работе"},
        {"deal_id": 803, "owner_name": "Антон Коломоец", "score": 62, "status_name": "В работе"},
        {"deal_id": 804, "owner_name": "Гордиенко Кирилл", "score": 63, "status_name": "В работе"},
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    managers = {str(r.get("Менеджер") or "") for r in payload.get("rows", [])}
    assert "Илья" in managers
    assert "Рустам" in managers
    assert "Антон Коломоец" not in managers
    assert "Гордиенко Кирилл" not in managers


def test_daily_control_base_mix_never_uses_segment_not_defined():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [{"deal_id": 901, "owner_name": "Илья", "score": 51, "status_name": "В работе"}]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    assert payload.get("rows")
    mix = str(payload["rows"][0].get("База микс") or "").strip().lower()
    assert mix != "сегмент не определен"
    assert mix


def test_daily_control_what_fix_is_not_same_as_growth():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 910,
            "owner_name": "Рустам",
            "score": 50,
            "status_name": "В работе",
            "growth_zones": ["Не зафиксирована боль клиента"],
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    row = payload["rows"][0]
    growth = str(row.get("Зоны роста") or "").strip()
    fix = str(row.get("Что исправить") or "").strip()
    assert growth
    assert fix
    assert growth != fix
    assert "на ближайший цикл" not in fix.lower()


def test_daily_control_coaching_has_numbered_list_format():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [{"deal_id": 920, "owner_name": "Илья", "score": 55, "status_name": "В работе"}]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    coaching = str(payload["rows"][0].get("Что донес сотруднику") or "")
    assert "донес" not in coaching.lower()
    assert "1)" in coaching and "2)" in coaching and "3)" in coaching


def test_daily_control_text_columns_can_be_llm_authored_not_template():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [{"deal_id": 921, "owner_name": "Илья", "score": 55, "status_name": "В работе"}]
    llm_columns = {
        "Ключевой вывод": "Фактура собрана нормально, провисает только фиксация следующего шага.",
        "Сильные стороны": "Держит структуру разговора и не теряет клиента в середине диалога.",
        "Зоны роста": "Сразу закрывать договоренность датой; Не оставлять размытый следующий шаг",
        "Почему это важно": "Менеджеру проще дожимать, когда шаг зафиксирован. Руководителю видно, что реально движется.",
        "Что закрепить": "Держать связку вопрос -> уточнение -> договоренность с датой.",
        "Что исправить": "После звонка сразу фиксировать шаг и срок в карточке.",
        "Что донес сотруднику": "1) Сверить шаг в двух свежих звонках.\n2) Проговорить формулировку следующего шага.\n3) Закрепить шаблон фиксации в CRM.",
        "Ожидаемый эффект - количество": "+1 подтвержденная встреча в неделю.",
        "Ожидаемый эффект - качество": "Шаги будут фиксироваться чище; переход между этапами станет стабильнее.",
    }

    with patch("src.deal_analyzer.cli._generate_daily_table_text_columns", return_value=llm_columns):
        payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    row = payload["rows"][0]
    assert row["Ключевой вывод"] == llm_columns["Ключевой вывод"]
    assert row["Что закрепить"] == llm_columns["Что закрепить"]
    assert row["Ожидаемый эффект - количество"] == llm_columns["Ожидаемый эффект - количество"]


def test_daily_control_rows_are_sorted_by_day_then_manager():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 701,
            "owner_name": "Рустам",
            "score": 55,
            "risk_flags": [],
            "growth_zones": ["Не терять следующий шаг"],
            "strong_sides": ["Вышел на ЛПР"],
            "manager_summary": "Собрал нормальный контекст по звонкам.",
            "employee_coaching": "Закрепить рабочий скрипт.",
            "status_name": "В работе",
            "updated_at": "2026-04-14T10:00:00+00:00",
        },
        {
            "deal_id": 702,
            "owner_name": "Илья",
            "score": 65,
            "risk_flags": [],
            "growth_zones": ["Не отпускать фиксацию результата"],
            "strong_sides": ["Есть зафиксированный следующий шаг"],
            "manager_summary": "Хорошо дожимает результат встречи.",
            "employee_coaching": "Продолжать в том же темпе.",
            "status_name": "В работе",
            "updated_at": "2026-04-14T09:00:00+00:00",
        },
        {
            "deal_id": 703,
            "owner_name": "Рустам",
            "score": 51,
            "risk_flags": [],
            "growth_zones": ["Дожимать назначение"],
            "strong_sides": ["Есть контакт с ЛПР"],
            "manager_summary": "По холодному контуру есть рабочая динамика.",
            "employee_coaching": "Сверять шаг с CRM в день звонка.",
            "status_name": "В работе",
            "updated_at": "2026-04-15T09:00:00+00:00",
        },
        {
            "deal_id": 704,
            "owner_name": "Илья",
            "score": 69,
            "risk_flags": [],
            "growth_zones": ["Фиксировать результат встречи"],
            "strong_sides": ["Двигает сделки по теплому этапу"],
            "manager_summary": "По встречам держит нужный темп.",
            "employee_coaching": "Дожимать следующий шаг после встречи.",
            "status_name": "В работе",
            "updated_at": "2026-04-15T08:00:00+00:00",
        },
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    order = [(str(x.get("Дата контроля") or ""), str(x.get("Менеджер") or "")) for x in payload.get("rows", [])]
    assert order == sorted(order, key=lambda x: (x[0], x[1]))


def test_daily_expected_quantity_has_no_percent_and_no_conversion_wording():
    qty_rustam = _expected_quantity_text(avg_score=35, deals=4, role="телемаркетолог")
    qty_ilya = _expected_quantity_text(avg_score=55, deals=4, role="менеджер по продажам")

    for value in (qty_rustam, qty_ilya):
        low = value.lower()
        assert "%" not in value
        assert "конверси" not in low
        assert "этап" not in low
        assert any(token in low for token in ("+1", "1-2", "-1", "1 дополнительный"))


def test_daily_expected_quality_allows_stage_or_conversion_hypothesis():
    quality_sales = _expected_quality_text(criticality="средняя", role="менеджер по продажам").lower()
    assert "этап" in quality_sales or "конверси" in quality_sales


def test_daily_expected_quantity_rustam_and_ilya_are_absolute_not_percent():
    qty_rustam = _expected_quantity_text(avg_score=60, deals=3, role="телемаркетолог").lower()
    qty_ilya = _expected_quantity_text(avg_score=60, deals=3, role="менеджер по продажам").lower()

    assert "%" not in qty_rustam
    assert "%" not in qty_ilya
    assert "встреч" in qty_rustam or "лпр" in qty_rustam or "сделк" in qty_rustam
    assert "встреч" in qty_ilya or "следующ" in qty_ilya or "сделк" in qty_ilya


def test_daily_payload_expected_quantity_is_absolute_and_no_percent():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {"deal_id": 931, "owner_name": "Илья", "score": 53, "status_name": "В работе"},
        {"deal_id": 932, "owner_name": "Рустам", "score": 48, "status_name": "В работе"},
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    for row in payload["rows"]:
        qty = str(row.get("Ожидаемый эффект - количество") or "")
        low = qty.lower()
        assert "%" not in qty
        assert "конверси" not in low

