import json
import shutil
from pathlib import Path
from unittest.mock import patch

from src.config import load_config
from src.deal_analyzer.cli import _derive_product_hypothesis, _run_analyze_period
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


def _snapshot_for_deal(deal_id: int, *, warnings=None, status_name: str = "Р’ СЂР°Р±РѕС‚Рµ"):
    return {
        "snapshot_generated_at": "2026-04-18T12:00:00+00:00",
        "crm": {"deal_id": deal_id, "amo_lead_id": deal_id, "deal_name": f"Deal {deal_id}", "status_name": status_name},
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
    assert "transcript_runtime_diagnostics" in summary
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
    assert "## Weekly Meeting Focus" in md
    assert "### Что просело сильнее всего" in md
    assert "### Что можно исправить за 1 неделю" in md
    assert "### Что нельзя интерпретировать уверенно из-за качества CRM" in md
    assert "## Qualified Loss / Market Mismatch" in md
    assert "## Top 10 Most Risky Deals" in md
    assert "## Top 10 Highest Score Deals" in md
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
    assert isinstance(tx_impact_json, list)
    if tx_impact_json:
        assert "without_transcript_aware" in tx_impact_json[0]
        assert "with_transcript_aware" in tx_impact_json[0]
        assert "changed_fields" in tx_impact_json[0]
    queue_dry_run = json.loads(queue_sheets_dry_run_path.read_text(encoding="utf-8"))
    assert queue_dry_run["mode"] == "dry_run"
    assert queue_dry_run["writer_scope"] == "deal_analyzer_only"
    assert "columns" in queue_dry_run and "rows" in queue_dry_run
    assert "why_in_queue_human" in queue_dry_run["columns"]
    assert "why_in_queue_technical" in queue_dry_run["columns"]
    if queue_dry_run["rows"]:
        first_row = queue_dry_run["rows"][0]
        assert first_row.get("why_in_queue_human")
        assert first_row.get("why_in_queue_technical")
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
    assert row["why_in_queue_technical"] == "active_risk"


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
