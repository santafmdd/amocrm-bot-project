import json
import shutil
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.config import load_config
from src.deal_analyzer.cli import (
    _call_role_signal,
    _build_dial_discipline_signals,
    _build_daily_control_sheet_payload,
    _merge_deal_company_tags,
    _normalize_phone_last7,
    _build_transcription_impact_row,
    _derive_product_hypothesis,
    _expected_quality_text,
    _expected_quantity_text,
    _llm_chat_json_with_runtime,
    _sanitize_daily_llm_columns,
    _daily_candidate_tier,
    _transcript_usability_score,
    _select_daily_package_records,
    _build_call_pool_artifacts,
    _build_transcription_shortlist_payload,
    _build_analysis_shortlist_payload,
    _build_daily_table_factual_payload,
    _expand_daily_rows_to_case_rows,
    _build_company_tag_propagation_dry_run_plan,
    _run_analyze_period,
)
from src.deal_analyzer.daily_case_modes import classify_daily_case, get_role_scope_policy
from src.deal_analyzer.config import DealAnalyzerConfig
from src.deal_analyzer.prompt_builder import build_daily_table_messages


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


def _cfg_hybrid() -> DealAnalyzerConfig:
    cfg = _cfg()
    return replace(cfg, analyzer_backend="hybrid")


def _snapshot_for_deal(deal_id: int, *, warnings=None, status_name: str = "В работе"):
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
        "call_evidence": {
            "items": [
                {
                    "call_id": f"c-{deal_id}",
                    "deal_id": str(deal_id),
                    "direction": "outbound",
                    "status": "secretary",
                    "phone": "+7 (999) 100-20-30",
                    "timestamp": "2026-04-16T10:15:00+00:00",
                    "duration_seconds": 55,
                    "recording_url": f"https://example.test/{deal_id}.mp3",
                    "audio_path": "",
                    "quality_flags": [],
                }
            ],
            "summary": {"calls_total": 1},
        },
        "transcripts": [
            {
                "call_id": f"c-{deal_id}",
                "transcript_status": "ok",
                "transcript_text": (
                    "Обсудили текущий процесс клиента, узкие места и что мешает сейчас. "
                    "Менеджер задал уточняющие вопросы, согласовали следующий шаг и время повторного контакта. "
                    "Отдельно зафиксировали, кто принимает решение и какие данные нужны до следующего звонка."
                ),
                "transcript_error": "",
            }
        ],
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
    call_pool_debug_json_path = run_dirs[0] / "call_pool_debug.json"
    call_pool_debug_md_path = run_dirs[0] / "call_pool_debug.md"
    conversation_pool_json_path = run_dirs[0] / "conversation_pool.json"
    conversation_pool_md_path = run_dirs[0] / "conversation_pool.md"
    discipline_pool_json_path = run_dirs[0] / "discipline_pool.json"
    discipline_pool_md_path = run_dirs[0] / "discipline_pool.md"
    discipline_report_json_path = run_dirs[0] / "discipline_report.json"
    discipline_report_md_path = run_dirs[0] / "discipline_report.md"
    transcription_shortlist_json_path = run_dirs[0] / "transcription_shortlist.json"
    transcription_shortlist_md_path = run_dirs[0] / "transcription_shortlist.md"
    call_review_payload_path = run_dirs[0] / "call_review_sheet_payload.json"
    daily_selection_debug_path = run_dirs[0] / "daily_selection_debug.json"
    company_tag_plan_json_path = run_dirs[0] / "company_tag_propagation_dry_run.json"
    company_tag_plan_md_path = run_dirs[0] / "company_tag_propagation_dry_run.md"
    assert summary_path.exists()
    assert summary_md_path.exists()
    assert top_risks_path.exists()
    assert manager_brief_path.exists()
    assert meeting_queue_json_path.exists()
    assert meeting_queue_md_path.exists()
    assert transcription_impact_md_path.exists()
    assert transcription_impact_json_path.exists()
    assert queue_sheets_dry_run_path.exists()
    assert call_pool_debug_json_path.exists()
    assert call_pool_debug_md_path.exists()
    assert conversation_pool_json_path.exists()
    assert conversation_pool_md_path.exists()
    assert discipline_pool_json_path.exists()
    assert discipline_pool_md_path.exists()
    assert discipline_report_json_path.exists()
    assert discipline_report_md_path.exists()
    assert transcription_shortlist_json_path.exists()
    assert transcription_shortlist_md_path.exists()
    assert call_review_payload_path.exists()
    assert daily_selection_debug_path.exists()
    assert company_tag_plan_json_path.exists()
    assert company_tag_plan_md_path.exists()
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
    assert "call_review_writer" in summary
    assert summary["call_review_writer"].get("mode") in {"dry_run", "real_write"}
    assert summary["daily_control_writer"].get("mode") == "inactive_for_analyze_period"
    assert summary["meeting_queue_writer"].get("mode") == "inactive_for_analyze_period"
    assert "call_review_rows_total" in summary
    assert "deals_total_before_limit" in summary
    assert "deals_with_any_calls" in summary
    assert "deals_with_recordings" in summary
    assert "company_tag_propagation_dry_run" in summary
    assert "deals_with_long_calls" in summary
    assert "deals_with_only_short_calls" in summary
    assert "deals_with_autoanswer_pattern" in summary
    assert "deals_with_redial_pattern" in summary
    assert "conversation_pool_total" in summary
    assert "discipline_pool_total" in summary
    assert "lpr_conversation_total" in summary
    assert "secretary_case_total" in summary
    assert "supplier_inbound_total" in summary
    assert "warm_inbound_total" in summary
    assert "redial_discipline_total" in summary
    assert "autoanswer_noise_total" in summary
    assert "transcription_shortlist_diagnostics" in summary
    assert "discipline_report_summary" in summary
    assert "daily_rows_from_conversation_pool" in summary
    assert "daily_rows_from_discipline_pool" in summary
    assert "daily_rows_skipped_crm_only" in summary
    assert "daily_rows_with_real_transcript" in summary
    assert "daily_rows_with_only_discipline_signals" in summary
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
    assert "transcriptions_usable" in tx_diag
    assert "transcriptions_weak" in tx_diag
    assert "transcriptions_noisy" in tx_diag
    assert "transcriptions_empty" in tx_diag
    assert "deals_with_usable_transcript" in tx_diag
    assert len(summary["artifact_paths"]) == 2
    md = summary_md_path.read_text(encoding="utf-8")
    assert "## Run Info" in md
    assert "## Score Aggregates" in md
    assert "## Top Risk Flags" in md
    assert "## Data Quality / Interpretation Confidence" in md
    assert "## Call-Aware Signals" in md
    assert "## Проверка транскрибации" in md
    assert "## E2E проверка звонков" in md
    assert "## Transcription Shortlist" in md
    assert "## negotiation_analysis" in md
    assert "## discipline_analysis" in md
    assert "## Weekly Meeting Focus" in md
    assert "### Что просело сильнее всего" in md
    assert "### Что можно исправить за 1 неделю" in md
    assert "### Что нельзя интерпретировать уверенно из-за качества CRM" in md
    assert "## Qualified Loss / Market Mismatch" in md
    assert "## Top 10 Most Risky Deals" in md
    assert "## Top 10 Highest Score Deals" in md
    assert "Call review writer:" in md
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
    call_pool_debug = json.loads(call_pool_debug_json_path.read_text(encoding="utf-8"))
    assert call_pool_debug["deals_total_before_limit"] == 2
    assert isinstance(call_pool_debug.get("items"), list)
    if call_pool_debug["items"]:
        first_pool = call_pool_debug["items"][0]
        for key in (
            "deal_id",
            "owner_name",
            "status_name",
            "pipeline_name",
            "runtime_effective_tags",
            "runtime_tag_source",
            "runtime_company_tag_promoted",
            "runtime_propagated_company_tags",
            "calls_total",
            "outbound_calls",
            "inbound_calls",
            "max_duration_seconds",
            "total_duration_seconds",
            "recording_url_count",
            "audio_path_count",
            "short_calls_0_20_count",
            "medium_calls_21_60_count",
            "long_calls_61_plus_count",
            "no_answer_like_count",
            "autoanswer_like_count",
            "repeated_dead_redial_count",
            "same_time_redial_pattern_flag",
            "unique_phone_count",
            "numbers_not_fully_covered_flag",
            "pool_type",
            "pool_reason",
            "pool_priority_score",
            "call_case_type",
        ):
            assert key in first_pool
    conversation_pool = json.loads(conversation_pool_json_path.read_text(encoding="utf-8"))
    discipline_pool = json.loads(discipline_pool_json_path.read_text(encoding="utf-8"))
    discipline_report = json.loads(discipline_report_json_path.read_text(encoding="utf-8"))
    assert "total" in conversation_pool and "items" in conversation_pool
    assert "total" in discipline_pool and "items" in discipline_pool
    assert "summary" in discipline_report and "items" in discipline_report
    if discipline_report["items"]:
        row = discipline_report["items"][0]
        for key in (
            "deal_id",
            "unique_phone_count",
            "attempts_total",
            "attempts_per_phone",
            "phones_over_2_attempts",
            "repeated_dead_redial_count",
            "same_time_redial_pattern_flag",
            "numbers_not_fully_covered_flag",
            "short_call_cluster_flag",
            "autoanswer_cluster_flag",
            "discipline_summary_short",
            "discipline_risk_level",
        ):
            assert key in row
    discipline_md = discipline_report_md_path.read_text(encoding="utf-8")
    assert "## Сделки, где дрочат один номер" in discipline_md
    assert "## Сделки, где не покрыты все номера" in discipline_md
    assert "## Сделки, где день ушел в пустые наборы" in discipline_md
    assert "## Сделки, где звонят в одно и то же время" in discipline_md
    transcription_shortlist = json.loads(transcription_shortlist_json_path.read_text(encoding="utf-8"))
    assert "items" in transcription_shortlist
    if transcription_shortlist["items"]:
        first = transcription_shortlist["items"][0]
        assert "selected_for_transcription" in first
        assert "transcription_selection_reason" in first
        assert "selected_call_ids" in first
        assert "selected_call_count" in first
    call_review_payload = json.loads(call_review_payload_path.read_text(encoding="utf-8"))
    assert call_review_payload.get("mode") == "call_review_sheet"
    assert "rows" in call_review_payload and isinstance(call_review_payload["rows"], list)
    if call_review_payload["rows"]:
        row = call_review_payload["rows"][0]
        assert "Deal ID" in row
        assert "Ссылка на сделку" in row
        assert "Тип кейса" in row
        assert str(row.get("Тип кейса") or "") not in {"warm_case", "supplier_inbound", "redial_discipline"}
    daily_selection_debug = json.loads(daily_selection_debug_path.read_text(encoding="utf-8"))
    assert "summary" in daily_selection_debug and "rows" in daily_selection_debug
    brief = manager_brief_path.read_text(encoding="utf-8")
    assert "# Manager Brief" in brief
    assert "Backend requested:" in brief
    assert "Backend used:" in brief
    assert "Owner ambiguity:" in brief
    assert "Низкая надежность интерпретации:" in brief
    assert "## Call-aware срез" in brief
    assert "## Проверка транскрибации" in brief
    assert "## negotiation_analysis" in brief
    assert "## discipline_analysis" in brief
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
                "daily_manager_allowlist": ("Илья", "Рустам"),
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
    llm_columns = {
        "Ключевой вывод": "Живой вывод по дню.",
        "Сильные стороны": "Есть рабочий контакт.",
        "Зоны роста": "Дожать следующий шаг",
        "Почему это важно": "Сотруднику проще дожимать; отделу видно реальное движение.",
        "Что закрепить": "Закрепить модуль фиксации шага.",
        "Что исправить": "Сразу фиксировать дату следующего касания.",
        "Что донес сотруднику": "1) Разобрали звонок.\n2) Дали модуль шага.\n3) Применяет в следующих касаниях.",
        "Ожидаемый эффект - количество": "+1 рабочий контакт за неделю",
        "Ожидаемый эффект - качество": "Этап станет управляемее.",
        "_llm_text_ready": True,
    }

    with patch("src.deal_analyzer.cli._prepare_call_review_llm_fields", side_effect=_mock_prepare_call_review_llm_fields), patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli._read_sheet_header_columns",
        return_value=["Дата анализа", "Дата кейса", "Менеджер", "Тип кейса", "Deal ID"],
    ), patch(
        "src.deal_analyzer.cli._resolve_daily_llm_runtime",
        return_value={"enabled": True, "selected": "main", "reason": "main_ok", "main_ok": True, "fallback_ok": False, "main": {"base_url": "http://m", "model": "m", "timeout_seconds": 10}},
    ), patch(
        "src.deal_analyzer.cli._generate_daily_table_text_columns",
        return_value=llm_columns,
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
    writer = summary.get("call_review_writer", {})
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
                "daily_manager_allowlist": ("Илья", "Рустам"),
            }
        )

    llm_columns = {
        "Ключевой вывод": "Живой вывод по дню.",
        "Сильные стороны": "Есть рабочий контакт.",
        "Зоны роста": "Дожать следующий шаг",
        "Почему это важно": "Сотруднику проще дожимать; отделу видно реальное движение.",
        "Что закрепить": "Закрепить модуль фиксации шага.",
        "Что исправить": "Сразу фиксировать дату следующего касания.",
        "Что донес сотруднику": "1) Разобрали звонок.\n2) Дали модуль шага.\n3) Применяет в следующих касаниях.",
        "Ожидаемый эффект - количество": "+1 рабочий контакт за неделю",
        "Ожидаемый эффект - качество": "Этап станет управляемее.",
        "_llm_text_ready": True,
    }

    with patch("src.deal_analyzer.cli._prepare_call_review_llm_fields", side_effect=_mock_prepare_call_review_llm_fields), patch(
        "src.deal_analyzer.cli._resolve_daily_llm_runtime",
        return_value={"enabled": True, "selected": "main", "reason": "main_ok", "main_ok": True, "fallback_ok": False, "main": {"base_url": "http://m", "model": "m", "timeout_seconds": 10}},
    ), patch(
        "src.deal_analyzer.cli._generate_daily_table_text_columns",
        return_value=llm_columns,
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
    writer = summary.get("call_review_writer", {})
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
    llm_columns = {
        "Ключевой вывод": "Живой вывод по дню.",
        "Сильные стороны": "Есть рабочий контакт.",
        "Зоны роста": "Дожать следующий шаг",
        "Почему это важно": "Сотруднику проще дожимать; отделу видно реальное движение.",
        "Что закрепить": "Закрепить модуль фиксации шага.",
        "Что исправить": "Сразу фиксировать дату следующего касания.",
        "Что донес сотруднику": "1) Разобрали звонок.\n2) Дали модуль шага.\n3) Применяет в следующих касаниях.",
        "Ожидаемый эффект - количество": "+1 рабочий контакт за неделю",
        "Ожидаемый эффект - качество": "Этап станет управляемее.",
        "_llm_text_ready": True,
    }

    with patch("src.deal_analyzer.cli._prepare_call_review_llm_fields", side_effect=_mock_prepare_call_review_llm_fields), patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli._read_sheet_header_columns",
        return_value=["Дата анализа", "Дата кейса", "Менеджер", "Тип кейса", "Deal ID"],
    ), patch(
        "src.deal_analyzer.cli._resolve_daily_llm_runtime",
        return_value={"enabled": True, "selected": "main", "reason": "main_ok", "main_ok": True, "fallback_ok": False, "main": {"base_url": "http://m", "model": "m", "timeout_seconds": 10}},
    ), patch(
        "src.deal_analyzer.cli._generate_daily_table_text_columns",
        return_value=llm_columns,
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
    writer = summary.get("call_review_writer", {})
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
    assert payload["sheet_name"] == "Разбор звонков"
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


def test_expand_daily_rows_to_case_rows_writes_one_row_per_meaningful_case():
    rows = [
        {
            "Дата контроля": "2026-04-21",
            "Менеджер": "Илья",
            "llm_text_ready": True,
            "Проанализировано сделок": 3,
            "Ссылки на сделки": "legacy",
            "selection_candidates_debug": [
                {"deal_id": "32162059", "daily_tier": "priority_1_meaningful_conversation", "transcript_usability_label": "usable"},
                {"deal_id": "32160389", "daily_tier": "priority_6_noise", "skip_for_daily_reason": "noise_short_or_autoanswer"},
                {"deal_id": "32165731", "daily_tier": "priority_2_secretary_case", "call_role_signal": "secretary"},
            ],
        }
    ]
    out = _expand_daily_rows_to_case_rows(rows=rows, base_domain="https://example.amocrm.ru")
    assert len(out) == 2
    links = [str(x.get("Ссылки на сделки") or "") for x in out]
    assert any("32162059" in x for x in links)
    assert any("32165731" in x for x in links)
    assert all("32160389" not in x for x in links)


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
                "daily_manager_allowlist": ("Илья", "Рустам"),
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
    llm_columns = {
        "Ключевой вывод": "Живой вывод по дню.",
        "Сильные стороны": "Есть рабочий контакт.",
        "Зоны роста": "Дожать следующий шаг",
        "Почему это важно": "Сотруднику проще дожимать; отделу видно реальное движение.",
        "Что закрепить": "Закрепить модуль фиксации шага.",
        "Что исправить": "Сразу фиксировать дату следующего касания.",
        "Что донес сотруднику": "1) Разобрали звонок.\n2) Дали модуль шага.\n3) Применяет в следующих касаниях.",
        "Ожидаемый эффект - количество": "+1 рабочий контакт за неделю",
        "Ожидаемый эффект - качество": "Этап станет управляемее.",
        "_llm_text_ready": True,
    }
    with patch("src.deal_analyzer.cli._prepare_call_review_llm_fields", side_effect=_mock_prepare_call_review_llm_fields), patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli._read_sheet_header_columns",
        return_value=["Дата анализа", "Дата кейса", "Менеджер", "Тип кейса", "Deal ID"],
    ), patch(
        "src.deal_analyzer.cli._resolve_daily_llm_runtime",
        return_value={"enabled": True, "selected": "main", "reason": "main_ok", "main_ok": True, "fallback_ok": False, "main": {"base_url": "http://m", "model": "m", "timeout_seconds": 10}},
    ), patch(
        "src.deal_analyzer.cli._generate_daily_table_text_columns",
        return_value=llm_columns,
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
                "daily_manager_allowlist": ("Илья", "Рустам"),
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
    llm_columns = {
        "Ключевой вывод": "Живой вывод по дню.",
        "Сильные стороны": "Есть рабочий контакт.",
        "Зоны роста": "Дожать следующий шаг",
        "Почему это важно": "Сотруднику проще дожимать; отделу видно реальное движение.",
        "Что закрепить": "Закрепить модуль фиксации шага.",
        "Что исправить": "Сразу фиксировать дату следующего касания.",
        "Что донес сотруднику": "1) Разобрали звонок.\n2) Дали модуль шага.\n3) Применяет в следующих касаниях.",
        "Ожидаемый эффект - количество": "+1 рабочий контакт за неделю",
        "Ожидаемый эффект - качество": "Этап станет управляемее.",
        "_llm_text_ready": True,
    }
    with patch("src.deal_analyzer.cli._prepare_call_review_llm_fields", side_effect=_mock_prepare_call_review_llm_fields), patch("src.deal_analyzer.cli.GoogleSheetsApiClient", return_value=fake_client), patch(
        "src.deal_analyzer.cli.load_config",
        return_value=SimpleNamespace(project_root=Path("D:/AI_Automation/amocrm_bot/project")),
    ), patch(
        "src.deal_analyzer.cli._resolve_daily_llm_runtime",
        return_value={"enabled": True, "selected": "main", "reason": "main_ok", "main_ok": True, "fallback_ok": False, "main": {"base_url": "http://m", "model": "m", "timeout_seconds": 10}},
    ), patch(
        "src.deal_analyzer.cli._generate_daily_table_text_columns",
        return_value=llm_columns,
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
    writer = summary.get("call_review_writer", {})
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
        "_llm_text_ready": True,
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


def test_daily_selection_prefers_call_rich_and_can_stay_thin():
    records = [
        {
            "deal_id": 1001,
            "updated_at": "2026-04-14T10:00:00+00:00",
            "transcript_available": True,
            "transcript_text_excerpt": "Обсудили боль, задачу, ЛПР и следующий шаг с датой",
            "call_signal_summary_short": "разговор предметный, следующий шаг подтвержден",
            "notes_summary_raw": [{"text": "фиксировали задачу"}],
            "tasks_summary_raw": [{"text": "контроль даты"}],
            "risk_flags": [],
        },
        {
            "deal_id": 1002,
            "updated_at": "2026-04-14T10:05:00+00:00",
            "transcript_available": False,
            "transcript_text_excerpt": "",
            "call_signal_summary_short": "",
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
            "risk_flags": [],
        },
        {
            "deal_id": 1003,
            "updated_at": "2026-04-14T10:10:00+00:00",
            "transcript_available": False,
            "transcript_text_excerpt": "",
            "call_signal_summary_short": "",
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
            "risk_flags": [],
        },
    ]
    selected = _select_daily_package_records(
        manager_records=records,
        control_day="2026-04-14",
        package_target=6,
        carryover_days=7,
    )
    selected_ids = [int(x.get("deal_id")) for x in selected]
    assert 1001 in selected_ids
    assert len(selected_ids) == 1


def test_daily_selection_blocks_crm_only_and_discipline_when_negotiation_exists():
    records = [
        {
            "deal_id": 4101,
            "updated_at": "2026-04-14T09:00:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_available": True,
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 78,
            "transcript_text_excerpt": "Говорили с ЛПР, договорились про следующий шаг",
            "call_signal_summary_short": "ЛПР и следующий шаг",
            "notes_summary_raw": [{"text": "контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
            "risk_flags": [],
        },
        {
            "deal_id": 4102,
            "updated_at": "2026-04-14T09:10:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_available": False,
            "transcript_usability_label": "empty",
            "transcript_usability_score_final": 0,
            "transcript_text_excerpt": "",
            "call_signal_summary_short": "",
            "notes_summary_raw": [{"text": "очень бедный комментарий"}],
            "tasks_summary_raw": [],
            "risk_flags": [],
        },
        {
            "deal_id": 4103,
            "updated_at": "2026-04-14T09:20:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_available": False,
            "transcript_usability_label": "empty",
            "transcript_usability_score_final": 0,
            "transcript_text_excerpt": "",
            "call_signal_summary_short": "",
            "repeated_dead_redial_day_flag": True,
            "repeated_dead_redial_count": 3,
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
            "risk_flags": [],
        },
    ]
    selected = _select_daily_package_records(
        manager_records=records,
        control_day="2026-04-14",
        package_target=3,
        carryover_days=7,
        exclude_deal_ids=set(),
    )
    selected_ids = {int(x.get("deal_id")) for x in selected if str(x.get("deal_id") or "").isdigit()}
    assert selected_ids == {4101}


def test_daily_payload_has_ranking_debug_fields():
    summary = {
        "period_start": "2026-04-13",
        "period_end": "2026-04-19",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [{"deal_id": 940, "owner_name": "Илья", "score": 55, "status_name": "В работе"}]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    row = payload["rows"][0]
    for key in (
        "transcript_usability_score",
        "evidence_richness_score",
        "funnel_relevance_score",
        "daily_selection_rank",
        "daily_selection_reason",
    ):
        assert key in row
    assert row["daily_selection_reason"] in {"has_call_priority", "rich_context_priority", "fallback_fill"}


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




def test_daily_selection_skips_weak_transcript_when_stronger_candidate_exists():
    records = [
        {
            "deal_id": 1,
            "updated_at": 1713132000,
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 80,
            "transcript_available": True,
            "call_signal_summary_short": "есть следующий шаг",
            "notes_summary_raw": [{"text": "контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
            "risk_flags": [],
            "score": 60,
        },
        {
            "deal_id": 2,
            "updated_at": 1713132000,
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_usability_label": "noisy",
            "transcript_usability_score_final": 10,
            "transcript_available": True,
            "call_signal_summary_short": "",
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
            "risk_flags": [],
            "score": 60,
        },
    ]
    selected = _select_daily_package_records(
        manager_records=records,
        control_day="2024-04-15",
        package_target=6,
        carryover_days=7,
        exclude_deal_ids=set(),
    )
    ids = {int(x.get("deal_id")) for x in selected if str(x.get("deal_id") or "").isdigit()}
    assert 1 in ids
    assert 2 not in ids


def test_daily_selection_keeps_secretary_fallback_when_lpr_missing():
    records = [
        {
            "deal_id": 11,
            "updated_at": 1713132000,
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_usability_label": "weak",
            "transcript_usability_score_final": 20,
            "transcript_available": True,
            "call_signal_summary_short": "Секретарь: перезвонить после обеда",
            "notes_summary_raw": [{"text": "секретарь попросил перенабрать"}],
            "tasks_summary_raw": [],
            "risk_flags": [],
            "score": 50,
        },
        {
            "deal_id": 12,
            "updated_at": 1713132000,
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_usability_label": "empty",
            "transcript_usability_score_final": 0,
            "transcript_available": False,
            "call_signal_summary_short": "",
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
            "risk_flags": [],
            "score": 50,
        },
    ]
    selected = _select_daily_package_records(
        manager_records=records,
        control_day="2024-04-15",
        package_target=2,
        carryover_days=7,
        exclude_deal_ids=set(),
    )
    assert selected
    assert int(selected[0].get("deal_id")) == 11
    assert str(selected[0].get("skip_for_daily_reason") or "") == ""


def test_daily_ranking_prioritizes_lpr_and_secretary_over_autoanswer_control_cases():
    records = [
        {
            "deal_id": 32162059,
            "updated_at": "2026-04-22T12:26:24+00:00",
            "status_name": "Первый контакт. Квалификация",
            "pipeline_name": "Привлечение",
            "transcript_available": True,
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 78,
            "transcript_text_excerpt": "Говорили с ЛПР, согласовали демонстрацию и следующий звонок.",
            "call_signal_summary_short": "ЛПР на связи, есть договоренность о следующем шаге",
            "notes_summary_raw": [{"text": "есть контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
        },
        {
            "deal_id": 32165731,
            "updated_at": "2026-04-22T09:46:42+00:00",
            "status_name": "Закрыто и не реализовано",
            "pipeline_name": "Привлечение",
            "transcript_available": True,
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 62,
            "transcript_text_excerpt": "Секретарь объяснил маршрутизацию через почту и отдел продаж.",
            "call_signal_summary_short": "Секретарь дал маршрут, нужно корректно дожать следующий шаг",
            "notes_summary_raw": [{"text": "секретарь"}],
            "tasks_summary_raw": [],
        },
        {
            "deal_id": 32160389,
            "updated_at": "2026-04-21T07:41:58+00:00",
            "status_name": "Верификация",
            "pipeline_name": "Привлечение",
            "transcript_available": True,
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 57,
            "transcript_text_excerpt": "К сожалению, абонент сейчас не может ответить. Оставьте сообщение после сигнала.",
            "call_signal_summary_short": "",
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
        },
    ]
    selected = _select_daily_package_records(
        manager_records=records,
        control_day="2026-04-22",
        package_target=6,
        carryover_days=7,
        exclude_deal_ids=set(),
    )
    selected_ids = [int(x.get("deal_id")) for x in selected if str(x.get("deal_id") or "").isdigit()]
    assert 32162059 in selected_ids
    assert 32165731 in selected_ids
    assert 32160389 not in selected_ids
    skipped = []
    for row in selected:
        skipped.extend(row.get("_daily_skipped_candidates", []) if isinstance(row, dict) else [])
    skip_32160389 = next((x for x in skipped if str(x.get("deal_id") or "") == "32160389"), {})
    assert str(skip_32160389.get("skip_for_daily_reason") or "") in {
        "autoanswer_low_priority_when_real_conversation_exists",
        "discipline_deferred_due_to_negotiation_cases",
    }


def test_control_cases_have_expected_daily_ranking_signals():
    lpr_case = {
        "deal_id": 32162059,
        "transcript_available": True,
        "transcript_usability_label": "usable",
        "transcript_usability_score_final": 80,
        "transcript_text_excerpt": "Говорили с ЛПР, согласовали следующий шаг.",
        "call_signal_summary_short": "ЛПР",
        "notes_summary_raw": [{"text": "контекст"}],
        "tasks_summary_raw": [{"text": "задача"}],
    }
    secretary_case = {
        "deal_id": 32165731,
        "transcript_available": True,
        "transcript_usability_label": "usable",
        "transcript_usability_score_final": 60,
        "transcript_text_excerpt": "Секретарь объяснил, куда отправить письмо для закупки.",
        "call_signal_summary_short": "секретарь",
        "notes_summary_raw": [{"text": "секретарь"}],
        "tasks_summary_raw": [],
    }
    autoanswer_case = {
        "deal_id": 32160389,
        "transcript_available": True,
        "transcript_usability_label": "usable",
        "transcript_usability_score_final": 57,
        "transcript_text_excerpt": "Абонент сейчас не может ответить, перезвоните позже.",
        "call_signal_summary_short": "",
        "notes_summary_raw": [],
        "tasks_summary_raw": [],
    }

    assert _call_role_signal(lpr_case) == "lpr"
    assert _call_role_signal(secretary_case) == "secretary"
    assert _call_role_signal(autoanswer_case) == "autoanswer"
    assert _transcript_usability_score(autoanswer_case) == 0

    lpr_tier, _ = _daily_candidate_tier(
        lpr_case,
        transcript_score=_transcript_usability_score(lpr_case),
        evidence_score=6,
    )
    sec_tier, _ = _daily_candidate_tier(
        secretary_case,
        transcript_score=_transcript_usability_score(secretary_case),
        evidence_score=4,
    )
    auto_tier, _ = _daily_candidate_tier(
        autoanswer_case,
        transcript_score=_transcript_usability_score(autoanswer_case),
        evidence_score=1,
    )

    assert lpr_tier < sec_tier
    assert sec_tier < auto_tier


def test_daily_selection_uses_redial_fallback_when_no_usable_conversations():
    records = [
        {
            "deal_id": 9901,
            "updated_at": "2026-04-22T10:00:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_available": False,
            "transcript_usability_label": "empty",
            "transcript_usability_score_final": 0,
            "transcript_text_excerpt": "",
            "call_signal_summary_short": "",
            "repeated_dead_redial_day_flag": True,
            "repeated_dead_redial_count": 3,
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
        },
        {
            "deal_id": 9902,
            "updated_at": "2026-04-22T10:05:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "transcript_available": True,
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 56,
            "transcript_text_excerpt": "Абонент сейчас не может ответить, оставьте сообщение.",
            "call_signal_summary_short": "",
            "notes_summary_raw": [],
            "tasks_summary_raw": [],
        },
    ]
    selected = _select_daily_package_records(
        manager_records=records,
        control_day="2026-04-22",
        package_target=2,
        carryover_days=7,
        exclude_deal_ids=set(),
    )
    selected_ids = [int(x.get("deal_id")) for x in selected if str(x.get("deal_id") or "").isdigit()]
    assert 9901 in selected_ids


def test_llm_rerank_receives_limited_shortlist_only():
    records = []
    for i in range(1, 25):
        records.append(
            {
                "deal_id": i,
                "updated_at": 1713132000,
                "status_name": "В работе",
                "pipeline_name": "Привлечение",
                "transcript_usability_label": "usable",
                "transcript_usability_score_final": 75,
                "transcript_available": True,
                "call_signal_summary_short": "есть контекст",
                "notes_summary_raw": [{"text": "note"}],
                "tasks_summary_raw": [{"text": "task"}],
                "risk_flags": [],
                "score": 60,
            }
        )
    cfg = replace(_cfg(), analyzer_backend="hybrid")
    seen_sizes = []

    def _fake_rerank(**kwargs):
        seen_sizes.append(len(kwargs.get("candidates", [])))
        return {}

    with patch("src.deal_analyzer.cli._llm_daily_rerank_candidates", side_effect=_fake_rerank):
        _select_daily_package_records(
            manager_records=records,
            control_day="2024-04-15",
            package_target=6,
            carryover_days=7,
            exclude_deal_ids=set(),
            cfg=cfg,
            logger=_Logger(),
            backend_effective="hybrid",
            manager="Илья",
            role="менеджер по продажам",
            style_source_excerpt="style",
        )

    assert seen_sizes
    assert max(seen_sizes) <= 12


def test_daily_payload_contains_effect_forecast_debug_and_quantity_in_units():
    summary = {
        "period_start": "2026-04-14",
        "period_end": "2026-04-18",
        "run_timestamp": "2026-04-18T12:00:00+00:00",
    }
    records = [
        {
            "deal_id": 100,
            "owner_name": "Илья",
            "updated_at": 1713132000,
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "score": 55,
            "risk_flags": [],
            "notes_summary_raw": [{"text": "Есть контекст"}],
            "tasks_summary_raw": [{"text": "Есть задача"}],
            "transcript_available": True,
            "transcript_text_excerpt": "Обсудили следующий шаг и бюджет",
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 78,
            "call_signal_summary_short": "В разговоре есть следующий шаг",
            "product_hypothesis": "info",
        }
    ]
    payload = _build_daily_control_sheet_payload(
        summary=summary,
        period_deal_records=records,
        manager_allowlist=["Илья", "Рустам"],
        cfg=_cfg(),
        logger=_Logger(),
        backend_effective="rules",
        style_source_excerpt="",
    )
    assert payload["rows"]
    row = payload["rows"][0]
    assert row.get("effect_forecast_source") in {"fallback", "roks"}
    assert row.get("effect_problem_stage")
    assert row.get("effect_downstream_stages")
    assert "%" not in str(row.get("Ожидаемый эффект - количество", ""))


def test_daily_payload_contains_package_quality_and_generation_diagnostics():
    summary = {
        "period_start": "2026-04-14",
        "period_end": "2026-04-18",
        "run_timestamp": "2026-04-18T12:00:00+00:00",
    }
    records = [
        {
            "deal_id": 201,
            "owner_name": "Илья",
            "updated_at": 1713132000,
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "score": 58,
            "risk_flags": [],
            "notes_summary_raw": [{"text": "контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
            "transcript_available": True,
            "transcript_text_excerpt": "Обсудили шаг и сроки",
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 80,
            "call_signal_summary_short": "Есть следующий шаг",
            "product_hypothesis": "info",
            "transcript_quality_retry_used": True,
            "transcript_quality_retry_improved": True,
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records, cfg=_cfg(), logger=_Logger())
    row = payload["rows"][0]
    assert row.get("daily_package_quality_label") in {"strong", "acceptable", "thin", "weak"}
    assert "daily_package_has_forced_fallback" in row
    assert "negotiation_signal_presence_score" in row
    assert "crm_only_bias_flag" in row
    assert "style_layer_applied" in row
    assert isinstance(row.get("text_generation_source_per_column"), dict)
    assert "role_scope_applied" in row
    assert "role_blocked_topics" in row
    assert "role_allowed_topics" in row
    assert "role_scope_conflict_flag" in row
    assert row.get("transcript_quality_retry_used") is True
    assert row.get("transcript_quality_retry_improved") is True
    assert row.get("daily_primary_source") in {"conversation_pool", "discipline_pool"}
    assert row.get("daily_case_type")
    assert row.get("daily_selection_reason_v2")
    assert "excluded_crm_only_cases_count" in row


def test_daily_payload_antirepeat_rewrites_duplicate_key_takeaway_for_same_manager():
    summary = {
        "period_start": "2026-04-14",
        "period_end": "2026-04-15",
        "run_timestamp": "2026-04-15T12:00:00+00:00",
    }
    base = {
        "owner_name": "Илья",
        "status_name": "В работе",
        "pipeline_name": "Привлечение",
        "score": 55,
        "risk_flags": [],
        "notes_summary_raw": [{"text": "контекст"}],
        "tasks_summary_raw": [{"text": "задача"}],
        "transcript_available": True,
        "transcript_text_excerpt": "Обсудили шаг",
        "transcript_usability_label": "usable",
        "transcript_usability_score_final": 76,
        "call_signal_summary_short": "Есть шаг",
        "product_hypothesis": "info",
    }
    records = [
        {**base, "deal_id": 3101, "updated_at": "2026-04-14T10:00:00+00:00"},
        {**base, "deal_id": 3102, "updated_at": "2026-04-15T10:00:00+00:00"},
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records, cfg=_cfg(), logger=_Logger())
    rows = [r for r in payload["rows"] if str(r.get("Менеджер") or "") == "Илья"]
    assert len(rows) >= 2
    assert str(rows[0].get("Ключевой вывод") or "").strip()
    assert str(rows[1].get("Ключевой вывод") or "").strip()
    assert str(rows[0].get("Ключевой вывод") or "").strip() != str(rows[1].get("Ключевой вывод") or "").strip()


def test_phone_normalization_uses_last_7_digits():
    assert _normalize_phone_last7("+7 (999) 123-45-67") == "1234567"
    assert _normalize_phone_last7("8-999-123-45-67") == "1234567"
    assert _normalize_phone_last7("12345") == ""


def test_dial_discipline_detects_dead_redials_and_same_time_pattern():
    snapshot = {
        "call_evidence": {
            "items": [
                {"phone": "+7 900 111 22 33", "status": "no_answer", "timestamp": "2026-04-21T10:00:00+00:00"},
                {"phone": "8(900)1112233", "status": "busy", "timestamp": "2026-04-21T10:00:30+00:00"},
                {"phone": "9001112233", "status": "voicemail", "timestamp": "2026-04-21T10:00:50+00:00"},
            ]
        }
    }
    sig = _build_dial_discipline_signals(snapshot, status_name="В работе")
    assert sig["dial_unique_phones_count"] == 1
    assert sig["dial_attempts_total"] == 3
    assert sig["repeated_dead_redial_day_flag"] is True
    assert sig["same_time_redial_pattern_flag"] is True
    assert sig["dial_discipline_pattern_label"] == "red_flag"


def test_secretary_touch_counts_as_attempt():
    snapshot = {
        "call_evidence": {
            "items": [
                {"phone": "+7 900 222 33 44", "status": "secretary", "timestamp": "2026-04-21T11:20:00+00:00"}
            ]
        }
    }
    sig = _build_dial_discipline_signals(snapshot, status_name="В работе")
    assert sig["dial_attempts_total"] == 1
    assert sig["dial_unique_phones_count"] == 1


def test_dial_discipline_tracks_known_vs_attempted_phones_for_closed_deal():
    snapshot = {
        "normalized_deal": {
            "contact_phone": "+7 (900) 111-22-33",
            "phones": ["8 900 222 33 44", "+7 (900) 333-44-55"],
        },
        "call_evidence": {
            "items": [
                {"phone": "+7 900 1112233", "status": "no_answer", "timestamp": "2026-04-21T10:00:00+00:00"},
                {"phone": "8(900)1112233", "status": "busy", "timestamp": "2026-04-21T12:00:00+00:00"},
            ]
        },
    }
    sig = _build_dial_discipline_signals(snapshot, status_name="Закрыто и не реализовано")
    assert sig["dial_known_unique_phones_count"] == 3
    assert sig["dial_unique_phones_count"] == 1
    assert sig["dial_attempted_phones"] == ["1112233"]
    assert sorted(sig["dial_not_attempted_phones"]) == ["2223344", "3334455"]
    assert sig["numbers_not_fully_covered_flag"] is True


def test_dial_discipline_attempt_types_include_autoanswer_no_answer_short_drop_and_secretary():
    snapshot = {
        "call_evidence": {
            "items": [
                {"phone": "+7 900 111 22 33", "status": "secretary", "timestamp": "2026-04-21T10:00:00+00:00", "duration_seconds": 18},
                {"phone": "+7 900 111 22 33", "status": "no_answer", "timestamp": "2026-04-21T10:10:00+00:00", "duration_seconds": 0},
                {"phone": "+7 900 111 22 33", "status": "autoanswer", "timestamp": "2026-04-21T10:20:00+00:00", "duration_seconds": 3},
                {"phone": "+7 900 111 22 33", "status": "", "timestamp": "2026-04-21T10:30:00+00:00", "duration_seconds": 2},
            ]
        }
    }
    sig = _build_dial_discipline_signals(snapshot, status_name="В работе")
    assert sig["dial_attempts_total"] == 4
    assert sig["dial_secretary_touch_count"] == 1
    assert sig["dial_no_answer_attempts_count"] == 1
    assert sig["dial_autoanswer_attempts_count"] == 1
    assert sig["dial_short_drop_attempts_count"] >= 1


def test_dial_discipline_detects_day_time_patterns_and_massive_empty_attempts():
    items = []
    for idx in range(10):
        items.append(
            {
                "phone": "+7 900 777 88 99",
                "status": "no_answer",
                "timestamp": f"2026-04-21T10:{idx:02d}:00+00:00",
            }
        )
    items.extend(
        [
            {"phone": "+7 900 777 88 99", "status": "busy", "timestamp": "2026-04-22T10:05:00+00:00"},
            {"phone": "+7 900 777 88 99", "status": "busy", "timestamp": "2026-04-23T10:05:00+00:00"},
            {"phone": "+7 900 777 88 99", "status": "busy", "timestamp": "2026-04-23T15:05:00+00:00"},
        ]
    )
    snapshot = {"call_evidence": {"items": items}}
    sig = _build_dial_discipline_signals(snapshot, status_name="В работе")
    assert sig["same_day_repeat_attempts_flag"] is True
    assert sig["different_days_same_time_flag"] is True
    assert sig["different_days_different_time_flag"] is True
    assert sig["massive_empty_attempts_day_flag"] is True
    assert sig["dial_redial_suspicion_flag"] is True


def test_daily_payload_can_use_dial_discipline_mode_without_negotiation():
    summary = {
        "period_start": "2026-04-20",
        "period_end": "2026-04-22",
        "run_timestamp": "2026-04-22T10:00:00+00:00",
    }
    records = [
        {
            "deal_id": 401,
            "owner_name": "Рустам",
            "updated_at": "2026-04-21T12:00:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "score": 42,
            "risk_flags": [],
            "notes_summary_raw": [{"text": "недозвоны"}],
            "tasks_summary_raw": [],
            "transcript_available": False,
            "transcript_text_excerpt": "",
            "transcript_usability_label": "empty",
            "transcript_usability_score_final": 0,
            "call_signal_summary_short": "",
            "repeated_dead_redial_day_flag": True,
            "repeated_dead_redial_count": 3,
            "same_time_redial_pattern_flag": True,
            "numbers_not_fully_covered_flag": True,
        }
    ]
    llm_cols = {
        "Ключевой вывод": "День ушел в повторные недозвоны по одним и тем же номерам.",
        "Сильные стороны": "",
        "Зоны роста": "Дисциплина набора и смена времени дозвона",
        "Почему это важно": "Сотруднику быстрее даст рабочие контакты, отделу снимет верхний шум.",
        "Что закрепить": "Модуль жесткой фиксации окна перезвона.",
        "Что исправить": "Остановить подрядные перезвоны в одно и то же время.",
        "Что донес сотруднику": "1) Разобрали паттерн недозвонов.\n2) Дали модуль смены окна дозвона.\n3) В следующих касаниях тестирует новый слот.",
        "Ожидаемый эффект - количество": "+1-2 живых контакта за неделю",
        "Ожидаемый эффект - качество": "Станет чище верхний этап и меньше пустых касаний.",
        "_llm_text_ready": True,
    }
    with patch(
        "src.deal_analyzer.cli._run_daily_multistep_pipeline",
        return_value={"ok": True, "columns": {k: v for k, v in llm_cols.items() if not k.startswith("_")}, "step_artifacts": {}, "source_of_truth": "styled_blocks", "assembler_only": True},
    ):
        payload = _build_daily_control_sheet_payload(
            summary=summary,
            period_deal_records=records,
            cfg=replace(_cfg(), analyzer_backend="hybrid"),
            backend_effective="hybrid",
            llm_runtime={"selected": "main", "main_ok": True, "main": {"base_url": "http://x", "model": "m", "timeout_seconds": 10}},
            daily_step_artifacts_dir=Path("workspace/tmp_tests/deal_analyzer/daily_steps_dial_discipline"),
        )
    assert payload["rows"]
    assert payload["rows"][0].get("daily_analysis_mode") == "discipline_analysis"


def test_no_real_write_when_daily_llm_unavailable():
    output_dir = _fresh_output_dir("period_batch_no_llm_write")
    payload = {"normalized_deals": [{"deal_id": 1}]}
    logger = _Logger()
    cfg = replace(_cfg(), analyzer_backend="hybrid", deal_analyzer_write_enabled=True)

    with patch("src.deal_analyzer.cli.build_deal_snapshot", side_effect=lambda **kw: _snapshot_for_deal(int(kw["normalized_deal"]["deal_id"]))), patch(
        "src.deal_analyzer.cli._analyze_one_with_isolation",
        side_effect=lambda normalized, cfg, logger, *, deal_hint, backend_override: _analysis_for_deal(int(normalized["deal_id"])),
    ), patch(
        "src.deal_analyzer.cli._resolve_daily_llm_runtime",
        return_value={"enabled": True, "selected": "none", "reason": "main_and_fallback_failed", "main_ok": False, "fallback_ok": False},
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
    writer = summary.get("call_review_writer", {})
    assert writer.get("mode") == "dry_run"
    assert writer.get("enabled") is False
    assert writer.get("error") == "write_forced_dry_run_no_live_llm"
    assert summary.get("daily_control_writer", {}).get("mode") == "inactive_for_analyze_period"
    llm_runtime = summary.get("daily_llm_runtime", {})
    assert llm_runtime.get("selected") == "none"


def test_llm_runtime_generation_uses_fallback_when_main_call_fails():
    runtime = {
        "selected": "main",
        "main_ok": True,
        "fallback_ok": True,
        "main": {"base_url": "http://main", "model": "m1", "timeout_seconds": 5},
        "fallback": {"base_url": "http://fb", "model": "m2", "timeout_seconds": 5},
    }

    class _MainClient:
        def chat_json(self, *, messages):
            raise RuntimeError("main_generation_failed")

    class _FallbackClient:
        def chat_json(self, *, messages):
            return SimpleNamespace(payload={"key_takeaway": "ok"})

    def _fake_make(runtime_payload):
        selected = str(runtime_payload.get("selected") or "")
        if selected == "main":
            return _MainClient()
        if selected == "fallback":
            return _FallbackClient()
        return None

    with patch("src.deal_analyzer.cli._make_llm_client_from_runtime", side_effect=_fake_make):
        payload, source = _llm_chat_json_with_runtime(
            runtime=runtime,
            messages=[{"role": "user", "content": "{}"}],
            repair_messages=None,
            logger=_Logger(),
            log_prefix="test",
        )

    assert isinstance(payload, dict)
    assert source == "fallback"


def test_company_tags_are_propagated_to_deal_tags():
    merged, company, propagated = _merge_deal_company_tags(
        deal_tags=["expo", "priority"],
        company_tags=["priority", "roks", "expo-2026"],
    )
    assert "roks" in merged
    assert "expo-2026" in merged
    assert "roks" in propagated
    assert "priority" not in propagated
    assert company == ["expo-2026", "priority", "roks"]


def test_daily_payload_drops_rows_when_llm_text_not_ready():
    summary = {
        "period_start": "2026-04-20",
        "period_end": "2026-04-22",
        "run_timestamp": "2026-04-22T10:00:00+00:00",
    }
    records = [
        {
            "deal_id": 501,
            "owner_name": "Илья",
            "updated_at": "2026-04-21T12:00:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "score": 55,
            "risk_flags": [],
            "notes_summary_raw": [{"text": "контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
            "transcript_available": True,
            "transcript_text_excerpt": "разговор",
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 80,
            "call_signal_summary_short": "есть следующий шаг",
        }
    ]
    with patch("src.deal_analyzer.cli._generate_daily_table_text_columns", return_value={"_llm_text_ready": False}):
        payload = _build_daily_control_sheet_payload(
            summary=summary,
            period_deal_records=records,
            cfg=replace(_cfg(), analyzer_backend="hybrid"),
            backend_effective="hybrid",
            llm_runtime={"selected": "none"},
        )
    assert payload["rows_count"] == 0


def test_daily_case_classifier_separates_secretary_and_lpr():
    secretary_profile = classify_daily_case(
        role="телемаркетолог",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "Секретарь попросил отправить на почту и уточнить тему обращения.",
                "call_signal_summary_short": "Маршрутизация через секретаря",
            }
        ],
    )
    lpr_profile = classify_daily_case(
        role="телемаркетолог",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "Говорили с ЛПР, согласовали встречу на четверг.",
                "call_signal_summary_short": "Есть следующий шаг с ЛПР",
            }
        ],
    )
    assert secretary_profile.mode == "secretary_analysis"
    assert lpr_profile.mode == "negotiation_lpr_analysis"


def test_sanitize_daily_columns_applies_case_bans_for_secretary():
    payload = {
        "key_takeaway": "Не подтверждена презентация и нет результата демонстрации.",
        "strong_sides": "Хорошо держал секретаря.",
        "growth_zones": "Нужно заполнить бриф и подтвердить презентацию.",
        "why_important": "Без этого будет срыв.",
        "reinforce": "Модуль захода через инфоповод.",
        "fix_action": "Уточнить маршрут и убрать пустые перезвоны.",
        "coaching_list": "1) Разобрали модуль секретаря.\n2) Дали модуль маршрутизации.\n3) Тестирует новый заход.",
        "expected_quantity": "+0 встреч в неделю",
        "expected_quality": "Качество этапа вырастет.",
    }
    cols = _sanitize_daily_llm_columns(
        payload=payload,
        fallback={"Ожидаемый эффект - количество": "+0.3 встречи в неделю"},
        role="телемаркетолог",
        case_policy={"banned_topics": ["презентация", "демонстрация", "бриф"]},
    )
    assert "презентац" not in cols["Зоны роста"].lower()
    assert "бриф" not in cols["Зоны роста"].lower()
    assert cols["Ожидаемый эффект - количество"] == "+0.3 встречи в неделю"


def test_role_scope_policy_telemarketer_blocks_warm_topics_by_default():
    policy = get_role_scope_policy(
        role="телемаркетолог",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "Секретарь переключил на отдел, без демо.",
                "call_signal_summary_short": "маршрутизация через секретаря",
            }
        ],
    )
    blocked = [str(x).lower() for x in policy.get("role_blocked_topics", [])]
    assert policy.get("role_scope_applied") is True
    assert policy.get("role_scope_conflict_flag") is False
    assert any("презентац" in x for x in blocked)
    assert any("бриф" in x for x in blocked)


def test_role_scope_policy_telemarketer_allows_warm_override_on_explicit_signal():
    policy = get_role_scope_policy(
        role="телемаркетолог",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "С ЛПР обсудили демонстрацию и тест, зафиксировали следующий шаг.",
                "call_signal_summary_short": "явный переход в демо/тест",
            }
        ],
    )
    blocked = [str(x).lower() for x in policy.get("role_blocked_topics", [])]
    assert policy.get("role_scope_applied") is True
    assert policy.get("role_scope_conflict_flag") is True
    assert not any("презентац" in x for x in blocked)
    assert not any("бриф" in x for x in blocked)


def test_role_scope_policy_sales_manager_includes_upper_funnel_topics():
    policy = get_role_scope_policy(
        role="менеджер по продажам",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "Первый контакт, прошли секретаря, вышли на ЛПР и зафиксировали встречу.",
            }
        ],
    )
    allowed = [str(x).lower() for x in policy.get("role_allowed_topics", [])]
    assert any("секретар" in x for x in allowed)
    assert any("лпр" in x for x in allowed)
    assert any("демонстрац" in x for x in allowed)


def test_classify_daily_case_sales_manager_allows_upper_funnel_secretary_case():
    profile = classify_daily_case(
        role="менеджер по продажам",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "Секретарь маршрутизировал на закупки, попросил отправить на почту.",
                "call_signal_summary_short": "секретарь, маршрутизация, следующий шаг",
            }
        ],
    )
    assert profile.mode == "secretary_analysis"


def test_classify_daily_case_sales_manager_allows_upper_funnel_lpr_case():
    profile = classify_daily_case(
        role="Илья",
        items=[
            {
                "transcript_usability_label": "usable",
                "transcript_text_excerpt": "Говорили с ЛПР, уточнили актуальность и назначили встречу.",
                "call_signal_summary_short": "ЛПР, актуальность, встреча",
            }
        ],
    )
    assert profile.mode == "negotiation_lpr_analysis"


def test_role_scope_policy_telemarketer_keeps_low_funnel_blocked_without_explicit_signal():
    policy = get_role_scope_policy(
        role="Рустам",
        items=[
            {
                "transcript_usability_label": "weak",
                "transcript_text_excerpt": "Недозвон, перезвон позже.",
                "call_signal_summary_short": "без содержательного warm-сигнала",
            }
        ],
    )
    blocked = [str(x).lower() for x in policy.get("role_blocked_topics", [])]
    assert any("демонстрац" in x for x in blocked)
    assert any("оплат" in x for x in blocked)
    assert any("коммерческое предложение" in x or x == "кп" for x in blocked)


def test_sanitize_daily_columns_role_scope_filters_fix_and_coaching_for_telemarketer():
    payload = {
        "key_takeaway": "Не подтверждена презентация.",
        "strong_sides": "Хорошо провел демонстрацию.",
        "growth_zones": "Нужно заполнить бриф и подтвердить презентацию.",
        "why_important": "Поможет быстрее закрывать оплату.",
        "reinforce": "Модуль выхода на ЛПР.",
        "fix_action": "Дожать демо и отправить коммерческое предложение.",
        "coaching_list": "1) Разобрали презентацию.\n2) Дали модуль demo close.\n3) Вести клиента до оплаты.",
        "expected_quantity": "+1 встреча в неделю",
        "expected_quality": "Конверсия демо в тест улучшится.",
    }
    cols = _sanitize_daily_llm_columns(
        payload=payload,
        fallback={"Ожидаемый эффект - количество": "+0.4 встречи в неделю"},
        role="телемаркетолог",
        case_policy={
            "banned_topics": ["презентация", "демонстрация", "бриф", "кп", "оплата"],
            "role_scope_applied": True,
            "role_allowed_topics": ["проход секретаря", "выход на ЛПР"],
            "role_blocked_topics": ["презентация", "демонстрация", "бриф", "тест", "кп", "оплата"],
            "role_scope_conflict_flag": False,
        },
    )


def _mock_prepare_call_review_llm_fields(**kwargs):
    records = kwargs.get("period_deal_records") or []
    for record in records:
        if not isinstance(record, dict):
            continue
        record["call_review_llm_ready"] = True
        record["call_review_llm_source"] = "main"
        record["call_review_llm_fields"] = {
            "key_takeaway": "Есть предметный разговор и понятный следующий шаг.",
            "strong_sides": "Удержал структуру разговора и зафиксировал следующий шаг.",
            "growth_zones": "Добавить точнее критерии квалификации в начале звонка.",
            "why_important": "Сотруднику проще дожимать, отдел получает меньше зависаний.",
            "reinforce": "Модуль фиксации следующего шага.",
            "fix_action": "В конце звонка фиксировать дату и формат следующего контакта.",
            "coaching_list": "1) Разобрали ход разговора.\n2) Дали модуль фиксации шага.\n3) Применяет в следующем касании.",
            "expected_quantity": "+0.2-0.4 рабочего шага в неделю.",
            "expected_quality": "Этап станет стабильнее и прозрачнее.",
            "evidence_quote": "Согласовали следующий шаг и время повторного контакта.",
            "stage_secretary_comment": "",
            "stage_lpr_comment": "Есть осмысленный контакт с нужной ролью.",
            "stage_need_comment": "Выявил потребность через уточняющие вопросы.",
            "stage_presentation_comment": "Показал логику следующего шага без давления.",
            "stage_closing_comment": "Закрыл звонок в конкретный следующий шаг.",
            "stage_objections_comment": "",
            "stage_speech_comment": "Речь собранная и понятная.",
            "stage_crm_comment": "CRM фиксирует факт разговора и следующий шаг.",
            "stage_discipline_comment": "",
            "stage_demo_comment": "",
        }
    return {
        "selected_runtime": "main",
        "generated_rows": sum(1 for x in records if isinstance(x, dict)),
        "failed_rows": 0,
        "skipped_rows": 0,
        "skip_reasons": {},
        "llm_sources": {"main": sum(1 for x in records if isinstance(x, dict))},
    }
    low_join = " ".join(str(cols.get(k) or "") for k in ("Сильные стороны", "Зоны роста", "Что исправить", "Что донес сотруднику")).lower()
    assert "презентац" not in low_join
    assert "демонстрац" not in low_join
    assert "бриф" not in low_join
    assert "оплат" not in low_join
    assert cols.get("_role_scope_applied") is True
    assert cols.get("_role_scope_conflict_flag") is False


def test_quantity_effect_keeps_decimals_and_never_plus_zero():
    payload = {
        "key_takeaway": "test",
        "strong_sides": "test",
        "growth_zones": "test",
        "why_important": "test",
        "reinforce": "test",
        "fix_action": "test",
        "coaching_list": "1) a\n2) b\n3) c",
        "expected_quantity": "+0.0 встреч в неделю",
        "expected_quality": "ok",
    }
    cols = _sanitize_daily_llm_columns(
        payload=payload,
        fallback={"Ожидаемый эффект - количество": "+0.4 встречи в неделю"},
        role="менеджер по продажам",
        case_policy={},
    )
    assert "+0.0" not in cols["Ожидаемый эффект - количество"]
    assert cols["Ожидаемый эффект - количество"] == "+0.4 встречи в неделю"


def test_daily_multistep_pipeline_failure_skips_row_and_collects_failure() -> None:
    summary = {
        "period_start": "2026-04-20",
        "period_end": "2026-04-22",
        "run_timestamp": "2026-04-22T10:00:00+00:00",
    }
    records = [
        {
            "deal_id": 601,
            "owner_name": "Илья",
            "updated_at": "2026-04-21T12:00:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "score": 58,
            "risk_flags": [],
            "notes_summary_raw": [{"text": "контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
            "transcript_available": True,
            "transcript_text_excerpt": "разговор",
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 85,
            "call_signal_summary_short": "Есть следующий шаг",
        }
    ]
    with patch(
        "src.deal_analyzer.cli._run_daily_multistep_pipeline",
        return_value={"ok": False, "failed_step": "block_split", "error": "missing_blocks=growth_zones"},
    ):
        payload = _build_daily_control_sheet_payload(
            summary=summary,
            period_deal_records=records,
            cfg=replace(_cfg(), analyzer_backend="hybrid"),
            logger=_Logger(),
            backend_effective="hybrid",
            llm_runtime={"selected": "main", "main_ok": True},
            daily_step_artifacts_dir=Path("workspace/tmp_tests/deal_analyzer/daily_steps_fail"),
        )
    assert payload["rows_count"] == 0
    diag = payload.get("daily_multistep_pipeline", {})
    assert diag.get("step_failures_count", 0) >= 1
    assert diag.get("step_failures", [])[0].get("failed_step") == "block_split"


def test_daily_multistep_pipeline_success_populates_llm_columns() -> None:
    summary = {
        "period_start": "2026-04-20",
        "period_end": "2026-04-22",
        "run_timestamp": "2026-04-22T10:00:00+00:00",
    }
    records = [
        {
            "deal_id": 602,
            "owner_name": "Рустам",
            "updated_at": "2026-04-21T12:00:00+00:00",
            "status_name": "В работе",
            "pipeline_name": "Привлечение",
            "score": 62,
            "risk_flags": [],
            "notes_summary_raw": [{"text": "контекст"}],
            "tasks_summary_raw": [{"text": "задача"}],
            "transcript_available": True,
            "transcript_text_excerpt": "разговор",
            "transcript_usability_label": "usable",
            "transcript_usability_score_final": 88,
            "call_signal_summary_short": "Секретарь дал маршрут",
        }
    ]
    cols = {
        "Ключевой вывод": "Есть рабочий контакт, но шаг нужно фиксировать жестче.",
        "Сильные стороны": "Уверенно прошел секретаря.",
        "Зоны роста": "Уточнять шаг и окно контакта.",
        "Почему это важно": "Так не теряем темп и быстрее доходим до решения.",
        "Что закрепить": "Модуль захода через инфоповод.",
        "Что исправить": "После маршрута сразу фиксировать конкретный слот.",
        "Что донес сотруднику": "1) Разобрали заход.\n2) Дали модуль.\n3) Пробует в следующих касаниях.",
        "Ожидаемый эффект - количество": "+0.3 ЛПР в неделю и +0.2 встречи.",
        "Ожидаемый эффект - качество": "Станет меньше пустых перезвонов.",
    }
    with patch(
        "src.deal_analyzer.cli._run_daily_multistep_pipeline",
        return_value={"ok": True, "columns": cols, "step_artifacts": {"1_candidate_selection": "a.json"}, "source_of_truth": "styled_blocks", "assembler_only": True},
    ):
        payload = _build_daily_control_sheet_payload(
            summary=summary,
            period_deal_records=records,
            cfg=replace(_cfg(), analyzer_backend="hybrid"),
            logger=_Logger(),
            backend_effective="hybrid",
            llm_runtime={"selected": "main", "main_ok": True},
            daily_step_artifacts_dir=Path("workspace/tmp_tests/deal_analyzer/daily_steps_ok"),
        )
    assert payload["rows_count"] == 1
    row = payload["rows"][0]
    assert row["Ключевой вывод"] == cols["Ключевой вывод"]
    assert row["daily_multistep_source_of_truth"] == "styled_blocks"
    assert row["daily_multistep_assembler_only"] is True
    diag = payload.get("daily_multistep_pipeline", {})
    assert diag.get("step_artifacts_count", 0) >= 1


def test_call_pool_split_prioritizes_conversation_over_autoanswer_noise() -> None:
    call_pool_debug = {
        "items": [
            {
                "deal_id": "32162059",
                "owner_name": "Рустам",
                "call_case_type": "lpr_conversation",
                "pool_type": "conversation_pool",
                "pool_reason": "lpr_conversation",
                "pool_priority_score": 78,
                "recording_url_count": 2,
                "max_duration_seconds": 412,
                "short_calls_0_20_count": 0,
                "repeated_dead_redial_count": 0,
            },
            {
                "deal_id": "32165731",
                "owner_name": "Рустам",
                "call_case_type": "secretary_case",
                "pool_type": "conversation_pool",
                "pool_reason": "secretary_case",
                "pool_priority_score": 64,
                "recording_url_count": 1,
                "max_duration_seconds": 44,
                "short_calls_0_20_count": 0,
                "repeated_dead_redial_count": 0,
            },
            {
                "deal_id": "32160389",
                "owner_name": "Рустам",
                "call_case_type": "autoanswer_noise",
                "pool_type": "discipline_pool",
                "pool_reason": "autoanswer_noise",
                "pool_priority_score": 62,
                "recording_url_count": 0,
                "max_duration_seconds": 7,
                "short_calls_0_20_count": 3,
                "repeated_dead_redial_count": 1,
            },
        ]
    }
    conv, disc, agg = _build_call_pool_artifacts(call_pool_debug=call_pool_debug)
    conv_ids = [str(x.get("deal_id") or "") for x in conv.get("items", [])]
    disc_ids = [str(x.get("deal_id") or "") for x in disc.get("items", [])]
    assert conv_ids[:2] == ["32162059", "32165731"]
    assert "32160389" in disc_ids
    assert agg["conversation_pool_total"] == 2
    assert agg["discipline_pool_total"] == 1


def test_transcription_shortlist_selects_only_conversation_calls() -> None:
    conv_payload = {
        "items": [
            {
                "deal_id": "10",
                "pool_type": "conversation_pool",
                "call_case_type": "lpr_conversation",
                "call_items": [
                    {"call_id": "a1", "duration_seconds": 160, "recording_url": "https://x/1.mp3", "direction": "outbound", "status": "", "result": "", "disposition": "", "quality_flags": [], "timestamp": "2026-04-21T10:10:00+00:00"},
                    {"call_id": "a2", "duration_seconds": 48, "recording_url": "https://x/2.mp3", "direction": "outbound", "status": "", "result": "", "disposition": "", "quality_flags": [], "timestamp": "2026-04-21T11:10:00+00:00"},
                    {"call_id": "a3", "duration_seconds": 9, "recording_url": "", "direction": "outbound", "status": "no_answer", "result": "", "disposition": "", "quality_flags": [], "timestamp": "2026-04-21T12:10:00+00:00"},
                ],
            }
        ]
    }
    disc_payload = {
        "items": [
            {
                "deal_id": "20",
                "pool_type": "discipline_pool",
                "call_case_type": "autoanswer_noise",
                "calls_total": 3,
                "call_items": [],
            }
        ]
    }
    shortlist = _build_transcription_shortlist_payload(
        conversation_pool_payload=conv_payload,
        discipline_pool_payload=disc_payload,
    )
    by_deal = {str(x.get("deal_id")): x for x in shortlist.get("items", [])}
    assert by_deal["10"]["selected_for_transcription"] is True
    assert by_deal["10"]["selected_call_count"] >= 1
    assert "a1" in by_deal["10"]["selected_call_ids"]
    assert by_deal["20"]["selected_for_transcription"] is False
    assert by_deal["20"]["transcription_selection_reason"] == "discipline_pool_not_in_main_stt"


def test_analyze_period_limit_applies_to_conversation_shortlist_not_raw_rows() -> None:
    output_dir = _fresh_output_dir("period_batch_limit_conversation_shortlist")
    payload = {
        "normalized_deals": [
            {"deal_id": 1, "deal_name": "d1", "responsible_user_name": "Рустам"},
            {"deal_id": 2, "deal_name": "d2", "responsible_user_name": "Рустам"},
            {"deal_id": 3, "deal_name": "d3", "responsible_user_name": "Рустам"},
        ]
    }
    logger = _Logger()
    processed_ids: list[int] = []

    fake_call_pool_debug = {
        "deals_total_before_limit": 3,
        "deals_with_any_calls": 3,
        "deals_with_recordings": 3,
        "deals_with_long_calls": 2,
        "deals_with_only_short_calls": 1,
        "deals_with_autoanswer_pattern": 1,
        "deals_with_redial_pattern": 1,
        "items": [
            {"deal_id": "1", "pool_type": "conversation_pool", "call_case_type": "lpr_conversation", "pool_priority_score": 90, "call_items": [{"call_id": "c1", "duration_seconds": 120, "recording_url": "https://x/1.mp3", "direction": "outbound", "status": "", "result": "", "disposition": "", "quality_flags": []}]},
            {"deal_id": "2", "pool_type": "discipline_pool", "call_case_type": "autoanswer_noise", "pool_priority_score": 80, "calls_total": 2, "call_items": []},
            {"deal_id": "3", "pool_type": "conversation_pool", "call_case_type": "secretary_case", "pool_priority_score": 70, "call_items": [{"call_id": "c3", "duration_seconds": 50, "recording_url": "https://x/3.mp3", "direction": "outbound", "status": "", "result": "", "disposition": "", "quality_flags": []}]},
        ],
    }

    def _fake_snapshot(*, normalized_deal, config, logger, raw_bundle, selected_call_ids=None, transcription_selection_reason=""):
        did = int(normalized_deal["deal_id"])
        processed_ids.append(did)
        return _snapshot_for_deal(did)

    def _fake_analyze(normalized, cfg, logger, *, deal_hint, backend_override):
        return _analysis_for_deal(int(normalized["deal_id"]), score=60)

    with patch("src.deal_analyzer.cli._collect_call_pool_debug", return_value=fake_call_pool_debug), patch(
        "src.deal_analyzer.cli.build_deal_snapshot", side_effect=_fake_snapshot
    ), patch("src.deal_analyzer.cli._analyze_one_with_isolation", side_effect=_fake_analyze):
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

    # limit=1 should apply to conversation shortlist [deal 1, deal 3], so only deal 1 is processed.
    assert processed_ids == [1]


def test_analysis_shortlist_ranking_prefers_meaningful_over_autoanswer() -> None:
    shortlist_items = [
        {
            "deal_id": "32160389",
            "pool_type": "discipline_pool",
            "call_case_type": "autoanswer_noise",
            "pool_priority_score": 90,
            "selected_for_transcription": False,
            "selected_call_count": 0,
            "calls_total": 3,
            "short_calls_0_20_count": 3,
            "autoanswer_like_count": 3,
        },
        {
            "deal_id": "32165731",
            "pool_type": "conversation_pool",
            "call_case_type": "secretary_case",
            "pool_priority_score": 70,
            "selected_for_transcription": True,
            "selected_call_count": 1,
            "calls_total": 1,
            "short_calls_0_20_count": 0,
            "autoanswer_like_count": 0,
        },
        {
            "deal_id": "32162059",
            "pool_type": "conversation_pool",
            "call_case_type": "lpr_conversation",
            "pool_priority_score": 65,
            "selected_for_transcription": True,
            "selected_call_count": 2,
            "calls_total": 2,
            "short_calls_0_20_count": 0,
            "autoanswer_like_count": 0,
        },
    ]
    payload = _build_analysis_shortlist_payload(
        shortlist_items=shortlist_items,
        normalized_rows_ranked=[],
        limit=2,
    )
    selected = payload.get("selected_items", [])
    assert isinstance(selected, list)
    selected_ids = [str(x.get("deal_id") or "") for x in selected]
    assert selected_ids == ["32162059", "32165731"]
    assert all(not bool(x.get("forced_fallback")) for x in selected)


def test_analysis_shortlist_forced_fallback_is_explicit_when_no_candidates() -> None:
    payload = _build_analysis_shortlist_payload(
        shortlist_items=[],
        normalized_rows_ranked=[
            {"deal_id": 10},
            {"deal_id": 20},
        ],
        limit=1,
    )
    selected = payload.get("selected_items", [])
    assert isinstance(selected, list)
    assert len(selected) == 1
    row = selected[0]
    assert str(row.get("deal_id") or "") == "10"
    assert bool(row.get("forced_fallback")) is True
    assert str(row.get("shortlist_reason") or "") == "forced_fallback_no_call_signal"
    assert int(payload.get("total_meaningful_candidates", 0) or 0) == 0
    assert int(payload.get("total_forced_fallback_candidates", 0) or 0) >= 1


def test_analysis_shortlist_drops_forced_candidates_when_meaningful_exists() -> None:
    payload = _build_analysis_shortlist_payload(
        shortlist_items=[
            {
                "deal_id": "100",
                "pool_type": "conversation_pool",
                "call_case_type": "lpr_conversation",
                "pool_priority_score": 70,
                "selected_for_transcription": True,
                "selected_call_count": 1,
                "calls_total": 1,
                "short_calls_0_20_count": 0,
                "autoanswer_like_count": 0,
            },
            {
                "deal_id": "200",
                "pool_type": "conversation_pool",
                "call_case_type": "autoanswer_noise",
                "pool_priority_score": 100,
                "selected_for_transcription": False,
                "selected_call_count": 0,
                "calls_total": 3,
                "short_calls_0_20_count": 3,
                "autoanswer_like_count": 3,
            },
        ],
        normalized_rows_ranked=[{"deal_id": 100}, {"deal_id": 200}],
        limit=10,
    )
    selected = payload.get("selected_items", [])
    assert isinstance(selected, list)
    assert [str(x.get("deal_id") or "") for x in selected] == ["100"]
    assert all(not bool(x.get("forced_fallback")) for x in selected)


def test_daily_factual_payload_includes_reference_stack_and_disabled_external_retrieval() -> None:
    cfg = replace(
        _cfg_hybrid(),
        sales_module_references=("docs/sales_context/scripts/link_base.md",),
        product_reference_urls={"info": "https://info.example.local", "link": "https://link.example.local"},
    )
    payload = _build_daily_table_factual_payload(
        cfg=cfg,
        logger=_Logger(),
        manager="Илья",
        role="менеджер по продажам",
        control_day="2026-04-20",
        period_start="2026-04-20",
        period_end="2026-04-24",
        package_items=[
            {
                "deal_id": "32162059",
                "status_name": "В работе",
                "pipeline_name": "Продажи",
                "risk_flags": ["process_hygiene: missing next step"],
                "call_signal_summary_short": "обсудили следующий шаг по демо",
                "transcript_text_excerpt": "договорились о следующем созвоне",
                "manager_summary": "нужно дожать следующий шаг",
                "employee_coaching": "добавить уточняющие вопросы",
            }
        ],
        links="https://example.test/deal/32162059",
        focus="линк",
        base_mix="выставка",
        avg_score=61,
        criticality="средняя",
        selection_reason="has_call_priority",
        growth_candidates=["не дожал следующий шаг"],
        fallback_columns={"Ключевой вывод": "x"},
        effect_forecast={"source": "fallback"},
        case_policy={"daily_analysis_mode": "negotiation_lpr_analysis"},
    )
    reference_stack = payload.get("reference_stack", {})
    assert isinstance(reference_stack, dict)
    assert isinstance(reference_stack.get("source_order"), list)
    assert "internal_references" in reference_stack.get("source_order", [])
    assert "role_context" in reference_stack.get("source_order", [])
    assert "product_reference_urls" in reference_stack.get("source_order", [])
    assert isinstance(reference_stack.get("internal_sources"), list)
    required_layers = reference_stack.get("required_layers", {})
    assert isinstance(required_layers, dict)
    assert bool((required_layers.get("role_context", {}) if isinstance(required_layers.get("role_context"), dict) else {}).get("ok")) is True
    assert bool((required_layers.get("product_reference_urls", {}) if isinstance(required_layers.get("product_reference_urls"), dict) else {}).get("ok")) is True
    layer_presence = reference_stack.get("layer_presence", {})
    assert isinstance(layer_presence, dict)
    assert bool(layer_presence.get("role_context_in_prompt")) is True
    assert bool(layer_presence.get("product_reference_in_prompt")) is True
    external = reference_stack.get("external_retrieval", {})
    assert isinstance(external, dict)
    assert external.get("enabled") is False
    assert external.get("used") is False


def test_daily_table_prompt_includes_reference_stack_and_style_mode() -> None:
    cfg = replace(_cfg_hybrid(), daily_style_mode="work_rude")
    factual_payload = {
        "manager_name": "Илья",
        "role": "менеджер по продажам",
        "case_policy": {"daily_analysis_mode": "negotiation_lpr_analysis", "allowed_axes": [], "banned_topics": []},
        "role_allowed_topics": ["демо"],
        "role_forbidden_topics": ["бриф"],
        "role_scope_conflict_flag": False,
        "reference_stack": {
            "source_order": ["internal_references", "role_context", "product_reference_urls", "external_retrieval_optional"],
            "required_layers": {
                "internal_references": {"ok": True, "snippets_used": 1},
                "role_context": {"ok": True, "snippets_used": 1},
                "product_reference_urls": {"ok": True, "snippets_used": 1},
            },
            "prompt_snippets": [
                {"layer": "internal", "source": "docs/sales.md", "snippet": "Фокус на следующий шаг."},
                {"layer": "role_context", "source": "factual_payload.role_context", "snippet": "role context: manager=Илья; role=менеджер по продажам"},
                {"layer": "product_url", "source": "https://istock.link/", "snippet": "LINK: сценарии для закупок."},
            ],
            "external_retrieval": {"enabled": True, "used": False, "reason": "disabled_by_config"},
        },
    }
    messages = build_daily_table_messages(
        factual_payload=factual_payload,
        config=cfg,
        style_source_excerpt="живой стиль",
        style_mode="work_rude",
    )
    assert len(messages) == 2
    combined = "\n".join(str(x.get("content") or "") for x in messages if isinstance(x, dict))
    assert "Reference stack order" in combined
    assert "external_retrieval" in combined or "External retrieval" in combined
    assert "Фокус на следующий шаг." in combined
    assert "role context: manager=Илья" in combined
    assert "required[role_context] ok=True" in combined
    assert "required[product_reference_urls] ok=True" in combined
    assert "Режим стиля" in combined or "Стиль:" in combined



def test_daily_control_base_mix_debug_fields_present():
    summary = {
        "period_start": "2026-04-14",
        "period_end": "2026-04-20",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 101,
            "owner_name": "Илья",
            "product_name": "ИНФО",
            "score": 55,
            "risk_flags": [],
            "strong_sides": ["Есть рабочий контакт с ЛПР"],
            "growth_zones": ["Дожать следующий шаг"],
            "employee_coaching": "",
            "status_name": "В работе",
            "deal_tags_raw": ["машэкспо"],
            "company_tags": ["тендерные"],
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    row = payload["rows"][0]
    assert row.get("base_mix_selected_source") == "deal_tags"
    assert str(row.get("base_mix_selected_value") or "").strip()
    assert isinstance(row.get("base_mix_raw_tags_deal"), list)
    assert isinstance(row.get("base_mix_raw_tags_company"), list)
    assert isinstance(row.get("base_mix_fallback_used"), bool)
    assert isinstance(row.get("base_mix_deal_tag_entries"), list)
    assert isinstance(row.get("base_mix_company_tag_entries"), list)
    first_deal_entry = row.get("base_mix_deal_tag_entries")[0]
    assert first_deal_entry["raw_tag"] == "машэкспо"
    assert first_deal_entry["normalized_tag"] == "машэкспо"
    assert first_deal_entry["source_of_tag"] == "deal"


def test_daily_control_base_mix_uses_company_tag_when_deal_tag_missing():
    summary = {
        "period_start": "2026-04-14",
        "period_end": "2026-04-20",
        "run_timestamp": "2026-04-19T10:10:00+00:00",
    }
    records = [
        {
            "deal_id": 202,
            "owner_name": "Илья",
            "product_name": "ИНФО",
            "score": 60,
            "risk_flags": [],
            "strong_sides": ["Есть содержательный контакт"],
            "growth_zones": ["Зафиксировать следующий шаг"],
            "employee_coaching": "",
            "status_name": "В работе",
            "deal_tags_raw": [],
            "company_tags": ["машиностроение"],
        }
    ]
    payload = _build_daily_control_sheet_payload(summary=summary, period_deal_records=records)
    row = payload["rows"][0]
    assert row.get("base_mix_selected_source") == "company_tags"
    assert row.get("base_mix_selected_value") == "машиностроение"
    assert row.get("base_mix_fallback_used") is False
    assert row.get("base_mix_deal_tag_entries") == []
    company_entries = row.get("base_mix_company_tag_entries")
    assert isinstance(company_entries, list) and company_entries
    assert company_entries[0]["source_of_tag"] == "company"


def test_company_tag_propagation_dry_run_plan_safe_and_conflict_cases():
    plan = _build_company_tag_propagation_dry_run_plan(
        rows=[
            {
                "deal_id": 1,
                "company_id": 101,
                "tags": [],
                "company_tags": ["машиностроение"],
                "company_tags_source": "api_tags",
            },
            {
                "deal_id": 2,
                "company_id": 102,
                "tags": ["инфо"],
                "company_tags": ["машиностроение"],
                "company_tags_source": "api_tags",
            },
            {
                "deal_id": 3,
                "company_id": 103,
                "tags": [],
                "company_tags": ["инфо", "линк"],
                "company_tags_source": "api_tags",
            },
        ]
    )
    assert plan["rows_total"] == 3
    assert plan["safe_to_propagate_total"] == 1
    rows = plan["items"]
    assert rows[0]["safe_to_propagate"] is True
    assert rows[0]["proposed_tags_to_add"] == ["машиностроение"]
    assert rows[0]["reason"] == "single_company_tag_safe_to_propagate"
    assert rows[1]["safe_to_propagate"] is False
    assert rows[1]["reason"] == "deal_has_own_tags"
    assert rows[2]["safe_to_propagate"] is False
    assert rows[2]["reason"] == "company_has_multiple_tags_conflict"
