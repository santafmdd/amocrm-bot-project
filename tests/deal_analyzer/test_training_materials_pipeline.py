from __future__ import annotations

import json
import uuid
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

from src.integrations.google_sheets_api_client import _scopes_match
from src.deal_analyzer.training_materials.cli import (
    _run_build,
    _run_write,
    _extract_generation_failures_rows,
    _load_retry_failed_keys,
    _merge_status_with_generation_failures,
    _parse_model_pool,
    _resolve_build_block_reason,
)
from src.deal_analyzer.training_materials import source_collector
from src.deal_analyzer.training_materials.docs_writer import (
    build_task_markdown,
    build_training_markdown,
    detect_google_api_capabilities,
    load_created_docs_artifact,
    materialize_docs_for_write,
    training_materials_required_scopes,
)
from src.deal_analyzer.training_materials.models import SourceCoverage, TrainingCandidate, TrainingDraft
from src.deal_analyzer.training_materials.training_analyzer import (
    _apply_targeted_quality_repairs,
    _build_messages,
    _build_runtime,
    _classify_training_error,
    _enforce_role_topic_scope,
    analyze_training_candidates,
)
from src.deal_analyzer.training_materials.sheets_link_writer import execute_links_write
from src.deal_analyzer.training_materials.validation import is_valid_url_or_empty, review_task_quality, review_training_quality, validate_draft_row


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        config_path=tmp_path / "config" / "deal_analyzer.local.json",
        deal_analyzer_write_enabled=True,
    )


def _new_tmp_root() -> Path:
    root = Path("workspace/tmp_tests/training_materials") / f"tmp_{uuid.uuid4().hex[:8]}"
    root.mkdir(parents=True, exist_ok=True)
    return root


class _DummyLogger:
    def warning(self, *_args, **_kwargs):
        return None


def _candidate() -> TrainingCandidate:
    return TrainingCandidate(
        row_number=2,
        plan_week_start="2026-04-27",
        plan_week_end="2026-05-01",
        plan_date="2026-04-29",
        recipient="Тест Менеджер",
        manager_role_profile="manager",
        activity_type="обучение",
        status="запланировано",
        what_i_do="Разобрать квалификацию",
        task_to_assign="10 звонков по новой технике",
        what_to_check="ЛПР/боль/следующий шаг",
        daily_meeting_thesis="Фокус на квалификации",
        expected_quantity_effect="Рост конверсии",
        expected_quality_effect="Чистая фиксация в CRM",
        training_link="",
        post_training_task_link="",
        topic_hash="abc123",
        idempotency_key="k1",
    )


def test_build_block_reason_uses_llm_generation_failed_not_rows_empty() -> None:
    block_reason = _resolve_build_block_reason(rows_training_candidates=6, rows_docs_prepared=0, llm_failed_count=6)
    assert block_reason == "llm_generation_failed"
    assert block_reason != "rows_empty"


def test_build_block_reason_uses_source_coverage_failed_not_rows_empty() -> None:
    block_reason = _resolve_build_block_reason(
        rows_training_candidates=6,
        rows_docs_prepared=0,
        llm_failed_count=0,
        source_coverage_failed_rows=2,
    )
    assert block_reason == "source_coverage_failed"
    assert block_reason != "rows_empty"


def test_training_block_reason_quality_gate_failed_not_rows_empty() -> None:
    block_reason = _resolve_build_block_reason(
        rows_training_candidates=3,
        rows_docs_prepared=0,
        llm_failed_count=0,
        quality_rows_failed=3,
        source_coverage_failed_rows=0,
    )
    assert block_reason == "quality_gate_failed"
    assert block_reason != "rows_empty"


def test_training_runtime_defaults_to_qwen_when_models_missing() -> None:
    cfg = SimpleNamespace(
        ollama_model="",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    runtime = _build_runtime(
        cfg=cfg,
        main_model_override="",
        fallback_model_override="",
        main_timeout_override=0,
        fallback_timeout_override=0,
    )
    assert runtime["main"]["model"] == "qwen3.5:397b-cloud"
    assert runtime["fallback"]["model"] == "deepseek-v3.1:671b-cloud"


def test_training_runtime_cli_override_has_priority() -> None:
    cfg = SimpleNamespace(
        ollama_model="cfg-main",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="cfg-fallback",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    runtime = _build_runtime(
        cfg=cfg,
        main_model_override="override-main",
        fallback_model_override="override-fallback",
        main_timeout_override=0,
        fallback_timeout_override=0,
    )
    assert runtime["main"]["model"] == "override-main"
    assert runtime["fallback"]["model"] == "override-fallback"


def test_parse_model_pool_deduplicates_and_trims() -> None:
    models = _parse_model_pool(" qwen3.5:397b-cloud, ,gpt-oss:120b-cloud,qwen3.5:397b-cloud ")
    assert models == ["qwen3.5:397b-cloud", "gpt-oss:120b-cloud"]


def test_extract_generation_failures_rows_contains_llm_errors() -> None:
    rows = _extract_generation_failures_rows(
        [
            {
                "row_number": 7,
                "recipient": "Рустам Хомидов",
                "plan_date": "2026-04-09",
                "training_topic": "Разобрать фиксацию шага",
                "main_model": "gpt-oss:120b-cloud",
                "fallback_model": "deepseek-v3.1:671b-cloud",
                "main_error": "HTTP 404 model not found",
                "fallback_error": "HTTP 429 rate limit",
                "final_reason": "invalid_schema:training_doc_too_short",
                "error_type": "invalid_json",
            }
        ]
    )
    assert len(rows) == 1
    assert rows[0]["row_number"] == 7
    assert rows[0]["main_error"]
    assert rows[0]["fallback_error"]


def test_merge_status_with_generation_failures_promotes_block_reason() -> None:
    status = {
        "write_allowed": False,
        "block_reason": "rows_empty",
        "rows_quarantined": 0,
    }
    failures = [
        {
            "row_number": 12,
            "recipient": "Рустам Хомидов",
            "plan_date": "2026-04-08",
            "final_reason": "invalid_json",
            "error_type": "invalid_json",
        }
    ]
    merged = _merge_status_with_generation_failures(status=status, generation_failures=failures)
    assert merged["block_reason"] == "llm_generation_failed"
    assert merged["rows_quarantined"] == 1
    assert merged["write_allowed"] is False
    assert isinstance(merged.get("llm_error_examples"), list)
    assert merged["llm_error_examples"][0]["row_number"] == 12


def test_merge_status_with_generation_failures_keeps_non_empty_block_reason() -> None:
    status = {
        "write_allowed": False,
        "block_reason": "generated_links_missing",
        "rows_quarantined": 0,
    }
    failures = [
        {
            "row_number": 17,
            "recipient": "Илья Бочков",
            "plan_date": "2026-04-09",
            "final_reason": "timeout",
            "error_type": "timeout",
        }
    ]
    merged = _merge_status_with_generation_failures(status=status, generation_failures=failures)
    assert merged["block_reason"] == "generated_links_missing"
    assert isinstance(merged.get("llm_error_examples"), list)
    assert merged["llm_error_examples"][0]["error_type"] == "timeout"


def test_merge_status_with_generation_failures_sets_partial_success_and_quarantine_rows() -> None:
    status = {
        "write_allowed": True,
        "block_reason": "",
        "rows_written": 4,
        "rows_quarantined": 0,
        "quarantined_rows": [],
    }
    failures = [
        {
            "row_number": 13,
            "recipient": "Рустам Хомидов",
            "plan_date": "2026-04-06",
            "final_reason": "quality_gate_failed:speech_modules_count_below_min:0",
            "error_type": "quality_gate_failed",
            "quality_fail_reasons": ["speech_modules_count_below_min:0"],
            "quality_metrics": {"training": {"speech_modules_count": 0}},
        }
    ]
    merged = _merge_status_with_generation_failures(status=status, generation_failures=failures)
    assert merged["rows_quarantined"] == 1
    assert merged["status"] == "partial_success"
    assert isinstance(merged.get("quarantined_rows"), list)
    assert merged["quarantined_rows"][0]["row_number"] == 13
    assert merged["quarantined_rows"][0]["quality_fail_reasons"] == ["speech_modules_count_below_min:0"]


def test_classify_dns_error_as_ollama_dns_failure() -> None:
    error_type = _classify_training_error("Ollama HTTP error 502: dial tcp: lookup ollama.com: no such host")
    assert error_type == "ollama_dns_failure"


def test_network_error_is_not_quality_gate_failed() -> None:
    error_type = _classify_training_error("dial tcp 34.10.10.10:443: i/o timeout")
    assert error_type == "ollama_network_failure"
    assert error_type != "quality_gate_failed"


def test_load_retry_failed_keys_reads_generation_failures() -> None:
    run_dir = _new_tmp_root() / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "rows_total": 2,
        "rows": [
            {"idempotency_key": "k1", "row_number": 12, "error_type": "ollama_dns_failure"},
            {"idempotency_key": "k2", "row_number": 13, "error_type": "ollama_timeout"},
        ],
    }
    (run_dir / "training_materials_generation_failures.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    keys = _load_retry_failed_keys(run_dir)
    assert "k1" in keys
    assert "k2" in keys


def test_targeted_repair_adds_speech_modules_and_checklist_items() -> None:
    payload = {
        "training_title": "Тест",
        "training_material": (
            "# Название обучения\nТест\n\n"
            "## Речевые модули\n\n"
            "## Чек-лист на следующий рабочий день\n"
            "- Уже есть один пункт"
        ),
        "task_title": "Задание",
        "task_material": "# Задание после обучения\n## Цель задания\nПроверка",
    }
    repaired, applied = _apply_targeted_quality_repairs(
        payload=payload,
        errors=["speech_modules_count_below_min:0", "checklist_items_count_below_min:1"],
    )
    assert "speech_modules_targeted_repair" in applied
    assert "checklist_targeted_repair" in applied
    training_q = review_training_quality(str(repaired.get("training_material") or ""))
    assert training_q.get("speech_modules_count", 0) >= 10
    assert training_q.get("checklist_items_count", 0) >= 7


def test_analyzer_attempts_fallback_when_main_fails(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="main-model",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="fallback-model",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    candidate = _candidate()

    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})

    def _fake_call_llm(*, model: str, base_url: str, timeout_seconds: int, messages: list[dict[str, str]]):
        _ = base_url, timeout_seconds, messages
        if model == "main-model":
            return None, {"ok": False, "error": "timeout", "elapsed_ms": 1, "repair_applied": False}
        return {
            "training_title": "Готовый черновик",
            "training_material": "Документ",
            "task_title": "Задача",
            "task_material": "Задача",
        }, {"ok": True, "error": "", "elapsed_ms": 1, "repair_applied": False}

    monkeypatch.setattr(analyzer, "_call_llm", _fake_call_llm)
    monkeypatch.setattr(analyzer, "_validate_payload", lambda payload: (True, []))

    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[candidate],
        snippets_by_key={candidate.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="main-model",
        fallback_model_override="fallback-model",
        llm_max_attempts=6,
    )
    assert len(drafts) == 1
    assert quarantined == []
    assert diagnostics["llm_attempts_main"] >= 1
    assert diagnostics["llm_attempts_fallback"] >= 1
    assert diagnostics["fallback_used_count"] >= 1


def test_training_materials_sales_manager_topics_do_not_become_cold_calling() -> None:
    candidate = replace(
        _candidate(),
        recipient="Илья Бочков",
        manager_role_profile="менеджер по продажам",
    )
    payload = {
        "training_title": "20 звонков по базе",
        "training_material": "Сделай массовый холодный обзвон по базе и дозвоны.",
        "task_title": "Холодный обзвон",
        "task_material": "Нужно сделать прозвон базы и наборы.",
    }
    repaired, diag = _enforce_role_topic_scope(candidate=candidate, payload=payload)
    assert diag["applied"] is True
    merged = " ".join(
        [
            str(repaired.get("training_title") or ""),
            str(repaired.get("training_material") or ""),
            str(repaired.get("task_title") or ""),
            str(repaired.get("task_material") or ""),
        ]
    ).lower()
    assert "холодный обзвон" not in merged
    assert "прозвон базы" not in merged


def test_training_materials_include_guided_demo_methodology() -> None:
    candidate = replace(
        _candidate(),
        recipient="Илья Бочков",
        manager_role_profile="менеджер по продажам",
    )
    messages = _build_messages(
        candidate=candidate,
        snippets=[],
        repair_mode=False,
        previous_error="",
        compact=False,
    )
    system_text = str(messages[0].get("content") or "").lower()
    user_text = str(messages[1].get("content") or "").lower()
    merged = f"{system_text}\n{user_text}"
    assert "guided discovery" in merged
    assert "hands-on" in merged
    assert "consultative demo" in merged


def test_analyzer_uses_model_pool_when_primary_model_fails(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="cfg-main",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="cfg-fallback",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    candidate = _candidate()

    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})

    def _fake_call_llm(*, model: str, base_url: str, timeout_seconds: int, messages: list[dict[str, str]]):
        _ = base_url, timeout_seconds, messages
        if model == "qwen3.5:397b-cloud":
            return None, {"ok": False, "error": "HTTP 503", "elapsed_ms": 1, "repair_applied": False}
        return {
            "training_title": "Готовый черновик",
            "training_material": "Документ",
            "task_title": "Задача",
            "task_material": "Задача",
        }, {"ok": True, "error": "", "elapsed_ms": 1, "repair_applied": False}

    monkeypatch.setattr(analyzer, "_call_llm", _fake_call_llm)
    monkeypatch.setattr(analyzer, "_validate_payload", lambda payload: (True, []))

    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[candidate],
        snippets_by_key={candidate.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="",
        fallback_model_override="",
        model_pool_override=["qwen3.5:397b-cloud", "gpt-oss:120b-cloud"],
        llm_max_attempts=6,
    )
    assert len(drafts) == 1
    assert quarantined == []
    assert diagnostics["llm_model_pool_effective"][:2] == ["qwen3.5:397b-cloud", "gpt-oss:120b-cloud"]
    assert diagnostics["fallback_used_count"] >= 1
    assert diagnostics["model_used_by_row"][0]["selected_model"] == "gpt-oss:120b-cloud"


def test_analyzer_llm_generation_failed_without_template_fallback(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="main-model",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="fallback-model",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    candidate = _candidate()

    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})
    monkeypatch.setattr(
        analyzer,
        "_call_llm",
        lambda **kwargs: (None, {"ok": False, "error": "HTTP 404 model not found", "elapsed_ms": 1, "repair_applied": False}),
    )

    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[candidate],
        snippets_by_key={candidate.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="main-model",
        fallback_model_override="fallback-model",
        llm_max_attempts=6,
        allow_template_fallback=False,
    )
    assert drafts == []
    assert len(quarantined) == 1
    assert diagnostics["llm_failed_count"] == 1
    assert diagnostics["llm_error_examples"]


def test_analyzer_template_fallback_prepares_docs_when_llm_fails(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="main-model",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="fallback-model",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    candidate = _candidate()

    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})
    monkeypatch.setattr(
        analyzer,
        "_call_llm",
        lambda **kwargs: (None, {"ok": False, "error": "timeout", "elapsed_ms": 1, "repair_applied": False}),
    )

    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[candidate],
        snippets_by_key={candidate.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="main-model",
        fallback_model_override="fallback-model",
        llm_max_attempts=6,
        allow_template_fallback=True,
    )
    assert len(drafts) == 1
    assert drafts[0].analysis_backend_used == "template_fallback"
    assert diagnostics["template_fallback_used_count"] == 1
    assert quarantined == []


def test_analyzer_dns_failures_stop_run_early(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="main-model",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="fallback-model",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    c1 = _candidate()
    c2 = TrainingCandidate(**{**asdict(c1), "idempotency_key": "k2", "row_number": 3})
    c3 = TrainingCandidate(**{**asdict(c1), "idempotency_key": "k3", "row_number": 4})

    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})
    monkeypatch.setattr(
        analyzer,
        "_call_llm",
        lambda **kwargs: (None, {"ok": False, "error": "lookup ollama.com: no such host", "elapsed_ms": 1, "repair_applied": False}),
    )

    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[c1, c2, c3],
        snippets_by_key={c1.idempotency_key: [], c2.idempotency_key: [], c3.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="main-model",
        fallback_model_override="fallback-model",
        llm_max_attempts=2,
        allow_template_fallback=False,
        network_retry_attempts_main=1,
        network_retry_attempts_fallback=1,
        enable_backoff_sleep=False,
    )
    assert drafts == []
    assert len(quarantined) >= 2
    assert diagnostics["stopped_reason"] == "network_or_ollama_cloud_unavailable"
    assert diagnostics["llm_error_summary_by_type"].get("ollama_dns_failure", 0) >= 2


def test_analyzer_stops_on_max_llm_calls(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="main-model",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="fallback-model",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    candidate = _candidate()
    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})
    monkeypatch.setattr(
        analyzer,
        "_call_llm",
        lambda **kwargs: (None, {"ok": False, "error": "timeout", "elapsed_ms": 1, "repair_applied": False}),
    )

    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[candidate, candidate],
        snippets_by_key={candidate.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="main-model",
        fallback_model_override="fallback-model",
        llm_max_attempts=6,
        max_llm_calls=1,
    )
    assert drafts == []
    assert quarantined == []
    assert diagnostics["stopped_reason"] == "max_llm_calls_exceeded"


def test_analyzer_stops_on_max_runtime(monkeypatch) -> None:
    cfg = SimpleNamespace(
        ollama_model="main-model",
        ollama_base_url="http://localhost:11434",
        ollama_timeout_seconds=30,
        ollama_preflight_timeout_seconds=5,
        ollama_fallback_model="fallback-model",
        ollama_fallback_base_url="http://localhost:11434",
        ollama_fallback_timeout_seconds=30,
        ollama_fallback_preflight_timeout_seconds=5,
    )
    candidate = _candidate()
    import src.deal_analyzer.training_materials.training_analyzer as analyzer

    monkeypatch.setattr(analyzer, "_preflight_model", lambda **kwargs: {"ok": True, "error": "", "elapsed_ms": 1, "prompt_size_chars": 10})
    monkeypatch.setattr(
        analyzer,
        "_call_llm",
        lambda **kwargs: (None, {"ok": False, "error": "timeout", "elapsed_ms": 1, "repair_applied": False}),
    )

    ticks = {"value": 0}

    def _time_now():
        ticks["value"] += 2
        return ticks["value"]

    monkeypatch.setattr(analyzer.time, "time", _time_now)
    drafts, quarantined, diagnostics = analyze_training_candidates(
        candidates=[candidate],
        snippets_by_key={candidate.idempotency_key: []},
        cfg=cfg,
        logger=None,
        main_model_override="main-model",
        fallback_model_override="fallback-model",
        llm_max_attempts=6,
        max_runtime_seconds=1,
    )
    assert drafts == []
    assert quarantined == []
    assert diagnostics["stopped_reason"] == "max_runtime_exceeded"


def test_collect_training_candidates_only_training_rows(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    cfg = _cfg(tmp_path)

    headers = [
        "План недели с",
        "План недели по",
        "Дата",
        "Адресат",
        "Тип активности",
        "Статус",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
    ]
    rows = [
        ["2026-04-27", "2026-05-03", "2026-04-28", "Рустам Хомидов", "обучение", "запланировано", "Разобрать квалификацию", "10 отработок", "Шаг в CRM", "Тезис", "", ""],
        ["2026-04-27", "2026-05-03", "2026-04-29", "Рустам Хомидов", "контроль", "запланировано", "Проверить отчеты", "", "", "", "", ""],
    ]

    monkeypatch.setattr(source_collector, "resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")
    monkeypatch.setattr(source_collector, "_read_plan_matrix", lambda **_kwargs: (headers, rows, 1))

    candidates, diag = source_collector.collect_training_candidates(
        cfg=cfg,
        plan_sheet_name="План недели",
        week_start="2026-04-27",
        week_end="2026-05-03",
        manager="",
        plan_date="",
        limit=0,
        logger=None,
    )

    assert len(candidates) == 1
    assert candidates[0].activity_type == "обучение"
    assert diag["rows_training_candidates"] == 1


def test_collect_training_candidates_allows_week_start_plan_date_match_on_week_end_mismatch(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    cfg = _cfg(tmp_path)

    headers = [
        "План недели с",
        "План недели по",
        "Дата",
        "Адресат",
        "Тип активности",
        "Статус",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
    ]
    rows = [
        ["2026-04-27", "2026-05-03", "2026-04-30", "Рустам Хомидов", "обучение", "запланировано", "Разобрать квалификацию", "10 отработок", "Шаг в CRM", "Тезис", "", ""],
    ]

    monkeypatch.setattr(source_collector, "resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")
    monkeypatch.setattr(source_collector, "_read_plan_matrix", lambda **_kwargs: (headers, rows, 1))

    candidates, diag = source_collector.collect_training_candidates(
        cfg=cfg,
        plan_sheet_name="План недели",
        week_start="2026-04-27",
        week_end="2026-05-01",
        manager="",
        plan_date="",
        limit=0,
        logger=None,
    )

    assert len(candidates) == 1
    assert diag["plan_rows_in_week_by_exact_key"] == 0
    assert diag["plan_rows_in_week_by_start_only"] == 1
    assert diag["plan_rows_training_activity_in_period"] == 1


def test_collect_training_candidates_skips_non_working_day_training(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    cfg = _cfg(tmp_path)

    headers = [
        "План недели с",
        "План недели по",
        "Дата",
        "Адресат",
        "Тип активности",
        "Статус",
        "Что делаю",
        "Какую задачу даю",
        "Что проверяю",
        "Общий тезис на дейлик",
        "Ссылка на обучение / материал",
        "Ссылка на задачи после обучения",
    ]
    rows = [
        ["2026-04-27", "2026-05-03", "2026-05-01", "Рустам Хомидов", "обучение", "запланировано", "Разобрать квалификацию", "10 отработок", "Шаг в CRM", "Тезис", "", ""],
    ]

    monkeypatch.setattr(source_collector, "resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")
    monkeypatch.setattr(source_collector, "_read_plan_matrix", lambda **_kwargs: (headers, rows, 1))

    candidates, diag = source_collector.collect_training_candidates(
        cfg=cfg,
        plan_sheet_name="План недели",
        week_start="2026-04-27",
        week_end="2026-05-01",
        manager="",
        plan_date="",
        limit=0,
        logger=None,
    )

    assert len(candidates) == 0
    assert any(item.get("reason") == "non_working_day" for item in diag["rejected_rows_with_reason"])


def test_collect_training_candidates_reports_week_end_mismatch_reason(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    cfg = _cfg(tmp_path)

    headers = [
        "План недели с",
        "План недели по",
        "Дата",
        "Адресат",
        "Тип активности",
        "Статус",
        "Что делаю",
        "Какую задачу даю",
    ]
    rows = [
        ["2026-04-27", "2026-05-03", "2026-05-02", "Рустам Хомидов", "обучение", "запланировано", "Разобрать квалификацию", "10 отработок"],
    ]
    monkeypatch.setattr(source_collector, "resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")
    monkeypatch.setattr(source_collector, "_read_plan_matrix", lambda **_kwargs: (headers, rows, 1))

    candidates, diag = source_collector.collect_training_candidates(
        cfg=cfg,
        plan_sheet_name="План недели",
        week_start="2026-04-27",
        week_end="2026-05-01",
        manager="",
        plan_date="",
        limit=0,
        logger=None,
    )
    assert len(candidates) == 0
    assert any(item.get("reason") == "week_end_mismatch" for item in diag["rejected_rows_with_reason"])


def test_collect_training_candidates_deduplicates_idempotency(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    cfg = _cfg(tmp_path)
    headers = [
        "План недели с",
        "План недели по",
        "Дата",
        "Адресат",
        "Тип активности",
        "Статус",
        "Что делаю",
        "Какую задачу даю",
    ]
    rows = [
        ["2026-04-27", "2026-05-03", "2026-04-28", "Илья Бочков", "обучение", "запланировано", "Разобрать фиксацию", "5 отработок"],
        ["2026-04-27", "2026-05-03", "2026-04-28", "Илья Бочков", "обучение", "запланировано", "Разобрать фиксацию", "5 отработок"],
    ]
    monkeypatch.setattr(source_collector, "resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")
    monkeypatch.setattr(source_collector, "_read_plan_matrix", lambda **_kwargs: (headers, rows, 1))

    candidates, diag = source_collector.collect_training_candidates(
        cfg=cfg,
        plan_sheet_name="План недели",
        week_start="2026-04-27",
        week_end="2026-05-03",
        manager="",
        plan_date="",
        limit=0,
        logger=None,
    )
    assert len(candidates) == 1
    assert any(item.get("reason") == "duplicate_idempotency_key" for item in diag["rows_skipped"])


def test_docs_api_unavailable_has_clear_status() -> None:
    tmp_path = _new_tmp_root()
    project_root = tmp_path / "proj"
    project_root.mkdir(parents=True, exist_ok=True)
    status = detect_google_api_capabilities(project_root=project_root)
    assert status["docs_api_available"] is False
    assert status["status"] == "docs_api_unavailable"
    assert "https://www.googleapis.com/auth/documents" in status["required_scopes"]


def test_scope_mismatch_has_reauth_instruction() -> None:
    tmp_path = _new_tmp_root()
    project_root = tmp_path / "proj"
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "credentials.json").write_text("{}", encoding="utf-8")
    (project_root / "token.json").write_text(
        json.dumps({"scopes": ["https://www.googleapis.com/auth/spreadsheets"]}, ensure_ascii=False),
        encoding="utf-8",
    )
    status = detect_google_api_capabilities(project_root=project_root)
    assert status["scope_mismatch_detected"] is True
    assert status["reauth_required"] is True
    assert "Удалите token.json и пройдите OAuth заново" in status["reauth_instruction"]


def test_collect_training_candidates_requests_expanded_scopes(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    cfg = _cfg(tmp_path)
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args
            captured["scopes"] = kwargs.get("scopes", [])
            captured["auth_mode"] = kwargs.get("auth_mode", "")

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id, rng
            return [
                [
                    "План недели с",
                    "План недели по",
                    "Дата",
                    "Адресат",
                    "Тип активности",
                    "Статус",
                    "Что делаю",
                    "Какую задачу даю",
                    "Что проверяю",
                    "Общий тезис на дейлик",
                    "Ссылка на обучение / материал",
                    "Ссылка на задачи после обучения",
                ],
                [
                    "2026-04-27",
                    "2026-05-03",
                    "2026-04-28",
                    "Рустам Хомидов",
                    "обучение",
                    "запланировано",
                    "Разобрать квалификацию",
                    "10 отработок",
                    "Фиксация следующего шага",
                    "Тезис",
                    "",
                    "",
                ],
            ]

    monkeypatch.setattr(source_collector, "GoogleSheetsApiClient", FakeClient)
    monkeypatch.setattr(source_collector, "resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")
    scopes = training_materials_required_scopes()
    candidates, _diag = source_collector.collect_training_candidates(
        cfg=cfg,
        plan_sheet_name="План недели",
        week_start="2026-04-27",
        week_end="2026-05-03",
        manager="",
        plan_date="",
        limit=0,
        logger=None,
        scopes=scopes,
        auth_mode="auto",
    )
    assert len(candidates) == 1
    assert set(captured.get("scopes", [])) == set(scopes)


def test_external_provider_auto_uses_curated_file_when_live_search_fails(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    config_root = project_root / "config"
    config_root.mkdir(parents=True, exist_ok=True)
    curated_file = project_root / "docs" / "training_materials_external_sources.json"
    curated_file.parent.mkdir(parents=True, exist_ok=True)
    curated_file.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "title": "SPIN",
                        "url": "https://example.com/spin",
                        "summary": "SPIN summary",
                    },
                    {
                        "title": "BANT",
                        "url": "https://example.com/bant",
                        "summary": "BANT summary",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    cfg = SimpleNamespace(
        config_path=config_root / "deal_analyzer.local.json",
        external_retrieval_enabled=False,
        external_retrieval_adapter="none",
        external_retrieval_timeout_seconds=3,
        training_materials_external_sources_file="docs/training_materials_external_sources.json",
        training_materials_external_curated_urls=(),
        training_materials_external_fetch_timeout_seconds=3,
    )
    provider = source_collector.ExternalMethodSourceProvider(cfg=cfg, provider="auto", timeout_seconds=3)
    monkeypatch.setattr(
        provider,
        "_search_http_json",
        lambda **_kwargs: provider._empty("disabled", provider="http_json", fetch_errors=["disabled"]),
    )
    monkeypatch.setattr(
        provider,
        "_search_duckduckgo_html",
        lambda **_kwargs: provider._empty("provider_error", provider="duckduckgo_html", fetch_errors=["ddg_failed"]),
    )
    monkeypatch.setattr(
        provider,
        "_fetch_external_page",
        lambda **_kwargs: {"title": "", "snippet": "", "error": "network_down"},
    )
    result = provider.search(query="SPIN вопросы в звонке", limit=5)
    assert result["used"] is True
    assert result["provider"] == "manual_curated_urls"
    assert int(result.get("count", 0) or 0) >= 2
    assert str(result.get("status") or "") == "ok"


def test_scope_match_accepts_superset_token_scopes() -> None:
    expected = ["https://www.googleapis.com/auth/spreadsheets"]
    actual = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/documents",
    ]
    assert _scopes_match(expected, actual) is True


def test_link_url_validation() -> None:
    assert is_valid_url_or_empty("") is True
    assert is_valid_url_or_empty("https://docs.google.com/document/d/abc") is True
    assert is_valid_url_or_empty("not-a-url") is False


def test_sheets_link_writer_dry_run_no_updates(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "rows": [
            {
                "row_number": 2,
                "training_link": "",
                "post_training_task_link": "",
                "existing_training_link": "",
                "existing_post_training_task_link": "",
            }
        ]
    }
    (run_dir / "training_materials_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class FakeClient:
        calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [["План недели с", "План недели по", "Дата", "Адресат", "Тип активности", "Статус", "Что делаю", "Ссылка на обучение / материал", "Ссылка на задачи после обучения"]]
            if "A1:AZ1" in rng:
                return [["План недели с", "План недели по", "Дата", "Адресат", "Тип активности", "Статус", "Что делаю", "Ссылка на обучение / материал", "Ссылка на задачи после обучения"]]
            return []

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            FakeClient.calls += 1

    monkeypatch.setattr("src.deal_analyzer.training_materials.sheets_link_writer.GoogleSheetsApiClient", FakeClient)
    monkeypatch.setattr("src.deal_analyzer.training_materials.sheets_link_writer.resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")

    status = execute_links_write(
        cfg=SimpleNamespace(config_path=tmp_path / "cfg.json", deal_analyzer_write_enabled=True),
        run_dir=run_dir,
        plan_sheet_name="План недели",
        dry_run=True,
        write=False,
        overwrite_links=False,
        strict_preflight=False,
        logger=None,
    )
    assert status["mode"] == "dry_run"
    assert status["rows_written"] == 0
    assert FakeClient.calls == 0


def test_sheets_link_writer_dry_run_strict_preflight_does_not_block_generated_missing(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "rows": [
            {
                "row_number": 2,
                "training_link": "",
                "post_training_task_link": "",
                "existing_training_link": "",
                "existing_post_training_task_link": "",
            }
        ]
    }
    (run_dir / "training_materials_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [["План недели с", "План недели по", "Дата", "Адресат", "Тип активности", "Статус", "Что делаю", "Ссылка на обучение / материал", "Ссылка на задачи после обучения"]]
            if "A1:AZ1" in rng:
                return [["План недели с", "План недели по", "Дата", "Адресат", "Тип активности", "Статус", "Что делаю", "Ссылка на обучение / материал", "Ссылка на задачи после обучения"]]
            return []

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data

    monkeypatch.setattr("src.deal_analyzer.training_materials.sheets_link_writer.GoogleSheetsApiClient", FakeClient)
    monkeypatch.setattr("src.deal_analyzer.training_materials.sheets_link_writer.resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")

    status = execute_links_write(
        cfg=SimpleNamespace(config_path=tmp_path / "cfg.json", deal_analyzer_write_enabled=True),
        run_dir=run_dir,
        plan_sheet_name="План недели",
        dry_run=True,
        write=False,
        overwrite_links=False,
        strict_preflight=True,
        logger=None,
    )
    assert status["mode"] == "dry_run"
    assert status["block_reason"] == ""
    assert status["rows_missing_generated_links"] == 1


def test_sheets_link_writer_real_write_blocks_when_generated_links_missing(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "rows": [
            {
                "row_number": 2,
                "training_link": "",
                "post_training_task_link": "",
                "existing_training_link": "",
                "existing_post_training_task_link": "",
            }
        ]
    }
    (run_dir / "training_materials_payload.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    class FakeClient:
        calls = 0

        def __init__(self, *args, **kwargs):
            _ = args, kwargs

        def get_values(self, spreadsheet_id: str, rng: str):
            _ = spreadsheet_id
            if "A1:AZ30" in rng:
                return [["План недели с", "План недели по", "Дата", "Адресат", "Тип активности", "Статус", "Что делаю", "Ссылка на обучение / материал", "Ссылка на задачи после обучения"]]
            if "A1:AZ1" in rng:
                return [["План недели с", "План недели по", "Дата", "Адресат", "Тип активности", "Статус", "Что делаю", "Ссылка на обучение / материал", "Ссылка на задачи после обучения"]]
            return []

        def batch_update_values(self, spreadsheet_id: str, data):
            _ = spreadsheet_id, data
            FakeClient.calls += 1

    monkeypatch.setattr("src.deal_analyzer.training_materials.sheets_link_writer.GoogleSheetsApiClient", FakeClient)
    monkeypatch.setattr("src.deal_analyzer.training_materials.sheets_link_writer.resolve_spreadsheet_id", lambda *_args, **_kwargs: "sheet")

    status = execute_links_write(
        cfg=SimpleNamespace(config_path=tmp_path / "cfg.json", deal_analyzer_write_enabled=True),
        run_dir=run_dir,
        plan_sheet_name="План недели",
        dry_run=False,
        write=True,
        overwrite_links=False,
        strict_preflight=True,
        logger=None,
    )
    assert status["write_allowed"] is False
    assert status["block_reason"] == "generated_links_missing"
    assert status["rows_written"] == 0
    assert FakeClient.calls == 0


def test_materialize_docs_for_write_dry_run_keeps_links_uncreated() -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg = _cfg(tmp_path)
    rows = [
        {
            "row_number": 2,
            "idempotency_key": "2026-04-27|2026-05-01|rustam|2026-04-27|обучение|abc",
            "plan_week_start": "2026-04-27",
            "plan_week_end": "2026-05-01",
            "plan_date": "2026-04-27",
            "recipient": "Рустам Хомидов",
            "activity_type": "обучение",
            "topic_hash": "abc",
            "training_title": "Т1",
            "task_title": "З1",
            "training_material": "M1",
            "task_material": "M2",
            "training_link": "",
            "post_training_task_link": "",
            "existing_training_link": "",
            "existing_post_training_task_link": "",
        }
    ]
    out_rows, stats = materialize_docs_for_write(
        cfg=cfg,
        run_dir=run_dir,
        payload_rows=rows,
        write_enabled=False,
        overwrite_links=False,
        logger=None,
    )
    assert stats["docs_creation_mode"] == "dry_run"
    assert stats["rows_docs_created"] == 0
    assert stats["rows_task_docs_created"] == 0
    assert out_rows[0]["training_link"] == ""
    assert out_rows[0]["post_training_task_link"] == ""


def test_materialize_docs_for_write_reuses_idempotency_artifact(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    cfg = _cfg(tmp_path)
    key = "2026-04-27|2026-05-01|rustam|2026-04-27|обучение|abc"
    artifact = {
        "docs_by_idempotency_key": {
            key: {
                "training_link": "https://docs.google.com/document/d/training/edit",
                "post_training_task_link": "https://docs.google.com/document/d/task/edit",
            }
        }
    }
    (run_dir / "training_materials_created_docs.json").write_text(json.dumps(artifact, ensure_ascii=False), encoding="utf-8")

    def _boom(*args, **kwargs):
        _ = args, kwargs
        raise AssertionError("services must not be called when artifact has links")

    monkeypatch.setattr("src.deal_analyzer.training_materials.docs_writer._build_docs_and_drive_services", _boom)

    rows = [
        {
            "row_number": 2,
            "idempotency_key": key,
            "plan_week_start": "2026-04-27",
            "plan_week_end": "2026-05-01",
            "plan_date": "2026-04-27",
            "recipient": "Рустам Хомидов",
            "activity_type": "обучение",
            "topic_hash": "abc",
            "training_title": "Т1",
            "task_title": "З1",
            "training_material": "M1",
            "task_material": "M2",
            "training_link": "",
            "post_training_task_link": "",
            "existing_training_link": "",
            "existing_post_training_task_link": "",
        }
    ]
    out_rows, stats = materialize_docs_for_write(
        cfg=cfg,
        run_dir=run_dir,
        payload_rows=rows,
        write_enabled=True,
        overwrite_links=False,
        logger=None,
    )
    assert out_rows[0]["training_link"].startswith("https://docs.google.com/document/")
    assert out_rows[0]["post_training_task_link"].startswith("https://docs.google.com/document/")
    assert stats["rows_docs_created"] == 0
    assert stats["rows_task_docs_created"] == 0
    assert stats["rows_docs_reused_from_artifact"] >= 2
    loaded = load_created_docs_artifact(run_dir)
    assert key in loaded.get("docs_by_idempotency_key", {})


def test_materialize_docs_for_write_classifies_google_docs_service_disabled(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "credentials.json").write_text(
        json.dumps({"installed": {"project_id": "my-gcp-project"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    cfg = SimpleNamespace(
        config_path=project_root / "config" / "deal_analyzer.local.json",
        deal_analyzer_write_enabled=True,
    )
    rows = [
        {
            "row_number": 2,
            "idempotency_key": "2026-04-27|2026-05-01|rustam|2026-04-27|обучение|abc",
            "plan_week_start": "2026-04-27",
            "plan_week_end": "2026-05-01",
            "plan_date": "2026-04-27",
            "recipient": "Рустам Хомидов",
            "activity_type": "обучение",
            "topic_hash": "abc",
            "training_title": "Т1",
            "task_title": "З1",
            "training_material": "M1",
            "task_material": "M2",
            "training_link": "",
            "post_training_task_link": "",
            "existing_training_link": "",
            "existing_post_training_task_link": "",
        }
    ]

    def _raise_docs_disabled(*args, **kwargs):
        _ = args, kwargs
        raise RuntimeError(
            "HttpError 403 SERVICE_DISABLED: Google Docs API has not been used in project my-gcp-project before or it is disabled. docs.googleapis.com"
        )

    monkeypatch.setattr("src.deal_analyzer.training_materials.docs_writer._build_docs_and_drive_services", _raise_docs_disabled)
    _rows, stats = materialize_docs_for_write(
        cfg=cfg,
        run_dir=run_dir,
        payload_rows=rows,
        write_enabled=True,
        overwrite_links=False,
        logger=None,
    )
    assert stats["docs_api_status"] == "service_disabled"
    assert stats["docs_api_error_type"] == "google_docs_api_disabled"
    assert "Enable Google Docs API in Google Cloud project my-gcp-project" in stats["action_required"]
    assert stats["docs_creation_errors_count"] == 1
    assert stats["docs_creation_error_examples"]


def test_materialize_docs_for_write_classifies_google_drive_service_disabled(monkeypatch) -> None:
    tmp_path = _new_tmp_root()
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    project_root = tmp_path / "project"
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "credentials.json").write_text(
        json.dumps({"installed": {"project_id": "my-gcp-project"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    cfg = SimpleNamespace(
        config_path=project_root / "config" / "deal_analyzer.local.json",
        deal_analyzer_write_enabled=True,
    )
    rows = [
        {
            "row_number": 2,
            "idempotency_key": "2026-04-27|2026-05-01|rustam|2026-04-27|обучение|abc",
            "plan_week_start": "2026-04-27",
            "plan_week_end": "2026-05-01",
            "plan_date": "2026-04-27",
            "recipient": "Рустам Хомидов",
            "activity_type": "обучение",
            "topic_hash": "abc",
            "training_title": "Т1",
            "task_title": "З1",
            "training_material": "M1",
            "task_material": "M2",
            "training_link": "",
            "post_training_task_link": "",
            "existing_training_link": "",
            "existing_post_training_task_link": "",
        }
    ]

    def _raise_drive_disabled(*args, **kwargs):
        _ = args, kwargs
        raise RuntimeError(
            "HttpError 403 SERVICE_DISABLED: Google Drive API has not been used in project my-gcp-project before or it is disabled. drive.googleapis.com"
        )

    monkeypatch.setattr("src.deal_analyzer.training_materials.docs_writer._build_docs_and_drive_services", _raise_drive_disabled)
    _rows, stats = materialize_docs_for_write(
        cfg=cfg,
        run_dir=run_dir,
        payload_rows=rows,
        write_enabled=True,
        overwrite_links=False,
        logger=None,
    )
    assert stats["drive_api_status"] == "service_disabled"
    assert stats["docs_api_error_type"] == "google_drive_api_disabled"
    assert "Enable Google Drive API in Google Cloud project my-gcp-project" in stats["action_required"]
    assert stats["docs_creation_errors_count"] == 1


def _long_training_material() -> str:
    speech_lines = "\n".join([f'- "Используй: фраза {idx} для развития диалога и фиксации следующего шага."' for idx in range(1, 12)])
    checklist_lines = "\n".join([f"- Чек-лист пункт {idx}: проверяем конкретный факт внедрения в звонке и CRM." for idx in range(1, 9)])
    theory_block = (
        "Теория: клиент редко формулирует проблему с первого ответа, поэтому менеджер переводит общий сигнал в конкретику через уточнения. "
        "Психологическая механика проста: когда клиент проговаривает последствия своими словами, он повышает ценность изменения и легче соглашается на следующий шаг. "
        "Этот переход помогает от разговора о продукте перейти к разговору о бизнес-рисках клиента.\n"
    )
    theory_long = theory_block * 16
    return "\n".join(
        [
            "# Название обучения",
            "## Для кого",
            "- Сотрудник: Тест Менеджер",
            "- Неделя: 2026-04-27..2026-05-01",
            "- Дата обучения: 2026-04-29",
            "## Зачем это обучение",
            "Нужно убрать поверхностные звонки и повысить управляемость следующего шага.",
            "## Что увидели в звонках / дневном контроле",
            "Повторяется ошибка: менеджер рано уходит в презентацию, не фиксирует ЛПР и следующий шаг.",
            "## Теория простыми словами",
            theory_long,
            "## Основная модель / алгоритм",
            "1. Зафиксировать контекст.\n2. Уточнить процесс.\n3. Найти слабое место.\n4. Уточнить последствия.\n5. Перевести боль в ценность.\n6. Зафиксировать следующий шаг.\n7. Записать итог в CRM.",
            "## Как применять в звонке",
            "Если клиент отвечает коротко — раскрываем ответ уточняющим вопросом. Если говорит \"не актуально\" — переводим в потери и риски.",
            "## Речевые модули",
            speech_lines,
            "## Частые ошибки",
            "- Ранний переход к презентации.\n- Отсутствие фиксации ЛПР.\n- Нерабочие общие формулировки без следующего шага.",
            "## Мини-тренировка",
            "1. Переписать 5 слабых фраз.\n2. Подготовить 5 уточняющих вопросов.\n3. Ролевой прогон.\n4. Вход в разговор / переход / завершение.\n5. Разбор своего звонка по чек-листу.",
            "## Чек-лист на следующий рабочий день",
            checklist_lines,
            "## Как руководитель будет проверять внедрение",
            "Проверяю 3 звонка, комментарии в CRM, наличие следующего шага с датой/временем, фактическое внедрение речевых модулей.",
        ]
    )


def _long_training_material_with_speech_formats() -> str:
    speech_lines = "\n".join(
        [
            'Используй: "Подтвердите, пожалуйста, ключевую боль в одном предложении."',
            'Скажи: "Чтобы не потерять детали, зафиксируем следующий шаг прямо сейчас."',
            'Формулировка: "Если оставить текущий процесс без изменений, что будет через месяц?"',
            'Вместо "пришлю материалы" используй "давайте назначим короткий демо-слот на 15 минут".',
            '"Подтвердите, пожалуйста, кто согласует запуск и на каком шаге вы сейчас."',  # quoted example
            '1. "Если удобно, выберите один из двух слотов: сегодня в 16:00 или завтра в 11:00."',
            '2. "Что для вас будет признаком, что пилот реально полезен?"',
            '3. "Какие риски останутся, если решение не внедрить в ближайший месяц?"',
            '4. "Давайте закрепим ответственного и дату следующего касания."',
            '5. "Я фиксирую договоренность в amoCRM и отправляю подтверждение на email."',
        ]
    )
    checklist_lines = "\n".join([f"- Чек-лист пункт {idx}: фиксация факта внедрения." for idx in range(1, 9)])
    theory_long = (
        "Теория: клиент формулирует глубинную причину постепенно, поэтому менеджер управляет разговором вопросами и фиксацией шага. "
        * 18
    )
    return "\n".join(
        [
            "# Название обучения",
            "## Для кого",
            "- Сотрудник: Тест Менеджер",
            "- Неделя: 2026-04-27..2026-05-01",
            "- Дата обучения: 2026-04-29",
            "## Зачем это обучение",
            "Убираем потерю управления разговором и повышаем конверсию в следующий шаг.",
            "## Что увидели в звонках / дневном контроле",
            "Повторяется ранний уход в презентацию и отсутствие конкретного следующего шага.",
            "## Теория простыми словами",
            theory_long,
            "## Основная модель / алгоритм",
            "1. Контекст.\n2. Процесс.\n3. Слабое место.\n4. Последствия.\n5. Ценность.\n6. Следующий шаг.\n7. Фиксация в CRM.",
            "## Как применять в звонке",
            "При коротком ответе клиента задаем уточняющий вопрос и фиксируем шаг по времени.",
            "## Речевые модули",
            speech_lines,
            "## Частые ошибки",
            "- Общие фразы без шага.\n- Отсутствие проверки ЛПР.\n- Нет фиксации в CRM.",
            "## Мини-тренировка",
            "1. Переписать 5 фраз.\n2. Подготовить 5 вопросов.\n3. Ролевой прогон.\n4. Разбор звонка.\n5. Фиксация результата.",
            "## Чек-лист на следующий рабочий день",
            checklist_lines,
            "## Как руководитель будет проверять внедрение",
            "Проверка 3 звонков, проверка записей в CRM и факта назначенного следующего шага.",
        ]
    )


def _long_task_material() -> str:
    base = (
        "Сотрудник выполняет задание на реальных звонках и фиксирует результат в CRM в структурированном виде: "
        "контекст, выявленная боль, следующий шаг, подтверждение даты и времени, короткий итог по разговору.\n"
    )
    filler = base * 22
    return "\n".join(
        [
            "# Задание после обучения",
            "## Цель задания",
            "Закрепить технику выявления боли и фиксации следующего шага в управляемой структуре.",
            "## Что нужно сделать",
            "1. Провести 10 звонков по новой технике.\n2. В каждом звонке использовать минимум 3 уточняющих вопроса.\n3. Зафиксировать следующий шаг.",
            "## На каких звонках применить",
            "На теплых и повторных звонках, где ранее был срыв по дисциплине фиксации и квалификации.",
            "## Что записать после звонка",
            "Кто ЛПР, какая боль, какие последствия, какой следующий шаг и дата/время.",
            "## Критерии выполнения",
            "Минимум 10 звонков, минимум 5 звонков с полным чек-листом внедрения, 3 ссылки на записи для проверки.",
            "## Срок",
            "До конца текущей рабочей недели.",
            "## Как будет проверяться",
            filler,
        ]
    )


def test_speech_modules_count_accepts_multiple_formats() -> None:
    training_q = review_training_quality(_long_training_material_with_speech_formats())
    assert training_q["speech_modules_count"] >= 10


def test_training_quality_counts_ispolzuy_speech_modules() -> None:
    text = _long_training_material_with_speech_formats().replace(
        'Скажи: "Чтобы не потерять детали, зафиксируем следующий шаг прямо сейчас."',
        '- Используй: "Чтобы не потерять детали, зафиксируем следующий шаг прямо сейчас."',
    )
    q = review_training_quality(text)
    assert q["speech_modules_count"] >= 10


def test_training_quality_counts_numbered_checklist() -> None:
    checklist_lines = "\n".join(
        [
            "1. Проверил контекст звонка.",
            "2) Уточнил ЛПР.",
            "- [ ] Зафиксировал боль и последствия.",
            "- Закрепил следующий шаг с датой.",
            "* Отправил подтверждение на email.",
            "5. Проверил CRM-запись на конкретику.",
            "6) Подготовил материалы к следующему касанию.",
        ]
    )
    text = _long_training_material().replace(
        "\n## Чек-лист на следующий рабочий день\n"
        + "\n".join([f"- Чек-лист пункт {idx}: проверяем конкретный факт внедрения в звонке и CRM." for idx in range(1, 9)]),
        "\n## Чек-лист на следующий рабочий день\n" + checklist_lines,
    )
    q = review_training_quality(text)
    assert q["checklist_items_count"] >= 7


def test_training_quality_not_single_paragraph_when_sections_present() -> None:
    compact = (
        "# Название обучения\n"
        "## Для кого\nСотрудник: тест\n"
        "## Зачем это обучение\nФокус на внедрении.\n"
        "## Что увидели в звонках / дневном контроле\nЕсть повторяющиеся ошибки.\n"
        "## Теория простыми словами\nТеория и объяснение механики.\n"
        "## Основная модель / алгоритм\n1. Контекст 2. Процесс 3. Боль 4. Шаг.\n"
        "## Как применять в звонке\nПрактика на реальных диалогах.\n"
        "## Речевые модули\nИспользуй: \"Подтвердите, что это приоритет?\" Используй: \"Кто принимает решение?\" Используй: \"Когда вернемся к вопросу?\" Используй: \"Что мешает перейти к тесту?\" Используй: \"Какой следующий шаг фиксируем?\" Используй: \"Кто ответственный за запуск?\" Используй: \"Какие риски у текущего процесса?\" Используй: \"Когда удобно провести демо?\" Используй: \"Какой критерий успеха теста?\" Используй: \"Подтвердите дату следующего контакта.\"\n"
        "## Частые ошибки\nРанний переход к презентации.\n"
        "## Мини-тренировка\n1. Переписать фразы 2. Подготовить вопросы.\n"
        "## Чек-лист на следующий рабочий день\n1. Контекст 2. ЛПР 3. Боль 4. Последствия 5. Ценность 6. Следующий шаг 7. Фиксация\n"
        "## Как руководитель будет проверять внедрение\nПроверка записей и звонков."
    )
    q = review_training_quality(compact)
    assert q["sections_count"] >= 8
    assert q["no_single_paragraph_doc"] is True


def test_training_quality_passes_realistic_generated_doc() -> None:
    q = review_training_quality(_long_training_material())
    assert q["quality_passed"] is True


def test_foreign_words_allowlist_is_not_blocker() -> None:
    text = _long_training_material_with_speech_formats() + "\n\n" + (
        "Используем amoCRM, CRM, LPR, demo, email, Tilda, KPI, ROKS, call, lead, pipeline, budget, launch date. "
        "Контекст берем из link/info/plm и файла info_plm_light_industry.md."
    )
    training_q = review_training_quality(text)
    assert training_q["foreign_words_count"] == 0
    assert all("foreign_words_detected" not in reason for reason in training_q["quality_fail_reasons"])
    assert "info_plm_light_industry.md" in training_q.get("foreign_words_warning_examples", [])


def test_training_quality_forbidden_sources_section_and_external_urls_blocked() -> None:
    text = _long_training_material() + "\n\n## Использованные источники\n- https://example.com/source"
    q = review_training_quality(text)
    assert q["quality_passed"] is False
    assert "sources_section_not_allowed" in q["quality_fail_reasons"]
    assert "external_urls_not_allowed" in q["quality_fail_reasons"]


def test_training_quality_business_terms_become_warning_not_blocker() -> None:
    text = _long_training_material_with_speech_formats() + "\n\n" + (
        "Используем Challenger Sale, Sandler, SPIN, BANT, MEDDIC, discovery call, "
        "follow-up, value proposition, pipeline, launch date, SMART."
    )
    q = review_training_quality(text)
    assert all("foreign_words_detected" not in reason for reason in q["quality_fail_reasons"])
    assert q["foreign_words_count"] >= 0


def test_training_quality_validation_rejects_short_single_paragraph() -> None:
    ok, errors = validate_draft_row(
        {
            "training_title": "Короткий документ",
            "training_material": "Однострочный текст без структуры и без нормального объема.",
            "task_title": "Короткое задание",
            "task_material": "Слишком коротко.",
        }
    )
    assert ok is False
    assert any("training_quality:" in item for item in errors)


def test_training_quality_validation_accepts_structured_long_docs() -> None:
    training_text = _long_training_material()
    task_text = _long_task_material()
    ok, errors = validate_draft_row(
        {
            "training_title": "Структурированное обучение",
            "training_material": training_text,
            "task_title": "Структурированное задание",
            "task_material": task_text,
        }
    )
    assert ok is True
    assert errors == []
    training_q = review_training_quality(training_text)
    task_q = review_task_quality(task_text)
    assert training_q["training_chars"] >= 7000
    assert training_q["sections_count"] >= 8
    assert training_q["speech_modules_count"] >= 10
    assert training_q["checklist_items_count"] >= 7
    assert task_q["task_chars"] >= 2500


def test_docs_writer_outputs_multiline_structure() -> None:
    candidate = TrainingCandidate(
        row_number=2,
        plan_week_start="2026-04-27",
        plan_week_end="2026-05-01",
        plan_date="2026-04-29",
        recipient="Тест Менеджер",
        manager_role_profile="manager",
        activity_type="обучение",
        status="запланировано",
        what_i_do="Разбор квалификации",
        task_to_assign="10 звонков по новой технике",
        what_to_check="ЛПР/боль/следующий шаг",
        daily_meeting_thesis="Фокус на квалификации",
        expected_quantity_effect="Рост конверсии",
        expected_quality_effect="Чистая фиксация в CRM",
        training_link="",
        post_training_task_link="",
        topic_hash="abc123",
        idempotency_key="k1",
    )
    draft = TrainingDraft(
        candidate=candidate,
        training_title="Тест обучения",
        training_material=_long_training_material(),
        task_title="Тест задания",
        task_material=_long_task_material(),
        analysis_backend_used="main",
    )
    training_doc = build_training_markdown(draft=draft)
    task_doc = build_task_markdown(draft=draft)
    assert "\n## Для кого\n" in training_doc
    assert "\n## Источник проблемы\n" in training_doc
    assert len([ln for ln in training_doc.splitlines() if ln.strip()]) > 20
    assert "\n## Критерии контроля\n" in task_doc


def test_build_dry_run_without_limit_uses_default_two(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    captured: dict[str, int] = {}
    candidates = [
        TrainingCandidate(
            row_number=i + 2,
            plan_week_start="2026-04-06",
            plan_week_end="2026-04-10",
            plan_date="2026-04-07",
            recipient=f"Менеджер {i}",
            manager_role_profile="manager",
            activity_type="обучение",
            status="запланировано",
            what_i_do="Разобрать квалификацию",
            task_to_assign="10 звонков",
            what_to_check="Следующий шаг",
            daily_meeting_thesis="Тезис",
            expected_quantity_effect="Рост",
            expected_quality_effect="Качество",
            training_link="",
            post_training_task_link="",
            topic_hash=f"h{i}",
            idempotency_key=f"k{i}",
        )
        for i in range(5)
    ]

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: (candidates, {"rows_skipped_existing_links": 0, "plan_rows_total": len(candidates)}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, True, 2, ["A", "B"], ["https://a", "https://b"], [], "ok", []),
        ),
    )

    def _fake_analyze_training_candidates(**kwargs):
        captured["count"] = len(kwargs.get("candidates", []))
        return [], [], {"llm_failed_count": 0, "llm_requests": [], "llm_responses": [], "llm_runtime": {}, "llm_error_examples": []}

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.analyze_training_candidates", _fake_analyze_training_candidates)
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.prepare_local_docs", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.build_post_training_task_payload", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.summarize_task_payload", lambda *_args, **_kwargs: {})

    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir="",
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=None,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=False,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
    )
    _run_build(args)
    assert captured.get("count") == 2


def test_build_blocks_when_external_sources_required_but_unavailable(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidates = [_candidate()]

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: (candidates, {"rows_skipped_existing_links": 0, "plan_rows_total": len(candidates)}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, False, 0, [], [], ["provider_error"], "unavailable", ["external_sources_missing"]),
        ),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.analyze_training_candidates",
        lambda **_kwargs: ([], [], {"llm_failed_count": 0, "llm_requests": [], "llm_responses": [], "llm_runtime": {}, "llm_error_examples": []}),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.prepare_local_docs", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.build_post_training_task_payload", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.summarize_task_payload", lambda *_args, **_kwargs: {})

    run_dir = tmp_root / "run_ext_required"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=1,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
        model_pool="",
        require_external_sources=True,
        allow_no_external_sources=False,
        external_search_provider="auto",
        external_search_limit=5,
        external_source_min_count=2,
        resume_run_dir="",
        retry_failed_from_run_dir="",
    )
    _run_build(args)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["block_reason"] == "external_sources_unavailable"
    assert summary["source_coverage_passed"] is False
    assert summary["source_coverage_failed_rows"] >= 1
    assert (run_dir / "training_materials_external_sources_debug.json").exists()


def test_build_with_allow_no_external_sources_continues_with_warning(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidate = _candidate()

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: ([candidate], {"rows_skipped_existing_links": 0, "plan_rows_total": 1}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, False, 0, [], [], ["provider_error"], "unavailable", ["external_sources_missing"]),
        ),
    )
    draft = TrainingDraft(
        candidate=candidate,
        training_title="Структурированное обучение",
        training_material=_long_training_material(),
        task_title="Структурированное задание",
        task_material=_long_task_material(),
        analysis_backend_used="main",
        quality_metrics={
            "training": review_training_quality(_long_training_material()),
            "task": review_task_quality(_long_task_material()),
        },
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.analyze_training_candidates",
        lambda **_kwargs: (
            [draft],
            [],
            {
                "llm_failed_count": 0,
                "llm_requests": [],
                "llm_responses": [],
                "llm_runtime": {},
                "llm_error_examples": [],
            },
        ),
    )
    run_dir = tmp_root / "run_ext_allow"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=1,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
        model_pool="",
        require_external_sources=True,
        allow_no_external_sources=True,
        external_search_provider="auto",
        external_search_limit=5,
        external_source_min_count=2,
        resume_run_dir="",
        retry_failed_from_run_dir="",
    )
    _run_build(args)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["rows_docs_prepared"] >= 1
    assert summary["block_reason"] != "external_sources_unavailable"
    assert summary["external_sources_used"] is False
    assert summary["source_coverage_passed"] is True


def test_build_passes_source_coverage_when_external_sources_present(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidate = _candidate()

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: ([candidate], {"rows_skipped_existing_links": 0, "plan_rows_total": 1}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(
                1,
                1,
                1,
                True,
                2,
                ["SPIN model", "BANT framework"],
                ["https://example.com/spin", "https://example.com/bant"],
                [],
                "ok",
                [],
            ),
        ),
    )

    draft = TrainingDraft(
        candidate=candidate,
        training_title="Структурированное обучение",
        training_material=_long_training_material(),
        task_title="Структурированное задание",
        task_material=_long_task_material(),
        analysis_backend_used="main",
        quality_metrics={
            "training": review_training_quality(_long_training_material()),
            "task": review_task_quality(_long_task_material()),
        },
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.analyze_training_candidates",
        lambda **_kwargs: (
            [draft],
            [],
            {
                "llm_failed_count": 0,
                "llm_requests": [],
                "llm_responses": [],
                "llm_runtime": {"main": {"model": "qwen3.5:397b-cloud"}, "fallback": {"model": "deepseek-v3.1:671b-cloud"}},
                "llm_error_examples": [],
            },
        ),
    )

    run_dir = tmp_root / "run_ext_ok"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=1,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
        model_pool="",
        require_external_sources=True,
        allow_no_external_sources=False,
        external_search_provider="auto",
        external_search_limit=5,
        external_source_min_count=2,
        resume_run_dir="",
        retry_failed_from_run_dir="",
    )
    _run_build(args)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["source_coverage_passed"] is True
    assert summary["external_sources_used"] is True
    assert summary["external_sources_count"] >= 2
    assert summary["block_reason"] != "external_sources_unavailable"


def test_external_search_limit_does_not_limit_training_candidates(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidates = [
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k1", "row_number": 2}),
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k2", "row_number": 3}),
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k3", "row_number": 4}),
    ]
    captured: dict[str, int] = {}

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: (candidates, {"rows_skipped_existing_links": 0, "plan_rows_total": len(candidates)}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: ([], SourceCoverage(1, 1, 1, True, 2, ["A", "B"], ["https://a", "https://b"], [], "ok", [])),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.analyze_training_candidates",
        lambda **kwargs: (
            [],
            [],
            {
                "llm_failed_count": 0,
                "llm_requests": [],
                "llm_responses": [],
                "llm_runtime": {},
                "llm_error_examples": [],
                "captured_count": captured.setdefault("count", len(kwargs.get("candidates", []))),
            },
        ),
    )
    run_dir = tmp_root / "run_ext_limit"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=0,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
        model_pool="",
        require_external_sources=True,
        allow_no_external_sources=False,
        external_search_provider="auto",
        external_search_limit=1,
        external_source_min_count=2,
        resume_run_dir="",
        retry_failed_from_run_dir="",
    )
    _run_build(args)
    assert captured.get("count") == 3


def test_build_retry_failed_from_run_dir_processes_only_failed_rows(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    failed_run = tmp_root / "failed_run"
    failed_run.mkdir(parents=True, exist_ok=True)
    (failed_run / "training_materials_generation_failures.json").write_text(
        json.dumps({"rows_total": 1, "rows": [{"idempotency_key": "k1", "row_number": 2, "error_type": "ollama_dns_failure"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    candidates = [
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k1", "row_number": 2}),
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k2", "row_number": 3}),
    ]
    captured: dict[str, list[str]] = {}

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: (candidates, {"rows_skipped_existing_links": 0, "plan_rows_total": len(candidates)}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, True, 2, ["A", "B"], ["https://a", "https://b"], [], "ok", []),
        ),
    )

    def _fake_analyze_training_candidates(**kwargs):
        rows = kwargs.get("candidates", [])
        captured["keys"] = [item.idempotency_key for item in rows]
        return [], [], {"llm_failed_count": 0, "llm_requests": [], "llm_responses": [], "llm_runtime": {}, "llm_error_examples": []}

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.analyze_training_candidates", _fake_analyze_training_candidates)
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.prepare_local_docs", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.build_post_training_task_payload", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.summarize_task_payload", lambda *_args, **_kwargs: {})

    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir="",
        resume_run_dir="",
        retry_failed_from_run_dir=str(failed_run),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=0,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
    )
    _run_build(args)
    assert captured.get("keys") == ["k1"]


def test_build_keyboard_interrupt_persists_started_artifacts(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidate = _candidate()

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: ([candidate], {"rows_skipped_existing_links": 0, "plan_rows_total": 1}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, True, 2, ["A", "B"], ["https://a", "https://b"], [], "ok", []),
        ),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.analyze_training_candidates", lambda **_kwargs: (_ for _ in ()).throw(KeyboardInterrupt()))

    run_dir = tmp_root / "explicit_run"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=1,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=False,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
    )
    _run_build(args)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    progress = json.loads((run_dir / "training_materials_progress.json").read_text(encoding="utf-8"))
    assert summary.get("status") == "interrupted"
    assert progress.get("current_stage") == "build_interrupted"
    assert (run_dir / "training_materials_runtime_status.json").exists()
    assert (run_dir / "training_materials_candidate_debug.json").exists()


def test_training_summary_not_passed_after_repair_when_final_quality_failed(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidate = _candidate()

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: ([candidate], {"rows_skipped_existing_links": 0, "plan_rows_total": 1}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, True, 2, ["A", "B"], ["https://a", "https://b"], [], "ok", []),
        ),
    )
    bad_training = "# Название обучения\n## Для кого\nТест"
    bad_task = "# Задание после обучения\n## Цель задания\nТест"
    draft = TrainingDraft(
        candidate=candidate,
        training_title="Короткий документ",
        training_material=bad_training,
        task_title="Короткое задание",
        task_material=bad_task,
        analysis_backend_used="main_targeted_repair",
        quality_metrics={
            "training": review_training_quality(bad_training),
            "task": review_task_quality(bad_task),
        },
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.analyze_training_candidates",
        lambda **_kwargs: (
            [draft],
            [],
            {
                "llm_failed_count": 0,
                "llm_requests": [],
                "llm_responses": [],
                "llm_runtime": {},
                "llm_error_examples": [],
                "model_used_by_row": [
                    {
                        "idempotency_key": candidate.idempotency_key,
                        "row_number": candidate.row_number,
                        "recipient": candidate.recipient,
                        "plan_date": candidate.plan_date,
                        "selected_backend": "main_targeted_repair",
                        "selected_model": "qwen3.5:397b-cloud",
                        "passed_after_repair": True,
                        "final_quality_passed": None,
                    }
                ],
                "rows_passed_after_repair": 1,
            },
        ),
    )
    run_dir = tmp_root / "run_quality_fail"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-20",
        week_end="2026-04-24",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=1,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
        model_pool="",
        require_external_sources=True,
        allow_no_external_sources=False,
        external_search_provider="auto",
        external_search_limit=5,
        external_source_min_count=2,
        resume_run_dir="",
        retry_failed_from_run_dir="",
    )
    _run_build(args)
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    quarantine = json.loads((run_dir / "training_materials_quarantine.json").read_text(encoding="utf-8"))
    assert summary["block_reason"] == "quality_gate_failed"
    assert summary["quality_rows_failed"] >= 1
    assert quarantine["rows_quarantined"] >= 1
    assert summary["model_used_by_row"][0]["passed_after_repair"] is False
    assert summary["model_used_by_row"][0]["final_quality_passed"] is False


def test_training_progress_increments_candidates(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    project_root = tmp_root / "project"
    logs_dir = tmp_root / "logs"
    project_root.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    candidates = [
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k1", "row_number": 2}),
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k2", "row_number": 3}),
        TrainingCandidate(**{**asdict(_candidate()), "idempotency_key": "k3", "row_number": 4}),
    ]

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=project_root, logs_dir=logs_dir),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "missing_scopes": [], "scope_mismatch_detected": False, "reauth_required": False},
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_training_candidates",
        lambda **_kwargs: (candidates, {"rows_skipped_existing_links": 0, "plan_rows_total": len(candidates)}),
    )
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.collect_source_snippets",
        lambda **_kwargs: (
            [],
            SourceCoverage(1, 1, 1, True, 2, ["A", "B"], ["https://a", "https://b"], [], "ok", []),
        ),
    )

    def _fake_analyze_training_candidates(**kwargs):
        on_progress = kwargs.get("on_progress")
        rows = kwargs.get("candidates", [])
        for idx, item in enumerate(rows):
            if callable(on_progress):
                on_progress({"stage": "candidate_started", "candidate_index": idx, "row_number": item.row_number, "recipient": item.recipient})
                on_progress({"stage": "candidate_prepared", "candidate_index": idx, "row_number": item.row_number, "recipient": item.recipient})
        return [], [], {"llm_failed_count": 0, "llm_requests": [], "llm_responses": [], "llm_runtime": {}, "llm_error_examples": []}

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.analyze_training_candidates", _fake_analyze_training_candidates)
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.prepare_local_docs", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.build_post_training_task_payload", lambda **_kwargs: [])
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.summarize_task_payload", lambda *_args, **_kwargs: {})

    run_dir = tmp_root / "run_progress"
    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        week_start="2026-04-06",
        week_end="2026-04-10",
        plan_sheet="План недели",
        daily_sheet="Дневной контроль",
        call_review_sheet="Разбор звонков",
        manager="",
        plan_date="",
        limit=0,
        offset=0,
        max_runtime_minutes=0,
        max_llm_calls=0,
        main_timeout=0,
        fallback_timeout=0,
        allow_template_fallback=False,
        allow_full_run=True,
        resume=False,
        dry_run=True,
        force_reauth=False,
        main_model="",
        fallback_model="",
        model_pool="",
        require_external_sources=True,
        allow_no_external_sources=False,
        external_search_provider="auto",
        external_search_limit=5,
        external_source_min_count=2,
        resume_run_dir="",
        retry_failed_from_run_dir="",
    )
    _run_build(args)
    progress_log = (run_dir / "progress.log").read_text(encoding="utf-8")
    assert "candidate_started 1/3" in progress_log
    assert "candidate_started 2/3" in progress_log
    assert "candidate_started 3/3" in progress_log


def test_write_with_empty_payload_and_generation_failures_returns_llm_generation_failed(monkeypatch) -> None:
    tmp_root = _new_tmp_root()
    run_dir = tmp_root / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "training_materials_payload.json").write_text(
        json.dumps({"rows": [], "rows_count": 0}, ensure_ascii=False),
        encoding="utf-8",
    )
    (run_dir / "training_materials_generation_failures.json").write_text(
        json.dumps(
            {
                "rows_total": 1,
                "rows": [
                    {
                        "idempotency_key": "k1",
                        "row_number": 12,
                        "recipient": "Рустам",
                        "plan_date": "2026-04-10",
                        "error_type": "ollama_dns_failure",
                        "final_reason": "lookup ollama.com: no such host",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "week_start": "2026-04-06",
                "week_end": "2026-04-10",
                "daily_sheet": "Дневной контроль",
                "call_review_sheet": "Разбор звонков",
                "llm_main_model": "qwen3.5:397b-cloud",
                "llm_fallback_model": "deepseek-v3.1:671b-cloud",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.load_deal_analyzer_config", lambda _p: _cfg(tmp_root))
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.load_config",
        lambda: SimpleNamespace(project_root=tmp_root, logs_dir=tmp_root / "logs"),
    )
    monkeypatch.setattr("src.deal_analyzer.training_materials.cli.setup_logging", lambda *_args, **_kwargs: _DummyLogger())
    monkeypatch.setattr(
        "src.deal_analyzer.training_materials.cli.ensure_training_materials_oauth_scopes",
        lambda **_kwargs: {"status": "ok", "docs_api_available": True, "drive_api_status": "available"},
    )

    args = SimpleNamespace(
        config=str(tmp_root / "cfg.json"),
        run_dir=str(run_dir),
        plan_sheet="План недели",
        dry_run=True,
        write=False,
        strict_preflight=True,
        overwrite_links=False,
        force_reauth=False,
    )
    _run_write(args)
    status = json.loads((run_dir / "training_materials_writer_status.json").read_text(encoding="utf-8"))
    assert status["block_reason"] == "llm_generation_failed"
    assert status["write_allowed"] is False
    assert status["llm_error_summary_by_type"]["ollama_dns_failure"] == 1
    assert "retry-failed-from-run-dir" in status.get("retry_command_suggestion", "")
