from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from src.deal_analyzer.employee_dashboard.aggregator import build_employee_dashboard


_WORK_TMP_ROOT = Path("workspace") / "tmp_tests" / "employee_dashboard"


def _new_case_dir() -> Path:
    case_dir = _WORK_TMP_ROOT / uuid.uuid4().hex
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _seed_employee_sources(root: Path, *, employee: str = "Илья Бочков") -> None:
    call_run = root / "workspace" / "deal_analyzer" / "period_runs" / "20260401_000000"
    _write_json(
        call_run / "call_review_sheet_payload.json",
        {
            "rows": [
                {
                    "Дата кейса": "2026-04-10",
                    "Менеджер": employee,
                    "Роль": "менеджер по продажам",
                    "Deal ID": "123",
                    "Ссылка на сделку": "https://crm/deals/123",
                    "Сильная сторона": "Четко фиксирует следующий шаг",
                    "Зона роста": "Слабо отрабатывает возражение дорого",
                    "Что исправить": "Уточнять бюджет и критерии решения",
                    "Что донести сотруднику": "Используй: \"Давайте согласуем дату демо\"",
                    "Комментарий по этапу (отработка возражений)": "Клиент сказал дорого, менеджер ушел в скидку",
                }
            ]
        },
    )
    _write_json(
        call_run / "call_review_v3" / "transcript_readiness_debug.json",
        [
            {
                "deal_id": "123",
                "transcript_chars": 2500,
                "transcript_source": "workspace/cache/transcripts/123.txt",
            }
        ],
    )

    daily_run = root / "workspace" / "daily_control" / "20260411_000000"
    _write_json(
        daily_run / "daily_control_payload.json",
        {
            "rows": [
                {
                    "control_day_date": "2026-04-10",
                    "manager_name": employee,
                    "manager_role_profile": "менеджер по продажам",
                    "deal_links": "https://crm/deals/123",
                    "strong_sides": "Держит структуру разговора",
                    "growth_zones": "Теряет инициативу на возражении не актуально",
                    "what_to_fix": "Фиксировать время следующего контакта",
                    "what_to_tell_employee": "Скажи: \"Какой следующий шаг фиксируем сейчас?\"",
                }
            ]
        },
    )

    weekly_run = root / "workspace" / "weekly_manager_summary" / "20260412_000000"
    _write_json(
        weekly_run / "weekly_manager_payload.json",
        {
            "rows": [
                {
                    "week_start": "2026-04-06",
                    "week_end": "2026-04-10",
                    "manager_name": employee,
                    "manager_role_profile": "менеджер по продажам",
                    "improved": "Лучше закрывает на демонстрацию",
                    "not_improved": "Плавает в критериях теста",
                    "repeating_mistakes": "Не фиксирует дату решения",
                    "employee_message": "Формулировка: \"Когда согласуем финальный созвон?\"",
                    "manager_actions_next_week": "Тренировка закрытия на следующий шаг",
                    "training_for_employee": "Дожим после демо",
                    "training_link": "https://docs/train1",
                }
            ]
        },
    )

    training_run = root / "workspace" / "training_materials" / "20260413_000000"
    _write_json(
        training_run / "training_materials_payload.json",
        {
            "rows": [
                {
                    "plan_date": "2026-04-10",
                    "recipient": employee,
                    "training_title": "Тест -> счет",
                    "training_material": "## Речевые модули\n1. Используй: \"Подтвердите критерий успеха теста\"\n2. \"Когда удобно обсудить счет?\"",
                    "task_material": "Провести 3 дожима по активным тестам",
                    "training_doc_local_path": "workspace/training/train.md",
                    "task_doc_local_path": "workspace/training/task.md",
                }
            ]
        },
    )


def test_employee_dashboard_aggregates_strengths() -> None:
    root = _new_case_dir()
    try:
        _seed_employee_sources(root)
        summary, evidence_index, _speech_debug, _extra = build_employee_dashboard(
            project_root=root,
            employee_name="Илья Бочков",
            period_start="2026-03-30",
            period_end="2026-04-30",
        )
        assert summary.evidence_count > 0
        assert summary.source_coverage_passed is True
        assert any("следующий шаг" in item.lower() for item in summary.strengths)
        assert evidence_index["rows_total"] == summary.evidence_count
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_employee_dashboard_tracks_speech_modules() -> None:
    root = _new_case_dir()
    try:
        _seed_employee_sources(root)
        summary, _evidence_index, speech_debug, _extra = build_employee_dashboard(
            project_root=root,
            employee_name="Илья Бочков",
            period_start="2026-03-30",
            period_end="2026-04-30",
        )
        assert len(summary.successful_speech_modules) > 0 or len(summary.failed_speech_modules) > 0
        rows = speech_debug.get("rows", [])
        assert isinstance(rows, list)
        assert any("демо" in str(row.get("phrase", "")).lower() for row in rows)
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_employee_dashboard_tracks_objections() -> None:
    root = _new_case_dir()
    try:
        _seed_employee_sources(root)
        summary, _evidence_index, _speech_debug, extra = build_employee_dashboard(
            project_root=root,
            employee_name="Илья Бочков",
            period_start="2026-03-30",
            period_end="2026-04-30",
        )
        objections_debug = extra.get("objection_patterns", {})
        failures = summary.objection_failures
        assert isinstance(failures, tuple)
        assert any("дорого" in str(row.get("objection", "")).lower() for row in failures)
        assert "summary" in objections_debug
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_employee_dashboard_requires_evidence() -> None:
    root = _new_case_dir()
    try:
        summary, evidence_index, speech_debug, extra = build_employee_dashboard(
            project_root=root,
            employee_name="Рустам Хомидов",
            period_start="2026-03-30",
            period_end="2026-04-30",
        )
        assert summary.evidence_count == 0
        assert summary.source_coverage_passed is False
        assert summary.confidence_score == 0
        assert evidence_index["rows_total"] == 0
        assert speech_debug.get("rows", []) == []
        assert "objection_patterns" in extra
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_employee_dashboard_confidence_score() -> None:
    root = _new_case_dir()
    try:
        rich_root = root / "rich"
        sparse_root = root / "sparse"
        _seed_employee_sources(rich_root)

        _write_json(
            sparse_root / "workspace" / "daily_control" / "20260411_000000" / "daily_control_payload.json",
            {
                "rows": [
                    {
                        "control_day_date": "2026-04-10",
                        "manager_name": "Илья Бочков",
                        "strong_sides": "Есть контакт с клиентом",
                    }
                ]
            },
        )

        rich, *_ = build_employee_dashboard(
            project_root=rich_root,
            employee_name="Илья Бочков",
            period_start="2026-03-30",
            period_end="2026-04-30",
        )
        sparse, *_ = build_employee_dashboard(
            project_root=sparse_root,
            employee_name="Илья Бочков",
            period_start="2026-03-30",
            period_end="2026-04-30",
        )

        assert rich.confidence_score > sparse.confidence_score
    finally:
        shutil.rmtree(root, ignore_errors=True)
