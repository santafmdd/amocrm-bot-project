from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import EmployeeDashboardSummary, EvidenceItem
from .objection_analyzer import analyze_objections
from .pattern_analyzer import analyze_behavior_patterns
from .speech_module_extractor import collect_speech_module_items, summarize_speech_modules


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()


def _norm(value: Any) -> str:
    return _clean(value).lower()


def _safe_json_load(path: Path) -> Any:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def _get_row_value(row: dict[str, Any], *aliases: str) -> Any:
    if not isinstance(row, dict):
        return ""
    for alias in aliases:
        if alias in row:
            return row.get(alias)
    folded = {str(k).strip().lower(): v for k, v in row.items()}
    for alias in aliases:
        key = alias.strip().lower()
        if key in folded:
            return folded.get(key)
    return ""


def _parse_date(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    probe = text[:10]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(probe, fmt)
        except Exception:
            continue
    return None


def _date_in_period(value: Any, *, period_start: datetime, period_end: datetime) -> bool:
    parsed = _parse_date(value)
    if parsed is None:
        return False
    return period_start <= parsed <= period_end


def _week_overlaps_period(
    week_start: Any,
    week_end: Any,
    *,
    period_start: datetime,
    period_end: datetime,
) -> bool:
    ws = _parse_date(week_start)
    we = _parse_date(week_end)
    if ws is None or we is None:
        return False
    if we < period_start:
        return False
    if ws > period_end:
        return False
    return True


def _append_evidence(
    out: list[EvidenceItem],
    *,
    source: str,
    employee_name: str,
    role: str,
    evidence_date: str,
    text: Any,
    evidence_link: str,
    category: str,
    outcome: str,
    confidence: float,
    meta: dict[str, Any] | None = None,
) -> None:
    payload = _clean(text)
    if not payload:
        return
    out.append(
        EvidenceItem(
            source=source,
            employee_name=_clean(employee_name),
            role=_clean(role),
            evidence_date=_clean(evidence_date),
            text=payload,
            evidence_link=_clean(evidence_link),
            category=category,
            outcome="success" if outcome == "success" else "failure" if outcome == "failure" else "neutral",
            confidence=max(0.0, min(1.0, float(confidence or 0.0))),
            meta=dict(meta or {}),
        )
    )


def _collect_call_review_rows(
    *,
    project_root: Path,
    employee: str,
    period_start: datetime,
    period_end: datetime,
) -> tuple[list[EvidenceItem], dict[str, dict[str, Any]]]:
    out: list[EvidenceItem] = []
    deal_to_context: dict[str, dict[str, Any]] = {}
    runs_root = project_root / "workspace" / "deal_analyzer" / "period_runs"
    if not runs_root.exists():
        return out, deal_to_context

    for run_dir in sorted((item for item in runs_root.iterdir() if item.is_dir()), key=lambda item: item.name):
        payload = _safe_json_load(run_dir / "call_review_sheet_payload.json")
        rows = payload.get("rows", []) if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []

        for row in rows:
            if not isinstance(row, dict):
                continue
            manager_name = _clean(_get_row_value(row, "Менеджер", "manager_name"))
            if _norm(manager_name) != _norm(employee):
                continue

            case_date = _clean(_get_row_value(row, "Дата кейса", "Дата анализа", "analysis_date"))
            if not _date_in_period(case_date, period_start=period_start, period_end=period_end):
                continue

            role = _clean(_get_row_value(row, "Роль", "manager_role_profile"))
            link = _clean(_get_row_value(row, "Ссылка на сделку", "deal_url", "contact_url"))
            deal_id = _clean(_get_row_value(row, "Deal ID", "deal_id"))
            if deal_id:
                deal_to_context[deal_id] = {
                    "employee_name": manager_name,
                    "role": role,
                    "evidence_date": case_date,
                    "link": link,
                }

            _append_evidence(
                out,
                source="call_review",
                employee_name=manager_name,
                role=role,
                evidence_date=case_date,
                text=_get_row_value(row, "Сильная сторона", "strong_side"),
                evidence_link=link,
                category="strength",
                outcome="success",
                confidence=0.78,
                meta={"field": "Сильная сторона", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="call_review",
                employee_name=manager_name,
                role=role,
                evidence_date=case_date,
                text=_get_row_value(row, "Зона роста", "growth_zone"),
                evidence_link=link,
                category="growth_zone",
                outcome="failure",
                confidence=0.8,
                meta={"field": "Зона роста", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="call_review",
                employee_name=manager_name,
                role=role,
                evidence_date=case_date,
                text=_get_row_value(row, "Что исправить", "what_to_fix"),
                evidence_link=link,
                category="recurring_mistake",
                outcome="failure",
                confidence=0.79,
                meta={"field": "Что исправить", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="call_review",
                employee_name=manager_name,
                role=role,
                evidence_date=case_date,
                text=_get_row_value(row, "Что донести сотруднику", "employee_message"),
                evidence_link=link,
                category="speech_module",
                outcome="neutral",
                confidence=0.7,
                meta={"field": "Что донести сотруднику", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="call_review",
                employee_name=manager_name,
                role=role,
                evidence_date=case_date,
                text=_get_row_value(
                    row,
                    "Комментарий по этапу (отработка возражений)",
                    "Комментарий по этапу (разговор с ЛПР)",
                    "objection_comment",
                ),
                evidence_link=link,
                category="objection",
                outcome="failure",
                confidence=0.72,
                meta={"field": "objection_comment", "run_dir": str(run_dir)},
            )

        transcript_debug = run_dir / "call_review_v3" / "transcript_readiness_debug.json"
        items = _safe_json_load(transcript_debug)
        transcript_rows = items if isinstance(items, list) else []
        for item in transcript_rows:
            if not isinstance(item, dict):
                continue
            deal_id = _clean(item.get("deal_id"))
            context = deal_to_context.get(deal_id, {})
            if _norm(context.get("employee_name")) != _norm(employee):
                continue
            date_hint = _clean(context.get("evidence_date"))
            if not _date_in_period(date_hint, period_start=period_start, period_end=period_end):
                continue
            transcript_chars = int(item.get("transcript_chars", 0) or 0)
            if transcript_chars <= 0:
                continue
            _append_evidence(
                out,
                source="transcript",
                employee_name=_clean(context.get("employee_name")),
                role=_clean(context.get("role")),
                evidence_date=date_hint,
                text=f"Transcript available: {transcript_chars} chars",
                evidence_link=_clean(context.get("link")) or _clean(item.get("transcript_source")),
                category="transcript",
                outcome="neutral",
                confidence=0.66,
                meta={
                    "deal_id": deal_id,
                    "transcript_chars": transcript_chars,
                    "run_dir": str(run_dir),
                },
            )

    return out, deal_to_context


def _collect_daily_control_rows(
    *,
    project_root: Path,
    employee: str,
    period_start: datetime,
    period_end: datetime,
) -> list[EvidenceItem]:
    out: list[EvidenceItem] = []
    runs_root = project_root / "workspace" / "daily_control"
    if not runs_root.exists():
        return out

    for run_dir in sorted((item for item in runs_root.iterdir() if item.is_dir()), key=lambda item: item.name):
        payload = _safe_json_load(run_dir / "daily_control_payload.json")
        rows = payload.get("rows", []) if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            manager_name = _clean(_get_row_value(row, "manager_name", "Менеджер"))
            if _norm(manager_name) != _norm(employee):
                continue
            date_hint = _clean(_get_row_value(row, "control_day_date", "date"))
            if not _date_in_period(date_hint, period_start=period_start, period_end=period_end):
                continue
            role = _clean(_get_row_value(row, "manager_role_profile", "Роль"))
            link = _clean(_get_row_value(row, "deal_links", "Ссылка на сделку"))

            _append_evidence(
                out,
                source="daily_control",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "strong_sides", "Сильные стороны"),
                evidence_link=link,
                category="strength",
                outcome="success",
                confidence=0.78,
                meta={"field": "strong_sides", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="daily_control",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "growth_zones", "Зоны роста"),
                evidence_link=link,
                category="growth_zone",
                outcome="failure",
                confidence=0.8,
                meta={"field": "growth_zones", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="daily_control",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "what_to_fix", "Что исправить"),
                evidence_link=link,
                category="recurring_mistake",
                outcome="failure",
                confidence=0.8,
                meta={"field": "what_to_fix", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="daily_control",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "what_to_tell_employee", "Что донести сотруднику"),
                evidence_link=link,
                category="speech_module",
                outcome="neutral",
                confidence=0.69,
                meta={"field": "what_to_tell_employee", "run_dir": str(run_dir)},
            )

    return out


def _collect_weekly_summary_rows(
    *,
    project_root: Path,
    employee: str,
    period_start: datetime,
    period_end: datetime,
) -> list[EvidenceItem]:
    out: list[EvidenceItem] = []
    runs_root = project_root / "workspace" / "weekly_manager_summary"
    if not runs_root.exists():
        return out

    for run_dir in sorted((item for item in runs_root.iterdir() if item.is_dir()), key=lambda item: item.name):
        payload = _safe_json_load(run_dir / "weekly_manager_payload.json")
        rows = payload.get("rows", []) if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            manager_name = _clean(_get_row_value(row, "manager_name", "Менеджер"))
            if _norm(manager_name) != _norm(employee):
                continue
            if not _week_overlaps_period(
                _get_row_value(row, "week_start", "Период с"),
                _get_row_value(row, "week_end", "Период по"),
                period_start=period_start,
                period_end=period_end,
            ):
                continue

            role = _clean(_get_row_value(row, "manager_role_profile", "Роль"))
            date_hint = _clean(_get_row_value(row, "week_end", "week_start"))

            _append_evidence(
                out,
                source="weekly_manager_summary",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "improved", "Улучшилось"),
                evidence_link="",
                category="strength",
                outcome="success",
                confidence=0.74,
                meta={"field": "improved", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="weekly_manager_summary",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "not_improved", "Не улучшилось"),
                evidence_link="",
                category="growth_zone",
                outcome="failure",
                confidence=0.74,
                meta={"field": "not_improved", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="weekly_manager_summary",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "repeating_mistakes", "Повторяющиеся ошибки"),
                evidence_link="",
                category="recurring_mistake",
                outcome="failure",
                confidence=0.78,
                meta={"field": "repeating_mistakes", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="weekly_manager_summary",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "employee_message", "Сообщение сотруднику"),
                evidence_link="",
                category="speech_module",
                outcome="neutral",
                confidence=0.66,
                meta={"field": "employee_message", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="weekly_manager_summary",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "manager_actions_next_week", "Действия руководителя на следующую неделю"),
                evidence_link="",
                category="recommended_theme",
                outcome="neutral",
                confidence=0.75,
                meta={"field": "manager_actions_next_week", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="weekly_manager_summary",
                employee_name=manager_name,
                role=role,
                evidence_date=date_hint,
                text=_get_row_value(row, "training_for_employee", "Обучение сотруднику"),
                evidence_link=_clean(_get_row_value(row, "training_link", "Ссылка на обучение")),
                category="recommended_theme",
                outcome="neutral",
                confidence=0.74,
                meta={"field": "training_for_employee", "run_dir": str(run_dir)},
            )

    return out


def _collect_training_rows(
    *,
    project_root: Path,
    employee: str,
    period_start: datetime,
    period_end: datetime,
) -> list[EvidenceItem]:
    out: list[EvidenceItem] = []
    runs_root = project_root / "workspace" / "training_materials"
    if not runs_root.exists():
        return out

    for run_dir in sorted((item for item in runs_root.iterdir() if item.is_dir()), key=lambda item: item.name):
        payload = _safe_json_load(run_dir / "training_materials_payload.json")
        rows = payload.get("rows", []) if isinstance(payload, dict) and isinstance(payload.get("rows"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            recipient = _clean(_get_row_value(row, "recipient", "Адресат"))
            if _norm(recipient) != _norm(employee):
                continue
            date_hint = _clean(_get_row_value(row, "plan_date", "Дата"))
            if not _date_in_period(date_hint, period_start=period_start, period_end=period_end):
                continue

            training_link = _clean(_get_row_value(row, "training_doc_local_path", "training_link", "Ссылка на обучение / материал"))
            task_link = _clean(_get_row_value(row, "task_doc_local_path", "post_training_task_link", "Ссылка на задачи после обучения"))

            _append_evidence(
                out,
                source="training_materials",
                employee_name=recipient,
                role="",
                evidence_date=date_hint,
                text=_get_row_value(row, "training_title", "training_topic", "Общий тезис на дейлик"),
                evidence_link=training_link,
                category="recommended_theme",
                outcome="neutral",
                confidence=0.77,
                meta={"field": "training_title", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="training_materials",
                employee_name=recipient,
                role="",
                evidence_date=date_hint,
                text=_get_row_value(row, "training_material"),
                evidence_link=training_link,
                category="speech_module",
                outcome="success",
                confidence=0.7,
                meta={"field": "training_material", "run_dir": str(run_dir)},
            )
            _append_evidence(
                out,
                source="training_materials",
                employee_name=recipient,
                role="",
                evidence_date=date_hint,
                text=_get_row_value(row, "task_material"),
                evidence_link=task_link,
                category="recurring_mistake",
                outcome="neutral",
                confidence=0.64,
                meta={"field": "task_material", "run_dir": str(run_dir)},
            )

    return out


def _top_texts(
    evidence_items: list[EvidenceItem],
    *,
    category: str,
    outcome: str | None = None,
    top_n: int = 12,
) -> list[str]:
    counter: Counter[str] = Counter()
    for item in evidence_items:
        if item.category != category:
            continue
        if outcome and item.outcome != outcome:
            continue
        text = _clean(item.text)
        if text:
            counter[text] += 1
    return [text for text, _count in counter.most_common(max(1, int(top_n or 12)))]


def _build_recommended_themes(
    *,
    evidence_items: list[EvidenceItem],
    objection_failures: list[dict[str, Any]],
    recurring_mistakes: list[str],
    top_n: int = 8,
) -> list[str]:
    counter: Counter[str] = Counter()
    for text in _top_texts(evidence_items, category="recommended_theme", top_n=top_n * 2):
        counter[text] += 2
    for text in recurring_mistakes[: top_n * 2]:
        counter[text] += 1
    for row in objection_failures:
        objection = _clean(row.get("objection"))
        if objection:
            counter[f"Отработка возражения: {objection}"] += int(row.get("count", 0) or 0)
    return [text for text, _count in counter.most_common(max(1, int(top_n or 8)))]


def _resolve_role(evidence_items: list[EvidenceItem]) -> str:
    counter: Counter[str] = Counter()
    for item in evidence_items:
        role = _clean(item.role)
        if role:
            counter[role] += 1
    return counter.most_common(1)[0][0] if counter else ""


def _build_evidence_links(evidence_items: list[EvidenceItem], *, limit: int = 200) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for item in evidence_items:
        link = _clean(item.evidence_link)
        if not link or link in seen:
            continue
        seen.add(link)
        links.append(link)
        if len(links) >= max(1, int(limit or 200)):
            break
    return links


def _build_source_coverage(evidence_items: list[EvidenceItem]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in evidence_items:
        counter[item.source] += 1
    return dict(counter)


def _confidence_score(
    *,
    evidence_count: int,
    source_coverage: dict[str, int],
    speech_total: int,
    objection_total: int,
) -> int:
    if evidence_count <= 0:
        return 0
    sources_present = len([name for name, count in source_coverage.items() if int(count or 0) > 0])
    score = (
        min(35, evidence_count * 2)
        + min(20, speech_total)
        + min(20, objection_total * 3)
        + min(25, sources_present * 6)
    )
    return max(0, min(100, int(score)))


def build_employee_dashboard(
    *,
    project_root: Path,
    employee_name: str,
    period_start: str,
    period_end: str,
) -> tuple[EmployeeDashboardSummary, dict[str, Any], dict[str, Any], dict[str, Any]]:
    start_dt = datetime.strptime(str(period_start), "%Y-%m-%d")
    end_dt = datetime.strptime(str(period_end), "%Y-%m-%d")

    call_review_items, _ = _collect_call_review_rows(
        project_root=project_root,
        employee=employee_name,
        period_start=start_dt,
        period_end=end_dt,
    )
    daily_items = _collect_daily_control_rows(
        project_root=project_root,
        employee=employee_name,
        period_start=start_dt,
        period_end=end_dt,
    )
    weekly_items = _collect_weekly_summary_rows(
        project_root=project_root,
        employee=employee_name,
        period_start=start_dt,
        period_end=end_dt,
    )
    training_items = _collect_training_rows(
        project_root=project_root,
        employee=employee_name,
        period_start=start_dt,
        period_end=end_dt,
    )

    evidence_items: list[EvidenceItem] = [*call_review_items, *daily_items, *weekly_items, *training_items]

    strengths = _top_texts(evidence_items, category="strength", outcome="success", top_n=12)
    growth_zones = _top_texts(evidence_items, category="growth_zone", outcome="failure", top_n=12)

    speech_items = collect_speech_module_items(evidence_items)
    successful_speech_modules, failed_speech_modules, speech_debug = summarize_speech_modules(speech_items, top_n=12)

    objection_success, objection_failures, objection_debug = analyze_objections(evidence_items, top_n=8)

    behavior_patterns, recurring_mistakes, pattern_debug = analyze_behavior_patterns(
        strengths=strengths,
        growth_zones=growth_zones,
        evidence_items=evidence_items,
        top_n=10,
    )

    recommended_training_topics = _build_recommended_themes(
        evidence_items=evidence_items,
        objection_failures=objection_failures,
        recurring_mistakes=recurring_mistakes,
        top_n=10,
    )

    evidence_links = _build_evidence_links(evidence_items, limit=300)
    source_coverage = _build_source_coverage(evidence_items)
    confidence = _confidence_score(
        evidence_count=len(evidence_items),
        source_coverage=source_coverage,
        speech_total=len(successful_speech_modules) + len(failed_speech_modules),
        objection_total=len(objection_success) + len(objection_failures),
    )
    source_coverage_passed = len([k for k, v in source_coverage.items() if int(v or 0) > 0]) >= 2 and len(evidence_items) >= 5

    summary = EmployeeDashboardSummary(
        employee_name=employee_name,
        role=_resolve_role(evidence_items),
        period_start=period_start,
        period_end=period_end,
        strengths=tuple(strengths),
        growth_zones=tuple(growth_zones),
        successful_speech_modules=tuple(successful_speech_modules),
        failed_speech_modules=tuple(failed_speech_modules),
        objection_success=tuple(objection_success),
        objection_failures=tuple(objection_failures),
        behavior_patterns=tuple(behavior_patterns),
        recurring_mistakes=tuple(recurring_mistakes),
        recommended_training_topics=tuple(recommended_training_topics),
        evidence_links=tuple(evidence_links),
        confidence_score=confidence,
        evidence_count=len(evidence_items),
        source_coverage=source_coverage,
        source_coverage_passed=source_coverage_passed,
    )

    evidence_index = {
        "rows_total": len(evidence_items),
        "rows": [
            {
                "source": item.source,
                "employee_name": item.employee_name,
                "role": item.role,
                "evidence_date": item.evidence_date,
                "category": item.category,
                "outcome": item.outcome,
                "confidence": item.confidence,
                "evidence_link": item.evidence_link,
                "text_preview": item.text[:300],
                "meta": item.meta,
            }
            for item in evidence_items
        ],
    }

    speech_debug_full = {
        "summary": speech_debug,
        "examples_total": len(speech_items),
        "rows": [
            {
                "phrase": item.phrase,
                "outcome": item.outcome,
                "source": item.source,
                "evidence_link": item.evidence_link,
                "evidence_date": item.evidence_date,
            }
            for item in speech_items
        ],
    }

    objection_debug_full = {
        "summary": objection_debug,
        "source_coverage": source_coverage,
    }

    pattern_debug_full = {
        "summary": pattern_debug,
        "behavior_patterns": behavior_patterns,
        "recurring_mistakes": recurring_mistakes,
    }

    debug = {
        "speech_modules": speech_debug_full,
        "objections": objection_debug_full,
        "patterns": pattern_debug_full,
    }

    return summary, evidence_index, speech_debug_full, {
        "objection_patterns": objection_debug_full,
        "behavior_patterns": pattern_debug_full,
        "debug": debug,
    }
