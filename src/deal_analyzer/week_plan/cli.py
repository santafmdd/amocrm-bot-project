from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from src.deal_analyzer.config import DealAnalyzerConfig, load_deal_analyzer_config
from src.deal_analyzer.daily_control.source_reader import clean_text, day_label_from_iso
from src.deal_analyzer.week_plan.artifacts import write_json, write_markdown
from src.deal_analyzer.week_plan.idempotency import build_exact_key
from src.deal_analyzer.week_plan.plan_analyzer import analyze_week_plan_groups
from src.deal_analyzer.week_plan.roks_enrichment import build_roks_oap_snapshot
from src.deal_analyzer.week_plan.sheets_writer import (
    build_discovery_markdown,
    discover_week_plan_sheet,
    execute_week_plan_write,
)
from src.deal_analyzer.week_plan.source_reader import map_source_headers, read_daily_control_source, resolve_spreadsheet_id
from src.deal_analyzer.week_plan.validation import (
    lint_has_blockers,
    lint_week_plan_text_rows,
    payload_has_blockers,
    validate_week_plan_payload_rows,
)
from src.deal_analyzer.week_plan.weekly_signal_builder import group_daily_rows_into_week_signals
from src.deal_analyzer.weekly_shared.week_plan_reader import read_week_plan_rows


def _parse_iso_date(value: str, *, field: str) -> datetime:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except Exception as exc:
        raise RuntimeError(f"Invalid {field}: {value}. Expected YYYY-MM-DD") from exc


def _new_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "week_plan" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _manager_allowlist(cfg: DealAnalyzerConfig, cli_values: list[str] | None) -> tuple[str, ...]:
    if cli_values:
        values = tuple(str(x).strip() for x in cli_values if str(x).strip())
        if values:
            return values
    cfg_values = tuple(str(x).strip() for x in (cfg.daily_manager_allowlist or ()) if str(x).strip())
    if cfg_values:
        return cfg_values
    return ("Илья Бочков", "Рустам Хомидов")


def _resolve_signal_and_plan_periods(args: argparse.Namespace) -> dict[str, Any]:
    signal_start_raw = str(getattr(args, "signal_start", "") or "").strip()
    signal_end_raw = str(getattr(args, "signal_end", "") or "").strip()
    plan_start_raw = str(getattr(args, "plan_week_start", "") or "").strip()
    plan_end_raw = str(getattr(args, "plan_week_end", "") or "").strip()
    legacy_period_start_raw = str(getattr(args, "period_start", "") or "").strip()
    legacy_period_end_raw = str(getattr(args, "period_end", "") or "").strip()

    has_new = any([signal_start_raw, signal_end_raw, plan_start_raw, plan_end_raw])
    warnings: list[str] = []
    if has_new:
        missing: list[str] = []
        if not signal_start_raw:
            missing.append("signal_start")
        if not signal_end_raw:
            missing.append("signal_end")
        if not plan_start_raw:
            missing.append("plan_week_start")
        if not plan_end_raw:
            missing.append("plan_week_end")
        if missing:
            raise RuntimeError(f"Missing required period args: {', '.join(missing)}")
        signal_start = _parse_iso_date(signal_start_raw, field="signal_start").date()
        signal_end = _parse_iso_date(signal_end_raw, field="signal_end").date()
        plan_week_start = _parse_iso_date(plan_start_raw, field="plan_week_start").date()
        plan_week_end = _parse_iso_date(plan_end_raw, field="plan_week_end").date()
    else:
        if not legacy_period_start_raw or not legacy_period_end_raw:
            raise RuntimeError(
                "Provide either --signal-start/--signal-end + --plan-week-start/--plan-week-end "
                "or legacy --period-start/--period-end."
            )
        signal_start = _parse_iso_date(legacy_period_start_raw, field="period_start").date()
        signal_end = _parse_iso_date(legacy_period_end_raw, field="period_end").date()
        plan_week_start = signal_start
        plan_week_end = signal_end
        warnings.append("legacy_period_used_for_signal_and_plan")

    if signal_end < signal_start:
        raise RuntimeError("signal_end must be >= signal_start")
    if plan_week_end < plan_week_start:
        raise RuntimeError("plan_week_end must be >= plan_week_start")

    return {
        "signal_start": signal_start,
        "signal_end": signal_end,
        "plan_week_start": plan_week_start,
        "plan_week_end": plan_week_end,
        "period_warnings": warnings,
    }


def _clean_technical_text(value: Any) -> str:
    text = str(value or "")
    text = (
        text.replace("«", '"')
        .replace("»", '"')
        .replace("“", '"')
        .replace("”", '"')
        .replace("‘", '"')
        .replace("’", '"')
    )
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _clean_rows_technical(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    fields = (
        "what_i_do",
        "task_to_assign",
        "what_to_check",
        "daily_meeting_thesis",
        "expected_quantity_effect",
        "expected_quality_effect",
    )
    out: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        for field in fields:
            if field in updated:
                before = str(updated.get(field, "") or "")
                after = _clean_technical_text(before)
                if after != before:
                    counts["normalize_whitespace"] = int(counts.get("normalize_whitespace", 0) or 0) + 1
                updated[field] = after
        out.append(updated)
    return out, counts


def _resolve_allowed_activity_types_from_discovery(discovery: dict[str, Any]) -> list[str]:
    dropdown = discovery.get("target_dropdown_values", {}) if isinstance(discovery.get("target_dropdown_values"), dict) else {}
    spec = dropdown.get("activity_type", {}) if isinstance(dropdown.get("activity_type"), dict) else {}
    allowed_raw = spec.get("allowed_values", []) if isinstance(spec.get("allowed_values"), list) else []
    allowed: list[str] = []
    seen: set[str] = set()
    for value in allowed_raw:
        text = clean_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        allowed.append(text)
    return allowed


def _normalize_activity_types_for_rows(
    *,
    rows: list[dict[str, Any]],
    allowed_values: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not rows:
        return [], []

    aliases = {
        "операционная": {"операционная", "дейлик", "daily", "оперативка", "отдел"},
        "контроль": {"контроль", "задача", "проверка"},
        "обучение": {"обучение", "личный разбор", "разбор", "коучинг"},
        "развитие": {"развитие"},
        "стратегическая": {"стратегическая", "стратегия", "strategy"},
    }
    by_norm = {clean_text(item).lower(): clean_text(item) for item in allowed_values if clean_text(item)}
    default_allowed = ["операционная", "контроль", "обучение", "развитие", "стратегическая"]
    normalized_rows: list[dict[str, Any]] = []
    debug_rows: list[dict[str, Any]] = []

    def _match(raw_value: str) -> tuple[str, str]:
        raw = clean_text(raw_value)
        if not raw:
            return "", "empty"
        raw_norm = raw.lower()
        if raw_norm in by_norm:
            return by_norm[raw_norm], "exact_or_casefold"

        canonical = ""
        for key, probes in aliases.items():
            if raw_norm in probes:
                canonical = key
                break
        if not canonical:
            return raw, "no_mapping_kept_raw"

        if canonical in by_norm:
            return by_norm[canonical], f"alias_to_{canonical}"

        probe_order = {
            "обучение": ["обучение", "развитие", "операционная", "контроль"],
            "операционная": ["операционная", "контроль", "обучение", "развитие"],
            "контроль": ["контроль", "операционная", "обучение", "развитие"],
            "развитие": ["развитие", "обучение", "операционная", "контроль"],
            "стратегическая": ["стратегическая", "развитие", "операционная", "контроль"],
        }
        for candidate in probe_order.get(canonical, default_allowed):
            if candidate in by_norm:
                return by_norm[candidate], f"alias_fallback_to_{candidate}"

        if not by_norm:
            return canonical, f"alias_without_dropdown_to_{canonical}"
        return raw, "alias_unmapped_kept_raw"

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        original = clean_text(updated.get("activity_type", ""))
        normalized, reason = _match(original)
        if normalized:
            updated["activity_type"] = normalized
        normalized_rows.append(updated)
        debug_rows.append(
            {
                "row_index": idx,
                "recipient": str(row.get("recipient") or ""),
                "plan_date": str(row.get("plan_date") or ""),
                "original_activity_type": original,
                "normalized_activity_type": clean_text(updated.get("activity_type", "")),
                "allowed_values": allowed_values,
                "normalization_reason": reason,
                "valid_after_normalization": bool(
                    not allowed_values
                    or clean_text(updated.get("activity_type", "")).lower() in {clean_text(x).lower() for x in allowed_values}
                ),
            }
        )
    return normalized_rows, debug_rows


def _is_person_recipient(name: str) -> bool:
    probe = clean_text(name)
    if not probe:
        return False
    low = probe.lower()
    if low in {"отдел", "команда", "все", "общий"}:
        return False
    if "отдел" in low and len(low.split()) <= 2:
        return False
    return True


def _name_in_allowlist(name: str, allowlist: tuple[str, ...]) -> bool:
    probe = clean_text(name).lower()
    if not probe:
        return False
    for item in allowlist:
        token = clean_text(item).lower()
        if not token:
            continue
        if probe == token or probe in token or token in probe:
            return True
    return False


def _resolve_bootstrap_managers(
    *,
    discovery: dict[str, Any],
    roks_snapshot: dict[str, Any],
    source_headers: list[str],
    source_rows: list[list[str]],
    manager_allowlist: tuple[str, ...],
) -> tuple[list[str], dict[str, list[str]], dict[str, str], list[dict[str, Any]]]:
    source_map: dict[str, list[str]] = {}
    role_by_manager: dict[str, str] = {}
    diagnostics: list[dict[str, Any]] = []

    def _add(name: str, source: str) -> None:
        normalized = clean_text(name)
        if not normalized:
            return
        if not _is_person_recipient(normalized):
            diagnostics.append({"manager_name": normalized, "source": source, "reason": "non_person_recipient"})
            return
        if manager_allowlist and not _name_in_allowlist(normalized, manager_allowlist):
            diagnostics.append({"manager_name": normalized, "source": source, "reason": "outside_allowlist"})
            return
        bucket = source_map.setdefault(normalized, [])
        if source not in bucket:
            bucket.append(source)

    dropdown = discovery.get("target_dropdown_values", {}) if isinstance(discovery.get("target_dropdown_values"), dict) else {}
    recipient_spec = dropdown.get("recipient", {}) if isinstance(dropdown.get("recipient"), dict) else {}
    allowed_recipients = recipient_spec.get("allowed_values", []) if isinstance(recipient_spec.get("allowed_values"), list) else []
    for value in allowed_recipients:
        _add(str(value or ""), "plan_sheet_dropdown")

    manager_metrics = roks_snapshot.get("manager_metrics", {}) if isinstance(roks_snapshot.get("manager_metrics"), dict) else {}
    for manager_name in manager_metrics.keys():
        _add(str(manager_name or ""), "roks_oap")

    mapped = map_source_headers(source_headers)
    manager_idx = int(mapped.get("manager_name", -1))
    role_idx = int(mapped.get("manager_role_profile", -1))
    for row in source_rows:
        if not isinstance(row, list):
            continue
        manager_name = clean_text(row[manager_idx]) if manager_idx >= 0 and manager_idx < len(row) else ""
        role = clean_text(row[role_idx]) if role_idx >= 0 and role_idx < len(row) else ""
        if manager_name:
            _add(manager_name, "daily_control_history")
            if role and manager_name not in role_by_manager:
                role_by_manager[manager_name] = role

    resolved = sorted(source_map.keys(), key=lambda item: item.lower())
    return resolved, source_map, role_by_manager, diagnostics


def _bootstrap_activity_templates() -> list[dict[str, str]]:
    return [
        {
            "activity_type": "контроль",
            "priority": "high",
            "what_i_do": "Стартовая постановка контроля: синхронизирую фокус недели по ЛПР, боли и следующему шагу.",
            "task_to_assign": "Заполнить по активным сделкам: кто ЛПР, какая задача клиента и какой следующий контакт назначен.",
            "what_to_check": "Проверяю наличие конкретного следующего шага с датой и временем по каждой активной сделке.",
            "daily_meeting_thesis": "Стартовая проверка дисциплины: фиксируем факты разговора и управляемый следующий шаг.",
            "expected_quantity_effect": "Больше управляемых касаний и меньше потерянных контактов в течение недели.",
            "expected_quality_effect": "Единый стандарт фиксации в CRM и стабильное качество коммуникации.",
        },
        {
            "activity_type": "обучение",
            "priority": "high",
            "what_i_do": "Стартовая тренировка выявления ЛПР и потребности на первом контакте без лишней воды.",
            "task_to_assign": "Подготовить и отработать 10 вопросов для выявления роли собеседника и бизнес-задачи.",
            "what_to_check": "Сверяю, что менеджер в разговоре уточняет роль, задачу и критерий решения клиента.",
            "daily_meeting_thesis": "Обучение на старте недели: задаем структуру вопросов и удерживаем управление разговором.",
            "expected_quantity_effect": "Рост доли результативных первичных диалогов с подтвержденным ЛПР.",
            "expected_quality_effect": "Более точная квалификация и понятная логика следующего шага.",
        },
        {
            "activity_type": "личный разбор",
            "priority": "medium",
            "what_i_do": "Личный разбор стандартов: как переводить разговор от интереса к конкретному следующему действию.",
            "task_to_assign": "Сделать 5 ролевых отработок перехода к назначению демо или контрольного созвона.",
            "what_to_check": "Проверяю, что в конце контакта всегда зафиксированы действие, дата и ответственный.",
            "daily_meeting_thesis": "Первичный контроль фиксации следующего шага: закрываем разговор на конкретику.",
            "expected_quantity_effect": "Больше подтвержденных встреч и меньше зависших сделок.",
            "expected_quality_effect": "Уверенное завершение разговора с понятным планом для клиента.",
        },
        {
            "activity_type": "задача",
            "priority": "medium",
            "what_i_do": "Стартовая проверка базы: обновляю приоритетные сделки и убираю пустые формулировки в карточках.",
            "task_to_assign": "Обновить карточки по приоритетным сделкам: ЛПР, потребность, следующий шаг, срок.",
            "what_to_check": "Контролирую, что записи в CRM содержат факты, а не общие фразы.",
            "daily_meeting_thesis": "Порядок в базе = управляемая неделя: каждая сделка должна иметь конкретный следующий шаг.",
            "expected_quantity_effect": "Снижение потерь из-за неактуальных карточек и пропущенных контактов.",
            "expected_quality_effect": "Прозрачная воронка и более точная управленческая диагностика.",
        },
        {
            "activity_type": "контроль",
            "priority": "medium",
            "what_i_do": "Стартовый дейлик по стандартам недели: закрепляю договоренности и контрольные точки внедрения.",
            "task_to_assign": "Подготовить короткий отчет по внедрению: что применено, где есть риск, какой следующий шаг.",
            "what_to_check": "Проверяю выполнение недельных договоренностей и готовность к следующему циклу контроля.",
            "daily_meeting_thesis": "Неделю ведем через конкретные действия: ЛПР, потребность, следующий шаг, срок, фиксация.",
            "expected_quantity_effect": "Стабильный темп работы и предсказуемый объем управляемых контактов.",
            "expected_quality_effect": "Единый стандарт коммуникации и повышение дисциплины исполнения.",
        },
    ]


def _workdays_between(start: date, end: date) -> list[date]:
    days: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() <= 4:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def _compute_manager_week_coverage(
    *,
    rows: list[dict[str, Any]],
    managers_in_scope: list[str],
    expected_workdays: list[str],
) -> dict[str, Any]:
    normalized_expected = sorted({str(item or "").strip() for item in expected_workdays if str(item or "").strip()})
    rows_by_manager: dict[str, list[str]] = {}
    rows_count_by_manager: dict[str, int] = {}
    missing_dates_by_manager: dict[str, list[str]] = {}

    for manager in managers_in_scope:
        manager_name = clean_text(manager)
        if not manager_name:
            continue
        dates = sorted(
            {
                str(item.get("plan_date") or "").strip()
                for item in rows
                if isinstance(item, dict) and clean_text(item.get("recipient") or "") == manager_name
            }
        )
        dates = [item for item in dates if item]
        rows_by_manager[manager_name] = dates
        rows_count_by_manager[manager_name] = len(dates)
        missing = [item for item in normalized_expected if item not in dates]
        if missing:
            missing_dates_by_manager[manager_name] = missing

    return {
        "managers_in_planning_scope": sorted(rows_by_manager.keys()),
        "expected_workdays": normalized_expected,
        "rows_by_manager": rows_by_manager,
        "rows_count_by_manager": rows_count_by_manager,
        "missing_dates_by_manager": missing_dates_by_manager,
        "coverage_complete": not bool(missing_dates_by_manager),
    }


def _expand_missing_manager_week_rows(
    *,
    rows: list[dict[str, Any]],
    missing_dates_by_manager: dict[str, list[str]],
) -> tuple[list[dict[str, Any]], int]:
    out = [dict(item) for item in rows if isinstance(item, dict)]
    existing_exact_keys = {build_exact_key(item) for item in out if isinstance(item, dict)}
    added = 0

    for manager_name, missing_dates in (missing_dates_by_manager or {}).items():
        recipient = clean_text(manager_name)
        if not recipient:
            continue
        manager_rows = [
            item
            for item in out
            if isinstance(item, dict) and clean_text(item.get("recipient") or "") == recipient
        ]
        if not manager_rows:
            continue
        manager_rows.sort(
            key=lambda item: (
                str(item.get("plan_date") or ""),
                str(item.get("activity_type") or ""),
                str(item.get("priority") or ""),
            )
        )
        for offset, missing_date in enumerate(missing_dates):
            seed = dict(manager_rows[min(offset, len(manager_rows) - 1)])
            seed["plan_date"] = str(missing_date)
            seed["day_label"] = day_label_from_iso(str(missing_date))
            seed["analysis_backend_used"] = (
                str(seed.get("analysis_backend_used") or "main") + "|coverage_repair"
            )
            seed["status"] = clean_text(seed.get("status") or "") or "запланировано"
            seed["idempotency_key"] = build_exact_key(seed)
            exact_key = build_exact_key(seed)
            if exact_key in existing_exact_keys:
                continue
            existing_exact_keys.add(exact_key)
            out.append(seed)
            added += 1

    return out, added


def _build_bootstrap_rows(
    *,
    managers: list[str],
    role_by_manager: dict[str, str],
    plan_week_start: date,
    plan_week_end: date,
    source_run_id: str,
) -> list[dict[str, Any]]:
    templates = _bootstrap_activity_templates()
    workdays = _workdays_between(plan_week_start, plan_week_end)
    rows: list[dict[str, Any]] = []
    for manager_name in managers:
        manager_role = clean_text(role_by_manager.get(manager_name, "")) or "менеджер"
        for idx, day in enumerate(workdays):
            template = templates[idx % len(templates)]
            row = {
                "plan_week_start": plan_week_start.isoformat(),
                "plan_week_end": plan_week_end.isoformat(),
                "plan_date": day.isoformat(),
                "day_label": day_label_from_iso(day.isoformat()),
                "recipient": manager_name,
                "manager_role_profile": manager_role,
                "activity_type": template["activity_type"],
                "priority": template["priority"],
                "what_i_do": template["what_i_do"],
                "task_to_assign": template["task_to_assign"],
                "what_to_check": template["what_to_check"],
                "daily_meeting_thesis": template["daily_meeting_thesis"],
                "training_link": "",
                "post_training_task_link": "",
                "expected_quantity_effect": template["expected_quantity_effect"],
                "expected_quality_effect": template["expected_quality_effect"],
                "status": "запланировано",
                "source_deals_count": 0,
                "source_calls_count": 0,
                "source_day_count": 0,
                "analysis_backend_used": "bootstrap_no_signal_history",
                "source_run_id": source_run_id,
            }
            row["idempotency_key"] = build_exact_key(row)
            rows.append(row)
    return rows


def _safe_read_sheet_rows(
    *,
    cfg: DealAnalyzerConfig,
    spreadsheet_id: str,
    sheet_name: str,
    logger: Any,
) -> dict[str, Any]:
    try:
        from src.integrations.google_sheets_api_client import GoogleSheetsApiClient
        from src.deal_analyzer.daily_control.source_reader import detect_header_row, clean_text

        app_root = Path(cfg.config_path).resolve().parents[1]
        client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
        matrix = client.get_values(spreadsheet_id, f"'{sheet_name}'!A1:AZ")
        if not matrix:
            return {"ok": True, "sheet_name": sheet_name, "headers": [], "rows": [], "header_row_number": 1}
        header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=3)
        header_idx = max(0, header_row_number - 1)
        headers = [clean_text(item) for item in matrix[header_idx]]
        rows = [list(map(clean_text, row)) for row in matrix[header_idx + 1 :]]
        return {
            "ok": True,
            "sheet_name": sheet_name,
            "headers": headers,
            "rows": rows,
            "header_row_number": header_row_number,
        }
    except Exception as exc:
        return {"ok": False, "sheet_name": sheet_name, "error": str(exc), "headers": [], "rows": []}


def _payload_row_validation_rejections(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        lint = lint_week_plan_text_rows([row])
        payload_validation = validate_week_plan_payload_rows([row])
        if lint_has_blockers(lint) or payload_has_blockers(payload_validation):
            rejected_rows.append(
                {
                    "row_index": idx,
                    "recipient": str(row.get("recipient") or ""),
                    "plan_date": str(row.get("plan_date") or ""),
                    "reason": "payload_validator_blocker",
                    "text_lint": lint,
                    "payload_validator": payload_validation,
                    "row": row,
                }
            )
            continue
        valid_rows.append(row)
    return valid_rows, rejected_rows


def _build_quality_review(rows: list[dict[str, Any]], *, limit: int = 10) -> dict[str, Any]:
    lint = lint_week_plan_text_rows(rows)
    by_row: dict[int, dict[str, Any]] = {}
    for item in lint.get("problem_examples", []) if isinstance(lint.get("problem_examples"), list) else []:
        if not isinstance(item, dict):
            continue
        row_idx = int(item.get("row_index", -1))
        if row_idx < 0:
            continue
        current = by_row.setdefault(
            row_idx,
            {
                "row_index": row_idx,
                "recipient": item.get("recipient", ""),
                "markers": set(),
                "fields": set(),
                "examples": [],
            },
        )
        for marker in item.get("markers", []) if isinstance(item.get("markers"), list) else []:
            current["markers"].add(str(marker))
        current["fields"].add(str(item.get("field", "")))
        if len(current["examples"]) < 2:
            current["examples"].append(str(item.get("value", "")))

    problem_rows = []
    for row in by_row.values():
        row["markers"] = sorted(list(row["markers"]))
        row["fields"] = sorted(list(row["fields"]))
        problem_rows.append(row)
    problem_rows.sort(key=lambda item: len(item.get("markers", [])), reverse=True)
    return {
        "rows_total": len(rows),
        "problem_rows_total": len(problem_rows),
        "problem_rows": problem_rows[: max(1, int(limit or 10))],
        "text_lint": lint,
    }


def _summary_markdown_lines(summary: dict[str, Any]) -> list[str]:
    return [
        f"mode_requested: {summary.get('mode_requested', '')}",
        f"effective_mode: {summary.get('effective_mode', '')}",
        f"signal_period_start: {summary.get('signal_period_start', '')}",
        f"signal_period_end: {summary.get('signal_period_end', '')}",
        f"plan_week_start: {summary.get('plan_week_start', '')}",
        f"plan_week_end: {summary.get('plan_week_end', '')}",
        f"source_rows_count: {summary.get('source_rows_count', 0)}",
        f"signal_rows_count: {summary.get('signal_rows_count', 0)}",
        f"groups_count: {summary.get('groups_count', 0)}",
        f"signals_count: {summary.get('signals_count', 0)}",
        f"bootstrap_mode_used: {summary.get('bootstrap_mode_used', False)}",
        f"bootstrap_reason: {summary.get('bootstrap_reason', '')}",
        f"rows_prepared: {summary.get('rows_prepared', 0)}",
        f"rows_after_llm_analyzer: {summary.get('rows_after_llm_analyzer', 0)}",
        f"rows_after_payload_validator: {summary.get('rows_after_payload_validator', 0)}",
        f"rows_in_writer_payload: {summary.get('rows_in_writer_payload', 0)}",
        f"rows_quarantined: {summary.get('rows_quarantined', 0)}",
        f"rows_to_insert: {summary.get('rows_to_insert', 0)}",
        f"rows_to_update: {summary.get('rows_to_update', 0)}",
        f"rows_skipped_existing: {summary.get('rows_skipped_existing', 0)}",
        f"conflicts_count: {summary.get('conflicts_count', 0)}",
        f"llm_main_model: {summary.get('llm_main_model', '')}",
        f"llm_fallback_model: {summary.get('llm_fallback_model', '')}",
        f"llm_success_main: {summary.get('llm_success_main', 0)}",
        f"llm_success_fallback: {summary.get('llm_success_fallback', 0)}",
        f"llm_failed_count: {summary.get('llm_failed_count', 0)}",
        f"fallback_used_count: {summary.get('fallback_used_count', 0)}",
        f"roks_oap_snapshot_status: {summary.get('roks_oap_snapshot_status', '')}",
        f"selected_current_month_sheet: {summary.get('selected_current_month_sheet', '')}",
        f"selected_previous_month_sheet: {summary.get('selected_previous_month_sheet', '')}",
        f"writer mode: {summary.get('writer_mode', '')}",
        f"write_allowed: {summary.get('write_allowed', False)}",
        f"block_reason: {summary.get('block_reason', '')}",
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Week plan CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover source/target sheets for week plan")
    discover.add_argument("--config", required=True)
    discover.add_argument("--workbook", default="РОКС 2026")
    discover.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    discover.add_argument("--target-sheet", default="План недели")

    build = sub.add_parser("build", help="Build week plan payload from daily control")
    build.add_argument("--config", required=True)
    build.add_argument("--period-start", default="")
    build.add_argument("--period-end", default="")
    build.add_argument("--signal-start", default="")
    build.add_argument("--signal-end", default="")
    build.add_argument("--plan-week-start", default="")
    build.add_argument("--plan-week-end", default="")
    build.add_argument("--mode", choices=("bootstrap", "normal", "auto"), default="auto")
    build.add_argument("--daily-sheet", "--source-sheet", "--source-daily-sheet", dest="daily_sheet", default="Дневной контроль")
    build.add_argument("--target-sheet", default="План недели")
    build.add_argument("--manager-summary-sheet", default="Недельный свод менеджеров")
    build.add_argument("--week-summary-sheet", default="Свод недели")
    build.add_argument("--manager", dest="managers", action="append", default=None)
    build.add_argument("--main-model", default="")
    build.add_argument("--fallback-model", default="")
    build.add_argument("--llm-max-attempts", type=int, default=6)
    build.add_argument("--limit", type=int, default=0)
    build.add_argument("--bootstrap-if-empty", action="store_true")
    build.add_argument(
        "--require-full-manager-week-coverage",
        dest="require_full_manager_week_coverage",
        action="store_true",
    )
    build.add_argument(
        "--no-require-full-manager-week-coverage",
        dest="require_full_manager_week_coverage",
        action="store_false",
    )
    build.add_argument("--dry-run", action="store_true")
    build.set_defaults(require_full_manager_week_coverage=True)

    write = sub.add_parser("write", help="Write prepared week plan payload to target sheet")
    write.add_argument("--config", required=True)
    write.add_argument("--run-dir", required=True)
    write.add_argument("--target-sheet", default="План недели")
    write.add_argument("--dry-run", action="store_true")
    write.add_argument("--write", action="store_true")
    write.add_argument("--strict-preflight", action="store_true")
    write.add_argument("--allow-partial-write", dest="allow_partial_write", action="store_true")
    write.add_argument("--no-allow-partial-write", dest="allow_partial_write", action="store_false")
    write.add_argument("--quarantine-unrepaired", dest="quarantine_unrepaired", action="store_true")
    write.add_argument("--no-quarantine-unrepaired", dest="quarantine_unrepaired", action="store_false")
    write.set_defaults(allow_partial_write=True, quarantine_unrepaired=True)

    return parser.parse_args()


def _run_discover(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)

    discovery = discover_week_plan_sheet(
        cfg=cfg,
        workbook_name=str(args.workbook or "РОКС 2026"),
        source_sheet_name=str(args.daily_sheet or "Дневной контроль"),
        target_sheet_name=str(args.target_sheet or "План недели"),
        logger=logger,
    )

    write_json(run_dir / "week_plan_sheet_discovery.json", discovery)
    write_markdown(
        run_dir / "week_plan_sheet_discovery.md",
        title="Week Plan Discovery",
        lines=build_discovery_markdown(discovery),
    )
    print(str(run_dir))


def _run_build(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)

    periods = _resolve_signal_and_plan_periods(args)
    signal_start = periods["signal_start"]
    signal_end = periods["signal_end"]
    plan_week_start = periods["plan_week_start"]
    plan_week_end = periods["plan_week_end"]
    period_warnings = list(periods.get("period_warnings", []) or [])

    discovery = discover_week_plan_sheet(
        cfg=cfg,
        workbook_name="РОКС 2026",
        source_sheet_name=str(args.daily_sheet or "Дневной контроль"),
        target_sheet_name=str(args.target_sheet or "План недели"),
        logger=logger,
    )
    write_json(run_dir / "week_plan_sheet_discovery.json", discovery)
    write_markdown(
        run_dir / "week_plan_sheet_discovery.md",
        title="Week Plan Discovery",
        lines=build_discovery_markdown(discovery),
    )

    spreadsheet_id = resolve_spreadsheet_id(cfg)
    source_sheet_name = (
        (discovery.get("source_sheet", {}) if isinstance(discovery.get("source_sheet"), dict) else {}).get("title")
        or str(args.daily_sheet or "Дневной контроль")
    )
    snapshot = read_daily_control_source(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        source_sheet_name=source_sheet_name,
        logger=logger,
    )
    existing_plan_snapshot = read_week_plan_rows(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=str(args.target_sheet or "План недели"),
        logger=logger,
    )
    normal_manager_snapshot = _safe_read_sheet_rows(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=str(args.manager_summary_sheet or "Недельный свод менеджеров"),
        logger=logger,
    )
    normal_week_summary_snapshot = _safe_read_sheet_rows(
        cfg=cfg,
        spreadsheet_id=spreadsheet_id,
        sheet_name=str(args.week_summary_sheet or "Свод недели"),
        logger=logger,
    )

    managers = _manager_allowlist(cfg, args.managers)
    groups, grouping_diag = group_daily_rows_into_week_signals(
        headers=snapshot.headers,
        rows=snapshot.rows,
        period_start=signal_start,
        period_end=signal_end,
        manager_allowlist=managers,
        plan_week_start_override=plan_week_start.isoformat(),
        plan_week_end_override=plan_week_end.isoformat(),
    )

    groups_total_before_limit = len(groups)
    if int(args.limit or 0) > 0:
        groups = groups[: int(args.limit)]

    grouping_diag = dict(grouping_diag or {})
    grouping_diag["groups_total_before_limit"] = groups_total_before_limit
    grouping_diag["groups_total_after_limit"] = len(groups)
    grouping_diag["groups_limit_applied"] = int(args.limit or 0)

    mode_requested = str(args.mode or "auto").strip().lower()
    normal_inputs = {
        "existing_plan_rows": len(existing_plan_snapshot.rows),
        "manager_summary_rows": len(normal_manager_snapshot.get("rows", [])) if isinstance(normal_manager_snapshot.get("rows"), list) else 0,
        "week_summary_rows": len(normal_week_summary_snapshot.get("rows", [])) if isinstance(normal_week_summary_snapshot.get("rows"), list) else 0,
    }
    normal_inputs_available = any(int(value or 0) > 0 for value in normal_inputs.values())
    mode_warnings: list[str] = []
    if mode_requested in {"normal", "auto"}:
        if normal_inputs_available:
            effective_mode = "normal"
        else:
            effective_mode = "bootstrap"
            mode_warnings.append("normal_mode_inputs_missing_fallback_to_bootstrap")
    else:
        effective_mode = "bootstrap"
    grouping_diag["mode_requested"] = mode_requested
    grouping_diag["effective_mode"] = effective_mode
    grouping_diag["normal_inputs"] = normal_inputs
    grouping_diag["mode_warnings"] = [*mode_warnings, *period_warnings]
    grouping_diag["signal_period_start"] = signal_start.isoformat()
    grouping_diag["signal_period_end"] = signal_end.isoformat()
    grouping_diag["plan_week_start"] = plan_week_start.isoformat()
    grouping_diag["plan_week_end"] = plan_week_end.isoformat()

    app_root = Path(cfg.config_path).resolve().parents[1]
    sheet_client = None
    try:
        from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

        sheet_client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    except Exception:
        sheet_client = None

    if sheet_client is None:
        roks_snapshot = {
            "status": "access_error",
            "parse_status": "access_error",
            "warnings": ["google_sheets_client_init_failed"],
            "selected_current_month_sheet": "",
            "selected_previous_month_sheet": "",
            "manager_metrics": {},
            "parsed_metrics_by_manager": {},
        }
    else:
        roks_snapshot = build_roks_oap_snapshot(
            client=sheet_client,
            spreadsheet_id=spreadsheet_id,
            week_start=plan_week_start.isoformat(),
            week_end=plan_week_end.isoformat(),
            manager_allowlist=managers,
        )

    llm_runtime = {
        "main": {
            "model": str(args.main_model or "").strip() or "deepseek-v4-pro:cloud",
            "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
        "fallback": {
            "enabled": True,
            "model": str(args.fallback_model or "").strip() or "deepseek-v4-flash:cloud",
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 120),
            "preflight_timeout_seconds": int(cfg.ollama_fallback_preflight_timeout_seconds or cfg.ollama_preflight_timeout_seconds or 20),
        },
    }
    allowed_activity_types = _resolve_allowed_activity_types_from_discovery(discovery)

    signal_rows_count = int(grouping_diag.get("signal_rows_count", 0) or 0)
    signals_count = int(grouping_diag.get("signals_count", 0) or 0)
    bootstrap_if_empty = bool(args.bootstrap_if_empty)
    bootstrap_mode_used = False
    bootstrap_reason = ""
    managers_resolved: list[str] = []
    managers_resolution_source: dict[str, list[str]] = {}
    managers_resolution_diagnostics: list[dict[str, Any]] = []
    signal_rows_empty = signal_rows_count <= 0
    signals_blocked = bool(snapshot.rows) and bool(groups) and signals_count <= 0

    if bootstrap_if_empty and effective_mode == "bootstrap" and signal_rows_empty:
        bootstrap_mode_used = True
        bootstrap_reason = "signal_rows_empty_bootstrap_if_empty"
        managers_resolved, managers_resolution_source, role_by_manager, managers_resolution_diagnostics = _resolve_bootstrap_managers(
            discovery=discovery,
            roks_snapshot=roks_snapshot,
            source_headers=snapshot.headers,
            source_rows=snapshot.rows,
            manager_allowlist=managers,
        )
        if managers_resolved:
            rows = _build_bootstrap_rows(
                managers=managers_resolved,
                role_by_manager=role_by_manager,
                plan_week_start=plan_week_start,
                plan_week_end=plan_week_end,
                source_run_id=run_dir.name,
            )
            llm_diag = {
                "llm_runtime": {"main": llm_runtime.get("main", {}), "fallback": llm_runtime.get("fallback", {}), "selected": "bootstrap"},
                "llm_attempts_total": 0,
                "llm_success_main": 0,
                "llm_success_main_repair": 0,
                "llm_success_main_compact_retry": 0,
                "llm_success_fallback": 0,
                "llm_success_fallback_repair": 0,
                "llm_success_fallback_compact_retry": 0,
                "llm_failed_count": 0,
                "fallback_used_count": 0,
                "quarantined_count": 0,
                "quarantined_rows": [],
                "llm_requests": [],
                "llm_responses": [],
                "max_prompt_size_chars_seen": 0,
                "block_reason": "",
                "bootstrap_mode_used": True,
                "bootstrap_reason": bootstrap_reason,
            }
        else:
            bootstrap_reason = "bootstrap_managers_not_resolved"
            llm_diag = {
                "llm_runtime": {"main": llm_runtime.get("main", {}), "fallback": llm_runtime.get("fallback", {}), "selected": "none"},
                "llm_attempts_total": 0,
                "llm_success_main": 0,
                "llm_success_main_repair": 0,
                "llm_success_main_compact_retry": 0,
                "llm_success_fallback": 0,
                "llm_success_fallback_repair": 0,
                "llm_success_fallback_compact_retry": 0,
                "llm_failed_count": 0,
                "fallback_used_count": 0,
                "quarantined_count": 0,
                "quarantined_rows": [],
                "llm_requests": [],
                "llm_responses": [],
                "max_prompt_size_chars_seen": 0,
                "block_reason": "bootstrap_managers_not_resolved",
                "bootstrap_mode_used": True,
                "bootstrap_reason": bootstrap_reason,
            }
            rows = []
    elif signals_blocked:
        rows = []
        llm_diag = {
            "llm_runtime": {"main": llm_runtime.get("main", {}), "fallback": llm_runtime.get("fallback", {}), "selected": "none"},
            "llm_attempts_total": 0,
            "llm_success_main": 0,
            "llm_success_main_repair": 0,
            "llm_success_main_compact_retry": 0,
            "llm_success_fallback": 0,
            "llm_success_fallback_repair": 0,
            "llm_success_fallback_compact_retry": 0,
            "llm_failed_count": 0,
            "fallback_used_count": 0,
            "quarantined_count": 0,
            "quarantined_rows": [],
            "llm_requests": [],
            "llm_responses": [],
            "max_prompt_size_chars_seen": 0,
            "block_reason": "signals_empty_from_non_empty_source",
        }
    else:
        rows, llm_diag = analyze_week_plan_groups(
            groups=groups,
            cfg=cfg,
            roks_snapshot=roks_snapshot,
            llm_runtime=llm_runtime,
            logger=logger,
            source_run_id=run_dir.name,
            main_model_override=str(args.main_model or "").strip() or None,
            fallback_model_override=str(args.fallback_model or "").strip() or None,
            llm_max_attempts=int(args.llm_max_attempts or 6),
            allowed_activity_types=allowed_activity_types,
        )

    rows_cleaned, cleanup_counts = _clean_rows_technical(rows)
    rows_cleaned, activity_type_normalization_debug = _normalize_activity_types_for_rows(
        rows=rows_cleaned,
        allowed_values=allowed_activity_types,
    )
    require_full_manager_week_coverage = bool(getattr(args, "require_full_manager_week_coverage", True))
    expected_workdays = [item.isoformat() for item in _workdays_between(plan_week_start, plan_week_end)]
    managers_in_planning_scope = sorted(
        {
            clean_text(group.manager_name)
            for group in groups
            if clean_text(getattr(group, "manager_name", ""))
        }
    )
    if bootstrap_mode_used and managers_resolved:
        managers_in_planning_scope = sorted(
            {
                *managers_in_planning_scope,
                *[clean_text(item) for item in managers_resolved if clean_text(item)],
            }
        )
    coverage_before = _compute_manager_week_coverage(
        rows=rows_cleaned,
        managers_in_scope=managers_in_planning_scope,
        expected_workdays=expected_workdays,
    )
    coverage_repair_attempted = False
    coverage_repair_added = 0
    coverage_repair_success = bool(coverage_before.get("coverage_complete", False))
    if require_full_manager_week_coverage and managers_in_planning_scope and not coverage_repair_success:
        coverage_repair_attempted = True
        rows_cleaned, coverage_repair_added = _expand_missing_manager_week_rows(
            rows=rows_cleaned,
            missing_dates_by_manager=coverage_before.get("missing_dates_by_manager", {}),
        )
        coverage_after_repair = _compute_manager_week_coverage(
            rows=rows_cleaned,
            managers_in_scope=managers_in_planning_scope,
            expected_workdays=expected_workdays,
        )
        coverage_repair_success = bool(coverage_after_repair.get("coverage_complete", False))
    else:
        coverage_after_repair = coverage_before

    writer_rows, payload_validator_rejected = _payload_row_validation_rejections(rows_cleaned)

    llm_quarantined = llm_diag.get("quarantined_rows", []) if isinstance(llm_diag.get("quarantined_rows"), list) else []
    quarantined_rows = [*llm_quarantined, *payload_validator_rejected]
    managers_in_daily_control = grouping_diag.get("managers_in_daily_control", [])
    managers_in_signal_period = grouping_diag.get("managers_in_signal_period", [])
    managers_in_groups = list(grouping_diag.get("managers_in_groups", []) or [])
    if bootstrap_mode_used and managers_resolved:
        managers_in_groups = sorted({*managers_in_groups, *managers_resolved})
    managers_in_payload = sorted(
        {
            str(item.get("recipient") or "").strip()
            for item in writer_rows
            if isinstance(item, dict) and str(item.get("recipient") or "").strip()
        }
    )
    coverage_after_validation = _compute_manager_week_coverage(
        rows=writer_rows,
        managers_in_scope=managers_in_planning_scope,
        expected_workdays=expected_workdays,
    )
    coverage_incomplete = bool(
        require_full_manager_week_coverage
        and managers_in_planning_scope
        and not bool(coverage_after_validation.get("coverage_complete", False))
    )
    managers_skipped_with_reason = list(grouping_diag.get("managers_skipped_with_reason", []) or [])
    if bootstrap_mode_used and (not managers_resolved):
        managers_skipped_with_reason.append(
            {
                "manager_name": "",
                "reason": "bootstrap_managers_not_resolved",
                "stage": "bootstrap_manager_resolution",
            }
        )
    for item in llm_quarantined:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("manager_name") or "").strip(),
                "reason": str(item.get("reason") or "llm_failed"),
                "stage": "llm_analyzer",
            }
        )
    for item in payload_validator_rejected:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("recipient") or "").strip(),
                "reason": str(item.get("reason") or "payload_validator_blocker"),
                "stage": "payload_validator",
            }
        )
    if coverage_incomplete:
        for manager_name, missing_dates in (
            coverage_after_validation.get("missing_dates_by_manager", {})
            if isinstance(coverage_after_validation.get("missing_dates_by_manager"), dict)
            else {}
        ).items():
            managers_skipped_with_reason.append(
                {
                    "manager_name": str(manager_name or ""),
                    "reason": "manager_week_coverage_incomplete",
                    "stage": "coverage_gate",
                    "missing_dates": list(missing_dates) if isinstance(missing_dates, list) else [],
                }
            )

    payload = {
        "mode": "week_plan",
        "period_start": signal_start.isoformat(),
        "period_end": signal_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "source_sheet": source_sheet_name,
        "target_sheet": str(args.target_sheet or "План недели"),
        "rows": writer_rows,
        "rows_count": len(writer_rows),
        "rows_prepared": len(rows),
        "rows_quarantined": len(quarantined_rows),
        "llm_runtime": llm_diag.get("llm_runtime", {}),
        "cleanup_counts": cleanup_counts,
        "require_full_manager_week_coverage": bool(require_full_manager_week_coverage),
        "manager_week_coverage": {
            "managers_in_planning_scope": managers_in_planning_scope,
            "expected_workdays": expected_workdays,
            "before_repair": coverage_before,
            "after_repair": coverage_after_repair,
            "after_payload_validation": coverage_after_validation,
            "repair_attempted": bool(coverage_repair_attempted),
            "repair_rows_added": int(coverage_repair_added),
            "repair_success": bool(coverage_repair_success),
            "coverage_incomplete": bool(coverage_incomplete),
        },
    }

    signals_payload = {
        "period_start": signal_start.isoformat(),
        "period_end": signal_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "mode_requested": mode_requested,
        "effective_mode": effective_mode,
        "mode_warnings": [*mode_warnings, *period_warnings],
        "period_warnings": period_warnings,
        "normal_mode_inputs": normal_inputs,
        "groups_count": len(groups),
        "groups": [group.__dict__ for group in groups],
        "grouping_diagnostics": grouping_diag,
    }

    quality_review = _build_quality_review(writer_rows, limit=10)

    row_flow_filtered: list[dict[str, Any]] = []
    for item in llm_quarantined:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "llm_analyzer",
                    "group_index": item.get("group_index"),
                    "manager_name": item.get("manager_name", ""),
                    "plan_week_start": item.get("plan_week_start", ""),
                    "plan_week_end": item.get("plan_week_end", ""),
                    "reason": item.get("reason", "llm_failed"),
                    "error_type": item.get("error_type", ""),
                    "models_attempted": item.get("models_attempted", []),
                    "errors_by_attempt": item.get("errors_by_attempt", []),
                    "raw_response_preview": item.get("raw_response_preview", ""),
                    "prompt_size_chars": item.get("prompt_size_chars", 0),
                }
            )
    for item in payload_validator_rejected:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "payload_validator",
                    "row_index": item.get("row_index"),
                    "recipient": item.get("recipient", ""),
                    "plan_date": item.get("plan_date", ""),
                    "reason": item.get("reason", "payload_validator_blocker"),
                }
            )
    if signals_blocked:
        row_flow_filtered.append(
            {
                "stage": "signal_builder",
                "reason": "signals_empty_from_non_empty_source",
                "source_rows_count": len(snapshot.rows),
                "groups_count": len(groups),
            }
        )
    if bootstrap_mode_used:
        row_flow_filtered.append(
            {
                "stage": "bootstrap",
                "reason": bootstrap_reason or "signal_rows_empty_bootstrap_if_empty",
                "signal_rows_count": signal_rows_count,
                "managers_resolved_count": len(managers_resolved),
                "managers_resolution_source": managers_resolution_source,
                "managers_resolution_diagnostics": managers_resolution_diagnostics[:50],
            }
        )
    if coverage_incomplete:
        row_flow_filtered.append(
            {
                "stage": "coverage_gate",
                "reason": "manager_week_coverage_incomplete",
                "managers_in_planning_scope": managers_in_planning_scope,
                "expected_workdays": expected_workdays,
                "missing_dates_by_manager": coverage_after_validation.get("missing_dates_by_manager", {}),
                "repair_attempted": bool(coverage_repair_attempted),
                "repair_rows_added": int(coverage_repair_added),
                "repair_success": bool(coverage_repair_success),
            }
        )

    empty_payload_blocked = len(writer_rows) == 0

    row_flow_debug = {
        "signal_rows_count": signal_rows_count,
        "bootstrap_mode_used": bool(bootstrap_mode_used),
        "bootstrap_reason": bootstrap_reason,
        "managers_resolved": managers_resolved,
        "managers_resolution_source": managers_resolution_source,
        "managers_resolution_diagnostics": managers_resolution_diagnostics[:50],
        "rows_prepared": len(rows),
        "rows_in_writer_payload": len(writer_rows),
        "rows_quarantined": len(quarantined_rows),
        "empty_payload_blocked": bool(empty_payload_blocked),
        "groups_count": len(groups),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_technical_cleanup": len(rows_cleaned),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_week_plan_payload": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "quarantine_count": len(quarantined_rows),
        "rejected_rows_count": len(row_flow_filtered),
        "filtered_rows": row_flow_filtered,
        "mode_requested": mode_requested,
        "effective_mode": effective_mode,
        "mode_warnings": [*mode_warnings, *period_warnings],
        "period_warnings": period_warnings,
        "managers_in_daily_control": managers_in_daily_control,
        "managers_in_signal_period": managers_in_signal_period,
        "managers_in_groups": managers_in_groups,
        "managers_in_payload": managers_in_payload,
        "managers_in_plan_payload": managers_in_payload,
        "managers_skipped_with_reason": managers_skipped_with_reason,
        "managers_in_planning_scope": managers_in_planning_scope,
        "expected_workdays": expected_workdays,
        "require_full_manager_week_coverage": bool(require_full_manager_week_coverage),
        "coverage_before": coverage_before,
        "coverage_after_repair": coverage_after_repair,
        "coverage_after_payload_validation": coverage_after_validation,
        "coverage_repair_attempted": bool(coverage_repair_attempted),
        "coverage_repair_rows_added": int(coverage_repair_added),
        "coverage_repair_success": bool(coverage_repair_success),
        "coverage_incomplete": bool(coverage_incomplete),
        "activity_type_allowed_values": allowed_activity_types,
        "activity_type_normalization_count": len(activity_type_normalization_debug),
    }

    bootstrap_debug = {
        "signal_rows_count": signal_rows_count,
        "bootstrap_mode_used": bool(bootstrap_mode_used),
        "bootstrap_reason": bootstrap_reason,
        "managers_resolved": managers_resolved,
        "managers_resolution_source": managers_resolution_source,
        "managers_resolution_diagnostics": managers_resolution_diagnostics[:50],
        "rows_prepared": len(rows),
        "rows_in_writer_payload": len(writer_rows),
        "rows_quarantined": len(quarantined_rows),
        "empty_payload_blocked": bool(empty_payload_blocked),
    }

    write_json(run_dir / "week_plan_source_rows.json", {"headers": snapshot.headers, "rows": snapshot.rows})
    write_json(
        run_dir / "week_plan_existing_plan_rows.json",
        {"headers": existing_plan_snapshot.headers, "rows": existing_plan_snapshot.rows, "header_row_number": existing_plan_snapshot.header_row_number},
    )
    write_json(run_dir / "week_plan_normal_mode_manager_summary_rows.json", normal_manager_snapshot)
    write_json(run_dir / "week_plan_normal_mode_week_summary_rows.json", normal_week_summary_snapshot)
    write_json(run_dir / "week_plan_input_signals.json", signals_payload)
    write_json(run_dir / "roks_oap_snapshot.json", roks_snapshot)
    write_json(run_dir / "week_plan_llm_requests.json", llm_diag.get("llm_requests", []))
    write_json(run_dir / "week_plan_llm_responses.json", llm_diag.get("llm_responses", []))
    write_json(run_dir / "week_plan_payload.json", payload)
    write_json(run_dir / "week_plan_quarantine.json", {"rows_quarantined": len(quarantined_rows), "rows": quarantined_rows})
    write_json(run_dir / "week_plan_row_flow_debug.json", row_flow_debug)
    write_json(
        run_dir / "week_plan_activity_type_normalization_debug.json",
        {
            "allowed_values": allowed_activity_types,
            "rows_total": len(activity_type_normalization_debug),
            "rows": activity_type_normalization_debug,
        },
    )
    write_json(
        run_dir / "week_plan_manager_coverage_debug.json",
        {
            "managers_in_planning_scope": managers_in_planning_scope,
            "expected_workdays": expected_workdays,
            "rows_by_manager": coverage_after_validation.get("rows_by_manager", {}),
            "missing_dates_by_manager": coverage_after_validation.get("missing_dates_by_manager", {}),
            "repair_attempted": bool(coverage_repair_attempted),
            "repair_rows_added": int(coverage_repair_added),
            "repair_success": bool(coverage_repair_success),
            "final_decision": "blocked_manager_week_coverage_incomplete"
            if coverage_incomplete
            else "coverage_ok",
        },
    )
    write_json(run_dir / "week_plan_bootstrap_debug.json", bootstrap_debug)
    write_json(run_dir / "week_plan_quality_review.json", quality_review)

    if bootstrap_mode_used and not managers_resolved:
        build_block_reason = "bootstrap_managers_not_resolved"
    elif signals_blocked:
        build_block_reason = "signals_empty_from_non_empty_source"
    elif coverage_incomplete:
        build_block_reason = "manager_week_coverage_incomplete"
    elif empty_payload_blocked:
        build_block_reason = "rows_empty"
    else:
        build_block_reason = "dry_run_build_only"

    summary = {
        "run_id": run_dir.name,
        "period_start": signal_start.isoformat(),
        "period_end": signal_end.isoformat(),
        "signal_period_start": signal_start.isoformat(),
        "signal_period_end": signal_end.isoformat(),
        "plan_week_start": plan_week_start.isoformat(),
        "plan_week_end": plan_week_end.isoformat(),
        "mode_requested": mode_requested,
        "effective_mode": effective_mode,
        "mode_warnings": [*mode_warnings, *period_warnings],
        "period_warnings": period_warnings,
        "source_sheet": source_sheet_name,
        "target_sheet": str(args.target_sheet or "План недели"),
        "source_rows_count": len(snapshot.rows),
        "groups_count": len(groups),
        "signals_count": int(grouping_diag.get("signals_count", 0) or 0),
        "signal_rows_count": signal_rows_count,
        "bootstrap_if_empty": bool(bootstrap_if_empty),
        "bootstrap_mode_used": bool(bootstrap_mode_used),
        "bootstrap_reason": bootstrap_reason,
        "managers_resolved": managers_resolved,
        "managers_resolution_source": managers_resolution_source,
        "rows_prepared": len(rows),
        "rows_after_llm_analyzer": len(rows),
        "rows_after_payload_validator": len(writer_rows),
        "rows_in_writer_payload": len(writer_rows),
        "rows_quarantined": len(quarantined_rows),
        "managers_in_daily_control": managers_in_daily_control,
        "managers_in_signal_period": managers_in_signal_period,
        "managers_in_groups": managers_in_groups,
        "managers_in_payload": managers_in_payload,
        "managers_in_plan_payload": managers_in_payload,
        "managers_skipped_with_reason": managers_skipped_with_reason,
        "managers_in_planning_scope": managers_in_planning_scope,
        "expected_workdays": expected_workdays,
        "rows_by_manager": coverage_after_validation.get("rows_by_manager", {}),
        "missing_dates_by_manager": coverage_after_validation.get("missing_dates_by_manager", {}),
        "coverage_repair_attempted": bool(coverage_repair_attempted),
        "coverage_repair_rows_added": int(coverage_repair_added),
        "coverage_repair_success": bool(coverage_repair_success),
        "coverage_incomplete": bool(coverage_incomplete),
        "require_full_manager_week_coverage": bool(require_full_manager_week_coverage),
        "activity_type_allowed_values": allowed_activity_types,
        "activity_type_normalization_count": len(activity_type_normalization_debug),
        "rows_to_insert": 0,
        "rows_to_update": 0,
        "rows_skipped_existing": 0,
        "rows_skipped_stale": 0,
        "conflicts_count": 0,
        "llm_main_model": (llm_diag.get("llm_runtime", {}).get("main", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("main", {}), dict) else {}).get("model", ""),
        "llm_fallback_model": (llm_diag.get("llm_runtime", {}).get("fallback", {}) if isinstance(llm_diag.get("llm_runtime", {}).get("fallback", {}), dict) else {}).get("model", ""),
        "llm_attempts_total": llm_diag.get("llm_attempts_total", 0),
        "llm_success_main": llm_diag.get("llm_success_main", 0),
        "llm_success_main_repair": llm_diag.get("llm_success_main_repair", 0),
        "llm_success_main_compact_retry": llm_diag.get("llm_success_main_compact_retry", 0),
        "llm_success_fallback": llm_diag.get("llm_success_fallback", 0),
        "llm_success_fallback_repair": llm_diag.get("llm_success_fallback_repair", 0),
        "llm_success_fallback_compact_retry": llm_diag.get("llm_success_fallback_compact_retry", 0),
        "llm_failed_count": llm_diag.get("llm_failed_count", 0),
        "fallback_used_count": llm_diag.get("fallback_used_count", 0),
        "roks_oap_snapshot_status": roks_snapshot.get("status", ""),
        "selected_current_month_sheet": roks_snapshot.get("selected_current_month_sheet", ""),
        "selected_previous_month_sheet": roks_snapshot.get("selected_previous_month_sheet", ""),
        "writer_mode": "dry_run",
        "write_allowed": False,
        "empty_payload_blocked": bool(empty_payload_blocked),
        "block_reason": build_block_reason,
    }

    write_json(run_dir / "summary.json", summary)
    write_markdown(run_dir / "summary.md", title="Week Plan Summary", lines=_summary_markdown_lines(summary))
    print(str(run_dir))


def _run_write(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = Path(str(args.run_dir)).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    dry_run = not bool(args.write and not args.dry_run)

    status = execute_week_plan_write(
        cfg=cfg,
        run_dir=run_dir,
        target_sheet_name=str(args.target_sheet or "План недели"),
        dry_run=dry_run,
        strict_preflight=bool(args.strict_preflight),
        allow_partial_write=bool(args.allow_partial_write),
        quarantine_unrepaired=bool(args.quarantine_unrepaired),
        logger=logger,
    )
    write_json(run_dir / "week_plan_writer_status.json", status)

    summary_path = run_dir / "summary.json"
    summary = {}
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                summary = payload
        except Exception:
            summary = {}

    summary.update(
        {
            "writer_mode": status.get("mode", "dry_run"),
            "write_strategy": status.get("write_strategy", "values_only"),
            "rows_to_insert": status.get("rows_to_insert", 0),
            "rows_to_update": status.get("rows_to_update", 0),
            "rows_skipped_existing": status.get("rows_skipped_existing", 0),
            "rows_skipped_stale": status.get("rows_skipped_stale", 0),
            "rows_quarantined": status.get("rows_quarantined", 0),
            "conflicts_count": status.get("conflicts_count", 0),
            "write_allowed": status.get("write_allowed", False),
            "block_reason": status.get("block_reason", ""),
        }
    )

    write_json(summary_path, summary)
    write_markdown(run_dir / "summary.md", title="Week Plan Summary", lines=_summary_markdown_lines(summary))
    print(json.dumps(status, ensure_ascii=False, indent=2))


def main() -> None:
    args = _parse_args()
    if args.command == "discover":
        _run_discover(args)
        return
    if args.command == "build":
        _run_build(args)
        return
    if args.command == "write":
        _run_write(args)
        return
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
