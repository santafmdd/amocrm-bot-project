from __future__ import annotations

import argparse
import difflib
import json
import re
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from src.deal_analyzer.config import DealAnalyzerConfig, load_deal_analyzer_config
from src.deal_analyzer.client_list.normalizer import (
    build_header_mapping as build_client_list_header_mapping,
)
from src.deal_analyzer.client_list.normalizer import normalize_client_rows
from src.deal_analyzer.client_list.prioritizer import (
    build_manager_client_context,
    build_priority_summary as build_client_priority_summary,
)
from src.deal_analyzer.client_list.reader import (
    discover_client_list_sheet,
    read_client_list_sheet,
)
from src.deal_analyzer.daily_control.source_reader import clean_text, day_label_from_iso
from src.deal_analyzer.employee_profiles.analyzer import (
    apply_profile_to_row_fields,
    build_behavior_markers,
    build_employee_profile_context,
)
from src.deal_analyzer.employee_profiles.registry import (
    build_employee_profile_registry,
    resolve_employee_profile,
)
from src.deal_analyzer.progress import ProgressReporter
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
from src.deal_analyzer.weekly_shared.role_policy import (
    contains_forbidden_upper_funnel_for_sales_manager,
    resolve_role_policy,
)


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
    url_re = re.compile(r"https?://\\S+", re.IGNORECASE)
    substitutions = (
        (re.compile(r"\bdownstream\b", re.IGNORECASE), "следующие этапы"),
        (re.compile(r"\bnext\s*step\b", re.IGNORECASE), "следующий шаг"),
        (re.compile(r"\bpipeline\b", re.IGNORECASE), "воронка"),
        (re.compile(r"\bfollow[\s-]?up\b", re.IGNORECASE), "повторный контакт"),
    )

    def _normalize_non_url(segment: str) -> str:
        out = segment
        for pattern, repl in substitutions:
            out = pattern.sub(repl, out)
        return out

    rebuilt: list[str] = []
    cursor = 0
    for match in url_re.finditer(text):
        rebuilt.append(_normalize_non_url(text[cursor : match.start()]))
        rebuilt.append(match.group(0))
        cursor = match.end()
    rebuilt.append(_normalize_non_url(text[cursor:]))
    text = "".join(rebuilt)

    text = (
        text.replace("«", '"')
        .replace("»", '"')
        .replace("“", '"')
        .replace("”", '"')
        .replace("‘", '"')
        .replace("’", '"')
    )
    text = re.sub(r"[\u4e00-\u9fff]+", " ", text)
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


def _apply_employee_profiles_to_week_rows(
    *,
    rows: list[dict[str, Any]],
    cfg: DealAnalyzerConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    registry = build_employee_profile_registry(getattr(cfg, "employee_profiles", None))
    out_rows: list[dict[str, Any]] = []
    context_rows: list[dict[str, Any]] = []
    behavior_rows: list[dict[str, Any]] = []

    grouped_source_rows: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        manager = clean_text(row.get("recipient") or "")
        if not manager:
            continue
        bucket = grouped_source_rows.setdefault(manager.lower(), [])
        bucket.append(row)

    for row in rows:
        if not isinstance(row, dict):
            continue
        manager_name = clean_text(row.get("recipient") or "")
        profile = resolve_employee_profile(
            manager_name=manager_name,
            manager_role_profile=clean_text(row.get("manager_role_profile") or ""),
            registry=registry,
        )
        updated_row, changes = apply_profile_to_row_fields(
            row=row,
            profile=profile,
            fields=("task_to_assign", "daily_meeting_thesis", "what_i_do"),
            date_hint_field="plan_date",
        )
        out_rows.append(updated_row)
        context = build_employee_profile_context(
            manager_name=manager_name,
            manager_role_profile=clean_text(row.get("manager_role_profile") or ""),
            source_rows=grouped_source_rows.get(manager_name.lower(), []),
            registry_raw=getattr(cfg, "employee_profiles", None),
        )
        context_rows.append(
            {
                "manager_name": manager_name,
                "plan_date": str(row.get("plan_date") or ""),
                "communication_style": context.get("communication_style", ""),
                "motivators": context.get("motivators", []),
                "avoid": context.get("avoid", []),
                "profile_source": context.get("profile_source", ""),
                "changed_fields": changes.get("changed_fields", []),
            }
        )
    for manager_name, manager_rows in grouped_source_rows.items():
        if not manager_rows:
            continue
        manager_clean = clean_text(manager_rows[0].get("recipient") or manager_name)
        profile = resolve_employee_profile(
            manager_name=manager_clean,
            manager_role_profile=clean_text(manager_rows[0].get("manager_role_profile") or ""),
            registry=registry,
        )
        markers = build_behavior_markers(
            manager_name=manager_clean,
            source_rows=manager_rows,
            profile=profile,
        )
        behavior_rows.append(
            {
                "manager_name": manager_clean,
                "communication_style": profile.communication_style,
                "repeated_growth_zones": list(markers.repeated_growth_zones),
                "repeated_strong_sides": list(markers.repeated_strong_sides),
                "repeated_objections_handled_badly": list(markers.repeated_objections_handled_badly),
                "repeated_objections_handled_well": list(markers.repeated_objections_handled_well),
                "preferred_behavior_pattern_under_pressure": markers.preferred_behavior_pattern_under_pressure,
                "coaching_response_style": markers.coaching_response_style,
                "source_rows_count": markers.source_rows_count,
            }
        )
    return out_rows, context_rows, behavior_rows


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


def _parse_iso_day(value: str) -> date | None:
    try:
        return date.fromisoformat(str(value or "").strip())
    except Exception:
        return None


def _progression_templates_for_role(role: str) -> list[dict[str, str]]:
    role_norm = clean_text(role).lower()
    if role_norm == "telemarketer":
        return [
            {
                "activity_type": "операционная",
                "priority": "high",
                "what_i_do": "Провожу диагностику верхнего контура: качество базы, дозвон и первичный контакт.",
                "task_to_assign": "Сделать 15 целевых касаний с фиксацией результата по каждому номеру.",
                "what_to_check": "Проверяю долю дозвонов и корректность фиксации результата звонка в amoCRM.",
                "daily_meeting_thesis": "День 1: наводим порядок в верхнем контуре и убираем пустые фиксации.",
                "expected_quantity_effect": "Рост управляемых дозвонов и снижение потерянных контактов.",
                "expected_quality_effect": "Чистая фиксация первичного статуса и причин недозвона.",
            },
            {
                "activity_type": "обучение",
                "priority": "high",
                "what_i_do": "Тренирую выявление ЛПР и бизнес-задачи на первом разговоре.",
                "task_to_assign": "Отработать 10 сценариев вопроса к ЛПР и проверки роли собеседника.",
                "what_to_check": "Контролирую, что в каждом диалоге зафиксированы ЛПР, потребность и следующий шаг.",
                "daily_meeting_thesis": "День 2: усиливаем ЛПР и качество квалификации на холодном входе.",
                "expected_quantity_effect": "Рост доли диалогов с подтвержденным ЛПР.",
                "expected_quality_effect": "Более точное понимание процесса принятия решения у клиента.",
            },
            {
                "activity_type": "контроль",
                "priority": "medium",
                "what_i_do": "Проверяю отработку возражений и перевод в интерес без потери инициативы.",
                "task_to_assign": "Провести 8 звонков с обязательной фиксацией причины интереса клиента.",
                "what_to_check": "Сверяю, что менеджер завершает разговор конкретным следующим действием.",
                "daily_meeting_thesis": "День 3: не информируем, а назначаем управляемый следующий шаг.",
                "expected_quantity_effect": "Больше разговоров, доведенных до договоренности о следующем контакте.",
                "expected_quality_effect": "Стабильная отработка типовых возражений без потери структуры звонка.",
            },
            {
                "activity_type": "развитие",
                "priority": "medium",
                "what_i_do": "Усложняю практику: прохождение секретаря и назначение встречи на конкретный слот.",
                "task_to_assign": "Подготовить 6 вариантов перехода к назначению встречи и применить в звонках.",
                "what_to_check": "Проверяю, что время встречи согласовано и отражено в карточке без размытых формулировок.",
                "daily_meeting_thesis": "День 4: переводим интерес в встречу с датой и временем.",
                "expected_quantity_effect": "Рост числа назначенных встреч и подтвержденных слотов.",
                "expected_quality_effect": "Более точное управление переходом из интереса к встрече.",
            },
            {
                "activity_type": "стратегическая",
                "priority": "medium",
                "what_i_do": "Закрепляю недельный цикл: разбираю срывы и формирую план улучшений на следующую неделю.",
                "task_to_assign": "Подготовить отчет: где теряем дозвон, ЛПР и интерес, и какие действия внедряем далее.",
                "what_to_check": "Контролирую исполнение плана и персональные точки роста по верхнему контуру.",
                "daily_meeting_thesis": "День 5: закрепляем практику и фиксируем управленческий план на новую неделю.",
                "expected_quantity_effect": "Прогнозируемый объем результативных касаний на следующей неделе.",
                "expected_quality_effect": "Устойчивый стандарт качества звонка и фиксации результата.",
            },
        ]
    return [
        {
            "activity_type": "операционная",
            "priority": "high",
            "what_i_do": "Диагностирую теплую/текущую воронку: где теряется переход из интереса в демо и следующий шаг.",
            "task_to_assign": "Разобрать 5 активных сделок и по каждой зафиксировать следующий шаг с датой и временем.",
            "what_to_check": "Проверяю полноту фиксации ЛПР, потребности и обязательств клиента в amoCRM.",
            "daily_meeting_thesis": "День 1: работаем по текущим и теплым сделкам, без массового холодного обзвона.",
            "expected_quantity_effect": "Рост управляемых переходов из интереса в встречу/демо.",
            "expected_quality_effect": "Стабильное качество фиксации следующего шага в активных сделках.",
        },
        {
            "activity_type": "обучение",
            "priority": "high",
            "what_i_do": "Тренирую квалификацию потребности на теплых сделках и перевод к демо.",
            "task_to_assign": "Подготовить 8 вопросов по боли/ценности и применить в текущих активных контактах.",
            "what_to_check": "Проверяю, что после разговора есть критерий решения и подтвержденный следующий шаг.",
            "daily_meeting_thesis": "День 2: усиливаем качество квалификации на теплой воронке.",
            "expected_quantity_effect": "Увеличение числа сделок, доведенных до демонстрации.",
            "expected_quality_effect": "Более точное попадание в реальную потребность клиента.",
        },
        {
            "activity_type": "контроль",
            "priority": "medium",
            "what_i_do": "Контролирую качество проведения демо и перевод в тест/пилот.",
            "task_to_assign": "Провести разбор 3 демо и зафиксировать улучшения по структуре презентации.",
            "what_to_check": "Сверяю, что после демо согласованы критерии теста и сроки следующего действия.",
            "daily_meeting_thesis": "День 3: каждое демо должно заканчиваться управляемым продолжением.",
            "expected_quantity_effect": "Рост доли демо, переходящих в тест/следующий этап.",
            "expected_quality_effect": "Более структурное и управляемое проведение демо-встреч.",
        },
        {
            "activity_type": "развитие",
            "priority": "medium",
            "what_i_do": "Усиливаю дожим этапов тест -> счет и счет -> оплата по текущим сделкам.",
            "task_to_assign": "Сформировать план дожима по 5 зависшим сделкам с четким сроком каждого касания.",
            "what_to_check": "Проверяю движение по этапам и аргументацию стоимости/ценности в коммуникации.",
            "daily_meeting_thesis": "День 4: убираем зависание после теста, фиксируем путь до оплаты.",
            "expected_quantity_effect": "Сокращение зависших сделок и рост конверсии в счет.",
            "expected_quality_effect": "Предсказуемый ритм дожима и четкие контрольные точки по клиенту.",
        },
        {
            "activity_type": "стратегическая",
            "priority": "medium",
            "what_i_do": "Подвожу итоги по теплой воронке и формирую план продлений/реактиваций на следующую неделю.",
            "task_to_assign": "Подготовить отчет по активным, продлеваемым и реактивируемым сделкам с приоритетами.",
            "what_to_check": "Контролирую, что по каждой приоритетной сделке определен владелец и следующий контакт.",
            "daily_meeting_thesis": "День 5: закрепляем результат недели и готовим управляемый план продолжения.",
            "expected_quantity_effect": "Рост объема сделок с подтвержденным следующим действием.",
            "expected_quality_effect": "Системная работа по удержанию и продлению клиентской базы.",
        },
    ]


def _rewrite_row_with_progression(
    *,
    row: dict[str, Any],
    manager_role: str,
    day_slot: int,
    reason_tag: str,
) -> dict[str, Any]:
    out = dict(row)
    role_norm = clean_text(manager_role).lower()
    role_key = "telemarketer" if role_norm == "telemarketer" else "sales_manager"
    triad = _triad_template_for_role_day(role=role_key, day_slot=day_slot)
    progression = _progression_templates_for_role(role_key)[day_slot % 5]
    out["activity_type"] = progression["activity_type"]
    out["priority"] = progression["priority"]
    out["what_i_do"] = triad["focus"]
    out["task_to_assign"] = (
        f"1. Развитие: {triad['development']} "
        f"2. Коммерческий результат: {triad['commercial']} "
        f"3. Контроль: {triad['control']}"
    )
    out["what_to_check"] = triad["control"]
    out["daily_meeting_thesis"] = triad["thesis"]
    out["expected_quantity_effect"] = progression["expected_quantity_effect"]
    out["expected_quality_effect"] = progression["expected_quality_effect"]
    out["status"] = clean_text(out.get("status") or "") or "запланировано"
    out["analysis_backend_used"] = f"{clean_text(out.get('analysis_backend_used') or 'main')}|{reason_tag}"
    out["idempotency_key"] = build_exact_key(out)
    return out


def _expand_missing_manager_week_rows(
    *,
    rows: list[dict[str, Any]],
    missing_dates_by_manager: dict[str, list[str]],
    cfg: DealAnalyzerConfig | None = None,
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
        manager_rows.sort(key=lambda item: str(item.get("plan_date") or ""))
        manager_role = clean_text(manager_rows[0].get("manager_role_profile") or "")
        role_hint = resolve_role_policy(
            manager_name=recipient,
            manager_role_profile=manager_role,
            manager_role_registry=getattr(cfg, "manager_role_registry", None) if cfg is not None else None,
            role_policy_registry=getattr(cfg, "role_policy_registry", None) if cfg is not None else None,
        )
        resolved_role = str(role_hint.get("role") or "sales_manager")
        all_dates = sorted(
            {
                *[str(item.get("plan_date") or "").strip() for item in manager_rows if str(item.get("plan_date") or "").strip()],
                *[str(item).strip() for item in missing_dates if str(item).strip()],
            }
        )
        day_slot_by_date = {iso: idx for idx, iso in enumerate(all_dates)}
        for offset, missing_date in enumerate(missing_dates):
            seed = dict(manager_rows[min(offset, len(manager_rows) - 1)])
            seed["plan_date"] = str(missing_date)
            seed["day_label"] = day_label_from_iso(str(missing_date))
            slot = int(day_slot_by_date.get(str(missing_date), offset))
            seed = _rewrite_row_with_progression(
                row=seed,
                manager_role=resolved_role,
                day_slot=slot,
                reason_tag="coverage_repair_progression",
            )
            exact_key = build_exact_key(seed)
            if exact_key in existing_exact_keys:
                continue
            existing_exact_keys.add(exact_key)
            out.append(seed)
            added += 1

    return out, added


def _row_signature(row: dict[str, Any]) -> str:
    text = " ".join(
        [
            clean_text(row.get("what_i_do", "")),
            clean_text(row.get("task_to_assign", "")),
            clean_text(row.get("what_to_check", "")),
            clean_text(row.get("daily_meeting_thesis", "")),
        ]
    ).lower()
    text = re.sub(r"\d{4}-\d{2}-\d{2}", " ", text)
    text = re.sub(r"\b\d+\b", " ", text)
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    return re.sub(r"\s{2,}", " ", text).strip()


def _evaluate_duplicate_guard(rows: list[dict[str, Any]], *, threshold: float = 0.82) -> dict[str, Any]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("plan_week_start") or ""),
            str(row.get("plan_week_end") or ""),
            clean_text(row.get("recipient") or ""),
        )
        groups.setdefault(key, []).append(row)

    manager_groups: list[dict[str, Any]] = []
    global_issues = 0
    for (week_start, week_end, manager), group_rows in sorted(groups.items(), key=lambda item: (item[0][0], item[0][2].lower())):
        sorted_rows = sorted(group_rows, key=lambda item: str(item.get("plan_date") or ""))
        signatures = [_row_signature(item) for item in sorted_rows]
        exact_dups: list[dict[str, Any]] = []
        similarity_dups: list[dict[str, Any]] = []
        thesis_counter: dict[str, int] = {}
        what_counter: dict[str, int] = {}
        for idx, row in enumerate(sorted_rows):
            sig = signatures[idx]
            if not sig:
                continue
            for prev_idx in range(idx):
                prev_sig = signatures[prev_idx]
                if not prev_sig:
                    continue
                if sig == prev_sig:
                    exact_dups.append(
                        {
                            "row_index": idx,
                            "plan_date": row.get("plan_date", ""),
                            "matched_row_index": prev_idx,
                            "reason": "exact_duplicate_signature",
                        }
                    )
                    break
                similarity = difflib.SequenceMatcher(None, sig, prev_sig).ratio()
                if similarity > float(threshold):
                    similarity_dups.append(
                        {
                            "row_index": idx,
                            "plan_date": row.get("plan_date", ""),
                            "matched_row_index": prev_idx,
                            "similarity": round(similarity, 4),
                            "reason": "duplicate_task_semantic_similarity",
                        }
                    )
                    break
            thesis = clean_text(row.get("daily_meeting_thesis") or "").lower()
            if thesis:
                thesis_counter[thesis] = int(thesis_counter.get(thesis, 0) or 0) + 1
            what_i_do = clean_text(row.get("what_i_do") or "").lower()
            if what_i_do:
                what_counter[what_i_do] = int(what_counter.get(what_i_do, 0) or 0) + 1

        thesis_repeats = {k: v for k, v in thesis_counter.items() if int(v or 0) >= 3}
        what_repeats_raw = {k: v for k, v in what_counter.items() if int(v or 0) >= 2}
        what_repeats: dict[str, int] = {}
        for text, count in what_repeats_raw.items():
            matching_rows = [item for item in sorted_rows if clean_text(item.get("what_i_do") or "").lower() == text]
            if all(clean_text(item.get("activity_type") or "").lower() == "контроль" for item in matching_rows):
                what_checks = {clean_text(item.get("what_to_check") or "").lower() for item in matching_rows if clean_text(item.get("what_to_check") or "")}
                if len(what_checks) >= len(matching_rows):
                    continue
            what_repeats[text] = count

        issues_count = len(exact_dups) + len(similarity_dups) + len(thesis_repeats) + len(what_repeats)
        if issues_count > 0:
            global_issues += issues_count
        manager_groups.append(
            {
                "manager_name": manager,
                "plan_week_start": week_start,
                "plan_week_end": week_end,
                "rows_total": len(sorted_rows),
                "duplicate_exact_count": len(exact_dups),
                "duplicate_similarity_count": len(similarity_dups),
                "daily_meeting_thesis_repeat_count": len(thesis_repeats),
                "what_i_do_repeat_count": len(what_repeats),
                "exact_duplicates": exact_dups[:50],
                "similarity_duplicates": similarity_dups[:50],
                "thesis_repeats": [
                    {"text": text, "count": count}
                    for text, count in sorted(thesis_repeats.items(), key=lambda item: (-item[1], item[0]))
                ],
                "what_i_do_repeats": [
                    {"text": text, "count": count}
                    for text, count in sorted(what_repeats.items(), key=lambda item: (-item[1], item[0]))
                ],
                "status": "failed" if issues_count > 0 else "passed",
            }
        )

    return {
        "status": "failed" if global_issues > 0 else "passed",
        "global_issues_count": int(global_issues),
        "similarity_threshold": float(threshold),
        "manager_weeks": manager_groups,
    }


def _apply_role_policy_guard(
    *,
    rows: list[dict[str, Any]],
    cfg: DealAnalyzerConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    checked_fields = (
        "what_i_do",
        "task_to_assign",
        "what_to_check",
        "daily_meeting_thesis",
        "expected_quantity_effect",
        "expected_quality_effect",
    )
    demo_stage_markers = ("демо", "тест", "счет", "оплат")
    out_rows: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    debug_rows: list[dict[str, Any]] = []

    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        policy = resolve_role_policy(
            manager_name=str(updated.get("recipient") or ""),
            manager_role_profile=str(updated.get("manager_role_profile") or ""),
            manager_role_registry=getattr(cfg, "manager_role_registry", None),
            role_policy_registry=getattr(cfg, "role_policy_registry", None),
        )
        role = str(policy.get("role") or "")
        policy_demo_markers = {
            str(item or "").strip().lower().replace("_", " ").replace("-", " ")
            for item in (policy.get("demo_methodology", []) if isinstance(policy.get("demo_methodology"), list) else [])
            if str(item or "").strip()
        }
        demo_methodology_markers = {
            "consultative demo",
            "educational demo",
            "guided discovery",
            "client-led product walkthrough",
            "client led product walkthrough",
            "hands-on",
            "hands on",
            "soft influence",
            "problem-based demo",
            "problem based demo",
            "next_step commitment",
            "next step commitment",
            "совместн",
            "обучающ",
            "клиент сам",
            "критери",
            "следующ",
        }
        demo_methodology_markers.update(policy_demo_markers)
        violation_field = ""
        violation_pattern = ""
        for field in checked_fields:
            text = clean_text(updated.get(field, ""))
            blocked, marker = contains_forbidden_upper_funnel_for_sales_manager(text=text, policy=policy)
            if blocked:
                violation_field = field
                violation_pattern = marker
                break
        if not violation_field and role == "sales_manager":
            merged_text = " ".join(clean_text(updated.get(field, "")) for field in checked_fields).lower()
            merged_text_norm = merged_text.replace("_", " ").replace("-", " ")
            has_demo_stage = any(marker in merged_text for marker in demo_stage_markers)
            has_demo_methodology = any(marker in merged_text_norm for marker in demo_methodology_markers)
            if has_demo_stage and not has_demo_methodology:
                violation_field = "task_to_assign"
                violation_pattern = "demo_methodology_missing_for_sales_manager"

        debug_entry = {
            "row_index": idx,
            "recipient": str(updated.get("recipient") or ""),
            "plan_date": str(updated.get("plan_date") or ""),
            "manager_role_profile": str(updated.get("manager_role_profile") or ""),
            "resolved_role": role,
            "demo_methodology": list(policy.get("demo_methodology", []) or []),
            "demo_quality_checklist": list(policy.get("demo_quality_checklist", []) or []),
            "violation_detected": bool(violation_field),
            "violation_field": violation_field,
            "violation_pattern": violation_pattern,
            "action": "pass",
            "valid_after_repair": True,
        }
        if violation_field:
            plan_day = _parse_iso_day(str(updated.get("plan_date") or ""))
            day_slot = int(plan_day.weekday()) if plan_day is not None else 0
            updated = _rewrite_row_with_progression(
                row=updated,
                manager_role=role,
                day_slot=day_slot,
                reason_tag="role_policy_repair",
            )
            repair_still_blocked = False
            repair_field = ""
            repair_pattern = ""
            for field in checked_fields:
                blocked, marker = contains_forbidden_upper_funnel_for_sales_manager(
                    text=clean_text(updated.get(field, "")),
                    policy=policy,
                )
                if blocked:
                    repair_still_blocked = True
                    repair_field = field
                    repair_pattern = marker
                    break
            if not repair_still_blocked and role == "sales_manager":
                merged_repaired = " ".join(clean_text(updated.get(field, "")) for field in checked_fields).lower()
                merged_repaired_norm = merged_repaired.replace("_", " ").replace("-", " ")
                has_demo_stage_repaired = any(marker in merged_repaired for marker in demo_stage_markers)
                has_demo_methodology_repaired = any(marker in merged_repaired_norm for marker in demo_methodology_markers)
                if has_demo_stage_repaired and not has_demo_methodology_repaired:
                    repair_still_blocked = True
                    repair_field = "task_to_assign"
                    repair_pattern = "demo_methodology_missing_for_sales_manager"
            if repair_still_blocked:
                debug_entry["action"] = "quarantine"
                debug_entry["valid_after_repair"] = False
                debug_entry["repair_violation_field"] = repair_field
                debug_entry["repair_violation_pattern"] = repair_pattern
                quarantined.append(
                    {
                        "row_index": idx,
                        "recipient": str(row.get("recipient") or ""),
                        "plan_date": str(row.get("plan_date") or ""),
                        "reason": "role_policy_forbidden_upper_funnel",
                        "field": repair_field or violation_field,
                        "pattern": repair_pattern or violation_pattern,
                        "row": row,
                    }
                )
                debug_rows.append(debug_entry)
                continue
            debug_entry["action"] = "repaired_to_role_policy"
        out_rows.append(updated)
        debug_rows.append(debug_entry)

    return out_rows, quarantined, {
        "rows_total": len(rows),
        "rows_repaired": len([item for item in debug_rows if item.get("action") == "repaired_to_role_policy"]),
        "rows_quarantined": len(quarantined),
        "rows": debug_rows,
    }


def _apply_duplicate_guard_with_repair(
    *,
    rows: list[dict[str, Any]],
    cfg: DealAnalyzerConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    before = _evaluate_duplicate_guard(rows)
    out = [dict(item) for item in rows if isinstance(item, dict)]
    repaired_rows_count = 0
    repair_attempted = False
    repair_success = True
    if str(before.get("status")) == "failed":
        repair_attempted = True
        groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        for row in out:
            key = (
                str(row.get("plan_week_start") or ""),
                str(row.get("plan_week_end") or ""),
                clean_text(row.get("recipient") or ""),
            )
            groups.setdefault(key, []).append(row)
        out_rewritten: list[dict[str, Any]] = []
        for (_ws, _we, manager_name), group_rows in groups.items():
            if not manager_name:
                out_rewritten.extend(group_rows)
                continue
            group_rows_sorted = sorted(group_rows, key=lambda item: str(item.get("plan_date") or ""))
            role_policy = resolve_role_policy(
                manager_name=manager_name,
                manager_role_profile=str(group_rows_sorted[0].get("manager_role_profile") or ""),
                manager_role_registry=getattr(cfg, "manager_role_registry", None),
                role_policy_registry=getattr(cfg, "role_policy_registry", None),
            )
            role = str(role_policy.get("role") or "sales_manager")
            for slot, row in enumerate(group_rows_sorted):
                rewritten = _rewrite_row_with_progression(
                    row=row,
                    manager_role=role,
                    day_slot=slot,
                    reason_tag="duplicate_guard_repair",
                )
                out_rewritten.append(rewritten)
            repaired_rows_count += len(group_rows_sorted)
        out = out_rewritten
        after = _evaluate_duplicate_guard(out)
        repair_success = str(after.get("status")) == "passed"
    else:
        after = before
    return out, {
        "before": before,
        "after": after,
        "repair_attempted": bool(repair_attempted),
        "repair_success": bool(repair_success),
        "repair_rows_rewritten": int(repaired_rows_count),
        "status": str(after.get("status") or "passed"),
    }


_COMMERCIAL_MARKERS = (
    "демо",
    "тест",
    "счет",
    "оплат",
    "клиент",
    "сделк",
    "встреч",
    "лпр",
    "интерес",
    "конверс",
    "следующ",
    "продлен",
    "реактива",
)

_STRONG_COMMERCIAL_MARKERS = (
    "демо",
    "тест",
    "счет",
    "оплат",
    "встреч",
    "сделк",
    "клиент",
    "конверс",
)

_CRM_ONLY_PATTERNS = (
    "заполнить поле",
    "заполнить crm",
    "обновить карточ",
    "проверить crm",
    "проверить карточ",
    "контроль crm",
    "внести в crm",
)


def _contains_commercial_markers(text: str) -> bool:
    probe = clean_text(text).lower()
    if not probe:
        return False
    return any(marker in probe for marker in _COMMERCIAL_MARKERS)


def _contains_strong_commercial_markers(text: str) -> bool:
    probe = clean_text(text).lower()
    if not probe:
        return False
    return any(marker in probe for marker in _STRONG_COMMERCIAL_MARKERS)


def _is_crm_only_primary_task(text: str) -> bool:
    probe = clean_text(text).lower()
    if not probe:
        return False
    if any(marker in probe for marker in _CRM_ONLY_PATTERNS) and not _contains_strong_commercial_markers(probe):
        return True
    return False


def _parse_task_triad(text: str) -> dict[str, Any]:
    raw = clean_text(text)
    low = raw.lower()
    has_development = bool(re.search(r"(?:^|\s)1\.\s*развит", low))
    has_commercial = bool(re.search(r"(?:^|\s)2\.\s*коммерческ", low))
    has_control = bool(re.search(r"(?:^|\s)3\.\s*контрол", low))
    return {
        "triad_present": bool(has_development and has_commercial and has_control),
        "has_development": bool(has_development),
        "has_commercial_result": bool(has_commercial),
        "has_control": bool(has_control),
    }


def _evaluate_smart_task_row(*, row: dict[str, Any], task_text: str) -> dict[str, Any]:
    text = clean_text(task_text)
    low = text.lower()
    plan_date = clean_text(row.get("plan_date") or "")
    has_specific = bool(len(text) >= 50 and re.search(r"(сделать|провести|подготовить|назначить|разобрать|проверить|зафиксировать)", low))
    has_measurable = bool(re.search(r"\b\d+\b", text) or re.search(r"(минимум|не менее|доля|конверси|процент)", low))
    has_timebound = bool(
        (plan_date and plan_date in text)
        or re.search(r"(до конца дня|сегодня|в течение дня|к \d{1,2}:\d{2})", low)
    )
    has_pipeline_link = _contains_commercial_markers(text)
    has_check_criteria = bool(
        clean_text(row.get("what_to_check") or "")
        and re.search(r"(провер|свер|контрол|фикс|подтверд)", clean_text(row.get("what_to_check") or "").lower())
    )
    passed = bool(has_specific and has_measurable and has_timebound and has_pipeline_link and has_check_criteria)
    fail_reasons: list[str] = []
    if not has_specific:
        fail_reasons.append("smart_missing_specific_action")
    if not has_measurable:
        fail_reasons.append("smart_missing_measurable_result")
    if not has_timebound:
        fail_reasons.append("smart_missing_time_bound")
    if not has_pipeline_link:
        fail_reasons.append("smart_missing_pipeline_link")
    if not has_check_criteria:
        fail_reasons.append("smart_missing_check_criteria")
    return {
        "smart_passed": passed,
        "has_specific": has_specific,
        "has_measurable": has_measurable,
        "has_timebound": has_timebound,
        "has_pipeline_link": has_pipeline_link,
        "has_check_criteria": has_check_criteria,
        "fail_reasons": fail_reasons,
    }


def _triad_template_for_role_day(*, role: str, day_slot: int) -> dict[str, str]:
    sales = [
        {
            "focus": "Совместная диагностика теплых/текущих сделок и подготовка обучающей демонстрации.",
            "development": "Разобрать 3 кейса и подготовить гипотезу боли клиента для consultative demo.",
            "commercial": "До конца дня по 3 приоритетным сделкам назначить демо с датой/временем и согласовать критерий успеха теста.",
            "control": "Проверить в amoCRM: ЛПР, гипотеза боли, критерий успеха и точный следующий шаг зафиксированы.",
            "thesis": "Демо проводим как guided discovery: сначала задача клиента, потом релевантный сценарий и следующий шаг.",
        },
        {
            "focus": "Практика client-led product walkthrough на теплой воронке.",
            "development": "Отработать 8 формулировок вопросов: как сейчас, где узкое место, что изменится после внедрения.",
            "commercial": "До конца дня по 2 сделкам провести educational demo: клиент сам выполняет 2-3 действия в сервисе, итогом является следующий шаг.",
            "control": "Проверить, что после демо в карточке есть вывод клиента, дата теста/следующего контакта и ответственный.",
            "thesis": "Не показываем все функции подряд: ведем клиента через его задачу и hands-on действие.",
        },
        {
            "focus": "Контроль качества обучающей демонстрации и перевод в тест/пилот.",
            "development": "Разобрать 2 демо и доработать блок problem-based demo с фиксацией критерия успеха.",
            "commercial": "До конца дня по минимум 2 сделкам после demo согласовать тест/пилот и срок контрольного контакта.",
            "control": "Проверить, что после демо зафиксированы критерий успеха теста, дата и владелец следующего шага.",
            "thesis": "Демо дает результат, когда клиент сам понимает ценность и подтверждает тест с датой.",
        },
        {
            "focus": "Дожим тест -> счет через мягкое влияние и next_step commitment.",
            "development": "Отработать аргументацию ценности на 3 сделках этапа тест/согласование без агрессивного давления.",
            "commercial": "До конца дня по минимум 2 сделкам перевести этап тест -> счет и согласовать дату принятия решения.",
            "control": "Проверить, что по целевым сделкам есть счет, согласованный дедлайн и подтвержденный повторный контакт.",
            "thesis": "Закрываем не давлением, а ясным критерием результата и конкретной датой решения.",
        },
        {
            "focus": "Закрепление результатов недели и план закрытия счета в оплату.",
            "development": "Подготовить разбор лучших практик consultative demo и guided discovery за неделю.",
            "commercial": "До конца дня сформировать план закрытия минимум 2 счетов в оплату с поэтапным контролем следующего шага.",
            "control": "Проверить, что по каждому приоритетному счету назначены дата контакта, владелец и критерий результата.",
            "thesis": "Финальный фокус недели: от демо к оплате через совместную диагностику и дисциплину next step.",
        },
    ]
    tele = [
        {
            "focus": "Диагностика базы и рост качества дозвона в холодном контуре.",
            "development": "Разобрать 3 звонка и отработать старт разговора с прохождением секретаря.",
            "commercial": "До конца дня сделать 20 целевых касаний по базе и получить минимум 5 валидных дозвонов.",
            "control": "Проверить в CRM фиксацию статуса дозвона и следующего шага по каждому контакту.",
            "thesis": "Качество базы и дисциплина фиксации определяют устойчивый объем дозвонов.",
        },
        {
            "focus": "Усиление выявления ЛПР на первичном контакте.",
            "development": "Отработать 10 формулировок вопросов для подтверждения ЛПР и роли собеседника.",
            "commercial": "До конца дня провести 15 звонков и получить минимум 4 подтвержденных ЛПР.",
            "control": "Проверить заполнение поля ЛПР и причины отказа в карточках после звонков.",
            "thesis": "Без подтвержденного ЛПР интерес не считается управляемым результатом.",
        },
        {
            "focus": "Перевод диалога в интерес и назначение встречи.",
            "development": "Разобрать возражения 'не актуально' и отработать 6 ответов с переходом к встрече.",
            "commercial": "До конца дня по минимум 3 лидам перевести контакт в этап есть интерес/назначенная встреча.",
            "control": "Проверить в CRM дату встречи, канал связи и подтвержденный следующий шаг.",
            "thesis": "Интерес имеет ценность только с зафиксированной датой следующего контакта.",
        },
        {
            "focus": "Усложнение сценариев: отработка секретаря и перехват инициативы.",
            "development": "Отработать 8 сценариев обхода секретаря и возврата к ЛПР.",
            "commercial": "До конца дня сделать 18 целевых контактов и получить минимум 3 новых ЛПР/встречи.",
            "control": "Проверить, что по новым ЛПР зафиксированы цель звонка и план следующего шага.",
            "thesis": "Управление верхом воронки = четкая цель звонка и конкретный следующий шаг.",
        },
        {
            "focus": "Закрепление верхнего контура и план на следующую неделю.",
            "development": "Собрать лучшие речевые формулировки недели и подготовить чек-лист команды.",
            "commercial": "До конца дня сформировать план минимум на 25 целевых касаний и 5 ЛПР на следующую неделю.",
            "control": "Проверить, что план по базе и KPI верхней воронки отражен в задачах CRM.",
            "thesis": "Финал недели: превращаем факты дозвонов и ЛПР в управляемый недельный план.",
        },
    ]
    templates = tele if clean_text(role).lower() == "telemarketer" else sales
    return dict(templates[int(day_slot) % len(templates)])


def _apply_daily_task_triad_and_business_rules(
    *,
    rows: list[dict[str, Any]],
    cfg: DealAnalyzerConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    output_rows: list[dict[str, Any]] = []
    quarantined_rows: list[dict[str, Any]] = []
    triad_rows: list[dict[str, Any]] = []
    smart_rows: list[dict[str, Any]] = []
    commercial_rows: list[dict[str, Any]] = []

    for row_index, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        current = dict(row)
        manager_name = str(current.get("recipient") or "")
        role_policy = resolve_role_policy(
            manager_name=manager_name,
            manager_role_profile=str(current.get("manager_role_profile") or ""),
            manager_role_registry=getattr(cfg, "manager_role_registry", None),
            role_policy_registry=getattr(cfg, "role_policy_registry", None),
        )
        resolved_role = str(role_policy.get("role") or "sales_manager")
        plan_day = _parse_iso_day(str(current.get("plan_date") or ""))
        day_slot = int(plan_day.weekday()) if plan_day else 0
        template = _triad_template_for_role_day(role=resolved_role, day_slot=day_slot)

        task_before = _parse_task_triad(str(current.get("task_to_assign") or ""))
        smart_before = _evaluate_smart_task_row(row=current, task_text=str(current.get("task_to_assign") or ""))
        crm_only_primary_before = _is_crm_only_primary_task(str(current.get("what_i_do") or ""))
        commercial_before = bool(task_before.get("has_commercial_result", False) and _contains_commercial_markers(str(current.get("task_to_assign") or "")))
        before_failed = (
            (not task_before.get("triad_present", False))
            or (not smart_before.get("smart_passed", False))
            or (not commercial_before)
            or bool(crm_only_primary_before)
        )

        repaired = False
        if before_failed:
            repaired = True
            if crm_only_primary_before or not _contains_commercial_markers(str(current.get("what_i_do") or "")):
                current["what_i_do"] = template["focus"]
            task_to_assign = (
                f"1. Развитие: {template['development']} "
                f"2. Коммерческий результат: {template['commercial']} "
                f"3. Контроль: {template['control']}"
            )
            current["task_to_assign"] = task_to_assign
            if not clean_text(current.get("what_to_check") or "") or not re.search(
                r"(провер|свер|контрол|фикс|подтверд)",
                clean_text(current.get("what_to_check") or "").lower(),
            ):
                current["what_to_check"] = template["control"]
            thesis_probe = str(current.get("daily_meeting_thesis") or "")
            if (not clean_text(thesis_probe)) or bool(re.search(r"[\u4e00-\u9fff]", thesis_probe)):
                current["daily_meeting_thesis"] = template["thesis"]
            if not clean_text(current.get("expected_quantity_effect") or ""):
                current["expected_quantity_effect"] = "До конца дня выполнить количественный KPI по целевым сделкам/контактам."
            if not clean_text(current.get("expected_quality_effect") or ""):
                current["expected_quality_effect"] = "До конца дня подтвердить качество фиксации следующего шага в CRM."
            current["analysis_backend_used"] = f"{clean_text(current.get('analysis_backend_used') or 'main')}|daily_task_triad_smart_repair"
            current["idempotency_key"] = build_exact_key(current)

        task_after = _parse_task_triad(str(current.get("task_to_assign") or ""))
        smart_after = _evaluate_smart_task_row(row=current, task_text=str(current.get("task_to_assign") or ""))
        crm_only_primary_after = _is_crm_only_primary_task(str(current.get("what_i_do") or ""))
        commercial_after = bool(task_after.get("has_commercial_result", False) and _contains_commercial_markers(str(current.get("task_to_assign") or "")))

        fail_reasons: list[str] = []
        if not task_after.get("triad_present", False):
            fail_reasons.append("daily_task_triad_missing")
        if not smart_after.get("smart_passed", False):
            fail_reasons.extend(list(smart_after.get("fail_reasons", [])))
        if not commercial_after:
            fail_reasons.append("commercial_result_missing")
        if crm_only_primary_after:
            fail_reasons.append("crm_only_task_as_primary")

        triad_rows.append(
            {
                "row_index": row_index,
                "recipient": manager_name,
                "plan_date": str(current.get("plan_date") or ""),
                "resolved_role": resolved_role,
                "triad_before": task_before,
                "triad_after": task_after,
                "repaired": bool(repaired),
                "task_to_assign_preview": clean_text(current.get("task_to_assign") or "")[:300],
            }
        )
        smart_rows.append(
            {
                "row_index": row_index,
                "recipient": manager_name,
                "plan_date": str(current.get("plan_date") or ""),
                "resolved_role": resolved_role,
                "smart_before": smart_before,
                "smart_after": smart_after,
                "repaired": bool(repaired),
                "task_to_assign_preview": clean_text(current.get("task_to_assign") or "")[:300],
            }
        )
        commercial_rows.append(
            {
                "row_index": row_index,
                "recipient": manager_name,
                "plan_date": str(current.get("plan_date") or ""),
                "resolved_role": resolved_role,
                "commercial_before_passed": bool(commercial_before),
                "commercial_after_passed": bool(commercial_after),
                "crm_only_primary_before": bool(crm_only_primary_before),
                "crm_only_primary_after": bool(crm_only_primary_after),
                "what_i_do_preview": clean_text(current.get("what_i_do") or "")[:200],
            }
        )

        if fail_reasons:
            quarantined_rows.append(
                {
                    "row_index": row_index,
                    "recipient": manager_name,
                    "plan_date": str(current.get("plan_date") or ""),
                    "reason": "daily_task_triad_or_smart_failed",
                    "fail_reasons": fail_reasons,
                    "row": current,
                }
            )
            continue
        output_rows.append(current)

    triad_debug = {
        "rows_total": len(rows),
        "rows_with_triad_before": len([item for item in triad_rows if bool(item.get("triad_before", {}).get("triad_present", False))]),
        "rows_with_triad_after": len([item for item in triad_rows if bool(item.get("triad_after", {}).get("triad_present", False))]),
        "rows_repaired": len([item for item in triad_rows if bool(item.get("repaired", False))]),
        "rows_failed": len(quarantined_rows),
        "rows": triad_rows,
    }
    smart_debug = {
        "rows_total": len(rows),
        "rows_passed_after": len([item for item in smart_rows if bool(item.get("smart_after", {}).get("smart_passed", False))]),
        "rows_failed_after": len([item for item in smart_rows if not bool(item.get("smart_after", {}).get("smart_passed", False))]),
        "rows_repaired": len([item for item in smart_rows if bool(item.get("repaired", False))]),
        "rows": smart_rows,
    }
    commercial_debug = {
        "rows_total": len(rows),
        "rows_passed_after": len([item for item in commercial_rows if bool(item.get("commercial_after_passed", False))]),
        "rows_failed_after": len([item for item in commercial_rows if not bool(item.get("commercial_after_passed", False))]),
        "crm_only_primary_count_after": len([item for item in commercial_rows if bool(item.get("crm_only_primary_after", False))]),
        "rows": commercial_rows,
    }
    return output_rows, quarantined_rows, triad_debug, smart_debug, commercial_debug


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


def _load_client_list_context(
    *,
    cfg: DealAnalyzerConfig,
    logger: Any,
    managers_in_scope: list[str],
    period_start: date,
    period_end: date,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    if not bool(getattr(cfg, "client_list_enabled", False)):
        return (
            {"status": "disabled", "client_list_enabled": False, "warnings": ["client_list_disabled"]},
            {"rows_total": 0, "rows": [], "rejected_rows": []},
            {"rows_total": 0, "categories": {}, "top_rows": []},
            {},
        )

    try:
        discovery = discover_client_list_sheet(cfg=cfg, logger=logger)
        snapshot = read_client_list_sheet(cfg=cfg, logger=logger)
        mapping = build_client_list_header_mapping(snapshot.headers, cfg=cfg)
        normalized_rows, rejected_rows = normalize_client_rows(
            headers=snapshot.headers,
            rows=snapshot.rows,
            mapping=mapping,
            header_row_number=snapshot.header_row_number,
        )
        priority_summary = build_client_priority_summary(normalized_rows)
        context_by_manager: dict[str, dict[str, Any]] = {}
        manager_role_registry = getattr(cfg, "manager_role_registry", None)
        role_policy_registry = getattr(cfg, "role_policy_registry", None)
        for manager_name in managers_in_scope:
            manager = clean_text(manager_name)
            if not manager:
                continue
            context = build_manager_client_context(
                rows=normalized_rows,
                manager_name=manager,
                period_start=period_start.isoformat(),
                period_end=period_end.isoformat(),
                manager_role_registry=manager_role_registry,
                role_policy_registry=role_policy_registry,
            )
            context_by_manager[manager.lower()] = asdict(context)

        normalized_payload = {
            "rows_total": len(normalized_rows),
            "rows": [asdict(item) for item in normalized_rows],
            "rejected_rows": rejected_rows,
            "mapped_columns": {
                field: snapshot.headers[idx]
                for field, idx in mapping.items()
                if isinstance(idx, int) and 0 <= idx < len(snapshot.headers)
            },
        }
        return discovery, normalized_payload, priority_summary, context_by_manager
    except Exception as exc:
        return (
            {
                "status": "read_error",
                "client_list_enabled": True,
                "warnings": ["client_list_read_failed"],
                "error": str(exc),
            },
            {"rows_total": 0, "rows": [], "rejected_rows": [{"reason": "client_list_read_failed", "error": str(exc)}]},
            {"rows_total": 0, "categories": {}, "top_rows": [], "error": str(exc)},
            {},
        )


def _inject_client_context_into_sales_manager_rows(
    *,
    rows: list[dict[str, Any]],
    client_context_by_manager: dict[str, dict[str, Any]],
    cfg: DealAnalyzerConfig,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out: list[dict[str, Any]] = []
    touched_rows = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        updated = dict(row)
        recipient = clean_text(updated.get("recipient", ""))
        policy = resolve_role_policy(
            manager_name=recipient,
            manager_role_profile=clean_text(updated.get("manager_role_profile", "")),
            manager_role_registry=getattr(cfg, "manager_role_registry", None),
            role_policy_registry=getattr(cfg, "role_policy_registry", None),
        )
        if str(policy.get("role") or "") != "sales_manager":
            out.append(updated)
            continue
        context = client_context_by_manager.get(recipient.lower(), {})
        top_items = context.get("top_priority_items", []) if isinstance(context.get("top_priority_items"), list) else []
        if not top_items:
            out.append(updated)
            continue
        stage_labels = [clean_text(item.get("priority_category", "")) for item in top_items if clean_text(item.get("priority_category", ""))]
        deal_links = [
            clean_text(item.get("deal_link", "")) or clean_text(item.get("contact_link", "")) or clean_text(item.get("company_link", ""))
            for item in top_items
            if any(
                [
                    clean_text(item.get("deal_link", "")),
                    clean_text(item.get("contact_link", "")),
                    clean_text(item.get("company_link", "")),
                ]
            )
        ]
        stage_text = ", ".join(stage_labels[:3])
        link_text = "; ".join(deal_links[:2])
        original_task = clean_text(updated.get("task_to_assign", ""))
        original_focus = clean_text(updated.get("what_i_do", ""))
        if stage_text and stage_text not in original_task.lower():
            original_task = f"{original_task} Приоритетные категории: {stage_text}.".strip()
        if link_text and "http" not in original_task.lower():
            original_task = f"{original_task} Сделки в работе: {link_text}.".strip()
        if "заполн" in original_focus.lower() and "crm" in original_focus.lower():
            original_focus = "Дожимаю теплые сделки по этапам demo/test/invoice/payment с фокусом на ближайший коммерческий шаг."
        updated["task_to_assign"] = re.sub(r"\s{2,}", " ", original_task).strip()
        updated["what_i_do"] = re.sub(r"\s{2,}", " ", original_focus).strip()
        out.append(updated)
        touched_rows += 1
        if len(examples) < 8:
            examples.append(
                {
                    "recipient": recipient,
                    "plan_date": str(updated.get("plan_date", "")),
                    "priority_categories": stage_labels[:3],
                    "deal_links_used": deal_links[:2],
                }
            )

    return out, {"rows_touched": touched_rows, "examples": examples}


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
    progress = ProgressReporter(
        process="week_plan",
        run_dir=run_dir,
        heartbeat_seconds=int(getattr(cfg, "progress_heartbeat_seconds", 30) or 30),
        logger=logger,
        step_name="init",
        total=0,
    )

    periods = _resolve_signal_and_plan_periods(args)
    signal_start = periods["signal_start"]
    signal_end = periods["signal_end"]
    plan_week_start = periods["plan_week_start"]
    plan_week_end = periods["plan_week_end"]
    period_warnings = list(periods.get("period_warnings", []) or [])
    progress.update(
        step_name="period_resolved",
        current=0,
        total=0,
        current_item={
            "stage": "period",
            "date": f"{signal_start.isoformat()}..{signal_end.isoformat()}",
            "plan_date": f"{plan_week_start.isoformat()}..{plan_week_end.isoformat()}",
        },
    )

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
    progress.update(step_name="sheet_discovery", current=0, total=0, current_item={"stage": "discover"})

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
    progress.update(
        step_name="source_read_completed",
        current=0,
        total=0,
        current_item={"stage": "source_read", "rows": len(snapshot.rows)},
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
    progress.update(
        step_name="signals_grouped",
        current=0,
        total=len(groups),
        current_item={"stage": "grouping", "groups": len(groups)},
    )
    managers_in_scope_for_client_list = sorted(
        {
            clean_text(getattr(group, "manager_name", ""))
            for group in groups
            if clean_text(getattr(group, "manager_name", ""))
        }
    )
    (
        client_list_discovery,
        client_list_rows_normalized,
        client_list_priority_summary,
        client_context_by_manager,
    ) = _load_client_list_context(
        cfg=cfg,
        logger=logger,
        managers_in_scope=managers_in_scope_for_client_list,
        period_start=plan_week_start,
        period_end=plan_week_end,
    )

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
            client_context_by_manager=client_context_by_manager,
            llm_runtime=llm_runtime,
            logger=logger,
            source_run_id=run_dir.name,
            main_model_override=str(args.main_model or "").strip() or None,
            fallback_model_override=str(args.fallback_model or "").strip() or None,
            llm_max_attempts=int(args.llm_max_attempts or 6),
            allowed_activity_types=allowed_activity_types,
        )
    progress.update(
        step_name="llm_completed",
        current=len(rows),
        total=max(len(groups), len(rows)),
        current_item={"stage": "llm", "model": str(args.main_model or "") or "default"},
    )

    rows_cleaned, cleanup_counts = _clean_rows_technical(rows)
    rows_cleaned, client_context_apply_debug = _inject_client_context_into_sales_manager_rows(
        rows=rows_cleaned,
        client_context_by_manager=client_context_by_manager,
        cfg=cfg,
    )
    rows_cleaned, activity_type_normalization_debug = _normalize_activity_types_for_rows(
        rows=rows_cleaned,
        allowed_values=allowed_activity_types,
    )
    rows_cleaned, role_policy_quarantined, role_policy_debug = _apply_role_policy_guard(
        rows=rows_cleaned,
        cfg=cfg,
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
            cfg=cfg,
        )
        coverage_after_repair = _compute_manager_week_coverage(
            rows=rows_cleaned,
            managers_in_scope=managers_in_planning_scope,
            expected_workdays=expected_workdays,
        )
        coverage_repair_success = bool(coverage_after_repair.get("coverage_complete", False))
    else:
        coverage_after_repair = coverage_before

    rows_cleaned, triad_quarantined, triad_debug, smart_debug, commercial_debug = _apply_daily_task_triad_and_business_rules(
        rows=rows_cleaned,
        cfg=cfg,
    )
    rows_cleaned, triad_cleanup_counts = _clean_rows_technical(rows_cleaned)
    for key, value in triad_cleanup_counts.items():
        cleanup_counts[key] = int(cleanup_counts.get(key, 0) or 0) + int(value or 0)

    rows_cleaned, employee_profile_context_rows, employee_behavior_rows = _apply_employee_profiles_to_week_rows(
        rows=rows_cleaned,
        cfg=cfg,
    )
    rows_cleaned, profile_cleanup_counts = _clean_rows_technical(rows_cleaned)
    for key, value in profile_cleanup_counts.items():
        cleanup_counts[key] = int(cleanup_counts.get(key, 0) or 0) + int(value or 0)

    rows_cleaned, duplicate_guard_debug = _apply_duplicate_guard_with_repair(
        rows=rows_cleaned,
        cfg=cfg,
    )
    rows_cleaned, duplicate_cleanup_counts = _clean_rows_technical(rows_cleaned)
    for key, value in duplicate_cleanup_counts.items():
        cleanup_counts[key] = int(cleanup_counts.get(key, 0) or 0) + int(value or 0)
    duplicate_guard_failed = str(duplicate_guard_debug.get("status") or "passed") != "passed"

    writer_rows, payload_validator_rejected = _payload_row_validation_rejections(rows_cleaned)
    duplicate_guard_blocked_rows: list[dict[str, Any]] = []
    if duplicate_guard_failed and require_full_manager_week_coverage:
        for row_index, row in enumerate(writer_rows):
            if not isinstance(row, dict):
                continue
            duplicate_guard_blocked_rows.append(
                {
                    "row_index": row_index,
                    "recipient": str(row.get("recipient") or ""),
                    "plan_date": str(row.get("plan_date") or ""),
                    "reason": "duplicate_task_semantic_similarity",
                    "row": row,
                }
            )
        writer_rows = []

    llm_quarantined = llm_diag.get("quarantined_rows", []) if isinstance(llm_diag.get("quarantined_rows"), list) else []
    quarantined_rows = [
        *llm_quarantined,
        *role_policy_quarantined,
        *triad_quarantined,
        *payload_validator_rejected,
        *duplicate_guard_blocked_rows,
    ]
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
    for item in triad_quarantined:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("recipient") or "").strip(),
                "reason": str(item.get("reason") or "daily_task_triad_or_smart_failed"),
                "stage": "daily_task_triad",
                "plan_date": str(item.get("plan_date") or ""),
                "fail_reasons": list(item.get("fail_reasons", []) or []),
            }
        )
    for item in role_policy_quarantined:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("recipient") or "").strip(),
                "reason": str(item.get("reason") or "role_policy_forbidden_upper_funnel"),
                "stage": "role_policy_guard",
                "plan_date": str(item.get("plan_date") or ""),
            }
        )
    for item in duplicate_guard_blocked_rows:
        if not isinstance(item, dict):
            continue
        managers_skipped_with_reason.append(
            {
                "manager_name": str(item.get("recipient") or "").strip(),
                "reason": str(item.get("reason") or "duplicate_task_semantic_similarity"),
                "stage": "duplicate_guard",
                "plan_date": str(item.get("plan_date") or ""),
            }
        )
    if str(duplicate_guard_debug.get("status") or "passed") != "passed":
        for group_item in duplicate_guard_debug.get("after", {}).get("manager_weeks", []):
            if not isinstance(group_item, dict):
                continue
            if str(group_item.get("status") or "passed") == "passed":
                continue
            managers_skipped_with_reason.append(
                {
                    "manager_name": str(group_item.get("manager_name") or ""),
                    "reason": "duplicate_task_semantic_similarity",
                    "stage": "duplicate_guard",
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
        "role_policy_guard": role_policy_debug,
        "duplicate_guard": duplicate_guard_debug,
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
        "daily_task_triad": triad_debug,
        "smart_validation": smart_debug,
        "commercial_effect": commercial_debug,
        "client_list_context_status": client_list_discovery.get("status", ""),
        "client_list_context_by_manager": client_context_by_manager,
        "employee_profile_context_rows": employee_profile_context_rows,
        "employee_behavior_markers": employee_behavior_rows,
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
    for item in role_policy_quarantined:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "role_policy_guard",
                    "row_index": item.get("row_index"),
                    "recipient": item.get("recipient", ""),
                    "plan_date": item.get("plan_date", ""),
                    "reason": item.get("reason", "role_policy_forbidden_upper_funnel"),
                    "field": item.get("field", ""),
                    "pattern": item.get("pattern", ""),
                }
            )
    for item in triad_quarantined:
        if isinstance(item, dict):
            row_flow_filtered.append(
                {
                    "stage": "daily_task_triad",
                    "row_index": item.get("row_index"),
                    "recipient": item.get("recipient", ""),
                    "plan_date": item.get("plan_date", ""),
                    "reason": item.get("reason", "daily_task_triad_or_smart_failed"),
                    "fail_reasons": item.get("fail_reasons", []),
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
    if str(duplicate_guard_debug.get("status") or "passed") != "passed":
        row_flow_filtered.append(
            {
                "stage": "duplicate_guard",
                "reason": "duplicate_task_semantic_similarity",
                "before": duplicate_guard_debug.get("before", {}),
                "after": duplicate_guard_debug.get("after", {}),
                "repair_attempted": bool(duplicate_guard_debug.get("repair_attempted", False)),
                "repair_success": bool(duplicate_guard_debug.get("repair_success", False)),
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
        "duplicate_guard_blocked_rows_count": len(duplicate_guard_blocked_rows),
        "role_policy_guard": role_policy_debug,
        "daily_task_triad": triad_debug,
        "smart_validation": smart_debug,
        "commercial_effect": commercial_debug,
        "duplicate_guard": duplicate_guard_debug,
        "client_list_context_status": client_list_discovery.get("status", ""),
        "client_context_apply_debug": client_context_apply_debug,
        "employee_profile_context_rows_count": len(employee_profile_context_rows),
        "employee_behavior_markers_count": len(employee_behavior_rows),
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
    write_json(run_dir / "client_list_discovery.json", client_list_discovery)
    write_json(run_dir / "client_list_rows_normalized.json", client_list_rows_normalized)
    write_json(run_dir / "client_list_priority_summary.json", client_list_priority_summary)
    write_json(
        run_dir / "week_plan_client_context_debug.json",
        {
            "client_list_status": client_list_discovery.get("status", ""),
            "managers_in_scope": managers_in_scope_for_client_list,
            "context_by_manager": client_context_by_manager,
            "context_apply_debug": client_context_apply_debug,
        },
    )
    write_json(
        run_dir / "employee_profile_context_debug.json",
        {
            "rows_total": len(employee_profile_context_rows),
            "rows": employee_profile_context_rows,
        },
    )
    write_json(
        run_dir / "employee_behavior_markers.json",
        {
            "rows_total": len(employee_behavior_rows),
            "rows": employee_behavior_rows,
        },
    )
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
    write_json(run_dir / "week_plan_role_policy_debug.json", role_policy_debug)
    write_json(run_dir / "week_plan_daily_task_triad_debug.json", triad_debug)
    write_json(run_dir / "week_plan_smart_validation_debug.json", smart_debug)
    write_json(run_dir / "week_plan_commercial_effect_debug.json", commercial_debug)
    write_json(run_dir / "week_plan_duplicate_guard_debug.json", duplicate_guard_debug)
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
            "duplicate_guard_status": str(duplicate_guard_debug.get("status") or "passed"),
            "final_decision": "blocked_manager_week_coverage_incomplete"
            if coverage_incomplete
            else "blocked_duplicate_guard"
            if duplicate_guard_failed
            else "coverage_ok",
        },
    )
    write_json(run_dir / "week_plan_bootstrap_debug.json", bootstrap_debug)
    write_json(run_dir / "week_plan_quality_review.json", quality_review)

    if bootstrap_mode_used and not managers_resolved:
        build_block_reason = "bootstrap_managers_not_resolved"
    elif signals_blocked:
        build_block_reason = "signals_empty_from_non_empty_source"
    elif duplicate_guard_failed:
        build_block_reason = "duplicate_task_semantic_similarity"
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
        "duplicate_guard_status": str(duplicate_guard_debug.get("status") or "passed"),
        "duplicate_guard_repair_attempted": bool(duplicate_guard_debug.get("repair_attempted", False)),
        "duplicate_guard_repair_success": bool(duplicate_guard_debug.get("repair_success", False)),
        "role_policy_rows_repaired": int(role_policy_debug.get("rows_repaired", 0) or 0),
        "role_policy_rows_quarantined": int(role_policy_debug.get("rows_quarantined", 0) or 0),
        "daily_task_triad_rows_repaired": int(triad_debug.get("rows_repaired", 0) or 0),
        "daily_task_triad_rows_failed": int(triad_debug.get("rows_failed", 0) or 0),
        "smart_validation_rows_passed_after": int(smart_debug.get("rows_passed_after", 0) or 0),
        "smart_validation_rows_failed_after": int(smart_debug.get("rows_failed_after", 0) or 0),
        "commercial_effect_rows_passed_after": int(commercial_debug.get("rows_passed_after", 0) or 0),
        "commercial_effect_rows_failed_after": int(commercial_debug.get("rows_failed_after", 0) or 0),
        "crm_only_primary_count_after": int(commercial_debug.get("crm_only_primary_count_after", 0) or 0),
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
        "client_list_status": client_list_discovery.get("status", ""),
        "client_list_rows_total": int(client_list_rows_normalized.get("rows_total", 0) or 0),
        "client_list_context_managers": sorted(client_context_by_manager.keys()),
        "client_list_rows_touched": int(client_context_apply_debug.get("rows_touched", 0) or 0),
        "employee_profile_context_rows": len(employee_profile_context_rows),
        "employee_behavior_markers_rows": len(employee_behavior_rows),
        "writer_mode": "dry_run",
        "write_allowed": False,
        "empty_payload_blocked": bool(empty_payload_blocked),
        "block_reason": build_block_reason,
    }

    write_json(run_dir / "summary.json", summary)
    write_markdown(run_dir / "summary.md", title="Week Plan Summary", lines=_summary_markdown_lines(summary))
    progress.update(
        step_name="artifacts_written",
        current=len(writer_rows),
        total=max(len(rows), len(writer_rows)),
        current_item={"stage": "artifacts", "rows": len(writer_rows)},
    )
    progress.finish(status="completed", step_name="build_completed")
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
