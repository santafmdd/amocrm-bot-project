from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.config import load_config
from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id


@dataclass(frozen=True)
class EnrichmentSourceMatch:
    matched: bool
    method: str
    confidence_score: int
    row_id: str
    row: dict[str, str]


@dataclass(frozen=True)
class EnrichmentContext:
    client_rows: list[dict[str, str]]
    appointment_rows: list[dict[str, str]]


_DEFAULT_HEADER_FALLBACKS: dict[str, dict[str, list[str]]] = {
    "client_list": {
        "deal_id": ["deal_id", "сделка id", "id сделки", "amo_lead_id", "lead_id"],
        "phone": ["phone", "телефон", "контактный телефон", "номер"],
        "email": ["email", "почта", "e-mail"],
        "company_name": ["company", "компания", "название компании"],
        "contact_name": ["contact", "контакт", "фио", "имя контакта"],
        "test_started": ["тест начат", "test started", "start_test"],
        "test_completed": ["тест завершен", "test completed", "end_test"],
        "test_status": ["статус теста", "test status"],
        "test_comments": ["комментарии по тесту", "комментарий", "test comments"],
    },
    "appointment_list": {
        "deal_id": ["deal_id", "сделка id", "id сделки", "amo_lead_id", "lead_id"],
        "phone": ["phone", "телефон", "контактный телефон", "номер"],
        "email": ["email", "почта", "e-mail"],
        "company_name": ["company", "компания", "название компании"],
        "contact_name": ["contact", "контакт", "фио", "имя контакта"],
        "appointment_date": ["дата назначения", "appointment date", "date"],
        "assigned_by": ["кто назначил", "назначил", "assigned by"],
        "conducted_by": ["кто проводил", "проводил", "conducted by"],
        "meeting_status": ["статус встречи", "meeting status", "status"],
        "transfer_cancel_flag": ["перенос/отмена", "перенос", "отмена", "cancel", "reschedule"],
    },
}


def enrich_rows(
    rows: list[dict[str, Any]],
    *,
    config,
    logger,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    if not (bool(config.client_list_enrich_enabled) or bool(config.appointment_list_enrich_enabled)):
        return [_mark_enrichment_disabled(dict(row)) for row in rows]

    context = _load_context(config=config, logger=logger)
    enriched: list[dict[str, Any]] = []
    for row in rows:
        enriched.append(_enrich_one(dict(row), context=context, config=config, logger=logger))
    return enriched


def build_operator_outputs(*, deal: dict[str, Any], analysis: dict[str, Any]) -> dict[str, Any]:
    deal_name = _clean_text(deal.get("deal_name")) or f"Сделка {deal.get('deal_id') or deal.get('amo_lead_id') or '-'}"
    pipeline = _clean_text(deal.get("pipeline_name"))
    status = _clean_text(deal.get("status_name"))
    match_status = _clean_text(deal.get("enrichment_match_status")) or "unknown"

    risk_flags = analysis.get("risk_flags") if isinstance(analysis.get("risk_flags"), list) else []
    growth_zones = analysis.get("growth_zones") if isinstance(analysis.get("growth_zones"), list) else []
    risk_flags_norm = [str(x).strip().lower() for x in risk_flags]
    status_norm = _clean_text(deal.get("status_name")).lower().replace("ё", "е")
    is_closed_lost = any(x in status_norm for x in ("закрыто", "не реализ", "отказ"))
    has_qualified_loss = any(x.startswith("qualified_loss:") for x in risk_flags_norm)
    has_evidence_context = any(x.startswith("evidence_context:") for x in risk_flags_norm)
    notes_present = isinstance(deal.get("notes_summary_raw"), list) and len(deal.get("notes_summary_raw")) > 0
    tasks_present = isinstance(deal.get("tasks_summary_raw"), list) and len(deal.get("tasks_summary_raw")) > 0
    comments_present = bool(_clean_text(deal.get("company_comment")) or _clean_text(deal.get("contact_comment")))
    has_closed_context = notes_present or tasks_present or comments_present
    analysis_confidence = _clean_text(analysis.get("analysis_confidence")).lower()
    owner_ambiguity = bool(analysis.get("owner_ambiguity_flag"))
    analysis_dq_flags = [str(x).strip().lower() for x in (analysis.get("data_quality_flags") or []) if str(x).strip()]
    severe_low_confidence_markers = {
        "crm_context_missing_with_stage_movement",
        "crm_context_sparse_with_activity_signals",
        "closed_lost_without_documented_reason",
        "owner_ambiguity_detected",
        "owner_ambiguity_responsible_mismatch",
        "owner_ambiguity_assigned_by_mismatch",
        "owner_ambiguity_attribution_limited",
    }

    manager_summary = (
        f"{deal_name}: этап '{status or '-'}'"
        + (f" в воронке '{pipeline}'" if pipeline else "")
        + ". "
        + f"Сопоставление с внешними таблицами: {match_status}. "
        + (
            f"Ключевые риски: {', '.join(str(x) for x in risk_flags[:2])}. "
            if risk_flags
            else "Критичные риски не зафиксированы. "
        )
        + "Следующий шаг: зафиксировать конкретное действие и срок в CRM."
    )

    if has_qualified_loss:
        return {
            "manager_summary": (
                f"{deal_name}: зафиксирован qualified loss (anti-fit/market mismatch). "
                "Нужна корректная closeout-классификация без pressure follow-up."
            ),
            "employee_coaching": (
                "Кейс не про дожим: фокус на ранней квалификации, корректной фиксации anti-fit/market mismatch "
                "и аккуратном закрытии сделки с полезным контекстом в CRM."
            ),
            "employee_fix_tasks": [
                "Зафиксировать причину отказа в формате «что не совпало и почему это anti-fit/market mismatch».",
                "Обновить closeout-классификацию: тип причины, этап выявления и критерий нецелевого кейса.",
                "Добавить короткую заметку по раннему отсеву похожих кейсов в будущих сделках.",
            ],
        }

    low_confidence_guard = analysis_confidence == "low" and (
        is_closed_lost
        or owner_ambiguity
        or any(flag in severe_low_confidence_markers for flag in analysis_dq_flags)
    )
    if low_confidence_guard and not has_qualified_loss:
        manager_caution = (
            "Интерпретация ограничена качеством CRM-данных и возможной owner ambiguity."
            if owner_ambiguity
            else "Интерпретация ограничена качеством CRM-данных."
        )
        if is_closed_lost:
            return {
                "manager_summary": (
                    f"{deal_name}: closed-lost кейс с ограниченной надежностью выводов. "
                    f"{manager_caution} Приоритет — корректная фиксация причины потери и closeout-cleanup."
                ),
                "employee_coaching": (
                    "Не делаем вывод о бездействии: сначала восстановить фактическую причину потери, "
                    "проверить атрибуцию owner и заполнить CRM-контекст."
                ),
                "employee_fix_tasks": [
                    "Зафиксировать причину потери в нейтральном формате «кто/почему/на каком этапе».",
                    "Проверить соответствие owner в CRM фактическому ведущему сделки.",
                    "Сделать closeout-cleanup: убрать пустые/противоречивые поля и обновить классификацию.",
                ],
            }
        return {
            "manager_summary": (
                f"{deal_name}: вывод по сделке ограничен качеством CRM-данных. {manager_caution} "
                "До персональных выводов подтвердить фактического ведущего и контекст последних касаний."
            ),
            "employee_coaching": (
                "Сфокусироваться на фактах и полноте CRM: коротко зафиксировать контекст, следующий шаг и владельца коммуникации."
            ),
            "employee_fix_tasks": [
                "Проверить атрибуцию owner и фактического ведущего сделки.",
                "Добавить короткий evidence note по последнему значимому контакту.",
                "Зафиксировать следующий шаг и срок в нейтральной формулировке.",
            ],
        }

    if is_closed_lost and not has_qualified_loss and has_closed_context:
        return {
            "manager_summary": (
                f"{deal_name}: closed-lost кейс с базовым контекстом. "
                "Нужно завершить корректную closeout-классификацию и зафиксировать причину потери."
            ),
            "employee_coaching": (
                "Фокус на качестве фиксации причины потери: кто отказал, что именно не совпало, "
                "и какой anti-pattern нужно учесть в будущей квалификации."
            ),
            "employee_fix_tasks": [
                "Уточнить и зафиксировать ключевую причину потери в финальной записи сделки.",
                "Проставить closeout-классификацию (тип причины + этап, где выявлен разрыв).",
                "Сделать CRM-cleanup: убрать пустые/противоречивые поля в закрытой сделке.",
                "Добавить короткий anti-pattern note для раннего отсева похожих кейсов.",
            ],
        }

    if is_closed_lost and has_evidence_context:
        return {
            "manager_summary": (
                f"{deal_name}: закрытая сделка с дефицитом evidence-контекста. "
                "Приоритет — восстановить причину потери и корректно классифицировать closeout."
            ),
            "employee_coaching": (
                "Сначала восстановить фактологию потери "
                "(кто отказал, почему, на каком этапе, что именно не совпало)."
            ),
            "employee_fix_tasks": [
                "Дописать причину потери в карточке: кто принял решение и по какой причине.",
                "Проставить closeout-классификацию (цена/срок/продукт/нецелевой запрос/другое).",
                "Сделать CRM-cleanup: убрать пустые поля и противоречащие записи по финальному статусу.",
            ],
        }

    coaching_parts: list[str] = []
    if growth_zones:
        coaching_parts.append(f"Зоны роста: {', '.join(str(x) for x in growth_zones[:2])}.")
    if not _clean_text(deal.get("pain_text")):
        coaching_parts.append("Уточни и зафиксируй боль клиента одной короткой формулировкой.")
    if not _clean_text(deal.get("business_tasks_text")):
        coaching_parts.append("Добавь бизнес-задачу клиента и критерий результата.")
    if not coaching_parts:
        coaching_parts.append("Сделка оформлена достаточно полно; удерживай качество фиксации в CRM.")
    employee_coaching = " ".join(coaching_parts)

    fix_tasks: list[str] = []
    if not _clean_text(deal.get("pain_text")):
        fix_tasks.append("Записать боль клиента в карточку сделки (1-2 предложения).")
    if not _clean_text(deal.get("business_tasks_text")):
        fix_tasks.append("Заполнить блок бизнес-задач клиента в CRM.")
    if not _clean_text(deal.get("brief_url")):
        fix_tasks.append("Добавить ссылку на бриф/материалы или указать причину отсутствия.")
    if not analysis.get("followup_quality_flag") == "ok":
        fix_tasks.append("Поставить follow-up задачу с датой, ответственным и ожидаемым результатом.")
    if _clean_text(deal.get("enrichment_match_status")) in {"none", "disabled"}:
        fix_tasks.append("Проверить совпадение с клиентским списком/встречами и актуализировать контакты.")
    if not fix_tasks:
        fix_tasks.append("Проверить, что следующий шаг сделки зафиксирован и подтвержден клиентом.")
    while len(fix_tasks) < 3:
        fix_tasks.append("Уточнить у клиента следующий контакт и зафиксировать дедлайн в CRM.")
    return {
        "manager_summary": manager_summary,
        "employee_coaching": employee_coaching,
        "employee_fix_tasks": fix_tasks[:7],
    }


def _mark_enrichment_disabled(row: dict[str, Any]) -> dict[str, Any]:
    row["enrichment_match_status"] = "disabled"
    row["enrichment_match_source"] = "none"
    row["enrichment_confidence"] = 0.0
    row["matched_client_list_row_id"] = ""
    row["matched_appointment_row_id"] = ""
    return row


def _load_context(*, config, logger) -> EnrichmentContext:
    app_cfg = load_config()
    gs_client = GoogleSheetsApiClient(project_root=app_cfg.project_root, logger=logger)

    client_rows: list[dict[str, str]] = []
    appointment_rows: list[dict[str, str]] = []

    if config.client_list_enrich_enabled and config.client_list_source_url and config.client_list_sheet_name:
        client_rows = _load_sheet_records(
            gs_client=gs_client,
            source_url=config.client_list_source_url,
            sheet_name=config.client_list_sheet_name,
            source_label="client_list",
            logger=logger,
        )
    elif config.client_list_enrich_enabled:
        logger.warning("deal analyzer enrich: client_list enabled but source url/sheet missing")

    if config.appointment_list_enrich_enabled and config.appointment_list_source_url and config.appointment_list_sheet_name:
        appointment_rows = _load_sheet_records(
            gs_client=gs_client,
            source_url=config.appointment_list_source_url,
            sheet_name=config.appointment_list_sheet_name,
            source_label="appointment_list",
            logger=logger,
        )
    elif config.appointment_list_enrich_enabled:
        logger.warning("deal analyzer enrich: appointment_list enabled but source url/sheet missing")

    return EnrichmentContext(client_rows=client_rows, appointment_rows=appointment_rows)


def _load_sheet_records(*, gs_client, source_url: str, sheet_name: str, source_label: str, logger) -> list[dict[str, str]]:
    try:
        spreadsheet_id = extract_spreadsheet_id(source_url)
        range_a1 = f"'{sheet_name}'!A:ZZ"
        matrix = gs_client.get_values(spreadsheet_id, range_a1)
    except Exception as exc:
        logger.warning("deal analyzer enrich: failed loading %s source: %s", source_label, exc)
        return []

    if not matrix:
        logger.info("deal analyzer enrich: source %s is empty", source_label)
        return []

    header = [_normalize_header(cell) for cell in (matrix[0] if matrix else [])]
    rows: list[dict[str, str]] = []
    for idx, raw_row in enumerate(matrix[1:], start=2):
        values = [str(cell or "").strip() for cell in raw_row]
        if not any(values):
            continue
        row: dict[str, str] = {"__row_id": str(idx)}
        for col_idx, key in enumerate(header):
            if not key:
                continue
            row[key] = values[col_idx] if col_idx < len(values) else ""
        rows.append(row)

    logger.info("deal analyzer enrich: loaded %s rows from %s", len(rows), source_label)
    return rows


def _enrich_one(row: dict[str, Any], *, context: EnrichmentContext, config, logger) -> dict[str, Any]:
    client_match = _match_row(
        deal=row,
        source_rows=context.client_rows,
        source_kind="client_list",
        fields_mapping=(config.fields_mapping or {}),
    )
    appointment_match = _match_row(
        deal=row,
        source_rows=context.appointment_rows,
        source_kind="appointment_list",
        fields_mapping=(config.fields_mapping or {}),
    )

    if client_match.matched and appointment_match.matched:
        status = "full"
        source = "both"
    elif client_match.matched:
        status = "partial"
        source = "client_list"
    elif appointment_match.matched:
        status = "partial"
        source = "appointment_list"
    else:
        status = "none"
        source = "none"

    confidence = max(client_match.confidence_score, appointment_match.confidence_score) / 100.0

    row["enrichment_match_status"] = status
    row["enrichment_match_source"] = source
    row["enrichment_confidence"] = round(confidence, 2)
    row["matched_client_list_row_id"] = client_match.row_id
    row["matched_appointment_row_id"] = appointment_match.row_id

    _apply_client_fields(row, client_match.row, (config.fields_mapping or {}))
    _apply_appointment_fields(row, appointment_match.row, (config.fields_mapping or {}))
    _apply_owner_ambiguity_hints(row)

    logger.info(
        "deal analyzer enrich: deal=%s status=%s source=%s confidence=%.2f client_row=%s appointment_row=%s",
        row.get("deal_id") or row.get("amo_lead_id") or "-",
        status,
        source,
        row["enrichment_confidence"],
        row["matched_client_list_row_id"],
        row["matched_appointment_row_id"],
    )
    return row


def _apply_owner_ambiguity_hints(row: dict[str, Any]) -> None:
    flags = row.get("data_quality_flags")
    if not isinstance(flags, list):
        flags = []
    responsible = _normalize_text(str(row.get("responsible_user_name") or ""))
    conducted = _normalize_text(str(row.get("enriched_conducted_by") or ""))
    assigned = _normalize_text(str(row.get("enriched_assigned_by") or ""))
    if responsible and conducted and responsible != conducted:
        row["owner_ambiguity_flag"] = True
        flags.append("owner_ambiguity_responsible_mismatch")
    elif responsible and assigned and responsible != assigned:
        row["owner_ambiguity_flag"] = True
        flags.append("owner_ambiguity_assigned_by_mismatch")
    else:
        row.setdefault("owner_ambiguity_flag", False)
    row["data_quality_flags"] = _dedup(flags)


def _normalize_text(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _dedup(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _apply_client_fields(row: dict[str, Any], source_row: dict[str, str], fields_mapping: dict[str, dict[str, str]]) -> None:
    row["enriched_test_started"] = _read_mapped(source_row, "client_list", "test_started", fields_mapping)
    row["enriched_test_completed"] = _read_mapped(source_row, "client_list", "test_completed", fields_mapping)
    row["enriched_test_status"] = _read_mapped(source_row, "client_list", "test_status", fields_mapping)
    row["enriched_test_comments"] = _read_mapped(source_row, "client_list", "test_comments", fields_mapping)


def _apply_appointment_fields(row: dict[str, Any], source_row: dict[str, str], fields_mapping: dict[str, dict[str, str]]) -> None:
    row["enriched_appointment_date"] = _read_mapped(source_row, "appointment_list", "appointment_date", fields_mapping)
    row["enriched_assigned_by"] = _read_mapped(source_row, "appointment_list", "assigned_by", fields_mapping)
    row["enriched_conducted_by"] = _read_mapped(source_row, "appointment_list", "conducted_by", fields_mapping)
    row["enriched_meeting_status"] = _read_mapped(source_row, "appointment_list", "meeting_status", fields_mapping)
    row["enriched_transfer_cancel_flag"] = _read_mapped(source_row, "appointment_list", "transfer_cancel_flag", fields_mapping)


def _match_row(*, deal: dict[str, Any], source_rows: list[dict[str, str]], source_kind: str, fields_mapping: dict[str, dict[str, str]]) -> EnrichmentSourceMatch:
    if not source_rows:
        return EnrichmentSourceMatch(False, "no_source_rows", 0, "", {})

    deal_ids = _collect_deal_ids(deal)
    deal_phones = _collect_phones(deal)
    deal_emails = _collect_emails(deal)
    deal_company = _normalize_name(deal.get("company_name"))
    deal_contact = _normalize_name(deal.get("contact_name"))

    best: tuple[int, str, dict[str, str]] | None = None

    for src in source_rows:
        src_id = _clean_text(_read_mapped(src, source_kind, "deal_id", fields_mapping))
        src_phone = _normalize_phone(_read_mapped(src, source_kind, "phone", fields_mapping))
        src_email = _normalize_email(_read_mapped(src, source_kind, "email", fields_mapping))
        src_company = _normalize_name(_read_mapped(src, source_kind, "company_name", fields_mapping))
        src_contact = _normalize_name(_read_mapped(src, source_kind, "contact_name", fields_mapping))

        score = 0
        method = ""
        if src_id and src_id in deal_ids:
            score, method = 100, "deal_id"
        elif src_phone and src_phone in deal_phones:
            score, method = 90, "phone"
        elif src_email and src_email in deal_emails:
            score, method = 85, "email"
        elif src_company and src_contact and src_company == deal_company and src_contact == deal_contact:
            score, method = 70, "company_contact"
        elif src_company and src_company == deal_company:
            score, method = 55, "company"

        if score > 0 and (best is None or score > best[0]):
            best = (score, method, src)

    if not best:
        return EnrichmentSourceMatch(False, "none", 0, "", {})

    return EnrichmentSourceMatch(
        matched=True,
        method=best[1],
        confidence_score=best[0],
        row_id=str(best[2].get("__row_id", "")),
        row=best[2],
    )


def _collect_deal_ids(deal: dict[str, Any]) -> set[str]:
    values = {deal.get("deal_id"), deal.get("amo_lead_id")}
    out: set[str] = set()
    for value in values:
        text = _clean_text(value)
        if text:
            out.add(text)
    return out


def _collect_phones(deal: dict[str, Any]) -> set[str]:
    raw = deal.get("contact_phone")
    values: list[str] = []
    if isinstance(raw, list):
        values = [str(x) for x in raw]
    elif raw is not None:
        values = [str(raw)]
    out = {_normalize_phone(v) for v in values}
    return {x for x in out if x}


def _collect_emails(deal: dict[str, Any]) -> set[str]:
    raw = deal.get("contact_email")
    values: list[str] = []
    if isinstance(raw, list):
        values = [str(x) for x in raw]
    elif raw is not None:
        values = [str(raw)]
    out = {_normalize_email(v) for v in values}
    return {x for x in out if x}


def _read_mapped(
    source_row: dict[str, str],
    source_kind: str,
    logical_field: str,
    fields_mapping: dict[str, dict[str, str]],
) -> str:
    if not source_row:
        return ""

    configured = fields_mapping.get(source_kind, {}).get(logical_field, "")
    if configured:
        key = _normalize_header(configured)
        if key in source_row:
            return _clean_text(source_row.get(key, ""))

    for fallback in _DEFAULT_HEADER_FALLBACKS.get(source_kind, {}).get(logical_field, []):
        key = _normalize_header(fallback)
        if key in source_row and _clean_text(source_row.get(key, "")):
            return _clean_text(source_row.get(key, ""))
    return ""


def _normalize_header(value: Any) -> str:
    text = _clean_text(value).lower()
    for ch in ("\n", "\r", "\t", "/", "\\", "-", ":", ";", "(", ")", "[", "]", "{", "}"):
        text = text.replace(ch, " ")
    text = " ".join(text.split())
    return text


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_phone(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) >= 10:
        return digits[-10:]
    return digits


def _normalize_email(value: Any) -> str:
    return _clean_text(value).lower()


def _normalize_name(value: Any) -> str:
    text = _clean_text(value).lower().replace("ё", "е")
    return " ".join(text.split())



