from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient


SOURCE_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "case_date": ("дата кейса", "дата звонка", "дата"),
    "manager": ("менеджер", "сотрудник"),
    "role": ("роль",),
    "deal_id": ("deal id", "id сделки", "сделка id"),
    "deal_link": ("ссылка на сделку", "ссылка"),
    "deal_name": ("сделка", "название сделки"),
    "company": ("компания",),
    "product_focus": ("продукт / фокус", "продукт", "фокус"),
    "base_tag": ("база / тег", "база", "тег"),
    "case_type": ("тип кейса",),
    "listened_calls": ("прослушанные звонки",),
    "key_takeaway": ("ключевой вывод",),
    "strong": ("сильная сторона", "сильные стороны"),
    "growth": ("зона роста", "зоны роста"),
    "why_important": ("почему это важно", "почему важно"),
    "reinforce": ("что закрепить",),
    "fix": ("что исправить",),
    "tell_employee": ("что донести сотруднику", "что донес сотруднику"),
    "expected_qty": ("эффект количество / неделя", "ожидаемый эффект - количество", "эффект количество"),
    "expected_quality": ("эффект качество", "ожидаемый эффект - качество", "эффект - качество"),
    "score": ("оценка 0-100", "оценка"),
    "criticality": ("критичность",),
}

DAILY_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "period_start": ("начало периода", "period_start", "неделя с", "период с"),
    "period_end": ("конец периода", "period_end", "неделя по", "период по"),
    "control_day_date": ("дата", "день контроля", "дата контроля", "control_day_date"),
    "day_label": ("день", "день недели", "day_label"),
    "manager_name": ("менеджер", "сотрудник", "manager_name"),
    "manager_role_profile": ("роль", "роль менеджера", "профиль роли", "manager_role_profile"),
    "sample_size": ("sample_size", "разборов", "объем выборки", "размер выборки"),
    "deals_count": ("количество сделок", "проанализировано сделок", "deals_count", "сделки"),
    "calls_count": ("количество звонков", "calls_count", "звонки"),
    "deal_ids": ("deal_ids", "id сделок", "список deal id"),
    "deal_links": ("ссылки на сделки", "deal_links", "ссылки"),
    "product_mix": ("продукт / фокус", "product_mix", "продукт микс"),
    "base_mix": ("база микс", "база / тег", "base_mix"),
    "main_pattern": ("основной паттерн", "main_pattern", "ключевой вывод", "главный паттерн"),
    "strong_sides": ("сильные стороны", "сильная сторона", "strong_sides"),
    "growth_zones": ("зоны роста", "зона роста", "growth_zones"),
    "why_it_matters": ("почему это важно", "why_it_matters"),
    "what_to_reinforce": ("что закрепить", "what_to_reinforce"),
    "what_to_fix": ("что исправить", "what_to_fix"),
    "what_to_tell_employee": ("что донести сотруднику", "что донес сотруднику", "what_to_tell_employee"),
    "expected_quant_impact": ("ожидаемый эффект количество", "эффект количество / неделя", "expected_quant_impact"),
    "expected_qual_impact": ("ожидаемый эффект качество", "эффект качество", "expected_qual_impact"),
    "score_0_100": ("оценка 0-100", "оценка", "score_0_100"),
    "criticality": ("критичность",),
    "analysis_backend_used": ("analysis_backend_used", "backend", "источник анализа"),
    "source_run_id": ("source_run_id", "source_hash", "source"),
}

RUSSIAN_WEEKDAY_LABELS: tuple[str, ...] = (
    "понедельник",
    "вторник",
    "среда",
    "четверг",
    "пятница",
    "суббота",
    "воскресенье",
)


@dataclass(frozen=True)
class HeaderMappingResult:
    mapped: dict[str, int]
    unmapped_columns: list[str]


@dataclass(frozen=True)
class SourceSheetSnapshot:
    headers: list[str]
    rows: list[list[str]]
    header_row_number: int
    source_sheet_name: str
    spreadsheet_id: str


def clean_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def norm_text(value: Any) -> str:
    return " ".join(re.sub(r"[^0-9a-zа-яё/ ]+", " ", str(value or "").lower().replace("ё", "е")).split())


def parse_date(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    for pattern in (r"^(\d{4})-(\d{2})-(\d{2})", r"^(\d{2})\.(\d{2})\.(\d{4})"):
        m = re.search(pattern, text)
        if not m:
            continue
        try:
            if pattern.startswith(r"^(\d{4})"):
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            else:
                y, mo, d = int(m.group(3)), int(m.group(2)), int(m.group(1))
            from datetime import date as _date

            return _date(y, mo, d).isoformat()
        except Exception:
            continue
    return ""


def extract_date_from_listened_calls(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return ""


def parse_listened_calls_count(value: str) -> tuple[int, str]:
    text = clean_text(value)
    if not text:
        return 0, "none"
    chunks = [chunk.strip() for chunk in text.split(";") if chunk.strip()]
    if not chunks:
        return 0, "none"

    parsed = 0
    for chunk in chunks:
        if re.search(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s*-\s*\d{2}:\d{2}", chunk):
            parsed += 1
            continue
        if re.search(r"\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}\s*-\s*\d{2}:\d{2}", chunk):
            parsed += 1

    if parsed == len(chunks):
        return parsed, "high"
    if parsed > 0:
        return parsed, "medium"
    return len(chunks), "low"


def detect_header_row(matrix: list[list[str]], *, start_row: int = 1, min_nonempty: int = 5) -> int:
    for offset, row in enumerate(matrix):
        norm_row = [norm_text(cell) for cell in row]
        joined = " ".join(norm_row)
        if ("deal id" in joined or "id сделки" in joined or "сделка id" in joined) and "менеджер" in joined:
            if "дата кейса" in joined or "дата" in joined:
                return start_row + offset
    for offset, row in enumerate(matrix):
        nonempty = sum(1 for cell in row if clean_text(cell))
        if nonempty >= min_nonempty:
            return start_row + offset
    return 1


def map_headers(headers: list[str], aliases: dict[str, tuple[str, ...]]) -> HeaderMappingResult:
    normalized_headers = [norm_text(h) for h in headers]
    mapped: dict[str, int] = {}
    used_indexes: set[int] = set()

    for field, variants in aliases.items():
        candidates = [norm_text(v) for v in variants if norm_text(v)]
        selected_idx = -1

        for idx, h_norm in enumerate(normalized_headers):
            if idx in used_indexes:
                continue
            if h_norm in candidates:
                selected_idx = idx
                break

        if selected_idx < 0:
            for idx, h_norm in enumerate(normalized_headers):
                if idx in used_indexes:
                    continue
                if any(candidate and candidate in h_norm for candidate in candidates):
                    selected_idx = idx
                    break

        if selected_idx >= 0:
            mapped[field] = selected_idx
            used_indexes.add(selected_idx)

    unmapped_columns = [headers[idx] for idx in range(len(headers)) if idx not in used_indexes and clean_text(headers[idx])]
    return HeaderMappingResult(mapped=mapped, unmapped_columns=unmapped_columns)


def row_to_dict(headers: list[str], row: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for idx, header in enumerate(headers):
        key = clean_text(header)
        if not key:
            continue
        out[key] = clean_text(row[idx] if idx < len(row) else "")
    return out


def pick_by_mapping(row: list[str], mapping: dict[str, int], field: str) -> str:
    idx = mapping.get(field, -1)
    if idx < 0 or idx >= len(row):
        return ""
    return clean_text(row[idx])


def day_label_from_iso(value: str) -> str:
    from datetime import date as _date

    parsed = parse_date(value)
    if not parsed:
        return ""
    try:
        dt = _date.fromisoformat(parsed)
    except Exception:
        return ""
    weekday = int(dt.weekday())
    if 0 <= weekday < len(RUSSIAN_WEEKDAY_LABELS):
        return RUSSIAN_WEEKDAY_LABELS[weekday]
    return ""


def read_call_review_source(
    *,
    cfg: Any,
    spreadsheet_id: str,
    source_sheet_name: str,
    logger: Any,
) -> SourceSheetSnapshot:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    matrix = client.get_values(spreadsheet_id, f"'{source_sheet_name}'!A1:ZZ")
    if not matrix:
        raise RuntimeError(f"Source sheet is empty: {source_sheet_name}")
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=5)
    header_idx = max(0, header_row_number - 1)
    headers = [clean_text(x) for x in matrix[header_idx]]
    rows = [list(map(clean_text, row)) for row in matrix[header_idx + 1 :]]
    return SourceSheetSnapshot(
        headers=headers,
        rows=rows,
        header_row_number=header_row_number,
        source_sheet_name=source_sheet_name,
        spreadsheet_id=spreadsheet_id,
    )
