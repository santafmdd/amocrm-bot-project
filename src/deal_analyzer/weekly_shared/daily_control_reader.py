from __future__ import annotations

from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers
from .models import SheetSnapshot


DAILY_CONTROL_WEEKLY_ALIASES: dict[str, tuple[str, ...]] = {
    "week_start": ("Неделя с", "week_start"),
    "week_end": ("Неделя по", "week_end"),
    "control_day_date": ("Дата", "Дата контроля", "control_day_date"),
    "day_label": ("День", "День недели", "day_label"),
    "manager_name": ("Менеджер", "Сотрудник", "manager_name"),
    "manager_role_profile": ("Роль менеджера", "Роль", "manager_role_profile"),
    "deals_count": ("Проанализировано сделок", "Количество сделок", "deals_count"),
    "calls_count": ("Количество звонков", "calls_count"),
    "deal_ids": ("deal_ids", "ID сделок"),
    "deal_links": ("Ссылки на сделки", "deal_links"),
    "product_mix": ("Продукт / фокус", "Продукт", "product_mix"),
    "base_mix": ("База микс", "База / тег", "base_mix"),
    "main_pattern": ("Ключевой вывод", "main_pattern"),
    "strong_sides": ("Сильные стороны", "strong_sides"),
    "growth_zones": ("Зоны роста", "growth_zones"),
    "what_to_fix": ("Что исправить", "what_to_fix"),
    "what_to_tell_employee": ("Что донес сотруднику", "Что донести сотруднику", "what_to_tell_employee"),
    "expected_quant_impact": ("Ожидаемый эффект - количество", "expected_quant_impact"),
    "expected_qual_impact": ("Ожидаемый эффект - качество", "expected_qual_impact"),
    "score_0_100": ("Оценка 0-100", "score_0_100"),
    "criticality": ("Критичность",),
}


def read_daily_control_rows(
    *,
    cfg: Any,
    spreadsheet_id: str,
    sheet_name: str,
    logger: Any,
    end_col: str = "ZZ",
) -> SheetSnapshot:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    matrix = client.get_values(spreadsheet_id, f"'{sheet_name}'!A1:{end_col}")
    if not matrix:
        return SheetSnapshot(headers=[], rows=[], header_row_number=1, sheet_name=sheet_name, spreadsheet_id=spreadsheet_id)
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=5)
    header_index = max(0, header_row_number - 1)
    headers = [clean_text(item) for item in matrix[header_index]]
    rows = [list(map(clean_text, row)) for row in matrix[header_index + 1 :]]
    return SheetSnapshot(
        headers=headers,
        rows=rows,
        header_row_number=header_row_number,
        sheet_name=sheet_name,
        spreadsheet_id=spreadsheet_id,
    )


def map_daily_control_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, DAILY_CONTROL_WEEKLY_ALIASES).mapped
