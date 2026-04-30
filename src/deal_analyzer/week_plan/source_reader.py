from __future__ import annotations

from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id

from ..config import DealAnalyzerConfig
from ..daily_control.source_reader import clean_text, detect_header_row, map_headers
from .models import WeekPlanSourceSnapshot


WEEK_PLAN_SOURCE_ALIASES: dict[str, tuple[str, ...]] = {
    "week_start": ("Неделя с", "week_start"),
    "week_end": ("Неделя по", "week_end"),
    "control_day_date": ("Дата", "Дата контроля", "День контроля", "control_day_date"),
    "day_label": ("День", "День недели", "day_label"),
    "manager_name": ("Менеджер", "Сотрудник", "manager_name"),
    "manager_role_profile": ("Роль менеджера", "Роль", "manager_role_profile"),
    "deals_count": ("Проанализировано сделок", "Количество сделок", "deals_count"),
    "calls_count": ("Количество звонков", "calls_count"),
    "deal_ids": ("deal_ids", "ID сделок", "Deal ID", "deal id"),
    "deal_links": ("Ссылки на сделки", "deal_links", "Ссылка на сделку"),
    "product_mix": ("Продукт / фокус", "Продукт", "product_mix"),
    "base_mix": ("База микс", "База / тег", "base_mix"),
    "main_pattern": ("Ключевой вывод", "main_pattern"),
    "strong_sides": ("Сильные стороны", "Сильная сторона", "strong_sides"),
    "growth_zones": ("Зоны роста", "Зона роста", "growth_zones"),
    "why_it_matters": ("Почему это важно", "why_it_matters"),
    "what_to_reinforce": ("Что закрепить", "what_to_reinforce"),
    "what_to_fix": ("Что исправить", "what_to_fix"),
    "what_to_tell_employee": ("Что донес сотруднику", "Что донести сотруднику", "what_to_tell_employee"),
    "expected_quant_impact": ("Ожидаемый эффект - количество", "Эффект количество / неделя", "expected_quant_impact"),
    "expected_qual_impact": ("Ожидаемый эффект - качество", "Эффект качество", "expected_qual_impact"),
    "score_0_100": ("Оценка 0-100", "score_0_100"),
    "criticality": ("Критичность",),
}


WEEK_PLAN_TARGET_ALIASES: dict[str, tuple[str, ...]] = {
    "plan_week_start": ("План недели с", "Неделя с", "plan_week_start"),
    "plan_week_end": ("План недели по", "Неделя по", "plan_week_end"),
    "plan_date": ("Дата", "Дата действия", "plan_date"),
    "day_label": ("День", "day_label"),
    "recipient": ("Адресат", "Сотрудник", "recipient"),
    "manager_role_profile": ("Роль менеджера", "Роль", "manager_role_profile"),
    "activity_type": ("Тип активности", "activity_type"),
    "priority": ("Приоритет", "priority"),
    "what_i_do": ("Что делаю", "what_i_do"),
    "task_to_assign": ("Какую задачу даю", "task_to_assign"),
    "what_to_check": ("Что проверяю", "what_to_check"),
    "daily_meeting_thesis": ("Общий тезис на дейлик", "daily_meeting_thesis"),
    "training_link": ("Ссылка на обучение / материал", "Ссылка на обучение", "training_link"),
    "post_training_task_link": ("Ссылка на задачи после обучения", "post_training_task_link"),
    "expected_quantity_effect": ("Ожидаемый эффект - количество", "expected_quantity_effect"),
    "expected_quality_effect": ("Ожидаемый эффект - качество", "expected_quality_effect"),
    "status": ("Статус", "status"),
    "source_deals_count": ("Проанализировано сделок", "source_deals_count"),
    "source_calls_count": ("Количество звонков", "source_calls_count"),
    "idempotency_key": ("idempotency_key", "Ключ идемпотентности"),
}


def resolve_spreadsheet_id(cfg: DealAnalyzerConfig) -> str:
    if str(cfg.deal_analyzer_spreadsheet_id or "").strip():
        return str(cfg.deal_analyzer_spreadsheet_id).strip()
    if str(cfg.deal_analyzer_sheet_url or "").strip():
        return extract_spreadsheet_id(str(cfg.deal_analyzer_sheet_url).strip())
    raise RuntimeError("deal_analyzer_spreadsheet_id/deal_analyzer_sheet_url is not set in config")


def read_daily_control_source(
    *,
    cfg: Any,
    spreadsheet_id: str,
    source_sheet_name: str,
    logger: Any,
) -> WeekPlanSourceSnapshot:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    matrix = client.get_values(spreadsheet_id, f"'{source_sheet_name}'!A1:ZZ")
    if not matrix:
        raise RuntimeError(f"Source sheet is empty: {source_sheet_name}")
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=5)
    header_idx = max(0, header_row_number - 1)
    headers = [clean_text(x) for x in matrix[header_idx]]
    rows = [list(map(clean_text, row)) for row in matrix[header_idx + 1 :]]
    return WeekPlanSourceSnapshot(
        headers=headers,
        rows=rows,
        header_row_number=header_row_number,
        source_sheet_name=source_sheet_name,
        spreadsheet_id=spreadsheet_id,
    )


def map_source_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, WEEK_PLAN_SOURCE_ALIASES).mapped


def map_target_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, WEEK_PLAN_TARGET_ALIASES).mapped
