from __future__ import annotations

from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers
from ..weekly_shared.sheets_discovery import resolve_spreadsheet_id
from .models import WeekSummarySourceSnapshot


WEEK_SUMMARY_MANAGER_ALIASES: dict[str, tuple[str, ...]] = {
    "week_start": ("Неделя с", "week_start"),
    "week_end": ("Неделя по", "week_end"),
    "manager_name": ("Менеджер", "manager_name"),
    "deals_count": ("Проанализировано сделок", "deals_count"),
    "avg_score_0_100": ("Средняя оценка 0-100", "avg_score_0_100"),
    "weekly_result": ("Итог недели", "weekly_result"),
}


WEEK_SUMMARY_PLAN_ALIASES: dict[str, tuple[str, ...]] = {
    "plan_week_start": ("План недели с", "Неделя с", "plan_week_start"),
    "plan_week_end": ("План недели по", "Неделя по", "plan_week_end"),
    "status": ("Статус", "status"),
    "training_link": ("Ссылка на обучение / материал", "Ссылка на обучение", "training_link"),
    "post_training_task_link": ("Ссылка на задачи после обучения", "post_training_task_link"),
    "what_i_do": ("Что делаю", "what_i_do"),
}


WEEK_SUMMARY_TARGET_ALIASES: dict[str, tuple[str, ...]] = {
    "week_start": ("Неделя с", "week_start"),
    "week_end": ("Неделя по", "week_end"),
    "brief_report": ("Краткий отчет за прошлую неделю", "brief_report"),
    "quantity_delta": ("Что изменилось количественно", "quantity_delta"),
    "quality_delta": ("Что изменилось качественно", "quality_delta"),
    "what_failed": ("Что не сработало", "what_failed"),
    "focus_next_week": ("Фокус следующей недели", "focus_next_week"),
    "next_week_plan": ("План следующей недели", "next_week_plan"),
    "meeting_message": ("Что говорю на еженедельном собрании", "meeting_message"),
    "strategic_accents": ("Около-стратегические акценты", "strategic_accents"),
    "risks": ("Риски", "risks"),
    "manager_report_phrase": ("Формулировка для руководителя", "manager_report_phrase"),
    "deals_count": ("Проанализировано сделок", "deals_count"),
}


def read_sheet_snapshot(
    *,
    cfg: Any,
    spreadsheet_id: str,
    sheet_name: str,
    logger: Any,
    end_col: str = "AZ",
) -> WeekSummarySourceSnapshot:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    matrix = client.get_values(spreadsheet_id, f"'{sheet_name}'!A1:{end_col}")
    if not matrix:
        return WeekSummarySourceSnapshot(
            headers=[],
            rows=[],
            header_row_number=1,
            source_sheet_name=sheet_name,
            spreadsheet_id=spreadsheet_id,
        )
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=3)
    header_idx = max(0, header_row_number - 1)
    headers = [clean_text(x) for x in matrix[header_idx]]
    rows = [list(map(clean_text, row)) for row in matrix[header_idx + 1 :]]
    return WeekSummarySourceSnapshot(
        headers=headers,
        rows=rows,
        header_row_number=header_row_number,
        source_sheet_name=sheet_name,
        spreadsheet_id=spreadsheet_id,
    )


def map_manager_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, WEEK_SUMMARY_MANAGER_ALIASES).mapped


def map_plan_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, WEEK_SUMMARY_PLAN_ALIASES).mapped


def map_target_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, WEEK_SUMMARY_TARGET_ALIASES).mapped


__all__ = [
    "WEEK_SUMMARY_MANAGER_ALIASES",
    "WEEK_SUMMARY_PLAN_ALIASES",
    "WEEK_SUMMARY_TARGET_ALIASES",
    "map_manager_headers",
    "map_plan_headers",
    "map_target_headers",
    "read_sheet_snapshot",
    "resolve_spreadsheet_id",
]
