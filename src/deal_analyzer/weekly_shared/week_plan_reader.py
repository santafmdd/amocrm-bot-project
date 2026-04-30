from __future__ import annotations

from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers, parse_date
from .models import SheetSnapshot


WEEK_PLAN_ALIASES: dict[str, tuple[str, ...]] = {
    "plan_week_start": ("План недели с", "Неделя с", "plan_week_start"),
    "plan_week_end": ("План недели по", "Неделя по", "plan_week_end"),
    "plan_date": ("Дата", "Дата действия", "plan_date"),
    "recipient": ("Адресат", "Сотрудник", "recipient"),
    "activity_type": ("Тип активности", "activity_type"),
    "status": ("Статус", "status"),
    "training_link": ("Ссылка на обучение / материал", "Ссылка на обучение", "training_link"),
    "post_training_task_link": ("Ссылка на задачи после обучения", "post_training_task_link"),
    "what_i_do": ("Что делаю", "what_i_do"),
    "daily_meeting_thesis": ("Общий тезис на дейлик", "daily_meeting_thesis"),
    "task_to_assign": ("Какую задачу даю", "task_to_assign"),
}


def read_week_plan_rows(
    *,
    cfg: Any,
    spreadsheet_id: str,
    sheet_name: str,
    logger: Any,
    end_col: str = "AZ",
) -> SheetSnapshot:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    matrix = client.get_values(spreadsheet_id, f"'{sheet_name}'!A1:{end_col}")
    if not matrix:
        return SheetSnapshot(headers=[], rows=[], header_row_number=1, sheet_name=sheet_name, spreadsheet_id=spreadsheet_id)
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=3)
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


def map_week_plan_headers(headers: list[str]) -> dict[str, int]:
    return map_headers(headers, WEEK_PLAN_ALIASES).mapped


def _is_training_activity(value: str) -> bool:
    probe = clean_text(value).lower()
    if not probe:
        return False
    if probe in {"обучение", "коучинг"}:
        return True
    return "обуч" in probe


def build_plan_fact_index(*, headers: list[str], rows: list[list[str]]) -> dict[str, dict[str, Any]]:
    mapped = map_week_plan_headers(headers)
    out: dict[str, dict[str, Any]] = {}

    def _pick(row: list[str], field: str) -> str:
        idx = mapped.get(field)
        if idx is None:
            return ""
        if idx < 0 or idx >= len(row):
            return ""
        return clean_text(row[idx])

    for row in rows:
        if not isinstance(row, list):
            continue
        week_start = parse_date(_pick(row, "plan_week_start"))
        week_end = parse_date(_pick(row, "plan_week_end"))
        recipient = _pick(row, "recipient")
        if not week_start or not week_end or not recipient:
            continue
        key = f"{week_start}|{week_end}|{recipient.lower()}"
        item = out.setdefault(
            key,
            {
                "plan_actions_total": 0,
                "plan_done_count": 0,
                "plan_in_progress_count": 0,
                "plan_postponed_count": 0,
                "plan_no_status_count": 0,
                "plan_training_links": [],
                "plan_post_training_task_links": [],
                "unresolved_actions": [],
                "training_rows_found_count": 0,
                "training_rows_used_count": 0,
                "training_rows_used": [],
                "plan_training_topics": [],
                "plan_training_status": "not_planned",
            },
        )
        item["plan_actions_total"] = int(item.get("plan_actions_total", 0) or 0) + 1

        status = _pick(row, "status").lower()
        activity_type = _pick(row, "activity_type")
        plan_date = parse_date(_pick(row, "plan_date"))
        action = _pick(row, "what_i_do")
        thesis = _pick(row, "daily_meeting_thesis")
        task_to_assign = _pick(row, "task_to_assign")

        if status in {"выполнено", "выполнена", "done", "completed"}:
            item["plan_done_count"] = int(item.get("plan_done_count", 0) or 0) + 1
        elif status in {"в работе", "в процессе", "in_progress", "in progress"}:
            item["plan_in_progress_count"] = int(item.get("plan_in_progress_count", 0) or 0) + 1
            if action:
                item["unresolved_actions"].append(action)
        elif status in {"перенесено", "перенесена", "postponed"}:
            item["plan_postponed_count"] = int(item.get("plan_postponed_count", 0) or 0) + 1
            if action:
                item["unresolved_actions"].append(action)
        else:
            item["plan_no_status_count"] = int(item.get("plan_no_status_count", 0) or 0) + 1

        train_link = _pick(row, "training_link")
        if train_link and train_link not in item["plan_training_links"]:
            item["plan_training_links"].append(train_link)
        task_link = _pick(row, "post_training_task_link")
        if task_link and task_link not in item["plan_post_training_task_links"]:
            item["plan_post_training_task_links"].append(task_link)

        if _is_training_activity(activity_type):
            item["training_rows_found_count"] = int(item.get("training_rows_found_count", 0) or 0) + 1
            # Important policy: training topic for weekly manager summary is taken only from week plan "Что делаю".
            training_topic = action
            if training_topic:
                item["training_rows_used_count"] = int(item.get("training_rows_used_count", 0) or 0) + 1
                if training_topic not in item["plan_training_topics"]:
                    item["plan_training_topics"].append(training_topic)
            if len(item["training_rows_used"]) < 50:
                item["training_rows_used"].append(
                    {
                        "plan_date": plan_date,
                        "activity_type": activity_type,
                        "topic": training_topic,
                        "training_link": train_link,
                        "post_training_task_link": task_link,
                        "status": status,
                        "task_to_assign": task_to_assign,
                        "daily_meeting_thesis": thesis,
                    }
                )
            item["plan_training_status"] = "planned"
    return out
