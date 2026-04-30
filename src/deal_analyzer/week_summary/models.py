from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WeekSummarySourceSnapshot:
    headers: list[str]
    rows: list[list[str]]
    header_row_number: int
    source_sheet_name: str
    spreadsheet_id: str


@dataclass
class WeekSummaryGroup:
    period_start: str
    period_end: str
    week_start: str
    week_end: str
    source_manager_rows: list[dict[str, Any]] = field(default_factory=list)
    source_plan_rows: list[dict[str, Any]] = field(default_factory=list)
    managers_count: int = 0
    deals_count: int = 0
    avg_score_0_100: int = 0
    planned_actions_total: int = 0
    done_actions_count: int = 0
    in_progress_actions_count: int = 0
    postponed_actions_count: int = 0
    no_status_actions_count: int = 0
    training_links: list[str] = field(default_factory=list)
    post_training_task_links: list[str] = field(default_factory=list)
    unresolved_actions: list[str] = field(default_factory=list)
