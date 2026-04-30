from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WeeklySourceSnapshot:
    headers: list[str]
    rows: list[list[str]]
    header_row_number: int
    source_sheet_name: str
    spreadsheet_id: str


@dataclass
class WeeklyManagerGroup:
    period_start: str
    period_end: str
    week_start: str
    week_end: str
    manager_name: str
    manager_role_profile: str
    source_rows: list[dict[str, Any]] = field(default_factory=list)
    source_day_count: int = 0
    deals_count: int = 0
    calls_count: int = 0
    avg_score_0_100: int = 0
    deal_links: list[str] = field(default_factory=list)
    product_mix_week: str = ""
    base_mix_week: str = ""
    repeated_growth_zones: list[str] = field(default_factory=list)
    repeated_strong_sides: list[str] = field(default_factory=list)
    repeated_fix_points: list[str] = field(default_factory=list)
    repeated_messages: list[str] = field(default_factory=list)
    plan_actions_total: int = 0
    plan_done_count: int = 0
    plan_in_progress_count: int = 0
    plan_postponed_count: int = 0
    plan_no_status_count: int = 0
    plan_training_links: list[str] = field(default_factory=list)
    plan_post_training_task_links: list[str] = field(default_factory=list)
    plan_training_topics: list[str] = field(default_factory=list)
    plan_training_rows_found_count: int = 0
    plan_training_rows_used_count: int = 0
    plan_training_rows_used: list[dict[str, Any]] = field(default_factory=list)
    unresolved_plan_actions: list[str] = field(default_factory=list)
    analyzed_deals_count: int = 0
    analyzed_calls_count: int = 0
    quality_sample_size: int = 0
