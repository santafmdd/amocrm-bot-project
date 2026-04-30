from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WeekPlanSourceSnapshot:
    headers: list[str]
    rows: list[list[str]]
    header_row_number: int
    source_sheet_name: str
    spreadsheet_id: str


@dataclass
class WeekPlanSignalGroup:
    period_start: str
    period_end: str
    plan_week_start: str
    plan_week_end: str
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
    training_signal_count: int = 0
    criticality_histogram: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class WeekPlanDiscovery:
    workbook_name: str
    spreadsheet_id: str
    source_sheet_title: str
    target_sheet_title: str
    source_header_row: int
    target_header_row: int
