from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class IdempotencyKey:
    period_start: str
    period_end: str
    control_day_date: str
    manager_name: str
    source_deals_count: int
    source_calls_count: int

    def as_string(self) -> str:
        return "|".join(
            [
                str(self.period_start or "").strip(),
                str(self.period_end or "").strip(),
                str(self.control_day_date or "").strip(),
                str(self.manager_name or "").strip(),
                str(int(self.source_deals_count or 0)),
                str(int(self.source_calls_count or 0)),
            ]
        )


@dataclass
class DailyControlInputGroup:
    period_start: str
    period_end: str
    control_day_date: str
    day_label: str
    manager_name: str
    manager_role_profile: str
    source_rows: list[dict[str, Any]] = field(default_factory=list)
    sample_size: int = 0
    deals_count: int = 0
    calls_count: int = 0
    deal_ids: list[str] = field(default_factory=list)
    deal_names: list[str] = field(default_factory=list)
    deal_links: list[str] = field(default_factory=list)
    product_mix: str = ""
    base_mix: str = ""
    insights: dict[str, list[str]] = field(default_factory=dict)
    discipline_signals: dict[str, Any] = field(default_factory=dict)


@dataclass
class DailyControlPayload:
    mode: str
    period_start: str
    period_end: str
    source_sheet: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    rows_count: int = 0
    columns: list[str] = field(default_factory=list)
    llm_runtime: dict[str, Any] = field(default_factory=dict)


@dataclass
class DailyControlRunMeta:
    run_id: str
    run_dir: str
    source_sheet: str
    daily_sheet: str
    period_start: str
    period_end: str


@dataclass
class DailyControlWriterPlan:
    payload: dict[str, Any]


@dataclass
class DailyControlLintResult:
    payload: dict[str, Any]


@dataclass
class DailyControlStyleResult:
    rows: list[dict[str, Any]]
    metrics: dict[str, Any] = field(default_factory=dict)
    style_dir: str = ""


@dataclass
class RoksOapSnapshot:
    payload: dict[str, Any]
