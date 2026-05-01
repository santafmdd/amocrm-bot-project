from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ClientListSheetSnapshot:
    spreadsheet_id: str
    sheet_name: str
    header_row_number: int
    headers: list[str]
    rows: list[list[str]]


@dataclass(frozen=True)
class ClientListRow:
    row_number: int
    manager_name: str
    client_name: str
    deal_name: str
    contact_name: str
    company_name: str
    status_text: str
    comment_text: str
    value_text: str
    value_amount: float | None
    next_step_text: str
    next_step_date: str
    risk_stalled: bool
    amocrm_link: str
    deal_link: str
    contact_link: str
    company_link: str
    deal_id: str
    contact_id: str
    company_id: str
    priority_category: str = ""
    priority_reason: str = ""
    priority_score: float = 0.0


@dataclass(frozen=True)
class ManagerClientContext:
    manager_name: str
    period_start: str
    period_end: str
    rows_total: int
    categories: dict[str, int]
    top_priority_items: list[dict[str, Any]]
    summary_lines: list[str]
    warnings: list[str] = field(default_factory=list)

