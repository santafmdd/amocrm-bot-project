from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SheetSnapshot:
    headers: list[str]
    rows: list[list[str]]
    header_row_number: int
    sheet_name: str
    spreadsheet_id: str


@dataclass(frozen=True)
class RoksSelection:
    selected_current_month_sheet: str
    selected_previous_month_sheet: str
    selection_reason: str
    candidates: list[str]
    warnings: list[str]
