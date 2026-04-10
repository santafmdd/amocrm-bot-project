"""Writer models for compiled analytics result and destination config."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class CompiledProfileAnalyticsResult:
    """Compiled profile result ready for writing/export."""

    report_id: str
    display_name: str
    generated_at: datetime
    source_kind: str
    filter_values: list[str]
    tabs: list[str]
    top_cards_by_tab: dict[str, list[dict[str, Any]]]
    stages_by_tab: dict[str, list[dict[str, Any]]]
    totals_by_tab: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["generated_at"] = self.generated_at.isoformat()
        return payload


@dataclass(frozen=True)
class WriterDestinationConfig:
    """Destination settings for writer MVP and layout writer."""

    sheet_url: str
    tab_name: str
    write_mode: str = "overwrite_tab"
    start_cell: str = "A1"
    kind: str = "google_sheets_ui"
    layout_config: dict[str, Any] = field(default_factory=dict)
