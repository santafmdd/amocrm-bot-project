"""Data models for amoCRM analytics browser reader MVP."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


SourceKind = Literal["tag", "utm_source"]
TabMode = Literal["all", "active", "closed"]
ParseMethod = Literal["analytics_text_structured", "dom", "fallback"]


class StageCount(BaseModel):
    """Single stage row with its numeric value."""

    stage_name: str = Field(min_length=1)
    count: int = Field(ge=0)


class AnalyticsSnapshot(BaseModel):
    """Structured analytics data captured from current amoCRM UI view."""

    source_kind: SourceKind
    filter_id: str = Field(min_length=1)
    tab_mode: TabMode
    read_at: datetime
    stages: list[StageCount]
    total_count: int = Field(ge=0)

    # Top cards are captured separately from the right panel list.
    top_cards: list[StageCount] = Field(default_factory=list)

    url: str
    export_name: str
    screenshot_path: str
    parse_method: ParseMethod
    debug_text_path: str
    debug_selectors_path: str
