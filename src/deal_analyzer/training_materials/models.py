from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TrainingCandidate:
    row_number: int
    plan_week_start: str
    plan_week_end: str
    plan_date: str
    recipient: str
    manager_role_profile: str
    activity_type: str
    status: str
    what_i_do: str
    task_to_assign: str
    what_to_check: str
    daily_meeting_thesis: str
    expected_quantity_effect: str
    expected_quality_effect: str
    training_link: str
    post_training_task_link: str
    topic_hash: str
    idempotency_key: str


@dataclass
class TrainingDraft:
    candidate: TrainingCandidate
    training_title: str
    training_material: str
    task_title: str
    task_material: str
    analysis_backend_used: str
    llm_attempt_trace: list[dict[str, Any]] = field(default_factory=list)
    quality_metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceSnippet:
    source_type: str
    source: str
    text: str


@dataclass(frozen=True)
class SourceCoverage:
    style_sources_used: int
    speech_sources_used: int
    product_sources_used: int
    external_sources_used: bool
    external_sources_count: int
    external_source_titles: list[str]
    external_source_urls: list[str]
    external_source_fetch_errors: list[str]
    external_search_status: str
    warnings: list[str]
