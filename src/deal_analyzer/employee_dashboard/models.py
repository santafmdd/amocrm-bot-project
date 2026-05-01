from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

EvidenceOutcome = Literal["success", "failure", "neutral"]


@dataclass(frozen=True)
class EvidenceItem:
    source: str
    employee_name: str
    role: str
    evidence_date: str
    text: str
    evidence_link: str
    category: str
    outcome: EvidenceOutcome = "neutral"
    confidence: float = 0.5
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SpeechModuleItem:
    phrase: str
    outcome: EvidenceOutcome
    source: str
    evidence_link: str
    evidence_date: str


@dataclass(frozen=True)
class ObjectionItem:
    objection: str
    outcome: EvidenceOutcome
    count: int
    evidence_links: tuple[str, ...]


@dataclass(frozen=True)
class EmployeeDashboardSummary:
    employee_name: str
    role: str
    period_start: str
    period_end: str
    strengths: tuple[str, ...]
    growth_zones: tuple[str, ...]
    successful_speech_modules: tuple[str, ...]
    failed_speech_modules: tuple[str, ...]
    objection_success: tuple[dict[str, Any], ...]
    objection_failures: tuple[dict[str, Any], ...]
    behavior_patterns: tuple[str, ...]
    recurring_mistakes: tuple[str, ...]
    recommended_training_topics: tuple[str, ...]
    evidence_links: tuple[str, ...]
    confidence_score: int
    evidence_count: int
    source_coverage: dict[str, int]
    source_coverage_passed: bool
