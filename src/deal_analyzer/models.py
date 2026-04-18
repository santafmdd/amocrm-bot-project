from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

AnalysisBackend = str


@dataclass(frozen=True)
class DealAnalysis:
    deal_id: int | None
    amo_lead_id: int | None
    deal_name: str
    score_0_100: int
    strong_sides: list[str]
    growth_zones: list[str]
    risk_flags: list[str]
    presentation_quality_flag: str
    followup_quality_flag: str
    data_completeness_flag: str
    recommended_actions_for_manager: list[str]
    recommended_training_tasks_for_employee: list[str]
    manager_message_draft: str
    employee_training_message_draft: str
    data_quality_flags: list[str] | None = None
    owner_ambiguity_flag: bool = False
    crm_hygiene_confidence: str = "medium"
    analysis_confidence: str = "medium"
    loss_reason_short: str = ""
    manager_insight_short: str = ""
    coaching_hint_short: str = ""
    llm_error: bool = False
    llm_fallback: bool = False
    analysis_backend_requested: str = "rules"
    analysis_backend_used: str = "rules"
    llm_repair_applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnalysisRunMetadata:
    executed_at: str
    period_mode_resolved: str
    period_start: str
    period_end: str
    public_period_label: str
    as_of_date: str
    llm_success_count: int | None = None
    llm_success_repaired_count: int | None = None
    llm_fallback_count: int | None = None
    llm_error_count: int | None = None
    backend_requested: str | None = None
    backend_effective_summary: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_public_dict(self, *, include_executed_at: bool) -> dict[str, Any]:
        payload = {
            "period_mode_resolved": self.period_mode_resolved,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "public_period_label": self.public_period_label,
            "as_of_date": self.as_of_date,
        }
        if include_executed_at:
            payload["executed_at"] = self.executed_at
        if self.llm_success_count is not None:
            payload["llm_success_count"] = self.llm_success_count
            payload["llm_success"] = self.llm_success_count
        if self.llm_success_repaired_count is not None:
            payload["llm_success_repaired_count"] = self.llm_success_repaired_count
            payload["llm_success_repaired"] = self.llm_success_repaired_count
        if self.llm_fallback_count is not None:
            payload["llm_fallback_count"] = self.llm_fallback_count
            payload["llm_fallback"] = self.llm_fallback_count
        if self.llm_error_count is not None:
            payload["llm_error_count"] = self.llm_error_count
            payload["llm_error"] = self.llm_error_count
        if self.backend_requested is not None:
            payload["backend_requested"] = self.backend_requested
        if self.backend_effective_summary is not None:
            payload["backend_effective_summary"] = self.backend_effective_summary
        return payload
