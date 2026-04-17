from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
