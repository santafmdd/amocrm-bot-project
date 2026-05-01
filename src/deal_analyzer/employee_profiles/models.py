from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EmployeeProfile:
    manager_name: str
    communication_style: str
    motivators: tuple[str, ...] = ()
    avoid: tuple[str, ...] = ()
    role_hint: str = ""
    source: str = "default"


@dataclass(frozen=True)
class EmployeeBehaviorMarkers:
    manager_name: str
    repeated_growth_zones: tuple[str, ...] = ()
    repeated_strong_sides: tuple[str, ...] = ()
    repeated_objections_handled_badly: tuple[str, ...] = ()
    repeated_objections_handled_well: tuple[str, ...] = ()
    preferred_behavior_pattern_under_pressure: str = "unknown"
    coaching_response_style: str = ""
    source_rows_count: int = 0
    extra: dict[str, str] = field(default_factory=dict)

