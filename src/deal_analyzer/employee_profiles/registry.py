from __future__ import annotations

from typing import Any

from .models import EmployeeProfile


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _norm(value: Any) -> str:
    return _clean(value).lower()


DEFAULT_EMPLOYEE_PROFILES: dict[str, EmployeeProfile] = {
    "рустам хомидов": EmployeeProfile(
        manager_name="Рустам Хомидов",
        communication_style="direct_accountability",
        motivators=("responsibility", "discipline", "visible_progress"),
        avoid=("soft_generic_advice",),
        role_hint="telemarketer",
        source="default",
    ),
    "илья бочков": EmployeeProfile(
        manager_name="Илья Бочков",
        communication_style="expert_to_expert",
        motivators=("commercial_effect", "autonomy", "professional_mastery"),
        avoid=("tool_for_tool_sake", "crm_moralizing"),
        role_hint="sales_manager",
        source="default",
    ),
}


def build_employee_profile_registry(raw_registry: dict[str, Any] | None) -> dict[str, EmployeeProfile]:
    registry: dict[str, EmployeeProfile] = {**DEFAULT_EMPLOYEE_PROFILES}
    if not isinstance(raw_registry, dict):
        return registry
    for raw_name, raw_payload in raw_registry.items():
        manager_name = _clean(raw_name)
        if not manager_name:
            continue
        if not isinstance(raw_payload, dict):
            continue
        communication_style = _clean(raw_payload.get("communication_style"))
        if not communication_style:
            continue
        motivators = tuple(_clean(item) for item in (raw_payload.get("motivators") or []) if _clean(item))
        avoid = tuple(_clean(item) for item in (raw_payload.get("avoid") or []) if _clean(item))
        role_hint = _clean(raw_payload.get("role_hint") or raw_payload.get("role") or "")
        registry[_norm(manager_name)] = EmployeeProfile(
            manager_name=manager_name,
            communication_style=communication_style,
            motivators=motivators,
            avoid=avoid,
            role_hint=role_hint,
            source="config",
        )
    return registry


def resolve_employee_profile(
    *,
    manager_name: str,
    manager_role_profile: str = "",
    registry: dict[str, EmployeeProfile] | None = None,
) -> EmployeeProfile:
    payload = registry or DEFAULT_EMPLOYEE_PROFILES
    probe_name = _norm(manager_name)
    if probe_name in payload:
        return payload[probe_name]

    for key, profile in payload.items():
        if key and (probe_name == key or probe_name in key or key in probe_name):
            return profile

    role_probe = _norm(manager_role_profile)
    if any(token in role_probe for token in ("телемаркет", "telemarketer", "cold", "верх")):
        return EmployeeProfile(
            manager_name=_clean(manager_name),
            communication_style="direct_accountability",
            motivators=("responsibility", "discipline", "visible_progress"),
            avoid=("soft_generic_advice",),
            role_hint="telemarketer",
            source="role_fallback",
        )
    if any(token in role_probe for token in ("продаж", "sales_manager", "closer", "account")):
        return EmployeeProfile(
            manager_name=_clean(manager_name),
            communication_style="expert_to_expert",
            motivators=("commercial_effect", "autonomy", "professional_mastery"),
            avoid=("tool_for_tool_sake", "crm_moralizing"),
            role_hint="sales_manager",
            source="role_fallback",
        )
    return EmployeeProfile(
        manager_name=_clean(manager_name),
        communication_style="balanced_managerial",
        motivators=("commercial_effect", "discipline"),
        avoid=("insults",),
        role_hint="",
        source="generic_fallback",
    )

