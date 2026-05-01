from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _norm(value: Any) -> str:
    return _clean(value).lower()


_SALES_MANAGER_DEMO_METHODOLOGY: tuple[str, ...] = (
    "educational_demo",
    "guided_discovery",
    "client_hands_on",
    "soft_influence",
    "problem_based_demo",
    "next_step_commitment",
)

_DEMO_QUALITY_CHECKLIST: tuple[str, ...] = (
    "была ли выявлена задача клиента до показа",
    "было ли hands-on действие клиента",
    "показаны ли только релевантные функции",
    "был ли вопрос после каждого смыслового блока",
    "зафиксирован ли критерий успеха теста",
    "назначен ли следующий шаг",
)


@dataclass(frozen=True)
class RolePolicy:
    role: str
    allowed_primary_funnel_focus: tuple[str, ...]
    restricted_upper_funnel: tuple[str, ...]
    forbidden_main_task_patterns: tuple[str, ...]
    allowed_warm_exception_patterns: tuple[str, ...]
    max_upper_funnel_tasks_per_week: int
    demo_methodology: tuple[str, ...]
    demo_quality_checklist: tuple[str, ...]


DEFAULT_ROLE_POLICIES: dict[str, RolePolicy] = {
    "sales_manager": RolePolicy(
        role="sales_manager",
        allowed_primary_funnel_focus=(
            "inbound_leads",
            "warm_pipeline",
            "interest_to_demo",
            "demo_to_test",
            "test_to_invoice",
            "invoice_to_payment",
            "renewals",
            "reactivation",
            "crm_next_step_control",
            "client_problem_discovery",
            "decision_process",
            "proposal_followup",
        ),
        restricted_upper_funnel=(
            "cold_calling",
            "mass_lpr_discovery",
            "raw_database_calling",
            "наборы",
            "дозвоны",
            "cold_base_prospecting",
        ),
        forbidden_main_task_patterns=(
            "20 звонков по базе",
            "холодные звонки",
            "по базе с целью выявления лпр",
            "массовый обзвон",
            "наборы",
            "дозвоны",
            "прозвон базы",
            "массовый прозвон",
            "raw database",
            "cold base",
        ),
        allowed_warm_exception_patterns=(
            "по текущим сделкам",
            "по теплым сделкам",
            "по входящим заявкам",
            "по активным сделкам",
            "тепл",
            "входящ",
            "текущ",
            "активн",
        ),
        max_upper_funnel_tasks_per_week=1,
        demo_methodology=_SALES_MANAGER_DEMO_METHODOLOGY,
        demo_quality_checklist=_DEMO_QUALITY_CHECKLIST,
    ),
    "telemarketer": RolePolicy(
        role="telemarketer",
        allowed_primary_funnel_focus=(
            "cold_calling",
            "lpr_discovery",
            "interest_creation",
            "appointment_setting",
            "objection_handling",
            "crm_fixation_after_call",
            "base_quality",
            "contact_rate_improvement",
        ),
        restricted_upper_funnel=(),
        forbidden_main_task_patterns=(),
        allowed_warm_exception_patterns=(),
        max_upper_funnel_tasks_per_week=5,
        demo_methodology=("next_step_commitment",),
        demo_quality_checklist=_DEMO_QUALITY_CHECKLIST,
    ),
}


DEFAULT_MANAGER_ROLE_HINTS: dict[str, str] = {
    "илья бочков": "sales_manager",
    "рустам хомидов": "telemarketer",
}


def _role_from_text(role_text: str) -> str:
    probe = _norm(role_text)
    if not probe:
        return ""
    if any(token in probe for token in ("телемаркет", "telemarketer", "cold", "лидоген", "верх воронк")):
        return "telemarketer"
    if any(token in probe for token in ("sales_manager", "менеджер по продаж", "executive", "closer", "account")):
        return "sales_manager"
    return ""


def resolve_manager_role(
    *,
    manager_name: str,
    manager_role_profile: str,
    manager_role_registry: dict[str, str] | None = None,
    role_policy_registry: dict[str, dict[str, Any]] | None = None,
) -> str:
    by_profile = _role_from_text(manager_role_profile)
    if by_profile:
        return by_profile

    probe_name = _norm(manager_name)
    if role_policy_registry:
        for raw_name, policy in role_policy_registry.items():
            if _norm(raw_name) == probe_name and isinstance(policy, dict):
                by_cfg = _role_from_text(policy.get("role", ""))
                if by_cfg:
                    return by_cfg
    if manager_role_registry:
        for raw_name, raw_role in manager_role_registry.items():
            raw_name_norm = _norm(raw_name)
            if raw_name_norm and (probe_name == raw_name_norm or probe_name in raw_name_norm or raw_name_norm in probe_name):
                by_registry = _role_from_text(raw_role)
                if by_registry:
                    return by_registry
    if probe_name in DEFAULT_MANAGER_ROLE_HINTS:
        return DEFAULT_MANAGER_ROLE_HINTS[probe_name]
    if "бочков" in probe_name:
        return "sales_manager"
    if "хомидов" in probe_name:
        return "telemarketer"
    return "sales_manager"


def resolve_role_policy(
    *,
    manager_name: str,
    manager_role_profile: str,
    manager_role_registry: dict[str, str] | None = None,
    role_policy_registry: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    role = resolve_manager_role(
        manager_name=manager_name,
        manager_role_profile=manager_role_profile,
        manager_role_registry=manager_role_registry,
        role_policy_registry=role_policy_registry,
    )
    base = DEFAULT_ROLE_POLICIES.get(role, DEFAULT_ROLE_POLICIES["sales_manager"])
    policy: dict[str, Any] = {
        "role": base.role,
        "allowed_primary_funnel_focus": list(base.allowed_primary_funnel_focus),
        "restricted_upper_funnel": list(base.restricted_upper_funnel),
        "forbidden_main_task_patterns": list(base.forbidden_main_task_patterns),
        "allowed_warm_exception_patterns": list(base.allowed_warm_exception_patterns),
        "max_upper_funnel_tasks_per_week": int(base.max_upper_funnel_tasks_per_week),
        "demo_methodology": list(base.demo_methodology),
        "demo_quality_checklist": list(base.demo_quality_checklist),
    }

    probe_name = _norm(manager_name)
    cfg_node: dict[str, Any] = {}
    if role_policy_registry:
        for raw_name, raw_policy in role_policy_registry.items():
            if _norm(raw_name) == probe_name and isinstance(raw_policy, dict):
                cfg_node = raw_policy
                break
    if cfg_node:
        cfg_role = _role_from_text(cfg_node.get("role", ""))
        if cfg_role:
            policy["role"] = cfg_role
            cfg_default = DEFAULT_ROLE_POLICIES.get(cfg_role)
            if cfg_default:
                policy["allowed_primary_funnel_focus"] = list(cfg_default.allowed_primary_funnel_focus)
                policy["restricted_upper_funnel"] = list(cfg_default.restricted_upper_funnel)
                policy["forbidden_main_task_patterns"] = list(cfg_default.forbidden_main_task_patterns)
                policy["allowed_warm_exception_patterns"] = list(cfg_default.allowed_warm_exception_patterns)
                policy["max_upper_funnel_tasks_per_week"] = int(cfg_default.max_upper_funnel_tasks_per_week)
                policy["demo_methodology"] = list(cfg_default.demo_methodology)
                policy["demo_quality_checklist"] = list(cfg_default.demo_quality_checklist)

        for list_field in ("primary_funnel_scope", "allowed_primary_funnel_focus"):
            value = cfg_node.get(list_field)
            if isinstance(value, list) and value:
                policy["allowed_primary_funnel_focus"] = [_clean(item) for item in value if _clean(item)]
                break

        for list_field in ("restricted_funnel_scope", "restricted_upper_funnel"):
            value = cfg_node.get(list_field)
            if isinstance(value, list):
                policy["restricted_upper_funnel"] = [_clean(item) for item in value if _clean(item)]
                break

        for list_field in ("demo_methodology", "sales_demo_methodology"):
            value = cfg_node.get(list_field)
            if isinstance(value, list):
                normalized = [_clean(item) for item in value if _clean(item)]
                if normalized:
                    policy["demo_methodology"] = normalized
                    break

        for list_field in ("demo_quality_checklist",):
            value = cfg_node.get(list_field)
            if isinstance(value, list):
                normalized = [_clean(item) for item in value if _clean(item)]
                if normalized:
                    policy["demo_quality_checklist"] = normalized
                    break

        max_upper = cfg_node.get("max_upper_funnel_tasks_per_week")
        try:
            if max_upper is not None:
                policy["max_upper_funnel_tasks_per_week"] = max(0, int(max_upper))
        except Exception:
            pass

    return policy


def contains_forbidden_upper_funnel_for_sales_manager(*, text: str, policy: dict[str, Any]) -> tuple[bool, str]:
    if _norm(policy.get("role")) != "sales_manager":
        return False, ""
    probe = _norm(text)
    if not probe:
        return False, ""

    warm_markers = [str(item).lower() for item in policy.get("allowed_warm_exception_patterns", []) if _clean(item)]
    has_warm_exception = any(marker in probe for marker in warm_markers)
    hard_markers = [str(item).lower() for item in policy.get("forbidden_main_task_patterns", []) if _clean(item)]

    for marker in hard_markers:
        if marker and marker in probe:
            if has_warm_exception and marker in {"наборы", "дозвоны"}:
                continue
            return True, marker

    soft_patterns = (
        r"\b\d{1,3}\s+звонк\w*\s+по\s+баз",
        r"массов\w*\s+обзвон",
        r"прозвон\w*\s+баз",
    )
    for raw_pattern in soft_patterns:
        if re.search(raw_pattern, probe, flags=re.IGNORECASE):
            if has_warm_exception:
                continue
            return True, raw_pattern
    return False, ""


def is_sales_manager(role: str) -> bool:
    return _role_from_text(role) == "sales_manager"


def is_telemarketer(role: str) -> bool:
    return _role_from_text(role) == "telemarketer"


def demo_quality_checklist(policy: dict[str, Any] | None = None) -> list[str]:
    if isinstance(policy, dict):
        items = policy.get("demo_quality_checklist")
        if isinstance(items, list):
            normalized = [_clean(item) for item in items if _clean(item)]
            if normalized:
                return normalized
    return list(_DEMO_QUALITY_CHECKLIST)
