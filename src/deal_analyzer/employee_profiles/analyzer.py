from __future__ import annotations

import re
from collections import Counter
from typing import Any

from .models import EmployeeBehaviorMarkers, EmployeeProfile
from .registry import build_employee_profile_registry, resolve_employee_profile


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _clean_multiline(value: Any) -> str:
    raw = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    out_lines: list[str] = []
    for line in raw.split("\n"):
        compact = " ".join(str(line).split()).strip()
        if compact:
            out_lines.append(compact)
        elif out_lines and out_lines[-1] != "":
            out_lines.append("")
    while out_lines and out_lines[0] == "":
        out_lines.pop(0)
    while out_lines and out_lines[-1] == "":
        out_lines.pop()
    return "\n".join(out_lines).strip()


def _split_semicolon_chunks(value: Any) -> list[str]:
    raw = _clean(value)
    if not raw:
        return []
    out: list[str] = []
    for chunk in re.split(r"[;|]", raw):
        text = _clean(chunk)
        if text:
            out.append(text)
    return out


_INSULT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\bидиот\w*\b", re.IGNORECASE),
    re.compile(r"\bдебил\w*\b", re.IGNORECASE),
    re.compile(r"\bтуп(ой|ая|ые|ой|ишь|о)\b", re.IGNORECASE),
    re.compile(r"\bмудак\w*\b", re.IGNORECASE),
    re.compile(r"\bлох\w*\b", re.IGNORECASE),
    re.compile(r"\bпизд\w*\b", re.IGNORECASE),
    re.compile(r"\bбляд\w*\b", re.IGNORECASE),
    re.compile(r"\bху[йяеё]\w*\b", re.IGNORECASE),
)


def sanitize_employee_text(value: Any, *, preserve_multiline: bool = False) -> str:
    text = _clean_multiline(value) if preserve_multiline else _clean(value)
    if not text:
        return ""
    sanitized = text
    for pattern in _INSULT_PATTERNS:
        sanitized = pattern.sub("неконструктивная формулировка", sanitized)
    if preserve_multiline:
        sanitized = re.sub(r"\n{3,}", "\n\n", sanitized).strip()
    else:
        sanitized = re.sub(r"\s{2,}", " ", sanitized).strip()
    return sanitized


def _profile_hint_for_field(*, profile: EmployeeProfile, field: str, date_hint: str = "") -> str:
    style = _clean(profile.communication_style).lower()
    if style == "direct_accountability":
        if field in {
            "what_to_tell_employee",
            "employee_message",
            "daily_meeting_thesis",
            "task_to_assign",
            "training_material",
            "task_material",
        }:
            suffix = f" Контроль срока: фиксируем факт выполнения в этот день {date_hint}.".strip()
            return suffix
        return ""
    if style == "expert_to_expert":
        if field in {
            "what_to_tell_employee",
            "employee_message",
            "daily_meeting_thesis",
            "task_to_assign",
            "manager_report_phrase",
            "training_material",
            "task_material",
        }:
            return " Коммерческий фокус: действие должно усилить конверсию и следующий оплачиваемый этап."
        return ""
    return ""


def apply_profile_to_row_fields(
    *,
    row: dict[str, Any],
    profile: EmployeeProfile,
    fields: tuple[str, ...],
    date_hint_field: str = "plan_date",
    preserve_multiline_fields: tuple[str, ...] = (),
) -> tuple[dict[str, Any], dict[str, Any]]:
    current = dict(row)
    changed_fields: list[str] = []
    replacements: list[dict[str, str]] = []
    date_hint = _clean(current.get(date_hint_field, ""))
    multiline_fields = {str(item) for item in preserve_multiline_fields}
    for field in fields:
        if field not in current:
            continue
        preserve_multiline = str(field) in multiline_fields
        before = _clean_multiline(current.get(field)) if preserve_multiline else _clean(current.get(field))
        if not before:
            continue
        sanitized = sanitize_employee_text(before, preserve_multiline=preserve_multiline)
        hint = _profile_hint_for_field(profile=profile, field=field, date_hint=date_hint)
        after = sanitized
        if hint and hint.lower() not in sanitized.lower():
            hint_text = _clean_multiline(hint) if preserve_multiline else _clean(hint)
            after = f"{sanitized}\n\n{hint_text}" if preserve_multiline else f"{sanitized}{hint_text}"
        after = _clean_multiline(after) if preserve_multiline else _clean(after)
        if after != before:
            changed_fields.append(field)
            replacements.append({"field": field, "before": before[:240], "after": after[:240]})
            current[field] = after
        else:
            current[field] = sanitized
    return current, {
        "manager_name": profile.manager_name,
        "communication_style": profile.communication_style,
        "changed_fields": changed_fields,
        "replacements": replacements,
    }


def _top_items(values: list[str], *, limit: int = 5) -> tuple[str, ...]:
    counter = Counter(_clean(item).lower() for item in values if _clean(item))
    ordered: list[str] = []
    for key, _count in counter.most_common(max(1, int(limit or 5))):
        for value in values:
            if _clean(value).lower() == key:
                ordered.append(_clean(value))
                break
    return tuple(ordered)


def build_behavior_markers(
    *,
    manager_name: str,
    source_rows: list[dict[str, Any]],
    profile: EmployeeProfile,
) -> EmployeeBehaviorMarkers:
    growth_raw: list[str] = []
    strong_raw: list[str] = []
    objections_bad_raw: list[str] = []
    objections_good_raw: list[str] = []
    pressure_tokens = {"stress_risk": 0, "control_loss": 0, "stable_control": 0}

    for row in source_rows:
        if not isinstance(row, dict):
            continue
        growth_raw.extend(_split_semicolon_chunks(row.get("growth_zones") or row.get("repeated_growth_zones")))
        strong_raw.extend(_split_semicolon_chunks(row.get("strong_sides") or row.get("repeated_strong_sides")))
        fix_text = _clean(row.get("what_to_fix") or row.get("repeated_fix_points"))
        if re.search(r"(возраж|сопротив|неактуаль|дорого|подумаем)", fix_text, flags=re.IGNORECASE):
            objections_bad_raw.append(fix_text)
        strong_text = _clean(row.get("strong_sides") or "")
        if re.search(r"(возраж|перехват инициативы|удерживает диалог)", strong_text, flags=re.IGNORECASE):
            objections_good_raw.append(strong_text)
        probe = " ".join(
            [
                _clean(row.get("what_to_tell_employee")),
                _clean(row.get("daily_meeting_thesis")),
                _clean(row.get("main_pattern")),
            ]
        ).lower()
        if any(token in probe for token in ("теряет", "срывает", "нет срока", "не фиксирует")):
            pressure_tokens["control_loss"] += 1
        if any(token in probe for token in ("дисциплин", "срок", "контрол")):
            pressure_tokens["stable_control"] += 1
        if any(token in probe for token in ("стресс", "давление", "проваливает")):
            pressure_tokens["stress_risk"] += 1

    preferred_behavior = "unknown"
    if pressure_tokens["stable_control"] > pressure_tokens["control_loss"]:
        preferred_behavior = "structure_and_control"
    elif pressure_tokens["control_loss"] > 0:
        preferred_behavior = "needs_hard_framework"

    if _clean(profile.communication_style).lower() == "expert_to_expert":
        coaching_response_style = "expert_to_expert"
    elif _clean(profile.communication_style).lower() == "direct_accountability":
        coaching_response_style = "direct_accountability"
    else:
        coaching_response_style = "balanced_managerial"

    return EmployeeBehaviorMarkers(
        manager_name=_clean(manager_name),
        repeated_growth_zones=_top_items(growth_raw),
        repeated_strong_sides=_top_items(strong_raw),
        repeated_objections_handled_badly=_top_items(objections_bad_raw),
        repeated_objections_handled_well=_top_items(objections_good_raw),
        preferred_behavior_pattern_under_pressure=preferred_behavior,
        coaching_response_style=coaching_response_style,
        source_rows_count=len([item for item in source_rows if isinstance(item, dict)]),
        extra={"pressure_tokens": str(pressure_tokens)},
    )


def build_employee_profile_context(
    *,
    manager_name: str,
    manager_role_profile: str = "",
    source_rows: list[dict[str, Any]] | None = None,
    registry_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registry = build_employee_profile_registry(registry_raw)
    profile = resolve_employee_profile(
        manager_name=manager_name,
        manager_role_profile=manager_role_profile,
        registry=registry,
    )
    markers = build_behavior_markers(
        manager_name=manager_name,
        source_rows=list(source_rows or []),
        profile=profile,
    )
    return {
        "manager_name": profile.manager_name,
        "communication_style": profile.communication_style,
        "motivators": list(profile.motivators),
        "avoid": list(profile.avoid),
        "role_hint": profile.role_hint,
        "profile_source": profile.source,
        "safeguards": {
            "no_insults": True,
            "no_humiliation": True,
            "direct_but_managerial_for_direct_accountability": True,
            "expert_and_commercial_for_expert_to_expert": True,
        },
        "behavior_markers": {
            "repeated_growth_zones": list(markers.repeated_growth_zones),
            "repeated_strong_sides": list(markers.repeated_strong_sides),
            "repeated_objections_handled_badly": list(markers.repeated_objections_handled_badly),
            "repeated_objections_handled_well": list(markers.repeated_objections_handled_well),
            "preferred_behavior_pattern_under_pressure": markers.preferred_behavior_pattern_under_pressure,
            "coaching_response_style": markers.coaching_response_style,
            "source_rows_count": markers.source_rows_count,
            "extra": dict(markers.extra),
        },
    }
