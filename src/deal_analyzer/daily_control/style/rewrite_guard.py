from __future__ import annotations

import re
from typing import Any


PROTECTED_FIELDS_DAILY: tuple[str, ...] = (
    "period_start",
    "period_end",
    "control_day_date",
    "day_label",
    "manager_name",
    "manager_role_profile",
    "deals_count",
    "calls_count",
    "deal_ids",
    "deal_links",
    "product_mix",
    "base_mix",
    "score_0_100",
    "criticality",
)


def _clean(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _numbers(value: str) -> list[str]:
    return [m.group(0).replace(",", ".") for m in re.finditer(r"-?\d+[\.,]?\d*", str(value or ""))]


def _has_cjk(value: str) -> bool:
    return bool(re.search(r"[\u3400-\u9FFF]", value))


def _has_foreign_text(value: str) -> bool:
    words = re.findall(r"\b[a-z]{3,}\b", str(value or "").lower())
    allow = {"amocrm", "url", "http", "https", "api", "json", "id"}
    filtered = [w for w in words if w not in allow]
    return len(filtered) >= 2


def validate_rewrite_row(
    *,
    original: dict[str, Any],
    candidate: dict[str, Any],
    narrative_fields: tuple[str, ...],
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for field in PROTECTED_FIELDS_DAILY:
        if str(original.get(field, "")) != str(candidate.get(field, "")):
            errors.append(f"protected_field_changed:{field}")

    for field in narrative_fields:
        before = _clean(original.get(field, ""))
        after = _clean(candidate.get(field, ""))
        if not after:
            continue
        if _has_cjk(after):
            errors.append(f"cjk_text:{field}")
        if _has_foreign_text(after):
            errors.append(f"foreign_text:{field}")
        if "```" in after:
            errors.append(f"markdown_fence:{field}")
        if len(before) > 20 and len(after) > int(len(before) * 1.3):
            errors.append(f"length_growth_gt_30pct:{field}")
        if field == "expected_quant_impact" and _numbers(before):
            if _numbers(before) != _numbers(after):
                errors.append(f"numbers_changed:{field}")
    return (len(errors) == 0), errors
