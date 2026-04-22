from __future__ import annotations

from typing import Any


CRM_REQUIRED_FIELDS = (
    "status_name",
    "pipeline_name",
    "responsible_user_name",
)


def build_crm_consistency_layer(
    *,
    crm: dict[str, Any] | None,
    analysis: dict[str, Any] | None,
) -> dict[str, Any]:
    deal = crm if isinstance(crm, dict) else {}
    out = analysis if isinstance(analysis, dict) else {}

    filled_fields: list[str] = []
    required_missing: list[str] = []
    crm_hygiene_flags: list[str] = []
    mismatch: list[str] = []

    for field in CRM_REQUIRED_FIELDS:
        value = str(deal.get(field) or "").strip()
        if value:
            filled_fields.append(field)
        else:
            required_missing.append(field)

    notes_count = _list_len(deal.get("notes_summary_raw"))
    tasks_count = _list_len(deal.get("tasks_summary_raw"))
    tags_count = _list_len(deal.get("tags"))
    product_values_count = _list_len(deal.get("product_values"))
    has_contact = bool(str(deal.get("contact_phone") or "").strip() or str(deal.get("contact_email") or "").strip())
    has_company = bool(str(deal.get("company_name") or "").strip() or str(deal.get("company_inn") or "").strip())

    if notes_count > 0:
        filled_fields.append("notes_summary_raw")
    if tasks_count > 0:
        filled_fields.append("tasks_summary_raw")
    if tags_count > 0:
        filled_fields.append("tags")
    if product_values_count > 0:
        filled_fields.append("product_values")
    if has_contact:
        filled_fields.append("contact")
    if has_company:
        filled_fields.append("company")

    if notes_count == 0 and tasks_count == 0:
        crm_hygiene_flags.append("crm_context_missing_notes_tasks")
    if not has_contact:
        crm_hygiene_flags.append("crm_contact_missing")
    if not has_company:
        crm_hygiene_flags.append("crm_company_missing")
    if product_values_count == 0:
        crm_hygiene_flags.append("crm_product_missing")

    transcript_available = bool(out.get("transcript_available"))
    has_call_summary = bool(str(out.get("call_signal_summary_short") or "").strip())
    call_signal_next_step = bool(out.get("call_signal_next_step_present"))
    call_signal_product = bool(out.get("call_signal_product_info") or out.get("call_signal_product_link"))
    call_objection_no_need = bool(out.get("call_signal_objection_no_need"))
    call_objection_not_target = bool(out.get("call_signal_objection_not_target"))

    status_norm = str(deal.get("status_name") or "").strip().lower()
    is_closed_lost = ("закрыт" in status_norm and "не реализ" in status_norm) or ("отказ" in status_norm)

    if transcript_available and has_call_summary and notes_count == 0:
        mismatch.append("call_context_present_but_notes_empty")
    if call_signal_next_step and tasks_count == 0:
        mismatch.append("next_step_in_call_missing_in_crm_tasks")
    if call_signal_product and product_values_count == 0:
        mismatch.append("product_signal_in_call_missing_in_crm_product")
    if (call_objection_no_need or call_objection_not_target) and not is_closed_lost:
        mismatch.append("loss_objection_in_call_but_status_not_closed_lost")

    # Score is only for debug confidence in this layer, not global deal score.
    score = 100
    score -= len(required_missing) * 12
    score -= len(crm_hygiene_flags) * 6
    score -= len(mismatch) * 9
    score = max(0, min(100, score))

    if mismatch:
        summary = (
            "По звонку и CRM есть расхождения: сначала подтянуть фиксацию фактов в CRM, "
            "потом использовать это в управленческом разборе."
        )
    elif required_missing or crm_hygiene_flags:
        summary = "CRM-контекст частично заполнен: для уверенного разбора не хватает части обязательных фиксаций."
    else:
        summary = "CRM заполнена достаточно ровно и не конфликтует со звонковыми сигналами."

    return {
        "crm_consistency_summary": summary,
        "crm_hygiene_flags": _dedup(crm_hygiene_flags),
        "crm_vs_call_mismatch": _dedup(mismatch),
        "crm_consistency_debug": {
            "filled_fields": _dedup(filled_fields),
            "required_fields": list(CRM_REQUIRED_FIELDS),
            "required_missing": _dedup(required_missing),
            "notes_count": notes_count,
            "tasks_count": tasks_count,
            "tags_count": tags_count,
            "product_values_count": product_values_count,
            "consistency_score_0_100": score,
            "call_signals_considered": {
                "transcript_available": transcript_available,
                "has_call_signal_summary": has_call_summary,
                "call_signal_next_step_present": call_signal_next_step,
                "call_signal_product_any": call_signal_product,
                "call_signal_objection_no_need": call_objection_no_need,
                "call_signal_objection_not_target": call_objection_not_target,
            },
        },
    }


def _list_len(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _dedup(items: list[str]) -> list[str]:
    out: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out

