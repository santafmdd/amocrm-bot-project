from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from .base_mix import resolve_base_mix
from .call_review_v3_row_schema import CALL_REVIEW_V3_COLUMNS

MSK_TZ = timezone(timedelta(hours=3))


CASE_MODE_DISPLAY_MAP = {
    "negotiation_lpr_analysis": "разговор с лпр",
    "secretary_analysis": "разговор с секретарем",
    "supplier_inbound_analysis": "входящий от поставщика",
    "warm_inbound_analysis": "теплый входящий",
    "presentation_analysis": "презентация",
    "test_analysis": "работа с тестом",
    "dozhim_analysis": "дожим",
    "redial_discipline_analysis": "недозвоны / дисциплина дозвонов",
}

ROLE_DISPLAY_MAP = {
    "telemarketer": "телемаркетолог",
    "sales_manager": "менеджер по продажам",
    "телемаркетолог": "телемаркетолог",
    "менеджер по продажам": "менеджер по продажам",
}

DISALLOWED_CASES_FOR_ACTIVE_WRITE = {
    "skip_no_meaningful_case",
    "redial_discipline_analysis",
}


def build_call_review_v3_payload(
    *,
    summary: dict[str, Any],
    period_deal_records: list[dict[str, Any]],
    analysis_shortlist_payload: dict[str, Any],
    base_domain: str,
    manager_allowlist: list[str] | tuple[str, ...] | None,
    manager_role_registry: dict[str, str] | None,
    run_dir: Path | None = None,
) -> dict[str, Any]:
    records_by_deal = {
        str(item.get("deal_id") or "").strip(): item
        for item in period_deal_records
        if isinstance(item, dict) and str(item.get("deal_id") or "").strip()
    }
    shortlist_items = (
        analysis_shortlist_payload.get("selected_items", [])
        if isinstance(analysis_shortlist_payload.get("selected_items"), list)
        else []
    )
    allowlist = [str(x or "").strip() for x in (manager_allowlist or []) if str(x or "").strip()]

    ledger: list[dict[str, Any]] = []
    cases_debug: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    skipped_reasons: dict[str, int] = {}

    for order, candidate in enumerate(shortlist_items, start=1):
        if not isinstance(candidate, dict):
            continue
        deal_id = str(candidate.get("deal_id") or "").strip()
        if not deal_id:
            _bump(skipped_reasons, "missing_deal_id")
            continue
        record = records_by_deal.get(deal_id)
        if not isinstance(record, dict):
            _bump(skipped_reasons, "deal_not_found_in_period_records")
            continue
        case = _build_case(
            candidate=candidate,
            record=record,
            base_domain=base_domain,
            allowlist=allowlist,
            manager_role_registry=manager_role_registry or {},
            ledger_out=ledger,
        )
        case_debug = {
            "order": order,
            "deal_id": deal_id,
            "pool_type": str(candidate.get("pool_type") or ""),
            "call_case_type": str(candidate.get("call_case_type") or ""),
            "shortlist_reason": str(candidate.get("shortlist_reason") or ""),
            "selected_for_transcription": bool(candidate.get("selected_for_transcription")),
            "selected_call_count": int(candidate.get("selected_call_count", 0) or 0),
            "analysis_shortlist_rank_group": int(candidate.get("rank_group", 0) or 0),
        }
        if not isinstance(case, dict):
            reason = str(case) if isinstance(case, str) else "case_build_failed"
            _bump(skipped_reasons, reason)
            case_debug["skipped"] = True
            case_debug["skip_reason"] = reason
            cases_debug.append(case_debug)
            continue

        row = _build_row_from_case(case=case)
        rows.append(row)
        case_debug["skipped"] = False
        case_debug["manager_name"] = case.get("manager_name", "")
        case_debug["role"] = case.get("manager_role_display", "")
        case_debug["case_mode"] = case.get("case_mode", "")
        case_debug["case_type_display"] = case.get("case_type_display", "")
        case_debug["anchor_call_id"] = case.get("anchor_call_id", "")
        case_debug["anchor_call_timestamp"] = case.get("anchor_call_timestamp", "")
        case_debug["anchor_call_duration_seconds"] = int(case.get("anchor_call_duration_seconds", 0) or 0)
        case_debug["llm_source"] = case.get("llm_source", "")
        case_debug["llm_ready"] = True
        cases_debug.append(case_debug)

    rows.sort(
        key=lambda x: (
            str(x.get("Дата анализа") or ""),
            str(x.get("Менеджер") or ""),
            str(x.get("Дата кейса") or ""),
            str(x.get("Deal ID") or ""),
        )
    )

    artifacts = _write_debug_artifacts(
        run_dir=run_dir,
        ledger=ledger,
        cases_debug=cases_debug,
        rows=rows,
        skipped_reasons=skipped_reasons,
    )

    return {
        "mode": "call_review_sheet",
        "schema_version": "v3",
        "sheet_name": "Разбор звонков",
        "start_cell": "A2",
        "columns": list(CALL_REVIEW_V3_COLUMNS),
        "rows": rows,
        "rows_count": len(rows),
        "selection_debug": {
            "rows_total": len(rows),
            "rows_skipped": int(sum(skipped_reasons.values())),
            "skip_reasons": skipped_reasons,
            "details": cases_debug,
        },
        "v3_debug_artifacts": artifacts,
    }


def _build_case(
    *,
    candidate: dict[str, Any],
    record: dict[str, Any],
    base_domain: str,
    allowlist: list[str],
    manager_role_registry: dict[str, str],
    ledger_out: list[dict[str, Any]],
) -> dict[str, Any] | str:
    pool_type = str(candidate.get("pool_type") or "").strip().lower()
    if pool_type != "conversation_pool":
        return "pool_not_conversation"

    if not bool(candidate.get("selected_for_transcription")):
        return "not_selected_for_transcription"

    case_mode = _resolve_case_mode(candidate=candidate, record=record)
    if case_mode in DISALLOWED_CASES_FOR_ACTIVE_WRITE:
        return f"disallowed_case_mode:{case_mode}"

    llm_ready = bool(record.get("call_review_llm_ready"))
    llm_fields = record.get("call_review_llm_fields") if isinstance(record.get("call_review_llm_fields"), dict) else {}
    if not llm_ready or not llm_fields:
        return "llm_not_ready"
    llm_validation_error = _validate_llm_fields(case_mode=case_mode, llm_fields=llm_fields)
    if llm_validation_error:
        return llm_validation_error

    snapshot = _load_snapshot(record=record)
    calls = _extract_calls(snapshot=snapshot)
    selected_ids = _selected_call_ids(candidate=candidate, record=record)
    anchor_call = _select_anchor_call(calls=calls, selected_call_ids=selected_ids)
    if anchor_call is None:
        return "missing_anchor_call"

    if _call_is_noise(anchor_call):
        return "anchor_call_noise"

    anchor_duration = int(anchor_call.get("duration_seconds", 0) or 0)
    if anchor_duration < 25:
        return "anchor_call_too_short"

    related_calls = _select_related_calls(
        calls=calls,
        anchor_call=anchor_call,
        selected_call_ids=selected_ids,
    )
    if not related_calls:
        related_calls = [anchor_call]

    anchor_ts = _parse_ts(anchor_call.get("timestamp"))
    analysis_date = _choose_analysis_date(candidate=candidate, anchor_ts=anchor_ts)
    case_date = _choose_case_date(anchor_ts=anchor_ts)

    manager_name = _sanitize_person_name(str(anchor_call.get("manager_name") or ""))
    if not manager_name:
        manager_name = _sanitize_person_name(str(record.get("owner_name") or ""))
    if allowlist and not _is_manager_allowed(manager_name=manager_name, allowlist=allowlist):
        return "manager_not_in_allowlist"

    manager_role_internal = _resolve_manager_role(manager_name=manager_name, registry=manager_role_registry)
    manager_role_display = ROLE_DISPLAY_MAP.get(manager_role_internal, "менеджер по продажам")

    if manager_role_internal == "telemarketer" and case_mode in {"presentation_analysis", "test_analysis", "dozhim_analysis"}:
        return "telemarketer_case_out_of_scope"

    transcript_label = str(
        candidate.get("transcript_usability_label")
        or record.get("transcript_usability_label")
        or ""
    ).strip().lower()
    transcript_score = int(
        candidate.get("transcript_usability_score_final", 0)
        or record.get("transcript_usability_score_final", 0)
        or 0
    )
    transcript_excerpt = _text(record.get("transcript_text_excerpt"))
    call_summary = _text(record.get("call_signal_summary_short"))
    has_textual_conversation_evidence = bool(transcript_excerpt or call_summary)
    if transcript_label in {"empty", "noisy"}:
        return "transcript_not_usable_for_battle_write"
    if transcript_score < 2 and not has_textual_conversation_evidence:
        return "transcript_not_usable_for_battle_write"

    listened_calls = _format_listened_calls(related_calls)
    if not listened_calls:
        return "listened_calls_empty"

    base_mix_resolution = resolve_base_mix([record])
    base_mix_value = str(base_mix_resolution.get("selected_value") or "").strip() or "солянка"
    product_focus = _resolve_product_focus(record=record)

    quote_text = _sanitize_user_text(str(llm_fields.get("evidence_quote") or ""))
    llm_source = str(record.get("call_review_llm_source") or "")
    core_text = {
        key: _sanitize_user_text(llm_fields.get(key))
        for key in (
            "key_takeaway",
            "strong_sides",
            "growth_zones",
            "why_important",
            "reinforce",
            "fix_action",
            "coaching_list",
            "expected_quantity",
            "expected_quality",
            "stage_secretary_comment",
            "stage_lpr_comment",
            "stage_need_comment",
            "stage_presentation_comment",
            "stage_closing_comment",
            "stage_objections_comment",
            "stage_speech_comment",
            "stage_crm_comment",
            "stage_discipline_comment",
            "stage_demo_intro_comment",
            "stage_demo_context_comment",
            "stage_demo_relevant_comment",
            "stage_demo_process_comment",
            "stage_demo_objections_comment",
            "stage_demo_next_step_comment",
            "stage_demo_comment",
            "stage_test_launch_comment",
            "stage_test_criteria_comment",
            "stage_test_owners_comment",
            "stage_test_support_comment",
            "stage_test_feedback_comment",
            "stage_test_objections_comment",
            "stage_test_comment",
            "stage_dozhim_recontact_comment",
            "stage_dozhim_doubts_comment",
            "stage_dozhim_terms_comment",
            "stage_dozhim_decision_comment",
            "stage_dozhim_flow_comment",
            "stage_dozhim_comment",
        )
    }

    case = {
        "deal_id": str(record.get("deal_id") or ""),
        "deal_url": _deal_url(base_domain=base_domain, deal_id=str(record.get("deal_id") or "")),
        "deal_name": str(record.get("deal_name") or ""),
        "company_name": str(record.get("company_name") or ""),
        "manager_name": manager_name,
        "manager_role_internal": manager_role_internal,
        "manager_role_display": manager_role_display,
        "analysis_date": analysis_date,
        "case_date": case_date,
        "case_mode": case_mode,
        "case_type_display": CASE_MODE_DISPLAY_MAP.get(case_mode, "разговор"),
        "product_focus": product_focus,
        "base_mix": base_mix_value,
        "listened_calls": listened_calls,
        "llm_fields": core_text,
        "quote_text": quote_text,
        "score": record.get("score"),
        "criticality": _criticality_from_score(record.get("score")),
        "anchor_call_id": str(anchor_call.get("call_id") or ""),
        "anchor_call_timestamp": str(anchor_call.get("timestamp") or ""),
        "anchor_call_duration_seconds": anchor_duration,
        "selected_call_count": len(related_calls),
        "selected_call_ids": [str(x.get("call_id") or "") for x in related_calls if str(x.get("call_id") or "").strip()],
        "llm_source": llm_source,
        "call_review_llm_provenance": (
            record.get("call_review_llm_provenance")
            if isinstance(record.get("call_review_llm_provenance"), dict)
            else {}
        ),
    }

    for call in calls:
        ledger_out.append(
            {
                "deal_id": str(record.get("deal_id") or ""),
                "deal_name": str(record.get("deal_name") or ""),
                "manager_name_from_call_author": str(call.get("manager_name") or ""),
                "manager_role": manager_role_display,
                "call_id": str(call.get("call_id") or ""),
                "call_datetime": str(call.get("timestamp") or ""),
                "call_duration_seconds": int(call.get("duration_seconds", 0) or 0),
                "direction": str(call.get("direction") or ""),
                "recording_url": str(call.get("recording_url") or ""),
                "audio_path": str(call.get("audio_path") or ""),
                "phone_raw": str(call.get("phone") or call.get("phone_number") or call.get("contact_phone") or ""),
                "phone_normalized_last7": _normalize_phone_last7(
                    str(call.get("phone") or call.get("phone_number") or call.get("contact_phone") or "")
                ),
                "is_anchor": str(call.get("call_id") or "") == str(anchor_call.get("call_id") or ""),
                "selected_for_case": str(call.get("call_id") or "") in set(case["selected_call_ids"]),
                "recording_available": bool(str(call.get("recording_url") or "").strip()),
                "pool_type": pool_type,
                "case_mode": case_mode,
            }
        )
    return case

def _build_row_from_case(*, case: dict[str, Any]) -> dict[str, Any]:
    row = {col: "" for col in CALL_REVIEW_V3_COLUMNS}
    llm = case.get("llm_fields", {}) if isinstance(case.get("llm_fields"), dict) else {}
    quote = str(case.get("quote_text") or "")
    case_mode = str(case.get("case_mode") or "").strip().lower()
    role_internal = str(case.get("manager_role_internal") or "").strip().lower()

    row["Дата анализа"] = str(case.get("analysis_date") or "")
    row["Дата кейса"] = str(case.get("case_date") or "")
    row["Менеджер"] = str(case.get("manager_name") or "")
    row["Роль"] = str(case.get("manager_role_display") or "")
    row["Deal ID"] = str(case.get("deal_id") or "")
    row["Ссылка на сделку"] = str(case.get("deal_url") or "")
    row["Сделка"] = str(case.get("deal_name") or "")
    row["Компания"] = str(case.get("company_name") or "")
    row["Продукт / фокус"] = str(case.get("product_focus") or "")
    row["База / тег"] = str(case.get("base_mix") or "")
    row["Тип кейса"] = str(case.get("case_type_display") or "")
    row["Прослушанные звонки"] = str(case.get("listened_calls") or "")

    secretary_comment = _merge_comment_with_quote(str(llm.get("stage_secretary_comment") or ""), quote)
    lpr_comment = _merge_comment_with_quote(str(llm.get("stage_lpr_comment") or ""), quote)
    need_comment = _merge_comment_with_quote(str(llm.get("stage_need_comment") or ""), quote)
    presentation_comment = _merge_comment_with_quote(str(llm.get("stage_presentation_comment") or ""), quote)
    closing_comment = _merge_comment_with_quote(str(llm.get("stage_closing_comment") or ""), quote)
    objections_comment = _merge_comment_with_quote(str(llm.get("stage_objections_comment") or ""), quote)
    speech_comment = _merge_comment_with_quote(str(llm.get("stage_speech_comment") or ""), quote)
    crm_comment = _merge_comment_with_quote(str(llm.get("stage_crm_comment") or ""), quote)
    discipline_comment = _merge_comment_with_quote(str(llm.get("stage_discipline_comment") or ""), quote)
    confirm_demo_comment = _merge_comment_with_quote(str(llm.get("stage_demo_comment") or ""), quote)
    demo_comment = _merge_comment_with_quote(
        _join_non_empty(
            llm.get("stage_demo_intro_comment"),
            llm.get("stage_demo_context_comment"),
            llm.get("stage_demo_relevant_comment"),
            llm.get("stage_demo_process_comment"),
            llm.get("stage_demo_objections_comment"),
            llm.get("stage_demo_next_step_comment"),
            llm.get("stage_demo_comment"),
        ),
        quote,
    )
    test_comment = _merge_comment_with_quote(
        _join_non_empty(
            llm.get("stage_test_launch_comment"),
            llm.get("stage_test_criteria_comment"),
            llm.get("stage_test_owners_comment"),
            llm.get("stage_test_support_comment"),
            llm.get("stage_test_feedback_comment"),
            llm.get("stage_test_objections_comment"),
            llm.get("stage_test_comment"),
        ),
        quote,
    )
    dozhim_comment = _merge_comment_with_quote(
        _join_non_empty(
            llm.get("stage_dozhim_recontact_comment"),
            llm.get("stage_dozhim_doubts_comment"),
            llm.get("stage_dozhim_terms_comment"),
            llm.get("stage_dozhim_decision_comment"),
            llm.get("stage_dozhim_flow_comment"),
            llm.get("stage_dozhim_comment"),
        ),
        quote,
    )

    if role_internal == "telemarketer":
        # Cold-stage owner: do not leak low-funnel blocks without explicit direct evidence mode.
        confirm_demo_comment = ""
        demo_comment = ""
        test_comment = ""
        dozhim_comment = ""

    if case_mode not in {"presentation_analysis"}:
        demo_comment = ""
    if case_mode not in {"test_analysis"}:
        test_comment = ""
    if case_mode not in {"dozhim_analysis"}:
        dozhim_comment = ""
    if case_mode not in {"presentation_analysis", "warm_inbound_analysis", "supplier_inbound_analysis"}:
        confirm_demo_comment = ""

    row["Комментарий по этапу (секретарь)"] = secretary_comment
    row["Комментарий по этапу (лпр)"] = lpr_comment
    row["Комментарий по этапу (актуальность и потребность)"] = need_comment
    row["Комментарий по этапу (презентация встречи)"] = presentation_comment
    row["Комментарий по этапу (закрытие на встречу)"] = closing_comment
    row["Комментарий по этапу (отработка возражений)"] = objections_comment
    row["Комментарий по этапу (чистота речи)"] = speech_comment
    row["Комментарий по этапу (работа с црм)"] = crm_comment
    row["Комментарий по этапу (дисциплина дозвонов)"] = discipline_comment
    row["Комментарий по этапу (подтверждение презентации)"] = confirm_demo_comment
    row["Комментарий по этапу (презентация)"] = demo_comment
    row["Комментарий по этапу (работа с тестом)"] = test_comment
    row["Комментарий по этапу (дожим / кп)"] = dozhim_comment

    row["Ключевой вывод"] = str(llm.get("key_takeaway") or "")
    row["Сильная сторона"] = str(llm.get("strong_sides") or "")
    row["Зона роста"] = str(llm.get("growth_zones") or "")
    row["Почему это важно"] = str(llm.get("why_important") or "")
    row["Что закрепить"] = str(llm.get("reinforce") or "")
    row["Что исправить"] = str(llm.get("fix_action") or "")
    row["Что донести сотруднику"] = _normalize_coaching(str(llm.get("coaching_list") or ""))
    row["Эффект количество / неделя"] = _normalize_expected_quantity(str(llm.get("expected_quantity") or ""))
    row["Эффект качество"] = str(llm.get("expected_quality") or "")
    row["Оценка 0-100"] = case.get("score") if case.get("score") is not None else ""
    row["Критичность"] = str(case.get("criticality") or "")
    return row


def _write_debug_artifacts(
    *,
    run_dir: Path | None,
    ledger: list[dict[str, Any]],
    cases_debug: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    skipped_reasons: dict[str, int],
) -> dict[str, str]:
    if run_dir is None:
        return {}
    root = run_dir / "call_review_v3"
    root.mkdir(parents=True, exist_ok=True)
    ledger_path = root / "call_ledger.json"
    cases_path = root / "anchor_cases.json"
    rows_path = root / "writer_rows.json"
    summary_path = root / "pipeline_summary.json"
    ledger_path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2), encoding="utf-8")
    cases_path.write_text(json.dumps(cases_debug, ensure_ascii=False, indent=2), encoding="utf-8")
    rows_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_path.write_text(
        json.dumps(
            {
                "ledger_total": len(ledger),
                "cases_total": len(cases_debug),
                "rows_total": len(rows),
                "skipped_reasons": skipped_reasons,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "root": str(root),
        "call_ledger_json": str(ledger_path),
        "anchor_cases_json": str(cases_path),
        "writer_rows_json": str(rows_path),
        "pipeline_summary_json": str(summary_path),
    }


def _resolve_case_mode(*, candidate: dict[str, Any], record: dict[str, Any]) -> str:
    raw = str(record.get("call_review_case_mode") or "").strip().lower()
    if raw:
        return raw
    case_type = str(candidate.get("call_case_type") or "").strip().lower()
    mapping = {
        "lpr_conversation": "negotiation_lpr_analysis",
        "secretary_case": "secretary_analysis",
        "supplier_inbound": "supplier_inbound_analysis",
        "warm_inbound": "warm_inbound_analysis",
        "warm_case": "warm_inbound_analysis",
        "presentation": "presentation_analysis",
        "demo": "presentation_analysis",
        "test": "test_analysis",
        "pilot": "test_analysis",
        "dozhim": "dozhim_analysis",
        "closing": "dozhim_analysis",
        "redial_discipline": "redial_discipline_analysis",
        "autoanswer_noise": "redial_discipline_analysis",
    }
    return mapping.get(case_type, "skip_no_meaningful_case")


def _validate_llm_fields(*, case_mode: str, llm_fields: dict[str, Any]) -> str:
    required = (
        "key_takeaway",
        "strong_sides",
        "growth_zones",
        "why_important",
        "reinforce",
        "fix_action",
        "coaching_list",
        "expected_quantity",
        "expected_quality",
    )
    for key in required:
        if not _text(llm_fields.get(key)):
            return f"llm_missing_{key}"
    if case_mode == "secretary_analysis" and not any(
        _text(llm_fields.get(k))
        for k in ("stage_secretary_comment", "stage_need_comment", "stage_lpr_comment")
    ):
        return "llm_missing_stage_secretary_comment"
    if case_mode == "presentation_analysis" and not any(
        _text(llm_fields.get(k))
        for k in (
            "stage_demo_intro_comment",
            "stage_demo_context_comment",
            "stage_demo_relevant_comment",
            "stage_demo_comment",
        )
    ):
        return "llm_missing_stage_presentation_comment"
    if case_mode == "test_analysis" and not any(
        _text(llm_fields.get(k))
        for k in ("stage_test_launch_comment", "stage_test_criteria_comment", "stage_test_comment")
    ):
        return "llm_missing_stage_test_comment"
    if case_mode == "dozhim_analysis" and not any(
        _text(llm_fields.get(k))
        for k in ("stage_dozhim_recontact_comment", "stage_dozhim_terms_comment", "stage_dozhim_comment")
    ):
        return "llm_missing_stage_dozhim_comment"
    if case_mode not in {"presentation_analysis", "test_analysis", "dozhim_analysis", "secretary_analysis"}:
        if not any(
            _text(llm_fields.get(k))
            for k in (
                "stage_lpr_comment",
                "stage_need_comment",
                "stage_presentation_comment",
                "stage_closing_comment",
                "stage_objections_comment",
                "stage_speech_comment",
            )
        ):
            return "llm_missing_conversation_stage_comment"
    return ""


def _extract_calls(*, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    call_evidence = snapshot.get("call_evidence", {}) if isinstance(snapshot, dict) else {}
    items = call_evidence.get("items", []) if isinstance(call_evidence, dict) and isinstance(call_evidence.get("items"), list) else []
    return [x for x in items if isinstance(x, dict)]


def _load_snapshot(*, record: dict[str, Any]) -> dict[str, Any]:
    artifact_path = Path(str(record.get("artifact_path") or "").strip())
    if not artifact_path.exists():
        return {}
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload.get("snapshot", {}) if isinstance(payload, dict) and isinstance(payload.get("snapshot"), dict) else {}


def _selected_call_ids(*, candidate: dict[str, Any], record: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for source in (
        candidate.get("selected_call_ids"),
        record.get("selected_call_ids"),
    ):
        if not isinstance(source, list):
            continue
        for item in source:
            call_id = str(item or "").strip()
            if call_id:
                out.add(call_id)
    return out


def _select_anchor_call(*, calls: list[dict[str, Any]], selected_call_ids: set[str]) -> dict[str, Any] | None:
    candidates = []
    for call in calls:
        if not isinstance(call, dict):
            continue
        call_id = str(call.get("call_id") or "").strip()
        if selected_call_ids and call_id and call_id not in selected_call_ids:
            continue
        ts = _parse_ts(call.get("timestamp"))
        duration = int(call.get("duration_seconds", 0) or 0)
        score = duration
        if bool(str(call.get("recording_url") or "").strip()):
            score += 120
        if not _call_is_noise(call):
            score += 80
        if str(call.get("direction") or "").strip().lower() in {"outbound", "inbound"}:
            score += 10
        if ts is not None:
            score += 5
        candidates.append((score, ts, call))
    if not candidates:
        return None
    candidates.sort(
        key=lambda x: (
            -int(x[0]),
            str((x[1] or datetime(1970, 1, 1, tzinfo=timezone.utc)).isoformat()),
            str((x[2] or {}).get("call_id") or ""),
        )
    )
    return candidates[0][2]


def _select_related_calls(
    *,
    calls: list[dict[str, Any]],
    anchor_call: dict[str, Any],
    selected_call_ids: set[str],
) -> list[dict[str, Any]]:
    anchor_ts = _parse_ts(anchor_call.get("timestamp"))
    anchor_id = str(anchor_call.get("call_id") or "")
    out: list[dict[str, Any]] = [anchor_call]
    for call in calls:
        if not isinstance(call, dict):
            continue
        call_id = str(call.get("call_id") or "").strip()
        if not call_id or call_id == anchor_id:
            continue
        if selected_call_ids and call_id not in selected_call_ids:
            continue
        ts = _parse_ts(call.get("timestamp"))
        if anchor_ts is not None and ts is not None and ts > anchor_ts:
            continue
        if _call_is_noise(call):
            continue
        out.append(call)
    out.sort(
        key=lambda x: (
            str(x.get("timestamp") or ""),
            str(x.get("call_id") or ""),
        )
    )
    return out[:3]


def _call_is_noise(call: dict[str, Any]) -> bool:
    duration = int(call.get("duration_seconds", 0) or 0)
    if duration <= 20:
        return True
    blob = " ".join(
        _text(call.get(k)).lower()
        for k in ("status", "result", "disposition", "quality_flags")
    )
    noise_tokens = (
        "no_answer",
        "not_answered",
        "busy",
        "voicemail",
        "autoanswer",
        "автоответ",
        "недозвон",
        "занято",
    )
    return any(token in blob for token in noise_tokens)


def _choose_analysis_date(*, candidate: dict[str, Any], anchor_ts: datetime | None) -> str:
    by_shortlist = _text(candidate.get("business_window_date"))
    if by_shortlist:
        return by_shortlist
    if anchor_ts is not None:
        return _business_bucket_date(anchor_ts).isoformat()
    return ""


def _choose_case_date(*, anchor_ts: datetime | None) -> str:
    if anchor_ts is None:
        return ""
    return anchor_ts.astimezone(MSK_TZ).date().isoformat()


def _business_bucket_date(ts_utc: datetime) -> date:
    local = ts_utc.astimezone(MSK_TZ)
    # Weekend calls belong to Monday business bucket.
    if local.weekday() >= 5:
        cur = local.date()
        while cur.weekday() >= 5:
            cur = cur + timedelta(days=1)
        return cur
    cutoff = datetime.combine(local.date(), time(15, 0), tzinfo=MSK_TZ)
    if local < cutoff:
        return local.date()
    cur = local.date() + timedelta(days=1)
    while cur.weekday() >= 5:
        cur = cur + timedelta(days=1)
    return cur


def _resolve_manager_role(*, manager_name: str, registry: dict[str, str]) -> str:
    low = manager_name.strip().lower()
    for key, role in registry.items():
        token = str(key or "").strip().lower()
        if token and (token in low or low in token):
            role_low = str(role or "").strip().lower()
            if role_low in {"telemarketer", "sales_manager"}:
                return role_low
    if "рустам" in low:
        return "telemarketer"
    if "илья" in low:
        return "sales_manager"
    return "sales_manager"


def _resolve_product_focus(*, record: dict[str, Any]) -> str:
    hyp = _text(record.get("product_hypothesis")).lower()
    if hyp in {"info", "link", "mixed", "unknown"}:
        return {
            "info": "инфо",
            "link": "линк",
            "mixed": "оба",
            "unknown": "до продукта разговор не дошел",
        }[hyp]
    name = _text(record.get("product_name")).lower()
    if "линк" in name or "link" in name:
        return "линк"
    if "инфо" in name or "info" in name:
        return "инфо"
    return "до продукта разговор не дошел"


def _format_listened_calls(calls: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for call in calls:
        ts = _parse_ts(call.get("timestamp"))
        duration = int(call.get("duration_seconds", 0) or 0)
        if ts is None:
            continue
        local = ts.astimezone(MSK_TZ)
        stamp = local.strftime("%Y-%m-%d %H:%M")
        chunks.append(f"{stamp} - {_format_duration(duration)}")
    return "; ".join(chunks[:8]).strip()


def _format_duration(seconds: int) -> str:
    sec = max(0, int(seconds))
    mm, ss = divmod(sec, 60)
    hh, mm = divmod(mm, 60)
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return f"{mm:02d}:{ss:02d}"


def _deal_url(*, base_domain: str, deal_id: str) -> str:
    did = _text(deal_id)
    if not did:
        return ""
    domain = _text(base_domain).rstrip("/")
    if domain:
        return f"{domain}/leads/detail/{did}"
    return did


def _criticality_from_score(score: Any) -> str:
    try:
        value = int(float(score))
    except Exception:
        return ""
    if value < 40:
        return "высокая"
    if value < 70:
        return "средняя"
    return "низкая"


def _normalize_coaching(value: str) -> str:
    text = _sanitize_user_text(value)
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if lines and all(re.match(r"^\d+\)", line) for line in lines):
        return "\n".join(lines[:3])
    parts = [x.strip() for x in re.split(r"[;\n]+", text) if x.strip()]
    if not parts:
        return ""
    numbered = [f"{idx + 1}) {part}" for idx, part in enumerate(parts[:3])]
    return "\n".join(numbered)


def _normalize_expected_quantity(value: str) -> str:
    text = _sanitize_user_text(value)
    text = text.replace("%", "")
    if not text:
        return ""
    if re.search(r"\b\+?0(\.0+)?\b", text):
        text = re.sub(r"\b\+?0(\.0+)?\b", "0.2", text)
    return text[:220]


def _sanitize_user_text(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    replacements = {
        "—": "-",
        "–": "-",
        "CRM": "црм",
        "crm": "црм",
        "LLM": "",
        "llm": "",
        "call signal": "сигнал разговора",
        "next step": "следующий шаг",
        "follow-up": "следующий шаг",
        "follow up": "следующий шаг",
        "qualified loss": "потеря по делу",
        "anti-fit": "не наш кейс",
        "owner ambiguity": "кто вел сделку",
        "транскрипт": "разговор",
        "расшифровк": "разбор",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:900]


def _merge_comment_with_quote(comment: str, quote: str) -> str:
    c = _sanitize_user_text(comment)
    q = _sanitize_user_text(quote)
    if not c:
        return ""
    if q and '"' not in c and "цитата" not in c.lower():
        quote_short = q[:180]
        c = f'{c} Цитата: "{quote_short}"'
    return c[:900]


def _join_non_empty(*parts: Any) -> str:
    out = [str(x).strip() for x in parts if str(x).strip()]
    return "; ".join(out)


def _parse_ts(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except Exception:
            return None
    raw = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_phone_last7(value: str) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-7:] if len(digits) >= 7 else digits


def _is_manager_allowed(*, manager_name: str, allowlist: list[str]) -> bool:
    if not allowlist:
        return True
    low = manager_name.strip().lower()
    if not low:
        return False
    for item in allowlist:
        token = str(item or "").strip().lower()
        if token and (token in low or low in token):
            return True
    return False


def _sanitize_person_name(value: str) -> str:
    text = _text(value)
    return re.sub(r"\s+", " ", text).strip()


def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = int(counter.get(key, 0) or 0) + 1


def _text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()
