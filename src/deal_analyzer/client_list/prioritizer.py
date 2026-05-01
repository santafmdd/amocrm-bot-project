from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Any

from ..daily_control.source_reader import clean_text, parse_date
from ..weekly_shared.role_policy import resolve_role_policy
from .models import ClientListRow, ManagerClientContext


_CATEGORY_PRIORITY: dict[str, float] = {
    "invoice_to_payment": 100.0,
    "renewal": 95.0,
    "test_to_invoice": 90.0,
    "demo_to_test": 80.0,
    "interest_to_demo": 70.0,
    "stalled_warm": 75.0,
    "reactivation": 60.0,
    "low_priority": 25.0,
    "no_action": 0.0,
}


def _norm(value: Any) -> str:
    return clean_text(value).lower()


def _to_date(value: str) -> date | None:
    parsed = parse_date(value)
    if not parsed:
        return None
    try:
        return date.fromisoformat(parsed)
    except ValueError:
        return None


def _manager_match(manager_name: str, target: str) -> bool:
    left = _norm(manager_name)
    right = _norm(target)
    if not left or not right:
        return False
    return left == right or left in right or right in left


def classify_priority_category(row: ClientListRow) -> tuple[str, str]:
    status = _norm(row.status_text)
    comment = _norm(row.comment_text)
    next_step = _norm(row.next_step_text)
    text = " ".join([status, comment, next_step])
    test_open_markers = ("тест открыт", "в тесте", "активный тест", "pilot started", "test started")
    if any(token in text for token in ("оплач", "ожидание оплаты", "счет отправлен", "invoice sent", "кп отправ")):
        return "invoice_to_payment", "Есть счет/КП и требуется дожим до оплаты"
    if any(token in text for token in ("продлен", "продление", "renewal", "пролонгац")):
        return "renewal", "Кейс продления/удержания клиента"
    if any(token in text for token in test_open_markers):
        return "test_to_invoice", "Открыт тест, нужен перевод в счет"
    status_comment = " ".join([status, comment])
    if any(token in text for token in ("демо", "demo", "показ")) and not any(
        token in status_comment for token in test_open_markers
    ):
        return "demo_to_test", "Проведено демо, но нет активного теста"
    if any(token in text for token in ("интерес", "встреч", "созвон", "discovery")) and not any(
        token in text for token in ("демо", "demo", "тест", "pilot")
    ):
        return "interest_to_demo", "Есть теплый интерес, нужен перевод в демо"
    if row.risk_stalled and any(token in text for token in ("тепл", "в работе", "active", "перезвон")):
        return "stalled_warm", "Теплая сделка зависла без подтвержденного шага"
    if any(token in text for token in ("реактивац", "вернуть", "возобнов", "пауза", "потом")):
        return "reactivation", "Кейс реактивации"
    if any(token in text for token in ("закрыто и не реализовано", "lost", "архив", "нецел")):
        return "no_action", "Закрытый/нецелевой кейс"
    return "low_priority", "Недостаточно сигналов для приоритета"


def _row_score(row: ClientListRow) -> float:
    base = _CATEGORY_PRIORITY.get(row.priority_category, 0.0)
    amount = float(row.value_amount or 0.0)
    stalled_bonus = 12.0 if row.risk_stalled else 0.0
    return base + stalled_bonus + min(amount / 100000.0, 35.0)


def prioritize_rows(rows: list[ClientListRow]) -> list[ClientListRow]:
    out: list[ClientListRow] = []
    for row in rows:
        category, reason = classify_priority_category(row)
        enriched = replace(row, priority_category=category, priority_reason=reason)
        out.append(replace(enriched, priority_score=_row_score(enriched)))
    out.sort(key=lambda item: (-float(item.priority_score or 0.0), item.next_step_date or "9999-99-99", item.row_number))
    return out


def build_priority_summary(rows: list[ClientListRow]) -> dict[str, Any]:
    prioritized = prioritize_rows(rows)
    categories: dict[str, int] = {}
    for row in prioritized:
        categories[row.priority_category] = int(categories.get(row.priority_category, 0) or 0) + 1
    return {
        "rows_total": len(rows),
        "rows_prioritized": len(prioritized),
        "categories": categories,
        "top_rows": [
            {
                "row_number": row.row_number,
                "manager_name": row.manager_name,
                "client_name": row.client_name or row.company_name or row.contact_name,
                "deal_name": row.deal_name,
                "deal_id": row.deal_id,
                "deal_link": row.deal_link,
                "priority_category": row.priority_category,
                "priority_reason": row.priority_reason,
                "priority_score": row.priority_score,
                "next_step_text": row.next_step_text,
                "next_step_date": row.next_step_date,
            }
            for row in prioritized[:30]
        ],
    }


def build_manager_client_context(
    *,
    rows: list[ClientListRow],
    manager_name: str,
    period_start: str,
    period_end: str,
    manager_role_registry: dict[str, str] | None = None,
    role_policy_registry: dict[str, dict[str, Any]] | None = None,
    max_items: int = 12,
) -> ManagerClientContext:
    start_date = _to_date(period_start)
    end_date = _to_date(period_end)
    filtered: list[ClientListRow] = []
    for row in rows:
        if row.manager_name and not _manager_match(row.manager_name, manager_name):
            continue
        row_day = _to_date(row.next_step_date)
        if start_date is not None and end_date is not None and row_day is not None:
            if row_day < start_date or row_day > end_date:
                # next step outside plan week can still be a follow-up target, so keep stalled/high-value rows.
                if not row.risk_stalled and float(row.value_amount or 0.0) <= 0.0:
                    continue
        filtered.append(row)

    prioritized = prioritize_rows(filtered)
    categories: dict[str, int] = {}
    for row in prioritized:
        categories[row.priority_category] = int(categories.get(row.priority_category, 0) or 0) + 1

    role_policy = resolve_role_policy(
        manager_name=manager_name,
        manager_role_profile="",
        manager_role_registry=manager_role_registry,
        role_policy_registry=role_policy_registry,
    )
    role = str(role_policy.get("role") or "sales_manager")
    sales_focus_categories = {"invoice_to_payment", "test_to_invoice", "demo_to_test", "interest_to_demo", "renewal", "stalled_warm", "reactivation"}
    if role == "sales_manager":
        prioritized = [row for row in prioritized if row.priority_category in sales_focus_categories]

    top = prioritized[: max(1, int(max_items or 12))]
    lines: list[str] = []
    for idx, row in enumerate(top, start=1):
        label = row.client_name or row.company_name or row.contact_name or row.deal_name or f"строка {row.row_number}"
        link = row.deal_link or row.amocrm_link or row.contact_link or row.company_link
        step = row.next_step_text or row.status_text or row.comment_text
        lines.append(
            f"{idx}) {row.priority_category}: {label}"
            + (f" | сделка {row.deal_id}" if row.deal_id else "")
            + (f" | link: {link}" if link else "")
            + (f" | next step: {step}" if step else "")
        )

    warnings: list[str] = []
    if not top:
        warnings.append("no_client_candidates_for_manager")
    return ManagerClientContext(
        manager_name=manager_name,
        period_start=period_start,
        period_end=period_end,
        rows_total=len(filtered),
        categories=categories,
        top_priority_items=[
            {
                "row_number": row.row_number,
                "client_name": row.client_name or row.company_name or row.contact_name,
                "deal_name": row.deal_name,
                "deal_id": row.deal_id,
                "deal_link": row.deal_link,
                "contact_link": row.contact_link,
                "company_link": row.company_link,
                "priority_category": row.priority_category,
                "priority_reason": row.priority_reason,
                "priority_score": row.priority_score,
                "next_step_text": row.next_step_text,
                "next_step_date": row.next_step_date,
                "value_amount": row.value_amount,
            }
            for row in top
        ],
        summary_lines=lines,
        warnings=warnings,
    )
