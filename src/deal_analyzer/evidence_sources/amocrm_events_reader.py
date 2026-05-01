from __future__ import annotations

from datetime import date, datetime, time, timezone, timedelta
from typing import Any

from src.amocrm_collector.client import AmoCollectorClient, ApiRequestError


INTEREST_STAGE_TOKENS = (
    "есть интерес",
    "интерес",
    "демо",
    "демонстрац",
    "тест",
    "счет",
    "оплат",
)


def collect_presentation_related_lead_ids(
    *,
    client: AmoCollectorClient,
    period_start: date,
    period_end: date,
    logger,
    max_pages: int = 40,
) -> tuple[set[str], dict[str, Any]]:
    from_unix = _day_start_unix(period_start)
    to_unix = _day_end_unix(period_end)
    selected: set[str] = set()
    debug_events: list[dict[str, Any]] = []
    pages_total = 0
    status_changes_seen = 0
    failed = ""
    for page in range(1, max_pages + 1):
        params = {
            "limit": 250,
            "page": page,
            "filter[created_at][from]": from_unix,
            "filter[created_at][to]": to_unix,
        }
        try:
            events, meta, request_path = client.get_events_page(params=params)
            pages_total += 1
        except ApiRequestError as exc:
            failed = f"api_request_error:{exc}"
            logger.warning("presentation events fetch failed: %s", exc)
            break
        except Exception as exc:
            failed = f"events_fetch_failed:{exc}"
            logger.warning("presentation events fetch unexpected fail: %s", exc)
            break
        count = len(events)
        for event in events:
            if not isinstance(event, dict):
                continue
            etype = str(event.get("type") or "").strip().lower()
            raw_val = ""
            for key in ("value_after", "value_before", "value", "text"):
                value = event.get(key)
                if isinstance(value, str) and value.strip():
                    raw_val = value
                    break
                if isinstance(value, dict):
                    blob = " ".join(str(v) for v in value.values())
                    if blob.strip():
                        raw_val = blob
                        break
            lower = raw_val.lower()
            if "status" not in etype and "lead" not in etype and not lower:
                continue
            status_changes_seen += 1
            matched = any(token in lower for token in INTEREST_STAGE_TOKENS)
            lead_id = _extract_lead_id(event)
            if matched and lead_id:
                selected.add(lead_id)
            if matched and len(debug_events) < 300:
                debug_events.append(
                    {
                        "event_id": str(event.get("id") or ""),
                        "lead_id": lead_id,
                        "type": etype,
                        "value_excerpt": " ".join(raw_val.split())[:280],
                        "matched": bool(matched),
                    }
                )
        if count < 250:
            break
    return selected, {
        "events_pages_total": int(pages_total),
        "events_status_like_seen": int(status_changes_seen),
        "lead_ids_selected": sorted(selected),
        "selected_leads_count": len(selected),
        "events_debug": debug_events,
        "error": failed,
    }


def _extract_lead_id(event: dict[str, Any]) -> str:
    for path in (
        ("entity_id",),
        ("entity", "id"),
        ("lead_id",),
        ("value_after", "lead_id"),
        ("value_before", "lead_id"),
    ):
        value: Any = event
        for key in path:
            if not isinstance(value, dict):
                value = None
                break
            value = value.get(key)
        if isinstance(value, int) and value > 0:
            return str(value)
        text = str(value or "").strip()
        if text.isdigit():
            return text
    return ""


def _day_start_unix(value: date) -> int:
    dt = datetime.combine(value, time(0, 0, 0), tzinfo=timezone(timedelta(hours=3)))
    return int(dt.timestamp())


def _day_end_unix(value: date) -> int:
    base = _day_start_unix(value)
    return base + 24 * 60 * 60 - 1
