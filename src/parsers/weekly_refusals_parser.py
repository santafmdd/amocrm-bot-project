"""Parser for weekly refusals dataset from amoCRM events list rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


def normalize_status_text(value: str) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    return " ".join(text.split())


@dataclass(frozen=True)
class WeeklyRefusalParsedResult:
    report_id: str
    display_name: str
    source_rows: list[dict[str, Any]]
    aggregated_before_status_counts: list[dict[str, Any]]
    aggregated_after_status_counts: list[dict[str, Any]]
    deal_refs: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _to_sorted_counts(counter: dict[str, int]) -> list[dict[str, Any]]:
    rows = [{"status": key, "count": int(value)} for key, value in counter.items() if key]
    rows.sort(key=lambda item: (-int(item["count"]), str(item["status"])))
    return rows


def parse_weekly_refusals_rows(
    *,
    report_id: str,
    display_name: str,
    rows: list[dict[str, Any]],
) -> WeeklyRefusalParsedResult:
    before_counter: dict[str, int] = {}
    after_counter: dict[str, int] = {}
    out_rows: list[dict[str, Any]] = []
    deal_refs: list[dict[str, Any]] = []
    seen_deals: set[str] = set()

    for row in rows:
        before_raw = str(row.get("status_before", "") or "")
        after_raw = str(row.get("status_after", "") or "")
        after_loss_raw = str(row.get("status_after_loss_reason", "") or "")

        before = normalize_status_text(before_raw)
        # Right table should aggregate by granular loss reason when available.
        after_agg_source = after_loss_raw.strip() if after_loss_raw.strip() else after_raw
        after = normalize_status_text(after_agg_source)

        if before:
            before_counter[before] = before_counter.get(before, 0) + 1
        if after:
            after_counter[after] = after_counter.get(after, 0) + 1

        deal_id = str(row.get("deal_id", "") or "").strip()
        deal_url = str(row.get("deal_url", "") or "").strip()
        dedupe_key = f"id:{deal_id}" if deal_id else (f"url:{deal_url}" if deal_url else "")
        if dedupe_key and dedupe_key not in seen_deals:
            seen_deals.add(dedupe_key)
            deal_refs.append({"deal_id": deal_id, "deal_url": deal_url})

        clean_row = dict(row)
        clean_row["status_before"] = before_raw.strip()
        clean_row["status_after"] = after_raw.strip()
        clean_row["status_after_loss_reason"] = after_loss_raw.strip()
        out_rows.append(clean_row)

    return WeeklyRefusalParsedResult(
        report_id=report_id,
        display_name=display_name,
        source_rows=out_rows,
        aggregated_before_status_counts=_to_sorted_counts(before_counter),
        aggregated_after_status_counts=_to_sorted_counts(after_counter),
        deal_refs=deal_refs,
    )
