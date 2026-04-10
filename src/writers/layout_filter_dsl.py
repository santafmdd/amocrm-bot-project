"""DSL parser for yellow layout rows in Google Sheets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SupportedOperator = Literal["=", "^="]


FIELD_ALIASES: dict[str, str] = {
    "теги": "tags",
    "тег": "tags",
    "tags": "tags",
    "tag": "tags",
    "tags": "tags",
    "utm source": "utm_source",
    "utm_source": "utm_source",
    "utm": "utm_source",
    "воронка": "pipeline",
    "pipeline": "pipeline",
    "funnel": "pipeline",
    "период": "period",
    "period": "period",
    "period mode": "period",
    "даты": "dates_mode",
    "дата": "dates_mode",
    "dates": "dates_mode",
    "dates mode": "dates_mode",
    "date mode": "dates_mode",
    "с": "date_from",
    "from": "date_from",
    "по": "date_to",
    "to": "date_to",
    "tabs": "tabs",
    "tab": "tabs",
    "tab mode": "tabs",
}


@dataclass(frozen=True)
class LayoutFilter:
    raw_field_name: str
    normalized_field_name: str
    operator: SupportedOperator
    values: list[str]
    raw_text: str


@dataclass(frozen=True)
class LayoutScenario:
    filters: list[LayoutFilter]
    raw_text: str


@dataclass(frozen=True)
class LayoutBlockConfig:
    display_name: str
    scenarios: list[LayoutScenario]
    raw_text: str


@dataclass(frozen=True)
class ScenarioRunResult:
    scenario_index: int
    success: bool
    total_count: int
    non_empty_stage_rows: int


def normalize_field_name(name: str) -> str:
    key = " ".join((name or "").strip().lower().replace("ё", "е").replace("_", " ").split())
    return FIELD_ALIASES.get(key, key)


def parse_layout_row(text: str) -> LayoutBlockConfig:
    raw = (text or "").strip()
    if not raw:
        raise ValueError("Empty layout row")
    if ":" not in raw:
        raise ValueError("Layout row must contain ':' separator")

    display_name, payload = raw.split(":", 1)
    display_name = display_name.strip()
    payload = payload.strip()
    if not display_name:
        raise ValueError("Layout row display name is empty")
    if not payload:
        raise ValueError("Layout row has empty DSL payload")

    scenario_chunks = [chunk.strip() for chunk in payload.split("||") if chunk.strip()]
    scenarios: list[LayoutScenario] = []
    for scenario_text in scenario_chunks:
        filters: list[LayoutFilter] = []
        filter_chunks = [chunk.strip() for chunk in scenario_text.split(";") if chunk.strip()]
        for filter_text in filter_chunks:
            operator: SupportedOperator = "="
            if "^=" in filter_text:
                left, right = filter_text.split("^=", 1)
                operator = "^="
            elif "=" in filter_text:
                left, right = filter_text.split("=", 1)
            else:
                raise ValueError(f"Filter chunk has no operator '=' or '^=': {filter_text}")

            raw_field = left.strip()
            value_blob = right.strip()
            if not raw_field:
                raise ValueError(f"Filter has empty field name: {filter_text}")
            if not value_blob:
                raise ValueError(f"Filter has empty value: {filter_text}")

            values = [v.strip() for v in value_blob.split("|") if v.strip()]
            if not values:
                raise ValueError(f"Filter has no values after split: {filter_text}")

            filters.append(
                LayoutFilter(
                    raw_field_name=raw_field,
                    normalized_field_name=normalize_field_name(raw_field),
                    operator=operator,
                    values=values,
                    raw_text=filter_text,
                )
            )

        scenarios.append(LayoutScenario(filters=filters, raw_text=scenario_text))

    if not scenarios:
        raise ValueError("No scenarios parsed from layout row")

    return LayoutBlockConfig(display_name=display_name, scenarios=scenarios, raw_text=raw)


def select_best_scenario(results: list[ScenarioRunResult]) -> ScenarioRunResult:
    successful = [r for r in results if r.success]
    if not successful:
        raise ValueError("No successful scenarios to select")

    # 1) max total_count
    # 2) max non_empty_stage_rows
    # 3) first successful by index/order
    return sorted(
        successful,
        key=lambda r: (r.total_count, r.non_empty_stage_rows, -r.scenario_index),
        reverse=True,
    )[0]
