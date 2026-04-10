from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.writers.layout_filter_dsl import LayoutBlockConfig, LayoutScenario, parse_layout_row


@dataclass(frozen=True)
class DslExecutionInput:
    original_text: str
    source_kind: str
    filter_field: str
    filter_operator: str
    filter_value: str
    pipeline_name: str
    period: str
    date_mode: str
    tabs: list[str]


def _first_non_empty(values: list[str]) -> str:
    for value in values:
        val = str(value).strip()
        if val:
            return val
    return ""


def _normalize_tabs_values(values: list[str]) -> list[str]:
    allowed = {"all", "active", "closed"}
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        val = str(raw).strip().lower()
        if val not in allowed or val in seen:
            continue
        seen.add(val)
        result.append(val)
    return result


def _extract_from_scenario(scenario: LayoutScenario, original_text: str) -> DslExecutionInput:
    tags_filter = next((f for f in scenario.filters if f.normalized_field_name == "tags"), None)
    utm_filter = next((f for f in scenario.filters if f.normalized_field_name == "utm_source"), None)
    pipeline_filter = next((f for f in scenario.filters if f.normalized_field_name == "pipeline"), None)
    period_filter = next((f for f in scenario.filters if f.normalized_field_name == "period"), None)
    dates_filter = next((f for f in scenario.filters if f.normalized_field_name == "dates_mode"), None)
    tabs_filter = next((f for f in scenario.filters if f.normalized_field_name in {"tabs", "tab", "tab_mode"}), None)

    source_kind = ""
    filter_field = ""
    filter_operator = "="
    filter_value = ""

    if tags_filter is not None:
        source_kind = "tag"
        filter_field = "tags"
        filter_operator = tags_filter.operator
        filter_value = _first_non_empty(tags_filter.values)
    elif utm_filter is not None:
        source_kind = "utm_prefix" if utm_filter.operator == "^=" else "utm_exact"
        filter_field = "utm_source"
        filter_operator = utm_filter.operator
        filter_value = _first_non_empty(utm_filter.values)

    return DslExecutionInput(
        original_text=original_text,
        source_kind=source_kind,
        filter_field=filter_field,
        filter_operator=filter_operator,
        filter_value=filter_value,
        pipeline_name=_first_non_empty(pipeline_filter.values) if pipeline_filter else "",
        period=_first_non_empty(period_filter.values) if period_filter else "",
        date_mode=_first_non_empty(dates_filter.values) if dates_filter else "",
        tabs=_normalize_tabs_values(tabs_filter.values) if tabs_filter else [],
    )


def build_execution_inputs_from_block_config(block: LayoutBlockConfig) -> list[DslExecutionInput]:
    return [_extract_from_scenario(scenario=scenario, original_text=scenario.raw_text) for scenario in block.scenarios]


def parse_dsl_execution_inputs(text: str) -> tuple[LayoutBlockConfig, list[DslExecutionInput]]:
    block = parse_layout_row(text)
    return block, build_execution_inputs_from_block_config(block)


def execution_input_to_dict(item: DslExecutionInput) -> dict[str, Any]:
    return {
        "original_text": item.original_text,
        "source_kind": item.source_kind,
        "filter_field": item.filter_field,
        "filter_operator": item.filter_operator,
        "filter_value": item.filter_value,
        "pipeline_name": item.pipeline_name,
        "period": item.period,
        "date_mode": item.date_mode,
        "tabs": list(item.tabs),
    }
