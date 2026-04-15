import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.writers.layout_dsl_routing import parse_dsl_execution_inputs


def test_parse_dsl_execution_inputs_tags() -> None:
    text = "Машэкспо: Даты=Созданы; Период=За все время; Воронка=Привлечение (2 месяца); Теги=машэкспо"
    block, inputs = parse_dsl_execution_inputs(text)
    assert block.display_name == "Машэкспо"
    assert len(inputs) == 1
    item = inputs[0]
    assert item.source_kind == "tag"
    assert item.filter_field == "tags"
    assert item.filter_operator == "="
    assert item.filter_value == "машэкспо"
    assert item.pipeline_name == "Привлечение (2 месяца)"


def test_parse_dsl_execution_inputs_utm_exact_and_prefix() -> None:
    text = (
        "Комбо: Даты=Созданы; Период=За все время; utm_source=conf_exact || "
        "Даты=Созданы; Период=За все время; utm_source^=conf_"
    )
    block, inputs = parse_dsl_execution_inputs(text)
    assert block.display_name == "Комбо"
    assert len(inputs) == 2
    assert inputs[0].source_kind == "utm_exact"
    assert inputs[0].filter_operator == "="
    assert inputs[0].filter_value == "conf_exact"
    assert inputs[1].source_kind == "utm_prefix"
    assert inputs[1].filter_operator == "^="
    assert inputs[1].filter_value == "conf_"


def test_parse_dsl_execution_inputs_tabs_override() -> None:
    text = "Тест: utm_source=conf_exact; tabs=all|closed"
    _block, inputs = parse_dsl_execution_inputs(text)
    assert len(inputs) == 1
    assert inputs[0].source_kind == "utm_exact"
    assert inputs[0].tabs == ["all", "closed"]


def test_parse_dsl_execution_inputs_date_values_are_canonical() -> None:
    text = "Тест: Даты=Созданы; Период=За все время; utm_source=conf_exact"
    _block, inputs = parse_dsl_execution_inputs(text)
    assert len(inputs) == 1
    assert inputs[0].date_mode == "created"
    assert inputs[0].period == "all_time"
