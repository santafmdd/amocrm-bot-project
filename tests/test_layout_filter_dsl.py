import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import unittest

from src.writers.layout_filter_dsl import (
    ScenarioRunResult,
    normalize_field_name,
    parse_layout_row,
    select_best_scenario,
)


class TestLayoutFilterDsl(unittest.TestCase):
    def test_parse_single_scenario(self):
        cfg = parse_layout_row("Машэкспо: Даты=Созданы; Период=За все время; Теги=машэкспо")
        self.assertEqual(cfg.display_name, "Машэкспо")
        self.assertEqual(len(cfg.scenarios), 1)
        self.assertEqual(len(cfg.scenarios[0].filters), 3)

    def test_parse_two_scenarios_with_double_pipe(self):
        cfg = parse_layout_row(
            "Блок: Теги=машэкспо || UTM Source^=conf_novosib_"
        )
        self.assertEqual(len(cfg.scenarios), 2)
        self.assertEqual(cfg.scenarios[1].filters[0].operator, "^=")

    def test_values_split_by_single_pipe(self):
        cfg = parse_layout_row("Все 3: Теги=машэкспо|инглегмаш-2026|уралстрой-2026")
        values = cfg.scenarios[0].filters[0].values
        self.assertEqual(values, ["машэкспо", "инглегмаш-2026", "уралстрой-2026"])

    def test_raw_and_normalized_field(self):
        cfg = parse_layout_row("X: UTM Source=abc")
        f = cfg.scenarios[0].filters[0]
        self.assertEqual(f.raw_field_name, "UTM Source")
        self.assertEqual(f.normalized_field_name, "utm_source")

    def test_normalize_field_name(self):
        self.assertEqual(normalize_field_name("Теги"), "tags")
        self.assertEqual(normalize_field_name("utm source"), "utm_source")
        self.assertEqual(normalize_field_name("Воронка"), "pipeline")

    def test_normalize_field_name_canonical_dates_from_to(self):
        self.assertEqual(normalize_field_name("\u0421"), "date_from")
        self.assertEqual(normalize_field_name("\u041f\u043e"), "date_to")
        self.assertEqual(normalize_field_name("utm_source"), "utm_source")


    def test_normalize_field_name_dates_mode_variants(self):
        self.assertEqual(normalize_field_name("dates_mode"), "dates_mode")
        self.assertEqual(normalize_field_name("dates mode"), "dates_mode")

    def test_select_best_scenario(self):
        winner = select_best_scenario(
            [
                ScenarioRunResult(0, True, total_count=10, non_empty_stage_rows=7),
                ScenarioRunResult(1, True, total_count=12, non_empty_stage_rows=5),
                ScenarioRunResult(2, True, total_count=12, non_empty_stage_rows=8),
            ]
        )
        self.assertEqual(winner.scenario_index, 2)

    def test_select_first_if_equal(self):
        winner = select_best_scenario(
            [
                ScenarioRunResult(0, True, total_count=10, non_empty_stage_rows=5),
                ScenarioRunResult(1, True, total_count=10, non_empty_stage_rows=5),
            ]
        )
        self.assertEqual(winner.scenario_index, 0)


if __name__ == "__main__":
    unittest.main()
