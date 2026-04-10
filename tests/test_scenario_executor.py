import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import unittest

from src.writers.layout_filter_dsl import parse_layout_row
from src.analytics.scenario_executor import ScenarioExecutionResult, ScenarioExecutor


class _DummyFlow:
    pass


class _TestExecutor(ScenarioExecutor):
    def __init__(self, prepared_results):
        super().__init__(flow=_DummyFlow(), project_root=Path('.'), tabs=['all','active','closed'], report_id='test_report')
        self.prepared_results = prepared_results

    def _execute_one_scenario(self, page, block_display_name, scenario_index, scenario):
        return self.prepared_results[scenario_index]


class TestScenarioExecutor(unittest.TestCase):
    def test_best_scenario_selection_from_results(self):
        cfg = parse_layout_row(
            "Блок: Теги=машэкспо || UTM Source^=conf_novo_"
        )
        prepared = [
            ScenarioExecutionResult(
                scenario_index=0,
                raw_text=cfg.scenarios[0].raw_text,
                normalized_filters=[],
                success=True,
                error="",
                snapshots=[],
                total_count=20,
                non_empty_stage_rows=7,
            ),
            ScenarioExecutionResult(
                scenario_index=1,
                raw_text=cfg.scenarios[1].raw_text,
                normalized_filters=[],
                success=True,
                error="",
                snapshots=[],
                total_count=22,
                non_empty_stage_rows=4,
            ),
        ]
        ex = _TestExecutor(prepared)
        result = ex.execute_block_scenarios(page=None, block_config=cfg)  # type: ignore[arg-type]
        self.assertIsNotNone(result.best_scenario)
        self.assertEqual(result.best_scenario.scenario_index, 1)

    def test_first_successful_if_equal_scores(self):
        cfg = parse_layout_row("Блок: Теги=машэкспо || Теги=инглегмаш")
        prepared = [
            ScenarioExecutionResult(0, cfg.scenarios[0].raw_text, [], True, "", [], 10, 5),
            ScenarioExecutionResult(1, cfg.scenarios[1].raw_text, [], True, "", [], 10, 5),
        ]
        ex = _TestExecutor(prepared)
        result = ex.execute_block_scenarios(page=None, block_config=cfg)  # type: ignore[arg-type]
        self.assertEqual(result.best_scenario.scenario_index, 0)


if __name__ == '__main__':
    unittest.main()


class _DummyFlowForApply:
    def __init__(self):
        self.tag_selection_mode = "script"
        self.reader = type("_R", (), {"open_analytics_page": staticmethod(lambda _page: None)})()

    def _apply_supported_filter(self, page, report_id, key, values, operator="="):
        return True


def test_unsupported_filter_field_raises_controlled_error():
    cfg = parse_layout_row("????: UnknownField=abc; ????=????????")
    ex = ScenarioExecutor(flow=_DummyFlowForApply(), project_root=Path('.'), tabs=['all'], report_id='rid')
    try:
        ex._apply_non_primary_filters(page=None, scenario=cfg.scenarios[0], primary_kind='tag')
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "Unsupported DSL filter for scenario execution" in str(exc)
        assert "unknownfield" in str(exc)
    assert raised
