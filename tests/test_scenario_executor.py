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


def test_unsupported_filter_field_logs_warning_and_does_not_crash(caplog):
    cfg = parse_layout_row("X: UnknownField=abc; ????=????????")
    ex = ScenarioExecutor(flow=_DummyFlowForApply(), project_root=Path('.'), tabs=['all'], report_id='rid')
    ex._apply_non_primary_filters(page=None, scenario=cfg.scenarios[0], primary_kind='tag')
    assert "unsupported dsl filter field" in caplog.text.lower()



def test_execute_one_scenario_passes_primary_operator_to_flow():
    cfg = parse_layout_row("????: utm_source^=conf_msk_light_industry_2026")

    class _Flow:
        def __init__(self):
            self.tag_selection_mode = "script"
            self.reader = type("_R", (), {
                "open_analytics_page": staticmethod(lambda _page: None),
                "build_tab_mode_url": staticmethod(lambda base, tab: base),
            })()
            self.captured_operator = None

        def _open_filter_panel(self, _page):
            return None

        def _select_filter_kind(self, _page, _source_kind, _report_id):
            return None

        def _apply_filter_values(self, _page, _report_id, _source_kind, _values, operator="="):
            self.captured_operator = operator

        def _click_apply(self, _page):
            return None

        def _wait_after_apply(self, _page):
            return None

        def _wait_for_tab_content_ready(self, _page, _tab):
            return True

        def _read_tab_with_stability_retries(self, _page, _profile, tab, precheck_ready=True):
            from src.browser.models import AnalyticsSnapshot, StageCount
            return AnalyticsSnapshot(
                source_kind="utm_source",
                filter_id="x",
                tab_mode=tab,
                captured_at="2026-01-01T00:00:00",
                total_count=1,
                stages=[StageCount(stage_name="stage", count=1)],
                top_cards={},
                parse_method="test",
                parse_debug={},
                raw_lines=[],
            )

        def _debug_screenshot(self, _page, _name):
            return None

    class _Page:
        url = "https://example.test"
        def wait_for_timeout(self, _ms):
            return None
        def goto(self, _url, wait_until="domcontentloaded"):
            return None

    flow = _Flow()
    ex = ScenarioExecutor(flow=flow, project_root=Path('.'), tabs=['all'], report_id='rid')
    ex._save_scenario_debug_result = lambda *args, **kwargs: None
    from types import SimpleNamespace
    ex._capture_tabs_from_current_view = lambda *_args, **_kwargs: [
        SimpleNamespace(tab_mode="all", total_count=1, stages=[SimpleNamespace(count=1)])
    ]
    result = ex._execute_one_scenario(_Page(), "block", 0, cfg.scenarios[0])
    assert result.success is True
    assert flow.captured_operator == "^="


def test_apply_non_primary_filters_raises_when_pipeline_apply_fails():
    cfg = parse_layout_row("X: tags=mashexpo; pipeline=Sales")

    class _Flow:
        tag_selection_mode = "script"
        reader = type("_R", (), {"open_analytics_page": staticmethod(lambda _page: None)})()
        def _apply_supported_filter(self, _page, _report_id, key, values, operator="="):
            if key == "pipeline":
                return False
            return True

    ex = ScenarioExecutor(flow=_Flow(), project_root=Path('.'), tabs=['all'], report_id='rid')
    try:
        ex._apply_non_primary_filters(page=None, scenario=cfg.scenarios[0], primary_kind='tag')
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "Scenario filter apply failed: field=pipeline" in str(exc)
    assert raised


def test_secondary_handler_key_resolution_keeps_primary_source_excluded():
    ex = ScenarioExecutor(flow=_DummyFlowForApply(), project_root=Path('.'), tabs=['all'], report_id='rid')
    assert ex._resolve_secondary_handler_key(field='utm_source', primary_kind='tag') == 'utm_as_secondary'
    assert ex._resolve_secondary_handler_key(field='tags', primary_kind='utm_source') == 'tag_as_secondary'
    assert ex._resolve_secondary_handler_key(field='pipeline', primary_kind='tag') == 'pipeline'
