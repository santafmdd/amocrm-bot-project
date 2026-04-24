import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace

if "playwright" not in sys.modules:
    playwright_module = types.ModuleType("playwright")
    playwright_sync_api = types.ModuleType("playwright.sync_api")
    playwright_sync_api.Locator = object
    playwright_sync_api.Page = object
    playwright_sync_api.Browser = object
    playwright_sync_api.BrowserContext = object
    playwright_sync_api.Playwright = object
    playwright_sync_api.TimeoutError = Exception
    playwright_sync_api.sync_playwright = lambda: None
    sys.modules["playwright"] = playwright_module
    sys.modules["playwright.sync_api"] = playwright_sync_api

from src.run_profile_analytics import _run_api_layout_batch_from_sheet_dsl
from src.writers.models import WriterDestinationConfig


class _FakeLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None


class _FakeInspector:
    def __init__(self, *args, **kwargs):
        pass

    def inspect(self, destination):
        return {
            "anchors": [
                {
                    "dsl_row": 1,
                    "dsl_col": 1,
                    "dsl_text": "Тест: Даты=Созданы; Период=За все время; utm_source=conf_exact",
                    "header_row": 3,
                    "stage_col": 1,
                    "all_col": 2,
                    "active_col": 3,
                    "closed_col": 4,
                }
            ]
        }


class _FakeScenarioExecutor:
    def __init__(self, *args, **kwargs):
        pass

    def execute_block_scenarios(self, page, block_config):
        compiled = SimpleNamespace(report_id="rid", source_kind="utm_source")
        best_scenario = SimpleNamespace(apply_confirmed_but_parse_suspicious=False)
        return SimpleNamespace(best_compiled_result=compiled, best_scenario=best_scenario)


class _FakeApiWriter:
    calls: list[bool] = []

    def __init__(self, *args, **kwargs):
        pass

    def write_profile_analytics_result(self, compiled_result, destination, dry_run=False, target_dsl_row=None, target_dsl_cell=None):
        self.__class__.calls.append(bool(dry_run))
        if dry_run:
            return {"dry_run": True, "planned_updates": 9, "artifact": "x"}
        return {"totalUpdatedCells": 9}



def _make_config(base_path: Path):
    return SimpleNamespace(project_root=base_path, exports_dir=base_path / "exports")


def test_batch_from_sheet_dsl_dry_run_does_not_use_live_writer(monkeypatch):
    base = Path('tests') / 'tmp_batch_contract'
    if base.exists():
        import shutil
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    config = _make_config(base)
    config.exports_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("src.run_profile_analytics.GoogleSheetsApiLayoutInspector", _FakeInspector)
    monkeypatch.setattr("src.run_profile_analytics.ScenarioExecutor", _FakeScenarioExecutor)
    monkeypatch.setattr("src.run_profile_analytics.GoogleSheetsApiLayoutWriter", _FakeApiWriter)
    monkeypatch.setattr("src.run_profile_analytics.save_compiled_result_json", lambda **kwargs: base / "compiled.json")
    monkeypatch.setattr("src.run_profile_analytics.compile_stage_pivot", lambda **kwargs: {"stage": {"all": 1, "active": 1, "closed": 0}})
    monkeypatch.setattr("src.run_profile_analytics.save_stage_pivot_json", lambda **kwargs: base / "pivot.json")

    _FakeApiWriter.calls = []
    destination = WriterDestinationConfig(
        kind="google_sheets_layout_ui",
        target_id="x",
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="layout_anchor_update",
        start_cell="A1",
        layout_config={},
    )

    _run_api_layout_batch_from_sheet_dsl(
        config=config,
        logger=_FakeLogger(),
        page=object(),
        flow=object(),
        report=SimpleNamespace(id="rid"),
        tabs=["all", "active", "closed"],
        destination=destination,
        source_kind="utm_source",
        filter_values=["x"],
        dry_run=True,
    )

    assert _FakeApiWriter.calls == [True]

    summaries = sorted((config.exports_dir / "debug").glob("layout_api_batch_from_sheet_dsl_summary_*.json"))
    assert summaries
    payload = json.loads(summaries[-1].read_text(encoding="utf-8"))
    assert payload["dry_run"] is True
    assert payload["rows"][0]["status"] == "dry_run_planned"
    assert payload["rows"][0]["updated_cells_count"] == 0
    assert payload["rows"][0]["planned_updates"] == 9


    # cleanup
    import shutil
    shutil.rmtree(base, ignore_errors=True)


def test_batch_marks_manual_review_required_on_suspicious_unmapped_stage_row(monkeypatch):
    base = Path('tests') / 'tmp_batch_contract_suspicious'
    if base.exists():
        import shutil
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    config = _make_config(base)
    config.exports_dir.mkdir(parents=True, exist_ok=True)

    class _SuspiciousScenarioExecutor:
        def __init__(self, *args, **kwargs):
            pass

        def execute_block_scenarios(self, page, block_config):
            compiled = SimpleNamespace(report_id="rid", source_kind="utm_source")
            best_scenario = SimpleNamespace(apply_confirmed_but_parse_suspicious=True)
            return SimpleNamespace(best_compiled_result=compiled, best_scenario=best_scenario)

    monkeypatch.setattr("src.run_profile_analytics.GoogleSheetsApiLayoutInspector", _FakeInspector)
    monkeypatch.setattr("src.run_profile_analytics.ScenarioExecutor", _SuspiciousScenarioExecutor)
    monkeypatch.setattr("src.run_profile_analytics.GoogleSheetsApiLayoutWriter", _FakeApiWriter)
    monkeypatch.setattr("src.run_profile_analytics.save_compiled_result_json", lambda **kwargs: base / "compiled.json")
    monkeypatch.setattr("src.run_profile_analytics.compile_stage_pivot", lambda **kwargs: {"stage": {"all": 1, "active": 1, "closed": 0}})
    monkeypatch.setattr("src.run_profile_analytics.save_stage_pivot_json", lambda **kwargs: base / "pivot.json")
    monkeypatch.setattr(
        "src.run_profile_analytics._get_latest_api_layout_summary",
        lambda _exports_dir: (
            base / "exports" / "debug" / "layout_api_write_summary_mock.json",
            {
                "stage_rows_selected_count": 0,
                "stop_reason": "unmapped_stage_row",
            },
        ),
    )
    monkeypatch.setattr("src.run_profile_analytics._log_latest_api_layout_summary", lambda *args, **kwargs: None)

    destination = WriterDestinationConfig(
        kind="google_sheets_layout_ui",
        target_id="x",
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="layout_anchor_update",
        start_cell="A1",
        layout_config={},
    )

    _run_api_layout_batch_from_sheet_dsl(
        config=config,
        logger=_FakeLogger(),
        page=object(),
        flow=object(),
        report=SimpleNamespace(id="rid"),
        tabs=["all", "active", "closed"],
        destination=destination,
        source_kind="utm_source",
        filter_values=["x"],
        dry_run=False,
    )

    summaries = sorted((config.exports_dir / "debug").glob("layout_api_batch_from_sheet_dsl_summary_*.json"))
    assert summaries
    payload = json.loads(summaries[-1].read_text(encoding="utf-8"))
    row = payload["rows"][0]
    assert row["status"] == "manual_review_required"
    assert row["suspicious_parse_skip"] is True
    assert row["block_status"] == "manual_review_required"
    assert row["skip_reason"] == "suspicious_parse_unmapped_stage_row"
    assert payload["manual_review_required_count"] == 1
    assert payload["successes"] == 0

    import shutil
    shutil.rmtree(base, ignore_errors=True)

