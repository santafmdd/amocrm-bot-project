import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.compiled_artifacts import find_latest_compiled_artifact


class _FakeFile:
    def __init__(self, name: str, mtime: float) -> None:
        self.name = name
        self._mtime = mtime

    def is_file(self) -> bool:
        return True

    def stat(self):
        return SimpleNamespace(st_mtime=self._mtime)


def test_find_latest_compiled_artifact_prefers_report_id() -> None:
    files = [
        _FakeFile("compiled_stage_pivot_other_20260101_000000.json", 1.0),
        _FakeFile("compiled_stage_pivot_analytics_tag_single_example_20260101_000001.json", 2.0),
    ]

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.glob", side_effect=lambda pattern: files
    ):
        found = find_latest_compiled_artifact(
            exports_dir=Path("d:/any"),
            pattern="compiled_stage_pivot_*.json",
            report_id="analytics_tag_single_example",
        )

    assert found is not None
    assert found.name == "compiled_stage_pivot_analytics_tag_single_example_20260101_000001.json"


def test_find_latest_compiled_artifact_falls_back_to_any_when_no_report_match() -> None:
    files = [
        _FakeFile("compiled_profile_x_20260101_000000.json", 1.0),
        _FakeFile("compiled_profile_y_20260101_000001.json", 3.0),
    ]

    with patch("pathlib.Path.exists", return_value=True), patch(
        "pathlib.Path.glob", side_effect=lambda pattern: files
    ):
        found = find_latest_compiled_artifact(
            exports_dir=Path("d:/any"),
            pattern="compiled_profile_*.json",
            report_id="non_existing_report_id",
        )

    assert found is not None
    assert found.name == "compiled_profile_y_20260101_000001.json"

import types

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

from src.run_profile_analytics import (
    _map_dsl_source_to_flow,
    _resolve_layout_api_routing,
    _select_execution_anchor_from_discovery,
    _run_layout_writer_with_routing,
)
from src.writers.models import WriterDestinationConfig


def _make_destination(kind: str = "google_sheets_layout_ui", layout: dict | None = None) -> WriterDestinationConfig:
    return WriterDestinationConfig(
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        kind=kind,
        layout_config=layout or {},
    )


def test_resolve_layout_api_routing_backward_compatible_defaults() -> None:
    args = SimpleNamespace(
        writer_layout_api_preferred=False,
        writer_layout_api_write=False,
        writer_layout_api_dry_run=False,
        writer_layout_api_fallback_to_ui=False,
    )
    routing = _resolve_layout_api_routing(args=args, destination=_make_destination())
    assert routing["api_preferred"] is False
    assert routing["api_write_enabled"] is False
    assert routing["api_dry_run"] is False
    assert routing["api_fallback_to_ui"] is False


def test_resolve_layout_api_routing_api_preferred_and_dry_run() -> None:
    args = SimpleNamespace(
        writer_layout_api_preferred=True,
        writer_layout_api_write=False,
        writer_layout_api_dry_run=True,
        writer_layout_api_fallback_to_ui=False,
    )
    routing = _resolve_layout_api_routing(args=args, destination=_make_destination())
    assert routing["api_preferred"] is True
    assert routing["api_write_enabled"] is True
    assert routing["api_dry_run"] is True


class _DummyApiWriterSuccess:
    called = False

    def __init__(self, project_root, logger=None):
        self.project_root = project_root

    def write_profile_analytics_result(self, **kwargs):
        _DummyApiWriterSuccess.called = True
        return {"ok": True}


class _DummyApiWriterFail:
    called = False

    def __init__(self, project_root, logger=None):
        self.project_root = project_root

    def write_profile_analytics_result(self, **kwargs):
        _DummyApiWriterFail.called = True
        raise RuntimeError("api failed")


class _DummyUiWriter:
    called = False

    def __init__(self, project_root):
        self.project_root = project_root

    def write_profile_analytics_result(self, **kwargs):
        _DummyUiWriter.called = True
        return None


def _reset_writer_flags() -> None:
    _DummyApiWriterSuccess.called = False
    _DummyApiWriterFail.called = False
    _DummyUiWriter.called = False


def test_layout_writer_routing_api_preferred_dry_run_path() -> None:
    _reset_writer_flags()
    logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)
    config = SimpleNamespace(project_root=Path("d:/any"))
    flow = object()
    report = SimpleNamespace(id="analytics_tag_single_example")

    mode, fallback_used = _run_layout_writer_with_routing(
        logger=logger,
        config=config,
        page=object(),
        flow=flow,
        tabs=["all", "active", "closed"],
        report=report,
        compiled_result=object(),
        destination=_make_destination(),
        layout_dry_run=False,
        api_write_enabled=True,
        api_preferred=True,
        api_dry_run=True,
        api_fallback_to_ui=False,
        target_dsl_row=None,
        target_dsl_text_contains=None,
        target_dsl_cell=None,
        api_writer_factory=_DummyApiWriterSuccess,
        ui_writer_factory=_DummyUiWriter,
    )

    assert mode == "api_preferred"
    assert fallback_used is False
    assert _DummyApiWriterSuccess.called is True
    assert _DummyUiWriter.called is False


def test_layout_writer_routing_fallback_to_ui_on_api_failure() -> None:
    _reset_writer_flags()
    logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)
    config = SimpleNamespace(project_root=Path("d:/any"))
    flow = object()
    report = SimpleNamespace(id="analytics_tag_single_example")

    mode, fallback_used = _run_layout_writer_with_routing(
        logger=logger,
        config=config,
        page=object(),
        flow=flow,
        tabs=["all", "active", "closed"],
        report=report,
        compiled_result=object(),
        destination=_make_destination(),
        layout_dry_run=False,
        api_write_enabled=True,
        api_preferred=True,
        api_dry_run=False,
        api_fallback_to_ui=True,
        target_dsl_row=None,
        target_dsl_text_contains=None,
        target_dsl_cell=None,
        api_writer_factory=_DummyApiWriterFail,
        ui_writer_factory=_DummyUiWriter,
    )

    assert mode == "layout_ui_fallback"
    assert fallback_used is True
    assert _DummyApiWriterFail.called is True
    assert _DummyUiWriter.called is True







def test_select_execution_anchor_from_discovery_by_row() -> None:
    anchors = [
        {"dsl_row": 1, "dsl_col": 6, "dsl_text": "A-right"},
        {"dsl_row": 1, "dsl_col": 1, "dsl_text": "A-left"},
        {"dsl_row": 14, "dsl_col": 1, "dsl_text": "B"},
    ]
    selected = _select_execution_anchor_from_discovery(
        anchors,
        target_dsl_row=14,
        target_dsl_text_contains=None,
        target_dsl_cell=None,
    )
    assert selected is not None
    assert int(selected.get("dsl_row", 0)) == 14



def test_map_dsl_source_to_flow_utm_prefix() -> None:
    source_kind, operator = _map_dsl_source_to_flow("utm_prefix", "^=")
    assert source_kind == "utm_source"
    assert operator == "^="


def test_select_execution_anchor_from_discovery_by_cell_same_row() -> None:
    anchors = [
        {"dsl_row": 1, "dsl_col": 1, "dsl_text": "left"},
        {"dsl_row": 1, "dsl_col": 6, "dsl_text": "right"},
        {"dsl_row": 14, "dsl_col": 1, "dsl_text": "bottom"},
    ]
    selected = _select_execution_anchor_from_discovery(
        anchors,
        target_dsl_row=None,
        target_dsl_text_contains=None,
        target_dsl_cell="F1",
    )
    assert selected is not None
    assert int(selected.get("dsl_row", 0)) == 1
    assert int(selected.get("dsl_col", 0)) == 6






