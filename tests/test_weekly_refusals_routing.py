import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

if "playwright" not in sys.modules:
    import types

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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.run_profile_analytics import _build_weekly_refusals_flow_input, _resolve_weekly_period_mode, _run_weekly_refusals_profile, _resolve_google_auth_mode, _apply_google_auth_mode_override, RuntimeOptions


def u(s: str) -> str:
    return s.encode("ascii").decode("unicode_escape")


def test_build_weekly_refusals_flow_input_maps_filters() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "date_mode": "created_mode",
            "period_mode": "week_mode",
            "period_strategy": "ui_period_control",
            "status_after": "closed_not_realized",
            "status_before": "qualification",
            "status_before_values": ["qualification", "first_contact"],
            "entity_kind": "deals",
            "event_type": "stage_change",
            "managers": [],
        },
    )

    flow_input = _build_weekly_refusals_flow_input(report)
    assert flow_input.pipeline_name == "pipeline_2m"
    assert flow_input.date_mode == "created_mode"
    assert flow_input.period_mode == "week_mode"
    assert flow_input.period_strategy == "raw=ui_period_control|resolved=profile_config|mode=profile_config"
    assert flow_input.status_after == "closed_not_realized"
    assert flow_input.status_before == "qualification"
    assert flow_input.status_before_values == ["qualification", "first_contact"]


def test_build_weekly_refusals_flow_input_requires_pipeline() -> None:
    report = SimpleNamespace(id="x", filters={})
    try:
        _build_weekly_refusals_flow_input(report)
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "requires filters.pipeline" in str(exc)
    assert raised


def test_build_weekly_refusals_flow_input_legacy_status_before_fallback() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "status_before": "qualification",
            "status_before_values": [],
            "status_after": "closed_not_realized",
        },
    )

    flow_input = _build_weekly_refusals_flow_input(report)
    assert flow_input.status_before == "qualification"
    assert flow_input.status_before_values == []


def test_resolve_weekly_period_mode_sunday_previous_week() -> None:
    assert _resolve_weekly_period_mode(date(2026, 4, 12)) == u("\\u0417\\u0430 \\u043f\\u0440\\u043e\\u0448\\u043b\\u0443\\u044e \\u043d\\u0435\\u0434\\u0435\\u043b\\u044e")


def test_resolve_weekly_period_mode_monday_current_week() -> None:
    assert _resolve_weekly_period_mode(date(2026, 4, 13)) == u("\\u0417\\u0430 \\u044d\\u0442\\u0443 \\u043d\\u0435\\u0434\\u0435\\u043b\\u044e")


def test_resolve_weekly_period_mode_tuesday_previous_week() -> None:
    assert _resolve_weekly_period_mode(date(2026, 4, 14)) == u("\\u0417\\u0430 \\u043f\\u0440\\u043e\\u0448\\u043b\\u0443\\u044e \\u043d\\u0435\\u0434\\u0435\\u043b\\u044e")


def test_build_weekly_refusals_flow_input_saved_preset_mode() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "status_after": "closed_not_realized",
            "filter_mode": "saved_preset",
            "saved_preset_name": "weekly base",
            "saved_preset_exact_match": True,
        },
    )

    flow_input = _build_weekly_refusals_flow_input(report)
    assert flow_input.filter_mode == "saved_preset"
    assert flow_input.saved_preset_name == "weekly base"
    assert flow_input.saved_preset_exact_match is True


def test_build_weekly_refusals_flow_input_supports_current_week_strategy() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "period_strategy": "current_week",
            "status_after": "closed_not_realized",
        },
    )
    flow_input = _build_weekly_refusals_flow_input(report)
    assert flow_input.period_mode == u("\\u0417\\u0430 \\u044d\\u0442\\u0443 \\u043d\\u0435\\u0434\\u0435\\u043b\\u044e")
    assert "resolved=current_week" in flow_input.period_strategy


def test_build_weekly_refusals_flow_input_supports_previous_week_strategy() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "period_strategy": "previous_week",
            "status_after": "closed_not_realized",
        },
    )
    flow_input = _build_weekly_refusals_flow_input(report)
    assert flow_input.period_mode == u("\\u0417\\u0430 \\u043f\\u0440\\u043e\\u0448\\u043b\\u0443\\u044e \\u043d\\u0435\\u0434\\u0435\\u043b\\u044e")
    assert "resolved=previous_week" in flow_input.period_strategy




def test_run_weekly_refusals_profile_uses_parsed_to_dict_for_compiled_and_writer(monkeypatch) -> None:
    import json
    from src.writers.models import WriterDestinationConfig

    base = Path("d:/AI_Automation/tmp_weekly_refusals_runner")
    base.mkdir(parents=True, exist_ok=True)

    payload = {
        "report_id": "weekly_refusals_weekly_2m",
        "display_name": "Weekly",
        "source_rows": [{"x": 1}],
        "aggregated_before_status_counts": [{"status": "s1", "count": 2}],
        "aggregated_after_status_counts": [{"status": "a1", "count": 3}],
        "deal_refs": [],
        "mode": "weekly",
    }

    class _Parsed:
        def to_dict(self):
            return dict(payload)

    class _FakePage:
        pass

    class _FakeSession:
        def __init__(self, _settings):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def new_page(self):
            return _FakePage()

    class _FakeEventsFlow:
        def __init__(self, **_kwargs):
            pass

        def run_capture(self, **_kwargs):
            return [{"row": 1}]

    captured = {"parsed_result": None}

    class _FakeWriteResult:
        dry_run = True
        planned_updates = 1
        updated_cells = 0
        summary_path = base / "summary.json"

    class _FakeWriter:
        def __init__(self, **_kwargs):
            pass

        def write_block(self, *, destination, parsed_result, dry_run):
            captured["parsed_result"] = parsed_result
            return _FakeWriteResult()

    monkeypatch.setattr("src.run_profile_analytics.BrowserSession", _FakeSession)
    monkeypatch.setattr("src.run_profile_analytics.EventsFlow", _FakeEventsFlow)
    monkeypatch.setattr("src.run_profile_analytics.parse_weekly_refusals_rows", lambda **_kwargs: _Parsed())
    monkeypatch.setattr("src.run_profile_analytics.WeeklyRefusalsBlockWriter", _FakeWriter)

    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        display_name="Weekly",
        filters={
            "pipeline": "p1",
            "status_after": "??????? ? ?? ???????????",
            "mode": "weekly",
            "period_mode": "?? ??? ??????",
        },
    )
    config = SimpleNamespace(project_root=base, exports_dir=base / "exports")
    settings = SimpleNamespace()
    logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)

    destination = WriterDestinationConfig(
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="weekly_refusals_block_update",
        start_cell="A1",
        kind="google_sheets_ui",
        target_id="weekly_refusals_weekly_2m_block",
        layout_config={},
    )

    _run_weekly_refusals_profile(
        config=config,
        settings=settings,
        logger=logger,
        report=report,
        destination=destination,
        wait_for_enter=False,
        weekly_dry_run=True,
    )

    compiled_dir = config.exports_dir / "compiled"
    artifacts = sorted(compiled_dir.glob("weekly_refusals_weekly_refusals_weekly_2m_*.json"))
    assert artifacts, "compiled artifact not created"
    compiled_payload = json.loads(artifacts[-1].read_text(encoding="utf-8"))
    assert compiled_payload.get("report_id") == payload["report_id"]
    assert compiled_payload.get("mode") == "weekly"
    assert captured["parsed_result"].get("report_id") == payload["report_id"]
    assert captured["parsed_result"].get("mode") == "weekly"


def test_google_auth_mode_cli_override(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_API_AUTH_MODE", "auto")
    mode = _apply_google_auth_mode_override("cache_only")
    assert mode == "cache_only"
    assert _resolve_google_auth_mode(None) == "cache_only"


def test_google_auth_mode_env_fallback(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_API_AUTH_MODE", "interactive_bootstrap")
    assert _resolve_google_auth_mode(None) == "interactive_bootstrap"


def test_run_weekly_refusals_profile_propagates_cumulative_mode(monkeypatch) -> None:
    from src.writers.models import WriterDestinationConfig

    base = Path("d:/AI_Automation/tmp_weekly_refusals_runner_cumulative")
    base.mkdir(parents=True, exist_ok=True)

    payload = {
        "report_id": "weekly_refusals_cumulative_2m",
        "display_name": "Weekly Cumulative",
        "source_rows": [{"x": 1}],
        "aggregated_before_status_counts": [{"status": "s1", "count": 2}],
        "aggregated_after_status_counts": [{"status": "a1", "count": 3}],
        "deal_refs": [],
    }

    class _Parsed:
        def to_dict(self):
            return dict(payload)

    class _FakePage:
        pass

    class _FakeSession:
        def __init__(self, _settings):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def new_page(self):
            return _FakePage()

    class _FakeEventsFlow:
        def __init__(self, **_kwargs):
            pass

        def run_capture(self, **_kwargs):
            return [{"row": 1}]

    captured = {"parsed_result": None}

    class _FakeWriteResult:
        dry_run = True
        planned_updates = 1
        updated_cells = 0
        summary_path = base / "summary.json"

    class _FakeWriter:
        def __init__(self, **_kwargs):
            pass

        def write_block(self, *, destination, parsed_result, dry_run):
            captured["parsed_result"] = parsed_result
            return _FakeWriteResult()

    monkeypatch.setattr("src.run_profile_analytics.BrowserSession", _FakeSession)
    monkeypatch.setattr("src.run_profile_analytics.EventsFlow", _FakeEventsFlow)
    monkeypatch.setattr("src.run_profile_analytics.parse_weekly_refusals_rows", lambda **_kwargs: _Parsed())
    monkeypatch.setattr("src.run_profile_analytics.WeeklyRefusalsBlockWriter", _FakeWriter)

    report = SimpleNamespace(
        id="weekly_refusals_cumulative_2m",
        display_name="Weekly Cumulative",
        filters={
            "pipeline": "p1",
            "status_after": "??????? ? ?? ???????????",
            "mode": "cumulative",
            "period_mode": "?? ??? ??????",
        },
    )
    config = SimpleNamespace(project_root=base, exports_dir=base / "exports")
    settings = SimpleNamespace()
    logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)

    destination = WriterDestinationConfig(
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="weekly_refusals_block_update",
        start_cell="A1",
        kind="google_sheets_ui",
        target_id="weekly_refusals_cumulative_2m_block",
        layout_config={},
    )

    _run_weekly_refusals_profile(
        config=config,
        settings=settings,
        logger=logger,
        report=report,
        destination=destination,
        wait_for_enter=False,
        weekly_dry_run=True,
    )

    assert captured["parsed_result"] is not None
    assert captured["parsed_result"].get("mode") == "cumulative"
    assert captured["parsed_result"].get("writer_mode_semantics") == "recompute_from_source"


def test_run_weekly_refusals_profile_propagates_cumulative_add_existing_strategy(monkeypatch) -> None:
    from src.writers.models import WriterDestinationConfig

    base = Path("d:/AI_Automation/tmp_weekly_refusals_runner_cumulative_add")
    base.mkdir(parents=True, exist_ok=True)

    payload = {
        "report_id": "weekly_refusals_cumulative_2m",
        "display_name": "Weekly Cumulative",
        "source_rows": [{"x": 1}],
        "aggregated_before_status_counts": [{"status": "s1", "count": 2}],
        "aggregated_after_status_counts": [{"status": "a1", "count": 3}],
        "deal_refs": [],
    }

    class _Parsed:
        def to_dict(self):
            return dict(payload)

    class _FakePage:
        pass

    class _FakeSession:
        def __init__(self, _settings):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def new_page(self):
            return _FakePage()

    class _FakeEventsFlow:
        def __init__(self, **_kwargs):
            pass

        def run_capture(self, **_kwargs):
            return [{"row": 1}]

    captured = {"parsed_result": None}

    class _FakeWriteResult:
        dry_run = True
        planned_updates = 1
        updated_cells = 0
        summary_path = base / "summary.json"

    class _FakeWriter:
        def __init__(self, **_kwargs):
            pass

        def write_block(self, *, destination, parsed_result, dry_run):
            captured["parsed_result"] = parsed_result
            return _FakeWriteResult()

    monkeypatch.setattr("src.run_profile_analytics.BrowserSession", _FakeSession)
    monkeypatch.setattr("src.run_profile_analytics.EventsFlow", _FakeEventsFlow)
    monkeypatch.setattr("src.run_profile_analytics.parse_weekly_refusals_rows", lambda **_kwargs: _Parsed())
    monkeypatch.setattr("src.run_profile_analytics.WeeklyRefusalsBlockWriter", _FakeWriter)

    report = SimpleNamespace(
        id="weekly_refusals_cumulative_2m",
        display_name="Weekly Cumulative",
        filters={
            "pipeline": "p1",
            "status_after": "x",
            "mode": "cumulative",
            "period_mode": "?? ??? ??????",
            "cumulative_write_strategy": "add_existing_values",
        },
    )
    config = SimpleNamespace(project_root=base, exports_dir=base / "exports")
    settings = SimpleNamespace()
    logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)

    destination = WriterDestinationConfig(
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="weekly_refusals_block_update",
        start_cell="A1",
        kind="google_sheets_ui",
        target_id="weekly_refusals_cumulative_2m_block",
        layout_config={},
    )

    _run_weekly_refusals_profile(
        config=config,
        settings=settings,
        logger=logger,
        report=report,
        destination=destination,
        wait_for_enter=False,
        weekly_dry_run=True,
    )

    assert captured["parsed_result"] is not None
    assert captured["parsed_result"].get("mode") == "cumulative"
    assert captured["parsed_result"].get("cumulative_write_strategy") == "add_existing_values"
    assert captured["parsed_result"].get("writer_mode_semantics") == "cumulative_add_existing_values"
    assert captured["parsed_result"].get("period_key")


def test_build_weekly_refusals_flow_input_runtime_strategy_override() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "period_strategy": "previous_week",
            "status_after": "closed_not_realized",
        },
    )
    options = RuntimeOptions(
        google_auth_mode="cache_only",
        weekly_period_strategy_override="current_week",
    )
    flow_input = _build_weekly_refusals_flow_input(report, runtime_options=options)
    assert "resolved=current_week" in flow_input.period_strategy


def test_build_weekly_refusals_flow_input_runtime_manual_period_override() -> None:
    report = SimpleNamespace(
        id="weekly_refusals_weekly_2m",
        filters={
            "pipeline": "pipeline_2m",
            "period_strategy": "auto_weekly",
            "status_after": "closed_not_realized",
        },
    )
    options = RuntimeOptions(
        google_auth_mode="cache_only",
        weekly_period_mode_override="?? ??????",
        weekly_date_from_override="2026-04-01",
        weekly_date_to_override="2026-04-07",
    )
    flow_input = _build_weekly_refusals_flow_input(report, runtime_options=options)
    assert flow_input.period_mode == "?? ??????"
    assert flow_input.date_from == "2026-04-01"
    assert flow_input.date_to == "2026-04-07"

def test_run_weekly_refusals_profile_zero_result_writes_block(monkeypatch) -> None:
    from src.writers.models import WriterDestinationConfig

    base = Path("d:/AI_Automation/tmp_weekly_refusals_runner_zero")
    base.mkdir(parents=True, exist_ok=True)

    class _Parsed:
        def to_dict(self):
            return {
                "report_id": "weekly_refusals_weekly_long",
                "display_name": "Weekly long",
                "source_rows": [],
                "aggregated_before_status_counts": [],
                "aggregated_after_status_counts": [],
                "deal_refs": [],
            }

    class _FakePage:
        pass

    class _FakeSession:
        def __init__(self, _settings):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def new_page(self):
            return _FakePage()

    class _FakeEventsFlow:
        def __init__(self, **_kwargs):
            pass

        def run_capture(self, **_kwargs):
            return []

    captured = {"parsed_result": None}

    class _FakeWriteResult:
        dry_run = True
        planned_updates = 0
        updated_cells = 0
        summary_path = base / "summary.json"

    class _FakeWriter:
        def __init__(self, **_kwargs):
            pass

        def write_block(self, *, destination, parsed_result, dry_run):
            captured["parsed_result"] = parsed_result
            return _FakeWriteResult()

    monkeypatch.setattr("src.run_profile_analytics.BrowserSession", _FakeSession)
    monkeypatch.setattr("src.run_profile_analytics.EventsFlow", _FakeEventsFlow)
    monkeypatch.setattr("src.run_profile_analytics.parse_weekly_refusals_rows", lambda **_kwargs: _Parsed())
    monkeypatch.setattr("src.run_profile_analytics.WeeklyRefusalsBlockWriter", _FakeWriter)

    report = SimpleNamespace(
        id="weekly_refusals_weekly_long",
        display_name="Weekly long",
        filters={
            "pipeline": "p_long",
            "status_after": "x",
            "mode": "weekly",
            "period_mode": "За прошлую неделю",
        },
    )
    config = SimpleNamespace(project_root=base, exports_dir=base / "exports")
    settings = SimpleNamespace()
    logger = SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None, error=lambda *a, **k: None)

    destination = WriterDestinationConfig(
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="weekly_refusals_block_update",
        start_cell="A1",
        kind="google_sheets_ui",
        target_id="weekly_refusals_weekly_long_block",
        layout_config={},
    )

    _run_weekly_refusals_profile(
        config=config,
        settings=settings,
        logger=logger,
        report=report,
        destination=destination,
        wait_for_enter=False,
        weekly_dry_run=True,
    )

    assert captured["parsed_result"] is not None
    assert captured["parsed_result"].get("source_rows") == []
