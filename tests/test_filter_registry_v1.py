from pathlib import Path
import sys
import types
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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

from src.browser.analytics_flow import AnalyticsFlow
from src.browser.filters import FilterRegistry
from src.browser.filters.base import FilterDebugContext


class _DummyReader:
    settings = SimpleNamespace(timeout_ms=1000)


class _HandlerOK:
    name = "ok"

    def __init__(self):
        self.applied = False
        self.verified = False

    def resolve(self, flow, page, report_id, values, operator="="):
        return {}

    def apply(self, flow, page, report_id, values, operator="="):
        self.applied = True
        return True

    def verify(self, flow, page, report_id, values, operator="="):
        self.verified = True
        return True

    def debug_dump(self, flow, page, report_id, reason, extra=None):
        return FilterDebugContext(diagnostics={"reason": reason}, artifacts={})


class _HandlerVerifyFail(_HandlerOK):
    def verify(self, flow, page, report_id, values, operator="="):
        self.verified = True
        return False


def test_filter_registry_contains_supported_v1_keys():
    registry = FilterRegistry()
    keys = set(registry.keys())
    assert {"tag", "pipeline", "date", "manager", "utm_source", "utm_exact", "utm_prefix"}.issubset(keys)


def test_apply_supported_filter_success(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    handler = _HandlerOK()
    monkeypatch.setattr(flow._filter_registry, "get", lambda key: handler)

    ok = flow._apply_supported_filter(
        page=object(),
        report_id="analytics_tag_single_example",
        filter_key="tag",
        values=["value"],
        operator="=",
    )

    assert ok is True
    assert handler.applied is True
    assert handler.verified is True


def test_apply_supported_filter_verify_fail(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    handler = _HandlerVerifyFail()
    monkeypatch.setattr(flow._filter_registry, "get", lambda key: handler)

    ok = flow._apply_supported_filter(
        page=object(),
        report_id="analytics_tag_single_example",
        filter_key="tag",
        values=["value"],
        operator="=",
    )

    assert ok is False
    assert handler.applied is True
    assert handler.verified is True
