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


class _DummyReader:
    settings = SimpleNamespace(timeout_ms=1000, exports_dir=Path.cwd() / "exports")


class _FakePage:
    def __init__(self):
        self.url = "https://officeistockinfo.amocrm.ru/stats/pipeline/?foo=1"
        self.wait_calls: list[int] = []

    def wait_for_timeout(self, ms):
        self.wait_calls.append(int(ms))

    def wait_for_load_state(self, _state, timeout=0):
        return None

    def locator(self, _selector):
        return _FakeLocatorList()


class _FakeLocatorList:
    def count(self):
        return 0

    def nth(self, _idx):
        raise IndexError


class _FakeApplyButton:
    def __init__(self, fail_normal=True, fail_force=True):
        self.fail_normal = fail_normal
        self.fail_force = fail_force
        self.click_calls: list[dict[str, object]] = []
        self.js_calls = 0

    def scroll_into_view_if_needed(self, timeout=0):
        return None

    def click(self, timeout=0, force=False):
        self.click_calls.append({"timeout": timeout, "force": force})
        if not force and self.fail_normal:
            raise RuntimeError("normal click intercepted")
        if force and self.fail_force:
            raise RuntimeError("force click intercepted")
        return None

    def evaluate(self, _script):
        self.js_calls += 1
        return None



def _make_flow(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    monkeypatch.setattr(flow, "_find_filter_panel_container", lambda _page: object())
    monkeypatch.setattr(flow, "_scroll_filter_panel_to_bottom", lambda _panel: None)
    monkeypatch.setattr(flow, "_collect_apply_scopes", lambda _page, _panel: [])
    monkeypatch.setattr(flow, "_debug_screenshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(flow, "_overlay_or_panel_closed", lambda _page, _panel: False)
    return flow


def test_dump_apply_button_diagnostics_returns_paths(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    debug_tmp = Path.cwd() / "exports" / "debug"
    monkeypatch.setattr(flow, "_debug_dir", lambda: debug_tmp)

    txt_path, json_path = flow._dump_apply_button_diagnostics(
        page=SimpleNamespace(url="https://example.test"),
        report_id="runtime",
        payloads=[{"selector": "#filter_apply", "text": "Применить"}],
    )

    assert isinstance(txt_path, Path)
    assert isinstance(json_path, Path)
    assert txt_path.name.endswith(".txt")
    assert json_path.name.endswith(".json")


def test_click_apply_in_panel_survives_diagnostics_failure(monkeypatch):
    flow = _make_flow(monkeypatch)
    page = _FakePage()

    monkeypatch.setattr(flow, "_find_apply_button", lambda *_args, **_kwargs: (None, "", "", [{"selector": "#x"}], None))

    def _boom(*_args, **_kwargs):
        raise RuntimeError("diag crashed")

    monkeypatch.setattr(flow, "_dump_apply_button_diagnostics", _boom)

    ok = flow._click_apply_in_panel(page=page, url_before=page.url, report_id="analytics_utm_layout_example")
    assert ok is False


def test_click_apply_in_panel_reaches_js_path_when_normal_and_force_fail(monkeypatch):
    flow = _make_flow(monkeypatch)
    page = _FakePage()
    btn = _FakeApplyButton(fail_normal=True, fail_force=True)

    monkeypatch.setattr(
        flow,
        "_find_apply_button",
        lambda *_args, **_kwargs: (
            btn,
            "#filter_apply",
            "panel",
            [{"selector": "#filter_apply", "text": "Применить", "score": 1000, "relevant": True}],
            {"is_exact_apply": True, "elementId": "filter_apply", "className": "filter__params_manage__apply", "text": "Применить", "score_reason": "id=filter_apply"},
        ),
    )

    checks = {"count": 0}

    def _confirm(_page, _url_before):
        checks["count"] += 1
        return checks["count"] >= 2

    monkeypatch.setattr(flow, "_is_filter_apply_confirmed_by_url", _confirm)

    ok = flow._click_apply_in_panel(page=page, url_before=page.url, report_id="analytics_utm_layout_example")

    assert ok is True
    assert len(btn.click_calls) == 2
    assert btn.click_calls[0]["force"] is False
    assert btn.click_calls[1]["force"] is True
    assert btn.js_calls == 1


def test_click_apply_in_panel_uses_polling_for_delayed_confirmation(monkeypatch):
    flow = _make_flow(monkeypatch)
    page = _FakePage()
    btn = _FakeApplyButton(fail_normal=False, fail_force=False)

    monkeypatch.setattr(
        flow,
        "_find_apply_button",
        lambda *_args, **_kwargs: (
            btn,
            "#filter_apply",
            "panel",
            [{"selector": "#filter_apply", "text": "Применить", "score": 1000, "relevant": True}],
            {"is_exact_apply": True, "elementId": "filter_apply", "className": "filter__params_manage__apply", "text": "Применить", "score_reason": "id=filter_apply"},
        ),
    )

    checks = {"count": 0}

    def _confirm(_page, _url_before):
        checks["count"] += 1
        return checks["count"] >= 5

    monkeypatch.setattr(flow, "_is_filter_apply_confirmed_by_url", _confirm)

    ok = flow._click_apply_in_panel(page=page, url_before=page.url, report_id="analytics_utm_layout_example")

    assert ok is True
    assert checks["count"] >= 5
    # polling waits should happen multiple times before confirmation
    assert len(page.wait_calls) >= 4
