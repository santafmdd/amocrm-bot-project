from pathlib import Path
import sys
import types

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

from src.browser.filters.date_filter import DateFilterHandler
from src.browser.filters.manager_filter import ManagerFilterHandler
from src.browser.filters.pipeline_filter import PipelineFilterHandler
from src.browser.filters.utm_filter import UTMFilterHandler


class _FakeLogger:
    def warning(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None


class _FakeRow:
    def __init__(self):
        self.clicked = False

    def click(self, timeout=0):
        self.clicked = True

    def locator(self, _selector):
        return _FakeList([_FakeRow()])


class _FakeList:
    def __init__(self, items):
        self._items = list(items)

    @property
    def first(self):
        return self._items[0]

    def count(self):
        return len(self._items)

    def is_visible(self, timeout=0):
        return True


class _FakePanel:
    def __init__(self):
        self._row = _FakeRow()

    def locator(self, _selector):
        return _FakeList([self._row])

    def inner_text(self, timeout=0):
        return "За все время Созданы Привлечение Иванов"

    def click(self, timeout=0):
        return None


class _FakeKeyboard:
    def __init__(self):
        self.typed = []
        self.pressed = []

    def type(self, value, delay=0):
        self.typed.append(value)

    def press(self, key):
        self.pressed.append(key)


class _FakePage:
    def __init__(self):
        self.keyboard = _FakeKeyboard()

    def wait_for_timeout(self, _ms):
        return None


class _FakeFlow:
    def __init__(self):
        self.panel = _FakePanel()
        self.logger = _FakeLogger()
        self.chosen = []
        self.applied = []
        self._holder = object()
        self.label_clicked = 0

    def _find_filter_panel_container(self, _page):
        return self.panel

    def _choose_option_text(self, _page, text):
        self.chosen.append(text)
        return True

    def _apply_utm_source_exact_values(self, page, panel, values, report_id):
        self.applied.append((tuple(values), report_id))

    def _find_utm_source_holder(self, panel):
        return self._holder

    def _has_utm_chip(self, panel, holder, target):
        return True

    def _normalize_filter_text(self, value):
        return str(value or "").strip().lower()

    def _collect_tag_chip_texts(self, panel, holder=None):
        return []

    def _resolve_utm_click_target_from_labels(self, page, panel, report_id):
        return None, None

    def _find_utm_label_item(self, panel):
        class _Label:
            def __init__(self, flow):
                self._flow = flow

            def click(self, timeout=0):
                self._flow.label_clicked += 1

        return _Label(self), {"text": "utm_source"}

    def _debug_screenshot(self, page, name):
        return None


def test_utm_filter_handler_exact_apply_and_verify():
    flow = _FakeFlow()
    page = _FakePage()
    handler = UTMFilterHandler(mode="exact")

    assert handler.apply(flow, page, "rid", ["conf_abc"]) is True
    assert flow.applied == [(("conf_abc",), "rid")]
    assert handler.verify(flow, page, "rid", ["conf_abc"]) is True


def test_pipeline_filter_handler_apply_smoke(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    handler = PipelineFilterHandler()

    monkeypatch.setattr(handler, "_find_row", lambda flow, panel, target: _FakeRow())

    assert handler.apply(flow, page, "rid", ["Привлечение"]) is True
    assert "Привлечение" in flow.chosen


def test_date_filter_handler_apply_smoke():
    flow = _FakeFlow()
    page = _FakePage()
    handler = DateFilterHandler()

    assert handler.apply(flow, page, "rid", ["Созданы", "За все время"]) is True
    assert flow.chosen[:2] == ["Созданы", "За все время"]


def test_manager_filter_handler_apply_smoke():
    flow = _FakeFlow()
    page = _FakePage()
    handler = ManagerFilterHandler()

    assert handler.apply(flow, page, "rid", ["Иванов"]) is True
    assert page.keyboard.typed == ["Иванов"]
    assert "Escape" in page.keyboard.pressed


def test_utm_filter_handler_verify_without_holder_uses_panel_text_fallback():
    flow = _FakeFlow()
    flow._holder = None
    page = _FakePage()
    handler = UTMFilterHandler(mode="exact")

    assert handler.verify(flow, page, "rid", ["Привлечение"]) is True


def test_utm_filter_handler_apply_activates_label_before_exact_when_holder_absent():
    flow = _FakeFlow()
    flow._holder = None
    page = _FakePage()
    handler = UTMFilterHandler(mode="exact")

    assert handler.apply(flow, page, "rid", ["conf_abc"]) is True
    assert flow.label_clicked == 1
    assert flow.applied == [(("conf_abc",), "rid")]


def test_utm_filter_handler_verify_accepts_direct_input_value_match():
    flow = _FakeFlow()
    page = _FakePage()
    handler = UTMFilterHandler(mode="exact")

    sentinel_row = object()
    sentinel_input = object()

    flow._resolve_utm_row_context = lambda **kwargs: (sentinel_row, None, {})
    flow._detect_utm_control_mode = lambda _row: "direct_text_input"
    flow._find_utm_direct_input = lambda _row: sentinel_input
    flow._read_input_value = lambda _input: "yandex"

    assert handler.verify(flow, page, "rid", ["yandex"]) is True
