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
    settings = SimpleNamespace(timeout_ms=1000)


def test_apply_filter_values_uses_utm_exact_branch(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())

    calls: dict[str, object] = {}

    def fake_apply_supported(*, page, report_id, filter_key, values, operator="="):
        calls["page"] = page
        calls["report_id"] = report_id
        calls["filter_key"] = filter_key
        calls["values"] = list(values)
        calls["operator"] = operator
        return True

    monkeypatch.setattr(flow, "_apply_supported_filter", fake_apply_supported)
    monkeypatch.setattr(flow, "_find_filter_panel_container", lambda _page: object())

    page = object()
    flow._apply_filter_values(
        page=page,
        report_id="analytics_utm_single_example",
        source_kind="utm_source",
        values=["yandex"],
    )

    assert calls["page"] is page
    assert calls["filter_key"] == "utm_source"
    assert calls["values"] == ["yandex"]
    assert calls["report_id"] == "analytics_utm_single_example"
    assert calls["operator"] == "="



def test_select_filter_kind_utm_source_uses_body_fallback(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())

    class _FakeBody:
        pass

    class _FakeInner:
        def count(self):
            return 0

        @property
        def first(self):
            return self

        def is_visible(self, timeout=0):
            return False

    class _FakeHolder:
        def __init__(self):
            self.clicked = 0

        def locator(self, _selector):
            return _FakeInner()

        def click(self, timeout=0):
            self.clicked += 1

    class _FakePage:
        def __init__(self, body):
            self._body = body

        def locator(self, selector):
            assert selector == "body"
            return self._body

        def wait_for_timeout(self, _ms):
            return None

    sentinel_panel = object()
    fake_body = _FakeBody()
    fake_holder = _FakeHolder()
    calls = {"panel": 0, "body": 0}

    def fake_find_panel(_page):
        return sentinel_panel

    def fake_find_holder(root):
        if root is sentinel_panel:
            calls["panel"] += 1
            return None
        if root is fake_body:
            calls["body"] += 1
            return fake_holder
        return None

    monkeypatch.setattr(flow, "_find_filter_panel_container", fake_find_panel)
    monkeypatch.setattr(flow, "_find_utm_source_holder", fake_find_holder)

    page = _FakePage(fake_body)
    flow._select_filter_kind(page=page, source_kind="utm_source", report_id="analytics_utm_single_example")

    assert calls["panel"] == 1
    assert calls["body"] == 1
    assert fake_holder.clicked == 1


def test_pick_utm_row_candidate_accepts_custom_settings_item_row():
    candidate = {
        "strategy": "self",
        "class_name": "filter__custom_settings__item",
        "text_preview": "utm_source",
        "strong_label": True,
        "has_control": True,
        "broad": False,
        "area": 12000.0,
    }
    selected = AnalyticsFlow._pick_utm_row_candidate([candidate])
    assert selected is not None
    assert selected["class_name"] == "filter__custom_settings__item"


def test_pick_utm_row_candidate_prefers_shared_parent_when_self_has_no_control():
    self_candidate = {
        "strategy": "self",
        "class_name": "filter__custom_settings__item",
        "text_preview": "utm_source",
        "strong_label": True,
        "has_control": False,
        "broad": False,
        "area": 6000.0,
    }
    shared_parent_candidate = {
        "strategy": "shared_parent",
        "class_name": "filter__custom_settings__row",
        "text_preview": "utm_source control",
        "strong_label": True,
        "has_control": True,
        "broad": False,
        "area": 10000.0,
    }
    selected = AnalyticsFlow._pick_utm_row_candidate([self_candidate, shared_parent_candidate])
    assert selected is not None
    assert selected["strategy"] == "shared_parent"


def test_select_filter_kind_utm_source_does_not_fail_when_label_presence_confirmed(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())

    class _FakeBody:
        pass

    class _FakePage:
        def __init__(self, body):
            self._body = body

        def locator(self, selector):
            assert selector == "body"
            return self._body

        def wait_for_timeout(self, _ms):
            return None

    sentinel_panel = object()
    fake_body = _FakeBody()

    monkeypatch.setattr(flow, "_find_filter_panel_container", lambda _page: sentinel_panel)
    monkeypatch.setattr(flow, "_find_utm_source_holder", lambda _root: None)
    monkeypatch.setattr(flow, "_resolve_utm_click_target_from_labels", lambda **kwargs: (None, None))
    monkeypatch.setattr(flow, "_find_utm_label_item", lambda _panel: (None, None))
    monkeypatch.setattr(flow, "_has_strong_utm_label_presence", lambda _panel: True)

    page = _FakePage(fake_body)
    # should not raise: select step delegates real open/apply to handler
    flow._select_filter_kind(page=page, source_kind="utm_source", report_id="analytics_utm_single_example")


class _FakeCountList:
    def __init__(self, items):
        self._items = list(items)

    def count(self):
        return len(self._items)

    def nth(self, idx):
        return self._items[idx]


class _FakeLabelItem:
    def __init__(self, text='utm_source', class_name='filter__custom_settings__item'):
        self._text = text
        self._class_name = class_name
        self.clicked = 0

    def is_visible(self, timeout=0):
        return True

    def click(self, timeout=0):
        self.clicked += 1

    def get_attribute(self, name):
        if name == 'data-input-name':
            return ''
        if name == 'data-title':
            return ''
        return ''


class _FakePanelForLabel:
    def __init__(self, item):
        self._item = item

    def locator(self, _selector):
        return _FakeCountList([self._item])


def test_find_utm_label_item_accepts_standalone_custom_settings_item(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    item = _FakeLabelItem()
    panel = _FakePanelForLabel(item)

    monkeypatch.setattr(
        flow,
        '_element_debug_payload',
        lambda _item: {
            'text': 'utm_source',
            'className': 'filter__custom_settings__item',
            'bbox': {'x': 1, 'y': 1, 'width': 10, 'height': 10},
        },
    )

    label_item, payload = flow._find_utm_label_item(panel)
    assert label_item is item
    assert payload is not None


def test_select_filter_kind_utm_source_clicks_label_item_without_early_fail(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())

    class _FakeBody:
        pass

    class _FakePage:
        def __init__(self, body):
            self._body = body

        def locator(self, selector):
            assert selector == 'body'
            return self._body

        def wait_for_timeout(self, _ms):
            return None

    sentinel_panel = object()
    fake_body = _FakeBody()
    fake_label_item = _FakeLabelItem()

    monkeypatch.setattr(flow, '_find_filter_panel_container', lambda _page: sentinel_panel)
    monkeypatch.setattr(flow, '_find_utm_source_holder', lambda _root: None)
    monkeypatch.setattr(flow, '_find_utm_label_item', lambda _panel: (fake_label_item, {'text': 'utm_source'}))
    monkeypatch.setattr(flow, '_resolve_utm_click_target_from_labels', lambda **kwargs: (None, None))

    page = _FakePage(fake_body)
    flow._select_filter_kind(page=page, source_kind='utm_source', report_id='analytics_utm_single_example')

    assert fake_label_item.clicked == 1


class _FakeKeyboard:
    def __init__(self):
        self.typed = []
        self.pressed = []

    def press(self, key):
        self.pressed.append(key)

    def type(self, value, delay=0):
        self.typed.append(value)


class _FakePageForDirect:
    def __init__(self):
        self.keyboard = _FakeKeyboard()

    def wait_for_timeout(self, _ms):
        return None


class _FakeDirectInput:
    def click(self, timeout=0):
        return None


def test_apply_utm_source_exact_values_direct_text_input_mode(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    page = _FakePageForDirect()
    panel = object()
    row = object()
    direct_input = _FakeDirectInput()

    monkeypatch.setattr(flow, '_resolve_utm_row_context', lambda **kwargs: (row, None, {'row_container': row}))
    monkeypatch.setattr(flow, '_detect_utm_control_mode', lambda _row: 'direct_text_input')
    monkeypatch.setattr(flow, '_find_utm_direct_input', lambda _row: direct_input)
    monkeypatch.setattr(flow, '_element_debug_payload', lambda _loc: {'tagName': 'input'})
    monkeypatch.setattr(flow, '_read_input_value', lambda _loc: 'yandex')

    flow._apply_utm_source_exact_values(page=page, panel=panel, values=['yandex'], report_id='rid')

    assert page.keyboard.typed == ['yandex']
    assert 'Escape' in page.keyboard.pressed


def test_resolve_utm_row_context_uses_row_container_from_payload(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    row = object()

    monkeypatch.setattr(flow, '_resolve_utm_click_target_from_labels', lambda **kwargs: (None, {'row_container': row}))
    monkeypatch.setattr(flow, '_find_utm_direct_input', lambda _row: object())
    monkeypatch.setattr(flow, '_element_debug_payload', lambda _loc: {'tagName': 'input'})
    monkeypatch.setattr(flow, '_locator_multisuggest_id', lambda _loc: '')

    resolved_row, resolved_target, _payload = flow._resolve_utm_row_context(page=object(), panel=object(), report_id='rid')

    assert resolved_row is row
    assert resolved_target is None


class _FakeDirectSelfInput:
    def evaluate(self, _script):
        return {'match': True}

    def locator(self, _selector):
        return _FakeCountList([])


class _FakeDirectSelfInputNoMatch:
    def evaluate(self, _script):
        return {'match': False}

    def locator(self, _selector):
        return _FakeCountList([])


def test_find_utm_direct_input_returns_row_container_self_when_input():
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    row = _FakeDirectSelfInput()
    resolved = flow._find_utm_direct_input(row)
    assert resolved is row


def test_detect_utm_control_mode_direct_when_row_is_input():
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())
    row = _FakeDirectSelfInput()
    mode = flow._detect_utm_control_mode(row)
    assert mode == 'direct_text_input'



def test_resolve_utm_row_context_prefers_payload_row_over_target_input(monkeypatch):
    flow = AnalyticsFlow(reader=_DummyReader(), project_root=Path.cwd())

    payload_row = object()
    target_input = object()

    monkeypatch.setattr(
        flow,
        '_resolve_utm_click_target_from_labels',
        lambda **kwargs: (target_input, {'row_container': payload_row}),
    )
    monkeypatch.setattr(flow, '_find_utm_direct_input', lambda row: object() if row is payload_row else None)
    monkeypatch.setattr(flow, '_element_debug_payload', lambda _loc: {'tagName': 'input'})
    monkeypatch.setattr(flow, '_locator_multisuggest_id', lambda _loc: '')

    resolved_row, resolved_target, _payload = flow._resolve_utm_row_context(page=object(), panel=object(), report_id='rid')

    assert resolved_row is payload_row
    assert resolved_target is target_input
