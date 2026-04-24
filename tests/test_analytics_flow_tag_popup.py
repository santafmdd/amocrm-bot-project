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

from src.browser.analytics_flow import AnalyticsFlow
from src.browser.filters import tag_filter


class _FakeLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None


class _FakeKeyboard:
    def __init__(self):
        self.pressed = []
        self.typed = []

    def press(self, key):
        self.pressed.append(key)

    def type(self, value, delay=0):
        self.typed.append((value, delay))


class _FakePage:
    def __init__(self):
        self.keyboard = _FakeKeyboard()
        self.url = "https://example.test/stats"

    def wait_for_timeout(self, _ms):
        return None


class _FakeNodeList:
    def __init__(self, nodes):
        self._nodes = list(nodes)

    def count(self):
        return len(self._nodes)

    @property
    def first(self):
        return self._nodes[0]


class _FakeHolder:
    def __init__(self):
        self._inner = _FakeNodeList([])

    def locator(self, _selector):
        return self._inner

    def click(self, timeout=0):
        return None

    def dblclick(self, timeout=0):
        return None

    def evaluate(self, _script):
        return None


class _FakePopup:
    def evaluate(self, _script):
        return "ms-1"


class _FakeInput:
    def __init__(self):
        self.value = ""

    def click(self, timeout=0):
        return None

    def input_value(self, timeout=0):
        return self.value

    def get_attribute(self, name):
        if name == "value":
            return self.value
        return ""


class _FakeFlow:
    def __init__(self):
        self.logger = _FakeLogger()
        self._apply_already_confirmed = False
        self.option_click_called = False

    def _find_strict_tag_holder(self, panel):
        return _FakeHolder(), "ms-1"

    def _click_locator_point(self, page, locator, px, py):
        return True

    def _holder_outer_html_snippet(self, holder, limit=3000):
        return "<holder/>"

    def _find_active_tag_popup(self, panel, holder):
        return _FakePopup(), {"id": "ms-1"}

    def _collect_visible_tag_suggestion_texts(self, panel, popup=None, expected_multisuggest_id=None):
        return ["????????"]

    def _select_real_tag_option(self, panel, value, holder=None, expected_multisuggest_id=None):
        self.option_click_called = True
        return True, "selector", {"text": value}

    def _save_tag_holder_after_enter_artifacts(self, holder, chip_texts, target_value):
        return Path("holder.html"), Path("holder.txt")

    def _click_apply_in_panel(self, page, url_before=None, report_id="runtime"):
        return True

    def _find_filter_panel_container(self, page):
        return _FakePanel(page)

    def _scroll_filter_panel_to_bottom(self, panel):
        return None

    def _is_filter_apply_confirmed_by_url(self, page, url_before):
        return True

    def _is_filter_apply_confirmed(self, page, panel, url_before, target_value):
        return True

    def _open_filter_panel(self, _page):
        return None


class _FakePanel:
    def __init__(self, page):
        self.page = page


def test_choose_active_popup_candidate_ignores_id_mismatch_and_prefers_nearest() -> None:
    holder_bbox = {"x": 100.0, "y": 400.0, "width": 320.0, "height": 60.0}
    popup_candidates = [
        {
            "multisuggest_id": "6863",
            "bbox": {"x": 90.0, "y": 390.0, "width": 360.0, "height": 180.0},
        },
        {
            "multisuggest_id": "9999",
            "bbox": {"x": 90.0, "y": 1200.0, "width": 360.0, "height": 180.0},
        },
    ]

    selected = AnalyticsFlow._choose_active_popup_candidate(holder_bbox, popup_candidates)

    assert selected is not None
    assert selected["multisuggest_id"] == "6863"


def test_choose_active_popup_candidate_returns_first_without_holder_bbox() -> None:
    popup_candidates = [
        {"multisuggest_id": "6863", "bbox": {"x": 1.0, "y": 1.0, "width": 10.0, "height": 10.0}},
        {"multisuggest_id": "7777", "bbox": {"x": 1.0, "y": 500.0, "width": 10.0, "height": 10.0}},
    ]

    selected = AnalyticsFlow._choose_active_popup_candidate(None, popup_candidates)

    assert selected is popup_candidates[0]


def test_tag_script_happy_path_value_reflected_and_chip_detected(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)
    fake_input = _FakeInput()

    monkeypatch.setattr(
        tag_filter,
        "_resolve_tag_input_strict",
        lambda **kwargs: (fake_input, "strict_popup_only", _FakePopup(), "ms-1", ""),
    )
    monkeypatch.setattr(
        tag_filter,
        "_wait_until_tag_value_reflected",
        lambda **kwargs: (True, {"mode": "input_value", "attempts": 1}),
    )
    monkeypatch.setattr(
        tag_filter,
        "_poll_chip_detect",
        lambda *args, **kwargs: (True, ["????????"]),
    )

    ok = tag_filter.apply_tag_values_via_holder_popup(
        flow=flow,
        page=page,
        panel=panel,
        report_id="analytics_tag_single_example",
        values=["????????"],
    )

    assert ok is True
    assert any(k == "Enter" for k in page.keyboard.pressed)


def test_tag_focus_fallback_requires_matching_multisuggest_id():
    payload = {"tagName": "input", "className": "multisuggest__input js-multisuggest-input"}
    valid, reason = tag_filter._is_focused_payload_valid(payload, focused_multisuggest_id="ms-x", holder_multisuggest_id="ms-1")
    assert valid is False
    assert reason == "focused_multisuggest_id_mismatch"

    valid2, reason2 = tag_filter._is_focused_payload_valid(payload, focused_multisuggest_id="ms-1", holder_multisuggest_id="ms-1")
    assert valid2 is True
    assert reason2 == ""


def test_tag_script_fallback_click_real_option_after_enter_no_chip(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)
    fake_input = _FakeInput()

    monkeypatch.setattr(
        tag_filter,
        "_resolve_tag_input_strict",
        lambda **kwargs: (fake_input, "strict_popup_only", _FakePopup(), "ms-1", ""),
    )
    monkeypatch.setattr(
        tag_filter,
        "_wait_until_tag_value_reflected",
        lambda **kwargs: (True, {"mode": "popup_suggestion", "attempts": 2}),
    )

    state = {"calls": 0}

    def fake_poll(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            return False, []
        return True, ["????????"]

    monkeypatch.setattr(tag_filter, "_poll_chip_detect", fake_poll)

    ok = tag_filter.apply_tag_values_via_holder_popup(
        flow=flow,
        page=page,
        panel=panel,
        report_id="analytics_tag_single_example",
        values=["????????"],
    )

    assert ok is True
    assert flow.option_click_called is True


def test_tag_handler_verify_accepts_url_marker_after_apply():
    flow = _FakeFlow()
    flow._apply_already_confirmed = True
    page = _FakePage()
    page.url = "https://officeistockinfo.amocrm.ru/stats/pipeline/?tag%5B%5D=278652&useFilter=y"

    handler = tag_filter.TagFilterHandler()
    assert handler.verify(flow, page, "analytics_tag_single_example", ["????????"]) is True


def test_duplicate_candidates_use_direct_click_not_arrowdown(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)
    fake_input = _FakeInput()
    clicked = {"called": False, "candidate": None}

    monkeypatch.setattr(
        tag_filter,
        "_resolve_tag_input_strict",
        lambda **kwargs: (fake_input, "strict_popup_only", _FakePopup(), "ms-1", ""),
    )
    monkeypatch.setattr(
        tag_filter,
        "_wait_until_tag_value_reflected",
        lambda **kwargs: (True, {"mode": "input_value", "attempts": 1}),
    )
    monkeypatch.setattr(
        tag_filter,
        "_collect_visible_tag_suggestion_candidates",
        lambda **kwargs: [
            {"text": "Инглегмаш-2026", "id": "100", "index": 0, "selector": "li.multisuggest__list-item"},
            {"text": "Инглегмаш-2026", "id": "200", "index": 1, "selector": "li.multisuggest__list-item"},
        ],
    )
    monkeypatch.setattr(tag_filter, "_poll_chip_detect", lambda *args, **kwargs: (True, ["Инглегмаш-2026"]))
    monkeypatch.setattr(
        tag_filter,
        "_click_suggestion_candidate",
        lambda _panel, _popup, candidate: clicked.update({"called": True, "candidate": candidate}) or True,
    )

    ok, info = tag_filter._apply_single_tag_value_with_candidate(
        flow=flow,
        page=page,
        panel=panel,
        holder=_FakeHolder(),
        holder_id="ms-1",
        report_id="rid",
        value="Инглегмаш-2026",
    )
    assert ok is True
    assert clicked["called"] is True
    assert info["selected_candidate_id"] == "100"
    assert "ArrowDown" not in page.keyboard.pressed
    assert "Enter" not in page.keyboard.pressed


def test_duplicate_retry_switches_to_second_candidate(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)

    calls = {"count": 0, "forced": []}

    def fake_apply_once(**kwargs):
        calls["count"] += 1
        forced = kwargs.get("forced_candidate")
        calls["forced"].append(forced)
        if calls["count"] == 1:
            return True, {
                "duplicate_candidates": [
                    {"text": "Инглегмаш-2026", "id": "100", "index": 0, "selector": "li"},
                    {"text": "Инглегмаш-2026", "id": "200", "index": 1, "selector": "li"},
                ],
                "selected_candidate_id": "100",
                "selected_candidate_index": 0,
                "apply_confirmed_but_parse_suspicious": True,
            }
        return True, {
            "duplicate_candidates": [],
            "selected_candidate_id": "200",
            "selected_candidate_index": 1,
            "apply_confirmed_but_parse_suspicious": False,
        }

    monkeypatch.setattr(tag_filter, "_apply_single_tag_value_with_candidate", fake_apply_once)
    monkeypatch.setattr(tag_filter, "_clear_tag_holder_selection", lambda *args, **kwargs: True)

    ok = tag_filter.apply_tag_values_via_holder_popup(
        flow=flow,
        page=page,
        panel=panel,
        report_id="rid",
        values=["Инглегмаш-2026"],
    )
    assert ok is True
    assert calls["count"] == 2
    assert calls["forced"][0] is None
    assert isinstance(calls["forced"][1], dict)
    assert calls["forced"][1]["id"] == "200"


def test_duplicate_retry_exhausted_returns_controlled_failure(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)

    calls = {"count": 0}

    def fake_apply_once(**kwargs):
        calls["count"] += 1
        forced = kwargs.get("forced_candidate")
        selected_id = "100" if forced is None else str(forced.get("id", "200"))
        return True, {
            "duplicate_candidates": [
                {"text": "Инглегмаш-2026", "id": "100", "index": 0, "selector": "li"},
                {"text": "Инглегмаш-2026", "id": "200", "index": 1, "selector": "li"},
            ],
            "selected_candidate_id": selected_id,
            "selected_candidate_index": 0 if forced is None else 1,
            "apply_confirmed_but_parse_suspicious": True,
        }

    monkeypatch.setattr(tag_filter, "_apply_single_tag_value_with_candidate", fake_apply_once)
    monkeypatch.setattr(tag_filter, "_clear_tag_holder_selection", lambda *args, **kwargs: True)

    ok = tag_filter.apply_tag_values_via_holder_popup(
        flow=flow,
        page=page,
        panel=panel,
        report_id="rid",
        values=["Инглегмаш-2026"],
    )
    assert ok is False
    assert calls["count"] == 2


def test_multi_tag_chip_regression_fails_before_apply(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)
    fake_input = _FakeInput()
    apply_calls = {"count": 0}

    monkeypatch.setattr(
        tag_filter,
        "_resolve_tag_input_strict",
        lambda **kwargs: (fake_input, "strict_popup_only", _FakePopup(), "ms-1", ""),
    )
    monkeypatch.setattr(
        tag_filter,
        "_wait_until_tag_value_reflected",
        lambda **kwargs: (True, {"mode": "input_value", "attempts": 1}),
    )
    monkeypatch.setattr(
        tag_filter,
        "_collect_visible_tag_suggestion_candidates",
        lambda **kwargs: [
            {"text": "tag-a", "id": "100", "index": 0, "selector": "li.multisuggest__list-item"},
            {"text": "tag-b", "id": "200", "index": 1, "selector": "li.multisuggest__list-item"},
        ],
    )
    monkeypatch.setattr(tag_filter, "_click_suggestion_candidate", lambda *_args, **_kwargs: True)

    state = {"calls": 0}

    def fake_poll(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            return True, ["tag-a"]
        return True, ["tag-b"]  # regression: previous chip disappeared

    monkeypatch.setattr(tag_filter, "_poll_chip_detect", fake_poll)
    monkeypatch.setattr(flow, "_collect_tag_chip_texts", lambda panel, holder=None: ["tag-b"], raising=False)
    monkeypatch.setattr(
        flow,
        "_click_apply_in_panel",
        lambda *args, **kwargs: apply_calls.update({"count": apply_calls["count"] + 1}) or True,
    )

    ok = tag_filter.apply_tag_values_via_holder_popup(
        flow=flow,
        page=page,
        panel=panel,
        report_id="rid",
        values=["tag-a", "tag-b"],
    )
    assert ok is False
    assert apply_calls["count"] == 0


def test_multi_tag_final_apply_guard_blocks_partial_set(monkeypatch):
    flow = _FakeFlow()
    page = _FakePage()
    panel = _FakePanel(page)
    fake_input = _FakeInput()
    apply_calls = {"count": 0}

    monkeypatch.setattr(
        tag_filter,
        "_resolve_tag_input_strict",
        lambda **kwargs: (fake_input, "strict_popup_only", _FakePopup(), "ms-1", ""),
    )
    monkeypatch.setattr(
        tag_filter,
        "_wait_until_tag_value_reflected",
        lambda **kwargs: (True, {"mode": "input_value", "attempts": 1}),
    )
    monkeypatch.setattr(
        tag_filter,
        "_collect_visible_tag_suggestion_candidates",
        lambda **kwargs: [
            {"text": "tag-a", "id": "100", "index": 0, "selector": "li.multisuggest__list-item"},
            {"text": "tag-b", "id": "200", "index": 1, "selector": "li.multisuggest__list-item"},
        ],
    )
    monkeypatch.setattr(tag_filter, "_click_suggestion_candidate", lambda *_args, **_kwargs: True)

    state = {"calls": 0}

    def fake_poll(*args, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            return True, ["tag-a"]
        return True, ["tag-a", "tag-b"]

    monkeypatch.setattr(tag_filter, "_poll_chip_detect", fake_poll)
    monkeypatch.setattr(flow, "_collect_tag_chip_texts", lambda panel, holder=None: ["tag-a"], raising=False)  # missing tag-b before apply
    monkeypatch.setattr(
        flow,
        "_click_apply_in_panel",
        lambda *args, **kwargs: apply_calls.update({"count": apply_calls["count"] + 1}) or True,
    )

    ok = tag_filter.apply_tag_values_via_holder_popup(
        flow=flow,
        page=page,
        panel=panel,
        report_id="rid",
        values=["tag-a", "tag-b"],
    )
    assert ok is False
    assert apply_calls["count"] == 0
