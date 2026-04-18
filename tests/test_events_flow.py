import sys
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

from src.browser.events_flow import EventsFlow, EventsFlowInput


def u(s: str) -> str:
    try:
        return s.encode("ascii").decode("unicode_escape")
    except UnicodeEncodeError:
        return s


class _FakeElement:
    def __init__(self, *, text="", visible=True, bbox=None, tag="div", cls="", click_ok=True):
        self.text = text
        self.visible = visible
        self.bbox = bbox or {"x": 12.0, "y": 50.0, "width": 120.0, "height": 20.0}
        self.tag = tag
        self.cls = cls
        self.click_ok = click_ok
        self.clicks = 0

    def is_visible(self, timeout=0):
        return self.visible

    def click(self, timeout=0, force=False):
        if not self.click_ok:
            raise RuntimeError("click failed")
        self.clicks += 1

    def evaluate(self, script):
        src = str(script)
        if "tagName" in src and "className" in src:
            return {
                "tagName": self.tag,
                "className": self.cls,
                "text": self.text,
                "role": "button" if self.tag in {"div", "span", "button", "a"} else "",
                "title": "",
                "ariaLabel": "",
            }
        if "parentElement" in src:
            return True
        return None

    def bounding_box(self):
        return self.bbox

    def locator(self, _selector):
        return _FakeLocator([self])

    def inner_text(self, timeout=0):
        return self.text


class _FakeLocator:
    def __init__(self, elements=None):
        self.elements = list(elements or [])

    def count(self):
        return len(self.elements)

    @property
    def first(self):
        return self.nth(0)

    def nth(self, idx):
        if not self.elements:
            return _FakeElement(visible=False)
        if idx >= len(self.elements):
            idx = len(self.elements) - 1
        return self.elements[idx]

    def locator(self, _selector):
        return self


class _FakeMouse:
    def __init__(self):
        self.clicks = []

    def click(self, x, y):
        self.clicks.append((x, y))


class _FakeKeyboard:
    def __init__(self):
        self.pressed = []

    def press(self, key):
        self.pressed.append(key)


class _FakePage:
    def __init__(self, locator_map=None):
        self.locator_map = locator_map or {}
        self.mouse = _FakeMouse()
        self.keyboard = _FakeKeyboard()
        self.url = "https://officeistockinfo.amocrm.ru/events/list/"

    def locator(self, selector):
        return self.locator_map.get(selector, _FakeLocator([]))

    def wait_for_timeout(self, _ms):
        return None

    def wait_for_load_state(self, _state, timeout=0):
        return None

    def goto(self, _url, wait_until="domcontentloaded"):
        return None

    def screenshot(self, path, full_page=True):
        Path(path).write_text("", encoding="utf-8")

    def evaluate(self, script):
        src = str(script)
        if "innerText" in src:
            return f"{u('\u0424\u0438\u043b\u044c\u0442\u0440')}\n{u('\u0421\u043f\u0438\u0441\u043e\u043a \u0441\u043e\u0431\u044b\u0442\u0438\u0439')}"
        if "querySelectorAll('body *')" in src:
            return f"<header><div>{u('\u0424\u0438\u043b\u044c\u0442\u0440')}</div></header>"
        return ""


def _make_flow() -> EventsFlow:
    settings = SimpleNamespace(base_url="https://officeistockinfo.amocrm.ru", exports_dir=Path("exports"))
    return EventsFlow(settings=settings, project_root=Path("."))


def _flow_input(**overrides):
    base = dict(
        report_id="weekly_refusals_weekly_2m",
        pipeline_name=u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435 (2 \u043c\u0435\u0441\u044f\u0446\u0430)"),
        date_mode=u("\u0421\u043e\u0437\u0434\u0430\u043d\u044b"),
        period_mode=u("\u0417\u0430 \u044d\u0442\u0443 \u043d\u0435\u0434\u0435\u043b\u044e"),
        date_from="",
        date_to="",
        status_before="",
        status_before_values=[],
        status_after=u("\u0417\u0430\u043a\u0440\u044b\u0442\u043e \u0438 \u043d\u0435 \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e"),
        entity_kind=u("\u0421\u0434\u0435\u043b\u043a\u0438"),
        event_type=u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"),
        period_strategy="auto_weekly",
        managers=[],
    )
    base.update(overrides)
    return EventsFlowInput(**base)


def test_open_filter_panel_returns_when_already_open(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    panel = object()
    monkeypatch.setattr(flow, "_find_filter_panel", lambda _page: panel)
    result = flow._open_filter_panel(page)
    assert result is panel


def test_find_filter_panel_by_three_markers():
    flow = _make_flow()
    managers = u("\u041c\u0435\u043d\u0435\u0434\u0436\u0435\u0440\u044b")
    entities = u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438")
    types = u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439")
    markers = {
        f"text={managers}": _FakeLocator([_FakeElement(text=managers)]),
        f"text={entities}": _FakeLocator([_FakeElement(text=entities)]),
        f"text={types}": _FakeLocator([_FakeElement(text=types)]),
    }
    page = _FakePage(locator_map=markers)
    panel = flow._find_filter_panel(page)
    assert panel is not None


def test_resolve_filter_control_prefers_clickable_container_over_text_leaf(monkeypatch):
    flow = _make_flow()
    label = u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438")
    leaf = {
        "selector": f"*:has-text('{label}')",
        "text": label,
        "className": "label-text",
        "clickable": False,
        "bbox": {"x": 10.0, "y": 200.0, "width": 60.0, "height": 16.0},
        "matched_count": 14,
        "_locator": _FakeElement(text=label, cls="label-text", tag="span"),
    }
    container = {
        "selector": f".control--select:has-text('{label}')",
        "text": f"{label} {u('\u0421\u0434\u0435\u043b\u043a\u0438')}",
        "className": "control--select filter__custom_settings__item",
        "clickable": True,
        "bbox": {"x": 10.0, "y": 195.0, "width": 380.0, "height": 34.0},
        "matched_count": 2,
        "_locator": _FakeElement(text=f"{label} value", cls="control--select", tag="div"),
    }
    monkeypatch.setattr(flow, "_collect_control_candidates", lambda _panel, *, control_label: [leaf, container])
    monkeypatch.setattr(flow, "_promote_to_control_container", lambda loc: loc)

    control, ranked = flow._resolve_filter_control(object(), label)
    assert control is container["_locator"]
    assert ranked[0]["selector"].startswith(".control--select")


def test_apply_control_values_multiselect_confirms_ok(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text=u("\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e"))

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_wait_for_options_popup", lambda _page, timeout_ms=1500, control_label=None: True)
    monkeypatch.setattr(flow, "_pick_option", lambda _page, *, value, stage: True)
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: None)

    called = {"ok": False}

    def fake_confirm(_page):
        called["ok"] = True
        return True

    monkeypatch.setattr(flow, "_confirm_popup_if_open", fake_confirm)
    monkeypatch.setattr(flow, "_verify_selected_values", lambda _c, _v, require_any=False: (True, "control_text"))

    flow._apply_control_values(
        page,
        object(),
        control_label=u("\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e"),
        values=[u("\u041a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f"), u("\u041f\u0435\u0440\u0432\u0438\u0447\u043d\u044b\u0439 \u043a\u043e\u043d\u0442\u0430\u043a\u0442")],
        stage="status_before",
        allow_multi=True,
    )
    assert called["ok"] is True


def test_set_entity_and_event_type_uses_expected_controls(monkeypatch):
    flow = _make_flow()
    called = []

    def fake_apply(_page, _panel, *, control_label, values, stage, allow_multi):
        called.append((control_label, list(values), stage, allow_multi))

    monkeypatch.setattr(flow, "_apply_control_values", fake_apply)
    flow._set_entity_and_event_type(_FakePage(), object(), _flow_input())

    assert called[0][0] == u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438")
    assert called[0][1] == [u("\u0421\u0434\u0435\u043b\u043a\u0438")]
    assert called[1][0] == u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439")
    assert called[1][1] == [u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")]


def test_before_status_supports_multiselect_values(monkeypatch):
    flow = _make_flow()
    called = []

    def fake_apply(_page, _panel, *, control_label, values, stage, allow_multi):
        called.append((control_label, list(values), stage, allow_multi))

    monkeypatch.setattr(flow, "_apply_control_values", fake_apply)
    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (None, []))
    flow._set_pipeline_and_statuses(
        _FakePage(),
        object(),
        _flow_input(status_before_values=[u("\u041a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f"), u("\u041f\u0435\u0440\u0432\u0438\u0447\u043d\u044b\u0439 \u043a\u043e\u043d\u0442\u0430\u043a\u0442. \u041a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f")]),
    )

    status_before_calls = [x for x in called if x[2] == "status_before"]
    assert status_before_calls
    assert status_before_calls[0][3] is True


def test_pipeline_control_missing_is_skipped_and_statuses_are_applied(monkeypatch):
    flow = _make_flow()
    called = []

    def fake_apply(_page, _panel, *, control_label, values, stage, allow_multi):
        called.append((control_label, list(values), stage, allow_multi))

    monkeypatch.setattr(flow, "_apply_control_values", fake_apply)

    def fake_resolve(_panel, control_label):
        if control_label == u("\u0412\u043e\u0440\u043e\u043d\u043a\u0430"):
            return None, []
        return _FakeElement(text=control_label), []

    monkeypatch.setattr(flow, "_resolve_filter_control", fake_resolve)

    flow._set_pipeline_and_statuses(
        _FakePage(),
        object(),
        _flow_input(
            status_before_values=[u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435(2 \u043c\u0435\u0441\u044f\u0446\u0430) \u0432\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f")],
            status_after=u("\u0417\u0430\u043a\u0440\u044b\u0442\u043e \u0438 \u043d\u0435 \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e"),
        ),
    )

    stages = [item[2] for item in called]
    assert "pipeline" not in stages
    assert "status_before" in stages
    assert "status_after" in stages


def test_resolve_filter_control_for_stage_uses_exact_status_before_selector():
    flow = _make_flow()

    class _Panel:
        def locator(self, selector):
            if selector == ".js-control-checkboxes-search[data-name='filter[value_before][status_lead][]']":
                return _FakeLocator([_FakeElement(text=u("\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e"), cls="js-control-checkboxes-search")])
            return _FakeLocator([])

    control, payload = flow._resolve_filter_control_for_stage(_Panel(), control_label=u("\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e"), stage="status_before")
    assert control is not None
    assert payload
    assert payload[0].get("exact_selector_used") is True


def test_status_normalization_matches_profile_and_dom_formats():
    flow = _make_flow()
    desired_pipeline, desired_status = flow._split_profile_status_value(u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435(2 \u043c\u0435\u0441\u044f\u0446\u0430) \u043d\u0435\u0440\u0430\u0437\u043e\u0431\u0440\u0430\u043d\u043d\u043e\u0435"))
    dom_pipeline, dom_status = flow._split_dom_status_value({
        "pipeline_text": u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435 (2 \u043c\u0435\u0441\u044f\u0446\u0430)"),
        "status_text": u("\u041d\u0435\u0440\u0430\u0437\u043e\u0431\u0440\u0430\u043d\u043d\u043e\u0435"),
        "data_value": "",
        "full_text": "",
    })
    assert desired_pipeline == dom_pipeline
    assert desired_status == dom_status


def test_status_after_pipeline_hint_is_applied_when_profile_has_terminal_status(monkeypatch):
    flow = _make_flow()
    called = []

    def fake_apply(_page, _panel, *, control_label, values, stage, allow_multi):
        called.append((control_label, list(values), stage, allow_multi))

    monkeypatch.setattr(flow, "_apply_control_values", fake_apply)
    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (None, []))

    flow._set_pipeline_and_statuses(
        _FakePage(),
        object(),
        _flow_input(
            pipeline_name=u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435 (2 \u043c\u0435\u0441\u044f\u0446\u0430)"),
            status_after=u("\u0417\u0430\u043a\u0440\u044b\u0442\u043e \u0438 \u043d\u0435 \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e"),
            status_before_values=[u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435(2 \u043c\u0435\u0441\u044f\u0446\u0430) \u0432\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f")],
        ),
    )

    status_after_calls = [x for x in called if x[2] == "status_after"]
    assert status_after_calls
    assert u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435 (2 \u043c\u0435\u0441\u044f\u0446\u0430)") in status_after_calls[0][1][0]
    assert "/" in status_after_calls[0][1][0]


def test_apply_control_values_routes_status_before_to_status_search_handler(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text=u("\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e"), cls="checkboxes-search js-control-checkboxes-search")
    called = {"status_search": 0}

    monkeypatch.setattr(flow, "_resolve_filter_control_for_stage", lambda _panel, *, control_label, stage: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")

    def fake_status_search(_page, _control, *, stage, values, pipeline_hint, allow_multi):
        called["status_search"] += 1
        assert stage == "status_before"
        assert allow_multi is True
        assert values
        return True, {"popup_found": True, "apply_clicked": True, "popup_closed": True}

    monkeypatch.setattr(flow, "_select_status_popup_values", fake_status_search)

    flow._apply_control_values(
        page,
        object(),
        control_label=u("\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e"),
        values=[u("\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435(2 \u043c\u0435\u0441\u044f\u0446\u0430) \u0432\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f")],
        stage="status_before",
        allow_multi=True,
    )
    assert called["status_search"] == 1


def test_set_date_filters_created_all_time_without_switch_input(monkeypatch):
    flow = _make_flow()
    page = _FakePage()

    class _PresetInput(_FakeElement):
        def __init__(self):
            super().__init__(text="", tag="input")
            self.value = "today"

        def evaluate(self, script):
            src = str(script)
            if "String(el.value" in src:
                return self.value
            if "el.value = ''" in src:
                self.value = ""
                return None
            return super().evaluate(script)

    preset = _PresetInput()

    class _Panel:
        def locator(self, selector):
            if selector == "input[name='filter[date_preset]']":
                return _FakeLocator([preset])
            if selector in (".date_filter__period", ".date_filter [class*='period']", "[class*='date_filter'] [class*='period']"):
                return _FakeLocator([_FakeElement(text=u("\u0417\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f"), cls="date_filter__period")])
            return _FakeLocator([])

    flow._set_date_filters(page, _Panel(), _flow_input(date_mode=u("\u0421\u043e\u0437\u0434\u0430\u043d\u044b"), period_mode=u("\u0417\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f")))
    assert preset.value == ""




def test_set_date_filters_created_resolves_non_empty_created_input():
    flow = _make_flow()
    page = _FakePage()

    class _ModeInput(_FakeElement):
        def __init__(self):
            super().__init__(text="", tag="input")
            self.checked = False

        def evaluate(self, script):
            src = str(script)
            if "!!el.checked" in src:
                return bool(self.checked)
            return super().evaluate(script)

        def check(self, timeout=0):
            self.checked = True

        def click(self, timeout=0, force=False):
            self.checked = True

    class _PresetInput(_FakeElement):
        def __init__(self):
            super().__init__(text="", tag="input")
            self.value = "week"

        def evaluate(self, script):
            src = str(script)
            if "String(el.value" in src:
                return self.value
            if "el.value = ''" in src:
                self.value = ""
                return None
            return super().evaluate(script)

    mode_created = _ModeInput()
    mode_closed = _ModeInput()
    mode_closed.checked = False
    preset = _PresetInput()

    class _Panel:
        def locator(self, selector):
            if selector == "input[name='filter[date_mode]'][value='created']":
                return _FakeLocator([mode_created])
            if selector == "input[name='filter[date_mode]'][value='closed']:checked":
                return _FakeLocator([])
            if selector == "input[name='filter[date_mode]'][value='created']:checked":
                return _FakeLocator([mode_created] if mode_created.checked else [])
            if selector == "input[name='filter_date_switch'][value='']:checked":
                return _FakeLocator([])
            if selector == "input[name='filter_date_switch'][value='closed']:checked":
                return _FakeLocator([])
            if selector == "input[name='filter[date_preset]']":
                return _FakeLocator([preset])
            if selector in (".date_filter__period", ".date_filter [class*='period']", "[class*='date_filter'] [class*='period']"):
                return _FakeLocator([_FakeElement(text=u("\u0417\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f"), cls="date_filter__period")])
            return _FakeLocator([])

    flow._set_date_filters(page, _Panel(), _flow_input(date_mode=u("\u0421\u043e\u0437\u0434\u0430\u043d\u044b"), period_mode=u("\u0417\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f")))
    assert mode_created.checked is True
    assert preset.value == ""

def test_set_date_filters_closed_requires_switch_and_fails_if_missing(monkeypatch):
    flow = _make_flow()
    page = _FakePage()

    class _Panel:
        def locator(self, selector):
            return _FakeLocator([])

    monkeypatch.setattr(flow, "_dump_stage_failure_artifacts", lambda **kwargs: {"json": "x"})

    failed = False
    try:
        flow._set_date_filters(page, _Panel(), _flow_input(date_mode=u("\u0417\u0430\u043a\u0440\u044b\u0442\u044b"), period_mode=u("\u0417\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f")))
    except RuntimeError as exc:
        failed = True
        assert "stage=date_mode" in str(exc)
    assert failed is True


def test_click_apply_selectors_include_prinyat(monkeypatch):
    flow = _make_flow()
    selectors_seen = []

    class _Panel:
        def locator(self, selector):
            selectors_seen.append(selector)
            return _FakeLocator([])

    monkeypatch.setattr(flow, "_dump_stage_failure_artifacts", lambda **kwargs: {"json": "x"})
    try:
        flow._click_apply(_FakePage(), panel=_Panel())
    except RuntimeError:
        pass

    assert f"button:has-text('{u('\u041f\u0440\u0438\u043d\u044f\u0442\u044c')}')" in selectors_seen
    assert f"[role='button']:has-text('{u('\u041f\u0440\u0438\u043d\u044f\u0442\u044c')}')" in selectors_seen


def test_weekly_profile_utf8_values_not_mojibake():
    import yaml

    root = Path(__file__).resolve().parents[1]
    data = yaml.safe_load((root / "config" / "report_profiles.yaml").read_text(encoding="utf-8")) or {}
    items = data.get("report_profiles", [])
    weekly = [x for x in items if str(x.get("id", "")).startswith("weekly_refusals_")]
    assert weekly
    for profile in weekly:
        filters = profile.get("filters", {}) or {}
        for key in ("pipeline", "date_mode", "period_mode", "status_after", "entity_kind", "event_type"):
            value = str(filters.get(key, ""))
            assert "?" not in value and "?" not in value and "?" not in value and "?" not in value


def test_click_control_target_uses_right_side_bbox_fallback(monkeypatch):
    flow = _make_flow()
    page = _FakePage()

    class _Control:
        def locator(self, _selector):
            return _FakeLocator([])

        def bounding_box(self):
            return {"x": 100.0, "y": 200.0, "width": 100.0, "height": 20.0}

    monkeypatch.setattr(flow, "_resolve_control_click_target", lambda _control: (None, {}))
    ok, mode, payload = flow._click_control_target(page, _Control())

    assert ok is True
    assert mode == "bbox_right_click"
    assert page.mouse.clicks
    x, y = page.mouse.clicks[-1]
    assert abs(x - 182.0) < 0.01
    assert abs(y - 210.0) < 0.01
    assert payload.get("x") == x


def test_verify_selected_values_can_pass_via_input_or_chip(monkeypatch):
    flow = _make_flow()
    dummy = object()

    monkeypatch.setattr(flow, "_control_text", lambda _control: "")
    monkeypatch.setattr(flow, "_control_input_values", lambda _control: [u("\u0421\u0434\u0435\u043b\u043a\u0438")])
    monkeypatch.setattr(flow, "_control_chip_texts", lambda _control: [])
    ok, source = flow._verify_selected_values(dummy, [u("\u0421\u0434\u0435\u043b\u043a\u0438")], require_any=False)
    assert ok is True
    assert source == "input_value"

    monkeypatch.setattr(flow, "_control_input_values", lambda _control: [])
    monkeypatch.setattr(flow, "_control_chip_texts", lambda _control: [u("\u0421\u0434\u0435\u043b\u043a\u0438")])
    ok, source = flow._verify_selected_values(dummy, [u("\u0421\u0434\u0435\u043b\u043a\u0438")], require_any=False)
    assert ok is True
    assert source == "chip_text"


def test_weekly_profiles_have_non_empty_status_before_values():
    import yaml

    root = Path(__file__).resolve().parents[1]
    data = yaml.safe_load((root / "config" / "report_profiles.yaml").read_text(encoding="utf-8")) or {}
    items = data.get("report_profiles", [])
    target_ids = {
        "weekly_refusals_weekly_2m",
        "weekly_refusals_weekly_long",
        "weekly_refusals_cumulative_2m",
        "weekly_refusals_cumulative_long",
    }
    for profile in items:
        rid = str(profile.get("id", ""))
        if rid not in target_ids:
            continue
        values = list((profile.get("filters") or {}).get("status_before_values") or [])
        assert values, f"status_before_values must be non-empty for {rid}"


def test_resolved_container_prefers_clickable_descendant(monkeypatch):
    flow = _make_flow()

    class _Candidate(_FakeElement):
        def evaluate(self, script):
            src = str(script)
            if "clickable" in src:
                return {
                    "clickable": True,
                    "tagName": "button",
                    "className": "control--select--button",
                    "text": "open",
                    "bbox": {"x": 10.0, "y": 10.0, "width": 20.0, "height": 20.0},
                }
            return super().evaluate(script)

    class _Control:
        def __init__(self):
            self.desc = _Candidate(text="open", tag="button", cls="control--select--button")

        def locator(self, selector):
            if selector == ".control--select--button":
                return _FakeLocator([self.desc])
            return _FakeLocator([])

        def bounding_box(self):
            return {"x": 0.0, "y": 0.0, "width": 200.0, "height": 40.0}

    target, payload = flow._resolve_control_click_target(_Control())
    assert target is not None
    assert payload.get("className") == "control--select--button"


def test_checkbox_dropdown_open_prefers_title_wrapper(monkeypatch):
    flow = _make_flow()

    class _Control:
        def locator(self, selector):
            if selector == ".checkboxes_dropdown__title_wrapper":
                return _FakeLocator([_FakeElement(text="open", cls="checkboxes_dropdown__title_wrapper", tag="div")])
            return _FakeLocator([])

    ok, mode = flow._open_checkbox_dropdown_control(_FakePage(), _Control())
    assert ok is True
    assert mode.startswith("checkbox_title:.checkboxes_dropdown__title_wrapper")


def test_checkbox_dropdown_closes_via_escape():
    flow = _make_flow()
    page = _FakePage()
    flow._close_checkbox_dropdown_with_escape(page)
    assert "Escape" in page.keyboard.pressed


def test_apply_mvp_filters_saved_preset_mode_routes(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    called = {"preset": 0, "entity": 0, "date": 0}

    monkeypatch.setattr(flow, "_clear_managers", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(flow, "_apply_saved_preset", lambda *_args, **_kwargs: called.__setitem__("preset", called["preset"] + 1))
    monkeypatch.setattr(flow, "_set_entity_and_event_type", lambda *_args, **_kwargs: called.__setitem__("entity", called["entity"] + 1))
    monkeypatch.setattr(flow, "_set_pipeline_and_statuses", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(flow, "_set_date_filters", lambda *_args, **_kwargs: called.__setitem__("date", called["date"] + 1))

    flow_input = _flow_input(filter_mode="saved_preset", saved_preset_name="weekly base", saved_preset_exact_match=True)
    flow._apply_mvp_filters(page, panel=object(), flow_input=flow_input)

    assert called["preset"] == 1
    assert called["date"] == 1
    assert called["entity"] == 0


def test_generic_path_kept_when_not_checkbox(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="generic")

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: None)
    monkeypatch.setattr(flow, "_wait_for_options_popup", lambda _page, timeout_ms=1500, control_label=None: True)
    monkeypatch.setattr(flow, "_pick_option", lambda _page, *, value, stage: True)
    monkeypatch.setattr(flow, "_verify_selected_values", lambda _c, _v, require_any=False: (True, "control_text"))

    flow._apply_control_values(page, object(), control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"), values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")], stage="entity", allow_multi=False)


def test_checkbox_path_skips_generic_click_target(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="checkbox control", cls="checkboxes_dropdown")

    called = {"generic_click": 0, "open": 0, "verify_primary": 0}

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "dropdown")
    monkeypatch.setattr(flow, "_click_control_target", lambda *_args, **_kwargs: called.__setitem__("generic_click", called["generic_click"] + 1) or (True, "descendant_click", {}))
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: called.__setitem__("open", called["open"] + 1) or (True, "checkbox_title"))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_select_checkbox_like_value", lambda *_args, **_kwargs: True)

    def _verify(*_args, **_kwargs):
        called["verify_primary"] += 1
        return True, {"selector": "x"}

    monkeypatch.setattr(flow, "_verify_checkbox_dropdown_selection", lambda *_args, **_kwargs: _verify())
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: page.keyboard.press("Escape"))
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (False, "none"))
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_scope"))

    flow._apply_control_values(page, object(), control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"), values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")], stage="entity", allow_multi=False)

    assert called["open"] == 1
    assert called["generic_click"] == 0
    assert called["verify_primary"] == 0


def test_checkbox_primary_success_allows_reflection_none(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="")

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "dropdown")
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: (True, "checkbox_title"))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_select_checkbox_like_value", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_verify_checkbox_dropdown_selection", lambda *_args, **_kwargs: (True, {"selector": "item"}))
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: page.keyboard.press("Escape"))
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (False, "none"))
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_scope"))

    flow._apply_control_values(page, object(), control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"), values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")], stage="entity", allow_multi=False)
    assert "Escape" in page.keyboard.pressed


def test_checkbox_single_value_tries_clear_first(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="")
    called = {"clear": 0}

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "dropdown")
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: (True, "checkbox_title"))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_select_checkbox_like_value", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_verify_checkbox_dropdown_selection", lambda *_args, **_kwargs: (True, {"selector": "item"}))
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: None)
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (True, "control_text"))
    monkeypatch.setattr(flow, "_try_clear_checkbox_dropdown", lambda *_args, **_kwargs: called.__setitem__("clear", called["clear"] + 1) or True)

    flow._apply_control_values(page, object(), control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"), values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")], stage="entity", allow_multi=False)
    assert called["clear"] == 1


def test_checkboxes_search_detected_as_checkbox_like_control():
    flow = _make_flow()

    class _Control:
        def locator(self, selector):
            if selector == ".checkboxes-search, .js-control-checkboxes-search":
                return _FakeLocator([_FakeElement(text="search", cls="checkboxes-search", tag="div")])
            return _FakeLocator([])

        def evaluate(self, _script):
            return "filter__custom_settings__item checkboxes-search js-control-checkboxes-search"

    control = _Control()
    assert flow._detect_checkbox_control_kind(control) == "search"
    assert flow._is_checkbox_like_control(control) is True


def test_checkboxes_search_open_path_uses_title_wrapper():
    flow = _make_flow()

    class _Control:
        def locator(self, selector):
            if selector == ".checkboxes-search__title-wrapper":
                return _FakeLocator([_FakeElement(text="open", cls="checkboxes-search__title-wrapper", tag="div")])
            return _FakeLocator([])

    ok, mode = flow._open_checkbox_like_control(_FakePage(), _Control(), kind="search")
    assert ok is True
    assert mode.startswith("checkbox_search_title:.checkboxes-search__title-wrapper")


def test_event_type_search_path_does_not_use_generic_option_picking(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="search control", cls="checkboxes-search")
    called = {"pick_option": 0}

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")
    monkeypatch.setattr(flow, "_open_event_type_search_control", lambda *_args, **_kwargs: (True, "event_type_search:title_click", {"root_found": True, "title_found": True}))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_scope"))
    monkeypatch.setattr(flow, "_select_event_type_search_value", lambda *_args, **_kwargs: (True, {"selected_option_found": True, "selected_option_text": u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"), "apply_button_found": True, "checkbox_checked": True}))
    monkeypatch.setattr(flow, "_verify_checkbox_search_selection", lambda *_args, **_kwargs: (True, {"selector": "item", "checkbox_checked": True}))
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: None)
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (False, "none"))

    def _pick(*_args, **_kwargs):
        called["pick_option"] += 1
        return True

    monkeypatch.setattr(flow, "_pick_option", _pick)

    flow._apply_control_values(page, object(), control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")], stage="event_type", allow_multi=False)
    assert called["pick_option"] == 0


def test_event_type_search_requires_scoped_container(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="search control", cls="checkboxes-search")

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: (True, "checkbox_search_title"))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (None, "scope_not_found"))

    try:
        flow._apply_control_values(page, object(), control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")], stage="event_type", allow_multi=False)
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "event_type" in str(exc)
    assert raised is True


def test_checkboxes_search_primary_verification_allows_empty_reflection(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="")

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")
    monkeypatch.setattr(flow, "_open_event_type_search_control", lambda *_args, **_kwargs: (True, "event_type_search:title_click", {"root_found": True, "title_found": True}))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_scope"))
    monkeypatch.setattr(flow, "_select_event_type_search_value", lambda *_args, **_kwargs: (True, {"selected_option_found": True, "selected_option_text": u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"), "apply_button_found": True, "checkbox_checked": True}))
    monkeypatch.setattr(flow, "_verify_checkbox_search_selection", lambda *_args, **_kwargs: (True, {"selector": "item", "checkbox_checked": True}))
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: page.keyboard.press("Escape"))
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (False, "none"))

    flow._apply_control_values(page, object(), control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")], stage="event_type", allow_multi=False)
    assert "Escape" in page.keyboard.pressed


def test_dropdown_recovery_reflection_allows_missing_option_node(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="")

    monkeypatch.setattr(flow, "_verify_checkbox_like_value_selected", lambda *_args, **_kwargs: (False, {"found": False, "selector": ""}))
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (True, "input_value"))

    ok, payload = flow._verify_checkbox_dropdown_selection(page, control, value=u("\u0421\u0434\u0435\u043b\u043a\u0438"), stage="entity", scope=page, allow_reopen_once=False)
    assert ok is True
    assert payload.get("source") == "input_value"


def test_search_path_clicks_ok_after_selection(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="search control", cls="checkboxes-search")
    called = {"ok": 0}

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")
    monkeypatch.setattr(flow, "_open_event_type_search_control", lambda *_args, **_kwargs: (True, "event_type_search:title_click", {"root_found": True, "title_found": True}))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_scope"))
    monkeypatch.setattr(flow, "_select_event_type_search_value", lambda *_args, **_kwargs: (True, {"selected_option_found": True, "selected_option_text": u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"), "apply_button_found": True, "checkbox_checked": True}))
    monkeypatch.setattr(flow, "_verify_checkbox_search_selection", lambda *_args, **_kwargs: (True, {"selector": "item", "checkbox_checked": True}))
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: None)
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (False, "none"))

    def _ok(_page):
        called["ok"] += 1
        return True

    monkeypatch.setattr(flow, "_click_checkbox_search_ok", _ok)

    flow._apply_control_values(page, object(), control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")], stage="event_type", allow_multi=False)
    assert called["ok"] == 0


def test_collect_checkbox_search_debug_snapshot_contains_expected_sections(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="???? ???????", cls="checkboxes-search")

    monkeypatch.setattr(page, "evaluate", lambda _script: {"tagName": "input", "className": "x"})
    monkeypatch.setattr(flow, "_collect_visible_element_payloads", lambda *_args, **_kwargs: [{"selector": "mock", "text": "x"}])

    snap = flow._collect_checkbox_search_debug_snapshot(
        page,
        control,
        control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"),
        expected_value=u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"),
    )

    assert "active_element" in snap
    assert "control_scope_elements" in snap
    assert "checkbox_class_elements" in snap
    assert "ok_buttons" in snap
    assert "event_type_text_elements" in snap


def test_search_popup_not_opened_includes_debug_snapshot_in_summary(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="???? ???????", cls="checkboxes-search")
    captured = {}

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: (True, "checkbox_search_title:.checkboxes-search__title-wrapper"))
    wait_calls = {"count": 0}

    def _wait_checkbox(*_args, **_kwargs):
        wait_calls["count"] += 1
        return wait_calls["count"] == 1

    monkeypatch.setattr(flow, "_wait_checkbox_like_open", _wait_checkbox)
    monkeypatch.setattr(flow, "_collect_checkbox_search_open_markers", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(flow, "_collect_checkbox_search_debug_snapshot", lambda *_args, **_kwargs: {"active_element": {"tagName": "input"}, "ok_buttons": []})

    def _dump(*, page, stage, summary, candidates=None):
        captured["stage"] = stage
        captured["summary"] = summary
        return {"json": "x", "txt": "y", "screenshot": "z"}

    monkeypatch.setattr(flow, "_dump_stage_failure_artifacts", _dump)

    try:
        flow._apply_control_values(
            page,
            object(),
            control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"),
            values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")],
            stage="event_type",
            allow_multi=False,
        )
        raised = False
    except RuntimeError:
        raised = True

    assert raised is True
    assert captured["stage"] == "event_type_open_failed"
    assert "checkbox_search_debug_snapshot" in captured["summary"]
    assert captured["summary"]["checkbox_search_debug_snapshot"]["active_element"]["tagName"] == "input"


def test_resolve_checkbox_scope_search_self_root():
    flow = _make_flow()

    class _Control(_FakeElement):
        def __init__(self):
            super().__init__(text="???? ???????", cls="filter__custom_settings__item checkboxes-search js-control-checkboxes-search")

        def locator(self, _selector):
            return _FakeLocator([])

        def evaluate(self, script):
            if "className" in str(script):
                return self.cls
            return super().evaluate(script)

    control = _Control()
    scope, reason = flow._resolve_checkbox_scope(_FakePage(), control, kind="search")
    assert scope is control
    assert reason == "control_self_scope"


def test_wait_checkbox_like_open_search_uses_scope_first():
    flow = _make_flow()

    class _Scope:
        def locator(self, selector):
            if selector == ".checkboxes-search__search-input":
                return _FakeLocator([_FakeElement(text="", cls="checkboxes-search__search-input")])
            return _FakeLocator([])

    class _Page(_FakePage):
        def locator(self, selector):
            return _FakeLocator([])

    assert flow._wait_checkbox_like_open(_Page(), kind="search", control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), timeout_ms=300, scope=_Scope()) is True


def test_find_checkbox_option_in_scope_search_item_label_selector():
    flow = _make_flow()

    class _Scope:
        def locator(self, selector):
            if selector.startswith('.checkboxes-search__item-label:has-text('):
                return _FakeLocator([_FakeElement(text=u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"), cls="checkboxes-search__item-label")])
            return _FakeLocator([])

    item, meta = flow._find_checkbox_option_in_scope(_Scope(), value=u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"), kind="search")
    assert item is not None
    assert meta.get("selector", "").startswith('.checkboxes-search__item-label')


def test_find_checkbox_option_in_scope_search_data_value_selector():
    flow = _make_flow()

    class _Input(_FakeElement):
        def __init__(self):
            super().__init__(text="", tag="input", cls="")

        def locator(self, selector):
            if selector.startswith('xpath=ancestor-or-self::label'):
                return _FakeLocator([_FakeElement(text=u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"), tag="label")])
            return _FakeLocator([])

    class _Scope:
        def locator(self, selector):
            if selector.startswith("input[type='checkbox'][data-value='"):
                return _FakeLocator([_Input()])
            return _FakeLocator([])

    item, meta = flow._find_checkbox_option_in_scope(_Scope(), value="evt_stage_change", kind="search")
    assert item is not None
    assert meta.get("selector", "").startswith("input[type='checkbox'][data-value='")


def test_click_checkbox_search_ok_supports_div_apply_selector():
    flow = _make_flow()
    page = _FakePage(locator_map={
        '.checkboxes-search__buttons-wrapper .js-checkboxes-search-list-apply': _FakeLocator([_FakeElement(text='OK', cls='js-checkboxes-search-list-apply button-input', tag='div')])
    })
    assert flow._click_checkbox_search_ok(page) is True


def test_event_type_special_open_path_click_order_prefers_native_then_fallbacks(monkeypatch):
    flow = _make_flow()

    state = {"hidden": True}

    class _Title(_FakeElement):
        def __init__(self):
            super().__init__(text="open", cls="checkboxes-search__title-wrapper")
            self.calls = []

        def click(self, timeout=0, force=False):
            self.calls.append(("click", bool(force)))
            if not force:
                raise RuntimeError("normal click failed")
            raise RuntimeError("force click failed")

        def evaluate(self, script):
            src = str(script)
            self.calls.append(("evaluate", src))
            if "el.click" in src:
                state["hidden"] = False
                return True
            return super().evaluate(script)

    class _Opening(_FakeElement):
        def evaluate(self, script):
            if "className" in str(script):
                return "checkboxes-search__opening-list hidden" if state["hidden"] else "checkboxes-search__opening-list"
            return super().evaluate(script)

    title = _Title()

    class _Root(_FakeElement):
        def locator(self, selector):
            if selector == ".checkboxes-search__title-wrapper":
                return _FakeLocator([title])
            if selector == ".checkboxes-search__opening-list":
                return _FakeLocator([_Opening(text="", cls="checkboxes-search__opening-list")])
            return _FakeLocator([])

    class _Mouse:
        def click(self, _x, _y):
            state["hidden"] = False

    page = _FakePage()
    page.mouse = _Mouse()
    root = _Root(text=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), cls="filter__custom_settings__item checkboxes-search js-control-checkboxes-search")
    monkeypatch.setattr(flow, "_resolve_event_type_search_root", lambda _page, _control: (root, "root", True))

    ok, mode, _diag = flow._open_event_type_search_control(page, root)
    assert ok is True
    assert mode == "event_type_search:title_bbox_click"
    click_calls = [call for call in title.calls if call[0] == "click"]
    assert click_calls[0] == ("click", False)
    assert click_calls[1] == ("click", True)

def test_event_type_open_state_accepts_global_search_markers(monkeypatch):
    flow = _make_flow()

    class _Title(_FakeElement):
        def click(self, timeout=0, force=False):
            return None

    class _Root(_FakeElement):
        def locator(self, selector):
            if selector == ".checkboxes-search__title-wrapper":
                return _FakeLocator([_Title(text="open", cls="checkboxes-search__title-wrapper")])
            if selector == ".checkboxes-search__opening-list":
                return _FakeLocator([_FakeElement(text="", cls="checkboxes-search__opening-list hidden")])
            return _FakeLocator([])

    root = _Root(text=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), cls="filter__custom_settings__item checkboxes-search js-control-checkboxes-search")
    page = _FakePage(locator_map={
        ".checkboxes-search__search-input": _FakeLocator([_FakeElement(text="", cls="checkboxes-search__search-input")]),
    })
    monkeypatch.setattr(flow, "_resolve_event_type_search_root", lambda _page, _control: (root, "root", True))

    ok, mode, diag = flow._open_event_type_search_control(page, root)
    assert ok is True
    assert mode == "event_type_search:title_click"
    assert diag["global_search_input_found_after"] is True
    assert diag["open_method_used"] == "title_click"



def test_apply_event_type_uses_special_open_path_over_generic(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="search control", cls="checkboxes-search js-control-checkboxes-search")
    called = {"special": 0, "generic": 0}

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")

    def _special(*_args, **_kwargs):
        called["special"] += 1
        return True, "event_type_search:title_js_click", {"root_found": True, "title_found": True}

    def _generic(*_args, **_kwargs):
        called["generic"] += 1
        return True, "checkbox_search_title"

    monkeypatch.setattr(flow, "_open_event_type_search_control", _special)
    monkeypatch.setattr(flow, "_open_checkbox_like_control", _generic)
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_self_scope"))
    monkeypatch.setattr(flow, "_select_event_type_search_value", lambda *_args, **_kwargs: (True, {"selected_option_found": True, "selected_option_text": u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")}))
    monkeypatch.setattr(flow, "_verify_checkbox_search_selection", lambda *_args, **_kwargs: (True, {"selector": "x"}))
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (True, "control_text"))
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: None)

    flow._apply_control_values(page, object(), control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")], stage="event_type", allow_multi=False)
    assert called["special"] == 1
    assert called["generic"] == 0


def test_event_type_selection_path_uses_exact_option_text(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    target = u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")

    class _SearchInput(_FakeElement):
        def fill(self, _value):
            return None

        def type(self, _value, delay=0):
            return None

    class _EventTypeInput(_FakeElement):
        def __init__(self):
            super().__init__(text="", tag="input", cls="")
            self.checked = False

        def evaluate(self, script):
            src = str(script)
            if "!!el.checked" in src:
                return bool(self.checked)
            return super().evaluate(script)

        def click(self, timeout=0, force=False):
            self.checked = True

    class _ApplyButton(_FakeElement):
        pass

    class _Popup:
        def __init__(self, event_input):
            self.event_input = event_input

        def locator(self, selector):
            if selector == ".checkboxes-search__search-input":
                return _FakeLocator([_SearchInput(text="", cls="checkboxes-search__search-input", tag="input")])
            if selector == f"input[name='filter[event_type][]'][data-value='{target}']":
                return _FakeLocator([self.event_input])
            if selector.startswith("label.checkboxes-search__item-label:has-text("):
                return _FakeLocator([_FakeElement(text=target, cls="checkboxes-search__item-label", tag="label")])
            if selector in (".checkboxes-search__item", ".checkboxes-search__item-inner", ".checkboxes-search__item-label", "label.checkboxes-search__item-label"):
                return _FakeLocator([_FakeElement(text=target, cls="checkboxes-search__item-label", tag="label")])
            if selector == ".js-checkboxes-search-list-apply":
                return _FakeLocator([_ApplyButton(text="OK", cls="js-checkboxes-search-list-apply button-input", tag="div")])
            return _FakeLocator([])

    event_input = _EventTypeInput()
    popup = _Popup(event_input)

    states = [{"popup": popup, "meta": {"popup_found": True, "popup_candidates_count": 1, "popup_has_event_type_inputs": True}}, {"popup": None, "meta": {"popup_found": False, "popup_candidates_count": 0, "popup_has_event_type_inputs": False}}]

    def _find_popup(_page):
        state = states.pop(0) if states else {"popup": None, "meta": {"popup_found": False, "popup_candidates_count": 0, "popup_has_event_type_inputs": False}}
        return state["popup"], state["meta"]

    monkeypatch.setattr(flow, "_find_event_type_global_popup", _find_popup)

    ok, meta = flow._select_event_type_search_value(page, _FakeElement(text="root"), target, stage="event_type", scope=page)
    assert ok is True
    assert meta["selected_option_found"] is True
    assert target in meta["selected_option_text"]
    assert meta["apply_button_found"] is True


def test_event_type_selection_uses_clear_and_apply_from_global_popup(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    target = u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")

    class _SearchInput(_FakeElement):
        def fill(self, _value):
            return None

        def type(self, _value, delay=0):
            return None

    class _EventTypeInput(_FakeElement):
        def __init__(self):
            super().__init__(text="", tag="input", cls="")
            self.checked = False

        def evaluate(self, script):
            if "!!el.checked" in str(script):
                return bool(self.checked)
            return super().evaluate(script)

        def click(self, timeout=0, force=False):
            self.checked = True

    class _ClearButton(_FakeElement):
        pass

    class _ApplyButton(_FakeElement):
        pass

    class _Popup:
        def __init__(self, event_input, clear_btn):
            self.event_input = event_input
            self.clear_btn = clear_btn

        def locator(self, selector):
            if selector == ".checkboxes-search__search-input":
                return _FakeLocator([_SearchInput(text="", cls="checkboxes-search__search-input", tag="input")])
            if selector == ".js-checkboxes-search-clear-all":
                return _FakeLocator([self.clear_btn])
            if selector == f"input[name='filter[event_type][]'][data-value='{target}']":
                return _FakeLocator([self.event_input])
            if selector.startswith("label.checkboxes-search__item-label:has(input[name='filter[event_type][]'][data-value="):
                return _FakeLocator([_FakeElement(text=target, cls="checkboxes-search__item-label", tag="label")])
            if selector in (".checkboxes-search__item", ".checkboxes-search__item-inner", ".checkboxes-search__item-label", "label.checkboxes-search__item-label"):
                return _FakeLocator([_FakeElement(text=target, cls="checkboxes-search__item-label", tag="label")])
            if selector == ".js-checkboxes-search-list-apply":
                return _FakeLocator([_ApplyButton(text="OK", cls="js-checkboxes-search-list-apply button-input", tag="div")])
            return _FakeLocator([])

    event_input = _EventTypeInput()
    clear_btn = _ClearButton(text=u("\u041e\u0447\u0438\u0441\u0442\u0438\u0442\u044c"), cls="js-checkboxes-search-clear-all", tag="div")
    popup = _Popup(event_input, clear_btn)

    states = [
        {"popup": popup, "meta": {"popup_found": True, "popup_candidates_count": 2, "popup_has_event_type_inputs": True}},
        {"popup": None, "meta": {"popup_found": False, "popup_candidates_count": 0, "popup_has_event_type_inputs": False}},
    ]

    def _find_popup(_page):
        state = states.pop(0) if states else {"popup": None, "meta": {"popup_found": False, "popup_candidates_count": 0, "popup_has_event_type_inputs": False}}
        return state["popup"], state["meta"]

    monkeypatch.setattr(flow, "_wait_for_event_type_global_popup", lambda *_args, **_kwargs: (popup, {"popup_found": True, "popup_candidates_count": 2, "popup_has_event_type_inputs": True}))
    monkeypatch.setattr(flow, "_find_event_type_global_popup", _find_popup)

    ok, meta = flow._select_event_type_search_value(page, _FakeElement(text="root"), target, stage="event_type", scope=page)
    assert ok is True
    assert meta["clear_button_found"] is True
    assert meta["clear_clicked"] is True
    assert meta["apply_button_found"] is True
    assert meta["apply_clicked"] is True
    assert meta["popup_closed"] is True


def test_event_type_selection_sends_escape_after_pick(monkeypatch):
    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="search control", cls="checkboxes-search js-control-checkboxes-search")

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "search")
    monkeypatch.setattr(flow, "_open_event_type_search_control", lambda *_args, **_kwargs: (True, "event_type_search:title_js_click", {"root_found": True, "title_found": True}))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_self_scope"))
    monkeypatch.setattr(flow, "_select_event_type_search_value", lambda *_args, **_kwargs: (True, {"selected_option_found": True, "selected_option_text": "x"}))
    monkeypatch.setattr(flow, "_verify_checkbox_search_selection", lambda *_args, **_kwargs: (True, {"selector": "x"}))
    monkeypatch.setattr(flow, "_verify_selected_values", lambda *_args, **_kwargs: (True, "control_text"))

    flow._apply_control_values(page, object(), control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), values=[u("\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438")], stage="event_type", allow_multi=False)
    assert "Escape" in page.keyboard.pressed


def test_wait_checkbox_like_open_search_detects_scoped_buttons_wrapper():
    flow = _make_flow()

    class _Scope:
        def locator(self, selector):
            if selector == ".checkboxes-search__buttons-wrapper":
                return _FakeLocator([_FakeElement(text="", cls="checkboxes-search__buttons-wrapper")])
            return _FakeLocator([])

    class _Page(_FakePage):
        def locator(self, selector):
            return _FakeLocator([])

    assert flow._wait_checkbox_like_open(_Page(), kind="search", control_label=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), timeout_ms=300, scope=_Scope()) is True


def test_event_type_open_path_retries_before_marking_open(monkeypatch):
    flow = _make_flow()

    class _Title(_FakeElement):
        def click(self, timeout=0, force=False):
            return None

    class _Root(_FakeElement):
        def locator(self, selector):
            if selector == ".checkboxes-search__title-wrapper":
                return _FakeLocator([_Title(text="open", cls="checkboxes-search__title-wrapper")])
            return _FakeLocator([])

    root = _Root(text=u("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439"), cls="filter__custom_settings__item checkboxes-search js-control-checkboxes-search")
    monkeypatch.setattr(flow, "_resolve_event_type_search_root", lambda _page, _control: (root, "root", True))

    states = [
        {
            "root_opening_found": False,
            "root_opening_hidden": True,
            "global_opening_found": False,
            "global_opening_hidden": True,
            "global_search_input_found": False,
            "global_section_found": False,
            "global_item_label_found": False,
            "root_class": "x",
            "title_class": "y",
            "active_element": {},
        },
        {
            "root_opening_found": False,
            "root_opening_hidden": True,
            "global_opening_found": False,
            "global_opening_hidden": True,
            "global_search_input_found": False,
            "global_section_found": False,
            "global_item_label_found": False,
            "root_class": "x",
            "title_class": "y",
            "active_element": {},
        },
        {
            "root_opening_found": False,
            "root_opening_hidden": True,
            "global_opening_found": False,
            "global_opening_hidden": True,
            "global_search_input_found": False,
            "global_section_found": True,
            "global_item_label_found": False,
            "root_class": "x",
            "title_class": "y",
            "active_element": {},
        },
    ]

    calls = {"n": 0}

    def _state(*_args, **_kwargs):
        idx = min(calls["n"], len(states) - 1)
        calls["n"] += 1
        return states[idx]

    monkeypatch.setattr(flow, "_read_event_type_open_state_global", _state)

    ok, mode, _diag = flow._open_event_type_search_control(_FakePage(), root)
    assert ok is True
    assert mode == "event_type_search:title_click"
    assert calls["n"] >= 3

def test_read_events_rows_returns_empty_list_for_empty_state(monkeypatch):
    flow = _make_flow()
    page = _FakePage(
        locator_map={
            ":text('Нет данных')": _FakeLocator([_FakeElement(text=u("\u041d\u0435\u0442 \u0434\u0430\u043d\u043d\u044b\u0445"), visible=True)]),
        }
    )
    rows = flow._read_events_rows(page)
    assert rows == []


def test_read_events_rows_raises_with_debug_when_rows_visible_but_unparsed(monkeypatch):
    flow = _make_flow()
    page = _FakePage(
        locator_map={
            ".events-list__table tbody tr": _FakeLocator([_FakeElement(text="", visible=False)]),
        }
    )
    called = {"dump": False}

    def fake_dump(*, page, summary):
        called["dump"] = True
        assert summary.get("reason") in {"rows_visible_but_parse_empty", "rows_not_found"}
        return {"json": "debug.json"}

    monkeypatch.setattr(flow, "_dump_results_read_debug_artifacts", fake_dump)

    raised = False
    try:
        flow._read_events_rows(page)
    except RuntimeError as exc:
        raised = True
        assert "debug_artifacts_path" in str(exc)
    assert raised is True
    assert called["dump"] is True


def test_entity_fast_verification_returns_without_deep_reflection(monkeypatch):
    flow = _make_flow()

    class _TimedPage(_FakePage):
        def __init__(self):
            super().__init__()
            self.wait_calls = []

        def wait_for_timeout(self, ms):
            self.wait_calls.append(int(ms))
            return None

    page = _TimedPage()
    control = _FakeElement(text="checkbox control", cls="checkboxes_dropdown")

    monkeypatch.setattr(flow, "_resolve_filter_control", lambda _panel, _label: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "dropdown")
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: (True, "checkbox_title"))
    wait_calls = {"count": 0}

    def _wait_checkbox(*_args, **_kwargs):
        wait_calls["count"] += 1
        return wait_calls["count"] == 1

    monkeypatch.setattr(flow, "_wait_checkbox_like_open", _wait_checkbox)
    monkeypatch.setattr(flow, "_select_checkbox_like_value", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: None)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "control_scope"))

    called = {"verify_selected_values": 0}

    def _forbidden(*_args, **_kwargs):
        called["verify_selected_values"] += 1
        return True, "control_text"

    monkeypatch.setattr(flow, "_verify_selected_values", _forbidden)

    flow._apply_control_values(
        page,
        object(),
        control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"),
        values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")],
        stage="entity",
        allow_multi=False,
    )

    assert called["verify_selected_values"] == 0
    assert sum(page.wait_calls) <= 3000




def test_entity_branch_emits_timing_logs(monkeypatch, caplog):
    import logging

    flow = _make_flow()
    page = _FakePage()
    control = _FakeElement(text="checkbox control", cls="checkboxes_dropdown")

    monkeypatch.setattr(flow, "_resolve_filter_control_for_stage", lambda *_args, **_kwargs: (control, []))
    monkeypatch.setattr(flow, "_detect_checkbox_control_kind", lambda _control: "dropdown")
    monkeypatch.setattr(flow, "_open_checkbox_like_control", lambda *_args, **_kwargs: (True, "checkbox_title"))
    monkeypatch.setattr(flow, "_wait_checkbox_like_open", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_resolve_checkbox_scope", lambda *_args, **_kwargs: (page, "scope"))
    monkeypatch.setattr(flow, "_select_checkbox_like_value", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(flow, "_close_checkbox_dropdown_with_escape", lambda _page: None)

    with caplog.at_level(logging.INFO):
        flow._apply_control_values(page, object(), control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"), values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")], stage="entity", allow_multi=False)

    messages = "\n".join(rec.getMessage() for rec in caplog.records)
    assert "checkpoint=after_control_resolve" in messages
    assert "checkpoint=after_option_click" in messages
    assert "entity_timing_probe_version=v2" in messages
    assert "checkpoint=before_close_escape" in messages
    assert "checkpoint=after_close_escape" in messages
    assert "checkpoint=before_control_text_after" in messages
    assert "checkpoint=after_control_text_after" in messages
    assert "checkpoint=before_entity_return" in messages
    assert "total_entity_verification_ms" in messages

def test_click_apply_skips_when_button_disabled_and_empty_results(monkeypatch):
    flow = _make_flow()

    class _DisabledButton(_FakeElement):
        def evaluate(self, script):
            src = str(script)
            if "attrDisabled" in src or "button-input-disabled" in src:
                return True
            return super().evaluate(script)

    disabled_button = _DisabledButton(text=u("\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c"), cls="button-input-disabled", tag="button")

    class _Panel:
        def locator(self, selector):
            if selector == "button:has-text('Применить')":
                return _FakeLocator([disabled_button])
            return _FakeLocator([])

    monkeypatch.setattr(
        flow,
        "_collect_results_area_state",
        lambda _page: {
            "row_visible_counts": {},
            "empty_visible_counts": {".list__no-items": 1},
            "loader_counts": {},
            "container_counts": {},
        },
    )

    flow._click_apply(_FakePage(), panel=_Panel())
