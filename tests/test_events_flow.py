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
    monkeypatch.setattr(flow, "_is_checkbox_dropdown_control", lambda _control: False)

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
    flow._set_pipeline_and_statuses(
        _FakePage(),
        object(),
        _flow_input(status_before_values=[u("\u041a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f"), u("\u041f\u0435\u0440\u0432\u0438\u0447\u043d\u044b\u0439 \u043a\u043e\u043d\u0442\u0430\u043a\u0442. \u041a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f")]),
    )

    status_before_calls = [x for x in called if x[2] == "status_before"]
    assert status_before_calls
    assert status_before_calls[0][3] is True


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
    monkeypatch.setattr(flow, "_is_checkbox_dropdown_control", lambda _control: False)
    monkeypatch.setattr(flow, "_wait_for_options_popup", lambda _page, timeout_ms=1500, control_label=None: True)
    monkeypatch.setattr(flow, "_pick_option", lambda _page, *, value, stage: True)
    monkeypatch.setattr(flow, "_verify_selected_values", lambda _c, _v, require_any=False: (True, "control_text"))

    flow._apply_control_values(page, object(), control_label=u("\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438"), values=[u("\u0421\u0434\u0435\u043b\u043a\u0438")], stage="entity", allow_multi=False)
