"""Tag filter logic extracted from analytics flow."""

from __future__ import annotations

from typing import Any

from playwright.sync_api import Locator, Page


POLL_ATTEMPTS_REFLECTION = 10
POLL_DELAY_REFLECTION_MS = 150
POLL_ATTEMPTS_CHIP = 8
POLL_DELAY_CHIP_MS = 170


def _normalize_text(value: str) -> str:
    return str(value or "").strip().lower().replace("?", "?")


def _contains_target(values: list[str], target: str) -> bool:
    target_norm = _normalize_text(target)
    return any(target_norm in _normalize_text(item) for item in values)


def _locator_multisuggest_id(locator: Locator | None) -> str:
    if locator is None:
        return ""
    try:
        return str(
            locator.evaluate(
                """el => {
                    const n = el.closest('[data-multisuggest-id]');
                    return n ? (n.getAttribute('data-multisuggest-id') || '') : '';
                }"""
            )
            or ""
        ).strip()
    except Exception:
        return ""


def _is_focused_payload_valid(
    payload: dict[str, object],
    focused_multisuggest_id: str,
    authoritative_multisuggest_id: str | None = None,
    holder_multisuggest_id: str | None = None,
) -> tuple[bool, str]:
    if not authoritative_multisuggest_id and holder_multisuggest_id:
        authoritative_multisuggest_id = holder_multisuggest_id
    tag = str(payload.get("tagName", "") or "").lower()
    cls = str(payload.get("className", "") or "").lower()
    if tag != "input":
        return False, "focused_tag_not_input"
    if "multisuggest__input" not in cls and "js-multisuggest-input" not in cls:
        return False, "focused_class_not_multisuggest"
    if not focused_multisuggest_id:
        return False, "focused_multisuggest_id_missing"
    if authoritative_multisuggest_id and focused_multisuggest_id != authoritative_multisuggest_id:
        return False, "focused_multisuggest_id_mismatch"
    return True, ""


def _resolve_authoritative_multisuggest_context(
    flow: Any,
    panel: Locator,
    holder: Locator,
    fallback_holder_multisuggest_id: str | None,
) -> tuple[str | None, str, Locator | None, str, Locator | None]:
    active_popup, _payload = flow._find_active_tag_popup(panel, holder)
    popup_multisuggest_id = _locator_multisuggest_id(active_popup)
    popup_input_finder = getattr(flow, "_find_tag_input_in_popup", None)
    popup_input = (
        popup_input_finder(active_popup)
        if (active_popup is not None and callable(popup_input_finder))
        else None
    )
    holder_multisuggest_id_refreshed = _locator_multisuggest_id(holder)
    holder_is_in_edit = False
    try:
        holder_class = str(holder.get_attribute("class") or "").lower()
        holder_is_in_edit = "js-in-edit" in holder_class
    except Exception:
        holder_is_in_edit = False

    authoritative_multisuggest_id: str | None = None
    authoritative_id_source = "fallback_holder_id"
    if holder_is_in_edit and holder_multisuggest_id_refreshed:
        authoritative_multisuggest_id = holder_multisuggest_id_refreshed
        authoritative_id_source = "holder_in_edit"
    elif popup_input is not None and popup_multisuggest_id:
        authoritative_multisuggest_id = popup_multisuggest_id
        authoritative_id_source = "popup_input"
    else:
        authoritative_multisuggest_id = fallback_holder_multisuggest_id or holder_multisuggest_id_refreshed or None

    flow.logger.info("holder_multisuggest_id_refreshed=%s", holder_multisuggest_id_refreshed or "")
    flow.logger.info("popup_multisuggest_id=%s", popup_multisuggest_id or "")
    flow.logger.info("authoritative_multisuggest_id=%s", authoritative_multisuggest_id or "")
    flow.logger.info("authoritative_id_source=%s", authoritative_id_source)
    return (
        authoritative_multisuggest_id,
        authoritative_id_source,
        active_popup,
        popup_multisuggest_id,
        popup_input,
    )


def _resolve_tag_input_strict(
    flow: Any,
    page: Page,
    panel: Locator,
    holder: Locator,
    holder_multisuggest_id: str | None,
    authoritative_multisuggest_id: str | None = None,
    authoritative_id_source: str = "fallback_holder_id",
) -> tuple[Locator | None, str, Locator | None, str, str]:
    active_popup, _payload = flow._find_active_tag_popup(panel, holder)
    popup_multisuggest_id = _locator_multisuggest_id(active_popup)
    if active_popup is not None:
        popup_input = flow._find_tag_input_in_popup(active_popup)
        if popup_input is not None:
            input_msid = _locator_multisuggest_id(popup_input)
            if not authoritative_multisuggest_id or input_msid == authoritative_multisuggest_id:
                flow.logger.info("tag_input_resolution_mode=strict_popup_only")
                flow.logger.info("tag_input_multisuggest_id=%s", input_msid)
                flow.logger.info("holder_multisuggest_id=%s", holder_multisuggest_id or "")
                flow.logger.info("strict_resolution_result=accepted_popup_by_authoritative_id")
                return popup_input, "strict_popup_only", active_popup, popup_multisuggest_id, ""
            # Relaxed acceptance: nearest popup + valid popup input beats stale initial holder id.
            if popup_multisuggest_id and input_msid and popup_multisuggest_id == input_msid:
                flow.logger.info("tag_input_resolution_mode=strict_popup_nearest_relaxed")
                flow.logger.info("tag_input_multisuggest_id=%s", input_msid)
                flow.logger.info("holder_multisuggest_id=%s", holder_multisuggest_id or "")
                flow.logger.info("strict_resolution_result=accepted_popup_relaxed_nearest")
                return popup_input, "strict_popup_nearest_relaxed", active_popup, popup_multisuggest_id, ""
            flow.logger.info("tag_input_rejected_reason=popup_input_multisuggest_id_mismatch")

    # Fallback to focused element only when fully confirmed.
    try:
        focused = page.locator(":focus")
        if focused.count() > 0:
            focused_item = focused.first
            payload = flow._element_debug_payload(focused_item)
            focused_msid = _locator_multisuggest_id(focused_item)
            valid, reason = _is_focused_payload_valid(payload, focused_msid, authoritative_multisuggest_id)
            flow.logger.info("focused_multisuggest_id=%s", focused_msid)
            flow.logger.info("holder_multisuggest_id=%s", holder_multisuggest_id or "")
            flow.logger.info("authoritative_multisuggest_id=%s", authoritative_multisuggest_id or "")
            flow.logger.info("authoritative_id_source=%s", authoritative_id_source)
            if valid:
                flow.logger.info("tag_input_resolution_mode=strict_popup_or_focused_confirmed")
                flow.logger.info("tag_input_multisuggest_id=%s", focused_msid)
                flow.logger.info("strict_resolution_result=accepted_focused_by_authoritative_id")
                return focused_item, "strict_popup_or_focused_confirmed", active_popup, popup_multisuggest_id, ""
            flow.logger.info("tag_input_rejected_reason=%s", reason)
    except Exception:
        flow.logger.info("tag_input_rejected_reason=focused_lookup_failed")

    flow.logger.info("strict_resolution_result=rejected_input_not_resolved")
    return None, "", active_popup, popup_multisuggest_id, "input_not_resolved"


def _wait_until_tag_value_reflected(
    flow: Any,
    page: Page,
    panel: Locator,
    holder: Locator,
    input_target: Locator,
    active_popup: Locator | None,
    target_value: str,
    expected_multisuggest_id: str | None,
) -> tuple[bool, dict[str, object]]:
    diagnostics: dict[str, object] = {
        "attempts": 0,
        "mode": "none",
        "input_value": "",
        "focused_value": "",
        "holder_text": "",
        "popup_open": False,
        "suggestions": [],
    }
    target_norm = _normalize_text(target_value)

    for attempt in range(1, POLL_ATTEMPTS_REFLECTION + 1):
        diagnostics["attempts"] = attempt
        page.wait_for_timeout(POLL_DELAY_REFLECTION_MS)

        popup_now, _ = flow._find_active_tag_popup(panel, holder)
        popup_in_use = popup_now or active_popup
        diagnostics["popup_open"] = popup_in_use is not None

        # A) input.value reflected
        try:
            value_now = str(input_target.input_value(timeout=120) or "").strip()
        except Exception:
            try:
                value_now = str(input_target.get_attribute("value") or "").strip()
            except Exception:
                value_now = ""
        diagnostics["input_value"] = value_now
        if target_norm and target_norm in _normalize_text(value_now):
            diagnostics["mode"] = "input_value"
            return True, diagnostics

        # A2) focused input value reflected (helps with widget input re-binding)
        try:
            focused = page.locator(":focus")
            if focused.count() > 0:
                focused_item = focused.first
                try:
                    focused_value = str(focused_item.input_value(timeout=120) or "").strip()
                except Exception:
                    focused_value = str(focused_item.get_attribute("value") or "").strip()
            else:
                focused_value = ""
        except Exception:
            focused_value = ""
        diagnostics["focused_value"] = focused_value
        if target_norm and target_norm in _normalize_text(focused_value):
            diagnostics["mode"] = "focused_value"
            return True, diagnostics

        # B/C) popup hint/suggestion reflected
        suggestions = flow._collect_visible_tag_suggestion_texts(
            panel,
            popup=popup_in_use,
            expected_multisuggest_id=expected_multisuggest_id,
        )
        diagnostics["suggestions"] = suggestions[:20]
        if _contains_target(suggestions, target_value):
            diagnostics["mode"] = "popup_suggestion"
            return True, diagnostics

        try:
            popup_text = (popup_in_use.inner_text(timeout=120).strip() if popup_in_use is not None else "")
        except Exception:
            popup_text = ""
        if popup_text and target_norm in _normalize_text(popup_text):
            diagnostics["mode"] = "popup_hint"
            return True, diagnostics

        # D) holder text can briefly reflect query/hint when popup re-binds
        try:
            holder_text = holder.inner_text(timeout=120).strip()
        except Exception:
            holder_text = ""
        diagnostics["holder_text"] = holder_text[:240]
        if holder_text and target_norm in _normalize_text(holder_text):
            diagnostics["mode"] = "holder_text"
            return True, diagnostics

    return False, diagnostics


def _poll_chip_detect(flow: Any, panel: Locator, holder: Locator, target_value: str, attempts: int = POLL_ATTEMPTS_CHIP) -> tuple[bool, list[str]]:
    chips: list[str] = []
    target_norm = _normalize_text(target_value)
    for _ in range(attempts):
        panel.page.wait_for_timeout(POLL_DELAY_CHIP_MS)
        chips = flow._collect_tag_chip_texts(panel, holder=holder)
        if any(target_norm in _normalize_text(chip) for chip in chips):
            return True, chips
    return False, chips


def _candidate_backend_id(item: Locator) -> str:
    attrs = (
        "data-id",
        "data-value",
        "data-tag-id",
        "data-value-id",
        "lol-id",
        "id",
    )
    for attr in attrs:
        try:
            value = str(item.get_attribute(attr) or "").strip()
        except Exception:
            value = ""
        if value:
            return value
    try:
        eval_value = item.evaluate(
            """el => {
                const attrs = ['data-id','data-value','data-tag-id','data-value-id','lol-id','id'];
                for (const key of attrs) {
                    const v = el.getAttribute(key);
                    if (v) return String(v);
                }
                const ds = el.dataset || {};
                for (const key of Object.keys(ds)) {
                    const v = ds[key];
                    if (v) return String(v);
                }
                return '';
            }"""
        )
        return str(eval_value or "").strip()
    except Exception:
        return ""


def _collect_visible_tag_suggestion_candidates(
    flow: Any,
    panel: Locator,
    popup: Locator | None,
    expected_multisuggest_id: str | None,
) -> list[dict[str, object]]:
    selectors = (
        "li.multisuggest__list-item",
        "[role='option']",
        "[class*='suggest-item']",
    )
    out: list[dict[str, object]] = []
    for selector in selectors:
        root = popup if (popup is not None and hasattr(popup, "locator")) else panel
        if not hasattr(root, "locator"):
            continue
        loc = root.locator(selector)
        try:
            count = min(loc.count(), 120)
        except Exception:
            continue
        for idx in range(count):
            item = loc.nth(idx)
            try:
                if not item.is_visible(timeout=120):
                    continue
                cls = (item.get_attribute("class") or "").lower()
                if "multisuggest__list-item_input" in cls or "js-multisuggest-input" in cls:
                    continue
                text = item.inner_text(timeout=120).strip()
                if not text:
                    continue
                ms_id = str(
                    item.evaluate(
                        """el => {
                            const n = el.closest('[data-multisuggest-id]');
                            return n ? (n.getAttribute('data-multisuggest-id') || '') : '';
                        }"""
                    )
                    or ""
                ).strip()
                if expected_multisuggest_id and ms_id and ms_id != expected_multisuggest_id:
                    continue
                out.append(
                    {
                        "text": text,
                        "index": idx,
                        "selector": selector,
                        "id": _candidate_backend_id(item),
                        "popup_multisuggest_id": ms_id,
                    }
                )
            except Exception:
                continue
    return out


def _matching_candidates_for_value(candidates: list[dict[str, object]], value: str) -> list[dict[str, object]]:
    target = _normalize_text(value)
    exact = [item for item in candidates if _normalize_text(str(item.get("text", ""))) == target]
    if exact:
        return exact
    return [item for item in candidates if target in _normalize_text(str(item.get("text", "")))]


def _click_suggestion_candidate(panel: Locator, popup: Locator | None, candidate: dict[str, object]) -> bool:
    selector = str(candidate.get("selector", "") or "")
    if not selector:
        return False
    try:
        index = int(candidate.get("index", -1))
    except Exception:
        index = -1
    if index < 0:
        return False
    root = popup or panel
    try:
        locator = root.locator(selector)
        if locator.count() <= index:
            return False
        item = locator.nth(index)
        if not item.is_visible(timeout=220):
            return False
        try:
            item.click(timeout=1200)
            return True
        except Exception:
            try:
                item.evaluate("el => el.click()")
                return True
            except Exception:
                return False
    except Exception:
        return False


def _resolve_holder_click_target(holder: Locator) -> tuple[Locator, str]:
    candidates = (
        ("inner_multisuggest", "div.multisuggest.filter-tags-items.js-multisuggest.js-can-add"),
        ("filter_search_tags", "div.filter-search__tags"),
        ("holder", None),
    )
    click_target = holder
    click_target_name = "holder"
    for name, selector in candidates:
        if selector is None:
            click_target = holder
            click_target_name = name
            break
        try:
            loc = holder.locator(selector)
            if loc.count() > 0 and loc.first.is_visible(timeout=220):
                click_target = loc.first
                click_target_name = name
                break
        except Exception:
            continue
    return click_target, click_target_name


def _normalized_unique_values(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        norm = _normalize_text(item)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _chip_set_guard(
    expected_values: list[str],
    actual_chip_texts: list[str],
) -> tuple[bool, list[str], list[str]]:
    expected = _normalized_unique_values(expected_values)
    actual = _normalized_unique_values(actual_chip_texts)
    ok = all(item in actual for item in expected)
    return ok, expected, actual


def _clear_tag_holder_selection(flow: Any, page: Page, panel: Locator, holder: Locator) -> bool:
    for _ in range(10):
        chips = flow._collect_tag_chip_texts(panel, holder=holder)
        if not chips:
            return True
        cleared = False
        delete_selectors = (
            "li.js-multisuggest-item [class*='delete']",
            "li.js-multisuggest-item [class*='remove']",
            "li.js-multisuggest-item [class*='close']",
            "li.js-multisuggest-item [data-action='delete']",
            "li.js-multisuggest-item [data-role='delete']",
        )
        for selector in delete_selectors:
            try:
                items = holder.locator(selector)
                count = min(items.count(), 6)
            except Exception:
                count = 0
            for idx in range(count):
                try:
                    node = items.nth(idx)
                    if not node.is_visible(timeout=120):
                        continue
                    node.click(timeout=600)
                    page.wait_for_timeout(120)
                    cleared = True
                    break
                except Exception:
                    continue
            if cleared:
                break
        if cleared:
            continue
        try:
            holder.click(timeout=500)
        except Exception:
            pass
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        page.keyboard.press("Backspace")
        page.wait_for_timeout(140)
    return len(flow._collect_tag_chip_texts(panel, holder=holder)) == 0


def _get_post_apply_parse_signal(flow: Any, page: Page, report_id: str, target_value: str) -> dict[str, object]:
    callback = getattr(flow, "_get_post_apply_parse_signal", None)
    if not callable(callback):
        return {
            "apply_confirmed_but_parse_suspicious": False,
            "source": "none",
        }
    try:
        payload = callback(page=page, report_id=report_id, target_value=target_value)
    except TypeError:
        try:
            payload = callback(page, report_id, target_value)
        except Exception:
            payload = None
    except Exception:
        payload = None
    if not isinstance(payload, dict):
        return {
            "apply_confirmed_but_parse_suspicious": False,
            "source": "invalid_payload",
        }
    if "apply_confirmed_but_parse_suspicious" not in payload:
        payload["apply_confirmed_but_parse_suspicious"] = False
    return payload


def find_strict_tag_holder(flow: Any, panel: Locator) -> tuple[Locator | None, str | None]:
    selectors = (
        "div.filter-search__tags-holder[data-title='\u0422\u0435\u0433\u0438'][data-input-name='tag[]']",
        "div.filter-search__tags-holder[data-input-name='tag[]'][data-title='\u0422\u0435\u0433\u0438']",
    )
    holder: Locator | None = None
    for selector in selectors:
        locator = panel.locator(selector)
        try:
            count = min(locator.count(), 6)
        except Exception:
            continue
        for idx in range(count):
            item = locator.nth(idx)
            try:
                if item.is_visible(timeout=250):
                    holder = item
                    break
            except Exception:
                continue
        if holder is not None:
            break

    if holder is None:
        flow.logger.info("tag_holder_found: false")
        return None, None

    holder_id = None
    try:
        ms = holder.locator(".filter-search__tags .js-multisuggest[data-multisuggest-id], .js-multisuggest[data-multisuggest-id]")
        if ms.count() > 0:
            holder_id = (ms.first.get_attribute("data-multisuggest-id") or "").strip() or None
    except Exception:
        holder_id = None

    flow.logger.info("tag_holder_found: true")
    flow.logger.info("tag_holder_multisuggest_id=%s", holder_id or "")
    try:
        html = holder.evaluate("el => (el.outerHTML || '').slice(0, 1200)")
    except Exception:
        html = ""
    flow.logger.info("tag_holder_outer_html_snippet=%s", str(html or "").replace("\n", " "))
    return holder, holder_id


def collect_tag_chip_texts(flow: Any, panel: Locator, holder: Locator | None = None) -> list[str]:
    root = holder if holder is not None else panel

    if holder is not None:
        chip_selectors = (
            "li.js-multisuggest-item",
            "li.multisuggest__list-item.js-multisuggest-item",
            "span.tag",
            "span.tag[title]",
        )
    else:
        chip_selectors = (
            ".filter-search__tags-holder li.js-multisuggest-item",
            ".filter-search__tags-holder li.multisuggest__list-item.js-multisuggest-item",
            ".filter-search__tags-holder span.tag",
            ".filter-search__tags-holder span.tag[title]",
        )

    chips: list[str] = []
    seen: set[str] = set()
    for selector in chip_selectors:
        items = root.locator(selector)
        try:
            count = min(items.count(), 120)
        except Exception:
            continue
        for idx in range(count):
            item = items.nth(idx)
            try:
                if not item.is_visible(timeout=120):
                    continue
                cls = (item.get_attribute("class") or "").lower()
                if "multisuggest__list-item_input" in cls or "js-multisuggest-input" in cls:
                    continue
                li_cls = str(
                    item.evaluate(
                        """el => {
                            const li = el.closest('li');
                            return li ? (li.className || '') : '';
                        }"""
                    )
                    or ""
                ).lower()
                if "multisuggest__list-item_input" in li_cls:
                    continue
                txt = item.inner_text(timeout=120).strip()
                if not txt:
                    txt = (item.get_attribute("title") or "").strip()
            except Exception:
                continue
            if not txt:
                continue
            key = txt.lower()
            if key in seen:
                continue
            seen.add(key)
            chips.append(txt)

    flow.logger.info("collected_tag_chip_texts=%s", chips[:30])
    return chips


def has_selected_tag_chip(flow: Any, panel: Locator, target_value: str, holder: Locator | None = None) -> bool:
    value = target_value.strip().lower()
    if not value:
        flow.logger.info("tag_chip_present=false")
        flow.logger.info("tag_chip_texts=[]")
        return False

    chip_texts = collect_tag_chip_texts(flow, panel, holder=holder)
    normalized_target = value.strip().lower().replace("?", "?")
    present = False
    for text in chip_texts:
        norm = text.strip().lower().replace("?", "?")
        if normalized_target in norm:
            present = True
            break

    flow.logger.info("tag_chip_present=%s", str(present).lower())
    flow.logger.info("tag_chip_texts=%s", chip_texts[:20])
    return present


def _apply_single_tag_value_with_candidate(
    flow: Any,
    page: Page,
    panel: Locator,
    holder: Locator,
    holder_id: str | None,
    report_id: str,
    value: str,
    forced_candidate: dict[str, object] | None = None,
) -> tuple[bool, dict[str, object]]:
    info: dict[str, object] = {
        "duplicate_candidates": [],
        "selected_candidate_id": "",
        "selected_candidate_index": -1,
        "apply_confirmed_but_parse_suspicious": False,
        "parse_signal": {},
    }
    input_target: Locator | None = None
    input_selector = ""
    active_popup: Locator | None = None
    popup_multisuggest_id = ""
    holder_multisuggest_id_initial = holder_id or ""
    flow.logger.info("holder_multisuggest_id_initial=%s", holder_multisuggest_id_initial)

    click_target, click_target_name = _resolve_holder_click_target(holder)
    flow.logger.info("holder_click_target=%s", click_target_name)

    for attempt in range(1, 6):
        flow.logger.info("holder_click_attempt=%s/5", attempt)
        try:
            if attempt == 1:
                flow._click_locator_point(page, click_target, 0.50, 0.50)
            elif attempt == 2:
                flow._click_locator_point(page, click_target, 0.86, 0.50)
            elif attempt == 3:
                flow._click_locator_point(page, click_target, 0.60, 0.82)
            elif attempt == 4:
                click_target.dblclick(timeout=1000)
            else:
                click_target.evaluate(
                    "el => { const m=(t)=>new MouseEvent(t,{bubbles:true,cancelable:true,view:window}); el.dispatchEvent(m('mousedown')); el.dispatchEvent(m('mouseup')); el.dispatchEvent(m('click')); if (typeof el.focus === 'function') el.focus(); }"
                )
        except Exception:
            try:
                holder.click(timeout=1000)
            except Exception:
                pass

        page.wait_for_timeout(180)
        refreshed_holder, refreshed_holder_id = flow._find_strict_tag_holder(panel)
        if refreshed_holder is not None:
            holder = refreshed_holder
            holder_id = refreshed_holder_id
            click_target, click_target_name = _resolve_holder_click_target(holder)
            flow.logger.info("holder_click_target_refreshed=%s", click_target_name)
        authoritative_id, authoritative_source, _active_popup_ctx, _popup_id_ctx, _popup_input_ctx = _resolve_authoritative_multisuggest_context(
            flow=flow,
            panel=panel,
            holder=holder,
            fallback_holder_multisuggest_id=holder_id,
        )
        input_target, input_selector, active_popup, popup_multisuggest_id, reject_reason = _resolve_tag_input_strict(
            flow=flow,
            page=page,
            panel=panel,
            holder=holder,
            holder_multisuggest_id=holder_id,
            authoritative_multisuggest_id=authoritative_id,
            authoritative_id_source=authoritative_source,
        )
        flow.logger.info("active_popup_found=%s", str(active_popup is not None).lower())
        flow.logger.info("tag_popup_input_found=%s", str(input_target is not None).lower())
        if input_target is not None:
            flow.logger.info("popup_selected_input=%s", input_selector)
            flow.logger.info("tag_popup_input_selector=%s", input_selector)
            flow.logger.info("popup_multisuggest_id=%s", popup_multisuggest_id)
            flow.logger.info("popup_id_matches_holder=%s", str(bool(holder_id and popup_multisuggest_id == holder_id)).lower())
            flow.logger.info("strict_resolution_result=accepted")
            break
        flow.logger.info("tag_input_rejected_reason=%s", reject_reason)

    if input_target is None:
        flow.logger.error("final_fail_reason=no_active_popup_or_input_after_attempts")
        return False, info

    reflected = False
    reflection_diag: dict[str, object] = {}
    for cycle in range(1, 4):
        refreshed_holder, refreshed_holder_id = flow._find_strict_tag_holder(panel)
        if refreshed_holder is not None:
            holder = refreshed_holder
            holder_id = refreshed_holder_id
        authoritative_id, authoritative_source, _active_popup_ctx, _popup_id_ctx, _popup_input_ctx = _resolve_authoritative_multisuggest_context(
            flow=flow,
            panel=panel,
            holder=holder,
            fallback_holder_multisuggest_id=holder_id,
        )
        refreshed_input, refreshed_selector, refreshed_popup, refreshed_popup_id, reject_reason = _resolve_tag_input_strict(
            flow=flow,
            page=page,
            panel=panel,
            holder=holder,
            holder_multisuggest_id=holder_id,
            authoritative_multisuggest_id=authoritative_id,
            authoritative_id_source=authoritative_source,
        )
        if refreshed_input is not None:
            input_target = refreshed_input
            input_selector = refreshed_selector
            active_popup = refreshed_popup
            popup_multisuggest_id = refreshed_popup_id
            flow.logger.info("tag_reflection_cycle=%s refreshed_input=true selector=%s", cycle, input_selector)
        else:
            flow.logger.info("tag_reflection_cycle=%s refreshed_input=false reason=%s", cycle, reject_reason)

        try:
            input_target.click(timeout=1200)
        except Exception:
            pass
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        page.keyboard.type(value, delay=20)
        flow.logger.info('tag_typed="%s"', value)

        reflected, reflection_diag = _wait_until_tag_value_reflected(
            flow=flow,
            page=page,
            panel=panel,
            holder=holder,
            input_target=input_target,
            active_popup=active_popup,
            target_value=value,
            expected_multisuggest_id=authoritative_id,
        )
        flow.logger.info("tag_value_reflection_success=%s", str(bool(reflected)).lower())
        flow.logger.info("tag_value_reflection_mode=%s", reflection_diag.get("mode", "none"))
        flow.logger.info("tag_value_reflection_diagnostics=%s", reflection_diag)
        if reflected:
            break
        page.wait_for_timeout(120)

    if not reflected:
        html_path, txt_path = flow._save_tag_holder_after_enter_artifacts(
            holder=holder,
            chip_texts=[],
            target_value=value,
        )
        flow.logger.info("tag_holder_after_enter_html=%s", html_path)
        flow.logger.info("tag_holder_after_enter_txt=%s", txt_path)
        flow.logger.error("final_fail_reason=value_not_reflected_before_enter")
        return False, info

    popup, _payload = flow._find_active_tag_popup(panel, holder)
    suggestions = flow._collect_visible_tag_suggestion_texts(
        panel,
        popup=popup,
        expected_multisuggest_id=popup_multisuggest_id or holder_id,
    )
    flow.logger.info("tag_visible_suggestions_before_confirm=%s", suggestions[:20])
    candidates = _collect_visible_tag_suggestion_candidates(
        flow=flow,
        panel=panel,
        popup=popup,
        expected_multisuggest_id=popup_multisuggest_id or holder_id,
    )
    matching_candidates = _matching_candidates_for_value(candidates, value)
    info["duplicate_candidates"] = matching_candidates
    duplicate_found = len(matching_candidates) > 1
    flow.logger.info("duplicate_tag_candidates_found=%s", str(duplicate_found).lower())
    if matching_candidates:
        flow.logger.info("duplicate_tag_candidates=%s", matching_candidates)

    selected_candidate = forced_candidate
    if selected_candidate is None and matching_candidates:
        selected_candidate = matching_candidates[0]

    confirm_strategy = "fail"
    if selected_candidate is not None:
        clicked_selected = _click_suggestion_candidate(panel, popup, selected_candidate)
        flow.logger.info("tag_selected_candidate_click_success=%s", str(bool(clicked_selected)).lower())
        if clicked_selected:
            confirm_strategy = "click_duplicate_candidate"
            info["selected_candidate_id"] = str(selected_candidate.get("id", "") or "")
            try:
                info["selected_candidate_index"] = int(selected_candidate.get("index", -1))
            except Exception:
                info["selected_candidate_index"] = -1
    elif _contains_target(suggestions, value):
        page.keyboard.press("Enter")
        flow.logger.info("tag_enter_pressed=true")
        confirm_strategy = "enter_exact_visible"
    elif popup is not None:
        page.keyboard.press("ArrowDown")
        page.keyboard.press("Enter")
        flow.logger.info("tag_enter_pressed=true")
        flow.logger.info("tag_enter_fallback_arrowdown_used=true")
        confirm_strategy = "arrowdown_enter"
    else:
        flow.logger.info("tag_enter_fallback_arrowdown_used=false")

    chip_ok, chips = _poll_chip_detect(flow, panel, holder, value)
    fallback_option_click_used = False
    chip_after_fallback = chip_ok

    if not chip_ok:
        popup_retry, _ = flow._find_active_tag_popup(panel, holder)
        popup_id_retry = _locator_multisuggest_id(popup_retry)
        option_clicked, _option_strategy, _option_payload = flow._select_real_tag_option(
            panel,
            value,
            holder=holder,
            expected_multisuggest_id=holder_id or popup_id_retry or None,
        )
        fallback_option_click_used = bool(option_clicked)
        flow.logger.info("tag_fallback_option_click_used=%s", str(fallback_option_click_used).lower())
        if option_clicked:
            confirm_strategy = "click_real_option"
            chip_after_fallback, chips = _poll_chip_detect(flow, panel, holder, value)
            flow.logger.info("tag_chip_detect_after_fallback=%s", str(bool(chip_after_fallback)).lower())
        else:
            flow.logger.info("tag_chip_detect_after_fallback=false")

    flow.logger.info("tag_confirm_strategy=%s", confirm_strategy)
    flow.logger.info("selected_tag_candidate_id=%s", info.get("selected_candidate_id", ""))
    flow.logger.info("selected_tag_candidate_index=%s", info.get("selected_candidate_index", -1))

    if not chip_after_fallback:
        html_path, txt_path = flow._save_tag_holder_after_enter_artifacts(
            holder=holder,
            chip_texts=chips,
            target_value=value,
        )
        flow.logger.info("tag_holder_after_enter_html=%s", html_path)
        flow.logger.info("tag_holder_after_enter_txt=%s", txt_path)
        flow.logger.error("final_fail_reason=chip_not_detected_after_enter_and_fallback")
        return False, info

    page.keyboard.press("Escape")
    flow.logger.info("tag_escape_sent=true")
    page.wait_for_timeout(220)

    url_before = str(page.url)
    clicked = flow._click_apply_in_panel(page, url_before=url_before, report_id=report_id)
    if not clicked:
        panel = flow._find_filter_panel_container(page)
        flow._scroll_filter_panel_to_bottom(panel)
        clicked = flow._click_apply_in_panel(page, url_before=url_before, report_id=report_id)
    flow.logger.info("tag_apply_clicked=%s", str(bool(clicked)).lower())
    if not clicked:
        flow.logger.error("final_fail_reason=apply_not_clicked")
        return False, info

    page.wait_for_timeout(600)
    panel = flow._find_filter_panel_container(page)
    confirmed = flow._is_filter_apply_confirmed_by_url(page, url_before)
    if not confirmed:
        confirmed = flow._is_filter_apply_confirmed(page, panel, url_before, value)
    if not confirmed:
        flow.logger.error("final_fail_reason=apply_not_confirmed")
        return False, info

    parse_signal = _get_post_apply_parse_signal(flow, page, report_id=report_id, target_value=value)
    info["parse_signal"] = parse_signal
    info["apply_confirmed_but_parse_suspicious"] = bool(parse_signal.get("apply_confirmed_but_parse_suspicious"))
    flow.logger.info("apply_confirmed_but_parse_suspicious=%s", str(bool(info["apply_confirmed_but_parse_suspicious"])).lower())
    flow._apply_already_confirmed = True
    return True, info


def apply_tag_values_via_holder_popup(
    flow: Any,
    page: Page,
    panel: Locator,
    report_id: str,
    values: list[str],
) -> bool:
    flow.logger.info("tag_path_used=holder_popup")
    tag_values = [str(v).strip() for v in values if str(v).strip()]
    if not tag_values:
        flow.logger.error("final_fail_reason=empty_tag_values")
        return False

    holder, holder_id = flow._find_strict_tag_holder(panel)
    flow.logger.info("tag_legacy_holder_found=%s", str(holder is not None).lower())
    if holder is None:
        flow.logger.error("final_fail_reason=holder_not_found")
        return False

    if len(tag_values) == 1:
        value = tag_values[0]
        success, info = _apply_single_tag_value_with_candidate(
            flow=flow,
            page=page,
            panel=panel,
            holder=holder,
            holder_id=holder_id,
            report_id=report_id,
            value=value,
            forced_candidate=None,
        )
        if not success:
            return False

        candidates = list(info.get("duplicate_candidates") or [])
        parse_suspicious = bool(info.get("apply_confirmed_but_parse_suspicious"))
        final_candidate_id = str(info.get("selected_candidate_id", "") or "")
        if parse_suspicious and len(candidates) > 1:
            retry_index = 1
            exhausted = True
            while retry_index < len(candidates):
                flow.logger.info("duplicate_retry_attempt=%s/%s", retry_index + 1, len(candidates))
                flow._open_filter_panel(page)
                panel_retry = flow._find_filter_panel_container(page)
                holder_retry, holder_retry_id = flow._find_strict_tag_holder(panel_retry)
                if holder_retry is None:
                    flow.logger.error("duplicate_retry_failed_reason=holder_not_found")
                    break
                _clear_tag_holder_selection(flow, page, panel_retry, holder_retry)
                success_retry, info_retry = _apply_single_tag_value_with_candidate(
                    flow=flow,
                    page=page,
                    panel=panel_retry,
                    holder=holder_retry,
                    holder_id=holder_retry_id,
                    report_id=report_id,
                    value=value,
                    forced_candidate=candidates[retry_index],
                )
                if success_retry:
                    final_candidate_id = str(info_retry.get("selected_candidate_id", "") or final_candidate_id)
                    if not bool(info_retry.get("apply_confirmed_but_parse_suspicious")):
                        exhausted = False
                        break
                retry_index += 1
            if exhausted:
                flow.logger.error("duplicate_retry_exhausted=true")
                return False
        flow.logger.info("final_selected_tag_candidate_id=%s", final_candidate_id)
        return True

    click_target, click_target_name = _resolve_holder_click_target(holder)
    flow.logger.info("holder_click_target=%s", click_target_name)
    expected_selected_values_so_far: list[str] = []

    for value_idx, value in enumerate(tag_values, start=1):
        if not value:
            continue

        input_target: Locator | None = None
        input_selector = ""
        active_popup: Locator | None = None
        popup_multisuggest_id = ""

        for attempt in range(1, 6):
            flow.logger.info("holder_click_attempt=%s/5", attempt)
            try:
                if attempt == 1:
                    flow._click_locator_point(page, click_target, 0.50, 0.50)
                elif attempt == 2:
                    flow._click_locator_point(page, click_target, 0.86, 0.50)
                elif attempt == 3:
                    flow._click_locator_point(page, click_target, 0.60, 0.82)
                elif attempt == 4:
                    click_target.dblclick(timeout=1000)
                else:
                    click_target.evaluate(
                        "el => { const m=(t)=>new MouseEvent(t,{bubbles:true,cancelable:true,view:window}); el.dispatchEvent(m('mousedown')); el.dispatchEvent(m('mouseup')); el.dispatchEvent(m('click')); if (typeof el.focus === 'function') el.focus(); }"
                    )
            except Exception:
                try:
                    holder.click(timeout=1000)
                except Exception:
                    pass

            page.wait_for_timeout(180)
            refreshed_holder, refreshed_holder_id = flow._find_strict_tag_holder(panel)
            if refreshed_holder is not None:
                holder = refreshed_holder
                holder_id = refreshed_holder_id
                click_target, click_target_name = _resolve_holder_click_target(holder)
                flow.logger.info("holder_click_target_refreshed=%s", click_target_name)
            input_target, input_selector, active_popup, popup_multisuggest_id, reject_reason = _resolve_tag_input_strict(
                flow=flow,
                page=page,
                panel=panel,
                holder=holder,
                holder_multisuggest_id=holder_id,
            )
            flow.logger.info("active_popup_found=%s", str(active_popup is not None).lower())
            flow.logger.info("tag_popup_input_found=%s", str(input_target is not None).lower())
            if input_target is not None:
                flow.logger.info("popup_selected_input=%s", input_selector)
                flow.logger.info("tag_popup_input_selector=%s", input_selector)
                flow.logger.info("popup_multisuggest_id=%s", popup_multisuggest_id)
                flow.logger.info("popup_id_matches_holder=%s", str(bool(holder_id and popup_multisuggest_id == holder_id)).lower())
                break
            flow.logger.info("tag_input_rejected_reason=%s", reject_reason)

        if input_target is None:
            flow.logger.error("final_fail_reason=no_active_popup_or_input_after_attempts")
            return False

        holder_before = flow._holder_outer_html_snippet(holder)
        flow.logger.info("holder outerHTML before typing=%s", holder_before)

        reflected = False
        reflection_diag: dict[str, object] = {}
        for cycle in range(1, 4):
            refreshed_input, refreshed_selector, refreshed_popup, refreshed_popup_id, reject_reason = _resolve_tag_input_strict(
                flow=flow,
                page=page,
                panel=panel,
                holder=holder,
                holder_multisuggest_id=holder_id,
            )
            if refreshed_input is not None:
                input_target = refreshed_input
                input_selector = refreshed_selector
                active_popup = refreshed_popup
                popup_multisuggest_id = refreshed_popup_id
                flow.logger.info("tag_reflection_cycle=%s refreshed_input=true selector=%s", cycle, input_selector)
            else:
                flow.logger.info("tag_reflection_cycle=%s refreshed_input=false reason=%s", cycle, reject_reason)

            try:
                input_target.click(timeout=1200)
            except Exception:
                pass
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            page.keyboard.type(value, delay=20)
            flow.logger.info('tag_typed="%s"', value)

            reflected, reflection_diag = _wait_until_tag_value_reflected(
                flow=flow,
                page=page,
                panel=panel,
                holder=holder,
                input_target=input_target,
                active_popup=active_popup,
                target_value=value,
                expected_multisuggest_id=holder_id,
            )
            flow.logger.info("tag_value_reflection_success=%s", str(bool(reflected)).lower())
            flow.logger.info("tag_value_reflection_mode=%s", reflection_diag.get("mode", "none"))
            flow.logger.info("tag_value_reflection_diagnostics=%s", reflection_diag)
            if reflected:
                break
            page.wait_for_timeout(120)

        if not reflected:
            html_path, txt_path = flow._save_tag_holder_after_enter_artifacts(
                holder=holder,
                chip_texts=[],
                target_value=value,
            )
            flow.logger.info("tag_holder_after_enter_html=%s", html_path)
            flow.logger.info("tag_holder_after_enter_txt=%s", txt_path)
            flow.logger.error("final_fail_reason=value_not_reflected_before_enter")
            return False

        popup, _payload = flow._find_active_tag_popup(panel, holder)
        suggestions = flow._collect_visible_tag_suggestion_texts(
            panel,
            popup=popup,
            expected_multisuggest_id=holder_id,
        )
        flow.logger.info("tag_visible_suggestions_before_confirm=%s", suggestions[:20])
        candidate_items = _collect_visible_tag_suggestion_candidates(
            flow=flow,
            panel=panel,
            popup=popup,
            expected_multisuggest_id=holder_id,
        )
        matching_candidates = _matching_candidates_for_value(candidate_items, value)

        confirm_strategy = "fail"
        if matching_candidates:
            clicked_selected = _click_suggestion_candidate(panel, popup, matching_candidates[0])
            if clicked_selected:
                confirm_strategy = "click_exact_candidate"
        elif _contains_target(suggestions, value):
            page.keyboard.press("Enter")
            flow.logger.info("tag_enter_pressed=true")
            confirm_strategy = "enter_exact_visible"
        else:
            flow.logger.info("tag_enter_fallback_arrowdown_used=false")
            flow.logger.info("tag_multi_no_exact_candidate=true")

        chip_ok, chips = _poll_chip_detect(flow, panel, holder, value)
        fallback_option_click_used = False
        chip_after_fallback = chip_ok

        if not chip_ok:
            popup_retry, _ = flow._find_active_tag_popup(panel, holder)
            popup_id_retry = _locator_multisuggest_id(popup_retry)
            option_clicked, _option_strategy, _option_payload = flow._select_real_tag_option(
                panel,
                value,
                holder=holder,
                expected_multisuggest_id=holder_id or popup_id_retry or None,
            )
            fallback_option_click_used = bool(option_clicked)
            flow.logger.info("tag_fallback_option_click_used=%s", str(fallback_option_click_used).lower())
            if option_clicked:
                confirm_strategy = "click_real_option"
                chip_after_fallback, chips = _poll_chip_detect(flow, panel, holder, value)
                flow.logger.info("tag_chip_detect_after_fallback=%s", str(bool(chip_after_fallback)).lower())
            else:
                flow.logger.info("tag_chip_detect_after_fallback=false")

        flow.logger.info("tag_confirm_strategy=%s", confirm_strategy)

        holder_after = flow._holder_outer_html_snippet(holder)
        flow.logger.info("holder outerHTML after Enter=%s", holder_after)
        flow.logger.info("chip_texts_after_enter=%s", chips[:30])
        flow.logger.info("success_detected=%s", str(bool(chip_after_fallback)).lower())
        flow.logger.info("tag_chip_texts=%s", chips[:20])
        flow.logger.info("tag_selection_success=%s", str(bool(chip_after_fallback)).lower())

        if not chip_after_fallback:
            html_path, txt_path = flow._save_tag_holder_after_enter_artifacts(
                holder=holder,
                chip_texts=chips,
                target_value=value,
            )
            flow.logger.info("tag_holder_after_enter_html=%s", html_path)
            flow.logger.info("tag_holder_after_enter_txt=%s", txt_path)
            flow.logger.error("final_fail_reason=chip_not_detected_after_enter_and_fallback")
            return False

        expected_selected_values_so_far.append(value)
        guard_ok, expected_set, actual_set = _chip_set_guard(
            expected_values=expected_selected_values_so_far,
            actual_chip_texts=chips,
        )
        flow.logger.info("tag_chip_regression_detected=%s", str((not guard_ok)).lower())
        flow.logger.info("expected_chip_set=%s", expected_set)
        flow.logger.info("actual_chip_set=%s", actual_set)
        if not guard_ok:
            flow.logger.error("final_fail_reason=tag_chip_regression_detected")
            return False

    final_chip_texts = flow._collect_tag_chip_texts(panel, holder=holder)
    final_guard_ok, expected_final_set, actual_final_set = _chip_set_guard(
        expected_values=tag_values,
        actual_chip_texts=final_chip_texts,
    )
    final_count_ok = len(actual_final_set) >= len(expected_final_set)
    flow.logger.info(
        "multi_tag_apply_guard_passed=%s",
        str(bool(final_guard_ok and final_count_ok)).lower(),
    )
    if not (final_guard_ok and final_count_ok):
        reason = "missing_requested_chip" if not final_guard_ok else "actual_chip_count_less_than_requested_unique"
        flow.logger.info("multi_tag_apply_guard_reason=%s", reason)
        flow.logger.info("expected_chip_set=%s", expected_final_set)
        flow.logger.info("actual_chip_set=%s", actual_final_set)
        return False
    flow.logger.info("multi_tag_apply_guard_reason=ok")

    page.keyboard.press("Escape")
    flow.logger.info("tag_escape_sent=true")
    page.wait_for_timeout(220)

    url_before = str(page.url)
    clicked = flow._click_apply_in_panel(page, url_before=url_before, report_id=report_id)
    if not clicked:
        panel = flow._find_filter_panel_container(page)
        flow._scroll_filter_panel_to_bottom(panel)
        clicked = flow._click_apply_in_panel(page, url_before=url_before, report_id=report_id)
    flow.logger.info("tag_apply_clicked=%s", str(bool(clicked)).lower())
    if not clicked:
        flow.logger.error("final_fail_reason=apply_not_clicked")
        return False

    page.wait_for_timeout(600)
    panel = flow._find_filter_panel_container(page)
    confirmed = flow._is_filter_apply_confirmed_by_url(page, url_before)
    if not confirmed:
        confirmed = flow._is_filter_apply_confirmed(page, panel, url_before, tag_values[0])
    if not confirmed:
        flow.logger.error("final_fail_reason=apply_not_confirmed")
        return False

    flow._apply_already_confirmed = True
    return True


class TagFilterHandler:
    name = "tag"

    def resolve(self, flow: Any, page: Page, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        holder, holder_id = flow._find_strict_tag_holder(panel)
        return {
            "panel": panel,
            "holder": holder,
            "holder_id": holder_id,
            "values": [str(v).strip() for v in values if str(v).strip()],
            "operator": operator,
        }

    def apply(self, flow: Any, page: Page, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        # We select each provided tag value as a chip in the same widget.
        # OR/AND semantics are not explicitly controlled here and follow amoCRM filter behavior.
        return apply_tag_values_via_holder_popup(flow, page=page, panel=panel, report_id=report_id, values=values)

    def verify(self, flow: Any, page: Page, report_id: str, values: list[str], operator: str = "=") -> bool:
        current_url = ""
        try:
            current_url = str(page.url or "")
        except Exception:
            current_url = ""
        lower_url = current_url.lower()
        url_has_apply = "usefilter=y" in lower_url
        url_has_tag = "tag%5b%5d=" in lower_url or "tag[]=" in lower_url

        if getattr(flow, "_apply_already_confirmed", False) and url_has_apply and url_has_tag:
            flow.logger.info("tag_verify_success=url_marker_after_apply")
            return True

        panel = flow._find_filter_panel_container(page)
        holder, _holder_id = flow._find_strict_tag_holder(panel)
        if holder is None:
            if url_has_apply and url_has_tag:
                flow.logger.info("tag_verify_success=url_marker_without_holder")
                return True
            return False

        for value in [str(v).strip() for v in values if str(v).strip()]:
            if not has_selected_tag_chip(flow, panel, value, holder=holder):
                if url_has_apply and url_has_tag:
                    flow.logger.info("tag_verify_success=url_marker_chip_fallback")
                    return True
                return False
        return True

    def debug_dump(self, flow: Any, page: Page, report_id: str, reason: str, extra: dict[str, Any] | None = None):
        from .base import FilterDebugContext

        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"tag_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        ctx.diagnostics = {"reason": reason, **(extra or {})}
        return ctx
