"""Google Sheets writer MVP via browser UI (no Google API)."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from playwright.sync_api import Locator, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.writers.models import CompiledProfileAnalyticsResult, WriterDestinationConfig


CELL_REF_PATTERN = re.compile(r"^(?P<col>[A-Za-z]+)(?P<row>\d+)$")


class GoogleSheetsUIWriter:
    """Write compiled analytics result into test tab through Google Sheets UI."""

    def __init__(self) -> None:
        self.logger = logging.getLogger("project")

    def write_profile_analytics_result(
        self,
        page: Page,
        compiled_result: CompiledProfileAnalyticsResult,
        destination: WriterDestinationConfig,
        debug_navigation_only: bool = False,
    ) -> None:
        if not destination.sheet_url.strip():
            raise RuntimeError("Google Sheets destination URL is empty. Set sheet_url in table_mappings.yaml.")

        self.logger.info("opening google sheet: %s", destination.sheet_url)
        page.goto(destination.sheet_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("load", timeout=12000)
        except PlaywrightTimeoutError:
            self.logger.info("google sheet load state not fully idle, continue in mvp mode")

        self._select_target_tab(page, destination.tab_name)
        self.logger.info("target tab found: %s", destination.tab_name)

        if debug_navigation_only:
            self.run_cell_navigation_debug_flow(page=page)
            self.logger.info("writer debug navigation flow finished successfully (no data write)")
            return

        self._clear_current_tab(page)
        self.logger.info("target tab cleared: %s", destination.tab_name)

        rows = self._build_rows(compiled_result)
        row_count = len(rows)
        col_count = max((len(row) for row in rows), default=0)
        self.logger.info("writer matrix prepared: rows=%s cols=%s", row_count, col_count)
        self.logger.info("writer matrix preview(first 3 rows): %s", rows[:3])

        start_cell = (destination.start_cell or "A1").strip().upper() or "A1"
        if start_cell != "A1":
            self.logger.warning("writer mvp currently enforces A1 start. requested_start_cell=%s", start_cell)

        self._write_rows_tsv(page=page, rows=rows, start_cell="A1")

        paste_screenshot = self._save_debug_screenshot(page, prefix="gsheets_after_tsv_paste")
        self.logger.info("writer post-paste screenshot: %s", paste_screenshot)

        self._post_validate_written_content(
            page=page,
            compiled_result=compiled_result,
            rows=rows,
            start_cell="A1",
        )
        self.logger.info("writer post-check passed")

        top_cards_count = sum(len(v) for v in compiled_result.top_cards_by_tab.values())
        stages_count = sum(len(v) for v in compiled_result.stages_by_tab.values())
        self.logger.info("rows written for top_cards: %s", top_cards_count)
        self.logger.info("rows written for stages: %s", stages_count)
        self.logger.info("writer finished successfully")

    def run_cell_navigation_debug_flow(self, page: Page) -> None:
        self.logger.info("writer debug flow start: A1 -> B1 -> A3")
        self.goto_cell(page=page, cell_ref="A1")
        self.assert_active_cell(page=page, cell_ref="A1", retries=4)

        self.goto_cell(page=page, cell_ref="B1")
        self.assert_active_cell(page=page, cell_ref="B1", retries=4)

        self.goto_cell(page=page, cell_ref="A3")
        self.assert_active_cell(page=page, cell_ref="A3", retries=4)

    def focus_sheet_grid(self, page: Page) -> None:
        page.keyboard.press("Escape")
        page.wait_for_timeout(80)
        page.keyboard.press("Escape")
        page.wait_for_timeout(80)

        selectors = [
            "div[role='grid']",
            ".grid-container",
            ".docs-sheet-grid",
            ".grid4-inner-container",
            ".waffle-grid-container",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            try:
                if locator.count() <= 0:
                    continue
                locator.first.click(timeout=1500)
                page.wait_for_timeout(150)
                if self._is_focus_on_sheet(page):
                    return
            except Exception:
                continue

        page.locator("body").click(timeout=1500)
        page.wait_for_timeout(120)

    def goto_cell(self, page: Page, cell_ref: str) -> None:
        """Navigate to target cell with robust name-box + keyboard fallback."""
        target = cell_ref.strip().upper()
        self.logger.info("goto_cell requested target=%s", target)

        page.keyboard.press("Escape")
        page.wait_for_timeout(80)
        page.keyboard.press("Escape")
        page.wait_for_timeout(80)

        self.focus_sheet_grid(page)
        self._wait_for_sheet_ui_ready(page)

        state = self._get_name_box_state(page)
        self._log_name_box_state(state, stage="goto_cell_initial")

        if bool(state.get("dom_found")):
            locator = state.get("locator")
            if locator is not None:
                try:
                    locator.scroll_into_view_if_needed(timeout=1200)
                except Exception:
                    pass
                try:
                    locator.focus()
                except Exception:
                    pass
                try:
                    locator.click(timeout=1200)
                except Exception:
                    pass

                try:
                    locator.fill("")
                except Exception:
                    page.keyboard.press("Control+A")
                    page.keyboard.press("Backspace")

                try:
                    locator.type(target)
                except Exception:
                    page.keyboard.type(target, delay=20)
                page.keyboard.press("Enter")
                page.wait_for_timeout(250)

                after_value = self._read_name_box_value_precise(page)
                exact_match = after_value == target
                range_match = self._cell_in_ref(target, after_value)
                returned_is_range = ":" in (after_value or "")
                success = exact_match or range_match
                self.logger.info(
                    "goto_cell result strategy=name_box target_cell=%s returned_ref=%s returned_is_range=%s exact_match=%s range_match=%s success=%s",
                    target,
                    after_value or "<empty>",
                    str(returned_is_range).lower(),
                    str(exact_match).lower(),
                    str(range_match).lower(),
                    str(success).lower(),
                )
                if success:
                    return

        self.logger.info("goto_cell switching to keyboard fallback")
        self._goto_cell_via_keyboard(page, target)
        after_keyboard = self.read_active_cell(page)
        exact_match = after_keyboard == target
        range_match = self._cell_in_ref(target, after_keyboard)
        returned_is_range = ":" in (after_keyboard or "")
        success = exact_match or range_match
        self.logger.info(
            "goto_cell result strategy=keyboard target_cell=%s returned_ref=%s returned_is_range=%s exact_match=%s range_match=%s success=%s",
            target,
            after_keyboard or "<empty>",
            str(returned_is_range).lower(),
            str(exact_match).lower(),
            str(range_match).lower(),
            str(success).lower(),
        )

        if not success:
            self._log_sheet_runtime_state(page, stage=f"goto_cell_failed_{target}")
            screenshot_path = self._save_debug_screenshot(page, prefix=f"gsheets_goto_cell_failed_{target}")
            raise RuntimeError(
                "Google Sheets UI loaded but name box unavailable or navigation failed. "
                f"target={target} screenshot={screenshot_path}"
            )
    def read_active_cell(self, page: Page) -> str:
        """Read active cell; primary source is exact name-box input value."""
        value_name_box = self._read_name_box_value_precise(page)
        self.logger.info("active cell read strategy=name_box_exact value=%s", value_name_box or "<empty>")
        if value_name_box:
            return value_name_box

        value_dom = self._read_active_cell_from_dom_selected(page)
        self.logger.info("active cell read strategy=dom_selected value=%s", value_dom or "<empty>")
        if value_dom:
            return value_dom

        focused = self._describe_focused_element(page)
        self.logger.info("active cell read strategy=focused_element value=<empty> focused=%s", focused)
        return ""

    def assert_active_cell(self, page: Page, cell_ref: str, retries: int = 3) -> None:
        target = cell_ref.strip().upper()
        for attempt in range(1, retries + 1):
            actual = self.read_active_cell(page)
            exact_match = actual == target
            range_match = self._cell_in_ref(target, actual)
            returned_is_range = ":" in (actual or "")
            if exact_match or range_match:
                self.logger.info(
                    "active cell confirmed: target_cell=%s returned_ref=%s returned_is_range=%s exact_match=%s range_match=%s attempt=%s/%s",
                    target,
                    actual or "<empty>",
                    str(returned_is_range).lower(),
                    str(exact_match).lower(),
                    str(range_match).lower(),
                    attempt,
                    retries,
                )
                return

            focus_payload = self._describe_focused_element(page)
            self.logger.info(
                "active cell not confirmed yet: target_cell=%s returned_ref=%s returned_is_range=%s exact_match=%s range_match=%s attempt=%s/%s focused=%s",
                target,
                actual or "<empty>",
                str(returned_is_range).lower(),
                str(exact_match).lower(),
                str(range_match).lower(),
                attempt,
                retries,
                focus_payload,
            )
            if attempt < retries:
                self.goto_cell(page=page, cell_ref=target)

        screenshot_path = self._save_debug_screenshot(page, prefix=f"gsheets_active_cell_assert_failed_{target}")
        raise RuntimeError(
            f"Could not confirm active cell {target} in Google Sheets. screenshot={screenshot_path}"
        )

    def _find_name_box_locator(self, page: Page) -> tuple[Locator | None, str]:
        candidates = [
            "input#t-name-box",
            "input.waffle-name-box",
            "input[aria-label*='name box' i]",
            "input[aria-label*='name' i]",
            "[aria-label*='box' i] input",
            "[data-tooltip*='Ctrl + J' i] input",
        ]
        for selector in candidates:
            locator = page.locator(selector)
            try:
                if locator.count() > 0:
                    return locator.first, selector
            except Exception:
                continue
        return None, ""
    def _focus_name_box(self, page: Page) -> tuple[Locator | None, str]:
        for attempt in range(1, 4):
            locator, selector = self._find_name_box_locator(page)
            if locator is not None:
                try:
                    locator.scroll_into_view_if_needed(timeout=1200)
                except Exception:
                    pass
                try:
                    locator.focus()
                except Exception:
                    pass
                try:
                    locator.click(timeout=1200)
                except Exception:
                    pass
                return locator, selector

            page.keyboard.press("Control+J")
            page.wait_for_timeout(220)

        self.logger.info("name box locator found=false")
        self._log_sheet_runtime_state(page, stage="name_box_not_found")
        dump_json = self._dump_top_input_candidates(page, prefix="gsheets_name_box_not_found")
        area_dump = self._dump_name_box_area_json(page, prefix="gsheets_name_box_area")
        screenshot = self._save_debug_screenshot(page, prefix="gsheets_name_box_not_found")
        self.logger.warning(
            "name box unavailable; inputs_dump=%s area_dump=%s screenshot=%s",
            dump_json,
            area_dump,
            screenshot,
        )
        return None, ""
    def _get_name_box_state(self, page: Page) -> dict[str, Any]:
        locator, selector = self._find_name_box_locator(page)
        state: dict[str, Any] = {
            "dom_found": locator is not None,
            "visible": False,
            "editable": False,
            "enabled": False,
            "focusable": False,
            "value_readable": False,
            "value": "",
            "selector": selector,
            "bbox": {},
            "active_element": self._describe_focused_element(page),
            "locator": locator,
            "grid_mode": self._is_focus_on_sheet(page),
        }
        if locator is None:
            return state

        try:
            state["visible"] = bool(locator.is_visible(timeout=250))
        except Exception:
            state["visible"] = False
        try:
            data = locator.evaluate(
                """el => ({
                    disabled: !!el.disabled,
                    readOnly: !!el.readOnly,
                    tabIndex: Number.isFinite(el.tabIndex) ? el.tabIndex : -1,
                    value: (el.value || '').toString(),
                })"""
            )
            state["enabled"] = not bool(data.get("disabled", False))
            state["editable"] = not bool(data.get("readOnly", False))
            state["focusable"] = int(data.get("tabIndex", -1)) >= -1
            value = str(data.get("value", "")).strip().upper()
            state["value"] = value
            state["value_readable"] = bool(value or value == "")
        except Exception:
            pass
        try:
            state["bbox"] = locator.bounding_box() or {}
        except Exception:
            state["bbox"] = {}
        return state

    def _log_name_box_state(self, state: dict[str, Any], stage: str) -> None:
        self.logger.info(
            "name_box_state stage=%s dom_found=%s visible=%s editable=%s enabled=%s focusable=%s value_readable=%s selector=%s bbox=%s value=%s active_element=%s grid_mode=%s",
            stage,
            str(bool(state.get("dom_found"))).lower(),
            str(bool(state.get("visible"))).lower(),
            str(bool(state.get("editable"))).lower(),
            str(bool(state.get("enabled"))).lower(),
            str(bool(state.get("focusable"))).lower(),
            str(bool(state.get("value_readable"))).lower(),
            state.get("selector") or "<none>",
            state.get("bbox") or {},
            state.get("value") or "<empty>",
            state.get("active_element") or "{}",
            str(bool(state.get("grid_mode"))).lower(),
        )

    def _safe_input_value(self, locator: Locator) -> str:
        try:
            value = (locator.input_value(timeout=250) or "").strip().upper()
            if value:
                return value
        except Exception:
            pass
        try:
            value = locator.evaluate("el => (el.value || '').toString().trim().toUpperCase()")
            if isinstance(value, str):
                return value
        except Exception:
            pass
        return ""

    def _read_name_box_value_precise(self, page: Page) -> str:
        locator, _ = self._find_name_box_locator(page)
        if locator is None:
            return ""
        return self._safe_input_value(locator)

    def _select_target_tab(self, page: Page, tab_name: str) -> None:
        candidates = [
            page.locator(f".docs-sheet-tab-name:has-text('{tab_name}')"),
            page.locator(f"[role='tab']:has-text('{tab_name}')"),
            page.locator(f"[role='button']:has-text('{tab_name}')"),
            page.get_by_text(tab_name, exact=True),
        ]

        for locator in candidates:
            try:
                count = min(locator.count(), 10)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=500):
                        continue
                    item.click(timeout=2000)
                    page.wait_for_timeout(250)
                    return
                except Exception:
                    continue

        raise RuntimeError(
            f"Google Sheets tab not found: '{tab_name}'. Open the sheet and ensure tab exists."
        )

    def _is_focus_on_sheet(self, page: Page) -> bool:
        payload = page.evaluate(
            """() => {
                const ae = document.activeElement;
                if (!ae) return { ok: false };
                const inGrid = !!ae.closest("[role='grid'], .docs-sheet-grid, .grid-container, .waffle-grid-container");
                const formulaBar = !!ae.closest(".docs-formula-bar");
                const toolbar = !!ae.closest(".docs-titlebar-buttons, .docs-toolbar-wrapper");
                const tabStrip = !!ae.closest(".docs-sheet-tab-strip");
                return { ok: inGrid || (!formulaBar && !toolbar && !tabStrip) };
            }"""
        )
        return bool(payload.get("ok", False))

    def _read_active_cell_from_dom_selected(self, page: Page) -> str:
        payload = page.evaluate(
            """() => {
                const selected = document.querySelector("[role='gridcell'][aria-selected='true']");
                if (!selected) return { row: null, col: null };
                const rowRaw = selected.getAttribute('data-row') || selected.getAttribute('row') || '';
                const colRaw = selected.getAttribute('data-col') || selected.getAttribute('col') || '';
                const row = Number.parseInt(rowRaw, 10);
                const col = Number.parseInt(colRaw, 10);
                if (Number.isNaN(row) || Number.isNaN(col)) return { row: null, col: null };
                return { row: row + 1, col: col + 1 };
            }"""
        )
        row = payload.get("row")
        col = payload.get("col")
        if not isinstance(row, int) or not isinstance(col, int):
            return ""
        return f"{self._to_col_label(col)}{row}"

    def _describe_focused_element(self, page: Page) -> str:
        payload = page.evaluate(
            """() => {
                const ae = document.activeElement;
                if (!ae) return {};
                return {
                    tagName: ae.tagName ? ae.tagName.toLowerCase() : '',
                    role: ae.getAttribute('role') || '',
                    ariaLabel: ae.getAttribute('aria-label') || '',
                    className: (ae.className || '').toString(),
                    id: ae.id || '',
                };
            }"""
        )
        return json.dumps(payload, ensure_ascii=False)

    def _dump_top_input_candidates(self, page: Page, prefix: str) -> Path:
        debug_dir = Path.cwd() / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        payload: list[dict[str, Any]] = []

        candidates = page.locator("input, textarea, [role='searchbox'], [role='combobox'], [contenteditable='true']")
        try:
            count = min(candidates.count(), 80)
        except Exception:
            count = 0

        for idx in range(count):
            item = candidates.nth(idx)
            try:
                box = item.bounding_box() or {}
                data = item.evaluate(
                    """el => ({
                        tagName: el.tagName ? el.tagName.toLowerCase() : "",
                        role: el.getAttribute("role") || "",
                        ariaLabel: el.getAttribute("aria-label") || "",
                        placeholder: el.getAttribute("placeholder") || "",
                        className: (el.className || "").toString(),
                    })"""
                )
                payload.append(
                    {
                        "index": idx,
                        "tagName": data.get("tagName", ""),
                        "role": data.get("role", ""),
                        "ariaLabel": data.get("ariaLabel", ""),
                        "placeholder": data.get("placeholder", ""),
                        "className": data.get("className", ""),
                        "bbox": box,
                    }
                )
            except Exception:
                continue

        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.info("writer debug input candidates dump saved: %s", output)
        return output

    def _dump_name_box_area_json(self, page: Page, prefix: str) -> Path:
        debug_dir = Path.cwd() / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        payload: list[dict[str, Any]] = []
        selectors = [
            "#t-name-box",
            ".waffle-name-box",
            ".docs-formula-bar",
            "[data-tooltip*='Ctrl + J' i]",
            "[aria-label*='box' i]",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 5)
            except Exception:
                count = 0
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    box = item.bounding_box() or {}
                    data = item.evaluate(
                        """el => ({
                            tagName: (el.tagName || '').toLowerCase(),
                            role: el.getAttribute('role') || '',
                            ariaLabel: el.getAttribute('aria-label') || '',
                            id: el.id || '',
                            className: (el.className || '').toString(),
                            placeholder: el.getAttribute('placeholder') || '',
                            value: (el.value || '').toString ? (el.value || '').toString() : '',
                            visible: !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length),
                        })"""
                    )
                    payload.append({"selector": selector, "index": idx, "bbox": box, **data})
                except Exception:
                    continue
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.info("writer debug name-box area json dump saved: %s", output)
        return output

    def _wait_for_sheet_ui_ready(self, page: Page, retries: int = 5) -> None:
        for _ in range(retries):
            ready = (page.evaluate("() => document.readyState") or "").strip().lower()
            grid_found = self._is_grid_present(page)
            if ready in {"interactive", "complete"} and grid_found:
                return
            page.wait_for_timeout(250)

    def _is_grid_present(self, page: Page) -> bool:
        selectors = [
            "div[role='grid']",
            ".docs-sheet-grid",
            ".grid-container",
            ".waffle-grid-container",
            "[role='gridcell']",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            try:
                if locator.count() > 0:
                    return True
            except Exception:
                continue
        return False

    def _is_formula_bar_present(self, page: Page) -> bool:
        selectors = [
            ".docs-formula-bar",
            "textarea.docs-formula-bar-input",
            "[aria-label*='formula' i]",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            try:
                if locator.count() > 0:
                    return True
            except Exception:
                continue
        return False

    def _log_sheet_runtime_state(self, page: Page, stage: str) -> None:
        try:
            ready_state = page.evaluate("() => document.readyState")
        except Exception:
            ready_state = "<unknown>"
        try:
            title = page.title()
        except Exception:
            title = "<unknown>"
        url = str(page.url)
        grid_found = self._is_grid_present(page)
        formula_found = self._is_formula_bar_present(page)
        primary_found = False
        secondary_found = False
        try:
            primary_found = page.locator("input#t-name-box").count() > 0
        except Exception:
            primary_found = False
        try:
            secondary_found = page.locator("input.waffle-name-box").count() > 0
        except Exception:
            secondary_found = False
        _name_locator, name_selector = self._find_name_box_locator(page)
        self.logger.info(
            "gsheets runtime state stage=%s url=%s readyState=%s title=%s grid_found=%s formula_bar_found=%s name_box_found=%s name_box_primary_found=%s name_box_secondary_found=%s name_box_selector=%s",
            stage,
            url,
            ready_state,
            title,
            str(grid_found).lower(),
            str(formula_found).lower(),
            str(bool(name_selector)).lower(),
            str(primary_found).lower(),
            str(secondary_found).lower(),
            name_selector or "<none>",
        )

    def _goto_cell_via_keyboard(self, page: Page, cell_ref: str) -> None:
        match = CELL_REF_PATTERN.match(cell_ref.strip().upper())
        if not match:
            raise RuntimeError(f"Invalid cell reference for keyboard navigation: {cell_ref}")
        col_label = match.group("col").upper()
        row = int(match.group("row"))
        col = self._col_label_to_index(col_label)

        self.focus_sheet_grid(page)
        page.keyboard.press("Control+Home")
        page.wait_for_timeout(120)
        for _ in range(max(0, col - 1)):
            page.keyboard.press("ArrowRight")
        for _ in range(max(0, row - 1)):
            page.keyboard.press("ArrowDown")
        page.wait_for_timeout(180)

    def _col_label_to_index(self, label: str) -> int:
        value = 0
        for ch in label:
            if not ("A" <= ch <= "Z"):
                raise RuntimeError(f"Invalid column label: {label}")
            value = value * 26 + (ord(ch) - ord("A") + 1)
        return max(1, value)

    def _normalize_cell_or_range_ref(self, ref: str) -> str:
        raw = (ref or "").strip().upper()
        if not raw:
            return ""
        if "!" in raw:
            raw = raw.split("!")[-1].strip()
        raw = raw.replace("$", "")
        return raw

    def _parse_cell_ref(self, ref: str) -> tuple[int, int] | None:
        raw = self._normalize_cell_or_range_ref(ref)
        match = CELL_REF_PATTERN.match(raw)
        if not match:
            return None
        col = self._col_label_to_index(match.group("col").upper())
        row = int(match.group("row"))
        return row, col

    def _parse_range_ref(self, ref: str) -> tuple[int, int, int, int] | None:
        raw = self._normalize_cell_or_range_ref(ref)
        if not raw:
            return None
        if ":" not in raw:
            cell = self._parse_cell_ref(raw)
            if cell is None:
                return None
            row, col = cell
            return row, col, row, col

        left, right = raw.split(":", 1)
        start = self._parse_cell_ref(left)
        end = self._parse_cell_ref(right)
        if start is None or end is None:
            return None
        sr, sc = start
        er, ec = end
        return min(sr, er), min(sc, ec), max(sr, er), max(sc, ec)

    def _cell_in_ref(self, target_cell: str, returned_ref: str) -> bool:
        target = self._parse_cell_ref(target_cell)
        bounds = self._parse_range_ref(returned_ref)
        if target is None or bounds is None:
            return False
        tr, tc = target
        sr, sc, er, ec = bounds
        return sr <= tr <= er and sc <= tc <= ec

    def _save_debug_screenshot(self, page: Page, prefix: str) -> Path | None:
        debug_dir = Path.cwd() / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        try:
            page.screenshot(path=str(output), full_page=True, timeout=4000)
            self.logger.info("writer debug screenshot saved: %s", output)
            return output
        except Exception as exc:
            self.logger.warning("writer debug screenshot failed (best-effort): prefix=%s error=%s", prefix, exc)
            return None
    def _clear_current_tab(self, page: Page) -> None:
        self.focus_sheet_grid(page)
        page.keyboard.press("Control+A")
        page.keyboard.press("Control+A")
        page.keyboard.press("Delete")
        page.wait_for_timeout(350)
        self.goto_cell(page=page, cell_ref="A1")
        self.assert_active_cell(page=page, cell_ref="A1", retries=3)

    def _to_col_label(self, col_idx: int) -> str:
        value = max(1, col_idx)
        chars: list[str] = []
        while value > 0:
            value, remainder = divmod(value - 1, 26)
            chars.append(chr(ord("A") + remainder))
        return "".join(reversed(chars))

    def _to_tsv(self, rows: Sequence[Sequence[str]]) -> str:
        lines: list[str] = []
        for row in rows:
            lines.append("\t".join(str(cell) for cell in row))
        return "\n".join(lines)

    def _set_clipboard_text(self, page: Page, text: str) -> None:
        try:
            page.evaluate(
                """async (payload) => {
                    if (!navigator.clipboard || !navigator.clipboard.writeText) {
                        throw new Error('Clipboard API is unavailable in this context.');
                    }
                    await navigator.clipboard.writeText(payload);
                }""",
                text,
            )
        except Exception as exc:
            raise RuntimeError(
                "Could not write TSV payload into clipboard. Ensure clipboard access is allowed for docs.google.com."
            ) from exc

    def _write_rows_tsv(self, page: Page, rows: Sequence[Sequence[str]], start_cell: str) -> None:
        if start_cell.strip().upper() != "A1":
            raise RuntimeError("Writer MVP supports deterministic write only from A1.")

        self.goto_cell(page=page, cell_ref="A1")
        self.assert_active_cell(page=page, cell_ref="A1", retries=3)
        tsv = self._to_tsv(rows)
        self._set_clipboard_text(page, tsv)
        page.keyboard.press("Control+V")
        page.wait_for_timeout(800)

    def _read_formula_bar_value(self, page: Page) -> str:
        selectors = [
            "textarea.docs-formula-bar-input",
            "textarea[aria-label*='formula' i]",
            "textarea[aria-label*='формул' i]",
            "div[role='textbox'][aria-label*='formula' i]",
            "div[role='textbox'][aria-label*='формул' i]",
        ]
        for selector in selectors:
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 4)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=250):
                        continue
                    tag_name = item.evaluate("el => el.tagName.toLowerCase()")
                    if tag_name == "textarea":
                        value = (item.input_value(timeout=250) or "").strip()
                    else:
                        value = (item.inner_text(timeout=250) or "").strip()
                    if value:
                        return value
                except Exception:
                    continue
        return ""

    def _read_cell_value(self, page: Page, cell_ref: str) -> str:
        self.goto_cell(page=page, cell_ref=cell_ref)
        self.assert_active_cell(page=page, cell_ref=cell_ref, retries=3)
        page.wait_for_timeout(120)
        return self._read_formula_bar_value(page)

    def _post_validate_written_content(
        self,
        page: Page,
        compiled_result: CompiledProfileAnalyticsResult,
        rows: Sequence[Sequence[str]],
        start_cell: str,
    ) -> None:
        if start_cell.strip().upper() != "A1":
            raise RuntimeError("Writer MVP post-check currently supports only A1 mode.")

        control_checks: list[tuple[str, str]] = [
            ("A1", "report_id"),
            ("B1", compiled_result.report_id),
            ("A2", "display_name"),
            ("B2", compiled_result.display_name),
        ]

        failures: list[str] = []
        for cell_ref, expected in control_checks:
            actual = self._read_cell_value(page=page, cell_ref=cell_ref)
            if actual != expected:
                failures.append(f"{cell_ref}: expected='{expected}' actual='{actual}'")

        expected_last_nonempty_row = 1
        for idx, row in enumerate(rows, start=1):
            if any(str(cell).strip() for cell in row):
                expected_last_nonempty_row = idx

        last_col_value = self._read_cell_value(page=page, cell_ref=f"A{expected_last_nonempty_row}")
        if not last_col_value.strip():
            failures.append(
                f"A{expected_last_nonempty_row}: expected non-empty marker, got empty value"
            )

        self.logger.info(
            "writer post-check summary: control_cells=%s expected_nonempty_rows=%s",
            len(control_checks),
            expected_last_nonempty_row,
        )

        if failures:
            raise RuntimeError(
                "Google Sheets writer post-check failed. "
                f"Validation mismatches: {json.dumps(failures, ensure_ascii=False)}"
            )

    def _build_rows(self, compiled: CompiledProfileAnalyticsResult) -> list[list[str]]:
        rows: list[list[str]] = [
            ["report_id", compiled.report_id],
            ["display_name", compiled.display_name],
            ["generated_at", compiled.generated_at.isoformat()],
            ["source_kind", compiled.source_kind],
            ["filter_values", ", ".join(compiled.filter_values)],
            [],
            ["tab", "card_index", "label", "value", "raw_value"],
        ]

        for tab in compiled.tabs:
            for card in compiled.top_cards_by_tab.get(tab, []):
                rows.append(
                    [
                        str(card.get("tab", tab)),
                        str(card.get("card_index", "")),
                        str(card.get("label", "")),
                        str(card.get("value", "")),
                        str(card.get("raw_value", "")),
                    ]
                )

        rows.append([])
        rows.append(["tab", "stage_index", "stage_name", "deals_count", "budget_text", "raw_line"])

        for tab in compiled.tabs:
            for stage in compiled.stages_by_tab.get(tab, []):
                rows.append(
                    [
                        str(stage.get("tab", tab)),
                        str(stage.get("stage_index", "")),
                        str(stage.get("stage_name", "")),
                        str(stage.get("deals_count", "")),
                        str(stage.get("budget_text", "")),
                        str(stage.get("raw_line", "")),
                    ]
                )

        return rows
