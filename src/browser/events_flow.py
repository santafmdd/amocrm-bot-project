"""Browser flow for amoCRM events list weekly refusals capture (MVP)."""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from playwright.sync_api import Page

from src.browser.session import BrowserSettings
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class EventsFlowInput:
    report_id: str
    pipeline_name: str
    date_mode: str
    period_mode: str
    date_from: str
    date_to: str
    status_before: str
    status_before_values: list[str]
    status_after: str
    entity_kind: str
    event_type: str
    period_strategy: str
    managers: list[str]
    filter_mode: str = "ui_controls"
    saved_preset_name: str = ""
    saved_preset_exact_match: bool = False


class EventsFlow:
    PANEL_MARKERS: tuple[str, ...] = (
        "\u041c\u0435\u043d\u0435\u0434\u0436\u0435\u0440\u044b",
        "\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438",
        "\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439",
        "\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e",
        "\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u043e\u0441\u043b\u0435",
        "\u0417\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f",
    )

    FILTER_OPEN_SELECTORS: tuple[str, ...] = (
        "text=\u0424\u0438\u043b\u044c\u0442\u0440",
        ':text("\u0424\u0438\u043b\u044c\u0442\u0440")',
        "[placeholder='\u0424\u0438\u043b\u044c\u0442\u0440']",
        "input[placeholder='\u0424\u0438\u043b\u044c\u0442\u0440']",
        "[aria-label*='\u0424\u0438\u043b\u044c\u0442\u0440']",
        "[title*='\u0424\u0438\u043b\u044c\u0442\u0440']",
        "button:has-text('\u0424\u0438\u043b\u044c\u0442\u0440')",
        "button:has-text('\u0424\u0438\u043b\u044c\u0442\u0440\u044b')",
        "[role='button']:has-text('\u0424\u0438\u043b\u044c\u0442\u0440')",
        "[role='button']:has-text('\u0424\u0438\u043b\u044c\u0442\u0440\u044b')",
        "[data-test*='filter']",
        "[data-testid*='filter']",
        "header [role='button']",
        "header button",
        "header a[role='button']",
        "header div[role='button']",
        "header span[role='button']",
        "[class*='toolbar'] [role='button']",
        "[class*='toolbar'] button",
        "[class*='filter'] button",
    )

    def __init__(self, settings: BrowserSettings, project_root: Path) -> None:
        self.settings = settings
        self.project_root = project_root
        self.logger = logging.getLogger("project")

    def run_capture(self, page: Page, flow_input: EventsFlowInput) -> list[dict[str, Any]]:
        self._open_events_page(page)
        panel = self._open_filter_panel(page)
        self._apply_mvp_filters(page, panel=panel, flow_input=flow_input)
        self._click_apply(page, panel=panel)
        return self._read_events_rows(page)

    def _norm(self, value: str) -> str:
        return " ".join(str(value or "").strip().lower().replace("\u0451", "\u0435").split())

    def _resolve_events_url(self) -> str:
        base = str(self.settings.base_url or "").strip().rstrip("/")
        if not base:
            raise RuntimeError("AMO_BASE_URL is empty; cannot open events list page.")
        return f"{base}/events/list/"

    def _open_events_page(self, page: Page) -> None:
        page.goto(self._resolve_events_url(), wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("domcontentloaded", timeout=6000)
        except Exception:
            pass
        page.wait_for_timeout(900)

    def _collect_panel_marker_info(self, page: Page) -> dict[str, Any]:
        visible_markers: list[str] = []
        for marker in self.PANEL_MARKERS:
            loc = page.locator(f"text={marker}")
            try:
                count = min(int(loc.count()), 8)
            except Exception:
                count = 0
            for idx in range(count):
                try:
                    if loc.nth(idx).is_visible(timeout=120):
                        visible_markers.append(marker)
                        break
                except Exception:
                    continue
        return {"visible_marker_count": len(visible_markers), "visible_markers": visible_markers}

    def _find_filter_panel(self, page: Page):
        marker_info = self._collect_panel_marker_info(page)
        if marker_info["visible_marker_count"] < 3:
            return None
        for selector in (
            ".filter-search__custom_settings",
            ".filter-search",
            "[class*='filter-search']",
            ".modal-body [class*='filter']",
            "aside [class*='filter']",
        ):
            loc = page.locator(selector)
            try:
                if loc.count() > 0 and loc.first.is_visible(timeout=160):
                    return loc.first
            except Exception:
                continue
        return page

    def _build_candidate_payload(self, locator: Any, *, selector: str, idx: int, matched_count: int) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "selector": selector,
            "index": idx,
            "matched_count": int(matched_count),
            "visible": False,
            "text": "",
            "text_norm": "",
            "tagName": "",
            "className": "",
            "bbox": None,
            "_locator": locator,
        }
        try:
            payload["visible"] = bool(locator.is_visible(timeout=150))
        except Exception:
            payload["visible"] = False
        try:
            meta = locator.evaluate("""el => ({tagName:(el.tagName||'').toLowerCase(), className:String(el.className||''), text:String((el.innerText||el.textContent||'')).trim()})""")
            if isinstance(meta, dict):
                payload.update(meta)
        except Exception:
            pass
        try:
            payload["bbox"] = locator.bounding_box()
        except Exception:
            pass
        payload["text_norm"] = self._norm(str(payload.get("text", "")))
        return payload

    def _candidate_score(self, candidate: dict[str, Any]) -> int:
        score = 0
        text_norm = str(candidate.get("text_norm", ""))
        selector = str(candidate.get("selector", ""))
        bbox = candidate.get("bbox") if isinstance(candidate.get("bbox"), dict) else None
        y = float((bbox or {}).get("y", 9999))
        if text_norm == "\u0444\u0438\u043b\u044c\u0442\u0440":
            score += 120
        elif "\u0444\u0438\u043b\u044c\u0442\u0440" in text_norm:
            score += 70
        if y <= 140:
            score += 80
        elif y <= 220:
            score += 30
        if selector == "[class*='filter'] button":
            score -= 120
        return score

    def _sort_filter_candidates(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(candidates, key=lambda c: (self._candidate_score(c), -float(((c.get("bbox") or {}).get("y", 9999)) if isinstance(c.get("bbox"), dict) else 9999)), reverse=True)
    def _collect_filter_open_candidates(self, page: Page, selectors: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        candidates: list[dict[str, Any]] = []
        checked: list[dict[str, Any]] = []
        for selector in selectors:
            loc = page.locator(selector)
            try:
                count = int(loc.count())
            except Exception as exc:
                checked.append({"selector": selector, "matched_count": 0, "click_success": False, "error": str(exc)})
                self.logger.info("events filter_open_try selector=%s matched_count=%s", selector, 0)
                continue
            checked.append({"selector": selector, "matched_count": count, "click_success": False})
            self.logger.info("events filter_open_try selector=%s matched_count=%s", selector, count)
            for idx in range(min(max(count, 0), 8)):
                payload = self._build_candidate_payload(loc.nth(idx), selector=selector, idx=idx, matched_count=count)
                if payload.get("visible", False):
                    candidates.append(payload)
        return candidates, checked

    def _click_candidate_direct(self, page: Page, candidate: dict[str, Any]) -> bool:
        locator = candidate.get("_locator")
        if locator is None:
            return False
        try:
            locator.click(timeout=1300)
            return True
        except Exception:
            return False

    def _click_candidate_via_ancestor(self, page: Page, candidate: dict[str, Any]) -> bool:
        locator = candidate.get("_locator")
        if locator is None:
            return False
        try:
            return bool(locator.evaluate("""el => { let cur=el; for(let i=0;i<6&&cur;i++){const r=cur.getBoundingClientRect(); const s=window.getComputedStyle(cur); const role=String(cur.getAttribute('role')||'').toLowerCase(); const ok=['div','span','a','button','label'].includes(String(cur.tagName||'').toLowerCase()) && r.width>2 && r.height>2 && s.display!=='none' && s.visibility!=='hidden' && (s.cursor==='pointer' || typeof cur.onclick==='function' || role==='button' || cur.tagName.toLowerCase()==='button'); if(ok){cur.click(); return true;} cur=cur.parentElement;} return false; }"""))
        except Exception:
            return False

    def _click_candidate_via_bbox(self, page: Page, candidate: dict[str, Any]) -> bool:
        locator = candidate.get("_locator")
        if locator is None:
            return False
        try:
            box = locator.bounding_box()
            if not box:
                return False
            page.mouse.click(float(box.get("x", 0)) + float(box.get("width", 0)) / 2, float(box.get("y", 0)) + float(box.get("height", 0)) / 2)
            return True
        except Exception:
            return False

    def _dump_filter_open_failure_artifacts(self, *, page: Page, checked: list[dict[str, Any]], candidates: list[dict[str, Any]], marker_info: dict[str, Any], explanation: str) -> dict[str, str]:
        debug_dir = ensure_inside_root(self.settings.exports_dir / "debug", self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        png_path = ensure_inside_root(debug_dir / f"weekly_refusals_filter_open_failed_{stamp}.png", self.project_root)
        json_path = ensure_inside_root(debug_dir / f"weekly_refusals_filter_open_failed_{stamp}.json", self.project_root)
        txt_path = ensure_inside_root(debug_dir / f"weekly_refusals_filter_open_failed_{stamp}.txt", self.project_root)
        html_path = ensure_inside_root(debug_dir / f"weekly_refusals_filter_open_failed_{stamp}.html", self.project_root)
        try:
            page.screenshot(path=str(png_path), full_page=True)
        except Exception as exc:
            self.logger.warning("weekly filter-open screenshot failed: error=%s", str(exc))
        payload = {
            "current_url": str(getattr(page, "url", "") or ""),
            "explanation": explanation,
            "checked_selectors": checked,
            "candidates": [{k: v for k, v in c.items() if k != "_locator"} for c in candidates],
            "marker_info": marker_info,
        }
        try:
            json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            txt_path.write_text(f"explanation={explanation}\nurl={payload['current_url']}", encoding="utf-8")
            html_path.write_text(str(page.evaluate("() => document.body ? document.body.outerHTML.slice(0, 15000) : ''")), encoding="utf-8")
        except Exception:
            pass
        return {"screenshot": str(png_path), "json": str(json_path), "txt": str(txt_path), "html": str(html_path)}

    def _open_filter_panel(self, page: Page):
        try:
            page.wait_for_load_state("domcontentloaded", timeout=4000)
        except Exception:
            pass
        page.wait_for_timeout(250)

        already = self._find_filter_panel(page)
        if already is not None:
            self.logger.info("events filter panel already open")
            return already

        self.logger.info("events filter_open_selector_candidates=%s", list(self.FILTER_OPEN_SELECTORS))
        candidates, checked = self._collect_filter_open_candidates(page, list(self.FILTER_OPEN_SELECTORS))
        sorted_candidates = self._sort_filter_candidates(candidates)

        for candidate in sorted_candidates:
            if self._click_candidate_direct(page, candidate):
                page.wait_for_timeout(380)
                panel = self._find_filter_panel(page)
                if panel is not None:
                    self.logger.info("events filter panel opened: path=direct_selector selector=%s", candidate.get("selector", ""))
                    return panel

        text_candidates = [c for c in sorted_candidates if str(c.get("text_norm", "")) == "\u0444\u0438\u043b\u044c\u0442\u0440"]
        for candidate in text_candidates:
            if self._click_candidate_via_ancestor(page, candidate):
                page.wait_for_timeout(420)
                panel = self._find_filter_panel(page)
                if panel is not None:
                    self.logger.info("events filter panel opened: path=ancestor_click selector=%s", candidate.get("selector", ""))
                    return panel

        for candidate in text_candidates:
            if self._click_candidate_via_bbox(page, candidate):
                page.wait_for_timeout(420)
                panel = self._find_filter_panel(page)
                if panel is not None:
                    self.logger.info("events filter panel opened: path=bbox_click selector=%s", candidate.get("selector", ""))
                    return panel

        marker_info = self._collect_panel_marker_info(page)
        visible_candidates_count = sum(1 for c in sorted_candidates if bool(c.get("visible", False)))
        artifacts = self._dump_filter_open_failure_artifacts(page=page, checked=checked, candidates=sorted_candidates, marker_info=marker_info, explanation="panel_not_opened_after_selector_clicks")
        raise RuntimeError(
            "Events filter panel was not opened. "
            f"checked_selectors_count={len(checked)} visible_candidates_count={visible_candidates_count} "
            f"current_url={getattr(page, 'url', '')} debug_artifacts_path={artifacts}"
        )

    def _dump_stage_failure_artifacts(self, *, page: Page, stage: str, summary: dict[str, Any], candidates: list[dict[str, Any]] | None = None) -> dict[str, str]:
        debug_dir = ensure_inside_root(self.settings.exports_dir / "debug", self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"weekly_refusals_{stage}_failed_{stamp}"
        png_path = ensure_inside_root(debug_dir / f"{base}.png", self.project_root)
        json_path = ensure_inside_root(debug_dir / f"{base}.json", self.project_root)
        txt_path = ensure_inside_root(debug_dir / f"{base}.txt", self.project_root)
        try:
            page.screenshot(path=str(png_path), full_page=True)
        except Exception as exc:
            self.logger.warning("weekly debug screenshot failed: stage=%s error=%s", stage, str(exc))
        payload = {"stage": stage, "url": str(getattr(page, 'url', '') or ''), "summary": summary, "candidates": candidates or []}
        try:
            json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            txt_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
        return {"screenshot": str(png_path), "json": str(json_path), "txt": str(txt_path)}
    def _read_visible_option_texts(self, page: Page) -> list[str]:
        values: list[str] = []
        for selector in (
            "ul.control--select--list li.control--select--list--item",
            "[role='listbox'] [role='option']",
            ".multisuggest__suggest-item",
            ".multisuggest__list-item",
            ".suggest-manager li",
            "li[data-value]",
        ):
            loc = page.locator(selector)
            try:
                count = min(int(loc.count()), 80)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=100):
                        continue
                    txt = (item.inner_text(timeout=120) or "").strip()
                except Exception:
                    continue
                if txt:
                    values.append(txt)
        unique: list[str] = []
        seen: set[str] = set()
        for val in values:
            key = self._norm(val)
            if not key or key in seen:
                continue
            seen.add(key)
            unique.append(val)
        return unique

    def _collect_dropdown_like_elements(self, page: Page) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for selector in (
            "[class*='select']",
            "[class*='dropdown']",
            "[role='listbox']",
            "[role='option']",
            "ul",
            "li",
        ):
            loc = page.locator(selector)
            try:
                count = min(int(loc.count()), 50)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=70):
                        continue
                    data = item.evaluate(
                        """el => {
                            const r = el.getBoundingClientRect();
                            return {
                                tagName: String(el.tagName || '').toLowerCase(),
                                className: String(el.className || ''),
                                text: String((el.innerText || el.textContent || '')).trim(),
                                role: String(el.getAttribute('role') || ''),
                                bbox: {x:r.x,y:r.y,width:r.width,height:r.height}
                            };
                        }"""
                    )
                except Exception:
                    continue
                if isinstance(data, dict) and str(data.get("text", "")).strip():
                    data["selector"] = selector
                    payloads.append(data)
        return payloads[:80]


    def _collect_visible_element_payloads(self, scope: Any, selectors: tuple[str, ...], *, max_items: int = 60) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for selector in selectors:
            loc = scope.locator(selector)
            try:
                count = min(int(loc.count()), max_items)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=80):
                        continue
                    data = item.evaluate(
                        """el => {
                            const r = el.getBoundingClientRect();
                            return {
                                tagName: String(el.tagName || '').toLowerCase(),
                                className: String(el.className || ''),
                                text: String((el.innerText || el.textContent || '')).trim(),
                                role: String(el.getAttribute('role') || ''),
                                bbox: {x:r.x,y:r.y,width:r.width,height:r.height},
                                outerHTML: String(el.outerHTML || '').slice(0, 800),
                            };
                        }"""
                    )
                except Exception:
                    continue
                if isinstance(data, dict):
                    data["selector"] = selector
                    payloads.append(data)
        uniq: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in payloads:
            key = "|".join([
                str(item.get("selector", "")),
                str(item.get("className", "")),
                str(item.get("text", ""))[:160],
                str((item.get("bbox") or {}).get("x", "")),
                str((item.get("bbox") or {}).get("y", "")),
            ])
            if key in seen:
                continue
            seen.add(key)
            uniq.append(item)
            if len(uniq) >= max_items:
                break
        return uniq

    def _collect_checkbox_search_debug_snapshot(self, page: Page, control: Any, *, control_label: str, expected_value: str) -> dict[str, Any]:
        def _outer_html(locator: Any) -> str:
            try:
                return str(locator.evaluate("el => String(el.outerHTML || '')") or "")[:8000]
            except Exception:
                return ""

        active_element: dict[str, Any] = {}
        try:
            data = page.evaluate(
                """() => {
                    const el = document.activeElement;
                    if (!el) return {};
                    const r = el.getBoundingClientRect ? el.getBoundingClientRect() : {x:0,y:0,width:0,height:0};
                    return {
                        tagName: String(el.tagName || '').toLowerCase(),
                        className: String(el.className || ''),
                        id: String(el.id || ''),
                        name: String(el.getAttribute?.('name') || ''),
                        role: String(el.getAttribute?.('role') || ''),
                        type: String(el.getAttribute?.('type') || ''),
                        value: String(el.value || ''),
                        text: String((el.innerText || el.textContent || '')).trim(),
                        bbox: {x:r.x,y:r.y,width:r.width,height:r.height},
                        outerHTML: String(el.outerHTML || '').slice(0, 800),
                    };
                }"""
            )
            if isinstance(data, dict):
                active_element = data
        except Exception:
            active_element = {}

        control_payloads = self._collect_visible_element_payloads(control, selectors=(
            ".checkboxes-search",
            ".js-control-checkboxes-search",
            ".checkboxes_dropdown",
            ".js-control-checkboxes_dropdown",
            ".checkboxes-search__title-wrapper",
            ".checkboxes-search__title",
            ".checkboxes-search__title-item",
            ".checkboxes-search__opening-list",
            ".checkboxes-search__search-input",
            ".checkboxes-search__section-common",
            ".checkboxes-search__item-label",
            ".js-checkboxes-search-list-apply",
        ), max_items=40)

        class_elements = self._collect_visible_element_payloads(page, selectors=(
            "[class*='checkboxes-search']",
            "[class*='checkboxes_search']",
            "[class*='checkboxes_dropdown']",
            "[class*='checkboxes-dropdown']",
        ), max_items=80)

        ok_buttons = self._collect_visible_element_payloads(page, selectors=(
            "button:has-text('OK')",
            "[role='button']:has-text('OK')",
            "button:has-text('ОК')",
            "[role='button']:has-text('ОК')",
        ), max_items=20)

        event_text_elements = self._collect_visible_element_payloads(page, selectors=(
            "*:has-text('Изменение этапа продажи')",
            "*:has-text('Новая сделка')",
            "*:has-text('Сделка удалена')",
        ), max_items=60)

        scope, scope_reason = self._resolve_checkbox_scope(page, control, kind="search")

        return {
            "control_label": control_label,
            "expected_value": expected_value,
            "control_outer_html": _outer_html(control),
            "scope_reason": scope_reason,
            "scope_outer_html": _outer_html(scope) if scope is not None else "",
            "active_element": active_element,
            "control_scope_elements": control_payloads,
            "checkbox_class_elements": class_elements,
            "ok_buttons": ok_buttons,
            "event_type_text_elements": event_text_elements,
        }

    def _collect_control_candidates(self, panel: Any, *, control_label: str) -> list[dict[str, Any]]:
        label_norm = self._norm(control_label)
        selectors = (
            f".control--select:has-text('{control_label}')",
            f".filter__custom_settings__item:has-text('{control_label}')",
            f".filter-search__custom_settings__item:has-text('{control_label}')",
            f".date_filter:has-text('{control_label}')",
            f"[data-title='{control_label}']",
            f"[data-title*='{control_label}']",
            f"label:has-text('{control_label}')",
            f"*:has-text('{control_label}')",
        )
        candidates: list[dict[str, Any]] = []
        for selector in selectors:
            loc = panel.locator(selector)
            try:
                count = min(int(loc.count()), 40)
            except Exception:
                count = 0
            self.logger.info("weekly control candidate scan: label=%s selector=%s matched_count=%s", control_label, selector, count)
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=100):
                        continue
                except Exception:
                    continue
                payload = {
                    "selector": selector,
                    "index": idx,
                    "matched_count": count,
                    "text": "",
                    "className": "",
                    "tagName": "",
                    "bbox": None,
                    "clickable": False,
                    "_locator": item,
                }
                try:
                    meta = item.evaluate(
                        """el => {
                            const rect = el.getBoundingClientRect();
                            const txt = String((el.innerText || el.textContent || '')).trim();
                            const className = String(el.className || '');
                            const tagName = String(el.tagName || '').toLowerCase();
                            const role = String(el.getAttribute('role') || '').toLowerCase();
                            const style = window.getComputedStyle(el);
                            const clickable = role === 'button' || typeof el.onclick === 'function' || ['button','a','label'].includes(tagName) || style.cursor === 'pointer';
                            return {
                                text: txt,
                                className,
                                tagName,
                                clickable,
                                bbox: {x: rect.x, y: rect.y, width: rect.width, height: rect.height}
                            };
                        }"""
                    )
                    if isinstance(meta, dict):
                        payload.update(meta)
                except Exception:
                    pass
                text_norm = self._norm(str(payload.get("text", "")))
                if label_norm not in text_norm:
                    continue
                candidates.append(payload)
        return candidates

    def _promote_to_control_container(self, locator: Any):
        for xpath in (
            "xpath=ancestor-or-self::*[contains(@class, 'control--select')][1]",
            "xpath=ancestor-or-self::*[contains(@class, 'filter__custom_settings__item')][1]",
            "xpath=ancestor-or-self::*[contains(@class, 'filter-search__custom_settings__item')][1]",
            "xpath=ancestor-or-self::*[contains(@class, 'date_filter')][1]",
            "xpath=ancestor-or-self::*[@data-title][1]",
        ):
            try:
                promoted = locator.locator(xpath)
                if promoted.count() > 0 and promoted.first.is_visible(timeout=120):
                    return promoted.first
            except Exception:
                continue
        return locator

    def _resolve_filter_control(self, panel: Any, control_label: str):
        label_norm = self._norm(control_label)
        candidates = self._collect_control_candidates(panel, control_label=control_label)

        def score(item: dict[str, Any]) -> int:
            s = 0
            text_norm = self._norm(str(item.get("text", "")))
            cls = self._norm(str(item.get("className", "")))
            selector = str(item.get("selector", ""))
            bbox = item.get("bbox") if isinstance(item.get("bbox"), dict) else None
            width = float((bbox or {}).get("width", 0) or 0)
            height = float((bbox or {}).get("height", 0) or 0)
            area = width * height
            if text_norm == label_norm:
                s += 280
            elif label_norm in text_norm:
                s += 160
            if bool(item.get("clickable", False)):
                s += 120
            if any(x in cls for x in ("control", "select", "filter", "date_filter")):
                s += 140
            if selector.startswith(".control--select") or selector.startswith(".filter__custom_settings__item") or selector.startswith(".date_filter"):
                s += 90
            if selector.startswith("*:has-text"):
                s -= 220
            if 60 <= width <= 1200 and 18 <= height <= 220:
                s += 50
            if area > 600000:
                s -= 200
            if width < 20 or height < 10:
                s -= 120
            return s

        ranked = sorted(candidates, key=score, reverse=True)
        if not ranked:
            return None, []
        chosen = ranked[0]
        chosen_locator = self._promote_to_control_container(chosen.get("_locator"))
        self.logger.info(
            "weekly control resolved: label=%s chosen_selector=%s matched_count=%s chosen_score=%s",
            control_label,
            str(chosen.get("selector", "")),
            int(chosen.get("matched_count", 0) or 0),
            score(chosen),
        )
        return chosen_locator, [{k: v for k, v in c.items() if k != "_locator"} for c in ranked[:12]]

    def _resolve_filter_control_for_stage(self, panel: Any, *, control_label: str, stage: str):
        stage_key = self._norm(stage)
        exact_selector = ""
        if stage_key == "status_before":
            exact_selector = ".js-control-checkboxes-search[data-name='filter[value_before][status_lead][]']"
        elif stage_key == "status_after":
            exact_selector = ".js-control-checkboxes-search[data-name='filter[value_after][status_lead][]']"

        if exact_selector and hasattr(panel, "locator"):
            exact = panel.locator(exact_selector)
            try:
                count = min(int(exact.count()), 8)
            except Exception:
                count = 0
            for idx in range(count):
                node = exact.nth(idx)
                try:
                    if not node.is_visible(timeout=120):
                        continue
                    promoted = self._promote_to_control_container(node)
                    self.logger.info(
                        "weekly control resolved by exact stage selector: stage=%s label=%s selector=%s",
                        stage,
                        control_label,
                        exact_selector,
                    )
                    payload = [{"selector": exact_selector, "stage": stage, "exact_selector_used": True, "matched_count": count}]
                    return promoted, payload
                except Exception:
                    continue
        return self._resolve_filter_control(panel, control_label)

    def _status_norm_token(self, value: str) -> str:
        raw = str(value or "").lower().replace("ё", "е")
        cleaned = re.sub(r"[^0-9a-zа-я]+", " ", raw)
        return " ".join(cleaned.split())

    def _split_profile_status_value(self, value: str, *, pipeline_hint: str = "") -> tuple[str, str]:
        raw = str(value or "").strip()
        if not raw:
            return "", ""
        pipeline = ""
        status = raw
        if "/" in raw:
            left, right = raw.split("/", 1)
            pipeline, status = left.strip(), right.strip()
        elif ")" in raw:
            idx = raw.find(")")
            if idx >= 0:
                pipeline = raw[: idx + 1].strip()
                status = raw[idx + 1 :].strip()
        if not pipeline and str(pipeline_hint or "").strip():
            pipeline = str(pipeline_hint).strip()
        return self._status_norm_token(pipeline), self._status_norm_token(status)

    def _split_dom_status_value(self, item: dict[str, Any]) -> tuple[str, str]:
        pipeline = str(item.get("pipeline_text", "") or "").strip()
        status = str(item.get("status_text", "") or "").strip()
        if pipeline and status:
            return self._status_norm_token(pipeline), self._status_norm_token(status)

        data_value = str(item.get("data_value", "") or "").strip()
        if "/" in data_value:
            left, right = data_value.split("/", 1)
            return self._status_norm_token(left), self._status_norm_token(right)

        full_text = str(item.get("full_text", "") or "").strip()
        if "/" in full_text:
            left, right = full_text.split("/", 1)
            return self._status_norm_token(left), self._status_norm_token(right)

        return "", self._status_norm_token(data_value or full_text)

    def _find_status_global_popup(self, page: Page, *, stage: str):
        stage_key = self._norm(stage)
        input_name = ""
        if stage_key == "status_before":
            input_name = "filter[value_before][status_lead][]"
        elif stage_key == "status_after":
            input_name = "filter[value_after][status_lead][]"
        popup_selector = ".checkboxes-search__opening-list"
        popup = page.locator(popup_selector)
        candidates = 0
        try:
            total = min(int(popup.count()), 16)
        except Exception:
            total = 0
        for idx in range(total):
            cand = popup.nth(idx)
            try:
                if not cand.is_visible(timeout=80):
                    continue
            except Exception:
                continue
            candidates += 1
            try:
                if input_name and cand.locator(f"input[name='{input_name}']").count() > 0:
                    return cand, {
                        "popup_selector": popup_selector,
                        "popup_candidates_count": candidates,
                        "popup_found": True,
                        "popup_has_status_inputs": True,
                        "popup_input_name": input_name,
                    }
            except Exception:
                continue
        return None, {
            "popup_selector": popup_selector,
            "popup_candidates_count": candidates,
            "popup_found": False,
            "popup_has_status_inputs": False,
            "popup_input_name": input_name,
        }

    def _wait_for_status_global_popup(self, page: Page, *, stage: str, timeout_ms: int = 4500, poll_ms: int = 150):
        steps = max(1, timeout_ms // max(50, poll_ms))
        last_meta: dict[str, Any] = {}
        for _ in range(steps):
            popup, meta = self._find_status_global_popup(page, stage=stage)
            if isinstance(meta, dict):
                last_meta = meta
            if popup is not None and bool(last_meta.get("popup_has_status_inputs", False)):
                return popup, last_meta
            page.wait_for_timeout(poll_ms)
        return None, last_meta

    def _collect_status_popup_items(self, popup: Any, *, stage: str) -> list[dict[str, Any]]:
        labels = popup.locator("label.checkboxes-search__item-label")
        items: list[dict[str, Any]] = []
        try:
            total = min(int(labels.count()), 300)
        except Exception:
            total = 0
        for idx in range(total):
            node = labels.nth(idx)
            try:
                data = node.evaluate(
                    """el => {
                        const input = el.querySelector("input[type='checkbox']");
                        const pipeline = String(el.querySelector('.checkboxes-search__slash-first-name')?.innerText || '').trim();
                        const status = String(el.querySelector('.checkboxes-search__slash-name')?.innerText || '').trim();
                        const full = String((el.innerText || el.textContent || '')).trim();
                        return {
                            pipeline_text: pipeline,
                            status_text: status,
                            full_text: full,
                            data_value: String(input?.getAttribute('data-value') || input?.value || '').trim(),
                            checked: !!(input && input.checked),
                            className: String(el.className || ''),
                            has_input: !!input,
                        };
                    }"""
                )
            except Exception:
                data = None
            if isinstance(data, dict):
                data["index"] = idx
                items.append(data)
        return items

    def _select_status_popup_values(self, page: Page, control: Any, *, stage: str, values: list[str], pipeline_hint: str = "", allow_multi: bool = True) -> tuple[bool, dict[str, Any]]:
        opened, click_mode = self._open_checkbox_like_control(page, control, kind="search")
        if not opened:
            return False, {"reason": "status_popup_open_failed", "click_mode": click_mode}

        popup, popup_meta = self._wait_for_status_global_popup(page, stage=stage, timeout_ms=4500, poll_ms=150)
        if popup is None:
            return False, {
                "reason": "status_popup_not_found",
                "click_mode": click_mode,
                "popup_candidates_count": int(popup_meta.get("popup_candidates_count", 0)),
                "popup_has_status_inputs": bool(popup_meta.get("popup_has_status_inputs", False)),
            }

        clear_button_found = False
        clear_clicked = False
        for selector in (
            ".js-checkboxes-search-clear-all",
            ".checkboxes-search__links-wrapper .js-checkboxes-search-clear-all",
            "button:has-text('Очистить')",
            "[role='button']:has-text('Очистить')",
        ):
            loc = popup.locator(selector)
            try:
                if loc.count() <= 0:
                    continue
            except Exception:
                continue
            clear_button_found = True
            try:
                loc.first.click(timeout=700)
                clear_clicked = True
                break
            except Exception:
                try:
                    loc.first.click(timeout=700, force=True)
                    clear_clicked = True
                    break
                except Exception:
                    continue
        if clear_clicked:
            page.wait_for_timeout(180)

        considered: list[dict[str, Any]] = []
        matches: list[dict[str, Any]] = []
        all_matched = True
        for desired_raw in values:
            desired = str(desired_raw or "").strip()
            if not desired:
                continue
            desired_pipeline, desired_status = self._split_profile_status_value(desired, pipeline_hint=pipeline_hint)
            items = self._collect_status_popup_items(popup, stage=stage)
            best: dict[str, Any] | None = None
            best_score = -10**9
            for item in items:
                item_pipeline, item_status = self._split_dom_status_value(item)
                score = 0
                if desired_status and item_status:
                    if desired_status == item_status:
                        score += 120
                    elif desired_status in item_status or item_status in desired_status:
                        score += 70
                    else:
                        score -= 90
                if desired_pipeline:
                    if desired_pipeline == item_pipeline:
                        score += 40
                    elif desired_pipeline in item_pipeline or item_pipeline in desired_pipeline:
                        score += 20
                    else:
                        score -= 60
                if bool(item.get("checked", False)):
                    score += 3
                if score > best_score:
                    best_score = score
                    best = item
                considered.append({
                    "desired": desired,
                    "desired_pipeline": desired_pipeline,
                    "desired_status": desired_status,
                    "dom_pipeline": item_pipeline,
                    "dom_status": item_status,
                    "dom_data_value": str(item.get("data_value", "")),
                    "dom_full_text": str(item.get("full_text", "")),
                    "score": score,
                })

            if best is None or best_score < 10:
                all_matched = False
                matches.append({"desired": desired, "matched": False, "reason": "no_dom_match"})
                continue

            idx = int(best.get("index", -1))
            checked_before = bool(best.get("checked", False))
            clicked = False
            click_mode_local = "none"
            if idx >= 0 and not checked_before:
                label = popup.locator("label.checkboxes-search__item-label").nth(idx)
                for mode in ("normal", "force", "js"):
                    try:
                        if mode == "normal":
                            label.click(timeout=900)
                        elif mode == "force":
                            label.click(timeout=900, force=True)
                        else:
                            label.evaluate("el => { el.click(); return true; }")
                        clicked = True
                        click_mode_local = mode
                        break
                    except Exception:
                        continue

            checked_after = checked_before
            data_value = str(best.get("data_value", "")).strip()
            input_name = str(popup_meta.get("popup_input_name", "")).strip()
            if data_value and input_name:
                inp = popup.locator(f"input[name='{input_name}'][data-value='{data_value}']")
                try:
                    if inp.count() > 0:
                        checked_after = bool(inp.first.evaluate("el => !!el.checked"))
                except Exception:
                    checked_after = checked_before

            matched = bool(checked_after and (checked_before or clicked or idx >= 0))
            matches.append({
                "desired": desired,
                "matched": matched,
                "click_mode": click_mode_local,
                "dom_data_value": data_value,
                "dom_full_text": str(best.get("full_text", "")),
                "checked_before": checked_before,
                "checked_after": bool(checked_after),
            })
            if not matched:
                all_matched = False

        apply_button_found = False
        apply_clicked = False
        apply_selector = ""
        for _ in range(20):
            for selector in (
                ".js-checkboxes-search-list-apply",
                ".checkboxes-search__buttons-wrapper .js-checkboxes-search-list-apply",
                ".checkboxes-search__buttons-wrapper .button-input",
                "button:has-text('OK')",
                "button:has-text('ОК')",
            ):
                loc = popup.locator(selector)
                try:
                    if loc.count() <= 0:
                        continue
                except Exception:
                    continue
                apply_button_found = True
                btn = loc.first
                try:
                    cls = self._norm(str(btn.evaluate("el => String(el.className || '')") or ""))
                except Exception:
                    cls = ""
                if "disabled" in cls or "button-input-disabled" in cls:
                    continue
                try:
                    btn.click(timeout=900)
                    apply_clicked = True
                    apply_selector = selector
                    break
                except Exception:
                    try:
                        btn.click(timeout=900, force=True)
                        apply_clicked = True
                        apply_selector = f"{selector}:force"
                        break
                    except Exception:
                        continue
            if apply_clicked:
                break
            page.wait_for_timeout(200)

        popup_closed = False
        for _ in range(20):
            page.wait_for_timeout(150)
            p2, _m2 = self._find_status_global_popup(page, stage=stage)
            if p2 is None:
                popup_closed = True
                break

        final_reflection = self._control_text(control)
        self.logger.info(
            "weekly status search select: stage=%s popup_found=%s popup_candidates=%s clear_found=%s clear_clicked=%s item_count=%s apply_found=%s apply_clicked=%s popup_closed=%s final_reflection_text=%s",
            stage,
            str(popup_meta.get("popup_found", False)).lower(),
            int(popup_meta.get("popup_candidates_count", 0)),
            str(clear_button_found).lower(),
            str(clear_clicked).lower(),
            len(self._collect_status_popup_items(popup, stage=stage)),
            str(apply_button_found).lower(),
            str(apply_clicked).lower(),
            str(popup_closed).lower(),
            final_reflection,
        )

        return bool(all_matched and apply_clicked and popup_closed), {
            "click_mode": click_mode,
            "popup_found": bool(popup_meta.get("popup_found", False)),
            "popup_candidates_count": int(popup_meta.get("popup_candidates_count", 0)),
            "clear_button_found": clear_button_found,
            "clear_clicked": clear_clicked,
            "considered_dom_values": considered[:120],
            "matches": matches,
            "apply_button_found": apply_button_found,
            "apply_clicked": apply_clicked,
            "apply_selector": apply_selector,
            "popup_closed": popup_closed,
            "final_reflection_text": final_reflection,
        }

    def _detect_checkbox_control_kind(self, control: Any) -> str | None:
        checks: tuple[tuple[str, str], ...] = (
            ("dropdown", ".checkboxes_dropdown, .js-control-checkboxes_dropdown"),
            ("search", ".checkboxes-search, .js-control-checkboxes-search"),
        )
        for kind, selector in checks:
            try:
                if control.locator(selector).count() > 0:
                    return kind
            except Exception:
                continue
        try:
            cls = str(control.evaluate("el => String(el.className || '')") or "")
        except Exception:
            cls = ""
        cls_norm = self._norm(cls)
        if "checkboxes_dropdown" in cls_norm:
            return "dropdown"
        if "checkboxes-search" in cls_norm or "checkboxes_search" in cls_norm:
            return "search"
        return None

    def _is_checkbox_like_control(self, control: Any) -> bool:
        return self._detect_checkbox_control_kind(control) is not None

    def _is_checkbox_dropdown_control(self, control: Any) -> bool:
        return self._detect_checkbox_control_kind(control) == "dropdown"

    def _open_checkbox_dropdown_control(self, page: Page, control: Any) -> tuple[bool, str]:
        return self._open_checkbox_like_control(page, control, kind="dropdown")

    def _read_event_type_open_state(self, scope: Any) -> dict[str, Any]:
        opening = scope.locator(".checkboxes-search__opening-list")
        found = False
        hidden = True
        class_name = ""
        try:
            if opening.count() > 0:
                found = True
                class_name = str(opening.first.evaluate("el => String(el.className || '')") or "")
                hidden = "hidden" in self._norm(class_name)
        except Exception:
            found = False
            hidden = True
            class_name = ""
        return {
            "opening_found": found,
            "opening_hidden": hidden,
            "opening_class": class_name,
        }

    def _read_event_type_open_state_global(self, page: Page, root: Any) -> dict[str, Any]:
        def _class_value(locator: Any) -> str:
            try:
                return str(locator.evaluate("el => String(el.className || '')") or "")
            except Exception:
                return ""

        def _opening_state(scope: Any, selector: str) -> tuple[bool, bool]:
            loc = scope.locator(selector)
            try:
                if loc.count() <= 0:
                    return False, True
                cls = _class_value(loc.first)
                return True, ("hidden" in self._norm(cls))
            except Exception:
                return False, True

        root_found = False
        try:
            root_found = bool(root is not None and root.is_visible(timeout=120))
        except Exception:
            root_found = False

        title = root.locator(".checkboxes-search__title-wrapper")
        title_found = False
        try:
            title_found = title.count() > 0 and title.first.is_visible(timeout=120)
        except Exception:
            title_found = False

        root_opening_found, root_opening_hidden = _opening_state(root, ".checkboxes-search__opening-list")
        global_opening_found, global_opening_hidden = _opening_state(page, ".checkboxes-search__opening-list")

        def _is_global_visible(selector: str) -> bool:
            loc = page.locator(selector)
            try:
                return bool(loc.count() > 0 and loc.first.is_visible(timeout=100))
            except Exception:
                return False

        global_search_input_found = _is_global_visible(".checkboxes-search__search-input")
        global_section_found = _is_global_visible(".checkboxes-search__section-common")
        global_item_label_found = _is_global_visible(".checkboxes-search__item-label")
        root_class = _class_value(root) if root_found else ""
        title_class = _class_value(title.first) if title_found else ""

        active_element: dict[str, Any] = {}
        try:
            active_element = page.evaluate(
                """() => {
                    const el = document.activeElement;
                    if (!el) { return {}; }
                    const r = el.getBoundingClientRect();
                    return {
                        tagName: String(el.tagName || '').toLowerCase(),
                        className: String(el.className || ''),
                        text: String((el.innerText || el.textContent || '')).trim().slice(0, 200),
                        role: String(el.getAttribute('role') || ''),
                        ariaLabel: String(el.getAttribute('aria-label') || ''),
                        bbox: {x:r.x,y:r.y,width:r.width,height:r.height},
                    };
                }"""
            ) or {}
        except Exception:
            active_element = {}

        return {
            "root_found": bool(root_found),
            "title_found": bool(title_found),
            "root_opening_found": bool(root_opening_found),
            "root_opening_hidden": bool(root_opening_hidden),
            "global_opening_found": bool(global_opening_found),
            "global_opening_hidden": bool(global_opening_hidden),
            "global_search_input_found": bool(global_search_input_found),
            "global_section_found": bool(global_section_found),
            "global_item_label_found": bool(global_item_label_found),
            "root_class": root_class,
            "title_class": title_class,
            "active_element": active_element if isinstance(active_element, dict) else {},
        }

    def _resolve_event_type_search_root(self, page: Page, control: Any):
        selector = ".filter__custom_settings__item.checkboxes-search.js-control-checkboxes-search[title='Типы событий']"
        root = page.locator(selector)
        try:
            if root.count() > 0 and root.first.is_visible(timeout=120):
                return root.first, selector, True
        except Exception:
            pass
        return control, "fallback_control", False

    def _find_event_type_global_popup(self, page: Page):
        popup_selector = ".checkboxes-search__opening-list"
        inputs_selector = "input[name='filter[event_type][]']"
        popup = page.locator(popup_selector)
        candidates = 0
        try:
            total = min(int(popup.count()), 12)
        except Exception:
            total = 0
        for idx in range(total):
            cand = popup.nth(idx)
            try:
                if not cand.is_visible(timeout=80):
                    continue
            except Exception:
                continue
            candidates += 1
            try:
                if cand.locator(inputs_selector).count() > 0:
                    return cand, {
                        "popup_selector": popup_selector,
                        "popup_candidates_count": candidates,
                        "popup_found": True,
                        "popup_has_event_type_inputs": True,
                    }
            except Exception:
                continue
        return None, {
            "popup_selector": popup_selector,
            "popup_candidates_count": candidates,
            "popup_found": False,
            "popup_has_event_type_inputs": False,
        }

    def _wait_for_event_type_global_popup(self, page: Page, timeout_ms: int = 4500, poll_ms: int = 150):
        steps = max(1, timeout_ms // max(50, poll_ms))
        last_meta = {
            "popup_selector": ".checkboxes-search__opening-list",
            "popup_candidates_count": 0,
            "popup_found": False,
            "popup_has_event_type_inputs": False,
        }
        for _ in range(steps):
            popup, meta = self._find_event_type_global_popup(page)
            if isinstance(meta, dict):
                last_meta = meta
            if popup is not None and bool(last_meta.get("popup_has_event_type_inputs", False)):
                return popup, last_meta
            page.wait_for_timeout(poll_ms)
        return None, last_meta

    def _open_event_type_search_control(self, page: Page, control: Any) -> tuple[bool, str, dict[str, Any]]:
        root, root_selector, exact_root_found = self._resolve_event_type_search_root(page, control)
        title = root.locator(".checkboxes-search__title-wrapper")
        title_found = False
        try:
            title_found = title.count() > 0 and title.first.is_visible(timeout=120)
        except Exception:
            title_found = False

        before = self._read_event_type_open_state_global(page, root)
        diag: dict[str, Any] = {
            "root_selector": root_selector,
            "root_found": bool(exact_root_found),
            "title_found": bool(title_found),
            "root_opening_found_before": bool(before.get("root_opening_found", False)),
            "root_opening_hidden_before": bool(before.get("root_opening_hidden", True)),
            "global_opening_found_before": bool(before.get("global_opening_found", False)),
            "global_opening_hidden_before": bool(before.get("global_opening_hidden", True)),
            "global_search_input_found_before": bool(before.get("global_search_input_found", False)),
            "global_section_found_before": bool(before.get("global_section_found", False)),
            "global_item_label_found_before": bool(before.get("global_item_label_found", False)),
            "root_class_before": str(before.get("root_class", "")),
            "title_class_before": str(before.get("title_class", "")),
            "active_element_before": before.get("active_element", {}) if isinstance(before.get("active_element", {}), dict) else {},
            "open_method_used": "",
            "root_opening_found_after": False,
            "root_opening_hidden_after": True,
            "global_opening_found_after": False,
            "global_opening_hidden_after": True,
            "global_search_input_found_after": False,
            "global_section_found_after": False,
            "global_item_label_found_after": False,
            "root_class_after": "",
            "title_class_after": "",
            "active_element_after": {},
        }

        def _after(method: str) -> tuple[bool, dict[str, Any]]:
            for _ in range(7):
                page.wait_for_timeout(150)
                state = self._read_event_type_open_state_global(page, root)
                popup, popup_meta = self._find_event_type_global_popup(page)
                open_markers = self._collect_checkbox_search_open_markers(root)
                diag["open_method_used"] = method
                diag["root_opening_found_after"] = bool(state.get("root_opening_found", False))
                diag["root_opening_hidden_after"] = bool(state.get("root_opening_hidden", True))
                diag["global_opening_found_after"] = bool(state.get("global_opening_found", False))
                diag["global_opening_hidden_after"] = bool(state.get("global_opening_hidden", True))
                diag["global_search_input_found_after"] = bool(state.get("global_search_input_found", False))
                diag["global_section_found_after"] = bool(state.get("global_section_found", False))
                diag["global_item_label_found_after"] = bool(state.get("global_item_label_found", False))
                diag["popup_candidates_count_after"] = int(popup_meta.get("popup_candidates_count", 0))
                diag["popup_found_after"] = bool(popup_meta.get("popup_found", False))
                diag["popup_has_event_type_inputs_after"] = bool(popup_meta.get("popup_has_event_type_inputs", False))
                diag["found_open_markers_after"] = list(open_markers)
                diag["root_class_after"] = str(state.get("root_class", ""))
                diag["title_class_after"] = str(state.get("title_class", ""))
                diag["active_element_after"] = state.get("active_element", {}) if isinstance(state.get("active_element", {}), dict) else {}
                root_opened = bool(state.get("root_opening_found", False)) and not bool(state.get("root_opening_hidden", True))
                popup_opened = popup is not None and bool(popup_meta.get("popup_has_event_type_inputs", False))
                class_state_changed = (
                    self._norm(str(state.get("root_class", ""))) != self._norm(str(before.get("root_class", "")))
                    or self._norm(str(state.get("title_class", ""))) != self._norm(str(before.get("title_class", "")))
                )
                opened = (
                    root_opened
                    or popup_opened
                    or bool(open_markers)
                    or bool(state.get("global_search_input_found", False))
                    or bool(state.get("global_section_found", False))
                    or bool(state.get("global_item_label_found", False))
                    or class_state_changed
                )
                if opened:
                    return True, diag
            return False, diag

        if title_found:
            try:
                title.first.click(timeout=1200, force=False)
                opened, diag = _after("title_click")
                if opened:
                    return True, "event_type_search:title_click", diag
            except Exception:
                pass
            try:
                title.first.click(timeout=1200, force=True)
                opened, diag = _after("title_force_click")
                if opened:
                    return True, "event_type_search:title_force_click", diag
            except Exception:
                pass
            try:
                box = title.first.bounding_box()
                if isinstance(box, dict):
                    center_x = float(box.get("x", 0.0)) + float(box.get("width", 0.0)) / 2.0
                    center_y = float(box.get("y", 0.0)) + float(box.get("height", 0.0)) / 2.0
                    page.mouse.click(center_x, center_y)
                    opened, diag = _after("title_bbox_click")
                    if opened:
                        return True, "event_type_search:title_bbox_click", diag
            except Exception:
                pass
            try:
                title.first.evaluate("el => { el.click(); return true; }")
                opened, diag = _after("title_js_click")
                if opened:
                    return True, "event_type_search:title_js_click", diag
            except Exception:
                pass
            for method, js in (
                ("title_dispatch_mousedown", "el => el.dispatchEvent(new MouseEvent('mousedown', {bubbles:true}))"),
                ("title_dispatch_mouseup", "el => el.dispatchEvent(new MouseEvent('mouseup', {bubbles:true}))"),
                ("title_dispatch_click", "el => el.dispatchEvent(new MouseEvent('click', {bubbles:true}))"),
            ):
                try:
                    title.first.evaluate(js)
                    opened, diag = _after(method)
                    if opened:
                        return True, f"event_type_search:{method}", diag
                except Exception:
                    continue

        try:
            root.evaluate("el => { el.click(); return true; }")
            opened, diag = _after("root_js_click")
            if opened:
                return True, "event_type_search:root_js_click", diag
        except Exception:
            pass

        diag["open_method_used"] = diag.get("open_method_used") or "none"
        return False, "event_type_search:open_failed", diag

    def _open_checkbox_like_control(self, page: Page, control: Any, *, kind: str) -> tuple[bool, str]:
        # Search-kind must stay scoped to the event_type widget; page-wide option picking can hit left presets.
        if kind == "search":
            selectors = (
                ".checkboxes-search__title-wrapper",
                ".checkboxes-search__title-selected",
                ".checkboxes-search__title",
                ".checkboxes-search__title-item",
            )
            mode_prefix = "checkbox_search_title"
        else:
            selectors = (
                ".checkboxes_dropdown__title_wrapper",
                ".checkboxes_dropdown__title-selected",
                ".checkboxes_dropdown__title",
            )
            mode_prefix = "checkbox_title"
        for selector in selectors:
            try:
                loc = control.locator(selector)
                if loc.count() > 0 and loc.first.is_visible(timeout=120):
                    try:
                        loc.first.click(timeout=1200)
                        return True, f"{mode_prefix}:{selector}"
                    except Exception:
                        loc.first.click(timeout=1200, force=True)
                        return True, f"{mode_prefix}_force:{selector}"
            except Exception:
                continue
        ok, mode, _payload = self._click_control_target(page, control)
        return ok, f"fallback:{mode}"

    def _wait_checkbox_dropdown_open(self, page: Page, *, control_label: str, timeout_ms: int = 1500) -> bool:
        return self._wait_checkbox_like_open(page, kind="dropdown", control_label=control_label, timeout_ms=timeout_ms)

    def _collect_checkbox_search_open_markers(self, scope: Any) -> list[str]:
        markers: list[str] = []
        for selector in (
            ".checkboxes-search__content-scroll",
            ".checkboxes-search__search-input",
            ".checkboxes-search__section-common",
            ".checkboxes-search__buttons-wrapper",
            ".js-checkboxes-search-list-apply",
        ):
            loc = scope.locator(selector)
            try:
                if loc.count() > 0:
                    markers.append(selector)
            except Exception:
                continue
        return markers
    def _wait_checkbox_like_open(self, page: Page, *, kind: str, control_label: str, timeout_ms: int = 1500, scope: Any | None = None) -> bool:
        steps = max(1, timeout_ms // 150)
        if kind == "search":
            scoped_selectors = (
                ".checkboxes-search__opening-list:not(.hidden)",
                ".checkboxes-search__opening-list",
                ".checkboxes-search__search-input",
                ".checkboxes-search__section-common",
                ".checkboxes-search__buttons-wrapper",
                ".js-checkboxes-search-list-apply",
                ".checkboxes-search__item-label",
                "input[type='checkbox'][data-value]",
                "input[type='checkbox']",
            )
            page_fallback_selectors = (
                ".js-control-checkboxes-search .checkboxes-search__opening-list:not(.hidden)",
                ".js-control-checkboxes-search .checkboxes-search__search-input",
                ".js-control-checkboxes-search .checkboxes-search__section-common",
                ".js-control-checkboxes-search .checkboxes-search__item-label",
                ".js-control-checkboxes-search input[type='checkbox'][data-value]",
            )
        else:
            scoped_selectors = (
                ".checkboxes_dropdown__items",
                ".checkboxes_dropdown__item",
                ".checkboxes_dropdown__list",
                ".checkboxes_dropdown__list-wrapper",
                ".checkboxes_dropdown__menu",
                "label:has-text('{}')".format(control_label),
                "input[type='checkbox']",
            )
            page_fallback_selectors = (
                ".js-control-checkboxes_dropdown .checkboxes_dropdown__item",
                ".checkboxes_dropdown__item",
                "input[type='checkbox']",
            )

        for _ in range(steps):
            if scope is not None:
                for selector in scoped_selectors:
                    loc = scope.locator(selector)
                    try:
                        if loc.count() <= 0:
                            continue
                        if selector == ".checkboxes-search__opening-list":
                            cls = self._norm(str(loc.first.evaluate("el => String(el.className || '')") or ""))
                            if "hidden" not in cls:
                                return True
                            continue
                        if selector in (
                            ".checkboxes-search__content-scroll",
                            ".checkboxes-search__search-input",
                            ".checkboxes-search__section-common",
                            ".checkboxes-search__buttons-wrapper",
                            ".js-checkboxes-search-list-apply",
                            ".checkboxes-search__item-label",
                            "input[type='checkbox'][data-value]",
                            "input[type='checkbox']",
                        ):
                            return True
                        try:
                            if loc.first.is_visible(timeout=80):
                                return True
                        except Exception:
                            if selector.startswith("input[type='checkbox']"):
                                return True
                    except Exception:
                        continue
            for selector in page_fallback_selectors:
                loc = page.locator(selector)
                try:
                    if loc.count() > 0 and loc.first.is_visible(timeout=80):
                        return True
                except Exception:
                    continue
            page.wait_for_timeout(150)
        return False

    def _resolve_checkbox_scope(self, page: Page, control: Any, *, kind: str):
        if kind == "search":
            try:
                control_class = self._norm(str(control.evaluate("el => String(el.className || '')") or ""))
            except Exception:
                control_class = ""
            if "checkboxes-search" in control_class or "js-control-checkboxes-search" in control_class or "checkboxes_search" in control_class:
                return control, "control_self_scope"
            selectors = (
                ".js-control-checkboxes-search, .checkboxes-search",
                ".checkboxes-search",
            )
            for selector in selectors:
                try:
                    loc = control.locator(selector)
                    if loc.count() > 0 and loc.first.is_visible(timeout=120):
                        return loc.first, f"control_scope:{selector}"
                except Exception:
                    continue
            return None, "scope_not_found"

        selectors = (
            ".js-control-checkboxes_dropdown, .checkboxes_dropdown",
            ".checkboxes_dropdown",
        )
        for selector in selectors:
            try:
                loc = control.locator(selector)
                if loc.count() > 0 and loc.first.is_visible(timeout=120):
                    return loc.first, f"control_scope:{selector}"
            except Exception:
                continue
        return page, "page_scope_fallback"

    def _collect_visible_texts_in_scope(self, scope: Any, selectors: tuple[str, ...]) -> list[str]:
        values: list[str] = []
        for selector in selectors:
            loc = scope.locator(selector)
            try:
                count = min(int(loc.count()), 100)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=70):
                        continue
                    txt = str(item.inner_text(timeout=100) or "").strip()
                except Exception:
                    continue
                if txt:
                    values.append(txt)
        uniq: list[str] = []
        seen: set[str] = set()
        for v in values:
            k = self._norm(v)
            if not k or k in seen:
                continue
            seen.add(k)
            uniq.append(v)
        return uniq

    def _find_checkbox_option_in_scope(self, scope: Any, *, value: str, kind: str):
        needle = self._norm(value)
        if kind == "search":
            selectors = (
                f".checkboxes-search__item-label:has-text('{value}')",
                f".checkboxes-search__section-common .checkboxes-search__item-label:has-text('{value}')",
                f"input[type='checkbox'][data-value='{value}']",
                f"input[type='checkbox'][value='{value}']",
                f"label:has(input[data-value='{value}'])",
                f".checkboxes-search__opening-list *:has-text('{value}')",
                f".checkboxes-search__item:has-text('{value}')",
                f"label:has-text('{value}')",
                f"[role='option']:has-text('{value}')",
                f"li:has-text('{value}')",
                ".checkboxes-search__item",
                "label",
                "li",
            )
        else:
            selectors = (
                f".checkboxes_dropdown__item:has-text('{value}')",
                f"label:has-text('{value}')",
                f"li:has-text('{value}')",
                f"[role='option']:has-text('{value}')",
                ".checkboxes_dropdown__item",
                "label",
                "li",
            )
        for selector in selectors:
            loc = scope.locator(selector)
            try:
                count = min(int(loc.count()), 120)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=80):
                        continue
                    txt = str(item.inner_text(timeout=120) or "").strip()
                except Exception:
                    txt = ""
                if kind == "search" and selector.startswith("input[type='checkbox']"):
                    try:
                        parent_label = item.locator("xpath=ancestor-or-self::label[1]")
                        if parent_label.count() > 0 and parent_label.first.is_visible(timeout=80):
                            item = parent_label.first
                            txt = str(item.inner_text(timeout=120) or "").strip()
                    except Exception:
                        pass
                if needle not in self._norm(txt) and "has-text" not in selector and not selector.startswith("input[type='checkbox']"):
                    continue
                return item, {"selector": selector, "text": txt}
        return None, {"selector": "", "text": ""}

    def _select_checkbox_dropdown_value(self, page: Page, *, value: str, stage: str) -> bool:
        return self._select_checkbox_like_value(page, value=value, stage=stage, kind="dropdown")

    def _select_checkbox_like_value(self, page: Page, *, value: str, stage: str, kind: str, scope: Any | None = None) -> bool:
        if scope is None:
            scope = page
        item, meta = self._find_checkbox_option_in_scope(scope, value=value, kind=kind)
        if item is None:
            return False
        txt = str(meta.get("text", ""))
        selector = str(meta.get("selector", ""))
        try:
            item.click(timeout=1100)
            mode = "normal"
        except Exception:
            try:
                item.click(timeout=1100, force=True)
                mode = "force"
            except Exception:
                if kind == "search":
                    try:
                        item.evaluate("el => { el.click(); return true; }")
                        mode = "js"
                    except Exception:
                        return False
                else:
                    return False
        self.logger.info("weekly checkbox option clicked: stage=%s selector=%s mode=%s text=%s", stage, selector, mode, txt)
        return True

    def _select_event_type_search_value(self, page: Page, control: Any, value: str, *, stage: str, scope: Any) -> tuple[bool, dict[str, Any]]:
        popup, popup_meta = self._wait_for_event_type_global_popup(page, timeout_ms=4500, poll_ms=150)
        if popup is None:
            return False, {
                "selected_option_found": False,
                "selected_option_text": "",
                "selected_option_selector": "",
                "select_mode": "none",
                "popup_found": False,
                "popup_candidates_count": int(popup_meta.get("popup_candidates_count", 0)),
                "popup_has_event_type_inputs": False,
                "search_input_found": False,
                "option_texts_found": [],
                "clear_button_found": False,
                "clear_clicked": False,
                "apply_button_found": False,
                "apply_clicked": False,
                "popup_closed": False,
            }

        search_input_found = False
        typed_into_search = False
        search_input = popup.locator(".checkboxes-search__search-input")
        try:
            if search_input.count() > 0:
                search_input_found = True
                inp = search_input.first
                inp.click(timeout=700)
                inp.fill("")
                inp.type(value, delay=15)
                typed_into_search = True
                page.wait_for_timeout(180)
        except Exception:
            typed_into_search = False

        clear_button_found = False
        clear_clicked = False
        for clear_selector in (
            ".js-checkboxes-search-clear-all",
            ".checkboxes-search__links-wrapper .js-checkboxes-search-clear-all",
            "button:has-text('Очистить')",
            "[role='button']:has-text('Очистить')",
        ):
            loc = popup.locator(clear_selector)
            try:
                if loc.count() <= 0:
                    continue
                clear_button_found = True
                btn = loc.first
                try:
                    btn.click(timeout=700)
                    clear_clicked = True
                    break
                except Exception:
                    try:
                        btn.click(timeout=700, force=True)
                        clear_clicked = True
                        break
                    except Exception:
                        continue
            except Exception:
                continue
        if clear_clicked:
            page.wait_for_timeout(180)

        option_texts = self._collect_visible_texts_in_scope(
            popup,
            selectors=(
                ".checkboxes-search__item",
                ".checkboxes-search__item-inner",
                ".checkboxes-search__item-label",
                "label.checkboxes-search__item-label",
            ),
        )

        target_selector = f"input[name='filter[event_type][]'][data-value='{value}']"
        target_input = popup.locator(target_selector)
        checked_before = False
        checked_after = False
        clicked_option_selector = ""
        selected_option_found = False

        try:
            if target_input.count() > 0:
                selected_option_found = True
                target = target_input.first
                checked_before = bool(target.evaluate("el => !!el.checked"))
                if not checked_before:
                    clicked = False
                    for sel in (
                        f"label.checkboxes-search__item-label:has(input[name='filter[event_type][]'][data-value='{value}'])",
                        f"label.checkboxes-search__item-label:has-text('{value}')",
                        f".checkboxes-search__item:has-text('{value}')",
                        f".checkboxes-search__item-inner:has-text('{value}')",
                        f".checkboxes-search__opening-list *:has-text('{value}')",
                    ):
                        loc = popup.locator(sel)
                        try:
                            if loc.count() <= 0:
                                continue
                            node = loc.first
                            try:
                                node = node.locator("xpath=ancestor-or-self::label[1]").first
                            except Exception:
                                pass
                            try:
                                node.click(timeout=900)
                                clicked_option_selector = sel
                                clicked = True
                                break
                            except Exception:
                                try:
                                    node.click(timeout=900, force=True)
                                    clicked_option_selector = f"{sel}:force"
                                    clicked = True
                                    break
                                except Exception:
                                    try:
                                        node.evaluate("el => { el.click(); return true; }")
                                        clicked_option_selector = f"{sel}:js"
                                        clicked = True
                                        break
                                    except Exception:
                                        continue
                        except Exception:
                            continue
                    if not clicked:
                        try:
                            target.click(timeout=900)
                            clicked_option_selector = target_selector
                        except Exception:
                            try:
                                target.click(timeout=900, force=True)
                                clicked_option_selector = f"{target_selector}:force"
                            except Exception:
                                pass
                page.wait_for_timeout(160)
                checked_after = bool(target.evaluate("el => !!el.checked"))
        except Exception:
            selected_option_found = False

        apply_button_found = False
        apply_clicked = False
        apply_selector = ""
        for _ in range(25):
            for sel in (
                ".js-checkboxes-search-list-apply",
                ".checkboxes-search__buttons-wrapper .js-checkboxes-search-list-apply",
                ".checkboxes-search__buttons-wrapper .button-input",
                "button:has-text('OK')",
                "button:has-text('ОК')",
            ):
                loc = popup.locator(sel)
                try:
                    if loc.count() <= 0:
                        continue
                except Exception:
                    continue
                btn = loc.first
                apply_button_found = True
                try:
                    cls = self._norm(str(btn.evaluate("el => String(el.className || '')") or ""))
                except Exception:
                    cls = ""
                if "button-input-disabled" in cls or "disabled" in cls:
                    continue
                try:
                    btn.click(timeout=900)
                    apply_clicked = True
                    apply_selector = sel
                    break
                except Exception:
                    try:
                        btn.click(timeout=900, force=True)
                        apply_clicked = True
                        apply_selector = f"{sel}:force"
                        break
                    except Exception:
                        continue
            if apply_clicked:
                break
            page.wait_for_timeout(200)

        popup_closed = False
        for _ in range(20):
            page.wait_for_timeout(150)
            p2, _m2 = self._find_event_type_global_popup(page)
            if p2 is None:
                popup_closed = True
                break

        final_reflection = self._control_text(control)
        self.logger.info(
            "weekly event_type search select: popup_visible=%s popup_candidates=%s clear_button_found=%s clear_clicked=%s target_input_found=%s checked_before=%s checked_after=%s apply_button_found=%s apply_clicked=%s popup_closed=%s clicked_option_selector=%s option_texts_found=%s final_reflection_text=%s",
            str(popup_meta.get("popup_found", False)).lower(),
            int(popup_meta.get("popup_candidates_count", 0)),
            str(clear_button_found).lower(),
            str(clear_clicked).lower(),
            str(selected_option_found).lower(),
            str(checked_before).lower(),
            str(checked_after).lower(),
            str(apply_button_found).lower(),
            str(apply_clicked).lower(),
            str(popup_closed).lower(),
            clicked_option_selector,
            option_texts[:20],
            final_reflection,
        )

        success = bool(checked_after) or (apply_clicked and popup_closed) or (self._norm(value) in self._norm(final_reflection))
        return success, {
            "selected_option_found": selected_option_found,
            "selected_option_text": value,
            "selected_option_selector": clicked_option_selector,
            "select_mode": "search_popup" if success else "none",
            "popup_found": bool(popup_meta.get("popup_found", False)),
            "popup_candidates_count": int(popup_meta.get("popup_candidates_count", 0)),
            "popup_has_event_type_inputs": bool(popup_meta.get("popup_has_event_type_inputs", False)),
            "search_input_found": search_input_found,
            "typed_into_search": typed_into_search,
            "clear_button_found": clear_button_found,
            "clear_clicked": clear_clicked,
            "checked_before": checked_before,
            "checked_after": checked_after,
            "option_texts_found": option_texts[:20],
            "clicked_option_selector": clicked_option_selector,
            "apply_button_found": apply_button_found,
            "apply_clicked": apply_clicked,
            "apply_selector": apply_selector,
            "popup_closed": popup_closed,
            "final_reflection_text": final_reflection,
            "checkbox_checked": bool(checked_after),
        }

    def _close_checkbox_dropdown_with_escape(self, page: Page) -> None:
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(120)
        except Exception as exc:
            self.logger.warning("weekly checkbox dropdown escape failed: %s", str(exc))

    def _find_checkbox_dropdown_option(self, page: Page, value: str):
        return self._find_checkbox_option_in_scope(page, value=value, kind="dropdown")

    def _get_checkbox_dropdown_option_state(self, page: Page, value: str, *, stage: str) -> dict[str, Any]:
        return self._get_checkbox_like_option_state(page, value, stage=stage, kind="dropdown")

    def _get_checkbox_like_option_state(self, scope: Any, value: str, *, stage: str, kind: str) -> dict[str, Any]:
        item, meta = self._find_checkbox_option_in_scope(scope, value=value, kind=kind)
        payload: dict[str, Any] = {
            "stage": stage,
            "value": value,
            "kind": kind,
            "selector": str(meta.get("selector", "")),
            "text": str(meta.get("text", "")),
            "found": item is not None,
            "checkbox_checked": False,
            "aria_checked": "",
            "aria_selected": "",
            "className": "",
            "data_selected": "",
        }
        if item is None:
            return payload
        try:
            state = item.evaluate(
                """el => {
                    const checkbox = el.matches("input[type='checkbox']") ? el : el.querySelector("input[type='checkbox']");
                    const checked = !!(checkbox && checkbox.checked);
                    const ariaChecked = String(el.getAttribute('aria-checked') || checkbox?.getAttribute('aria-checked') || '');
                    const ariaSelected = String(el.getAttribute('aria-selected') || checkbox?.getAttribute('aria-selected') || '');
                    const cls = String(el.className || '');
                    const dataSelected = String(el.getAttribute('data-selected') || checkbox?.getAttribute('data-selected') || '');
                    return {
                        checkbox_checked: checked,
                        aria_checked: ariaChecked,
                        aria_selected: ariaSelected,
                        className: cls,
                        data_selected: dataSelected,
                        text: String((el.innerText || el.textContent || '')).trim(),
                    };
                }"""
            )
            if isinstance(state, dict):
                payload.update(state)
        except Exception:
            pass
        return payload

    def _verify_checkbox_dropdown_value_selected(self, page: Page, value: str, *, stage: str) -> tuple[bool, dict[str, Any]]:
        return self._verify_checkbox_like_value_selected(page, value, stage=stage, kind="dropdown")

    def _verify_checkbox_dropdown_selection(
        self,
        page: Page,
        control: Any,
        *,
        value: str,
        stage: str,
        scope: Any | None = None,
        allow_reopen_once: bool = False,
    ) -> tuple[bool, dict[str, Any]]:
        """Verify dropdown selection resiliently even if option node was rerendered."""
        if scope is None:
            scope = page
        selected, state = self._verify_checkbox_like_value_selected(scope, value, stage=stage, kind="dropdown")
        if selected:
            state["source"] = "option_state"
            return True, state

        reflection_ok, reflection_source = self._verify_selected_values(control, [value], require_any=True)
        if reflection_ok:
            return True, {
                **state,
                "source": reflection_source,
                "reflection_ok": True,
                "reopen_attempted": False,
            }

        checked_snapshot: list[dict[str, Any]] = []
        checked_match = False
        try:
            checked_snapshot = page.evaluate(
                """() => Array.from(document.querySelectorAll('input[type="checkbox"]'))
                    .filter(cb => cb.checked)
                    .map(cb => ({
                        name: cb.name || '',
                        value: cb.value || '',
                        className: String(cb.className || ''),
                        ariaChecked: String(cb.getAttribute('aria-checked') || ''),
                        ariaSelected: String(cb.getAttribute('aria-selected') || ''),
                        labelText: String(
                            (
                                cb.closest('label')?.innerText ||
                                cb.closest('li')?.innerText ||
                                cb.parentElement?.innerText ||
                                ''
                            )
                        ).trim(),
                    }))"""
            ) or []
            if not isinstance(checked_snapshot, list):
                checked_snapshot = []
        except Exception:
            checked_snapshot = []
        needle = self._norm(value)
        for item in checked_snapshot:
            text_blob = " ".join(
                [
                    str(item.get("value", "")),
                    str(item.get("labelText", "")),
                    str(item.get("name", "")),
                ]
            )
            if needle and needle in self._norm(text_blob):
                checked_match = True
                break
        if checked_match:
            return True, {
                **state,
                "source": "checked_snapshot",
                "checked_inputs_snapshot": checked_snapshot,
                "reflection_ok": False,
                "reopen_attempted": False,
            }

        if allow_reopen_once:
            opened, mode = self._open_checkbox_like_control(page, control, kind="dropdown")
            if opened:
                self._wait_checkbox_like_open(page, kind="dropdown", control_label=stage, timeout_ms=700)
                reopen_scope, _reason = self._resolve_checkbox_scope(page, control, kind="dropdown")
                if reopen_scope is None:
                    reopen_scope = page
                retry_ok, retry_state = self._verify_checkbox_like_value_selected(reopen_scope, value, stage=stage, kind="dropdown")
                self._close_checkbox_dropdown_with_escape(page)
                if retry_ok:
                    retry_state["source"] = "option_state_reopen"
                    retry_state["reopen_mode"] = mode
                    return True, retry_state
                state = {
                    **state,
                    "reopen_attempted": True,
                    "reopen_mode": mode,
                    "reopen_state": retry_state,
                    "checked_inputs_snapshot": checked_snapshot,
                }

        return False, {
            **state,
            "source": "none",
            "reflection_ok": reflection_ok,
            "reflection_source": reflection_source,
            "checked_inputs_snapshot": checked_snapshot,
            "reopen_attempted": bool(allow_reopen_once),
        }

    def _click_checkbox_search_ok(self, page: Page) -> bool:
        selectors = (
            ".checkboxes-search__buttons-wrapper .js-checkboxes-search-list-apply",
            ".js-checkboxes-search-list-apply",
            ".checkboxes-search__buttons-wrapper .button-input",
            "button:has-text('OK')",
            "[role='button']:has-text('OK')",
            "button:has-text('ОК')",
            "[role='button']:has-text('ОК')",
        )
        for pass_idx in range(2):
            saw_disabled = False
            for selector in selectors:
                loc = page.locator(selector)
                try:
                    count = min(int(loc.count()), 6)
                except Exception:
                    count = 0
                for idx in range(count):
                    item = loc.nth(idx)
                    try:
                        if not item.is_visible(timeout=90):
                            continue
                    except Exception:
                        continue
                    try:
                        cls = self._norm(str(item.evaluate("el => String(el.className || '')") or ""))
                    except Exception:
                        cls = ""
                    if "button-input-disabled" in cls or "disabled" in cls:
                        saw_disabled = True
                        continue
                    try:
                        item.click(timeout=900)
                        mode = "normal"
                    except Exception:
                        try:
                            item.click(timeout=900, force=True)
                            mode = "force"
                        except Exception:
                            continue
                    self.logger.info("weekly checkbox search OK clicked: selector=%s mode=%s", selector, mode)
                    page.wait_for_timeout(180)
                    return True
            if saw_disabled and pass_idx == 0:
                page.wait_for_timeout(220)
        self.logger.info("weekly checkbox search OK not found")
        return False

    def _verify_checkbox_search_selection(
        self,
        page: Page,
        control: Any,
        *,
        value: str,
        stage: str,
        scope: Any,
    ) -> tuple[bool, dict[str, Any]]:
        selected, state = self._verify_checkbox_like_value_selected(scope, value, stage=stage, kind="search")
        if selected:
            state["source"] = "option_state"
            return True, state
        reflection_ok, reflection_source = self._verify_selected_values(control, [value], require_any=True)
        if reflection_ok:
            return True, {**state, "source": reflection_source}
        return False, {**state, "source": "none", "reflection_source": reflection_source}

    def _verify_checkbox_like_value_selected(self, scope: Any, value: str, *, stage: str, kind: str) -> tuple[bool, dict[str, Any]]:
        state = self._get_checkbox_like_option_state(scope, value, stage=stage, kind=kind)
        cls_norm = self._norm(str(state.get("className", "")))
        selected_marked = any(marker in cls_norm for marker in ("selected", "checked", "active", "is-selected", "is-checked"))
        aria_checked = str(state.get("aria_checked", "")).strip().lower()
        aria_selected = str(state.get("aria_selected", "")).strip().lower()
        data_selected = str(state.get("data_selected", "")).strip().lower()
        ok = bool(state.get("checkbox_checked", False)) or aria_checked == "true" or aria_selected == "true" or data_selected in {"1", "true", "yes"} or selected_marked
        self.logger.info(
            "weekly checkbox state: stage=%s kind=%s value=%s selector=%s found=%s checked=%s aria_checked=%s aria_selected=%s data_selected=%s class=%s text=%s",
            stage,
            kind,
            value,
            str(state.get("selector", "")),
            str(bool(state.get("found", False))).lower(),
            str(bool(state.get("checkbox_checked", False))).lower(),
            aria_checked,
            aria_selected,
            data_selected,
            str(state.get("className", "")),
            str(state.get("text", "")),
        )
        return ok, state

    def _try_clear_checkbox_dropdown(self, page: Page, *, stage: str, kind: str = "dropdown") -> bool:
        selectors = [
            "button:has-text('????????')",
            "[role='button']:has-text('????????')",
        ]
        if kind == "search":
            selectors.extend([
                ".checkboxes-search [class*='clear']",
                ".js-control-checkboxes-search [class*='clear']",
            ])
        else:
            selectors.extend([
                ".checkboxes_dropdown__clear",
                ".js-control-checkboxes_dropdown [class*='clear']",
            ])
        for selector in selectors:
            loc = page.locator(selector)
            try:
                if loc.count() <= 0 or not loc.first.is_visible(timeout=80):
                    continue
                try:
                    loc.first.click(timeout=900)
                    mode = "normal"
                except Exception:
                    loc.first.click(timeout=900, force=True)
                    mode = "force"
                self.logger.info("weekly checkbox clear used: stage=%s kind=%s selector=%s mode=%s", stage, kind, selector, mode)
                page.wait_for_timeout(120)
                return True
            except Exception:
                continue
        self.logger.info("weekly checkbox clear not available: stage=%s kind=%s", stage, kind)
        return False

    def _resolve_control_click_target(self, control: Any):
        selectors = (
            ".control--select--button",
            ".control--select--list-open-button",
            ".control--select__button",
            "[role='button']",
            "input",
            ".js-control--select-open",
            "button",
            "a",
            "label",
            "div",
            "span",
        )
        for selector in selectors:
            try:
                loc = control.locator(selector)
                count = min(int(loc.count()), 12)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=100):
                        continue
                except Exception:
                    continue
                try:
                    payload = item.evaluate(
                        """el => {
                            const r = el.getBoundingClientRect();
                            const s = window.getComputedStyle(el);
                            const role = String(el.getAttribute('role') || '').toLowerCase();
                            const tag = String(el.tagName || '').toLowerCase();
                            const clickable = role === 'button' || typeof el.onclick === 'function' || s.cursor === 'pointer' || ['button','a','label','input'].includes(tag);
                            return {
                                clickable,
                                tagName: tag,
                                className: String(el.className || ''),
                                text: String((el.innerText || el.textContent || '')).trim(),
                                bbox: {x:r.x,y:r.y,width:r.width,height:r.height},
                            };
                        }"""
                    )
                except Exception:
                    payload = {}
                if not bool((payload or {}).get("clickable", False)):
                    continue
                return item, payload
        return None, {}

    def _click_control_target(self, page: Page, control: Any) -> tuple[bool, str, dict[str, Any]]:
        target, payload = self._resolve_control_click_target(control)
        if target is not None:
            try:
                target.click(timeout=1300)
                return True, "descendant_click", dict(payload or {})
            except Exception:
                try:
                    target.click(timeout=1300, force=True)
                    return True, "descendant_click_force", dict(payload or {})
                except Exception:
                    pass
        try:
            bbox = control.bounding_box()
        except Exception:
            bbox = None
        if isinstance(bbox, dict) and float(bbox.get("width", 0) or 0) > 0 and float(bbox.get("height", 0) or 0) > 0:
            x = float(bbox.get("x", 0) or 0) + float(bbox.get("width", 0) or 0) * 0.82
            y = float(bbox.get("y", 0) or 0) + float(bbox.get("height", 0) or 0) * 0.50
            try:
                page.mouse.click(x, y)
                return True, "bbox_right_click", {"bbox": bbox, "x": x, "y": y}
            except Exception:
                pass
        return False, "click_target_not_found", {}

    def _wait_for_options_popup(self, page: Page, *, timeout_ms: int = 1500, control_label: str | None = None) -> bool:
        steps = max(1, timeout_ms // 150)
        selectors = (
            "ul.control--select--list li.control--select--list--item",
            "[role='listbox'] [role='option']",
            ".multisuggest__suggest-item",
            ".multisuggest__list-item",
            ".suggest-manager li",
            ".control--select--list",
            ".multisuggest_show-list",
            ".multisuggest_show-suggest",
        )
        for _ in range(steps):
            for selector in selectors:
                loc = page.locator(selector)
                try:
                    if loc.count() > 0 and loc.first.is_visible(timeout=80):
                        return True
                except Exception:
                    continue
            page.wait_for_timeout(150)
        self.logger.info("weekly popup_opened=false control_label=%s", str(control_label or ""))
        return False

    def _confirm_popup_if_open(self, page: Page) -> bool:
        ok_selectors = (
            "button:has-text('OK')",
            "[role='button']:has-text('OK')",
            "button:has-text('\u041e\u041a')",
            "[role='button']:has-text('\u041e\u041a')",
        )
        popup_open = self._wait_for_options_popup(page, timeout_ms=350)
        if not popup_open:
            return True
        for selector in ok_selectors:
            loc = page.locator(selector)
            try:
                if loc.count() <= 0 or not loc.first.is_visible(timeout=100):
                    continue
                try:
                    loc.first.click(timeout=900)
                except Exception:
                    loc.first.click(timeout=900, force=True)
                page.wait_for_timeout(180)
                return True
            except Exception:
                continue
        self.logger.info("weekly popup OK not found while popup open")
        return False

    def _pick_option(self, page: Page, *, value: str, stage: str) -> bool:
        needle = self._norm(value)
        for selector in (
            f"label:has-text('{value}')",
            f"li:has-text('{value}')",
            f".control--select--list--item:has-text('{value}')",
            f".multisuggest__suggest-item:has-text('{value}')",
            f"[role='option']:has-text('{value}')",
            "ul.control--select--list li.control--select--list--item",
            "[role='listbox'] [role='option']",
            ".multisuggest__suggest-item",
            ".multisuggest__list-item",
            ".suggest-manager li",
            "li[data-value]",
        ):
            loc = page.locator(selector)
            try:
                count = min(int(loc.count()), 120)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=80):
                        continue
                    txt = (item.inner_text(timeout=120) or "").strip()
                except Exception:
                    continue
                text_ok = needle in self._norm(txt)
                if not text_ok and selector.endswith(":has-text('{value}')"):
                    text_ok = True
                if not text_ok:
                    continue
                try:
                    item.click(timeout=1100)
                    click_mode = "normal"
                except Exception:
                    try:
                        item.click(timeout=1100, force=True)
                        click_mode = "force"
                    except Exception:
                        continue
                self.logger.info("weekly option clicked: stage=%s selector=%s mode=%s text=%s", stage, selector, click_mode, txt)
                page.wait_for_timeout(120)
                return True
        self.logger.warning("weekly option not found: stage=%s value=%s options=%s", stage, value, self._read_visible_option_texts(page))
        return False

    def _control_text(self, control: Any) -> str:
        try:
            return str(control.inner_text(timeout=250) or "").strip()
        except Exception:
            try:
                return str(control.evaluate("el => String((el.innerText || el.textContent || '')).trim()") or "").strip()
            except Exception:
                return ""

    def _control_input_values(self, control: Any) -> list[str]:
        values: list[str] = []
        for selector in ("input", "input[type='hidden']", "input[type='text']", "textarea"):
            loc = control.locator(selector)
            try:
                count = min(int(loc.count()), 30)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=80) and "hidden" not in selector:
                        continue
                except Exception:
                    pass
                try:
                    val = str(item.input_value(timeout=80) or "").strip()
                except Exception:
                    try:
                        val = str(item.get_attribute("value") or "").strip()
                    except Exception:
                        val = ""
                if val:
                    values.append(val)
        uniq: list[str] = []
        seen: set[str] = set()
        for v in values:
            k = self._norm(v)
            if not k or k in seen:
                continue
            seen.add(k)
            uniq.append(v)
        return uniq

    def _control_chip_texts(self, control: Any) -> list[str]:
        values: list[str] = []
        for selector in (".tag", ".chip", ".multisuggest__list-item", "[data-value]", "li"):
            loc = control.locator(selector)
            try:
                count = min(int(loc.count()), 50)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=80):
                        continue
                    txt = str(item.inner_text(timeout=80) or "").strip()
                except Exception:
                    continue
                if txt:
                    values.append(txt)
        uniq: list[str] = []
        seen: set[str] = set()
        for v in values:
            k = self._norm(v)
            if not k or k in seen:
                continue
            seen.add(k)
            uniq.append(v)
        return uniq

    def _verify_selected_values(self, control: Any, expected_values: list[str], *, require_any: bool = False) -> tuple[bool, str]:
        expected = [self._norm(v) for v in expected_values if str(v).strip()]
        if not expected:
            return True, "empty_expected"

        control_text = self._control_text(control)
        control_norm = self._norm(control_text)
        input_values = self._control_input_values(control)
        input_norm = [self._norm(v) for v in input_values]
        chip_values = self._control_chip_texts(control)
        chip_norm = [self._norm(v) for v in chip_values]

        def matched(values: list[str]) -> bool:
            if require_any:
                return any(ev in entry for ev in expected for entry in values)
            return all(any(ev in entry for entry in values) for ev in expected)

        if matched([control_norm]):
            return True, "control_text"
        if matched(input_norm):
            return True, "input_value"
        if matched(chip_norm):
            return True, "chip_text"
        return False, "none"

    def _apply_control_values(self, page: Page, panel: Any, *, control_label: str, values: list[str], stage: str, allow_multi: bool) -> None:
        stage_started_at = time.monotonic()
        clean = [str(v).strip() for v in values if str(v).strip()]
        if not clean:
            return

        control, candidates = self._resolve_filter_control_for_stage(panel, control_label=control_label, stage=stage)
        if control is None:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "control_resolve_failed", "control_label": control_label}, candidates=candidates)
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

        self.logger.info("weekly stage timing: stage=%s checkpoint=after_control_resolve elapsed_ms=%s", stage, int((time.monotonic() - stage_started_at) * 1000))
        before_text = self._control_text(control)
        picks = clean if allow_multi else [clean[0]]
        checkbox_kind = self._detect_checkbox_control_kind(control)

        if checkbox_kind is not None:
            stage_key = self._norm(stage)
            if checkbox_kind == "search" and stage_key in {"status_before", "status_after"}:
                pipeline_hint = ""
                if stage_key == "status_after" and clean:
                    head_pipeline, _head_status = self._split_profile_status_value(clean[0])
                    if not head_pipeline:
                        pipeline_hint = self._control_text(control)
                ok, status_meta = self._select_status_popup_values(
                    page,
                    control,
                    stage=stage,
                    values=picks,
                    pipeline_hint=pipeline_hint,
                    allow_multi=allow_multi,
                )
                self.logger.info(
                    "weekly status stage debug: stage=%s root_selector=%s popup_found=%s popup_candidates=%s clear_found=%s clear_clicked=%s apply_clicked=%s popup_closed=%s",
                    stage,
                    ".js-control-checkboxes-search[data-name='filter[value_before][status_lead][]']" if stage_key == "status_before" else ".js-control-checkboxes-search[data-name='filter[value_after][status_lead][]']",
                    str(bool(status_meta.get("popup_found", False))).lower(),
                    int(status_meta.get("popup_candidates_count", 0)),
                    str(bool(status_meta.get("clear_button_found", False))).lower(),
                    str(bool(status_meta.get("clear_clicked", False))).lower(),
                    str(bool(status_meta.get("apply_clicked", False))).lower(),
                    str(bool(status_meta.get("popup_closed", False))).lower(),
                )
                if ok:
                    return
                artifacts = self._dump_stage_failure_artifacts(
                    page=page,
                    stage=f"{stage}_search",
                    summary={
                        "reason": "status_search_selection_failed",
                        "control_label": control_label,
                        "stage": stage,
                        "status_search_debug": status_meta,
                    },
                    candidates=candidates,
                )
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

            event_type_open_diag: dict[str, Any] = {}
            is_event_type_search = checkbox_kind == "search" and self._norm(stage) == "event_type" and self._norm(control_label) == self._norm("\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439")
            if is_event_type_search:
                opened, checkbox_open_mode, event_type_open_diag = self._open_event_type_search_control(page, control)
            else:
                opened, checkbox_open_mode = self._open_checkbox_like_control(page, control, kind=checkbox_kind)
            if not opened:
                artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "checkbox_control_open_failed", "control_label": control_label, "kind": checkbox_kind, "click_mode": checkbox_open_mode, "control_before": before_text}, candidates=candidates)
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

            search_scope = control if checkbox_kind == "search" else None
            event_type_popup_meta: dict[str, Any] = {}
            if is_event_type_search:
                popup, event_type_popup_meta = self._wait_for_event_type_global_popup(page, timeout_ms=4500, poll_ms=150)
                popup_open = popup is not None
            else:
                popup_open = self._wait_checkbox_like_open(page, kind=checkbox_kind, control_label=control_label, scope=search_scope, timeout_ms=3000)
            try:
                control_class = str(control.evaluate("el => String(el.className || '')") or "")
                control_html_before = str(control.evaluate("el => String(el.outerHTML || '')") or "")[:2500]
            except Exception:
                control_class = ""
                control_html_before = ""
            search_root_detected = False
            found_open_markers: list[str] = []
            if checkbox_kind == "search":
                norm_class = self._norm(control_class)
                search_root_detected = "checkboxes-search" in norm_class or "js-control-checkboxes-search" in norm_class or "checkboxes_search" in norm_class
                found_open_markers = self._collect_checkbox_search_open_markers(search_scope if search_scope is not None else control)
                if found_open_markers:
                    popup_open = True
            if is_event_type_search:
                p2, pm2 = self._find_event_type_global_popup(page)
                if p2 is not None and bool(pm2.get("popup_has_event_type_inputs", False)):
                    popup_open = True
                    event_type_popup_meta = pm2
            visible_options = self._read_visible_option_texts(page)
            self.logger.info(
                "weekly control click result: stage=%s label=%s kind=%s click_mode=%s popup_opened=%s open_markers=%s control_before=%s options=%s",
                stage,
                control_label,
                checkbox_kind,
                checkbox_open_mode,
                str(popup_open).lower(),
                found_open_markers,
                before_text,
                visible_options,
            )
            if not popup_open:
                search_snapshot = {}
                if checkbox_kind == "search":
                    try:
                        search_snapshot = self._collect_checkbox_search_debug_snapshot(
                            page,
                            control,
                            control_label=control_label,
                            expected_value=clean[0],
                        )
                    except Exception as exc:
                        self.logger.warning("weekly checkbox search debug snapshot failed: stage=%s error=%s", stage, str(exc))
                        search_snapshot = {"snapshot_error": str(exc)}
                fail_stage = "event_type_open_failed" if is_event_type_search else (f"{stage}_search" if checkbox_kind == "search" else stage)
                artifacts = self._dump_stage_failure_artifacts(page=page, stage=fail_stage, summary={
                    "reason": "checkbox_popup_not_opened",
                    "control_label": control_label,
                    "kind": checkbox_kind,
                    "click_mode": checkbox_open_mode,
                    "control_before": before_text,
                    "control_outer_html_before": control_html_before,
                    "found_open_markers": found_open_markers,
                    "visible_options": visible_options,
                    "dropdown_like_elements": self._collect_dropdown_like_elements(page),
                    "checkbox_search_debug_snapshot": search_snapshot,
                    "control_class": control_class,
                    "scope_reason": "not_resolved_before_open_check",
                    "checkbox_kind": checkbox_kind,
                    "scoped_visible_texts": self._collect_visible_texts_in_scope(control, selectors=("label", ".checkboxes-search__item-label", ".checkboxes-search__section-common", "input[type='checkbox'][data-value]")) if checkbox_kind == "search" else [],
                    "search_root_detected": search_root_detected,
                    "root_found": bool(event_type_open_diag.get("root_found", False)),
                    "title_found": bool(event_type_open_diag.get("title_found", False)),
                    "root_opening_found_before": bool(event_type_open_diag.get("root_opening_found_before", False)),
                    "root_opening_hidden_before": bool(event_type_open_diag.get("root_opening_hidden_before", True)),
                    "global_opening_found_before": bool(event_type_open_diag.get("global_opening_found_before", False)),
                    "global_opening_hidden_before": bool(event_type_open_diag.get("global_opening_hidden_before", True)),
                    "global_search_input_found_before": bool(event_type_open_diag.get("global_search_input_found_before", False)),
                    "global_section_found_before": bool(event_type_open_diag.get("global_section_found_before", False)),
                    "global_item_label_found_before": bool(event_type_open_diag.get("global_item_label_found_before", False)),
                    "open_method_used": str(event_type_open_diag.get("open_method_used", "")),
                    "root_opening_found_after": bool(event_type_open_diag.get("root_opening_found_after", False)),
                    "root_opening_hidden_after": bool(event_type_open_diag.get("root_opening_hidden_after", True)),
                    "global_opening_found_after": bool(event_type_open_diag.get("global_opening_found_after", False)),
                    "global_opening_hidden_after": bool(event_type_open_diag.get("global_opening_hidden_after", True)),
                    "global_search_input_found_after": bool(event_type_open_diag.get("global_search_input_found_after", False)),
                    "global_section_found_after": bool(event_type_open_diag.get("global_section_found_after", False)),
                    "global_item_label_found_after": bool(event_type_open_diag.get("global_item_label_found_after", False)),
                    "event_type_popup_candidates_count": int(event_type_popup_meta.get("popup_candidates_count", 0)) if is_event_type_search else 0,
                    "event_type_popup_found": bool(event_type_popup_meta.get("popup_found", False)) if is_event_type_search else False,
                    "event_type_popup_has_inputs": bool(event_type_popup_meta.get("popup_has_event_type_inputs", False)) if is_event_type_search else False,
                    "active_element_before": event_type_open_diag.get("active_element_before", {}),
                    "active_element_after": event_type_open_diag.get("active_element_after", {}),
                    "selected_option_found": False,
                    "selected_option_text": "",
                }, candidates=candidates)
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

            scope, scope_reason = self._resolve_checkbox_scope(page, control, kind=checkbox_kind)
            if scope is None:
                scoped_candidates: list[str] = []
                unscoped_candidates = self._read_visible_option_texts(page)
                left_panel_texts = self._collect_visible_texts_in_scope(
                    page,
                    selectors=(
                        ".filters-set__list-item",
                        ".filters-set li",
                        "aside li",
                        "[class*='preset'] li",
                    ),
                )
                artifacts = self._dump_stage_failure_artifacts(
                    page=page,
                    stage=f"{stage}_search" if checkbox_kind == "search" else stage,
                    summary={
                        "reason": "checkbox_scope_not_found",
                        "control_label": control_label,
                        "kind": checkbox_kind,
                        "scope_reason": scope_reason,
                        "scoped_candidates": scoped_candidates,
                        "unscoped_candidates": unscoped_candidates,
                        "left_preset_panel_texts": left_panel_texts,
                        "control_class": control_class,
                        "scope_reason": scope_reason,
                        "checkbox_kind": checkbox_kind,
                        "scoped_visible_texts": self._collect_visible_texts_in_scope(control, selectors=("label", ".checkboxes-search__item-label", ".checkboxes-search__section-common", "input[type='checkbox'][data-value]")) if checkbox_kind == "search" else scoped_candidates,
                        "search_root_detected": search_root_detected,
                    },
                    candidates=candidates,
                )
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

            if not allow_multi:
                self._try_clear_checkbox_dropdown(page, stage=stage, kind=checkbox_kind)

            option_states: list[dict[str, Any]] = []
            primary_success = True
            event_type_option_states: list[dict[str, Any]] = []
            for value in picks:
                option_selected = False
                option_meta: dict[str, Any] = {}
                if is_event_type_search:
                    option_selected, option_meta = self._select_event_type_search_value(page, control, value, stage=stage, scope=scope)
                    event_type_option_states.append(option_meta)
                else:
                    option_selected = self._select_checkbox_like_value(page, value=value, stage=stage, kind=checkbox_kind, scope=scope)
                if not option_selected:
                    scoped_texts = self._collect_visible_texts_in_scope(scope, selectors=("label", "li", "[role='option']", "input[type='checkbox']"))
                    unscoped_texts = self._read_visible_option_texts(page)
                    artifacts = self._dump_stage_failure_artifacts(
                        page=page,
                        stage=f"{stage}_search" if checkbox_kind == "search" else stage,
                        summary={
                            "reason": "checkbox_option_not_found",
                            "control_label": control_label,
                            "kind": checkbox_kind,
                            "value": value,
                            "scope_reason": scope_reason,
                            "scoped_candidates": scoped_texts,
                            "unscoped_candidates": unscoped_texts,
                            "control_class": control_class,
                            "scope_reason": scope_reason,
                            "checkbox_kind": checkbox_kind,
                            "scoped_visible_texts": scoped_texts,
                            "search_root_detected": search_root_detected,
                            "selected_option_found": bool(option_meta.get("selected_option_found", False)),
                            "selected_option_text": str(option_meta.get("selected_option_text", "")),
                        },
                        candidates=candidates,
                    )
                    raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={value} control={control_label} debug_artifacts_path={artifacts}")
                self.logger.info(
                    "weekly stage timing: stage=%s checkpoint=after_option_click elapsed_ms=%s value=%s",
                    stage,
                    int((time.monotonic() - stage_started_at) * 1000),
                    value,
                )
                if checkbox_kind == "search":
                    selected, state = self._verify_checkbox_search_selection(page, control, value=value, stage=stage, scope=scope)
                else:
                    if stage_key == "entity":
                        verify_started = time.monotonic()
                        page.wait_for_timeout(450)
                        popup_closed_fast = not self._wait_checkbox_like_open(
                            page,
                            kind="dropdown",
                            control_label=control_label,
                            timeout_ms=900,
                            scope=scope,
                        )
                        verification_duration_ms = int((time.monotonic() - verify_started) * 1000)
                        selected = bool(option_selected or popup_closed_fast)
                        state = {
                            "source": "entity_fast_verification",
                            "selected_fast": selected,
                            "option_selected": bool(option_selected),
                            "popup_closed": bool(popup_closed_fast),
                            "verification_duration_ms": verification_duration_ms,
                        }
                        if not selected:
                            self.logger.warning(
                                "weekly entity fast verification inconclusive: stage=%s label=%s option_selected=%s popup_closed=%s verification_duration_ms=%s",
                                stage,
                                control_label,
                                str(bool(option_selected)).lower(),
                                str(bool(popup_closed_fast)).lower(),
                                verification_duration_ms,
                            )
                        else:
                            self.logger.info(
                                "weekly entity fast verification success: stage=%s label=%s option_selected=%s popup_closed=%s verification_duration_ms=%s",
                                stage,
                                control_label,
                                str(bool(option_selected)).lower(),
                                str(bool(popup_closed_fast)).lower(),
                                verification_duration_ms,
                            )
                    else:
                        selected, state = self._verify_checkbox_dropdown_selection(
                            page,
                            control,
                            value=value,
                            stage=stage,
                            scope=scope,
                            allow_reopen_once=True,
                        )
                option_states.append(state)
                if not selected and stage_key != "entity":
                    primary_success = False

            apply_button_found = False
            if is_event_type_search:
                apply_button_found = any(bool(item.get("apply_button_found", False)) for item in event_type_option_states)
            elif checkbox_kind == "search":
                apply_button_found = self._click_checkbox_search_ok(page)

            if stage_key == "entity" and checkbox_kind == "dropdown":
                self.logger.info("entity_timing_probe_version=v2")
                self.logger.info(
                    "weekly stage timing: stage=%s checkpoint=before_close_escape elapsed_ms=%s",
                    stage,
                    int((time.monotonic() - stage_started_at) * 1000),
                )
                escape_started = time.monotonic()
                self._close_checkbox_dropdown_with_escape(page)
                escape_duration_ms = int((time.monotonic() - escape_started) * 1000)
                self.logger.info(
                    "weekly stage timing: stage=%s checkpoint=after_close_escape elapsed_ms=%s close_escape_duration_ms=%s",
                    stage,
                    int((time.monotonic() - stage_started_at) * 1000),
                    escape_duration_ms,
                )

                self.logger.info(
                    "weekly stage timing: stage=%s checkpoint=before_control_text_after elapsed_ms=%s",
                    stage,
                    int((time.monotonic() - stage_started_at) * 1000),
                )
                control_text_started = time.monotonic()
                try:
                    after_text = self._control_text(control)
                except Exception:
                    after_text = ""
                control_text_duration_ms = int((time.monotonic() - control_text_started) * 1000)
                self.logger.info(
                    "weekly stage timing: stage=%s checkpoint=after_control_text_after elapsed_ms=%s control_text_duration_ms=%s",
                    stage,
                    int((time.monotonic() - stage_started_at) * 1000),
                    control_text_duration_ms,
                )

                total_entity_verification_ms = int((time.monotonic() - stage_started_at) * 1000)
                self.logger.info(
                    "weekly control reflection: stage=%s label=%s control_after=%s source=%s verification_duration_ms=%s total_entity_verification_ms=%s",
                    stage,
                    control_label,
                    after_text,
                    "entity_fast_path",
                    0,
                    total_entity_verification_ms,
                )
                self.logger.info(
                    "weekly stage timing: stage=%s checkpoint=before_entity_return elapsed_ms=%s entity_post_return_gap_probe_ms=%s",
                    stage,
                    int((time.monotonic() - stage_started_at) * 1000),
                    0,
                )
                if primary_success:
                    self.logger.info(
                        "weekly checkbox primary verification success: stage=%s label=%s kind=%s total_entity_verification_ms=%s",
                        stage,
                        control_label,
                        checkbox_kind,
                        total_entity_verification_ms,
                    )
                    return
                self.logger.warning(
                    "weekly entity fast verification inconclusive, continuing without blocking reflection: stage=%s label=%s total_entity_verification_ms=%s",
                    stage,
                    control_label,
                    total_entity_verification_ms,
                )
                return

            self._close_checkbox_dropdown_with_escape(page)
            page.wait_for_timeout(180)
            after_text = self._control_text(control)
            try:
                control_html_after = str(control.evaluate("el => String(el.outerHTML || '')") or "")[:2500]
            except Exception:
                control_html_after = ""
            reflection_started = time.monotonic()
            reflection_ok, reflection_source = self._verify_selected_values(control, picks, require_any=allow_multi)
            verification_duration_ms = int((time.monotonic() - reflection_started) * 1000)
            self.logger.info("weekly control reflection: stage=%s label=%s control_after=%s source=%s verification_duration_ms=%s", stage, control_label, after_text, reflection_source, verification_duration_ms)
            if is_event_type_search:
                list_closed = not self._wait_checkbox_like_open(page, kind="search", control_label=control_label, timeout_ms=500, scope=scope)
                option_clicked = any(bool(item.get("selected_option_found", False)) for item in event_type_option_states)
                selected_state_ok = any(bool(item.get("checkbox_checked", False)) or bool(item.get("selected_marked", False)) for item in option_states)
                self.logger.info(
                    "weekly event_type search debug: stage=%s root_class=%s found_open_markers=%s search_input_found=%s option_texts_found=%s clicked_option_selector=%s apply_button_found=%s final_reflection_text=%s list_closed=%s",
                    stage,
                    control_class,
                    found_open_markers,
                    str(any(".checkboxes-search__search-input" == m for m in found_open_markers)).lower(),
                    [str(item.get("selected_option_text", "")) for item in event_type_option_states][:20],
                    ",".join(str(item.get("selected_option_selector", "")) for item in event_type_option_states if item),
                    str(apply_button_found).lower(),
                    after_text,
                    str(list_closed).lower(),
                )
                if reflection_ok or selected_state_ok or (option_clicked and (apply_button_found or list_closed)):
                    self.logger.info("weekly event_type verification success: reflection=%s selected_state=%s option_clicked=%s apply_button=%s list_closed=%s", str(reflection_ok).lower(), str(selected_state_ok).lower(), str(option_clicked).lower(), str(apply_button_found).lower(), str(list_closed).lower())
                    return

            if primary_success:
                self.logger.info("weekly checkbox primary verification success: stage=%s label=%s kind=%s", stage, control_label, checkbox_kind)
                return
            if reflection_ok:
                return

            scoped_texts = self._collect_visible_texts_in_scope(scope, selectors=("label", "li", "[role='option']", "input[type='checkbox']"))
            unscoped_texts = self._read_visible_option_texts(page)
            left_panel_texts = self._collect_visible_texts_in_scope(
                page,
                selectors=(
                    ".filters-set__list-item",
                    ".filters-set li",
                    "aside li",
                    "[class*='preset'] li",
                ),
            )
            artifacts = self._dump_stage_failure_artifacts(
                page=page,
                stage=f"{stage}_checkbox_verify",
                summary={
                    "reason": "checkbox_selected_state_not_confirmed",
                    "control_label": control_label,
                    "kind": checkbox_kind,
                    "selected_values": picks,
                    "control_before": before_text,
                    "control_after": after_text,
                    "control_outer_html_after": control_html_after,
                    "found_open_markers": found_open_markers,
                    "scope_reason": scope_reason,
                    "option_states": option_states,
                    "scoped_candidates": scoped_texts,
                    "unscoped_candidates": unscoped_texts,
                    "left_preset_panel_texts": left_panel_texts,
                    "checked_inputs_snapshot": page.evaluate("""() => Array.from(document.querySelectorAll('input[type=\"checkbox\"]')).filter(cb => cb.checked).map(cb => ({name: cb.name || '', value: cb.value || '', className: String(cb.className || ''), ariaChecked: String(cb.getAttribute('aria-checked') || ''), ariaSelected: String(cb.getAttribute('aria-selected') || '')}))"""),
                    "reflection_source": reflection_source,
                },
                candidates=candidates,
            )
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={picks[0]} control={control_label} debug_artifacts_path={artifacts}")

        click_ok, click_mode, click_payload = self._click_control_target(page, control)
        if not click_ok:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "control_click_failed", "control_label": control_label, "click_mode": click_mode, "click_payload": click_payload}, candidates=candidates)
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

        popup_open = self._wait_for_options_popup(page, control_label=control_label)
        visible_options = self._read_visible_option_texts(page)
        self.logger.info("weekly control click result: stage=%s label=%s click_mode=%s popup_opened=%s control_before=%s options=%s", stage, control_label, click_mode, str(popup_open).lower(), before_text, visible_options)
        if not popup_open:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "popup_not_opened", "control_label": control_label, "click_mode": click_mode, "control_before": before_text, "visible_options": visible_options, "dropdown_like_elements": self._collect_dropdown_like_elements(page)}, candidates=candidates)
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")
        for value in picks:
            if not self._pick_option(page, value=value, stage=stage):
                artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "option_not_found", "value": value, "control_label": control_label, "visible_options": self._read_visible_option_texts(page)}, candidates=[{"text": t} for t in self._read_visible_option_texts(page)])
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={value} control={control_label} debug_artifacts_path={artifacts}")
        if allow_multi:
            self._confirm_popup_if_open(page)

        page.wait_for_timeout(180)
        after_text = self._control_text(control)
        reflection_ok, reflection_source = self._verify_selected_values(control, picks, require_any=allow_multi)
        self.logger.info("weekly control reflection: stage=%s label=%s control_after=%s source=%s", stage, control_label, after_text, reflection_source)
        if not reflection_ok:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "control_reflection_failed", "control_label": control_label, "selected_values": picks, "control_before": before_text, "control_after": after_text, "input_values": self._control_input_values(control), "chip_values": self._control_chip_texts(control), "reflection_source": reflection_source}, candidates=candidates)
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={picks[0]} control={control_label} debug_artifacts_path={artifacts}")

    def _set_entity_and_event_type(self, page: Page, panel: Any, flow_input: EventsFlowInput) -> None:
        self._apply_control_values(page, panel, control_label="\u0412\u0441\u0435 \u0441\u0443\u0449\u043d\u043e\u0441\u0442\u0438", values=[str(flow_input.entity_kind or "").strip() or "\u0421\u0434\u0435\u043b\u043a\u0438"], stage="entity", allow_multi=False)
        self._apply_control_values(page, panel, control_label="\u0422\u0438\u043f\u044b \u0441\u043e\u0431\u044b\u0442\u0438\u0439", values=[str(flow_input.event_type or "").strip() or "\u0418\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0435 \u044d\u0442\u0430\u043f\u0430 \u043f\u0440\u043e\u0434\u0430\u0436\u0438"], stage="event_type", allow_multi=False)

    def _set_pipeline_and_statuses(self, page: Page, panel: Any, flow_input: EventsFlowInput) -> None:
        pipeline_name = str(flow_input.pipeline_name or "").strip()
        if pipeline_name:
            pipeline_control, _pipeline_candidates = self._resolve_filter_control(panel, "Воронка")
            pipeline_found = pipeline_control is not None
            self.logger.info(
                "weekly pipeline control resolution: found=%s pipeline_name=%s",
                str(pipeline_found).lower(),
                pipeline_name,
            )
            if pipeline_found:
                self._apply_control_values(page, panel, control_label="Воронка", values=[pipeline_name], stage="pipeline", allow_multi=False)
            else:
                self.logger.info(
                    "pipeline control not found, skipping separate pipeline selection and relying on full status labels"
                )
        else:
            self.logger.info("weekly pipeline value empty, skipping separate pipeline selection")

        before_values = [v for v in flow_input.status_before_values if str(v).strip()] or ([flow_input.status_before] if str(flow_input.status_before).strip() else [])
        self.logger.info(
            "weekly status labels for filtering: status_before_values=%s status_after=%s",
            before_values,
            str(flow_input.status_after or "").strip(),
        )
        if before_values:
            self._apply_control_values(page, panel, control_label="Значение до", values=before_values, stage="status_before", allow_multi=True)

        status_after_value = str(flow_input.status_after or "").strip()
        if status_after_value and pipeline_name and "/" not in status_after_value and ")" not in status_after_value:
            status_after_value = f"{pipeline_name} / {status_after_value}"
        self._apply_control_values(page, panel, control_label="Значение после", values=[status_after_value], stage="status_after", allow_multi=False)

    def _set_date_filters(self, page: Page, panel: Any, flow_input: EventsFlowInput) -> None:
        def _safe_text(loc: Any, limit: int = 3) -> list[str]:
            vals: list[str] = []
            try:
                count = min(int(loc.count()), limit)
            except Exception:
                count = 0
            for idx in range(count):
                try:
                    node = loc.nth(idx)
                    if not node.is_visible(timeout=100):
                        continue
                    txt = str(node.inner_text(timeout=120) or "").strip()
                    if txt:
                        vals.append(txt)
                except Exception:
                    continue
            return vals

        def _read_date_state() -> dict[str, Any]:
            checked_mode = ""
            for selector, mode in (
                ("input[name='filter_date_switch'][value='']:checked", "created"),
                ("input[name='filter_date_switch'][value='closed']:checked", "closed"),
                ("input[name='filter[date_mode]'][value='created']:checked", "created"),
                ("input[name='filter[date_mode]'][value='closed']:checked", "closed"),
            ):
                try:
                    loc = panel.locator(selector)
                    if loc.count() > 0:
                        checked_mode = mode
                        break
                except Exception:
                    continue

            period_texts: list[str] = []
            for selector in (
                ".date_filter__period",
                ".date_filter [class*='period']",
                ".date_filter [data-before]",
                "[class*='date_filter'] [class*='period']",
            ):
                period_texts.extend(_safe_text(panel.locator(selector), limit=4))
            preset_value = ""
            try:
                preset = panel.locator("input[name='filter[date_preset]']")
                if preset.count() > 0:
                    preset_value = str(preset.first.evaluate("el => String(el.value || '')") or "")
            except Exception:
                preset_value = ""
            return {
                "checked_mode": checked_mode,
                "period_texts": period_texts[:8],
                "preset_value": preset_value,
            }

        def _infer_mode_from_visual() -> tuple[str, str]:
            text_blob = self._norm(" ".join(_safe_text(panel.locator(".date_filter, [class*='date_filter']"), limit=8)))
            if "??????" in text_blob:
                return "closed", "visual_text"
            if "??????" in text_blob:
                return "created", "visual_text"
            return "", "none"

        def _resolve_date_mode_input(target: str):
            target_label = "???????" if target == "closed" else "???????"
            selector_sets = {
                "closed": (
                    "input[name='filter_date_switch'][value='closed']",
                    "input[name='filter[date_mode]'][value='closed']",
                    "input[type='radio'][value='closed']",
                    "input[type='radio'][value='2']",
                ),
                "created": (
                    "input[name='filter_date_switch'][value='created']",
                    "input[name='filter_date_switch'][value='']",
                    "input[name='filter_date_switch']:not([value])",
                    "input[name='filter[date_mode]'][value='created']",
                    "input[type='radio'][value='created']",
                    "input[type='radio'][value='1']",
                ),
            }
            for selector in selector_sets[target]:
                loc = panel.locator(selector)
                try:
                    if loc.count() > 0:
                        node = loc.first
                        if node.is_visible(timeout=120):
                            return node, f"selector:{selector}"
                except Exception:
                    continue

            for selector in (
                f"label:has-text('{target_label}') input",
                f"*:has-text('{target_label}') input[name='filter_date_switch']",
                f"*:has-text('{target_label}') input[name='filter[date_mode]']",
            ):
                loc = panel.locator(selector)
                try:
                    if loc.count() > 0:
                        node = loc.first
                        if node.is_visible(timeout=120):
                            return node, f"label:{selector}"
                except Exception:
                    continue
            return None, "not_found"

        mode_norm = self._norm(flow_input.date_mode)
        target_mode = "closed" if "??????" in mode_norm else "created"
        target_period = str(flow_input.period_mode or "").strip()
        target_period_norm = self._norm(target_period)
        state_before = _read_date_state()
        mode_before = str(state_before.get("checked_mode", "") or "")
        self.logger.info(
            "weekly date filters before: target_mode=%s target_period=%s state=%s",
            target_mode,
            target_period,
            state_before,
        )

        mode_applied = False
        mode_reason = ""
        mode_verification_source = "none"
        mode_input, mode_input_source = _resolve_date_mode_input(target_mode)

        if mode_before == target_mode and mode_before:
            mode_applied = True
            mode_reason = "already_checked_before"
            mode_verification_source = "checked_before"
        elif mode_input is not None:
            try:
                already_checked = bool(mode_input.evaluate("el => !!el.checked"))
            except Exception:
                already_checked = False
            if already_checked:
                mode_applied = True
                mode_reason = f"already_checked:{mode_input_source}"
                mode_verification_source = "checked_after"
            else:
                clicked = False
                for apply_mode in ("check", "click", "force"):
                    try:
                        if apply_mode == "check":
                            mode_input.check(timeout=900)
                        elif apply_mode == "click":
                            mode_input.click(timeout=900)
                        else:
                            mode_input.click(timeout=900, force=True)
                        clicked = True
                        mode_reason = f"mode_set:{apply_mode}:{mode_input_source}"
                        break
                    except Exception:
                        continue
                if clicked:
                    page.wait_for_timeout(120)
                    mode_applied = True
        else:
            mode_reason = "mode_input_not_found"

        state_mode_after = _read_date_state()
        mode_after = str(state_mode_after.get("checked_mode", "") or "")
        if mode_applied:
            if mode_after == target_mode and mode_after:
                mode_verification_source = "checked_after"
            else:
                visual_mode, visual_source = _infer_mode_from_visual()
                if visual_mode == target_mode and visual_mode:
                    mode_verification_source = visual_source
                elif target_mode == "created" and mode_input is None:
                    mode_verification_source = "default_created_no_switch"
                else:
                    mode_applied = False
                    mode_reason = f"mode_not_verified:{mode_reason}"
                    mode_verification_source = "none"

        if not mode_applied and target_mode == "created" and mode_input is None:
            # events/list often has implicit Created mode without explicit radio.
            mode_applied = True
            mode_reason = "switch_missing_default_created"
            mode_verification_source = "implicit_created"

        self.logger.info(
            "weekly date mode verify: mode_before=%s mode_after=%s target_mode=%s mode_input_source=%s mode_applied=%s mode_reason=%s verification_source=%s",
            mode_before,
            mode_after,
            target_mode,
            mode_input_source,
            str(mode_applied).lower(),
            mode_reason,
            mode_verification_source,
        )

        period_applied = False
        period_reason = ""
        all_time = target_period_norm in {"за все время", "все время", "all time", "all_time"}
        if all_time:
            try:
                preset = panel.locator("input[name='filter[date_preset]']")
                if preset.count() > 0:
                    preset.first.evaluate("""el => { el.value = ''; el.dispatchEvent(new Event('input', {bubbles: true})); el.dispatchEvent(new Event('change', {bubbles: true})); }""")
                    period_applied = True
                    period_reason = "preset_cleared"
                else:
                    current_text = " ".join(state_before.get("period_texts", []))
                    if target_period_norm in self._norm(current_text):
                        period_applied = True
                        period_reason = "already_reflected"
            except Exception:
                period_applied = False
                period_reason = "preset_update_failed"
        else:
            trigger = None
            for selector in (
                ".date_filter__period",
                ".date_filter [class*='period']",
                "[class*='date_filter'] [class*='period']",
            ):
                loc = panel.locator(selector)
                try:
                    if loc.count() > 0 and loc.first.is_visible(timeout=120):
                        trigger = loc.first
                        break
                except Exception:
                    continue

            if trigger is not None:
                try:
                    trigger.click(timeout=900)
                except Exception:
                    try:
                        trigger.click(timeout=900, force=True)
                    except Exception:
                        trigger = None
                if trigger is not None:
                    page.wait_for_timeout(180)

            if trigger is not None:
                for selector in (
                    f".date_filter__period_item:has-text('{target_period}')",
                    f"[data-period]:has-text('{target_period}')",
                    f"[class*='date_filter'] li:has-text('{target_period}')",
                    f".date_filter :text('{target_period}')",
                ):
                    loc = panel.locator(selector)
                    try:
                        if loc.count() <= 0:
                            continue
                        item = loc.first
                        try:
                            item.click(timeout=900)
                        except Exception:
                            item.click(timeout=900, force=True)
                        page.wait_for_timeout(160)
                        period_applied = True
                        period_reason = f"period_item_click:{selector}"
                        break
                    except Exception:
                        continue

            if not period_applied:
                state_mid = _read_date_state()
                joined = self._norm(" ".join(state_mid.get("period_texts", [])))
                if target_period_norm and target_period_norm in joined:
                    period_applied = True
                    period_reason = "already_reflected"

        state_after = _read_date_state()
        self.logger.info(
            "weekly date filters after: mode_applied=%s mode_reason=%s mode_verification_source=%s period_applied=%s period_reason=%s state=%s",
            str(mode_applied).lower(),
            mode_reason,
            mode_verification_source,
            str(period_applied).lower(),
            period_reason,
            state_after,
        )

        if not mode_applied or not period_applied:
            artifacts = self._dump_stage_failure_artifacts(
                page=page,
                stage="date_mode",
                summary={
                    "reason": "date_mode_or_period_apply_failed",
                    "target_mode": target_mode,
                    "target_period": target_period,
                    "mode_applied": mode_applied,
                    "mode_reason": mode_reason,
                    "mode_before": mode_before,
                    "mode_after": mode_after,
                    "mode_verification_source": mode_verification_source,
                    "period_applied": period_applied,
                    "period_reason": period_reason,
                    "state_before": state_before,
                    "state_after": state_after,
                },
            )
            raise RuntimeError(f"Weekly refusals filter apply failed: stage=date_mode debug_artifacts_path={artifacts}")

    def _clear_managers(self, page: Page, panel: Any) -> None:
        for selector in (
            ".filter-search__manager .js-multisuggest-remove",
            "[data-title='\u041c\u0435\u043d\u0435\u0434\u0436\u0435\u0440\u044b'] .js-multisuggest-remove",
            "[data-title='\u041e\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u0435\u043d\u043d\u044b\u0439'] .js-multisuggest-remove",
        ):
            loc = panel.locator(selector)
            try:
                count = min(int(loc.count()), 20)
            except Exception:
                count = 0
            for idx in range(count):
                try:
                    loc.nth(idx).click(timeout=400)
                except Exception:
                    continue

    def _apply_saved_preset(self, page: Page, panel: Any, *, preset_name: str, exact_match: bool) -> None:
        name = str(preset_name or "").strip()
        if not name:
            raise RuntimeError("saved_preset mode requires filters.saved_preset_name")
        selectors = (
            f"[class*='saved'] *:has-text('{name}')",
            f"[class*='filter'] *:has-text('{name}')",
            f"aside *:has-text('{name}')",
            f"*:has-text('{name}')",
        )
        for selector in selectors:
            loc = page.locator(selector)
            try:
                count = min(int(loc.count()), 50)
            except Exception:
                count = 0
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=120):
                        continue
                    text = str(item.inner_text(timeout=120) or "").strip()
                except Exception:
                    continue
                if exact_match and self._norm(text) != self._norm(name):
                    continue
                if not exact_match and self._norm(name) not in self._norm(text):
                    continue
                try:
                    item.click(timeout=1200)
                except Exception:
                    try:
                        item.click(timeout=1200, force=True)
                    except Exception:
                        continue
                page.wait_for_timeout(250)
                self.logger.info("weekly saved preset selected: name=%s exact_match=%s selector=%s", name, str(exact_match).lower(), selector)
                return
        artifacts = self._dump_stage_failure_artifacts(page=page, stage="saved_preset", summary={"reason": "saved_preset_not_found", "preset_name": name, "exact_match": bool(exact_match)})
        raise RuntimeError(f"Weekly refusals filter apply failed: stage=saved_preset value={name} debug_artifacts_path={artifacts}")

    def _apply_mvp_filters(self, page: Page, *, panel: Any, flow_input: EventsFlowInput) -> None:
        self._clear_managers(page, panel)
        if self._norm(flow_input.filter_mode) == "saved_preset":
            self._apply_saved_preset(
                page,
                panel,
                preset_name=flow_input.saved_preset_name,
                exact_match=bool(flow_input.saved_preset_exact_match),
            )
            self._set_date_filters(page, panel, flow_input)
            return
        self._set_entity_and_event_type(page, panel, flow_input)
        self._set_pipeline_and_statuses(page, panel, flow_input)
        self._set_date_filters(page, panel, flow_input)

    def _collect_results_area_state(self, page: Page) -> dict[str, Any]:
        def _count(selector: str) -> int:
            try:
                return int(page.locator(selector).count())
            except Exception:
                return 0

        def _visible_count(selector: str, *, limit: int = 80) -> int:
            loc = page.locator(selector)
            try:
                total = min(int(loc.count()), limit)
            except Exception:
                total = 0
            visible = 0
            for idx in range(total):
                try:
                    if loc.nth(idx).is_visible(timeout=80):
                        visible += 1
                except Exception:
                    continue
            return visible

        def _collect_text(selectors: tuple[str, ...], *, limit: int = 12) -> list[str]:
            values: list[str] = []
            for selector in selectors:
                loc = page.locator(selector)
                try:
                    count = min(int(loc.count()), limit)
                except Exception:
                    count = 0
                for idx in range(count):
                    try:
                        node = loc.nth(idx)
                        if not node.is_visible(timeout=80):
                            continue
                        text = str(node.inner_text(timeout=120) or '').strip()
                    except Exception:
                        continue
                    if text:
                        values.append(text)
            deduped: list[str] = []
            seen: set[str] = set()
            for value in values:
                key = self._norm(value)
                if not key or key in seen:
                    continue
                seen.add(key)
                deduped.append(value)
            return deduped[:limit]

        container_selectors = (
            '#list_table',
            '.events-list',
            '.events-list__table',
            '.events-table',
            "[class*='events'][class*='list']",
            "[class*='events'][class*='table']",
        )
        row_selectors = (
            '#list_table > div.list-row.js-list-row[data-id]:not(.list-row-head)',
            '#list_table div.list-row.js-list-row[data-id]:not(.list-row-head)',
            '.events-list__table tbody tr',
            '.events-table tbody tr',
            'table tbody tr',
            "[role='rowgroup'] [role='row']",
        )
        loader_selectors = (
            '.spinner',
            '.loader',
            '.loading',
            '.skeleton',
            "[class*='spinner']",
            "[class*='loader']",
            "[class*='loading']",
            "[class*='skeleton']",
        )
        empty_selectors = (
            '#list_table .list__no-items',
            ".list__no-items",
            ":text('Ничего не найдено')",
            ":text('События не найдены')",
            ":text('Нет данных')",
            ":text('Нет событий')",
            ":text('Пусто')",
            ":text('No results')",
            ":text('No events')",
        )
        row_counts = {selector: _count(selector) for selector in row_selectors}
        row_visible_counts = {selector: _visible_count(selector) for selector in row_selectors}
        container_counts = {selector: _count(selector) for selector in container_selectors}
        loader_counts = {selector: _count(selector) for selector in loader_selectors}
        empty_counts = {selector: _count(selector) for selector in empty_selectors}
        empty_visible_counts = {selector: _visible_count(selector) for selector in empty_selectors}
        return {
            'row_counts': row_counts,
            'row_visible_counts': row_visible_counts,
            'container_counts': container_counts,
            'loader_counts': loader_counts,
            'empty_counts': empty_counts,
            'empty_visible_counts': empty_visible_counts,
            'visible_texts': _collect_text(
                (
                    '#list_table',
                    '.events-list',
                    '.events-table',
                    "[class*='events']",
                    'table',
                    "[role='main']",
                ),
                limit=20,
            ),
        }

    def _wait_for_results_after_apply(self, page: Page, *, initial_url: str, timeout_ms: int = 10000) -> dict[str, Any]:
        steps = max(1, int(timeout_ms / 200))
        state = self._collect_results_area_state(page)
        for _ in range(steps):
            page.wait_for_timeout(200)
            panel_closed = self._find_filter_panel(page) is None
            current_url = str(getattr(page, 'url', '') or '')
            url_changed = current_url != initial_url
            state = self._collect_results_area_state(page)
            rows_ready = any(int(v) > 0 for v in state['row_visible_counts'].values())
            loader_visible = any(int(v) > 0 for v in state['loader_counts'].values())
            empty_visible = any(int(v) > 0 for v in state['empty_visible_counts'].values())
            container_visible = any(int(v) > 0 for v in state['container_counts'].values())
            if rows_ready:
                return {
                    'confirmed': True,
                    'reason': 'rows_ready',
                    'panel_closed': panel_closed,
                    'url_changed': url_changed,
                    'loader_visible': loader_visible,
                    'empty_visible': empty_visible,
                    'container_visible': container_visible,
                    'state': state,
                }
            if empty_visible and not loader_visible:
                return {
                    'confirmed': True,
                    'reason': 'empty_state',
                    'panel_closed': panel_closed,
                    'url_changed': url_changed,
                    'loader_visible': loader_visible,
                    'empty_visible': empty_visible,
                    'container_visible': container_visible,
                    'state': state,
                }
            if panel_closed and not loader_visible and (container_visible or url_changed):
                return {
                    'confirmed': True,
                    'reason': 'panel_closed_stable',
                    'panel_closed': panel_closed,
                    'url_changed': url_changed,
                    'loader_visible': loader_visible,
                    'empty_visible': empty_visible,
                    'container_visible': container_visible,
                    'state': state,
                }
        return {
            'confirmed': False,
            'reason': 'timeout',
            'panel_closed': self._find_filter_panel(page) is None,
            'url_changed': str(getattr(page, 'url', '') or '') != initial_url,
            'loader_visible': any(int(v) > 0 for v in state['loader_counts'].values()),
            'empty_visible': any(int(v) > 0 for v in state['empty_visible_counts'].values()),
            'container_visible': any(int(v) > 0 for v in state['container_counts'].values()),
            'state': state,
        }

    def _dump_results_read_debug_artifacts(self, *, page: Page, summary: dict[str, Any]) -> dict[str, str]:
        debug_dir = ensure_inside_root(self.settings.exports_dir / 'debug', self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base = f'weekly_refusals_results_read_failed_{stamp}'
        png_path = ensure_inside_root(debug_dir / f'{base}.png', self.project_root)
        json_path = ensure_inside_root(debug_dir / f'{base}.json', self.project_root)
        txt_path = ensure_inside_root(debug_dir / f'{base}.txt', self.project_root)
        html_path = ensure_inside_root(debug_dir / f'{base}.html', self.project_root)
        try:
            page.screenshot(path=str(png_path), full_page=True)
        except Exception as exc:
            self.logger.warning('weekly results-read screenshot failed: error=%s', str(exc))
        try:
            json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
            txt_path.write_text(json.dumps(summary, ensure_ascii=False), encoding='utf-8')
            html = str(
                page.evaluate(
                    """() => {
                        const roots = [
                          document.querySelector('#list_table'),
                          document.querySelector('.events-list'),
                          document.querySelector('.events-list__table'),
                          document.querySelector('.events-table'),
                          document.querySelector('[class*=\"events\"][class*=\"list\"]'),
                          document.querySelector('[class*=\"events\"][class*=\"table\"]'),
                          document.querySelector('table'),
                          document.body
                        ].filter(Boolean);
                        return roots[0] ? String(roots[0].outerHTML || '').slice(0, 20000) : '';
                    }"""
                )
            )
            html_path.write_text(html, encoding='utf-8')
        except Exception:
            pass
        return {'screenshot': str(png_path), 'json': str(json_path), 'txt': str(txt_path), 'html': str(html_path)}

    def _click_apply(self, page: Page, *, panel: Any) -> None:
        selectors = (
            '#filter_apply',
            "button:has-text('Применить')",
            "button:has-text('Принять')",
            "button:has-text('Готово')",
            "[role='button']:has-text('Принять')",
            "[role='button']:has-text('Применить')",
            '.js-filter-apply',
        )
        initial_url = str(getattr(page, 'url', '') or '')
        button_found = False
        disabled_found = False

        for selector in selectors:
            loc = panel.locator(selector)
            try:
                if loc.count() <= 0:
                    continue
                button = loc.first
            except Exception:
                continue

            button_found = True
            try:
                disabled_now = bool(button.evaluate("""el => {
                    const attrDisabled = el.hasAttribute('disabled') || String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                    const cls = String(el.className || '').toLowerCase();
                    return attrDisabled || cls.includes('disabled') || cls.includes('button-input-disabled');
                }"""))
            except Exception:
                disabled_now = False

            if disabled_now:
                disabled_found = True
                continue

            for mode in ('normal', 'force', 'js'):
                try:
                    if mode == 'normal':
                        button.click(timeout=1500)
                    elif mode == 'force':
                        button.click(timeout=1500, force=True)
                    else:
                        button.evaluate('el => el.click()')
                except Exception:
                    continue
                wait_info = self._wait_for_results_after_apply(page, initial_url=initial_url, timeout_ms=9500)
                self.logger.info(
                    'weekly apply confirm probe: selector=%s mode=%s confirmed=%s reason=%s panel_closed=%s url_changed=%s loader_visible=%s container_visible=%s empty_visible=%s',
                    selector,
                    mode,
                    str(bool(wait_info.get('confirmed'))).lower(),
                    str(wait_info.get('reason', '')),
                    str(bool(wait_info.get('panel_closed'))).lower(),
                    str(bool(wait_info.get('url_changed'))).lower(),
                    str(bool(wait_info.get('loader_visible'))).lower(),
                    str(bool(wait_info.get('container_visible'))).lower(),
                    str(bool(wait_info.get('empty_visible'))).lower(),
                )
                if bool(wait_info.get('confirmed')):
                    return

        if button_found and disabled_found:
            state = self._collect_results_area_state(page)
            rows_visible = any(int(v) > 0 for v in state.get('row_visible_counts', {}).values())
            empty_visible = any(int(v) > 0 for v in state.get('empty_visible_counts', {}).values())
            if rows_visible or empty_visible:
                self.logger.info(
                    'weekly apply skipped: button_disabled=true rows_visible=%s empty_visible=%s; proceeding with current filtered results',
                    str(rows_visible).lower(),
                    str(empty_visible).lower(),
                )
                try:
                    page.keyboard.press('Escape')
                except Exception:
                    pass
                return

        artifacts = self._dump_stage_failure_artifacts(
            page=page,
            stage='apply',
            summary={
                'reason': 'apply_button_not_confirmed',
                'selectors': list(selectors),
                'button_found': button_found,
                'disabled_found': disabled_found,
                'results_state': self._collect_results_area_state(page),
            },
        )
        raise RuntimeError(f'Events filter apply button was not clicked/confirmed. debug_artifacts_path={artifacts}')

    def _read_events_rows(self, page: Page) -> list[dict[str, Any]]:
        container_selector = '#list_table'
        row_selectors = (
            '#list_table > div.list-row.js-list-row[data-id]:not(.list-row-head)',
            '#list_table div.list-row.js-list-row[data-id]:not(.list-row-head)',
            '.events-list__table tbody tr',
            '.events-table tbody tr',
            'table tbody tr',
        )
        selector_counts: dict[str, int] = {}
        row_locator = None
        row_selector_used = ''

        container_count = 0
        try:
            container_count = int(page.locator(container_selector).count())
        except Exception:
            container_count = 0

        for selector in row_selectors:
            loc = page.locator(selector)
            try:
                count = int(loc.count())
            except Exception:
                count = 0
            selector_counts[selector] = count
            self.logger.info('weekly results row selector probe: selector=%s count=%s', selector, count)
            if count <= 0:
                continue
            try:
                visible_count = 0
                for idx in range(min(count, 200)):
                    try:
                        if loc.nth(idx).is_visible(timeout=80):
                            visible_count += 1
                    except Exception:
                        continue
                if visible_count <= 0:
                    continue
            except Exception:
                pass
            row_locator = loc
            row_selector_used = selector
            break

        results_state = self._collect_results_area_state(page)
        empty_visible = any(int(v) > 0 for v in results_state['empty_visible_counts'].values())
        if row_locator is None:
            if empty_visible:
                self.logger.info('weekly results matched 0 rows: empty_state_detected=true details=%s', results_state['empty_visible_counts'])
                return []
            summary = {
                'reason': 'rows_not_found',
                'container_selector': container_selector,
                'container_count': container_count,
                'row_selector': row_selector_used,
                'row_count': 0,
                'selector_counts': selector_counts,
                'results_state': results_state,
                'url': str(getattr(page, 'url', '') or ''),
                'row_outer_html_snippets': [],
                'row_text_snippets': [],
                'parsed_row_draft': {},
            }
            artifacts = self._dump_results_read_debug_artifacts(page=page, summary=summary)
            raise RuntimeError(f'Events table rows not found after filter apply. debug_artifacts_path={artifacts}')

        rows: list[dict[str, Any]] = []
        row_outer_html_snippets: list[str] = []
        row_text_snippets: list[str] = []
        parsed_row_draft: dict[str, Any] = {}
        try:
            total = min(int(row_locator.count()), 500)
        except Exception:
            total = 0

        def _cell_by_field(row: Any, field_code: str):
            return row.locator(f"div.list-row__cell[data-field-code='{field_code}']")

        def _cell_text(cell: Any) -> str:
            try:
                if cell.count() <= 0:
                    return ''
            except Exception:
                return ''
            try:
                return str(cell.first.inner_text(timeout=140) or '').strip()
            except Exception:
                return ''

        def _parse_status_cell(cell: Any) -> dict[str, str]:
            try:
                if cell.count() <= 0:
                    return {'pipe': '', 'status': '', 'loss_reason': '', 'raw': ''}
            except Exception:
                return {'pipe': '', 'status': '', 'loss_reason': '', 'raw': ''}
            target = cell.first
            pipe_text = ''
            status_text = ''
            loss_reason_text = ''
            raw_text = ''
            try:
                pipe_loc = target.locator('.node-lead__pipe-text')
                if pipe_loc.count() > 0:
                    pipe_text = str(pipe_loc.first.inner_text(timeout=120) or '').strip()
            except Exception:
                pipe_text = ''
            try:
                status_loc = target.locator('.note-lead__status-text')
                if status_loc.count() > 0:
                    status_text = str(status_loc.first.inner_text(timeout=120) or '').strip()
            except Exception:
                status_text = ''
            try:
                loss_loc = target.locator('.note-lead__loss-reason-text')
                if loss_loc.count() > 0:
                    loss_reason_text = str(loss_loc.first.inner_text(timeout=120) or '').strip()
            except Exception:
                loss_reason_text = ''
            try:
                raw_text = str(target.inner_text(timeout=120) or '').strip()
            except Exception:
                raw_text = ''
            return {
                'pipe': pipe_text,
                'status': status_text,
                'loss_reason': loss_reason_text,
                'raw': raw_text,
            }

        for idx in range(total):
            row = row_locator.nth(idx)
            try:
                if not row.is_visible(timeout=80):
                    continue
                row_class = str(row.evaluate("el => String(el.className || '')") or '')
                if 'list-row-head' in row_class:
                    continue
            except Exception:
                pass

            try:
                if len(row_outer_html_snippets) < 2:
                    row_outer_html_snippets.append(str(row.evaluate("el => String(el.outerHTML || '').slice(0, 3000)") or ''))
            except Exception:
                pass
            try:
                if len(row_text_snippets) < 2:
                    row_text_snippets.append(str(row.inner_text(timeout=120) or '').strip())
            except Exception:
                pass

            try:
                date_cell = _cell_by_field(row, 'date_create')
                author_cell = _cell_by_field(row, 'author')
                object_cell = _cell_by_field(row, 'object')
                name_cell = _cell_by_field(row, 'name')
                event_cell = _cell_by_field(row, 'event')
                before_cell = _cell_by_field(row, 'value_before')
                after_cell = _cell_by_field(row, 'value_after')

                event_at = _cell_text(date_cell)
                manager = _cell_text(author_cell)
                object_text = _cell_text(object_cell)
                entity_name = _cell_text(name_cell)
                event_type = _cell_text(event_cell)

                object_href = ''
                try:
                    link = object_cell.first.locator('a[href]') if object_cell.count() > 0 else None
                    if link is not None and link.count() > 0:
                        object_href = str(link.first.get_attribute('href') or '').strip()
                except Exception:
                    object_href = ''

                status_before_struct = _parse_status_cell(before_cell)
                status_after_struct = _parse_status_cell(after_cell)

                deal_url = object_href
                if not deal_url:
                    try:
                        row_link = row.locator("a[href*='/leads/detail/'], a[href*='/leads/']")
                        if row_link.count() > 0:
                            deal_url = str(row_link.first.get_attribute('href') or '').strip()
                    except Exception:
                        deal_url = ''

                parsed = {
                    'event_at': event_at,
                    'manager': manager,
                    'entity': object_text,
                    'entity_name': entity_name,
                    'event_type': event_type,
                    'object_text': object_text,
                    'object_href': object_href,
                    'pipeline': status_before_struct.get('pipe', '') or status_after_struct.get('pipe', ''),
                    'status_before': status_before_struct.get('status', '') or status_before_struct.get('raw', ''),
                    'status_after': status_after_struct.get('status', '') or status_after_struct.get('raw', ''),
                    'status_before_loss_reason': status_before_struct.get('loss_reason', ''),
                    'status_after_loss_reason': status_after_struct.get('loss_reason', ''),
                    'value_before': status_before_struct,
                    'value_after': status_after_struct,
                    'deal_url': deal_url,
                    'deal_id': self._extract_deal_id(deal_url),
                }
                rows.append(parsed)
                if not parsed_row_draft:
                    parsed_row_draft = parsed
            except Exception:
                continue

        if rows:
            self.logger.info('weekly results parsed: row_selector=%s parsed_rows=%s', row_selector_used, len(rows))
            self.logger.info('weekly results sample row: %s', rows[0])
            return rows

        if empty_visible:
            self.logger.info('weekly results matched 0 rows: empty_state_detected=true selector=%s', row_selector_used)
            return []

        summary = {
            'reason': 'rows_visible_but_parse_empty',
            'container_selector': container_selector,
            'container_count': container_count,
            'row_selector': row_selector_used,
            'row_count': int(selector_counts.get(row_selector_used, 0) or 0),
            'selector_counts': selector_counts,
            'results_state': results_state,
            'url': str(getattr(page, 'url', '') or ''),
            'row_outer_html_snippets': row_outer_html_snippets,
            'row_text_snippets': row_text_snippets,
            'parsed_row_draft': parsed_row_draft,
        }
        artifacts = self._dump_results_read_debug_artifacts(page=page, summary=summary)
        raise RuntimeError(f'Events table is visible but parsed rows are empty. debug_artifacts_path={artifacts}')

    def _extract_deal_id(self, deal_url: str) -> str:
        import re
        m = re.search(r"/leads/(?:detail/)?(\d+)", str(deal_url or ""))
        return m.group(1) if m else ""

    def _save_events_debug(self, page: Page, prefix: str) -> None:
        try:
            debug_dir = ensure_inside_root(self.settings.exports_dir / "debug", self.project_root)
            debug_dir.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(ensure_inside_root(debug_dir / f"{prefix}.png", self.project_root)), full_page=True)
        except Exception as exc:
            self.logger.warning("weekly save_events_debug screenshot failed: prefix=%s error=%s", prefix, str(exc))































