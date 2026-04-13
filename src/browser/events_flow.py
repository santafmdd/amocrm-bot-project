"""Browser flow for amoCRM events list weekly refusals capture (MVP)."""

from __future__ import annotations

import json
import logging
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

    def _is_checkbox_dropdown_control(self, control: Any) -> bool:
        try:
            if control.locator(".checkboxes_dropdown, .js-control-checkboxes_dropdown").count() > 0:
                return True
        except Exception:
            pass
        try:
            cls = str(control.evaluate("el => String(el.className || '')") or "")
            if "checkboxes_dropdown" in cls:
                return True
        except Exception:
            pass
        return False

    def _open_checkbox_dropdown_control(self, page: Page, control: Any) -> tuple[bool, str]:
        for selector in (
            ".checkboxes_dropdown__title_wrapper",
            ".checkboxes_dropdown__title-selected",
            ".checkboxes_dropdown__title",
        ):
            try:
                loc = control.locator(selector)
                if loc.count() > 0 and loc.first.is_visible(timeout=120):
                    try:
                        loc.first.click(timeout=1200)
                        return True, f"checkbox_title:{selector}"
                    except Exception:
                        loc.first.click(timeout=1200, force=True)
                        return True, f"checkbox_title_force:{selector}"
            except Exception:
                continue
        ok, mode, _payload = self._click_control_target(page, control)
        return ok, f"fallback:{mode}"

    def _wait_checkbox_dropdown_open(self, page: Page, *, control_label: str, timeout_ms: int = 1500) -> bool:
        steps = max(1, timeout_ms // 150)
        selectors = (
            ".checkboxes_dropdown__items",
            ".checkboxes_dropdown__item",
            ".checkboxes_dropdown__list",
            ".checkboxes_dropdown__list-wrapper",
            ".checkboxes_dropdown__menu",
            ".js-control-checkboxes_dropdown .checkboxes_dropdown__item",
            "label:has-text('{}')".format(control_label),
            "input[type='checkbox']",
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
        return False

    def _select_checkbox_dropdown_value(self, page: Page, *, value: str, stage: str) -> bool:
        needle = self._norm(value)
        selectors = (
            f".checkboxes_dropdown__item:has-text('{value}')",
            f"label:has-text('{value}')",
            f"li:has-text('{value}')",
            f"[role='option']:has-text('{value}')",
            ".checkboxes_dropdown__item",
            "label",
        )
        for selector in selectors:
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
                    txt = str(item.inner_text(timeout=120) or "").strip()
                except Exception:
                    continue
                if needle not in self._norm(txt) and "has-text" not in selector:
                    continue
                try:
                    item.click(timeout=1100)
                    mode = "normal"
                except Exception:
                    try:
                        item.click(timeout=1100, force=True)
                        mode = "force"
                    except Exception:
                        continue
                self.logger.info("weekly checkbox option clicked: stage=%s selector=%s mode=%s text=%s", stage, selector, mode, txt)
                return True
        return False

    def _close_checkbox_dropdown_with_escape(self, page: Page) -> None:
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(120)
        except Exception as exc:
            self.logger.warning("weekly checkbox dropdown escape failed: %s", str(exc))

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
        clean = [str(v).strip() for v in values if str(v).strip()]
        if not clean:
            return

        control, candidates = self._resolve_filter_control(panel, control_label)
        if control is None:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "control_resolve_failed", "control_label": control_label}, candidates=candidates)
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

        before_text = self._control_text(control)
        click_ok, click_mode, click_payload = self._click_control_target(page, control)
        if not click_ok:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "control_click_failed", "control_label": control_label, "click_mode": click_mode, "click_payload": click_payload}, candidates=candidates)
            raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")

        is_checkbox = self._is_checkbox_dropdown_control(control)
        picks = clean if allow_multi else [clean[0]]

        if is_checkbox:
            opened, checkbox_open_mode = self._open_checkbox_dropdown_control(page, control)
            if not opened:
                artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "checkbox_control_open_failed", "control_label": control_label, "click_mode": checkbox_open_mode, "control_before": before_text}, candidates=candidates)
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")
            popup_open = self._wait_checkbox_dropdown_open(page, control_label=control_label)
            visible_options = self._read_visible_option_texts(page)
            self.logger.info("weekly control click result: stage=%s label=%s click_mode=%s popup_opened=%s control_before=%s options=%s", stage, control_label, checkbox_open_mode, str(popup_open).lower(), before_text, visible_options)
            if not popup_open:
                artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "checkbox_popup_not_opened", "control_label": control_label, "click_mode": checkbox_open_mode, "control_before": before_text, "visible_options": visible_options, "dropdown_like_elements": self._collect_dropdown_like_elements(page)}, candidates=candidates)
                raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={clean[0]} control={control_label} debug_artifacts_path={artifacts}")
            for value in picks:
                if not self._select_checkbox_dropdown_value(page, value=value, stage=stage):
                    artifacts = self._dump_stage_failure_artifacts(page=page, stage=stage, summary={"reason": "checkbox_option_not_found", "value": value, "control_label": control_label, "visible_options": self._read_visible_option_texts(page)}, candidates=[{"text": t} for t in self._read_visible_option_texts(page)])
                    raise RuntimeError(f"Weekly refusals filter apply failed: stage={stage} value={value} control={control_label} debug_artifacts_path={artifacts}")
            self._close_checkbox_dropdown_with_escape(page)
        else:
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
        self._apply_control_values(page, panel, control_label="\u0412\u043e\u0440\u043e\u043d\u043a\u0430", values=[flow_input.pipeline_name], stage="pipeline", allow_multi=False)
        before_values = [v for v in flow_input.status_before_values if str(v).strip()] or ([flow_input.status_before] if str(flow_input.status_before).strip() else [])
        if before_values:
            self._apply_control_values(page, panel, control_label="\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u0434\u043e", values=before_values, stage="status_before", allow_multi=True)
        self._apply_control_values(page, panel, control_label="\u0417\u043d\u0430\u0447\u0435\u043d\u0438\u0435 \u043f\u043e\u0441\u043b\u0435", values=[flow_input.status_after], stage="status_after", allow_multi=False)

    def _set_date_filters(self, page: Page, panel: Any, flow_input: EventsFlowInput) -> None:
        mode_norm = self._norm(flow_input.date_mode)
        switch_value = "closed" if "\u0437\u0430\u043a\u0440\u044b\u0442" in mode_norm else ""
        mode_input = panel.locator(f"input[name='filter_date_switch'][value='{switch_value}']")
        try:
            if mode_input.count() > 0:
                mode_input.first.check(timeout=900)
                page.wait_for_timeout(120)
            else:
                raise RuntimeError("date_mode_input_not_found")
        except Exception as exc:
            artifacts = self._dump_stage_failure_artifacts(page=page, stage="date_mode", summary={"reason": "date_mode_switch_failed", "error": str(exc)})
            raise RuntimeError(f"Weekly refusals filter apply failed: stage=date_mode debug_artifacts_path={artifacts}") from exc

        period_norm = self._norm(flow_input.period_mode)
        all_time = period_norm in {"\u0437\u0430 \u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f", "\u0432\u0441\u0435 \u0432\u0440\u0435\u043c\u044f", "all time", "all_time"}
        if all_time:
            try:
                preset = panel.locator("input[name='filter[date_preset]']")
                if preset.count() > 0:
                    preset.first.evaluate("""el => { el.value = ''; el.dispatchEvent(new Event('input', {bubbles: true})); el.dispatchEvent(new Event('change', {bubbles: true})); }""")
            except Exception:
                pass

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

    def _click_apply(self, page: Page, *, panel: Any) -> None:
        selectors = (
            "#filter_apply",
            "button:has-text('\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c')",
            "button:has-text('\u041f\u0440\u0438\u043d\u044f\u0442\u044c')",
            "button:has-text('\u0413\u043e\u0442\u043e\u0432\u043e')",
            "[role='button']:has-text('\u041f\u0440\u0438\u043d\u044f\u0442\u044c')",
            "[role='button']:has-text('\u041f\u0440\u0438\u043c\u0435\u043d\u0438\u0442\u044c')",
            ".js-filter-apply",
        )
        initial_url = str(getattr(page, "url", "") or "")
        for selector in selectors:
            loc = panel.locator(selector)
            try:
                if loc.count() <= 0:
                    continue
                button = loc.first
            except Exception:
                continue
            for mode in ("normal", "force", "js"):
                try:
                    if mode == "normal":
                        button.click(timeout=1500)
                    elif mode == "force":
                        button.click(timeout=1500, force=True)
                    else:
                        button.evaluate("el => el.click()")
                except Exception:
                    continue
                for _ in range(10):
                    page.wait_for_timeout(200)
                    panel_closed = self._find_filter_panel(page) is None
                    url_changed = str(getattr(page, "url", "") or "") != initial_url
                    rows_ready = False
                    try:
                        rows_ready = page.locator("table tbody tr, .events-table tbody tr, .events-list__table tbody tr").count() > 0
                    except Exception:
                        rows_ready = False
                    if panel_closed or url_changed or rows_ready:
                        return
        artifacts = self._dump_stage_failure_artifacts(page=page, stage="apply", summary={"reason": "apply_button_not_confirmed", "selectors": list(selectors)})
        raise RuntimeError(f"Events filter apply button was not clicked/confirmed. debug_artifacts_path={artifacts}")

    def _read_events_rows(self, page: Page) -> list[dict[str, Any]]:
        row_locator = None
        for selector in ("table tbody tr", "[role='rowgroup'] [role='row']", ".events-list__table tbody tr", ".events-table tbody tr"):
            loc = page.locator(selector)
            try:
                if loc.count() > 0:
                    row_locator = loc
                    break
            except Exception:
                continue
        if row_locator is None:
            self._save_events_debug(page, "weekly_refusals_rows_not_found")
            raise RuntimeError("Events table rows not found after filter apply.")
        rows: list[dict[str, Any]] = []
        try:
            total = min(row_locator.count(), 500)
        except Exception:
            total = 0
        for idx in range(total):
            row = row_locator.nth(idx)
            try:
                if not row.is_visible(timeout=80):
                    continue
                cells = row.locator("td, [role='cell']")
                cell_texts = []
                for c in range(min(cells.count(), 12)):
                    cell_texts.append((cells.nth(c).inner_text(timeout=80) or "").strip())
                link = row.locator("a[href*='/leads/detail/'], a[href*='/leads/']")
                deal_url = str(link.first.get_attribute("href") or "").strip() if link.count() > 0 else ""
                rows.append({
                    "raw_cells": cell_texts,
                    "event_at": cell_texts[0] if len(cell_texts) > 0 else "",
                    "manager": cell_texts[1] if len(cell_texts) > 1 else "",
                    "entity": cell_texts[2] if len(cell_texts) > 2 else "",
                    "event_type": cell_texts[3] if len(cell_texts) > 3 else "",
                    "pipeline": cell_texts[4] if len(cell_texts) > 4 else "",
                    "status_before": cell_texts[5] if len(cell_texts) > 5 else "",
                    "status_after": cell_texts[6] if len(cell_texts) > 6 else "",
                    "deal_url": deal_url,
                    "deal_id": self._extract_deal_id(deal_url),
                })
            except Exception:
                continue
        if not rows:
            self._save_events_debug(page, "weekly_refusals_rows_empty")
            raise RuntimeError("Events table is visible but parsed rows are empty.")
        return rows

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
