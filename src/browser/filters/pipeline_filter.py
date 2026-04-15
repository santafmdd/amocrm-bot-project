"""Pipeline filter handler."""

from __future__ import annotations

from typing import Any

from .base import FilterDebugContext


class PipelineFilterHandler:
    name = "pipeline"

    def __init__(self) -> None:
        self._last_diag: dict[str, Any] = {}

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        target = next((str(v).strip() for v in values if str(v).strip()), "")
        return {"panel": panel, "target": target, "operator": operator}

    def _norm(self, flow: Any, value: str) -> str:
        try:
            return str(flow._normalize_filter_text(value or ""))
        except Exception:
            return " ".join(str(value or "").strip().lower().split())

    def _payload(self, flow: Any, locator: Any) -> dict[str, Any]:
        try:
            return dict(flow._element_debug_payload(locator))
        except Exception:
            return {}

    def _safe_inner_text(self, locator: Any, timeout: int = 300) -> str:
        try:
            return (locator.inner_text(timeout=timeout) or "").strip()
        except Exception:
            return ""

    def _safe_attr(self, locator: Any, name: str) -> str:
        try:
            return (locator.get_attribute(name) or "").strip()
        except Exception:
            return ""

    def _safe_outer_html(self, locator: Any) -> str:
        try:
            return str(locator.evaluate("el => el.outerHTML || ''"))[:4000]
        except Exception:
            return ""

    def _pick_nth(self, loc: Any, idx: int):
        try:
            return loc.nth(idx)
        except Exception:
            try:
                return loc.first
            except Exception:
                return None

    def _find_row(self, flow: Any, panel: Any, target: str):
        candidates = panel.locator("div, li, section, label")
        best = None
        best_score = -10**9
        debug_rows: list[dict[str, Any]] = []
        target_norm = self._norm(flow, target)
        try:
            count = min(candidates.count(), 260)
        except Exception:
            count = 0

        for idx in range(count):
            row = self._pick_nth(candidates, idx)
            if row is None:
                continue
            try:
                if not row.is_visible(timeout=90):
                    continue
                txt = (row.inner_text(timeout=90) or "").strip()
            except Exception:
                continue
            if not txt:
                continue

            txt_norm = self._norm(flow, txt)
            score = 0
            if "воронка" in txt_norm:
                score += 300
            if target_norm and target_norm in txt_norm:
                score += 120
            if "control--select" in str(self._payload(flow, row).get("className", "")).lower():
                score += 80
            score -= min(len(txt_norm), 500)

            debug_rows.append({"idx": idx, "text_preview": txt[:180], "score": score})
            if score > best_score:
                best_score = score
                best = row

        self._last_diag["pipeline_row_candidates"] = debug_rows[:20]
        self._last_diag["pipeline_row_selected_score"] = best_score
        if best is not None:
            self._last_diag["pipeline_row_payload"] = self._payload(flow, best)
        return best

    def _resolve_select_container(self, row: Any):
        selectors = (".control--select", "[class*='control--select']", "[class*='select']")
        for selector in selectors:
            try:
                loc = row.locator(selector)
                if loc.count() <= 0:
                    continue
                item = self._pick_nth(loc, 0)
                if item is None:
                    continue
                if not item.is_visible(timeout=150):
                    continue
                return item
            except Exception:
                continue
        return row

    def _resolve_click_target(self, select_container: Any):
        selectors = (
            ".control--select--button-inner",
            ".control--select--button",
            "[role='combobox']",
            "[class*='select']",
            "button",
            "div",
            "span",
        )
        for selector in selectors:
            try:
                loc = select_container.locator(selector)
                cnt = min(loc.count(), 8)
            except Exception:
                continue
            for idx in range(cnt):
                item = self._pick_nth(loc, idx)
                if item is None:
                    continue
                try:
                    if not item.is_visible(timeout=120):
                        continue
                except Exception:
                    continue
                return item, selector
        return select_container, "container_fallback"

    def _read_pipeline_state(self, flow: Any, container: Any) -> dict[str, Any]:
        button_text = ""
        hidden_value = ""
        selected_value = ""

        try:
            btn = container.locator(".control--select--button-inner")
            if btn.count() > 0:
                button_text = self._safe_inner_text(self._pick_nth(btn, 0))
        except Exception:
            button_text = ""

        try:
            hidden = container.locator("input.control--select--input[name='pipeline_id']")
            if hidden.count() > 0:
                hidden_item = self._pick_nth(hidden, 0)
                if hidden_item is not None:
                    hidden_value = self._safe_attr(hidden_item, "value")
        except Exception:
            hidden_value = ""

        try:
            selected = container.locator("li.control--select--list--item[data-value].control--select--list--item-selected")
            if selected.count() > 0:
                selected_value = self._safe_attr(self._pick_nth(selected, 0), "data-value")
        except Exception:
            selected_value = ""

        return {
            "button_text": button_text,
            "hidden_pipeline_id": hidden_value,
            "selected_data_value": selected_value,
        }

    def _collect_option_nodes(self, flow: Any, container: Any) -> tuple[list[dict[str, Any]], str]:
        selectors = (
            "ul.control--select--list li.control--select--list--item[data-value]",
            "li.control--select--list--item[data-value]",
            "[role='option']",
            "li",
        )
        nodes: list[dict[str, Any]] = []
        used = ""
        for selector in selectors:
            try:
                loc = container.locator(selector)
                cnt = min(loc.count(), 80)
            except Exception:
                continue
            if cnt <= 0:
                continue
            used = selector
            for idx in range(cnt):
                item = self._pick_nth(loc, idx)
                if item is None:
                    continue
                txt = self._safe_inner_text(item, timeout=120)
                data_value = self._safe_attr(item, "data-value")
                payload = self._payload(flow, item)
                nodes.append(
                    {
                        "idx": idx,
                        "text": txt,
                        "data_value": data_value,
                        "payload": payload,
                    }
                )
            if nodes:
                break
        return nodes, used

    def _select_target_option(self, flow: Any, page: Any, container: Any, target: str) -> tuple[bool, str, str]:
        target_norm = self._norm(flow, target)
        option_nodes, selector = self._collect_option_nodes(flow, container)
        self._last_diag["pipeline_option_nodes_count"] = len(option_nodes)
        self._last_diag["pipeline_option_nodes_selector"] = selector
        self._last_diag["pipeline_option_nodes"] = option_nodes[:50]
        self._last_diag["pipeline_visible_option_texts"] = [n.get("text", "") for n in option_nodes[:50]]

        selected_data_value = ""
        for node in option_nodes:
            txt = str(node.get("text", "")).strip()
            if not txt:
                continue
            if target_norm not in self._norm(flow, txt):
                continue

            idx = int(node.get("idx", 0) or 0)
            try:
                loc = container.locator(selector)
                item = self._pick_nth(loc, idx)
                if item is None:
                    continue
                item.click(timeout=1200)
                page.wait_for_timeout(170)
                selected_data_value = str(node.get("data_value", "") or "")
                return True, selected_data_value, selector
            except Exception as exc:
                self._last_diag["pipeline_option_click_error"] = str(exc)
                continue

        # deterministic fallback through generic helper
        fallback_selected = bool(flow._choose_option_text(page, target))
        if fallback_selected:
            return True, "", "flow._choose_option_text"

        return False, "", selector

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        self._last_diag = {"report_id": report_id, "operator": operator, "values": list(values)}
        ctx = self.resolve(flow, page, report_id, values, operator=operator)
        target = ctx["target"]
        if not target:
            self._last_diag["pipeline_apply_fail_reason"] = "target_empty"
            return False

        panel = ctx["panel"]
        row = self._find_row(flow, panel, target)
        if row is None:
            self._last_diag["pipeline_apply_fail_reason"] = "row_not_found"
            return False

        container = self._resolve_select_container(row)
        self._last_diag["pipeline_container_payload"] = self._payload(flow, container)
        self._last_diag["pipeline_container_outer_html"] = self._safe_outer_html(container)

        before_state = self._read_pipeline_state(flow, container)
        self._last_diag["pipeline_state_before"] = before_state

        target_norm = self._norm(flow, target)
        if target_norm and target_norm in self._norm(flow, before_state.get("button_text", "")):
            # already selected is success even without clicks
            self._last_diag["pipeline_already_selected"] = True
            self._last_diag["pipeline_selection_mode"] = "already_selected"
            return True

        click_target, click_selector = self._resolve_click_target(container)
        self._last_diag["pipeline_click_target_selector"] = click_selector
        self._last_diag["pipeline_click_target_payload"] = self._payload(flow, click_target)

        opened = False
        try:
            click_target.click(timeout=1000)
            opened = True
        except Exception as exc:
            self._last_diag["pipeline_click_error"] = str(exc)
            try:
                container.click(timeout=800)
                opened = True
            except Exception as exc2:
                self._last_diag["pipeline_container_click_error"] = str(exc2)

        self._last_diag["pipeline_dropdown_opened"] = bool(opened)
        page.wait_for_timeout(170)

        selected, selected_data_value, used_selector = self._select_target_option(flow, page, container, target)
        self._last_diag["pipeline_option_selected"] = bool(selected)
        self._last_diag["pipeline_option_selected_data_value"] = selected_data_value
        self._last_diag["pipeline_option_selection_strategy"] = used_selector

        after_state = self._read_pipeline_state(flow, container)
        self._last_diag["pipeline_state_after"] = after_state
        self._last_diag["pipeline_selected_value_after"] = after_state.get("button_text", "")

        row_text = self._safe_inner_text(row, timeout=350)
        panel_text = self._safe_inner_text(panel, timeout=350)
        row_match = target_norm in self._norm(flow, row_text)
        panel_match = target_norm in self._norm(flow, panel_text)
        button_match = target_norm in self._norm(flow, str(after_state.get("button_text", "")))
        hidden_ok = bool(str(after_state.get("hidden_pipeline_id", "")).strip())

        self._last_diag["pipeline_row_value_reflected"] = bool(row_match)
        self._last_diag["pipeline_panel_value_reflected"] = bool(panel_match)
        self._last_diag["pipeline_button_text_reflected"] = bool(button_match)
        self._last_diag["pipeline_hidden_input_reflected"] = bool(hidden_ok)

        apply_state = {"visible": False, "enabled": False}
        try:
            apply_state = flow._get_panel_apply_state(page, panel)
        except Exception:
            pass
        self._last_diag["pipeline_apply_button_state"] = apply_state

        success = bool((selected or button_match) and (button_match or row_match or panel_match))
        if not success:
            self._last_diag["pipeline_apply_fail_reason"] = (
                "option_not_selected" if not selected else "selected_value_not_reflected"
            )
        return success

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        target = next((str(v).strip() for v in values if str(v).strip()), "")
        if not target:
            return False
        target_norm = self._norm(flow, target)
        try:
            txt = panel.inner_text(timeout=400)
        except Exception:
            txt = ""
        return target_norm in self._norm(flow, txt)

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"pipeline_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        diagnostics = {"reason": reason, **(extra or {})}
        diagnostics.update(self._last_diag)
        ctx.diagnostics = diagnostics
        return ctx
