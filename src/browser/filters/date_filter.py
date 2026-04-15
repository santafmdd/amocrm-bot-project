"""Date/period filter handler."""

from __future__ import annotations

from typing import Any

from .base import FilterDebugContext


_DATE_MODE_CANONICAL: dict[str, str] = {
    "created": "created",
    "созданы": "created",
    "создано": "created",
    "созданные": "created",
    "closed": "closed",
    "close": "closed",
    "закрыты": "closed",
    "закрытые": "closed",
}

_PERIOD_CANONICAL: dict[str, str] = {
    "all time": "all_time",
    "за все время": "all_time",
    "за всё время": "all_time",
    "today": "current_day",
    "за сегодня": "current_day",
    "yesterday": "previous_day",
    "за вчера": "previous_day",
    "last 30 days": "last_30_days",
    "за последние 30 дней": "last_30_days",
    "this week": "current_week",
    "за эту неделю": "current_week",
    "last week": "previous_week",
    "за прошлую неделю": "previous_week",
    "this month": "current_month",
    "за этот месяц": "current_month",
    "last month": "previous_month",
    "за прошлый месяц": "previous_month",
    "quarter": "quarter",
    "за квартал": "quarter",
    "this year": "current_year",
    "за этот год": "current_year",
}


class DateFilterHandler:
    name = "date"

    def __init__(self) -> None:
        self._last_diag: dict[str, Any] = {}

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        clean = [str(v).strip() for v in values if str(v).strip()]
        return {"panel": panel, "values": clean, "operator": operator}

    def _norm(self, flow: Any, value: str) -> str:
        try:
            return str(flow._normalize_filter_text(value or ""))
        except Exception:
            return " ".join(str(value or "").strip().lower().replace("ё", "е").replace("_", " ").split())

    def _canonical_mode(self, flow: Any, value: str) -> str:
        n = self._norm(flow, value)
        if n in _DATE_MODE_CANONICAL:
            return _DATE_MODE_CANONICAL[n]
        if "закры" in n:
            return "closed"
        if "создан" in n:
            return "created"
        return n

    def _canonical_period(self, flow: Any, value: str) -> str:
        n = self._norm(flow, value)
        if n in _PERIOD_CANONICAL:
            return _PERIOD_CANONICAL[n]
        return n

    def _safe_attr(self, locator: Any, name: str) -> str:
        try:
            return (locator.get_attribute(name) or "").strip()
        except Exception:
            return ""

    def _safe_inner_text(self, locator: Any, timeout: int = 250) -> str:
        try:
            return (locator.inner_text(timeout=timeout) or "").strip()
        except Exception:
            return ""

    def _payload(self, flow: Any, locator: Any) -> dict[str, Any]:
        try:
            return dict(flow._element_debug_payload(locator))
        except Exception:
            return {}

    def _pick_first(self, loc: Any):
        try:
            if loc.count() <= 0:
                return None
            return loc.first
        except Exception:
            return None

    def _resolve_date_container(self, panel: Any):
        selectors = (".date_filter", "[class*='date_filter']")
        for selector in selectors:
            try:
                loc = panel.locator(selector)
                item = self._pick_first(loc)
                if item is None:
                    continue
                if not item.is_visible(timeout=120):
                    continue
                return item
            except Exception:
                continue
        return panel

    def _read_state(self, flow: Any, container: Any) -> dict[str, Any]:
        mode_value = ""
        mode_label = ""
        preset_value = ""
        period_caption = ""
        period_before = ""

        checked = self._pick_first(container.locator("input[name='filter_date_switch']:checked"))
        if checked is not None:
            mode_value = self._safe_attr(checked, "value")

        active_label = self._pick_first(container.locator(".date_filter__type_item.active, .date_filter__type_item_selected"))
        if active_label is not None:
            mode_label = self._safe_inner_text(active_label)

        preset = self._pick_first(container.locator("input[name='filter[date_preset]']"))
        if preset is not None:
            preset_value = self._safe_attr(preset, "value")

        period = self._pick_first(container.locator(".date_filter__period"))
        if period is not None:
            period_caption = self._safe_inner_text(period)
            period_before = self._safe_attr(period, "data-before")

        mode_canonical = "closed" if mode_value == "closed" else "created"
        period_canonical = self._canonical_period(flow, period_caption) or self._canonical_period(flow, period_before)
        if not period_canonical and preset_value == "":
            period_canonical = "all_time"

        return {
            "mode_value": mode_value,
            "mode_label": mode_label,
            "preset_value": preset_value,
            "period_caption": period_caption,
            "period_before": period_before,
            "mode_canonical": mode_canonical,
            "period_canonical": period_canonical,
        }

    def _mode_matches(self, state: dict[str, Any], target_mode: str) -> bool:
        return str(state.get("mode_canonical", "")) == target_mode

    def _period_matches(self, state: dict[str, Any], target_period: str) -> bool:
        if not target_period:
            return True
        return str(state.get("period_canonical", "")) == target_period

    def _set_mode(self, flow: Any, page: Any, container: Any, target_mode: str) -> bool:
        target_value = "closed" if target_mode == "closed" else ""
        selector = f"input[name='filter_date_switch'][value='{target_value}']"
        inp = self._pick_first(container.locator(selector))
        if inp is None:
            self._last_diag["date_mode_set_reason"] = "mode_input_not_found"
            return False

        self._last_diag["date_mode_target_value"] = target_value
        self._last_diag["date_mode_input_payload"] = self._payload(flow, inp)

        try:
            inp.click(timeout=900)
        except Exception:
            try:
                inp.evaluate(
                    """el => {
                        el.checked = true;
                        el.dispatchEvent(new Event('input', {bubbles: true}));
                        el.dispatchEvent(new Event('change', {bubbles: true}));
                    }"""
                )
            except Exception as exc:
                self._last_diag["date_mode_set_error"] = str(exc)
                return False

        page.wait_for_timeout(120)
        return True

    def _set_period(self, flow: Any, page: Any, container: Any, target_period: str) -> bool:
        if not target_period:
            return True

        if target_period == "all_time":
            preset = self._pick_first(container.locator("input[name='filter[date_preset]']"))
            if preset is not None:
                try:
                    preset.evaluate(
                        """el => {
                            el.value = '';
                            el.dispatchEvent(new Event('input', {bubbles: true}));
                            el.dispatchEvent(new Event('change', {bubbles: true}));
                        }"""
                    )
                    page.wait_for_timeout(120)
                except Exception as exc:
                    self._last_diag["date_period_set_error"] = str(exc)
            return True

        period_open = self._pick_first(container.locator(".date_filter__period"))
        if period_open is not None:
            try:
                period_open.click(timeout=900)
            except Exception:
                pass
            page.wait_for_timeout(120)

        items = container.locator(".date_filter__period_item[data-period]")
        try:
            count = min(items.count(), 40)
        except Exception:
            count = 0

        for idx in range(count):
            item = None
            try:
                item = items.nth(idx)
            except Exception:
                item = self._pick_first(items)
            if item is None:
                continue
            txt = self._safe_inner_text(item, timeout=120)
            if self._canonical_period(flow, txt) != target_period:
                continue
            try:
                item.click(timeout=900)
                page.wait_for_timeout(120)
                return True
            except Exception:
                continue

        self._last_diag["date_period_set_reason"] = "period_item_not_selected"
        return False

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        self._last_diag = {"report_id": report_id, "operator": operator, "raw_values": list(values)}
        ctx = self.resolve(flow, page, report_id, values, operator=operator)
        vals = ctx["values"]
        if not vals:
            self._last_diag["date_apply_fail_reason"] = "target_empty"
            return False

        panel = ctx["panel"]
        container = self._resolve_date_container(panel)
        self._last_diag["date_container_payload"] = self._payload(flow, container)

        raw_mode = vals[0] if len(vals) > 0 else ""
        raw_period = vals[1] if len(vals) > 1 else ""
        target_mode = self._canonical_mode(flow, raw_mode)
        target_period = self._canonical_period(flow, raw_period)
        self._last_diag["date_target_mode_raw"] = raw_mode
        self._last_diag["date_target_period_raw"] = raw_period
        self._last_diag["date_target_mode_normalized"] = target_mode
        self._last_diag["date_target_period_normalized"] = target_period

        before = self._read_state(flow, container)
        self._last_diag["date_state_before"] = before

        mode_ok_before = self._mode_matches(before, target_mode)
        period_ok_before = self._period_matches(before, target_period)
        self._last_diag["date_already_mode_match"] = bool(mode_ok_before)
        self._last_diag["date_already_period_match"] = bool(period_ok_before)

        if mode_ok_before and period_ok_before:
            self._last_diag["date_already_selected"] = True
            self._last_diag["date_selection_mode"] = "already_selected"
            return True

        mode_set_ok = self._set_mode(flow, page, container, target_mode)
        period_set_ok = self._set_period(flow, page, container, target_period)

        after = self._read_state(flow, container)
        self._last_diag["date_state_after"] = after

        mode_ok = self._mode_matches(after, target_mode)
        period_ok = self._period_matches(after, target_period)

        self._last_diag["date_mode_set_ok"] = bool(mode_set_ok)
        self._last_diag["date_period_set_ok"] = bool(period_set_ok)
        self._last_diag["date_mode_verified"] = bool(mode_ok)
        self._last_diag["date_period_verified"] = bool(period_ok)
        self._last_diag["date_current_mode_detected"] = after.get("mode_canonical", "")
        self._last_diag["date_current_period_detected"] = after.get("period_canonical", "")
        self._last_diag["date_current_preset_value"] = after.get("preset_value", "")

        success = bool(mode_ok and period_ok)
        if not success:
            self._last_diag["date_apply_fail_reason"] = "mode_or_period_not_verified"
        self._last_diag["date_current_state_matched"] = bool(success)
        return success

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        vals = [str(v).strip() for v in values if str(v).strip()]
        if not vals:
            return False

        target_mode = self._canonical_mode(flow, vals[0] if len(vals) > 0 else "")
        target_period = self._canonical_period(flow, vals[1] if len(vals) > 1 else "")

        container = self._resolve_date_container(panel)
        state = self._read_state(flow, container)
        return bool(self._mode_matches(state, target_mode) and self._period_matches(state, target_period))

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"date_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        diagnostics = {"reason": reason, **(extra or {})}
        diagnostics.update(self._last_diag)
        ctx.diagnostics = diagnostics
        return ctx
