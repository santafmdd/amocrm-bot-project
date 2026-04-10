"""Pipeline filter handler."""

from __future__ import annotations

from typing import Any

from .base import FilterDebugContext


class PipelineFilterHandler:
    name = "pipeline"

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        target = next((str(v).strip() for v in values if str(v).strip()), "")
        return {"panel": panel, "target": target, "operator": operator}

    def _find_row(self, flow: Any, panel: Any, target: str):
        candidates = panel.locator("div, li, section, label")
        try:
            count = min(candidates.count(), 200)
        except Exception:
            return None
        for idx in range(count):
            row = candidates.nth(idx)
            try:
                if not row.is_visible(timeout=80):
                    continue
                txt = row.inner_text(timeout=80).strip()
            except Exception:
                continue
            if not txt:
                continue
            lowered = txt.lower()
            if "\u0432\u043e\u0440\u043e\u043d\u043a\u0430" in lowered or (target and target.lower() in lowered):
                return row
        return None

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        ctx = self.resolve(flow, page, report_id, values, operator=operator)
        target = ctx["target"]
        if not target:
            return False
        row = self._find_row(flow, ctx["panel"], target)
        if row is None:
            return False
        for sel in ("[role='combobox']", "[class*='select']", "button", "div"):
            try:
                loc = row.locator(sel)
                if loc.count() > 0 and loc.first.is_visible(timeout=120):
                    loc.first.click(timeout=800)
                    break
            except Exception:
                continue
        page.wait_for_timeout(150)
        return bool(flow._choose_option_text(page, target))

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        target = next((str(v).strip() for v in values if str(v).strip()), "")
        if not target:
            return False
        try:
            txt = panel.inner_text(timeout=400)
        except Exception:
            txt = ""
        return target.lower() in txt.lower()

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"pipeline_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        ctx.diagnostics = {"reason": reason, **(extra or {})}
        return ctx
