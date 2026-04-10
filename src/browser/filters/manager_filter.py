"""Manager filter handler."""

from __future__ import annotations

from typing import Any

from .base import FilterDebugContext


class ManagerFilterHandler:
    name = "manager"

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        vals = [str(v).strip() for v in values if str(v).strip()]
        return {"panel": panel, "values": vals, "operator": operator}

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        ctx = self.resolve(flow, page, report_id, values, operator=operator)
        vals = ctx["values"]
        if not vals:
            return False
        panel = ctx["panel"]
        try:
            row = panel.locator("*:has-text('\\u041c\\u0435\\u043d\\u0435\\u0434\\u0436\\u0435\\u0440'), *:has-text('\\u041e\\u0442\\u0432\\u0435\\u0442\\u0441\\u0442\\u0432\\u0435\\u043d\\u043d\\u044b\\u0439')").first
            row.click(timeout=1000)
        except Exception:
            return False
        page.wait_for_timeout(150)
        success = True
        for value in vals:
            page.keyboard.type(value, delay=15)
            page.wait_for_timeout(120)
            chosen = flow._choose_option_text(page, value)
            success = success and bool(chosen)
        page.keyboard.press("Escape")
        return bool(success)

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        try:
            txt = panel.inner_text(timeout=400)
        except Exception:
            txt = ""
        lowered = txt.lower()
        checks = [str(v).strip().lower() for v in values if str(v).strip()]
        return bool(checks) and any(v in lowered for v in checks)

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"manager_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        ctx.diagnostics = {"reason": reason, **(extra or {})}
        return ctx
