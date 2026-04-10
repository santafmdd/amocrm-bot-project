"""Date/period filter handler."""

from __future__ import annotations

from typing import Any

from .base import FilterDebugContext


class DateFilterHandler:
    name = "date"

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        clean = [str(v).strip() for v in values if str(v).strip()]
        return {"panel": panel, "values": clean, "operator": operator}

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        ctx = self.resolve(flow, page, report_id, values, operator=operator)
        vals = ctx["values"]
        if not vals:
            return False
        panel = ctx["panel"]
        target_mode = vals[0]
        target_period = vals[1] if len(vals) > 1 else ""

        row = panel.locator("*:has-text('\\u0414\\u0430\\u0442\\u044b'), *:has-text('\\u041f\\u0435\\u0440\\u0438\\u043e\\u0434')").first
        try:
            row.click(timeout=1000)
        except Exception:
            try:
                panel.click(timeout=600)
            except Exception:
                pass
        page.wait_for_timeout(150)

        ok_mode = flow._choose_option_text(page, target_mode)
        ok_period = True
        if target_period:
            ok_period = flow._choose_option_text(page, target_period)
        return bool(ok_mode and ok_period)

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        try:
            txt = panel.inner_text(timeout=400)
        except Exception:
            txt = ""
        checks = [str(v).strip().lower() for v in values if str(v).strip()]
        if not checks:
            return False
        lowered = txt.lower()
        return all(v in lowered for v in checks[:2])

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"date_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        ctx.diagnostics = {"reason": reason, **(extra or {})}
        return ctx
