"""UTM source filter handler (exact/prefix)."""

from __future__ import annotations

from typing import Any

from .base import FilterDebugContext


class UTMFilterHandler:
    def __init__(self, mode: str = "exact") -> None:
        self.mode = mode
        self.name = f"utm_{mode}"

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> dict[str, Any]:
        panel = flow._find_filter_panel_container(page)
        return {"panel": panel, "values": [str(v).strip() for v in values if str(v).strip()], "operator": operator}

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        ctx = self.resolve(flow, page, report_id, values, operator=operator)
        vals = ctx["values"]
        if not vals:
            return False
        if self.mode == "prefix" and operator == "^=":
            flow.logger.warning("UTM prefix mode uses UI best-effort exact entry for prefix value=%s", vals[0])

        panel = ctx["panel"]
        holder = flow._find_utm_source_holder(panel)
        if holder is None:
            label_item, _payload = flow._find_utm_label_item(panel)
            if label_item is not None:
                activated = False
                try:
                    label_item.click(timeout=1200)
                    activated = True
                except Exception:
                    activated = False
                flow.logger.info("utm_filter_kind_activation=label_item_click")
                flow.logger.info("utm_filter_kind_activation_success=%s", str(bool(activated)).lower())
                page.wait_for_timeout(220)

            panel = flow._find_filter_panel_container(page)
            holder = flow._find_utm_source_holder(panel)

        flow._apply_utm_source_exact_values(page=page, panel=panel, values=vals, report_id=report_id)
        return True

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        panel = flow._find_filter_panel_container(page)
        target = next((str(v).strip() for v in values if str(v).strip()), "")
        if not target:
            return False

        row_container = None
        try:
            row_container, _target, _payload = flow._resolve_utm_row_context(page=page, panel=panel, report_id=report_id)
        except Exception:
            row_container = None

        if row_container is not None:
            try:
                mode = flow._detect_utm_control_mode(row_container)
            except Exception:
                mode = "unknown"
            if mode == "direct_text_input":
                direct_input = flow._find_utm_direct_input(row_container)
                if direct_input is not None:
                    current = flow._read_input_value(direct_input)
                    if flow._normalize_filter_text(current) == flow._normalize_filter_text(target):
                        flow.logger.info("utm_verify_success=direct_input_value")
                        return True

        holder = flow._find_utm_source_holder(panel)
        if holder is not None:
            if flow._has_utm_chip(panel, holder, target):
                return True

        # Fallback verification when holder is unavailable due panel re-render.
        target_norm = flow._normalize_filter_text(target)
        chip_texts = flow._collect_tag_chip_texts(panel, holder=holder)
        if any(target_norm in flow._normalize_filter_text(item) for item in chip_texts):
            flow.logger.info("utm_verify_success=panel_chip_fallback")
            return True

        try:
            panel_text = panel.inner_text(timeout=300)
        except Exception:
            panel_text = ""
        if target_norm and target_norm in flow._normalize_filter_text(panel_text):
            flow.logger.info("utm_verify_success=panel_text_fallback")
            return True

        return False

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ctx = FilterDebugContext()
        shot = flow._debug_screenshot(page, f"utm_filter_failed_{report_id}")
        if shot:
            ctx.artifacts["screenshot"] = str(shot)
        ctx.diagnostics = {"reason": reason, "mode": self.mode, **(extra or {})}
        return ctx
