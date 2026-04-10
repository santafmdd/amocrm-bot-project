"""Per-scenario amoCRM execution for layout DSL blocks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.sync_api import Page
    from src.browser.analytics_flow import AnalyticsFlow
else:
    Page = Any
    AnalyticsFlow = Any

from src.browser.models import AnalyticsSnapshot, SourceKind
from src.writers.compiler import compile_profile_analytics_result
from src.writers.layout_filter_dsl import (
    LayoutBlockConfig,
    LayoutScenario,
    ScenarioRunResult,
    normalize_field_name,
    select_best_scenario,
)
from src.writers.models import CompiledProfileAnalyticsResult


@dataclass(frozen=True)
class _ReadProfile:
    report_id: str
    source_kind: SourceKind
    filter_values: list[str]
    tabs: list[str]
    tag_selection_mode: str


@dataclass(frozen=True)
class ScenarioExecutionResult:
    scenario_index: int
    raw_text: str
    normalized_filters: list[dict[str, Any]]
    success: bool
    error: str
    snapshots: list[AnalyticsSnapshot]
    total_count: int
    non_empty_stage_rows: int


@dataclass(frozen=True)
class BlockExecutionResult:
    block_display_name: str
    scenario_results: list[ScenarioExecutionResult]
    best_scenario: ScenarioExecutionResult | None
    best_compiled_result: CompiledProfileAnalyticsResult | None


class ScenarioExecutor:
    """Execute each DSL scenario via real amoCRM UI and select best result."""

    def __init__(
        self,
        flow: AnalyticsFlow,
        project_root: Path,
        tabs: list[str],
        report_id: str,
    ) -> None:
        self.flow = flow
        self.project_root = project_root
        self.tabs = [t for t in tabs if t in {"all", "active", "closed"}] or ["all", "active", "closed"]
        self.report_id = report_id
        self.logger = logging.getLogger("project")

    def execute_block_scenarios(
        self,
        page: Page,
        block_config: LayoutBlockConfig,
    ) -> BlockExecutionResult:
        scenario_results: list[ScenarioExecutionResult] = []

        for idx, scenario in enumerate(block_config.scenarios):
            self.logger.info(
                "scenario execution start: block=%s scenario_index=%s raw=%s",
                block_config.display_name,
                idx,
                scenario.raw_text,
            )
            result = self._execute_one_scenario(page, block_config.display_name, idx, scenario)
            scenario_results.append(result)

        successful = [r for r in scenario_results if r.success]
        if not successful:
            self.logger.error("all scenarios failed for block=%s", block_config.display_name)
            return BlockExecutionResult(
                block_display_name=block_config.display_name,
                scenario_results=scenario_results,
                best_scenario=None,
                best_compiled_result=None,
            )

        scored = [
            ScenarioRunResult(
                scenario_index=item.scenario_index,
                success=item.success,
                total_count=item.total_count,
                non_empty_stage_rows=item.non_empty_stage_rows,
            )
            for item in successful
        ]
        best_score = select_best_scenario(scored)
        best_scenario = next(x for x in successful if x.scenario_index == best_score.scenario_index)

        source_kind = self._infer_source_kind_for_scenario(block_config.scenarios[best_scenario.scenario_index])
        filter_values = self._extract_primary_values(block_config.scenarios[best_scenario.scenario_index], source_kind)
        compiled = compile_profile_analytics_result(
            report=type("_TmpReport", (), {"id": self.report_id, "display_name": block_config.display_name})(),
            source_kind=source_kind,
            filter_values=filter_values,
            snapshots=best_scenario.snapshots,
        )

        self.logger.info(
            "selected_best_scenario: block=%s scenario_index=%s total_count=%s non_empty_stage_rows=%s",
            block_config.display_name,
            best_scenario.scenario_index,
            best_scenario.total_count,
            best_scenario.non_empty_stage_rows,
        )

        return BlockExecutionResult(
            block_display_name=block_config.display_name,
            scenario_results=scenario_results,
            best_scenario=best_scenario,
            best_compiled_result=compiled,
        )

    def _execute_one_scenario(
        self,
        page: Page,
        block_display_name: str,
        scenario_index: int,
        scenario: LayoutScenario,
    ) -> ScenarioExecutionResult:
        normalized_filters = self._normalize_filters_for_log(scenario)
        try:
            source_kind = self._infer_source_kind_for_scenario(scenario)
            values = self._extract_primary_values(scenario, source_kind)

            self._reset_to_clean_state(page)
            self.flow.reader.open_analytics_page(page)
            self.flow._open_filter_panel(page)
            self.flow._apply_already_confirmed = False

            self._apply_non_primary_filters(page, scenario, source_kind)

            self.flow._select_filter_kind(page, source_kind, self.report_id)
            self.flow._apply_filter_values(page, self.report_id, source_kind, values)
            self.flow._click_apply(page)
            self.flow._wait_after_apply(page)

            final_url = str(page.url)
            self.logger.info(
                "scenario filter apply success: block=%s scenario_index=%s final_url=%s",
                block_display_name,
                scenario_index,
                final_url,
            )

            snapshots = self._capture_tabs_from_current_view(page, source_kind, scenario_index)
            total = 0
            non_empty = 0
            for snap in snapshots:
                if snap.tab_mode == "all":
                    total = int(snap.total_count)
                non_empty += sum(1 for st in snap.stages if int(st.count) > 0)

            self._save_scenario_debug_result(block_display_name, scenario_index, snapshots, normalized_filters, "")
            self.logger.info(
                "scenario result: block=%s scenario_index=%s total_count=%s stages_count=%s non_empty_stage_rows=%s",
                block_display_name,
                scenario_index,
                total,
                len(snapshots[0].stages) if snapshots else 0,
                non_empty,
            )

            return ScenarioExecutionResult(
                scenario_index=scenario_index,
                raw_text=scenario.raw_text,
                normalized_filters=normalized_filters,
                success=True,
                error="",
                snapshots=snapshots,
                total_count=total,
                non_empty_stage_rows=non_empty,
            )
        except Exception as exc:
            error = str(exc)
            self.logger.error(
                "scenario failed: block=%s scenario_index=%s reason=%s",
                block_display_name,
                scenario_index,
                error,
            )
            try:
                self.flow._debug_screenshot(page, f"scenario_failed_{self.report_id}_{scenario_index}")
            except Exception:
                pass
            self._save_scenario_debug_result(block_display_name, scenario_index, [], normalized_filters, error)
            return ScenarioExecutionResult(
                scenario_index=scenario_index,
                raw_text=scenario.raw_text,
                normalized_filters=normalized_filters,
                success=False,
                error=error,
                snapshots=[],
                total_count=0,
                non_empty_stage_rows=0,
            )

    def _capture_tabs_from_current_view(self, page: Page, source_kind: SourceKind, scenario_index: int) -> list[AnalyticsSnapshot]:
        base_url = str(page.url)
        snapshots: list[AnalyticsSnapshot] = []
        for tab in self.tabs:
            target_url = self.flow.reader.build_tab_mode_url(base_url, tab)  # type: ignore[arg-type]
            page.goto(target_url, wait_until="domcontentloaded")
            ready = self.flow._wait_for_tab_content_ready(page, tab)  # type: ignore[arg-type]
            profile = _ReadProfile(
                report_id=f"{self.report_id}_scn{scenario_index}",
                source_kind=source_kind,
                filter_values=[],
                tabs=[tab],
                tag_selection_mode=self.flow.tag_selection_mode,
            )
            snapshot = self.flow._read_tab_with_stability_retries(page, profile, tab, precheck_ready=ready)  # type: ignore[arg-type]
            snapshots.append(snapshot)
        return snapshots

    def _reset_to_clean_state(self, page: Page) -> None:
        # Minimal robust reset strategy for scenario isolation.
        self.flow.reader.open_analytics_page(page)
        page.wait_for_timeout(300)

    def _normalize_filters_for_log(self, scenario: LayoutScenario) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for f in scenario.filters:
            out.append(
                {
                    "raw_field_name": f.raw_field_name,
                    "normalized_field_name": f.normalized_field_name,
                    "operator": f.operator,
                    "values": list(f.values),
                    "raw_text": f.raw_text,
                }
            )
        return out

    def _infer_source_kind_for_scenario(self, scenario: LayoutScenario) -> SourceKind:
        has_tags = any(f.normalized_field_name == "tags" for f in scenario.filters)
        has_utm = any(f.normalized_field_name == "utm_source" for f in scenario.filters)
        if has_tags:
            return "tag"
        if has_utm:
            return "utm_source"
        raise RuntimeError("Scenario has no source filter (tags or utm_source).")

    def _extract_primary_values(self, scenario: LayoutScenario, source_kind: SourceKind) -> list[str]:
        key = "tags" if source_kind == "tag" else "utm_source"
        for f in scenario.filters:
            if f.normalized_field_name == key:
                values = [str(v).strip() for v in f.values if str(v).strip()]
                if not values:
                    raise RuntimeError(f"Scenario source filter {key} has empty values")
                return values
        raise RuntimeError(f"Scenario source filter not found: {key}")

    def _apply_non_primary_filters(self, page: Page, scenario: LayoutScenario, primary_kind: SourceKind) -> None:
        # Apply additional supported filters before primary source filter.
        combined_dates_mode: list[str] = []
        combined_period: list[str] = []
        combined_date_from: list[str] = []
        combined_date_to: list[str] = []

        for f in scenario.filters:
            field = normalize_field_name(f.normalized_field_name)
            if field == "tags" and primary_kind == "tag":
                continue
            if field == "utm_source" and primary_kind == "utm_source":
                continue
            if field == "utm_source" and primary_kind == "tag":
                op = "^=" if f.operator == "^=" else "="
                key = "utm_prefix" if op == "^=" else "utm_source"
                ok = self.flow._apply_supported_filter(page, self.report_id, key, list(f.values), operator=op)
                self.logger.info(
                    "scenario ui control selected: field=utm_source strategy=handler key=%s ok=%s",
                    key,
                    str(bool(ok)).lower(),
                )
                continue

            if field == "pipeline":
                self.flow._apply_supported_filter(page, self.report_id, "pipeline", list(f.values), operator=f.operator)
                continue

            if field == "dates_mode":
                combined_dates_mode = list(f.values)
                continue

            if field == "period":
                combined_period = list(f.values)
                continue

            if field == "date_from":
                combined_date_from = list(f.values)
                continue

            if field == "date_to":
                combined_date_to = list(f.values)
                continue

            if field == "manager":
                self.flow._apply_supported_filter(page, self.report_id, "manager", list(f.values), operator=f.operator)
                continue

            if field == "tags" and primary_kind == "utm_source":
                self.flow._apply_supported_filter(page, self.report_id, "tag", list(f.values), operator=f.operator)
                continue

            self.logger.warning("Unsupported or out-of-scope DSL filter in scenario execution: field=%s", field)

        if combined_dates_mode or combined_period or combined_date_from or combined_date_to:
            date_values: list[str] = []
            if combined_dates_mode:
                date_values.append(str(combined_dates_mode[0]))
            if combined_period:
                date_values.append(str(combined_period[0]))
            if combined_date_from:
                date_values.append(str(combined_date_from[0]))
            if combined_date_to:
                date_values.append(str(combined_date_to[0]))
            self.flow._apply_supported_filter(page, self.report_id, "date", date_values, operator="=")

    def _apply_tags_before_primary(self, page: Page, values: list[str]) -> None:
        self.flow._select_filter_kind(page, "tag", self.report_id)
        panel = self.flow._find_filter_panel_container(page)
        focus_target, _strategy = self.flow._focus_tag_field_via_row(panel, self.report_id)
        if focus_target is None:
            focus_target = self.flow._scroll_until_tag_input_visible(panel, self.report_id)

        for value in [v.strip() for v in values if str(v).strip()]:
            focus_target.click(timeout=1200)
            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            page.keyboard.type(value, delay=20)
            page.wait_for_timeout(200)
            page.keyboard.press("Enter")
            page.wait_for_timeout(200)
            self.logger.info("scenario tag value selected (pre-primary): %s", value)

        page.keyboard.press("Escape")
        page.wait_for_timeout(200)

    def _apply_pipeline_filter(self, page: Page, values: list[str], operator: str) -> None:
        target = next((str(v).strip() for v in values if str(v).strip()), "")
        if not target:
            return
        panel = self.flow._find_filter_panel_container(page)

        row = self._find_filter_row_by_tokens(panel, primary_tokens=["Воронка"], fallback_value=target)
        strategy = "row_by_label" if row is not None else "panel_dropdown_fallback"
        if row is None:
            row = panel

        opened = self._open_dropdown_from_row(page, row)
        if not opened:
            self.logger.warning("Could not open pipeline dropdown for value=%s", target)
            return

        selected = self._choose_option_text(page, target)
        current_value = self._row_preview_text(row)
        self.logger.info(
            "scenario ui control selected: field=Воронка strategy=%s operator=%s target=%s selected=%s current_value=%s",
            strategy,
            operator,
            target,
            str(selected).lower(),
            current_value or "<empty>",
        )
        if not selected:
            self.logger.warning("Pipeline value not selected: value=%s", target)

    def _apply_dates_period_filter(
        self,
        page: Page,
        dates_mode: list[str],
        period: list[str],
        date_from: list[str],
        date_to: list[str],
    ) -> None:
        mode_value = next((str(v).strip() for v in dates_mode if str(v).strip()), "")
        period_value = next((str(v).strip() for v in period if str(v).strip()), "")
        from_value = next((str(v).strip() for v in date_from if str(v).strip()), "")
        to_value = next((str(v).strip() for v in date_to if str(v).strip()), "")

        panel = self.flow._find_filter_panel_container(page)
        row = self._find_filter_row_by_tokens(
            panel,
            primary_tokens=["Даты", "Период"],
            fallback_value=period_value or mode_value,
        )
        strategy = "shared_dates_period_control" if row is not None else "panel_dropdown_fallback"
        if row is None:
            row = panel

        opened = self._open_dropdown_from_row(page, row)
        if not opened:
            self.logger.warning("Could not open dates/period dropdown")
            return

        mode_selected = True
        if mode_value:
            mode_selected = self._choose_option_text(page, mode_value)
            if mode_selected and period_value:
                # Re-open same control because amoCRM often closes menu after first selection.
                self._open_dropdown_from_row(page, row)

        period_selected = True
        if period_value:
            period_selected = self._choose_option_text(page, period_value)

        if from_value or to_value:
            self.logger.warning(
                "DSL fields 'С/По' are parsed but direct date-range picker automation is not implemented yet. from=%s to=%s",
                from_value or "<empty>",
                to_value or "<empty>",
            )

        current_value = self._row_preview_text(row)
        self.logger.info(
            "scenario ui control selected: field=Даты/Период strategy=%s mode=%s mode_selected=%s period=%s period_selected=%s current_value=%s",
            strategy,
            mode_value or "<empty>",
            str(mode_selected).lower(),
            period_value or "<empty>",
            str(period_selected).lower(),
            current_value or "<empty>",
        )

    def _find_filter_row_by_tokens(self, panel: Any, primary_tokens: list[str], fallback_value: str = "") -> Any | None:
        for token in primary_tokens:
            try:
                row = panel.locator(f"*:has-text('{token}')").first
                if row.count() > 0 and row.is_visible(timeout=700):
                    return row
            except Exception:
                continue

        if fallback_value:
            try:
                row = panel.locator(f"*:has-text('{fallback_value}')").first
                if row.count() > 0 and row.is_visible(timeout=700):
                    return row
            except Exception:
                pass
        return None

    def _open_dropdown_from_row(self, page: Page, row: Any) -> bool:
        selectors = [
            "[class*='select']",
            "[class*='control--select']",
            "[role='combobox']",
            "button",
            "div",
            "span",
        ]
        for selector in selectors:
            try:
                candidate = row.locator(selector).first
                if candidate.count() <= 0:
                    continue
                if not candidate.is_visible(timeout=500):
                    continue
                candidate.click(timeout=1200)
                page.wait_for_timeout(180)
                return True
            except Exception:
                continue
        try:
            row.click(timeout=1200)
            page.wait_for_timeout(180)
            return True
        except Exception:
            return False

    def _choose_option_text(self, page: Page, target: str) -> bool:
        selectors = [
            f"li:has-text('{target}')",
            f"[role='option']:has-text('{target}')",
            f"[class*='select'] li:has-text('{target}')",
            f"[class*='dropdown'] *:has-text('{target}')",
            f"*:has-text('{target}')",
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = min(locator.count(), 8)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=500):
                        continue
                    txt = (item.inner_text(timeout=500) or "").strip()
                    if target not in txt:
                        continue
                    item.click(timeout=1200)
                    page.wait_for_timeout(150)
                    return True
                except Exception:
                    continue
        return False

    def _apply_generic_field_by_label(self, page: Page, field: str, values: list[str], operator: str) -> None:
        panel = self.flow._find_filter_panel_container(page)
        label_map = {
            "pipeline": ["Воронка", "PIPELINE"],
            "period": ["Период", "PERIOD"],
            "dates_mode": ["Даты", "DATES"],
        }
        labels = label_map.get(field, [])
        selected = False
        for label in labels:
            try:
                row = panel.locator(f"*:has-text('{label}')").first
                if row.count() > 0:
                    row.click(timeout=1200)
                    selected = True
                    break
            except Exception:
                continue

        if not selected:
            self.logger.warning("Generic filter label not found for field=%s (values=%s)", field, values)
            return

        # Best-effort value input/select.
        value = next((str(v).strip() for v in values if str(v).strip()), "")
        if not value:
            return

        try:
            input_locator = panel.locator("input[type='text'], input:not([type]), [contenteditable='true']").first
            input_locator.click(timeout=1200)
            try:
                input_locator.fill(value, timeout=1200)
            except Exception:
                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.keyboard.type(value, delay=20)
            page.keyboard.press("Enter")
            self.logger.info(
                "Generic filter applied: field=%s operator=%s value=%s",
                field,
                operator,
                value,
            )
        except Exception as exc:
            self.logger.warning(
                "Could not apply generic field=%s operator=%s value=%s: %s",
                field,
                operator,
                value,
                exc,
            )

    def _row_preview_text(self, row: Any) -> str:
        try:
            text = (row.inner_text(timeout=600) or "").strip()
            return " ".join(text.split())[:160]
        except Exception:
            return ""

    def _save_scenario_debug_result(
        self,
        block_display_name: str,
        scenario_index: int,
        snapshots: list[AnalyticsSnapshot],
        normalized_filters: list[dict[str, Any]],
        error: str,
    ) -> None:
        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        safe_block = "".join(ch for ch in block_display_name.lower() if ch.isalnum() or ch in ("_", "-"))[:40] or "block"
        out = debug_dir / f"scenario_result_{self.report_id}_{safe_block}_{scenario_index}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        payload = {
            "report_id": self.report_id,
            "block_display_name": block_display_name,
            "scenario_index": scenario_index,
            "normalized_filters": normalized_filters,
            "error": error,
            "snapshots": [s.model_dump() for s in snapshots],
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
