"""Profile-driven analytics browser flow for read/report automation."""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Locator, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.browser.amo_reader import AmoAnalyticsReader
from src.browser.models import AnalyticsSnapshot, SourceKind, TabMode
from src.browser.analytics_selectors import (
    APPLY_SELECTORS,
    FILTER_INPUT_SELECTORS,
    FILTER_KIND_LABELS,
    FILTER_OPEN_SELECTORS,
    FILTER_PANEL_CONTAINER_SELECTORS,
    FILTER_PANEL_SCROLLABLE_SELECTORS,
    FILTER_PANEL_SELECTOR_DIAGNOSTICS,
    TAG_PLACEHOLDER_SELECTORS,
    TAG_REFERENCE_INPUT_SELECTOR,
    TARGET_PANEL_LABELS,
    UTM_HOLDER_SELECTORS,
    UTM_INPUT_SELECTORS,
)
from src.browser.analytics_debug import (
    debug_dir as analytics_debug_dir,
    debug_screenshot as analytics_debug_screenshot,
    save_debug_text as analytics_save_debug_text,
    save_external_agent_handoff_context as analytics_save_external_agent_handoff_context,
    save_tag_holder_after_enter_artifacts as analytics_save_tag_holder_after_enter_artifacts,
    save_tag_input_resolution_debug as analytics_save_tag_input_resolution_debug,
    save_utm_click_debug_artifacts as analytics_save_utm_click_debug_artifacts,
)
from src.browser.filters import FilterRegistry, tag_filter
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class AnalyticsFlowInput:
    """Normalized input for analytics profile flow."""

    report_id: str
    source_kind: SourceKind
    filter_values: list[str]
    tabs: list[TabMode]
    tag_selection_mode: str = "script"
    filter_operator: str = "="


class AnalyticsFlow:
    """Automates analytics open -> filter setup -> capture flow (read/report safe)."""
    TAB_READY_DEALS_PATTERN = re.compile(r"\d+\s*\u0441\u0434\u0435\u043b", flags=re.IGNORECASE)
    TAB_READY_STAGE_NAMES: tuple[str, ...] = (
        "\u041d\u0435\u0440\u0430\u0437\u043e\u0431\u0440\u0430\u043d\u043d\u043e\u0435",
        "\u0412\u0415\u0420\u0418\u0424\u0418\u041a\u0410\u0426\u0418\u042f",
        "\u041f\u0415\u0420\u0412\u042b\u0419 \u041a\u041e\u041d\u0422\u0410\u041a\u0422. \u041a\u0412\u0410\u041b\u0418\u0424\u0418\u041a\u0410\u0426\u0418\u042f",
        "\u0415\u0421\u0422\u042c \u0418\u041d\u0422\u0415\u0420\u0415\u0421 \u041a \u041f\u0420\u041e\u0414\u0423\u041a\u0422\u0423",
        "\u041f\u0420\u041e\u0412\u0415\u0414\u0415\u041d\u0410 \u0414\u0415\u041c\u041e\u041d\u0421\u0422\u0420\u0410\u0426\u0418\u042f",
        "\u0442\u0435\u0441\u0442\u0438\u0440\u043e\u0432\u0430\u043d\u0438\u0435 \u0441\u0438\u0441\u0442\u0435\u043c\u044b",
        "\u041f\u0440\u0438\u043d\u0438\u043c\u0430\u044e\u0442 \u0440\u0435\u0448\u0435\u043d\u0438\u0435",
        "\u0432\u044b\u0441\u0442\u0430\u0432\u043b\u0435\u043d\u043e \u043a\u043f",
        "\u043e\u043f\u043b\u0430\u0442\u0430",
        "\u0423\u0441\u043f\u0435\u0448\u043d\u043e \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e",
        "\u0417\u0430\u043a\u0440\u044b\u0442\u043e \u0438 \u043d\u0435 \u0440\u0435\u0430\u043b\u0438\u0437\u043e\u0432\u0430\u043d\u043e",
    )
    TAB_READY_FORBIDDEN_LABELS: tuple[str, ...] = (
        "\u0421\u0435\u0433\u043e\u0434\u043d\u044f",
        "5 \u0434\u043d\u0435\u0439",
        "10 \u0434\u043d\u0435\u0439",
        "15 \u0434\u043d\u0435\u0439",
        "\u0421\u0440\u0435\u0434\u043d\u0435\u0435 \u0432\u0440\u0435\u043c\u044f \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0438 \u0437\u0430\u044f\u0432\u043a\u0438",
        "\u041f\u0440\u043e\u0433\u043d\u043e\u0437 \u043f\u0440\u043e\u0434\u0430\u0436",
    )
    TAB_READY_FORBIDDEN_DAY_PATTERN = re.compile(
        r"^\d+\s*(?:\u0434(?:\u043d(?:\u0435\u0439)?)?\.?)?$",
        flags=re.IGNORECASE,
    )
    TAB_READY_MIN_RIGHT_LINES = 15
    TAB_READY_MIN_VALID_STAGE_NAMES = 5
    TAB_READY_MIN_STRUCTURED_STAGES = 8
    TAB_READY_MIN_STRUCTURED_DEALS_HITS = 2
    TAB_READY_MIN_STRUCTURED_KNOWN_STAGE_NAMES = 3
    TAB_READY_CONSECUTIVE_REQUIRED = 2

    # Temporary pragmatic fix for current amoCRM account/profile.
    # Later this can be generalized via filter field mapping/registry.
    TAG_REFERENCE_INPUT_SELECTOR = TAG_REFERENCE_INPUT_SELECTOR

    FILTER_OPEN_SELECTORS: tuple[str, ...] = FILTER_OPEN_SELECTORS

    APPLY_SELECTORS: tuple[str, ...] = APPLY_SELECTORS

    FILTER_INPUT_SELECTORS: tuple[str, ...] = FILTER_INPUT_SELECTORS

    FILTER_KIND_LABELS: dict[SourceKind, tuple[str, ...]] = FILTER_KIND_LABELS  # type: ignore[assignment]

    TAG_PLACEHOLDER_SELECTORS: tuple[str, ...] = TAG_PLACEHOLDER_SELECTORS

    UTM_HOLDER_SELECTORS: tuple[str, ...] = UTM_HOLDER_SELECTORS

    UTM_INPUT_SELECTORS: tuple[str, ...] = UTM_INPUT_SELECTORS

    FILTER_PANEL_CONTAINER_SELECTORS: tuple[str, ...] = FILTER_PANEL_CONTAINER_SELECTORS

    FILTER_PANEL_SCROLLABLE_SELECTORS: tuple[str, ...] = FILTER_PANEL_SCROLLABLE_SELECTORS

    FILTER_PANEL_SELECTOR_DIAGNOSTICS: tuple[str, ...] = FILTER_PANEL_SELECTOR_DIAGNOSTICS

    TARGET_PANEL_LABELS: tuple[str, ...] = TARGET_PANEL_LABELS

    def __init__(
        self,
        reader: AmoAnalyticsReader,
        project_root: Path,
        tag_selection_mode: str = "script",
        external_agent_bridge_cmd: str | None = None,
        external_agent_bridge_timeout_sec: int = 180,
    ) -> None:
        self.reader = reader
        self.project_root = project_root
        self.logger = logging.getLogger("project")
        normalized_mode = str(tag_selection_mode).strip().lower()
        if normalized_mode not in {"script", "agent_assisted", "external_agent"}:
            normalized_mode = "script"
        self.tag_selection_mode = normalized_mode
        self.external_agent_bridge_cmd = (external_agent_bridge_cmd or "").strip()
        self.external_agent_bridge_timeout_sec = max(10, int(external_agent_bridge_timeout_sec))
        self._apply_already_confirmed = False
        self._filter_registry = FilterRegistry()

    def run_profile_capture(self, page: Page, profile: AnalyticsFlowInput) -> list[AnalyticsSnapshot]:
        """Run full profile-driven flow and return captured snapshots."""
        if profile.tag_selection_mode not in {"script", "agent_assisted", "external_agent"}:
            self.logger.warning("Unknown tag_selection_mode=%s, fallback to script", profile.tag_selection_mode)
            profile = AnalyticsFlowInput(
                report_id=profile.report_id,
                source_kind=profile.source_kind,
                filter_values=profile.filter_values,
                tabs=profile.tabs,
                tag_selection_mode="script",
                filter_operator=profile.filter_operator,
            )

        self.logger.info("Tag selection mode: %s", profile.tag_selection_mode)
        self.tag_selection_mode = profile.tag_selection_mode

        self.reader.open_analytics_page(page)
        self._debug_screenshot(page, f"profile_{profile.report_id}_01_opened")

        self._log_filter_diagnostics(page)
        self._open_filter_panel(page)
        self._debug_screenshot(page, f"profile_{profile.report_id}_02_filter_opened")

        self._apply_already_confirmed = False
        self._select_filter_kind(page, profile.source_kind, profile.report_id)
        self._apply_filter_values(
            page,
            profile.report_id,
            profile.source_kind,
            profile.filter_values,
            operator=profile.filter_operator,
        )
        self._click_apply(page)
        self._wait_after_apply(page)
        self._debug_screenshot(page, f"profile_{profile.report_id}_03_filter_applied")

        base_url = page.url
        if not base_url.strip():
            raise RuntimeError("Profile flow failed: page URL is empty after filter apply.")

        self.logger.info("Profile flow base URL after filter apply: %s", base_url)

        snapshots: list[AnalyticsSnapshot] = []
        for tab_mode in profile.tabs:
            target_url = self.reader.build_tab_mode_url(base_url, tab_mode)
            self.logger.info("Profile tab URL tab=%s: %s", tab_mode, target_url)

            page.goto(target_url, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("load", timeout=min(self.reader.settings.timeout_ms, 8000))
            except PlaywrightTimeoutError as exc:
                self.logger.warning(
                    "Soft wait timeout in profile flow tab=%s url=%s: %s",
                    tab_mode,
                    target_url,
                    exc,
                )

            precheck_ready = self._wait_for_tab_content_ready(page, tab_mode)
            snapshot = self._read_tab_with_stability_retries(page, profile, tab_mode, precheck_ready=precheck_ready)
            snapshots.append(snapshot)
            self.logger.info(
                "Profile tab captured: tab=%s stages=%s total_count=%s parse_method=%s",
                snapshot.tab_mode,
                len(snapshot.stages),
                snapshot.total_count,
                snapshot.parse_method,
            )

        return snapshots

    def _assess_tab_content_ready(self, page: Page) -> dict[str, object]:
        # Primary signal: DOM rows from pipeline block.
        pipeline_row_count = self.reader.get_pipeline_row_count(page)
        right_lines = self.reader.extract_pipeline_stage_lines(page)

        deals_hits = sum(1 for line in right_lines if self.reader.is_deals_line(line))
        parsed_stages = self.reader._parse_right_panel_from_lines(right_lines)

        forbidden_hits: list[str] = []
        valid_stage_like_names: list[str] = []
        for stage in parsed_stages:
            stage_name = stage.stage_name.strip()
            if self.reader._is_forbidden_bucket_label(stage_name):
                if len(forbidden_hits) < 10:
                    forbidden_hits.append(stage_name)
                continue
            if len(valid_stage_like_names) < 20:
                valid_stage_like_names.append(stage_name)

        right_text_lower = "\n".join(right_lines).lower()
        has_processing_widget = "СЃСЂРµРґРЅРµРµ РІСЂРµРјСЏ РѕР±СЂР°Р±РѕС‚РєРё Р·Р°СЏРІРєРё" in right_text_lower
        valid_stage_like_count = len(valid_stage_like_names)
        right_count = len(right_lines)

        known_stage_hits = [
            name
            for name in self.TAB_READY_STAGE_NAMES
            if any(stage.stage_name.strip().lower() == name.lower() for stage in parsed_stages)
        ]

        dominated_by_forbidden = (
            len(parsed_stages) > 0 and len(forbidden_hits) >= max(2, len(parsed_stages) - 1)
        )

        dom_ready = (
            pipeline_row_count >= 3
            and right_count >= self.TAB_READY_MIN_RIGHT_LINES
            and deals_hits >= 1
            and valid_stage_like_count >= self.TAB_READY_MIN_VALID_STAGE_NAMES
            and not dominated_by_forbidden
            and not has_processing_widget
        )

        # Fallback signal: structured slice from current visible text.
        structured_right_lines_count = 0
        structured_stages_count = 0
        structured_deals_hits = 0
        structured_valid_stage_count = 0
        structured_known_stage_count = 0
        structured_weak_reasons: list[str] = []
        structured_ready = False
        try:
            body_text = page.locator("body").inner_text(timeout=min(self.reader.settings.timeout_ms, 2500))
            lines = [line.strip() for line in body_text.splitlines() if line.strip()]
            structured_right_lines, _start_found, _stop_found = self.reader._slice_right_panel_section(
                lines,
                warn_on_early_stop=False,
            )
            structured_right_lines_count = len(structured_right_lines)
            structured_deals_hits = sum(1 for line in structured_right_lines if self.reader.is_deals_line(line))
            structured_stages = self.reader._parse_right_panel_from_lines(structured_right_lines)
            structured_stages_count = len(structured_stages)
            structured_quality = self._evaluate_structured_precheck_quality(
                right_lines_count=structured_right_lines_count,
                stages=structured_stages,
                deals_hits=structured_deals_hits,
            )
            structured_valid_stage_count = int(structured_quality["valid_stage_count"])
            structured_known_stage_count = int(structured_quality["known_stage_count"])
            structured_known_stage_hits = list(structured_quality["known_stage_names"])
            structured_ready = bool(structured_quality["strong"])
            structured_weak_reasons = list(structured_quality["reasons"])
        except Exception:
            structured_ready = False
            structured_weak_reasons = ["structured_parse_exception"]

        ready = dom_ready or structured_ready

        why_ready_true: list[str] = []
        why_ready_false: list[str] = []

        if ready:
            if dom_ready:
                why_ready_true.append("dom_ready")
            if structured_ready:
                why_ready_true.append("structured_ready")
        else:
            if pipeline_row_count < 3:
                why_ready_false.append("pipeline_rows<3")
            if right_count < self.TAB_READY_MIN_RIGHT_LINES:
                why_ready_false.append(f"right_lines<{self.TAB_READY_MIN_RIGHT_LINES}")
            if deals_hits < 1:
                why_ready_false.append("no_deals_hits")
            if valid_stage_like_count < self.TAB_READY_MIN_VALID_STAGE_NAMES:
                why_ready_false.append(
                    f"valid_stage_like_count<{self.TAB_READY_MIN_VALID_STAGE_NAMES}"
                )
            if dominated_by_forbidden:
                why_ready_false.append("forbidden_bucket_dominant")
            if has_processing_widget:
                why_ready_false.append("processing_time_widget_detected")
            if not structured_ready:
                why_ready_false.append("structured_not_ready")
                why_ready_false.extend([f"structured_weak:{item}" for item in structured_weak_reasons])

        return {
            "ready": ready,
            "dom_ready": dom_ready,
            "structured_ready": structured_ready,
            "deals_hits": deals_hits,
            "known_stage_hits": len(known_stage_hits),
            "known_stage_names": known_stage_hits,
            "forbidden_bucket_hits": forbidden_hits,
            "valid_stage_like_count": valid_stage_like_count,
            "valid_stage_like_names": valid_stage_like_names,
            "right_lines": right_count,
            "pipeline_row_count": pipeline_row_count,
            "structured_right_lines": structured_right_lines_count,
            "structured_stages": structured_stages_count,
            "structured_deals_hits": structured_deals_hits,
            "structured_valid_stage_count": structured_valid_stage_count,
            "structured_known_stage_count": structured_known_stage_count,
            "structured_weak_reasons": structured_weak_reasons,
            "why_ready_true": why_ready_true,
            "why_ready_false": why_ready_false,
        }

    def _evaluate_structured_precheck_quality(
        self,
        right_lines_count: int,
        stages: list[StageCount],
        deals_hits: int,
    ) -> dict[str, object]:
        valid_stage_names = [
            item.stage_name for item in stages if not self.reader._is_forbidden_bucket_label(item.stage_name)
        ]
        known_stage_names = [
            name
            for name in self.TAB_READY_STAGE_NAMES
            if any(item.stage_name.strip().lower() == name.lower() for item in stages)
        ]
        forbidden_count = len(stages) - len(valid_stage_names)
        dominated_by_forbidden = (
            len(stages) > 0 and forbidden_count >= max(2, len(stages) - 1)
        )

        reasons: list[str] = []
        if right_lines_count < self.TAB_READY_MIN_RIGHT_LINES:
            reasons.append(f"right_lines<{self.TAB_READY_MIN_RIGHT_LINES}")
        if deals_hits < self.TAB_READY_MIN_STRUCTURED_DEALS_HITS:
            reasons.append(f"deals_hits<{self.TAB_READY_MIN_STRUCTURED_DEALS_HITS}")
        if len(stages) < self.TAB_READY_MIN_STRUCTURED_STAGES:
            reasons.append(f"stages<{self.TAB_READY_MIN_STRUCTURED_STAGES}")
        if len(valid_stage_names) < self.TAB_READY_MIN_VALID_STAGE_NAMES:
            reasons.append(f"valid_stages<{self.TAB_READY_MIN_VALID_STAGE_NAMES}")
        if len(known_stage_names) < self.TAB_READY_MIN_STRUCTURED_KNOWN_STAGE_NAMES:
            reasons.append(
                f"known_stages<{self.TAB_READY_MIN_STRUCTURED_KNOWN_STAGE_NAMES}"
            )
        if dominated_by_forbidden:
            reasons.append("forbidden_bucket_dominant")

        strong = len(reasons) == 0
        return {
            "strong": strong,
            "reasons": reasons,
            "valid_stage_count": len(valid_stage_names),
            "known_stage_count": len(known_stage_names),
            "known_stage_names": known_stage_names,
            "dominated_by_forbidden": dominated_by_forbidden,
        }

    def _wait_for_tab_content_ready(self, page: Page, tab_mode: TabMode, max_attempts: int = 10) -> bool:
        stable_ready_count = 0

        for attempt in range(1, max_attempts + 1):
            status = self._assess_tab_content_ready(page)
            if bool(status["ready"]):
                stable_ready_count += 1
            else:
                stable_ready_count = 0

            self.logger.info(
                "tab precheck: tab=%s attempt=%s/%s ready=%s dom_ready=%s structured_ready=%s "
                "stable_ready_count=%s pipeline_rows=%s right_lines=%s structured_right_lines=%s "
                "deals_hits=%s structured_deals_hits=%s valid_stage_like_count=%s structured_valid_stage_count=%s structured_known_stage_count=%s "
                "forbidden_bucket_hits=%s structured_weak_reasons=%s why_ready_true=%s why_ready_false=%s",
                tab_mode,
                attempt,
                max_attempts,
                str(status["ready"]).lower(),
                str(status["dom_ready"]).lower(),
                str(status["structured_ready"]).lower(),
                stable_ready_count,
                status["pipeline_row_count"],
                status["right_lines"],
                status["structured_right_lines"],
                status["deals_hits"],
                status["structured_deals_hits"],
                status["valid_stage_like_count"],
                status["structured_valid_stage_count"],
                status["structured_known_stage_count"],
                status["forbidden_bucket_hits"],
                status["structured_weak_reasons"],
                status["why_ready_true"],
                status["why_ready_false"],
            )

            if bool(status["structured_ready"]):
                self.logger.info(
                    "tab precheck ready: true tab=%s strategy=structured_early_exit attempt=%s",
                    tab_mode,
                    attempt,
                )
                return True

            if stable_ready_count >= self.TAB_READY_CONSECUTIVE_REQUIRED:
                self.logger.info(
                    "tab precheck ready: true tab=%s strategy=dom_stable stable_ready_count=%s",
                    tab_mode,
                    stable_ready_count,
                )
                return True

            if attempt < max_attempts:
                page.wait_for_timeout(800)

        self.logger.info(
            "tab precheck inconclusive: tab=%s stable_ready_count=%s (continue with parser retries)",
            tab_mode,
            stable_ready_count,
        )
        return False

    def _right_lines_count_from_page(self, page: Page) -> int:
        try:
            return len(self.reader.extract_pipeline_stage_lines(page))
        except Exception:
            return 0

    def _structured_right_lines_count_from_snapshot(self, snapshot: AnalyticsSnapshot) -> int:
        try:
            debug_text_path = Path(snapshot.debug_text_path)
            if not debug_text_path.exists():
                return 0
            body_text = debug_text_path.read_text(encoding="utf-8")
            lines = [line.strip() for line in body_text.splitlines() if line.strip()]
            right_lines, _start_found, _stop_found = self.reader._slice_right_panel_section(lines)
            return len(right_lines)
        except Exception:
            return 0

    def _is_forbidden_stage_name_for_readiness(self, stage_name: str) -> bool:
        return self.reader._is_forbidden_bucket_label(stage_name)

    def _is_invalid_stage_snapshot(self, snapshot: AnalyticsSnapshot) -> tuple[bool, int, int, list[str]]:
        if not snapshot.stages:
            return True, 0, 0, []

        forbidden = 0
        valid = 0
        forbidden_names: list[str] = []
        for item in snapshot.stages:
            if self._is_forbidden_stage_name_for_readiness(item.stage_name):
                forbidden += 1
                if len(forbidden_names) < 10:
                    forbidden_names.append(item.stage_name)
            else:
                valid += 1

        invalid = valid < self.TAB_READY_MIN_VALID_STAGE_NAMES and forbidden >= max(2, len(snapshot.stages) - 1)
        return invalid, valid, forbidden, forbidden_names

    def _read_tab_with_stability_retries(
        self,
        page: Page,
        profile: AnalyticsFlowInput,
        tab_mode: TabMode,
        max_attempts: int = 3,
        precheck_ready: bool = True,
    ) -> AnalyticsSnapshot:
        last_snapshot: AnalyticsSnapshot | None = None

        for attempt in range(1, max_attempts + 1):
            snapshot = self.reader.read_current_view(
                page=page,
                source_kind=profile.source_kind,
                filter_id=profile.report_id,
                tab_mode=tab_mode,
            )
            last_snapshot = snapshot
            dom_right_lines_count = self._right_lines_count_from_page(page)
            structured_right_lines_count = self._structured_right_lines_count_from_snapshot(snapshot)
            effective_right_lines_count = max(dom_right_lines_count, structured_right_lines_count)
            invalid_stage_snapshot, valid_stage_count, forbidden_stage_count, forbidden_names = self._is_invalid_stage_snapshot(snapshot)

            has_strong_structured_parse = (
                snapshot.parse_method in {"analytics_text_structured", "dom_pipeline_first", "dom"}
                and structured_right_lines_count >= self.TAB_READY_MIN_RIGHT_LINES
                and len(snapshot.stages) >= self.TAB_READY_MIN_VALID_STAGE_NAMES
                and not invalid_stage_snapshot
            )

            unstable = (
                (effective_right_lines_count < self.TAB_READY_MIN_RIGHT_LINES and not has_strong_structured_parse)
                or len(snapshot.stages) == 0
                or invalid_stage_snapshot
            )

            fallback_reason: list[str] = []
            if effective_right_lines_count < self.TAB_READY_MIN_RIGHT_LINES and not has_strong_structured_parse:
                fallback_reason.append(f"effective_right_lines<{self.TAB_READY_MIN_RIGHT_LINES}")
            if len(snapshot.stages) == 0:
                fallback_reason.append("stages==0")
            if invalid_stage_snapshot:
                fallback_reason.append("forbidden_bucket_dominant_or_valid_stages_low")

            self.logger.info(
                "tab parse stability: tab=%s retry_attempt=%s/%s unstable=%s "
                "dom_right_lines=%s structured_right_lines=%s effective_right_lines=%s "
                "has_strong_structured_parse=%s stages=%s "
                "valid_stage_count=%s forbidden_stage_count=%s forbidden_stage_names=%s parse_method=%s fallback_reason=%s",
                tab_mode,
                attempt,
                max_attempts,
                str(unstable).lower(),
                dom_right_lines_count,
                structured_right_lines_count,
                effective_right_lines_count,
                str(has_strong_structured_parse).lower(),
                len(snapshot.stages),
                valid_stage_count,
                forbidden_stage_count,
                forbidden_names,
                snapshot.parse_method,
                ";".join(fallback_reason) if fallback_reason else "none",
            )

            if not unstable:
                if not precheck_ready and has_strong_structured_parse:
                    self.logger.info(
                        "tab precheck resolved by strong parse: tab=%s precheck_ready=false "
                        "structured_right_lines=%s stages=%s method=%s",
                        tab_mode,
                        structured_right_lines_count,
                        len(snapshot.stages),
                        snapshot.parse_method,
                    )
                self.logger.info("final parser used: tab=%s method=%s", tab_mode, snapshot.parse_method)
                return snapshot

            if attempt < max_attempts:
                page.wait_for_timeout(900)

        assert last_snapshot is not None
        self.logger.warning(
            "final parser used (after retries): tab=%s method=%s stages=%s",
            tab_mode,
            last_snapshot.parse_method,
            len(last_snapshot.stages),
        )
        return last_snapshot

    def _debug_screenshot(self, page: Page, name: str, timeout_ms: int = 5000) -> Path | None:
        return analytics_debug_screenshot(
            screenshots_dir=self.reader.settings.screenshots_dir,
            project_root=self.project_root,
            logger=self.logger,
            page=page,
            name=name,
            timeout_ms=max(1000, int(timeout_ms)),
        )

    def _debug_dir(self) -> Path:
        return analytics_debug_dir(self.reader.settings.exports_dir, self.project_root)

    def _find_filter_panel_container(self, page: Page) -> Locator:
        debug_dir = ensure_inside_root(self.reader.settings.exports_dir / "debug", self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        return debug_dir

    def _find_filter_panel_container(self, page: Page) -> Locator:
        """Find most likely filter panel container, prefer modal/drawer-like overlays."""
        best_locator: Locator | None = None
        best_score = -1

        for selector in self.FILTER_PANEL_CONTAINER_SELECTORS:
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 80)
            except Exception:
                continue

            for idx in range(count):
                item = locator.nth(idx)
                try:
                    text = item.inner_text(timeout=350).strip()
                except Exception:
                    continue

                if not text:
                    continue

                upper = text.upper()
                score = 0
                if "Р¤РР›Р¬РўР " in upper:
                    score += 3
                if "РџР РРњР•РќРРўР¬" in upper:
                    score += 3
                if "UTM" in upper:
                    score += 2
                if "РўР•Р“" in upper:
                    score += 2
                score += min(len(text) // 200, 3)

                if score > best_score:
                    best_score = score
                    best_locator = item

        if best_locator is not None:
            self.logger.info("Filter panel container selected with score=%s", best_score)
            return best_locator

        self.logger.warning("Filter panel container not found by candidates; fallback to main container.")
        return page.locator("main").first

    def _panel_text(self, panel: Locator) -> str:
        try:
            return panel.inner_text(timeout=1000)
        except Exception:
            return ""

    def _save_debug_text(self, file_name: str, text: str) -> Path:
        return analytics_save_debug_text(self.reader.settings.exports_dir, self.project_root, file_name, text)

    def _find_scrollable_container(self, panel: Locator) -> Locator | None:
        """Find best scrollable element inside panel by overflow and height delta."""
        best_locator: Locator | None = None
        best_delta = 0
        best_metrics: dict[str, object] | None = None
        checked = 0

        def _collect_metrics(item: Locator) -> dict[str, object] | None:
            try:
                result = item.evaluate(
                    """el => {
                        const style = window.getComputedStyle(el);
                        return {
                            scrollHeight: el.scrollHeight || 0,
                            clientHeight: el.clientHeight || 0,
                            overflowY: style.overflowY || '',
                            tagName: el.tagName || '',
                            className: (el.className || '').toString()
                        };
                    }"""
                )
            except Exception:
                return None
            return result if isinstance(result, dict) else None

        panel_metrics = _collect_metrics(panel)
        if panel_metrics is not None:
            checked += 1
            scroll_height = int(panel_metrics.get("scrollHeight", 0))
            client_height = int(panel_metrics.get("clientHeight", 0))
            overflow_y = str(panel_metrics.get("overflowY", "")).lower()
            delta = scroll_height - client_height
            if delta > 8 and overflow_y in {"auto", "scroll", "overlay"}:
                best_locator = panel
                best_delta = delta
                best_metrics = panel_metrics

        descendants = panel.locator("*")
        try:
            desc_count = min(descendants.count(), 220)
        except Exception:
            desc_count = 0

        for idx in range(desc_count):
            item = descendants.nth(idx)
            metrics = _collect_metrics(item)
            if metrics is None:
                continue
            checked += 1
            scroll_height = int(metrics.get("scrollHeight", 0))
            client_height = int(metrics.get("clientHeight", 0))
            overflow_y = str(metrics.get("overflowY", "")).lower()
            delta = scroll_height - client_height

            if delta <= 8:
                continue
            if overflow_y not in {"auto", "scroll", "overlay"}:
                continue
            if delta <= best_delta:
                continue

            best_locator = item
            best_delta = delta
            best_metrics = metrics

        if best_locator is not None:
            self.logger.info(
                "Scrollable filter panel container found: checked=%s delta=%s metrics=%s",
                checked,
                best_delta,
                best_metrics,
            )
        else:
            self.logger.warning("Scrollable filter panel container not found. checked_candidates=%s", checked)

        return best_locator
    def _scroll_panel_debug(self, panel: Locator, report_id: str) -> tuple[list[Path], Path, list[bool], bool, list[str]]:
        """Collect step-by-step panel text while scrolling to expand visible filter list."""
        step_paths: list[Path] = []
        step_texts: list[str] = []
        text_changed_flags: list[bool] = []

        scrollable = self._find_scrollable_container(panel)
        found_scrollable = scrollable is not None

        max_steps = 8
        previous_text = ""

        for step in range(1, max_steps + 1):
            current_text = self._panel_text(panel)
            step_texts.append(current_text)
            step_path = self._save_debug_text(
                f"{report_id}_filter_panel_scroll_step_{step:02d}.txt",
                current_text,
            )
            step_paths.append(step_path)

            changed = current_text != previous_text if step > 1 else True
            text_changed_flags.append(changed)

            if step > 1 and not changed:
                break

            previous_text = current_text

            if not found_scrollable:
                break

            try:
                scrollable.evaluate(
                    "el => el.scrollBy({ top: Math.max(240, Math.floor(el.clientHeight * 0.8)), behavior: 'auto' })"
                )
            except Exception:
                break

            panel.page.wait_for_timeout(250)

        unique_lines: list[str] = []
        seen_lines: set[str] = set()
        for text in step_texts:
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                if line in seen_lines:
                    continue
                seen_lines.add(line)
                unique_lines.append(line)

        merged_text = "\n".join(unique_lines)
        merged_path = self._save_debug_text(f"{report_id}_filter_panel_scroll_merged.txt", merged_text)

        lowered = merged_text.lower()
        found_targets = [label for label in self.TARGET_PANEL_LABELS if label in lowered]

        return step_paths, merged_path, text_changed_flags, found_scrollable, found_targets

    def _dump_filter_panel_selectors(self, panel: Locator, report_id: str, stamp: str) -> tuple[Path, dict[str, int], list[str]]:
        selectors_path = ensure_inside_root(
            self._debug_dir() / f"{report_id}_filter_panel_selectors_{stamp}.json",
            self.project_root,
        )

        report: list[dict[str, object]] = []
        counts: dict[str, int] = {}
        filter_like_labels: list[str] = []

        for selector in self.FILTER_PANEL_SELECTOR_DIAGNOSTICS:
            locator = panel.locator(selector)
            try:
                count = locator.count()
            except Exception as exc:
                counts[selector] = -1
                report.append({"selector": selector, "error": str(exc)})
                continue

            counts[selector] = count
            sample_texts: list[str] = []
            for idx in range(min(count, 10)):
                try:
                    text = locator.nth(idx).inner_text(timeout=500).strip()
                except Exception:
                    text = ""

                if text:
                    sample_texts.append(text[:300])
                    lowered = text.lower()
                    if any(token in lowered for token in ("С„РёР»СЊС‚СЂ", "tag", "utm", "РїСЂРёРјРµРЅ", "РёСЃС‚РѕС‡РЅРёРє")):
                        filter_like_labels.append(text[:120])

            report.append({"selector": selector, "count": count, "sample_texts": sample_texts})

        selectors_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        unique_labels = list(dict.fromkeys(filter_like_labels))
        return selectors_path, counts, unique_labels

    def _collect_filter_panel_diagnostics(
        self, page: Page, report_id: str
    ) -> tuple[Path, Path, list[str], dict[str, int], list[Path], Path, list[bool], bool, list[str]]:
        """Collect text + selector diagnostics from open filter panel only."""
        panel = self._find_filter_panel_container(page)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        text_path = ensure_inside_root(
            self._debug_dir() / f"{report_id}_filter_panel_visible_text_{stamp}.txt",
            self.project_root,
        )
        text_path.write_text(self._panel_text(panel), encoding="utf-8")

        selectors_path, counts, labels = self._dump_filter_panel_selectors(panel, report_id, stamp)
        step_paths, merged_path, changed_flags, found_scrollable, found_targets = self._scroll_panel_debug(panel, report_id)

        return (
            text_path,
            selectors_path,
            labels,
            counts,
            step_paths,
            merged_path,
            changed_flags,
            found_scrollable,
            found_targets,
        )

    def _log_filter_diagnostics(self, page: Page) -> None:
        candidates = ("button", "[role='button']", "a", "label", "span")
        found: list[str] = []

        for selector in candidates:
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 80)
            except Exception:
                continue

            for idx in range(count):
                try:
                    text = locator.nth(idx).inner_text(timeout=250).strip()
                except Exception:
                    continue

                if not text or len(text) > 40:
                    continue

                lowered = text.lower()
                if any(token in lowered for token in ("С„РёР»СЊС‚СЂ", "tag", "utm", "РїСЂРёРјРµРЅ")):
                    found.append(text)

        unique = list(dict.fromkeys(found))
        self.logger.info("Filter-like labels detected on page: %s", unique[:25])

        counts: dict[str, int] = {}
        for selector in self.FILTER_OPEN_SELECTORS:
            try:
                counts[selector] = page.locator(selector).count()
            except Exception:
                counts[selector] = -1
        self.logger.info("Filter selector candidate counts: %s", counts)

    def _open_filter_panel(self, page: Page) -> None:
        self.logger.info("filter_open_selector_candidates=%s", list(self.FILTER_OPEN_SELECTORS))
        for selector in self.FILTER_OPEN_SELECTORS:
            locator = page.locator(selector)
            try:
                matched_count = locator.count()
            except Exception:
                matched_count = -1
            self.logger.info("filter_open_try selector=%s matched_count=%s", selector, matched_count)
            try:
                if matched_count > 0:
                    locator.first.click(timeout=3000)
                    self.logger.info("Filter panel opened via selector: %s", selector)
                    page.wait_for_timeout(700)
                    return
            except Exception:
                continue

        raise RuntimeError("Could not open analytics filter panel. No filter selector worked.")

    def _find_tag_input_by_placeholder(self, panel: Locator) -> tuple[Locator | None, list[str]]:
        """Try to find tag field by placeholder 'РўРµРіРё' inside filter panel."""
        tried_selectors: list[str] = []
        for selector in self.TAG_PLACEHOLDER_SELECTORS:
            tried_selectors.append(selector)
            locator = panel.locator(selector)
            try:
                if locator.count() > 0:
                    return locator.first, tried_selectors
            except Exception:
                continue
        return None, tried_selectors

    def _element_debug_payload(self, locator: Locator) -> dict[str, object]:
        payload: dict[str, object] = {
            "tagName": "",
            "name": "",
            "className": "",
            "placeholder": "",
            "ariaLabel": "",
            "value": "",
            "bbox": None,
            "text": "",
        }
        try:
            attrs = locator.evaluate(
                """el => ({
                    tagName: (el.tagName || '').toLowerCase(),
                    name: (el.getAttribute('name') || ''),
                    className: (el.className || '').toString(),
                    placeholder: (el.getAttribute('placeholder') || ''),
                    ariaLabel: (el.getAttribute('aria-label') || ''),
                    value: (el.value || ''),
                    text: (el.innerText || '').slice(0, 200)
                })"""
            )
            if isinstance(attrs, dict):
                payload.update(attrs)
        except Exception:
            pass
        try:
            payload["bbox"] = locator.bounding_box()
        except Exception:
            pass
        return payload

    def _tag_focus_candidate_payload(self, locator: Locator) -> dict[str, object]:
        payload = self._element_debug_payload(locator)
        extra = {
            "type": "",
            "role": "",
            "contenteditable": "",
            "readonly": False,
            "disabled": False,
            "editable": False,
            "kind": "div",
        }
        try:
            attrs = locator.evaluate(
                """el => {
                    const tag = (el.tagName || '').toLowerCase();
                    const role = (el.getAttribute('role') || '').toLowerCase();
                    const contenteditable = (el.getAttribute('contenteditable') || '').toLowerCase();
                    const isInputLike = tag === 'input' || tag === 'textarea';
                    const editable = (
                        (isInputLike && !el.disabled && !el.readOnly) ||
                        contenteditable === 'true' ||
                        role === 'textbox'
                    );
                    let kind = 'div';
                    if (tag === 'input') kind = 'input';
                    else if (tag === 'textarea') kind = 'textarea';
                    else if (contenteditable === 'true') kind = 'contenteditable';
                    else if (role === 'textbox') kind = 'textbox';
                    return {
                        type: (el.getAttribute('type') || '').toLowerCase(),
                        role,
                        contenteditable,
                        readonly: !!el.readOnly,
                        disabled: !!el.disabled,
                        editable,
                        kind,
                    };
                }"""
            )
            if isinstance(attrs, dict):
                extra.update(attrs)
        except Exception:
            pass
        payload.update(extra)
        return payload

    def _is_valid_tag_focus_target(self, locator: Locator) -> tuple[bool, str, dict[str, object]]:
        payload = self._tag_focus_candidate_payload(locator)
        kind = str(payload.get("kind", "div")).strip().lower()
        editable = bool(payload.get("editable", False))
        class_name = str(payload.get("className", "")).lower()
        text = str(payload.get("text", "")).strip()

        if kind not in {"input", "textarea", "contenteditable", "textbox"}:
            return False, "kind_not_editable", payload
        if not editable:
            return False, "editable_false", payload
        if "multisuggest__list-item_input" in class_name and kind not in {"input", "textarea"}:
            return False, "multisuggest_container_without_real_input", payload
        if kind in {"contenteditable", "textbox"} and text and len(text) > 0:
            value = str(payload.get("value", "")).strip()
            if not value:
                return False, "chip_text_container_without_input_value", payload
        return True, "ok", payload

    def _find_editable_descendant_in_row(self, row: Locator) -> tuple[Locator | None, str]:
        descendant_selectors = (
            "input",
            "textarea",
            "[contenteditable='true']",
            "[role='textbox']",
        )
        for selector in descendant_selectors:
            locator = row.locator(selector)
            try:
                count = min(locator.count(), 25)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=250):
                        continue
                except Exception:
                    continue
                valid, reason, payload = self._is_valid_tag_focus_target(item)
                self.logger.info(
                    "Tag focus candidate kind=%s editable=%s chosen=%s reason=%s payload=%s",
                    payload.get("kind", "div"),
                    str(bool(payload.get("editable", False))).lower(),
                    str(valid).lower(),
                    reason,
                    payload,
                )
                if not valid:
                    continue
                try:
                    item.click(timeout=1200)
                    row.page.wait_for_timeout(180)
                except Exception:
                    pass
                return item, selector
        return None, ""

    def _click_row_right_side(self, row: Locator) -> bool:
        try:
            box = row.bounding_box()
            if not box:
                return False
            x = box["x"] + max(10, box["width"] - 16)
            y = box["y"] + max(6, min(box["height"] / 2, box["height"] - 6))
            row.page.mouse.click(x, y)
            row.page.wait_for_timeout(180)
            return True
        except Exception:
            return False

    def _find_primary_tag_reference_input(self, panel: Locator) -> tuple[Locator | None, str, dict[str, object]]:
        try:
            candidate = panel.locator(self.TAG_REFERENCE_INPUT_SELECTOR)
            if candidate.count() == 0:
                self.logger.info(
                    "tag_primary_input_detected chosen=false reason=reference_not_found selector=%s",
                    self.TAG_REFERENCE_INPUT_SELECTOR,
                )
                return None, "reference_not_found", {}
            reference_input = candidate.first
        except Exception as exc:
            self.logger.info(
                "tag_primary_input_detected chosen=false reason=reference_lookup_error error=%s selector=%s",
                exc,
                self.TAG_REFERENCE_INPUT_SELECTOR,
            )
            return None, "reference_lookup_error", {}

        valid, reason, payload = self._is_valid_tag_focus_target(reference_input)
        try:
            visible = reference_input.is_visible(timeout=300)
        except Exception:
            visible = False
        try:
            enabled = reference_input.is_enabled(timeout=300)
        except Exception:
            enabled = False
        bbox = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else None

        if str(payload.get("tagName", "")).lower() != "input":
            valid = False
            reason = "reference_not_input"
        elif not visible:
            valid = False
            reason = "reference_not_visible"
        elif not enabled:
            valid = False
            reason = "reference_not_enabled"
        elif not bbox:
            valid = False
            reason = "reference_missing_bbox"

        self.logger.info(
            "tag_primary_input_detected chosen=%s reason=%s payload=%s",
            str(valid).lower(),
            reason if reason else "ok",
            payload,
        )
        return (reference_input if valid else None), (reason if reason else "ok"), payload

    def _read_input_value(self, locator: Locator) -> str:
        try:
            value = locator.evaluate("el => String(el.value || '')")
            if isinstance(value, str):
                return value.strip()
        except Exception:
            pass
        return ""

    def _find_tag_row_below_reference(self, panel: Locator) -> Locator | None:
        """Temporary pragmatic account-specific strategy: find nearest visible row below reference field."""
        try:
            reference = panel.locator(self.TAG_REFERENCE_INPUT_SELECTOR)
            if reference.count() == 0:
                self.logger.info("Tag reference field not found: %s", self.TAG_REFERENCE_INPUT_SELECTOR)
                return None
            reference_input = reference.first
        except Exception:
            self.logger.info("Tag reference field lookup failed: %s", self.TAG_REFERENCE_INPUT_SELECTOR)
            return None

        scrollable = self._find_scrollable_container(panel)
        if scrollable is not None:
            try:
                scrollable.evaluate("el => { el.scrollTop = el.scrollHeight; }")
                panel.page.wait_for_timeout(220)
            except Exception:
                pass

        reference_row = reference_input.locator("xpath=ancestor::*[self::div or self::label][1]")
        try:
            reference_row = reference_row.first if reference_row.count() > 0 else reference_input
        except Exception:
            reference_row = reference_input

        ref_payload = self._element_debug_payload(reference_input)
        ref_row_payload = self._element_debug_payload(reference_row)
        ref_row_box = ref_row_payload.get("bbox") if isinstance(ref_row_payload.get("bbox"), dict) else None
        if not ref_row_box:
            self.logger.info("Reference row has no bbox, cannot find lower row.")
            return None
        ref_y = float(ref_row_box.get("y", 0.0))

        candidates = panel.locator("div, label, section, li, article, form")
        try:
            count = min(candidates.count(), 500)
        except Exception:
            count = 0

        scored: list[tuple[float, Locator, dict[str, object]]] = []
        for idx in range(count):
            row = candidates.nth(idx)
            try:
                if not row.is_visible(timeout=250):
                    continue
            except Exception:
                continue
            payload = self._element_debug_payload(row)
            box = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else None
            if not box:
                continue
            y = float(box.get("y", -1.0))
            if y <= ref_y + 1.0:
                continue
            if not str(payload.get("className", "")).strip() and not str(payload.get("text", "")).strip():
                continue
            scored.append((y - ref_y, row, payload))

        if not scored:
            self.logger.info("No row candidates found below reference field: %s", self.TAG_REFERENCE_INPUT_SELECTOR)
            return None

        scored.sort(key=lambda x: x[0])
        row_debug: list[dict[str, object]] = []
        for delta, _, payload in scored[:8]:
            box = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else {}
            row_debug.append(
                {
                    "tagName": payload.get("tagName", ""),
                    "name": payload.get("name", ""),
                    "className": payload.get("className", ""),
                    "bbox_y": box.get("y"),
                    "bbox_height": box.get("height"),
                    "value": payload.get("value", ""),
                    "text": payload.get("text", ""),
                    "delta_y": round(delta, 2),
                }
            )

        best_delta, best_row, best_payload = scored[0]
        self.logger.info("Tag reference input payload: %s", ref_payload)
        self.logger.info("Tag reference row payload: %s", ref_row_payload)
        self.logger.info("Tag lower row candidates (nearest 8): %s", row_debug)
        self.logger.info("Tag selected lower row payload: %s", best_payload)
        self.logger.info("Tag selected lower row delta_y=%s", round(best_delta, 2))
        return best_row

    def _is_bbox_inside_panel(self, candidate_box: dict[str, object] | None, panel_box: dict[str, object] | None) -> bool:
        if not isinstance(candidate_box, dict) or not isinstance(panel_box, dict):
            return False
        try:
            cx = float(candidate_box.get("x", 0.0))
            cy = float(candidate_box.get("y", 0.0))
            cw = float(candidate_box.get("width", 0.0))
            ch = float(candidate_box.get("height", 0.0))
            px = float(panel_box.get("x", 0.0))
            py = float(panel_box.get("y", 0.0))
            pw = float(panel_box.get("width", 0.0))
            ph = float(panel_box.get("height", 0.0))
        except Exception:
            return False
        if cw <= 0 or ch <= 0 or pw <= 0 or ph <= 0:
            return False
        return (cx + cw) >= px and cx <= (px + pw) and (cy + ch) >= py and cy <= (py + ph)

    def _find_tag_label_y(self, panel: Locator) -> float | None:
        selectors = (
            "label:has-text('????')",
            "div:has-text('????')",
            "span:has-text('????')",
            "label:has-text('????')",
            "div:has-text('????')",
            "span:has-text('????')",
        )
        best_y: float | None = None
        for selector in selectors:
            locator = panel.locator(selector)
            try:
                count = min(locator.count(), 8)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=200):
                        continue
                    box = item.bounding_box()
                except Exception:
                    continue
                if not box:
                    continue
                y = float(box.get("y", 0.0))
                if best_y is None or y > best_y:
                    best_y = y
        return best_y

    def _build_tag_input_candidate(self, locator: Locator, selector: str, idx: int, panel_box: dict[str, object] | None) -> dict[str, object]:
        payload = self._tag_focus_candidate_payload(locator)
        try:
            extra = locator.evaluate(
                """el => {
                    const classes = [];
                    let node = el;
                    for (let i = 0; i < 5 && node; i += 1) {
                        classes.push(((node.className || '').toString()).toLowerCase());
                        node = node.parentElement;
                    }
                    const joined = classes.join(' ');
                    const isTagContainer = (
                        joined.includes('filter-search__tags-holder') ||
                        joined.includes('tags-holder') ||
                        joined.includes('multisuggest') ||
                        joined.includes('suggest-manager') ||
                        joined.includes('js-multisuggest')
                    );
                    return {
                        ancestorClassChain: joined,
                        insideTagContainer: isTagContainer,
                    };
                }"""
            )
        except Exception:
            extra = {"ancestorClassChain": "", "insideTagContainer": False}

        box = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else None
        payload.update(
            {
                "candidateIndex": idx,
                "selector": selector,
                "insideTagContainer": bool((extra or {}).get("insideTagContainer", False)),
                "ancestorClassChain": str((extra or {}).get("ancestorClassChain", "")),
                "insidePanelViewport": self._is_bbox_inside_panel(box, panel_box),
            }
        )
        return payload

    def _score_tag_input_candidate(
        self,
        payload: dict[str, object],
        reference_y: float | None,
        tag_label_y: float | None,
    ) -> tuple[float, bool, str]:
        valid_kind = str(payload.get("kind", "")).lower() in {"input", "textarea", "contenteditable", "textbox"}
        editable = bool(payload.get("editable", False))
        box = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else None
        in_view = bool(payload.get("insidePanelViewport", False))

        if not valid_kind:
            return -9999.0, True, "invalid_kind"
        if not editable:
            return -9999.0, True, "not_editable"
        if not box:
            return -9999.0, True, "missing_bbox"
        if not in_view:
            return -9999.0, True, "outside_panel_viewport"
        if bool(payload.get("disabled", False)) or bool(payload.get("readonly", False)):
            return -9999.0, True, "disabled_or_readonly"

        score = 0.0
        reasons: list[str] = []

        y = float(box.get("y", 0.0))
        h = float(box.get("height", 0.0))
        if h < 8:
            return -9999.0, True, "tiny_bbox"

        if bool(payload.get("insideTagContainer", False)):
            score += 80
            reasons.append("inside_tag_container")

        cls = str(payload.get("className", "")).lower()
        anc = str(payload.get("ancestorClassChain", "")).lower()
        name = str(payload.get("name", "")).lower()
        placeholder = str(payload.get("placeholder", "")).lower()
        text = str(payload.get("text", "")).lower()

        if any(t in cls or t in anc for t in ("multisuggest", "suggest-manager", "tags-holder", "js-multisuggest")):
            score += 35
            reasons.append("tag_suggest_class")

        if any(t in placeholder or t in cls or t in anc for t in ("???", "tag", "tags")):
            score += 20
            reasons.append("tag_signal")

        if reference_y is not None:
            if y > reference_y + 2.0:
                score += 28
                reasons.append("below_reference")
            else:
                score -= 40
                reasons.append("above_or_same_as_reference")

        if tag_label_y is not None:
            dy = abs(y - tag_label_y)
            if dy <= 260:
                score += 24
                reasons.append("near_tag_label")
            else:
                score -= min(20, dy / 30)
                reasons.append("far_from_tag_label")

        value = str(payload.get("value", "")).strip()
        if value and "?????-??-?????" in (value.lower() + " " + text):
            score -= 40
            reasons.append("looks_like_non_tag_existing_value")

        if name == "filter[cf][735215]":
            score -= 10
            reasons.append("reference_input_only_candidate")

        return score, False, ",".join(reasons) if reasons else "scored"

    def _verify_focus_on_candidate(self, candidate: Locator) -> tuple[bool, str]:
        try:
            ok = candidate.evaluate(
                """el => {
                    const active = document.activeElement;
                    if (!active) return false;
                    if (active === el) return true;
                    return el.contains(active);
                }"""
            )
            if bool(ok):
                return True, "active_element_matches_candidate"
        except Exception:
            pass
        return False, "active_element_not_candidate"

    def _save_tag_input_resolution_debug(
        self,
        report_id: str,
        candidates: list[dict[str, object]],
        chosen_idx: int | None,
        stop_reason: str,
    ) -> tuple[Path, Path]:
        lines = [
            f"report_id={report_id}",
            f"chosen_idx={chosen_idx}",
            f"stop_reason={stop_reason}",
            f"candidates={len(candidates)}",
        ]
        for row in candidates[:40]:
            lines.append(
                " | ".join(
                    [
                        f"idx={row.get('candidateIndex')}",
                        f"selector={row.get('selector')}",
                        f"score={row.get('score')}",
                        f"chosen={row.get('chosen')}",
                        f"rejected_reason={row.get('rejected_reason')}",
                        f"chosen_reason={row.get('chosen_reason')}",
                        f"bbox={row.get('bbox')}",
                        f"name={row.get('name')}",
                        f"class={row.get('className')}",
                        f"value={row.get('value')}",
                        f"insideTagContainer={row.get('insideTagContainer')}",
                        f"distanceToTagLabel={row.get('distance_to_tag_label')}",
                    ]
                )
            )
        return analytics_save_tag_input_resolution_debug(
            exports_dir=self.reader.settings.exports_dir,
            project_root=self.project_root,
            candidates=candidates,
            summary_lines=lines,
        )

    def _resolve_tag_input_target(self, panel: Locator, report_id: str) -> tuple[Locator | None, str]:
        panel_box = None
        try:
            panel_box = panel.bounding_box()
        except Exception:
            panel_box = None

        reference_y: float | None = None
        try:
            ref = panel.locator(self.TAG_REFERENCE_INPUT_SELECTOR)
            if ref.count() > 0:
                ref_box = ref.first.bounding_box()
                if ref_box:
                    reference_y = float(ref_box.get("y", 0.0))
        except Exception:
            reference_y = None

        tag_label_y = self._find_tag_label_y(panel)

        candidate_sources: list[tuple[str, Locator]] = [
            ("reference_exact", panel.locator(self.TAG_REFERENCE_INPUT_SELECTOR)),
            ("row_desc_input", panel.locator("div, label, section, li, article, form >> input")),
            ("row_desc_textarea", panel.locator("div, label, section, li, article, form >> textarea")),
            ("row_desc_contenteditable", panel.locator("div, label, section, li, article, form >> [contenteditable='true']")),
            ("row_desc_textbox", panel.locator("div, label, section, li, article, form >> [role='textbox']")),
            ("panel_input", panel.locator("input")),
            ("panel_textarea", panel.locator("textarea")),
            ("panel_contenteditable", panel.locator("[contenteditable='true']")),
            ("panel_textbox", panel.locator("[role='textbox']")),
        ]

        raw_candidates: list[tuple[Locator, dict[str, object]]] = []
        dedup: set[str] = set()
        for selector, locator in candidate_sources:
            try:
                count = min(locator.count(), 80)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=150):
                        continue
                except Exception:
                    continue
                payload = self._build_tag_input_candidate(item, selector, idx, panel_box)
                box = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else {}
                key = "|".join(
                    [
                        str(payload.get("tagName", "")),
                        str(payload.get("name", "")),
                        str(payload.get("className", ""))[:120],
                        str(int(float(box.get("x", 0.0))) if box else 0),
                        str(int(float(box.get("y", 0.0))) if box else 0),
                    ]
                )
                if key in dedup:
                    continue
                dedup.add(key)
                raw_candidates.append((item, payload))

        scored: list[tuple[float, Locator, dict[str, object]]] = []
        for item, payload in raw_candidates:
            score, rejected, reason = self._score_tag_input_candidate(payload, reference_y, tag_label_y)
            box = payload.get("bbox") if isinstance(payload.get("bbox"), dict) else {}
            payload["score"] = round(score, 2)
            payload["rejected_reason"] = reason if rejected else ""
            payload["chosen"] = False
            payload["chosen_reason"] = ""
            payload["distance_to_tag_label"] = None if tag_label_y is None else round(abs(float(box.get("y", 0.0)) - tag_label_y), 2)
            self.logger.info(
                "tag_input_candidate idx=%s bbox=%s class=%s name=%s value=%s visible=true editable=%s inside_tag_container=%s chosen=%s reason=%s",
                payload.get("candidateIndex"),
                payload.get("bbox"),
                payload.get("className", ""),
                payload.get("name", ""),
                payload.get("value", ""),
                str(bool(payload.get("editable", False))).lower(),
                str(bool(payload.get("insideTagContainer", False))).lower(),
                "false",
                reason,
            )
            if rejected:
                continue
            scored.append((score, item, payload))

        scored.sort(key=lambda x: x[0], reverse=True)

        chosen_idx: int | None = None
        stop_reason = "no_valid_candidates"
        for rank, (score, item, payload) in enumerate(scored):
            try:
                item.click(timeout=1200)
                panel.page.wait_for_timeout(160)
            except Exception as exc:
                payload["rejected_reason"] = f"click_failed:{exc}"
                continue

            focus_ok, focus_reason = self._verify_focus_on_candidate(item)
            if not focus_ok:
                payload["rejected_reason"] = focus_reason
                self.logger.info(
                    "tag_input_candidate idx=%s chosen=false reason=%s",
                    payload.get("candidateIndex"),
                    focus_reason,
                )
                continue

            payload["chosen"] = True
            payload["chosen_reason"] = f"rank={rank};score={round(score,2)};{focus_reason}"
            chosen_idx = int(payload.get("candidateIndex", rank))
            stop_reason = "candidate_selected"
            self.logger.info(
                "tag_input_candidate idx=%s chosen=true reason=%s",
                payload.get("candidateIndex"),
                payload["chosen_reason"],
            )
            json_path, txt_path = self._save_tag_input_resolution_debug(
                report_id=report_id,
                candidates=[row for _, row in raw_candidates],
                chosen_idx=chosen_idx,
                stop_reason=stop_reason,
            )
            self.logger.info("Tag input resolution debug: candidates=%s resolution=%s", json_path, txt_path)
            return item, "ranked_tag_input"

        json_path, txt_path = self._save_tag_input_resolution_debug(
            report_id=report_id,
            candidates=[row for _, row in raw_candidates],
            chosen_idx=chosen_idx,
            stop_reason=stop_reason,
        )
        self.logger.info("Tag input resolution debug: candidates=%s resolution=%s", json_path, txt_path)
        return None, "not_found"

    def _focus_tag_field_via_row(self, panel: Locator, report_id: str, allow_primary: bool = True) -> tuple[Locator | None, str]:
        """Resolve real editable tag input with ranked candidates; row/container path stays fallback only."""
        target, strategy = self._resolve_tag_input_target(panel, report_id)
        if target is not None:
            return target, strategy

        self.logger.info("fallback_to_lower_row reason=ranked_resolution_not_found")
        row = self._find_tag_row_below_reference(panel)
        if row is not None:
            item, selector = self._find_editable_descendant_in_row(row)
            if item is not None:
                self.logger.info(
                    "Tag focus strategy=below_reference_row_descendant selector=%s payload=%s",
                    selector,
                    self._tag_focus_candidate_payload(item),
                )
                return item, "below_reference_row_descendant"

            if self._click_row_right_side(row):
                self.logger.info(
                    "Tag focus row right-side click executed for editable retry payload=%s",
                    self._element_debug_payload(row),
                )
                item_retry, selector_retry = self._find_editable_descendant_in_row(row)
                if item_retry is not None:
                    self.logger.info(
                        "Tag focus strategy=below_reference_row_right_side_retry selector=%s payload=%s",
                        selector_retry,
                        self._tag_focus_candidate_payload(item_retry),
                    )
                    return item_retry, "below_reference_row_right_side_retry"

        placeholder_input, _ = self._find_tag_input_by_placeholder(panel)
        if placeholder_input is not None:
            valid, reason, payload = self._is_valid_tag_focus_target(placeholder_input)
            self.logger.info(
                "Tag focus candidate kind=%s editable=%s chosen=%s reason=%s payload=%s",
                payload.get("kind", "div"),
                str(bool(payload.get("editable", False))).lower(),
                str(valid).lower(),
                reason,
                payload,
            )
            if not valid:
                placeholder_input = None
        if placeholder_input is not None:
            try:
                placeholder_input.click(timeout=1200)
            except Exception:
                pass
            panel.page.wait_for_timeout(150)
            self.logger.info(
                "Tag focus strategy=placeholder_direct payload=%s",
                self._input_debug_payload(placeholder_input, panel),
            )
            return placeholder_input, "placeholder_direct"

        scrollable = self._find_scrollable_container(panel)
        fallback = self._find_bottom_tag_input(panel, scrollable) or self._find_tag_input_near_apply_button(panel)
        if fallback is not None:
            valid, reason, payload = self._is_valid_tag_focus_target(fallback)
            self.logger.info(
                "Tag focus candidate kind=%s editable=%s chosen=%s reason=%s payload=%s",
                payload.get("kind", "div"),
                str(bool(payload.get("editable", False))).lower(),
                str(valid).lower(),
                reason,
                payload,
            )
            if not valid:
                fallback = None
        if fallback is not None:
            try:
                fallback.click(timeout=1200)
            except Exception:
                pass
            panel.page.wait_for_timeout(150)
            self.logger.info(
                "Tag focus strategy=fallback_generic payload=%s",
                self._input_debug_payload(fallback, panel),
            )
            return fallback, "fallback_generic"
        return None, "not_found"

    def _find_tag_input_near_apply_button(self, panel: Locator) -> Locator | None:
        """Fallback: find likely tag input near 'РџСЂРёРјРµРЅРёС‚СЊ' button block."""
        apply_button = panel.locator("button:has-text('РџСЂРёРјРµРЅРёС‚СЊ')")
        try:
            if apply_button.count() == 0:
                return None
        except Exception:
            return None

        container_candidates = (
            panel.locator("div:has(button:has-text('РџСЂРёРјРµРЅРёС‚СЊ'))"),
            panel.locator("section:has(button:has-text('РџСЂРёРјРµРЅРёС‚СЊ'))"),
            panel,
        )

        for container in container_candidates:
            try:
                if container.count() == 0:
                    continue
            except Exception:
                continue

            text_inputs = container.first.locator("input[type='text'], textarea, input:not([type])")
            try:
                count = text_inputs.count()
            except Exception:
                continue
            for idx in range(count - 1, -1, -1):
                candidate = text_inputs.nth(idx)
                if self._is_probably_tag_input(candidate, panel):
                    return candidate

        return None

    def _input_debug_payload(self, locator: Locator, panel: Locator) -> dict[str, object]:
        payload: dict[str, object] = {
            "tagName": "",
            "type": "",
            "placeholder": "",
            "ariaLabel": "",
            "name": "",
            "className": "",
            "value": "",
            "bbox": None,
            "containerText": "",
        }
        try:
            attrs = locator.evaluate(
                """el => ({
                    tagName: (el.tagName || '').toLowerCase(),
                    type: (el.getAttribute('type') || ''),
                    placeholder: (el.getAttribute('placeholder') || ''),
                    ariaLabel: (el.getAttribute('aria-label') || ''),
                    name: (el.getAttribute('name') || ''),
                    className: (el.className || '').toString(),
                    value: (el.value || ''),
                    containerText: ((el.closest('div,section,form,label') || el.parentElement || {}).innerText || '').slice(0, 500)
                })"""
            )
            if isinstance(attrs, dict):
                payload.update(attrs)
        except Exception:
            pass
        try:
            payload["bbox"] = locator.bounding_box()
        except Exception:
            pass
        return payload

    def _is_probably_tag_input(self, locator: Locator, panel: Locator) -> bool:
        """Allow only inputs with explicit tag-specific signals."""
        info = self._input_debug_payload(locator, panel)
        joined = " ".join(
            [
                str(info.get("placeholder", "")),
                str(info.get("ariaLabel", "")),
                str(info.get("name", "")),
                str(info.get("className", "")),
                str(info.get("containerText", "")),
            ]
        ).lower()

        signals = ("С‚РµРі", "С‚РµРіРё", "tag", "tags", "РјРµС‚РєР°", "РјРµС‚РєРё")
        return any(signal in joined for signal in signals)
    def _find_bottom_tag_input(self, panel: Locator, scrollable: Locator | None) -> Locator | None:
        """Find bottom-most visible text input near apply button as tag fallback."""
        try:
            apply = panel.locator("button:has-text('РџСЂРёРјРµРЅРёС‚СЊ')")
            apply_box = apply.first.bounding_box() if apply.count() > 0 else None
        except Exception:
            apply_box = None

        scroll_box = None
        if scrollable is not None:
            try:
                scroll_box = scrollable.bounding_box()
            except Exception:
                scroll_box = None

        candidates = panel.locator("input, textarea, input[type='text'], input:not([type])")
        try:
            count = min(candidates.count(), 120)
        except Exception:
            count = 0

        best_locator: Locator | None = None
        best_y = -1.0

        for idx in range(count):
            item = candidates.nth(idx)
            try:
                if not item.is_visible(timeout=300):
                    continue
            except Exception:
                continue

            try:
                if not item.is_enabled(timeout=300):
                    continue
            except Exception:
                continue

            try:
                box = item.bounding_box()
            except Exception:
                box = None
            if not box:
                continue

            y = float(box.get("y", 0))
            h = float(box.get("height", 0))
            if h < 10:
                continue

            if scroll_box is not None:
                scroll_top = float(scroll_box.get("y", 0))
                scroll_h = float(scroll_box.get("height", 0))
                if y < scroll_top + scroll_h * 0.45:
                    continue

            if apply_box is not None:
                apply_y = float(apply_box.get("y", 0))
                if y > apply_y + 40:
                    continue

            if y > best_y:
                if self._is_probably_tag_input(item, panel):
                    best_y = y
                    best_locator = item

        return best_locator

    def _log_bottom_input_candidates(self, panel: Locator, scrollable: Locator | None) -> None:
        """Log DOM attrs for last few bottom input candidates to debug hidden tag field."""
        try:
            candidates = panel.locator("input, textarea, input[type='text'], input:not([type])")
            count = min(candidates.count(), 120)
        except Exception:
            self.logger.info("Bottom input candidate debug: no candidates available.")
            return

        rows: list[dict[str, object]] = []
        for idx in range(count):
            item = candidates.nth(idx)
            try:
                attrs = item.evaluate(
                    """el => ({
                        tagName: (el.tagName || '').toLowerCase(),
                        type: (el.getAttribute('type') || ''),
                        placeholder: (el.getAttribute('placeholder') || ''),
                        value: (el.value || ''),
                        ariaLabel: (el.getAttribute('aria-label') || ''),
                        name: (el.getAttribute('name') || ''),
                        className: (el.className || '').toString()
                    })"""
                )
            except Exception:
                continue
            try:
                box = item.bounding_box()
            except Exception:
                box = None
            if not isinstance(attrs, dict):
                continue
            row = dict(attrs)
            row["bbox"] = box
            row["tagSignals"] = any(
                s in (
                    f"{row.get('placeholder', '')} {row.get('ariaLabel', '')} "
                    f"{row.get('name', '')} {row.get('className', '')}"
                ).lower()
                for s in ("С‚РµРі", "С‚РµРіРё", "tag", "tags", "РјРµС‚РєР°", "РјРµС‚РєРё")
            )
            rows.append(row)

        rows_sorted = sorted(rows, key=lambda x: float((x.get("bbox") or {}).get("y", -1)), reverse=True)
        self.logger.info("Bottom input candidates (last 5): %s", rows_sorted[:5])

    def _scroll_until_tag_input_visible(self, panel: Locator, report_id: str) -> Locator:
        """Scroll and attempt to focus tag field with row-first strategy."""
        focus_target, strategy = self._focus_tag_field_via_row(panel, report_id)
        if focus_target is not None:
            self.logger.info("Tag input strategy selected: %s", strategy)
            return focus_target

        scrollable = self._find_scrollable_container(panel)
        if scrollable is not None:
            for step in range(1, 8):
                try:
                    scrollable.evaluate(
                        "el => el.scrollBy({ top: Math.max(260, Math.floor(el.clientHeight * 0.9)), behavior: 'auto' })"
                    )
                    panel.page.wait_for_timeout(220)
                except Exception:
                    break

                focus_target, strategy = self._focus_tag_field_via_row(panel, report_id)
                if focus_target is not None:
                    self.logger.info("Tag input strategy selected during scroll step=%s: %s", step, strategy)
                    return focus_target

        raise RuntimeError("Tag field was not focused after row/container search and fallback strategies.")

    def _select_filter_kind(self, page: Page, source_kind: SourceKind, report_id: str) -> None:
        panel = self._find_filter_panel_container(page)

        if source_kind == "tag":
            if self.tag_selection_mode == "script":
                self.logger.info("generic_ranked_path_skipped=true")
                self.logger.info("tag_path_used=holder_popup")
                holder, _holder_id = self._find_strict_tag_holder(panel)
                if holder is None:
                    raise RuntimeError("Tag holder not found for script holder_popup path")
                click_target = holder
                click_target_name = "holder"
                candidates = (
                    ("inner_multisuggest", "div.multisuggest.filter-tags-items.js-multisuggest.js-can-add"),
                    ("filter_search_tags", "div.filter-search__tags"),
                )
                for name, sel in candidates:
                    try:
                        loc = holder.locator(sel)
                        if loc.count() > 0 and loc.first.is_visible(timeout=250):
                            click_target = loc.first
                            click_target_name = name
                            break
                    except Exception:
                        continue
                self.logger.info("holder_click_target=%s", click_target_name)
                try:
                    click_target.click(timeout=1200)
                except Exception:
                    try:
                        holder.click(timeout=1200)
                    except Exception:
                        pass
                self._debug_screenshot(page, f"profile_{report_id}_02b_tag_search_state")
                return
            try:
                holder, holder_id = self._find_strict_tag_holder(panel)
                focus_target: Locator | None = None
                strategy = "strict_tag_holder"
                if holder is not None:
                    focus_target = self._find_tag_input_in_holder(holder, holder_id)
                if focus_target is None:
                    focus_target, strategy = self._focus_tag_field_via_row(panel, report_id)
                if focus_target is None:
                    focus_target = self._scroll_until_tag_input_visible(panel, report_id)
                    strategy = "fallback_generic"

                self._debug_screenshot(page, f"profile_{report_id}_02b_tag_search_state")
                try:
                    focus_target.click(timeout=1500)
                except Exception:
                    pass
                self.logger.info("Tag input detected and focused strategy=%s payload=%s", strategy, self._element_debug_payload(focus_target))
                return
            except RuntimeError as exc:
                self._debug_screenshot(page, f"profile_{report_id}_02b_tag_search_state")
                (
                    text_path,
                    selectors_path,
                    panel_labels,
                    panel_counts,
                    step_paths,
                    merged_path,
                    changed_flags,
                    found_scrollable,
                    found_targets,
                ) = self._collect_filter_panel_diagnostics(page, report_id)

                self.logger.info("Filter panel debug text dump: %s", text_path)
                self.logger.info("Filter panel debug selectors dump: %s", selectors_path)
                self.logger.info(
                    "Filter panel scroll steps=%s scrollable_found=%s text_changed_flags=%s",
                    len(step_paths),
                    found_scrollable,
                    changed_flags,
                )
                self.logger.info("Filter panel scroll step dumps: %s", [str(p) for p in step_paths])
                self.logger.info("Filter panel scroll merged dump: %s", merged_path)
                self.logger.info("Filter panel filter-like labels: %s", panel_labels[:30])
                self.logger.info("Filter panel candidate selector counts: %s", panel_counts)
                self.logger.info("Filter panel target labels found after scroll: %s", found_targets)
                raise RuntimeError(str(exc)) from exc

        if source_kind == "utm_source":
            utm_holder = self._find_utm_source_holder(panel)
            if utm_holder is None:
                try:
                    utm_holder = self._find_utm_source_holder(page.locator("body"))
                except Exception:
                    utm_holder = None
            if utm_holder is not None:
                click_target = utm_holder
                try:
                    inner = utm_holder.locator(".filter-search__tags")
                    if inner.count() > 0 and inner.first.is_visible(timeout=200):
                        click_target = inner.first
                except Exception:
                    pass
                try:
                    click_target.click(timeout=1200)
                except Exception:
                    try:
                        utm_holder.click(timeout=1200)
                    except Exception:
                        pass
                self.logger.info("Filter source selected: source_kind=utm_source strategy=holder_popup")
                page.wait_for_timeout(250)
                return

            label_item, _label_payload = self._find_utm_label_item(panel)
            if label_item is not None:
                activated = False
                try:
                    label_item.click(timeout=1200)
                    activated = True
                except Exception:
                    activated = False
                self.logger.info("utm_filter_kind_activation=label_item_click")
                self.logger.info("utm_filter_kind_activation_success=%s", str(bool(activated)).lower())
                page.wait_for_timeout(220)

                holder_after = self._find_utm_source_holder(panel)
                if holder_after is None:
                    utm_target, _utm_target_payload = self._resolve_utm_click_target_from_labels(
                        page=page,
                        panel=panel,
                        report_id=report_id,
                    )
                    if utm_target is not None:
                        self.logger.info("Filter source selected: source_kind=utm_source strategy=post_activation_row_target")
                        return

                if holder_after is not None:
                    self.logger.info("Filter source selected: source_kind=utm_source strategy=label_activation_holder_ready")
                    return

                self.logger.warning("UTM source label item found, but control did not appear after activation click")
                self.logger.info("UTM label presence confirmed; defer exact open/apply to UTM filter handler")
                return

            if self._has_strong_utm_label_presence(panel):
                self.logger.info("UTM label presence confirmed; defer exact open/apply to UTM filter handler")
                return

        (
            text_path,
            selectors_path,
            panel_labels,
            panel_counts,
            step_paths,
            merged_path,
            changed_flags,
            found_scrollable,
            found_targets,
        ) = self._collect_filter_panel_diagnostics(page, report_id)

        self.logger.info("Filter panel debug text dump: %s", text_path)
        self.logger.info("Filter panel debug selectors dump: %s", selectors_path)
        self.logger.info(
            "Filter panel scroll steps=%s scrollable_found=%s text_changed_flags=%s",
            len(step_paths),
            found_scrollable,
            changed_flags,
        )
        self.logger.info("Filter panel scroll step dumps: %s", [str(p) for p in step_paths])
        self.logger.info("Filter panel scroll merged dump: %s", merged_path)
        self.logger.info("Filter panel filter-like labels: %s", panel_labels[:30])
        self.logger.info("Filter panel candidate selector counts: %s", panel_counts)
        self.logger.info("Filter panel target labels found after scroll: %s", found_targets)

        if not found_targets:
            raise RuntimeError(
                "Could not detect target labels in scrolled filter panel content "
                f"for source_kind={source_kind}. Check dumps: {merged_path} and step files in exports/debug/."
            )

        labels = self.FILTER_KIND_LABELS[source_kind]
        for label in labels:
            candidates = [
                page.get_by_text(label, exact=True),
                page.locator(f"[role='button']:has-text('{label}')"),
                page.locator(f"label:has-text('{label}')"),
                page.locator(f"div:has-text('{label}')"),
            ]

            for locator in candidates:
                try:
                    if locator.count() > 0:
                        locator.first.click(timeout=2500)
                        self.logger.info("Filter source selected: source_kind=%s label=%s", source_kind, label)
                        page.wait_for_timeout(500)
                        return
                except Exception:
                    continue

        raise RuntimeError(f"Could not select filter source_kind={source_kind}. Labels not found in UI.")

    def _apply_supported_filter(
        self,
        page: Page,
        report_id: str,
        filter_key: str,
        values: list[str],
        operator: str = "=",
    ) -> bool:
        handler = self._filter_registry.get(filter_key)
        if handler is None:
            self.logger.warning("Unsupported filter handler requested: %s", filter_key)
            return False

        applied = bool(handler.apply(self, page, report_id, values, operator=operator))
        if not applied:
            ctx = handler.debug_dump(self, page, report_id, reason="apply_failed", extra={"filter_key": filter_key, "values": values, "operator": operator})
            self.logger.warning("filter handler apply failed: key=%s diagnostics=%s artifacts=%s", filter_key, ctx.diagnostics, ctx.artifacts)
            return False

        verified = bool(handler.verify(self, page, report_id, values, operator=operator))
        if not verified:
            ctx = handler.debug_dump(self, page, report_id, reason="verify_failed", extra={"filter_key": filter_key, "values": values, "operator": operator})
            self.logger.warning("filter handler verify failed: key=%s diagnostics=%s artifacts=%s", filter_key, ctx.diagnostics, ctx.artifacts)
            return False

        self.logger.info("filter handler applied: key=%s operator=%s values=%s", filter_key, operator, values)
        return True

    def _apply_filter_values(
        self,
        page: Page,
        report_id: str,
        source_kind: SourceKind,
        values: list[str],
        operator: str = "=",
    ) -> None:
        if not values:
            raise RuntimeError("Filter values are empty. Provide at least one value in report profile.")

        panel = self._find_filter_panel_container(page)

        if source_kind == "tag":
            if self.tag_selection_mode == "external_agent":
                self.logger.info("External agent tag flow started")
                target_value = values[0].strip()
                if not target_value:
                    raise RuntimeError("First tag filter value is empty.")

                url_before = str(page.url)
                handoff_json_path = self._prepare_external_agent_handoff(page, panel, target_value, report_id)
                self.logger.info("External agent handoff issued: %s", handoff_json_path)
                bridge_used = self._run_external_agent_bridge(
                    handoff_json_path=handoff_json_path,
                    target_value=target_value,
                    url_before=url_before,
                )
                if bridge_used:
                    self.logger.info("External agent handoff confirmed by bridge command")
                else:
                    self._wait_for_agent_or_manual_confirmation()
                page.wait_for_timeout(250)
                panel = self._find_filter_panel_container(page)

                if not self._is_filter_apply_confirmed(page, panel, url_before, target_value):
                    self.logger.error("External agent handoff failed")
                    raise RuntimeError(
                        "External agent handoff failed: filter apply not confirmed. "
                        "Select exact tag option, click gray area, click ?????????, then press Enter."
                    )

                self.logger.info("External agent handoff confirmed")
                self.logger.info("Filter apply confirmed after external agent step")
                self._apply_already_confirmed = True
                return

            if self.tag_selection_mode == "agent_assisted":
                target_value = values[0].strip()
                if not target_value:
                    raise RuntimeError("First tag filter value is empty.")
                url_before = str(page.url)
                self._prepare_tag_dropdown_for_agent(page, panel, target_value, report_id)
                self._wait_for_agent_or_manual_confirmation()
                page.wait_for_timeout(250)
                if not self._is_filter_apply_confirmed(page, panel, url_before, target_value):
                    raise RuntimeError(
                        "Filter apply was not confirmed after agent/manual step. "
                        "Please select exact tag, click gray area, click '?????????', then press Enter."
                    )
                self.logger.info("Filter apply confirmed after agent/manual step")
                self._apply_already_confirmed = True
                return

            if self.tag_selection_mode == "script":
                self.logger.info("generic_ranked_path_skipped=true")
                applied = self._apply_supported_filter(
                    page=page,
                    report_id=report_id,
                    filter_key="tag",
                    values=values,
                    operator="=",
                )
                if applied:
                    return
                self.logger.error("final_fail_reason=holder_popup_tag_selection_failed")
                raise RuntimeError("Tag holder popup flow failed in script mode.")

            target_value = values[0].strip()
            if not target_value:
                raise RuntimeError("First tag filter value is empty.")

            tag_holder, tag_holder_multisuggest_id = self._find_strict_tag_holder(panel)
            focus_target: Locator | None = None
            selector_strategy = "strict_tag_holder"

            if tag_holder is None:
                raise RuntimeError("Tag holder not found: expected div.filter-search__tags-holder[data-title='????'][data-input-name='tag[]']")

            try:
                tag_holder.click(timeout=1200)
            except Exception:
                pass
            page.wait_for_timeout(140)

            active_popup, _active_popup_payload = self._find_active_tag_popup(panel, tag_holder)
            focus_target = self._find_tag_input_in_popup(active_popup) if active_popup is not None else None
            selector_strategy = "strict_tag_holder_active_popup"

            if focus_target is None:
                self.logger.info("fallback_to_lower_row reason=active_popup_or_input_missing_after_first_click")
                retry_clicks: list[tuple[str, Locator | None]] = []
                try:
                    tags_area = tag_holder.locator(".filter-search__tags")
                    retry_clicks.append(("holder_tags_area", tags_area.first if tags_area.count() > 0 else None))
                except Exception:
                    retry_clicks.append(("holder_tags_area", None))
                retry_clicks.append(("holder_root", tag_holder))

                for retry_name, retry_target in retry_clicks:
                    if retry_target is not None:
                        try:
                            retry_target.click(timeout=1000)
                        except Exception:
                            pass
                    if active_popup is not None:
                        try:
                            input_row = active_popup.locator("li.multisuggest__list-item_input")
                            if input_row.count() > 0:
                                input_row.first.click(timeout=700)
                        except Exception:
                            pass
                    page.wait_for_timeout(180)
                    active_popup, _active_popup_payload = self._find_active_tag_popup(panel, tag_holder)
                    focus_target = self._find_tag_input_in_popup(active_popup) if active_popup is not None else None
                    if focus_target is not None:
                        self.logger.info("tag_popup_retry_success strategy=%s", retry_name)
                        break

            if focus_target is None:
                raise RuntimeError("Tag holder found, but active multisuggest popup input was not found.")

            url_before = str(page.url)

            try:
                self.logger.info("Tag target value: %s", target_value)
                self.logger.info("Tag selector strategy: %s", selector_strategy)
                if focus_target is not None:
                    self.logger.info("Tag focus target payload: %s", self._element_debug_payload(focus_target))

                if focus_target is None:
                    raise RuntimeError("Tag focus target is empty.")

                click_ok = False
                try:
                    focus_target.click(timeout=1500)
                    click_ok = True
                except Exception:
                    click_ok = False
                self.logger.info("tag_primary_input_click success=%s", str(click_ok).lower())
                page.wait_for_timeout(180)

                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.keyboard.type(target_value, delay=20)
                self.logger.info('tag_primary_input_fill typed="%s"', target_value)
                self.logger.info("typed_value=%s", target_value)
                page.wait_for_timeout(260)

                postfill_value = self._read_input_value(focus_target)
                self.logger.info('tag_primary_input_postfill value="%s"', postfill_value)

                popup_multisuggest_id = None
                try:
                    if active_popup is not None:
                        popup_multisuggest_id = (active_popup.get_attribute("data-multisuggest-id") or "").strip() or None
                except Exception:
                    popup_multisuggest_id = None
                popup_id_matches_holder = (
                    not popup_multisuggest_id
                    or not tag_holder_multisuggest_id
                    or popup_multisuggest_id == tag_holder_multisuggest_id
                )
                self.logger.info("popup_multisuggest_id=%s", popup_multisuggest_id or "")
                self.logger.info("popup_id_matches_holder=%s", str(bool(popup_id_matches_holder)).lower())

                visible_suggestion_texts = self._collect_visible_tag_suggestion_texts(
                    panel,
                    popup=active_popup,
                )
                self.logger.info("visible_suggestion_texts=%s", visible_suggestion_texts[:20])
                self.logger.info("Tag confirm via Enter")
                page.keyboard.press("Enter")
                page.wait_for_timeout(220)

                page.keyboard.press("Escape")
                self.logger.info("Tag dropdown closed via Escape")

                dropdown_closed = self._wait_for_tag_dropdown_hidden(panel, timeout_ms=3500)
                apply_state = self._wait_for_apply_button_ready(panel, timeout_ms=2000)
                chip_present = self._has_selected_tag_chip(panel, target_value, holder=tag_holder)
                chip_texts_after_selection = self._collect_tag_chip_texts(panel, holder=tag_holder)
                self.logger.info("chip_texts_after_selection=%s", chip_texts_after_selection[:20])
                self.logger.info(
                    "Apply button visible/enabled: visible=%s enabled=%s",
                    apply_state["visible"],
                    apply_state["enabled"],
                )
                self.logger.info("apply_visible=%s apply_enabled=%s", apply_state["visible"], apply_state["enabled"])

                if chip_present:
                    self.logger.info("tag_selection_success=true")
                    self.logger.info("Tag selected successfully (chip detected)")
                else:
                    self.logger.warning(
                        "Keyboard tag selection path incomplete; switching to dropdown click fallback. "
                        "dropdown_closed=%s apply_visible=%s apply_enabled=%s",
                        str(dropdown_closed).lower(),
                        apply_state["visible"],
                        apply_state["enabled"],
                    )
                    option_clicked, option_strategy, option_payload = self._select_real_tag_option(panel, target_value, holder=tag_holder, expected_multisuggest_id=popup_multisuggest_id)
                    self.logger.info("Tag option click confirmed: %s", str(option_clicked).lower())
                    if option_payload:
                        self.logger.info("Tag real option candidate payload: %s", option_payload)
                    if option_clicked:
                        self.logger.info("Tag real option selected: %s", target_value)
                        self.logger.info("Tag real option selector used: %s", option_strategy)

                    if not option_clicked:
                        self.logger.error("tag_selection_success=false fail_reason=chip_not_present_and_exact_suggestion_not_selected")
                        raise RuntimeError(
                            "Tag selection failed in script mode: Enter+Esc did not confirm chip and "
                            "dropdown fallback could not select option."
                        )

                    finalize_strategy, dropdown_closed, chip_present, apply_state = self._finalize_tag_selection_after_option_click(
                        page=page,
                        panel=panel,
                        tag_focus_target=focus_target,
                        target_value=target_value,
                    )
                    self.logger.info("Tag finalize strategy: %s", finalize_strategy)
                    self.logger.info("Tag chip present: %s", str(chip_present).lower())
                    self.logger.info(
                        "Apply button visible/enabled: visible=%s enabled=%s",
                        apply_state["visible"],
                        apply_state["enabled"],
                    )

                    if not chip_present:
                        self.logger.error("tag_selection_success=false fail_reason=chip_not_detected_after_fallback_finalize")
                        raise RuntimeError("Tag selection failed: chip was not detected after fallback.")

                panel = self._find_filter_panel_container(page)
                self._scroll_filter_panel_to_bottom(panel)
                self.logger.info("Scrolled filter panel to bottom before apply")

                if not self._click_apply_in_panel(page, url_before=url_before, report_id=report_id):
                    panel = self._find_filter_panel_container(page)
                    self._scroll_filter_panel_to_bottom(panel)
                    self.logger.info("Scrolled filter panel to bottom before apply (retry)")
                    if not self._click_apply_in_panel(page, url_before=url_before, report_id=report_id):
                        raise RuntimeError("Tag selected successfully, but apply button could not be detected/clicked.")

                page.wait_for_timeout(700)
                panel = self._find_filter_panel_container(page)
                if not self._is_filter_apply_confirmed_by_url(page, url_before):
                    if not self._is_filter_apply_confirmed(page, panel, url_before, target_value):
                        raise RuntimeError(
                            "Filter apply was not confirmed after tag Enter+Esc flow. "
                            "Expected URL with useFilter/tag[]."
                        )

                self.logger.info("Filter apply confirmed")
                self._apply_already_confirmed = True
                self._debug_screenshot(page, f"profile_{report_id}_02c_tag_dropdown_closed")
                return
            except Exception as exc:
                (
                    text_path,
                    selectors_path,
                    _labels,
                    _counts,
                    step_paths,
                    merged_path,
                    _changed_flags,
                    _found_scrollable,
                    _found_targets,
                ) = self._collect_filter_panel_diagnostics(page, report_id)
                self.logger.error(
                    "Tag value/apply failed. debug_text=%s debug_selectors=%s merged=%s steps=%s",
                    text_path,
                    selectors_path,
                    merged_path,
                    [str(p) for p in step_paths],
                )
                raise RuntimeError(f"Could not set/apply tag filter value '{target_value}': {exc}") from exc

        if source_kind == "utm_source":
            applied = self._apply_supported_filter(
                page=page,
                report_id=report_id,
                filter_key="utm_source",
                values=values,
                operator=operator if operator in {"=", "^="} else "=",
            )
            if not applied:
                raise RuntimeError("UTM source exact filter handler failed in script flow.")
            return

        input_locator: Locator | None = None
        for selector in self.FILTER_INPUT_SELECTORS:
            locator = panel.locator(selector)
            try:
                if locator.count() > 0:
                    input_locator = locator.first
                    self.logger.info("Filter value input selected via: %s", selector)
                    break
            except Exception:
                continue

        if input_locator is None:
            raise RuntimeError("Could not find filter input field to set filter values.")

        for value in values:
            clean = value.strip()
            if not clean:
                continue

            try:
                input_locator.click(timeout=2000)
                try:
                    input_locator.fill(clean, timeout=2000)
                except Exception:
                    input_locator.press("Control+A")
                    input_locator.type(clean, delay=20)
                input_locator.press("Enter")
                self.logger.info("Filter value applied: %s", clean)
                page.wait_for_timeout(300)
            except Exception as exc:
                raise RuntimeError(f"Could not apply filter value: {clean}. Error: {exc}") from exc

    def _prepare_tag_dropdown_for_agent(self, page: Page, panel: Locator, target_value: str, report_id: str) -> None:
        """Prepare tag dropdown and pause for external/manual selection step."""
        focus_target, selector_strategy = self._focus_tag_field_via_row(panel, report_id, allow_primary=True)
        if focus_target is None:
            focus_target = self._scroll_until_tag_input_visible(panel, report_id)
            selector_strategy = "fallback_generic"

        self.logger.info("Tag selector strategy: %s", selector_strategy)
        if focus_target is not None:
            self.logger.info("Tag focus target payload: %s", self._element_debug_payload(focus_target))
            try:
                focus_target.click(timeout=1500)
            except Exception:
                pass

        page.wait_for_timeout(220)
        page.keyboard.press("Control+A")
        page.keyboard.press("Backspace")
        page.keyboard.type(target_value, delay=20)
        self.logger.info("Tag target value typed for agent/manual step: %s", target_value)

        page.wait_for_timeout(300)
        dropdown_open = self._is_tag_dropdown_open(panel)
        self._debug_screenshot(page, f"profile_{report_id}_02c_agent_tag_ready")
        self.logger.info("Tag dropdown open before agent/manual step: %s", str(dropdown_open).lower())
        self.logger.warning(
            "AGENT_STEP_REQUIRED: select exact tag option from dropdown, click gray area, click ?????????"
        )

    def _wait_for_agent_or_manual_confirmation(self) -> None:
        self.logger.info("Waiting for manual/agent confirmation before continuing")
        print()
        print("AGENT/MANUAL TAG STEP REQUIRED:")
        print("1) Select exact tag option in dropdown.")
        print("2) Click gray area to close dropdown.")
        print("3) Click '?????????' in filter panel.")
        input("After completing the steps in browser, press Enter to continue... ")

    def _wait_for_tag_dropdown_hidden(self, panel: Locator, timeout_ms: int = 3000) -> bool:
        """Wait until tag dropdown is hidden."""
        step_ms = 150
        elapsed = 0
        while elapsed <= timeout_ms:
            if not self._is_tag_dropdown_open(panel):
                return True
            panel.page.wait_for_timeout(step_ms)
            elapsed += step_ms
        return not self._is_tag_dropdown_open(panel)

    def _wait_for_apply_button_ready(self, panel: Locator, timeout_ms: int = 3000) -> dict[str, bool]:
        """Wait until panel apply button is visible/enabled."""
        step_ms = 150
        elapsed = 0
        state = self._get_panel_apply_state(panel.page, panel)
        while elapsed <= timeout_ms:
            state = self._get_panel_apply_state(panel.page, panel)
            if state["visible"] and state["enabled"]:
                return state
            panel.page.wait_for_timeout(step_ms)
            elapsed += step_ms
        return state

    def _scroll_filter_panel_to_bottom(self, panel: Locator) -> None:
        """Scroll filter panel to bottom so apply area becomes discoverable."""
        scrollable = self._find_scrollable_container(panel)
        target = scrollable or panel
        try:
            target.evaluate("el => { el.scrollTop = el.scrollHeight; }")
            panel.page.wait_for_timeout(220)
            target.evaluate("el => { el.scrollTop = el.scrollHeight; }")
            panel.page.wait_for_timeout(220)
        except Exception as exc:
            self.logger.warning("Could not force-scroll filter panel to bottom: %s", exc)

    def _is_filter_apply_confirmed_by_url(self, page: Page, url_before: str) -> bool:
        """URL confirmation for tag apply: require tag marker, plus useFilter or url change."""
        current_url = str(page.url)
        lowered_url = current_url.lower()
        url_changed = current_url != str(url_before)
        has_usefilter = "usefilter=y" in lowered_url
        has_tag_marker = "tag%5b%5d=" in lowered_url or "tag[]=" in lowered_url or "tag[" in lowered_url
        self.logger.info(
            "Filter apply URL check: url_changed=%s has_useFilter=%s has_tag_marker=%s current_url=%s",
            str(url_changed).lower(),
            str(has_usefilter).lower(),
            str(has_tag_marker).lower(),
            current_url,
        )
        return has_tag_marker and (has_usefilter or url_changed)

    def _run_external_agent_bridge(self, handoff_json_path: Path, target_value: str, url_before: str) -> bool:
        """Run external bridge command for tag selection step. Return True if bridge was executed."""
        cmd = self.external_agent_bridge_cmd or str(os.getenv("EXTERNAL_AGENT_BRIDGE_CMD", "")).strip()
        if not cmd:
            self.logger.info("External agent bridge command is not set; fallback to manual confirmation.")
            return False

        self.logger.info("External agent bridge command started.")
        env = os.environ.copy()
        env["EXTERNAL_AGENT_HANDOFF_PATH"] = str(handoff_json_path)
        env["EXTERNAL_AGENT_TARGET_VALUE"] = target_value
        env["EXTERNAL_AGENT_URL_BEFORE"] = url_before
        env["EXTERNAL_AGENT_CDP_URL"] = str(os.getenv("OPENCLAW_CDP_URL", "http://127.0.0.1:18800"))

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                cwd=str(self.project_root),
                env=env,
                text=True,
                capture_output=True,
                timeout=self.external_agent_bridge_timeout_sec,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"External agent bridge timed out after {self.external_agent_bridge_timeout_sec}s: {cmd}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"External agent bridge failed to start: {cmd}. Error: {exc}") from exc

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if stdout:
            self.logger.info("External agent bridge stdout: %s", stdout[:1200])
        if stderr:
            self.logger.warning("External agent bridge stderr: %s", stderr[:1200])

        if result.returncode != 0:
            raise RuntimeError(
                "External agent handoff failed: bridge command returned non-zero exit code "
                f"{result.returncode}. Command: {cmd}"
            )

        self.logger.info("External agent bridge finished successfully.")
        return True

    def _is_filter_apply_confirmed(self, page: Page, panel: Locator, url_before: str, target_value: str) -> bool:
        if self._is_filter_apply_confirmed_by_url(page, url_before):
            return True

        dropdown_closed = not self._is_tag_dropdown_open(panel)
        chip_present = self._has_selected_tag_chip(panel, target_value)
        apply_state = self._get_panel_apply_state(page, panel)

        try:
            panel_visible = panel.is_visible(timeout=200)
        except Exception:
            panel_visible = False

        self.logger.info(
            "Post-confirmation state: dropdown_closed=%s chip_present=%s apply_visible=%s apply_enabled=%s panel_visible=%s",
            str(dropdown_closed).lower(),
            str(chip_present).lower(),
            apply_state["visible"],
            apply_state["enabled"],
            str(panel_visible).lower(),
        )

        # Fallback state-change confirmation when URL markers are delayed.
        return chip_present and dropdown_closed and (not panel_visible)

    def _build_external_agent_handoff_context(
        self,
        page: Page,
        panel: Locator,
        report_id: str,
        target_value: str,
        screenshot_path: Path,
    ) -> dict[str, object]:
        dropdown_open = self._is_tag_dropdown_open(panel)
        active_element = page.evaluate(
            """() => {
                const el = document.activeElement;
                if (!el) return null;
                return {
                    tagName: (el.tagName || '').toLowerCase(),
                    type: (el.getAttribute('type') || ''),
                    name: (el.getAttribute('name') || ''),
                    className: (el.className || '').toString(),
                    placeholder: (el.getAttribute('placeholder') || ''),
                    ariaLabel: (el.getAttribute('aria-label') || '')
                };
            }"""
        )

        tag_row_payload: dict[str, object] | None = None
        tag_row = self._find_tag_row_below_reference(panel)
        if tag_row is not None:
            tag_row_payload = self._element_debug_payload(tag_row)

        return {
            "event": "EXTERNAL_AGENT_HANDOFF_READY",
            "report_id": report_id,
            "target_value": target_value,
            "current_url": str(page.url),
            "dropdown_open": dropdown_open,
            "screenshot_path": str(screenshot_path),
            "tag_row_payload": tag_row_payload,
            "active_element": active_element,
            "required_actions": [
                "select exact tag option equal to target_value",
                "click gray area to close dropdown",
                "click ?????????",
            ],
        }

    def _save_external_agent_handoff_context(self, report_id: str, context: dict[str, object]) -> Path:
        return analytics_save_external_agent_handoff_context(
            exports_dir=self.reader.settings.exports_dir,
            project_root=self.project_root,
            report_id=report_id,
            context=context,
        )

    def _prepare_external_agent_handoff(self, page: Page, panel: Locator, target_value: str, report_id: str) -> Path:
        self._prepare_tag_dropdown_for_agent(page, panel, target_value, report_id)
        screenshot_path = self._debug_screenshot(page, f"profile_{report_id}_02c_agent_tag_ready")
        context = self._build_external_agent_handoff_context(
            page=page,
            panel=panel,
            report_id=report_id,
            target_value=target_value,
            screenshot_path=screenshot_path,
        )
        context_path = self._save_external_agent_handoff_context(report_id, context)
        self.logger.warning("Tag selection mode: external_agent")
        self.logger.warning("EXTERNAL_AGENT_HANDOFF_READY")
        self.logger.warning(
            "Agent handoff context: tag dropdown is open, target value typed, waiting for external agent integration"
        )
        self.logger.info("External agent handoff screenshot: %s", screenshot_path)
        self.logger.info("External agent handoff json: %s", context_path)
        return context_path

    def _click_apply(self, page: Page) -> None:
        if self._apply_already_confirmed:
            self.logger.info("Skipping apply click: already confirmed in agent/manual step")
            return

        if self._click_apply_in_panel(page):
            return

        for selector in self.APPLY_SELECTORS:
            locator = page.locator(selector)
            try:
                if locator.count() > 0:
                    locator.first.click(timeout=3000)
                    self.logger.info("Filter applied via global selector: %s", selector)
                    return
            except Exception:
                continue

        raise RuntimeError("Could not click filter apply button.")


    @staticmethod
    def _normalize_filter_text(value: str) -> str:
        return " ".join((value or "").strip().lower().replace("?", "?").split())

    @staticmethod
    def _is_strong_utm_label_text(value: str) -> bool:
        normalized = re.sub(r"[^a-z0-9]", "", str(value or "").lower())
        if not normalized:
            return False
        return "utmsource" in normalized

    @staticmethod
    def _is_broad_row_container(payload: dict[str, object], row_text: str) -> bool:
        cls = str(payload.get("className", "") or "").lower()
        text = str(row_text or "")
        line_count = len([ln for ln in text.splitlines() if ln.strip()])
        char_count = len(text.strip())
        broad_tokens = (
            "form",
            "wrapper",
            "panel",
            "content",
            "body",
            "settings",
            "custom_settings",
            "scroll",
            "overlay",
            "modal",
            "drawer",
        )
        token_hit = any(tok in cls for tok in broad_tokens)
        too_large = line_count > 12 or char_count > 900
        return bool(token_hit and too_large)

    def _save_utm_click_debug_artifacts(
        self,
        report_id: str,
        row_container_html: str,
        candidates: list[dict[str, object]],
    ) -> tuple[Path, Path]:
        return analytics_save_utm_click_debug_artifacts(
            exports_dir=self.reader.settings.exports_dir,
            project_root=self.project_root,
            report_id=report_id,
            row_container_html=row_container_html,
            candidates=candidates,
        )


    def _has_strong_utm_label_presence(self, panel: Locator) -> bool:
        selectors = (
            "[data-input-name*='utm_source' i]",
            "[data-title*='utm_source' i]",
            "*:has-text('utm_source')",
            "*:has-text('UTM_SOURCE')",
        )
        for selector in selectors:
            loc = panel.locator(selector)
            try:
                count = min(loc.count(), 30)
            except Exception:
                continue
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=100):
                        continue
                    payload = self._element_debug_payload(item)
                except Exception:
                    continue
                text = str(payload.get("text", "") or "")
                attrs = " ".join([
                    str(item.get_attribute("data-input-name") or ""),
                    str(item.get_attribute("data-title") or ""),
                    text,
                ])
                if self._is_strong_utm_label_text(attrs):
                    return True
        return False

    def _find_utm_label_item(self, panel: Locator) -> tuple[Locator | None, dict[str, object] | None]:
        selectors = (
            "div.filter__custom_settings__item:has-text('utm_source')",
            "div.filter__custom_settings__item:has-text('UTM_SOURCE')",
            "[data-input-name*='utm_source' i]",
            "[data-title*='utm_source' i]",
            "*:has-text('utm_source')",
            "*:has-text('UTM_SOURCE')",
        )

        best_item: Locator | None = None
        best_payload: dict[str, object] | None = None
        best_area = 10**12

        for selector in selectors:
            try:
                loc = panel.locator(selector)
                count = min(loc.count(), 40)
            except Exception:
                continue
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=120):
                        continue
                    payload = self._element_debug_payload(item)
                except Exception:
                    continue
                text = str(payload.get("text", "") or "")
                attrs = " ".join(
                    [
                        str(item.get_attribute("data-input-name") or ""),
                        str(item.get_attribute("data-title") or ""),
                        str(payload.get("className", "") or ""),
                        text,
                    ]
                )
                if not self._is_strong_utm_label_text(attrs):
                    continue
                bbox = payload.get("bbox")
                area = 10**12
                if isinstance(bbox, dict):
                    area = float(bbox.get("width", 0.0) or 0.0) * float(bbox.get("height", 0.0) or 0.0)
                if area < best_area:
                    best_area = area
                    best_item = item
                    best_payload = payload

        self.logger.info("utm_label_item_found=%s", str(best_item is not None).lower())
        self.logger.info("utm_label_item_payload=%s", best_payload)
        return best_item, best_payload



    @staticmethod
    def _pick_utm_row_candidate(candidates: list[dict[str, object]]) -> dict[str, object] | None:
        if not candidates:
            return None

        strategy_rank = {
            "self": 0,
            "parent": 1,
            "grandparent": 2,
            "shared_parent": 3,
        }

        ranked: list[tuple[int, float, dict[str, object]]] = []
        for cand in candidates:
            if not bool(cand.get("strong_label", False)):
                continue
            if not bool(cand.get("has_control", False)):
                continue
            if bool(cand.get("broad", False)):
                continue

            strategy = str(cand.get("strategy", "") or "")
            rank = strategy_rank.get(strategy, 9)
            area = float(cand.get("area", 10**12) or 10**12)
            ranked.append((rank, area, cand))

        if not ranked:
            return None
        ranked.sort(key=lambda x: (x[0], x[1]))
        return ranked[0][2]

    def _resolve_utm_click_target_from_labels(
        self,
        page: Page,
        panel: Locator,
        report_id: str,
    ) -> tuple[Locator | None, dict[str, object] | None]:
        label_selectors = (
            "[data-input-name*='utm_source' i]",
            "[data-title*='utm_source' i]",
            "*:has-text('utm_source')",
            "*:has-text('UTM_SOURCE')",
            "label:has-text('utm_source')",
            "span:has-text('utm_source')",
            "div.filter__custom_settings__item:has-text('utm_source')",
        )

        label_nodes: list[tuple[Locator, str, dict[str, object]]] = []
        seen_keys: set[str] = set()
        for selector in label_selectors:
            loc = panel.locator(selector)
            try:
                count = min(loc.count(), 40)
            except Exception:
                continue
            for idx in range(count):
                node = loc.nth(idx)
                try:
                    if not node.is_visible(timeout=120):
                        continue
                    payload = self._element_debug_payload(node)
                except Exception:
                    continue
                node_text = str(payload.get("text", "") or "").strip()
                attrs = " ".join([
                    str(node.get_attribute("data-input-name") or ""),
                    str(node.get_attribute("data-title") or ""),
                    node_text,
                ])
                if not self._is_strong_utm_label_text(attrs):
                    continue
                key = f"{selector}|{node_text}|{(payload.get('bbox') or {}).get('y')}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                label_nodes.append((node, node_text, payload))

        self.logger.info("utm_label_nodes_found_count=%s", len(label_nodes))
        for _, node_text, payload in label_nodes[:20]:
            self.logger.info("utm_label_node_text=%s payload=%s", node_text[:220], payload)

        click_candidate_payloads: list[dict[str, object]] = []
        row_html_debug: list[str] = []

        chosen_row_locator: Locator | None = None
        chosen_label_text = ""

        for node, node_text, _node_payload in label_nodes:
            row_candidates: list[dict[str, object]] = []

            strategy_steps: list[tuple[str, str]] = [
                ("self", "xpath=."),
                ("parent", "xpath=ancestor::*[1]"),
                ("grandparent", "xpath=ancestor::*[2]"),
                ("shared_parent", "xpath=ancestor::*[contains(@class,'filter__custom_settings__row') or contains(@class,'filter__custom_settings')][1]"),
            ]

            for strategy, sel in strategy_steps:
                try:
                    cand_loc = node.locator(sel)
                    if cand_loc.count() <= 0 or not cand_loc.first.is_visible(timeout=120):
                        continue
                    cand = cand_loc.first
                    payload = self._element_debug_payload(cand)
                    class_name = str(payload.get("className", "") or "")
                    text_preview = str(payload.get("text", "") or "")[:400]
                    attrs = " ".join([
                        str(cand.get_attribute("data-input-name") or ""),
                        str(cand.get_attribute("data-title") or ""),
                        class_name,
                        text_preview,
                    ])
                    strong_label = self._is_strong_utm_label_text(attrs)
                    broad = self._is_broad_row_container(payload, text_preview)

                    control_selectors = (
                        ".filter-search__tags",
                        ".js-multisuggest",
                        "input.multisuggest__input.js-multisuggest-input",
                        "li.multisuggest__list-item_input input",
                        ".filter__custom_input.text-input",
                        "input.filter__custom_input",
                        ".filter__custom_settings__item__value-wrapper input",
                        "input[type='text']",
                    )
                    has_control = False
                    for csel in control_selectors:
                        try:
                            cl = cand.locator(csel)
                            if cl.count() > 0 and cl.first.is_visible(timeout=80):
                                has_control = True
                                break
                        except Exception:
                            continue

                    area = 10**12
                    bbox = payload.get("bbox")
                    if isinstance(bbox, dict):
                        area = float(bbox.get("width", 0.0) or 0.0) * float(bbox.get("height", 0.0) or 0.0)

                    row_candidates.append(
                        {
                            "strategy": strategy,
                            "locator": cand,
                            "class_name": class_name,
                            "text_preview": text_preview,
                            "strong_label": strong_label,
                            "has_control": has_control,
                            "broad": broad,
                            "area": area,
                            "label_text": node_text,
                        }
                    )
                except Exception:
                    continue

            chosen = self._pick_utm_row_candidate(row_candidates)
            if chosen is None:
                self.logger.info("utm_row_container_found=false")
                continue

            row_locator = chosen["locator"]
            row_strategy = str(chosen.get("strategy", "") or "")
            row_class = str(chosen.get("class_name", "") or "")
            row_preview = str(chosen.get("text_preview", "") or "")

            self.logger.info("utm_row_container_found=true")
            self.logger.info("utm_row_container_strategy=%s", row_strategy)
            self.logger.info("utm_row_container_class=%s", row_class)
            self.logger.info("utm_row_container_text_preview=%s", row_preview[:240])

            try:
                row_outer_html = str(row_locator.evaluate("el => (el.outerHTML || '').slice(0, 2400)") or "")
            except Exception:
                row_outer_html = ""
            self.logger.info("utm_row_container_outer_html_snippet=%s", row_outer_html.replace("\n", " "))
            row_html_debug.append(f"label={node_text}\n{row_outer_html}")

            chosen_row_locator = row_locator
            chosen_label_text = str(chosen.get("label_text", node_text) or node_text)
            break

        if chosen_row_locator is None:
            html_path, json_path = self._save_utm_click_debug_artifacts(
                report_id=report_id,
                row_container_html="\n\n".join(row_html_debug),
                candidates=click_candidate_payloads,
            )
            self.logger.info("utm_click_target_resolved=false")
            self.logger.info("UTM row/container debug html: %s", html_path)
            self.logger.info("UTM click candidates json: %s", json_path)
            return None, None

        row_multisuggest_id = self._locator_multisuggest_id(chosen_row_locator)
        best_target: Locator | None = None
        best_payload: dict[str, object] | None = None
        target_selectors = (
            ".filter-search__tags",
            ".js-multisuggest",
            "input.multisuggest__input.js-multisuggest-input",
            "li.multisuggest__list-item_input input",
            ".filter__custom_input.text-input",
            "input.filter__custom_input",
            ".filter__custom_settings__item__value-wrapper input",
            "input[type='text']",
        )

        for selector in target_selectors:
            loc = chosen_row_locator.locator(selector)
            try:
                count = min(loc.count(), 20)
            except Exception:
                continue
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=120):
                        continue
                    payload = self._element_debug_payload(item)
                    clickable = bool(item.is_enabled(timeout=120))
                except Exception:
                    continue
                candidate_payload = {
                    "selector": selector,
                    "tagName": payload.get("tagName"),
                    "className": payload.get("className"),
                    "text": payload.get("text"),
                    "bbox": payload.get("bbox"),
                    "clickable": clickable,
                    "nearest_label_text": chosen_label_text,
                }
                click_candidate_payloads.append(candidate_payload)
                if clickable:
                    best_target = item
                    best_payload = candidate_payload
                    break
            if best_target is not None:
                break

        if best_target is None:
            html_path, json_path = self._save_utm_click_debug_artifacts(
                report_id=report_id,
                row_container_html="\n\n".join(row_html_debug),
                candidates=click_candidate_payloads,
            )
            self.logger.info("utm_click_target_resolved=false")
            self.logger.info("UTM row/container debug html: %s", html_path)
            self.logger.info("UTM click candidates json: %s", json_path)
            return None, {
                "row_container": chosen_row_locator,
                "row_multisuggest_id": row_multisuggest_id,
                "nearest_label_text": chosen_label_text,
                "direct_input_candidate_found": False,
            }

        self.logger.info("utm_click_target_selector=%s", str((best_payload or {}).get("selector", "")))
        self.logger.info("utm_click_target_resolved=true")
        self.logger.info("utm_click_target_payload=%s", best_payload)
        return best_target, {
            **(best_payload or {}),
            "row_container": chosen_row_locator,
            "row_multisuggest_id": row_multisuggest_id,
        }

    def _locator_multisuggest_id(self, locator: Locator | None) -> str:
        if locator is None:
            return ""
        try:
            return str(
                locator.evaluate("el => { const n = el.closest('[data-multisuggest-id]'); return n ? (n.getAttribute('data-multisuggest-id') || '') : ''; }")
                or ""
            ).strip()
        except Exception:
            return ""

    def _resolve_utm_row_context(
        self,
        page: Page,
        panel: Locator,
        report_id: str,
    ) -> tuple[Locator | None, Locator | None, dict[str, object] | None]:
        target, payload = self._resolve_utm_click_target_from_labels(page=page, panel=panel, report_id=report_id)
        row_container_from_payload = None
        if isinstance(payload, dict):
            row_container_from_payload = payload.get("row_container")
            if not isinstance(row_container_from_payload, Locator):
                row_container_from_payload = None

        row_container: Locator | None = None
        row_context_source = "none"

        # Prefer wrapper row from resolver payload when available.
        if row_container_from_payload is not None:
            row_container = row_container_from_payload
            row_context_source = "row_payload"
        elif target is not None:
            try:
                row = target.locator(
                    "xpath=ancestor::*[@data-input-name][1]"
                )
                if row.count() > 0 and row.first.is_visible(timeout=150):
                    candidate = row.first
                    candidate_payload = self._element_debug_payload(candidate)
                    candidate_text = str(candidate_payload.get("text", "") or "").strip()
                    if not self._is_broad_row_container(candidate_payload, candidate_text):
                        row_container = candidate
                        row_context_source = "target_ancestor"
            except Exception:
                row_container = None

            if row_container is None:
                row_container = target
                row_context_source = "target_self"

        if row_container is None:
            return None, None, payload

        direct_input = self._find_utm_direct_input(row_container)
        direct_input_found = direct_input is not None
        direct_input_payload = self._element_debug_payload(direct_input) if direct_input is not None else None
        self.logger.info("utm_row_context_source=%s", row_context_source)
        self.logger.info("utm_direct_input_found=%s", str(direct_input_found).lower())
        self.logger.info("utm_direct_input_payload=%s", direct_input_payload)

        row_ms_id = self._locator_multisuggest_id(row_container)
        try:
            row_html = str(row_container.evaluate("el => (el.outerHTML || '').slice(0, 2400)") or "")
        except Exception:
            row_html = ""
        self.logger.info("utm_row_scope_resolved=true")
        self.logger.info("utm_row_multisuggest_id=%s", row_ms_id)
        self.logger.info("utm_row_scope_outer_html_snippet=%s", row_html.replace("\n", " "))

        return row_container, target, payload

    def _find_utm_direct_input(self, row_container: Locator) -> Locator | None:
        # Row can occasionally be the input itself.
        try:
            self_match = row_container.evaluate(
                """el => {
                    const tag = (el.tagName || '').toLowerCase();
                    const cls = (el.className || '').toString().toLowerCase();
                    const type = ((el.getAttribute && el.getAttribute('type')) || '').toLowerCase();
                    const visible = !!(el.offsetParent || (el.getClientRects && el.getClientRects().length));
                    const enabled = !el.disabled && !el.readOnly;
                    const direct = tag === 'input' && (
                        cls.includes('filter__custom_input') ||
                        cls.includes('text-input') ||
                        type === 'text'
                    );
                    return {match: !!(direct && visible && enabled)};
                }"""
            )
            if isinstance(self_match, dict) and bool(self_match.get("match", False)):
                self.logger.info("utm_direct_input_self_match=true")
                return row_container
        except Exception:
            pass

        self.logger.info("utm_direct_input_self_match=false")
        selectors = (
            ".filter__custom_input.text-input",
            "input.filter__custom_input",
            ".filter__custom_settings__item__value-wrapper input",
            "input[type='text']",
        )
        for selector in selectors:
            loc = row_container.locator(selector)
            try:
                count = min(loc.count(), 8)
            except Exception:
                continue
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=160):
                        continue
                    if not item.is_enabled(timeout=160):
                        continue
                except Exception:
                    continue
                return item
        return None

    def _detect_utm_control_mode(self, row_container: Locator) -> str:
        direct_input = self._find_utm_direct_input(row_container)
        if direct_input is not None:
            return "direct_text_input"
        try:
            ms = row_container.locator(".js-multisuggest, input.multisuggest__input, .multisuggest__input")
            if ms.count() > 0 and ms.first.is_visible(timeout=100):
                return "multisuggest"
        except Exception:
            pass
        return "unknown"

    def _find_utm_source_holder(self, panel: Locator) -> Locator | None:
        for selector in self.UTM_HOLDER_SELECTORS:
            locator = panel.locator(selector)
            try:
                count = min(locator.count(), 20)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=200):
                        continue
                    payload = self._element_debug_payload(item)
                    joined = " ".join(
                        [
                            str(payload.get("className", "") or ""),
                            str(payload.get("text", "") or ""),
                            str(item.get_attribute("data-input-name") or ""),
                            str(item.get_attribute("data-title") or ""),
                        ]
                    )
                    if "utm" not in joined.lower():
                        continue
                    self.logger.info("utm_holder_found=true selector=%s payload=%s", selector, payload)
                    return item
                except Exception:
                    continue
        self.logger.info("utm_holder_found=false")
        return None

    def _resolve_utm_input(self, page: Page, panel: Locator, holder: Locator) -> tuple[Locator | None, str]:
        active_popup, _ = self._find_active_tag_popup(panel, holder)
        if active_popup is not None:
            popup_input = self._find_tag_input_in_popup(active_popup)
            if popup_input is not None:
                return popup_input, "active_popup"

        for selector in self.UTM_INPUT_SELECTORS:
            locator = holder.locator(selector)
            try:
                count = min(locator.count(), 8)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=180):
                        continue
                    if not item.is_enabled(timeout=180):
                        continue
                except Exception:
                    continue
                return item, f"holder:{selector}"

        try:
            focused = page.locator(":focus")
            if focused.count() > 0:
                item = focused.first
                payload = self._element_debug_payload(item)
                cls = str(payload.get("className", "") or "").lower()
                if str(payload.get("tagName", "") or "").lower() == "input" and (
                    "multisuggest__input" in cls or "js-multisuggest-input" in cls
                ):
                    return item, "focused_multisuggest_input"
        except Exception:
            pass

        return None, ""

    def _has_utm_chip(self, panel: Locator, holder: Locator, target_value: str) -> bool:
        chips = self._collect_tag_chip_texts(panel, holder=holder)
        target_norm = self._normalize_filter_text(target_value)
        matched = any(target_norm in self._normalize_filter_text(item) for item in chips)
        self.logger.info("utm_chip_texts=%s", chips[:20])
        self.logger.info("utm_chip_match=%s", str(bool(matched)).lower())
        return bool(matched)


    def _apply_utm_source_exact_values(self, page: Page, panel: Locator, values: list[str], report_id: str) -> None:
        self.logger.info("utm_exact_flow_started report_id=%s values=%s", report_id, [v.strip() for v in values if v and v.strip()])

        row_container, click_target, _ctx_payload = self._resolve_utm_row_context(
            page=page,
            panel=panel,
            report_id=report_id,
        )
        if row_container is None:
            self.logger.info("utm_row_scope_resolved=false")
            self.logger.info("utm_exact_selection_success=false")
            raise RuntimeError("UTM source row context was not resolved in analytics filter panel.")

        mode = self._detect_utm_control_mode(row_container)
        self.logger.info("utm_control_mode=%s", mode)

        if mode == "direct_text_input":
            input_locator = self._find_utm_direct_input(row_container)
            self.logger.info("utm_direct_input_found=%s", str(input_locator is not None).lower())
            self.logger.info(
                "utm_direct_input_payload=%s",
                self._element_debug_payload(input_locator) if input_locator is not None else None,
            )
            if input_locator is None:
                self.logger.info("utm_exact_selection_success=false")
                self.logger.info("utm_exact_fail_reason=direct_input_not_found")
                raise RuntimeError("UTM exact selection failed: direct text input was not found.")

            for raw in values:
                value = raw.strip()
                if not value:
                    continue
                self.logger.info("utm_direct_input_fill_started=true")
                try:
                    input_locator.click(timeout=1200)
                except Exception:
                    pass
                page.wait_for_timeout(120)

                page.keyboard.press("Control+A")
                page.keyboard.press("Backspace")
                page.keyboard.type(value, delay=20)
                self.logger.info('utm_direct_input_value="%s"', value)

                reflected = False
                for _ in range(8):
                    current = self._read_input_value(input_locator)
                    if self._normalize_filter_text(current) == self._normalize_filter_text(value):
                        reflected = True
                        break
                    page.wait_for_timeout(150)

                self.logger.info("utm_direct_input_reflection_success=%s", str(reflected).lower())
                if not reflected:
                    self.logger.info("utm_exact_selection_success=false")
                    self.logger.info("utm_exact_fail_reason=direct_input_value_not_reflected")
                    raise RuntimeError(
                        f"UTM exact selection failed: direct input value '{value}' was not reflected."
                    )

            page.keyboard.press("Escape")
            self.logger.info("utm_exact_selection_mode=direct_text_input")
            self.logger.info("utm_exact_selection_success=true values=%s", [v.strip() for v in values if v.strip()])
            return

        if click_target is None:
            self.logger.info("utm_exact_selection_success=false")
            self.logger.info("utm_exact_fail_reason=no_click_target_for_multisuggest_mode")
            raise RuntimeError("UTM source click target was not resolved for multisuggest mode.")

        row_multisuggest_id = self._locator_multisuggest_id(row_container)

        for raw in values:
            value = raw.strip()
            if not value:
                continue

            input_locator: Locator | None = None
            input_strategy = ""
            active_popup: Locator | None = None
            popup_multisuggest_id = ""

            for attempt in range(1, 6):
                try:
                    click_target.click(timeout=1000)
                except Exception:
                    try:
                        row_container.click(timeout=1000)
                    except Exception:
                        pass
                page.wait_for_timeout(180)

                active_popup, popup_payload = self._find_active_tag_popup(panel, row_container)
                popup_multisuggest_id = str((popup_payload or {}).get("multisuggest_id", "") or "").strip()
                self.logger.info("active_popup_found=%s", str(active_popup is not None).lower())
                self.logger.info("utm_popup_multisuggest_id=%s", popup_multisuggest_id)
                self.logger.info(
                    "utm_popup_id_matches_row=%s",
                    str(bool(not row_multisuggest_id or not popup_multisuggest_id or popup_multisuggest_id == row_multisuggest_id)).lower(),
                )

                if active_popup is None:
                    self.logger.info("utm_exact_input_attempt=%s found=false strategy=no_active_popup", attempt)
                    continue

                popup_input = self._find_tag_input_in_popup(active_popup)
                if popup_input is not None:
                    input_locator = popup_input
                    input_strategy = "active_popup"
                else:
                    for selector in (
                        "input.multisuggest__input.js-multisuggest-input",
                        "li.multisuggest__list-item_input input",
                    ):
                        loc = row_container.locator(selector)
                        try:
                            if loc.count() > 0 and loc.first.is_visible(timeout=150) and loc.first.is_enabled(timeout=150):
                                input_locator = loc.first
                                input_strategy = f"row_scoped:{selector}"
                                break
                        except Exception:
                            continue

                input_multisuggest_id = self._locator_multisuggest_id(input_locator)
                self.logger.info("utm_input_multisuggest_id=%s", input_multisuggest_id)
                self.logger.info(
                    "utm_input_id_matches_popup=%s",
                    str(bool(not popup_multisuggest_id or not input_multisuggest_id or input_multisuggest_id == popup_multisuggest_id)).lower(),
                )

                if input_locator is None:
                    self.logger.info("utm_exact_input_attempt=%s found=false strategy=no_popup_input", attempt)
                    continue

                if popup_multisuggest_id and input_multisuggest_id and popup_multisuggest_id != input_multisuggest_id:
                    self.logger.info("utm_exact_input_attempt=%s found=false strategy=multisuggest_id_mismatch", attempt)
                    input_locator = None
                    continue

                self.logger.info(
                    "utm_exact_input_attempt=%s found=true strategy=%s",
                    attempt,
                    input_strategy,
                )
                break

            if active_popup is None:
                self.logger.info("utm_exact_selection_success=false")
                self.logger.info("utm_exact_fail_reason=active_popup_not_opened")
                raise RuntimeError("UTM exact selection failed: active popup was not opened for utm_source row.")

            if input_locator is None:
                self.logger.info("utm_exact_selection_success=false")
                self.logger.info("utm_exact_fail_reason=row_scoped_input_not_activated")
                raise RuntimeError("UTM source input was not activated in row-scoped popup.")

            try:
                input_locator.click(timeout=1200)
            except Exception:
                pass
            page.wait_for_timeout(120)

            page.keyboard.press("Control+A")
            page.keyboard.press("Backspace")
            page.keyboard.type(value, delay=20)
            self.logger.info('utm_exact_typed_value="%s"', value)
            page.wait_for_timeout(220)

            page.keyboard.press("Enter")
            page.wait_for_timeout(260)

            if not self._has_utm_chip(panel, row_container, value):
                option_clicked, option_strategy, option_payload = self._select_real_tag_option(
                    panel,
                    value,
                    holder=row_container,
                    expected_multisuggest_id=popup_multisuggest_id or row_multisuggest_id or None,
                )
                self.logger.info(
                    "utm_exact_option_click_success=%s strategy=%s payload=%s",
                    str(bool(option_clicked)).lower(),
                    option_strategy,
                    option_payload,
                )
                if not option_clicked or not self._has_utm_chip(panel, row_container, value):
                    self.logger.info("utm_exact_selection_success=false")
                    self.logger.info("utm_exact_fail_reason=chip_not_detected")
                    raise RuntimeError(f"UTM exact selection failed: chip for '{value}' was not detected.")

        page.keyboard.press("Escape")
        self.logger.info("utm_exact_selection_success=true values=%s", [v.strip() for v in values if v.strip()])

    def _find_strict_tag_holder(self, panel: Locator) -> tuple[Locator | None, str | None]:
        return tag_filter.find_strict_tag_holder(self, panel)

    def _find_tag_input_in_holder(self, holder: Locator, holder_multisuggest_id: str | None) -> Locator | None:
        selectors: list[str] = []
        if holder_multisuggest_id:
            selectors.extend(
                [
                    f".js-multisuggest[data-multisuggest-id='{holder_multisuggest_id}'] input.multisuggest__input.js-multisuggest-input",
                    f".js-multisuggest[data-multisuggest-id='{holder_multisuggest_id}'] li.multisuggest__list-item_input input",
                ]
            )
        selectors.extend(
            [
                "input.multisuggest__input.js-multisuggest-input",
                "li.multisuggest__list-item_input input",
            ]
        )

        for selector in selectors:
            locator = holder.locator(selector)
            try:
                count = min(locator.count(), 12)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=250):
                        continue
                except Exception:
                    continue
                valid, reason, payload = self._is_valid_tag_focus_target(item)
                if not valid:
                    self.logger.info(
                        "tag_focus_candidate kind=%s editable=%s chosen=false reason=%s payload=%s",
                        payload.get("kind", "div"),
                        str(bool(payload.get("editable", False))).lower(),
                        reason,
                        payload,
                    )
                    continue
                candidate_ms_id = ""
                try:
                    candidate_ms_id = str(
                        item.evaluate(
                            """el => {
                                const node = el.closest('.js-multisuggest[data-multisuggest-id]');
                                return node ? (node.getAttribute('data-multisuggest-id') || '') : '';
                            }"""
                        )
                        or ""
                    ).strip()
                except Exception:
                    candidate_ms_id = ""
                if holder_multisuggest_id and candidate_ms_id != holder_multisuggest_id:
                    self.logger.info(
                        "tag_focus_candidate kind=%s editable=%s chosen=false reason=multisuggest_id_mismatch expected=%s actual=%s payload=%s",
                        payload.get("kind", "div"),
                        str(bool(payload.get("editable", False))).lower(),
                        holder_multisuggest_id,
                        candidate_ms_id,
                        payload,
                    )
                    continue
                self.logger.info(
                    "tag_input_found=true selector=%s payload=%s",
                    selector,
                    payload,
                )
                return item
        self.logger.info("tag_input_found=false")
        return None


    @staticmethod
    def _choose_active_popup_candidate(
        holder_bbox: dict[str, float] | None,
        popup_candidates: list[dict[str, object]],
    ) -> dict[str, object] | None:
        if not popup_candidates:
            return None
        if holder_bbox is None:
            return popup_candidates[0]

        holder_cy = float(holder_bbox.get("y", 0.0)) + float(holder_bbox.get("height", 0.0)) / 2.0
        ranked: list[tuple[float, dict[str, object]]] = []
        for payload in popup_candidates:
            bbox = payload.get("bbox")
            if not isinstance(bbox, dict):
                continue
            cy = float(bbox.get("y", 0.0)) + float(bbox.get("height", 0.0)) / 2.0
            distance = abs(cy - holder_cy)
            ranked.append((distance, payload))
        if ranked:
            ranked.sort(key=lambda item: item[0])
            return ranked[0][1]
        return popup_candidates[0]

    def _find_active_tag_popup(self, panel: Locator, holder: Locator | None) -> tuple[Locator | None, dict[str, object] | None]:
        page = panel.page
        holder_bbox: dict[str, float] | None = None
        if holder is not None:
            try:
                raw = holder.bounding_box()
                if isinstance(raw, dict):
                    holder_bbox = {
                        "x": float(raw.get("x", 0.0)),
                        "y": float(raw.get("y", 0.0)),
                        "width": float(raw.get("width", 0.0)),
                        "height": float(raw.get("height", 0.0)),
                    }
            except Exception:
                holder_bbox = None

        selectors = (
            ".multisuggest.multisuggest_show-suggest.multisuggest_show-list",
            ".multisuggest_show-suggest.multisuggest_show-list",
            ".multisuggest_show-suggest",
            ".multisuggest_show-list",
        )
        candidates: list[tuple[Locator, dict[str, object]]] = []
        for selector in selectors:
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 20)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=180):
                        continue
                except Exception:
                    continue
                payload = self._element_debug_payload(item)
                popup_bbox = payload.get("bbox")
                distance = None
                if isinstance(holder_bbox, dict) and isinstance(popup_bbox, dict):
                    holder_cy = float(holder_bbox.get("y", 0.0)) + float(holder_bbox.get("height", 0.0)) / 2.0
                    popup_cy = float(popup_bbox.get("y", 0.0)) + float(popup_bbox.get("height", 0.0)) / 2.0
                    distance = abs(popup_cy - holder_cy)
                payload["popup_distance_to_holder"] = distance
                payload["selector"] = selector
                try:
                    payload["multisuggest_id"] = (item.get_attribute("data-multisuggest-id") or "").strip()
                except Exception:
                    payload["multisuggest_id"] = ""
                candidates.append((item, payload))

        self.logger.info("popup_candidates_count=%s", len(candidates))
        for _, payload in candidates[:6]:
            self.logger.info("popup_candidate payload=%s", payload)

        chosen_payload = self._choose_active_popup_candidate(
            holder_bbox=holder_bbox,
            popup_candidates=[payload for _, payload in candidates],
        )
        if chosen_payload is None:
            self.logger.info("active_popup_found=false")
            return None, None

        chosen_locator: Locator | None = None
        for locator, payload in candidates:
            if payload is chosen_payload:
                chosen_locator = locator
                break
        if chosen_locator is None:
            self.logger.info("active_popup_found=false")
            return None, None

        self.logger.info("active_popup_found=true")
        self.logger.info("popup_selected_by_proximity=%s", str(bool(holder_bbox)).lower())
        self.logger.info("active_popup_bbox=%s", chosen_payload.get("bbox"))
        self.logger.info("popup_distance_to_holder=%s", chosen_payload.get("popup_distance_to_holder"))
        self.logger.info("popup_multisuggest_id=%s", chosen_payload.get("multisuggest_id", ""))
        try:
            html = chosen_locator.evaluate("el => (el.outerHTML || '').slice(0, 1200)")
        except Exception:
            html = ""
        self.logger.info("active_popup_outer_html_snippet=%s", str(html or "").replace("\n", " "))
        return chosen_locator, chosen_payload

    def _find_tag_input_in_popup(self, popup: Locator) -> Locator | None:
        selectors = (
            "input.multisuggest__input.js-multisuggest-input",
            "li.multisuggest__list-item_input input",
        )
        for selector in selectors:
            locator = popup.locator(selector)
            try:
                count = min(locator.count(), 12)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=250):
                        continue
                except Exception:
                    continue
                valid, reason, payload = self._is_valid_tag_focus_target(item)
                if not valid:
                    self.logger.info(
                        "tag_focus_candidate kind=%s editable=%s chosen=false reason=%s payload=%s",
                        payload.get("kind", "div"),
                        str(bool(payload.get("editable", False))).lower(),
                        reason,
                        payload,
                    )
                    continue
                self.logger.info("popup_input_found=true selector=%s payload=%s", selector, payload)
                return item
        self.logger.info("popup_input_found=false")
        return None


    def _resolve_active_multisuggest_input_legacy(self, page: Page, panel: Locator, holder: Locator) -> tuple[Locator | None, str]:
        active_popup, _payload = self._find_active_tag_popup(panel, holder)
        if active_popup is not None:
            popup_input = self._find_tag_input_in_popup(active_popup)
            if popup_input is not None:
                return popup_input, "active_popup"

        # Active element fallback, but only multisuggest input classes.
        try:
            active = page.locator(":focus")
            if active.count() > 0:
                focused = active.first
                payload = self._element_debug_payload(focused)
                cls = str(payload.get("className", "") or "").lower()
                tag = str(payload.get("tagName", "") or "").lower()
                if tag == "input" and ("multisuggest__input" in cls or "js-multisuggest-input" in cls):
                    return focused, "active_element_multisuggest"
        except Exception:
            pass

        return None, ""

    def _click_locator_point(self, page: Page, locator: Locator, px: float, py: float) -> bool:
        try:
            box = locator.bounding_box()
            if not isinstance(box, dict):
                return False
            x = float(box.get("x", 0.0)) + float(box.get("width", 0.0)) * px
            y = float(box.get("y", 0.0)) + float(box.get("height", 0.0)) * py
            page.mouse.click(x, y)
            return True
        except Exception:
            return False

    def _apply_tag_values_via_holder_popup(self, page: Page, panel: Locator, report_id: str, values: list[str]) -> bool:
        return tag_filter.apply_tag_values_via_holder_popup(self, page=page, panel=panel, report_id=report_id, values=values)

    # Backward-compat alias for old callsites.
    def _apply_tag_values_legacy_fast(self, page: Page, panel: Locator, report_id: str, values: list[str]) -> bool:
        return self._apply_tag_values_via_holder_popup(page=page, panel=panel, report_id=report_id, values=values)

    def _holder_outer_html_snippet(self, holder: Locator, limit: int = 3000) -> str:
        try:
            html = holder.evaluate(f"el => (el.outerHTML || '').slice(0, {int(limit)})")
            return str(html or "").replace("\n", " ")
        except Exception:
            return ""

    def _save_tag_holder_after_enter_artifacts(
        self,
        holder: Locator,
        chip_texts: list[str],
        target_value: str,
    ) -> tuple[Path, Path]:
        holder_html = self._holder_outer_html_snippet(holder, limit=12000)
        return analytics_save_tag_holder_after_enter_artifacts(
            exports_dir=self.reader.settings.exports_dir,
            project_root=self.project_root,
            holder_html=holder_html,
            chip_texts=chip_texts,
            target_value=target_value,
        )

    def _get_visible_popup_multisuggest_id(self, panel: Locator) -> str | None:
        selectors = (
            ".js-multisuggest.multisuggest_show-suggest[data-multisuggest-id]",
            ".multisuggest_show-suggest[data-multisuggest-id]",
            "[data-multisuggest-id].multisuggest_show-suggest",
        )
        for selector in selectors:
            locator = panel.locator(selector)
            try:
                count = min(locator.count(), 20)
            except Exception:
                continue
            for idx in range(count):
                item = locator.nth(idx)
                try:
                    if not item.is_visible(timeout=150):
                        continue
                except Exception:
                    continue
                try:
                    val = (item.get_attribute("data-multisuggest-id") or "").strip()
                except Exception:
                    val = ""
                if val:
                    return val
        return None

    def _collect_visible_tag_suggestion_texts(
        self,
        panel: Locator,
        popup: Locator | None = None,
        expected_multisuggest_id: str | None = None,
    ) -> list[str]:
        selectors = (
            "li.multisuggest__list-item",
            "[role='option']",
            "[class*='suggest-item']",
        )
        out: list[str] = []
        seen: set[str] = set()
        for selector in selectors:
            root = popup or panel
            loc = root.locator(selector)
            try:
                count = min(loc.count(), 80)
            except Exception:
                continue
            for idx in range(count):
                item = loc.nth(idx)
                try:
                    if not item.is_visible(timeout=120):
                        continue
                    cls = (item.get_attribute("class") or "").lower()
                    if "multisuggest__list-item_input" in cls or "js-multisuggest-input" in cls:
                        continue
                    txt = item.inner_text(timeout=120).strip()
                    if not txt:
                        continue
                    ms_id = str(
                        item.evaluate(
                            """el => {
                                const n = el.closest('[data-multisuggest-id]');
                                return n ? (n.getAttribute('data-multisuggest-id') || '') : '';
                            }"""
                        )
                        or ""
                    ).strip()
                except Exception:
                    continue
                if expected_multisuggest_id and ms_id and ms_id != expected_multisuggest_id:
                    continue
                key = txt.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(txt)
        return out

    def _collect_tag_chip_texts(self, panel: Locator, holder: Locator | None = None) -> list[str]:
        return tag_filter.collect_tag_chip_texts(self, panel=panel, holder=holder)

    def _is_tag_dropdown_open(self, panel: Locator) -> bool:
        dropdown_selectors = (
            "[class*='multisuggest_show-suggest']",
            "[role='option']",
            "[class*='suggest']",
            "[class*='dropdown']",
            "[class*='select__menu']",
        )
        for selector in dropdown_selectors:
            locator = panel.locator(selector)
            try:
                count = min(locator.count(), 40)
            except Exception:
                continue
            for idx in range(count):
                try:
                    if locator.nth(idx).is_visible(timeout=200):
                        return True
                except Exception:
                    continue
        return False

    def _has_selected_tag_chip(self, panel: Locator, target_value: str, holder: Locator | None = None) -> bool:
        return tag_filter.has_selected_tag_chip(self, panel=panel, target_value=target_value, holder=holder)

    def _select_real_tag_option(self, panel: Locator, target_value: str, holder: Locator | None = None, expected_multisuggest_id: str | None = None) -> tuple[bool, str, dict[str, object] | None]:
        """Select a real dropdown option (not input row) and confirm by chip presence."""
        value = target_value.strip()
        if not value:
            return False, "", None

        # Reference input-row Y for UI where options expand upward.
        input_y = None
        try:
            input_row = panel.locator("li.multisuggest__list-item.multisuggest__list-item_input")
            if input_row.count() > 0:
                box = input_row.first.bounding_box()
                if box:
                    input_y = float(box.get("y", 0.0))
        except Exception:
            input_y = None

        base_selectors = (
            "li[class*='multisuggest__list-item']",
            "[class*='suggest-item']",
            "[role='option']",
            "li",
        )

        candidates: list[tuple[float, str, Locator, dict[str, object]]] = []
        for selector in base_selectors:
            locator = panel.locator(selector)
            try:
                count = min(locator.count(), 120)
            except Exception:
                continue

            for idx in range(count):
                candidate = locator.nth(idx)
                try:
                    if not candidate.is_visible(timeout=180):
                        continue
                    tag_name = candidate.evaluate("el => (el.tagName || '').toLowerCase()") or ""
                    class_name = candidate.get_attribute("class") or ""
                    txt = candidate.inner_text(timeout=180).strip()
                    box = candidate.bounding_box()
                except Exception:
                    continue

                if not box:
                    continue
                if value.lower() not in txt.lower():
                    continue

                lowered_cls = class_name.lower()
                if "multisuggest__list-item_input" in lowered_cls or "js-multisuggest-input" in lowered_cls:
                    continue
                if tag_name in {"input", "textarea"}:
                    continue
                try:
                    contenteditable = (candidate.get_attribute("contenteditable") or "").lower()
                except Exception:
                    contenteditable = ""
                if contenteditable == "true":
                    continue

                candidate_ms_id = ""
                try:
                    candidate_ms_id = str(
                        candidate.evaluate(
                            """el => {
                                const node = el.closest('[data-multisuggest-id]');
                                return node ? (node.getAttribute('data-multisuggest-id') || '') : '';
                            }"""
                        )
                        or ""
                    ).strip()
                except Exception:
                    candidate_ms_id = ""

                payload = {
                    "tagName": tag_name,
                    "className": class_name,
                    "text": txt,
                    "bbox": box,
                    "selector": selector,
                    "popup_multisuggest_id": candidate_ms_id,
                }
                self.logger.info("Tag real option candidate payload: %s", payload)

                cy = float(box.get("y", 0.0))
                # Prefer options above input row when dropdown opens upward.
                priority = 0.0
                if input_y is not None:
                    priority = 0.0 if cy < input_y else 10000.0
                    priority += abs((input_y - cy))
                if expected_multisuggest_id and candidate_ms_id == expected_multisuggest_id:
                    priority -= 100.0
                candidates.append((priority, selector, candidate, payload))

        candidates.sort(key=lambda t: t[0])
        last_payload: dict[str, object] | None = None

        for _, selector, candidate, payload in candidates:
            last_payload = payload
            try:
                candidate.hover(timeout=600)
            except Exception:
                pass

            clicked = False
            try:
                box = payload.get("bbox") or {}
                cx = float(box.get("x", 0.0)) + float(box.get("width", 0.0)) / 2
                cy = float(box.get("y", 0.0)) + float(box.get("height", 0.0)) / 2
                panel.page.mouse.click(cx, cy)
                clicked = True
            except Exception:
                try:
                    candidate.click(timeout=1000)
                    clicked = True
                except Exception:
                    clicked = False

            if not clicked:
                continue

            self.logger.info("Tag real option clicked: %s", payload)
            self.logger.info("clicked_suggestion_text=%s", payload.get("text", ""))
            panel.page.wait_for_timeout(220)
            chip_present = self._has_selected_tag_chip(panel, value, holder=holder)
            if chip_present:
                return True, selector, payload

            self.logger.info("Tag real option click failed to create chip: %s", payload)

        return False, "", last_payload

    def _get_apply_button_selectors(self) -> tuple[str, ...]:
        ru_apply = "?????????"
        return (
            "#filter_apply",
            "button#filter_apply",
            "button.filter__params_manage__apply",
            ".filter__params_manage .filter__params_manage__apply",
            f"button:has-text('{ru_apply}')",
            f"[role='button']:has-text('{ru_apply}')",
            f"input[type='submit'][value*='{ru_apply}']",
            "[class*='apply']",
            "[class*='footer'] button",
            "[class*='actions'] button",
            "[class*='sticky'] button",
            "[class*='fixed'] button",
            "[class*='drawer'] button",
            "[class*='modal'] button",
            "[class*='popup'] button",
            "button:has-text('Apply')",
            "[role='button']:has-text('Apply')",
            "[class*='filter'] button",
        )

    def _scroll_container_to_bottom(self, container: Locator) -> None:
        try:
            container.evaluate("el => { el.scrollTop = el.scrollHeight; }")
            container.page.wait_for_timeout(180)
        except Exception:
            pass

    def _collect_apply_scopes(self, page: Page, panel: Locator) -> list[tuple[str, Locator]]:
        scopes: list[tuple[str, Locator]] = [("panel", panel)]

        overlay_selector = (
            "[role='dialog'], [class*='modal'], [class*='drawer'], "
            "[class*='overlay'], [class*='popup'], [class*='filter']"
        )
        overlays = page.locator(overlay_selector)
        try:
            overlay_count = min(overlays.count(), 20)
        except Exception:
            overlay_count = 0

        for idx in range(overlay_count):
            item = overlays.nth(idx)
            try:
                if not item.is_visible(timeout=120):
                    continue
            except Exception:
                continue
            scopes.append(("overlay", item))

        scopes.append(("page", page.locator("body").first))
        return scopes

    def _collect_apply_button_candidates(self, page: Page, panel: Locator) -> list[tuple[Locator, str, str]]:
        """Collect apply button candidates from panel, overlays, and page scopes."""
        candidates: list[tuple[Locator, str, str]] = []
        selectors = self._get_apply_button_selectors()
        scopes = self._collect_apply_scopes(page, panel)

        for scope_name, scope in scopes:
            # Ensure action areas at bottom become visible.
            self._scroll_container_to_bottom(scope)
            for selector in selectors:
                locator = scope.locator(selector)
                try:
                    count = min(locator.count(), 12)
                except Exception:
                    continue
                for idx in range(count):
                    candidates.append((locator.nth(idx), selector, scope_name))

        return candidates

    def _apply_candidate_debug_payload(self, candidate: Locator, selector: str, scope: str) -> dict[str, object]:
        payload: dict[str, object] = {
            "selector": selector,
            "scope": scope,
            "outside_panel": scope != "panel",
        }
        try:
            payload["tagName"] = candidate.evaluate("el => (el.tagName || '').toLowerCase()")
        except Exception:
            payload["tagName"] = ""
        try:
            payload["elementId"] = candidate.get_attribute("id") or ""
        except Exception:
            payload["elementId"] = ""
        try:
            payload["className"] = candidate.get_attribute("class") or ""
        except Exception:
            payload["className"] = ""
        try:
            text_value = candidate.inner_text(timeout=200).strip()
        except Exception:
            text_value = ""
        if not text_value:
            try:
                text_value = candidate.get_attribute("value") or ""
            except Exception:
                text_value = ""
        payload["text"] = text_value
        try:
            payload["bbox"] = candidate.bounding_box()
        except Exception:
            payload["bbox"] = None
        try:
            payload["visible"] = candidate.is_visible(timeout=200)
        except Exception:
            payload["visible"] = False
        try:
            payload["enabled"] = candidate.is_enabled(timeout=200)
        except Exception:
            payload["enabled"] = False
        try:
            payload["disabled"] = candidate.get_attribute("disabled")
        except Exception:
            payload["disabled"] = None
        try:
            payload["aria-disabled"] = candidate.get_attribute("aria-disabled")
        except Exception:
            payload["aria-disabled"] = None
        try:
            style_info = candidate.evaluate(
                """el => {
                    const s = window.getComputedStyle(el);
                    return {
                        pointerEvents: s.pointerEvents || '',
                        opacity: s.opacity || '',
                        display: s.display || '',
                        visibility: s.visibility || '',
                        position: s.position || ''
                    };
                }"""
            )
        except Exception:
            style_info = {}
        payload["pointer-events"] = (style_info or {}).get("pointerEvents", "")
        payload["opacity"] = (style_info or {}).get("opacity", "")
        payload["display"] = (style_info or {}).get("display", "")
        payload["visibility"] = (style_info or {}).get("visibility", "")
        payload["position"] = (style_info or {}).get("position", "")
        payload["fixed_or_sticky"] = str((style_info or {}).get("position", "")).lower() in {"fixed", "sticky"}
        try:
            payload["in_opened_select_list"] = bool(
                candidate.evaluate("el => !!el.closest('.control--select--list-opened')")
            )
        except Exception:
            payload["in_opened_select_list"] = False
        return payload

    def _is_apply_candidate_relevant(self, payload: dict[str, object]) -> bool:
        text_value = str(payload.get("text", "")).strip().lower()
        class_name = str(payload.get("className", "")).strip().lower()
        tag_name = str(payload.get("tagName", "")).strip().lower()
        element_id = str(payload.get("elementId", "")).strip().lower()

        if not bool(payload.get("visible", False)):
            return False
        if bool(payload.get("in_opened_select_list", False)):
            return False
        if "control--select--button" in class_name:
            return False
        if any(token in class_name for token in ("dropdown", "listbox", "multisuggest")) and "apply" not in class_name:
            return False

        button_like = tag_name in {"button", "input", "a", "div", "span"}
        if not button_like:
            return False

        text_match = (text_value == "?????????") or ("??????" in text_value) or ("apply" in text_value)
        class_match = ("filter__params_manage__apply" in class_name) or ("apply" in class_name)
        id_match = element_id == "filter_apply"
        return id_match or class_match or text_match

    def _apply_candidate_score(self, payload: dict[str, object]) -> tuple[int, bool, str]:
        text_value = str(payload.get("text", "")).strip().lower()
        class_name = str(payload.get("className", "")).strip().lower()
        selector = str(payload.get("selector", "")).strip().lower()
        element_id = str(payload.get("elementId", "")).strip().lower()

        score = 0
        reasons: list[str] = []
        is_exact_apply = False

        if element_id == "filter_apply":
            score += 1000
            is_exact_apply = True
            reasons.append("id=filter_apply")
        if "filter__params_manage__apply" in class_name:
            score += 800
            is_exact_apply = True
            reasons.append("class=filter__params_manage__apply")
        if text_value == "?????????":
            score += 500
            reasons.append("text_exact=?????????")
        elif "??????" in text_value or "apply" in text_value:
            score += 250
            reasons.append("text_contains_apply")

        if selector in {"#filter_apply", "button#filter_apply", "button.filter__params_manage__apply", ".filter__params_manage .filter__params_manage__apply"}:
            score += 300
            reasons.append("selector_priority")

        if "control--select--button" in class_name:
            score -= 900
            reasons.append("penalty_control_select_button")
        if bool(payload.get("in_opened_select_list", False)):
            score -= 900
            reasons.append("penalty_opened_select_list")
        if any(token in class_name for token in ("dropdown", "listbox", "multisuggest")) and "apply" not in class_name:
            score -= 500
            reasons.append("penalty_dropdown_like")
        if text_value and ("??????" not in text_value) and ("apply" not in text_value) and not is_exact_apply:
            score -= 300
            reasons.append("penalty_text_not_apply")

        if not bool(payload.get("enabled", False)):
            score -= 120
            reasons.append("penalty_disabled")

        return score, is_exact_apply, ";".join(reasons)

    def _find_apply_button(
        self,
        page: Page,
        panel: Locator,
        report_id: str,
    ) -> tuple[Locator | None, str, str, list[dict[str, object]], dict[str, object] | None]:
        candidates = self._collect_apply_button_candidates(page, panel)
        payloads: list[dict[str, object]] = []

        best_idx = -1
        best_score = -10**9

        for idx, (candidate, selector, scope) in enumerate(candidates):
            payload = self._apply_candidate_debug_payload(candidate, selector, scope)
            if not self._is_apply_candidate_relevant(payload):
                payload["relevant"] = False
                payloads.append(payload)
                continue

            score, is_exact_apply, reason = self._apply_candidate_score(payload)
            payload["relevant"] = True
            payload["score"] = score
            payload["is_exact_apply"] = is_exact_apply
            payload["score_reason"] = reason
            payloads.append(payload)

            if score > best_score:
                best_score = score
                best_idx = idx

        if best_idx >= 0 and best_idx < len(candidates):
            candidate, selector, scope = candidates[best_idx]
            winner = payloads[best_idx]
            winner["winner"] = True
            return candidate, selector, scope, payloads, winner

        return None, "", "", payloads, None

    def _get_panel_apply_button(self, page: Page, panel: Locator) -> tuple[Locator | None, str]:
        candidate, selector, _scope, _payloads, _winner = self._find_apply_button(page, panel, report_id="runtime")
        return candidate, selector

    def _dump_apply_button_diagnostics(
        self,
        page: Page,
        report_id: str,
        payloads: list[dict[str, object]],
    ) -> tuple[Path, Path]:
        """Save apply-button candidate diagnostics (best-effort, never raises)."""
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = self._debug_dir()
        text_path = ensure_inside_root(base / f"{report_id}_apply_candidates_{stamp}.txt", self.project_root)
        json_path = ensure_inside_root(base / f"{report_id}_apply_candidates_{stamp}.json", self.project_root)

        def _safe_write(path: Path, content: str) -> None:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
            except Exception as exc:
                self.logger.warning("Apply diagnostics write warning path=%s error=%s", path, exc)

        try:
            lines: list[str] = [
                f"report_id={report_id}",
                f"page_url={getattr(page, 'url', '')}",
                f"payload_count={len(payloads)}",
                "",
            ]
            for idx, payload in enumerate(payloads):
                lines.append(
                    (
                        f"[{idx}] selector={payload.get('selector', '')} scope={payload.get('scope', '')} "
                        f"score={payload.get('score', '')} relevant={payload.get('relevant', '')} "
                        f"is_exact_apply={payload.get('is_exact_apply', '')} "
                        f"id={payload.get('elementId', '')} class={payload.get('className', '')} "
                        f"text={payload.get('text', '')} visible={payload.get('visible', '')} "
                        f"enabled={payload.get('enabled', '')} bbox={payload.get('bbox', '')}"
                    )
                )
            _safe_write(text_path, "\n".join(lines))
            _safe_write(json_path, json.dumps(payloads, ensure_ascii=False, indent=2))
            return text_path, json_path
        except Exception as exc:
            fallback_txt = ensure_inside_root(base / f"{report_id}_apply_candidates_{stamp}_fallback.txt", self.project_root)
            fallback_json = ensure_inside_root(base / f"{report_id}_apply_candidates_{stamp}_fallback.json", self.project_root)
            _safe_write(fallback_txt, f"apply diagnostics collection failed: {exc}")
            _safe_write(fallback_json, "[]")
            self.logger.warning("Apply diagnostics fallback used report_id=%s error=%s", report_id, exc)
            return fallback_txt, fallback_json

    def _get_panel_apply_state(self, page: Page, panel: Locator) -> dict[str, bool]:
        apply_btn, _ = self._get_panel_apply_button(page, panel)
        if apply_btn is None:
            return {"visible": False, "enabled": False}
        try:
            visible = apply_btn.is_visible(timeout=300)
        except Exception:
            visible = False
        try:
            enabled = apply_btn.is_enabled(timeout=300)
        except Exception:
            enabled = False
        return {"visible": visible, "enabled": enabled}

    def _overlay_or_panel_closed(self, page: Page, panel: Locator) -> bool:
        try:
            if not panel.is_visible(timeout=150):
                return True
        except Exception:
            return True

        overlay_selector = "[role='dialog'], [class*='modal'], [class*='drawer'], [class*='overlay'], [class*='popup']"
        overlays = page.locator(overlay_selector)
        try:
            count = min(overlays.count(), 20)
        except Exception:
            count = 0

        for idx in range(count):
            item = overlays.nth(idx)
            try:
                if item.is_visible(timeout=120):
                    return False
            except Exception:
                continue

        return True

    def _click_apply_in_panel(self, page: Page, url_before: str | None = None, report_id: str = "runtime") -> bool:
        panel = self._find_filter_panel_container(page)
        self._scroll_filter_panel_to_bottom(panel)
        self.logger.info("Scrolled filter panel to bottom before apply")

        # Also try bringing overlay containers to bottom; apply can live in sticky/footer action area.
        for scope_name, scope in self._collect_apply_scopes(page, panel):
            if scope_name == "panel":
                continue
            self._scroll_container_to_bottom(scope)

        apply_btn, selector, scope, payloads, winner_payload = self._find_apply_button(page, panel, report_id=report_id)

        self.logger.info("Apply button candidates collected: count=%s", len(payloads))
        for payload in payloads[:5]:
            self.logger.info("Apply candidate payload: %s", payload)

        if apply_btn is None:
            try:
                text_path, selectors_path = self._dump_apply_button_diagnostics(page, report_id, payloads)
            except Exception as exc:
                self.logger.warning("Apply diagnostics failed in not-found path report_id=%s error=%s", report_id, exc)
                text_path = Path("<apply_diagnostics_failed>")
                selectors_path = Path("<apply_diagnostics_failed>")
            self.logger.warning("Apply candidates debug text dump: %s", text_path)
            self.logger.warning("Apply candidates debug selector dump: %s", selectors_path)
            self._debug_screenshot(page, f"profile_{report_id}_apply_not_found")
            return False

        self.logger.info("Apply button selected: selector=%s scope=%s", selector, scope)
        if winner_payload is not None:
            self.logger.info(
                "Apply winner details: is_exact_apply=%s id=%s class=%s text=%s reason=%s",
                winner_payload.get("is_exact_apply"),
                winner_payload.get("elementId"),
                winner_payload.get("className"),
                winner_payload.get("text"),
                winner_payload.get("score_reason"),
            )

        def _apply_effect_observed() -> bool:
            if url_before and self._is_filter_apply_confirmed_by_url(page, url_before):
                return True
            if self._overlay_or_panel_closed(page, panel):
                return True
            try:
                page.wait_for_load_state("domcontentloaded", timeout=500)
            except Exception:
                pass
            return bool(url_before and str(page.url) != str(url_before))

        try:
            apply_btn.scroll_into_view_if_needed(timeout=1000)
        except Exception:
            pass

        attempts = (
            ("normal", lambda: apply_btn.click(timeout=1500)),
            ("force", lambda: apply_btn.click(timeout=1500, force=True)),
            ("js", lambda: apply_btn.evaluate("el => el.click()")),
        )

        for attempt_name, action in attempts:
            self.logger.info("Apply click attempt: %s selector=%s scope=%s", attempt_name, selector, scope)
            try:
                action()
            except Exception as exc:
                self.logger.info("Apply click attempt failed: %s error=%s", attempt_name, exc)
                continue

            poll_attempts = 10
            poll_step_ms = 200
            for poll_idx in range(poll_attempts):
                if _apply_effect_observed():
                    self.logger.info("Filter apply confirmed attempt=%s poll=%s", attempt_name, poll_idx + 1)
                    return True
                try:
                    page.wait_for_timeout(poll_step_ms)
                except Exception:
                    break

        try:
            text_path, selectors_path = self._dump_apply_button_diagnostics(page, report_id, payloads)
        except Exception as exc:
            self.logger.warning("Apply diagnostics failed in click-failed path report_id=%s error=%s", report_id, exc)
            text_path = Path("<apply_diagnostics_failed>")
            selectors_path = Path("<apply_diagnostics_failed>")
        self.logger.warning("Apply click failed. debug text dump: %s", text_path)
        self.logger.warning("Apply click failed. debug selector dump: %s", selectors_path)
        self._debug_screenshot(page, f"profile_{report_id}_apply_click_failed")
        return False

    def _finalize_tag_selection_after_option_click(
        self,
        page: Page,
        panel: Locator,
        tag_focus_target: Locator | None,
        target_value: str,
    ) -> tuple[str, bool, bool, dict[str, bool]]:
        panel_box = None
        try:
            panel_box = panel.bounding_box()
        except Exception:
            panel_box = None

        apply_btn, _ = self._get_panel_apply_button(page, panel)
        apply_box = None
        if apply_btn is not None:
            try:
                apply_box = apply_btn.bounding_box()
            except Exception:
                apply_box = None

        tag_row = None
        tag_row_box = None
        if tag_focus_target is not None:
            try:
                tag_row = tag_focus_target.locator("xpath=ancestor::*[self::div or self::label][1]").first
                tag_row_box = tag_row.bounding_box()
            except Exception:
                tag_row = None
                tag_row_box = None

        def state_tuple() -> tuple[bool, bool, dict[str, bool]]:
            dropdown_closed = not self._is_tag_dropdown_open(panel)
            chip_present = self._has_selected_tag_chip(panel, target_value)
            apply_state = self._get_panel_apply_state(page, panel)
            return dropdown_closed, chip_present, apply_state

        actions: list[tuple[str, callable]] = []

        if panel_box and tag_row_box:
            rel_x = int(min(max((tag_row_box['x'] - panel_box['x']) + tag_row_box['width'] + 24, 20), panel_box['width'] - 20))
            rel_y = int(min(max((tag_row_box['y'] - panel_box['y']) + max(12, tag_row_box['height'] / 2), 15), panel_box['height'] - 15))
            actions.append(("after_option_panel_right_click", lambda: panel.click(position={"x": rel_x, "y": rel_y}, timeout=900)))

        actions.append(("after_option_panel_header_click", lambda: panel.get_by_text("??????", exact=False).first.click(timeout=900)))

        if panel_box and tag_row_box and apply_box:
            gap_y_global = (tag_row_box['y'] + tag_row_box['height'] + apply_box['y']) / 2
            rel_gap_y = int(min(max(gap_y_global - panel_box['y'], 20), panel_box['height'] - 20))
            rel_gap_x = int(min(max(panel_box['width'] * 0.65, 20), panel_box['width'] - 20))
            actions.append(("after_option_gap_click", lambda: panel.click(position={"x": rel_gap_x, "y": rel_gap_y}, timeout=900)))
        else:
            actions.append(("after_option_gap_click", lambda: panel.click(position={"x": 40, "y": 80}, timeout=900)))

        for strategy, action in actions:
            try:
                action()
            except Exception:
                continue
            page.wait_for_timeout(220)
            dropdown_closed, chip_present, apply_state = state_tuple()
            if dropdown_closed and chip_present and apply_state["visible"] and apply_state["enabled"]:
                return strategy, dropdown_closed, chip_present, apply_state

        dropdown_closed, chip_present, apply_state = state_tuple()
        return "none", dropdown_closed, chip_present, apply_state

    def _wait_after_apply(self, page: Page) -> None:
        page.wait_for_timeout(1200)
        try:
            page.wait_for_load_state("load", timeout=min(self.reader.settings.timeout_ms, 8000))
        except PlaywrightTimeoutError as exc:
            self.logger.warning("Soft wait timeout after filter apply, continue with current screen: %s", exc)








