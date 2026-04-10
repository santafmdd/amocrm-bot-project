"""amoCRM analytics reader (MVP): read current screen and export snapshot."""

from __future__ import annotations

import csv
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from playwright.sync_api import Locator, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.browser.models import AnalyticsSnapshot, SourceKind, StageCount, TabMode
from src.browser.session import BrowserSettings
from src.safety import ensure_inside_root


class AmoAnalyticsReader:
    """Read-only reader for current amoCRM analytics view."""

    PIPELINE_ROOT_SELECTOR = "#pipeline_first"
    PIPELINE_ROW_SELECTORS: tuple[str, ...] = (
        "#pipeline_first .list-content.calculation_rows ul.list_row li.item_row",
        "#pipeline_first .list-content.calculation_rows li.item_row",
        "#pipeline_first ul.list_row li.item_row",
        "#pipeline_first li.item_row",
    )

    CANDIDATE_SELECTORS: tuple[str, ...] = (
        "[data-testid*='analytics']",
        "[data-test*='analytics']",
        "[data-qa*='analytics']",
        "[class*='analytics']",
        "[class*='pipeline']",
        "[class*='stage']",
        "[class*='status']",
        "[role='list'] [role='listitem']",
        "[role='table'] [role='row']",
        "main div",
        "section div",
        "aside div",
    )

    TAB_ANCHORS: tuple[str, ...] = ("ВСЕ", "АКТИВНЫЕ", "ЗАКРЫТЫЕ")
    SORT_ANCHOR = "ПО КОЛИЧЕСТВУ"

    TOP_START_ANCHOR = "СДЕЛКИ"
    # Keep stop anchors minimal to avoid cutting real stage list too early.
    RIGHT_STOP_ANCHORS: tuple[str, ...] = ("ПРОГНОЗ ПРОДАЖ",)

    TOP_NOISE_LABELS: tuple[str, ...] = (
        "НАСТРОЙКИ",
        "ВАШ НОМЕР КЛИЕНТА",
        "РАБОЧИЙ СТОЛ",
        "ПОЧТА",
        "КАЛЕНДАРЬ",
        "КЛИЕНТЫ",
        "КОМПАНИИ",
        "ЗАДАЧИ",
        "СПИСКИ",
        "АНАЛИТИКА",
        "AMOМАРКЕТ",
        "AMOMARKET",
        "АМОМАРКЕТ",
    )

    SERVICE_STAGE_LABELS: tuple[str, ...] = (
        "СРЕДНЯЯ ДЛИТЕЛЬНОСТЬ СДЕЛКИ",
        "ПО КОЛИЧЕСТВУ",
        "ПО БЮДЖЕТУ",
        "ВСЕ",
        "АКТИВНЫЕ",
        "ЗАКРЫТЫЕ",
    )

    NON_STAGE_PREFIXES: tuple[str, ...] = (
        "ТЕЛ.",
        "ФОРМА",
        "ЧАТ",
        "ВХОДЯЩИЕ ЗАЯВКИ",
        "ПЕРЕШЛИ В СДЕЛКИ",
        "ВСЕГО ПРОДАЖ",
        "ПРОГНОЗ ПРОДАЖ",
    )

    FORBIDDEN_BUCKET_LABELS: tuple[str, ...] = (
        "\u0421\u0435\u0433\u043e\u0434\u043d\u044f",
        "5 \u0434\u043d\u0435\u0439",
        "10 \u0434\u043d\u0435\u0439",
        "15 \u0434\u043d\u0435\u0439",
        "\u0421\u0440\u0435\u0434\u043d\u0435\u0435 \u0432\u0440\u0435\u043c\u044f \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u043a\u0438 \u0437\u0430\u044f\u0432\u043a\u0438",
        "\u041f\u0440\u043e\u0433\u043d\u043e\u0437 \u043f\u0440\u043e\u0434\u0430\u0436",
    )
    FORBIDDEN_BUCKET_DAY_PATTERN = re.compile(
        r"^\d+\s*(?:\u0434(?:\u043d(?:\u0435\u0439)?)?\.?)?$",
        flags=re.IGNORECASE,
    )

    DEALS_LINE_PATTERN = re.compile(
        r"^\s*(?P<count>\d[\d\s]*)\s*(?:\u0441\u0434\u0435\u043b\u043a\u0430|\u0441\u0434\u0435\u043b\u043a\u0438|\u0441\u0434\u0435\u043b\u043e\u043a)\b",
        flags=re.IGNORECASE,
    )
    PERCENT_PATTERN = re.compile(r"^\d{1,3}%$")
    DURATION_PATTERN = re.compile(r"^\d+\s*\u0434\.$", flags=re.IGNORECASE)
    WHITESPACE_PATTERN = re.compile(r"\s+")
    TOP_CARD_FORBIDDEN_PATTERN = re.compile(
        r"(?:\u20bd|\$|\u20ac|\u00a3|\d+\s*\u20bd|\d+[\s\u202f]\d+)",
        flags=re.IGNORECASE,
    )
    TOP_CARDS_MAX_ITEMS = 8
    TOP_CARD_LABEL_WHITELIST: tuple[str, ...] = (
        "\u041d\u041e\u0412\u042b\u0415",
        "\u0412\u042b\u0418\u0413\u0420\u0410\u041d\u041e",
        "\u041f\u0420\u041e\u0418\u0413\u0420\u0410\u041d\u041e",
    )

    def __init__(self, settings: BrowserSettings, project_root: Path) -> None:
        self.settings = settings
        self.project_root = project_root
        self.logger = logging.getLogger("project")

    def open_analytics_page(self, page: Page, tolerate_timeout: bool = True) -> None:
        if not self.settings.analytics_url:
            return

        page.goto(self.settings.analytics_url, wait_until="domcontentloaded")

        try:
            page.wait_for_load_state("load", timeout=min(self.settings.timeout_ms, 8000))
        except PlaywrightTimeoutError as exc:
            if not tolerate_timeout:
                raise
            self.logger.warning(
                "Page did not reach 'load' state in soft wait, continue anyway: %s", exc
            )

    def capture_view(self, page: Page, file_stem: str) -> Path:
        screenshot_path = self.settings.screenshots_dir / f"{file_stem}.png"
        safe_path = ensure_inside_root(screenshot_path, self.project_root)
        page.screenshot(path=str(safe_path), full_page=True)
        return safe_path

    def dump_visible_text(self, file_stem: str, visible_text: str) -> Path:
        debug_dir = self._debug_dir()
        text_path = ensure_inside_root(debug_dir / f"{file_stem}_visible_text.txt", self.project_root)
        text_path.write_text(visible_text, encoding="utf-8")
        return text_path

    def dump_right_section(self, file_stem: str, right_lines: list[str]) -> Path:
        """Save only the sliced right-panel section text for parser debugging."""
        debug_dir = self._debug_dir()
        right_path = ensure_inside_root(debug_dir / f"{file_stem}_right_section.txt", self.project_root)
        payload = "\n".join(right_lines)
        right_path.write_text(payload, encoding="utf-8")
        return right_path

    def dump_right_section_indexed(self, file_stem: str, right_lines: list[str]) -> Path:
        """Save indexed line-by-line classification debug for right section."""
        debug_dir = self._debug_dir()
        indexed_path = ensure_inside_root(
            debug_dir / f"{file_stem}_right_section_indexed.txt",
            self.project_root,
        )

        rows: list[str] = []
        for idx, line in enumerate(right_lines):
            escaped = line.replace('"', '\\"')
            rows.append(
                f"[{idx:02d}] text=\"{escaped}\" | "
                f"percent={self.is_percent_line(line)} "
                f"duration={self.is_duration_line(line)} "
                f"deals={self.is_deals_line(line)} "
                f"service={self.is_service_line(line)} "
                f"stage={self.is_stage_candidate(line)}"
            )

        indexed_path.write_text("\n".join(rows), encoding="utf-8")
        return indexed_path

    def dump_candidate_selectors(self, page: Page, file_stem: str) -> Path:
        debug_dir = self._debug_dir()
        selectors_path = ensure_inside_root(debug_dir / f"{file_stem}_selectors.json", self.project_root)

        report: list[dict[str, object]] = []
        for selector in self.CANDIDATE_SELECTORS:
            locator = page.locator(selector)
            try:
                count = locator.count()
            except Exception as exc:  # pragma: no cover
                report.append({"selector": selector, "error": str(exc)})
                continue

            samples: list[str] = []
            for idx in range(min(count, 10)):
                try:
                    text = locator.nth(idx).inner_text(timeout=1000).strip()
                except Exception:
                    text = ""
                if text:
                    samples.append(text[:500])

            report.append({"selector": selector, "count": count, "sample_texts": samples})

        selectors_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return selectors_path

    def _tab_label_for_mode(self, tab_mode: TabMode) -> str:
        mapping = {
            "all": "ВСЕ",
            "active": "АКТИВНЫЕ",
            "closed": "ЗАКРЫТЫЕ",
        }
        return mapping[tab_mode]

    def build_tab_mode_url(self, current_url: str, tab_mode: TabMode) -> str:
        """Build analytics URL for selected tab mode via deals_type query param."""
        if not current_url.strip():
            raise RuntimeError("Cannot build tab URL from empty current URL.")

        parts = urlsplit(current_url)
        query_pairs = parse_qsl(parts.query, keep_blank_values=True)
        cleaned_pairs = [(key, value) for key, value in query_pairs if key.lower() != "deals_type"]
        cleaned_pairs.append(("deals_type", tab_mode))
        new_query = urlencode(cleaned_pairs, doseq=True)

        return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))

    def _normalize_label(self, text: str) -> str:
        return self.WHITESPACE_PATTERN.sub(" ", text.strip()).upper()

    def _is_analytics_url(self, url: str) -> bool:
        normalized = url.lower()
        return "/stats/" in normalized or "/stats/pipeline/" in normalized

    def _labels_from_container(self, container: Locator) -> list[str]:
        """Collect visible tab/button-like labels inside one container."""
        candidate_selectors = (
            "[role='tab']",
            "button",
            "a",
            "[class*='tab']",
            "[class*='switch']",
            "[class*='segment']",
            "[class*='tabs'] *",
        )
        labels: list[str] = []
        seen: set[str] = set()

        for selector in candidate_selectors:
            locator = container.locator(selector)
            try:
                count = min(locator.count(), 120)
            except Exception:
                continue

            for idx in range(count):
                try:
                    raw = locator.nth(idx).inner_text(timeout=300)
                except Exception:
                    continue

                normalized = self._normalize_label(raw)
                if not normalized:
                    continue
                if len(normalized) > 60:
                    continue
                if normalized in seen:
                    continue

                seen.add(normalized)
                labels.append(normalized)

        return labels

    def _score_tab_container_text(self, normalized_text: str) -> int:
        score = 0
        for anchor in self.TAB_ANCHORS:
            if anchor in normalized_text:
                score += 1
        if self.SORT_ANCHOR in normalized_text:
            score += 2
        if "ПО БЮДЖЕТУ" in normalized_text:
            score += 2
        return score

    def find_analytics_tab_container(self, page: Page) -> Locator | None:
        """Find analytics block container that contains tabs and sort anchors."""
        candidate_selectors = ("main", "section", "div", "article")
        best_score = -1
        best_locator: Locator | None = None
        best_source = "n/a"
        best_idx = -1
        in_analytics = self._is_analytics_url(page.url)

        # Strategy 1 (preferred): pre-filter containers that already contain tab anchor texts.
        anchored = page.locator("main, section, div, article").filter(has_text="ВСЕ")
        anchored = anchored.filter(has_text="АКТИВНЫЕ").filter(has_text="ЗАКРЫТЫЕ")
        try:
            anchored_count = min(anchored.count(), 80)
        except Exception:
            anchored_count = 0

        for idx in range(anchored_count):
            item = anchored.nth(idx)
            try:
                text = item.inner_text(timeout=500)
            except Exception:
                continue

            normalized = self._normalize_label(text)
            if not normalized:
                continue

            score = self._score_tab_container_text(normalized)
            if score > best_score:
                best_score = score
                best_locator = item
                best_source = "anchored"
                best_idx = idx

        # Strategy 2: broader scan with larger limit for layouts where anchored filtering is not enough.
        if best_score < 5:
            for selector in candidate_selectors:
                locator = page.locator(selector)
                try:
                    count = min(locator.count(), 500)
                except Exception:
                    continue

                for idx in range(count):
                    item = locator.nth(idx)
                    try:
                        text = item.inner_text(timeout=300)
                    except Exception:
                        continue

                    normalized = self._normalize_label(text)
                    if not normalized:
                        continue

                    score = self._score_tab_container_text(normalized)
                    if score > best_score:
                        best_score = score
                        best_locator = item
                        best_source = selector
                        best_idx = idx

        if best_locator is None:
            self.logger.warning(
                "Analytics tab container was not found. best_score=%s in_analytics=%s url=%s",
                best_score,
                in_analytics,
                page.url,
            )
            return None

        # Strong match: tabs + sorting anchors.
        if best_score >= 5:
            self.logger.info(
                "Analytics tab container selected: source=%s index=%s score=%s in_analytics=%s",
                best_source,
                best_idx,
                best_score,
                in_analytics,
            )
            return best_locator

        # Fallback in analytics URL: keep run possible even if only tab anchors were detected.
        if in_analytics and best_score >= 3:
            self.logger.warning(
                "Analytics tab container selected by fallback (weak score=%s source=%s index=%s url=%s).",
                best_score,
                best_source,
                best_idx,
                page.url,
            )
            return best_locator

        self.logger.warning(
            "Analytics tab container not found or weak match (score=%s, source=%s, in_analytics=%s, url=%s).",
            best_score,
            best_source,
            in_analytics,
            page.url,
        )
        return None

    def list_available_tab_labels(self, page: Page) -> list[str]:
        """Collect tab labels only inside analytics container."""
        container = self.find_analytics_tab_container(page)
        if container is None:
            return []
        return self._labels_from_container(container)

    def switch_tab_mode(self, page: Page, tab_mode: TabMode) -> None:
        """Switch analytics tab in read-only mode by clicking tab labels."""
        label = self._tab_label_for_mode(tab_mode)
        normalized_label = self._normalize_label(label)
        self.logger.info("Switching analytics tab: %s", tab_mode)

        container = self.find_analytics_tab_container(page)
        if container is None:
            if self._is_analytics_url(page.url):
                self.logger.warning(
                    "Using fallback container=main because analytics container was not found. url=%s",
                    page.url,
                )
                container = page.locator("main").first
            else:
                raise RuntimeError("Could not switch tab. Analytics tab container not found.")

        available_labels = self._labels_from_container(container)
        if available_labels:
            self.logger.info("Available tab labels in analytics container: %s", available_labels[:25])
        else:
            self.logger.warning("No tab labels found inside analytics container.")

        tab_locators = [
            ("role=tab exact", container.get_by_role("tab", name=label, exact=True)),
            ("role=button exact", container.get_by_role("button", name=label, exact=True)),
            ("text exact", container.get_by_text(label, exact=True)),
            ("role=tab contains", container.locator(f"[role='tab']:has-text('{label}')")),
            ("button contains", container.locator(f"button:has-text('{label}')")),
            ("link contains", container.locator(f"a:has-text('{label}')")),
            ("text contains", container.get_by_text(label)),
        ]

        clicked = False
        for strategy_name, locator in tab_locators:
            try:
                if locator.count() > 0:
                    locator.first.click(timeout=4000)
                    self.logger.info("Tab switch click strategy worked: %s", strategy_name)
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            clicked = self._click_tab_by_candidates(container, normalized_label)
            if clicked:
                self.logger.info("Tab switch click strategy worked: container candidate-scan")

        if not clicked:
            self.logger.error(
                "Could not switch tab. target_label=%s available_labels=%s",
                label,
                available_labels[:25],
            )
            raise RuntimeError(f"Could not switch tab. Tab label not found: {label}")

        # Short wait to let analytics section refresh after tab switch.
        page.wait_for_timeout(1200)
        url_ok = self._is_analytics_url(page.url)
        self.logger.info("URL after tab click: %s | in_analytics=%s", page.url, url_ok)
        if not url_ok:
            self.logger.error("Tab click moved out of analytics context. current_url=%s", page.url)
            raise RuntimeError(f"Could not switch tab safely. Left analytics page: {page.url}")

    def _click_tab_by_candidates(self, container: Locator, normalized_label: str) -> bool:
        candidate_selectors = (
            "[role='tab']",
            "button",
            "a",
            "[class*='tab']",
            "[class*='switch']",
            "[class*='segment']",
        )

        for selector in candidate_selectors:
            locator = container.locator(selector)
            try:
                count = min(locator.count(), 120)
            except Exception:
                continue

            for idx in range(count):
                item = locator.nth(idx)
                try:
                    text = item.inner_text(timeout=300)
                except Exception:
                    continue

                if normalized_label not in self._normalize_label(text):
                    continue

                try:
                    item.click(timeout=3000)
                    return True
                except Exception:
                    continue

        return False

    def read_all_tab_modes_by_click(
        self,
        page: Page,
        source_kind: SourceKind,
        filter_id: str,
        on_snapshot: Callable[[AnalyticsSnapshot], None] | None = None,
    ) -> list[AnalyticsSnapshot]:
        """Legacy click-based mode (kept as fallback/debug)."""
        snapshots: list[AnalyticsSnapshot] = []

        for mode in ("all", "active", "closed"):
            tab_mode: TabMode = mode
            try:
                self.switch_tab_mode(page, tab_mode)
                snapshot = self.read_current_view(
                    page=page,
                    source_kind=source_kind,
                    filter_id=filter_id,
                    tab_mode=tab_mode,
                )
            except Exception as exc:
                self.logger.error("Stopped click-based all-tab-modes on tab=%s: %s", tab_mode, exc)
                break

            snapshots.append(snapshot)
            self.logger.info(
                "Tab read ok (click): tab=%s stages=%s total_count=%s",
                snapshot.tab_mode,
                len(snapshot.stages),
                snapshot.total_count,
            )

            if on_snapshot is not None:
                on_snapshot(snapshot)

        return snapshots

    def read_all_tab_modes_by_url(
        self,
        page: Page,
        source_kind: SourceKind,
        filter_id: str,
        on_snapshot: Callable[[AnalyticsSnapshot], None] | None = None,
    ) -> list[AnalyticsSnapshot]:
        """Primary mode: switch analytics tabs by updating deals_type in URL."""
        snapshots: list[AnalyticsSnapshot] = []

        base_url = page.url
        if not base_url.strip():
            raise RuntimeError("Cannot run URL-based tab switching: current page URL is empty.")

        built_urls: dict[TabMode, str] = {
            "all": self.build_tab_mode_url(base_url, "all"),
            "active": self.build_tab_mode_url(base_url, "active"),
            "closed": self.build_tab_mode_url(base_url, "closed"),
        }

        self.logger.info("URL-based all-tab-modes source URL: %s", base_url)
        self.logger.info("URL for tab=all: %s", built_urls["all"])
        self.logger.info("URL for tab=active: %s", built_urls["active"])
        self.logger.info("URL for tab=closed: %s", built_urls["closed"])

        for mode in ("all", "active", "closed"):
            tab_mode: TabMode = mode
            target_url = built_urls[tab_mode]

            try:
                page.goto(target_url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("load", timeout=min(self.settings.timeout_ms, 8000))
                except PlaywrightTimeoutError as exc:
                    self.logger.warning(
                        "Soft wait timeout after URL tab switch tab=%s url=%s: %s",
                        tab_mode,
                        target_url,
                        exc,
                    )

                snapshot = self.read_current_view(
                    page=page,
                    source_kind=source_kind,
                    filter_id=filter_id,
                    tab_mode=tab_mode,
                )
            except Exception as exc:
                self.logger.error("Stopped URL-based all-tab-modes on tab=%s url=%s: %s", tab_mode, target_url, exc)
                break

            snapshots.append(snapshot)
            self.logger.info(
                "Tab read ok (url): tab=%s stages=%s total_count=%s url=%s",
                snapshot.tab_mode,
                len(snapshot.stages),
                snapshot.total_count,
                snapshot.url,
            )

            if on_snapshot is not None:
                on_snapshot(snapshot)

        return snapshots

    def read_all_tab_modes(
        self,
        page: Page,
        source_kind: SourceKind,
        filter_id: str,
        on_snapshot: Callable[[AnalyticsSnapshot], None] | None = None,
    ) -> list[AnalyticsSnapshot]:
        """Read all tabs using URL-based deals_type switching (primary mode)."""
        return self.read_all_tab_modes_by_url(
            page=page,
            source_kind=source_kind,
            filter_id=filter_id,
            on_snapshot=on_snapshot,
        )

    def read_current_view(
        self,
        page: Page,
        source_kind: SourceKind,
        filter_id: str,
        tab_mode: TabMode,
    ) -> AnalyticsSnapshot:
        now = datetime.now()
        export_name = f"analytics_{source_kind}_{filter_id}_{tab_mode}_{now.strftime('%Y%m%d_%H%M%S')}"

        screenshot_path = self.capture_view(page, export_name)
        body_text = page.locator("body").inner_text(timeout=self.settings.timeout_ms)
        debug_text_path = self.dump_visible_text(export_name, body_text)
        debug_selectors_path = self.dump_candidate_selectors(page, export_name)

        structured_right_panel, top_cards = self._extract_from_analytics_text(body_text, export_name)
        stages_right_panel: list[StageCount] = []
        parse_method = "analytics_text_structured"

        pipeline_row_count = self.get_pipeline_row_count(page)
        if pipeline_row_count > 0:
            self.logger.info(
                "DOM-first right panel parsing: root=%s row_count=%s",
                self.PIPELINE_ROOT_SELECTOR,
                pipeline_row_count,
            )
            stages_right_panel = self._extract_pipeline_first_stages(page)
            if stages_right_panel:
                parse_method = "dom_pipeline_first"
            else:
                self.logger.warning(
                    "DOM-first parser did not return valid stages from %s. "
                    "Fallback to structured text parser.",
                    self.PIPELINE_ROOT_SELECTOR,
                )
        else:
            self.logger.warning(
                "DOM-first right panel parsing skipped: no rows found under %s",
                self.PIPELINE_ROOT_SELECTOR,
            )

        if not stages_right_panel:
            stages_right_panel = structured_right_panel
            parse_method = "analytics_text_structured"

        if not stages_right_panel:
            self.logger.warning("Structured right-panel section was not parsed. Falling back to DOM parser.")
            stages_right_panel = self._extract_stages_from_dom(page)
            parse_method = "dom"

        if not stages_right_panel:
            self.logger.warning("DOM parser did not find stages. Falling back to generic text parser.")
            stages_right_panel = self._extract_stages_fallback(body_text)
            parse_method = "fallback"

        total_count = sum(item.count for item in stages_right_panel)

        return AnalyticsSnapshot(
            source_kind=source_kind,
            filter_id=filter_id,
            tab_mode=tab_mode,
            read_at=now,
            stages=stages_right_panel,
            total_count=total_count,
            top_cards=top_cards,
            url=page.url,
            export_name=export_name,
            screenshot_path=str(screenshot_path),
            parse_method=parse_method,
            debug_text_path=str(debug_text_path),
            debug_selectors_path=str(debug_selectors_path),
        )

    def get_pipeline_row_count(self, page: Page) -> int:
        for selector in self.PIPELINE_ROW_SELECTORS:
            locator = page.locator(selector)
            try:
                count = locator.count()
            except Exception:
                continue
            if count > 0:
                return count
        return 0

    def extract_pipeline_stage_lines(self, page: Page) -> list[str]:
        """Read raw stage/deals lines from #pipeline_first rows."""
        for selector in self.PIPELINE_ROW_SELECTORS:
            locator = page.locator(selector)
            try:
                count = min(locator.count(), 500)
            except Exception:
                continue

            if count <= 0:
                continue

            lines: list[str] = []
            for idx in range(count):
                try:
                    raw_text = locator.nth(idx).inner_text(timeout=600)
                except Exception:
                    continue

                for raw_line in raw_text.splitlines():
                    line = raw_line.strip()
                    if line:
                        lines.append(line)

            if lines:
                return lines

        return []

    def _extract_pipeline_first_stages(self, page: Page) -> list[StageCount]:
        """DOM-first parse of right panel from #pipeline_first block."""
        lines = self.extract_pipeline_stage_lines(page)
        if not lines:
            self.logger.warning("DOM-first parser: no lines extracted from %s", self.PIPELINE_ROOT_SELECTOR)
            return []

        parsed = self._parse_right_panel_from_lines(lines)
        invalid, valid_count, forbidden_count, forbidden_names = self._is_invalid_structured_right_panel(parsed)
        self.logger.info(
            "DOM-first parser summary: lines=%s parsed_stages=%s valid_count=%s forbidden_count=%s",
            len(lines),
            len(parsed),
            valid_count,
            forbidden_count,
        )

        if invalid:
            self.logger.warning(
                "DOM-first parser rejected: valid_count=%s forbidden_count=%s forbidden_names=%s",
                valid_count,
                forbidden_count,
                forbidden_names,
            )
            return []

        return parsed

    def export_snapshot(self, snapshot: AnalyticsSnapshot) -> tuple[Path, Path]:
        json_path = ensure_inside_root(self.settings.exports_dir / f"{snapshot.export_name}.json", self.project_root)
        csv_path = ensure_inside_root(self.settings.exports_dir / f"{snapshot.export_name}.csv", self.project_root)

        json_payload = snapshot.model_dump(mode="json")
        json_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=[
                    "export_name",
                    "read_at",
                    "source_kind",
                    "filter_id",
                    "tab_mode",
                    "parse_method",
                    "stage_name",
                    "count",
                    "total_count",
                    "url",
                    "screenshot_path",
                    "debug_text_path",
                    "debug_selectors_path",
                ],
            )
            writer.writeheader()
            for stage in snapshot.stages:
                writer.writerow(
                    {
                        "export_name": snapshot.export_name,
                        "read_at": snapshot.read_at.isoformat(),
                        "source_kind": snapshot.source_kind,
                        "filter_id": snapshot.filter_id,
                        "tab_mode": snapshot.tab_mode,
                        "parse_method": snapshot.parse_method,
                        "stage_name": stage.stage_name,
                        "count": stage.count,
                        "total_count": snapshot.total_count,
                        "url": snapshot.url,
                        "screenshot_path": snapshot.screenshot_path,
                        "debug_text_path": snapshot.debug_text_path,
                        "debug_selectors_path": snapshot.debug_selectors_path,
                    }
                )

        return json_path, csv_path

    def _extract_from_analytics_text(self, body_text: str, file_stem: str) -> tuple[list[StageCount], list[StageCount]]:
        lines = [line.strip() for line in body_text.splitlines() if line.strip()]

        top_lines, top_start_found, top_end_found = self._slice_top_cards_section(lines)
        right_lines, right_start_found, right_stop_found = self._slice_right_panel_section(lines)
        right_section_path = self.dump_right_section(file_stem, right_lines)
        right_section_indexed_path = self.dump_right_section_indexed(file_stem, right_lines)

        self.logger.info(
            "Structured sections: top_lines=%s right_lines=%s anchor_sort_found=%s stop_anchor_found=%s",
            len(top_lines),
            len(right_lines),
            right_start_found,
            right_stop_found,
        )
        self.logger.info("Right section debug dump: %s", right_section_path)
        self.logger.info("Right section indexed debug dump: %s", right_section_indexed_path)

        if not top_start_found or not top_end_found:
            self.logger.warning(
                "Top cards anchors are incomplete (start=%s end=%s). Top cards may be partial.",
                top_start_found,
                top_end_found,
            )

        if not right_start_found:
            self.logger.warning("Right panel start anchor 'ПО КОЛИЧЕСТВУ' not found.")

        top_cards = self._parse_top_cards_from_lines(top_lines)
        right_panel = self._parse_right_panel_from_lines(right_lines)
        invalid_structured, valid_count, forbidden_count, forbidden_names = self._is_invalid_structured_right_panel(right_panel)
        if invalid_structured:
            self.logger.warning(
                "Structured right panel rejected: valid_count=%s forbidden_count=%s forbidden_names=%s",
                valid_count,
                forbidden_count,
                forbidden_names,
            )
            right_panel = []

        self.logger.info(
            "Structured parse results: right_panel_stages=%s top_cards=%s",
            len(right_panel),
            len(top_cards),
        )

        return right_panel, top_cards

    def _slice_top_cards_section(self, lines: list[str]) -> tuple[list[str], bool, bool]:
        tabs_start_idx = self._find_tabs_block_start(lines)
        if tabs_start_idx is None:
            return [], False, False

        start_idx: int | None = None
        for idx in range(tabs_start_idx - 1, -1, -1):
            if lines[idx].strip().upper() == self.TOP_START_ANCHOR:
                start_idx = idx
                break

        if start_idx is None or start_idx >= tabs_start_idx:
            return [], start_idx is not None, True

        return lines[start_idx + 1 : tabs_start_idx], True, True

    def _find_tabs_block_start(self, lines: list[str]) -> int | None:
        for idx in range(len(lines)):
            if lines[idx].strip().upper() != "ВСЕ":
                continue

            window = {line.strip().upper() for line in lines[idx : idx + 8]}
            if "АКТИВНЫЕ" in window and "ЗАКРЫТЫЕ" in window:
                return idx

        return None

    def _slice_right_panel_section(
        self,
        lines: list[str],
        warn_on_early_stop: bool = True,
    ) -> tuple[list[str], bool, bool]:
        start_idx = self._find_line_index(lines, self.SORT_ANCHOR, exact=False)
        if start_idx is None:
            self.logger.info("Right section slicing: start_idx=None stop_idx=None lines=0")
            return [], False, False

        stop_idx: int | None = None
        stop_anchor: str | None = None
        for anchor in self.RIGHT_STOP_ANCHORS:
            idx = self._find_line_index(lines, anchor, start=start_idx + 1, exact=False)
            if idx is None:
                continue
            if stop_idx is None or idx < stop_idx:
                stop_idx = idx
                stop_anchor = anchor

        if stop_idx is None:
            sliced = lines[start_idx + 1 :]
            self.logger.info(
                "Right section slicing: start_idx=%s stop_idx=None stop_anchor=None sliced_lines=%s",
                start_idx,
                len(sliced),
            )
            return sliced, True, False

        sliced = lines[start_idx + 1 : stop_idx]
        # Guard against too-early stop anchor hits that cut real stage list.
        if len(sliced) < 15:
            sliced_tail = lines[start_idx + 1 :]
            log_message = (
                "Right section slicing stop anchor too early: start_idx=%s stop_idx=%s stop_anchor=%s "
                "sliced_lines=%s -> fallback_to_tail_lines=%s"
            )
            log_args = (start_idx, stop_idx, stop_anchor, len(sliced), len(sliced_tail))
            if warn_on_early_stop:
                self.logger.warning(log_message, *log_args)
            else:
                self.logger.info(log_message, *log_args)
            return sliced_tail, True, False

        self.logger.info(
            "Right section slicing: start_idx=%s stop_idx=%s stop_anchor=%s sliced_lines=%s",
            start_idx,
            stop_idx,
            stop_anchor,
            len(sliced),
        )
        return sliced, True, True

    def _find_line_index(self, lines: list[str], anchor: str, start: int = 0, exact: bool = False) -> int | None:
        target = anchor.strip().upper()
        for idx in range(start, len(lines)):
            line = lines[idx].strip().upper()
            if exact and line == target:
                return idx
            if not exact and target in line:
                return idx
        return None

    def is_percent_line(self, text: str) -> bool:
        return bool(self.PERCENT_PATTERN.match(text.strip()))

    def is_duration_line(self, text: str) -> bool:
        return bool(self.DURATION_PATTERN.match(text.strip()))

    def is_deals_line(self, text: str) -> bool:
        candidate = text.strip()
        return bool(self.DEALS_LINE_PATTERN.match(candidate))

    def is_service_line(self, text: str) -> bool:
        upper = text.strip().upper()
        if upper in self.SERVICE_STAGE_LABELS:
            return True
        return any(upper.startswith(prefix) for prefix in self.NON_STAGE_PREFIXES)

    def _is_forbidden_bucket_label(self, text: str) -> bool:
        candidate = text.strip()
        lower = candidate.lower()
        if any(lower == item.lower() for item in self.FORBIDDEN_BUCKET_LABELS):
            return True
        return bool(self.FORBIDDEN_BUCKET_DAY_PATTERN.match(candidate))

    def is_stage_candidate(self, text: str) -> bool:
        candidate = text.strip()
        if not candidate:
            return False

        if self._is_forbidden_bucket_label(candidate):
            return False
        if self.is_service_line(candidate):
            return False
        if self.is_percent_line(candidate):
            return False
        if self.is_duration_line(candidate):
            return False
        if self.is_deals_line(candidate):
            return False
        if "?" in candidate:
            return False
        if re.match(r"^\d[\d\s]*$", candidate):
            return False

        return True

    def _is_invalid_structured_right_panel(self, stages: list[StageCount]) -> tuple[bool, int, int, list[str]]:
        if not stages:
            return True, 0, 0, []

        valid_count = 0
        forbidden_count = 0
        forbidden_names: list[str] = []
        for stage in stages:
            if self._is_forbidden_bucket_label(stage.stage_name):
                forbidden_count += 1
                if len(forbidden_names) < 10:
                    forbidden_names.append(stage.stage_name)
            else:
                valid_count += 1

        invalid = valid_count == 0 or forbidden_count >= max(2, len(stages) - 1)
        return invalid, valid_count, forbidden_count, forbidden_names

    def _parse_right_panel_from_lines(self, lines: list[str]) -> list[StageCount]:
        """Two-pass parse: normalize lines, then bind each deals line to nearest previous stage."""
        normalized_lines = [line.strip() for line in lines if line.strip()]

        stages: dict[str, StageCount] = {}
        deals_lines_found = 0
        stage_candidates_found = 0
        stage_deals_pairs = 0
        missing_stage_samples = 0
        deals_samples: list[str] = []
        stage_samples: list[str] = []

        for idx, line in enumerate(normalized_lines):
            is_deals = self.is_deals_line(line)
            if is_deals:
                deals_lines_found += 1
                if len(deals_samples) < 10:
                    deals_samples.append(line)
            elif self.is_stage_candidate(line) and len(stage_samples) < 10:
                stage_samples.append(line)

            if not is_deals:
                continue

            deals_count = self._extract_deals_count(line)
            if deals_count is None:
                continue

            stage_name: str | None = None
            for back in (1, 2):
                prev_idx = idx - back
                if prev_idx < 0:
                    continue

                candidate = normalized_lines[prev_idx].strip()
                if self.is_percent_line(candidate) or self.is_duration_line(candidate):
                    continue
                if self.is_stage_candidate(candidate):
                    stage_candidates_found += 1
                    stage_name = candidate
                    break

            if stage_name is None:
                if missing_stage_samples < 3:
                    self.logger.info("Deals line without stage sample: %s", line)
                    missing_stage_samples += 1
                continue

            if stage_name not in stages:
                stages[stage_name] = StageCount(stage_name=stage_name, count=deals_count)
                stage_deals_pairs += 1

        self.logger.info(
            "Right panel parsing: right_section_lines=%s deals_lines_found=%s stage_candidates_found=%s "
            "stage_deals_pairs=%s stages=%s",
            len(normalized_lines),
            deals_lines_found,
            stage_candidates_found,
            stage_deals_pairs,
            len(stages),
        )
        self.logger.info("Right panel deals_line samples: %s", deals_samples)
        self.logger.info("Right panel stage_candidate samples: %s", stage_samples)
        self.logger.info("Right panel stage names: %s", list(stages.keys()))

        return list(stages.values())

    def _extract_deals_count(self, line: str) -> int | None:
        match = self.DEALS_LINE_PATTERN.search(line)
        if not match:
            return None
        raw_count = match.group("count")
        return int(re.sub(r"\s+", "", raw_count))

    def _parse_top_cards_from_lines(self, lines: list[str]) -> list[StageCount]:
        """Parse only upper summary KPI cards to avoid side-block overcapture."""
        result: dict[str, StageCount] = {}
        normalized_allowed = {item.upper() for item in self.TOP_CARD_LABEL_WHITELIST}
        number_only_pattern = re.compile(r"^\d[\d\s]*$")

        for idx, raw_label in enumerate(lines):
            label = raw_label.strip()
            if not label:
                continue

            normalized = self.WHITESPACE_PATTERN.sub(" ", label).upper()
            if normalized not in normalized_allowed:
                continue
            if label in result:
                continue

            count_value: int | None = None
            for look_ahead in range(1, 7):
                value_idx = idx + look_ahead
                if value_idx >= len(lines):
                    break

                candidate = lines[value_idx].strip()
                if not number_only_pattern.match(candidate):
                    continue

                parsed = int(re.sub(r"\s+", "", candidate))
                if parsed < 0:
                    continue

                count_value = parsed
                break

            if count_value is None:
                continue

            result[label] = StageCount(stage_name=label, count=count_value)

        self.logger.info(
            "Top cards parsing: cards=%s labels=%s",
            len(result),
            list(result.keys()),
        )
        return list(result.values())

    def _is_valid_top_card_label(self, candidate: str) -> bool:
        cleaned = candidate.strip()
        if not cleaned:
            return False

        if not self.is_stage_candidate(cleaned):
            return False

        upper = cleaned.upper()
        if any(noise in upper for noise in self.TOP_NOISE_LABELS):
            return False

        # Drop monetary/amount-like labels accidentally captured from nearby blocks.
        if self.TOP_CARD_FORBIDDEN_PATTERN.search(cleaned):
            return False

        # Keep top-card names text-only to avoid "0 ?"-like artifacts.
        if any(ch.isdigit() for ch in cleaned):
            return False

        return True

    def _find_previous_valid_label(self, lines: list[str], current_index: int) -> str | None:
        for back_offset in (1, 2):
            idx = current_index - back_offset
            if idx < 0:
                continue

            candidate = lines[idx].strip()
            if not self._is_valid_top_card_label(candidate):
                continue

            return candidate

        return None

    def _extract_stages_from_dom(self, page: Page) -> list[StageCount]:
        collected: dict[str, StageCount] = {}

        for selector in self.CANDIDATE_SELECTORS:
            locator = page.locator(selector)
            try:
                count = locator.count()
            except Exception:
                continue

            for idx in range(min(count, 200)):
                try:
                    text = locator.nth(idx).inner_text(timeout=500).strip()
                except Exception:
                    continue

                for stage in self._parse_text_to_stages(text):
                    collected.setdefault(stage.stage_name, stage)

        return list(collected.values())

    def _extract_stages_fallback(self, body_text: str) -> list[StageCount]:
        unique: dict[str, StageCount] = {}
        for stage in self._parse_text_to_stages(body_text):
            unique.setdefault(stage.stage_name, stage)

        return list(unique.values())

    def _parse_text_to_stages(self, text: str) -> list[StageCount]:
        stages: list[StageCount] = []
        pattern = re.compile(r"^(?P<name>[^\d\n][^\n]*?)\s+(?P<count>\d{1,9})$")

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            match = pattern.match(line)
            if not match:
                continue

            name = match.group("name").strip(" -:\t")
            count_value = int(match.group("count"))

            if not name:
                continue
            if len(name) > 120:
                continue
            if name.isdigit():
                continue

            stages.append(StageCount(stage_name=name, count=count_value))

        return stages

    def _debug_dir(self) -> Path:
        debug_dir = ensure_inside_root(self.settings.exports_dir / "debug", self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        return debug_dir














