"""Google Sheets layout writer (anchor-based, formatting-safe numeric updates)."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.analytics.scenario_executor import ScenarioExecutor
from src.writers.compiler import compile_stage_pivot
from src.writers.google_sheets_ui_writer import GoogleSheetsUIWriter
from src.writers.layout_filter_dsl import (
    LayoutBlockConfig,
    LayoutScenario,
    ScenarioRunResult,
    normalize_field_name,
    parse_layout_row,
    select_best_scenario,
)
from src.writers.models import CompiledProfileAnalyticsResult, WriterDestinationConfig


DEFAULT_HEADER_ALIASES = {
    "stage": ["СЌС‚Р°Рї", "СЃС‚Р°С‚СѓСЃ"],
    "all": ["РІСЃРµ", "РІСЃРµ (С€С‚)", "РІСЃРµ С€С‚", "РІСЃРµ, С€С‚", "РІСЃРµ (РєРѕР»-РІРѕ)"],
    "active": ["Р°РєС‚РёРІРЅС‹Рµ", "Р°РєС‚РёРІРЅС‹Рµ (С€С‚)", "Р°РєС‚РёРІРЅС‹Рµ С€С‚"],
    "closed": ["Р·Р°РєСЂС‹С‚С‹Рµ", "Р·Р°РєСЂС‹С‚С‹Рµ (С€С‚)", "Р·Р°РєСЂС‹С‚С‹Рµ С€С‚"],
}


@dataclass(frozen=True)
class DslRowCandidate:
    row: int
    col: int
    raw_text: str
    config: LayoutBlockConfig


@dataclass(frozen=True)
class BlockCandidate:
    header_row: int
    stage_col: int
    all_col: int
    active_col: int
    closed_col: int
    title_row: int
    title_col: int
    title_raw: str
    title_norm: str
    score: int
    matched_alias: str
    dsl: LayoutBlockConfig | None


class GoogleSheetsUILayoutWriter:
    """Update existing layout blocks by text anchors without full-sheet overwrite."""

    def __init__(self, project_root: Path) -> None:
        self.logger = logging.getLogger("project")
        self.project_root = project_root
        self.raw_helper = GoogleSheetsUIWriter()
        self._grid_text_cache: dict[tuple[int, int], str] = {}
        self._discovery_max_rows = 120
        self._discovery_max_cols = 6
        self._discovery_empty_row_stop = 12
        self._discovery_max_candidates = 24
        self._header_search_window = 6
        self._block_scan_max_rows = 140
        self._block_scan_max_cols = 10
        self._block_scan_empty_row_stop = 12
        self._cell_read_count = 0
        self._cell_read_hard_limit = 1200
        self._read_limit_hit = False
        self._fallback_formula_reads = 0
        self._fallback_dom_reads = 0
        self._snapshot_source_name = "dom_snapshot"
        self._last_dsl_discovery_stop_reason = ""
        self._last_anchor_discovery_stop_reason = ""
        self._grid_text_source_cache: dict[tuple[int, int], str] = {}
        self._discovery_total_cells_scanned = 0
        self._discovery_non_empty_cells_seen = 0
        self._discovery_dsl_checks = 0
        self._discovery_dsl_matches = 0
        self._discovery_read_samples: list[dict[str, str]] = []
        self._discovery_source_counts: dict[str, int] = {"dom_snapshot": 0, "formula_bar": 0, "other": 0}
        self._discovery_mode = "screenshot_first"
        self._screenshot_scan_pages = 4
        self._screenshot_empty_views_stop = 2

    def write_profile_analytics_result(
        self,
        page: Page,
        compiled_result: CompiledProfileAnalyticsResult,
        destination: WriterDestinationConfig,
        dry_run: bool = False,
        scenario_executor: ScenarioExecutor | None = None,
    ) -> None:
        if not destination.sheet_url.strip():
            raise RuntimeError("Google Sheets destination URL is empty for layout writer.")

        self.logger.info("opening google sheet: %s", destination.sheet_url)
        page.goto(destination.sheet_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("load", timeout=12000)
        except PlaywrightTimeoutError:
            self.logger.info("google sheet load state not fully idle, continue in layout mode")

        self.raw_helper._select_target_tab(page, destination.tab_name)
        self.logger.info("target tab found: %s", destination.tab_name)

        layout = destination.layout_config or {}
        self._configure_discovery_limits(layout)
        self._grid_text_cache = {}
        self._grid_text_source_cache = {}
        self._cell_read_count = 0
        self._cell_read_hard_limit = self._to_int(layout.get("cell_read_hard_limit"), 1200)
        self._read_limit_hit = False
        self._fallback_formula_reads = 0
        self._fallback_dom_reads = 0
        self._discovery_total_cells_scanned = 0
        self._discovery_non_empty_cells_seen = 0
        self._discovery_dsl_checks = 0
        self._discovery_dsl_matches = 0
        self._discovery_read_samples = []
        self._discovery_source_counts = {"dom_snapshot": 0, "formula_bar": 0, "other": 0}
        self.logger.info(
            "bounded scan started: dsl_range=A1:%s%s block_range=A1:%s%s header_window=%s cell_read_hard_limit=%s",
            self.raw_helper._to_col_label(self._discovery_max_cols),
            self._discovery_max_rows,
            self.raw_helper._to_col_label(self._block_scan_max_cols),
            self._block_scan_max_rows,
            self._header_search_window,
            self._cell_read_hard_limit,
        )

        stage_aliases = self._coerce_stage_aliases(layout.get("stage_aliases", {}))
        pivot = compile_stage_pivot(compiled_result=compiled_result, stage_aliases=stage_aliases)
        self.logger.info("layout pivot prepared: stages=%s", len(pivot))

        block_aliases = self._resolve_block_aliases(compiled_result, layout)
        header_aliases = self._merge_header_aliases(layout.get("header_aliases", {}))

        dsl_rows = self._discover_dsl_rows(page, target_blocks=max(1, len(block_aliases)))
        self.logger.info("dsl_rows_found=%s discovery_stop_reason=%s", len(dsl_rows), self._last_dsl_discovery_stop_reason or "<none>")
        self._log_discovery_summary()
        for item in dsl_rows[:20]:
            self.logger.info(
                "dsl_row row=%s col=%s display_name=%s raw=%s",
                item.row,
                item.col,
                item.config.display_name,
                item.raw_text,
            )

        dsl_block_anchors = self._discover_block_anchors_from_dsl(
            page=page,
            dsl_rows=dsl_rows,
            header_aliases=header_aliases,
            target_blocks=max(1, len(block_aliases)),
        )
        self.logger.info(
            "block_anchors_found=%s discovery_stop_reason=%s",
            len(dsl_block_anchors),
            self._last_anchor_discovery_stop_reason or "<none>",
        )

        writes: dict[str, int] = {}
        missing: list[str] = []
        used_anchor_keys: set[tuple[int, int]] = set()
        skipped_blocks: list[dict[str, Any]] = []

        for block_name, aliases in block_aliases.items():
            if "docs.google.com/spreadsheets" not in str(page.url).lower():
                self.logger.info("returning to google sheet before block processing: block=%s", block_name)
                page.goto(destination.sheet_url, wait_until="domcontentloaded")
                self.raw_helper._select_target_tab(page, destination.tab_name)

            anchor = self._find_block_anchor(
                page=page,
                block_name=block_name,
                aliases=aliases,
                header_aliases=header_aliases,
                dsl_rows=dsl_rows,
                dsl_block_anchors=dsl_block_anchors,
                used_anchor_keys=used_anchor_keys,
            )
            if anchor is None:
                dump_path = ""
                screenshot = ""
                try:
                    dump_path = str(self._dump_visible_text(page, prefix=f"layout_block_not_found_{block_name}"))
                except Exception as exc:
                    self.logger.warning("layout block not-found text dump failed: block=%s error=%s", block_name, exc)
                try:
                    screenshot = str(self.raw_helper._save_debug_screenshot(page, prefix=f"layout_block_not_found_{block_name}"))
                except Exception as exc:
                    self.logger.warning("layout block not-found screenshot failed: block=%s error=%s", block_name, exc)

                reason = "block_anchor_not_found"
                skipped_blocks.append({
                    "block_name": block_name,
                    "aliases": list(aliases),
                    "reason": reason,
                    "dump_path": dump_path,
                    "screenshot": screenshot,
                })
                self.logger.warning(
                    "layout block skipped: block=%s aliases=%s reason=%s dump=%s screenshot=%s",
                    block_name,
                    aliases,
                    reason,
                    dump_path or "<none>",
                    screenshot or "<none>",
                )
                continue

            self.logger.info(
                "layout block found: block=%s title_cell=%s title=%s",
                block_name,
                self._cell_ref(anchor.title_col, anchor.title_row),
                anchor.title_raw,
            )
            self.logger.info("layout header row found: block=%s row=%s", block_name, anchor.header_row)
            self.logger.info(
                "layout column map resolved: block=%s stage_col=%s all_col=%s active_col=%s closed_col=%s",
                block_name,
                anchor.stage_col,
                anchor.all_col,
                anchor.active_col,
                anchor.closed_col,
            )

            block_pivot = pivot
            if anchor.dsl is not None:
                self._log_dsl_and_scenario_choice(anchor.dsl, compiled_result, pivot)
                if scenario_executor is not None:
                    self.logger.info(
                        "external per-scenario execution started: block=%s scenarios=%s",
                        anchor.dsl.display_name,
                        len(anchor.dsl.scenarios),
                    )
                    exec_result = scenario_executor.execute_block_scenarios(page=page, block_config=anchor.dsl)
                    self.logger.info("returning to google sheet after scenario execution: block=%s", anchor.dsl.display_name)
                    page.goto(destination.sheet_url, wait_until="domcontentloaded")
                    self.raw_helper._select_target_tab(page, destination.tab_name)

                    if exec_result.best_compiled_result is not None:
                        block_pivot = compile_stage_pivot(
                            compiled_result=exec_result.best_compiled_result,
                            stage_aliases=stage_aliases,
                        )
                        self.logger.info(
                            "external per-scenario execution finished: block=%s best_scenario=%s",
                            anchor.dsl.display_name,
                            exec_result.best_scenario.scenario_index if exec_result.best_scenario else -1,
                        )
                    else:
                        self.logger.error(
                            "all scenarios failed for block=%s; block writes will be skipped",
                            anchor.dsl.display_name,
                        )
                        continue

            stage_row_map = self._build_stage_row_map(
                page=page,
                start_row=anchor.header_row + 1,
                stage_col=anchor.stage_col,
                max_scan_rows=120,
            )
            self.logger.info("layout stage row map resolved: block=%s mapped_stages=%s", block_name, len(stage_row_map))

            block_writes, block_missing = self._build_planned_writes(
                stage_row_map=stage_row_map,
                pivot=block_pivot,
                all_col=anchor.all_col,
                active_col=anchor.active_col,
                closed_col=anchor.closed_col,
                stage_aliases=stage_aliases,
            )
            writes.update(block_writes)
            missing.extend(block_missing)

        self.logger.info("layout planned writes: count=%s dry_run=%s", len(writes), str(dry_run).lower())
        self.logger.info("layout planned writes sample: %s", list(writes.items())[:20])
        if missing:
            self.logger.warning("layout missing stages: %s", sorted(set(missing)))
        if skipped_blocks:
            self.logger.warning("layout skipped blocks count=%s details=%s", len(skipped_blocks), skipped_blocks)

        self.logger.info(
            "bounded scan finished: total_cell_reads=%s total_goto_cell_calls=%s hard_limit=%s limit_hit=%s source_snapshot=%s source_formula_reads=%s source_dom_fallback_reads=%s",
            self._cell_read_count,
            self._cell_read_count,
            self._cell_read_hard_limit,
            str(self._read_limit_hit).lower(),
            self._snapshot_source_name,
            self._fallback_formula_reads,
            self._fallback_dom_reads,
        )

        if dry_run:
            self.logger.info("layout dry-run mode: no sheet write performed")
            return

        if not writes:
            self.logger.warning("layout writer: no writes planned; all blocks were skipped or no stage rows matched")
            return

        for cell_ref, value in writes.items():
            self._write_numeric_cell(page, cell_ref, value)

        self.logger.info("layout cells updated count: %s", len(writes))
        self.logger.info("layout writer finished successfully")

    def debug_inspect_visible_grid(self, page: Page, destination: WriterDestinationConfig) -> None:
        """Isolated diagnostics for visible Google Sheets grid area (no write, no discovery scan)."""
        if not destination.sheet_url.strip():
            raise RuntimeError("Google Sheets destination URL is empty for layout grid inspector.")

        self.logger.info("layout grid inspector start: sheet_url=%s tab=%s", destination.sheet_url, destination.tab_name)
        page.goto(destination.sheet_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("load", timeout=12000)
        except PlaywrightTimeoutError:
            self.logger.info("layout grid inspector: load state not fully idle, continuing")

        self.raw_helper._select_target_tab(page, destination.tab_name)
        self.logger.info("layout grid inspector target tab found: %s", destination.tab_name)

        self.raw_helper._wait_for_sheet_ui_ready(page)
        self.raw_helper.focus_sheet_grid(page)
        page.wait_for_timeout(250)

        self.raw_helper._log_sheet_runtime_state(page, stage="layout_grid_inspector")

        runtime = page.evaluate(
            """() => {
                const clean = (v) => (v || '').toString().replace(/\s+/g, ' ').trim();
                const activeTabEl = document.querySelector('.docs-sheet-active-tab .docs-sheet-tab-name, .docs-sheet-tab[aria-selected="true"] .docs-sheet-tab-name, [role="tab"][aria-selected="true"]');
                return {
                    url: window.location.href,
                    title: document.title || '',
                    readyState: document.readyState || '',
                    gid: new URL(window.location.href).searchParams.get('gid') || '',
                    activeTabText: clean(activeTabEl ? (activeTabEl.innerText || activeTabEl.textContent || '') : ''),
                };
            }"""
        )

        active_cell = self.raw_helper.read_active_cell(page)
        name_box_value = self.raw_helper._read_name_box_value_precise(page)
        formula_value = self.raw_helper._read_formula_bar_value(page)

        self.logger.info(
            "layout inspector runtime: url=%s gid=%s active_tab=%s readyState=%s title=%s active_cell=%s name_box_value=%s formula_bar_value=%s",
            runtime.get("url", ""),
            runtime.get("gid", ""),
            runtime.get("activeTabText", ""),
            runtime.get("readyState", ""),
            runtime.get("title", ""),
            active_cell or "<empty>",
            name_box_value or "<empty>",
            formula_value or "<empty>",
        )

        strategies: list[tuple[str, str]] = [
            ("role_gridcell", "[role='gridcell']"),
            ("role_cell", "[role='cell']"),
            ("data_row_col", "div[data-row][data-col]"),
            ("waffle_descendants", ".waffle-grid-container *"),
            ("grid_descendants", "[role='grid'] *"),
            ("aria_cell_like", "[aria-label]")
        ]

        payload = page.evaluate(
            """({ strategies }) => {
                const clean = (v) => (v || '').toString().replace(/\s+/g, ' ').trim();
                const visible = (el) => {
                    const r = el.getBoundingClientRect();
                    const style = window.getComputedStyle(el);
                    if (!r || r.width <= 1 || r.height <= 1) return false;
                    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') <= 0.01) return false;
                    if (r.bottom < 0 || r.right < 0 || r.top > window.innerHeight || r.left > window.innerWidth) return false;
                    return true;
                };
                const rows = [];
                const counts = {};
                for (const pair of strategies) {
                    const name = pair[0];
                    const selector = pair[1];
                    const nodes = Array.from(document.querySelectorAll(selector));
                    let count = 0;
                    for (const el of nodes) {
                        if (!visible(el)) continue;
                        count += 1;
                        const rect = el.getBoundingClientRect();
                        rows.push({
                            strategy: name,
                            selector,
                            tagName: (el.tagName || '').toLowerCase(),
                            role: el.getAttribute('role') || '',
                            ariaLabel: clean(el.getAttribute('aria-label') || ''),
                            innerText: clean(el.innerText || ''),
                            textContent: clean(el.textContent || ''),
                            dataRow: el.getAttribute('data-row') || '',
                            dataCol: el.getAttribute('data-col') || '',
                            rowIndex: el.getAttribute('aria-rowindex') || '',
                            colIndex: el.getAttribute('aria-colindex') || '',
                            className: (el.className || '').toString(),
                            bbox: {
                                x: Math.round(rect.x || 0),
                                y: Math.round(rect.y || 0),
                                width: Math.round(rect.width || 0),
                                height: Math.round(rect.height || 0),
                            },
                        });
                    }
                    counts[name] = count;
                }

                const gridRoot = document.querySelector("[role='grid'], .waffle-grid-container, .docs-sheet-grid, .grid-container");
                const gridHtml = gridRoot ? (gridRoot.outerHTML || '').slice(0, 120000) : '';

                const textCandidates = [];
                const textNodes = Array.from((gridRoot || document.body).querySelectorAll('*'));
                for (const el of textNodes) {
                    if (!visible(el)) continue;
                    const text = clean(el.innerText || el.textContent || '');
                    if (!text) continue;
                    const rect = el.getBoundingClientRect();
                    textCandidates.push({
                        text,
                        tagName: (el.tagName || '').toLowerCase(),
                        className: (el.className || '').toString(),
                        ariaLabel: clean(el.getAttribute('aria-label') || ''),
                        bbox: {
                            x: Math.round(rect.x || 0),
                            y: Math.round(rect.y || 0),
                            width: Math.round(rect.width || 0),
                            height: Math.round(rect.height || 0),
                        },
                    });
                }
                textCandidates.sort((a, b) => (a.bbox.y - b.bbox.y) || (a.bbox.x - b.bbox.x));

                return {
                    counts,
                    elements: rows,
                    topVisibleTextElements: textCandidates.slice(0, 20),
                    gridHtmlSnippet: gridHtml,
                };
            }""",
            {"strategies": strategies},
        )

        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        screenshot_path = self.raw_helper._save_debug_screenshot(page, prefix=f"layout_grid_inspector_visible_{ts}")

        json_path = debug_dir / f"layout_grid_inspector_elements_{ts}.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        txt_lines: list[str] = []
        for item in payload.get("elements", []):
            txt_lines.append(
                " | ".join(
                    [
                        f"cell_guess={item.get('rowIndex') or item.get('dataRow')},{item.get('colIndex') or item.get('dataCol')}",
                        f"row={item.get('rowIndex') or item.get('dataRow')}",
                        f"col={item.get('colIndex') or item.get('dataCol')}",
                        f"aria_label={item.get('ariaLabel','')}",
                        f"inner_text={item.get('innerText','')}",
                        f"text_content={item.get('textContent','')}",
                        f"data_row={item.get('dataRow','')}",
                        f"data_col={item.get('dataCol','')}",
                        f"bbox={item.get('bbox',{})}",
                        f"selector_strategy={item.get('strategy','')}",
                    ]
                )
            )
        if not txt_lines:
            txt_lines.append("<no visible grid-like elements found>")
        txt_path = debug_dir / f"layout_grid_inspector_elements_{ts}.txt"
        txt_path.write_text("\n".join(txt_lines), encoding="utf-8")

        html_path = debug_dir / f"layout_grid_inspector_grid_snippet_{ts}.html"
        html_path.write_text(payload.get("gridHtmlSnippet", "") or "<empty>", encoding="utf-8")

        top_text_path = debug_dir / f"layout_grid_inspector_top_text_{ts}.json"
        top_text_path.write_text(
            json.dumps(payload.get("topVisibleTextElements", []), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        counts = payload.get("counts", {}) if isinstance(payload, dict) else {}
        self.logger.info("layout inspector selector counts: %s", counts)
        for strategy_name, _ in strategies:
            self.logger.info(
                "layout inspector strategy=%s visible_candidates=%s",
                strategy_name,
                int(counts.get(strategy_name, 0)) if isinstance(counts, dict) else 0,
            )

        if isinstance(counts, dict) and sum(int(v or 0) for v in counts.values()) == 0:
            self.logger.warning("layout inspector found 0 grid-like visible candidates; saved expanded diagnostics")

        self.logger.info(
            "layout grid inspector artifacts: screenshot=%s elements_json=%s elements_txt=%s grid_html=%s top_text=%s",
            screenshot_path,
            json_path,
            txt_path,
            html_path,
            top_text_path,
        )

    def _log_dsl_and_scenario_choice(
        self,
        dsl: LayoutBlockConfig,
        compiled_result: CompiledProfileAnalyticsResult,
        pivot: dict[str, dict[str, int | str]],
    ) -> None:
        self.logger.info("dsl parsed: display_name=%s scenarios=%s", dsl.display_name, len(dsl.scenarios))
        for idx, scenario in enumerate(dsl.scenarios):
            normalized_filters = [
                {
                    "raw_field": f.raw_field_name,
                    "canonical_field": self._canonical_field_label(normalize_field_name(f.normalized_field_name)),
                    "operator": f.operator,
                    "values": f.values,
                }
                for f in scenario.filters
            ]
            self.logger.info("scenario[%s] filters=%s raw=%s normalized=%s", idx, len(scenario.filters), scenario.raw_text, normalized_filters)

        results = [
            self._score_scenario_from_compiled(
                scenario=scenario,
                scenario_index=i,
                compiled_result=compiled_result,
                pivot=pivot,
            )
            for i, scenario in enumerate(dsl.scenarios)
        ]
        best = select_best_scenario(results)
        self.logger.info(
            "scenario scores: %s",
            [
                {
                    "idx": r.scenario_index,
                    "total_count": r.total_count,
                    "non_empty_stage_rows": r.non_empty_stage_rows,
                    "success": r.success,
                }
                for r in results
            ],
        )
        self.logger.info("best scenario selected: index=%s", best.scenario_index)

    def _score_scenario_from_compiled(
        self,
        scenario: LayoutScenario,
        scenario_index: int,
        compiled_result: CompiledProfileAnalyticsResult,
        pivot: dict[str, dict[str, int | str]],
    ) -> ScenarioRunResult:
        source_kind = (compiled_result.source_kind or "").strip().lower()
        compiled_filter_values = {self._norm(v) for v in compiled_result.filter_values if str(v).strip()}

        supported_fields = {"tags", "utm_source", "pipeline", "period", "dates_mode", "date_from", "date_to"}
        unknown_fields = {
            normalize_field_name(item.normalized_field_name)
            for item in scenario.filters
            if normalize_field_name(item.normalized_field_name) not in supported_fields
        }
        if unknown_fields:
            self.logger.warning(
                "scenario[%s] has unsupported fields for current stage: %s",
                scenario_index,
                sorted(unknown_fields),
            )
            return ScenarioRunResult(
                scenario_index=scenario_index,
                success=False,
                total_count=0,
                non_empty_stage_rows=0,
            )

        scenario_source = ""
        scenario_values: set[str] = set()
        for item in scenario.filters:
            field = normalize_field_name(item.normalized_field_name)
            if field in {"tags", "utm_source"}:
                scenario_source = "tag" if field == "tags" else "utm_source"
                scenario_values = {self._norm(v) for v in item.values if str(v).strip()}
                break

        # Current stage: scoring uses already captured compiled snapshot.
        # Full per-scenario amoCRM rerun will be connected in next step.
        if scenario_source and scenario_source != source_kind:
            return ScenarioRunResult(
                scenario_index=scenario_index,
                success=False,
                total_count=0,
                non_empty_stage_rows=0,
            )

        if scenario_values and compiled_filter_values and compiled_filter_values.isdisjoint(scenario_values):
            return ScenarioRunResult(
                scenario_index=scenario_index,
                success=False,
                total_count=0,
                non_empty_stage_rows=0,
            )

        total = int(compiled_result.totals_by_tab.get("all", 0))
        non_empty = sum(1 for values in pivot.values() if int(values.get("all", 0) or 0) > 0)
        return ScenarioRunResult(
            scenario_index=scenario_index,
            success=True,
            total_count=total,
            non_empty_stage_rows=non_empty,
        )

    def _resolve_block_aliases(self, compiled: CompiledProfileAnalyticsResult, layout: dict[str, Any]) -> dict[str, list[str]]:
        block_aliases: dict[str, list[str]] = {}

        generic_tag_aliases = [str(v).strip() for v in layout.get("tag_block_aliases", []) if str(v).strip()]
        compiled_aliases = [str(v).strip() for v in compiled.filter_values if str(v).strip()]
        allow_generic_fallback = bool(layout.get("allow_generic_tag_alias_fallback", True))

        tag_aliases: list[str] = []
        if compiled_aliases:
            # Primary contract: current execution filter values must drive tag block targeting.
            tag_aliases.extend(compiled_aliases)
            if allow_generic_fallback:
                tag_aliases.extend(generic_tag_aliases)
        else:
            tag_aliases.extend(generic_tag_aliases)

        if tag_aliases:
            block_aliases["tag_block"] = list(dict.fromkeys(tag_aliases))

        summary_aliases = [str(v).strip() for v in layout.get("summary_block_aliases", []) if str(v).strip()]
        if summary_aliases:
            block_aliases["summary_block"] = list(dict.fromkeys(summary_aliases))

        if not block_aliases:
            raise RuntimeError("Layout writer config is missing block aliases. Set tag_block_aliases/summary_block_aliases.")

        return block_aliases

    def _merge_header_aliases(self, custom: Any) -> dict[str, list[str]]:
        merged = {k: list(v) for k, v in DEFAULT_HEADER_ALIASES.items()}
        if isinstance(custom, dict):
            for key, values in custom.items():
                if key not in merged:
                    continue
                if isinstance(values, list):
                    merged[key].extend([str(v) for v in values])
        return {k: list(dict.fromkeys(v)) for k, v in merged.items()}

    def _coerce_stage_aliases(self, raw: Any) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        if not isinstance(raw, dict):
            return result
        for k, values in raw.items():
            key = str(k).strip()
            if not key:
                continue
            if isinstance(values, list):
                result[key] = [str(v).strip() for v in values if str(v).strip()]
        return result

    def _discover_dsl_rows(self, page: Page, target_blocks: int = 1) -> list[DslRowCandidate]:
        candidates: dict[tuple[int, int], DslRowCandidate] = {}
        rejected_parse_count = 0
        stop_reason = ""

        if self._discovery_mode == "screenshot_first":
            ss_candidates, ss_stop_reason = self._discover_dsl_rows_screenshot_first(page=page, target_blocks=target_blocks)
            if ss_candidates:
                self._last_dsl_discovery_stop_reason = ss_stop_reason or "screenshot_first_candidates_found"
                dump_path = self._save_discovery_read_dump(prefix="layout_discovery_reads")
                self.logger.info("layout discovery read dump saved: %s", dump_path)
                return ss_candidates
            self.logger.info("dsl discovery screenshot-first yielded no candidates; fallback to dom_snapshot")

        snapshot = self._capture_grid_text_snapshot(
            page=page,
            max_rows=self._discovery_max_rows,
            max_cols=self._discovery_max_cols,
        )
        self.logger.info(
            "dsl discovery mode=dom_snapshot scanned_rows=%s scanned_cols=%s snapshot_cells=%s",
            self._discovery_max_rows,
            self._discovery_max_cols,
            len(snapshot),
        )
        preview_path = self._save_visible_cell_text_snapshot(snapshot, "visible_cell_text_snapshot")
        merged_preview = self._collect_merged_anchor_texts(snapshot)
        dsl_preview = self._collect_dsl_candidate_texts(snapshot)
        dom_dump_path = self._save_visible_gridcell_dom_dump(page, "visible_gridcell_dom_dump", max_cells=350)
        self.logger.info("visible_cell_text_snapshot saved: %s", preview_path)
        self.logger.info("visible_gridcell_dom_dump saved: %s", dom_dump_path)
        self.logger.info("dsl candidate texts: %s", dsl_preview[:20])
        self.logger.info("merged anchor texts: %s", merged_preview[:20])
        self._grid_text_cache.update(snapshot)
        for key in snapshot:
            self._grid_text_source_cache[key] = "dom_snapshot"

        empty_streak = 0
        scanned_rows = 0
        for row in range(1, self._discovery_max_rows + 1):
            scanned_rows += 1
            row_has_data = False
            for col in range(1, self._discovery_max_cols + 1):
                text = (snapshot.get((row, col), "") or "").strip()
                self._record_discovery_read(row=row, col=col, text=text, source="dom_snapshot")
                if text:
                    row_has_data = True
                if not self._run_dsl_check(row=row, col=col, raw_text=text):
                    continue
                try:
                    cfg = parse_layout_row(text)
                except Exception:
                    rejected_parse_count += 1
                    self.logger.debug("dsl row rejected: row=%s col=%s text=%s", row, col, text)
                    continue
                key = (row, col)
                if key not in candidates:
                    candidates[key] = DslRowCandidate(row=row, col=col, raw_text=text, config=cfg)
                    if len(candidates) >= self._discovery_max_candidates:
                        stop_reason = "max_candidates_reached"
                        break
                    if len(candidates) >= target_blocks:
                        stop_reason = "target_blocks_reached"
                        break
            if stop_reason:
                break
            if row_has_data:
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= self._discovery_empty_row_stop and len(candidates) > 0:
                    stop_reason = "empty_row_streak_after_candidates"
                    break

        if candidates:
            self.logger.info(
                "dsl discovery stop: mode=dom_snapshot reason=%s scanned_rows=%s candidates=%s rejected=%s",
                stop_reason or "rows_exhausted",
                scanned_rows,
                len(candidates),
                rejected_parse_count,
            )
            self._last_dsl_discovery_stop_reason = stop_reason or "rows_exhausted"
            dump_path = self._save_discovery_read_dump(prefix="layout_discovery_reads")
            self.logger.info("layout discovery read dump saved: %s", dump_path)
            return sorted(candidates.values(), key=lambda x: (x.row, x.col))

        self.logger.info("dsl discovery fallback mode=cell_scan reason=no_candidates_in_snapshot")
        empty_streak = 0
        scanned_rows = 0
        for row in range(1, self._discovery_max_rows + 1):
            scanned_rows += 1
            row_has_data = False
            for col in range(1, self._discovery_max_cols + 1):
                text = self._read_grid_cell_cached(page=page, row=row, col=col).strip()
                source = self._grid_text_source_cache.get((row, col), "other")
                self._record_discovery_read(row=row, col=col, text=text, source=source)
                if text:
                    row_has_data = True
                if not self._run_dsl_check(row=row, col=col, raw_text=text):
                    continue
                try:
                    cfg = parse_layout_row(text)
                except Exception:
                    rejected_parse_count += 1
                    continue
                candidates[(row, col)] = DslRowCandidate(row=row, col=col, raw_text=text, config=cfg)
                if len(candidates) >= self._discovery_max_candidates:
                    stop_reason = "max_candidates_reached"
                    break
                if len(candidates) >= target_blocks:
                    stop_reason = "target_blocks_reached"
                    break
            if stop_reason:
                break
            if row_has_data:
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= self._discovery_empty_row_stop and row > 20:
                    stop_reason = "empty_row_streak"
                    break

        self.logger.info(
            "dsl discovery stop: mode=cell_scan reason=%s scanned_rows=%s candidates=%s rejected=%s",
            stop_reason or "rows_exhausted",
            scanned_rows,
            len(candidates),
            rejected_parse_count,
        )
        self._last_dsl_discovery_stop_reason = stop_reason or "rows_exhausted"
        dump_path = self._save_discovery_read_dump(prefix="layout_discovery_reads")
        self.logger.info("layout discovery read dump saved: %s", dump_path)
        return sorted(candidates.values(), key=lambda x: (x.row, x.col))

    def _discover_dsl_rows_screenshot_first(self, page: Page, target_blocks: int) -> tuple[list[DslRowCandidate], str]:
        self.logger.info(
            "dsl discovery mode=screenshot_first pages=%s cols=%s max_rows=%s",
            self._screenshot_scan_pages,
            self._discovery_max_cols,
            self._discovery_max_rows,
        )
        self.raw_helper._wait_for_sheet_ui_ready(page)
        self.raw_helper.focus_sheet_grid(page)
        page.wait_for_timeout(200)

        candidates: dict[tuple[int, int], DslRowCandidate] = {}
        empty_views = 0
        stop_reason = "pages_exhausted"
        view_artifacts: list[dict[str, Any]] = []

        window_size = max(12, min(40, self._discovery_max_rows // max(1, self._screenshot_scan_pages)))

        for page_idx in range(1, self._screenshot_scan_pages + 1):
            screenshot_path = self.raw_helper._save_debug_screenshot(page, prefix=f"layout_discovery_view_{page_idx:02d}")
            visible_meta = self._collect_visible_header_metadata(page)
            view_non_empty = int(visible_meta.get("non_empty_text_elements", 0) or 0)

            if view_non_empty <= 2:
                empty_views += 1
            else:
                empty_views = 0

            row_headers = [int(x) for x in visible_meta.get("row_headers", []) if str(x).isdigit()]
            if row_headers:
                start_row = max(1, min(row_headers))
                end_row = min(self._discovery_max_rows, max(row_headers) + 4)
            else:
                start_row = min(self._discovery_max_rows, 1 + (page_idx - 1) * window_size)
                end_row = min(self._discovery_max_rows, start_row + window_size - 1)

            scanned_cells = 0
            view_candidates = 0
            for row in range(start_row, end_row + 1):
                for col in range(1, self._discovery_max_cols + 1):
                    scanned_cells += 1
                    text = self._read_grid_cell_cached(page=page, row=row, col=col).strip()
                    source = self._grid_text_source_cache.get((row, col), "formula_bar")
                    self._record_discovery_read(row=row, col=col, text=text, source=source)
                    if not self._run_dsl_check(row=row, col=col, raw_text=text):
                        continue
                    try:
                        cfg = parse_layout_row(text)
                    except Exception:
                        continue
                    key = (row, col)
                    if key in candidates:
                        continue
                    candidates[key] = DslRowCandidate(row=row, col=col, raw_text=text, config=cfg)
                    view_candidates += 1
                    self.logger.info(
                        "screenshot-first anchor detected: cell=%s confidence=%.2f text=%s",
                        self._cell_ref(col, row),
                        0.92,
                        text,
                    )
                    if len(candidates) >= target_blocks:
                        stop_reason = "target_blocks_reached"
                        break
                if stop_reason == "target_blocks_reached":
                    break

            view_artifacts.append(
                {
                    "page_idx": page_idx,
                    "screenshot": str(screenshot_path) if screenshot_path else "",
                    "visible_meta": visible_meta,
                    "scan_window": {"start_row": start_row, "end_row": end_row, "max_cols": self._discovery_max_cols},
                    "scanned_cells": scanned_cells,
                    "detected_anchors": view_candidates,
                    "empty_view_streak": empty_views,
                }
            )

            if stop_reason == "target_blocks_reached":
                break

            if empty_views >= self._screenshot_empty_views_stop:
                stop_reason = "end_of_sheet_visual_empty"
                break

            moved = self._scroll_grid_viewport(page)
            if not moved:
                stop_reason = "grid_scroll_not_moved"
                break

        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        debug_path = debug_dir / f"layout_screenshot_discovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        summary_payload = {
            "mode": "screenshot_first",
            "stop_reason": stop_reason,
            "confidence_policy": "formula_scan_anchor=0.92",
            "detected_anchors": [
                {
                    "cell": self._cell_ref(col, row),
                    "row": row,
                    "col": col,
                    "raw_text": c.raw_text,
                    "display_name": c.config.display_name,
                    "confidence": 0.92,
                }
                for (row, col), c in sorted(candidates.items())
            ],
            "views": view_artifacts,
        }
        debug_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self.logger.info("layout screenshot-first discovery artifact: %s", debug_path)

        sorted_candidates = sorted(candidates.values(), key=lambda x: (x.row, x.col))
        return sorted_candidates, stop_reason

    def _collect_visible_header_metadata(self, page: Page) -> dict[str, Any]:
        return page.evaluate(
            """() => {
                const clean = (v) => (v || '').toString().replace(/\s+/g, ' ').trim();
                const visible = (el) => {
                    const r = el.getBoundingClientRect();
                    if (!r || r.width <= 1 || r.height <= 1) return false;
                    if (r.bottom < 0 || r.right < 0 || r.top > window.innerHeight || r.left > window.innerWidth) return false;
                    const style = window.getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity || '1') <= 0.01) return false;
                    return true;
                };

                const rowHeaders = new Set();
                const colHeaders = new Set();
                let nonEmpty = 0;

                const selectors = [
                    '.docs-sheet-row-header',
                    '.waffle-row-header',
                    '.docs-sheet-column-header',
                    '.waffle-column-header',
                    '[aria-label]',
                ];

                for (const selector of selectors) {
                    const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 600);
                    for (const el of nodes) {
                        if (!visible(el)) continue;
                        const txt = clean(el.innerText || el.textContent || el.getAttribute('aria-label') || '');
                        if (!txt) continue;
                        nonEmpty += 1;
                        if (/^\d{1,4}$/.test(txt)) rowHeaders.add(txt);
                        if (/^[A-Z]{1,3}$/.test(txt)) colHeaders.add(txt);
                    }
                }

                return {
                    row_headers: Array.from(rowHeaders).sort((a,b) => Number(a) - Number(b)),
                    col_headers: Array.from(colHeaders).sort(),
                    non_empty_text_elements: nonEmpty,
                };
            }"""
        )

    def _scroll_grid_viewport(self, page: Page) -> bool:
        try:
            self.raw_helper.focus_sheet_grid(page)
            before = self.raw_helper.read_active_cell(page)
            page.keyboard.press("PageDown")
            page.wait_for_timeout(220)
            after = self.raw_helper.read_active_cell(page)
            moved = before != after
            self.logger.info("screenshot-first scroll step: active_before=%s active_after=%s moved=%s", before, after, str(moved).lower())
            return True
        except Exception as exc:
            self.logger.warning("screenshot-first scroll step failed: %s", exc)
            return False

    def _discover_block_anchors_from_dsl(
        self,
        page: Page,
        dsl_rows: list[DslRowCandidate],
        header_aliases: dict[str, list[str]],
        target_blocks: int,
    ) -> list[BlockCandidate]:
        anchors: list[BlockCandidate] = []
        seen: set[tuple[int, int]] = set()
        stop_reason = "rows_exhausted"

        for dsl in dsl_rows:
            if self._is_ignored_non_stage_block(dsl.config.display_name):
                self.logger.info(
                    "dsl anchor rejected: row=%s title=%s reason=out_of_scope",
                    dsl.row,
                    dsl.config.display_name,
                )
                continue

            header_match = self._find_header_near_dsl_row(page=page, dsl_row=dsl.row, header_aliases=header_aliases)
            if header_match is None:
                self.logger.info("dsl anchor rejected: row=%s reason=no_header_in_window", dsl.row)
                continue

            header_row, stage_col, all_col, active_col, closed_col = header_match
            key = (header_row, stage_col)
            if key in seen:
                self.logger.info("dsl anchor dedup hit: dsl_row=%s header_row=%s stage_col=%s", dsl.row, header_row, stage_col)
                continue
            seen.add(key)
            anchors.append(
                BlockCandidate(
                    header_row=header_row,
                    stage_col=stage_col,
                    all_col=all_col,
                    active_col=active_col,
                    closed_col=closed_col,
                    title_row=dsl.row,
                    title_col=dsl.col,
                    title_raw=dsl.raw_text,
                    title_norm=self._norm(dsl.raw_text),
                    score=100,
                    matched_alias="dsl_first",
                    dsl=dsl.config,
                )
            )
            self.logger.info(
                "dsl anchor accepted: dsl_row=%s header_row=%s stage_col=%s all_col=%s active_col=%s closed_col=%s",
                dsl.row,
                header_row,
                stage_col,
                all_col,
                active_col,
                closed_col,
            )

            if len(anchors) >= target_blocks:
                stop_reason = "target_blocks_reached"
                break

        self._last_anchor_discovery_stop_reason = stop_reason
        return anchors

    def _find_block_anchor(
        self,
        page: Page,
        block_name: str,
        aliases: list[str],
        header_aliases: dict[str, list[str]],
        dsl_rows: list[DslRowCandidate],
        dsl_block_anchors: list[BlockCandidate],
        used_anchor_keys: set[tuple[int, int]],
    ) -> BlockCandidate | None:
        alias_norm = [(a, self._norm(a)) for a in aliases if a.strip()]
        slug_tokens = self._extract_slug_tokens(aliases)

        available_dsl = [a for a in dsl_block_anchors if (a.header_row, a.stage_col) not in used_anchor_keys]
        if available_dsl:
            scored: list[tuple[int, BlockCandidate]] = []
            for a in available_dsl:
                score, matched_alias = self._score_alias_match(
                    title_raw=a.title_raw,
                    title_norm=a.title_norm,
                    alias_norm=alias_norm,
                    slug_tokens=slug_tokens,
                )
                candidate = BlockCandidate(
                    header_row=a.header_row,
                    stage_col=a.stage_col,
                    all_col=a.all_col,
                    active_col=a.active_col,
                    closed_col=a.closed_col,
                    title_row=a.title_row,
                    title_col=a.title_col,
                    title_raw=a.title_raw,
                    title_norm=a.title_norm,
                    score=score,
                    matched_alias=matched_alias,
                    dsl=a.dsl,
                )
                scored.append((score, candidate))

            scored.sort(key=lambda item: item[0], reverse=True)
            best_score, best = scored[0]
            if best_score <= 0:
                best = available_dsl[0]
                self.logger.info(
                    "dsl-first anchor selected without alias match: block=%s header_row=%s stage_col=%s",
                    block_name,
                    best.header_row,
                    best.stage_col,
                )
            else:
                self.logger.info(
                    "dsl-first anchor selected by alias: block=%s alias=%s header_row=%s stage_col=%s",
                    block_name,
                    best.matched_alias or "<none>",
                    best.header_row,
                    best.stage_col,
                )
            used_anchor_keys.add((best.header_row, best.stage_col))
            return best

        self.logger.info(
            "dsl-first anchors unavailable for block=%s; fallback to structural candidate search",
            block_name,
        )
        candidates = self._collect_block_candidates(
            page=page,
            aliases=aliases,
            header_aliases=header_aliases,
            dsl_rows=dsl_rows,
        )
        self.logger.info("layout candidate headers found: %s", len(candidates))
        for c in candidates[:25]:
            self.logger.info(
                "candidate_header raw=%s normalized=%s row=%s col=%s alias_compare_result=%s score=%s",
                c.title_raw,
                c.title_norm,
                c.title_row,
                c.title_col,
                c.matched_alias or "<none>",
                c.score,
            )

        if candidates:
            best = candidates[0]
            if best.score > 0:
                return best

        if candidates:
            self.logger.warning(
                "layout alias match not found for block=%s. Using structural fallback header at row=%s",
                block_name,
                candidates[0].header_row,
            )
            return candidates[0]

        return None
    
    def _collect_block_candidates(
        self,
        page: Page,
        aliases: list[str],
        header_aliases: dict[str, list[str]],
        dsl_rows: list[DslRowCandidate],
    ) -> list[BlockCandidate]:
        candidates: list[BlockCandidate] = []
        alias_norm = [(a, self._norm(a)) for a in aliases if a.strip()]
        slug_tokens = self._extract_slug_tokens(aliases)

        dsl_by_row = {x.row: x for x in dsl_rows}

        # 1) DSL-first candidates: yellow-like row with parseable config + nearby table header.
        for dsl in dsl_rows:
            if self._is_ignored_non_stage_block(dsl.config.display_name):
                self.logger.info(
                    "layout block skipped (out-of-scope by display_name): row=%s title=%s",
                    dsl.row,
                    dsl.config.display_name,
                )
                continue

            header_match = self._find_header_near_dsl_row(
                page=page,
                dsl_row=dsl.row,
                header_aliases=header_aliases,
            )
            if header_match is None:
                self.logger.info(
                    "layout block skipped: dsl_row=%s reason=no_header_below",
                    dsl.row,
                )
                continue

            header_row, stage_col, all_col, active_col, closed_col = header_match
            score, matched_alias = self._score_alias_match(
                title_raw=dsl.raw_text,
                title_norm=self._norm(dsl.raw_text),
                alias_norm=alias_norm,
                slug_tokens=slug_tokens,
            )
            score = max(score, 5)
            candidates.append(
                BlockCandidate(
                    header_row=header_row,
                    stage_col=stage_col,
                    all_col=all_col,
                    active_col=active_col,
                    closed_col=closed_col,
                    title_row=dsl.row,
                    title_col=dsl.col,
                    title_raw=dsl.raw_text,
                    title_norm=self._norm(dsl.raw_text),
                    score=score,
                    matched_alias=matched_alias,
                    dsl=dsl.config,
                )
            )

        # 2) Structural scan fallback (bounded window).
        if candidates:
            self.logger.info(
                "layout structural scan skipped: reason=dsl_first_candidates_found count=%s",
                len(candidates),
            )
        else:
            empty_row_streak = 0
            for row in range(1, self._block_scan_max_rows + 1):
                row_values = [self._read_grid_cell_cached(page, row, col) for col in range(1, self._block_scan_max_cols + 1)]
                row_norm = [self._norm(v) for v in row_values]

                stage_col = self._find_column_by_aliases(row_norm, header_aliases["stage"])
                all_col = self._find_column_by_aliases(row_norm, header_aliases["all"])
                active_col = self._find_column_by_aliases(row_norm, header_aliases["active"])
                closed_col = self._find_column_by_aliases(row_norm, header_aliases["closed"])
                if not all([stage_col, all_col, active_col, closed_col]):
                    if not any(self._norm(v) for v in row_values):
                        empty_row_streak += 1
                        if empty_row_streak >= self._block_scan_empty_row_stop:
                            reason = "empty_row_streak_with_candidates" if candidates else "empty_row_streak_no_candidates"
                            self.logger.info("layout structural scan stop: reason=%s row=%s", reason, row)
                            break
                    else:
                        empty_row_streak = 0
                    continue

                empty_row_streak = 0

                title_row, title_col, title_raw = self._find_title_above_header(page, header_row=row)
                title_norm = self._norm(title_raw)
                score, matched_alias = self._score_alias_match(
                    title_raw=title_raw,
                    title_norm=title_norm,
                    alias_norm=alias_norm,
                    slug_tokens=slug_tokens,
                )

                dsl = None
                dsl_row_candidate = dsl_by_row.get(title_row)
                if dsl_row_candidate is not None:
                    dsl = dsl_row_candidate.config
                    score = max(score, 2)
                    if self._is_ignored_non_stage_block(dsl.display_name):
                        self.logger.info(
                            "layout structural candidate skipped (out-of-scope by DSL): header_row=%s dsl_title=%s",
                            row,
                            dsl.display_name,
                        )
                        continue

                if self._is_ignored_non_stage_block(title_raw):
                    self.logger.info(
                        "layout structural candidate skipped (title out-of-scope): header_row=%s title=%s",
                        row,
                        title_raw,
                    )
                    continue

                candidates.append(
                    BlockCandidate(
                        header_row=row,
                        stage_col=stage_col,
                        all_col=all_col,
                        active_col=active_col,
                        closed_col=closed_col,
                        title_row=title_row,
                        title_col=title_col,
                        title_raw=title_raw,
                        title_norm=title_norm,
                        score=score,
                        matched_alias=matched_alias,
                        dsl=dsl,
                    )
                )

        candidates.sort(key=lambda c: (c.score, c.header_row), reverse=True)
        # Deduplicate by header row + stage column.
        uniq: dict[tuple[int, int], BlockCandidate] = {}
        for c in candidates:
            key = (c.header_row, c.stage_col)
            if key not in uniq:
                uniq[key] = c
        return list(uniq.values())

    def _find_header_near_dsl_row(
        self,
        page: Page,
        dsl_row: int,
        header_aliases: dict[str, list[str]],
    ) -> tuple[int, int, int, int, int] | None:
        start = dsl_row + 1
        end = min(dsl_row + self._header_search_window, self._block_scan_max_rows)
        for row in range(start, end + 1):
            row_values = [self._read_grid_cell_cached(page, row, col) for col in range(1, self._block_scan_max_cols + 1)]
            row_norm = [self._norm(v) for v in row_values]
            stage_col = self._find_column_by_aliases(row_norm, header_aliases["stage"])
            all_col = self._find_column_by_aliases(row_norm, header_aliases["all"])
            active_col = self._find_column_by_aliases(row_norm, header_aliases["active"])
            closed_col = self._find_column_by_aliases(row_norm, header_aliases["closed"])
            if all([stage_col, all_col, active_col, closed_col]):
                return row, stage_col, all_col, active_col, closed_col
        return None

    def _find_title_above_header(self, page: Page, header_row: int) -> tuple[int, int, str]:
        max_title_col = min(self._block_scan_max_cols, 8)
        for probe_row in range(max(1, header_row - 4), header_row):
            for probe_col in range(1, max_title_col + 1):
                text = self._read_grid_cell_cached(page, probe_row, probe_col).strip()
                if text:
                    return probe_row, probe_col, text
        return header_row, 1, ""

    def _score_alias_match(
        self,
        title_raw: str,
        title_norm: str,
        alias_norm: list[tuple[str, str]],
        slug_tokens: list[str],
    ) -> tuple[int, str]:
        if not title_raw and not title_norm:
            return 0, ""

        title_variants = {title_norm}
        repaired = self._try_repair_mojibake(title_raw)
        if repaired:
            title_variants.add(self._norm(repaired))

        for alias_raw, alias_n in alias_norm:
            if not alias_n:
                continue
            if alias_n in title_variants:
                return 3, alias_raw
            for variant in title_variants:
                if alias_n and alias_n in variant:
                    return 2, alias_raw

        lower_raw = title_raw.lower()
        for token in slug_tokens:
            if token and token in lower_raw:
                return 1, token

        return 0, ""

    def _extract_slug_tokens(self, aliases: list[str]) -> list[str]:
        tokens: list[str] = []
        for alias in aliases:
            for token in re.findall(r"[a-z0-9_]{6,}", alias.lower()):
                tokens.append(token)
        return list(dict.fromkeys(tokens))

    def _try_repair_mojibake(self, text: str) -> str:
        try:
            repaired = text.encode("cp1251", errors="ignore").decode("utf-8", errors="ignore").strip()
        except Exception:
            return ""
        return repaired

    def _find_column_by_aliases(self, row_norm: list[str], aliases: list[str]) -> int:
        alias_keys = [self._norm(a) for a in aliases if self._norm(a)]
        for idx, value in enumerate(row_norm, start=1):
            if value in alias_keys:
                return idx
        return 0

    def _is_ignored_non_stage_block(self, text: str) -> bool:
        norm = self._norm(text)
        if not norm:
            return False
        # Out-of-scope in current stage: lower rejection tables and non-stage analytics blocks.
        blocked_tokens = ("\u043e\u0442\u043a\u0430\u0437", "\u0440\u0435\u0430\u043d\u0438\u043c\u0430\u0446", "\u043f\u0440\u043e\u0434\u0443\u043a\u0442", "\u043c\u0435\u043d\u0435\u0434\u0436\u0435\u0440", "\u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a \u0441\u0434\u0435\u043b\u043a\u0438")
        return any(token in norm for token in blocked_tokens)

    def _build_stage_row_map(self, page: Page, start_row: int, stage_col: int, max_scan_rows: int) -> dict[str, int]:
        mapping: dict[str, int] = {}
        empty_streak = 0
        for row in range(start_row, start_row + max_scan_rows):
            text = self._read_cell_text(page, self._cell_ref(stage_col, row))
            norm = self._norm(text)
            if not norm:
                empty_streak += 1
                if empty_streak >= 8:
                    break
                continue
            empty_streak = 0
            if norm in {"СЌС‚Р°Рї", "СЃС‚Р°С‚СѓСЃ", "РІСЃРµ", "Р°РєС‚РёРІРЅС‹Рµ", "Р·Р°РєСЂС‹С‚С‹Рµ"}:
                continue
            mapping.setdefault(norm, row)
        return mapping

    def _build_planned_writes(
        self,
        stage_row_map: dict[str, int],
        pivot: dict[str, dict[str, int | str]],
        all_col: int,
        active_col: int,
        closed_col: int,
        stage_aliases: dict[str, list[str]],
    ) -> tuple[dict[str, int], list[str]]:
        writes: dict[str, int] = {}
        missing: list[str] = []

        reverse_alias: dict[str, str] = {}
        for canonical, aliases in stage_aliases.items():
            canonical_norm = self._norm(canonical)
            if canonical_norm:
                reverse_alias[canonical_norm] = canonical_norm
            for alias in aliases:
                alias_norm = self._norm(alias)
                if alias_norm:
                    reverse_alias[alias_norm] = canonical_norm

        for stage_name, values in pivot.items():
            stage_norm = self._norm(stage_name)
            row = stage_row_map.get(stage_norm)
            if row is None:
                canonical = reverse_alias.get(stage_norm)
                if canonical:
                    row = stage_row_map.get(canonical)
            if row is None:
                missing.append(stage_name)
                continue

            writes[self._cell_ref(all_col, row)] = int(values.get("all", 0) or 0)
            writes[self._cell_ref(active_col, row)] = int(values.get("active", 0) or 0)
            writes[self._cell_ref(closed_col, row)] = int(values.get("closed", 0) or 0)

        return writes, missing

    def _write_numeric_cell(self, page: Page, cell_ref: str, value: int) -> None:
        self.raw_helper.goto_cell(page=page, cell_ref=cell_ref)
        page.keyboard.press("Control+A")
        page.keyboard.type(str(value))
        page.keyboard.press("Enter")
        page.wait_for_timeout(80)

    def _configure_discovery_limits(self, layout: dict[str, Any]) -> None:
        self._discovery_max_rows = self._to_int(layout.get("dsl_scan_max_rows"), 120)
        self._discovery_max_cols = self._to_int(layout.get("dsl_scan_max_cols"), 6)
        self._discovery_empty_row_stop = self._to_int(layout.get("dsl_empty_row_stop"), 12)
        self._discovery_max_candidates = self._to_int(layout.get("dsl_max_candidates"), 24)
        self._header_search_window = self._to_int(layout.get("header_search_window"), 6)
        self._block_scan_max_rows = self._to_int(layout.get("block_scan_max_rows"), 140)
        self._block_scan_max_cols = self._to_int(layout.get("block_scan_max_cols"), 10)
        self._block_scan_empty_row_stop = self._to_int(layout.get("block_empty_row_stop"), 12)
        self._discovery_mode = str(layout.get("discovery_mode", "screenshot_first")).strip().lower() or "screenshot_first"
        self._screenshot_scan_pages = self._to_int(layout.get("screenshot_scan_pages"), 4)
        self._screenshot_empty_views_stop = self._to_int(layout.get("screenshot_empty_views_stop"), 2)

    def _to_int(self, value: Any, default: int) -> int:
        try:
            parsed = int(value)
            return parsed if parsed > 0 else default
        except Exception:
            return default

    def _looks_like_dsl_row_text(self, text: str) -> bool:
        matched, _, _ = self._evaluate_dsl_text(text)
        return matched

    def _evaluate_dsl_text(self, text: str) -> tuple[bool, str, str]:
        value = (text or "").strip()
        normalized = self._norm(value)
        if not value:
            return False, "empty", normalized
        if ":" not in value:
            return False, "missing_colon", normalized
        if "||" in value:
            return True, "contains_or_or", normalized
        if ";" in value:
            return True, "contains_and_separator", normalized
        if "^=" in value:
            return True, "contains_prefix_operator", normalized
        if "=" in value:
            return True, "contains_equals_operator", normalized
        return False, "missing_filter_operator", normalized

    def _run_dsl_check(self, row: int, col: int, raw_text: str) -> bool:
        self._discovery_dsl_checks += 1
        matched, reason, normalized = self._evaluate_dsl_text(raw_text)
        if matched:
            self._discovery_dsl_matches += 1
        cell = self._cell_ref(col, row)
        self.logger.info(
            "dsl_check cell=%s raw_text=%r normalized=%r matched=%s reason=%s",
            cell,
            raw_text,
            normalized,
            matched,
            reason,
        )
        return matched

    def _record_discovery_read(self, row: int, col: int, text: str, source: str) -> None:
        source_key = source if source in {"dom_snapshot", "formula_bar"} else "other"
        self._discovery_source_counts[source_key] = self._discovery_source_counts.get(source_key, 0) + 1
        self._discovery_total_cells_scanned += 1
        if (text or "").strip():
            self._discovery_non_empty_cells_seen += 1

        cell = self._cell_ref(col, row)
        normalized = self._norm(text)
        self.logger.info("discovery_read cell=%s source=%s text=%r", cell, source, text)

        if len(self._discovery_read_samples) < 150:
            self._discovery_read_samples.append(
                {
                    "cell": cell,
                    "source": source,
                    "text": (text or "").strip(),
                    "normalized": normalized,
                }
            )

    def _save_discovery_read_dump(self, prefix: str) -> Path:
        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        lines: list[str] = []
        lines.append(
            "summary: total_cells_scanned="
            f"{self._discovery_total_cells_scanned} non_empty_cells_seen={self._discovery_non_empty_cells_seen} "
            f"dsl_checks={self._discovery_dsl_checks} dsl_matches={self._discovery_dsl_matches}"
        )
        lines.append(f"used_sources={self._discovery_source_counts}")
        lines.append("samples:")
        for sample in self._discovery_read_samples:
            lines.append(
                f"{sample['cell']} source={sample['source']} text={sample['text']!r} normalized={sample['normalized']!r}"
            )
        if len(lines) == 3:
            lines.append("<no samples>")
        output.write_text("\n".join(lines), encoding="utf-8")
        return output

    def _save_visible_gridcell_dom_dump(self, page: Page, prefix: str, max_cells: int = 300) -> Path:
        payload = page.evaluate(
            """({ maxCells }) => {
                const clean = (value) => (value || '').toString().replace(/\s+/g, ' ').trim();
                const out = [];
                const nodes = Array.from(document.querySelectorAll("[role='gridcell']")).slice(0, maxCells);
                for (const el of nodes) {
                    const rect = el.getBoundingClientRect();
                    const row = el.getAttribute('aria-rowindex') || el.getAttribute('data-row') || el.getAttribute('row') || '';
                    const col = el.getAttribute('aria-colindex') || el.getAttribute('data-col') || el.getAttribute('col') || '';
                    out.push({
                        row,
                        col,
                        ariaLabel: clean(el.getAttribute('aria-label') || ''),
                        innerText: clean(el.innerText || ''),
                        textContent: clean(el.textContent || ''),
                        dataRow: el.getAttribute('data-row') || '',
                        dataCol: el.getAttribute('data-col') || '',
                        bbox: {
                            x: Math.round(rect.x || 0),
                            y: Math.round(rect.y || 0),
                            width: Math.round(rect.width || 0),
                            height: Math.round(rect.height || 0),
                        },
                    });
                }
                return out;
            }""",
            {"maxCells": max_cells},
        )

        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output.write_text(__import__("json").dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return output

    def _log_discovery_summary(self) -> None:
        self.logger.info(
            "layout discovery summary: total_cells_scanned=%s non_empty_cells_seen=%s dsl_checks=%s dsl_matches=%s merged_ranges_seen=%s used_sources=%s",
            self._discovery_total_cells_scanned,
            self._discovery_non_empty_cells_seen,
            self._discovery_dsl_checks,
            self._discovery_dsl_matches,
            len(self._collect_merged_anchor_texts(self._grid_text_cache)),
            self._discovery_source_counts,
        )

    def _capture_grid_text_snapshot(self, page: Page, max_rows: int, max_cols: int) -> dict[tuple[int, int], str]:
        payload = page.evaluate(
            """({ maxRows, maxCols }) => {
                const out = [];
                const nodes = Array.from(document.querySelectorAll("[role='gridcell']"));
                const clean = (value) => (value || '').toString().replace(/\s+/g, ' ').trim();
                const stripRef = (value) => clean(value).replace(/^[A-Z]+\d+(?::[A-Z]+\d+)?\s*[:\-;,]?\s*/i, '').trim();
                const getDisplayText = (el) => {
                    const rich = el.querySelector('.waffle-cell-text, .cell-text, .docs-sheet-cell-value, [class*="cell-text"]');
                    const richText = clean(rich ? (rich.innerText || rich.textContent || '') : '');
                    if (richText) return richText;
                    const inner = clean(el.innerText || '');
                    if (inner) return inner;
                    const txt = clean(el.textContent || '');
                    if (txt) return txt;
                    const aria = stripRef(el.getAttribute('aria-label') || '');
                    if (aria) return aria;
                    return '';
                };
                for (const el of nodes) {
                    const getNum = (name) => {
                        const raw = el.getAttribute(name);
                        if (!raw) return null;
                        const n = Number.parseInt(raw, 10);
                        return Number.isFinite(n) ? n : null;
                    };
                    const rowIdx = getNum('aria-rowindex');
                    const colIdx = getNum('aria-colindex');
                    const dataRow = getNum('data-row');
                    const dataCol = getNum('data-col');
                    const rowRaw = getNum('row');
                    const colRaw = getNum('col');

                    let row = rowIdx;
                    let col = colIdx;
                    if (!Number.isFinite(row) && Number.isFinite(dataRow)) row = dataRow + 1;
                    if (!Number.isFinite(col) && Number.isFinite(dataCol)) col = dataCol + 1;
                    if (!Number.isFinite(row) && Number.isFinite(rowRaw)) row = rowRaw + 1;
                    if (!Number.isFinite(col) && Number.isFinite(colRaw)) col = colRaw + 1;
                    if (!Number.isFinite(row) || !Number.isFinite(col)) continue;
                    if (row < 1 || col < 1 || row > maxRows || col > maxCols) continue;

                    const rowSpan = getNum('aria-rowspan') || getNum('rowspan') || 1;
                    const colSpan = getNum('aria-colspan') || getNum('colspan') || 1;
                    const text = getDisplayText(el);
                    if (!text) continue;
                    out.push({ row, col, text, rowSpan, colSpan });
                }
                return out;
            }""",
            {"maxRows": max_rows, "maxCols": max_cols},
        )

        snapshot: dict[tuple[int, int], str] = {}
        merged_seen: set[tuple[int, int, int, int]] = set()
        merged_dedup_hits = 0
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                try:
                    row = int(item.get("row", 0))
                    col = int(item.get("col", 0))
                    row_span = max(1, int(item.get("rowSpan", 1) or 1))
                    col_span = max(1, int(item.get("colSpan", 1) or 1))
                except Exception:
                    continue
                value = str(item.get("text", "")).strip()
                if row <= 0 or col <= 0 or not value:
                    continue

                if row_span > 1 or col_span > 1:
                    range_key = (row, col, row + row_span - 1, col + col_span - 1)
                    if range_key in merged_seen:
                        merged_dedup_hits += 1
                        continue
                    merged_seen.add(range_key)

                snapshot[(row, col)] = value

        self.logger.info(
            "dsl snapshot merged handling: merged_ranges=%s merged_dedup_hits=%s unique_cells=%s",
            len(merged_seen),
            merged_dedup_hits,
            len(snapshot),
        )
        return snapshot

    def _read_grid_cell_cached(self, page: Page, row: int, col: int) -> str:
        key = (row, col)
        if key in self._grid_text_cache:
            return self._grid_text_cache[key]

        if self._cell_read_count >= self._cell_read_hard_limit:
            self._read_limit_hit = True
            reason = f"layout discovery hard stop: cell_read_limit_exceeded count={self._cell_read_count} limit={self._cell_read_hard_limit}"
            self.logger.error(reason)
            raise RuntimeError(reason)

        self._cell_read_count += 1
        cell_ref = self._cell_ref(col, row)

        dom_value = self._read_single_cell_text_from_dom(page, row=row, col=col)
        if dom_value:
            value = dom_value
            source = "dom_snapshot"
            self._fallback_dom_reads += 1
        else:
            value = self.raw_helper._read_cell_value(page=page, cell_ref=cell_ref)
            source = "formula_bar"
            if self._looks_like_cell_ref_or_range(value):
                value = ""
            self._fallback_formula_reads += 1

        self._grid_text_cache[key] = value
        self._grid_text_source_cache[key] = source
        self.logger.debug("cell text source used: cell=%s source=%s value=%s", cell_ref, source, value)
        return value

    def _looks_like_cell_ref_or_range(self, text: str) -> bool:
        value = (text or "").strip().upper()
        return bool(re.fullmatch(r"[A-Z]+\d+(?::[A-Z]+\d+)?", value))

    def _read_single_cell_text_from_dom(self, page: Page, row: int, col: int) -> str:
        payload = page.evaluate(
            """({ row, col }) => {
                const clean = (value) => (value || '').toString().replace(/\s+/g, ' ').trim();
                const stripRef = (value) => clean(value).replace(/^[A-Z]+\d+(?::[A-Z]+\d+)?\s*[:\-;,]?\s*/i, '').trim();
                const matches = Array.from(document.querySelectorAll("[role='gridcell']"));
                for (const el of matches) {
                    const getNum = (name) => {
                        const raw = el.getAttribute(name);
                        if (!raw) return null;
                        const n = Number.parseInt(raw, 10);
                        return Number.isFinite(n) ? n : null;
                    };
                    const r = getNum('aria-rowindex') ?? ((getNum('data-row') ?? getNum('row')) !== null ? (getNum('data-row') ?? getNum('row')) + 1 : null);
                    const c = getNum('aria-colindex') ?? ((getNum('data-col') ?? getNum('col')) !== null ? (getNum('data-col') ?? getNum('col')) + 1 : null);
                    if (r !== row || c !== col) continue;
                    const rich = el.querySelector('.waffle-cell-text, .cell-text, .docs-sheet-cell-value, [class*="cell-text"]');
                    const richText = clean(rich ? (rich.innerText || rich.textContent || '') : '');
                    if (richText) return richText;
                    const inner = clean(el.innerText || '');
                    if (inner) return inner;
                    const txt = clean(el.textContent || '');
                    if (txt) return txt;
                    const aria = stripRef(el.getAttribute('aria-label') || '');
                    if (aria) return aria;
                }
                return '';
            }""",
            {"row": row, "col": col},
        )
        return str(payload or "").strip()

    def _save_visible_cell_text_snapshot(self, snapshot: dict[tuple[int, int], str], prefix: str) -> Path:
        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        max_rows = min(self._discovery_max_rows, 20)
        max_cols = min(max(self._discovery_max_cols, 10), 10)
        lines: list[str] = []
        for row in range(1, max_rows + 1):
            row_items: list[str] = []
            for col in range(1, max_cols + 1):
                value = (snapshot.get((row, col), "") or "").strip()
                if value:
                    row_items.append(f"{self._cell_ref(col, row)}={value}")
            if row_items:
                lines.append(" | ".join(row_items))
        if not lines:
            lines.append("<empty snapshot>")
        output.write_text("\n".join(lines), encoding="utf-8")
        return output

    def _collect_dsl_candidate_texts(self, snapshot: dict[tuple[int, int], str]) -> list[str]:
        values = [v.strip() for v in snapshot.values() if self._looks_like_dsl_row_text(v)]
        return list(dict.fromkeys(values))

    def _collect_merged_anchor_texts(self, snapshot: dict[tuple[int, int], str]) -> list[str]:
        # For current snapshot contract merged anchors are already deduped at source.
        values = [v.strip() for v in snapshot.values() if v.strip()]
        return list(dict.fromkeys(values))

    def _read_cell_text(self, page: Page, cell_ref: str) -> str:
        match = re.match(r"^([A-Z]+)(\d+)$", cell_ref.strip().upper())
        if not match:
            return self.raw_helper._read_cell_value(page=page, cell_ref=cell_ref)
        col = self.raw_helper._col_label_to_index(match.group(1))
        row = int(match.group(2))
        return self._read_grid_cell_cached(page=page, row=row, col=col)

    def _cell_ref(self, col: int, row: int) -> str:
        return f"{self.raw_helper._to_col_label(col)}{row}"

    def _dump_visible_text(self, page: Page, prefix: str) -> Path:
        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        output = debug_dir / f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        text = page.locator("body").inner_text(timeout=2000)
        output.write_text(text, encoding="utf-8")
        self.logger.info("layout debug text dump: %s", output)
        return output

    def _canonical_field_label(self, field: str) -> str:
        mapping = {
            "tags": "РўРµРіРё",
            "utm_source": "utm_source",
            "pipeline": "Р’РѕСЂРѕРЅРєР°",
            "dates_mode": "Р”Р°С‚С‹",
            "period": "РџРµСЂРёРѕРґ",
            "date_from": "РЎ",
            "date_to": "РџРѕ",
        }
        return mapping.get(field, field)

    def _norm(self, text: str) -> str:
        value = (text or "").strip().lower().replace("С‘", "Рµ")
        value = re.sub(r"[\.;:,]+", " ", value)
        value = re.sub(r"\s+", " ", value)
        return value.strip()


