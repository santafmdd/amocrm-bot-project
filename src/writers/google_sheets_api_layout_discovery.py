"""Read-only Google Sheets API discovery for layout DSL/block anchors."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id
from src.writers.layout_filter_dsl import parse_layout_row
from src.writers.models import WriterDestinationConfig


def _norm(text: str) -> str:
    value = (text or "").strip().lower().replace("ё", "е").replace("_", " ")
    value = re.sub(r"[\.;:,]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _to_col_label(col_idx: int) -> str:
    value = max(1, int(col_idx))
    chars: list[str] = []
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        chars.append(chr(ord("A") + remainder))
    return "".join(reversed(chars))


@dataclass(frozen=True)
class ApiDslCandidate:
    row: int
    col: int
    raw_text: str
    normalized_text: str


@dataclass(frozen=True)
class ApiHeaderCandidate:
    row: int
    stage_col: int
    all_col: int
    active_col: int
    closed_col: int
    col_start: int
    col_end: int


@dataclass(frozen=True)
class ApiBlockAnchor:
    dsl_row: int
    dsl_col: int
    dsl_text: str
    header_row: int
    stage_col: int
    all_col: int
    active_col: int
    closed_col: int
    table_row_start: int
    table_row_end: int
    table_col_start: int
    table_col_end: int
    topology: str


class GoogleSheetsApiLayoutInspector:
    """Read-only API discovery inspector for DSL rows and stage block anchors."""

    def __init__(self, project_root: Path, logger: logging.Logger | None = None) -> None:
        self.project_root = project_root
        self.logger = logger or logging.getLogger("project")
        self.client = GoogleSheetsApiClient(project_root=project_root, logger=self.logger)

    def inspect(self, destination: WriterDestinationConfig) -> dict[str, Any]:
        if not destination.sheet_url.strip():
            raise RuntimeError("Google Sheets API inspector: destination.sheet_url is empty")
        tab_name = (destination.tab_name or "").strip()
        if not tab_name:
            raise RuntimeError("Google Sheets API inspector: destination.tab_name is empty")

        spreadsheet_id = extract_spreadsheet_id(destination.sheet_url)
        layout = destination.layout_config or {}

        sheets = self.client.list_sheets(spreadsheet_id)
        tab_meta = next((s for s in sheets if _norm(str(s.get("title", ""))) == _norm(tab_name)), None)
        self.logger.info(
            "api discovery tab metadata: tab=%s found=%s rowCount=%s columnCount=%s",
            tab_name,
            str(tab_meta is not None).lower(),
            tab_meta.get("rowCount") if isinstance(tab_meta, dict) else None,
            tab_meta.get("columnCount") if isinstance(tab_meta, dict) else None,
        )
        if tab_meta is None:
            raise RuntimeError(
                f"DSL discovery sheet tab not found: {tab_name}. "
                "Check execution_input.target_id/table_mappings.yaml"
            )

        tab_row_count = self._to_int(tab_meta.get("rowCount"), 1000)
        tab_col_count = self._to_int(tab_meta.get("columnCount"), 52)

        scan_rows = min(self._to_int(layout.get("api_scan_max_rows"), tab_row_count), tab_row_count)
        # Not A:Z only. Default scans wider part of used grid.
        scan_cols = min(self._to_int(layout.get("api_scan_max_cols"), min(tab_col_count, 182)), tab_col_count)
        band_rows = self._to_int(layout.get("api_scan_band_rows"), 120)
        band_cols = self._to_int(layout.get("api_scan_band_cols"), 52)
        empty_bands_stop = self._to_int(layout.get("api_empty_bands_stop"), 0)  # 0 = disabled
        header_window = self._to_int(layout.get("header_search_window"), 16)
        header_fallback_window = self._to_int(layout.get("api_header_fallback_window"), 120)
        max_anchors = self._to_int(layout.get("api_target_anchors"), 20)
        scan_cell_budget = max(1, scan_rows * scan_cols)
        configured_hard_cell_limit = self._to_int(layout.get("cell_read_hard_limit"), 0)
        hard_cell_limit = max(configured_hard_cell_limit, scan_cell_budget * 3)
        if configured_hard_cell_limit > 0 and hard_cell_limit != configured_hard_cell_limit:
            self.logger.info(
                "api discovery raised cell_read_hard_limit to avoid premature stop: configured=%s effective=%s",
                configured_hard_cell_limit,
                hard_cell_limit,
            )

        self.logger.info(
            "api discovery start: spreadsheet_id=%s tab=%s scan_rows=%s scan_cols=%s band_rows=%s band_cols=%s",
            spreadsheet_id,
            tab_name,
            scan_rows,
            scan_cols,
            band_rows,
            band_cols,
        )

        stage_aliases = self._header_aliases(layout.get("header_aliases", {}))
        field_markers = {
            "теги",
            "utm_source",
            "воронка",
            "даты",
            "период",
            "с",
            "по",
            "tags",
            "pipeline",
            "dates",
            "period",
        }

        scanned_ranges: list[str] = []
        cells: dict[tuple[int, int], str] = {}
        non_empty_rows: set[int] = set()
        used_max_row = 0
        used_max_col = 0
        empty_row_bands_streak = 0
        total_cells_scanned = 0
        stop_reason = "scan_range_exhausted"

        for row_start in range(1, scan_rows + 1, band_rows):
            row_end = min(scan_rows, row_start + band_rows - 1)
            row_band_non_empty = False

            for col_start in range(1, scan_cols + 1, band_cols):
                col_end = min(scan_cols, col_start + band_cols - 1)
                range_a1 = f"{tab_name}!{_to_col_label(col_start)}{row_start}:{_to_col_label(col_end)}{row_end}"
                scanned_ranges.append(range_a1)
                self.logger.info("api discovery scanned_range=%s", range_a1)

                values = self.client.get_values(spreadsheet_id=spreadsheet_id, range_a1=range_a1)
                matrix = self.client.normalize_matrix(values, rows=(row_end - row_start + 1), cols=(col_end - col_start + 1))

                for local_r, row_vals in enumerate(matrix):
                    abs_r = row_start + local_r
                    has_non_empty = False
                    for local_c, cell in enumerate(row_vals):
                        abs_c = col_start + local_c
                        total_cells_scanned += 1
                        if total_cells_scanned >= hard_cell_limit:
                            stop_reason = "cell_read_hard_limit"
                            break
                        value = str(cell or "").strip()
                        if not value:
                            continue
                        cells[(abs_r, abs_c)] = value
                        has_non_empty = True
                        used_max_row = max(used_max_row, abs_r)
                        used_max_col = max(used_max_col, abs_c)
                    if has_non_empty:
                        row_band_non_empty = True
                        non_empty_rows.add(abs_r)

                if stop_reason == "cell_read_hard_limit":
                    break

            if stop_reason == "cell_read_hard_limit":
                break

            if row_band_non_empty:
                empty_row_bands_streak = 0
            else:
                empty_row_bands_streak += 1

            if empty_bands_stop > 0 and empty_row_bands_streak >= empty_bands_stop and used_max_row > 0:
                stop_reason = "empty_row_bands_stop"
                break

        if used_max_row == 0 or used_max_col == 0:
            result = {
                "spreadsheet_id": spreadsheet_id,
                "tab_name": tab_name,
                "scanned_ranges": scanned_ranges,
                "rows_scanned": 0,
                "non_empty_rows_count": 0,
                "dsl_candidates": [],
                "header_candidates": [],
                "anchors": [],
                "stop_reason": stop_reason,
                "used_max_row": 0,
                "used_max_col": 0,
                "discovery_mode": "metadata_banded_scan",
                "total_cells_scanned": total_cells_scanned,
            }
            self._save_artifacts(result)
            return result

        row_map = self._build_row_map(cells=cells, used_max_row=used_max_row, used_max_col=used_max_col)

        dsl_candidates = sorted(
            self._discover_dsl_candidates(row_map=row_map, field_markers=field_markers),
            key=lambda x: (x.row, x.col),
        )
        header_candidates = self._discover_header_candidates(row_map=row_map, header_aliases=stage_aliases)
        anchors = sorted(
            self._map_dsl_to_headers(
                dsl_candidates=dsl_candidates,
                header_candidates=header_candidates,
                row_map=row_map,
                used_max_row=used_max_row,
                header_window=header_window,
                header_fallback_window=header_fallback_window,
            ),
            key=lambda x: (x.dsl_row, x.dsl_col),
        )
        if max_anchors > 0:
            anchors = anchors[:max_anchors]
            if len(anchors) >= max_anchors and stop_reason in {"scan_range_exhausted", "scan_complete"}:
                stop_reason = "anchors_found_limit_reached"

        result = {
            "spreadsheet_id": spreadsheet_id,
            "tab_name": tab_name,
            "scanned_ranges": scanned_ranges,
            "rows_scanned": used_max_row,
            "non_empty_rows_count": len(non_empty_rows),
            "dsl_candidates": [d.__dict__ for d in dsl_candidates],
            "header_candidates": [h.__dict__ for h in header_candidates],
            "anchors": [{**a.__dict__, "dsl_cell": f"{_to_col_label(a.dsl_col)}{a.dsl_row}"} for a in anchors],
            "stop_reason": stop_reason,
            "used_max_row": used_max_row,
            "used_max_col": used_max_col,
            "discovery_mode": "metadata_banded_scan",
            "total_cells_scanned": total_cells_scanned,
        }
        self._save_artifacts(result)

        self.logger.info("discovery_stop_reason=%s", stop_reason)
        self.logger.info(
            "api_discovery_summary: rows_scanned=%s anchors_found=%s non_empty_rows=%s used_max_col=%s",
            used_max_row,
            len(anchors),
            len(non_empty_rows),
            used_max_col,
        )
        return result

    def _build_row_map(self, cells: dict[tuple[int, int], str], used_max_row: int, used_max_col: int) -> dict[int, list[str]]:
        row_map: dict[int, list[str]] = {}
        for row in range(1, used_max_row + 1):
            vals = [""] * used_max_col
            row_map[row] = vals
        for (r, c), v in cells.items():
            if 1 <= r <= used_max_row and 1 <= c <= used_max_col:
                row_map[r][c - 1] = v
        return row_map

    def _discover_dsl_candidates(self, row_map: dict[int, list[str]], field_markers: set[str]) -> list[ApiDslCandidate]:
        found: list[ApiDslCandidate] = []
        for row_idx, row_vals in row_map.items():
            for col_idx, value in enumerate(row_vals, start=1):
                raw = (value or "").strip()
                if not raw:
                    continue
                if not self._is_dsl_cell(raw, field_markers):
                    continue
                found.append(ApiDslCandidate(row=row_idx, col=col_idx, raw_text=raw, normalized_text=_norm(raw)))
                self.logger.info("dsl_candidate row=%s col=%s text=%s", row_idx, col_idx, raw)
        return found

    def _discover_header_candidates(self, row_map: dict[int, list[str]], header_aliases: dict[str, set[str]]) -> list[ApiHeaderCandidate]:
        out: list[ApiHeaderCandidate] = []
        for row_idx, row_vals in row_map.items():
            norm_vals = [_norm(v) for v in row_vals]
            stage_cols = [i + 1 for i, v in enumerate(norm_vals) if v in header_aliases["stage"]]
            all_cols = [i + 1 for i, v in enumerate(norm_vals) if v in header_aliases["all"]]
            active_cols = [i + 1 for i, v in enumerate(norm_vals) if v in header_aliases["active"]]
            closed_cols = [i + 1 for i, v in enumerate(norm_vals) if v in header_aliases["closed"]]
            if not (stage_cols and all_cols and active_cols and closed_cols):
                continue

            for stage_col in stage_cols:
                all_col = self._nearest_col_right_preferred(stage_col, all_cols)
                active_col = self._nearest_col_right_preferred(stage_col, active_cols)
                closed_col = self._nearest_col_right_preferred(stage_col, closed_cols)
                if not all_col or not active_col or not closed_col:
                    continue
                col_start = min(stage_col, all_col, active_col, closed_col)
                col_end = max(stage_col, all_col, active_col, closed_col)
                candidate = ApiHeaderCandidate(
                    row=row_idx,
                    stage_col=stage_col,
                    all_col=all_col,
                    active_col=active_col,
                    closed_col=closed_col,
                    col_start=col_start,
                    col_end=col_end,
                )
                if not any(
                    x.row == candidate.row
                    and x.stage_col == candidate.stage_col
                    and x.all_col == candidate.all_col
                    and x.active_col == candidate.active_col
                    and x.closed_col == candidate.closed_col
                    for x in out
                ):
                    out.append(candidate)
                    self.logger.info(
                        "header_candidate row=%s stage_col=%s all_col=%s active_col=%s closed_col=%s",
                        candidate.row,
                        candidate.stage_col,
                        candidate.all_col,
                        candidate.active_col,
                        candidate.closed_col,
                    )
        return out

    def _map_dsl_to_headers(
        self,
        dsl_candidates: list[ApiDslCandidate],
        header_candidates: list[ApiHeaderCandidate],
        row_map: dict[int, list[str]],
        used_max_row: int,
        header_window: int,
        header_fallback_window: int,
    ) -> list[ApiBlockAnchor]:
        anchors: list[ApiBlockAnchor] = []
        taken_headers: set[tuple[int, int, int, int, int]] = set()

        for dsl in sorted(dsl_candidates, key=lambda x: (x.row, x.col)):
            best: tuple[int, ApiHeaderCandidate] | None = None
            for header in header_candidates:
                if header.row <= dsl.row:
                    continue
                row_dist = header.row - dsl.row
                if row_dist > max(header_window, header_fallback_window):
                    continue
                col_dist = abs(header.stage_col - dsl.col)
                relaxed = row_dist <= header_fallback_window
                strict = row_dist <= header_window
                if not strict and not relaxed:
                    continue
                # Favor near rows first, then near columns.
                score = row_dist * 100 + col_dist
                if best is None or score < best[0]:
                    best = (score, header)

            if best is None:
                self.logger.info("anchor_rejected dsl_row=%s reason=no_header_match", dsl.row)
                continue

            header = best[1]
            header_key = (header.row, header.stage_col, header.all_col, header.active_col, header.closed_col)
            if header_key in taken_headers:
                self.logger.info("anchor_rejected dsl_row=%s reason=header_already_mapped", dsl.row)
                continue
            taken_headers.add(header_key)

            bounds = self._measure_table_bounds(
                row_map=row_map,
                header=header,
                used_max_row=used_max_row,
            )

            anchor = ApiBlockAnchor(
                dsl_row=dsl.row,
                dsl_col=dsl.col,
                dsl_text=dsl.raw_text,
                header_row=header.row,
                stage_col=header.stage_col,
                all_col=header.all_col,
                active_col=header.active_col,
                closed_col=header.closed_col,
                table_row_start=bounds["row_start"],
                table_row_end=bounds["row_end"],
                table_col_start=header.col_start,
                table_col_end=header.col_end,
                topology="vertical_stage_table",
            )
            anchors.append(anchor)
            self.logger.info(
                "anchor_accepted dsl_row=%s header_row=%s dsl_cell=%s row_bounds=%s..%s col_bounds=%s..%s",
                anchor.dsl_row,
                anchor.header_row,
                f"{_to_col_label(anchor.dsl_col)}{anchor.dsl_row}",
                anchor.table_row_start,
                anchor.table_row_end,
                anchor.table_col_start,
                anchor.table_col_end,
            )

        return anchors

    def _measure_table_bounds(self, row_map: dict[int, list[str]], header: ApiHeaderCandidate, used_max_row: int) -> dict[str, int]:
        row_start = header.row + 1
        row_end = row_start
        empty_streak = 0

        for row_idx in range(row_start, used_max_row + 1):
            row_vals = row_map.get(row_idx, [])
            segment = row_vals[header.col_start - 1 : header.col_end] if row_vals else []
            stage_text = row_vals[header.stage_col - 1] if len(row_vals) >= header.stage_col else ""

            if self._is_new_dsl_row(stage_text):
                break
            if self._is_header_row_like(segment):
                break

            if not any(str(v or "").strip() for v in segment):
                empty_streak += 1
                if empty_streak >= 2:
                    break
                continue

            empty_streak = 0
            if str(stage_text or "").strip():
                row_end = row_idx

        if row_end < row_start:
            row_end = row_start
        return {"row_start": row_start, "row_end": row_end}

    def _is_dsl_cell(self, raw: str, field_markers: set[str]) -> bool:
        norm = _norm(raw)
        has_dsl_syntax = ":" in raw and (";" in raw or "=" in raw or "||" in raw or "^=" in raw)
        has_markers = any(marker in norm for marker in field_markers)
        if not (has_dsl_syntax or has_markers):
            return False
        try:
            parse_layout_row(raw)
            return True
        except Exception:
            return has_dsl_syntax

    def _is_new_dsl_row(self, stage_text: str) -> bool:
        raw = str(stage_text or "").strip()
        if not raw:
            return False
        if ":" in raw and ("=" in raw or ";" in raw or "||" in raw or "^=" in raw):
            return True
        return False

    def _is_header_row_like(self, segment: list[str]) -> bool:
        norm = [_norm(v) for v in segment]
        if not norm:
            return False
        has_stage = any(v in {"этап", "статус", "stage", "status"} for v in norm)
        has_all = any(v in {"все", "все (шт)", "все шт", "all", "all (qty)"} for v in norm)
        has_active = any(v in {"активные", "активные (шт)", "активные шт", "active", "active (qty)"} for v in norm)
        has_closed = any(v in {"закрытые", "закрытые (шт)", "закрытые шт", "closed", "closed (qty)"} for v in norm)
        return bool(has_stage and has_all and has_active and has_closed)


    def _nearest_col_right_preferred(self, base_col: int, cols: list[int]) -> int:
        if not cols:
            return 0
        right = [c for c in cols if c >= base_col]
        if right:
            return sorted(right, key=lambda c: (c - base_col, c))[0]
        return sorted(cols, key=lambda c: abs(c - base_col))[0]

    def _nearest_col(self, base_col: int, cols: list[int]) -> int:
        if not cols:
            return 0
        return sorted(cols, key=lambda c: abs(c - base_col))[0]

    def _find_dsl_col(self, row_vals: list[str], field_markers: set[str]) -> int:
        for idx, value in enumerate(row_vals, start=1):
            raw = (value or "").strip()
            if not raw:
                continue
            if self._is_dsl_cell(raw, field_markers):
                return idx
        return 0

    def _find_col_by_alias(self, values: list[str], aliases: set[str]) -> int:
        for idx, value in enumerate(values, start=1):
            if _norm(value) in aliases:
                return idx
        return 0

    def _header_aliases(self, custom: Any) -> dict[str, set[str]]:
        base = {
            "stage": {"этап", "статус", "stage", "status"},
            "all": {"все", "все (шт)", "все шт", "all", "all (qty)"},
            "active": {"активные", "активные (шт)", "активные шт", "active", "active (qty)"},
            "closed": {"закрытые", "закрытые (шт)", "закрытые шт", "closed", "closed (qty)"},
        }
        if not isinstance(custom, dict):
            return base
        for key in ["stage", "all", "active", "closed"]:
            vals = custom.get(key, [])
            if isinstance(vals, list):
                for v in vals:
                    base[key].add(_norm(str(v)))
        return base

    def _save_artifacts(self, result: dict[str, Any]) -> None:
        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        reads_path = debug_dir / f"layout_api_discovery_reads_{ts}.json"
        reads_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        summary_path = debug_dir / f"layout_api_discovery_summary_{ts}.txt"
        summary_lines = [
            f"tab={result.get('tab_name')}",
            f"scanned_ranges={result.get('scanned_ranges')}",
            f"rows_scanned={result.get('rows_scanned')}",
            f"non_empty_rows_count={result.get('non_empty_rows_count')}",
            f"used_max_col={result.get('used_max_col')}",
            f"dsl_candidates={len(result.get('dsl_candidates', []))}",
            f"header_candidates={len(result.get('header_candidates', []))}",
            f"anchors_found={len(result.get('anchors', []))}",
            f"stop_reason={result.get('stop_reason')}",
            f"discovery_mode={result.get('discovery_mode')}",
            f"total_cells_scanned={result.get('total_cells_scanned')}",
        ]
        summary_path.write_text("\n".join(summary_lines), encoding="utf-8")
        self.logger.info("api discovery artifacts: reads=%s summary=%s", reads_path, summary_path)

    def _to_int(self, value: Any, default: int) -> int:
        try:
            parsed = int(value)
            return parsed if parsed > 0 else default
        except Exception:
            return default









