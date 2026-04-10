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
    value = (text or "").strip().lower().replace("?", "?")
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

        scan_cols = self._to_int(layout.get("api_scan_max_cols"), 26)
        band_rows = self._to_int(layout.get("api_scan_band_rows"), 120)
        max_rows = self._to_int(layout.get("api_scan_max_rows"), 720)
        empty_bands_stop = self._to_int(layout.get("api_empty_bands_stop"), 2)
        header_window = self._to_int(layout.get("header_search_window"), 6)
        max_anchors = self._to_int(layout.get("api_target_anchors"), 8)

        self.logger.info("api discovery start: spreadsheet_id=%s tab=%s", spreadsheet_id, tab_name)

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

        col_label = _to_col_label(scan_cols)
        scanned_ranges: list[str] = []
        dsl_candidates: list[ApiDslCandidate] = []
        header_candidates: list[ApiHeaderCandidate] = []
        anchors: list[ApiBlockAnchor] = []

        total_rows_scanned = 0
        non_empty_rows_count = 0
        band_index = 0
        empty_bands = 0
        stop_reason = "max_scan_reached"

        stage_aliases = self._header_aliases(layout.get("header_aliases", {}))
        field_markers = {"????", "utm_source", "???????", "????", "??????", "?", "??"}

        for start_row in range(1, max_rows + 1, band_rows):
            band_index += 1
            end_row = min(max_rows, start_row + band_rows - 1)
            range_a1 = f"{tab_name}!A{start_row}:{col_label}{end_row}"
            scanned_ranges.append(range_a1)
            self.logger.info("api discovery scanned_range=%s", range_a1)

            values = self.client.get_values(spreadsheet_id=spreadsheet_id, range_a1=range_a1)
            matrix = self.client.normalize_matrix(values, rows=(end_row - start_row + 1), cols=scan_cols)

            band_non_empty_rows = 0
            for local_idx, row_vals in enumerate(matrix):
                abs_row = start_row + local_idx
                total_rows_scanned += 1

                row_join = " | ".join(v for v in row_vals if v.strip()).strip()
                if row_join:
                    band_non_empty_rows += 1
                    non_empty_rows_count += 1

                dsl_col = self._find_dsl_col(row_vals, field_markers)
                if dsl_col > 0:
                    raw = row_vals[dsl_col - 1].strip()
                    norm = _norm(raw)
                    self.logger.info("dsl_candidate row=%s col=%s text=%s", abs_row, dsl_col, raw)
                    dsl_candidates.append(ApiDslCandidate(row=abs_row, col=dsl_col, raw_text=raw, normalized_text=norm))

            if band_non_empty_rows == 0:
                empty_bands += 1
            else:
                empty_bands = 0

            # header candidates and anchors under dsl rows that are in this band or nearby
            dsl_rows_here = [d for d in dsl_candidates if start_row <= d.row <= end_row]
            for dsl in dsl_rows_here:
                header = self._find_header_below(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=tab_name,
                    dsl_row=dsl.row,
                    header_window=header_window,
                    scan_cols=scan_cols,
                    header_aliases=stage_aliases,
                )
                if header is None:
                    self.logger.info("anchor_rejected dsl_row=%s reason=no_header_in_window", dsl.row)
                    continue

                header_candidates.append(header)
                self.logger.info(
                    "header_candidate row=%s stage_col=%s all_col=%s active_col=%s closed_col=%s",
                    header.row,
                    header.stage_col,
                    header.all_col,
                    header.active_col,
                    header.closed_col,
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
                )
                if not any(a.dsl_row == anchor.dsl_row and a.header_row == anchor.header_row for a in anchors):
                    anchors.append(anchor)
                    self.logger.info(
                        "anchor_accepted dsl_row=%s header_row=%s dsl_cell=%s",
                        anchor.dsl_row,
                        anchor.header_row,
                        f"{_to_col_label(anchor.dsl_col)}{anchor.dsl_row}",
                    )

            if len(anchors) >= max_anchors:
                stop_reason = "anchors_found_limit_reached"
                break

            if empty_bands >= empty_bands_stop:
                stop_reason = "empty_row_bands_stop"
                break

        result = {
            "spreadsheet_id": spreadsheet_id,
            "tab_name": tab_name,
            "scanned_ranges": scanned_ranges,
            "rows_scanned": total_rows_scanned,
            "non_empty_rows_count": non_empty_rows_count,
            "dsl_candidates": [d.__dict__ for d in dsl_candidates],
            "header_candidates": [h.__dict__ for h in header_candidates],
            "anchors": [a.__dict__ for a in anchors],
            "stop_reason": stop_reason,
        }

        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        reads_path = debug_dir / f"layout_api_discovery_reads_{ts}.json"
        reads_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

        summary_path = debug_dir / f"layout_api_discovery_summary_{ts}.txt"
        summary_lines = [
            f"tab={tab_name}",
            f"scanned_ranges={scanned_ranges}",
            f"rows_scanned={total_rows_scanned}",
            f"non_empty_rows_count={non_empty_rows_count}",
            f"dsl_candidates={len(dsl_candidates)}",
            f"header_candidates={len(header_candidates)}",
            f"anchors_found={len(anchors)}",
            f"stop_reason={stop_reason}",
        ]
        summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

        self.logger.info("discovery_stop_reason=%s", stop_reason)
        self.logger.info(
            "api_discovery_summary: rows_scanned=%s anchors_found=%s non_empty_rows=%s",
            total_rows_scanned,
            len(anchors),
            non_empty_rows_count,
        )
        self.logger.info("api discovery artifacts: reads=%s summary=%s", reads_path, summary_path)

        return result

    def _find_dsl_col(self, row_vals: list[str], field_markers: set[str]) -> int:
        for idx, value in enumerate(row_vals, start=1):
            raw = (value or "").strip()
            if not raw:
                continue
            norm = _norm(raw)
            has_dsl_syntax = ":" in raw and (";" in raw or "=" in raw or "||" in raw or "^=" in raw)
            has_markers = any(marker in norm for marker in field_markers)
            if has_dsl_syntax or has_markers:
                try:
                    parse_layout_row(raw)
                    return idx
                except Exception:
                    if has_dsl_syntax:
                        return idx
        return 0

    def _find_header_below(
        self,
        spreadsheet_id: str,
        tab_name: str,
        dsl_row: int,
        header_window: int,
        scan_cols: int,
        header_aliases: dict[str, set[str]],
    ) -> ApiHeaderCandidate | None:
        start = dsl_row + 1
        end = dsl_row + max(1, header_window)
        col_label = _to_col_label(scan_cols)
        range_a1 = f"{tab_name}!A{start}:{col_label}{end}"
        values = self.client.get_values(spreadsheet_id=spreadsheet_id, range_a1=range_a1)
        matrix = self.client.normalize_matrix(values, rows=(end - start + 1), cols=scan_cols)

        for local_idx, row_vals in enumerate(matrix):
            abs_row = start + local_idx
            norm_vals = [_norm(v) for v in row_vals]
            stage_col = self._find_col_by_alias(norm_vals, header_aliases["stage"])
            all_col = self._find_col_by_alias(norm_vals, header_aliases["all"])
            active_col = self._find_col_by_alias(norm_vals, header_aliases["active"])
            closed_col = self._find_col_by_alias(norm_vals, header_aliases["closed"])
            if all([stage_col, all_col, active_col, closed_col]):
                return ApiHeaderCandidate(
                    row=abs_row,
                    stage_col=stage_col,
                    all_col=all_col,
                    active_col=active_col,
                    closed_col=closed_col,
                )
        return None

    def _find_col_by_alias(self, values: list[str], aliases: set[str]) -> int:
        for idx, value in enumerate(values, start=1):
            if value in aliases:
                return idx
        return 0

    def _header_aliases(self, custom: Any) -> dict[str, set[str]]:
        base = {
            "stage": {"????", "??????"},
            "all": {"???", "??? (??)", "??? ??"},
            "active": {"????????", "???????? (??)", "???????? ??"},
            "closed": {"????????", "???????? (??)", "???????? ??"},
        }
        if not isinstance(custom, dict):
            return base
        for key in ["stage", "all", "active", "closed"]:
            vals = custom.get(key, [])
            if isinstance(vals, list):
                for v in vals:
                    base[key].add(_norm(str(v)))
        return base

    def _to_int(self, value: Any, default: int) -> int:
        try:
            parsed = int(value)
            return parsed if parsed > 0 else default
        except Exception:
            return default
