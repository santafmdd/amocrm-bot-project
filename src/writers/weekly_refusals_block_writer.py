"""API writer for weekly refusals blocks (before/after status tables)."""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id
from src.safety import ensure_inside_root
from src.writers.models import WriterDestinationConfig


@dataclass(frozen=True)
class WeeklyRefusalsWriteResult:
    dry_run: bool
    planned_updates: int
    updated_cells: int
    summary_path: Path
    update_response: dict[str, Any]


@dataclass
class _RowRecord:
    before_status: str = ""
    before_manual: str = ""
    before_count_existing: str = ""
    after_status: str = ""
    after_manual: str = ""
    after_count_existing: str = ""


class WeeklyRefusalsBlockWriter:
    def __init__(
        self,
        *,
        project_root: Path,
        exports_dir: Path,
        logger: logging.Logger | None = None,
        client_factory: Callable[[Path, logging.Logger | None], GoogleSheetsApiClient] | None = None,
    ) -> None:
        self.project_root = project_root
        self.exports_dir = exports_dir
        self.logger = logger or logging.getLogger("project")
        self._client_factory = client_factory or (lambda root, log: GoogleSheetsApiClient(project_root=root, logger=log))

    def write_block(
        self,
        *,
        destination: WriterDestinationConfig,
        parsed_result: dict[str, Any],
        dry_run: bool,
    ) -> WeeklyRefusalsWriteResult:
        requested_tab = str(destination.tab_name or "").strip()
        if not requested_tab:
            raise RuntimeError("Weekly refusals writer: destination.tab_name is empty.")

        spreadsheet_id = extract_spreadsheet_id(destination.sheet_url)
        client = self._client_factory(self.project_root, self.logger)
        resolved_tab = client.resolve_sheet_title(spreadsheet_id, requested_tab)

        layout = dict(destination.layout_config or {})
        block_width = max(1, int(layout.get("block_width", 6) or 6))
        data_start_offset = max(1, int(layout.get("data_start_row_offset", 2) or 2))
        after_col_offset = max(1, int(layout.get("after_table_col_offset", 3) or 3))

        writer_mode = self._norm_mode_value(parsed_result.get("mode", "weekly"))
        before_rows_raw = parsed_result.get("aggregated_before_status_counts", []) or []
        after_rows_raw = parsed_result.get("aggregated_after_status_counts", []) or []

        before_rows, inserted_new_before, final_before_order = self._sort_before_rows(
            rows=before_rows_raw,
            canonical_order=layout.get("canonical_before_order", []),
        )
        after_rows, inserted_new_after_groups, inserted_new_after_items, final_after_order = self._sort_after_rows(
            rows=self._prepare_after_rows(after_rows_raw),
            canonical_group_order=layout.get("canonical_after_group_order", []),
            canonical_item_order=layout.get("canonical_after_item_order", {}),
        )

        anchor = self._resolve_section_anchor(
            client=client,
            spreadsheet_id=spreadsheet_id,
            tab_title=resolved_tab,
            destination=destination,
        )
        anchor_row = int(anchor["anchor_row"])
        anchor_col = int(anchor["anchor_col"])

        header_row = anchor_row + data_start_offset - 1
        if bool(layout.get("detect_header_row", False)):
            detected = self._detect_header_row(
                client=client,
                spreadsheet_id=spreadsheet_id,
                tab_title=resolved_tab,
                anchor_row=anchor_row,
                anchor_col=anchor_col,
            )
            if isinstance(detected, int) and detected >= anchor_row:
                header_row = detected
        data_start_row = header_row + 1

        next_section_row = self._resolve_next_section_row(
            client=client,
            spreadsheet_id=spreadsheet_id,
            tab_title=resolved_tab,
            anchor_row=anchor_row,
            anchor_col=anchor_col,
            destination=destination,
            anchor_context=anchor,
        )

        row_limit = (next_section_row - 1) if isinstance(next_section_row, int) else None
        existing_rows = self._read_existing_rows(
            client=client,
            spreadsheet_id=spreadsheet_id,
            tab_title=resolved_tab,
            data_start_row=data_start_row,
            row_limit=row_limit,
            anchor_col=anchor_col,
            block_width=block_width,
            min_rows=max(len(before_rows), len(after_rows), 1),
        )
        existing_before_rows_count = sum(1 for row in existing_rows if str(row.before_status).strip())
        existing_after_rows_count = sum(1 for row in existing_rows if str(row.after_status).strip())

        planned_rows, inserted_row_positions = self._merge_rows(
            existing_rows=existing_rows,
            before_rows=before_rows,
            after_rows=after_rows,
            canonical_before=layout.get("canonical_before_order", []),
            canonical_after_groups=layout.get("canonical_after_group_order", []),
            canonical_after_items=layout.get("canonical_after_item_order", {}),
            mode=writer_mode,
        )

        existing_total_rows = max(len(existing_rows), 1)
        final_total_rows = max(len(planned_rows), 1)
        rows_to_insert = max(0, final_total_rows - existing_total_rows)

        insert_operations: list[dict[str, int]] = []
        if rows_to_insert > 0:
            insert_at = data_start_row + existing_total_rows - 1
            insert_operations.append({"sheet_row": insert_at, "row_count": rows_to_insert})
            if not dry_run:
                client.insert_rows(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=resolved_tab,
                    start_index=insert_at - 1,
                    row_count=rows_to_insert,
                )
            if isinstance(next_section_row, int):
                next_section_row += rows_to_insert

        write_updates = self._build_structured_updates(
            tab=resolved_tab,
            data_start_row=data_start_row,
            anchor_col=anchor_col,
            after_col_offset=after_col_offset,
            planned_rows=planned_rows,
        )

        update_response: dict[str, Any] = {"dry_run": True, "responses": []}
        if not dry_run:
            update_response = client.batch_update_values(spreadsheet_id=spreadsheet_id, data=write_updates)

        summary_path = self._save_summary(
            destination=destination,
            parsed_result=parsed_result,
            requested_tab=requested_tab,
            resolved_tab=resolved_tab,
            updates=write_updates,
            dry_run=dry_run,
            update_response=update_response,
            anchor=anchor,
            data_start_row=data_start_row,
            header_row=header_row,
            next_section_row=next_section_row,
            clear_bounds={},
            write_bounds=self._compute_write_bounds(write_updates),
            inserted_new_before_statuses=inserted_new_before,
            inserted_new_after_groups=inserted_new_after_groups,
            inserted_new_after_items=inserted_new_after_items,
            final_before_order=final_before_order,
            final_after_order=final_after_order,
            anchor_required=bool(anchor.get("anchor_required", True)),
            anchor_found=bool(anchor.get("anchor_found", False)),
            fallback_allowed=bool(anchor.get("fallback_allowed", False)),
            fallback_used=bool(anchor.get("fallback_used", False)),
            clear_cells_count=0,
            write_cells_count=self._count_cells(write_updates),
            existing_before_rows_count=existing_before_rows_count,
            existing_after_rows_count=existing_after_rows_count,
            updated_before_rows_count=sum(1 for row in planned_rows if row.before_status),
            updated_after_rows_count=sum(1 for row in planned_rows if row.after_status),
            inserted_before_rows_count=max(0, sum(1 for pos in inserted_row_positions if pos.get("side") in ("before", "both"))),
            inserted_after_rows_count=max(0, sum(1 for pos in inserted_row_positions if pos.get("side") in ("after", "both"))),
            inserted_row_positions=inserted_row_positions,
            preserved_manual_columns=["C", "F"],
            insert_operations=insert_operations,
            writer_mode=writer_mode,
        )

        planned_updates = len(write_updates)
        updated_cells = int(update_response.get("totalUpdatedCells", 0) or 0) if not dry_run else 0
        self.logger.info(
            "weekly refusals write finished: dry_run=%s planned_updates=%s updated_cells=%s summary=%s",
            str(bool(dry_run)).lower(),
            planned_updates,
            updated_cells,
            summary_path,
        )
        return WeeklyRefusalsWriteResult(
            dry_run=bool(dry_run),
            planned_updates=planned_updates,
            updated_cells=updated_cells,
            summary_path=summary_path,
            update_response=update_response,
        )

    def _prepare_after_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        items = [dict(row) for row in rows if str(dict(row).get("status", "")).strip()]
        has_granular = any("(" in str(item.get("status", "")) and ")" in str(item.get("status", "")) for item in items)
        if has_granular:
            items = [item for item in items if _norm_phrase(str(item.get("status", ""))) != _norm_phrase("закрыто и не реализовано")]
        return items

    def _detect_header_row(
        self,
        *,
        client: GoogleSheetsApiClient,
        spreadsheet_id: str,
        tab_title: str,
        anchor_row: int,
        anchor_col: int,
    ) -> int | None:
        start = anchor_row + 1
        end = anchor_row + 8
        scan_suffix = f"{_col_to_label(anchor_col)}{start}:{_col_to_label(anchor_col+5)}{end}"
        matrix = client.get_values(spreadsheet_id=spreadsheet_id, range_a1=client.build_tab_a1_range(tab_title=tab_title, range_suffix=scan_suffix))
        for idx, row in enumerate(matrix):
            cells = [_norm(str(v or "")) for v in row]
            blob = " | ".join(cells)
            if "значение до" in blob and "количество" in blob and "значение после" in blob:
                return start + idx
        return None
    def _read_existing_rows(
        self,
        *,
        client: GoogleSheetsApiClient,
        spreadsheet_id: str,
        tab_title: str,
        data_start_row: int,
        row_limit: int | None,
        anchor_col: int,
        block_width: int,
        min_rows: int,
    ) -> list[_RowRecord]:
        probe_rows = max(min_rows + 10, 80)
        end_row = data_start_row + probe_rows - 1
        if isinstance(row_limit, int) and row_limit >= data_start_row:
            end_row = min(end_row, row_limit)
        scan_suffix = f"{_col_to_label(anchor_col)}{data_start_row}:{_col_to_label(anchor_col + block_width - 1)}{end_row}"
        matrix = client.get_values(spreadsheet_id=spreadsheet_id, range_a1=client.build_tab_a1_range(tab_title=tab_title, range_suffix=scan_suffix))

        rows: list[_RowRecord] = []
        empty_band = 0
        for row in matrix:
            cells = [str(cell or "").strip() for cell in (row or [])]
            while len(cells) < block_width:
                cells.append("")
            rec = _RowRecord(
                before_status=cells[0],
                before_count_existing=cells[1],
                before_manual=cells[2],
                after_status=cells[3],
                after_count_existing=cells[4],
                after_manual=cells[5],
            )
            has_meaningful = any([rec.before_status, rec.after_status, rec.before_manual, rec.after_manual])
            rows.append(rec)
            if has_meaningful:
                empty_band = 0
            else:
                empty_band += 1
                if len(rows) >= min_rows and empty_band >= 4:
                    break

        while len(rows) < min_rows:
            rows.append(_RowRecord())
        return rows

    def _merge_rows(
        self,
        *,
        existing_rows: list[_RowRecord],
        before_rows: list[dict[str, Any]],
        after_rows: list[dict[str, Any]],
        canonical_before: Any,
        canonical_after_groups: Any,
        canonical_after_items: Any,
        mode: str,
    ) -> tuple[list[_RowRecord], list[dict[str, Any]]]:
        records = list(existing_rows)
        insertions: list[dict[str, Any]] = []

        before_counts = defaultdict(int)
        for row in before_rows:
            status = str(row.get("status", "")).strip()
            if not status:
                continue
            before_counts[_extract_before_status_core(status)] += int(row.get("count", 0) or 0)

        existing_before_core = {_extract_before_status_core(r.before_status): idx for idx, r in enumerate(records) if r.before_status}
        before_order = [str(x).strip() for x in canonical_before if str(x).strip()] if isinstance(canonical_before, list) else []
        before_rank = {_extract_before_status_core(item): i for i, item in enumerate(before_order) if _extract_before_status_core(item)}

        for row in before_rows:
            src_status = str(row.get("status", "")).strip()
            if not src_status:
                continue
            core = _extract_before_status_core(src_status)
            if core in existing_before_core:
                continue
            pos = self._find_insert_position_for_before(records, core, before_rank)
            records.insert(pos, _RowRecord(before_status=src_status))
            existing_before_core = {_extract_before_status_core(r.before_status): idx for idx, r in enumerate(records) if r.before_status}
            insertions.append({"row_offset": pos, "side": "before", "status": src_status})

        after_counts = defaultdict(int)
        for row in after_rows:
            status = str(row.get("status", "")).strip()
            if not status:
                continue
            g, i = _split_after_status(status)
            after_counts[(g, i)] += int(row.get("count", 0) or 0)

        existing_after_key = {_split_after_status(r.after_status): idx for idx, r in enumerate(records) if r.after_status}
        group_order = [str(x).strip() for x in canonical_after_groups if str(x).strip()] if isinstance(canonical_after_groups, list) else []
        group_rank = {_norm_phrase(v): i for i, v in enumerate(group_order)}
        item_order_map: dict[str, dict[str, int]] = {}
        if isinstance(canonical_after_items, dict):
            for g, items in canonical_after_items.items():
                gk = _norm_phrase(str(g))
                if not gk or not isinstance(items, list):
                    continue
                item_order_map[gk] = {_norm_phrase(str(i)): idx for idx, i in enumerate(items) if str(i).strip()}

        for row in after_rows:
            src_status = str(row.get("status", "")).strip()
            if not src_status:
                continue
            key = _split_after_status(src_status)
            if key in existing_after_key:
                continue
            pos = self._find_insert_position_for_after(records, key, group_rank, item_order_map)
            records.insert(pos, _RowRecord(after_status=src_status))
            existing_after_key = {_split_after_status(r.after_status): idx for idx, r in enumerate(records) if r.after_status}
            insertions.append({"row_offset": pos, "side": "after", "status": src_status})

        for rec in records:
            core = _extract_before_status_core(rec.before_status)
            current_before = before_counts.get(core)
            rec.before_count_existing = self._resolve_mode_value(existing=rec.before_count_existing, current=current_before, mode=mode)

            key = _split_after_status(rec.after_status)
            current_after = after_counts.get(key)
            rec.after_count_existing = self._resolve_mode_value(existing=rec.after_count_existing, current=current_after, mode=mode)

        return records, insertions

    def _resolve_mode_value(self, *, existing: str, current: int | None, mode: str) -> str:
        existing_num = _parse_numeric(existing)
        if mode == "cumulative":
            if current is None:
                return str(existing).strip()
            return str((existing_num or 0) + int(current))
        if current is None:
            return ""
        return str(int(current))

    def _norm_mode_value(self, mode: Any) -> str:
        raw = str(mode or "weekly").strip().lower()
        return "cumulative" if raw == "cumulative" else "weekly"
    def _find_insert_position_for_before(self, records: list[_RowRecord], core: str, rank_map: dict[str, int]) -> int:
        if not records:
            return 0
        this_rank = rank_map.get(core, 10**6)
        for idx, rec in enumerate(records):
            other_core = _extract_before_status_core(rec.before_status)
            if not other_core:
                continue
            other_rank = rank_map.get(other_core, 10**6)
            if this_rank < other_rank:
                return idx
        return len(records)

    def _find_insert_position_for_after(
        self,
        records: list[_RowRecord],
        key: tuple[str, str],
        group_rank: dict[str, int],
        item_rank_map: dict[str, dict[str, int]],
    ) -> int:
        if not records:
            return 0
        g, i = key
        this_g_rank = group_rank.get(g, 10**6)
        this_i_rank = item_rank_map.get(g, {}).get(i, 10**6)
        for idx, rec in enumerate(records):
            rg, ri = _split_after_status(rec.after_status)
            if not rg and not ri:
                continue
            rg_rank = group_rank.get(rg, 10**6)
            ri_rank = item_rank_map.get(rg, {}).get(ri, 10**6)
            if (this_g_rank, this_i_rank, g, i) < (rg_rank, ri_rank, rg, ri):
                return idx
        return len(records)

    def _build_structured_updates(
        self,
        *,
        tab: str,
        data_start_row: int,
        anchor_col: int,
        after_col_offset: int,
        planned_rows: list[_RowRecord],
    ) -> list[dict[str, Any]]:
        rows = max(1, len(planned_rows))
        if not planned_rows:
            planned_rows = [_RowRecord()]

        before_matrix = [[r.before_status, r.before_count_existing] for r in planned_rows]
        after_matrix = [[r.after_status, r.after_count_existing] for r in planned_rows]

        before_range = _a1_range(tab, data_start_row, anchor_col, rows=rows, cols=2)
        after_range = _a1_range(tab, data_start_row, anchor_col + after_col_offset, rows=rows, cols=2)
        return [
            {"range": before_range, "values": before_matrix},
            {"range": after_range, "values": after_matrix},
        ]

    def _resolve_section_anchor(
        self,
        *,
        client: GoogleSheetsApiClient,
        spreadsheet_id: str,
        tab_title: str,
        destination: WriterDestinationConfig,
    ) -> dict[str, Any]:
        layout = dict(destination.layout_config or {})
        scan_rows = max(1, int(layout.get("anchor_scan_max_rows", 400) or 400))
        scan_cols = max(6, int(layout.get("anchor_scan_max_cols", 20) or 20))
        scan_suffix = f"A1:{_col_to_label(scan_cols)}{scan_rows}"
        matrix = client.get_values(spreadsheet_id=spreadsheet_id, range_a1=client.build_tab_a1_range(tab_title=tab_title, range_suffix=scan_suffix))

        block_kind = str(layout.get("block_kind", "")).strip().lower()
        is_weekly_refusals = block_kind == "weekly_refusals"
        anchor_required = bool(layout.get("anchor_required", True if is_weekly_refusals else False))
        fallback_allowed = bool(layout.get("allow_start_cell_fallback", False if is_weekly_refusals else True))

        fallback_start_cell = str(destination.start_cell or "A1").strip() or "A1"
        fallback_row, fallback_col = _parse_a1_cell(fallback_start_cell)

        section_matcher = _ensure_matcher(layout.get("section_title_text_contains", ""))
        anchor_cell = str(layout.get("anchor_cell", "")).strip().upper()

        if section_matcher:
            for r_idx, row in enumerate(matrix, start=1):
                for c_idx, value in enumerate(row, start=1):
                    raw = str(value or "")
                    if _match_text(raw, section_matcher):
                        return {
                            "anchor_source": "section_title",
                            "anchor_row": r_idx,
                            "anchor_col": c_idx,
                            "anchor_cell": f"{_col_to_label(c_idx)}{r_idx}",
                            "anchor_text": raw,
                            "anchor_required": anchor_required,
                            "anchor_found": True,
                            "fallback_allowed": fallback_allowed,
                            "fallback_used": False,
                            "scan_suffix": scan_suffix,
                            "scan_rows": scan_rows,
                            "scan_cols": scan_cols,
                        }

        if anchor_cell:
            try:
                row, col = _parse_a1_cell(anchor_cell)
                return {
                    "anchor_source": "anchor_cell",
                    "anchor_row": row,
                    "anchor_col": col,
                    "anchor_cell": f"{_col_to_label(col)}{row}",
                    "anchor_text": "",
                    "anchor_required": anchor_required,
                    "anchor_found": True,
                    "fallback_allowed": fallback_allowed,
                    "fallback_used": False,
                    "scan_suffix": scan_suffix,
                    "scan_rows": scan_rows,
                    "scan_cols": scan_cols,
                }
            except Exception:
                pass

        if fallback_allowed:
            return {
                "anchor_source": "start_cell_fallback",
                "anchor_row": fallback_row,
                "anchor_col": fallback_col,
                "anchor_cell": f"{_col_to_label(fallback_col)}{fallback_row}",
                "anchor_text": "",
                "anchor_required": anchor_required,
                "anchor_found": False,
                "fallback_allowed": fallback_allowed,
                "fallback_used": True,
                "scan_suffix": scan_suffix,
                "scan_rows": scan_rows,
                "scan_cols": scan_cols,
            }

        similar_titles: list[str] = []
        token = section_matcher[0] if section_matcher else ""
        for row in matrix:
            for value in row:
                raw = str(value or "").strip()
                if not raw:
                    continue
                norm = _norm(raw)
                if token and (token in norm or norm in token or "отказы" in norm):
                    if raw not in similar_titles:
                        similar_titles.append(raw)
                if len(similar_titles) >= 12:
                    break
            if len(similar_titles) >= 12:
                break

        raise RuntimeError(
            "Weekly refusals anchor not found: "
            f"target_id={destination.target_id} tab_name={tab_title} "
            f"section_title_text_contains={layout.get('section_title_text_contains', '')!r} "
            f"scan_range={scan_suffix} scan_rows={scan_rows} scan_cols={scan_cols} "
            f"fallback_allowed={str(fallback_allowed).lower()} "
            f"similar_titles={similar_titles}"
        )

    def _resolve_next_section_row(
        self,
        *,
        client: GoogleSheetsApiClient,
        spreadsheet_id: str,
        tab_title: str,
        anchor_row: int,
        anchor_col: int,
        destination: WriterDestinationConfig,
        anchor_context: dict[str, Any],
    ) -> int | None:
        layout = dict(destination.layout_config or {})
        scan_rows = int(anchor_context.get("scan_rows", 400) or 400)
        scan_cols = int(anchor_context.get("scan_cols", 20) or 20)
        scan_suffix = f"A1:{_col_to_label(scan_cols)}{scan_rows}"
        matrix = client.get_values(spreadsheet_id=spreadsheet_id, range_a1=client.build_tab_a1_range(tab_title=tab_title, range_suffix=scan_suffix))

        next_matcher = _ensure_matcher(layout.get("next_section_title_text_contains", ""))
        candidates_raw = layout.get("section_title_candidates", [])
        candidates: list[list[str]] = []
        if isinstance(candidates_raw, list):
            for item in candidates_raw:
                matcher = _ensure_matcher(item)
                if matcher:
                    candidates.append(matcher)

        for r_idx, row in enumerate(matrix, start=1):
            if r_idx <= anchor_row:
                continue
            row_values = [str(v or "") for v in row]
            if next_matcher and any(_match_text(v, next_matcher) for v in row_values):
                return r_idx
            if not next_matcher and candidates:
                for matcher in candidates:
                    if any(_match_text(v, matcher) for v in row_values):
                        return r_idx
        return None

    def _sort_before_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        canonical_order: Any,
    ) -> tuple[list[dict[str, Any]], list[str], list[str]]:
        order = [str(x).strip() for x in canonical_order if str(x).strip()] if isinstance(canonical_order, list) else []
        canonical_keys = [_extract_before_status_core(item) for item in order]
        order_idx = {key: i for i, key in enumerate(canonical_keys) if key}
        inserted: list[str] = []

        def key(row: dict[str, Any]):
            status = str(row.get("status", "")).strip()
            core_key = _extract_before_status_core(status)
            idx = order_idx.get(core_key)
            if idx is None and status and status not in inserted:
                inserted.append(status)
            return (idx if idx is not None else 10**6, core_key, _norm(status))

        sorted_rows = sorted(rows, key=key)
        final_order = [str(r.get("status", "")).strip() for r in sorted_rows if str(r.get("status", "")).strip()]
        return sorted_rows, inserted, final_order

    def _sort_after_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        canonical_group_order: Any,
        canonical_item_order: Any,
    ) -> tuple[list[dict[str, Any]], list[str], list[str], list[str]]:
        groups = [str(x).strip() for x in canonical_group_order if str(x).strip()] if isinstance(canonical_group_order, list) else []
        group_idx = {_norm_phrase(v): i for i, v in enumerate(groups)}
        item_order_map: dict[str, dict[str, int]] = {}
        if isinstance(canonical_item_order, dict):
            for g, items in canonical_item_order.items():
                if not isinstance(items, list):
                    continue
                g_norm = _norm_phrase(str(g))
                item_order_map[g_norm] = {_norm_phrase(str(it)): i for i, it in enumerate(items) if str(it).strip()}

        inserted_groups: list[str] = []
        inserted_items: list[str] = []

        def key(row: dict[str, Any]):
            status = str(row.get("status", "")).strip()
            g_norm, i_norm = _split_after_status(status)
            g_idx = group_idx.get(g_norm)
            if g_norm and g_idx is None:
                pretty = str(status).split(")", 1)[0].strip("() ") if status.startswith("(") else ""
                if pretty and pretty not in inserted_groups:
                    inserted_groups.append(pretty)
            i_idx = item_order_map.get(g_norm, {}).get(i_norm)
            if i_norm and i_idx is None and status not in inserted_items:
                inserted_items.append(status)
            return (g_idx if g_idx is not None else 10**6, g_norm, i_idx if i_idx is not None else 10**6, i_norm)

        sorted_rows = sorted(rows, key=key)
        final_order = [str(r.get("status", "")).strip() for r in sorted_rows if str(r.get("status", "")).strip()]
        return sorted_rows, inserted_groups, inserted_items, final_order

    def _compute_write_bounds(self, updates: list[dict[str, Any]]) -> dict[str, int]:
        row_min = 10**9
        row_max = 0
        col_min = 10**9
        col_max = 0
        for update in updates:
            rng = str(update.get("range", ""))
            _, payload = rng.split("!", 1)
            start, end = payload.split(":", 1)
            s_row, s_col = _parse_a1_cell(start)
            e_row, e_col = _parse_a1_cell(end)
            row_min = min(row_min, s_row)
            row_max = max(row_max, e_row)
            col_min = min(col_min, s_col)
            col_max = max(col_max, e_col)
        if row_max <= 0:
            return {}
        return {
            "row_start": row_min,
            "row_end": row_max,
            "col_start": col_min,
            "col_end": col_max,
        }

    def _count_cells(self, updates: list[dict[str, Any]]) -> int:
        total = 0
        for update in updates:
            values = update.get("values", [])
            if not isinstance(values, list):
                continue
            for row in values:
                if isinstance(row, list):
                    total += len(row)
        return total

    def _save_summary(
        self,
        *,
        destination: WriterDestinationConfig,
        parsed_result: dict[str, Any],
        requested_tab: str,
        resolved_tab: str,
        updates: list[dict[str, Any]],
        dry_run: bool,
        update_response: dict[str, Any],
        anchor: dict[str, Any],
        data_start_row: int,
        header_row: int,
        next_section_row: int | None,
        clear_bounds: dict[str, Any],
        write_bounds: dict[str, Any],
        inserted_new_before_statuses: list[str],
        inserted_new_after_groups: list[str],
        inserted_new_after_items: list[str],
        final_before_order: list[str],
        final_after_order: list[str],
        anchor_required: bool,
        anchor_found: bool,
        fallback_allowed: bool,
        fallback_used: bool,
        clear_cells_count: int,
        write_cells_count: int,
        existing_before_rows_count: int,
        existing_after_rows_count: int,
        updated_before_rows_count: int,
        updated_after_rows_count: int,
        inserted_before_rows_count: int,
        inserted_after_rows_count: int,
        inserted_row_positions: list[dict[str, Any]],
        preserved_manual_columns: list[str],
        insert_operations: list[dict[str, int]],
        writer_mode: str,
    ) -> Path:
        debug_dir = ensure_inside_root(self.exports_dir / "debug", self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_path = ensure_inside_root(debug_dir / f"weekly_refusals_write_summary_{stamp}.json", self.project_root)
        payload = {
            "report_id": parsed_result.get("report_id", ""),
            "display_name": parsed_result.get("display_name", ""),
            "destination_target_id": destination.target_id,
            "requested_tab_name": requested_tab,
            "resolved_sheet_title": resolved_tab,
            "dry_run": bool(dry_run),
            "writer_mode": writer_mode,
            "planned_updates": updates,
            "update_response": update_response,
            "anchor": {
                "found_section_title": anchor.get("anchor_source") == "section_title",
                "anchor_source": anchor.get("anchor_source", ""),
                "section_title_text": anchor.get("anchor_text", ""),
                "anchor_cell": anchor.get("anchor_cell", ""),
                "anchor_row": anchor.get("anchor_row"),
                "anchor_col": anchor.get("anchor_col"),
            },
            "data_start_row": data_start_row,
            "header_row": header_row,
            "next_section_title_row": next_section_row,
            "clear_bounds": clear_bounds,
            "write_bounds": write_bounds,
            "anchor_required": bool(anchor_required),
            "anchor_found": bool(anchor_found),
            "fallback_allowed": bool(fallback_allowed),
            "fallback_used": bool(fallback_used),
            "inserted_new_before_statuses": inserted_new_before_statuses,
            "inserted_new_after_groups": inserted_new_after_groups,
            "inserted_new_after_items": inserted_new_after_items,
            "final_before_order": final_before_order,
            "final_after_order": final_after_order,
            "clear_cells_count": int(clear_cells_count),
            "write_cells_count": int(write_cells_count),
            "source_rows_count": len(parsed_result.get("source_rows", []) or []),
            "existing_before_rows_count": int(existing_before_rows_count),
            "existing_after_rows_count": int(existing_after_rows_count),
            "updated_before_rows_count": int(updated_before_rows_count),
            "updated_after_rows_count": int(updated_after_rows_count),
            "inserted_before_rows_count": int(inserted_before_rows_count),
            "inserted_after_rows_count": int(inserted_after_rows_count),
            "preserved_manual_columns": preserved_manual_columns,
            "inserted_row_positions": inserted_row_positions,
            "insert_operations": insert_operations,
            "cleared_ranges": [],
        }
        summary_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary_path


def _parse_numeric(value: str) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.replace(" ", "")
    try:
        return int(float(raw.replace(",", ".")))
    except Exception:
        return None
def _norm(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _norm_phrase(value: str) -> str:
    raw = str(value or "").lower().replace("ё", "е")
    raw = re.sub(r"[^0-9a-zа-я]+", " ", raw)
    raw = re.sub(r"\bпервичный\b", "первый", raw)
    return " ".join(raw.split())


def _extract_before_status_core(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if "/" in raw:
        raw = raw.split("/", 1)[1]
    elif ")" in raw:
        idx = raw.find(")")
        if idx >= 0:
            raw = raw[idx + 1 :]
    return _norm_phrase(raw)


def _split_after_status(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    m = re.match(r"^\(([^)]+)\)\s*(.+)$", raw)
    if m:
        return _norm_phrase(m.group(1)), _norm_phrase(m.group(2))
    if "/" in raw:
        left, right = raw.split("/", 1)
        return _norm_phrase(left), _norm_phrase(right)
    return "", _norm_phrase(raw)


def _ensure_matcher(value: Any) -> list[str]:
    if isinstance(value, str):
        raw = value.strip()
        return [_norm(raw)] if raw else []
    if isinstance(value, list):
        out = []
        for item in value:
            text = str(item).strip()
            if text:
                out.append(_norm(text))
        return out
    return []


def _match_text(raw: str, matcher: list[str]) -> bool:
    text = _norm(raw)
    if not matcher:
        return False
    return all(token in text for token in matcher)


def _col_to_label(col: int) -> str:
    if col <= 0:
        raise ValueError(f"Invalid column index: {col}")
    out = ""
    value = col
    while value > 0:
        value, rem = divmod(value - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


def _parse_a1_cell(cell: str) -> tuple[int, int]:
    raw = str(cell or "").strip().upper().replace("'", "")
    m = re.match(r"^([A-Z]+)(\d+)$", raw)
    if not m:
        raise RuntimeError(f"Invalid A1 cell: {cell}")
    col_s, row_s = m.groups()
    row = int(row_s)
    col = 0
    for ch in col_s:
        col = col * 26 + (ord(ch) - ord("A") + 1)
    return row, col


def _a1_range(tab: str, start_row: int, start_col: int, *, rows: int, cols: int) -> str:
    end_row = start_row + max(rows, 1) - 1
    end_col = start_col + max(cols, 1) - 1
    safe_tab = str(tab).replace("'", "''")
    return f"'{safe_tab}'!{_col_to_label(start_col)}{start_row}:{_col_to_label(end_col)}{end_row}"










