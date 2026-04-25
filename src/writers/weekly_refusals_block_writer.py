
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

from src.domain.refusal_status_normalizer import (
    canonicalize_after_status,
    canonicalize_before_status,
    canonicalize_refusal_reason,
    format_grouped_status,
    format_grouped_status_display,
    normalize_basic_text,
    normalize_group_name,
    parse_group_and_reason,
)
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
    before_count_existing: Any = ""
    after_status: str = ""
    after_manual: str = ""
    after_count_existing: Any = ""


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
        cumulative_write_strategy = self._norm_cumulative_write_strategy(parsed_result.get("cumulative_write_strategy", "recompute_from_source"))
        if writer_mode != "cumulative":
            cumulative_write_strategy = "recompute_from_source"
        period_key = str(parsed_result.get("period_key", "") or "").strip()
        force_reapply = bool(parsed_result.get("cumulative_force_reapply", False))
        mode_semantics = self._resolve_mode_semantics(writer_mode=writer_mode, cumulative_write_strategy=cumulative_write_strategy)
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
            cumulative_write_strategy=cumulative_write_strategy,
        )

        existing_capacity = self._resolve_existing_capacity(data_start_row=data_start_row, row_limit=row_limit, existing_rows=existing_rows)
        final_total_rows = max(len(planned_rows), 1)
        rows_to_insert = max(0, final_total_rows - existing_capacity)

        self.logger.info(
            "weekly refusals expansion decision: tab=%s anchor=%s data_start_row=%s existing_capacity=%s planned_rows=%s rows_to_insert=%s mode=%s",
            resolved_tab,
            anchor.get("anchor_cell", ""),
            data_start_row,
            existing_capacity,
            final_total_rows,
            rows_to_insert,
            writer_mode,
        )

        insert_operations: list[dict[str, int]] = []
        if rows_to_insert > 0:
            insert_at = next_section_row if isinstance(next_section_row, int) else (data_start_row + existing_capacity)
            insert_operations.append({"sheet_row": int(insert_at), "row_count": rows_to_insert})
            if not dry_run:
                client.insert_rows(
                    spreadsheet_id=spreadsheet_id,
                    tab_name=resolved_tab,
                    start_index=int(insert_at) - 1,
                    row_count=rows_to_insert,
                )
            if isinstance(next_section_row, int):
                next_section_row += rows_to_insert

        post_capacity = existing_capacity + rows_to_insert

        write_updates = self._build_structured_updates(
            tab=resolved_tab,
            data_start_row=data_start_row,
            anchor_col=anchor_col,
            after_col_offset=after_col_offset,
            planned_rows=planned_rows,
        )
        tail_clear_updates = self._build_tail_clear_updates(
            tab=resolved_tab,
            data_start_row=data_start_row,
            anchor_col=anchor_col,
            after_col_offset=after_col_offset,
            final_total_rows=final_total_rows,
            post_capacity=post_capacity,
        )
        if tail_clear_updates:
            write_updates.extend(tail_clear_updates)

        guard_outcome = self._guard_duplicate_period_apply(
            destination=destination,
            writer_mode=writer_mode,
            cumulative_write_strategy=cumulative_write_strategy,
            period_key=period_key,
            dry_run=bool(dry_run),
            force_reapply=force_reapply,
        )

        update_response: dict[str, Any] = {"dry_run": True, "responses": []}
        if not dry_run:
            update_response = client.batch_update_values(spreadsheet_id=spreadsheet_id, data=write_updates)
            self._record_period_apply(
                destination=destination,
                writer_mode=writer_mode,
                cumulative_write_strategy=cumulative_write_strategy,
                period_key=period_key,
            )

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
            clear_bounds=self._compute_write_bounds(tail_clear_updates),
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
            clear_cells_count=self._count_cells(tail_clear_updates),
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
            mode_semantics=mode_semantics,
            cumulative_write_strategy=cumulative_write_strategy,
            period_key=period_key,
            duplicate_guard_outcome=guard_outcome,
        )

        planned_updates = len(write_updates)
        updated_cells = int(update_response.get("totalUpdatedCells", 0) or 0) if not dry_run else 0
        self.logger.info(
            "weekly refusals write finished: dry_run=%s planned_updates=%s updated_cells=%s mode=%s semantics=%s cumulative_strategy=%s period_key=%s summary=%s",
            str(bool(dry_run)).lower(),
            planned_updates,
            updated_cells,
            writer_mode,
            mode_semantics,
            cumulative_write_strategy,
            period_key,
            summary_path,
        )
        return WeeklyRefusalsWriteResult(
            dry_run=bool(dry_run),
            planned_updates=planned_updates,
            updated_cells=updated_cells,
            summary_path=summary_path,
            update_response=update_response,
        )

    def _resolve_existing_capacity(self, *, data_start_row: int, row_limit: int | None, existing_rows: list[_RowRecord]) -> int:
        if isinstance(row_limit, int) and row_limit >= data_start_row:
            return max(1, row_limit - data_start_row + 1)
        return max(len(existing_rows), 1)

    def _prepare_after_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        aggregated: dict[tuple[str, str], int] = defaultdict(int)
        display: dict[tuple[str, str], str] = {}
        for row in rows:
            status_raw = str(dict(row).get("status", "") or "").strip()
            if not status_raw:
                continue
            canonical = canonicalize_after_status(status_raw)
            if normalize_basic_text(canonical) == normalize_basic_text("закрыто и не реализовано"):
                continue
            g, i = _split_after_status(canonical)
            if not g and not i:
                continue
            key = (g, i)
            aggregated[key] += int(dict(row).get("count", 0) or 0)
            display[key] = display.get(key) or format_grouped_status_display(*key)
        out: list[dict[str, Any]] = []
        for (g, i), count in aggregated.items():
            out.append({"status": display.get((g, i), format_grouped_status(g, i)), "count": int(count)})
        return out

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
        scan_suffix = f"{_col_to_label(anchor_col)}{start}:{_col_to_label(anchor_col + 5)}{end}"
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
        cumulative_write_strategy: str,
    ) -> tuple[list[_RowRecord], list[dict[str, Any]]]:
        before_order = [str(x).strip() for x in canonical_before if str(x).strip()] if isinstance(canonical_before, list) else []
        before_rank = {_extract_before_status_core(item): i for i, item in enumerate(before_order) if _extract_before_status_core(item)}

        group_order = [str(x).strip() for x in canonical_after_groups if str(x).strip()] if isinstance(canonical_after_groups, list) else []
        group_rank = {normalize_group_name(v): i for i, v in enumerate(group_order)}

        item_order_map: dict[str, dict[str, int]] = {}
        if isinstance(canonical_after_items, dict):
            for g, items in canonical_after_items.items():
                if not isinstance(items, list):
                    continue
                g_key = normalize_group_name(str(g))
                item_order_map[g_key] = {canonicalize_refusal_reason(str(it)): idx for idx, it in enumerate(items) if str(it).strip()}

        existing_before, existing_after = self._index_existing_rows(existing_rows)
        input_before_counts, input_before_display = self._index_before_input(before_rows)
        input_after_counts, input_after_display = self._index_after_input(after_rows)

        before_keys = set(existing_before.keys()) | set(input_before_counts.keys())
        after_keys = set(existing_after.keys()) | set(input_after_counts.keys())

        ordered_before = sorted(before_keys, key=lambda k: (before_rank.get(k, 10**6), k))
        ordered_after = sorted(
            after_keys,
            key=lambda key: (
                group_rank.get(key[0], 10**6),
                1 if not key[0] else 0,
                key[0] or "~",
                item_order_map.get(key[0], {}).get(key[1], 10**6),
                key[1],
            ),
        )

        rows_count = max(len(ordered_before), len(ordered_after), 1)
        records: list[_RowRecord] = []
        insertions: list[dict[str, Any]] = []

        for idx in range(rows_count):
            rec = _RowRecord()
            if idx < len(ordered_before):
                b_key = ordered_before[idx]
                slot = existing_before.get(b_key, {})
                if b_key not in existing_before:
                    insertions.append({"row_offset": idx, "side": "before", "status": input_before_display.get(b_key, b_key)})
                rec.before_status = str(slot.get("status") or input_before_display.get(b_key) or b_key)
                rec.before_manual = str(slot.get("manual") or "")
                rec.before_count_existing = self._resolve_mode_value(
                    existing=slot.get("count"),
                    current=input_before_counts.get(b_key),
                    mode=mode,
                    cumulative_write_strategy=cumulative_write_strategy,
                )

            if idx < len(ordered_after):
                a_key = ordered_after[idx]
                slot = existing_after.get(a_key, {})
                if a_key not in existing_after:
                    insertions.append({"row_offset": idx, "side": "after", "status": input_after_display.get(a_key, format_grouped_status(*a_key))})
                rec.after_status = str(slot.get("status") or input_after_display.get(a_key) or format_grouped_status(*a_key))
                rec.after_manual = str(slot.get("manual") or "")
                rec.after_count_existing = self._resolve_mode_value(
                    existing=slot.get("count"),
                    current=input_after_counts.get(a_key),
                    mode=mode,
                    cumulative_write_strategy=cumulative_write_strategy,
                )

            records.append(rec)

        return records, insertions

    def _index_existing_rows(self, rows: list[_RowRecord]) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
        before_map: dict[str, dict[str, Any]] = {}
        after_map: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            if row.before_status:
                key = _extract_before_status_core(row.before_status)
                if key:
                    slot = before_map.get(key)
                    if slot is None:
                        before_map[key] = {
                            "status": row.before_status.strip(),
                            "manual": row.before_manual.strip(),
                            "count": _parse_numeric(row.before_count_existing),
                        }
                    else:
                        slot["status"] = _choose_display(slot.get("status", ""), row.before_status.strip(), key)
                        slot["manual"] = slot.get("manual") or row.before_manual.strip()
                        slot["count"] = _sum_counts(slot.get("count", ""), row.before_count_existing)
            if row.after_status:
                key = _split_after_status(row.after_status)
                if key != ("", ""):
                    canonical_display = format_grouped_status_display(*key)
                    slot = after_map.get(key)
                    if slot is None:
                        after_map[key] = {
                            "status": canonical_display,
                            "manual": row.after_manual.strip(),
                            "count": _parse_numeric(row.after_count_existing),
                        }
                    else:
                        slot["status"] = canonical_display
                        slot["manual"] = slot.get("manual") or row.after_manual.strip()
                        slot["count"] = _sum_counts(slot.get("count", ""), row.after_count_existing)
        return before_map, after_map

    def _index_before_input(self, rows: list[dict[str, Any]]) -> tuple[dict[str, int], dict[str, str]]:
        counts: dict[str, int] = defaultdict(int)
        display: dict[str, str] = {}
        for row in rows:
            status_raw = str(row.get("status", "") or "").strip()
            if not status_raw:
                continue
            key = _extract_before_status_core(status_raw)
            if not key:
                continue
            counts[key] += int(row.get("count", 0) or 0)
            display[key] = _choose_display(display.get(key, ""), status_raw, key)
        return counts, display

    def _index_after_input(self, rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], int], dict[tuple[str, str], str]]:
        counts: dict[tuple[str, str], int] = defaultdict(int)
        display: dict[tuple[str, str], str] = {}
        for row in rows:
            status_raw = str(row.get("status", "") or "").strip()
            if not status_raw:
                continue
            key = _split_after_status(status_raw)
            if key == ("", ""):
                continue
            counts[key] += int(row.get("count", 0) or 0)
            display[key] = format_grouped_status_display(*key)
        return counts, display

    def _resolve_mode_value(
        self,
        *,
        existing: Any,
        current: int | None,
        mode: str,
        cumulative_write_strategy: str,
    ) -> int | str:
        existing_num = _parse_numeric(existing)
        if mode == "cumulative":
            if cumulative_write_strategy == "add_existing_values":
                if current is None:
                    return existing_num if existing_num is not None else ""
                return int((existing_num or 0) + int(current))
            if current is None:
                return ""
            return int(current)
        if current is None:
            return ""
        return int(current)

    def _norm_mode_value(self, mode: Any) -> str:
        raw = str(mode or "weekly").strip().lower()
        return "cumulative" if raw == "cumulative" else "weekly"

    def _norm_cumulative_write_strategy(self, value: Any) -> str:
        raw = str(value or "recompute_from_source").strip().lower()
        if raw in {"add_existing_values", "add"}:
            return "add_existing_values"
        return "recompute_from_source"

    def _resolve_mode_semantics(self, *, writer_mode: str, cumulative_write_strategy: str) -> str:
        if writer_mode != "cumulative":
            return "weekly_overwrite_from_source"
        if cumulative_write_strategy == "add_existing_values":
            return "cumulative_add_existing_values"
        return "recompute_from_source"

    def _sort_before_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        canonical_order: Any,
    ) -> tuple[list[dict[str, Any]], list[str], list[str]]:
        order = [str(x).strip() for x in canonical_order if str(x).strip()] if isinstance(canonical_order, list) else []
        canonical_keys = [_extract_before_status_core(item) for item in order]
        order_idx = {key: i for i, key in enumerate(canonical_keys) if key}

        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            status_raw = str(row.get("status", "") or "").strip()
            if not status_raw:
                continue
            key = _extract_before_status_core(status_raw)
            if not key:
                continue
            slot = grouped.get(key)
            if slot is None:
                grouped[key] = {"status": status_raw, "count": int(row.get("count", 0) or 0)}
            else:
                slot["count"] = int(slot.get("count", 0) or 0) + int(row.get("count", 0) or 0)
                slot["status"] = _choose_display(str(slot.get("status") or ""), status_raw, key)

        inserted: list[str] = []
        sorted_rows = sorted(
            grouped.values(),
            key=lambda row: (
                order_idx.get(_extract_before_status_core(str(row.get("status", ""))), 10**6),
                _extract_before_status_core(str(row.get("status", ""))),
            ),
        )
        for row in sorted_rows:
            status = str(row.get("status", "")).strip()
            core_key = _extract_before_status_core(status)
            if core_key and core_key not in order_idx:
                inserted.append(status)

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
        group_idx = {normalize_group_name(v): i for i, v in enumerate(groups)}
        item_order_map: dict[str, dict[str, int]] = {}
        if isinstance(canonical_item_order, dict):
            for g, items in canonical_item_order.items():
                if not isinstance(items, list):
                    continue
                g_norm = normalize_group_name(str(g))
                item_order_map[g_norm] = {canonicalize_refusal_reason(str(it)): i for i, it in enumerate(items) if str(it).strip()}

        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            status_raw = str(row.get("status", "") or "").strip()
            if not status_raw:
                continue
            key = _split_after_status(status_raw)
            if key == ("", ""):
                continue
            slot = grouped.get(key)
            if slot is None:
                grouped[key] = {"status": format_grouped_status(*key), "count": int(row.get("count", 0) or 0)}
            else:
                slot["count"] = int(slot.get("count", 0) or 0) + int(row.get("count", 0) or 0)

        inserted_groups: list[str] = []
        inserted_items: list[str] = []
        sorted_rows = sorted(
            grouped.values(),
            key=lambda row: (
                group_idx.get(_split_after_status(str(row.get("status", "")))[0], 10**6),
                1 if not _split_after_status(str(row.get("status", "")))[0] else 0,
                _split_after_status(str(row.get("status", "")))[0] or "~",
                item_order_map.get(_split_after_status(str(row.get("status", "")))[0], {}).get(_split_after_status(str(row.get("status", "")))[1], 10**6),
                _split_after_status(str(row.get("status", "")))[1],
            ),
        )
        for row in sorted_rows:
            g, i = _split_after_status(str(row.get("status", "")))
            if g and g not in group_idx and g not in inserted_groups:
                inserted_groups.append(g)
            if i and i not in item_order_map.get(g, {}) and str(row.get("status", "")) not in inserted_items:
                inserted_items.append(str(row.get("status", "")))

        final_order = [str(r.get("status", "")).strip() for r in sorted_rows if str(r.get("status", "")).strip()]
        return sorted_rows, inserted_groups, inserted_items, final_order

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

    def _build_tail_clear_updates(
        self,
        *,
        tab: str,
        data_start_row: int,
        anchor_col: int,
        after_col_offset: int,
        final_total_rows: int,
        post_capacity: int,
    ) -> list[dict[str, Any]]:
        leftover = max(0, post_capacity - final_total_rows)
        if leftover <= 0:
            return []
        clear_start = data_start_row + final_total_rows
        blank = [["", ""] for _ in range(leftover)]
        before_range = _a1_range(tab, clear_start, anchor_col, rows=leftover, cols=2)
        after_range = _a1_range(tab, clear_start, anchor_col + after_col_offset, rows=leftover, cols=2)
        return [
            {"range": before_range, "values": blank},
            {"range": after_range, "values": blank},
        ]

    def _compute_write_bounds(self, updates: list[dict[str, Any]]) -> dict[str, int]:
        row_min = 10**9
        row_max = 0
        col_min = 10**9
        col_max = 0
        for update in updates:
            rng = str(update.get("range", ""))
            if "!" not in rng:
                continue
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
        mode_semantics: str,
        cumulative_write_strategy: str,
        period_key: str,
        duplicate_guard_outcome: dict[str, Any],
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
            "writer_mode_semantics": mode_semantics,
            "cumulative_write_strategy": cumulative_write_strategy,
            "period_key": period_key,
            "duplicate_period_guard": duplicate_guard_outcome,
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


    def _guard_state_path(self) -> Path:
        debug_dir = ensure_inside_root(self.exports_dir / "debug", self.project_root)
        debug_dir.mkdir(parents=True, exist_ok=True)
        return ensure_inside_root(debug_dir / "weekly_refusals_cumulative_guard_state.json", self.project_root)

    def _load_guard_state(self) -> dict[str, Any]:
        path = self._guard_state_path()
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _save_guard_state(self, payload: dict[str, Any]) -> None:
        path = self._guard_state_path()
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _guard_duplicate_period_apply(
        self,
        *,
        destination: WriterDestinationConfig,
        writer_mode: str,
        cumulative_write_strategy: str,
        period_key: str,
        dry_run: bool,
        force_reapply: bool,
    ) -> dict[str, Any]:
        if writer_mode != "cumulative" or cumulative_write_strategy != "add_existing_values":
            return {"status": "not_applicable"}
        if not period_key:
            raise RuntimeError(
                "Cumulative add_existing_values requires non-empty period_key for idempotency guard."
            )
        if dry_run:
            return {"status": "dry_run_skip", "period_key": period_key}

        state = self._load_guard_state()
        target_key = str(destination.target_id or "")
        target_state = state.get(target_key, {}) if isinstance(state.get(target_key, {}), dict) else {}
        if period_key in target_state and not force_reapply:
            prev = target_state.get(period_key, {})
            raise RuntimeError(
                "Duplicate cumulative period apply blocked: "
                f"target_id={target_key} period_key={period_key} "
                f"previous_applied_at={prev.get('applied_at', '')} "
                "(run dry-run preview first; cumulative_force_reapply=true is unsafe without idempotent delta check)."
            )
        return {
            "status": "ok",
            "period_key": period_key,
            "force_reapply": bool(force_reapply),
        }

    def _record_period_apply(
        self,
        *,
        destination: WriterDestinationConfig,
        writer_mode: str,
        cumulative_write_strategy: str,
        period_key: str,
    ) -> None:
        if writer_mode != "cumulative" or cumulative_write_strategy != "add_existing_values" or not period_key:
            return
        state = self._load_guard_state()
        target_key = str(destination.target_id or "")
        target_state = state.get(target_key, {}) if isinstance(state.get(target_key, {}), dict) else {}
        target_state[period_key] = {
            "applied_at": datetime.now().isoformat(timespec="seconds"),
            "strategy": cumulative_write_strategy,
        }
        state[target_key] = target_state
        self._save_guard_state(state)


def _sum_counts(a: Any, b: Any) -> int | str:
    a_num = _parse_numeric(a)
    b_num = _parse_numeric(b)
    if a_num is None and b_num is None:
        return ""
    return int((a_num or 0) + (b_num or 0))


def _choose_display(current: str, candidate: str, canonical_hint: str) -> str:
    current = str(current or "").strip()
    candidate = str(candidate or "").strip()
    if not current:
        return candidate
    if not candidate:
        return current
    hint = normalize_basic_text(canonical_hint)

    def score(value: str) -> tuple[int, int]:
        norm = normalize_basic_text(value)
        return (int(norm == hint), len(norm))

    return candidate if score(candidate) > score(current) else current


def _parse_numeric(value: Any) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.lstrip("'")
    raw = raw.replace(" ", "")
    try:
        return int(float(raw.replace(",", ".")))
    except Exception:
        return None


def _norm(value: str) -> str:
    return normalize_basic_text(value)


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
    return canonicalize_before_status(raw)


def _split_after_status(value: str) -> tuple[str, str]:
    canonical = canonicalize_after_status(value)
    group, reason = parse_group_and_reason(canonical)
    return normalize_group_name(group), canonicalize_refusal_reason(reason)


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
