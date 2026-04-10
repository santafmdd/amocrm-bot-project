"""Google Sheets API-based layout writer (stage block update via batch values)."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id
from src.writers.compiler import compile_stage_pivot
from src.writers.google_sheets_api_layout_discovery import GoogleSheetsApiLayoutInspector
from src.writers.models import CompiledProfileAnalyticsResult, WriterDestinationConfig


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


class GoogleSheetsApiLayoutWriter:
    """Write stage metrics to discovered layout block via Google Sheets API."""

    def __init__(self, project_root: Path, logger: logging.Logger | None = None) -> None:
        self.project_root = project_root
        self.logger = logger or logging.getLogger("project")
        self.client = GoogleSheetsApiClient(project_root=project_root, logger=self.logger)
        self.inspector = GoogleSheetsApiLayoutInspector(project_root=project_root, logger=self.logger)

    def write_profile_analytics_result(
        self,
        compiled_result: CompiledProfileAnalyticsResult,
        destination: WriterDestinationConfig,
        dry_run: bool = False,
        target_dsl_row: int | None = None,
        target_dsl_text_contains: str | None = None,
    ) -> dict[str, Any]:
        if not destination.sheet_url.strip():
            raise RuntimeError("Google Sheets API writer: destination.sheet_url is empty")
        if not destination.tab_name.strip():
            raise RuntimeError("Google Sheets API writer: destination.tab_name is empty")

        spreadsheet_id = extract_spreadsheet_id(destination.sheet_url)
        layout = destination.layout_config or {}

        discovery = self.inspector.inspect(destination=destination)
        anchors = discovery.get("anchors", []) if isinstance(discovery, dict) else []
        if not anchors:
            raise RuntimeError("API layout writer: no anchors found in API discovery")

        target_selector = {
            "dsl_row": target_dsl_row if target_dsl_row is not None else layout.get("api_target_dsl_row"),
            "dsl_text_contains": (
                target_dsl_text_contains
                if target_dsl_text_contains is not None
                else str(layout.get("api_target_dsl_text_contains", "")).strip()
            ),
        }
        selected_anchor = self._select_anchor(
            anchors=anchors,
            compiled_result=compiled_result,
            layout=layout,
            target_dsl_row=target_selector.get("dsl_row"),
            target_dsl_text_contains=target_selector.get("dsl_text_contains"),
        )
        if selected_anchor is None:
            raise RuntimeError("API layout writer: could not select target anchor for current compiled result")

        stage_aliases = layout.get("stage_aliases", {}) if isinstance(layout, dict) else {}
        pivot = compile_stage_pivot(compiled_result=compiled_result, stage_aliases=stage_aliases if isinstance(stage_aliases, dict) else {})

        row_count = self._resolve_tab_row_count(spreadsheet_id=spreadsheet_id, tab_name=destination.tab_name)
        next_anchor_dsl_row = self._find_next_anchor_dsl_row(anchors=anchors, selected_anchor=selected_anchor)
        boundary = self._read_stage_rows_for_anchor(
            spreadsheet_id=spreadsheet_id,
            tab_name=destination.tab_name,
            header_row=int(selected_anchor["header_row"]),
            stage_col=int(selected_anchor["stage_col"]),
            all_col=int(selected_anchor["all_col"]),
            active_col=int(selected_anchor["active_col"]),
            closed_col=int(selected_anchor["closed_col"]),
            row_count=row_count,
            pivot_keys={_norm(name) for name in pivot.keys()},
            next_anchor_dsl_row=next_anchor_dsl_row,
        )
        stage_rows = boundary["stage_rows"]

        updates, missing_stages = self._build_updates_for_stage_rows(
            tab_name=destination.tab_name,
            stage_rows=stage_rows,
            pivot=pivot,
            all_col=int(selected_anchor["all_col"]),
            active_col=int(selected_anchor["active_col"]),
            closed_col=int(selected_anchor["closed_col"]),
        )

        artifact = self._save_write_artifacts(
            anchor=selected_anchor,
            updates=updates,
            missing_stages=missing_stages,
            dry_run=dry_run,
            response=None,
            stage_rows_selected_count=len(stage_rows),
            stop_reason=str(boundary.get("stop_reason", "")),
            last_included_row=boundary.get("last_included_row"),
            first_excluded_row=boundary.get("first_excluded_row"),
            next_anchor_dsl_row=boundary.get("next_anchor_dsl_row"),
            hard_row_upper_bound=boundary.get("hard_row_upper_bound"),
            rows_considered_range=boundary.get("rows_considered_range"),
            target_selector=target_selector,
        )

        self.logger.info(
            "api layout write plan: anchor_header_row=%s updates=%s missing_stages=%s dry_run=%s target_selector=%s next_anchor_dsl_row=%s hard_row_upper_bound=%s stage_rows_selected_count=%s stop_reason=%s",
            selected_anchor.get("header_row"),
            len(updates),
            len(missing_stages),
            str(dry_run).lower(),
            target_selector,
            boundary.get("next_anchor_dsl_row"),
            boundary.get("hard_row_upper_bound"),
            len(stage_rows),
            boundary.get("stop_reason"),
        )

        if dry_run:
            self.logger.info("api layout writer dry-run: no batch update performed")
            return {"dry_run": True, "planned_updates": len(updates), "artifact": str(artifact)}

        data = [{"range": item["range"], "values": [[item["value"]]]} for item in updates]
        response = self.client.batch_update_values(spreadsheet_id=spreadsheet_id, data=data)

        artifact = self._save_write_artifacts(
            anchor=selected_anchor,
            updates=updates,
            missing_stages=missing_stages,
            dry_run=False,
            response=response,
            stage_rows_selected_count=len(stage_rows),
            stop_reason=str(boundary.get("stop_reason", "")),
            last_included_row=boundary.get("last_included_row"),
            first_excluded_row=boundary.get("first_excluded_row"),
            next_anchor_dsl_row=boundary.get("next_anchor_dsl_row"),
            hard_row_upper_bound=boundary.get("hard_row_upper_bound"),
            rows_considered_range=boundary.get("rows_considered_range"),
            target_selector=target_selector,
        )

        self.logger.info(
            "api layout writer update response: updatedRows=%s updatedColumns=%s updatedCells=%s responses=%s",
            response.get("totalUpdatedRows", 0),
            response.get("totalUpdatedColumns", 0),
            response.get("totalUpdatedCells", 0),
            len(response.get("responses", []) or []),
        )
        self.logger.info("api layout writer finished successfully artifact=%s", artifact)
        return response

    def _select_anchor(
        self,
        anchors: list[dict[str, Any]],
        compiled_result: CompiledProfileAnalyticsResult,
        layout: dict[str, Any],
        target_dsl_row: int | None = None,
        target_dsl_text_contains: str | None = None,
    ) -> dict[str, Any] | None:
        aliases: list[str] = []
        aliases.extend([str(v).strip() for v in compiled_result.filter_values if str(v).strip()])
        aliases.extend([str(v).strip() for v in layout.get("tag_block_aliases", []) if str(v).strip()])
        aliases.append(compiled_result.display_name)

        alias_norm = [_norm(a) for a in aliases if a]
        filter_norm = [_norm(v) for v in compiled_result.filter_values if str(v).strip()]

        row_selector: int | None = None
        if target_dsl_row is not None:
            try:
                row_selector = int(target_dsl_row)
            except Exception:
                row_selector = None
        text_selector = _norm(str(target_dsl_text_contains or ""))

        if row_selector is not None:
            by_row = [a for a in anchors if int(a.get("dsl_row", -1)) == row_selector]
            if text_selector:
                by_row = [a for a in by_row if text_selector in _norm(str(a.get("dsl_text", "")))]
            if by_row:
                chosen = by_row[0]
                self.logger.info(
                    "api layout writer selected anchor by target_dsl_row: dsl_row=%s header_row=%s dsl_text=%s",
                    chosen.get("dsl_row"),
                    chosen.get("header_row"),
                    chosen.get("dsl_text"),
                )
                return chosen
            self.logger.warning(
                "api layout writer target_dsl_row did not match any anchor: dsl_row=%s text_filter=%s",
                row_selector,
                text_selector,
            )

        if text_selector:
            by_text = [a for a in anchors if text_selector in _norm(str(a.get("dsl_text", "")))]
            if by_text:
                chosen = by_text[0]
                self.logger.info(
                    "api layout writer selected anchor by target_dsl_text_contains: dsl_row=%s header_row=%s dsl_text=%s selector=%s",
                    chosen.get("dsl_row"),
                    chosen.get("header_row"),
                    chosen.get("dsl_text"),
                    text_selector,
                )
                return chosen
            self.logger.warning(
                "api layout writer target_dsl_text_contains did not match any anchor: selector=%s",
                text_selector,
            )

        best: tuple[int, dict[str, Any]] | None = None
        for anchor in anchors:
            text = str(anchor.get("dsl_text", ""))
            text_norm = _norm(text)
            score = 0

            # Highest priority: explicit current filter values from compiled result.
            for fv in filter_norm:
                if fv and fv in text_norm:
                    score = max(score, 5)

            # Secondary priority: configured aliases for block matching.
            for a in alias_norm:
                if a and a in text_norm:
                    score = max(score, 3)

            if score == 0:
                score = 1  # structural fallback
            if best is None or score > best[0]:
                best = (score, anchor)

        if best is None:
            return None

        self.logger.info(
            "api layout writer selected anchor: dsl_row=%s header_row=%s dsl_text=%s score=%s",
            best[1].get("dsl_row"),
            best[1].get("header_row"),
            best[1].get("dsl_text"),
            best[0],
        )
        return best[1]

    def _find_next_anchor_dsl_row(
        self,
        anchors: list[dict[str, Any]],
        selected_anchor: dict[str, Any],
    ) -> int | None:
        try:
            current = int(selected_anchor.get("dsl_row", 0))
        except Exception:
            return None
        candidates: list[int] = []
        for anchor in anchors:
            try:
                row = int(anchor.get("dsl_row", 0))
            except Exception:
                continue
            if row > current:
                candidates.append(row)
        return min(candidates) if candidates else None

    def _resolve_tab_row_count(self, spreadsheet_id: str, tab_name: str) -> int:
        sheets = self.client.list_sheets(spreadsheet_id)
        for item in sheets:
            if _norm(str(item.get("title", ""))) == _norm(tab_name):
                rc = item.get("rowCount")
                if isinstance(rc, int) and rc > 0:
                    return rc
        return 800

    def _read_stage_rows_for_anchor(
        self,
        spreadsheet_id: str,
        tab_name: str,
        header_row: int,
        stage_col: int,
        all_col: int,
        active_col: int,
        closed_col: int,
        row_count: int,
        pivot_keys: set[str],
        next_anchor_dsl_row: int | None = None,
    ) -> dict[str, Any]:
        start_row = header_row + 1
        scan_end_row = min(row_count, header_row + 220)
        hard_row_upper_bound: int | None = None
        if isinstance(next_anchor_dsl_row, int) and next_anchor_dsl_row > start_row:
            hard_row_upper_bound = max(start_row, next_anchor_dsl_row - 1)
        end_row = min(scan_end_row, hard_row_upper_bound) if hard_row_upper_bound is not None else scan_end_row

        min_col = min(stage_col, all_col, active_col, closed_col)
        max_col = max(stage_col, all_col, active_col, closed_col)
        min_label = _to_col_label(min_col)
        max_label = _to_col_label(max_col)
        range_a1 = f"{tab_name}!{min_label}{start_row}:{max_label}{end_row}"
        values = self.client.get_values(spreadsheet_id=spreadsheet_id, range_a1=range_a1)

        stage_rows: list[tuple[int, str]] = []
        empty_streak = 0
        stop_reason = "hard_upper_bound" if hard_row_upper_bound is not None else "range_end"
        first_excluded_row: int | None = None

        stage_idx = stage_col - min_col
        all_idx = all_col - min_col
        active_idx = active_col - min_col
        closed_idx = closed_col - min_col

        for idx, row_vals in enumerate(values):
            row_idx = start_row + idx
            if hard_row_upper_bound is not None and row_idx > hard_row_upper_bound:
                stop_reason = "hard_upper_bound_reached"
                first_excluded_row = row_idx
                break
            row = [str(v).strip() for v in row_vals] if isinstance(row_vals, list) else [str(row_vals).strip()]

            stage_text = row[stage_idx] if stage_idx < len(row) else ""
            all_text = row[all_idx] if all_idx < len(row) else ""
            active_text = row[active_idx] if active_idx < len(row) else ""
            closed_text = row[closed_idx] if closed_idx < len(row) else ""

            stage_norm = _norm(stage_text)
            all_norm = _norm(all_text)
            active_norm = _norm(active_text)
            closed_norm = _norm(closed_text)

            if not stage_text:
                empty_streak += 1
                if empty_streak >= 2:
                    stop_reason = "empty_rows_streak"
                    first_excluded_row = row_idx
                    break
                continue
            empty_streak = 0

            # Stop on next DSL row.
            is_dsl_like = (":" in stage_text) and ("=" in stage_text or ";" in stage_text or "||" in stage_text or "^=" in stage_text)
            if is_dsl_like:
                stop_reason = "new_dsl_row"
                first_excluded_row = row_idx
                break

            # Stop on next header-like row for adjacent block.
            is_header_like = (
                stage_norm in {"????", "??????"}
                and all_norm in {"???", "??? (??)", "??? ??"}
                and active_norm in {"????????", "???????? (??)", "???????? ??"}
                and closed_norm in {"????????", "???????? (??)", "???????? ??"}
            )
            if is_header_like:
                stop_reason = "new_header_row"
                first_excluded_row = row_idx
                break

            # Stop on service rows / refusal block labels.
            if (
                stage_norm == "??????"
                or stage_norm.startswith("???????? ??")
                or stage_norm.startswith("??????")
            ):
                stop_reason = "service_row"
                first_excluded_row = row_idx
                break

            # Keep only mapped stage rows; stop once block structure leaves stage domain.
            if stage_norm not in pivot_keys:
                stop_reason = "unmapped_stage_row"
                first_excluded_row = row_idx
                break

            stage_rows.append((row_idx, stage_text))

        last_included_row = stage_rows[-1][0] if stage_rows else None
        return {
            "stage_rows": stage_rows,
            "stop_reason": stop_reason,
            "last_included_row": last_included_row,
            "first_excluded_row": first_excluded_row,
            "next_anchor_dsl_row": next_anchor_dsl_row,
            "hard_row_upper_bound": hard_row_upper_bound,
            "rows_considered_range": f"{start_row}..{end_row}",
        }

    def _build_updates_for_stage_rows(

        self,
        tab_name: str,
        stage_rows: list[tuple[int, str]],
        pivot: dict[str, dict[str, int | str]],
        all_col: int,
        active_col: int,
        closed_col: int,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        pivot_by_norm: dict[str, dict[str, int | str]] = {}
        for stage_name, data in pivot.items():
            pivot_by_norm[_norm(stage_name)] = data

        updates: list[dict[str, Any]] = []
        missing: list[str] = []

        for row_idx, stage_text in stage_rows:
            key = _norm(stage_text)
            values = pivot_by_norm.get(key)
            if values is None:
                missing.append(stage_text)
                values = {"all": 0, "active": 0, "closed": 0}

            updates.append({"range": f"{tab_name}!{_to_col_label(all_col)}{row_idx}", "value": int(values.get("all", 0) or 0), "stage": stage_text})
            updates.append({"range": f"{tab_name}!{_to_col_label(active_col)}{row_idx}", "value": int(values.get("active", 0) or 0), "stage": stage_text})
            updates.append({"range": f"{tab_name}!{_to_col_label(closed_col)}{row_idx}", "value": int(values.get("closed", 0) or 0), "stage": stage_text})

        return updates, missing

    def _save_write_artifacts(
        self,
        anchor: dict[str, Any],
        updates: list[dict[str, Any]],
        missing_stages: list[str],
        dry_run: bool,
        response: dict[str, Any] | None,
        stage_rows_selected_count: int,
        stop_reason: str,
        last_included_row: int | None,
        first_excluded_row: int | None,
        next_anchor_dsl_row: int | None,
        hard_row_upper_bound: int | None,
        rows_considered_range: str | None,
        target_selector: dict[str, Any] | None,
    ) -> Path:
        debug_dir = self.project_root / "exports" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = debug_dir / f"layout_api_write_summary_{ts}.json"
        payload = {
            "anchor": anchor,
            "planned_cell_updates": updates,
            "missing_stages": sorted(set(missing_stages)),
            "dry_run": dry_run,
            "updated_response": response,
            "stage_rows_selected_count": stage_rows_selected_count,
            "stop_reason": stop_reason,
            "last_included_row": last_included_row,
            "first_excluded_row": first_excluded_row,
            "next_anchor_dsl_row": next_anchor_dsl_row,
            "hard_row_upper_bound": hard_row_upper_bound,
            "rows_considered_range": rows_considered_range,
            "target_selector": target_selector,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
