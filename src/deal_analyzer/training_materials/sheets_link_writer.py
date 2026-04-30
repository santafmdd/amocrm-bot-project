from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import AUTH_MODE_AUTO, AUTH_MODE_INTERACTIVE_BOOTSTRAP, GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers
from ..weekly_shared.sheets_discovery import resolve_spreadsheet_id
from ..weekly_shared.week_plan_reader import WEEK_PLAN_ALIASES
from .docs_writer import training_materials_required_scopes
from .validation import is_valid_url_or_empty


def _col_letter(index: int) -> str:
    out = ""
    value = max(1, int(index))
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
    return out


def _plan_header_mapping(headers: list[str]) -> dict[str, int]:
    aliases = dict(WEEK_PLAN_ALIASES)
    return map_headers(headers, aliases).mapped


def execute_links_write(
    *,
    cfg: Any,
    run_dir: Path,
    plan_sheet_name: str,
    dry_run: bool,
    write: bool,
    overwrite_links: bool,
    strict_preflight: bool,
    logger: Any,
    force_reauth: bool = False,
) -> dict[str, Any]:
    payload_path = run_dir / "training_materials_payload.json"
    if not payload_path.exists():
        raise FileNotFoundError(f"training payload not found: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []

    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(
        project_root=app_root,
        logger=logger,
        scopes=training_materials_required_scopes(),
        auth_mode=AUTH_MODE_INTERACTIVE_BOOTSTRAP if bool(force_reauth) else AUTH_MODE_AUTO,
    )
    spreadsheet_id = resolve_spreadsheet_id(cfg)

    matrix = client.get_values(spreadsheet_id, f"'{plan_sheet_name}'!A1:AZ30")
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=3)
    header_row = client.get_values(spreadsheet_id, f"'{plan_sheet_name}'!A{header_row_number}:AZ{header_row_number}")
    headers = [clean_text(x) for x in (header_row[0] if header_row else [])]
    mapped = _plan_header_mapping(headers)

    missing = [field for field in ("training_link", "post_training_task_link") if field not in mapped]
    if missing:
        return {
            "mode": "dry_run" if dry_run or (not write) else "real_write",
            "write_allowed": False,
            "block_reason": "missing_plan_link_columns",
            "missing_columns": missing,
            "rows_links_to_write": 0,
            "rows_skipped_existing_links": 0,
            "rows_quarantined": len(rows),
            "planned_value_ranges": [],
            "write_strategy": "values_only",
            "structural_changes_required": False,
            "planned_structural_operations": [],
        }

    updates: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    rows_skipped_existing_links = 0
    rows_missing_generated_links = 0
    missing_generated_links_examples: list[dict[str, Any]] = []

    for item in rows:
        if not isinstance(item, dict):
            continue
        row_number = int(item.get("row_number", 0) or 0)
        if row_number <= 0:
            quarantined.append({"reason": "row_number_missing", "row": item})
            continue
        training_link = clean_text(item.get("training_link", ""))
        task_link = clean_text(item.get("post_training_task_link", ""))
        existing_training_link = clean_text(item.get("existing_training_link", ""))
        existing_task_link = clean_text(item.get("existing_post_training_task_link", ""))

        if not is_valid_url_or_empty(training_link) or not is_valid_url_or_empty(task_link):
            quarantined.append(
                {
                    "row_number": row_number,
                    "reason": "invalid_generated_link",
                    "training_link": training_link,
                    "post_training_task_link": task_link,
                }
            )
            continue
        if not training_link and not task_link:
            rows_missing_generated_links += 1
            if len(missing_generated_links_examples) < 10:
                missing_generated_links_examples.append(
                    {
                        "row_number": row_number,
                        "reason": "generated_links_missing",
                        "existing_training_link": existing_training_link,
                        "existing_post_training_task_link": existing_task_link,
                    }
                )
            continue

        training_writable = bool(training_link and (overwrite_links or (not existing_training_link)))
        task_writable = bool(task_link and (overwrite_links or (not existing_task_link)))
        if not training_writable and not task_writable:
            rows_skipped_existing_links += 1
            continue

        if training_writable:
            col = _col_letter(int(mapped["training_link"]) + 1)
            updates.append({"range": f"'{plan_sheet_name}'!{col}{row_number}:{col}{row_number}", "values": [[training_link]]})
        if task_writable:
            col = _col_letter(int(mapped["post_training_task_link"]) + 1)
            updates.append({"range": f"'{plan_sheet_name}'!{col}{row_number}:{col}{row_number}", "values": [[task_link]]})

    block_reason = ""
    if strict_preflight and not rows:
        block_reason = "rows_empty"
    if strict_preflight and write and (not dry_run) and rows and not updates and rows_missing_generated_links > 0:
        block_reason = "generated_links_missing"
    if strict_preflight and write and (not dry_run) and rows and not updates and not quarantined and rows_skipped_existing_links == 0 and rows_missing_generated_links == 0:
        block_reason = "nothing_to_write"

    write_allowed = bool(write and (not dry_run) and cfg.deal_analyzer_write_enabled and not block_reason)

    if write_allowed and updates:
        client.batch_update_values(spreadsheet_id, updates)

    planned_ranges = [str(item.get("range") or "") for item in updates]
    status = {
        "mode": "real_write" if write and (not dry_run) else "dry_run",
        "write_strategy": "values_only",
        "structural_changes_required": False,
        "planned_structural_operations": [],
        "write_allowed": write_allowed,
        "block_reason": block_reason if (not write_allowed) else "",
        "rows_links_to_write": len(updates),
        "rows_skipped_existing_links": rows_skipped_existing_links,
        "rows_missing_generated_links": rows_missing_generated_links,
        "missing_generated_links_examples": missing_generated_links_examples,
        "rows_quarantined": len(quarantined),
        "quarantined_rows": quarantined,
        "planned_value_ranges": planned_ranges,
        "rows_written": len(updates) if write_allowed else 0,
    }
    return status
