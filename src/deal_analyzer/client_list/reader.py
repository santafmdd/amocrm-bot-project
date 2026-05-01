from __future__ import annotations

from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row
from ..weekly_shared.sheets_discovery import resolve_spreadsheet_id
from .models import ClientListSheetSnapshot
from .normalizer import build_header_mapping


def _project_root(cfg: Any) -> Path:
    return Path(cfg.config_path).resolve().parents[1]


def resolve_client_list_settings(cfg: Any) -> tuple[str, str]:
    spreadsheet_id = clean_text(getattr(cfg, "client_list_spreadsheet_id", "")) or resolve_spreadsheet_id(cfg)
    sheet_name = clean_text(getattr(cfg, "client_list_sheet_name", "")) or "Клиентский список"
    return spreadsheet_id, sheet_name


def read_client_list_sheet(
    *,
    cfg: Any,
    logger: Any,
    spreadsheet_id: str = "",
    sheet_name: str = "",
) -> ClientListSheetSnapshot:
    resolved_spreadsheet_id, resolved_sheet_name = resolve_client_list_settings(cfg)
    spreadsheet_id = clean_text(spreadsheet_id) or resolved_spreadsheet_id
    sheet_name = clean_text(sheet_name) or resolved_sheet_name
    client = GoogleSheetsApiClient(project_root=_project_root(cfg), logger=logger)
    matrix: list[list[str]] = []
    selected_sheet_name = sheet_name
    requested_candidates = [
        clean_text(getattr(cfg, "client_list_sheet_name", "")),
        "Клиентский список",
        "база",
        "База",
        "Базы стратегия",
        "База настройки",
    ]
    fallback_candidates: list[str] = []
    seen: set[str] = set()
    for item in requested_candidates:
        value = clean_text(item)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        fallback_candidates.append(value)
    for candidate_sheet in fallback_candidates:
        try:
            matrix = client.get_values(spreadsheet_id, f"'{candidate_sheet}'!A1:AZ")
            selected_sheet_name = candidate_sheet
            break
        except Exception:
            continue
    if not matrix:
        # Raise original-style error for visibility
        matrix = client.get_values(spreadsheet_id, f"'{sheet_name}'!A1:AZ")
    if not matrix:
        return ClientListSheetSnapshot(
            spreadsheet_id=spreadsheet_id,
            sheet_name=selected_sheet_name,
            header_row_number=1,
            headers=[],
            rows=[],
        )
    header_row_number = detect_header_row(matrix, start_row=1, min_nonempty=3)
    header_idx = max(0, header_row_number - 1)
    headers = [clean_text(item) for item in matrix[header_idx]]
    rows = [list(map(clean_text, row)) for row in matrix[header_idx + 1 :]]
    return ClientListSheetSnapshot(
        spreadsheet_id=spreadsheet_id,
        sheet_name=selected_sheet_name,
        header_row_number=header_row_number,
        headers=headers,
        rows=rows,
    )


def discover_client_list_sheet(
    *,
    cfg: Any,
    logger: Any,
    spreadsheet_id: str = "",
    sheet_name: str = "",
) -> dict[str, Any]:
    snapshot = read_client_list_sheet(
        cfg=cfg,
        logger=logger,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
    )
    mapped = build_header_mapping(snapshot.headers, cfg=cfg)
    mapped_columns = {
        field: snapshot.headers[idx]
        for field, idx in mapped.items()
        if isinstance(idx, int) and 0 <= idx < len(snapshot.headers)
    }
    warnings: list[str] = []
    required = {"status_text", "next_step_text"}
    missing_required = sorted(required.difference(set(mapped.keys())))
    if missing_required:
        warnings.append(f"missing_required_columns:{','.join(missing_required)}")
    return {
        "status": "ok" if snapshot.headers else "empty_sheet",
        "client_list_enabled": bool(getattr(cfg, "client_list_enabled", False)),
        "spreadsheet_id": snapshot.spreadsheet_id,
        "sheet_name": snapshot.sheet_name,
        "header_row_number": snapshot.header_row_number,
        "headers": snapshot.headers,
        "rows_total": len(snapshot.rows),
        "mapped_columns": mapped_columns,
        "unmapped_aliases": sorted([key for key in mapped.keys() if key not in mapped_columns]),
        "warnings": warnings,
    }
