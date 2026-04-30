from __future__ import annotations

from pathlib import Path
from typing import Any

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient

from ..daily_control.source_reader import clean_text, detect_header_row, map_headers


def resolve_spreadsheet_id(cfg: Any) -> str:
    value = str(getattr(cfg, "deal_analyzer_spreadsheet_id", "") or "").strip()
    if value:
        return value
    from src.integrations.google_sheets_api_client import extract_spreadsheet_id

    url = str(getattr(cfg, "deal_analyzer_sheet_url", "") or "").strip()
    if url:
        return extract_spreadsheet_id(url)
    raise RuntimeError("deal_analyzer_spreadsheet_id/deal_analyzer_sheet_url is not set in config")


def discover_sheet_pair(
    *,
    cfg: Any,
    workbook_name: str,
    source_sheet_name: str,
    target_sheet_name: str,
    source_aliases: dict[str, tuple[str, ...]],
    target_aliases: dict[str, tuple[str, ...]],
    logger: Any,
    max_col: str = "AZ",
    source_min_nonempty: int = 5,
    target_min_nonempty: int = 3,
    extra_sheet_name: str = "",
    extra_aliases: dict[str, tuple[str, ...]] | None = None,
    extra_min_nonempty: int = 3,
) -> dict[str, Any]:
    app_root = Path(cfg.config_path).resolve().parents[1]
    client = GoogleSheetsApiClient(project_root=app_root, logger=logger)
    spreadsheet_id = resolve_spreadsheet_id(cfg)
    sheets = client.list_sheets(spreadsheet_id)
    sheet_titles = [str(item.get("title") or "").strip() for item in sheets if str(item.get("title") or "").strip()]

    source_resolved = client.resolve_sheet(spreadsheet_id, source_sheet_name)
    target_resolved = client.resolve_sheet(spreadsheet_id, target_sheet_name)

    source_matrix = client.get_values(spreadsheet_id, f"'{source_resolved['title']}'!A1:{max_col}60")
    target_matrix = client.get_values(spreadsheet_id, f"'{target_resolved['title']}'!A1:{max_col}60")
    source_header_row = detect_header_row(source_matrix, start_row=1, min_nonempty=max(1, int(source_min_nonempty)))
    target_header_row = detect_header_row(target_matrix, start_row=1, min_nonempty=max(1, int(target_min_nonempty)))
    source_headers = [clean_text(item) for item in (source_matrix[source_header_row - 1] if source_matrix else [])]
    target_headers = [clean_text(item) for item in (target_matrix[target_header_row - 1] if target_matrix else [])]
    source_mapping = map_headers(source_headers, source_aliases)
    target_mapping = map_headers(target_headers, target_aliases)

    out: dict[str, Any] = {
        "workbook_name": workbook_name,
        "spreadsheet_id": spreadsheet_id,
        "sheet_titles": sheet_titles,
        "source_sheet": {
            "title": source_resolved.get("title", source_sheet_name),
            "sheet_id": source_resolved.get("sheetId"),
            "header_row_number": source_header_row,
        },
        "target_sheet": {
            "title": target_resolved.get("title", target_sheet_name),
            "sheet_id": target_resolved.get("sheetId"),
            "header_row_number": target_header_row,
        },
        "source_headers": source_headers,
        "target_headers": target_headers,
        "source_mapped_columns": {field: source_headers[idx] for field, idx in source_mapping.mapped.items() if idx < len(source_headers)},
        "target_mapped_columns": {field: target_headers[idx] for field, idx in target_mapping.mapped.items() if idx < len(target_headers)},
        "source_unmapped_columns": source_mapping.unmapped_columns,
        "target_unmapped_columns": target_mapping.unmapped_columns,
        "source_preview": [
            {"row_number": source_header_row + i + 1, "values": row}
            for i, row in enumerate(source_matrix[source_header_row : source_header_row + 20])
            if any(clean_text(cell) for cell in row)
        ],
        "target_preview": [
            {"row_number": target_header_row + i + 1, "values": row}
            for i, row in enumerate(target_matrix[target_header_row : target_header_row + 20])
            if any(clean_text(cell) for cell in row)
        ],
    }

    if extra_sheet_name.strip():
        try:
            extra_resolved = client.resolve_sheet(spreadsheet_id, extra_sheet_name)
            extra_matrix = client.get_values(spreadsheet_id, f"'{extra_resolved['title']}'!A1:{max_col}60")
            extra_header_row = detect_header_row(
                extra_matrix, start_row=1, min_nonempty=max(1, int(extra_min_nonempty or 3))
            )
            extra_headers = [clean_text(item) for item in (extra_matrix[extra_header_row - 1] if extra_matrix else [])]
            extra_mapping = map_headers(extra_headers, extra_aliases or {})
            out["extra_sheet"] = {
                "title": extra_resolved.get("title", extra_sheet_name),
                "sheet_id": extra_resolved.get("sheetId"),
                "header_row_number": extra_header_row,
            }
            out["extra_headers"] = extra_headers
            out["extra_mapped_columns"] = {
                field: extra_headers[idx] for field, idx in extra_mapping.mapped.items() if idx < len(extra_headers)
            }
            out["extra_unmapped_columns"] = extra_mapping.unmapped_columns
            out["extra_preview"] = [
                {"row_number": extra_header_row + i + 1, "values": row}
                for i, row in enumerate(extra_matrix[extra_header_row : extra_header_row + 20])
                if any(clean_text(cell) for cell in row)
            ]
        except Exception as exc:
            out["extra_sheet_error"] = str(exc)
    return out
