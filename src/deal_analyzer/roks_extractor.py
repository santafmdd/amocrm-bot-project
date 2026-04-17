from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.config import load_config
from src.integrations.google_sheets_api_client import GoogleSheetsApiClient, extract_spreadsheet_id


@dataclass(frozen=True)
class RoksSnapshotResult:
    ok: bool
    scope: str
    manager: str
    source_url: str
    sheet_title: str
    generated_at: str
    employee_month_context: dict[str, Any]
    team_month_context: dict[str, Any]
    weekly_context: dict[str, Any]
    conversion_snapshot: dict[str, Any]
    forecast_residual: dict[str, Any]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "scope": self.scope,
            "manager": self.manager,
            "source_url": self.source_url,
            "sheet_title": self.sheet_title,
            "generated_at": self.generated_at,
            "employee_month_context": self.employee_month_context,
            "team_month_context": self.team_month_context,
            "weekly_context": self.weekly_context,
            "conversion_snapshot": self.conversion_snapshot,
            "forecast_residual": self.forecast_residual,
            "warnings": list(self.warnings),
        }


def extract_roks_snapshot(*, config, logger, manager: str | None = None, team: bool = False) -> RoksSnapshotResult:
    source_url = str(getattr(config, "roks_source_url", "") or "").strip()
    if not source_url:
        return _empty_result(scope="team" if team else "manager", manager=manager or "", source_url="", warning="roks source url is not configured")

    app_cfg = load_config()
    gs_client = GoogleSheetsApiClient(project_root=app_cfg.project_root, logger=logger)

    spreadsheet_id = extract_spreadsheet_id(source_url)
    titles = _resolve_sheet_titles(config=config, gs_client=gs_client, spreadsheet_id=spreadsheet_id, logger=logger)
    if not titles:
        return _empty_result(scope="team" if team else "manager", manager=manager or "", source_url=source_url, warning="roks tabs not resolved")

    best_sheet = titles[0]
    matrix = gs_client.get_values(spreadsheet_id, f"'{best_sheet}'!A:ZZ")
    rows = _sanitize_matrix(matrix)

    employee_month_context = _extract_scope_context(rows, manager_name=manager) if manager else {}
    team_month_context = _extract_team_context(rows)
    weekly_context = _extract_weekly_context(rows)
    conversion_snapshot = _extract_marked_metrics(rows, markers=("конвер", "conversion"))
    forecast_residual = _extract_marked_metrics(rows, markers=("прогноз", "остат", "forecast", "residual"))

    logger.info(
        "roks snapshot read-only extracted: scope=%s manager=%s sheet=%s rows=%s",
        "team" if team else "manager",
        manager or "",
        best_sheet,
        len(rows),
    )

    return RoksSnapshotResult(
        ok=True,
        scope="team" if team else "manager",
        manager=manager or "",
        source_url=source_url,
        sheet_title=best_sheet,
        generated_at=datetime.now(timezone.utc).isoformat(),
        employee_month_context=employee_month_context,
        team_month_context=team_month_context,
        weekly_context=weekly_context,
        conversion_snapshot=conversion_snapshot,
        forecast_residual=forecast_residual,
        warnings=[],
    )


def _resolve_sheet_titles(*, config, gs_client, spreadsheet_id: str, logger) -> list[str]:
    preferred = [str(getattr(config, "roks_sheet_name", "") or "").strip()]
    preferred.extend(list(getattr(config, "roks_sheet_candidates", []) or []))
    preferred = [x for x in preferred if x]

    all_sheets = gs_client.list_sheets(spreadsheet_id)
    all_titles = [str(x.get("title", "")).strip() for x in all_sheets if str(x.get("title", "")).strip()]

    resolved: list[str] = []
    for p in preferred:
        for t in all_titles:
            if t == p or t.lower() == p.lower():
                resolved.append(t)
                break

    if resolved:
        return resolved

    for t in all_titles:
        tl = t.lower()
        if any(marker in tl for marker in ("рокс", "roks", "2026", "план", "факт", "kpi")):
            resolved.append(t)

    if not resolved and all_titles:
        logger.warning("roks extractor fallback to first available tab: %s", all_titles[0])
        resolved.append(all_titles[0])

    return resolved


def _sanitize_matrix(matrix: list[list[str]]) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in matrix:
        sanitized: list[str] = []
        for cell in row:
            text = " ".join(str(cell or "").replace("\n", " ").split())
            if text.startswith("#"):
                sanitized.append("")
            elif text in {"NaN", "nan", "inf", "-inf"}:
                sanitized.append("")
            else:
                sanitized.append(text)
        rows.append(sanitized)
    return rows


def _extract_scope_context(rows: list[list[str]], manager_name: str) -> dict[str, Any]:
    manager_norm = _norm(manager_name)
    matched_rows: list[list[str]] = []
    for row in rows:
        if any(manager_norm and manager_norm in _norm(cell) for cell in row):
            matched_rows.append(row)
    return {
        "manager": manager_name,
        "matched_rows_count": len(matched_rows),
        "sample": matched_rows[:5],
    }


def _extract_team_context(rows: list[list[str]]) -> dict[str, Any]:
    markers = ("команда", "итог", "всего", "team", "total")
    matched: list[list[str]] = []
    for row in rows:
        text = " ".join(_norm(c) for c in row)
        if any(m in text for m in markers):
            matched.append(row)
    return {
        "matched_rows_count": len(matched),
        "sample": matched[:5],
    }


def _extract_weekly_context(rows: list[list[str]]) -> dict[str, Any]:
    markers = ("недел", "week")
    matched: list[list[str]] = []
    for row in rows:
        text = " ".join(_norm(c) for c in row)
        if any(m in text for m in markers):
            matched.append(row)
    return {
        "matched_rows_count": len(matched),
        "sample": matched[:8],
    }


def _extract_marked_metrics(rows: list[list[str]], markers: tuple[str, ...]) -> dict[str, Any]:
    items: list[dict[str, str]] = []
    for row in rows:
        if not row:
            continue
        key = row[0] if row else ""
        key_norm = _norm(key)
        if not key_norm:
            continue
        if any(marker in key_norm for marker in markers):
            value = ""
            for cell in row[1:]:
                if cell:
                    value = cell
                    break
            items.append({"metric": key, "value": value})
    return {
        "metrics_count": len(items),
        "items": items[:30],
    }


def _norm(value: Any) -> str:
    return " ".join(str(value or "").lower().replace("ё", "е").split())


def _empty_result(*, scope: str, manager: str, source_url: str, warning: str) -> RoksSnapshotResult:
    return RoksSnapshotResult(
        ok=False,
        scope=scope,
        manager=manager,
        source_url=source_url,
        sheet_title="",
        generated_at=datetime.now(timezone.utc).isoformat(),
        employee_month_context={},
        team_month_context={},
        weekly_context={},
        conversion_snapshot={},
        forecast_residual={},
        warnings=[warning],
    )
