from __future__ import annotations

from typing import Any

from ..weekly_shared.roks_oap import build_weekly_roks_oap_snapshot


def build_roks_oap_snapshot(
    *,
    client: Any,
    spreadsheet_id: str,
    week_start: str,
    week_end: str,
    manager_allowlist: tuple[str, ...],
) -> dict[str, Any]:
    return build_weekly_roks_oap_snapshot(
        client=client,
        spreadsheet_id=spreadsheet_id,
        week_start=week_start,
        week_end=week_end,
        manager_allowlist=manager_allowlist,
    )
