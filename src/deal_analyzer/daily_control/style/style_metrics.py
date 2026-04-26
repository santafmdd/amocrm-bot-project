from __future__ import annotations

from typing import Any


def build_style_metrics(
    *,
    llm_enabled: bool,
    llm_rows_used: int,
    llm_rows_failed: int,
    llm_rows_by_model: dict[str, int],
    rejected_rewrites_count: int,
    cleanup_counts: dict[str, int],
) -> dict[str, Any]:
    return {
        "llm_enabled": bool(llm_enabled),
        "llm_rows_used": int(llm_rows_used or 0),
        "llm_rows_failed": int(llm_rows_failed or 0),
        "llm_rows_by_model": {str(k): int(v or 0) for k, v in (llm_rows_by_model or {}).items()},
        "rejected_rewrites_count": int(rejected_rewrites_count or 0),
        "cleanup_counts": {str(k): int(v or 0) for k, v in (cleanup_counts or {}).items()},
    }

