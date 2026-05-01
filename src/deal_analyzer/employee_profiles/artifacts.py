from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_employee_profile_artifacts(
    *,
    run_dir: Path,
    profile_context_rows: list[dict[str, Any]],
    behavior_marker_rows: list[dict[str, Any]],
) -> tuple[Path, Path]:
    context_path = run_dir / "employee_profile_context_debug.json"
    markers_path = run_dir / "employee_behavior_markers.json"
    context_payload = {
        "rows_total": len(profile_context_rows),
        "rows": profile_context_rows,
    }
    markers_payload = {
        "rows_total": len(behavior_marker_rows),
        "rows": behavior_marker_rows,
    }
    context_path.write_text(json.dumps(context_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markers_path.write_text(json.dumps(markers_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return context_path, markers_path

