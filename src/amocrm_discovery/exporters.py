from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import load_config
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class ExportPaths:
    timestamped: Path
    latest: Path | None


def discovery_output_dir() -> Path:
    app = load_config()
    target = ensure_inside_root(app.workspace_dir / "amocrm_discovery", app.project_root)
    target.mkdir(parents=True, exist_ok=True)
    return target


def write_export(
    *,
    output_dir: Path,
    name: str,
    payload: dict[str, Any] | list[dict[str, Any]] | list[Any],
    write_latest: bool = True,
) -> ExportPaths:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_name = _safe_name(name)

    timestamped = output_dir / f"{safe_name}_{ts}.json"
    timestamped.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    latest_path: Path | None = None
    if write_latest:
        latest_path = output_dir / f"{safe_name}_latest.json"
        latest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return ExportPaths(timestamped=timestamped, latest=latest_path)


def _safe_name(value: str) -> str:
    raw = str(value or "export").strip().lower()
    if not raw:
        return "export"
    chars: list[str] = []
    for ch in raw:
        if ch.isalnum() or ch in {"_", "-"}:
            chars.append(ch)
        else:
            chars.append("_")
    return "".join(chars).strip("_") or "export"
