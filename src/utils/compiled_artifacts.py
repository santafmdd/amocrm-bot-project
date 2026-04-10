"""Helpers for resolving latest compiled artifacts in exports/compiled."""

from __future__ import annotations

from pathlib import Path


def find_latest_compiled_artifact(exports_dir: Path, pattern: str, report_id: str) -> Path | None:
    compiled_dir = exports_dir / "compiled"
    if not compiled_dir.exists():
        return None
    candidates = [p for p in compiled_dir.glob(pattern) if p.is_file() and report_id in p.name]
    if not candidates:
        candidates = [p for p in compiled_dir.glob(pattern) if p.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)
