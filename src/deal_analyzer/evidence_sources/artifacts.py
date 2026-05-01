from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_evidence_artifacts(
    *,
    run_dir: Path,
    presentation_discovery_debug: dict[str, Any],
    presentation_links_found: list[dict[str, Any]],
    presentation_transcription_status: list[dict[str, Any]],
    evidence_items: list[dict[str, Any]],
) -> dict[str, str]:
    out: dict[str, str] = {}
    run_dir.mkdir(parents=True, exist_ok=True)

    debug_path = run_dir / "presentation_discovery_debug.json"
    links_path = run_dir / "presentation_links_found.json"
    tx_path = run_dir / "presentation_transcription_status.json"
    evidence_path = run_dir / "evidence_items.json"

    _write_json(debug_path, presentation_discovery_debug)
    _write_json(links_path, presentation_links_found)
    _write_json(tx_path, presentation_transcription_status)
    _write_json(evidence_path, evidence_items)

    out["presentation_discovery_debug_json"] = str(debug_path)
    out["presentation_links_found_json"] = str(links_path)
    out["presentation_transcription_status_json"] = str(tx_path)
    out["evidence_items_json"] = str(evidence_path)
    return out


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

