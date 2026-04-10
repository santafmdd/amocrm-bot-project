"""Compile top-block data from all/active/closed analytics JSON snapshots."""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from src.browser.models import AnalyticsSnapshot, StageCount, TabMode
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class CompiledTopBlockResult:
    """Result object for compiled top-block export."""

    output_csv_path: Path
    snapshot_paths: dict[TabMode, Path]
    rows_count: int


def _resolve_input_path(path_value: str, project_root: Path) -> Path:
    candidate = Path(path_value)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return ensure_inside_root(candidate, project_root)


def _load_snapshot(path: Path, project_root: Path) -> AnalyticsSnapshot:
    safe_path = ensure_inside_root(path, project_root)
    raw = safe_path.read_text(encoding="utf-8")
    payload = json.loads(raw)
    return AnalyticsSnapshot.model_validate(payload)


def _discover_latest_snapshots(exports_dir: Path, project_root: Path) -> dict[TabMode, Path]:
    """Find latest snapshot JSON for each tab mode in exports directory."""
    latest: dict[TabMode, tuple[datetime, Path]] = {}

    safe_exports_dir = ensure_inside_root(exports_dir, project_root)
    for path in safe_exports_dir.glob("analytics_*.json"):
        try:
            snapshot = _load_snapshot(path, project_root)
        except Exception:
            continue

        tab_mode = snapshot.tab_mode
        current = latest.get(tab_mode)
        read_at = snapshot.read_at
        if current is None or read_at > current[0]:
            latest[tab_mode] = (read_at, path)

    result: dict[TabMode, Path] = {}
    for tab_mode in ("all", "active", "closed"):
        item = latest.get(tab_mode)
        if item is not None:
            result[tab_mode] = item[1]
    return result


def _cards_to_map(cards: list[StageCount]) -> dict[str, int]:
    mapped: dict[str, int] = {}
    for card in cards:
        # Keep first value if duplicate names appear.
        mapped.setdefault(card.stage_name, card.count)
    return mapped


def _build_stage_order(
    all_cards: dict[str, int],
    active_cards: dict[str, int],
    closed_cards: dict[str, int],
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    for source in (all_cards, active_cards, closed_cards):
        for stage_name in source.keys():
            if stage_name in seen:
                continue
            seen.add(stage_name)
            ordered.append(stage_name)

    return ordered


def _build_compiled_rows(snapshots: dict[TabMode, AnalyticsSnapshot]) -> list[dict[str, int | str]]:
    all_cards = _cards_to_map(snapshots["all"].top_cards)
    active_cards = _cards_to_map(snapshots["active"].top_cards)
    closed_cards = _cards_to_map(snapshots["closed"].top_cards)

    stage_names = _build_stage_order(all_cards, active_cards, closed_cards)
    rows: list[dict[str, int | str]] = []

    for stage_name in stage_names:
        rows.append(
            {
                "stage_name": stage_name,
                "all_count": all_cards.get(stage_name, 0),
                "active_count": active_cards.get(stage_name, 0),
                "closed_count": closed_cards.get(stage_name, 0),
            }
        )

    return rows


def compile_top_block_csv(
    *,
    exports_dir: Path,
    project_root: Path,
    all_json: str | None = None,
    active_json: str | None = None,
    closed_json: str | None = None,
    logger: logging.Logger | None = None,
) -> CompiledTopBlockResult:
    """Build one compiled CSV from all/active/closed snapshots."""
    log = logger or logging.getLogger("project")

    explicit_paths: dict[TabMode, Path] = {}
    if all_json:
        explicit_paths["all"] = _resolve_input_path(all_json, project_root)
    if active_json:
        explicit_paths["active"] = _resolve_input_path(active_json, project_root)
    if closed_json:
        explicit_paths["closed"] = _resolve_input_path(closed_json, project_root)

    discovered = _discover_latest_snapshots(exports_dir, project_root)
    snapshot_paths: dict[TabMode, Path] = {}
    for tab_mode in ("all", "active", "closed"):
        if tab_mode in explicit_paths:
            snapshot_paths[tab_mode] = explicit_paths[tab_mode]
        elif tab_mode in discovered:
            snapshot_paths[tab_mode] = discovered[tab_mode]

    missing = [mode for mode in ("all", "active", "closed") if mode not in snapshot_paths]
    if missing:
        raise RuntimeError(
            "Could not find snapshot JSON for required tab modes: "
            f"{', '.join(missing)}. Provide paths via --all-json/--active-json/--closed-json "
            "or run manual all-tab collection first."
        )

    snapshots: dict[TabMode, AnalyticsSnapshot] = {}
    for tab_mode in ("all", "active", "closed"):
        path = snapshot_paths[tab_mode]
        snapshot = _load_snapshot(path, project_root)
        snapshots[tab_mode] = snapshot
        log.info("Loaded snapshot tab=%s path=%s top_cards=%s stages=%s", tab_mode, path, len(snapshot.top_cards), len(snapshot.stages))

    rows = _build_compiled_rows(snapshots)

    compiled_dir = ensure_inside_root(exports_dir / "compiled", project_root)
    compiled_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"top_block_compiled_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = ensure_inside_root(compiled_dir / file_name, project_root)

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["stage_name", "all_count", "active_count", "closed_count"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    log.info("Compiled top-block CSV created: %s (rows=%s)", output_path, len(rows))

    return CompiledTopBlockResult(
        output_csv_path=output_path,
        snapshot_paths=snapshot_paths,
        rows_count=len(rows),
    )
