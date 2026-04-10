"""Compile captured snapshots into one profile-level result."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from src.browser.models import AnalyticsSnapshot
from src.config_loader import ReportProfile
from src.safety import ensure_inside_root
from src.writers.models import CompiledProfileAnalyticsResult


def _normalize_key(text: str) -> str:
    value = (text or "").strip().lower().replace("ё", "е")
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"[\.,;:]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def compile_profile_analytics_result(
    report: ReportProfile,
    source_kind: str,
    filter_values: list[str],
    snapshots: list[AnalyticsSnapshot],
) -> CompiledProfileAnalyticsResult:
    """Build one compiled object from all tab snapshots."""
    top_cards_by_tab: dict[str, list[dict[str, object]]] = {}
    stages_by_tab: dict[str, list[dict[str, object]]] = {}
    totals_by_tab: dict[str, int] = {}
    tabs: list[str] = []

    for snapshot in snapshots:
        tab = snapshot.tab_mode
        tabs.append(tab)
        totals_by_tab[tab] = snapshot.total_count

        top_cards_rows: list[dict[str, object]] = []
        for idx, card in enumerate(snapshot.top_cards, start=1):
            top_cards_rows.append(
                {
                    "tab": tab,
                    "card_index": idx,
                    "label": card.stage_name,
                    "value": card.count,
                    "raw_value": str(card.count),
                }
            )
        top_cards_by_tab[tab] = top_cards_rows

        stage_rows: list[dict[str, object]] = []
        for idx, stage in enumerate(snapshot.stages, start=1):
            stage_rows.append(
                {
                    "tab": tab,
                    "stage_index": idx,
                    "stage_name": stage.stage_name,
                    "deals_count": stage.count,
                    "budget_text": "",
                    "raw_line": "",
                }
            )
        stages_by_tab[tab] = stage_rows

    return CompiledProfileAnalyticsResult(
        report_id=report.id,
        display_name=report.display_name,
        generated_at=datetime.now(),
        source_kind=source_kind,
        filter_values=filter_values,
        tabs=tabs,
        top_cards_by_tab=top_cards_by_tab,
        stages_by_tab=stages_by_tab,
        totals_by_tab=totals_by_tab,
    )


def compile_stage_pivot(
    compiled_result: CompiledProfileAnalyticsResult,
    stage_aliases: dict[str, list[str]] | None = None,
) -> dict[str, dict[str, int | str]]:
    """Build pivot structure: stage_name -> {all, active, closed}."""
    alias_index: dict[str, str] = {}
    if stage_aliases:
        for canonical, aliases in stage_aliases.items():
            canonical_norm = _normalize_key(canonical)
            if not canonical_norm:
                continue
            alias_index[canonical_norm] = canonical
            for alias in aliases:
                alias_norm = _normalize_key(alias)
                if alias_norm:
                    alias_index[alias_norm] = canonical

    pivot: dict[str, dict[str, int | str]] = {}
    for tab, rows in compiled_result.stages_by_tab.items():
        tab_key = tab if tab in {"all", "active", "closed"} else "all"
        for item in rows:
            stage_raw = str(item.get("stage_name", "")).strip()
            if not stage_raw:
                continue
            stage_norm = _normalize_key(stage_raw)
            canonical = alias_index.get(stage_norm, stage_raw)

            current = pivot.setdefault(
                canonical,
                {
                    "stage_name": canonical,
                    "all": 0,
                    "active": 0,
                    "closed": 0,
                },
            )
            value = int(item.get("deals_count", 0) or 0)
            current[tab_key] = value

    return pivot


def save_compiled_result_json(
    compiled_result: CompiledProfileAnalyticsResult,
    exports_dir: Path,
    project_root: Path,
) -> Path:
    """Save compiled result to exports/compiled/*.json."""
    compiled_dir = ensure_inside_root(exports_dir / "compiled", project_root)
    compiled_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"compiled_profile_{compiled_result.report_id}_{compiled_result.generated_at.strftime('%Y%m%d_%H%M%S')}.json"
    output_path = ensure_inside_root(compiled_dir / file_name, project_root)
    payload = compiled_result.to_dict()
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def save_stage_pivot_json(
    pivot: dict[str, dict[str, int | str]],
    report_id: str,
    exports_dir: Path,
    project_root: Path,
) -> Path:
    """Save pivot mapping for debug and verification."""
    compiled_dir = ensure_inside_root(exports_dir / "compiled", project_root)
    compiled_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"compiled_stage_pivot_{report_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_path = ensure_inside_root(compiled_dir / file_name, project_root)
    output_path.write_text(json.dumps(pivot, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path
