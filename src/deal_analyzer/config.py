from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import load_config
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class DealAnalyzerConfig:
    config_path: Path
    output_dir: Path
    score_weights: dict[str, int]


def load_deal_analyzer_config(config_path: str | None = None) -> DealAnalyzerConfig:
    app = load_config()
    default_path = ensure_inside_root(app.project_root / "config" / "deal_analyzer.local.json", app.project_root)
    cfg_path = ensure_inside_root(Path(config_path).resolve() if config_path else default_path, app.project_root)

    raw: dict[str, Any] = {}
    if cfg_path.exists():
        payload = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"Invalid deal analyzer config format: {cfg_path}")
        raw = payload

    output_dir_raw = str(raw.get("output_dir", "workspace/deal_analyzer"))
    output_dir = ensure_inside_root((app.project_root / output_dir_raw).resolve(), app.project_root)

    default_weights: dict[str, int] = {
        "presentation": 20,
        "brief": 10,
        "demo_result": 10,
        "pain": 10,
        "business_tasks": 10,
        "followup_tasks": 10,
        "product_fit": 15,
        "probability": 5,
        "data_completeness": 10,
    }

    configured = raw.get("score_weights")
    if isinstance(configured, dict):
        for key, value in configured.items():
            try:
                default_weights[str(key)] = int(value)
            except (TypeError, ValueError):
                continue

    return DealAnalyzerConfig(
        config_path=cfg_path,
        output_dir=output_dir,
        score_weights=default_weights,
    )
