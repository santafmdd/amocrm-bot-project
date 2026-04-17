from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.config import load_config
from src.safety import ensure_inside_root

PERIOD_MODES = {
    "smart_manager_default",
    "current_week_to_date",
    "previous_calendar_week",
    "previous_workweek",
    "custom_range",
}

PERIOD_LABEL_MODES = {"period_only", "period_and_as_of"}
EXECUTED_AT_VISIBILITY = {"internal_only", "public"}


@dataclass(frozen=True)
class DealAnalyzerConfig:
    config_path: Path
    output_dir: Path
    score_weights: dict[str, int]
    analyzer_backend: str
    ollama_base_url: str
    ollama_model: str
    ollama_timeout_seconds: int
    style_profile_name: str
    period_mode: str = "smart_manager_default"
    custom_date_from: str | None = None
    custom_date_to: str | None = None
    period_label_mode: str = "period_only"
    hide_executed_at_from_public_exports: bool = True
    executed_at_visibility: str = "internal_only"
    client_list_enrich_enabled: bool = False
    appointment_list_enrich_enabled: bool = False
    client_list_source_name: str = ""
    appointment_list_source_name: str = ""


@dataclass(frozen=True)
class ResolvedPeriod:
    requested_mode: str
    resolved_mode: str
    period_start: date
    period_end: date
    as_of_date: date

    def public_period_label(self, label_mode: str) -> str:
        base = f"{self.period_start.isoformat()}..{self.period_end.isoformat()}"
        if label_mode == "period_and_as_of":
            return f"{base} (as of {self.as_of_date.isoformat()})"
        return base


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

    analyzer_backend = str(raw.get("analyzer_backend", "rules")).strip().lower() or "rules"
    if analyzer_backend not in {"rules", "ollama"}:
        raise RuntimeError(
            f"Unsupported analyzer_backend={analyzer_backend!r}. Allowed values: 'rules', 'ollama'."
        )

    ollama_base_url = str(raw.get("ollama_base_url", "http://127.0.0.1:11434")).strip() or "http://127.0.0.1:11434"
    ollama_model = str(raw.get("ollama_model", "gemma4:e4b")).strip() or "gemma4:e4b"

    timeout_raw = raw.get("ollama_timeout_seconds", 60)
    try:
        ollama_timeout_seconds = max(1, int(timeout_raw))
    except (TypeError, ValueError):
        ollama_timeout_seconds = 60

    style_profile_name = str(raw.get("style_profile_name", "manager_ru_v1")).strip() or "manager_ru_v1"

    period_mode = str(raw.get("period_mode", "smart_manager_default")).strip().lower() or "smart_manager_default"
    if period_mode not in PERIOD_MODES:
        raise RuntimeError(
            f"Unsupported period_mode={period_mode!r}. Allowed values: {sorted(PERIOD_MODES)}"
        )

    custom_date_from = _opt_str(raw.get("custom_date_from"))
    custom_date_to = _opt_str(raw.get("custom_date_to"))

    period_label_mode = str(raw.get("period_label_mode", "period_only")).strip().lower() or "period_only"
    if period_label_mode not in PERIOD_LABEL_MODES:
        raise RuntimeError(
            f"Unsupported period_label_mode={period_label_mode!r}. Allowed values: {sorted(PERIOD_LABEL_MODES)}"
        )

    hide_executed_at_from_public_exports = bool(raw.get("hide_executed_at_from_public_exports", True))

    executed_at_visibility = str(raw.get("executed_at_visibility", "internal_only")).strip().lower() or "internal_only"
    if executed_at_visibility not in EXECUTED_AT_VISIBILITY:
        raise RuntimeError(
            "Unsupported executed_at_visibility="
            f"{executed_at_visibility!r}. Allowed values: {sorted(EXECUTED_AT_VISIBILITY)}"
        )

    client_list_enrich_enabled = bool(raw.get("client_list_enrich_enabled", False))
    appointment_list_enrich_enabled = bool(raw.get("appointment_list_enrich_enabled", False))
    client_list_source_name = str(raw.get("client_list_source_name", "")).strip()
    appointment_list_source_name = str(raw.get("appointment_list_source_name", "")).strip()

    return DealAnalyzerConfig(
        config_path=cfg_path,
        output_dir=output_dir,
        score_weights=default_weights,
        analyzer_backend=analyzer_backend,
        ollama_base_url=ollama_base_url,
        ollama_model=ollama_model,
        ollama_timeout_seconds=ollama_timeout_seconds,
        style_profile_name=style_profile_name,
        period_mode=period_mode,
        custom_date_from=custom_date_from,
        custom_date_to=custom_date_to,
        period_label_mode=period_label_mode,
        hide_executed_at_from_public_exports=hide_executed_at_from_public_exports,
        executed_at_visibility=executed_at_visibility,
        client_list_enrich_enabled=client_list_enrich_enabled,
        appointment_list_enrich_enabled=appointment_list_enrich_enabled,
        client_list_source_name=client_list_source_name,
        appointment_list_source_name=appointment_list_source_name,
    )


def resolve_period(
    *,
    config: DealAnalyzerConfig,
    requested_mode: str | None = None,
    cli_date_from: str | None = None,
    cli_date_to: str | None = None,
    today: date | None = None,
) -> ResolvedPeriod:
    as_of = today or datetime.now().date()
    mode = (requested_mode or config.period_mode or "smart_manager_default").strip().lower()
    if mode not in PERIOD_MODES:
        raise RuntimeError(f"Unsupported period mode override={mode!r}")

    resolved_mode = mode
    if mode == "smart_manager_default":
        # Saturday/Sunday -> current week to date, weekdays -> previous workweek.
        resolved_mode = "current_week_to_date" if as_of.weekday() >= 5 else "previous_workweek"

    if resolved_mode == "current_week_to_date":
        start = as_of - timedelta(days=as_of.weekday())
        end = as_of
        return ResolvedPeriod(mode, resolved_mode, start, end, as_of)

    if resolved_mode == "previous_calendar_week":
        current_monday = as_of - timedelta(days=as_of.weekday())
        start = current_monday - timedelta(days=7)
        end = start + timedelta(days=6)
        return ResolvedPeriod(mode, resolved_mode, start, end, as_of)

    if resolved_mode == "previous_workweek":
        current_monday = as_of - timedelta(days=as_of.weekday())
        start = current_monday - timedelta(days=7)
        end = start + timedelta(days=4)
        return ResolvedPeriod(mode, resolved_mode, start, end, as_of)

    if resolved_mode == "custom_range":
        start_raw = cli_date_from or config.custom_date_from
        end_raw = cli_date_to or config.custom_date_to
        if not start_raw or not end_raw:
            raise RuntimeError("custom_range requires both date_from and date_to")
        start = _parse_date(start_raw, "date_from")
        end = _parse_date(end_raw, "date_to")
        if end < start:
            raise RuntimeError("custom_range date_to must be >= date_from")
        return ResolvedPeriod(mode, resolved_mode, start, end, as_of)

    raise RuntimeError(f"Unsupported resolved period mode={resolved_mode!r}")


def _parse_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise RuntimeError(f"Invalid {field_name} format, expected YYYY-MM-DD: {value!r}") from exc


def _opt_str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None

