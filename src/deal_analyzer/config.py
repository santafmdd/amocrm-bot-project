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
    client_list_source_url: str = ""
    appointment_list_source_url: str = ""
    client_list_sheet_name: str = ""
    appointment_list_sheet_name: str = ""
    matching_strategy: str = "priority_v1"
    fields_mapping: dict[str, dict[str, str]] | None = None
    operator_outputs_enabled: bool = True
    roks_source_url: str = ""
    roks_sheet_name: str = ""
    roks_sheet_candidates: tuple[str, ...] = ()
    transcription_backend: str = "disabled"
    transcription_base_url: str = ""
    transcription_model: str = ""
    transcription_timeout_seconds: int = 60
    transcription_cache_dir: str = "workspace/deal_analyzer/transcripts_cache"
    call_collection_mode: str = "disabled"
    call_backend: str = "amocrm_api"
    amocrm_auth_config_path: str = ""
    call_base_domain: str = ""
    janitor_enabled: bool = False
    janitor_dry_run_default: bool = True
    retention_days_exports: int = 30
    retention_days_audio_cache: int = 14
    retention_days_transcripts: int = 30
    keep_last_exports_per_family: int = 5
    max_audio_cache_gb: float = 2.0
    max_logs_mb: float = 300.0
    logs_dir: str = "logs"
    audio_cache_dir: str = "workspace/deal_analyzer/audio_cache"
    janitor_report_dir: str = "workspace/ops_storage"
    retention_days_screenshots: int = 14
    keep_last_screenshots: int = 200
    retention_days_tmp_dirs: int = 3
    screenshot_dir: str = "workspace/screenshots"
    tmp_dirs: tuple[str, ...] = ("workspace/tmp", "workspace/tmp_tests", "pytest-tmp", "pytest_tmp_env")


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

    client_list_source_url = str(raw.get("client_list_source_url", "")).strip()
    appointment_list_source_url = str(raw.get("appointment_list_source_url", "")).strip()
    client_list_sheet_name = str(raw.get("client_list_sheet_name", "")).strip()
    appointment_list_sheet_name = str(raw.get("appointment_list_sheet_name", "")).strip()

    matching_strategy = str(raw.get("matching_strategy", "priority_v1")).strip().lower() or "priority_v1"
    if matching_strategy not in {"priority_v1"}:
        raise RuntimeError("Unsupported matching_strategy. Allowed values: ['priority_v1']")

    operator_outputs_enabled = bool(raw.get("operator_outputs_enabled", True))

    transcription_backend = str(raw.get("transcription_backend", "disabled")).strip().lower() or "disabled"
    if transcription_backend not in {"disabled", "mock", "local_placeholder", "cloud_placeholder"}:
        raise RuntimeError(
            "Unsupported transcription_backend="
            f"{transcription_backend!r}. Allowed values: ['disabled', 'mock', 'local_placeholder', 'cloud_placeholder']"
        )

    transcription_base_url = str(raw.get("transcription_base_url", "")).strip()
    transcription_model = str(raw.get("transcription_model", "")).strip()
    try:
        transcription_timeout_seconds = max(1, int(raw.get("transcription_timeout_seconds", 60)))
    except (TypeError, ValueError):
        transcription_timeout_seconds = 60
    transcription_cache_dir = str(
        raw.get("transcription_cache_dir", "workspace/deal_analyzer/transcripts_cache")
    ).strip() or "workspace/deal_analyzer/transcripts_cache"

    call_collection_mode = str(raw.get("call_collection_mode", "disabled")).strip().lower() or "disabled"
    if call_collection_mode not in {"disabled", "api_first", "api_only", "raw_fallback", "raw_only"}:
        raise RuntimeError(
            "Unsupported call_collection_mode="
            f"{call_collection_mode!r}. Allowed values: ['disabled', 'api_first', 'api_only', 'raw_fallback', 'raw_only']"
        )

    call_backend = str(raw.get("call_backend", "amocrm_api")).strip().lower() or "amocrm_api"
    amocrm_auth_config_path = str(raw.get("amocrm_auth_config_path", "")).strip()
    call_base_domain = str(raw.get("call_base_domain", "")).strip()

    janitor_enabled = bool(raw.get("janitor_enabled", False))
    janitor_dry_run_default = bool(raw.get("janitor_dry_run_default", True))
    retention_days_exports = _parse_non_negative_int(raw.get("retention_days_exports", 30), field="retention_days_exports")
    retention_days_audio_cache = _parse_non_negative_int(raw.get("retention_days_audio_cache", 14), field="retention_days_audio_cache")
    retention_days_transcripts = _parse_non_negative_int(raw.get("retention_days_transcripts", 30), field="retention_days_transcripts")
    keep_last_exports_per_family = _parse_non_negative_int(raw.get("keep_last_exports_per_family", 5), field="keep_last_exports_per_family")
    max_audio_cache_gb = _parse_non_negative_float(raw.get("max_audio_cache_gb", 2.0), field="max_audio_cache_gb")
    max_logs_mb = _parse_non_negative_float(raw.get("max_logs_mb", 300.0), field="max_logs_mb")
    logs_dir = str(raw.get("logs_dir", "logs")).strip() or "logs"
    audio_cache_dir = str(raw.get("audio_cache_dir", "workspace/deal_analyzer/audio_cache")).strip() or "workspace/deal_analyzer/audio_cache"
    janitor_report_dir = str(raw.get("janitor_report_dir", "workspace/ops_storage")).strip() or "workspace/ops_storage"
    retention_days_screenshots = _parse_non_negative_int(raw.get("retention_days_screenshots", 14), field="retention_days_screenshots")
    keep_last_screenshots = _parse_non_negative_int(raw.get("keep_last_screenshots", 200), field="keep_last_screenshots")
    retention_days_tmp_dirs = _parse_non_negative_int(raw.get("retention_days_tmp_dirs", 3), field="retention_days_tmp_dirs")
    screenshot_dir = str(raw.get("screenshot_dir", "workspace/screenshots")).strip() or "workspace/screenshots"
    tmp_dirs = tuple(_parse_str_list(raw.get("tmp_dirs", ["workspace/tmp", "workspace/tmp_tests", "pytest-tmp", "pytest_tmp_env"])))

    roks_source_url = str(raw.get("roks_source_url", "")).strip()
    roks_sheet_name = str(raw.get("roks_sheet_name", "")).strip()
    roks_sheet_candidates = _parse_str_list(raw.get("roks_sheet_candidates"))

    fields_mapping = _parse_fields_mapping(raw.get("fields_mapping"))

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
        client_list_source_url=client_list_source_url,
        appointment_list_source_url=appointment_list_source_url,
        client_list_sheet_name=client_list_sheet_name,
        appointment_list_sheet_name=appointment_list_sheet_name,
        matching_strategy=matching_strategy,
        fields_mapping=fields_mapping,
        operator_outputs_enabled=operator_outputs_enabled,
        roks_source_url=roks_source_url,
        roks_sheet_name=roks_sheet_name,
        roks_sheet_candidates=tuple(roks_sheet_candidates),
        transcription_backend=transcription_backend,
        transcription_base_url=transcription_base_url,
        transcription_model=transcription_model,
        transcription_timeout_seconds=transcription_timeout_seconds,
        transcription_cache_dir=transcription_cache_dir,
        call_collection_mode=call_collection_mode,
        call_backend=call_backend,
        amocrm_auth_config_path=amocrm_auth_config_path,
        call_base_domain=call_base_domain,
        janitor_enabled=janitor_enabled,
        janitor_dry_run_default=janitor_dry_run_default,
        retention_days_exports=retention_days_exports,
        retention_days_audio_cache=retention_days_audio_cache,
        retention_days_transcripts=retention_days_transcripts,
        keep_last_exports_per_family=keep_last_exports_per_family,
        max_audio_cache_gb=max_audio_cache_gb,
        max_logs_mb=max_logs_mb,
        logs_dir=logs_dir,
        audio_cache_dir=audio_cache_dir,
        janitor_report_dir=janitor_report_dir,
        retention_days_screenshots=retention_days_screenshots,
        keep_last_screenshots=keep_last_screenshots,
        retention_days_tmp_dirs=retention_days_tmp_dirs,
        screenshot_dir=screenshot_dir,
        tmp_dirs=tmp_dirs,
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


def _parse_fields_mapping(raw: Any) -> dict[str, dict[str, str]]:
    if not isinstance(raw, dict):
        return {"client_list": {}, "appointment_list": {}}

    parsed: dict[str, dict[str, str]] = {"client_list": {}, "appointment_list": {}}
    for source in ("client_list", "appointment_list"):
        node = raw.get(source)
        if not isinstance(node, dict):
            continue
        for k, v in node.items():
            key = str(k or "").strip()
            val = str(v or "").strip()
            if key:
                parsed[source][key] = val
    return parsed




def _parse_str_list(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    items: list[str] = []
    for value in raw:
        text = str(value or "").strip()
        if text:
            items.append(text)
    return items

def _parse_non_negative_int(value: Any, *, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Invalid {field}: expected integer, got {value!r}") from exc
    if parsed < 0:
        raise RuntimeError(f"Invalid {field}: must be >= 0")
    return parsed


def _parse_non_negative_float(value: Any, *, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"Invalid {field}: expected float, got {value!r}") from exc
    if parsed < 0:
        raise RuntimeError(f"Invalid {field}: must be >= 0")
    return parsed


def _parse_date(value: str, field_name: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise RuntimeError(f"Invalid {field_name} format, expected YYYY-MM-DD: {value!r}") from exc


def _opt_str(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
