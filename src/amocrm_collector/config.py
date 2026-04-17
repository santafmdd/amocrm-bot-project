from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import load_config
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class PresentationLinkSearchConfig:
    scan_deal_custom_fields_url: bool
    scan_notes_common_text: bool
    scan_company_comment: bool
    scan_contact_comment: bool
    regexes: list[str]


@dataclass(frozen=True)
class PresentationDetectionConfig:
    min_call_duration_seconds: int
    require_any_of: list[str]


@dataclass(frozen=True)
class AmoCollectorConfig:
    config_path: Path
    auth_config_path: Path
    output_dir: Path
    base_domain: str

    manager_ids_include: list[int]
    manager_ids_exclude: list[int]
    manager_names_exclude: list[str]
    pipeline_ids_include: list[int]

    product_field_id: int | None
    source_field_id: int | None
    pain_field_id: int | None
    tasks_field_id: int | None
    brief_field_id: int | None
    demo_result_field_id: int | None
    test_result_field_id: int | None
    probability_field_id: int | None
    company_comment_field_id: int | None
    contact_comment_field_id: int | None

    presentation_link_search: PresentationLinkSearchConfig
    presentation_detection: PresentationDetectionConfig


FIELD_ID_ATTRS: tuple[str, ...] = (
    "product_field_id",
    "source_field_id",
    "pain_field_id",
    "tasks_field_id",
    "brief_field_id",
    "demo_result_field_id",
    "test_result_field_id",
    "probability_field_id",
    "company_comment_field_id",
    "contact_comment_field_id",
)


def load_collector_config(config_path: str | None = None) -> AmoCollectorConfig:
    app = load_config()
    default_path = ensure_inside_root(app.project_root / "config" / "amocrm_collector.local.json", app.project_root)
    cfg_path = ensure_inside_root(Path(config_path).resolve() if config_path else default_path, app.project_root)

    raw: dict[str, Any] = {}
    if cfg_path.exists():
        payload = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            raise RuntimeError(f"Invalid collector config format: {cfg_path}")
        raw = payload

    auth_path_raw = str(raw.get("auth_config_path", "config/amocrm_auth.local.json"))
    auth_path = ensure_inside_root((app.project_root / auth_path_raw).resolve(), app.project_root)

    output_dir_raw = str(raw.get("output_dir", "workspace/amocrm_collector"))
    output_dir = ensure_inside_root((app.project_root / output_dir_raw).resolve(), app.project_root)

    link_raw = raw.get("presentation_link_search", {})
    if not isinstance(link_raw, dict):
        link_raw = {}

    detect_raw = raw.get("presentation_detection", {})
    if not isinstance(detect_raw, dict):
        detect_raw = {}

    manager_ids_exclude = _as_int_list_strict(raw.get("manager_ids_exclude"), field_name="manager_ids_exclude")

    return AmoCollectorConfig(
        config_path=cfg_path,
        auth_config_path=auth_path,
        output_dir=output_dir,
        base_domain=str(raw.get("base_domain", "") or "").strip(),
        manager_ids_include=_as_int_list(raw.get("manager_ids_include")),
        manager_ids_exclude=manager_ids_exclude,
        manager_names_exclude=_as_str_list(raw.get("manager_names_exclude")),
        pipeline_ids_include=_as_int_list(raw.get("pipeline_ids_include")),
        product_field_id=_as_opt_int(raw.get("product_field_id")),
        source_field_id=_as_opt_int(raw.get("source_field_id")),
        pain_field_id=_as_opt_int(raw.get("pain_field_id")),
        tasks_field_id=_as_opt_int(raw.get("tasks_field_id")),
        brief_field_id=_as_opt_int(raw.get("brief_field_id")),
        demo_result_field_id=_as_opt_int(raw.get("demo_result_field_id")),
        test_result_field_id=_as_opt_int(raw.get("test_result_field_id")),
        probability_field_id=_as_opt_int(raw.get("probability_field_id")),
        company_comment_field_id=_as_opt_int(raw.get("company_comment_field_id")),
        contact_comment_field_id=_as_opt_int(raw.get("contact_comment_field_id")),
        presentation_link_search=PresentationLinkSearchConfig(
            scan_deal_custom_fields_url=bool(
                link_raw.get("scan_deal_custom_fields_url", link_raw.get("scan_lead_custom_fields_url", True))
            ),
            scan_notes_common_text=bool(link_raw.get("scan_notes_common_text", True)),
            scan_company_comment=bool(link_raw.get("scan_company_comment", True)),
            scan_contact_comment=bool(link_raw.get("scan_contact_comment", True)),
            regexes=_as_str_list(link_raw.get("regexes"), default=["docs.google.com", "drive.google.com"]),
        ),
        presentation_detection=PresentationDetectionConfig(
            min_call_duration_seconds=max(0, int(detect_raw.get("min_call_duration_seconds", 900) or 900)),
            require_any_of=_as_str_list(
                detect_raw.get("require_any_of"),
                default=["demo_result_present", "brief_present", "completed_meeting_task", "long_call", "comment_link_present"],
            ),
        ),
    )


def build_collector_config_summary(cfg: AmoCollectorConfig) -> dict[str, Any]:
    field_ids: dict[str, int | None] = {name: getattr(cfg, name) for name in FIELD_ID_ATTRS}
    field_state = {
        "set": [name for name, value in field_ids.items() if isinstance(value, int) and value > 0],
        "zero": [name for name, value in field_ids.items() if value == 0],
        "unset": [name for name, value in field_ids.items() if value is None],
    }
    return {
        "config_path": str(cfg.config_path),
        "auth_config_path": str(cfg.auth_config_path),
        "output_dir": str(cfg.output_dir),
        "base_domain": cfg.base_domain,
        "manager_exclusions": {
            "ids": list(cfg.manager_ids_exclude),
            "names": list(cfg.manager_names_exclude),
        },
        "field_ids": field_ids,
        "field_id_state": field_state,
    }


def collect_collector_config_warnings(cfg: AmoCollectorConfig) -> list[str]:
    warnings: list[str] = []
    for name in FIELD_ID_ATTRS:
        value = getattr(cfg, name)
        if value == 0:
            warnings.append(
                f"collector config: {name}=0, exact field_id mapping is disabled; fallback by field_name will be used"
            )
    return warnings


def _as_opt_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return int(text)


def _as_int_list(value: Any) -> list[int]:
    if value is None or not isinstance(value, list):
        return []
    out: list[int] = []
    for item in value:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return out


def _as_int_list_strict(value: Any, *, field_name: str) -> list[int]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise RuntimeError(f"Invalid collector config: {field_name} must be a list of numeric ids")

    out: list[int] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        try:
            out.append(int(text))
        except (TypeError, ValueError):
            raise RuntimeError(
                f"Invalid collector config: {field_name} must contain only numeric ids, got '{item}'"
            )
    return out


def _as_str_list(value: Any, default: list[str] | None = None) -> list[str]:
    if value is None or not isinstance(value, list):
        return list(default or [])
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out or list(default or [])
