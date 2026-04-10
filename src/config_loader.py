"""Config-driven profile loader for future report expansion."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from pathlib import Path
from typing import Any

import yaml

from src.config import AppConfig
from src.safety import ensure_inside_root


_LOGGER = logging.getLogger("project")


def _iter_strings(value: Any):
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _iter_strings(k)
            yield from _iter_strings(v)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_strings(item)


@dataclass(frozen=True)
class PageProfile:
    """One page profile definition from `config/page_profiles.yaml`."""

    id: str
    display_name: str
    description: str
    mode: str
    notes: list[str]


@dataclass(frozen=True)
class ReportProfile:
    """One report profile definition from `config/report_profiles.yaml`."""

    id: str
    display_name: str
    source: dict[str, Any]
    filters: dict[str, Any]
    tabs: list[str]
    compare_sources: list[dict[str, Any]]
    output: dict[str, Any]
    enabled: bool


@dataclass(frozen=True)
class TableMapping:
    """One table mapping definition from `config/table_mappings.yaml`."""

    id: str
    target_sheet_name: str
    target_block_id: str
    write_mode: str
    kind: str = "google_sheets_ui"
    tab_name: str = ""
    start_cell: str = "A1"
    sheet_url: str = ""
    layout: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ConfigProfiles:
    """Bundle with all loaded config-driven profiles."""

    page_profiles: dict[str, PageProfile]
    report_profiles: dict[str, ReportProfile]
    table_mappings: dict[str, TableMapping]


def _read_yaml_file(project_root: Path, relative_path: str) -> dict[str, Any]:
    """Safely read a YAML file from project-local `config/` directory."""
    file_path = ensure_inside_root(project_root / relative_path, project_root)
    if not file_path.exists():
        raise FileNotFoundError(f"Config file not found: {file_path}")

    text = ""
    used_encoding = "utf-8-sig"
    try:
        text = file_path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError:
        used_encoding = "cp1251_fallback"
        _LOGGER.warning(
            "Config encoding fallback used for %s: cp1251. Please save file as UTF-8 to avoid mojibake.",
            file_path,
        )
        text = file_path.read_text(encoding="cp1251")

    _LOGGER.info("Config loaded: path=%s encoding=%s", file_path, used_encoding)

    raw = yaml.safe_load(text)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"YAML root must be an object in: {file_path}")

    suspicious = [s for s in _iter_strings(raw) if "???" in s]
    if suspicious:
        _LOGGER.warning(
            "Config may contain mojibake/placeholders in %s. suspicious_entries=%s",
            file_path,
            suspicious[:10],
        )
    return raw


def load_page_profiles(config: AppConfig) -> dict[str, PageProfile]:
    """Load and index page profiles by id."""
    data = _read_yaml_file(config.project_root, "config/page_profiles.yaml")
    items = data.get("page_profiles", [])
    profiles: dict[str, PageProfile] = {}

    for item in items:
        profile = PageProfile(
            id=str(item["id"]),
            display_name=str(item.get("display_name", "")),
            description=str(item.get("description", "")),
            mode=str(item.get("mode", "manual_for_now")),
            notes=[str(note) for note in item.get("notes", [])],
        )
        profiles[profile.id] = profile

    return profiles


def load_report_profiles(config: AppConfig) -> dict[str, ReportProfile]:
    """Load and index report profiles by id."""
    data = _read_yaml_file(config.project_root, "config/report_profiles.yaml")
    items = data.get("report_profiles", [])
    profiles: dict[str, ReportProfile] = {}

    for item in items:
        profile = ReportProfile(
            id=str(item["id"]),
            display_name=str(item.get("display_name", "")),
            source=dict(item.get("source", {})),
            filters=dict(item.get("filters", {})),
            tabs=[str(tab) for tab in item.get("tabs", [])],
            compare_sources=[dict(source) for source in item.get("compare_sources", [])],
            output=dict(item.get("output", {})),
            enabled=bool(item.get("enabled", False)),
        )
        profiles[profile.id] = profile

    return profiles


def load_table_mappings(config: AppConfig) -> dict[str, TableMapping]:
    """Load and index table mappings by id."""
    data = _read_yaml_file(config.project_root, "config/table_mappings.yaml")
    items = data.get("table_mappings", [])
    mappings: dict[str, TableMapping] = {}

    for item in items:
        destination = item.get("destination", {})
        if not isinstance(destination, dict):
            destination = {}

        mapping_id = str(item.get("id", "")).strip().rstrip(":")
        if not mapping_id:
            continue

        mapping = TableMapping(
            id=mapping_id,
            target_sheet_name=str(item.get("target_sheet_name", destination.get("tab_name", ""))),
            target_block_id=str(item.get("target_block_id", destination.get("target_block_id", ""))),
            write_mode=str(item.get("write_mode", destination.get("write_mode", "replace_block"))),
            kind=str(item.get("kind", destination.get("kind", "google_sheets_ui"))),
            tab_name=str(item.get("tab_name", destination.get("tab_name", ""))),
            start_cell=str(item.get("start_cell", destination.get("start_cell", "A1"))),
            sheet_url=str(item.get("sheet_url", destination.get("sheet_url", ""))).strip(),
            layout=dict(item.get("layout", destination.get("layout", {})) or {}),
            notes=[str(note) for note in item.get("notes", [])],
        )
        mappings[mapping.id] = mapping

    return mappings


def load_all_profiles(config: AppConfig) -> ConfigProfiles:
    """Load all config-driven profile files in one call."""
    return ConfigProfiles(
        page_profiles=load_page_profiles(config),
        report_profiles=load_report_profiles(config),
        table_mappings=load_table_mappings(config),
    )
