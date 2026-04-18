from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class JanitorTargetConfig:
    name: str
    path: Path
    category: str
    retention_days: int | None
    keep_last_per_family: int
    keep_latest: bool
    max_bytes: int | None


@dataclass(frozen=True)
class JanitorConfig:
    enabled: bool
    dry_run_default: bool
    report_dir: Path
    allowlist_roots: tuple[Path, ...]
    targets: tuple[JanitorTargetConfig, ...]


def mb_to_bytes(value: float | int | None) -> int | None:
    if value is None:
        return None
    return max(0, int(float(value) * 1024 * 1024))


def gb_to_bytes(value: float | int | None) -> int | None:
    if value is None:
        return None
    return max(0, int(float(value) * 1024 * 1024 * 1024))


def build_janitor_config_from_analyzer(*, analyzer_config, app_config) -> JanitorConfig:
    project_root = app_config.project_root.resolve()

    logs_path = (project_root / str(getattr(analyzer_config, "logs_dir", "logs"))).resolve()
    audio_path = (project_root / str(getattr(analyzer_config, "audio_cache_dir", "workspace/deal_analyzer/audio_cache"))).resolve()
    transcript_path = (project_root / str(getattr(analyzer_config, "transcription_cache_dir", "workspace/deal_analyzer/transcripts_cache"))).resolve()
    report_path = (project_root / str(getattr(analyzer_config, "janitor_report_dir", "workspace/ops_storage"))).resolve()
    screenshot_path = (project_root / str(getattr(analyzer_config, "screenshot_dir", "workspace/screenshots"))).resolve()

    tmp_dirs_raw = getattr(analyzer_config, "tmp_dirs", ()) or ()
    tmp_paths = [
        (project_root / str(item)).resolve()
        for item in tmp_dirs_raw
        if str(item).strip()
    ]

    targets: list[JanitorTargetConfig] = [
        JanitorTargetConfig(
            name="deal_analyzer_exports",
            path=(project_root / "workspace" / "deal_analyzer").resolve(),
            category="exports",
            retention_days=int(getattr(analyzer_config, "retention_days_exports", 30)),
            keep_last_per_family=int(getattr(analyzer_config, "keep_last_exports_per_family", 5)),
            keep_latest=True,
            max_bytes=None,
        ),
        JanitorTargetConfig(
            name="amocrm_collector_exports",
            path=(project_root / "workspace" / "amocrm_collector").resolve(),
            category="exports",
            retention_days=int(getattr(analyzer_config, "retention_days_exports", 30)),
            keep_last_per_family=int(getattr(analyzer_config, "keep_last_exports_per_family", 5)),
            keep_latest=True,
            max_bytes=None,
        ),
        JanitorTargetConfig(
            name="screenshots",
            path=screenshot_path,
            category="screenshots",
            retention_days=int(getattr(analyzer_config, "retention_days_screenshots", 14)),
            keep_last_per_family=int(getattr(analyzer_config, "keep_last_screenshots", 200)),
            keep_latest=False,
            max_bytes=None,
        ),
        JanitorTargetConfig(
            name="transcript_cache",
            path=transcript_path,
            category="transcripts",
            retention_days=int(getattr(analyzer_config, "retention_days_transcripts", 30)),
            keep_last_per_family=1,
            keep_latest=False,
            max_bytes=None,
        ),
        JanitorTargetConfig(
            name="audio_cache",
            path=audio_path,
            category="audio_cache",
            retention_days=int(getattr(analyzer_config, "retention_days_audio_cache", 14)),
            keep_last_per_family=0,
            keep_latest=False,
            max_bytes=gb_to_bytes(getattr(analyzer_config, "max_audio_cache_gb", 2.0)),
        ),
        JanitorTargetConfig(
            name="logs",
            path=logs_path,
            category="logs",
            retention_days=int(getattr(analyzer_config, "retention_days_exports", 30)),
            keep_last_per_family=3,
            keep_latest=False,
            max_bytes=mb_to_bytes(getattr(analyzer_config, "max_logs_mb", 300.0)),
        ),
    ]

    for idx, tmp_path in enumerate(tmp_paths, start=1):
        targets.append(
            JanitorTargetConfig(
                name=f"tmp_{idx}",
                path=tmp_path,
                category="tmp_dirs",
                retention_days=int(getattr(analyzer_config, "retention_days_tmp_dirs", 3)),
                keep_last_per_family=0,
                keep_latest=False,
                max_bytes=None,
            )
        )

    allowlist_roots = tuple(dict.fromkeys([
        (project_root / "workspace").resolve(),
        logs_path,
        report_path,
        screenshot_path,
        *tmp_paths,
    ]))

    return JanitorConfig(
        enabled=bool(getattr(analyzer_config, "janitor_enabled", False)),
        dry_run_default=bool(getattr(analyzer_config, "janitor_dry_run_default", True)),
        report_dir=report_path,
        allowlist_roots=allowlist_roots,
        targets=tuple(targets),
    )
