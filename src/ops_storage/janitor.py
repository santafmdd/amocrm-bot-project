from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.safety import ensure_inside_root

from .config import JanitorConfig
from .reporting import build_report, render_markdown
from .retention import RetentionPlan, build_retention_plan


@dataclass(frozen=True)
class JanitorRunResult:
    mode: str
    deleted_files: int
    deleted_bytes: int
    report_json: Path
    report_md: Path
    report_payload: dict[str, Any]


def run_janitor_report(*, config: JanitorConfig, logger) -> JanitorRunResult:
    plans = _build_plans(config=config)
    report = build_report(plans=plans)
    json_path, md_path = _write_report_files(config=config, report=report, mode='report')
    logger.info('janitor report generated: json=%s md=%s reclaimable=%s', json_path, md_path, report.get('summary', {}).get('reclaimable_human', ''))
    return JanitorRunResult(mode='report', deleted_files=0, deleted_bytes=0, report_json=json_path, report_md=md_path, report_payload=report)


def run_janitor_clean(*, config: JanitorConfig, logger, apply: bool, dry_run_override: bool | None = None) -> JanitorRunResult:
    dry = config.dry_run_default if dry_run_override is None else bool(dry_run_override)
    if apply:
        dry = False

    plans = _build_plans(config=config)
    deleted_files = 0
    deleted_bytes = 0

    if not dry:
        for plan in plans:
            for item in plan.candidates:
                try:
                    size = item.size_bytes
                    item.path.unlink(missing_ok=True)
                    deleted_files += 1
                    deleted_bytes += size
                except Exception as exc:
                    logger.warning('janitor delete failed: path=%s error=%s', item.path, exc)

    report = build_report(plans=plans)
    report['clean_mode'] = 'apply' if not dry else 'dry_run'
    report['deleted_files'] = deleted_files
    report['deleted_bytes'] = deleted_bytes
    json_path, md_path = _write_report_files(config=config, report=report, mode='clean')

    logger.info(
        'janitor clean finished: mode=%s deleted_files=%s deleted_bytes=%s report=%s',
        report['clean_mode'],
        deleted_files,
        deleted_bytes,
        json_path,
    )

    return JanitorRunResult(
        mode=report['clean_mode'],
        deleted_files=deleted_files,
        deleted_bytes=deleted_bytes,
        report_json=json_path,
        report_md=md_path,
        report_payload=report,
    )


def _build_plans(*, config: JanitorConfig) -> list[RetentionPlan]:
    plans: list[RetentionPlan] = []
    for target in config.targets:
        _ensure_in_allowlist(path=target.path, allowlist=config.allowlist_roots)
        plans.append(build_retention_plan(target=target))
    return plans


def _ensure_in_allowlist(*, path: Path, allowlist: tuple[Path, ...]) -> None:
    resolved = path.resolve()
    for root in allowlist:
        try:
            resolved.relative_to(root.resolve())
            return
        except Exception:
            continue
    raise RuntimeError(f'janitor path is outside allowlist: {resolved}')


def _write_report_files(*, config: JanitorConfig, report: dict[str, Any], mode: str) -> tuple[Path, Path]:
    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    report_dir = config.report_dir
    report_dir.mkdir(parents=True, exist_ok=True)

    json_path = report_dir / f'janitor_{mode}_{ts}.json'
    md_path = report_dir / f'janitor_{mode}_{ts}.md'
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    md_path.write_text(render_markdown(report), encoding='utf-8')

    latest_json = report_dir / f'janitor_{mode}_latest.json'
    latest_md = report_dir / f'janitor_{mode}_latest.md'
    latest_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    latest_md.write_text(render_markdown(report), encoding='utf-8')
    return json_path, md_path
