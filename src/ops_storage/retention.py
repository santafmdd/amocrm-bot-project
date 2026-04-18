from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .config import JanitorTargetConfig


_TIMESTAMP_SUFFIX = re.compile(r"_(?:\d{8}_\d{6}|\d{4}-\d{2}-\d{2}(?:_\d{2}-\d{2}-\d{2})?)$")


@dataclass(frozen=True)
class ArtifactInfo:
    path: Path
    size_bytes: int
    modified_at: datetime
    family: str
    is_latest: bool
    extension: str


@dataclass(frozen=True)
class RetentionPlan:
    target_name: str
    target_path: Path
    category: str
    total_files: int
    total_size_bytes: int
    candidates: list[ArtifactInfo]
    protected: list[ArtifactInfo]
    reclaimable_bytes: int


def build_retention_plan(*, target: JanitorTargetConfig, now: datetime | None = None) -> RetentionPlan:
    current = now or datetime.now(timezone.utc)
    files = _collect_files(target.path)

    protected_paths: set[Path] = set()
    by_family: dict[str, list[ArtifactInfo]] = {}
    for item in files:
        by_family.setdefault(item.family, []).append(item)

    # keep *_latest.*
    if target.keep_latest:
        for item in files:
            if item.is_latest:
                protected_paths.add(item.path)

    # keep last N per family
    keep_last = max(0, int(target.keep_last_per_family or 0))
    if keep_last > 0:
        for fam_items in by_family.values():
            for item in sorted(fam_items, key=lambda x: x.modified_at, reverse=True)[:keep_last]:
                protected_paths.add(item.path)

    stale_candidates: set[Path] = set()
    if target.retention_days is not None and target.retention_days >= 0:
        for item in files:
            age_days = (current - item.modified_at).total_seconds() / 86400.0
            if age_days > float(target.retention_days):
                stale_candidates.add(item.path)

    candidates: set[Path] = {p for p in stale_candidates if p not in protected_paths}

    # enforce max size by deleting oldest non-protected first
    total_size = sum(x.size_bytes for x in files)
    if target.max_bytes is not None and total_size > target.max_bytes:
        need = total_size - target.max_bytes
        reclaim = sum(x.size_bytes for x in files if x.path in candidates)
        if reclaim < need:
            extra_pool = [x for x in files if x.path not in protected_paths and x.path not in candidates]
            for item in sorted(extra_pool, key=lambda x: x.modified_at):
                candidates.add(item.path)
                reclaim += item.size_bytes
                if reclaim >= need:
                    break

    cand_infos = [x for x in files if x.path in candidates]
    prot_infos = [x for x in files if x.path in protected_paths]
    reclaimable = sum(x.size_bytes for x in cand_infos)

    return RetentionPlan(
        target_name=target.name,
        target_path=target.path,
        category=target.category,
        total_files=len(files),
        total_size_bytes=total_size,
        candidates=sorted(cand_infos, key=lambda x: x.modified_at),
        protected=sorted(prot_infos, key=lambda x: x.modified_at),
        reclaimable_bytes=reclaimable,
    )


def _collect_files(root: Path) -> list[ArtifactInfo]:
    if not root.exists() or not root.is_dir():
        return []
    out: list[ArtifactInfo] = []
    for path in root.rglob('*'):
        if not path.is_file():
            continue
        stat = path.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        name = path.stem
        family = _derive_family(name)
        out.append(
            ArtifactInfo(
                path=path,
                size_bytes=int(stat.st_size),
                modified_at=mtime,
                family=family,
                is_latest=name.endswith('_latest'),
                extension=path.suffix.lower(),
            )
        )
    return out


def _derive_family(stem: str) -> str:
    base = stem
    if base.endswith('_latest'):
        base = base[:-7]
    base = _TIMESTAMP_SUFFIX.sub('', base)
    return base or stem


def bytes_to_human(size: int) -> str:
    value = float(max(0, size))
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if value < 1024 or unit == 'TB':
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"
