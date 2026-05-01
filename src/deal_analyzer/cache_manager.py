from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from .config import DealAnalyzerConfig, load_deal_analyzer_config


@dataclass(frozen=True)
class CacheBucket:
    name: str
    path: Path
    exists: bool
    files_count: int
    total_bytes: int
    oldest_mtime_utc: str
    newest_mtime_utc: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": str(self.path),
            "exists": self.exists,
            "files_count": self.files_count,
            "total_bytes": self.total_bytes,
            "oldest_mtime_utc": self.oldest_mtime_utc,
            "newest_mtime_utc": self.newest_mtime_utc,
        }


def _utc_iso(ts: float | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")


def _bytes_to_gb(value: int) -> float:
    return round(float(value) / (1024.0 * 1024.0 * 1024.0), 4)


def _resolve_cache_roots(cfg: DealAnalyzerConfig, project_root: Path) -> list[tuple[str, Path]]:
    raw: list[tuple[str, Path]] = [
        ("audio_cache", (project_root / str(cfg.audio_cache_dir or "workspace/deal_analyzer/audio_cache")).resolve()),
        (
            "transcripts_cache",
            (project_root / str(cfg.transcription_cache_dir or "workspace/deal_analyzer/transcripts_cache")).resolve(),
        ),
        ("presentations_cache", (project_root / "workspace" / "cache" / "presentations").resolve()),
        ("llm_cache", (project_root / "workspace" / "cache" / "llm").resolve()),
    ]
    uniq: list[tuple[str, Path]] = []
    seen: set[str] = set()
    for name, path in raw:
        key = str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append((name, path))
    return uniq


def _walk_files(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    out: list[dict[str, Any]] = []
    for base, _dirs, files in os.walk(root):
        base_path = Path(base)
        for filename in files:
            path = base_path / filename
            try:
                st = path.stat()
            except Exception:
                continue
            if not path.is_file():
                continue
            out.append(
                {
                    "path": path,
                    "size": int(st.st_size),
                    "mtime": float(st.st_mtime),
                }
            )
    return out


def _build_bucket(name: str, path: Path) -> CacheBucket:
    files = _walk_files(path)
    if not files:
        return CacheBucket(
            name=name,
            path=path,
            exists=path.exists(),
            files_count=0,
            total_bytes=0,
            oldest_mtime_utc="",
            newest_mtime_utc="",
        )
    mtimes = [float(item["mtime"]) for item in files]
    return CacheBucket(
        name=name,
        path=path,
        exists=True,
        files_count=len(files),
        total_bytes=sum(int(item["size"]) for item in files),
        oldest_mtime_utc=_utc_iso(min(mtimes)),
        newest_mtime_utc=_utc_iso(max(mtimes)),
    )


def build_cache_status(*, cfg: DealAnalyzerConfig, project_root: Path) -> dict[str, Any]:
    buckets = [_build_bucket(name, path) for name, path in _resolve_cache_roots(cfg, project_root)]
    total_bytes = sum(int(item.total_bytes) for item in buckets)
    total_files = sum(int(item.files_count) for item in buckets)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cache_cleanup_enabled": bool(getattr(cfg, "cache_cleanup_enabled", False)),
        "cache_retention_days": int(getattr(cfg, "cache_retention_days", 14) or 14),
        "cache_max_size_gb": float(getattr(cfg, "cache_max_size_gb", 20.0) or 20.0),
        "total_files": total_files,
        "total_bytes": total_bytes,
        "total_size_gb": _bytes_to_gb(total_bytes),
        "buckets": [item.to_dict() for item in buckets],
    }


def _is_path_allowed(path: Path, *, allowed_roots: list[Path]) -> bool:
    resolved = path.resolve()
    for root in allowed_roots:
        try:
            resolved.relative_to(root)
            return True
        except Exception:
            continue
    return False


def run_cleanup(
    *,
    cfg: DealAnalyzerConfig,
    project_root: Path,
    older_than_days: int,
    max_size_gb: float,
    delete: bool,
    logger: Any | None = None,
) -> dict[str, Any]:
    roots = [path for _name, path in _resolve_cache_roots(cfg, project_root)]
    allowed_roots = [item.resolve() for item in roots]
    all_files: list[dict[str, Any]] = []
    for root in roots:
        for item in _walk_files(root):
            all_files.append(item)
    total_bytes = sum(int(item["size"]) for item in all_files)
    now = datetime.now(timezone.utc)
    threshold = now - timedelta(days=max(0, int(older_than_days or 0)))
    threshold_ts = threshold.timestamp()

    by_age = [
        item
        for item in all_files
        if float(item.get("mtime", 0.0) or 0.0) < threshold_ts and _is_path_allowed(Path(item["path"]), allowed_roots=allowed_roots)
    ]
    by_age_paths = {str(Path(item["path"]).resolve()) for item in by_age}

    over_limit_extra: list[dict[str, Any]] = []
    max_bytes = int(max(0.0, float(max_size_gb or 0.0)) * 1024 * 1024 * 1024)
    projected_bytes = total_bytes - sum(int(item["size"]) for item in by_age)
    if max_bytes > 0 and projected_bytes > max_bytes:
        remaining = [
            item
            for item in sorted(all_files, key=lambda x: float(x.get("mtime", 0.0) or 0.0))
            if str(Path(item["path"]).resolve()) not in by_age_paths
            and _is_path_allowed(Path(item["path"]), allowed_roots=allowed_roots)
        ]
        bytes_to_free = projected_bytes - max_bytes
        freed = 0
        for item in remaining:
            over_limit_extra.append(item)
            freed += int(item["size"])
            if freed >= bytes_to_free:
                break

    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in [*by_age, *over_limit_extra]:
        path = Path(item["path"]).resolve()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(item)

    deleted_files: list[str] = []
    deleted_bytes = 0
    errors: list[str] = []
    for item in candidates:
        path = Path(item["path"]).resolve()
        if not _is_path_allowed(path, allowed_roots=allowed_roots):
            continue
        if not delete:
            continue
        try:
            size = int(item.get("size", 0) or 0)
            path.unlink(missing_ok=True)
            deleted_files.append(str(path))
            deleted_bytes += size
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    mode = "delete" if delete else "dry_run"
    out = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": mode,
        "older_than_days": int(older_than_days or 0),
        "max_size_gb": float(max_size_gb or 0.0),
        "allowed_roots": [str(x) for x in allowed_roots],
        "total_files": len(all_files),
        "total_bytes": total_bytes,
        "total_size_gb": _bytes_to_gb(total_bytes),
        "candidates_count": len(candidates),
        "candidates_bytes": sum(int(item.get("size", 0) or 0) for item in candidates),
        "candidates_size_gb": _bytes_to_gb(sum(int(item.get("size", 0) or 0) for item in candidates)),
        "deleted_count": len(deleted_files),
        "deleted_bytes": deleted_bytes,
        "deleted_size_gb": _bytes_to_gb(deleted_bytes),
        "errors_count": len(errors),
        "errors": errors[:50],
        "candidates_preview": [
            {
                "path": str(Path(item["path"]).resolve()),
                "size": int(item.get("size", 0) or 0),
                "mtime_utc": _utc_iso(float(item.get("mtime", 0.0) or 0.0)),
            }
            for item in candidates[:200]
        ],
    }
    if logger is not None:
        try:
            logger.info(
                "cache cleanup finished: mode=%s total_files=%s candidates=%s deleted=%s",
                mode,
                len(all_files),
                len(candidates),
                len(deleted_files),
            )
        except Exception:
            pass
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deal analyzer cache manager")
    sub = parser.add_subparsers(dest="command", required=True)

    status = sub.add_parser("status", help="Show cache usage status")
    status.add_argument("--config", required=True)

    cleanup = sub.add_parser("cleanup", help="Cleanup cache files (dry-run by default)")
    cleanup.add_argument("--config", required=True)
    cleanup.add_argument("--older-than-days", type=int, default=14)
    cleanup.add_argument("--max-size-gb", type=float, default=20.0)
    cleanup.add_argument("--dry-run", action="store_true")
    cleanup.add_argument("--delete", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")

    if args.command == "status":
        payload = build_cache_status(cfg=cfg, project_root=app_cfg.project_root)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if args.command == "cleanup":
        delete = bool(args.delete)
        if bool(args.dry_run) and delete:
            raise RuntimeError("Use either --dry-run or --delete, not both")
        payload = run_cleanup(
            cfg=cfg,
            project_root=app_cfg.project_root,
            older_than_days=int(args.older_than_days or 14),
            max_size_gb=float(args.max_size_gb or 20.0),
            delete=delete,
            logger=logger,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()

