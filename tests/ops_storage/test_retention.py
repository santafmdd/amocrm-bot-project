from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.ops_storage.config import JanitorConfig, JanitorTargetConfig
from src.ops_storage.janitor import run_janitor_clean, run_janitor_report
from src.ops_storage.retention import build_retention_plan


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


def _root() -> Path:
    root = Path(r"d:\AI_Automation\_tmp_ops_storage_tests")
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write(path: Path, text: str, *, days_ago: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    if days_ago > 0:
        ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).timestamp()
        import os
        os.utime(path, (ts, ts))


def test_keep_latest_behavior():
    root = _root()
    target_dir = root / "workspace" / "deal_analyzer"
    _write(target_dir / "analyze_period_20260418_010101.json", "1", days_ago=10)
    _write(target_dir / "analyze_period_latest.json", "latest", days_ago=10)

    target = JanitorTargetConfig(
        name="deal",
        path=target_dir,
        category="exports",
        retention_days=1,
        keep_last_per_family=0,
        keep_latest=True,
        max_bytes=None,
    )
    plan = build_retention_plan(target=target)
    cand = {x.path.name for x in plan.candidates}
    assert "analyze_period_20260418_010101.json" in cand
    assert "analyze_period_latest.json" not in cand


def test_keep_last_n_per_family():
    root = _root()
    target_dir = root / "workspace" / "amocrm_collector"
    _write(target_dir / "collect_period_20260401_010101.json", "a", days_ago=20)
    _write(target_dir / "collect_period_20260402_010101.json", "b", days_ago=10)
    _write(target_dir / "collect_period_20260403_010101.json", "c", days_ago=5)

    target = JanitorTargetConfig(
        name="collector",
        path=target_dir,
        category="exports",
        retention_days=1,
        keep_last_per_family=1,
        keep_latest=False,
        max_bytes=None,
    )
    plan = build_retention_plan(target=target)
    cand = {x.path.name for x in plan.candidates}
    assert "collect_period_20260403_010101.json" not in cand
    assert "collect_period_20260401_010101.json" in cand


def test_older_than_days_cleanup_candidates():
    root = _root()
    target_dir = root / "workspace" / "deal_analyzer"
    _write(target_dir / "f1.json", "old", days_ago=40)
    _write(target_dir / "f2.json", "new", days_ago=1)

    target = JanitorTargetConfig(
        name="deal",
        path=target_dir,
        category="exports",
        retention_days=30,
        keep_last_per_family=0,
        keep_latest=False,
        max_bytes=None,
    )
    plan = build_retention_plan(target=target)
    cand = {x.path.name for x in plan.candidates}
    assert "f1.json" in cand
    assert "f2.json" not in cand


def test_max_size_trimming_deletes_oldest_first():
    root = _root()
    target_dir = root / "workspace" / "audio_cache"
    _write(target_dir / "a1.wav", "x" * 1000, days_ago=20)
    _write(target_dir / "a2.wav", "x" * 1000, days_ago=10)
    _write(target_dir / "a3.wav", "x" * 1000, days_ago=1)

    target = JanitorTargetConfig(
        name="audio",
        path=target_dir,
        category="audio_cache",
        retention_days=None,
        keep_last_per_family=0,
        keep_latest=False,
        max_bytes=1800,
    )
    plan = build_retention_plan(target=target)
    cand = [x.path.name for x in plan.candidates]
    assert "a1.wav" in cand


def test_allowlist_safety_blocks_outside_path():
    root = _root()
    outside = Path(r"d:\AI_Automation\outside_test_dir")
    outside.mkdir(parents=True, exist_ok=True)
    _write(outside / "x.log", "x", days_ago=100)

    cfg = JanitorConfig(
        enabled=True,
        dry_run_default=True,
        report_dir=root / "workspace" / "ops_storage",
        allowlist_roots=((root / "workspace").resolve(),),
        targets=(
            JanitorTargetConfig(
                name="bad",
                path=outside,
                category="tmp_dirs",
                retention_days=1,
                keep_last_per_family=0,
                keep_latest=False,
                max_bytes=None,
            ),
        ),
    )

    try:
        run_janitor_report(config=cfg, logger=_Logger())
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert "outside allowlist" in str(exc)


def test_dry_run_no_delete_guarantee():
    root = _root()
    target_dir = root / "workspace" / "deal_analyzer"
    f = target_dir / "old.json"
    _write(f, "x", days_ago=100)

    cfg = JanitorConfig(
        enabled=True,
        dry_run_default=True,
        report_dir=root / "workspace" / "ops_storage",
        allowlist_roots=((root / "workspace").resolve(),),
        targets=(
            JanitorTargetConfig(
                name="deal",
                path=target_dir,
                category="exports",
                retention_days=1,
                keep_last_per_family=0,
                keep_latest=False,
                max_bytes=None,
            ),
        ),
    )

    result = run_janitor_clean(config=cfg, logger=_Logger(), apply=False)
    assert result.mode == "dry_run"
    assert f.exists()



def test_screenshots_retention_keeps_newest_n():
    root = _root()
    target_dir = root / "workspace" / "screenshots"
    _write(target_dir / "shot_20260101_010101.png", "1", days_ago=20)
    _write(target_dir / "shot_20260102_010101.png", "2", days_ago=10)
    _write(target_dir / "shot_20260103_010101.png", "3", days_ago=5)

    target = JanitorTargetConfig(
        name="screenshots",
        path=target_dir,
        category="screenshots",
        retention_days=1,
        keep_last_per_family=1,
        keep_latest=False,
        max_bytes=None,
    )
    plan = build_retention_plan(target=target)
    cand = {x.path.name for x in plan.candidates}
    assert "shot_20260103_010101.png" not in cand
    assert "shot_20260101_010101.png" in cand


def test_tmp_dir_cleanup_apply_removes_old_files():
    root = _root()
    tmp_dir = root / "workspace" / "tmp_tests"
    old_file = tmp_dir / "old.tmp"
    _write(old_file, "x", days_ago=10)

    cfg = JanitorConfig(
        enabled=True,
        dry_run_default=True,
        report_dir=root / "workspace" / "ops_storage",
        allowlist_roots=((root / "workspace").resolve(),),
        targets=(
            JanitorTargetConfig(
                name="tmp",
                path=tmp_dir,
                category="tmp_dirs",
                retention_days=1,
                keep_last_per_family=0,
                keep_latest=False,
                max_bytes=None,
            ),
        ),
    )

    result = run_janitor_clean(config=cfg, logger=_Logger(), apply=True)
    assert result.mode == "apply"
    assert old_file.exists() is False


def test_report_has_required_categories_keys():
    root = _root()
    screenshots = root / "workspace" / "screenshots"
    _write(screenshots / "a.png", "x", days_ago=10)
    logs = root / "workspace" / "logs"
    _write(logs / "l.log", "x", days_ago=10)

    cfg = JanitorConfig(
        enabled=True,
        dry_run_default=True,
        report_dir=root / "workspace" / "ops_storage",
        allowlist_roots=((root / "workspace").resolve(),),
        targets=(
            JanitorTargetConfig(
                name="screens",
                path=screenshots,
                category="screenshots",
                retention_days=1,
                keep_last_per_family=0,
                keep_latest=False,
                max_bytes=None,
            ),
            JanitorTargetConfig(
                name="logs",
                path=logs,
                category="logs",
                retention_days=1,
                keep_last_per_family=0,
                keep_latest=False,
                max_bytes=None,
            ),
        ),
    )

    result = run_janitor_report(config=cfg, logger=_Logger())
    cats = result.report_payload.get("grouped_by_category", {})
    assert "screenshots" in cats
    assert "logs" in cats

