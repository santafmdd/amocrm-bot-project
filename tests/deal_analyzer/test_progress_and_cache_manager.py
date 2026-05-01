from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace
from tempfile import mkdtemp

from src.deal_analyzer.cache_manager import build_cache_status, run_cleanup
from src.deal_analyzer.progress import ProgressReporter


def _cfg_for_cache(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        audio_cache_dir="workspace/deal_analyzer/audio_cache",
        transcription_cache_dir="workspace/deal_analyzer/transcripts_cache",
        cache_cleanup_enabled=True,
        cache_retention_days=14,
        cache_max_size_gb=20.0,
    )


def _new_tmp_root() -> Path:
    base = Path(r"d:\AI_Automation\amocrm_bot\project\workspace\tmp_tests\deal_analyzer")
    base.mkdir(parents=True, exist_ok=True)
    return Path(mkdtemp(prefix="progress_cache_", dir=str(base)))


def _touch_file(path: Path, *, size: int = 16, mtime_shift_seconds: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    if mtime_shift_seconds:
        ts = time.time() - int(mtime_shift_seconds)
        os.utime(path, (ts, ts))


def test_progress_reporter_eta_no_crash() -> None:
    tmp_root = _new_tmp_root()
    run_dir = tmp_root / "run_eta"
    reporter = ProgressReporter(process="training_materials", run_dir=run_dir, heartbeat_seconds=1)
    reporter.update(step_name="llm_attempt_started", current=0, total=10, current_item={"recipient": "Рустам", "stage": "llm"})
    reporter.update(step_name="llm_attempt_finished", current=1, total=10, current_item={"recipient": "Рустам", "stage": "llm"})
    payload = reporter.as_dict()
    assert payload["step_name"] == "llm_attempt_finished"
    assert int(payload["elapsed_seconds"]) >= 0
    assert payload["eta_seconds"] is None or int(payload["eta_seconds"]) >= 0
    reporter.finish(status="completed", step_name="build_completed")


def test_progress_artifact_written() -> None:
    tmp_root = _new_tmp_root()
    run_dir = tmp_root / "run_artifacts"
    reporter = ProgressReporter(process="call_review", run_dir=run_dir, heartbeat_seconds=1)
    reporter.update(step_name="transcription", current=3, total=8, current_item={"deal_id": "31228579", "call_id": "abc"})
    reporter.heartbeat(details={"note": "alive"})
    reporter.finish(status="completed", step_name="run_completed")
    assert (run_dir / "progress.json").exists()
    assert (run_dir / "progress.log").exists()
    assert (run_dir / "heartbeat.json").exists()
    text = (run_dir / "progress.log").read_text(encoding="utf-8")
    assert "[call_review]" in text


def test_cache_status_counts_files() -> None:
    project_root = _new_tmp_root()
    cfg = _cfg_for_cache(project_root)
    _touch_file(project_root / "workspace" / "deal_analyzer" / "audio_cache" / "a.wav", size=32)
    _touch_file(project_root / "workspace" / "deal_analyzer" / "transcripts_cache" / "a.json", size=24)
    _touch_file(project_root / "workspace" / "cache" / "presentations" / "p.mp4", size=40)
    status = build_cache_status(cfg=cfg, project_root=project_root)
    assert int(status["total_files"]) >= 3
    assert float(status["total_size_gb"]) >= 0.0
    assert any(item.get("name") == "audio_cache" for item in status.get("buckets", []))


def test_cache_cleanup_dry_run_does_not_delete() -> None:
    project_root = _new_tmp_root()
    cfg = _cfg_for_cache(project_root)
    target = project_root / "workspace" / "deal_analyzer" / "audio_cache" / "old.wav"
    _touch_file(target, size=16, mtime_shift_seconds=86400 * 30)
    result = run_cleanup(
        cfg=cfg,
        project_root=project_root,
        older_than_days=14,
        max_size_gb=20.0,
        delete=False,
    )
    assert target.exists()
    assert int(result["candidates_count"]) >= 1
    assert int(result["deleted_count"]) == 0


def test_cache_cleanup_delete_only_allowed_dirs() -> None:
    project_root = _new_tmp_root()
    cfg = _cfg_for_cache(project_root)
    allowed = project_root / "workspace" / "deal_analyzer" / "audio_cache" / "delete_me.wav"
    forbidden = project_root / "workspace" / "week_plan" / "must_stay.json"
    _touch_file(allowed, size=16, mtime_shift_seconds=86400 * 2)
    _touch_file(forbidden, size=16, mtime_shift_seconds=86400 * 2)
    result = run_cleanup(
        cfg=cfg,
        project_root=project_root,
        older_than_days=0,
        max_size_gb=20.0,
        delete=True,
    )
    assert not allowed.exists()
    assert forbidden.exists()
    assert int(result["deleted_count"]) >= 1
