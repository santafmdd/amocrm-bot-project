from __future__ import annotations

import json
from pathlib import Path
from tempfile import mkdtemp

from fastapi.testclient import TestClient

from src.deal_analyzer.ui_foundation import CommandResult, create_app


def _new_tmp_root() -> Path:
    base = Path(r"d:\AI_Automation\amocrm_bot\project\workspace\tmp_tests\deal_analyzer")
    base.mkdir(parents=True, exist_ok=True)
    return Path(mkdtemp(prefix="ui_foundation_", dir=str(base)))


def _prepare_root() -> Path:
    root = _new_tmp_root()
    cfg_dir = root / "config"
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "test.json").write_text(
        json.dumps({"deal_analyzer_write_enabled": True}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return root


def _fake_executor_factory(root: Path, calls: list[list[str]]):
    counter = {"n": 0}

    def _executor(command: list[str], cwd: Path, timeout_seconds: int) -> CommandResult:
        assert cwd == root
        assert timeout_seconds >= 1
        calls.append(command)
        counter["n"] += 1
        run_dir = root / "workspace" / "fake_runs" / f"run_{counter['n']}"
        run_dir.mkdir(parents=True, exist_ok=True)
        summary = {
            "status": "completed",
            "rows_prepared": 5,
            "rows_written": 0,
            "rows_quarantined": 0,
            "block_reason": "dry_run_build_only",
            "write_allowed": False,
        }
        (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        (run_dir / "progress.json").write_text(json.dumps({"step_name": "done"}, ensure_ascii=False, indent=2), encoding="utf-8")
        return CommandResult(returncode=0, stdout=str(run_dir), stderr="")

    return _executor


def test_ui_health() -> None:
    root = _prepare_root()
    app = create_app(project_root=root, confirmation_token="TOKEN_X")
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["real_write_requires_token"] is True


def test_ui_create_dry_run_job() -> None:
    root = _prepare_root()
    calls: list[list[str]] = []
    app = create_app(project_root=root, confirmation_token="TOKEN_X", executor=_fake_executor_factory(root, calls))
    client = TestClient(app)
    response = client.post(
        "/jobs/week-plan",
        json={
            "config": "config/test.json",
            "operation": "build",
            "signal_start": "2026-04-06",
            "signal_end": "2026-04-10",
            "plan_week_start": "2026-04-13",
            "plan_week_end": "2026-04-17",
            "wait_for_completion": True,
            "real_write": False,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"blocked", "succeeded"}
    assert payload["rows_prepared"] == 5
    assert payload["run_dir"]
    assert calls, "executor should be called for dry-run build"
    command_text = payload["command"]
    assert "--dry-run" in command_text
    assert "--write" not in command_text


def test_ui_real_write_requires_confirmation() -> None:
    root = _prepare_root()
    calls: list[list[str]] = []
    app = create_app(project_root=root, confirmation_token="CONFIRM_OK", executor=_fake_executor_factory(root, calls))
    client = TestClient(app)
    response = client.post(
        "/jobs/week-plan",
        json={
            "config": "config/test.json",
            "operation": "build",
            "signal_start": "2026-04-06",
            "signal_end": "2026-04-10",
            "plan_week_start": "2026-04-13",
            "plan_week_end": "2026-04-17",
            "wait_for_completion": True,
            "real_write": True,
            "confirmation_token": "",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "blocked"
    assert payload["block_reason"] == "real_write_confirmation_required"
    assert not calls, "executor must not run when token is missing"


def test_ui_job_status_artifacts() -> None:
    root = _prepare_root()
    calls: list[list[str]] = []
    app = create_app(project_root=root, confirmation_token="TOKEN_X", executor=_fake_executor_factory(root, calls))
    client = TestClient(app)
    created = client.post(
        "/jobs/training-materials",
        json={
            "config": "config/test.json",
            "operation": "build",
            "week_start": "2026-04-06",
            "week_end": "2026-04-10",
            "wait_for_completion": True,
            "real_write": False,
            "allow_no_external_sources": True,
            "limit": 1,
        },
    )
    assert created.status_code == 200
    job = created.json()
    job_id = job["id"]
    fetched = client.get(f"/jobs/{job_id}")
    assert fetched.status_code == 200
    payload = fetched.json()
    job_dir = Path(payload["job_report_dir"])
    assert (job_dir / "job.json").exists()
    assert (job_dir / "command.txt").exists()
    assert (job_dir / "stdout.log").exists()
    assert payload["artifact_paths"]["summary_json"].endswith("summary.json")


def test_ui_does_not_run_structural_operations() -> None:
    root = _prepare_root()
    calls: list[list[str]] = []
    app = create_app(project_root=root, confirmation_token="TOKEN_X", executor=_fake_executor_factory(root, calls))
    client = TestClient(app)
    fake_run_dir = root / "workspace" / "week_plan" / "fake_run"
    fake_run_dir.mkdir(parents=True, exist_ok=True)
    response = client.post(
        "/jobs/week-plan",
        json={
            "config": "config/test.json",
            "operation": "write",
            "run_dir": str(fake_run_dir),
            "wait_for_completion": True,
            "real_write": False,
            "strict_preflight": True,
            "allow_partial_write": True,
            "quarantine_unrepaired": True,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    command = payload["command"]
    assert "--strict-preflight" in command
    assert "--dry-run" in command
    assert "--write" not in command
    assert "insertDimension" not in command
    assert "insert_rows" not in command
