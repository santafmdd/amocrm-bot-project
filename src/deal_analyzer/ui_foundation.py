from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_json_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


class JobStatus:
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"


class JobRecord(BaseModel):
    id: str
    type: str
    status: str
    command: str
    started_at: str = ""
    finished_at: str = ""
    progress: dict[str, Any] = Field(default_factory=dict)
    run_dir: str = ""
    rows_prepared: int | None = None
    rows_written: int | None = None
    rows_quarantined: int | None = None
    block_reason: str = ""
    error: str = ""
    job_report_dir: str = ""
    created_at: str = Field(default_factory=_utc_now_iso)
    artifact_paths: dict[str, str] = Field(default_factory=dict)


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class BaseJobRequest(BaseModel):
    config: str
    real_write: bool = False
    confirmation_token: str = ""
    wait_for_completion: bool = False
    timeout_seconds: int = 7200
    allow_structural_operations: bool = False


class CallReviewJobRequest(BaseJobRequest):
    input: str
    period_mode: Literal[
        "smart_manager_default",
        "current_week_to_date",
        "previous_calendar_week",
        "previous_workweek",
        "custom_range",
        "control_day_window",
    ] = "control_day_window"
    date_from: str = ""
    date_to: str = ""
    control_date: str = ""
    business_cutoff: str = "15:00"
    business_timezone: str = "Europe/Moscow"
    limit: int | None = None
    discussion_limit: int | None = None
    include_presentations: bool | None = None


class DailyControlJobRequest(BaseJobRequest):
    operation: Literal["build", "write"] = "build"
    period_start: str = ""
    period_end: str = ""
    run_dir: str = ""
    source_sheet: str = "Разбор звонков"
    daily_sheet: str = "Дневной контроль"
    main_model: str = ""
    fallback_model: str = ""
    limit: int | None = None
    strict_preflight: bool = True
    allow_partial_write: bool = True
    quarantine_unrepaired: bool = True


class WeekPlanJobRequest(BaseJobRequest):
    operation: Literal["build", "write"] = "build"
    signal_start: str = ""
    signal_end: str = ""
    plan_week_start: str = ""
    plan_week_end: str = ""
    period_start: str = ""
    period_end: str = ""
    run_dir: str = ""
    daily_sheet: str = "Дневной контроль"
    target_sheet: str = "План недели"
    manager_summary_sheet: str = "Недельный свод менеджеров"
    week_summary_sheet: str = "Свод недели"
    main_model: str = ""
    fallback_model: str = ""
    limit: int | None = None
    bootstrap_if_empty: bool = False
    strict_preflight: bool = True
    allow_partial_write: bool = True
    quarantine_unrepaired: bool = True


class TrainingMaterialsJobRequest(BaseJobRequest):
    operation: Literal["build", "write"] = "build"
    week_start: str = ""
    week_end: str = ""
    run_dir: str = ""
    plan_sheet: str = "План недели"
    daily_sheet: str = "Дневной контроль"
    call_review_sheet: str = "Разбор звонков"
    main_model: str = ""
    fallback_model: str = ""
    model_pool: str = ""
    limit: int | None = None
    allow_full_run: bool = False
    require_external_sources: bool = True
    allow_no_external_sources: bool = False
    strict_preflight: bool = True
    overwrite_links: bool = False


class WeeklyManagerSummaryJobRequest(BaseJobRequest):
    operation: Literal["build", "write"] = "build"
    period_start: str = ""
    period_end: str = ""
    run_dir: str = ""
    daily_sheet: str = "Дневной контроль"
    plan_sheet: str = "План недели"
    target_sheet: str = "Недельный свод менеджеров"
    main_model: str = ""
    fallback_model: str = ""
    limit: int | None = None
    strict_preflight: bool = True
    allow_partial_write: bool = True
    quarantine_unrepaired: bool = True


class WeekSummaryJobRequest(BaseJobRequest):
    operation: Literal["build", "write"] = "build"
    period_start: str = ""
    period_end: str = ""
    run_dir: str = ""
    daily_sheet: str = "Дневной контроль"
    plan_sheet: str = "План недели"
    manager_summary_sheet: str = "Недельный свод менеджеров"
    target_sheet: str = "Свод недели"
    main_model: str = ""
    fallback_model: str = ""
    limit: int | None = None
    strict_preflight: bool = True
    allow_partial_write: bool = True
    quarantine_unrepaired: bool = True


class CacheCleanupDryRunRequest(BaseJobRequest):
    older_than_days: int = 14
    max_size_gb: float = 20.0


class JobStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._jobs: dict[str, JobRecord] = {}
        self._load_existing()

    def _job_dir(self, job_id: str) -> Path:
        return self.root / job_id

    def _job_file(self, job_id: str) -> Path:
        return self._job_dir(job_id) / "job.json"

    def _load_existing(self) -> None:
        for path in sorted(self.root.glob("*/job.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                job = JobRecord(**payload)
                self._jobs[job.id] = job
            except Exception:
                continue

    def _persist(self, job: JobRecord) -> None:
        job_dir = self._job_dir(job.id)
        job_dir.mkdir(parents=True, exist_ok=True)
        self._job_file(job.id).write_text(
            json.dumps(job.model_dump(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def create(self, job: JobRecord) -> JobRecord:
        with self._lock:
            self._jobs[job.id] = job
            self._persist(job)
        return job

    def update(self, job_id: str, **changes: Any) -> JobRecord:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(job_id)
            current = self._jobs[job_id]
            payload = current.model_dump()
            payload.update(changes)
            updated = JobRecord(**payload)
            self._jobs[job_id] = updated
            self._persist(updated)
        return updated

    def get(self, job_id: str) -> JobRecord | None:
        return self._jobs.get(job_id)

    def list(self) -> list[JobRecord]:
        return sorted(self._jobs.values(), key=lambda item: item.created_at, reverse=True)


def _default_executor(command: list[str], *, cwd: Path, timeout_seconds: int) -> CommandResult:
    env = dict(os.environ)
    env.setdefault("GOOGLE_API_CREDENTIALS_FILE", str((cwd / "credentials.json").resolve()))
    env.setdefault("GOOGLE_API_TOKEN_FILE", str((cwd / "token.json").resolve()))
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(30, int(timeout_seconds or 7200)),
    )
    return CommandResult(
        returncode=int(completed.returncode),
        stdout=str(completed.stdout or ""),
        stderr=str(completed.stderr or ""),
    )


def _clone_safe_config(config_path: Path, target_path: Path) -> Path:
    payload = _safe_json_load(config_path)
    if not payload:
        shutil.copyfile(config_path, target_path)
        return target_path
    payload["deal_analyzer_write_enabled"] = False
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path


def _command_to_text(command: list[str]) -> str:
    return " ".join(f'"{part}"' if " " in part else part for part in command)


def _extract_run_dir_from_output(output: str) -> str:
    if not output:
        return ""
    candidates: list[str] = []
    for line in output.splitlines():
        text = str(line or "").strip().strip('"')
        if not text:
            continue
        if Path(text).exists() and Path(text).is_dir():
            candidates.append(text)
            continue
        for token in re.findall(r"[A-Za-z]:\\\\[^\r\n]+", text):
            probe = token.strip().strip('"')
            if Path(probe).exists() and Path(probe).is_dir():
                candidates.append(probe)
    return candidates[0] if candidates else ""


def _extract_run_dir_from_command(command: list[str]) -> str:
    for idx, token in enumerate(command):
        if token == "--run-dir" and idx + 1 < len(command):
            probe = command[idx + 1]
            if Path(probe).exists() and Path(probe).is_dir():
                return str(Path(probe).resolve())
    return ""


def _extract_metrics(run_dir: Path) -> tuple[dict[str, Any], dict[str, str]]:
    summary = _safe_json_load(run_dir / "summary.json")
    writer_status = _safe_json_load(run_dir / "training_materials_writer_status.json")
    if not writer_status:
        writer_status = _safe_json_load(run_dir / "week_plan_writer_status.json")
    if not writer_status:
        writer_status = _safe_json_load(run_dir / "weekly_manager_writer_status.json")
    if not writer_status:
        writer_status = _safe_json_load(run_dir / "week_summary_writer_status.json")
    if not writer_status:
        writer_status = _safe_json_load(run_dir / "daily_control_writer_status.json")
    progress = _safe_json_load(run_dir / "progress.json")
    status_from_summary = str(summary.get("status") or "").lower()
    block_reason = str(summary.get("block_reason") or writer_status.get("block_reason") or "")
    rows_prepared = _safe_int(summary.get("rows_prepared"))
    if rows_prepared is None:
        rows_prepared = _safe_int(writer_status.get("rows_prepared"))
    rows_written = _safe_int(summary.get("rows_written"))
    if rows_written is None:
        rows_written = _safe_int(writer_status.get("rows_written"))
    if rows_written is None:
        rows_written = _safe_int(summary.get("rows_links_written"))
    rows_quarantined = _safe_int(summary.get("rows_quarantined"))
    if rows_quarantined is None:
        rows_quarantined = _safe_int(writer_status.get("rows_quarantined"))
    metrics = {
        "progress": progress if isinstance(progress, dict) else {},
        "rows_prepared": rows_prepared,
        "rows_written": rows_written,
        "rows_quarantined": rows_quarantined,
        "block_reason": block_reason,
        "status_from_summary": status_from_summary,
        "write_allowed": summary.get("write_allowed", writer_status.get("write_allowed")),
    }
    artifacts = {
        "summary_json": str(run_dir / "summary.json"),
        "summary_md": str(run_dir / "summary.md"),
        "progress_json": str(run_dir / "progress.json"),
    }
    return metrics, artifacts


class JobRunner:
    def __init__(
        self,
        *,
        project_root: Path,
        job_store: JobStore,
        confirmation_token: str,
        executor: Callable[[list[str], Path, int], CommandResult] | None = None,
    ) -> None:
        self.project_root = project_root
        self.job_store = job_store
        self.confirmation_token = confirmation_token
        self._executor = executor or (lambda command, cwd, timeout: _default_executor(command, cwd=cwd, timeout_seconds=timeout))

    def submit(
        self,
        *,
        job_type: str,
        request_payload: BaseJobRequest,
        command_builder: Callable[[str], list[str]],
    ) -> JobRecord:
        job_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        job_dir = self.job_store.root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        job = JobRecord(
            id=job_id,
            type=job_type,
            status=JobStatus.QUEUED,
            command="",
            job_report_dir=str(job_dir),
        )
        self.job_store.create(job)

        if request_payload.allow_structural_operations:
            return self.job_store.update(
                job_id,
                status=JobStatus.BLOCKED,
                block_reason="structural_operations_forbidden",
                error="UI safety policy forbids structural operations.",
                finished_at=_utc_now_iso(),
            )

        config_path = (self.project_root / request_payload.config).resolve()
        if not config_path.exists():
            return self.job_store.update(
                job_id,
                status=JobStatus.FAILED,
                block_reason="config_not_found",
                error=f"Config not found: {config_path}",
                finished_at=_utc_now_iso(),
            )

        if request_payload.real_write and request_payload.confirmation_token != self.confirmation_token:
            return self.job_store.update(
                job_id,
                status=JobStatus.BLOCKED,
                block_reason="real_write_confirmation_required",
                error="Real-write requires explicit confirmation token.",
                finished_at=_utc_now_iso(),
            )

        effective_config_path = config_path
        if not request_payload.real_write:
            effective_config_path = _clone_safe_config(
                config_path=config_path,
                target_path=job_dir / "effective_config_nowrite.json",
            )
        command = command_builder(str(effective_config_path))
        command_text = _command_to_text(command)
        (job_dir / "command.txt").write_text(command_text, encoding="utf-8")
        self.job_store.update(job_id, command=command_text)

        if request_payload.wait_for_completion:
            self._run_job(job_id=job_id, command=command, timeout_seconds=request_payload.timeout_seconds)
        else:
            thread = threading.Thread(
                target=self._run_job,
                kwargs={"job_id": job_id, "command": command, "timeout_seconds": request_payload.timeout_seconds},
                daemon=True,
            )
            thread.start()

        latest = self.job_store.get(job_id)
        if latest is None:
            raise RuntimeError(f"Job lost: {job_id}")
        return latest

    def _run_job(self, *, job_id: str, command: list[str], timeout_seconds: int) -> None:
        job = self.job_store.get(job_id)
        if job is None:
            return
        job_dir = Path(job.job_report_dir)
        self.job_store.update(job_id, status=JobStatus.RUNNING, started_at=_utc_now_iso())
        try:
            result = self._executor(command, self.project_root, timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            self.job_store.update(
                job_id,
                status=JobStatus.FAILED,
                block_reason="job_timeout",
                error=f"Timeout after {timeout_seconds}s: {exc}",
                finished_at=_utc_now_iso(),
            )
            return
        except Exception as exc:
            self.job_store.update(
                job_id,
                status=JobStatus.FAILED,
                block_reason="job_runtime_exception",
                error=str(exc),
                finished_at=_utc_now_iso(),
            )
            return

        stdout = str(result.stdout or "")
        stderr = str(result.stderr or "")
        (job_dir / "stdout.log").write_text(stdout, encoding="utf-8")
        (job_dir / "stderr.log").write_text(stderr, encoding="utf-8")
        run_dir = _extract_run_dir_from_command(command) or _extract_run_dir_from_output(stdout) or _extract_run_dir_from_output(stderr)
        metrics: dict[str, Any] = {}
        artifacts: dict[str, str] = {}
        if run_dir and Path(run_dir).exists():
            metrics, artifacts = _extract_metrics(Path(run_dir))

        if int(result.returncode) != 0:
            self.job_store.update(
                job_id,
                status=JobStatus.FAILED,
                run_dir=run_dir,
                block_reason=str(metrics.get("block_reason") or "command_failed"),
                error=(stderr.strip() or stdout.strip() or f"Command failed with code {result.returncode}")[:4000],
                rows_prepared=_safe_int(metrics.get("rows_prepared")),
                rows_written=_safe_int(metrics.get("rows_written")),
                rows_quarantined=_safe_int(metrics.get("rows_quarantined")),
                progress=metrics.get("progress") if isinstance(metrics.get("progress"), dict) else {},
                artifact_paths=artifacts,
                finished_at=_utc_now_iso(),
            )
            return

        summary_status = str(metrics.get("status_from_summary") or "").lower()
        write_allowed = metrics.get("write_allowed")
        block_reason = str(metrics.get("block_reason") or "")
        final_status = JobStatus.SUCCEEDED
        if block_reason and write_allowed is False:
            final_status = JobStatus.BLOCKED
        if summary_status in {"failed", "error"}:
            final_status = JobStatus.FAILED
        if summary_status == "blocked":
            final_status = JobStatus.BLOCKED

        self.job_store.update(
            job_id,
            status=final_status,
            run_dir=run_dir,
            block_reason=block_reason,
            rows_prepared=_safe_int(metrics.get("rows_prepared")),
            rows_written=_safe_int(metrics.get("rows_written")),
            rows_quarantined=_safe_int(metrics.get("rows_quarantined")),
            progress=metrics.get("progress") if isinstance(metrics.get("progress"), dict) else {},
            artifact_paths=artifacts,
            finished_at=_utc_now_iso(),
        )


def _python_command(*parts: str) -> list[str]:
    return [sys.executable, *parts]


def _build_call_review_command(request: CallReviewJobRequest, *, config_path: str) -> list[str]:
    cmd = _python_command(
        "-m",
        "src.deal_analyzer.cli",
        "--config",
        config_path,
        "analyze-period",
        "--input",
        request.input,
        "--period-mode",
        request.period_mode,
    )
    if request.period_mode == "custom_range":
        if not request.date_from or not request.date_to:
            raise HTTPException(status_code=400, detail="custom_range requires date_from and date_to")
        cmd.extend(["--date-from", request.date_from, "--date-to", request.date_to])
    if request.period_mode == "control_day_window":
        if not request.control_date:
            raise HTTPException(status_code=400, detail="control_day_window requires control_date")
        cmd.extend(["--control-date", request.control_date])
        if request.business_cutoff:
            cmd.extend(["--business-cutoff", request.business_cutoff])
        if request.business_timezone:
            cmd.extend(["--business-timezone", request.business_timezone])
    if request.date_from and request.period_mode != "custom_range":
        cmd.extend(["--date-from", request.date_from])
    if request.date_to and request.period_mode != "custom_range":
        cmd.extend(["--date-to", request.date_to])
    if request.limit is not None:
        cmd.extend(["--limit", str(request.limit)])
    if request.discussion_limit is not None:
        cmd.extend(["--discussion-limit", str(request.discussion_limit)])
    if request.include_presentations is not None:
        cmd.extend(["--include-presentations", "true" if request.include_presentations else "false"])
    return cmd


def _build_daily_control_command(request: DailyControlJobRequest, *, config_path: str) -> list[str]:
    base = _python_command("-m", "src.deal_analyzer.daily_control.cli")
    if request.operation == "build":
        if not request.period_start or not request.period_end:
            raise HTTPException(status_code=400, detail="daily-control build requires period_start and period_end")
        cmd = [
            *base,
            "build",
            "--config",
            config_path,
            "--period-start",
            request.period_start,
            "--period-end",
            request.period_end,
            "--source-sheet",
            request.source_sheet,
            "--daily-sheet",
            request.daily_sheet,
            "--dry-run",
        ]
        if request.main_model:
            cmd.extend(["--main-model", request.main_model])
        if request.fallback_model:
            cmd.extend(["--fallback-model", request.fallback_model])
        if request.limit is not None:
            cmd.extend(["--limit", str(request.limit)])
        return cmd
    if not request.run_dir:
        raise HTTPException(status_code=400, detail="daily-control write requires run_dir")
    cmd = [
        *base,
        "write",
        "--config",
        config_path,
        "--run-dir",
        request.run_dir,
        "--daily-sheet",
        request.daily_sheet,
        "--strict-preflight",
    ]
    if request.allow_partial_write:
        cmd.append("--allow-partial-write")
    else:
        cmd.append("--no-allow-partial-write")
    if request.quarantine_unrepaired:
        cmd.append("--quarantine-unrepaired")
    else:
        cmd.append("--no-quarantine-unrepaired")
    cmd.append("--write" if request.real_write else "--dry-run")
    return cmd


def _build_week_plan_command(request: WeekPlanJobRequest, *, config_path: str) -> list[str]:
    base = _python_command("-m", "src.deal_analyzer.week_plan.cli")
    if request.operation == "build":
        cmd = [
            *base,
            "build",
            "--config",
            config_path,
            "--daily-sheet",
            request.daily_sheet,
            "--manager-summary-sheet",
            request.manager_summary_sheet,
            "--week-summary-sheet",
            request.week_summary_sheet,
            "--target-sheet",
            request.target_sheet,
            "--dry-run",
        ]
        if request.signal_start:
            cmd.extend(["--signal-start", request.signal_start])
        if request.signal_end:
            cmd.extend(["--signal-end", request.signal_end])
        if request.plan_week_start:
            cmd.extend(["--plan-week-start", request.plan_week_start])
        if request.plan_week_end:
            cmd.extend(["--plan-week-end", request.plan_week_end])
        if request.period_start and request.period_end:
            cmd.extend(["--period-start", request.period_start, "--period-end", request.period_end])
        if request.main_model:
            cmd.extend(["--main-model", request.main_model])
        if request.fallback_model:
            cmd.extend(["--fallback-model", request.fallback_model])
        if request.limit is not None:
            cmd.extend(["--limit", str(request.limit)])
        if request.bootstrap_if_empty:
            cmd.append("--bootstrap-if-empty")
        return cmd
    if not request.run_dir:
        raise HTTPException(status_code=400, detail="week-plan write requires run_dir")
    cmd = [
        *base,
        "write",
        "--config",
        config_path,
        "--run-dir",
        request.run_dir,
        "--target-sheet",
        request.target_sheet,
        "--strict-preflight",
    ]
    if request.allow_partial_write:
        cmd.append("--allow-partial-write")
    else:
        cmd.append("--no-allow-partial-write")
    if request.quarantine_unrepaired:
        cmd.append("--quarantine-unrepaired")
    else:
        cmd.append("--no-quarantine-unrepaired")
    cmd.append("--write" if request.real_write else "--dry-run")
    return cmd


def _build_training_materials_command(request: TrainingMaterialsJobRequest, *, config_path: str) -> list[str]:
    base = _python_command("-m", "src.deal_analyzer.training_materials.cli")
    if request.operation == "build":
        if not request.week_start or not request.week_end:
            raise HTTPException(status_code=400, detail="training-materials build requires week_start and week_end")
        cmd = [
            *base,
            "build",
            "--config",
            config_path,
            "--plan-sheet",
            request.plan_sheet,
            "--daily-sheet",
            request.daily_sheet,
            "--call-review-sheet",
            request.call_review_sheet,
            "--week-start",
            request.week_start,
            "--week-end",
            request.week_end,
            "--dry-run",
        ]
        if request.main_model:
            cmd.extend(["--main-model", request.main_model])
        if request.fallback_model:
            cmd.extend(["--fallback-model", request.fallback_model])
        if request.model_pool:
            cmd.extend(["--model-pool", request.model_pool])
        if request.limit is not None:
            cmd.extend(["--limit", str(request.limit)])
        if request.allow_full_run:
            cmd.append("--allow-full-run")
        if request.require_external_sources:
            cmd.append("--require-external-sources")
        else:
            cmd.append("--no-require-external-sources")
        if request.allow_no_external_sources:
            cmd.append("--allow-no-external-sources")
        return cmd
    if not request.run_dir:
        raise HTTPException(status_code=400, detail="training-materials write requires run_dir")
    cmd = [
        *base,
        "write",
        "--config",
        config_path,
        "--run-dir",
        request.run_dir,
        "--plan-sheet",
        request.plan_sheet,
        "--strict-preflight",
    ]
    if request.overwrite_links:
        cmd.append("--overwrite-links")
    cmd.append("--write" if request.real_write else "--dry-run")
    return cmd


def _build_weekly_manager_summary_command(request: WeeklyManagerSummaryJobRequest, *, config_path: str) -> list[str]:
    base = _python_command("-m", "src.deal_analyzer.weekly_manager_summary.cli")
    if request.operation == "build":
        if not request.period_start or not request.period_end:
            raise HTTPException(status_code=400, detail="weekly-manager-summary build requires period_start and period_end")
        cmd = [
            *base,
            "build",
            "--config",
            config_path,
            "--period-start",
            request.period_start,
            "--period-end",
            request.period_end,
            "--daily-sheet",
            request.daily_sheet,
            "--plan-sheet",
            request.plan_sheet,
            "--target-sheet",
            request.target_sheet,
            "--dry-run",
        ]
        if request.main_model:
            cmd.extend(["--main-model", request.main_model])
        if request.fallback_model:
            cmd.extend(["--fallback-model", request.fallback_model])
        if request.limit is not None:
            cmd.extend(["--limit", str(request.limit)])
        return cmd
    if not request.run_dir:
        raise HTTPException(status_code=400, detail="weekly-manager-summary write requires run_dir")
    cmd = [
        *base,
        "write",
        "--config",
        config_path,
        "--run-dir",
        request.run_dir,
        "--daily-sheet",
        request.daily_sheet,
        "--plan-sheet",
        request.plan_sheet,
        "--target-sheet",
        request.target_sheet,
        "--strict-preflight",
    ]
    if request.allow_partial_write:
        cmd.append("--allow-partial-write")
    else:
        cmd.append("--no-allow-partial-write")
    if request.quarantine_unrepaired:
        cmd.append("--quarantine-unrepaired")
    else:
        cmd.append("--no-quarantine-unrepaired")
    cmd.append("--write" if request.real_write else "--dry-run")
    return cmd


def _build_week_summary_command(request: WeekSummaryJobRequest, *, config_path: str) -> list[str]:
    base = _python_command("-m", "src.deal_analyzer.week_summary.cli")
    if request.operation == "build":
        if not request.period_start or not request.period_end:
            raise HTTPException(status_code=400, detail="week-summary build requires period_start and period_end")
        cmd = [
            *base,
            "build",
            "--config",
            config_path,
            "--period-start",
            request.period_start,
            "--period-end",
            request.period_end,
            "--daily-sheet",
            request.daily_sheet,
            "--plan-sheet",
            request.plan_sheet,
            "--manager-summary-sheet",
            request.manager_summary_sheet,
            "--target-sheet",
            request.target_sheet,
            "--dry-run",
        ]
        if request.main_model:
            cmd.extend(["--main-model", request.main_model])
        if request.fallback_model:
            cmd.extend(["--fallback-model", request.fallback_model])
        if request.limit is not None:
            cmd.extend(["--limit", str(request.limit)])
        return cmd
    if not request.run_dir:
        raise HTTPException(status_code=400, detail="week-summary write requires run_dir")
    cmd = [
        *base,
        "write",
        "--config",
        config_path,
        "--run-dir",
        request.run_dir,
        "--daily-sheet",
        request.daily_sheet,
        "--plan-sheet",
        request.plan_sheet,
        "--manager-summary-sheet",
        request.manager_summary_sheet,
        "--target-sheet",
        request.target_sheet,
        "--strict-preflight",
    ]
    if request.allow_partial_write:
        cmd.append("--allow-partial-write")
    else:
        cmd.append("--no-allow-partial-write")
    if request.quarantine_unrepaired:
        cmd.append("--quarantine-unrepaired")
    else:
        cmd.append("--no-quarantine-unrepaired")
    cmd.append("--write" if request.real_write else "--dry-run")
    return cmd


def _build_cache_cleanup_command(request: CacheCleanupDryRunRequest, *, config_path: str) -> list[str]:
    cmd = _python_command(
        "-m",
        "src.deal_analyzer.cache_manager",
        "cleanup",
        "--config",
        config_path,
        "--older-than-days",
        str(max(0, int(request.older_than_days or 0))),
        "--max-size-gb",
        str(max(0.0, float(request.max_size_gb or 0.0))),
        "--dry-run",
    )
    return cmd


def create_app(
    *,
    project_root: Path | None = None,
    confirmation_token: str | None = None,
    executor: Callable[[list[str], Path, int], CommandResult] | None = None,
) -> FastAPI:
    root = (project_root or Path.cwd()).resolve()
    jobs_root = root / "workspace" / "ui_jobs"
    token = confirmation_token or os.getenv("DEAL_ANALYZER_UI_CONFIRM_TOKEN", "CONFIRM_REAL_WRITE")
    store = JobStore(jobs_root)
    runner = JobRunner(project_root=root, job_store=store, confirmation_token=token, executor=executor)

    app = FastAPI(title="Deal Analyzer UI Foundation", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "deal_analyzer_ui_foundation",
            "time_utc": _utc_now_iso(),
            "jobs_root": str(jobs_root),
            "real_write_requires_token": True,
        }

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return """
<html><head><title>Deal Analyzer UI Foundation</title></head>
<body>
  <h1>Deal Analyzer UI Foundation</h1>
  <p>Use REST endpoints to run dry-run jobs safely.</p>
  <ul>
    <li>GET /health</li>
    <li>GET /jobs</li>
    <li>GET /jobs/{id}</li>
    <li>POST /jobs/call-review</li>
    <li>POST /jobs/daily-control</li>
    <li>POST /jobs/week-plan</li>
    <li>POST /jobs/training-materials</li>
    <li>POST /jobs/weekly-manager-summary</li>
    <li>POST /jobs/week-summary</li>
    <li>POST /jobs/cache-cleanup-dry-run</li>
  </ul>
</body></html>
"""

    @app.get("/jobs")
    def list_jobs() -> list[dict[str, Any]]:
        return [item.model_dump() for item in store.list()]

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        return job.model_dump()

    @app.post("/jobs/call-review")
    def create_call_review_job(request: CallReviewJobRequest) -> dict[str, Any]:
        job = runner.submit(
            job_type="call_review",
            request_payload=request,
            command_builder=lambda cfg: _build_call_review_command(request, config_path=cfg),
        )
        return job.model_dump()

    @app.post("/jobs/daily-control")
    def create_daily_control_job(request: DailyControlJobRequest) -> dict[str, Any]:
        job = runner.submit(
            job_type="daily_control",
            request_payload=request,
            command_builder=lambda cfg: _build_daily_control_command(request, config_path=cfg),
        )
        return job.model_dump()

    @app.post("/jobs/week-plan")
    def create_week_plan_job(request: WeekPlanJobRequest) -> dict[str, Any]:
        job = runner.submit(
            job_type="week_plan",
            request_payload=request,
            command_builder=lambda cfg: _build_week_plan_command(request, config_path=cfg),
        )
        return job.model_dump()

    @app.post("/jobs/training-materials")
    def create_training_materials_job(request: TrainingMaterialsJobRequest) -> dict[str, Any]:
        job = runner.submit(
            job_type="training_materials",
            request_payload=request,
            command_builder=lambda cfg: _build_training_materials_command(request, config_path=cfg),
        )
        return job.model_dump()

    @app.post("/jobs/weekly-manager-summary")
    def create_weekly_manager_summary_job(request: WeeklyManagerSummaryJobRequest) -> dict[str, Any]:
        job = runner.submit(
            job_type="weekly_manager_summary",
            request_payload=request,
            command_builder=lambda cfg: _build_weekly_manager_summary_command(request, config_path=cfg),
        )
        return job.model_dump()

    @app.post("/jobs/week-summary")
    def create_week_summary_job(request: WeekSummaryJobRequest) -> dict[str, Any]:
        job = runner.submit(
            job_type="week_summary",
            request_payload=request,
            command_builder=lambda cfg: _build_week_summary_command(request, config_path=cfg),
        )
        return job.model_dump()

    @app.post("/jobs/cache-cleanup-dry-run")
    def create_cache_cleanup_job(request: CacheCleanupDryRunRequest) -> dict[str, Any]:
        request = request.model_copy(update={"real_write": False})
        job = runner.submit(
            job_type="cache_cleanup_dry_run",
            request_payload=request,
            command_builder=lambda cfg: _build_cache_cleanup_command(request, config_path=cfg),
        )
        return job.model_dump()

    return app


app = create_app()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deal Analyzer UI foundation (FastAPI)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8010)
    parser.add_argument("--reload", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    import uvicorn

    uvicorn.run("src.deal_analyzer.ui_foundation:app", host=args.host, port=args.port, reload=bool(args.reload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
