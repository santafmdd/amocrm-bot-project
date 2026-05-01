from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import json
import time
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _fmt_hhmmss(total_seconds: float) -> str:
    sec = max(0, int(total_seconds))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


@dataclass
class ProgressSnapshot:
    process: str
    step_name: str
    current: int
    total: int
    percent: float
    elapsed_seconds: int
    eta_seconds: int | None
    status: str
    started_at: str
    updated_at: str
    current_item_summary: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "process": self.process,
            "step_name": self.step_name,
            "current": self.current,
            "total": self.total,
            "percent": self.percent,
            "elapsed_seconds": self.elapsed_seconds,
            "eta_seconds": self.eta_seconds,
            "status": self.status,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "current_item_summary": self.current_item_summary,
            "details": self.details,
        }


class ProgressReporter:
    """Lightweight file-based progress reporter for long-running jobs."""

    def __init__(
        self,
        *,
        process: str,
        run_dir: Path,
        heartbeat_seconds: int = 30,
        logger: Any | None = None,
        step_name: str = "started",
        total: int = 0,
    ) -> None:
        self.process = str(process or "job").strip() or "job"
        self.run_dir = Path(run_dir).resolve()
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.heartbeat_seconds = max(5, int(heartbeat_seconds or 30))
        self.logger = logger

        self.progress_path = self.run_dir / "progress.json"
        self.progress_log_path = self.run_dir / "progress.log"
        self.heartbeat_path = self.run_dir / "heartbeat.json"

        self._started_ts = time.time()
        self._last_heartbeat_ts = 0.0
        self._last_log_key = ""
        started_at = _utc_now_iso()
        self._snapshot = ProgressSnapshot(
            process=self.process,
            step_name=str(step_name or "started"),
            current=0,
            total=max(0, int(total or 0)),
            percent=0.0,
            elapsed_seconds=0,
            eta_seconds=None,
            status="running",
            started_at=started_at,
            updated_at=started_at,
            current_item_summary="",
            details={},
        )
        self._flush(log=True, heartbeat=True, force_log_message="run_started")

    def update(
        self,
        *,
        step_name: str | None = None,
        current: int | None = None,
        total: int | None = None,
        current_item: dict[str, Any] | str | None = None,
        status: str | None = None,
        details: dict[str, Any] | None = None,
        log: bool = True,
    ) -> None:
        if step_name is not None:
            self._snapshot.step_name = str(step_name or self._snapshot.step_name)
        if current is not None:
            self._snapshot.current = max(0, int(current or 0))
        if total is not None:
            self._snapshot.total = max(0, int(total or 0))
        if status is not None:
            self._snapshot.status = str(status or self._snapshot.status)
        if details is not None:
            self._snapshot.details = dict(details)
        if current_item is not None:
            self._snapshot.current_item_summary = self._format_current_item_summary(current_item)

        self._recompute()
        self._flush(log=log, heartbeat=False, force_log_message="")

    def heartbeat(self, *, details: dict[str, Any] | None = None) -> None:
        if details is not None:
            self._snapshot.details = dict(details)
        self._recompute()
        self._flush(log=False, heartbeat=True, force_log_message="")

    def finish(self, *, status: str = "completed", step_name: str = "completed", error: str = "") -> None:
        self._snapshot.status = str(status or "completed")
        self._snapshot.step_name = str(step_name or "completed")
        if error:
            details = dict(self._snapshot.details or {})
            details["error"] = str(error)
            self._snapshot.details = details
        self._recompute()
        self._flush(log=True, heartbeat=True, force_log_message="run_finished")

    def as_dict(self) -> dict[str, Any]:
        return self._snapshot.to_dict()

    def _recompute(self) -> None:
        elapsed = int(max(0, time.time() - self._started_ts))
        self._snapshot.elapsed_seconds = elapsed
        self._snapshot.updated_at = _utc_now_iso()
        total = int(self._snapshot.total or 0)
        current = int(self._snapshot.current or 0)
        if total > 0:
            self._snapshot.percent = round(min(100.0, (current / total) * 100.0), 2)
            if current > 0:
                remaining = max(0, total - current)
                eta = int((elapsed / max(1, current)) * remaining)
                self._snapshot.eta_seconds = max(0, eta)
            else:
                self._snapshot.eta_seconds = None
        else:
            self._snapshot.percent = 0.0
            self._snapshot.eta_seconds = None

    def _flush(self, *, log: bool, heartbeat: bool, force_log_message: str) -> None:
        self._write_json(self.progress_path, self._snapshot.to_dict())
        now = time.time()
        heartbeat_due = heartbeat or (now - self._last_heartbeat_ts >= self.heartbeat_seconds)
        if heartbeat_due:
            self._write_json(
                self.heartbeat_path,
                {
                    "process": self.process,
                    "heartbeat_at": _utc_now_iso(),
                    "step_name": self._snapshot.step_name,
                    "status": self._snapshot.status,
                    "elapsed_seconds": self._snapshot.elapsed_seconds,
                    "current": self._snapshot.current,
                    "total": self._snapshot.total,
                    "percent": self._snapshot.percent,
                    "eta_seconds": self._snapshot.eta_seconds,
                    "current_item_summary": self._snapshot.current_item_summary,
                    "details": self._snapshot.details,
                },
            )
            self._last_heartbeat_ts = now

        if log:
            message = self._format_log_line(force_log_message=force_log_message)
            log_key = f"{self._snapshot.step_name}|{self._snapshot.current}|{self._snapshot.total}|{self._snapshot.current_item_summary}|{self._snapshot.status}"
            if force_log_message or log_key != self._last_log_key:
                with self.progress_log_path.open("a", encoding="utf-8") as fh:
                    fh.write(message + "\n")
                self._last_log_key = log_key
                if self.logger is not None:
                    try:
                        self.logger.info(message)
                    except Exception:
                        pass

    def _format_log_line(self, *, force_log_message: str) -> str:
        marker = force_log_message.strip()
        cur = int(self._snapshot.current or 0)
        total = int(self._snapshot.total or 0)
        pct = int(round(float(self._snapshot.percent or 0.0)))
        elapsed = _fmt_hhmmss(float(self._snapshot.elapsed_seconds or 0))
        eta = _fmt_hhmmss(float(self._snapshot.eta_seconds or 0)) if self._snapshot.eta_seconds is not None else "--:--:--"
        suffix = f", current={self._snapshot.current_item_summary}" if self._snapshot.current_item_summary else ""
        marker_prefix = f"{marker} " if marker else ""
        return (
            f"[{self.process}] {marker_prefix}{self._snapshot.step_name} "
            f"{cur}/{total}, {pct}%, elapsed {elapsed}, eta ~{eta}{suffix}"
        )

    def _format_current_item_summary(self, current_item: dict[str, Any] | str) -> str:
        if isinstance(current_item, str):
            return current_item.strip()
        if not isinstance(current_item, dict):
            return ""
        keys = (
            "manager",
            "recipient",
            "date",
            "plan_date",
            "deal_id",
            "call_id",
            "model",
            "stage",
        )
        parts: list[str] = []
        for key in keys:
            value = str(current_item.get(key) or "").strip()
            if value:
                parts.append(f"{key}={value}")
        if not parts:
            for key, value in current_item.items():
                text = str(value).strip()
                if text:
                    parts.append(f"{key}={text}")
                if len(parts) >= 4:
                    break
        return " ".join(parts)

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

