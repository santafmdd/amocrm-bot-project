from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from src.amocrm_auth.config import load_amocrm_auth_config
from src.amocrm_auth.state_store import load_auth_state
from src.amocrm_collector.client import AmoCollectorClient
from src.deal_analyzer.config import load_deal_analyzer_config
from src.deal_analyzer.training_materials.docs_writer import ensure_training_materials_oauth_scopes
from src.integrations.google_sheets_api_client import AUTH_MODE_CACHE_ONLY, GoogleSheetsApiClient, extract_spreadsheet_id
from src.logger import setup_logging

SHEET_CALL_REVIEW = "Разбор звонков"
SHEET_DAILY = "Дневной контроль"
SHEET_PLAN = "План недели"
SHEET_MANAGER = "Недельный свод менеджеров"
SHEET_WEEK_SUMMARY = "Свод недели"
CONTROL_CONTACT_ID = "36219401"


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_md(path: Path, title: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join([x for x in lines if str(x).strip()])
    path.write_text(f"# {title}\n\n{body}\n", encoding="utf-8")


def _run(cmd: list[str], cwd: Path, timeout: int) -> dict[str, Any]:
    t0 = time.time()
    cmd_line = " ".join(shlex.quote(str(x)) for x in cmd)
    try:
        p = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
        return {
            "command": cmd_line,
            "returncode": int(p.returncode),
            "stdout": str(p.stdout or ""),
            "stderr": str(p.stderr or ""),
            "duration_sec": round(time.time() - t0, 2),
            "timed_out": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": cmd_line,
            "returncode": 124,
            "stdout": str(exc.stdout or ""),
            "stderr": str(exc.stderr or ""),
            "duration_sec": round(time.time() - t0, 2),
            "timed_out": True,
            "error_type": "timeout",
            "error": f"step timed out after {int(timeout)}s",
        }
    except Exception as exc:
        return {
            "command": cmd_line,
            "returncode": 1,
            "stdout": "",
            "stderr": str(exc),
            "duration_sec": round(time.time() - t0, 2),
            "timed_out": False,
            "error_type": "runner_subprocess_error",
            "error": str(exc),
        }


def _latest_dir(root: Path, since: float) -> str:
    if not root.exists():
        return ""
    dirs = [x for x in root.iterdir() if x.is_dir()]
    dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    for d in dirs:
        if d.stat().st_mtime >= since:
            return str(d)
    return ""


def _extract_run_dir(step: dict[str, Any], fallback_root: Path, since: float) -> str:
    out = str(step.get("stdout") or "")
    for line in reversed(out.splitlines()):
        s = line.strip()
        if "workspace" in s.lower() and Path(s).exists():
            return s
    return _latest_dir(fallback_root, since)


def _sheet_id(cfg: Any) -> str:
    sid = str(getattr(cfg, "deal_analyzer_spreadsheet_id", "") or "").strip()
    if sid:
        return sid
    return extract_spreadsheet_id(str(getattr(cfg, "deal_analyzer_sheet_url", "") or ""))


def _sheet_values(client: GoogleSheetsApiClient, sid: str, sheet: str) -> list[list[str]]:
    return client.get_values(sid, f"{sheet}!A1:ZZ")


def _col_letter(n: int) -> str:
    n = max(1, int(n))
    out = ""
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s or "").lower().replace("ё", "е") if ch.isalnum())


def _find_col(headers: list[str], variants: list[str]) -> int:
    hs = [_norm(x) for x in headers]
    vv = {_norm(x) for x in variants}
    for i, h in enumerate(hs):
        if h in vv:
            return i
    return -1


def _date_token(x: str) -> str:
    import re

    m = re.search(r"(\d{4}-\d{2}-\d{2})", str(x or ""))
    return m.group(1) if m else ""


def _resolve_header_row(values: list[list[str]], variants: list[str], search_rows: int = 5) -> tuple[int, list[str]]:
    if not values:
        return (-1, [])
    best_idx = -1
    best_score = -1
    norm_variants = [_norm(v) for v in variants if str(v).strip()]
    for ridx, row in enumerate(values[: max(1, search_rows)]):
        headers = [str(x) for x in row]
        hs = [_norm(x) for x in headers]
        score = 0
        for nv in norm_variants:
            if nv in hs:
                score += 1
        if score > best_score:
            best_score = score
            best_idx = ridx
    if best_idx < 0:
        return (-1, [])
    return (best_idx, [str(x) for x in values[best_idx]])


def _clear_rows(client: GoogleSheetsApiClient, sid: str, sheet: str, row_ids: list[int], cols: int) -> dict[str, Any]:
    if not row_ids:
        return {"cleared_rows": 0, "ranges": []}
    e = _col_letter(cols)
    data = []
    ranges = []
    for r in sorted(set([int(x) for x in row_ids if int(x) >= 2])):
        rng = f"{sheet}!A{r}:{e}{r}"
        data.append({"range": rng, "values": [[""] * cols]})
        ranges.append(rng)
    client.batch_update_values(sid, data)
    return {"cleared_rows": len(data), "ranges": ranges}


def _safe_clear_by_date(client: GoogleSheetsApiClient, sid: str, sheet: str, date_value: str, col_variants: list[str]) -> dict[str, Any]:
    vals = _sheet_values(client, sid, sheet)
    if not vals:
        return {"safe": False, "reason": "sheet_empty"}
    hidx, headers = _resolve_header_row(vals, col_variants)
    if hidx < 0:
        return {"safe": False, "reason": "header_row_not_found"}
    idx = _find_col(headers, col_variants)
    if idx < 0:
        return {"safe": False, "reason": "date_column_not_found", "header_row_index": hidx}
    data_start = hidx + 2
    rows = vals[data_start - 1 :]
    hit = []
    for i, row in enumerate(rows, start=data_start):
        cell = row[idx] if idx < len(row) else ""
        if _date_token(cell) == date_value:
            hit.append(i)
    clr = _clear_rows(client, sid, sheet, hit, max(1, len(headers)))
    return {"safe": True, "matched_rows": len(hit), "header_row_index": hidx, **clr}


def _safe_clear_by_week(client: GoogleSheetsApiClient, sid: str, sheet: str, week_ranges: set[tuple[str, str]], start_variants: list[str], end_variants: list[str]) -> dict[str, Any]:
    vals = _sheet_values(client, sid, sheet)
    if not vals:
        return {"safe": False, "reason": "sheet_empty"}
    hidx, headers = _resolve_header_row(vals, start_variants + end_variants)
    if hidx < 0:
        return {"safe": False, "reason": "header_row_not_found"}
    sidx = _find_col(headers, start_variants)
    eidx = _find_col(headers, end_variants)
    if sidx < 0 or eidx < 0:
        return {"safe": False, "reason": "week_columns_not_found", "sidx": sidx, "eidx": eidx, "header_row_index": hidx}
    data_start = hidx + 2
    rows = vals[data_start - 1 :]
    hit = []
    for i, row in enumerate(rows, start=data_start):
        s = _date_token(row[sidx] if sidx < len(row) else "")
        e = _date_token(row[eidx] if eidx < len(row) else "")
        if (s, e) in week_ranges:
            hit.append(i)
    clr = _clear_rows(client, sid, sheet, hit, max(1, len(headers)))
    return {"safe": True, "matched_rows": len(hit), "header_row_index": hidx, **clr}


def _start_heartbeat(path: Path, state: dict[str, Any]) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()

    def _worker() -> None:
        while not stop.is_set():
            payload = {
                "timestamp": _now(),
                **state,
            }
            _write_json(path, payload)
            stop.wait(600)

    th = threading.Thread(target=_worker, daemon=True)
    th.start()
    return stop, th


def _check_watchdog(start_ts: float, limit_sec: int) -> bool:
    return (time.time() - start_ts) <= max(1, int(limit_sec))


def _check_contact_fallback(run_dir: str, contact_id: str) -> dict[str, Any]:
    out = {
        "contact_id": contact_id,
        "call_window_debug_found": False,
        "in_call_window_debug": False,
        "in_call_ledger_all": False,
        "final_entity_type": "",
        "resolution_reason": "",
    }
    if not run_dir:
        return out
    rd = Path(run_dir)
    p = rd / "call_window_debug.json"
    if p.exists():
        out["call_window_debug_found"] = True
        obj = _read_json(p, {})
        cands = obj.get("candidates", []) if isinstance(obj, dict) else []
        for c in cands:
            if str(c.get("contact_id", "")) == str(contact_id):
                out["in_call_window_debug"] = True
                out["final_entity_type"] = str(c.get("entity_type", "") or "")
                out["resolution_reason"] = str(c.get("resolution_reason", "") or "")
                out["contact_call_example"] = {
                    "deal_id": c.get("deal_id"),
                    "call_id": c.get("call_id"),
                    "normalized_call_datetime": c.get("normalized_call_datetime"),
                    "inside_window": c.get("inside_window"),
                    "exclude_reason": c.get("exclude_reason"),
                    "source_location": c.get("source_location"),
                }
                break
    p2 = rd / "call_ledger_all.json"
    if p2.exists():
        ledger = _read_json(p2, [])
        rows = ledger if isinstance(ledger, list) else ledger.get("rows", [])
        for row in rows:
            if str(row.get("contact_id", "")) == str(contact_id):
                out["in_call_ledger_all"] = True
                break
    if not out["in_call_window_debug"]:
        out["not_found_reason"] = "outside_window_or_missing_contacts_notes_fallback"
    return out


def _backup_sheet(client: GoogleSheetsApiClient, sid: str, sheet: str, backup_dir: Path) -> dict[str, Any]:
    vals = _sheet_values(client, sid, sheet)
    stem = "".join(ch if ch.isalnum() else "_" for ch in sheet).strip("_") or "sheet"
    jp = backup_dir / f"{stem}.json"
    cp = backup_dir / f"{stem}.csv"
    _write_json(jp, {"sheet": sheet, "captured_at": _now(), "values": vals})
    with cp.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for r in vals:
            w.writerow(r)
    return {"sheet": sheet, "json": str(jp), "csv": str(cp), "rows": max(0, len(vals) - 1)}


def _summary_lines(summary: dict[str, Any]) -> list[str]:
    lines = [
        f"status: {summary.get('status', '')}",
        f"started_at: {summary.get('started_at', '')}",
        f"finished_at: {summary.get('finished_at', '')}",
        f"real_write_started: {summary.get('real_write_started', False)}",
        f"real_write_completed: {summary.get('real_write_completed', False)}",
        f"backup_dir: {summary.get('backup_dir', '')}",
        "",
        "## Steps",
    ]
    for i, s in enumerate(summary.get("steps", []), start=1):
        lines.extend([
            f"{i}. {s.get('name', '')}",
            f"command: {s.get('command', '')}",
            f"result: {s.get('result', '')}",
            f"run_dir: {s.get('run_dir', '')}",
            f"rows_prepared: {s.get('rows_prepared', 0)}",
            f"rows_written: {s.get('rows_written', 0)}",
            f"rows_quarantined: {s.get('rows_quarantined', 0)}",
            f"block_reason: {s.get('block_reason', '')}",
            "",
        ])
    return lines


def _main() -> None:
    parser = argparse.ArgumentParser(description="Overnight real-write runner")
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run")
    run.add_argument("--config", required=True)
    run.add_argument("--real-write", action="store_true")
    run.add_argument("--control-date", default="2026-04-29")
    run.add_argument("--business-cutoff", default="15:00")
    run.add_argument("--business-timezone", default="Europe/Moscow")
    run.add_argument("--call-review-input", default="workspace/amocrm_collector/collect_period_2026-04-01_2026-04-07_latest.json")
    run.add_argument("--call-review-sheet", default=SHEET_CALL_REVIEW)
    run.add_argument("--daily-sheet", default=SHEET_DAILY)
    run.add_argument("--plan-sheet", default=SHEET_PLAN)
    run.add_argument("--manager-summary-sheet", default=SHEET_MANAGER)
    run.add_argument("--week-summary-sheet", default=SHEET_WEEK_SUMMARY)
    run.add_argument("--smoke-config", default="workspace/tmp_tests/deal_analyzer/deal_analyzer.call_review.deepseek.smoke_nowrite.json")
    run.add_argument("--discussion-limit", type=int, default=120)
    run.add_argument("--limit", type=int, default=120)
    run.add_argument("--timeout", type=int, default=3600)
    run.add_argument("--watchdog-max-hours", type=float, default=10.0)
    run.add_argument("--watchdog-max-week-hours", type=float, default=2.5)
    args = parser.parse_args()

    root = _project_root()
    logger = setup_logging(root / "logs", "INFO")
    cfg = load_deal_analyzer_config(str(args.config))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = root / "workspace" / "overnight_runs" / ts
    run_dir.mkdir(parents=True, exist_ok=True)
    backup_dir = root / "workspace" / "backups" / f"overnight_{ts}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "status": "started",
        "started_at": _now(),
        "real_write_requested": bool(args.real_write),
        "real_write_started": False,
        "real_write_completed": False,
        "backup_dir": str(backup_dir),
        "report_dir": str(run_dir),
        "steps": [],
        "clear_ops": [],
        "sheets_changed": [],
    }
    _write_json(run_dir / "overnight_summary.json", summary)
    heartbeat_state: dict[str, Any] = {
        "current_step": "started",
        "current_model": "",
        "current_week": "",
        "last_run_dir": "",
        "elapsed_seconds": 0,
    }
    heartbeat_stop, heartbeat_thread = _start_heartbeat(run_dir / "heartbeat.log", heartbeat_state)
    wall_start = time.time()

    try:
        # preflight
        heartbeat_state["current_step"] = "preflight"
        preflight = {"errors": []}
        try:
            ZoneInfo("Europe/Moscow")
            preflight["timezone"] = "ok"
        except Exception as e:
            preflight["timezone"] = f"error: {e}"
            preflight["errors"].append("timezone")
        git_step = _run(["git", "-C", str(root), "status", "--short"], root, 120)
        preflight["git_status_short"] = git_step.get("stdout", "")
        try:
            auth_cfg = load_amocrm_auth_config(getattr(cfg, "amocrm_auth_config_path", None))
            state = load_auth_state(auth_cfg.state_path)
            amo = AmoCollectorClient(base_domain=str(state.base_domain or auth_cfg.base_domain), access_token=str(state.access_token))
            preflight["amocrm"] = "ok" if isinstance(amo.get_account(), dict) else "failed"
            if preflight["amocrm"] != "ok":
                preflight["errors"].append("amocrm")
        except Exception as e:
            preflight["amocrm"] = f"error: {e}"
            preflight["errors"].append("amocrm")
        docs = ensure_training_materials_oauth_scopes(project_root=root, logger=logger, force_reauth=False)
        preflight["docs"] = docs
        try:
            sid = _sheet_id(cfg)
            c = GoogleSheetsApiClient(project_root=root, logger=logger, auth_mode=AUTH_MODE_CACHE_ONLY)
            preflight["sheets"] = {"ok": True, "tabs": len(c.list_sheets(sid))}
        except Exception as e:
            preflight["sheets"] = {"ok": False, "error": str(e)}
            preflight["errors"].append("sheets")
        _write_json(run_dir / "preflight.json", preflight)
        summary["preflight"] = preflight
        _write_json(run_dir / "overnight_summary.json", summary)
        if preflight["errors"]:
            summary["status"] = "blocked_preflight_failed"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return

        sid = _sheet_id(cfg)
        client = GoogleSheetsApiClient(project_root=root, logger=logger, auth_mode=AUTH_MODE_CACHE_ONLY)
        bkp = []
        heartbeat_state["current_step"] = "backup"
        for sheet in [args.call_review_sheet, args.daily_sheet, args.plan_sheet, args.manager_summary_sheet, args.week_summary_sheet]:
            bkp.append(_backup_sheet(client, sid, str(sheet), backup_dir))
        summary["backups"] = bkp
        _write_json(run_dir / "backups_manifest.json", {"backup_dir": str(backup_dir), "sheets": bkp})

        # pytest gate
        heartbeat_state["current_step"] = "pytest_gate"
        py = _run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests\\deal_analyzer"], root, 4 * 3600)
        py_step = {"name": "pytest_gate", **py, "result": "ok" if py["returncode"] == 0 else "failed"}
        summary["steps"].append(py_step)
        if py_step["result"] != "ok":
            summary["status"] = "blocked_pytest_failed"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return

        # smoke call_review
        heartbeat_state["current_step"] = "smoke_call_review"
        t0 = time.time()
        smoke_cmd = [
            sys.executable, "-m", "src.deal_analyzer.cli", "--config", str(args.smoke_config), "analyze-period",
            "--input", str(args.call_review_input),
            "--period-mode", "control_day_window", "--control-date", str(args.control_date),
            "--business-cutoff", str(args.business_cutoff), "--business-timezone", str(args.business_timezone),
            "--discussion-limit", str(args.discussion_limit), "--limit", str(args.limit),
        ]
        sm = _run(smoke_cmd, root, args.timeout)
        sm_run = _extract_run_dir(sm, root / "workspace" / "deal_analyzer" / "period_runs", t0)
        heartbeat_state["last_run_dir"] = str(sm_run or "")
        sm_sum = _read_json(Path(sm_run) / "summary.json", {}) if sm_run else {}
        sm_writer = sm_sum.get("call_review_writer", {}) if isinstance(sm_sum.get("call_review_writer"), dict) else {}
        sm_rows = int(sm_writer.get("rows_prepared", 0) or 0)
        sm_step = {"name": "smoke_call_review", **sm, "run_dir": sm_run, "rows_prepared": sm_rows, "result": "ok" if (sm["returncode"] == 0 and sm_rows > 0) else "failed", "block_reason": "rows_empty" if sm_rows <= 0 else ""}
        summary["steps"].append(sm_step)
        summary["call_review_contact_probe"] = _check_contact_fallback(str(sm_run or ""), CONTROL_CONTACT_ID)
        if sm_step["result"] != "ok":
            summary["status"] = "blocked_smoke_call_review"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return

        if not args.real_write:
            summary["status"] = "completed_smoke_only"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return

        summary["real_write_started"] = True

        # safe clear call_review by control date, then real-write
        heartbeat_state["current_step"] = "clear_call_review"
        clr = _safe_clear_by_date(
            client,
            sid,
            str(args.call_review_sheet),
            str(args.control_date),
            ["Дата анализа", "Дата кейса"],
        )
        summary["clear_ops"].append({"sheet": args.call_review_sheet, "op": "clear_control_date", **clr})
        if not clr.get("safe"):
            summary["status"] = "blocked_unsafe_clear_unavailable"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return

        heartbeat_state["current_step"] = "call_review_real_write"
        t1 = time.time()
        cr = _run([
            sys.executable, "-m", "src.deal_analyzer.cli", "--config", str(args.config), "analyze-period",
            "--input", str(args.call_review_input),
            "--period-mode", "control_day_window", "--control-date", str(args.control_date),
            "--business-cutoff", str(args.business_cutoff), "--business-timezone", str(args.business_timezone),
            "--discussion-limit", str(args.discussion_limit), "--limit", str(args.limit),
        ], root, args.timeout)
        cr_run = _extract_run_dir(cr, root / "workspace" / "deal_analyzer" / "period_runs", t1)
        heartbeat_state["last_run_dir"] = str(cr_run or "")
        cr_sum = _read_json(Path(cr_run) / "summary.json", {}) if cr_run else {}
        cr_writer = cr_sum.get("call_review_writer", {}) if isinstance(cr_sum.get("call_review_writer"), dict) else {}
        cr_written = int(cr_writer.get("rows_written", 0) or 0)
        cr_prepared = int(cr_writer.get("rows_prepared", 0) or 0)
        summary["steps"].append({"name": "call_review_real_write", **cr, "run_dir": cr_run, "rows_prepared": cr_prepared, "rows_written": cr_written, "result": "ok" if (cr["returncode"] == 0 and cr_written > 0) else "failed"})
        if cr_written > 0:
            summary["sheets_changed"].append(str(args.call_review_sheet))

        if summary["steps"][-1]["result"] != "ok":
            summary["status"] = "critical_partial_write_failure_call_review"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return

        # daily_control build -> clear -> write
        t2 = time.time()
        daily_build = _run(
            [
                sys.executable, "-m", "src.deal_analyzer.daily_control.cli", "build",
                "--config", str(args.config),
                "--period-start", str(args.control_date),
                "--period-end", str(args.control_date),
                "--source-sheet", str(args.call_review_sheet),
                "--daily-sheet", str(args.daily_sheet),
                "--main-model", "deepseek-v4-pro:cloud",
                "--fallback-model", "deepseek-v4-flash:cloud",
                "--dry-run",
            ],
            root,
            args.timeout,
        )
        daily_run = _extract_run_dir(daily_build, root / "workspace" / "daily_control", t2)
        daily_sum = _read_json(Path(daily_run) / "summary.json", {}) if daily_run else {}
        daily_rows = int(daily_sum.get("rows_in_writer_payload", 0) or 0)
        summary["steps"].append({
            "name": "daily_control_build",
            **daily_build,
            "run_dir": daily_run,
            "rows_prepared": int(daily_sum.get("rows_prepared", 0) or 0),
            "rows_in_writer_payload": daily_rows,
            "rows_quarantined": int(daily_sum.get("rows_quarantined", 0) or 0),
            "result": "ok" if (daily_build["returncode"] == 0 and daily_rows > 0) else "failed",
            "block_reason": "rows_empty" if daily_rows <= 0 else "",
        })
        if summary["steps"][-1]["result"] != "ok":
            summary["status"] = "blocked_daily_build_not_ready"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return
    
        heartbeat_state["current_step"] = "clear_daily_control"
        daily_clear = _safe_clear_by_date(
            client,
            sid,
            str(args.daily_sheet),
            str(args.control_date),
            ["Дата контроля", "Дата", "control_day_date"],
        )
        summary["clear_ops"].append({"sheet": args.daily_sheet, "op": "clear_control_date", **daily_clear})
        if not daily_clear.get("safe"):
            summary["status"] = "blocked_unsafe_clear_unavailable_daily"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return
    
        daily_write = _run(
            [
                sys.executable, "-m", "src.deal_analyzer.daily_control.cli", "write",
                "--config", str(args.config),
                "--run-dir", str(daily_run),
                "--daily-sheet", str(args.daily_sheet),
                "--write", "--strict-preflight", "--allow-partial-write", "--quarantine-unrepaired",
            ],
            root,
            args.timeout,
        )
        daily_status = _read_json(Path(daily_run) / "daily_control_writer_status.json", {}) if daily_run else {}
        daily_written = int(daily_status.get("rows_written", 0) or 0)
        summary["steps"].append({
            "name": "daily_control_write",
            **daily_write,
            "run_dir": daily_run,
            "rows_written": daily_written,
            "result": "ok" if (daily_write["returncode"] == 0 and daily_written > 0) else "failed",
            "block_reason": str(daily_status.get("block_reason", "")),
        })
        if daily_written > 0 and str(args.daily_sheet) not in summary["sheets_changed"]:
            summary["sheets_changed"].append(str(args.daily_sheet))
        if summary["steps"][-1]["result"] != "ok":
            summary["status"] = "critical_partial_write_failure_daily"
            summary["finished_at"] = _now()
            _write_json(run_dir / "overnight_summary.json", summary)
            _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
            print(str(run_dir))
            return
    
        week_specs = [
            ("week1", "2026-03-23", "2026-03-27", "2026-03-30", "2026-04-03", True, False, False),
            ("week2", "2026-03-30", "2026-04-03", "2026-04-06", "2026-04-10", False, True, True),
            ("week3", "2026-04-06", "2026-04-10", "2026-04-13", "2026-04-17", False, True, True),
            ("week4", "2026-04-13", "2026-04-17", "2026-04-20", "2026-04-24", False, True, True),
            ("current_week", "2026-04-20", "2026-04-24", "2026-04-27", "2026-04-30", False, False, False),
        ]
    
        for label, signal_start, signal_end, week_start, week_end, bootstrap, do_manager, do_week_summary in week_specs:
            if not _check_watchdog(wall_start, int(float(args.watchdog_max_hours) * 3600)):
                summary["status"] = "blocked_watchdog_total_timeout"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
            week_started = time.time()
            heartbeat_state["current_week"] = f"{label}:{week_start}..{week_end}"
            # week_plan build
            t_plan = time.time()
            heartbeat_state["current_step"] = f"week_plan_build_{label}"
            plan_cmd = [
                sys.executable, "-m", "src.deal_analyzer.week_plan.cli", "build",
                "--config", str(args.config),
                "--signal-start", signal_start,
                "--signal-end", signal_end,
                "--plan-week-start", week_start,
                "--plan-week-end", week_end,
                "--daily-sheet", str(args.daily_sheet),
                "--target-sheet", str(args.plan_sheet),
                "--main-model", "deepseek-v4-pro:cloud",
                "--fallback-model", "deepseek-v4-flash:cloud",
                "--dry-run",
            ]
            if bootstrap:
                plan_cmd.append("--bootstrap-if-empty")
            plan_build = _run(plan_cmd, root, args.timeout)
            plan_run = _extract_run_dir(plan_build, root / "workspace" / "week_plan", t_plan)
            plan_sum = _read_json(Path(plan_run) / "summary.json", {}) if plan_run else {}
            plan_rows = int(plan_sum.get("rows_in_writer_payload", 0) or 0)
            summary["steps"].append({
                "name": f"week_plan_build_{label}",
                **plan_build,
                "run_dir": plan_run,
                "rows_prepared": int(plan_sum.get("rows_prepared", 0) or 0),
                "rows_in_writer_payload": plan_rows,
                "rows_quarantined": int(plan_sum.get("rows_quarantined", 0) or 0),
                "result": "ok" if (plan_build["returncode"] == 0 and plan_rows > 0) else "failed",
                "block_reason": "rows_empty" if plan_rows <= 0 else "",
            })
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"blocked_week_plan_build_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            heartbeat_state["current_step"] = f"clear_week_plan_{label}"
            plan_clear = _safe_clear_by_week(
                client,
                sid,
                str(args.plan_sheet),
                {(week_start, week_end)},
                ["План недели с", "Неделя с"],
                ["План недели по", "Неделя по"],
            )
            summary["clear_ops"].append({"sheet": args.plan_sheet, "op": f"clear_{label}", **plan_clear})
            if not plan_clear.get("safe"):
                summary["status"] = f"blocked_unsafe_clear_unavailable_plan_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            plan_write = _run(
                [
                    sys.executable, "-m", "src.deal_analyzer.week_plan.cli", "write",
                    "--config", str(args.config),
                    "--run-dir", str(plan_run),
                    "--target-sheet", str(args.plan_sheet),
                    "--write", "--strict-preflight", "--allow-partial-write", "--quarantine-unrepaired",
                ],
                root,
                args.timeout,
            )
            plan_status = _read_json(Path(plan_run) / "week_plan_writer_status.json", {}) if plan_run else {}
            plan_written = int(plan_status.get("rows_written", 0) or 0)
            summary["steps"].append({
                "name": f"week_plan_write_{label}",
                **plan_write,
                "run_dir": plan_run,
                "rows_written": plan_written,
                "result": "ok" if (plan_write["returncode"] == 0 and plan_written > 0) else "failed",
                "block_reason": str(plan_status.get("block_reason", "")),
            })
            if plan_written > 0 and str(args.plan_sheet) not in summary["sheets_changed"]:
                summary["sheets_changed"].append(str(args.plan_sheet))
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"critical_partial_write_failure_plan_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            if not _check_watchdog(week_started, int(float(args.watchdog_max_week_hours) * 3600)):
                summary["status"] = f"blocked_watchdog_week_timeout_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            heartbeat_state["current_step"] = f"training_build_{label}"
            t_train = time.time()
            train_build = _run(
                [
                    sys.executable, "-m", "src.deal_analyzer.training_materials.cli", "build",
                    "--config", str(args.config),
                    "--plan-sheet", str(args.plan_sheet),
                    "--daily-sheet", str(args.daily_sheet),
                    "--call-review-sheet", str(args.call_review_sheet),
                    "--week-start", week_start,
                    "--week-end", week_end,
                    "--main-model", "deepseek-v3.1:671b-cloud",
                    "--fallback-model", "deepseek-v4-flash:cloud",
                    "--allow-template-fallback",
                    "--allow-full-run",
                    "--main-timeout", "5400",
                    "--fallback-timeout", "5400",
                    "--dry-run",
                ],
                root,
                max(args.timeout, 7200),
            )
            train_run = _extract_run_dir(train_build, root / "workspace" / "training_materials", t_train)
            train_sum = _read_json(Path(train_run) / "summary.json", {}) if train_run else {}
            links_to_write = int(train_sum.get("rows_links_to_write", 0) or 0)
            summary["steps"].append({
                "name": f"training_build_{label}",
                **train_build,
                "run_dir": train_run,
                "rows_prepared": int(train_sum.get("rows_docs_prepared", 0) or 0),
                "rows_quarantined": int(train_sum.get("rows_quarantined", 0) or 0),
                "rows_in_writer_payload": links_to_write,
                "result": "ok" if train_build["returncode"] == 0 else "failed",
                "block_reason": str(train_sum.get("block_reason", "")),
            })
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"blocked_training_build_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            if links_to_write > 0:
                train_write = _run(
                    [
                        sys.executable, "-m", "src.deal_analyzer.training_materials.cli", "write",
                        "--config", str(args.config),
                        "--run-dir", str(train_run),
                        "--plan-sheet", str(args.plan_sheet),
                        "--write", "--strict-preflight",
                    ],
                    root,
                    max(args.timeout, 7200),
                )
                train_status = _read_json(Path(train_run) / "training_materials_writer_status.json", {}) if train_run else {}
                train_written = int(train_status.get("rows_written", 0) or 0)
                summary["steps"].append({
                    "name": f"training_write_{label}",
                    **train_write,
                    "run_dir": train_run,
                    "rows_written": train_written,
                    "result": "ok" if train_write["returncode"] == 0 else "failed",
                    "block_reason": str(train_status.get("block_reason", "")),
                })
                if train_written > 0 and str(args.plan_sheet) not in summary["sheets_changed"]:
                    summary["sheets_changed"].append(str(args.plan_sheet))
                if summary["steps"][-1]["result"] != "ok":
                    summary["status"] = f"blocked_training_write_{label}"
                    summary["finished_at"] = _now()
                    _write_json(run_dir / "overnight_summary.json", summary)
                    _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                    print(str(run_dir))
                    return
            else:
                summary["steps"].append({"name": f"training_write_{label}", "command": "", "result": "skipped", "block_reason": "nothing_to_write", "rows_written": 0, "run_dir": train_run})
    
            if not do_manager:
                if label == "week1":
                    mgr_clear_skip = _safe_clear_by_week(
                        client,
                        sid,
                        str(args.manager_summary_sheet),
                        {(week_start, week_end)},
                        ["Неделя с"],
                        ["Неделя по"],
                    )
                    summary["clear_ops"].append({"sheet": args.manager_summary_sheet, "op": f"clear_skipped_{label}", **mgr_clear_skip})
                    if not mgr_clear_skip.get("safe"):
                        summary["status"] = f"blocked_unsafe_clear_unavailable_weekly_manager_{label}"
                        summary["finished_at"] = _now()
                        _write_json(run_dir / "overnight_summary.json", summary)
                        _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                        print(str(run_dir))
                        return
                    ws_clear_skip = _safe_clear_by_week(
                        client,
                        sid,
                        str(args.week_summary_sheet),
                        {(week_start, week_end)},
                        ["Неделя с"],
                        ["Неделя по"],
                    )
                    summary["clear_ops"].append({"sheet": args.week_summary_sheet, "op": f"clear_skipped_{label}", **ws_clear_skip})
                    if not ws_clear_skip.get("safe"):
                        summary["status"] = f"blocked_unsafe_clear_unavailable_week_summary_{label}"
                        summary["finished_at"] = _now()
                        _write_json(run_dir / "overnight_summary.json", summary)
                        _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                        print(str(run_dir))
                        return
                summary["steps"].append({"name": f"weekly_manager_{label}", "command": "", "result": "skipped", "block_reason": "policy_skip", "rows_written": 0})
                summary["steps"].append({"name": f"week_summary_{label}", "command": "", "result": "skipped", "block_reason": "policy_skip", "rows_written": 0})
                continue
    
            heartbeat_state["current_step"] = f"weekly_manager_build_{label}"
            t_mgr = time.time()
            mgr_build = _run(
                [
                    sys.executable, "-m", "src.deal_analyzer.weekly_manager_summary.cli", "build",
                    "--config", str(args.config),
                    "--period-start", week_start,
                    "--period-end", week_end,
                    "--daily-sheet", str(args.daily_sheet),
                    "--plan-sheet", str(args.plan_sheet),
                    "--target-sheet", str(args.manager_summary_sheet),
                    "--main-model", "deepseek-v4-pro:cloud",
                    "--fallback-model", "deepseek-v4-flash:cloud",
                    "--dry-run",
                ],
                root,
                args.timeout,
            )
            mgr_run = _extract_run_dir(mgr_build, root / "workspace" / "weekly_manager_summary", t_mgr)
            mgr_sum = _read_json(Path(mgr_run) / "summary.json", {}) if mgr_run else {}
            mgr_rows = int(mgr_sum.get("rows_in_writer_payload", 0) or 0)
            summary["steps"].append({
                "name": f"weekly_manager_build_{label}",
                **mgr_build,
                "run_dir": mgr_run,
                "rows_prepared": int(mgr_sum.get("rows_prepared", 0) or 0),
                "rows_in_writer_payload": mgr_rows,
                "rows_quarantined": int(mgr_sum.get("rows_quarantined", 0) or 0),
                "result": "ok" if (mgr_build["returncode"] == 0 and mgr_rows > 0) else "failed",
                "block_reason": "rows_empty" if mgr_rows <= 0 else "",
            })
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"blocked_weekly_manager_build_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            heartbeat_state["current_step"] = f"clear_weekly_manager_{label}"
            mgr_clear = _safe_clear_by_week(
                client,
                sid,
                str(args.manager_summary_sheet),
                {(week_start, week_end)},
                ["Неделя с"],
                ["Неделя по"],
            )
            summary["clear_ops"].append({"sheet": args.manager_summary_sheet, "op": f"clear_{label}", **mgr_clear})
            if not mgr_clear.get("safe"):
                summary["status"] = f"blocked_unsafe_clear_unavailable_weekly_manager_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            mgr_write = _run(
                [
                    sys.executable, "-m", "src.deal_analyzer.weekly_manager_summary.cli", "write",
                    "--config", str(args.config),
                    "--run-dir", str(mgr_run),
                    "--target-sheet", str(args.manager_summary_sheet),
                    "--write", "--strict-preflight", "--allow-partial-write", "--quarantine-unrepaired",
                ],
                root,
                args.timeout,
            )
            mgr_status = _read_json(Path(mgr_run) / "weekly_manager_writer_status.json", {}) if mgr_run else {}
            mgr_written = int(mgr_status.get("rows_written", 0) or 0)
            summary["steps"].append({
                "name": f"weekly_manager_write_{label}",
                **mgr_write,
                "run_dir": mgr_run,
                "rows_written": mgr_written,
                "result": "ok" if (mgr_write["returncode"] == 0 and mgr_written > 0) else "failed",
                "block_reason": str(mgr_status.get("block_reason", "")),
            })
            if mgr_written > 0 and str(args.manager_summary_sheet) not in summary["sheets_changed"]:
                summary["sheets_changed"].append(str(args.manager_summary_sheet))
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"critical_partial_write_failure_weekly_manager_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            heartbeat_state["current_step"] = f"week_summary_build_{label}"
            t_ws = time.time()
            ws_build = _run(
                [
                    sys.executable, "-m", "src.deal_analyzer.week_summary.cli", "build",
                    "--config", str(args.config),
                    "--period-start", week_start,
                    "--period-end", week_end,
                    "--daily-sheet", str(args.daily_sheet),
                    "--plan-sheet", str(args.plan_sheet),
                    "--manager-summary-sheet", str(args.manager_summary_sheet),
                    "--target-sheet", str(args.week_summary_sheet),
                    "--main-model", "deepseek-v4-pro:cloud",
                    "--fallback-model", "deepseek-v4-flash:cloud",
                    "--dry-run",
                ],
                root,
                args.timeout,
            )
            ws_run = _extract_run_dir(ws_build, root / "workspace" / "week_summary", t_ws)
            ws_sum = _read_json(Path(ws_run) / "summary.json", {}) if ws_run else {}
            ws_rows = int(ws_sum.get("rows_in_writer_payload", 0) or 0)
            summary["steps"].append({
                "name": f"week_summary_build_{label}",
                **ws_build,
                "run_dir": ws_run,
                "rows_prepared": int(ws_sum.get("rows_prepared", 0) or 0),
                "rows_in_writer_payload": ws_rows,
                "rows_quarantined": int(ws_sum.get("rows_quarantined", 0) or 0),
                "result": "ok" if (ws_build["returncode"] == 0 and ws_rows > 0) else "failed",
                "block_reason": "rows_empty" if ws_rows <= 0 else "",
            })
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"blocked_week_summary_build_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            heartbeat_state["current_step"] = f"clear_week_summary_{label}"
            ws_clear = _safe_clear_by_week(
                client,
                sid,
                str(args.week_summary_sheet),
                {(week_start, week_end)},
                ["Неделя с"],
                ["Неделя по"],
            )
            summary["clear_ops"].append({"sheet": args.week_summary_sheet, "op": f"clear_{label}", **ws_clear})
            if not ws_clear.get("safe"):
                summary["status"] = f"blocked_unsafe_clear_unavailable_week_summary_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
            ws_write = _run(
                [
                    sys.executable, "-m", "src.deal_analyzer.week_summary.cli", "write",
                    "--config", str(args.config),
                    "--run-dir", str(ws_run),
                    "--target-sheet", str(args.week_summary_sheet),
                    "--write", "--strict-preflight", "--allow-partial-write", "--quarantine-unrepaired",
                ],
                root,
                args.timeout,
            )
            ws_status = _read_json(Path(ws_run) / "week_summary_writer_status.json", {}) if ws_run else {}
            ws_written = int(ws_status.get("rows_written", 0) or 0)
            summary["steps"].append({
                "name": f"week_summary_write_{label}",
                **ws_write,
                "run_dir": ws_run,
                "rows_written": ws_written,
                "result": "ok" if (ws_write["returncode"] == 0 and ws_written > 0) else "failed",
                "block_reason": str(ws_status.get("block_reason", "")),
            })
            if ws_written > 0 and str(args.week_summary_sheet) not in summary["sheets_changed"]:
                summary["sheets_changed"].append(str(args.week_summary_sheet))
            if summary["steps"][-1]["result"] != "ok":
                summary["status"] = f"critical_partial_write_failure_week_summary_{label}"
                summary["finished_at"] = _now()
                _write_json(run_dir / "overnight_summary.json", summary)
                _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
                print(str(run_dir))
                return
    
        summary["status"] = "completed"
        summary["real_write_completed"] = True
        summary["finished_at"] = _now()
        _write_json(run_dir / "overnight_summary.json", summary)
        _write_md(run_dir / "OVERNIGHT_REPORT.md", "Overnight Report", _summary_lines(summary))
        print(str(run_dir))
    finally:
        heartbeat_state["current_step"] = "stopped"
        heartbeat_state["elapsed_seconds"] = int(time.time() - wall_start)
        _write_json(run_dir / "heartbeat.log", {"timestamp": _now(), **heartbeat_state})
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=2)


if __name__ == "__main__":
    _main()


