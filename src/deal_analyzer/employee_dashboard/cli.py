from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from ..config import load_deal_analyzer_config
from .aggregator import build_employee_dashboard
from .artifacts import write_dashboard_artifacts, write_json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Employee dashboard / coaching intelligence")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="Build employee dashboard artifacts")
    build.add_argument("--config", required=True)
    build.add_argument("--period-start", required=True)
    build.add_argument("--period-end", required=True)
    build.add_argument("--employee", required=True)
    build.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def _new_run_dir(project_root: Path, employee: str) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = "_".join(part for part in employee.lower().split() if part)
    run_dir = project_root / "workspace" / "employee_dashboard" / f"{run_id}_{slug or 'employee'}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _run_build(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root, str(args.employee or ""))

    logger.info(
        "employee_dashboard build employee=%s period=%s..%s dry_run=%s",
        args.employee,
        args.period_start,
        args.period_end,
        bool(args.dry_run),
    )

    summary, evidence_index, speech_debug, objection_and_patterns_debug = build_employee_dashboard(
        project_root=app_cfg.project_root,
        employee_name=str(args.employee or "").strip(),
        period_start=str(args.period_start or ""),
        period_end=str(args.period_end or ""),
    )

    write_dashboard_artifacts(
        run_dir=run_dir,
        summary=summary,
        evidence_index=evidence_index,
        speech_debug=speech_debug,
        objection_and_patterns_debug=objection_and_patterns_debug,
    )

    runtime_summary: dict[str, Any] = {
        "status": "completed",
        "dry_run": bool(args.dry_run),
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "employee": summary.employee_name,
        "role": summary.role,
        "period_start": summary.period_start,
        "period_end": summary.period_end,
        "confidence_score": summary.confidence_score,
        "evidence_count": summary.evidence_count,
        "source_coverage": summary.source_coverage,
        "source_coverage_passed": summary.source_coverage_passed,
        "strengths_count": len(summary.strengths),
        "growth_zones_count": len(summary.growth_zones),
        "successful_speech_modules_count": len(summary.successful_speech_modules),
        "failed_speech_modules_count": len(summary.failed_speech_modules),
        "objection_success_count": len(summary.objection_success),
        "objection_failures_count": len(summary.objection_failures),
        "recommended_training_topics_count": len(summary.recommended_training_topics),
        "llm_involved": False,
        "integration_hooks": {
            "week_plan_context_path": str(run_dir / "employee_dashboard_summary.json"),
            "training_materials_context_path": str(run_dir / "employee_dashboard_summary.json"),
            "ui_context_path": str(run_dir / "employee_dashboard_summary.md"),
        },
        "config_path": str(cfg.config_path),
    }
    write_json(run_dir / "summary.json", runtime_summary)

    print(str(run_dir))
    print(json.dumps(runtime_summary, ensure_ascii=False, indent=2))


def main() -> None:
    args = _parse_args()
    if args.command == "build":
        _run_build(args)
        return
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
