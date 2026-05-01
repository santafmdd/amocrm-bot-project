from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .models import EmployeeDashboardSummary


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_markdown(path: Path, *, title: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(line for line in lines if str(line).strip())
    path.write_text(f"# {title}\n\n{body}\n", encoding="utf-8")


def build_summary_markdown_lines(summary: EmployeeDashboardSummary) -> list[str]:
    payload = asdict(summary)

    def _bullets(items: tuple[str, ...] | list[str], *, fallback: str = "нет данных") -> list[str]:
        values = [str(item).strip() for item in items if str(item).strip()]
        if not values:
            return [f"- {fallback}"]
        return [f"- {value}" for value in values]

    lines: list[str] = [
        f"employee: `{summary.employee_name}`",
        f"role: `{summary.role or 'unknown'}`",
        f"period: `{summary.period_start}..{summary.period_end}`",
        f"confidence_score: `{summary.confidence_score}`",
        f"evidence_count: `{summary.evidence_count}`",
        f"source_coverage_passed: `{summary.source_coverage_passed}`",
        "",
        "## Strengths",
        *_bullets(summary.strengths),
        "",
        "## Growth Zones",
        *_bullets(summary.growth_zones),
        "",
        "## Successful Speech Modules",
        *_bullets(summary.successful_speech_modules),
        "",
        "## Failed Speech Modules",
        *_bullets(summary.failed_speech_modules),
        "",
        "## Behavior Patterns",
        *_bullets(summary.behavior_patterns),
        "",
        "## Recurring Mistakes",
        *_bullets(summary.recurring_mistakes),
        "",
        "## Recommended Training Topics",
        *_bullets(summary.recommended_training_topics),
        "",
        "## Source Coverage",
    ]
    for key, value in sorted(payload.get("source_coverage", {}).items()):
        lines.append(f"- {key}: {value}")
    return lines


def write_dashboard_artifacts(
    *,
    run_dir: Path,
    summary: EmployeeDashboardSummary,
    evidence_index: dict[str, Any],
    speech_debug: dict[str, Any],
    objection_and_patterns_debug: dict[str, Any],
) -> None:
    write_json(run_dir / "employee_dashboard_summary.json", asdict(summary))
    write_markdown(
        run_dir / "employee_dashboard_summary.md",
        title="Employee Dashboard Summary",
        lines=build_summary_markdown_lines(summary),
    )
    write_json(run_dir / "speech_modules_debug.json", speech_debug)
    write_json(
        run_dir / "objection_patterns_debug.json",
        {
            "objections": objection_and_patterns_debug.get("objection_patterns", {}),
            "patterns": objection_and_patterns_debug.get("behavior_patterns", {}),
        },
    )
    write_json(run_dir / "evidence_index.json", evidence_index)
