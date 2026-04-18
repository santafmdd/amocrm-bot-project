from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from .retention import ArtifactInfo, RetentionPlan, bytes_to_human


def build_report(*, plans: list[RetentionPlan], generated_at: datetime | None = None) -> dict[str, Any]:
    now = generated_at or datetime.now(timezone.utc)
    total_size = sum(p.total_size_bytes for p in plans)
    total_reclaim = sum(p.reclaimable_bytes for p in plans)
    total_files = sum(p.total_files for p in plans)
    deletable_files = sum(len(p.candidates) for p in plans)

    grouped_by_dir: list[dict[str, Any]] = []
    grouped_by_category: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "targets": 0,
        "total_files": 0,
        "total_size_bytes": 0,
        "deletable_files": 0,
        "reclaimable_bytes": 0,
    })
    ext_counter: Counter[str] = Counter()
    age_buckets: Counter[str] = Counter()
    all_files: list[ArtifactInfo] = []

    for plan in plans:
        category = getattr(plan, "category", "other")
        grouped_by_dir.append(
            {
                "target": plan.target_name,
                "category": category,
                "path": str(plan.target_path),
                "files_total": plan.total_files,
                "size_bytes": plan.total_size_bytes,
                "size_human": bytes_to_human(plan.total_size_bytes),
                "deletable_files": len(plan.candidates),
                "reclaimable_bytes": plan.reclaimable_bytes,
                "reclaimable_human": bytes_to_human(plan.reclaimable_bytes),
            }
        )

        cat = grouped_by_category[category]
        cat["targets"] += 1
        cat["total_files"] += plan.total_files
        cat["total_size_bytes"] += plan.total_size_bytes
        cat["deletable_files"] += len(plan.candidates)
        cat["reclaimable_bytes"] += plan.reclaimable_bytes

        all_files.extend(plan.candidates)
        for item in plan.candidates:
            ext_counter[item.extension or "<no_ext>"] += 1
            age_buckets[_age_bucket(item.modified_at, now)] += 1

    category_payload = {
        key: {
            **value,
            "total_size_human": bytes_to_human(value["total_size_bytes"]),
            "reclaimable_human": bytes_to_human(value["reclaimable_bytes"]),
        }
        for key, value in grouped_by_category.items()
    }

    top_biggest = sorted(all_files, key=lambda x: x.size_bytes, reverse=True)[:20]

    return {
        "generated_at": now.isoformat(),
        "summary": {
            "targets": len(plans),
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_human": bytes_to_human(total_size),
            "deletable_files": deletable_files,
            "reclaimable_bytes": total_reclaim,
            "reclaimable_human": bytes_to_human(total_reclaim),
        },
        "grouped_by_category": category_payload,
        "grouped_by_directory": grouped_by_dir,
        "grouped_by_file_type": dict(sorted(ext_counter.items(), key=lambda kv: kv[1], reverse=True)),
        "grouped_by_age_bucket": dict(age_buckets),
        "top_biggest_deletable_files": [
            {
                "path": str(item.path),
                "size_bytes": item.size_bytes,
                "size_human": bytes_to_human(item.size_bytes),
                "modified_at": item.modified_at.isoformat(),
            }
            for item in top_biggest
        ],
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Storage Janitor Report", ""]
    summary = report.get("summary", {}) if isinstance(report.get("summary"), dict) else {}
    lines.append(f"Generated at: {report.get('generated_at', '')}")
    lines.append(f"Total size: {summary.get('total_size_human', '')}")
    lines.append(f"Deletable files: {summary.get('deletable_files', 0)}")
    lines.append(f"Reclaimable: {summary.get('reclaimable_human', '')}")
    lines.append("")

    lines.append("## By category")
    for category, row in (report.get("grouped_by_category", {}) or {}).items():
        lines.append(
            f"- {category}: targets={row.get('targets')}, files={row.get('total_files')}, size={row.get('total_size_human')}, "
            f"deletable={row.get('deletable_files')}, reclaimable={row.get('reclaimable_human')}"
        )
    lines.append("")

    lines.append("## By directory")
    for row in report.get("grouped_by_directory", []):
        lines.append(
            f"- [{row.get('category')}] {row.get('target')}: files={row.get('files_total')}, size={row.get('size_human')}, "
            f"deletable={row.get('deletable_files')}, reclaimable={row.get('reclaimable_human')}"
        )
    lines.append("")

    lines.append("## Top biggest deletable files")
    for row in report.get("top_biggest_deletable_files", [])[:10]:
        lines.append(f"- {row.get('size_human')} {row.get('path')}")
    lines.append("")

    return "\n".join(lines).strip() + "\n"


def _age_bucket(modified_at: datetime, now: datetime) -> str:
    days = max(0.0, (now - modified_at).total_seconds() / 86400.0)
    if days < 1:
        return "<1d"
    if days < 7:
        return "1-7d"
    if days < 30:
        return "7-30d"
    return "30d+"
