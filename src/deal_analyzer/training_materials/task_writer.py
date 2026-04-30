from __future__ import annotations

from typing import Any

from .models import TrainingDraft


def build_post_training_task_payload(*, drafts: list[TrainingDraft]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for draft in drafts:
        out.append(
            {
                "row_number": draft.candidate.row_number,
                "recipient": draft.candidate.recipient,
                "plan_date": draft.candidate.plan_date,
                "task_title": draft.task_title,
                "task_material": draft.task_material,
                "idempotency_key": draft.candidate.idempotency_key,
                "topic_hash": draft.candidate.topic_hash,
            }
        )
    return out


def summarize_task_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "rows_total": len(rows),
        "rows_with_material": sum(1 for item in rows if str(item.get("task_material") or "").strip()),
    }
