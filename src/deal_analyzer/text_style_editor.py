from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .config import DealAnalyzerConfig
from .daily_control.style.deterministic_cleaner import (
    NARRATIVE_FIELDS_DAILY,
    clean_rows as clean_daily_rows,
)
from .daily_control.style.llm_rewriter import rewrite_rows_with_llm
from .daily_control.style.rewrite_guard import validate_rewrite_row
from .daily_control.validation.text_lint import lint_daily_text_rows, lint_has_blockers


CALL_REVIEW_NARRATIVE_FIELDS: tuple[str, ...] = (
    "Комментарий по этапу (секретарь)",
    "Комментарий по этапу (лпр)",
    "Комментарий по этапу (актуальность и потребность)",
    "Комментарий по этапу (презентация встречи)",
    "Комментарий по этапу (закрытие на встречу)",
    "Комментарий по этапу (отработка возражений)",
    "Комментарий по этапу (чистота речи)",
    "Комментарий по этапу (работа с црм)",
    "Комментарий по этапу (дисциплина дозвонов)",
    "Комментарий по этапу (подтверждение презентации)",
    "Комментарий по этапу (презентация)",
    "Комментарий по этапу (работа с тестом)",
    "Комментарий по этапу (дожим / кп)",
    "Ключевой вывод",
    "Сильная сторона",
    "Зона роста",
    "Почему это важно",
    "Что закрепить",
    "Что исправить",
    "Что донести сотруднику",
    "Эффект количество / неделя",
    "Эффект качество",
)

PROTECTED_FIELDS: dict[str, tuple[str, ...]] = {
    "daily_control": (
        "period_start",
        "period_end",
        "control_day_date",
        "day_label",
        "manager_name",
        "manager_role_profile",
        "deals_count",
        "calls_count",
        "deal_ids",
        "deal_links",
        "product_mix",
        "base_mix",
        "score_0_100",
        "criticality",
    ),
    "call_review": (
        "Deal ID",
        "Сделка",
        "Менеджер",
        "Дата кейса",
        "Прослушанные звонки",
        "Ссылка на сделку",
        "База / тег",
        "Продукт / фокус",
        "Оценка 0-100",
    ),
}


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _narrative_fields_for_mode(mode: str) -> tuple[str, ...]:
    if mode == "daily_control":
        return NARRATIVE_FIELDS_DAILY
    if mode == "call_review":
        return CALL_REVIEW_NARRATIVE_FIELDS
    raise RuntimeError(f"Unsupported style editor mode: {mode}")


def _technical_cleanup_row(*, mode: str, row: dict[str, Any]) -> dict[str, Any]:
    fields = _narrative_fields_for_mode(mode)
    updated = dict(row)
    for field in fields:
        if field in updated:
            updated[field] = _clean_text(updated.get(field, ""))
    return updated


def lint_daily_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return lint_daily_text_rows(rows)


def daily_text_lint_failed(lint: dict[str, Any]) -> bool:
    return lint_has_blockers(lint)


def _style_runtime_candidates(
    *,
    cfg: DealAnalyzerConfig,
    llm_runtime: dict[str, Any] | None,
    llm_model_override: str | None,
    llm_timeout_override: int | None,
) -> list[dict[str, Any]]:
    timeout_override = int(llm_timeout_override or 0) if llm_timeout_override else 0
    if llm_model_override:
        model = str(llm_model_override).strip()
        if not model:
            return []
        main_timeout = timeout_override or int(cfg.ollama_timeout_seconds or 60)
        return [
            {
                "name": "main",
                "model": model,
                "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
                "timeout_seconds": max(1, main_timeout),
            }
        ]

    runtime = dict(llm_runtime or {})
    order = ["main", "fallback"] if str(runtime.get("selected") or "") != "fallback" else ["fallback", "main"]
    candidates: list[dict[str, Any]] = []
    for name in order:
        node = runtime.get(name, {}) if isinstance(runtime.get(name), dict) else {}
        model = str(node.get("model") or "").strip()
        base_url = str(node.get("base_url") or "").strip()
        timeout = int(node.get("timeout_seconds") or timeout_override or cfg.ollama_timeout_seconds or 60)
        enabled = bool(node.get("enabled", True))
        if name == "fallback" and not enabled:
            continue
        if not model or not base_url:
            continue
        candidates.append(
            {
                "name": name,
                "model": model,
                "base_url": base_url,
                "timeout_seconds": max(1, timeout),
            }
        )
    return candidates


def _guard_call_review_rewrite(original: dict[str, Any], candidate: dict[str, Any], fields: tuple[str, ...]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    for field in PROTECTED_FIELDS["call_review"]:
        if str(original.get(field, "")) != str(candidate.get(field, "")):
            errors.append(f"protected_field_changed:{field}")
    for field in fields:
        before = _clean_text(original.get(field, ""))
        after = _clean_text(candidate.get(field, ""))
        if not after:
            continue
        if "```" in after:
            errors.append(f"markdown_fence:{field}")
        if len(before) > 20 and len(after) > int(len(before) * 1.3):
            errors.append(f"length_growth_gt_30pct:{field}")
    return len(errors) == 0, errors


def edit_rows(
    *,
    mode: str,
    rows: list[dict[str, Any]],
    run_id: str,
    project_root: Path,
    enable_llm_editor: bool,
    cfg: DealAnalyzerConfig | None,
    llm_runtime: dict[str, Any] | None,
    logger: Any | None,
    batch_limit: int = 3,
    llm_model_override: str | None = None,
    llm_timeout_override: int | None = None,
    llm_row_limit: int | None = None,
) -> dict[str, Any]:
    fields = _narrative_fields_for_mode(mode)
    style_dir = project_root / "workspace" / "style_editor" / run_id
    style_dir.mkdir(parents=True, exist_ok=True)

    original_rows = [dict(r) for r in rows if isinstance(r, dict)]

    if mode == "daily_control":
        cleaned_rows, cleanup_counts = clean_daily_rows(original_rows, fields=NARRATIVE_FIELDS_DAILY)
    else:
        cleaned_rows = [_technical_cleanup_row(mode=mode, row=row) for row in original_rows]
        cleanup_counts = {}

    final_rows = [dict(r) for r in cleaned_rows]
    rejected: list[dict[str, Any]] = []
    llm_rows_used = 0
    llm_rows_failed = 0
    llm_rows_by_model: dict[str, int] = {}
    llm_error_examples: list[str] = []

    if enable_llm_editor and cfg is not None:
        candidates = _style_runtime_candidates(
            cfg=cfg,
            llm_runtime=llm_runtime,
            llm_model_override=llm_model_override,
            llm_timeout_override=llm_timeout_override,
        )
        batch_size = max(1, min(3, int(batch_limit or 3)))
        max_rows = len(final_rows)
        if llm_row_limit is not None:
            max_rows = max(0, min(max_rows, int(llm_row_limit)))

        for start in range(0, max_rows, batch_size):
            chunk_indexes = list(range(start, min(start + batch_size, max_rows)))
            batch = []
            for idx in chunk_indexes:
                batch.append(
                    {
                        "row_index": idx,
                        "fields": {field: str(final_rows[idx].get(field, "") or "") for field in fields if field in final_rows[idx]},
                    }
                )

            chunk_ok = False
            chunk_error = ""
            chunk_candidate_rows = final_rows
            for candidate in candidates:
                rewritten_rows, debug = rewrite_rows_with_llm(
                    base_url=str(candidate.get("base_url") or ""),
                    model=str(candidate.get("model") or ""),
                    timeout_seconds=int(candidate.get("timeout_seconds") or cfg.ollama_timeout_seconds or 60),
                    mode=mode,
                    rows=batch,
                    fields=fields,
                )
                if not bool(debug.get("ok", False)):
                    chunk_error = str(debug.get("error") or "")
                    continue
                chunk_ok = True
                chunk_candidate_rows = [dict(item) for item in final_rows]
                for item in rewritten_rows:
                    if not isinstance(item, dict):
                        continue
                    idx = int(item.get("row_index", -1))
                    if idx < 0 or idx >= len(chunk_candidate_rows):
                        continue
                    fields_payload = item.get("fields", {}) if isinstance(item.get("fields"), dict) else {}
                    for field in fields:
                        if field in fields_payload:
                            chunk_candidate_rows[idx][field] = _clean_text(fields_payload.get(field, ""))

                llm_rows_used += len(chunk_indexes)
                model_name = str(candidate.get("model") or "")
                llm_rows_by_model[model_name] = int(llm_rows_by_model.get(model_name, 0) or 0) + len(chunk_indexes)
                break

            if not chunk_ok:
                llm_rows_failed += len(chunk_indexes)
                if chunk_error and len(llm_error_examples) < 10:
                    llm_error_examples.append(f"chunk={start}: {chunk_error}")
                if logger is not None and chunk_error:
                    logger.warning("style editor chunk failed mode=%s chunk=%s error=%s", mode, start, chunk_error)
                continue

            for idx in chunk_indexes:
                candidate_row = chunk_candidate_rows[idx]
                if mode == "daily_control":
                    ok, errors = validate_rewrite_row(
                        original=final_rows[idx],
                        candidate=candidate_row,
                        narrative_fields=NARRATIVE_FIELDS_DAILY,
                    )
                else:
                    ok, errors = _guard_call_review_rewrite(final_rows[idx], candidate_row, fields)
                if ok:
                    final_rows[idx] = candidate_row
                else:
                    rejected.append({"row_index": idx, "errors": errors})

    metrics = {
        "mode": mode,
        "rows_total": len(final_rows),
        "rows_cleaned": len(cleaned_rows),
        "llm_enabled": bool(enable_llm_editor),
        "llm_rows_used": llm_rows_used,
        "llm_rows_failed": llm_rows_failed,
        "llm_rows_by_model": llm_rows_by_model,
        "llm_error_examples": llm_error_examples,
        "cleanup_counts": cleanup_counts,
        "rejected_rewrites_count": len(rejected),
    }
    if mode == "daily_control":
        metrics["daily_text_lint"] = lint_daily_rows(final_rows)

    diff_lines = ["# Style Editor Diff", ""]
    for idx, (before, after) in enumerate(zip(original_rows, final_rows, strict=False)):
        if not isinstance(before, dict) or not isinstance(after, dict):
            continue
        for field in fields:
            before_val = str(before.get(field, "") or "")
            after_val = str(after.get(field, "") or "")
            if before_val != after_val:
                diff_lines.append(f"- row={idx} field={field}")
                diff_lines.append(f"  before: {before_val[:300]}")
                diff_lines.append(f"  after: {after_val[:300]}")

    (style_dir / "style_editor_input.json").write_text(
        json.dumps({"mode": mode, "rows": original_rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (style_dir / "style_editor_output.json").write_text(
        json.dumps({"mode": mode, "rows": final_rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (style_dir / "style_editor_metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (style_dir / "rejected_rewrites.json").write_text(
        json.dumps({"rejected": rejected}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (style_dir / "style_editor_diff.md").write_text("\n".join(diff_lines).strip() + "\n", encoding="utf-8")

    return {
        "rows": final_rows,
        "metrics": metrics,
        "style_dir": str(style_dir),
    }
