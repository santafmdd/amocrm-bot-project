from __future__ import annotations

import argparse
import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from .call_review_v3_builder import replay_call_review_payload_preflight
from .cli import _maybe_write_call_review_sheet
from .config import load_deal_analyzer_config


REPLAY_NARRATIVE_COLUMNS: tuple[str, ...] = (
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


def _clean_narrative_text(text: str, counters: dict[str, int]) -> str:
    value = str(text or "")
    if not value.strip():
        return value

    value, n = re.subn(
        r"Базовый\s+как\s+действовать\s+квалификац(?:ии|ию|ией|ия)\b",
        "базовая схема квалификации",
        value,
        flags=re.IGNORECASE,
    )
    counters["base_qual_phrase"] = int(counters.get("base_qual_phrase", 0) or 0) + int(n or 0)

    direct_replacements = (
        ("как действовать открытых вопросов", "как отрабатывать открытые вопросы"),
        ("как действовать фиксации фактов", "как отрабатывать фиксацию фактов"),
        ("как действовать квалификации", "как отрабатывать квалификацию"),
    )
    for src, dst in direct_replacements:
        local_n = value.lower().count(src)
        if local_n > 0:
            value = re.sub(re.escape(src), dst, value, flags=re.IGNORECASE)
            counters["specific_kak_deystvovat"] = int(counters.get("specific_kak_deystvovat", 0) or 0) + local_n

    value, n = re.subn(
        r"\bкак\s+действовать\b",
        "как отрабатывать",
        value,
        flags=re.IGNORECASE,
    )
    counters["generic_kak_deystvovat"] = int(counters.get("generic_kak_deystvovat", 0) or 0) + int(n or 0)

    value, n = re.subn(
        r"(?:^|[\s;,.])Вместо\s+нужно:\s*-\s*",
        " ",
        value,
        flags=re.IGNORECASE,
    )
    counters["vmesto_nuzhno_dash_removed"] = int(counters.get("vmesto_nuzhno_dash_removed", 0) or 0) + int(n or 0)

    value, n = re.subn(
        r"Лучше:\s*-\s*([^;.\n]+)",
        r"Лучше так: \1",
        value,
        flags=re.IGNORECASE,
    )
    counters["better_dash_with_tail"] = int(counters.get("better_dash_with_tail", 0) or 0) + int(n or 0)

    value, n = re.subn(r"Лучше:\s*-\s*", " ", value, flags=re.IGNORECASE)
    counters["better_dash_removed"] = int(counters.get("better_dash_removed", 0) or 0) + int(n or 0)

    value, n = re.subn(r"\b1\)\s*1\.", "1)", value, flags=re.IGNORECASE)
    counters["double_list_marker"] = int(counters.get("double_list_marker", 0) or 0) + int(n or 0)

    value, n = re.subn(
        r"\bчетк[а-яё]*\s+фраз[а-яё]*\b",
        "готовая формулировка",
        value,
        flags=re.IGNORECASE,
    )
    counters["chetkaya_fraza_label"] = int(counters.get("chetkaya_fraza_label", 0) or 0) + int(n or 0)

    value, n = re.subn(
        r"\bЛучше\s+Лучше\s+сказать\b",
        "Лучше сказать",
        value,
        flags=re.IGNORECASE,
    )
    counters["double_better_marker"] = int(counters.get("double_better_marker", 0) or 0) + int(n or 0)

    value, n = re.subn(r"[ \t]{2,}", " ", value)
    counters["extra_spaces"] = int(counters.get("extra_spaces", 0) or 0) + int(n or 0)
    value, n = re.subn(r"\s+([;:,.])", r"\1", value)
    counters["space_before_punct"] = int(counters.get("space_before_punct", 0) or 0) + int(n or 0)
    value, n = re.subn(r";{2,}", ";", value)
    counters["double_semicolon"] = int(counters.get("double_semicolon", 0) or 0) + int(n or 0)

    return value.strip()


def _cleanup_replay_payload_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    counters: dict[str, int] = {}
    cleaned_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        cleaned = dict(row)
        for col in REPLAY_NARRATIVE_COLUMNS:
            raw = cleaned.get(col)
            if raw is None:
                continue
            cleaned[col] = _clean_narrative_text(str(raw), counters)
        cleaned_rows.append(cleaned)
    counters["rows_processed"] = len(cleaned_rows)
    counters["columns_processed"] = len(REPLAY_NARRATIVE_COLUMNS)
    return cleaned_rows, counters


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay call review payload preflight/writer without rerunning STT/LLM."
    )
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Path to existing period run folder that already contains call_review_sheet_payload.json",
    )
    parser.add_argument(
        "--config",
        default="config/deal_analyzer.call_review.deepseek.realwrite.json",
        help="Deal analyzer config path. Used for writer settings.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Enable actual sheet write after successful semantic preflight.",
    )
    parser.add_argument(
        "--dry-run-writer",
        action="store_true",
        help="Force writer dry-run even when --write is passed.",
    )
    parser.add_argument(
        "--strict-preflight",
        action="store_true",
        help="Fail command when semantic preflight is not passed.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = Path(str(args.run_dir)).resolve()
    payload_path = run_dir / "call_review_sheet_payload.json"
    debug_payload_path = run_dir / "call_review_debug_payload.json"
    if not payload_path.exists():
        raise FileNotFoundError(f"call review payload not found: {payload_path}")

    payload = _read_json(payload_path)
    debug_payload = _read_json(debug_payload_path) if debug_payload_path.exists() else {}
    source_rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
    rows_for_cleanup = [dict(x) for x in source_rows if isinstance(x, dict)]
    cleaned_rows, cleanup_counters = _cleanup_replay_payload_rows(rows_for_cleanup)
    payload_cleaned = dict(payload)
    payload_cleaned["rows"] = cleaned_rows
    payload_cleaned["rows_count"] = len(cleaned_rows)

    replay = replay_call_review_payload_preflight(payload=payload_cleaned, debug_payload=debug_payload)
    payload_for_writer = replay.get("payload_for_writer", {})
    semantic_preflight = replay.get("semantic_preflight", {})

    rechecked_payload_path = run_dir / "call_review_sheet_payload.rechecked.json"
    cleaned_payload_path = run_dir / "call_review_sheet_payload.cleaned.json"
    rechecked_semantic_path = run_dir / "semantic_preflight.recheck.json"
    replay_write_result_path = run_dir / "replay_write_result.json"
    replay_cleanup_path = run_dir / "replay_cleanup_counts.json"
    _write_json(cleaned_payload_path, payload_cleaned if isinstance(payload_cleaned, dict) else {})
    _write_json(rechecked_payload_path, payload_for_writer if isinstance(payload_for_writer, dict) else {})
    _write_json(rechecked_semantic_path, semantic_preflight if isinstance(semantic_preflight, dict) else {})
    _write_json(replay_cleanup_path, {"cleanup_counts": cleanup_counters})

    cfg = load_deal_analyzer_config(str(args.config))
    write_requested = bool(args.write and not args.dry_run_writer)
    preflight_passed = bool(
        semantic_preflight.get("passed", True) if isinstance(semantic_preflight, dict) else True
    )
    if write_requested and not preflight_passed:
        status = {
            "mode": "dry_run",
            "write_requested": True,
            "write_executed": False,
            "reason": "semantic_preflight_failed",
            "rows_prepared": int(payload_for_writer.get("rows_count", 0) or 0),
            "rows_written": 0,
            "cleanup_counts": cleanup_counters,
            "failed_rules": (
                semantic_preflight.get("failed_rules", [])
                if isinstance(semantic_preflight, dict)
                else []
            ),
            "warning_rules": (
                semantic_preflight.get("warning_rules", [])
                if isinstance(semantic_preflight, dict)
                else []
            ),
            "cleaned_payload_path": str(cleaned_payload_path),
            "rechecked_payload_path": str(rechecked_payload_path),
            "rechecked_semantic_preflight_path": str(rechecked_semantic_path),
            "replay_cleanup_counts_path": str(replay_cleanup_path),
        }
        _write_json(replay_write_result_path, status)
        raise SystemExit(
            "replay write blocked: semantic preflight failed. "
            f"See {rechecked_semantic_path} and {replay_write_result_path}"
        )

    writer_cfg = cfg
    if not write_requested:
        writer_cfg = replace(cfg, deal_analyzer_write_enabled=False)

    writer_status = _maybe_write_call_review_sheet(
        cfg=writer_cfg,
        logger=logger,
        call_review_payload=payload_for_writer if isinstance(payload_for_writer, dict) else {},
    )
    result = {
        "mode": "real_write" if write_requested else "dry_run",
        "write_requested": write_requested,
        "write_executed": bool(write_requested and str(writer_status.get("mode") or "") == "real_write"),
        "rows_prepared": int(writer_status.get("rows_prepared", 0) or 0),
        "rows_written": int(writer_status.get("rows_written", 0) or 0),
        "cleanup_counts": cleanup_counters,
        "writer_status": writer_status,
        "semantic_preflight_passed": preflight_passed,
        "failed_rules": (
            semantic_preflight.get("failed_rules", [])
            if isinstance(semantic_preflight, dict)
            else []
        ),
        "warning_rules": (
            semantic_preflight.get("warning_rules", [])
            if isinstance(semantic_preflight, dict)
            else []
        ),
        "cleaned_payload_path": str(cleaned_payload_path),
        "rechecked_payload_path": str(rechecked_payload_path),
        "rechecked_semantic_preflight_path": str(rechecked_semantic_path),
        "replay_cleanup_counts_path": str(replay_cleanup_path),
    }
    _write_json(replay_write_result_path, result)

    if args.strict_preflight and not preflight_passed:
        raise SystemExit(
            "semantic preflight failed in strict mode. "
            f"See {rechecked_semantic_path}"
        )


if __name__ == "__main__":
    main()
