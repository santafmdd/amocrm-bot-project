from __future__ import annotations

import json
from typing import Any

from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError


def build_style_messages(*, mode: str, rows: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, str]]:
    system = (
        "Отредактируй только язык в user-facing полях. "
        "Не меняй факты, даты, id, ссылки, числа, имена менеджеров. "
        "Не добавляй новых фактов. "
        "Верни только JSON без markdown: {\"rows\":[{\"row_index\":0,\"fields\":{...}}]}."
    )
    user = json.dumps({"mode": mode, "fields": list(fields), "rows": rows}, ensure_ascii=False)
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def rewrite_rows_with_llm(
    *,
    base_url: str,
    model: str,
    timeout_seconds: int,
    mode: str,
    rows: list[dict[str, Any]],
    fields: tuple[str, ...],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    client = OllamaClient(base_url=base_url, model=model, timeout_seconds=max(1, int(timeout_seconds or 60)))
    parsed_rows = list(rows)
    debug = {"ok": False, "error": "", "model": model, "rows_used": 0, "repair_used": False}
    try:
        parsed = client.chat_json(messages=build_style_messages(mode=mode, rows=rows, fields=fields))
        payload = parsed.payload if isinstance(parsed.payload, dict) else {}
        output_rows = payload.get("rows", []) if isinstance(payload.get("rows"), list) else []
        by_index: dict[int, dict[str, Any]] = {}
        for item in output_rows:
            if not isinstance(item, dict):
                continue
            idx = int(item.get("row_index", -1))
            fields_payload = item.get("fields", {}) if isinstance(item.get("fields"), dict) else {}
            by_index[idx] = fields_payload
        for idx, values in by_index.items():
            if 0 <= idx < len(parsed_rows):
                for field in fields:
                    if field in values:
                        parsed_rows[idx][field] = str(values.get(field, "") or "").strip()
        debug["ok"] = True
        debug["rows_used"] = len(rows)
        debug["repair_used"] = bool(parsed.repair_applied)
        return parsed_rows, debug
    except (OllamaClientError, ValueError, TypeError) as exc:
        debug["error"] = str(exc)
        return parsed_rows, debug
