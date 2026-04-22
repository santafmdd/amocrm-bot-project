from __future__ import annotations

import re
from typing import Any

BLOCK_KEYS = [
    "key_takeaway",
    "strong_sides",
    "growth_zones",
    "why_important",
    "reinforce",
    "fix_action",
    "coaching_list",
    "expected_quantity",
    "expected_quality",
]

BLOCK_TO_COLUMN = {
    "key_takeaway": "Ключевой вывод",
    "strong_sides": "Сильные стороны",
    "growth_zones": "Зоны роста",
    "why_important": "Почему это важно",
    "reinforce": "Что закрепить",
    "fix_action": "Что исправить",
    "coaching_list": "Что донес сотруднику",
    "expected_quantity": "Ожидаемый эффект - количество",
    "expected_quality": "Ожидаемый эффект - качество",
}


def parse_blocks_markdown(markdown: str) -> dict[str, str]:
    lines = str(markdown or "").splitlines()
    out: dict[str, list[str]] = {k: [] for k in BLOCK_KEYS}
    current: str | None = None
    header = re.compile(r"^\s*###\s+([a-z_]+)\s*$")
    for ln in lines:
        m = header.match(ln.strip())
        if m:
            key = m.group(1).strip()
            current = key if key in out else None
            continue
        if current is not None:
            out[current].append(ln)
    return {k: "\n".join(v).strip() for k, v in out.items()}


def validate_blocks(blocks: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for key in BLOCK_KEYS:
        if not " ".join(str(blocks.get(key) or "").split()).strip():
            missing.append(key)
    return missing


def assemble_writer_columns(blocks: dict[str, Any]) -> dict[str, str]:
    return {column: " ".join(str(blocks.get(key) or "").split()).strip() for key, column in BLOCK_TO_COLUMN.items()}

