from __future__ import annotations

from typing import Any

from ..daily_control.source_reader import clean_text


def col_letter(index: int) -> str:
    out = ""
    value = max(1, int(index))
    while value:
        value, remainder = divmod(value - 1, 26)
        out = chr(65 + remainder) + out
    return out


def row_is_occupied(row: list[str], key_indexes: list[int]) -> bool:
    return any(idx < len(row) and clean_text(row[idx]) for idx in key_indexes)


def group_contiguous_row_items(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda item: int(item.get("row_number", 0) or 0))
    groups: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    previous = 0
    for item in sorted_rows:
        row_number = int(item.get("row_number", 0) or 0)
        if row_number <= 0:
            continue
        if not current:
            current = [item]
            previous = row_number
            continue
        if row_number == previous + 1:
            current.append(item)
        else:
            groups.append(current)
            current = [item]
        previous = row_number
    if current:
        groups.append(current)
    return groups
