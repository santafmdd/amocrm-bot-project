from __future__ import annotations

from pathlib import Path
from typing import Any

from ..weekly_shared.artifacts import write_json as _write_json
from ..weekly_shared.artifacts import write_markdown as _write_markdown


def write_json(path: Path, payload: Any) -> None:
    _write_json(path, payload)


def write_markdown(path: Path, *, title: str, lines: list[str]) -> None:
    _write_markdown(path, title=title, lines=lines)
