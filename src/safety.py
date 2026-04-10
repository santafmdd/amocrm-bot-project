"""Safety helpers to keep all file operations inside the project directory."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from src.config import AppConfig


class SafetyError(Exception):
    """Raised when an unsafe path operation is detected."""


def ensure_inside_root(path: Path, root: Path) -> Path:
    """Resolve and verify that `path` is located inside `root`."""
    resolved_path = Path(path).resolve()
    resolved_root = root.resolve()

    if resolved_root == resolved_path or resolved_root in resolved_path.parents:
        return resolved_path

    raise SafetyError(f"Unsafe path outside project root: {resolved_path}")


def ensure_project_structure(config: AppConfig) -> None:
    """Create required directories and validate they are safe."""
    required_dirs: Iterable[Path] = (
        config.workspace_dir,
        config.exports_dir,
        config.logs_dir,
        config.prompts_dir,
        config.docs_dir,
        config.tests_dir,
    )

    for directory in required_dirs:
        safe_dir = ensure_inside_root(directory, config.project_root)
        safe_dir.mkdir(parents=True, exist_ok=True)


def safe_join(base_dir: Path, *parts: str, project_root: Path) -> Path:
    """Build a child path and assert it stays inside project root."""
    candidate = base_dir.joinpath(*parts)
    return ensure_inside_root(candidate, project_root)


def write_text_safe(base_dir: Path, relative_path: str, content: str, project_root: Path) -> Path:
    """Safely write UTF-8 text file under a controlled directory."""
    target = safe_join(base_dir, relative_path, project_root=project_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target


def read_text_safe(base_dir: Path, relative_path: str, project_root: Path) -> str:
    """Safely read UTF-8 text file under a controlled directory."""
    target = safe_join(base_dir, relative_path, project_root=project_root)
    return target.read_text(encoding="utf-8")
