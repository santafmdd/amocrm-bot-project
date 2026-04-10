"""Configuration and path management for the local automation project."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    """Simple app configuration container."""

    project_root: Path
    workspace_dir: Path
    exports_dir: Path
    logs_dir: Path
    prompts_dir: Path
    docs_dir: Path
    tests_dir: Path


def load_config() -> AppConfig:
    """Load .env and build absolute project paths."""
    # We load environment variables from .env (if the file exists).
    load_dotenv()

    # Project root is the parent of the current `src` directory.
    project_root = Path(__file__).resolve().parent.parent

    return AppConfig(
        project_root=project_root,
        workspace_dir=project_root / "workspace",
        exports_dir=project_root / "exports",
        logs_dir=project_root / "logs",
        prompts_dir=project_root / "prompts",
        docs_dir=project_root / "docs",
        tests_dir=project_root / "tests",
    )
