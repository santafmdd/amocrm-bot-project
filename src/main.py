"""Project bootstrap entrypoint."""

from __future__ import annotations

import os
from pathlib import Path

from src.config import load_config
from src.logger import setup_logging
from src.safety import ensure_inside_root, ensure_project_structure


def main() -> None:
    """Load config, run safety checks, and write bootstrap log message."""
    config = load_config()

    # Extra safety: current working directory must stay inside project root.
    ensure_inside_root(config.project_root, config.project_root)
    ensure_inside_root(Path(os.getcwd()), config.project_root)
    ensure_project_structure(config)

    logger = setup_logging(config.logs_dir, level=os.getenv("LOG_LEVEL", "INFO"))
    logger.info("project bootstrap ok")


if __name__ == "__main__":
    main()
