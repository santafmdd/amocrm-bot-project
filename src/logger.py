"""Logging setup for console + file outputs."""

from __future__ import annotations

import logging
from pathlib import Path


def setup_logging(logs_dir: Path, level: str = "INFO") -> logging.Logger:
    """Configure root logger and return project logger."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "project.log"

    log_level = getattr(logging, level.upper(), logging.INFO)

    # Reset handlers to avoid duplicate logs on repeated bootstrap runs.
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return logging.getLogger("project")
