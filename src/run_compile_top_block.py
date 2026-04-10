"""CLI entrypoint: compile top-block CSV from all/active/closed snapshot JSON files."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.config import load_config
from src.logger import setup_logging
from src.safety import ensure_inside_root, ensure_project_structure
from src.write_top_block import compile_top_block_csv


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compile top-block CSV from amoCRM analytics snapshots (all/active/closed). "
            "This step is read-only and does not write into Google Sheets yet."
        )
    )
    parser.add_argument("--all-json", default=None, help="Path to ALL tab snapshot JSON")
    parser.add_argument("--active-json", default=None, help="Path to ACTIVE tab snapshot JSON")
    parser.add_argument("--closed-json", default=None, help="Path to CLOSED tab snapshot JSON")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    config = load_config()
    ensure_inside_root(Path(os.getcwd()), config.project_root)
    ensure_project_structure(config)

    logger = setup_logging(config.logs_dir, level=os.getenv("LOG_LEVEL", "INFO"))
    logger.info("Starting top-block compile step")

    result = compile_top_block_csv(
        exports_dir=config.exports_dir,
        project_root=config.project_root,
        all_json=args.all_json,
        active_json=args.active_json,
        closed_json=args.closed_json,
        logger=logger,
    )

    logger.info("Compiled file created: %s", result.output_csv_path)
    logger.info(
        "Snapshot sources: all=%s active=%s closed=%s",
        result.snapshot_paths["all"],
        result.snapshot_paths["active"],
        result.snapshot_paths["closed"],
    )
    logger.info("Rows written: %s", result.rows_count)


if __name__ == "__main__":
    main()
