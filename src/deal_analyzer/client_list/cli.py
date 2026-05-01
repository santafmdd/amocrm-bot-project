from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from src.config import load_config
from src.logger import setup_logging

from ..config import load_deal_analyzer_config
from .artifacts import write_json
from .normalizer import build_header_mapping, normalize_client_rows
from .prioritizer import build_manager_client_context, build_priority_summary
from .reader import discover_client_list_sheet, read_client_list_sheet


def _new_run_dir(project_root: Path) -> Path:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = project_root / "workspace" / "client_list" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Client list integration CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover client list mapping")
    discover.add_argument("--config", required=True)
    discover.add_argument("--sheet-name", default="")
    discover.add_argument("--spreadsheet-id", default="")

    build_context = sub.add_parser("build-context", help="Build manager client context")
    build_context.add_argument("--config", required=True)
    build_context.add_argument("--manager", required=True)
    build_context.add_argument("--period-start", required=True)
    build_context.add_argument("--period-end", required=True)
    build_context.add_argument("--sheet-name", default="")
    build_context.add_argument("--spreadsheet-id", default="")
    build_context.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def _run_discover(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)

    discovery = discover_client_list_sheet(
        cfg=cfg,
        logger=logger,
        spreadsheet_id=str(args.spreadsheet_id or "").strip(),
        sheet_name=str(args.sheet_name or "").strip(),
    )
    write_json(run_dir / "client_list_discovery.json", discovery)
    print(str(run_dir))


def _run_build_context(args: argparse.Namespace) -> None:
    cfg = load_deal_analyzer_config(str(args.config))
    app_cfg = load_config()
    logger = setup_logging(app_cfg.logs_dir, "INFO")
    run_dir = _new_run_dir(app_cfg.project_root)

    snapshot = read_client_list_sheet(
        cfg=cfg,
        logger=logger,
        spreadsheet_id=str(args.spreadsheet_id or "").strip(),
        sheet_name=str(args.sheet_name or "").strip(),
    )
    mapping = build_header_mapping(snapshot.headers, cfg=cfg)
    rows, rejected = normalize_client_rows(
        headers=snapshot.headers,
        rows=snapshot.rows,
        mapping=mapping,
        header_row_number=snapshot.header_row_number,
    )
    context = build_manager_client_context(
        rows=rows,
        manager_name=str(args.manager or "").strip(),
        period_start=str(args.period_start),
        period_end=str(args.period_end),
        manager_role_registry=getattr(cfg, "manager_role_registry", None),
        role_policy_registry=getattr(cfg, "role_policy_registry", None),
    )
    summary = build_priority_summary(rows)

    discovery = {
        "spreadsheet_id": snapshot.spreadsheet_id,
        "sheet_name": snapshot.sheet_name,
        "header_row_number": snapshot.header_row_number,
        "headers": snapshot.headers,
        "rows_total": len(snapshot.rows),
        "mapped_columns": {field: snapshot.headers[idx] for field, idx in mapping.items() if idx < len(snapshot.headers)},
    }
    write_json(run_dir / "client_list_discovery.json", discovery)
    write_json(run_dir / "client_list_rows_normalized.json", {"rows_total": len(rows), "rows": [asdict(item) for item in rows], "rejected_rows": rejected})
    write_json(run_dir / "client_list_priority_summary.json", summary)
    write_json(run_dir / "client_list_manager_context.json", asdict(context))
    write_json(
        run_dir / "summary.json",
        {
            "run_id": run_dir.name,
            "manager": str(args.manager or "").strip(),
            "period_start": str(args.period_start),
            "period_end": str(args.period_end),
            "rows_total": len(rows),
            "rows_rejected": len(rejected),
            "context_rows_total": context.rows_total,
            "categories": context.categories,
            "top_priority_items_count": len(context.top_priority_items),
            "dry_run": bool(args.dry_run),
        },
    )
    print(str(run_dir))


def main() -> None:
    args = _parse_args()
    if args.command == "discover":
        _run_discover(args)
        return
    if args.command == "build-context":
        _run_build_context(args)
        return
    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
