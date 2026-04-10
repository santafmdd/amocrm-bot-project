"""CLI entrypoint for read-only amoCRM analytics capture MVP."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.browser.amo_reader import AmoAnalyticsReader
from src.browser.models import AnalyticsSnapshot, TabMode
from src.browser.session import BrowserSession, load_browser_settings
from src.config import load_config
from src.logger import setup_logging
from src.safety import ensure_inside_root, ensure_project_structure


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read current amoCRM analytics screen and export JSON/CSV. "
            "MVP is read-only and does not write anything into amoCRM."
        )
    )
    parser.add_argument("--source-kind", choices=["tag", "utm_source"], default="tag")
    parser.add_argument("--filter-id", default="manual")
    parser.add_argument("--tab-mode", choices=["all", "active", "closed"], default="all")
    parser.add_argument(
        "--all-tab-modes",
        action="store_true",
        help=(
            "Read all tab modes sequentially (all -> active -> closed) from one prepared screen. "
            "When enabled, --tab-mode is ignored."
        ),
    )
    parser.add_argument(
        "--all-tab-modes-manual",
        action="store_true",
        help=(
            "Manual sequential mode: user switches tabs by hand (all -> active -> closed), "
            "reader captures after each Enter."
        ),
    )
    parser.add_argument(
        "--use-open-page",
        action="store_true",
        help=(
            "If set, reader tries to use already opened browser page in this session. "
            "Otherwise it opens analytics URL from AMO_ANALYTICS_URL."
        ),
    )
    parser.add_argument(
        "--wait-for-enter",
        action="store_true",
        help=(
            "Pause before reading so user can login, open analytics screen, "
            "set filters manually, then press Enter."
        ),
    )
    parser.add_argument(
        "--skip-open",
        action="store_true",
        help=(
            "Do not auto-open AMO_ANALYTICS_URL. Useful when user wants to "
            "navigate manually and keep UI state."
        ),
    )
    return parser


def _wait_for_manual_ready() -> None:
    """Pause CLI flow so user can manually prepare amoCRM analytics screen."""
    print()
    print("Manual preparation mode:")
    print("1) If needed, login to amoCRM in opened browser window.")
    print("2) Open the required analytics screen.")
    print("3) Set tab and filters manually.")
    print("4) Return to terminal and press Enter to continue.")
    input("Press Enter to read current screen... ")


def _wait_for_next_manual_tab(tab_mode: TabMode) -> None:
    """Ask user to switch tab manually and confirm by Enter."""
    if tab_mode == "active":
        input("Переключите вручную вкладку на АКТИВНЫЕ и нажмите Enter... ")
    elif tab_mode == "closed":
        input("Переключите вручную вкладку на ЗАКРЫТЫЕ и нажмите Enter... ")


def _export_and_log_snapshot(
    reader: AmoAnalyticsReader,
    logger,
    snapshot: AnalyticsSnapshot,
    exported_tabs: list[str],
) -> None:
    """Export one snapshot immediately and write compact logs."""
    json_path, csv_path = reader.export_snapshot(snapshot)
    exported_tabs.append(snapshot.tab_mode)
    logger.info(
        "Saved tab=%s stages=%s total_count=%s parse_method=%s",
        snapshot.tab_mode,
        len(snapshot.stages),
        snapshot.total_count,
        snapshot.parse_method,
    )
    logger.info(
        "Exports for tab=%s: json=%s csv=%s screenshot=%s",
        snapshot.tab_mode,
        json_path,
        csv_path,
        snapshot.screenshot_path,
    )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    config = load_config()
    ensure_inside_root(Path(os.getcwd()), config.project_root)
    ensure_project_structure(config)

    logger = setup_logging(config.logs_dir, level=os.getenv("LOG_LEVEL", "INFO"))
    settings = load_browser_settings(config)

    logger.info("Starting read-only amoCRM analytics reader MVP")
    logger.info("MVP note: current version reads current screen only; filters can be set manually in UI")

    if args.all_tab_modes and args.all_tab_modes_manual:
        logger.warning("Both --all-tab-modes and --all-tab-modes-manual are set. Manual mode will be used.")

    with BrowserSession(settings) as session:
        page = session.pages()[0] if args.use_open_page and session.pages() else session.new_page()

        reader = AmoAnalyticsReader(settings=settings, project_root=config.project_root)
        if not args.skip_open and not args.use_open_page:
            reader.open_analytics_page(page)
        elif args.skip_open:
            logger.info("Skip-open mode enabled. User can navigate manually before reading.")

        if args.wait_for_enter:
            logger.info("Wait-for-enter mode enabled. You can finish manual screen preparation before reading.")
            _wait_for_manual_ready()

        if args.all_tab_modes_manual:
            logger.info("All-tab-modes-manual run started: user switches tabs manually (all -> active -> closed)")
            exported_tabs: list[str] = []
            snapshots: list[AnalyticsSnapshot] = []

            manual_order: tuple[TabMode, ...] = ("all", "active", "closed")
            for tab_mode in manual_order:
                if tab_mode in ("active", "closed"):
                    _wait_for_next_manual_tab(tab_mode)

                logger.info("Manual tab read started: tab=%s", tab_mode)
                snapshot = reader.read_current_view(
                    page=page,
                    source_kind=args.source_kind,
                    filter_id=args.filter_id,
                    tab_mode=tab_mode,
                )
                snapshots.append(snapshot)
                _export_and_log_snapshot(reader, logger, snapshot, exported_tabs)

            logger.info("All-tab-modes-manual finished. successful_tabs=%s/%s", len(snapshots), 3)
            logger.info("Exported tabs: %s", exported_tabs)

        elif args.all_tab_modes:
            logger.info("All-tab-modes run started: URL-based deals_type switching (all -> active -> closed)")

            exported_tabs: list[str] = []

            def _export_snapshot_now(snapshot: AnalyticsSnapshot) -> None:
                _export_and_log_snapshot(reader, logger, snapshot, exported_tabs)

            snapshots = reader.read_all_tab_modes_by_url(
                page=page,
                source_kind=args.source_kind,
                filter_id=args.filter_id,
                on_snapshot=_export_snapshot_now,
            )

            logger.info("All-tab-modes finished. successful_tabs=%s/%s", len(snapshots), 3)
            logger.info("Exported tabs: %s", exported_tabs)
            if len(snapshots) < 3:
                logger.warning(
                    "All-tab-modes (URL-based) ended early. Already exported files remain in exports/. "
                    "Check previous tab-switch error logs for failed tab."
                )
        else:
            snapshot = reader.read_current_view(
                page=page,
                source_kind=args.source_kind,
                filter_id=args.filter_id,
                tab_mode=args.tab_mode,
            )
            json_path, csv_path = reader.export_snapshot(snapshot)

            logger.info(
                "Read complete. right_panel_stages=%s top_cards=%s total_count=%s parse_method=%s",
                len(snapshot.stages),
                len(snapshot.top_cards),
                snapshot.total_count,
                snapshot.parse_method,
            )
            logger.info("Exports: json=%s csv=%s screenshot=%s", json_path, csv_path, snapshot.screenshot_path)
            logger.info("Debug dumps: text=%s selectors=%s", snapshot.debug_text_path, snapshot.debug_selectors_path)


if __name__ == "__main__":
    main()

