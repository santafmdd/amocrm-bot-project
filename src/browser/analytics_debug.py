"""Debug artifact helpers for analytics flow."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from src.safety import ensure_inside_root


def debug_dir(exports_dir: Path, project_root: Path) -> Path:
    path = ensure_inside_root(exports_dir / "debug", project_root)
    path.mkdir(parents=True, exist_ok=True)
    return path


def debug_screenshot(
    screenshots_dir: Path,
    project_root: Path,
    logger: logging.Logger,
    page: Page,
    name: str,
    timeout_ms: int = 5000,
) -> Path | None:
    file_name = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    path = ensure_inside_root(screenshots_dir / file_name, project_root)
    try:
        page.screenshot(path=str(path), full_page=True, timeout=timeout_ms)
        logger.info("Screenshot saved: %s", path)
        return path
    except PlaywrightTimeoutError as exc:
        logger.warning("Screenshot skipped (timeout) step=%s error=%s", name, exc)
        return None
    except Exception as exc:
        logger.warning("Screenshot skipped (best-effort) step=%s error=%s", name, exc)
        return None


def save_debug_text(exports_dir: Path, project_root: Path, file_name: str, text: str) -> Path:
    path = ensure_inside_root(debug_dir(exports_dir, project_root) / file_name, project_root)
    path.write_text(text, encoding="utf-8")
    return path


def save_tag_input_resolution_debug(
    exports_dir: Path,
    project_root: Path,
    candidates: list[dict[str, object]],
    summary_lines: list[str],
) -> tuple[Path, Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = debug_dir(exports_dir, project_root)
    json_path = ensure_inside_root(base / f"tag_input_candidates_{stamp}.json", project_root)
    txt_path = ensure_inside_root(base / f"tag_input_resolution_{stamp}.txt", project_root)
    json_path.write_text(json.dumps(candidates, ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text("\n".join(summary_lines), encoding="utf-8")
    return json_path, txt_path


def save_external_agent_handoff_context(
    exports_dir: Path,
    project_root: Path,
    report_id: str,
    context: dict[str, object],
) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = ensure_inside_root(debug_dir(exports_dir, project_root) / f"{report_id}_external_agent_handoff_{stamp}.json", project_root)
    path.write_text(json.dumps(context, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def save_utm_click_debug_artifacts(
    exports_dir: Path,
    project_root: Path,
    report_id: str,
    row_container_html: str,
    candidates: list[dict[str, object]],
) -> tuple[Path, Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = debug_dir(exports_dir, project_root)
    html_path = ensure_inside_root(base / f"utm_row_container_{report_id}_{stamp}.html", project_root)
    json_path = ensure_inside_root(base / f"utm_click_candidates_{report_id}_{stamp}.json", project_root)
    html_path.write_text(row_container_html or "", encoding="utf-8")
    json_path.write_text(json.dumps(candidates, ensure_ascii=False, indent=2), encoding="utf-8")
    return html_path, json_path


def save_tag_holder_after_enter_artifacts(
    exports_dir: Path,
    project_root: Path,
    holder_html: str,
    chip_texts: list[str],
    target_value: str,
) -> tuple[Path, Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = debug_dir(exports_dir, project_root)
    html_path = ensure_inside_root(base / f"tag_holder_after_enter_{stamp}.html", project_root)
    txt_path = ensure_inside_root(base / f"tag_holder_after_enter_{stamp}.txt", project_root)
    html_path.write_text(holder_html, encoding="utf-8")
    txt_payload = [
        f"target_value={target_value}",
        f"chip_texts_after_enter={chip_texts}",
        f"holder_outer_html_after_enter={holder_html}",
    ]
    txt_path.write_text("\n".join(txt_payload), encoding="utf-8")
    return html_path, txt_path
