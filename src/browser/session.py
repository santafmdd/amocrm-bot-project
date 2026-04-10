"""Safe Playwright session wrapper for read-only analytics capture."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright

from src.config import AppConfig
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class BrowserSettings:
    """Runtime browser settings loaded from environment variables."""

    base_url: str
    analytics_url: str
    headless: bool
    slow_mo_ms: int
    timeout_ms: int
    viewport_width: int
    viewport_height: int
    storage_state_path: Path
    screenshots_dir: Path
    exports_dir: Path
    browser_backend: str
    openclaw_cdp_url: str


VALID_BACKENDS = {"playwright_local", "openclaw_cdp"}


def _str_to_bool(value: str, default: bool = False) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _resolve_project_path(raw_value: str, project_root: Path) -> Path:
    candidate = Path(raw_value)
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return ensure_inside_root(candidate, project_root)


def load_browser_settings(config: AppConfig, browser_backend_override: str | None = None) -> BrowserSettings:
    """Build browser settings from environment and enforce safe project paths."""
    base_url = os.getenv("AMO_BASE_URL", "").strip()
    analytics_url = os.getenv("AMO_ANALYTICS_URL", "").strip()

    headless = _str_to_bool(os.getenv("AMO_HEADLESS", "false"), default=False)
    slow_mo_ms = int(os.getenv("AMO_SLOW_MO_MS", "300"))
    timeout_ms = int(os.getenv("AMO_TIMEOUT_MS", "30000"))
    viewport_width = int(os.getenv("AMO_VIEWPORT_WIDTH", "1600"))
    viewport_height = int(os.getenv("AMO_VIEWPORT_HEIGHT", "1200"))

    raw_backend = (browser_backend_override or os.getenv("BROWSER_BACKEND", "playwright_local")).strip().lower()
    browser_backend = raw_backend if raw_backend in VALID_BACKENDS else "playwright_local"
    openclaw_cdp_url = os.getenv("OPENCLAW_CDP_URL", "http://127.0.0.1:18800").strip()

    storage_state_path = _resolve_project_path(
        os.getenv("AMO_STORAGE_STATE_PATH", "workspace/browser_storage_state.json"),
        config.project_root,
    )
    screenshots_dir = _resolve_project_path(
        os.getenv("AMO_SCREENSHOTS_DIR", "workspace/screenshots"),
        config.project_root,
    )
    exports_dir = _resolve_project_path(
        os.getenv("AMO_EXPORTS_DIR", "exports"),
        config.project_root,
    )

    screenshots_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path.parent.mkdir(parents=True, exist_ok=True)

    return BrowserSettings(
        base_url=base_url,
        analytics_url=analytics_url,
        headless=headless,
        slow_mo_ms=slow_mo_ms,
        timeout_ms=timeout_ms,
        viewport_width=viewport_width,
        viewport_height=viewport_height,
        storage_state_path=storage_state_path,
        screenshots_dir=screenshots_dir,
        exports_dir=exports_dir,
        browser_backend=browser_backend,
        openclaw_cdp_url=openclaw_cdp_url,
    )


class BrowserSession:
    """Owns Playwright browser context and provides a page for read-only operations."""

    def __init__(self, settings: BrowserSettings) -> None:
        self.settings = settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._selected_page: Page | None = None
        self._owns_context = False
        self._logger = logging.getLogger("project")

    def __enter__(self) -> "BrowserSession":
        self._playwright = sync_playwright().start()

        if self.settings.browser_backend == "openclaw_cdp":
            self._enter_openclaw_cdp()
        else:
            self._enter_playwright_local()

        return self

    def _enter_playwright_local(self) -> None:
        self._logger.info("Browser backend: playwright_local")
        assert self._playwright is not None

        browser = self._playwright.chromium.launch(
            headless=self.settings.headless,
            slow_mo=self.settings.slow_mo_ms,
            args=["--start-maximized"],
        )
        self._browser = browser

        context_options: dict[str, object] = {}
        if self.settings.storage_state_path.exists():
            context_options["storage_state"] = str(self.settings.storage_state_path)

        if self.settings.headless:
            context_options["viewport"] = {
                "width": self.settings.viewport_width,
                "height": self.settings.viewport_height,
            }
            self._logger.info(
                "Browser mode=headless no_viewport=False viewport=%sx%s",
                self.settings.viewport_width,
                self.settings.viewport_height,
            )
        else:
            context_options["no_viewport"] = True
            self._logger.info("Browser mode=headful no_viewport=True launch_arg=--start-maximized")

        self._context = browser.new_context(**context_options)
        self._context.set_default_timeout(self.settings.timeout_ms)
        self._owns_context = True

    def _probe_openclaw_cdp(self) -> None:
        probe_urls = [
            self.settings.openclaw_cdp_url.rstrip("/"),
            self.settings.openclaw_cdp_url.rstrip("/") + "/json/version",
        ]
        for probe_url in probe_urls:
            try:
                with urlopen(probe_url, timeout=2.0) as response:
                    _ = response.read(512)
                return
            except Exception:
                continue

        raise RuntimeError(
            "OpenClaw CDP browser is not running. Start OpenClaw browser profile first: "
            "openclaw browser --browser-profile openclaw start"
        )

    def _enter_openclaw_cdp(self) -> None:
        self._logger.info("Browser backend: openclaw_cdp")
        self._logger.info("OpenClaw CDP URL: %s", self.settings.openclaw_cdp_url)
        assert self._playwright is not None

        self._probe_openclaw_cdp()

        try:
            browser = self._playwright.chromium.connect_over_cdp(self.settings.openclaw_cdp_url)
        except Exception as exc:
            raise RuntimeError(
                "OpenClaw CDP browser is not running. Start OpenClaw browser profile first: "
                "openclaw browser --browser-profile openclaw start"
            ) from exc

        self._browser = browser
        contexts = list(browser.contexts)
        self._logger.info("OpenClaw CDP connected: contexts_found=%s", len(contexts))

        if contexts:
            self._context = contexts[0]
            self._owns_context = False
        else:
            self._context = browser.new_context()
            self._owns_context = True

        assert self._context is not None
        self._context.set_default_timeout(self.settings.timeout_ms)

        pages = list(self._context.pages)
        self._logger.info("OpenClaw CDP pages_found=%s", len(pages))

        if pages:
            candidate_urls: list[str] = []
            for idx, page in enumerate(pages):
                url = self._safe_page_url(page)
                candidate_urls.append(f"[{idx}] {url or '<empty>'}")
            self._logger.info("OpenClaw CDP page candidates: %s", " | ".join(candidate_urls))

        chosen = self._select_openclaw_page(pages)
        if chosen is None:
            self._logger.warning(
                "OpenClaw CDP did not find suitable existing page after filtering. Creating new page in default context."
            )
            chosen = self._context.new_page()

        self._selected_page = chosen
        self._logger.info("OpenClaw CDP selected_page_url=%s", self._safe_page_url(self._selected_page))

    def _safe_page_url(self, page: Page) -> str:
        try:
            return (page.url or "").strip()
        except Exception:
            return ""

    def _is_excluded_openclaw_url(self, url: str) -> tuple[bool, str]:
        lowered = url.strip().lower()
        if not lowered:
            return True, "empty_url"
        if lowered.startswith("about:blank"):
            return True, "about_blank"
        for prefix in ("devtools://", "chrome://", "chrome-extension://", "about:"):
            if lowered.startswith(prefix):
                return True, f"excluded_scheme:{prefix}"
        return False, "allowed"

    def _is_amocrm_target_url(self, url: str) -> bool:
        lowered = url.strip().lower()
        return "officeistockinfo.amocrm.ru" in lowered

    def _is_http_candidate_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    def _select_openclaw_page(self, pages: list[Page]) -> Page | None:
        if not pages:
            return None

        accepted: list[tuple[Page, str]] = []
        rejected_logs: list[str] = []

        for idx, page in enumerate(pages):
            url = self._safe_page_url(page)
            excluded, reason = self._is_excluded_openclaw_url(url)
            if excluded:
                rejected_logs.append(f"[{idx}] url={url or '<empty>'} rejected={reason}")
                continue
            accepted.append((page, url))

        if rejected_logs:
            self._logger.info("OpenClaw CDP page candidates rejected: %s", " | ".join(rejected_logs))

        if not accepted:
            return None

        for idx, (_page, url) in enumerate(accepted):
            if self._is_amocrm_target_url(url):
                self._logger.info(
                    "OpenClaw CDP page selected by amoCRM priority: accepted_idx=%s url=%s",
                    idx,
                    url,
                )
                return _page

        for idx, (_page, url) in enumerate(accepted):
            if self._is_http_candidate_url(url):
                self._logger.info(
                    "OpenClaw CDP page selected by http/https fallback: accepted_idx=%s url=%s",
                    idx,
                    url,
                )
                return _page

        fallback_page, fallback_url = accepted[0]
        self._logger.info(
            "OpenClaw CDP page selected by accepted fallback: url=%s",
            fallback_url,
        )
        return fallback_page

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        if self._context is not None and self._owns_context:
            try:
                self._context.storage_state(path=str(self.settings.storage_state_path))
            except Exception:
                pass
            self._context.close()

        if self._browser is not None:
            self._browser.close()

        if self._playwright is not None:
            self._playwright.stop()

    def new_page(self) -> Page:
        """Create or return a browser page depending on backend."""
        if self._context is None:
            raise RuntimeError("Browser session is not started. Use context manager.")

        if self._selected_page is not None:
            return self._selected_page

        return self._context.new_page()

    def pages(self) -> list[Page]:
        """Return currently opened pages from active context."""
        if self._context is None:
            raise RuntimeError("Browser session is not started. Use context manager.")
        return list(self._context.pages)
