"""Writer abstraction for compiled analytics result."""

from __future__ import annotations

from typing import Protocol

from playwright.sync_api import Page

from src.writers.models import CompiledProfileAnalyticsResult, WriterDestinationConfig


class ProfileAnalyticsWriter(Protocol):
    """Minimal writer contract for profile analytics result."""

    def write_profile_analytics_result(
        self,
        page: Page,
        compiled_result: CompiledProfileAnalyticsResult,
        destination: WriterDestinationConfig,
    ) -> None:
        """Write compiled profile result to destination."""
        ...
