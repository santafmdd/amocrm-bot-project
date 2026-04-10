"""Browser package namespace.

Keep __init__ import-light so unit tests can run without Playwright installed.
"""

__all__ = [
    "AmoAnalyticsReader",
    "AnalyticsFlow",
    "AnalyticsFlowInput",
    "AnalyticsSnapshot",
    "BrowserSession",
    "BrowserSettings",
    "StageCount",
    "load_browser_settings",
]
