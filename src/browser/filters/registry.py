"""Registry for browser filter handlers."""

from __future__ import annotations

from typing import Any

from .date_filter import DateFilterHandler
from .manager_filter import ManagerFilterHandler
from .pipeline_filter import PipelineFilterHandler
from .tag_filter import TagFilterHandler
from .utm_filter import UTMFilterHandler


class FilterRegistry:
    def __init__(self) -> None:
        self._handlers = {
            "tag": TagFilterHandler(),
            "utm_source": UTMFilterHandler(mode="exact"),
            "utm_exact": UTMFilterHandler(mode="exact"),
            "utm_prefix": UTMFilterHandler(mode="prefix"),
            "pipeline": PipelineFilterHandler(),
            "date": DateFilterHandler(),
            "manager": ManagerFilterHandler(),
        }

    def get(self, key: str) -> Any:
        return self._handlers.get(str(key).strip().lower())

    def require(self, key: str) -> Any:
        handler = self.get(key)
        if handler is None:
            raise RuntimeError(f"Unsupported filter handler: {key}")
        return handler

    def keys(self) -> list[str]:
        return sorted(self._handlers.keys())
