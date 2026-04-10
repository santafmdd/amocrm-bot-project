"""Common filter handler interface for analytics browser flow."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class FilterDebugContext:
    artifacts: dict[str, str] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)


class FilterHandler(Protocol):
    name: str

    def resolve(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> Any:
        ...

    def apply(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        ...

    def verify(self, flow: Any, page: Any, report_id: str, values: list[str], operator: str = "=") -> bool:
        ...

    def debug_dump(self, flow: Any, page: Any, report_id: str, reason: str, extra: dict[str, Any] | None = None) -> FilterDebugContext:
        ...
