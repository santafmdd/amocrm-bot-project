from __future__ import annotations


def main() -> None:
    # Lazy import avoids runpy warning when invoked as `python -m ...week_summary.cli`.
    from .cli import main as _main

    _main()


__all__ = ["main"]
