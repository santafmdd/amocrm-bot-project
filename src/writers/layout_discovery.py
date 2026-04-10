"""Layout block discovery helpers for stage tables."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LayoutBlockDiscovery:
    title_text: str
    title_row: int
    title_col: int
    header_row: int
    stage_col: int
    all_col: int
    active_col: int
    closed_col: int


def norm(text: str) -> str:
    return " ".join((text or "").strip().lower().replace("ё", "е").split())


def discover_stage_blocks_from_matrix(matrix: list[list[str]]) -> list[LayoutBlockDiscovery]:
    """Discover stage-like blocks in a plain text matrix (for tests/offline logic)."""
    blocks: list[LayoutBlockDiscovery] = []
    row_count = len(matrix)
    col_count = max((len(r) for r in matrix), default=0)

    stage_headers = {"этап", "статус"}
    all_headers = {"все", "все (шт)", "все шт"}
    active_headers = {"активные", "активные (шт)", "активные шт"}
    closed_headers = {"закрытые", "закрытые (шт)", "закрытые шт"}

    for r in range(row_count):
        row_vals = [norm(matrix[r][c] if c < len(matrix[r]) else "") for c in range(col_count)]
        stage_col = next((idx for idx, v in enumerate(row_vals) if v in stage_headers), -1)
        all_col = next((idx for idx, v in enumerate(row_vals) if v in all_headers), -1)
        active_col = next((idx for idx, v in enumerate(row_vals) if v in active_headers), -1)
        closed_col = next((idx for idx, v in enumerate(row_vals) if v in closed_headers), -1)
        if min(stage_col, all_col, active_col, closed_col) < 0:
            continue

        # find nearest non-empty title row above header
        title_row = r
        title_col = 0
        title_text = ""
        for rr in range(max(0, r - 4), r):
            for cc in range(min(8, col_count)):
                value = (matrix[rr][cc] if cc < len(matrix[rr]) else "").strip()
                if value:
                    title_row = rr
                    title_col = cc
                    title_text = value
                    break
            if title_text:
                break

        blocks.append(
            LayoutBlockDiscovery(
                title_text=title_text,
                title_row=title_row,
                title_col=title_col,
                header_row=r,
                stage_col=stage_col + 1,
                all_col=all_col + 1,
                active_col=active_col + 1,
                closed_col=closed_col + 1,
            )
        )

    return blocks
