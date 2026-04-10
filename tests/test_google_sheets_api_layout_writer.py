import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.writers.google_sheets_api_layout_writer import GoogleSheetsApiLayoutWriter


class DummyCompiled:
    def __init__(self):
        self.filter_values = ["conf_novosib_mechanical_engineering_2026"]
        self.display_name = "analytics tag single"


def test_select_anchor_prefers_filter_value_match() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    compiled = DummyCompiled()
    layout = {"tag_block_aliases": ["conf_ufa_stroyka_2026", "conf_novosib_mechanical_engineering_2026"]}
    anchors = [
        {"dsl_row": 2, "header_row": 3, "dsl_text": "conf_ufa_stroyka_2026: ...", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
        {"dsl_row": 20, "header_row": 21, "dsl_text": "conf_novosib_mechanical_engineering_2026: ...", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
    ]
    selected = writer._select_anchor(anchors=anchors, compiled_result=compiled, layout=layout)
    assert selected is not None
    assert selected["dsl_row"] == 20




def test_select_anchor_by_target_dsl_row() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    compiled = DummyCompiled()
    layout = {"tag_block_aliases": []}
    anchors = [
        {"dsl_row": 2, "header_row": 3, "dsl_text": "block one", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
        {"dsl_row": 14, "header_row": 16, "dsl_text": "???? 2 utm exact", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
        {"dsl_row": 27, "header_row": 29, "dsl_text": "???? 3 utm prefix", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
    ]
    selected = writer._select_anchor(
        anchors=anchors,
        compiled_result=compiled,
        layout=layout,
        target_dsl_row=27,
    )
    assert selected is not None
    assert selected["dsl_row"] == 27


def test_select_anchor_by_target_dsl_text_contains() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    compiled = DummyCompiled()
    layout = {"tag_block_aliases": []}
    anchors = [
        {"dsl_row": 2, "header_row": 3, "dsl_text": "block one", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
        {"dsl_row": 14, "header_row": 16, "dsl_text": "???? 2 utm exact", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
        {"dsl_row": 27, "header_row": 29, "dsl_text": "???? 3 utm prefix", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
    ]
    selected = writer._select_anchor(
        anchors=anchors,
        compiled_result=compiled,
        layout=layout,
        target_dsl_text_contains="utm prefix",
    )
    assert selected is not None
    assert selected["dsl_row"] == 27


def test_select_anchor_default_behavior_unchanged_without_target_selector() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    compiled = DummyCompiled()
    layout = {"tag_block_aliases": ["conf_ufa_stroyka_2026", "conf_novosib_mechanical_engineering_2026"]}
    anchors = [
        {"dsl_row": 2, "header_row": 3, "dsl_text": "conf_ufa_stroyka_2026: ...", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
        {"dsl_row": 20, "header_row": 21, "dsl_text": "conf_novosib_mechanical_engineering_2026: ...", "stage_col": 1, "all_col": 2, "active_col": 3, "closed_col": 4},
    ]
    selected = writer._select_anchor(anchors=anchors, compiled_result=compiled, layout=layout)
    assert selected is not None
    assert selected["dsl_row"] == 20

def test_build_updates_for_stage_rows() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    stage_rows = [(10, "stage_a"), (11, "missing_stage")]
    pivot = {
        "stage_a": {"all": 10, "active": 7, "closed": 3},
    }
    updates, missing = writer._build_updates_for_stage_rows(
        tab_name="analytics_writer_test",
        stage_rows=stage_rows,
        pivot=pivot,
        all_col=2,
        active_col=3,
        closed_col=4,
    )
    assert len(updates) == 6
    assert "missing_stage" in missing
    assert any(x["range"] == "analytics_writer_test!B10" and x["value"] == 10 for x in updates)
    assert any(x["range"] == "analytics_writer_test!D11" and x["value"] == 0 for x in updates)


class _FakeClient:
    def get_values(self, spreadsheet_id: str, range_a1: str):
        # Rows start from header_row+1. Simulate block 1 rows 4..13, then next DSL/header block at 14+.
        return [
            ["stage_a", "10", "7", "3"],   # row 4
            ["stage_b", "8", "5", "3"],    # row 5
            ["stage_c", "6", "4", "2"],    # row 6
            ["stage_d", "5", "3", "2"],    # row 7
            ["stage_e", "4", "2", "2"],    # row 8
            ["stage_f", "3", "2", "1"],    # row 9
            ["stage_g", "2", "1", "1"],    # row 10
            ["stage_h", "2", "1", "1"],    # row 11
            ["stage_i", "1", "1", "0"],    # row 12
            ["stage_j", "1", "0", "1"],    # row 13
            ["???? 2 utm exact: ????=???????; ??????=?? ??? ?????", "", "", ""],  # row 14 (must be excluded)
            ["??????", "??? (??)", "???????? (??)", "???????? (??)"],             # row 15
            ["???????? ?? (????)", "", "", ""],                                  # row 16
            ["??????", "", "", ""],                                               # row 17
        ]

def test_read_stage_rows_for_anchor_stops_at_next_block_boundary() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    writer.client = _FakeClient()  # type: ignore[assignment]

    pivot_keys = {f"stage_{ch}" for ch in "abcdefghij"}
    result = writer._read_stage_rows_for_anchor(
        spreadsheet_id="spreadsheet",
        tab_name="analytics_writer_test",
        header_row=3,
        stage_col=1,
        all_col=2,
        active_col=3,
        closed_col=4,
        row_count=200,
        pivot_keys=pivot_keys,
        next_anchor_dsl_row=14,
    )

    # Hard boundary from next anchor must limit first block to rows 4..13 only.
    assert result["stage_rows"] == [
        (4, "stage_a"),
        (5, "stage_b"),
        (6, "stage_c"),
        (7, "stage_d"),
        (8, "stage_e"),
        (9, "stage_f"),
        (10, "stage_g"),
        (11, "stage_h"),
        (12, "stage_i"),
        (13, "stage_j"),
    ]
    assert result["stop_reason"] in {"hard_upper_bound", "hard_upper_bound_reached"}
    assert result["last_included_row"] == 13
    assert result["first_excluded_row"] in {None, 14}
    assert result["next_anchor_dsl_row"] == 14
    assert result["hard_row_upper_bound"] == 13
    assert result["rows_considered_range"] == "4..13"


def test_planned_updates_are_bounded_by_next_anchor() -> None:
    writer = GoogleSheetsApiLayoutWriter(project_root=Path('.'))
    writer.client = _FakeClient()  # type: ignore[assignment]

    pivot = {f"stage_{ch}": {"all": 1, "active": 1, "closed": 0} for ch in "abcdefghij"}
    boundary = writer._read_stage_rows_for_anchor(
        spreadsheet_id="spreadsheet",
        tab_name="analytics_writer_test",
        header_row=3,
        stage_col=1,
        all_col=2,
        active_col=3,
        closed_col=4,
        row_count=200,
        pivot_keys=set(pivot.keys()),
        next_anchor_dsl_row=14,
    )
    updates, missing = writer._build_updates_for_stage_rows(
        tab_name="analytics_writer_test",
        stage_rows=boundary["stage_rows"],
        pivot=pivot,
        all_col=2,
        active_col=3,
        closed_col=4,
    )

    ranges = {u["range"] for u in updates}
    assert "analytics_writer_test!B4" in ranges
    assert "analytics_writer_test!D13" in ranges
    assert "analytics_writer_test!B14" not in ranges
    assert "analytics_writer_test!D14" not in ranges
    assert not any("14" in r or "15" in r or "16" in r for r in ranges)
    assert missing == []
