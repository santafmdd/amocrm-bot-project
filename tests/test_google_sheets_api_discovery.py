import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.writers.google_sheets_api_layout_discovery import GoogleSheetsApiLayoutInspector
from src.writers.models import WriterDestinationConfig


class _FakeClient:
    def __init__(self, matrix):
        self.matrix = matrix

    def list_sheets(self, _spreadsheet_id):
        rows = len(self.matrix)
        cols = max(len(r) for r in self.matrix)
        return [{"title": "analytics_writer_test", "rowCount": rows, "columnCount": cols}]

    def get_values(self, spreadsheet_id: str, range_a1: str):
        # Parse A1 range minimalistically.
        import re

        m = re.search(r"!([A-Z]+)(\d+):([A-Z]+)(\d+)$", range_a1)
        assert m, range_a1
        c1, r1, c2, r2 = m.groups()
        r1 = int(r1)
        r2 = int(r2)

        def col_to_num(label: str) -> int:
            n = 0
            for ch in label:
                n = n * 26 + (ord(ch) - 64)
            return n

        c1n = col_to_num(c1)
        c2n = col_to_num(c2)
        out = []
        for rr in range(r1, r2 + 1):
            if rr - 1 >= len(self.matrix):
                out.append([""] * (c2n - c1n + 1))
                continue
            src = self.matrix[rr - 1]
            row = []
            for cc in range(c1n, c2n + 1):
                row.append(src[cc - 1] if cc - 1 < len(src) else "")
            out.append(row)
        return out

    @staticmethod
    def normalize_matrix(matrix, rows: int, cols: int):
        out = []
        for r in range(rows):
            row = matrix[r] if r < len(matrix) else []
            out.append([str(row[c]).strip() if c < len(row) else "" for c in range(cols)])
        return out


def _mk_destination() -> WriterDestinationConfig:
    return WriterDestinationConfig(
        kind="google_sheets_layout_ui",
        target_id="x",
        sheet_url="https://docs.google.com/spreadsheets/d/test/edit",
        tab_name="analytics_writer_test",
        write_mode="layout_anchor_update",
        start_cell="A1",
        layout_config={
            "api_scan_max_rows": 120,
            "api_scan_max_cols": 12,
            "api_scan_band_rows": 40,
            "api_scan_band_cols": 6,
            "api_empty_bands_stop": 0,
            "header_search_window": 10,
            "api_header_fallback_window": 120,
        },
    )


def test_discovery_maps_vertical_side_and_bottom_blocks() -> None:
    # 60x12 synthetic sheet.
    matrix = [["" for _ in range(12)] for _ in range(60)]

    # Block 1 (left-top)
    matrix[0][0] = "Тест 1: Даты=Созданы; Период=За все время; Теги=машэкспо"
    matrix[2][0] = "Этап"
    matrix[2][1] = "Все (шт)"
    matrix[2][2] = "Активные (шт)"
    matrix[2][3] = "Закрытые (шт)"
    matrix[3][0] = "stage_a"
    matrix[4][0] = "stage_b"

    # Block 2 (side table right)
    matrix[0][6] = "Тест 2: Даты=Созданы; Период=За все время; utm_source=conf_novo"
    matrix[2][6] = "Этап"
    matrix[2][7] = "Все (шт)"
    matrix[2][8] = "Активные (шт)"
    matrix[2][9] = "Закрытые (шт)"
    matrix[3][6] = "stage_c"
    matrix[4][6] = "stage_d"

    # Block 3 (bottom after large gap)
    matrix[39][0] = "Тест 3: Даты=Созданы; Период=За все время; utm_source^=conf_"
    matrix[41][0] = "Этап"
    matrix[41][1] = "Все (шт)"
    matrix[41][2] = "Активные (шт)"
    matrix[41][3] = "Закрытые (шт)"
    matrix[42][0] = "stage_e"
    matrix[43][0] = "stage_f"

    inspector = GoogleSheetsApiLayoutInspector(project_root=Path("."))
    inspector.client = _FakeClient(matrix)

    result = inspector.inspect(_mk_destination())
    anchors = result["anchors"]

    assert len(anchors) == 3
    rows = sorted(int(a["dsl_row"]) for a in anchors)
    assert rows == [1, 1, 40]

    # Ensure side table preserved by col bounds.
    side = [a for a in anchors if int(a["dsl_col"]) == 7][0]
    assert int(side["table_col_start"]) >= 7
    assert int(side["table_col_end"]) >= 10

    # Ensure bottom block not skipped by empty row stop.
    bottom = [a for a in anchors if int(a["dsl_row"]) == 40][0]
    assert int(bottom["header_row"]) == 42
    assert int(bottom["table_row_start"]) == 43


def test_find_dsl_col_detects_row_command() -> None:
    inspector = GoogleSheetsApiLayoutInspector(project_root=Path("."))
    row = ["", "Тест 1: Даты=Созданы; Период=За все время; Теги=машэкспо", ""]
    col = inspector._find_dsl_col(row, {"теги", "utm_source", "воронка", "даты", "период", "с", "по"})
    assert col == 2


def test_find_col_by_alias() -> None:
    inspector = GoogleSheetsApiLayoutInspector(project_root=Path("."))
    values = ["Этап", "Все (шт)", "Активные", "Закрытые"]
    assert inspector._find_col_by_alias(values, {"этап", "статус"}) == 1
    assert inspector._find_col_by_alias(values, {"все", "все (шт)"}) == 2


def test_discovery_does_not_stop_early_on_small_cell_read_limit() -> None:
    matrix = [["" for _ in range(10)] for _ in range(80)]
    matrix[0][0] = "Тест 1: Даты=Созданы; Период=За все время; Теги=машэкспо"
    matrix[2][0] = "Этап"
    matrix[2][1] = "Все (шт)"
    matrix[2][2] = "Активные (шт)"
    matrix[2][3] = "Закрытые (шт)"
    matrix[3][0] = "stage_1"
    matrix[4][0] = "stage_2"

    matrix[55][0] = "Тест 2: Даты=Созданы; Период=За все время; utm_source=conf_exact"
    matrix[57][0] = "Этап"
    matrix[57][1] = "Все (шт)"
    matrix[57][2] = "Активные (шт)"
    matrix[57][3] = "Закрытые (шт)"
    matrix[58][0] = "stage_a"

    destination = _mk_destination()
    destination.layout_config["api_scan_max_rows"] = 80
    destination.layout_config["api_scan_max_cols"] = 10
    destination.layout_config["cell_read_hard_limit"] = 100  # intentionally tiny, should be auto-raised

    inspector = GoogleSheetsApiLayoutInspector(project_root=Path("."))
    inspector.client = _FakeClient(matrix)

    result = inspector.inspect(destination)
    assert result["stop_reason"] != "cell_read_hard_limit"
    assert len(result["anchors"]) == 2
    assert [a["dsl_cell"] for a in result["anchors"]] == ["A1", "A56"]

