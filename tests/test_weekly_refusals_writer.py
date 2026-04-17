import sys
from pathlib import Path
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.writers.models import WriterDestinationConfig
from src.writers.weekly_refusals_block_writer import WeeklyRefusalsBlockWriter


class _FakeClient:
    called = 0

    def __init__(self, project_root, logger=None):
        self.project_root = project_root
        self.logger = logger
        self._grid = [["" for _ in range(20)] for _ in range(220)]
        self.insert_calls = []
        self.last_updates = []

    def resolve_sheet_title(self, spreadsheet_id, requested_tab_name):
        assert spreadsheet_id
        return requested_tab_name

    def build_tab_a1_range(self, *, tab_title, range_suffix):
        return f"{tab_title}!{range_suffix}"

    def _parse(self, cell):
        col = 0
        row = 0
        i = 0
        while i < len(cell) and cell[i].isalpha():
            col = col * 26 + (ord(cell[i].upper()) - ord("A") + 1)
            i += 1
        while i < len(cell) and cell[i].isdigit():
            row = row * 10 + int(cell[i])
            i += 1
        return row, col

    def get_values(self, spreadsheet_id, range_a1):
        assert spreadsheet_id
        _, payload = range_a1.split("!", 1)
        payload = payload.strip("'")
        start, end = payload.split(":", 1)
        sr, sc = self._parse(start)
        er, ec = self._parse(end)
        out = []
        for r in range(sr, er + 1):
            row = []
            for c in range(sc, ec + 1):
                try:
                    row.append(self._grid[r - 1][c - 1])
                except Exception:
                    row.append("")
            out.append(row)
        return out

    def insert_rows(self, *, spreadsheet_id, tab_name, start_index, row_count):
        self.insert_calls.append({"start_index": start_index, "row_count": row_count, "tab_name": tab_name})
        for _ in range(row_count):
            self._grid.insert(start_index, ["" for _ in range(20)])
        return {"insertedRows": row_count}

    def batch_update_values(self, spreadsheet_id, data):
        _FakeClient.called += 1
        assert spreadsheet_id
        assert isinstance(data, list)
        self.last_updates = list(data)
        return {"totalUpdatedCells": sum(len(row) for upd in data for row in upd.get("values", [])), "responses": []}


class _AnchorClient(_FakeClient):
    def __init__(self, project_root, logger=None):
        super().__init__(project_root, logger)
        # section title at A1
        self._grid[0][0] = "отказы привлечение (2 месяца) - за неделю"
        self._grid[1][0] = "Значение ДО"
        self._grid[1][1] = "Количество"
        self._grid[1][3] = "Значение ПОСЛЕ"
        self._grid[1][4] = "Количество"
        # existing template rows
        self._grid[2][0] = "Привлечение (2 месяца) / Неразобранное"
        self._grid[2][2] = "manual C1"
        self._grid[2][3] = "(Верификация) Не дозвониться"
        self._grid[2][5] = "manual F1"
        self._grid[3][0] = "Привлечение (2 месяца) / Верификация"
        self._grid[3][2] = "manual C2"
        self._grid[3][3] = "(Верификация) Номер некорректный"
        self._grid[3][5] = "manual F2"


class _FallbackClient(_FakeClient):
    def __init__(self, project_root, logger=None):
        super().__init__(project_root, logger)
        self._grid[0][0] = "something else"


def _tmp_base(name: str) -> Path:
    base = Path(tempfile.gettempdir()) / "amocrm_bot_tests" / name
    base.mkdir(parents=True, exist_ok=True)
    return base


def _make_destination() -> WriterDestinationConfig:
    return WriterDestinationConfig(
        sheet_url="https://docs.google.com/spreadsheets/d/1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0/edit",
        tab_name="analytics_writer_test",
        start_cell="A1",
        write_mode="weekly_refusals_block_update",
        kind="google_sheets_ui",
        target_id="weekly_refusals_weekly_2m_block",
        layout_config={
            "block_kind": "weekly_refusals",
            "block_width": 6,
            "anchor_scan_max_rows": 80,
            "anchor_scan_max_cols": 10,
            "section_title_text_contains": "отказы привлечение (2 месяца) - за неделю",
            "data_start_row_offset": 2,
            "anchor_cell": "A1",
            "detect_header_row": True,
            "allow_start_cell_fallback": False,
            "canonical_before_order": [
                "Привлечение(2 месяца) неразобранное",
                "Привлечение(2 месяца) верификация",
                "Привлечение(2 месяца) первый контакт. квалификация",
            ],
            "canonical_after_group_order": ["Верификация", "Есть интерес к продукту"],
        },
    )


def _parsed_payload() -> dict:
    return {
        "report_id": "weekly_refusals_weekly_2m",
        "display_name": "Weekly refusals",
        "source_rows": [{"status_before": "x", "status_after": "y"}],
        "aggregated_before_status_counts": [
            {"status": "Привлечение(2 месяца) неразобранное", "count": 2},
        ],
        "aggregated_after_status_counts": [
            {"status": "(Верификация) Не дозвониться", "count": 5},
        ],
        "deal_refs": [{"deal_id": "1", "deal_url": "https://example/1"}],
    }


def test_summary_includes_recompute_semantics_marker() -> None:
    base = _tmp_base("test_weekly_refusals_summary_semantics")
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _AnchorClient(root, logger),
    )
    result = writer.write_block(destination=_make_destination(), parsed_result=_parsed_payload(), dry_run=True)
    payload = result.summary_path.read_text(encoding="utf-8")
    assert '"writer_mode_semantics": "weekly_overwrite_from_source"' in payload


def test_weekly_refusals_writer_dry_run_does_not_update_sheet() -> None:
    base = _tmp_base("test_weekly_refusals_writer_dry")
    _FakeClient.called = 0
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _AnchorClient(root, logger),
    )
    result = writer.write_block(destination=_make_destination(), parsed_result=_parsed_payload(), dry_run=True)

    assert result.dry_run is True
    assert result.updated_cells == 0
    assert _FakeClient.called == 0
    payload = result.summary_path.read_text(encoding="utf-8")
    assert '"anchor_source": "section_title"' in payload
    assert '"anchor_required": true' in payload
    assert '"anchor_found": true' in payload
    assert '"fallback_allowed": false' in payload
    assert '"fallback_used": false' in payload
    assert '"preserved_manual_columns": [' in payload


def test_weekly_refusals_writer_live_calls_batch_update() -> None:
    base = _tmp_base("test_weekly_refusals_writer_live")
    _FakeClient.called = 0
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _AnchorClient(root, logger),
    )
    result = writer.write_block(destination=_make_destination(), parsed_result=_parsed_payload(), dry_run=False)

    assert result.dry_run is False
    assert _FakeClient.called == 1
    assert result.updated_cells > 0


def test_existing_before_rows_preserved_when_missing_in_source() -> None:
    base = _tmp_base("test_weekly_refusals_preserve_before")
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _AnchorClient(root, logger),
    )
    payload = _parsed_payload()
    payload["aggregated_before_status_counts"] = [{"status": "Привлечение(2 месяца) неразобранное", "count": 1}]

    result = writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=True)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert '"existing_before_rows_count": 2' in summary
    assert '"updated_before_rows_count": 2' in summary


def test_existing_after_rows_preserved_when_missing_in_source() -> None:
    base = _tmp_base("test_weekly_refusals_preserve_after")
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _AnchorClient(root, logger),
    )
    payload = _parsed_payload()
    payload["aggregated_after_status_counts"] = [{"status": "(Верификация) Не дозвониться", "count": 1}]
    result = writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=True)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert '"existing_after_rows_count": 2' in summary
    assert '"updated_after_rows_count": 2' in summary


def test_manual_columns_c_and_f_are_preserved() -> None:
    base = _tmp_base("test_weekly_refusals_manual_columns")
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _AnchorClient(root, logger),
    )
    result = writer.write_block(destination=_make_destination(), parsed_result=_parsed_payload(), dry_run=True)
    summary = result.summary_path.read_text(encoding="utf-8")
    assert '"preserved_manual_columns": [' in summary
    assert '"C"' in summary and '"F"' in summary


def test_new_before_status_inserts_row() -> None:
    base = _tmp_base("test_weekly_refusals_insert_before")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_before_status_counts"].append({"status": "Привлечение(2 месяца) первый контакт. квалификация", "count": 3})
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    before_values = fake.last_updates[0]["values"]
    statuses = [str(row[0]).strip().lower() for row in before_values if str(row[0]).strip()]
    assert any("\u043f\u0435\u0440\u0432\u044b\u0439 \u043a\u043e\u043d\u0442\u0430\u043a\u0442. \u043a\u0432\u0430\u043b\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f" in s for s in statuses)


def test_new_after_item_inserts_row_inside_group() -> None:
    base = _tmp_base("test_weekly_refusals_insert_after")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_after_status_counts"].append({"status": "(Верификация) Перестал выходить на связь", "count": 2})
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    after_values = fake.last_updates[1]["values"]
    statuses = [str(row[0]).strip().lower() for row in after_values if str(row[0]).strip()]
    assert any("\u043f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c" in s for s in statuses)


def test_weekly_writer_uses_start_cell_fallback_when_anchor_not_found() -> None:
    base = _tmp_base("test_weekly_refusals_writer_fallback")
    destination = _make_destination()
    destination = WriterDestinationConfig(
        sheet_url=destination.sheet_url,
        tab_name=destination.tab_name,
        start_cell="F10",
        write_mode=destination.write_mode,
        kind=destination.kind,
        target_id=destination.target_id,
        layout_config={
            **destination.layout_config,
            "section_title_text_contains": "missing anchor text",
            "anchor_cell": "",
            "allow_start_cell_fallback": True,
        },
    )
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _FallbackClient(root, logger),
    )
    result = writer.write_block(destination=destination, parsed_result=_parsed_payload(), dry_run=True)
    payload = result.summary_path.read_text(encoding="utf-8")
    assert '"anchor_source": "start_cell_fallback"' in payload
    assert '"fallback_used": true' in payload
    assert '"anchor_cell": "F10"' in payload




def test_weekly_writer_raises_when_anchor_missing_and_fallback_disabled() -> None:
    base = _tmp_base("test_weekly_refusals_writer_no_fallback")
    destination = _make_destination()
    destination = WriterDestinationConfig(
        sheet_url=destination.sheet_url,
        tab_name=destination.tab_name,
        start_cell="F10",
        write_mode=destination.write_mode,
        kind=destination.kind,
        target_id=destination.target_id,
        layout_config={
            **destination.layout_config,
            "section_title_text_contains": "missing anchor text",
            "anchor_cell": "",
            "allow_start_cell_fallback": False,
        },
    )
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _FallbackClient(root, logger),
    )

    raised = False
    try:
        writer.write_block(destination=destination, parsed_result=_parsed_payload(), dry_run=True)
    except RuntimeError as exc:
        raised = True
        message = str(exc)
        assert "Weekly refusals anchor not found" in message
        assert "target_id=weekly_refusals_weekly_2m_block" in message
        assert "tab_name=analytics_writer_test" in message
        assert "section_title_text_contains='missing anchor text'" in message
    assert raised is True

def test_sort_before_rows_matches_known_statuses_across_format_variants() -> None:
    base = _tmp_base("test_weekly_refusals_before_known_variants")
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda root, logger=None: _FakeClient(root, logger),
    )
    rows = [
        {"status": "Привлечение (2 месяца) / Неразобранное", "count": 3},
        {"status": "Привлечение (2 месяца) / ВЕРИФИКАЦИЯ", "count": 2},
    ]
    canonical = [
        "Привлечение(2 месяца) неразобранное",
        "Привлечение(2 месяца) верификация",
    ]

    sorted_rows, inserted, final_order = writer._sort_before_rows(rows=rows, canonical_order=canonical)

    assert inserted == []
    assert final_order == [
        "Привлечение (2 месяца) / Неразобранное",
        "Привлечение (2 месяца) / ВЕРИФИКАЦИЯ",
    ]
    assert len(sorted_rows) == 2


def test_weekly_mode_overwrites_counts_only() -> None:
    base = _tmp_base("test_weekly_mode_overwrites")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["mode"] = "weekly"
    result = writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    assert result.updated_cells > 0
    before_values = fake.last_updates[0]["values"]
    after_values = fake.last_updates[1]["values"]
    assert before_values[0][0] == "Привлечение (2 месяца) / Неразобранное"
    assert before_values[1][0] == "Привлечение (2 месяца) / Верификация"
    assert before_values[1][1] == ""
    assert after_values[0][0] == "(Верификация) Не дозвониться"


def test_cumulative_mode_recomputes_from_source_without_adding_existing_sheet_values() -> None:
    base = _tmp_base("test_cumulative_mode_recompute")
    fake = _AnchorClient(base)
    fake._grid[2][1] = "5"
    fake._grid[2][4] = "7"
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["mode"] = "cumulative"
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    before_values = fake.last_updates[0]["values"]
    after_values = fake.last_updates[1]["values"]
    assert before_values[0][1] == 2
    assert after_values[0][1] == 5


def test_cumulative_add_existing_values_sums_existing_and_incoming() -> None:
    base = _tmp_base("test_cumulative_add_existing_values")
    fake = _AnchorClient(base)
    fake._grid[2][1] = "5"
    fake._grid[2][4] = "7"
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    guard_state = base / "debug" / "weekly_refusals_cumulative_guard_state.json"
    if guard_state.exists():
        guard_state.unlink()
    payload = _parsed_payload()
    payload["mode"] = "cumulative"
    payload["cumulative_write_strategy"] = "add_existing_values"
    payload["period_key"] = "weekly_refusals_cumulative_2m|period1"
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)

    before_values = fake.last_updates[0]["values"]
    after_values = fake.last_updates[1]["values"]
    assert before_values[0][1] == 7
    assert after_values[0][1] == 12


def test_cumulative_add_existing_values_blocks_duplicate_period() -> None:
    base = _tmp_base("test_cumulative_add_existing_duplicate")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    guard_state = base / "debug" / "weekly_refusals_cumulative_guard_state.json"
    if guard_state.exists():
        guard_state.unlink()
    payload = _parsed_payload()
    payload["mode"] = "cumulative"
    payload["cumulative_write_strategy"] = "add_existing_values"
    payload["period_key"] = "weekly_refusals_cumulative_2m|period2"

    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)

    raised = False
    try:
        writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    except RuntimeError as exc:
        raised = True
        assert "Duplicate cumulative period apply blocked" in str(exc)
    assert raised is True


class _GapAfterClient(_AnchorClient):
    def __init__(self, project_root, logger=None):
        super().__init__(project_root, logger)
        self._grid[2][3] = "(\u0412\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f) \u041d\u0435 \u0434\u043e\u0437\u0432\u043e\u043d\u0438\u0442\u044c\u0441\u044f"
        self._grid[3][3] = ""
        self._grid[4][3] = "(\u0412\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f) \u041d\u043e\u043c\u0435\u0440 \u043d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0439"


def test_after_layout_is_compact_and_tail_item_not_placed_into_internal_gap() -> None:
    base = _tmp_base("test_after_layout_compact_tail")
    fake = _GapAfterClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_after_status_counts"] = [
        {"status": "(\u0412\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f) \u041d\u0435 \u0434\u043e\u0437\u0432\u043e\u043d\u0438\u0442\u044c\u0441\u044f", "count": 2},
        {"status": "(\u0412\u0435\u0440\u0438\u0444\u0438\u043a\u0430\u0446\u0438\u044f) \u041d\u043e\u043c\u0435\u0440 \u043d\u0435\u043a\u043e\u0440\u0440\u0435\u043a\u0442\u043d\u044b\u0439", "count": 1},
        {"status": "\u0411\u0435\u0437 \u043f\u0440\u0438\u0447\u0438\u043d\u044b", "count": 4},
    ]
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)

    after_values = fake.last_updates[1]["values"]
    labels = [str(row[0]).strip() for row in after_values if str(row[0]).strip()]
    assert labels[-1].lower() == "\u0431\u0435\u0437 \u043f\u0440\u0438\u0447\u0438\u043d\u044b"
    non_empty_positions = [idx for idx, row in enumerate(after_values) if str(row[0]).strip()]
    assert non_empty_positions == list(range(min(non_empty_positions), max(non_empty_positions) + 1))


def test_header_row_not_included_in_data_updates() -> None:
    base = _tmp_base("test_header_not_in_data")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["mode"] = "weekly"
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    first_before_status = fake.last_updates[0]["values"][0][0]
    assert first_before_status != "Значение ДО"


def test_standalone_closed_not_inserted_when_granular_after_present() -> None:
    base = _tmp_base("test_no_standalone_closed")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_after_status_counts"] = [
        {"status": "закрыто и не реализовано", "count": 11},
        {"status": "(Верификация) Не дозвониться", "count": 3},
    ]
    payload["mode"] = "weekly"
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    after_values = fake.last_updates[1]["values"]
    statuses = [row[0] for row in after_values]
    assert "закрыто и не реализовано" not in [str(s).strip().lower() for s in statuses]


class _TightBlockClient(_AnchorClient):
    def __init__(self, project_root, logger=None):
        super().__init__(project_root, logger)
        # next section title directly below short capacity window
        self._grid[5][0] = "отказы привлечение (долгие) - накопительный свод"


def test_numeric_counts_are_written_as_numbers_not_quoted_strings() -> None:
    base = _tmp_base("test_weekly_refusals_numeric_counts")
    fake = _AnchorClient(base)
    fake._grid[2][1] = "'5"
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["mode"] = "cumulative"
    payload["aggregated_before_status_counts"] = [{"status": "\u041f\u0440\u0438\u0432\u043b\u0435\u0447\u0435\u043d\u0438\u0435(2 \u043c\u0435\u0441\u044f\u0446\u0430) \u043d\u0435\u0440\u0430\u0437\u043e\u0431\u0440\u0430\u043d\u043d\u043e\u0435", "count": 2}]
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)

    before_values = fake.last_updates[0]["values"]
    assert before_values[0][1] == 2
    assert isinstance(before_values[0][1], int)


def test_near_duplicate_after_reasons_compact_to_single_canonical_row() -> None:
    base = _tmp_base("test_weekly_refusals_no_near_duplicates")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_after_status_counts"] = [
        {"status": "(Верификация) Перестал выходить на свя", "count": 2},
        {"status": "(Верификация) Перестал выходить на связь", "count": 3},
    ]
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    after_values = fake.last_updates[1]["values"]
    statuses = [str(row[0]).strip().lower() for row in after_values if str(row[0]).strip()]
    counts = [str(row[1]).strip() for row in after_values if str(row[0]).strip()]
    assert statuses.count("(верификация) перестал выходить на связь") == 1
    assert "5" in counts


def test_safe_row_expansion_inserts_rows_when_capacity_is_tight() -> None:
    base = _tmp_base("test_weekly_refusals_safe_expansion")
    fake = _TightBlockClient(base)
    destination = _make_destination()
    destination = WriterDestinationConfig(
        sheet_url=destination.sheet_url,
        tab_name=destination.tab_name,
        start_cell=destination.start_cell,
        write_mode=destination.write_mode,
        kind=destination.kind,
        target_id=destination.target_id,
        layout_config={
            **destination.layout_config,
            "next_section_title_text_contains": "отказы привлечение (долгие) - накопительный свод",
        },
    )
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_before_status_counts"] = [
        {"status": "Привлечение(2 месяца) неразобранное", "count": 1},
        {"status": "Привлечение(2 месяца) верификация", "count": 2},
        {"status": "Привлечение(2 месяца) первый контакт. квалификация", "count": 3},
        {"status": "Привлечение(2 месяца) есть интерес к продукту", "count": 4},
    ]
    writer.write_block(destination=destination, parsed_result=payload, dry_run=False)
    assert fake.insert_calls, "writer must insert rows instead of risking overlap with next section"


def test_canonical_display_label_is_used_in_final_write_plan() -> None:
    base = _tmp_base("test_weekly_refusals_canonical_display")
    fake = _AnchorClient(base)
    fake._grid[2][3] = "(\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0430 \u0434\u0435\u043c\u043e\u043d\u0441\u0442\u0440\u0430\u0446\u0438\u044f) \u041f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437"
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_after_status_counts"] = [
        {"status": "(\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0430 \u0434\u0435\u043c\u043e\u043d\u0441\u0442\u0440\u0430\u0446\u0438\u044f) \u041f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f", "count": 1},
    ]
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)

    after_values = fake.last_updates[1]["values"]
    labels = [str(row[0]).strip() for row in after_values if str(row[0]).strip()]
    assert "(\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0430 \u0434\u0435\u043c\u043e\u043d\u0441\u0442\u0440\u0430\u0446\u0438\u044f) \u041f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437\u044c" in labels
    assert "(\u041f\u0440\u043e\u0432\u0435\u0434\u0435\u043d\u0430 \u0434\u0435\u043c\u043e\u043d\u0441\u0442\u0440\u0430\u0446\u0438\u044f) \u041f\u0435\u0440\u0435\u0441\u0442\u0430\u043b \u0432\u044b\u0445\u043e\u0434\u0438\u0442\u044c \u043d\u0430 \u0441\u0432\u044f\u0437" not in labels


def test_compact_layout_has_no_internal_empty_holes_between_filled_rows() -> None:
    base = _tmp_base("test_weekly_refusals_compact_layout")
    fake = _AnchorClient(base)
    writer = WeeklyRefusalsBlockWriter(
        project_root=base,
        exports_dir=base,
        client_factory=lambda _root, logger=None: fake,
    )
    payload = _parsed_payload()
    payload["aggregated_before_status_counts"] = [
        {"status": "Привлечение(2 месяца) неразобранное", "count": 1},
        {"status": "Привлечение(2 месяца) первый контакт. квалификация", "count": 2},
    ]
    writer.write_block(destination=_make_destination(), parsed_result=payload, dry_run=False)
    before_values = fake.last_updates[0]["values"]
    non_empty_positions = [idx for idx, row in enumerate(before_values) if str(row[0]).strip()]
    if non_empty_positions:
        assert non_empty_positions == list(range(min(non_empty_positions), max(non_empty_positions) + 1))

