import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.integrations.google_sheets_api_client import GoogleSheetsApiClient


class _FakeGetCall:
    def __init__(self, should_fail=False, payload=None):
        self.should_fail = should_fail
        self.payload = payload or {"values": [["x"]]}

    def execute(self):
        if self.should_fail:
            raise Exception("Unable to parse range: weekly_refusals!A1:N260")
        return self.payload


class _FakeValuesService:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.last_range = ""

    def get(self, spreadsheetId, range):
        self.last_range = range
        return _FakeGetCall(should_fail=self.should_fail)


class _FakeSpreadsheetsService:
    def __init__(self, values_service):
        self._values = values_service

    def values(self):
        return self._values


class _FakeService:
    def __init__(self, values_service):
        self._spreadsheets = _FakeSpreadsheetsService(values_service)

    def spreadsheets(self):
        return self._spreadsheets


def _client() -> GoogleSheetsApiClient:
    return GoogleSheetsApiClient(project_root=Path("."))


def test_build_tab_a1_range_quotes_title() -> None:
    client = _client()
    out = client.build_tab_a1_range(tab_title="weekly refusals", range_suffix="A1:F20")
    assert out == "'weekly refusals'!A1:F20"


def test_resolve_sheet_title_reports_available_tabs_on_missing() -> None:
    client = _client()
    client.list_sheets = lambda _sid: [{"title": "Sheet1"}, {"title": "weekly_refusals"}]  # type: ignore[method-assign]

    raised = False
    try:
        client.resolve_sheet_title("sid", "missing_tab")
    except RuntimeError as exc:
        raised = True
        text = str(exc)
        assert "missing_tab" in text
        assert "weekly_refusals" in text
    assert raised is True


def test_get_values_wraps_parse_range_error_with_tab_context() -> None:
    client = _client()
    values_service = _FakeValuesService(should_fail=True)
    client.build_service = lambda: _FakeService(values_service)  # type: ignore[method-assign]
    client.list_sheets = lambda _sid: [{"title": "weekly_refusals"}, {"title": "events_summary"}]  # type: ignore[method-assign]

    raised = False
    try:
        client.get_values("sid", "weekly_refusals!A1:N260")
    except RuntimeError as exc:
        raised = True
        text = str(exc)
        assert "requested_range=weekly_refusals!A1:N260" in text
        assert "available_tabs" in text
        assert "weekly_refusals" in text
    assert raised is True
