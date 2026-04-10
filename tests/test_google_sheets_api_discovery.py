import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pathlib import Path

from src.integrations.google_sheets_api_client import extract_spreadsheet_id
from src.writers.google_sheets_api_layout_discovery import GoogleSheetsApiLayoutInspector, _to_col_label


def test_extract_spreadsheet_id_from_url() -> None:
    url = "https://docs.google.com/spreadsheets/d/1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0/edit?gid=1057004550"
    assert extract_spreadsheet_id(url) == "1snOH42aIRUtxS3AU9PJPHSrk1vDyFQdiIJNJRhasxX0"


def test_to_col_label() -> None:
    assert _to_col_label(1) == "A"
    assert _to_col_label(26) == "Z"
    assert _to_col_label(27) == "AA"


def test_find_dsl_col_detects_row_command() -> None:
    inspector = GoogleSheetsApiLayoutInspector(project_root=Path('.'))
    row = ["", "????????: ????=???????; ??????=?? ??? ?????; ????=????????", ""]
    col = inspector._find_dsl_col(row, {"????", "utm_source", "???????", "????", "??????", "?", "??"})
    assert col == 2


def test_find_col_by_alias() -> None:
    inspector = GoogleSheetsApiLayoutInspector(project_root=Path('.'))
    values = ["????", "??? (??)", "????????", "????????"]
    assert inspector._find_col_by_alias(values, {"????", "??????"}) == 1
    assert inspector._find_col_by_alias(values, {"???", "??? (??)"}) == 2
