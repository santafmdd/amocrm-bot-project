from pathlib import Path
from unittest.mock import patch

from src.deal_analyzer.exporters import build_markdown_report, write_analysis_csv, write_json_export, write_markdown_export


def test_write_json_and_markdown_exports_use_expected_paths():
    out = Path("workspace/deal_analyzer")
    written: list[str] = []

    def _capture(self: Path, text: str, encoding: str = "utf-8"):
        written.append(str(self))
        return len(text)

    with patch("pathlib.Path.write_text", _capture):
        j = write_json_export(output_dir=out, name="analyze_deal", payload={"ok": True}, write_latest=True)
        m = write_markdown_export(output_dir=out, name="analyze_deal", markdown="# x\n", write_latest=True)

    assert str(j.timestamped).endswith(".json")
    assert j.latest is not None and str(j.latest).endswith("_latest.json")
    assert str(m.timestamped).endswith(".md")
    assert m.latest is not None and str(m.latest).endswith("_latest.md")
    assert len(written) == 4


def test_write_analysis_csv_calls_csv_writer_for_timestamped_and_latest():
    out = Path("workspace/deal_analyzer")
    rows = [{"deal_id": 1, "score_0_100": 80}]
    calls: list[Path] = []

    with patch("src.deal_analyzer.exporters._write_csv", side_effect=lambda p, r: calls.append(p)):
        files = write_analysis_csv(output_dir=out, name="analyze_period", rows=rows, write_latest=True)

    assert len(calls) == 2
    assert str(files.timestamped).endswith(".csv")
    assert files.latest is not None and str(files.latest).endswith("_latest.csv")


def test_build_markdown_report_contains_score_line():
    md = build_markdown_report(
        title="T",
        analyses=[
            {
                "deal_id": 1,
                "deal_name": "Deal",
                "score_0_100": 77,
                "presentation_quality_flag": "ok",
                "followup_quality_flag": "ok",
                "data_completeness_flag": "partial",
                "strong_sides": [],
                "growth_zones": [],
                "risk_flags": [],
            }
        ],
    )
    assert "Deal 1" in md
    assert "Score: 77" in md
