from pathlib import Path
import yaml


def _iter_strings(value):
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _iter_strings(k)
            yield from _iter_strings(v)
        return
    if isinstance(value, list):
        for item in value:
            yield from _iter_strings(item)


def _load_report_profiles():
    project_root = Path(__file__).resolve().parents[1]
    path = project_root / "config" / "report_profiles.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    items = raw.get("report_profiles", [])
    return {str(item.get("id", "")).strip(): item for item in items if str(item.get("id", "")).strip()}


def test_report_profiles_no_question_mark_placeholders():
    profiles = _load_report_profiles()
    suspicious = []
    for profile_id, payload in profiles.items():
        for value in _iter_strings(payload):
            if "???" in value:
                suspicious.append((profile_id, value))
    assert not suspicious, f"Found suspicious placeholder/mojibake entries: {suspicious[:10]}"


def test_layout_profiles_still_reference_layout_destination():
    profiles = _load_report_profiles()

    tag_layout = profiles["analytics_tag_layout_example"]
    utm_layout = profiles["analytics_utm_layout_example"]

    assert tag_layout["output"]["target_id"] == "analytics_layout_stage_blocks_destination"
    assert utm_layout["output"]["target_id"] == "analytics_layout_stage_blocks_destination"
    assert utm_layout["execution_input"]["target_id"] == "analytics_layout_stage_blocks_destination"
