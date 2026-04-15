from pathlib import Path
import yaml


def _load_report_profiles():
    project_root = Path(__file__).resolve().parents[1]
    path = project_root / "config" / "report_profiles.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    items = raw.get("report_profiles", [])
    return {str(item.get("id", "")).strip(): item for item in items if str(item.get("id", "")).strip()}


def test_weekly_refusals_expected_profile_ids_exist() -> None:
    profiles = _load_report_profiles()
    expected = {
        "weekly_refusals_weekly_2m",
        "weekly_refusals_weekly_long",
        "weekly_refusals_cumulative_2m",
        "weekly_refusals_cumulative_long",
    }
    missing = sorted(expected - set(profiles.keys()))
    assert not missing, f"Missing weekly refusals profile ids: {missing}"


def test_weekly_refusals_alias_example_exists_and_enabled() -> None:
    profiles = _load_report_profiles()
    assert "weekly_refusals_example" in profiles
    alias = profiles["weekly_refusals_example"]
    assert bool(alias.get("enabled", False)) is True
    assert alias.get("source", {}).get("page_type") == "events_list"
    assert alias.get("output", {}).get("target_id") == "weekly_refusals_weekly_2m_block"
    assert alias.get("filters", {}).get("alias_of") == "weekly_refusals_weekly_2m"
