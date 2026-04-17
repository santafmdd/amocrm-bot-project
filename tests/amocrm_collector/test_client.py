from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from src.amocrm_collector import cli
from src.amocrm_collector.client import AmoCollectorClient, ApiRequestError
from src.amocrm_collector.config import AmoCollectorConfig, PresentationDetectionConfig, PresentationLinkSearchConfig


class _ProbeClient(AmoCollectorClient):
    def __init__(self) -> None:
        super().__init__(base_domain="example.amocrm.ru", access_token="tok")

    def get_lead(self, lead_id: int):
        return {"id": lead_id, "name": "Deal"}

    def get_lead_links(self, lead_id: int):
        return []

    def get_notes_by_lead(self, lead_id: int, limit: int = 250):
        raise ApiRequestError(
            path=f"/api/v4/leads/{lead_id}/notes",
            message="amoCRM response is not JSON",
            status=200,
            content_type="text/html",
            body_preview="<html>bad</html>",
        )

    def get_tasks_by_lead(self, lead_id: int, limit: int = 250):
        return [{"id": 1, "text": "ok"}]

    def get_users_cache(self):
        return {}

    def get_pipelines_cache(self):
        return []

    def get_status_cache(self):
        return {}


class _ProbeTasksClient(_ProbeClient):
    def get_notes_by_lead(self, lead_id: int, limit: int = 250):
        return [{"id": 9, "text": "note"}]

    def get_tasks_by_lead(self, lead_id: int, limit: int = 250):
        raise ApiRequestError(
            path=f"/api/v4/tasks?filter[entity_id]={lead_id}",
            message="amoCRM response is not JSON",
            status=502,
            content_type="text/html",
            body_preview="<html>proxy</html>",
        )


def _collector_cfg() -> AmoCollectorConfig:
    return AmoCollectorConfig(
        config_path=Path("config/amocrm_collector.local.json"),
        auth_config_path=Path("config/amocrm_auth.local.json"),
        output_dir=Path("workspace/amocrm_collector"),
        base_domain="example.amocrm.ru",
        manager_ids_include=[],
        manager_ids_exclude=[101],
        manager_names_exclude=["Антон Коломоец"],
        pipeline_ids_include=[],
        product_field_id=0,
        source_field_id=None,
        pain_field_id=None,
        tasks_field_id=None,
        brief_field_id=None,
        demo_result_field_id=None,
        test_result_field_id=None,
        probability_field_id=None,
        company_comment_field_id=None,
        contact_comment_field_id=None,
        presentation_link_search=PresentationLinkSearchConfig(
            scan_deal_custom_fields_url=True,
            scan_notes_common_text=True,
            scan_company_comment=True,
            scan_contact_comment=True,
            regexes=["docs.google.com"],
        ),
        presentation_detection=PresentationDetectionConfig(
            min_call_duration_seconds=900,
            require_any_of=["demo_result_present"],
        ),
    )


def test_collect_lead_bundle_non_json_notes_fallback_without_crash():
    client = _ProbeClient()
    bundle = client.collect_lead_bundle(31913530)

    assert bundle["lead"]["id"] == 31913530
    assert bundle["notes"] == []
    assert bundle["tasks"][0]["id"] == 1

    warnings = bundle.get("warnings", [])
    assert isinstance(warnings, list) and warnings
    issue = warnings[0]
    assert issue["deal_id"] == 31913530
    assert issue["section"] == "notes"
    assert issue["endpoint_path"] == "/api/v4/leads/31913530/notes"
    assert issue["http_status"] == 200
    assert issue["content_type"] == "text/html"
    assert "<html>bad</html>" in issue["body_preview"]


def test_collect_lead_bundle_non_json_tasks_fallback_without_crash():
    client = _ProbeTasksClient()
    bundle = client.collect_lead_bundle(31913530)

    assert bundle["lead"]["id"] == 31913530
    assert bundle["notes"][0]["id"] == 9
    assert bundle["tasks"] == []

    warnings = [x for x in bundle.get("warnings", []) if x.get("section") == "tasks"]
    assert warnings
    issue = warnings[0]
    assert issue["deal_id"] == 31913530
    assert issue["http_status"] == 502
    assert issue["content_type"] == "text/html"
    assert "proxy" in issue["body_preview"]


def test_schema_check_payload_includes_config_summary():
    captured: dict[str, object] = {}

    class _SchemaClient(_ProbeClient):
        def get_account(self):
            return {"id": 1}

        def get_custom_fields(self, entity: str):
            return []

    def _fake_write_json_export(*, output_dir, name, payload, write_latest):
        captured["payload"] = payload

        class _Out:
            timestamped = Path("x.json")
            latest = Path("latest.json")

        return _Out()

    with patch("src.amocrm_collector.cli.write_json_export", side_effect=_fake_write_json_export):
        cli._run_schema_check(_SchemaClient(), _collector_cfg(), Path("workspace/amocrm_collector"), "example.amocrm.ru", True, _DummyLogger())

    payload = captured["payload"]
    assert isinstance(payload, dict)
    summary = payload.get("config_summary", {})
    assert summary.get("manager_exclusions", {}).get("ids") == [101]
    assert summary.get("manager_exclusions", {}).get("names") == ["Антон Коломоец"]
    assert "product_field_id" in summary.get("field_id_state", {}).get("zero", [])


def test_debug_deal_sections_command_exports_expected_structure():
    captured: dict[str, object] = {}
    logger = _DummyLogger()

    class _DebugClient(_ProbeClient):
        def debug_deal_sections(self, lead_id: int):
            return {
                "deal_id": lead_id,
                "sections": {
                    "lead": {"endpoint": "/api/v4/leads/1", "status": 200, "content_type": "application/json", "item_count": 1, "body_preview": "", "ok": True},
                    "notes": {"endpoint": "/api/v4/leads/1/notes", "status": 200, "content_type": "application/json", "item_count": 0, "body_preview": "", "ok": True},
                    "tasks": {"endpoint": "/api/v4/tasks", "status": 502, "content_type": "text/html", "item_count": 0, "body_preview": "<html>", "ok": False},
                },
            }

    def _fake_write_json_export(*, output_dir, name, payload, write_latest):
        captured["payload"] = payload

        class _Out:
            timestamped = Path("debug.json")
            latest = Path("debug_latest.json")

        return _Out()

    with patch("src.amocrm_collector.cli.write_json_export", side_effect=_fake_write_json_export):
        cli._run_debug_deal_sections(
            _DebugClient(),
            Path("workspace/amocrm_collector"),
            "example.amocrm.ru",
            31913530,
            True,
            logger,
        )

    payload = captured.get("payload")
    assert isinstance(payload, dict)
    assert payload.get("command") == "debug-deal-sections"
    sections = payload.get("sections", {})
    assert isinstance(sections, dict)
    assert sections.get("lead", {}).get("endpoint")
    assert sections.get("tasks", {}).get("status") == 502
    assert logger.info_calls


def test_collect_period_summary_contains_section_warning_counters():
    captured: dict[str, object] = {}

    class _PeriodClient(_ProbeClient):
        def __init__(self):
            super().__init__()
            self._page_calls = 0

        def get_leads_by_period(self, **kwargs):
            self._page_calls += 1
            if self._page_calls == 1:
                return [{"id": 1}, {"id": 2}, {"id": 3}]
            return []

        def collect_lead_bundle(self, lead_id: int):
            warnings = []
            if lead_id == 1:
                warnings.append({"deal_id": 1, "section": "notes", "endpoint_path": "/api/v4/leads/1/notes"})
            if lead_id == 2:
                warnings.append({"deal_id": 2, "section": "tasks", "endpoint_path": "/api/v4/tasks"})
            return {
                "lead": {"id": lead_id, "responsible_user_id": 7},
                "notes": [],
                "tasks": [],
                "contacts": [],
                "companies": [],
                "warnings": warnings,
                "errors": [],
            }

    class _Norm:
        def normalize_bundle(self, bundle):
            return {"deal_id": bundle["lead"]["id"], "manager_scope_allowed": True}

    def _fake_write_json_export(*, output_dir, name, payload, write_latest):
        captured["payload"] = payload

        class _Out:
            timestamped = Path("period.json")
            latest = Path("period_latest.json")

        return _Out()

    with patch("src.amocrm_collector.cli.write_json_export", side_effect=_fake_write_json_export), patch(
        "src.amocrm_collector.cli.write_normalized_jsonl"
    ), patch("src.amocrm_collector.cli.write_normalized_csv"):
        cli._run_collect_period(
            _PeriodClient(),
            _Norm(),
            _collector_cfg(),
            Path("workspace/amocrm_collector"),
            "example.amocrm.ru",
            "2026-04-01",
            "2026-04-07",
            True,
            _DummyLogger(),
        )

    payload = captured.get("payload")
    assert isinstance(payload, dict)
    counts = payload.get("counts", {})
    assert counts.get("deals_with_notes_warning") == 1
    assert counts.get("deals_with_tasks_warning") == 1

    section_summary = payload.get("section_warning_summary", {})
    assert section_summary.get("notes", {}).get("affected_deal_ids") == [1]
    assert section_summary.get("tasks", {}).get("affected_deal_ids") == [2]


class _DummyLogger:
    def __init__(self):
        self.info_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.warn_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.error_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def info(self, *args, **kwargs):
        self.info_calls.append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.warn_calls.append((args, kwargs))

    def error(self, *args, **kwargs):
        self.error_calls.append((args, kwargs))
