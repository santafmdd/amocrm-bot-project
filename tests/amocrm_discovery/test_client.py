from __future__ import annotations

import src.amocrm_discovery.client as client_module
from src.amocrm_discovery.client import AmoDiscoveryClient


class _StubDiscoveryClient(AmoDiscoveryClient):
    def __init__(self) -> None:
        super().__init__(base_domain="example.amocrm.ru", access_token="tok")

    def get_lead(self, lead_id: int):
        return {
            "id": lead_id,
            "pipeline_id": 10,
            "status_id": 100,
            "responsible_user_id": 7,
            "custom_fields_values": [{"field_id": 1, "values": [{"value": "x"}]}],
            "_embedded": {"tags": [{"id": 1, "name": "tag-a"}]},
        }

    def get_lead_links(self, lead_id: int):
        return [
            {"to_entity_type": "contacts", "to_entity_id": 11},
            {"to_entity_type": "companies", "to_entity_id": 12},
        ]

    def get_lead_notes(self, lead_id: int, *, limit: int = 250):
        return [{"id": 1, "text": "note"}]

    def get_lead_tasks(self, lead_id: int, *, limit: int = 250):
        return [{"id": 2, "text": "task"}]

    def get_contact(self, contact_id: int):
        return {"id": contact_id, "name": "contact"}

    def get_company(self, company_id: int):
        return {"id": company_id, "name": "company"}

    def get_user(self, user_id: int):
        return {"id": user_id, "name": "user"}

    def get_pipelines_with_statuses(self, *, limit: int = 250):
        return [{"id": 10, "name": "Pipeline", "_embedded": {"statuses": [{"id": 100, "name": "Status"}]}}]


def test_smoke_paths_for_account_and_users(monkeypatch):
    captured_urls: list[str] = []

    def _fake_open_json(req):
        captured_urls.append(req.full_url)
        if "/users" in req.full_url:
            return {"_embedded": {"users": []}}
        return {"id": 1}

    monkeypatch.setattr(client_module, "_open_json", _fake_open_json)

    client = AmoDiscoveryClient(base_domain="example.amocrm.ru", access_token="tok")
    account = client.get_account_snapshot()
    users = client.get_users(limit=5)

    assert account["id"] == 1
    assert users == []
    assert captured_urls[0] == "https://example.amocrm.ru/api/v4/account"
    assert captured_urls[1] == "https://example.amocrm.ru/api/v4/users?limit=5&page=1"


def test_build_lead_bundle_contains_required_sections():
    client = _StubDiscoveryClient()
    bundle = client.build_lead_bundle(31913530)

    assert bundle["lead"]["id"] == 31913530
    assert bundle["pipeline_status"]["pipeline_name"] == "Pipeline"
    assert bundle["pipeline_status"]["status_name"] == "Status"
    assert len(bundle["contacts"]) == 1
    assert len(bundle["companies"]) == 1
    assert len(bundle["notes"]) == 1
    assert len(bundle["tasks"]) == 1
    assert len(bundle["tags"]) == 1
    assert bundle["responsible_user"]["id"] == 7
    assert isinstance(bundle["custom_fields_values"], list)
