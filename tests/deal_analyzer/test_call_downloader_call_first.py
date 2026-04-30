from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from src.deal_analyzer.call_downloader import CallDownloader


class _Logger:
    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None


def _cfg():
    return SimpleNamespace(
        call_collection_mode="api_first",
        audio_cache_dir="workspace/tmp_tests/deal_analyzer/call_first_audio",
        amocrm_auth_config_path="",
        call_base_domain="https://officeistockinfo.amocrm.ru",
    )


class _FakeClient:
    def get_users_cache(self):
        return {22: {"name": "Рустам"}}

    def get_leads_notes_page(self, *, params=None):
        params = params or {}
        if int(params.get("page", 1) or 1) != 1:
            return [], {"status": 200}, "/api/v4/leads/notes"
        if str(params.get("filter[note_type]") or "") != "call_out":
            return [], {"status": 200}, "/api/v4/leads/notes"
        return [
            {
                "id": 1001,
                "entity_id": 101,
                "note_type": "call_out",
                "created_at": 1777310400,
                "responsible_user_id": 22,
                "params": {
                    "uniq": "dup-call-1",
                    "duration": 490,
                    "link": "https://rec/lead-dup-1.mp3",
                    "phone": "+7 (999) 100-20-30",
                },
            }
        ], {"status": 200}, "/api/v4/leads/notes"

    def get_contacts_notes_page(self, *, params=None):
        params = params or {}
        if int(params.get("page", 1) or 1) != 1:
            return [], {"status": 200}, "/api/v4/contacts/notes"
        if str(params.get("filter[note_type]") or "") != "call_out":
            return [], {"status": 200}, "/api/v4/contacts/notes"
        return [
            {
                "id": 2001,
                "entity_id": 36219401,
                "note_type": "call_out",
                "created_at": 1777310405,
                "responsible_user_id": 22,
                "params": {
                    "uniq": "dup-call-1",
                    "duration": 490,
                    "link": "https://rec/contact-dup-1.mp3",
                },
            },
            {
                "id": 2002,
                "entity_id": 36219402,
                "note_type": "call_out",
                "created_at": 1777310410,
                "responsible_user_id": 22,
                "params": {
                    "uniq": "contact-only-1",
                    "duration": 370,
                    "link": "https://rec/contact-only-1.mp3",
                    "phone_number": "+7 999 200 30 40",
                },
            },
        ], {"status": 200}, "/api/v4/contacts/notes"

    def get_contact_links(self, contact_id: int):
        if int(contact_id) == 36219401:
            return [{"to_entity_type": "leads", "to_entity_id": 101}]
        if int(contact_id) == 36219402:
            return []
        return []

    def get_lead(self, lead_id: int):
        return {
            "id": int(lead_id),
            "status_id": 123,
            "created_at": 1777000000,
            "updated_at": 1777310000,
        }

    def get_events_page(self, *, params=None):
        return [], {"status": 200}, "/api/v4/events"

    def get_notes_by_lead(self, lead_id: int):
        return []


def test_call_first_collects_contact_calls_and_keeps_lead_priority_on_duplicate_call_id():
    downloader = CallDownloader(config=_cfg(), logger=_Logger())

    with patch.object(
        downloader,
        "_make_api_client",
        return_value=(_FakeClient(), "https://officeistockinfo.amocrm.ru", "tok", ""),
    ):
        calls, audit = downloader.collect_period_calls_call_first(
            period_start=date(2026, 4, 28),
            period_end=date(2026, 4, 29),
        )

    by_id = {str(call.call_id): call for call in calls}
    assert "dup-call-1" in by_id
    assert "contact-only-1" in by_id

    # Duplicate present in both lead and contact notes -> keep lead entity.
    assert by_id["dup-call-1"].deal_id == "101"
    assert by_id["dup-call-1"].entity_type == "lead"

    # Contact-only call is kept with synthetic ID and contact metadata.
    assert by_id["contact-only-1"].deal_id == "contact_36219402"
    assert by_id["contact-only-1"].entity_type == "contact_only"
    assert by_id["contact-only-1"].contact_id == "36219402"
    assert by_id["contact-only-1"].contact_url.endswith("/contacts/detail/36219402")

    assert int(audit.get("contact_calls_seen", 0) or 0) >= 2
    assert int(audit.get("contact_calls_written_as_contact_only", 0) or 0) >= 1
    assert isinstance(audit.get("contact_call_resolution_debug"), list)
