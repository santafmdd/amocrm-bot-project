from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from src.amocrm_collector.config import load_collector_config
from src.amocrm_collector.normalizer import AmoDealNormalizer


def _cfg(include: list[int] | None = None, exclude: list[int] | None = None):
    base_dir = Path("workspace") / "tmp_tests" / f"collector_norm_{uuid.uuid4().hex}"
    base_dir.mkdir(parents=True, exist_ok=True)
    cfg_path = base_dir / "cfg.json"
    payload = {
        "base_domain": "example.amocrm.ru",
        "manager_ids_include": include or [],
        "manager_ids_exclude": exclude or [],
        "product_field_id": 101,
        "source_field_id": 102,
        "pain_field_id": 103,
        "tasks_field_id": 104,
        "brief_field_id": 105,
        "demo_result_field_id": 106,
        "test_result_field_id": 107,
        "probability_field_id": 108,
        "company_comment_field_id": 201,
        "contact_comment_field_id": 202,
        "presentation_link_search": {
            "scan_lead_custom_fields_url": True,
            "scan_notes_common_text": True,
            "scan_company_comment": True,
            "scan_contact_comment": True,
            "regexes": ["docs.google.com", "drive.google.com"]
        },
        "presentation_detection": {
            "min_call_duration_seconds": 900,
            "require_any_of": ["demo_result_present", "brief_present", "completed_meeting_task", "long_call", "comment_link_present"]
        }
    }
    cfg_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return load_collector_config(str(cfg_path)), base_dir


def test_normalizer_builds_required_fields_and_presentation_reason():
    cfg, base_dir = _cfg(include=[42], exclude=[])
    normalizer = AmoDealNormalizer(cfg)

    bundle = {
        "lead": {
            "id": 1,
            "name": "  Deal  ",
            "created_at": 10,
            "updated_at": 20,
            "responsible_user_id": 42,
            "pipeline_id": 100,
            "status_id": 200,
            "custom_fields_values": [
                {"field_id": 101, "values": [{"value": "Product A"}]},
                {"field_id": 102, "values": [{"value": "Source B"}]},
                {"field_id": 103, "values": [{"value": " Pain text "}]},
                {"field_id": 104, "values": [{"value": " Task text "}]},
                {"field_id": 105, "values": [{"value": "https://docs.google.com/presentation/d/abc"}]},
                {"field_id": 106, "values": [{"value": "Demo done"}]},
                {"field_id": 107, "values": [{"value": "Test ok"}]},
                {"field_id": 108, "values": [{"value": "75"}]},
            ],
            "_embedded": {"tags": [{"name": "Tag A"}]},
        },
        "contacts": [
            {
                "name": " Contact 1 ",
                "custom_fields_values": [
                    {"field_code": "PHONE", "values": [{"value": "+7 999"}]},
                    {"field_code": "EMAIL", "values": [{"value": "MAIL@EXAMPLE.COM"}]},
                    {"field_id": 202, "field_name": "Комментарий", "values": [{"value": "https://drive.google.com/file/d/1"}]},
                ],
            }
        ],
        "companies": [
            {
                "name": "Company",
                "custom_fields_values": [
                    {"field_name": "ИНН", "values": [{"value": "123"}]},
                    {"field_id": 201, "field_name": "Комментарий", "values": [{"value": "https://docs.google.com/document/d/1"}]}
                ],
            }
        ],
        "notes": [{"note_type": "call_out", "params": {"duration": 1850}}],
        "tasks": [{"is_completed": True, "text": "Демо встреча", "result": {"text": "Состоялась"}}],
        "users_cache": {42: {"name": "Manager"}},
        "pipelines_cache": [{"id": 100, "name": "Pipeline", "_embedded": {"statuses": [{"id": 200, "name": "Status"}]}}],
    }

    row = normalizer.normalize_bundle(bundle)

    assert row["deal_id"] == 1
    assert row["amo_lead_id"] == 1
    assert row["deal_name"] == "Deal"
    assert row["business_tasks_text"] == "Task text"
    assert row["company_comment"].startswith("https://docs.google.com")
    assert row["contact_comment"].startswith("https://drive.google.com")
    assert row["manager_scope_allowed"] is True
    assert row["responsible_user_name"] == "Manager"
    assert row["pipeline_name"] == "Pipeline"
    assert row["status_name"] == "Status"
    assert row["company_inn"] == "123"
    assert row["contact_email"] == "mail@example.com"
    assert row["presentation_detected"] is True
    assert "demo_result_present" in row["presentation_detect_reason"]
    assert "company_comment_link_present" in row["presentation_detect_reason"]
    assert "contact_comment_link_present" in row["presentation_detect_reason"]
    assert any(str(x).startswith("long_call_") for x in row["presentation_detect_reason"])
    assert row["longest_call_duration_seconds"] == 1850

    shutil.rmtree(base_dir, ignore_errors=True)


def test_manager_scope_include_exclude_applied():
    cfg, base_dir = _cfg(include=[42], exclude=[42])
    normalizer = AmoDealNormalizer(cfg)
    row = normalizer.normalize_bundle({"lead": {"id": 1, "responsible_user_id": 42}})
    assert row["manager_scope_allowed"] is False
    shutil.rmtree(base_dir, ignore_errors=True)

