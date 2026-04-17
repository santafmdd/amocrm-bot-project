from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from src.amocrm_collector.config import load_collector_config


def test_load_collector_config_defaults_and_bom_support():
    base_dir = Path("workspace") / "tmp_tests" / f"collector_cfg_{uuid.uuid4().hex}"
    base_dir.mkdir(parents=True, exist_ok=True)

    cfg_path = base_dir / "amocrm_collector.local.json"
    payload = {
        "base_domain": "test-account.amocrm.ru",
        "manager_ids_include": [1, "2", "x"],
        "manager_ids_exclude": [3, "4"],
        "pipeline_ids_include": [11, 22],
        "product_field_id": "101",
        "company_comment_field_id": "201",
        "contact_comment_field_id": 202,
        "presentation_link_search": {
            "regexes": ["docs.google.com"],
        },
        "output_dir": "workspace/amocrm_collector",
    }
    cfg_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8-sig")

    cfg = load_collector_config(str(cfg_path))

    assert cfg.base_domain == "test-account.amocrm.ru"
    assert cfg.manager_ids_include == [1, 2]
    assert cfg.manager_ids_exclude == [3, 4]
    assert cfg.pipeline_ids_include == [11, 22]
    assert cfg.product_field_id == 101
    assert cfg.company_comment_field_id == 201
    assert cfg.contact_comment_field_id == 202
    assert cfg.presentation_link_search.regexes == ["docs.google.com"]
    assert cfg.output_dir.name == "amocrm_collector"

    shutil.rmtree(base_dir, ignore_errors=True)
