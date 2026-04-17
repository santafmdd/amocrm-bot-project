from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

from src.amocrm_discovery.exporters import write_export


def _temp_dir() -> Path:
    path = Path("workspace") / "tmp_tests" / f"amocrm_discovery_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_write_export_creates_timestamped_and_latest():
    temp_dir = _temp_dir()
    payload = {"ok": True, "count": 2}
    result = write_export(output_dir=temp_dir, name="users", payload=payload, write_latest=True)

    assert result.timestamped.exists()
    assert result.latest is not None
    assert result.latest.exists()
    assert result.timestamped.name.startswith("users_")
    assert result.timestamped.suffix == ".json"
    assert result.latest.name == "users_latest.json"

    loaded = json.loads(result.latest.read_text(encoding="utf-8"))
    assert loaded["ok"] is True
    assert loaded["count"] == 2

    shutil.rmtree(temp_dir, ignore_errors=True)


def test_write_export_can_skip_latest():
    temp_dir = _temp_dir()
    result = write_export(output_dir=temp_dir, name="account snapshot", payload={"id": 1}, write_latest=False)

    assert result.timestamped.exists()
    assert result.latest is None
    assert result.timestamped.name.startswith("account_snapshot_")

    shutil.rmtree(temp_dir, ignore_errors=True)
