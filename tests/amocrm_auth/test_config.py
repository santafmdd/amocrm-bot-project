import json
import uuid
from pathlib import Path

from src.amocrm_auth.config import load_amocrm_auth_config


_ENV_KEYS = [
    "AMOCRM_BASE_DOMAIN",
    "AMOCRM_CLIENT_ID",
    "AMOCRM_CLIENT_SECRET",
    "AMOCRM_REDIRECT_URI",
    "AMOCRM_CALLBACK_HOST",
    "AMOCRM_CALLBACK_PORT",
    "AMOCRM_CALLBACK_PATH",
    "AMOCRM_AUTH_STATE_FILE",
]


def test_load_amocrm_auth_config_reads_utf8_bom_json(monkeypatch):
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    base_dir = Path("workspace") / "tmp_tests"
    base_dir.mkdir(parents=True, exist_ok=True)
    config_path = base_dir / f"amocrm_auth_bom_{uuid.uuid4().hex}.json"

    payload = {
        "base_domain": "test-account.amocrm.ru",
        "client_id": "cid_bom",
        "client_secret": "secret_bom",
        "redirect_uri": "http://127.0.0.1:18081/oauth/callback",
        "callback_host": "127.0.0.1",
        "callback_port": 18081,
        "callback_path": "/oauth/callback",
        "state_path": "workspace/amocrm_auth/state.json",
    }

    config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8-sig")

    cfg = load_amocrm_auth_config(str(config_path))

    assert cfg.base_domain == "test-account.amocrm.ru"
    assert cfg.client_id == "cid_bom"
    assert cfg.client_secret == "secret_bom"
    assert cfg.redirect_uri == "http://127.0.0.1:18081/oauth/callback"
    assert cfg.callback_port == 18081

    config_path.unlink(missing_ok=True)
