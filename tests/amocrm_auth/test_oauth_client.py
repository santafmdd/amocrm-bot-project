from pathlib import Path
from urllib.parse import parse_qs, urlparse

import src.amocrm_auth.oauth_client as oauth_client_module
from src.amocrm_auth.config import AmoAuthConfig
from src.amocrm_auth.oauth_client import AmoOAuthClient


def _cfg() -> AmoAuthConfig:
    return AmoAuthConfig(
        base_domain="example.amocrm.ru",
        client_id="cid",
        client_secret="sec",
        redirect_uri="http://127.0.0.1:18080/oauth/callback",
        callback_host="127.0.0.1",
        callback_port=18080,
        callback_path="/oauth/callback",
        config_path=Path("config/amocrm_auth.local.json"),
        state_path=Path("workspace/amocrm_auth/state.json"),
    )


def test_build_authorize_url_contains_required_params():
    client = AmoOAuthClient(_cfg())
    url, state = client.build_authorize_url(state="abc123")
    parsed = urlparse(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "example.amocrm.ru"
    assert parsed.path == "/oauth"
    qs = parse_qs(parsed.query)
    assert qs.get("client_id", [""])[0] == "cid"
    assert qs.get("redirect_uri", [""])[0] == "http://127.0.0.1:18080/oauth/callback"
    assert qs.get("response_type", [""])[0] == "code"
    assert qs.get("state", [""])[0] == "abc123"
    assert state == "abc123"


def test_smoke_test_account_uses_plain_account_endpoint(monkeypatch):
    captured_urls: list[str] = []

    def _fake_open_json(req):
        captured_urls.append(req.full_url)
        return {"id": 1, "name": "acc"}

    monkeypatch.setattr(oauth_client_module, "_open_json", _fake_open_json)

    client = AmoOAuthClient(_cfg())
    payload = client.smoke_test_account(base_domain="example.amocrm.ru", access_token="tok")

    assert payload["id"] == 1
    assert captured_urls == ["https://example.amocrm.ru/api/v4/account"]


def test_smoke_test_users_uses_limit_5_endpoint(monkeypatch):
    captured_urls: list[str] = []

    def _fake_open_json(req):
        captured_urls.append(req.full_url)
        return {"_embedded": {"users": []}}

    monkeypatch.setattr(oauth_client_module, "_open_json", _fake_open_json)

    client = AmoOAuthClient(_cfg())
    payload = client.smoke_test_users(base_domain="example.amocrm.ru", access_token="tok", limit=5)

    assert isinstance(payload, dict)
    assert captured_urls == ["https://example.amocrm.ru/api/v4/users?limit=5"]
