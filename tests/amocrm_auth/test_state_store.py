from pathlib import Path
import uuid

from src.amocrm_auth.state_store import (
    build_state_from_manual_token,
    build_state_from_token_response,
    load_auth_state,
    save_auth_state,
)


def test_state_roundtrip():
    base_dir = Path("workspace") / "tmp_tests"
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / f"amocrm_auth_state_{uuid.uuid4().hex}.json"
    state = build_state_from_token_response(
        base_domain="example.amocrm.ru",
        token_payload={
            "access_token": "a",
            "refresh_token": "r",
            "token_type": "Bearer",
            "expires_in": 3600,
        },
        code="c",
        state="s",
        referer="ref",
    )
    save_auth_state(path, state)
    loaded = load_auth_state(path)
    assert loaded.base_domain == "example.amocrm.ru"
    assert loaded.access_token == "a"
    assert loaded.refresh_token == "r"
    assert loaded.last_code == "c"
    path.unlink(missing_ok=True)


def test_manual_long_lived_token_not_expired():
    state = build_state_from_manual_token(base_domain="example.amocrm.ru", access_token="tok")
    assert state.manual_long_lived_token is True
    assert state.is_expired() is False
