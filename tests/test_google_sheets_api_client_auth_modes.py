import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.integrations.google_sheets_api_client import (
    AUTH_MODE_CACHE_ONLY,
    AUTH_MODE_INTERACTIVE_BOOTSTRAP,
    GoogleSheetsApiClient,
)


class _DummyCreds:
    def __init__(self, valid=False, expired=False, refresh_token=None):
        self.valid = valid
        self.expired = expired
        self.refresh_token = refresh_token

    @staticmethod
    def from_authorized_user_file(path: str, scopes):
        return _DummyCreds(valid=False, expired=False, refresh_token=None)

    def to_json(self) -> str:
        return '{"token": "x"}'


class _DummyFlow:
    called = False

    @staticmethod
    def from_client_secrets_file(path: str, scopes):
        class _Flow:
            @staticmethod
            def run_local_server(port=0):
                _DummyFlow.called = True
                return _DummyCreds(valid=True, expired=False, refresh_token="r")

        return _Flow()


def _mk_tmp_dir(name: str) -> Path:
    base = Path("d:/AI_Automation") / name
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    return base


def _mk_client(base: Path, mode: str) -> GoogleSheetsApiClient:
    client = GoogleSheetsApiClient(project_root=base)
    client.auth_mode = mode
    client.credentials_file = base / "credentials.json"
    client.token_file = base / "token.json"
    return client


def test_cache_only_mode_never_starts_interactive_flow() -> None:
    base = _mk_tmp_dir("tmp_google_auth_cache_only")
    client = _mk_client(base, AUTH_MODE_CACHE_ONLY)

    def _libs():
        def _build(service, version, credentials):
            return {"ok": True}

        class _Req:
            pass

        return _Req, _DummyCreds, _DummyFlow, _build

    client._load_google_libs = _libs  # type: ignore[method-assign]

    try:
        client.build_service()
    except RuntimeError as exc:
        msg = str(exc)
        assert "cache_only" in msg
        assert "forbidden" in msg.lower()
    else:
        raise AssertionError("cache_only mode must fail when token is unavailable")

    assert _DummyFlow.called is False
    shutil.rmtree(base, ignore_errors=True)


def test_interactive_bootstrap_mode_can_start_oauth_flow() -> None:
    base = _mk_tmp_dir("tmp_google_auth_bootstrap")
    client = _mk_client(base, AUTH_MODE_INTERACTIVE_BOOTSTRAP)
    client.credentials_file.write_text("{}", encoding="utf-8")
    _DummyFlow.called = False

    def _libs():
        def _build(service, version, credentials):
            return {"ok": True}

        class _Req:
            pass

        return _Req, _DummyCreds, _DummyFlow, _build

    client._load_google_libs = _libs  # type: ignore[method-assign]

    service = client.build_service()
    assert service == {"ok": True}
    assert _DummyFlow.called is True
    assert client.token_file.exists()
    shutil.rmtree(base, ignore_errors=True)
