from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import AmoAuthConfig


@dataclass(frozen=True)
class CallbackPayload:
    code: str
    state: str
    referer: str


class AmoOAuthClient:
    def __init__(self, config: AmoAuthConfig) -> None:
        self.config = config

    def build_authorize_url(self, *, state: str | None = None, base_domain: str | None = None) -> tuple[str, str]:
        actual_state = state or secrets.token_urlsafe(24)
        host = (base_domain or self.config.base_domain).strip()
        if not host:
            raise RuntimeError("amoCRM base_domain is required to build authorize URL")
        if not self.config.client_id:
            raise RuntimeError("amoCRM client_id is required to build authorize URL")

        query = urlencode(
            {
                "client_id": self.config.client_id,
                "redirect_uri": self.config.redirect_uri,
                "response_type": "code",
                "state": actual_state,
                "mode": "post_message",
            }
        )
        return f"https://{host}/oauth?{query}", actual_state

    def exchange_code(self, *, code: str, base_domain: str | None = None) -> dict[str, Any]:
        host = (base_domain or self.config.base_domain).strip()
        if not host:
            raise RuntimeError("amoCRM base_domain is required for token exchange")
        if not self.config.client_id or not self.config.client_secret:
            raise RuntimeError("amoCRM client_id/client_secret are required for token exchange")

        payload = {
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.config.redirect_uri,
        }
        return _post_json(f"https://{host}/oauth2/access_token", payload)

    def refresh_access_token(self, *, refresh_token: str, base_domain: str | None = None) -> dict[str, Any]:
        host = (base_domain or self.config.base_domain).strip()
        if not host:
            raise RuntimeError("amoCRM base_domain is required for token refresh")
        if not self.config.client_id or not self.config.client_secret:
            raise RuntimeError("amoCRM client_id/client_secret are required for token refresh")

        payload = {
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "redirect_uri": self.config.redirect_uri,
        }
        return _post_json(f"https://{host}/oauth2/access_token", payload)

    def smoke_test_account(self, *, base_domain: str, access_token: str) -> dict[str, Any]:
        host = str(base_domain or "").strip()
        if not host:
            raise RuntimeError("amoCRM base_domain is required for smoke test")
        if not access_token.strip():
            raise RuntimeError("amoCRM access token is empty")

        req = Request(
            f"https://{host}/api/v4/account?with=users",
            method="GET",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
        )
        return _open_json(req)


def _post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        url,
        method="POST",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    return _open_json(req)


def _open_json(req: Request) -> dict[str, Any]:
    try:
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(f"amoCRM HTTP error {exc.code}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"amoCRM network error: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"amoCRM response is not JSON: {raw[:400]}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"amoCRM JSON payload is not an object: {payload}")
    return payload
