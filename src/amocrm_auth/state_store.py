from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass
class AmoAuthState:
    base_domain: str = ""
    access_token: str = ""
    refresh_token: str = ""
    token_type: str = "Bearer"
    expires_at: str = ""
    manual_long_lived_token: bool = False
    last_code: str = ""
    last_state: str = ""
    last_referer: str = ""
    updated_at: str = ""

    def is_access_token_present(self) -> bool:
        return bool(self.access_token.strip())

    def is_expired(self, *, leeway_seconds: int = 60) -> bool:
        if self.manual_long_lived_token:
            return False
        if not self.expires_at:
            return True
        try:
            expires = datetime.fromisoformat(self.expires_at)
        except ValueError:
            return True
        now = datetime.now(timezone.utc)
        return now >= (expires - timedelta(seconds=max(0, leeway_seconds)))



def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_state_from_token_response(
    *,
    base_domain: str,
    token_payload: dict[str, Any],
    code: str = "",
    state: str = "",
    referer: str = "",
) -> AmoAuthState:
    expires_in = int(token_payload.get("expires_in", 0) or 0)
    expires_at = ""
    if expires_in > 0:
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in)).isoformat(timespec="seconds")

    return AmoAuthState(
        base_domain=str(base_domain or "").strip(),
        access_token=str(token_payload.get("access_token", "") or "").strip(),
        refresh_token=str(token_payload.get("refresh_token", "") or "").strip(),
        token_type=str(token_payload.get("token_type", "Bearer") or "Bearer").strip(),
        expires_at=expires_at,
        manual_long_lived_token=False,
        last_code=str(code or "").strip(),
        last_state=str(state or "").strip(),
        last_referer=str(referer or "").strip(),
        updated_at=_utc_now_iso(),
    )


def build_state_from_manual_token(*, base_domain: str, access_token: str) -> AmoAuthState:
    return AmoAuthState(
        base_domain=str(base_domain or "").strip(),
        access_token=str(access_token or "").strip(),
        refresh_token="",
        token_type="Bearer",
        expires_at="",
        manual_long_lived_token=True,
        updated_at=_utc_now_iso(),
    )


def load_auth_state(path: Path) -> AmoAuthState:
    if not path.exists():
        return AmoAuthState()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"Invalid amoCRM auth state format: {path}")
    return AmoAuthState(
        base_domain=str(payload.get("base_domain", "") or ""),
        access_token=str(payload.get("access_token", "") or ""),
        refresh_token=str(payload.get("refresh_token", "") or ""),
        token_type=str(payload.get("token_type", "Bearer") or "Bearer"),
        expires_at=str(payload.get("expires_at", "") or ""),
        manual_long_lived_token=bool(payload.get("manual_long_lived_token", False)),
        last_code=str(payload.get("last_code", "") or ""),
        last_state=str(payload.get("last_state", "") or ""),
        last_referer=str(payload.get("last_referer", "") or ""),
        updated_at=str(payload.get("updated_at", "") or ""),
    )


def save_auth_state(path: Path, state: AmoAuthState) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(state)
    payload["updated_at"] = _utc_now_iso()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
