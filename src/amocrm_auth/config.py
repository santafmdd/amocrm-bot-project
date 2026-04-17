from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from src.config import load_config
from src.safety import ensure_inside_root


@dataclass(frozen=True)
class AmoAuthConfig:
    base_domain: str
    client_id: str
    client_secret: str
    redirect_uri: str
    callback_host: str
    callback_port: int
    callback_path: str
    config_path: Path
    state_path: Path


def _normalize_base_domain(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        return parsed.netloc.strip("/")
    return raw.strip("/")


def _default_paths() -> tuple[Path, Path, Path]:
    app = load_config()
    config_path = ensure_inside_root(app.project_root / "config" / "amocrm_auth.local.json", app.project_root)
    state_path = ensure_inside_root(app.workspace_dir / "amocrm_auth" / "state.json", app.project_root)
    return app.project_root, config_path, state_path


def load_amocrm_auth_config(config_path: str | None = None) -> AmoAuthConfig:
    project_root, default_config_path, default_state_path = _default_paths()

    cfg_path = Path(config_path).resolve() if config_path else default_config_path
    cfg_path = ensure_inside_root(cfg_path, project_root)

    file_cfg: dict[str, object] = {}
    if cfg_path.exists():
        # utf-8-sig keeps loading resilient for both BOM and non-BOM UTF-8 files.
        file_cfg = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
        if not isinstance(file_cfg, dict):
            raise RuntimeError(f"Invalid amoCRM auth config format: {cfg_path}")

    redirect_uri = str(os.getenv("AMOCRM_REDIRECT_URI") or file_cfg.get("redirect_uri", "")).strip()
    callback_host = str(os.getenv("AMOCRM_CALLBACK_HOST") or file_cfg.get("callback_host", "127.0.0.1")).strip() or "127.0.0.1"
    callback_port_raw = os.getenv("AMOCRM_CALLBACK_PORT") or file_cfg.get("callback_port", 18080)
    callback_port = int(callback_port_raw)
    callback_path = str(os.getenv("AMOCRM_CALLBACK_PATH") or file_cfg.get("callback_path", "/oauth/callback")).strip() or "/oauth/callback"
    if not callback_path.startswith("/"):
        callback_path = "/" + callback_path

    if not redirect_uri:
        redirect_uri = f"http://{callback_host}:{callback_port}{callback_path}"

    state_path_raw = os.getenv("AMOCRM_AUTH_STATE_FILE") or file_cfg.get("state_path", str(default_state_path))
    state_path = ensure_inside_root(Path(str(state_path_raw)).resolve(), project_root)

    base_domain = _normalize_base_domain(os.getenv("AMOCRM_BASE_DOMAIN") or str(file_cfg.get("base_domain", "")))

    client_id = str(os.getenv("AMOCRM_CLIENT_ID") or file_cfg.get("client_id", "")).strip()
    client_secret = str(os.getenv("AMOCRM_CLIENT_SECRET") or file_cfg.get("client_secret", "")).strip()

    return AmoAuthConfig(
        base_domain=base_domain,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        callback_host=callback_host,
        callback_port=callback_port,
        callback_path=callback_path,
        config_path=cfg_path,
        state_path=state_path,
    )
