from __future__ import annotations

import argparse

from src.config import load_config
from src.logger import setup_logging

from .config import load_amocrm_auth_config
from .oauth_client import AmoOAuthClient
from .state_store import build_state_from_token_response, load_auth_state, save_auth_state


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="amoCRM API smoke test")
    parser.add_argument("--config", default="", help="Path to amocrm auth config JSON")
    parser.add_argument("--base-domain", default="", help="Optional base domain override")
    parser.add_argument("--refresh-if-needed", action="store_true", help="Try refresh token when access token expired")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_amocrm_auth_config(args.config or None)
    state = load_auth_state(cfg.state_path)
    oauth = AmoOAuthClient(cfg)

    base_domain = (args.base_domain or state.base_domain or cfg.base_domain).strip()
    if not base_domain:
        raise RuntimeError("Base domain is empty. Provide --base-domain or configure AMOCRM_BASE_DOMAIN")

    if not state.access_token.strip():
        raise RuntimeError(f"No access token found in state file: {cfg.state_path}")

    access_token = state.access_token

    if args.refresh_if_needed and state.is_expired() and state.refresh_token:
        logger.info("Access token expired, attempting refresh")
        token_payload = oauth.refresh_access_token(refresh_token=state.refresh_token, base_domain=base_domain)
        state = build_state_from_token_response(
            base_domain=base_domain,
            token_payload=token_payload,
            code=state.last_code,
            state=state.last_state,
            referer=state.last_referer,
        )
        save_auth_state(cfg.state_path, state)
        access_token = state.access_token
        logger.info("Access token refreshed and saved: %s", cfg.state_path)

    payload = oauth.smoke_test_account(base_domain=base_domain, access_token=access_token)
    account_id = payload.get("id", "")
    account_name = payload.get("name", "")
    users = payload.get("_embedded", {}).get("users", []) if isinstance(payload.get("_embedded", {}), dict) else []
    logger.info(
        "amoCRM API smoke test success: base_domain=%s account_id=%s account_name=%s users_count=%s",
        base_domain,
        account_id,
        account_name,
        len(users) if isinstance(users, list) else 0,
    )


if __name__ == "__main__":
    main()
