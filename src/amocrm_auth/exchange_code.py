from __future__ import annotations

import argparse

from src.config import load_config
from src.logger import setup_logging

from .config import load_amocrm_auth_config
from .oauth_client import AmoOAuthClient
from .state_store import build_state_from_token_response, save_auth_state


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Exchange amoCRM OAuth authorization code for tokens")
    parser.add_argument("--code", required=True, help="Authorization code from callback")
    parser.add_argument("--state", default="", help="Optional state value")
    parser.add_argument("--referer", default="", help="Optional referer from callback")
    parser.add_argument("--base-domain", default="", help="amoCRM base domain override")
    parser.add_argument("--config", default="", help="Path to amocrm auth config JSON")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_amocrm_auth_config(args.config or None)
    base_domain = (args.base_domain or cfg.base_domain).strip()
    if not base_domain:
        raise RuntimeError("base_domain is required (config or --base-domain)")

    oauth = AmoOAuthClient(cfg)
    token_payload = oauth.exchange_code(code=args.code, base_domain=base_domain)
    state = build_state_from_token_response(
        base_domain=base_domain,
        token_payload=token_payload,
        code=args.code,
        state=args.state,
        referer=args.referer,
    )
    save_auth_state(cfg.state_path, state)
    logger.info("amoCRM auth state saved: %s", cfg.state_path)


if __name__ == "__main__":
    main()
