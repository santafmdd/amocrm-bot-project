from __future__ import annotations

import argparse
import webbrowser

from src.config import load_config
from src.logger import setup_logging

from .config import load_amocrm_auth_config
from .local_callback_server import LocalCallbackServer
from .oauth_client import AmoOAuthClient
from .state_store import build_state_from_manual_token, build_state_from_token_response, save_auth_state


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="amoCRM OAuth bootstrap (external integration)")
    parser.add_argument("--config", default="", help="Path to amocrm auth config JSON")
    parser.add_argument("--base-domain", default="", help="amoCRM base domain, e.g. youraccount.amocrm.ru")
    parser.add_argument("--timeout", type=int, default=300, help="Callback wait timeout in seconds")
    parser.add_argument("--no-browser", action="store_true", help="Do not auto-open browser, only print auth URL")
    parser.add_argument(
        "--set-long-lived-token",
        default="",
        help="Store manual long-lived access token instead of OAuth flow",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = load_config()
    logger = setup_logging(app.logs_dir)

    cfg = load_amocrm_auth_config(args.config or None)
    base_domain = (args.base_domain or cfg.base_domain).strip()

    if args.set_long_lived_token:
        if not base_domain:
            raise RuntimeError("--base-domain is required with --set-long-lived-token")
        state = build_state_from_manual_token(base_domain=base_domain, access_token=args.set_long_lived_token)
        path = save_auth_state(cfg.state_path, state)
        logger.info("amoCRM manual token saved: %s", path)
        logger.info("Run smoke test: python -m src.amocrm_auth.smoke_test --base-domain %s", base_domain)
        return

    oauth = AmoOAuthClient(cfg)
    auth_url, expected_state = oauth.build_authorize_url(base_domain=base_domain)

    server = LocalCallbackServer(
        host=cfg.callback_host,
        port=cfg.callback_port,
        path=cfg.callback_path,
        logger=logger,
    )

    try:
        server.start()
        logger.info("Open this URL to authorize amoCRM integration:\n%s", auth_url)
        if not args.no_browser:
            webbrowser.open(auth_url)

        callback = server.wait_for_code(timeout_seconds=args.timeout)
        if not callback.code:
            raise RuntimeError("Callback received but code is empty")
        if callback.state and callback.state != expected_state:
            raise RuntimeError(f"State mismatch: expected={expected_state} got={callback.state}")

        token_payload = oauth.exchange_code(code=callback.code, base_domain=base_domain)
        state = build_state_from_token_response(
            base_domain=base_domain,
            token_payload=token_payload,
            code=callback.code,
            state=callback.state,
            referer=callback.referer,
        )
        state_path = save_auth_state(cfg.state_path, state)
        logger.info("amoCRM OAuth tokens saved: %s", state_path)

        smoke = oauth.smoke_test_account(base_domain=base_domain, access_token=state.access_token)
        account_id = smoke.get("id", "")
        account_name = smoke.get("name", "")
        logger.info("amoCRM smoke test success: account_id=%s account_name=%s", account_id, account_name)

    finally:
        server.stop()


if __name__ == "__main__":
    main()
